"""
HR Process Timeline Analyzer
----------------------------
For HR domain only. Events observed from columns and row data across DB/tables.

Goal:
- Find user column (employee_id, emp_id, user_id).
- Case IDs: sorted by datetime ascending.
- Each user can have multiple case IDs (timestamp-wise split).
- Events from event_name column (row data) OR timestamp column name OR row value patterns.
- 92 HR events across 7 categories: Recruitment, Onboarding, Attendance, Payroll, Performance, Training, Exit.
"""

from typing import Dict, List, Any, Optional, Tuple

import pandas as pd

from models import TableAnalysis


HR_CASE_GAP_HOURS = 24.0

# HR process categories and valid sequences
HR_PROCESS_CATEGORIES = {
    "Recruitment": [
        "Job Requisition creation", "Job posting / advertisement", "Candidate application received",
        "Resume shortlisted", "Screening completed", "Interview scheduled", "Interview conducted",
        "Interview feedback recorded", "HR discussion", "Salary negotiation", "Offer letter generated",
        "Offer letter sent", "Candidate accepted offer", "Candidate rejected offer", "Candidate dropped", "Position closed"
    ],
    "Onboarding": [
        "Employee profile creation", "Employee ID generated", "Document submission", "Document verification",
        "Background verification initiated", "Background verification completed", "Medical check",
        "Joining date confirmation", "Employee induction", "Orientation program", "System access request",
        "System access provided", "Asset allocation", "Employee activated"
    ],
    "Attendance": [
        "Clock-in", "Clock-out", "Shift assigned", "Shift changed", "Late entry marked", "Early exit marked",
        "Overtime marked", "Leave application submitted", "Leave approved", "Leave rejected", "Leave cancelled",
        "Work from home applied", "Work from home approved", "Holiday marked"
    ],
    "Payroll": [
        "Salary structure created", "Monthly payroll initiated", "Attendance locked", "Salary calculated",
        "Tax calculated", "Deduction applied", "Bonus allocated", "Incentive allocated", "Payroll approved",
        "Salary processed", "Bank transfer initiated", "Salary credited", "Payslip generated", "Payslip published"
    ],
    "Performance": [
        "Goal creation", "Goal assigned", "Goal updated", "Mid-year review started", "Self-assessment submitted",
        "Manager review submitted", "Feedback discussion", "Rating assigned", "Final review approved",
        "Promotion initiated", "Promotion approved", "Increment initiated", "Increment approved"
    ],
    "Training": [
        "Training need identified", "Training program created", "Trainer assigned", "Employees nominated",
        "Training enrollment", "Training started", "Training attendance", "Training completed",
        "Certification issued", "Post-training feedback"
    ],
    "Exit": [
        "Resignation submitted", "Resignation acknowledged", "Notice period started", "Exit interview scheduled",
        "Exit interview conducted", "Knowledge transfer", "Asset return", "Access removal",
        "Full & Final settlement initiated", "Full & Final settlement completed", "Employee status closed"
    ]
}

# Valid process flow order (high-level)
VALID_PROCESS_FLOW = ["Recruitment", "Onboarding", "Attendance", "Payroll", "Performance", "Training", "Exit"]

# Impossible event sequences (events that cannot follow each other)
IMPOSSIBLE_SEQUENCES = [
    # Exit events cannot be followed by onboarding events (except re-joining)
    ("Employee status closed", "Employee profile creation"),
    ("Employee status closed", "Employee ID generated"),
    # Recruitment events cannot follow exit
    ("Employee status closed", "Job posting / advertisement"),
    # Onboarding must come before activation
    ("Employee activated", "Document submission"),
    ("Employee activated", "Background verification initiated"),
    # Exit cannot happen before onboarding
    ("Resignation submitted", "Employee profile creation"),
    ("Resignation submitted", "Employee ID generated"),
]

# HR event column patterns - observed from data (columns + row values)
HR_EVENT_COLUMN_PATTERNS = [
    # Recruitment
    "job_requisition", "job_posting", "job_advertisement", "candidate_application", "resume_shortlisted",
    "screening_completed", "interview_scheduled", "interview_conducted", "interview_feedback", "hr_discussion",
    "salary_negotiation", "offer_letter_generated", "offer_letter_sent", "candidate_accepted", "candidate_rejected",
    "candidate_dropped", "position_closed",
    # Onboarding
    "employee_profile_creation", "employee_id_generated", "document_submission", "document_verification",
    "background_verification", "medical_check", "joining_date_confirmation", "employee_induction",
    "orientation_program", "system_access_request", "system_access_provided", "asset_allocation", "employee_activated",
    # Attendance
    "clock_in", "clock_out", "clockin", "clockout", "shift_assigned", "shift_changed", "late_entry", "early_exit",
    "overtime", "leave_application", "leave_approved", "leave_rejected", "leave_cancelled", "work_from_home",
    "holiday_marked",
    # Payroll
    "salary_structure", "monthly_payroll", "attendance_locked", "salary_calculated", "tax_calculated",
    "deduction_applied", "bonus_allocated", "incentive_allocated", "payroll_approved", "salary_processed",
    "bank_transfer", "salary_credited", "payslip_generated", "payslip_published",
    # Performance
    "goal_creation", "goal_assigned", "goal_updated", "mid_year_review", "self_assessment", "manager_review",
    "feedback_discussion", "rating_assigned", "final_review", "promotion_initiated", "promotion_approved",
    "increment_initiated", "increment_approved",
    # Training
    "training_need", "training_program", "trainer_assigned", "employees_nominated", "training_enrollment",
    "training_started", "training_attendance", "training_completed", "certification_issued", "post_training_feedback",
    # Exit
    "resignation_submitted", "resignation_acknowledged", "notice_period", "exit_interview", "knowledge_transfer",
    "asset_return", "access_removal", "full_final_settlement", "employee_status_closed",
    # Generic
    "event_time", "created_at", "timestamp", "activity_date", "record_date",
]

# Valid HR event display names (92 events)
HR_EVENT_DISPLAY = [
    # Recruitment (16 events)
    "Job Requisition creation",
    "Job posting / advertisement",
    "Candidate application received",
    "Resume shortlisted",
    "Screening completed",
    "Interview scheduled",
    "Interview conducted",
    "Interview feedback recorded",
    "HR discussion",
    "Salary negotiation",
    "Offer letter generated",
    "Offer letter sent",
    "Candidate accepted offer",
    "Candidate rejected offer",
    "Candidate dropped",
    "Position closed",
    # Onboarding (14 events)
    "Employee profile creation",
    "Employee ID generated",
    "Document submission",
    "Document verification",
    "Background verification initiated",
    "Background verification completed",
    "Medical check",
    "Joining date confirmation",
    "Employee induction",
    "Orientation program",
    "System access request",
    "System access provided",
    "Asset allocation",
    "Employee activated",
    # Attendance (14 events)
    "Clock-in",
    "Clock-out",
    "Shift assigned",
    "Shift changed",
    "Late entry marked",
    "Early exit marked",
    "Overtime marked",
    "Leave application submitted",
    "Leave approved",
    "Leave rejected",
    "Leave cancelled",
    "Work from home applied",
    "Work from home approved",
    "Holiday marked",
    # Payroll (14 events)
    "Salary structure created",
    "Monthly payroll initiated",
    "Attendance locked",
    "Salary calculated",
    "Tax calculated",
    "Deduction applied",
    "Bonus allocated",
    "Incentive allocated",
    "Payroll approved",
    "Salary processed",
    "Bank transfer initiated",
    "Salary credited",
    "Payslip generated",
    "Payslip published",
    # Performance (13 events)
    "Goal creation",
    "Goal assigned",
    "Goal updated",
    "Mid-year review started",
    "Self-assessment submitted",
    "Manager review submitted",
    "Feedback discussion",
    "Rating assigned",
    "Final review approved",
    "Promotion initiated",
    "Promotion approved",
    "Increment initiated",
    "Increment approved",
    # Training (10 events)
    "Training need identified",
    "Training program created",
    "Trainer assigned",
    "Employees nominated",
    "Training enrollment",
    "Training started",
    "Training attendance",
    "Training completed",
    "Certification issued",
    "Post-training feedback",
    # Exit (11 events)
    "Resignation submitted",
    "Resignation acknowledged",
    "Notice period started",
    "Exit interview scheduled",
    "Exit interview conducted",
    "Knowledge transfer",
    "Asset return",
    "Access removal",
    "Full & Final settlement initiated",
    "Full & Final settlement completed",
    "Employee status closed",
]


class HRTimelineAnalyzer:
    """
    Analyzes HR tables. Events from event_name column (row data),
    timestamp column name, or row value pattern match. Case ID sorted by time.
    """

    def __init__(self) -> None:
        self.step_order = list(HR_EVENT_DISPLAY)
        # Build event to category mapping
        self.event_to_category = {}
        for category, events in HR_PROCESS_CATEGORIES.items():
            for event in events:
                self.event_to_category[event] = category
        # Data-driven pattern learning
        self.observed_patterns: Dict[str, Dict[str, Any]] = {}  # column_name -> {patterns}
        self.value_to_event_map: Dict[str, str] = {}  # observed_value -> event_name

    def _time_column_to_hr_event(self, col_name: str) -> str:
        """Derive event from timestamp column name with confidence scoring."""
        if not col_name or not str(col_name).strip():
            return "Employee profile creation"
        c = str(col_name).lower().replace("-", "_").replace(" ", "_")
        for suf in ["_time", "_date", "_timestamp", "_at", "_datetime"]:
            if c.endswith(suf):
                c = c[: -len(suf)]
                break
        
        # Map column names to HR events based on patterns (strict mapping to avoid wrong events)
        m = {
            # Recruitment
            "job_requisition": "Job Requisition creation",
            "job_posting": "Job posting / advertisement",
            "job_advertisement": "Job posting / advertisement",
            "candidate_application": "Candidate application received",
            "resume_shortlisted": "Resume shortlisted",
            "screening_completed": "Screening completed",
            "interview_scheduled": "Interview scheduled",
            "interview_conducted": "Interview conducted",
            "interview_feedback": "Interview feedback recorded",
            "hr_discussion": "HR discussion",
            "salary_negotiation": "Salary negotiation",
            "offer_letter_generated": "Offer letter generated",
            "offer_letter_sent": "Offer letter sent",
            "candidate_accepted": "Candidate accepted offer",
            "candidate_rejected": "Candidate rejected offer",
            "candidate_dropped": "Candidate dropped",
            "position_closed": "Position closed",
            # Onboarding
            "employee_profile": "Employee profile creation",
            "employee_id_generated": "Employee ID generated",
            "document_submission": "Document submission",
            "document_verification": "Document verification",
            "background_verification": "Background verification completed",
            "medical_check": "Medical check",
            "joining_date": "Joining date confirmation",
            "employee_induction": "Employee induction",
            "orientation": "Orientation program",
            "system_access_request": "System access request",
            "system_access_provided": "System access provided",
            "asset_allocation": "Asset allocation",
            "employee_activated": "Employee activated",
            # Attendance
            "clock_in": "Clock-in",
            "clock_out": "Clock-out",
            "clockin": "Clock-in",
            "clockout": "Clock-out",
            "shift_assigned": "Shift assigned",
            "shift_changed": "Shift changed",
            "late_entry": "Late entry marked",
            "early_exit": "Early exit marked",
            "overtime": "Overtime marked",
            "leave_application": "Leave application submitted",
            "leave_approved": "Leave approved",
            "leave_rejected": "Leave rejected",
            "leave_cancelled": "Leave cancelled",
            "work_from_home": "Work from home applied",
            "holiday": "Holiday marked",
            # Payroll
            "salary_structure": "Salary structure created",
            "monthly_payroll": "Monthly payroll initiated",
            "attendance_locked": "Attendance locked",
            "salary_calculated": "Salary calculated",
            "tax_calculated": "Tax calculated",
            "deduction_applied": "Deduction applied",
            "bonus_allocated": "Bonus allocated",
            "incentive_allocated": "Incentive allocated",
            "payroll_approved": "Payroll approved",
            "salary_processed": "Salary processed",
            "bank_transfer": "Bank transfer initiated",
            "salary_credited": "Salary credited",
            "payslip_generated": "Payslip generated",
            "payslip_published": "Payslip published",
            # Performance
            "goal_creation": "Goal creation",
            "goal_assigned": "Goal assigned",
            "goal_updated": "Goal updated",
            "mid_year_review": "Mid-year review started",
            "self_assessment": "Self-assessment submitted",
            "manager_review": "Manager review submitted",
            "feedback_discussion": "Feedback discussion",
            "rating_assigned": "Rating assigned",
            "final_review": "Final review approved",
            "promotion_initiated": "Promotion initiated",
            "promotion_approved": "Promotion approved",
            "increment_initiated": "Increment initiated",
            "increment_approved": "Increment approved",
            # Training
            "training_need": "Training need identified",
            "training_program": "Training program created",
            "trainer_assigned": "Trainer assigned",
            "employees_nominated": "Employees nominated",
            "training_enrollment": "Training enrollment",
            "training_started": "Training started",
            "training_attendance": "Training attendance",
            "training_completed": "Training completed",
            "certification_issued": "Certification issued",
            "post_training_feedback": "Post-training feedback",
            # Exit
            "resignation_submitted": "Resignation submitted",
            "resignation_acknowledged": "Resignation acknowledged",
            "notice_period": "Notice period started",
            "exit_interview": "Exit interview scheduled",
            "knowledge_transfer": "Knowledge transfer",
            "asset_return": "Asset return",
            "access_removal": "Access removal",
            "full_final_settlement": "Full & Final settlement initiated",
            "employee_status_closed": "Employee status closed",
        }
        
        if c in m:
            return m[c]
        
        # Pattern matching (not hardcoded - flexible matching)
        for key, ev in m.items():
            if key in c:
                return ev
        
        # Conservative category-based fallback - only use if column name is very specific
        # Avoid generic mappings like "salary_col" -> "Salary processed"
        
        # Only apply fallback if column name contains specific event indicators
        if "recruitment" in c or "job" in c or "candidate" in c or "interview" in c or "offer" in c:
            # Only map if very specific
            if "offer" in c and ("sent" in c or "generated" in c):
                return "Offer letter sent" if "sent" in c else "Offer letter generated"
            if "interview" in c and ("scheduled" in c or "conducted" in c or "feedback" in c):
                return "Interview conducted" if "conducted" in c else ("Interview feedback recorded" if "feedback" in c else "Interview scheduled")
            # Don't map generic "job" or "candidate" columns
            if "job_posting" in c or "job_advertisement" in c:
                return "Job posting / advertisement"
        
        if "onboarding" in c or "employee_profile" in c or "joining" in c or "induction" in c:
            if "joining" in c and "date" in c:
                return "Joining date confirmation"
            if ("induction" in c or "orientation" in c) and ("date" in c or "time" in c):
                return "Employee induction"
            # Don't map generic "employee" columns
            if "employee_profile" in c and ("creation" in c or "created" in c):
                return "Employee profile creation"
        
        if "attendance" in c or "clock" in c or "shift" in c or "leave" in c:
            if "clock" in c and ("in" in c or "out" in c):
                return "Clock-out" if "out" in c else "Clock-in"
            if "leave" in c and ("approved" in c or "rejected" in c or "application" in c):
                return "Leave approved" if "approved" in c else ("Leave rejected" if "rejected" in c else "Leave application submitted")
            if "shift" in c and ("assigned" in c or "changed" in c):
                return "Shift assigned" if "assigned" in c else "Shift changed"
        
        if "payroll" in c or "salary" in c or "payslip" in c:
            # Be very conservative - don't map generic "salary" columns
            if "payslip" in c and ("generated" in c or "published" in c):
                return "Payslip published" if "published" in c else "Payslip generated"
            if "salary" in c and ("processed" in c or "calculated" in c or "structure" in c):
                return "Salary processed" if "processed" in c else ("Salary calculated" if "calculated" in c else "Salary structure created")
            if "payroll" in c and ("initiated" in c or "approved" in c):
                return "Monthly payroll initiated" if "initiated" in c else "Payroll approved"
        
        if "performance" in c or "goal" in c or "review" in c or "rating" in c or "promotion" in c:
            if "promotion" in c and ("initiated" in c or "approved" in c):
                return "Promotion approved" if "approved" in c else "Promotion initiated"
            if "goal" in c and ("creation" in c or "assigned" in c or "updated" in c):
                return "Goal updated" if "updated" in c else ("Goal assigned" if "assigned" in c else "Goal creation")
            if "review" in c and ("mid" in c or "final" in c or "self" in c):
                return "Final review approved" if "final" in c else ("Mid-year review started" if "mid" in c else "Self-assessment submitted")
        
        if "training" in c or "certification" in c:
            if "certification" in c and ("issued" in c or "date" in c):
                return "Certification issued"
            if "training" in c and ("started" in c or "completed" in c or "enrollment" in c):
                return "Training started" if "started" in c else ("Training completed" if "completed" in c else "Training enrollment")
        
        if "exit" in c or "resignation" in c or "settlement" in c:
            if "resignation" in c and ("submitted" in c or "acknowledged" in c):
                return "Resignation acknowledged" if "acknowledged" in c else "Resignation submitted"
            if "settlement" in c and ("initiated" in c or "completed" in c):
                return "Full & Final settlement completed" if "completed" in c else "Full & Final settlement initiated"
            if "employee_status" in c and "closed" in c:
                return "Employee status closed"
        
        # Default fallback only if no specific pattern matched
        return "Employee profile creation"

    def _find_user_col(self, df: pd.DataFrame) -> Optional[str]:
        """Dynamically detect user/employee column based on data patterns, not hardcoded names."""
        # First check: columns with user/employee keywords
        candidates = [
            "employee_id", "emp_id", "employeeid", "empid", "empcode", "emp_code",
            "user_id", "staff_id", "personnel_id", "user", "employee", "emp",
        ]
        cols_lower = {c.lower(): c for c in df.columns}
        for cand in candidates:
            if cand in cols_lower:
                col = cols_lower[cand]
                # Verify it has reasonable data (not all nulls, has some unique values)
                if df[col].notna().sum() > 0 and df[col].nunique() > 1:
                    return col
        
        # Second check: columns with user/employee keywords in name
        for col in df.columns:
            cl = col.lower()
            if any(k in cl for k in ["employee", "emp", "staff", "personnel", "user"]) and "id" in cl:
                if df[col].notna().sum() > 0 and df[col].nunique() > 1:
                    return col
        
        # Third check: Dynamic detection based on data patterns
        # Look for columns that behave like user IDs:
        # - High uniqueness (each row has different value or repeats per user)
        # - Not a date/time column
        # - Not an event/status column
        # - Reasonable number of unique values (not too many, not too few)
        
        for col in df.columns:
            # Skip obvious non-user columns
            cl = col.lower()
            if any(k in cl for k in ["date", "time", "timestamp", "event", "action", "status", "activity"]):
                continue
            
            # Skip if all nulls
            if df[col].notna().sum() == 0:
                continue
            
            # Check uniqueness pattern
            unique_count = df[col].nunique()
            total_count = len(df[col].dropna())
            
            # User ID columns typically have:
            # - Multiple unique values (at least 2, but not every row unique unless small dataset)
            # - Reasonable ratio of unique to total (between 0.1 and 1.0)
            if unique_count >= 2 and total_count > 0:
                uniqueness_ratio = unique_count / total_count
                
                # Check if values look like IDs (numeric strings, codes, etc.)
                sample_values = df[col].dropna().head(20).astype(str)
                id_like_count = 0
                for val in sample_values:
                    val_str = str(val).strip()
                    # ID-like patterns: numeric, alphanumeric codes, UUID-like
                    if (val_str.isdigit() or 
                        (val_str.replace("-", "").replace("_", "").isalnum() and len(val_str) >= 3) or
                        len(val_str) >= 8):  # UUID-like or long codes
                        id_like_count += 1
                
                # If >50% look like IDs and has reasonable uniqueness, it's likely a user column
                if id_like_count / len(sample_values) > 0.5 and uniqueness_ratio >= 0.1:
                    return col
        
        # Last resort: first non-date, non-event column with some uniqueness
        for col in df.columns:
            cl = col.lower()
            if any(k in cl for k in ["date", "time", "timestamp", "event", "action", "status"]):
                continue
            if df[col].notna().sum() > 0 and df[col].nunique() >= 2:
                return col
        
        return None

    def _learn_patterns_from_data(self, df: pd.DataFrame) -> None:
        """Learn event patterns from actual data values - data-driven approach."""
        # Scan all columns to find event-like values
        for col in df.columns:
            if col.startswith("__"):
                continue
            
            # Skip date/time columns
            col_lower = col.lower()
            if any(kw in col_lower for kw in ["date", "time", "timestamp", "_at"]):
                continue
            
            # Skip pure numeric columns
            if df[col].dtype.kind in ("i", "u", "f", "b"):
                continue
            
            # Get unique values from this column
            unique_vals = df[col].dropna().astype(str).unique()
            
            # Check each value to see if it matches any HR event
            for val in unique_vals[:100]:  # Limit to first 100 unique values
                val_str = str(val).strip()
                if not val_str or len(val_str) < 3:
                    continue
                
                # Skip pure numeric IDs
                if val_str.isdigit() and len(val_str) > 10:
                    continue
                
                # Try to match this value to an HR event
                matched_event = self._match_value_to_event(val_str)
                if matched_event and matched_event != "Employee profile creation":
                    # Store the mapping
                    val_normalized = val_str.lower().strip()
                    if val_normalized not in self.value_to_event_map:
                        self.value_to_event_map[val_normalized] = matched_event
                    
                    # Store pattern for this column
                    if col not in self.observed_patterns:
                        self.observed_patterns[col] = {
                            "values": [],
                            "matched_events": set(),
                            "confidence": 0.0
                        }
                    
                    self.observed_patterns[col]["values"].append(val_str)
                    self.observed_patterns[col]["matched_events"].add(matched_event)
            
            # Calculate confidence for this column
            if col in self.observed_patterns:
                total_vals = len(unique_vals)
                matched_count = len(self.observed_patterns[col]["values"])
                if total_vals > 0:
                    self.observed_patterns[col]["confidence"] = matched_count / total_vals
    
    def _match_value_to_event(self, value: str) -> Optional[str]:
        """Match a data value to an HR event using flexible pattern matching."""
        if not value or len(value) < 3:
            return None
        
        val_lower = value.lower().strip()
        val_normalized = val_lower.replace(" ", "_").replace("-", "_").replace("/", "_")
        
        # Direct exact match first
        if val_normalized in self.value_to_event_map:
            return self.value_to_event_map[val_normalized]
        
        # Try to match against all HR events using flexible matching
        best_match = None
        best_score = 0.0
        
        for event_name in HR_EVENT_DISPLAY:
            event_normalized = event_name.lower().replace(" ", "_").replace("/", "_").replace("&", "_").replace("-", "_")
            
            # Exact match
            if val_normalized == event_normalized:
                return event_name
            
            # Check if value contains event keywords or vice versa
            val_words = set(val_normalized.split("_"))
            event_words = set(event_normalized.split("_"))
            
            # Remove common words
            common_words = {"the", "a", "an", "and", "or", "of", "in", "on", "at", "by", "for", "to"}
            val_words = val_words - common_words
            event_words = event_words - common_words
            
            if not val_words or not event_words:
                continue
            
            # Calculate similarity score
            intersection = val_words & event_words
            union = val_words | event_words
            
            if union:
                score = len(intersection) / len(union)
                
                # Boost score if key words match
                key_words = ["leave", "interview", "offer", "resignation", "training", "promotion", 
                           "clock", "shift", "salary", "payroll", "goal", "review", "exit"]
                for kw in key_words:
                    if kw in val_words and kw in event_words:
                        score += 0.2
                        break
                
                if score > best_score and score >= 0.3:  # At least 30% match
                    best_match = event_name
                    best_score = score
        
        return best_match if best_score >= 0.3 else None
    
    def _find_event_name_col(self, df: pd.DataFrame) -> Optional[str]:
        """Detect column with event type as DATA (row values). Check row values match HR event patterns."""
        # Prefer obvious event columns by name first
        candidates = [
            "event_name", "event_type", "action", "event",
            "step_name", "activity", "status", "activity_type",
            "stage", "phase", "milestone", "event_description", "activity_name",
        ]
        cols_lower = {c.lower(): c for c in df.columns}
        for cand in candidates:
            if cand in cols_lower:
                col = cols_lower[cand]
                sample = df[col].dropna().astype(str).head(50)
                if len(sample) == 0:
                    continue
                vals = set(s.strip() for v in sample for s in str(v).split(",") if s.strip())
                if not vals:
                    continue
                # Row data pattern: values that look like HR events
                low_vals = [v.lower().replace(" ", "_").replace("-", "_") for v in vals]
                valid = sum(
                    1 for v, lv in zip(vals, low_vals)
                    if self._normalize_event_from_data(v) in HR_EVENT_DISPLAY
                    or any(tok in lv for tok in [
                        "recruitment", "job", "candidate", "interview", "offer",
                        "onboarding", "employee_profile", "joining", "induction", "document",
                        "attendance", "clock", "shift", "leave", "overtime",
                        "payroll", "salary", "payslip", "tax", "deduction", "bonus",
                        "performance", "goal", "review", "rating", "promotion", "increment",
                        "training", "certification",
                        "exit", "resignation", "settlement",
                    ])
                )
                # Be tolerant: even 1 strong HR-like value is enough to treat this as event column
                if valid >= 1:
                    return col
        # Prefer event_type, action, activity over event_id by name even if we
        # couldn't strongly classify by values.
        prefer = ["event_type", "event_name", "action", "activity", "step_name", "activity_type", "status"]
        for p in prefer:
            if p in cols_lower:
                col = cols_lower[p]
                sample = df[col].dropna().astype(str).head(20)
                if len(sample) > 0:
                    return col
        # Last-resort heuristic: scan ALL non-numeric columns and pick the first
        # one whose values look like HR events.
        for col in df.columns:
            if df[col].dtype.kind in ("i", "u", "f", "b"):
                continue
            sample = df[col].dropna().astype(str).head(50)
            if len(sample) == 0:
                continue
            vals = set(s.strip() for v in sample for s in str(v).split(",") if s.strip())
            if not vals:
                continue
            hits = 0
            for v in vals:
                norm = self._normalize_event_from_data(v)
                if norm in HR_EVENT_DISPLAY:
                    hits += 1
                    break
            if hits:
                return col
        for col in df.columns:
            cl = col.lower()
            if cl == "event_id":
                continue
            if "event" in cl and "time" not in cl and "date" not in cl and not cl.endswith("_id"):
                return col
            if "action" in cl or "activity" in cl or "status" in cl:
                return col
        return None

    def _find_case_id_col(self, df: pd.DataFrame) -> Optional[str]:
        """Detect case_id for grouping."""
        cols_lower = {c.lower(): c for c in df.columns}
        for cand in ["case_id", "caseid", "session_id", "journey_id", "process_id"]:
            if cand in cols_lower:
                return cols_lower[cand]
        return None

    def _normalize_event_from_data(self, val: Any) -> str:
        """Normalize event name from row data using learned patterns first, then fallback."""
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return "Employee profile creation"
        s = str(val).strip()
        if not s:
            return "Employee profile creation"
        
        # FIRST: Check learned patterns from actual data (data-driven)
        val_normalized = s.lower().strip()
        if val_normalized in self.value_to_event_map:
            matched = self.value_to_event_map[val_normalized]
            if matched and matched != "Employee profile creation":
                return matched
        
        # Try pattern matching with learned method
        matched = self._match_value_to_event(s)
        if matched and matched != "Employee profile creation":
            return matched
        
        # SECOND: Direct exact match against HR events
        low = s.lower().replace(" ", "_").replace("-", "_").replace("/", "_")
        for disp in HR_EVENT_DISPLAY:
            key = disp.lower().replace(" ", "_").replace("/", "_").replace("&", "_")
            if key == low:
                return disp
        
        # Create reverse mapping from common patterns to exact names
        variant_map = {
            # Recruitment
            "job_requisition": "Job Requisition creation",
            "job_posting": "Job posting / advertisement",
            "job_advertisement": "Job posting / advertisement",
            "candidate_application": "Candidate application received",
            "resume_shortlisted": "Resume shortlisted",
            "screening_completed": "Screening completed",
            "interview_scheduled": "Interview scheduled",
            "interview_conducted": "Interview conducted",
            "interview_feedback": "Interview feedback recorded",
            "hr_discussion": "HR discussion",
            "salary_negotiation": "Salary negotiation",
            "offer_letter_generated": "Offer letter generated",
            "offer_letter_sent": "Offer letter sent",
            "candidate_accepted": "Candidate accepted offer",
            "candidate_rejected": "Candidate rejected offer",
            "candidate_dropped": "Candidate dropped",
            "position_closed": "Position closed",
            # Onboarding
            "employee_profile": "Employee profile creation",
            "employee_profile_creation": "Employee profile creation",
            "employee_id_generated": "Employee ID generated",
            "document_submission": "Document submission",
            "document_verification": "Document verification",
            "background_verification": "Background verification completed",
            "medical_check": "Medical check",
            "joining_date": "Joining date confirmation",
            "employee_induction": "Employee induction",
            "orientation": "Orientation program",
            "system_access_request": "System access request",
            "system_access_provided": "System access provided",
            "asset_allocation": "Asset allocation",
            "employee_activated": "Employee activated",
            # Attendance
            "clock_in": "Clock-in",
            "clock_out": "Clock-out",
            "clockin": "Clock-in",
            "clockout": "Clock-out",
            "shift_assigned": "Shift assigned",
            "shift_changed": "Shift changed",
            "late_entry": "Late entry marked",
            "early_exit": "Early exit marked",
            "overtime": "Overtime marked",
            "leave_application": "Leave application submitted",
            "leave_approved": "Leave approved",
            "leave_rejected": "Leave rejected",
            "leave_cancelled": "Leave cancelled",
            "work_from_home": "Work from home applied",
            "holiday": "Holiday marked",
            # Payroll
            "salary_structure": "Salary structure created",
            "monthly_payroll": "Monthly payroll initiated",
            "attendance_locked": "Attendance locked",
            "salary_calculated": "Salary calculated",
            "tax_calculated": "Tax calculated",
            "deduction_applied": "Deduction applied",
            "bonus_allocated": "Bonus allocated",
            "incentive_allocated": "Incentive allocated",
            "payroll_approved": "Payroll approved",
            "salary_processed": "Salary processed",
            "bank_transfer": "Bank transfer initiated",
            "salary_credited": "Salary credited",
            "payslip_generated": "Payslip generated",
            "payslip_published": "Payslip published",
            # Performance
            "goal_creation": "Goal creation",
            "goal_assigned": "Goal assigned",
            "goal_updated": "Goal updated",
            "mid_year_review": "Mid-year review started",
            "self_assessment": "Self-assessment submitted",
            "manager_review": "Manager review submitted",
            "feedback_discussion": "Feedback discussion",
            "rating_assigned": "Rating assigned",
            "final_review": "Final review approved",
            "promotion_initiated": "Promotion initiated",
            "promotion_approved": "Promotion approved",
            "increment_initiated": "Increment initiated",
            "increment_approved": "Increment approved",
            # Training
            "training_need": "Training need identified",
            "training_program": "Training program created",
            "trainer_assigned": "Trainer assigned",
            "employees_nominated": "Employees nominated",
            "training_enrollment": "Training enrollment",
            "training_started": "Training started",
            "training_attendance": "Training attendance",
            "training_completed": "Training completed",
            "certification_issued": "Certification issued",
            "post_training_feedback": "Post-training feedback",
            # Exit
            "resignation_submitted": "Resignation submitted",
            "resignation_acknowledged": "Resignation acknowledged",
            "notice_period": "Notice period started",
            "exit_interview": "Exit interview scheduled",
            "knowledge_transfer": "Knowledge transfer",
            "asset_return": "Asset return",
            "access_removal": "Access removal",
            "full_final_settlement": "Full & Final settlement initiated",
            "employee_status_closed": "Employee status closed",
        }
        
        # Check variant map
        if low in variant_map:
            return variant_map[low]
        
        # Check if any variant key is contained in the input
        for variant, display_name in variant_map.items():
            if variant in low or low in variant:
                return display_name
        
        # Last resort: check if any display name key matches
        for disp in HR_EVENT_DISPLAY:
            key = disp.lower().replace(" ", "_").replace("/", "_").replace("&", "_")
            if key in low or low in key:
                return disp
        
        # Default fallback
        return "Employee profile creation"

    def _find_datetime_columns(self, df: pd.DataFrame) -> Optional[Tuple[str, Optional[str]]]:
        """Find (date_col, time_col). More lenient detection for HR domain files."""

        def is_parseable(col: str) -> bool:
            """Check if column contains parseable date/time values. More lenient for HR domain."""
            try:
                sample = df[col].dropna().head(20)  # Check more samples
                if len(sample) == 0:
                    return False
                
                # Check for numeric-like date formats FIRST (YYYYMMDD, YYYYMM, Unix timestamps)
                numeric_date_count = 0
                for v in sample:
                    v_str = str(v).strip()
                    # Check for YYYYMMDD format (8 digits)
                    if v_str.isdigit() and len(v_str) == 8:
                        try:
                            pd.to_datetime(v_str, format="%Y%m%d", errors="strict")
                            numeric_date_count += 1
                            continue
                        except:
                            pass
                    # Check for YYYYMM format (6 digits)
                    if v_str.isdigit() and len(v_str) == 6:
                        try:
                            pd.to_datetime(v_str, format="%Y%m", errors="strict")
                            numeric_date_count += 1
                            continue
                        except:
                            pass
                    # Check for Unix timestamp (10-13 digits)
                    if v_str.isdigit() and 10 <= len(v_str) <= 13:
                        try:
                            ts = int(v_str)
                            if 946684800 <= ts <= 4102444800:  # Reasonable range (2000-2100)
                                numeric_date_count += 1
                                continue
                        except:
                            pass
                
                # If we found numeric dates, be more lenient - accept if column name suggests date OR if >30% are dates
                if numeric_date_count > 0:
                    col_lower = col.lower()
                    has_date_keyword = any(k in col_lower for k in ["date", "time", "timestamp", "_at", "created", "updated", "modified"])
                    date_ratio = numeric_date_count / len(sample)
                    
                    # Accept if: has date keyword OR >30% are parseable dates
                    if has_date_keyword or date_ratio >= 0.3:
                        return True
                
                # Check for string date formats
                parsed = pd.to_datetime(sample, errors="coerce")
                parseable_count = parsed.notna().sum()
                parseable_ratio = parseable_count / len(sample) if len(sample) > 0 else 0
                
                # More lenient: accept if at least 30% are parseable (was 50%)
                if parseable_ratio >= 0.3:
                    return True
                
                # Special case: if column name strongly suggests date/time, be even more lenient
                col_lower = col.lower()
                strong_date_keywords = ["date", "time", "timestamp", "created_at", "updated_at", "event_time"]
                if any(kw in col_lower for kw in strong_date_keywords) and parseable_ratio >= 0.2:
                    return True
                
                return False
            except Exception:
                return False

        def is_event_column(col: str) -> bool:
            """Check if column looks like an event column (not a timestamp column)."""
            cl = col.lower()
            # Event column patterns (without timestamp indicators)
            event_patterns = [
                "event_name", "event_type", "action", "activity", "status",
                "step_name", "activity_type", "stage", "phase", "milestone",
                "event_description", "activity_name"
            ]
            # If it has event-related keywords but NOT timestamp-related keywords, it's likely an event column
            has_event_keyword = any(pat in cl for pat in event_patterns)
            has_timestamp_keyword = any(k in cl for k in ["date", "time", "timestamp", "_at"])
            return has_event_keyword and not has_timestamp_keyword

        def is_numeric_column(col: str) -> bool:
            """Check if column is primarily numeric AND NOT a date. More lenient for HR domain."""
            col_lower = col.lower()
            
            # If column name suggests date/time, don't exclude it even if numeric
            date_keywords = ["date", "time", "timestamp", "_at", "created", "updated", "modified", 
                           "event_time", "activity_date", "record_date", "joining", "interview",
                           "leave", "payroll", "review", "training", "resignation"]
            if any(kw in col_lower for kw in date_keywords):
                # Check if it's actually a date format, not just a number
                sample = df[col].dropna().head(10)
                if len(sample) > 0:
                    # Check for date-like patterns
                    date_like_count = 0
                    for v in sample:
                        v_str = str(v).strip()
                        # Check for YYYYMMDD (8 digits), YYYYMM (6 digits), or Unix timestamp
                        if v_str.isdigit():
                            if len(v_str) == 8 or len(v_str) == 6 or (10 <= len(v_str) <= 13):
                                try:
                                    if len(v_str) == 8:
                                        pd.to_datetime(v_str, format="%Y%m%d", errors="strict")
                                    elif len(v_str) == 6:
                                        pd.to_datetime(v_str, format="%Y%m", errors="strict")
                                    elif 10 <= len(v_str) <= 13:
                                        ts = int(v_str)
                                        if 946684800 <= ts <= 4102444800:
                                            date_like_count += 1
                                            continue
                                except:
                                    pass
                        # Try parsing as date string
                        try:
                            pd.to_datetime(v_str, errors="coerce")
                            if not pd.isna(pd.to_datetime(v_str, errors="coerce")):
                                date_like_count += 1
                        except:
                            pass
                    
                    # If >50% look like dates, it's a date column, not numeric
                    if date_like_count / len(sample) > 0.5:
                        return False
            
            # Check dtype
            if df[col].dtype.kind in ("i", "u", "f", "b"):
                # For integer/float columns, check if they might be dates
                sample = df[col].dropna().head(20)
                if len(sample) > 0:
                    # Check for date-like integer patterns
                    date_like = 0
                    for v in sample:
                        v_str = str(int(v)) if isinstance(v, float) else str(v)
                        # Check for YYYYMMDD (8 digits) or YYYYMM (6 digits)
                        if v_str.isdigit() and (len(v_str) == 8 or len(v_str) == 6):
                            try:
                                if len(v_str) == 8:
                                    pd.to_datetime(v_str, format="%Y%m%d", errors="strict")
                                elif len(v_str) == 6:
                                    pd.to_datetime(v_str, format="%Y%m", errors="strict")
                                date_like += 1
                            except:
                                pass
                    # If >30% are date-like, don't exclude
                    if date_like / len(sample) > 0.3:
                        return False
                return True
            
            # Check sample values for string columns
            sample = df[col].dropna().head(20)
            if len(sample) == 0:
                return False
            numeric_count = sum(1 for v in sample if isinstance(v, (int, float)) or (isinstance(v, str) and v.replace(".", "").replace("-", "").isdigit()))
            return numeric_count >= len(sample) * 0.8

        # Only check for timestamp-related patterns, not HR event words
        preferred = [
            "event_time", "event_timestamp", "created_at", "timestamp", "updated_at",
            "activity_date", "activity_time", "record_date", "record_time",
            "clock_in_time", "clock_out_time", "clockin_time", "clockout_time",
            "joining_date", "joining_time", "interview_date", "interview_time",
            "leave_date", "leave_time", "payroll_date", "payroll_time",
            "review_date", "review_time", "training_date", "training_time",
            "resignation_date", "resignation_time",
        ]
        
        # First pass: Check preferred columns (most likely to be dates)
        for col in df.columns:
            cl = col.lower()
            # Skip event columns, but be lenient with numeric columns
            if is_event_column(col):
                continue
            # For preferred columns, check parseability even if numeric
            if any(k in cl for k in preferred):
                if is_parseable(col):
                    return (col, None)
        
        # Second pass: Check any column with date/time keywords
        date_candidates = []
        for c in df.columns:
            if is_event_column(c):
                continue
            cl = c.lower()
            if any(k in cl for k in ["date", "time", "timestamp", "_at", "created", "updated"]):
                if is_parseable(c):
                    date_candidates.append(c)
        
        if date_candidates:
            # Prefer columns with stronger date keywords
            strong_keywords = ["date", "time", "timestamp"]
            for candidate in date_candidates:
                if any(kw in candidate.lower() for kw in strong_keywords):
                    return (candidate, None)
            return (date_candidates[0], None)
        
        # Third pass: Check ALL columns (even numeric ones) if they're parseable
        # This is a fallback for cases where date columns don't have obvious names
        for col in df.columns:
            if is_event_column(col):
                continue
            # Even check numeric columns if they're parseable as dates
            if is_parseable(col):
                return (col, None)
        
        # Last resort: Check columns that might be dates based on position/name patterns
        # Sometimes date columns are just named generically
        for col in df.columns:
            if is_event_column(col):
                continue
            col_lower = col.lower()
            # Check for common date column patterns even without explicit keywords
            if any(pattern in col_lower for pattern in ["_dt", "_ts", "when", "at_", "on_"]):
                if is_parseable(col):
                    return (col, None)
        
        return None

    def _build_event_story(
        self,
        event_name: str,
        user_id: Optional[str],
        ts_str: str,
        table_name: str,
        file_name: str,
        source_row_display: str,
        raw_record: Dict[str, Any],
    ) -> str:
        """Build explanation for each event step."""
        mapping = {ev: ev.lower() for ev in HR_EVENT_DISPLAY}
        core = mapping.get(event_name, event_name.replace("_", " ").lower())
        parts = [p for p in [table_name or "", file_name or "", f"row {source_row_display}" if source_row_display else ""] if p]
        origin = f" [{' · '.join(parts)}]" if parts else ""
        return f"{core}{origin}"

    def _is_valid_hr_event_text(self, text: str) -> bool:
        """Validate if text is likely an HR event (not normal text or long remarks)."""
        if not text or len(text) < 3:
            return False
        
        # Exclude long text (likely remarks/descriptions, not event names)
        if len(text) > 50:  # Event names are typically short
            return False
        
        text_lower = text.lower().strip()
        
        # Exclude sentences with temporal words (yesterday, today, tomorrow, etc.)
        temporal_words = ["yesterday", "today", "tomorrow", "last week", "next week", 
                         "last month", "next month", "ago", "before", "after", "when"]
        if any(tw in text_lower for tw in temporal_words):
            return False
        
        # Exclude sentences with action verbs in past tense (descriptions)
        description_patterns = [
            "approved by", "rejected by", "submitted by", "created by", "updated by",
            "was approved", "was rejected", "was submitted", "has been", "will be",
            "manager", "supervisor", "hr", "department"
        ]
        if any(pattern in text_lower for pattern in description_patterns):
            return False
        
        text_normalized = text_lower.replace(" ", "_").replace("-", "_")
        # Check if it contains HR domain keywords
        hr_keywords = [
            "recruitment", "job", "candidate", "interview", "offer", "onboarding",
            "employee", "joining", "induction", "document", "background", "verification",
            "attendance", "clock", "shift", "leave", "overtime", "payroll", "salary",
            "payslip", "tax", "deduction", "bonus", "incentive", "performance", "goal",
            "review", "rating", "promotion", "increment", "training", "certification",
            "exit", "resignation", "settlement", "asset", "access"
        ]
        # Must contain at least one HR keyword
        has_hr_keyword = any(kw in text_normalized for kw in hr_keywords)
        if not has_hr_keyword:
            return False
        
        # Exclude common non-event phrases
        excluded_phrases = [
            "the", "and", "or", "but", "for", "with", "from", "to", "of", "in", "on",
            "at", "by", "as", "is", "are", "was", "were", "been", "being", "have",
            "has", "had", "do", "does", "did", "will", "would", "could", "should",
            "may", "might", "must", "can", "cannot", "this", "that", "these", "those"
        ]
        words = text_normalized.split("_")
        # If all words are common words, it's likely not an event
        if all(w in excluded_phrases for w in words if w):
            return False
        
        # Event names are typically 2-4 words, not long sentences
        word_count = len([w for w in words if w and w not in excluded_phrases])
        if word_count > 5:  # Too many words = likely a description
            return False
        
        return True

    def _scan_row_for_event(self, row: Any, columns: List[str], df: pd.DataFrame) -> Optional[str]:
        """Dynamically scan ALL column values using learned patterns from data."""
        # First pass: Use learned value-to-event mappings (data-driven)
        for col in columns:
            if col.startswith("__"):
                continue
            
            val = row.get(col)
            if val is not None and not (isinstance(val, float) and pd.isna(val)):
                val_str = str(val).strip()
                if val_str:
                    # Check learned mappings first
                    val_normalized = val_str.lower().strip()
                    if val_normalized in self.value_to_event_map:
                        matched_event = self.value_to_event_map[val_normalized]
                        if matched_event and matched_event != "Employee profile creation":
                            return matched_event
                    
                    # Try pattern matching with learned method
                    matched_event = self._match_value_to_event(val_str)
                    if matched_event and matched_event != "Employee profile creation":
                        return matched_event
        
        # Second pass: Check obvious event columns (status, action, activity, etc.)
        event_column_keywords = ["event", "action", "activity", "status", "stage", "phase", "step", "milestone"]
        for col in columns:
            if col.startswith("__"):
                continue
            col_lower = col.lower()
            # Prioritize columns with event-related keywords
            if any(kw in col_lower for kw in event_column_keywords):
                val = row.get(col)
                if val is not None and not (isinstance(val, float) and pd.isna(val)):
                    s = str(val).strip()
                    if s and not s.isdigit():
                        # Use learned pattern matching
                        matched = self._match_value_to_event(s)
                        if matched and matched != "Employee profile creation":
                            return matched
                        
                        normalized = self._normalize_event_from_data(s)
                        if normalized in HR_EVENT_DISPLAY and normalized != "Employee profile creation":
                            return normalized
                        # Try partial matching
                        low = s.lower().replace(" ", "_").replace("-", "_")
                        for disp in HR_EVENT_DISPLAY:
                            if disp == "Employee profile creation":
                                continue
                            key = disp.lower().replace(" ", "_").replace("/", "_")
                            if key == low or key in low or low in key:
                                return disp
        
        # Second pass: Check ALL non-date, non-time columns for event-like values
        excluded_patterns = ["_id", "_code", "_number", "_num", "_ref"]
        date_time_keywords = ["date", "time", "timestamp", "_at", "created", "updated"]
        
        for col in columns:
            if col.startswith("__"):
                continue
            
            col_lower = col.lower()
            # Skip date/time columns
            if any(kw in col_lower for kw in date_time_keywords):
                continue
            
            val = row.get(col)
            if val is None or (isinstance(val, float) and pd.isna(val)):
                continue
            
            s = str(val).strip()
            if not s:
                continue
            
            # Skip pure numeric IDs (but allow alphanumeric codes that might be events)
            if s.isdigit() and len(s) > 10:  # Long numeric = likely ID
                continue
            
            # Check if value looks like an event
            # Try direct normalization first
            normalized = self._normalize_event_from_data(s)
            if normalized in HR_EVENT_DISPLAY and normalized != "Employee profile creation":
                return normalized
            
            # Validate that text is likely an HR event
            if self._is_valid_hr_event_text(s):
                # Try partial matching
                low = s.lower().replace(" ", "_").replace("-", "_")
                for disp in HR_EVENT_DISPLAY:
                    key = disp.lower().replace(" ", "_").replace("/", "_")
                    if key == low or key in low or low in key:
                        if disp != "Employee profile creation":  # Prefer specific events
                            return disp
        
        # Third pass: Derive event from column name + value combination
        # Some tables store events implicitly (e.g., column "leave_status" with value "approved" = "Leave approved")
        for col in columns:
            if col.startswith("__"):
                continue
            
            col_lower = col.lower()
            val = row.get(col)
            if val is None or (isinstance(val, float) and pd.isna(val)):
                continue
            
            val_str = str(val).strip().lower()
            if not val_str or val_str.isdigit():
                continue
            
            # Check column name patterns that suggest events
            # e.g., "leave_status" + "approved" -> "Leave approved"
            if "leave" in col_lower:
                if "approved" in val_str or "approve" in val_str:
                    return "Leave approved"
                elif "rejected" in val_str or "reject" in val_str:
                    return "Leave rejected"
                elif "application" in val_str or "applied" in val_str or "submit" in val_str:
                    return "Leave application submitted"
                elif "cancelled" in val_str or "cancel" in val_str:
                    return "Leave cancelled"
            
            if "interview" in col_lower:
                if "scheduled" in val_str or "schedule" in val_str:
                    return "Interview scheduled"
                elif "conducted" in val_str or "complete" in val_str:
                    return "Interview conducted"
                elif "feedback" in val_str:
                    return "Interview feedback recorded"
            
            if "offer" in col_lower:
                if "generated" in val_str or "create" in val_str:
                    return "Offer letter generated"
                elif "sent" in val_str:
                    return "Offer letter sent"
                elif "accepted" in val_str or "accept" in val_str:
                    return "Candidate accepted offer"
                elif "rejected" in val_str or "reject" in val_str:
                    return "Candidate rejected offer"
            
            if "resignation" in col_lower:
                if "submitted" in val_str or "submit" in val_str:
                    return "Resignation submitted"
                elif "acknowledged" in val_str or "acknowledge" in val_str:
                    return "Resignation acknowledged"
            
            if "clock" in col_lower:
                if "in" in col_lower or "in" in val_str:
                    return "Clock-in"
                elif "out" in col_lower or "out" in val_str:
                    return "Clock-out"
            
            if "shift" in col_lower:
                if "assigned" in val_str or "assign" in val_str:
                    return "Shift assigned"
                elif "changed" in val_str or "change" in val_str:
                    return "Shift changed"
            
            if "training" in col_lower:
                if "started" in val_str or "start" in val_str:
                    return "Training started"
                elif "completed" in val_str or "complete" in val_str:
                    return "Training completed"
                elif "enrollment" in val_str or "enroll" in val_str:
                    return "Training enrollment"
            
            if "promotion" in col_lower:
                if "initiated" in val_str or "initiate" in val_str:
                    return "Promotion initiated"
                elif "approved" in val_str or "approve" in val_str:
                    return "Promotion approved"
            
            if "increment" in col_lower:
                if "initiated" in val_str or "initiate" in val_str:
                    return "Increment initiated"
                elif "approved" in val_str or "approve" in val_str:
                    return "Increment approved"
        
        return None
    
    def _derive_event_from_columns(self, row: Any, columns: List[str], df: pd.DataFrame) -> Optional[str]:
        """Derive event from column name patterns combined with their values."""
        for col in columns:
            if col.startswith("__"):
                continue
            
            col_lower = col.lower()
            val = row.get(col)
            if val is None or (isinstance(val, float) and pd.isna(val)):
                continue
            
            val_str = str(val).strip().lower()
            if not val_str or val_str.isdigit():
                continue
            
            # Check for specific column + value patterns that map to HR events
            # This is more dynamic than hardcoding - it observes patterns
            
            # Leave-related
            if "leave" in col_lower:
                if any(word in val_str for word in ["approved", "approve", "accept"]):
                    return "Leave approved"
                elif any(word in val_str for word in ["rejected", "reject", "denied", "deny"]):
                    return "Leave rejected"
                elif any(word in val_str for word in ["application", "applied", "submit", "request"]):
                    return "Leave application submitted"
                elif any(word in val_str for word in ["cancelled", "cancel", "withdraw"]):
                    return "Leave cancelled"
            
            # Interview-related
            if "interview" in col_lower:
                if any(word in val_str for word in ["scheduled", "schedule", "booked"]):
                    return "Interview scheduled"
                elif any(word in val_str for word in ["conducted", "completed", "done", "finished"]):
                    return "Interview conducted"
                elif any(word in val_str for word in ["feedback", "review", "evaluation"]):
                    return "Interview feedback recorded"
            
            # Offer-related
            if "offer" in col_lower:
                if any(word in val_str for word in ["generated", "created", "prepared"]):
                    return "Offer letter generated"
                elif any(word in val_str for word in ["sent", "delivered", "dispatched"]):
                    return "Offer letter sent"
                elif any(word in val_str for word in ["accepted", "accept", "approved"]):
                    return "Candidate accepted offer"
                elif any(word in val_str for word in ["rejected", "reject", "declined"]):
                    return "Candidate rejected offer"
            
            # Resignation-related
            if "resignation" in col_lower or "resign" in col_lower:
                if any(word in val_str for word in ["submitted", "submit", "tendered"]):
                    return "Resignation submitted"
                elif any(word in val_str for word in ["acknowledged", "acknowledge", "accepted"]):
                    return "Resignation acknowledged"
            
            # Clock/Attendance-related
            if "clock" in col_lower:
                if "in" in col_lower or "in" in val_str:
                    return "Clock-in"
                elif "out" in col_lower or "out" in val_str:
                    return "Clock-out"
            
            # Shift-related
            if "shift" in col_lower:
                if any(word in val_str for word in ["assigned", "assign", "allocated"]):
                    return "Shift assigned"
                elif any(word in val_str for word in ["changed", "change", "modified", "updated"]):
                    return "Shift changed"
            
            # Training-related
            if "training" in col_lower:
                if any(word in val_str for word in ["started", "start", "began", "initiated"]):
                    return "Training started"
                elif any(word in val_str for word in ["completed", "complete", "finished", "done"]):
                    return "Training completed"
                elif any(word in val_str for word in ["enrollment", "enroll", "registered"]):
                    return "Training enrollment"
            
            # Promotion-related
            if "promotion" in col_lower:
                if any(word in val_str for word in ["initiated", "initiate", "started", "proposed"]):
                    return "Promotion initiated"
                elif any(word in val_str for word in ["approved", "approve", "confirmed", "granted"]):
                    return "Promotion approved"
            
            # Increment-related
            if "increment" in col_lower or "salary_increase" in col_lower:
                if any(word in val_str for word in ["initiated", "initiate", "started", "proposed"]):
                    return "Increment initiated"
                elif any(word in val_str for word in ["approved", "approve", "confirmed", "granted"]):
                    return "Increment approved"
            
            # Status columns that might indicate events
            if "status" in col_lower:
                val_normalized = self._normalize_event_from_data(val_str)
                if val_normalized and val_normalized in HR_EVENT_DISPLAY:
                    return val_normalized
        
        return None

    def _calculate_event_confidence(self, event_name: str, source: str, event_name_col: Optional[str], 
                                   time_col: Optional[str], row_data: Dict[str, Any]) -> float:
        """Calculate confidence score (0-1) for detected event."""
        confidence = 0.5  # Base confidence
        
        # Higher confidence if from event_name column
        if source == "event_column" and event_name_col:
            confidence += 0.3
        
        # Higher confidence if event matches column name pattern
        if source == "column_name" and time_col:
            confidence += 0.2
        
        # Lower confidence if from row scanning
        if source == "row_scan":
            confidence -= 0.1
        
        # Check if event is in standard list
        if event_name in HR_EVENT_DISPLAY:
            confidence += 0.1
        
        # Lower confidence for fallback events
        if event_name == "Employee profile creation" and source != "event_column":
            confidence -= 0.2
        
        return max(0.0, min(1.0, confidence))

    def _validate_event_sequence(self, prev_event: Optional[str], current_event: str) -> Tuple[bool, str]:
        """Validate if event sequence is possible. Returns (is_valid, reason)."""
        if not prev_event:
            return True, ""
        
        # Check impossible sequences
        for impossible_prev, impossible_next in IMPOSSIBLE_SEQUENCES:
            if prev_event == impossible_prev and current_event == impossible_next:
                return False, f"Impossible sequence: {prev_event} cannot be followed by {current_event}"
        
        # Check process flow order (high-level validation)
        prev_cat = self.event_to_category.get(prev_event)
        curr_cat = self.event_to_category.get(current_event)
        
        if prev_cat and curr_cat:
            prev_idx = VALID_PROCESS_FLOW.index(prev_cat) if prev_cat in VALID_PROCESS_FLOW else -1
            curr_idx = VALID_PROCESS_FLOW.index(curr_cat) if curr_cat in VALID_PROCESS_FLOW else -1
            
            # Allow same category (recurring processes)
            if prev_cat == curr_cat:
                return True, ""
            
            # Allow going back only for re-joining (Exit -> Onboarding)
            if prev_cat == "Exit" and curr_cat == "Onboarding":
                return True, "Re-joining detected"
            
            # Warn if going backwards significantly (but allow for data quality issues)
            if prev_idx > curr_idx and prev_idx - curr_idx > 2:
                return True, f"Warning: Process flow reversed ({prev_cat} -> {curr_cat})"
        
        return True, ""

    def _correct_invalid_sequences(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Correct invalid HR sequences by adjusting timestamps or reordering events."""
        if not events:
            return events
        
        # Group by user_id for sequence correction
        by_user: Dict[str, List[Dict[str, Any]]] = {}
        for ev in events:
            uid = ev.get("user_id", "unknown")
            by_user.setdefault(uid, []).append(ev)
        
        corrected_events = []
        corrections_made = []
        
        for uid, user_events in by_user.items():
            # Sort by timestamp
            user_events.sort(key=lambda x: x.get("timestamp") or pd.Timestamp.min)
            
            for i in range(len(user_events)):
                ev = user_events[i]
                event_name = ev.get("event", "")
                ts = ev.get("timestamp")
                
                if i == 0:
                    corrected_events.append(ev)
                    continue
                
                prev_ev = user_events[i - 1]
                prev_event = prev_ev.get("event", "")
                prev_ts = prev_ev.get("timestamp")
                
                # Check if sequence is invalid
                is_valid, reason = self._validate_event_sequence(prev_event, event_name)
                
                if not is_valid and ts and prev_ts:
                    # Try to correct by adjusting timestamp
                    prev_cat = self.event_to_category.get(prev_event)
                    curr_cat = self.event_to_category.get(event_name)
                    
                    if prev_cat and curr_cat:
                        prev_idx = VALID_PROCESS_FLOW.index(prev_cat) if prev_cat in VALID_PROCESS_FLOW else -1
                        curr_idx = VALID_PROCESS_FLOW.index(curr_cat) if curr_cat in VALID_PROCESS_FLOW else -1
                        
                        # If current event should come before previous, swap timestamps
                        if curr_idx < prev_idx and curr_idx >= 0 and prev_idx >= 0:
                            # Swap timestamps (current should be before previous)
                            new_ts = prev_ts - pd.Timedelta(seconds=1)
                            ev['timestamp'] = new_ts
                            ev['timestamp_str'] = new_ts.strftime("%Y-%m-%d %H:%M:%S")
                            ev['_timestamp_corrected'] = True
                            ev['_correction_reason'] = f"Corrected sequence: {event_name} should come before {prev_event}"
                            corrections_made.append(f"User {uid}: Adjusted timestamp for {event_name}")
                
                corrected_events.append(ev)
        
        return corrected_events

    def _separate_by_domain(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Separate events by HR domain to avoid mixing parallel processes."""
        # Domains that can run in parallel (should be separated)
        parallel_domains = ["Attendance", "Payroll", "Performance", "Training"]
        
        # Group events by user and domain
        by_user_domain: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
        
        for ev in events:
            uid = ev.get("user_id", "unknown")
            event_name = ev.get("event", "")
            domain = self.event_to_category.get(event_name, "Other")
            
            # For parallel domains, create separate timelines
            if domain in parallel_domains:
                key = (uid, domain)
                if key not in by_user_domain:
                    by_user_domain[key] = []
                by_user_domain[key].append(ev)
            else:
                # Sequential domains (Recruitment, Onboarding, Exit) stay together
                key = (uid, "Sequential")
                if key not in by_user_domain:
                    by_user_domain[key] = []
                by_user_domain[key].append(ev)
        
        # Reconstruct events with domain separation
        separated_events = []
        for (uid, domain), domain_events in by_user_domain.items():
            # Sort by timestamp within each domain
            domain_events.sort(key=lambda x: x.get("timestamp") or pd.Timestamp.min)
            for ev in domain_events:
                ev['_domain'] = domain
                separated_events.append(ev)
        
        # Sort all events by timestamp
        separated_events.sort(key=lambda x: x.get("timestamp") or pd.Timestamp.min)
        return separated_events

    def _remove_duplicate_events(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove duplicate events (same time, same type, same user)."""
        seen = {}
        unique_events = []
        for ev in events:
            user_id = ev.get("user_id", "unknown")
            event_name = ev.get("event", "")
            ts_str = ev.get("timestamp_str", "")
            
            # Create key: (user_id, event_name, timestamp_str)
            # Use timestamp_str for exact matching (within same second)
            key = (user_id, event_name, ts_str)
            if key not in seen:
                seen[key] = ev
                unique_events.append(ev)
            else:
                # If duplicate, keep the one with higher confidence
                existing = seen[key]
                if ev.get("confidence", 0.5) > existing.get("confidence", 0.5):
                    # Replace existing with better one
                    idx = unique_events.index(existing)
                    unique_events[idx] = ev
                    seen[key] = ev
        return unique_events

    def _group_events_by_relationships(self, events: List[Dict[str, Any]], 
                                       relationships: List[Any],
                                       tables: List[TableAnalysis],
                                       dataframes: Dict[str, pd.DataFrame]) -> List[Dict[str, Any]]:
        """Group events using PK-FK relationships to merge timelines across related tables."""
        if not relationships:
            return events
        
        # Build relationship map (bidirectional)
        rel_map = {}
        table_to_df = {t.table_name: dataframes.get(t.table_name) for t in tables if dataframes.get(t.table_name) is not None}
        
        for rel in relationships:
            if hasattr(rel, 'source_table') and hasattr(rel, 'target_table'):
                source = rel.source_table
                target = rel.target_table
                source_col = getattr(rel, 'source_column', None)
                target_col = getattr(rel, 'target_column', None)
                is_fk = getattr(rel, 'is_foreign_key', False)
                
                if source_col and target_col:
                    # Forward relationship (source -> target)
                    if source not in rel_map:
                        rel_map[source] = []
                    rel_map[source].append({
                        'target': target,
                        'source_col': source_col,
                        'target_col': target_col,
                        'is_fk': is_fk
                    })
                    # Reverse relationship (target -> source)
                    if target not in rel_map:
                        rel_map[target] = []
                    rel_map[target].append({
                        'target': source,
                        'source_col': target_col,
                        'target_col': source_col,
                        'is_fk': is_fk
                    })
        
        # Build index of events by table and FK values
        events_by_table_fk: Dict[str, Dict[Any, List[Dict[str, Any]]]] = {}
        for ev in events:
            table_name = ev.get("table_name", "")
            raw_record = ev.get("raw_record", {})
            
            if table_name not in events_by_table_fk:
                events_by_table_fk[table_name] = {}
            
            # Find FK columns for this table
            if table_name in rel_map:
                for rel_info in rel_map[table_name]:
                    if rel_info['is_fk'] and rel_info['source_col'] in raw_record:
                        fk_value = raw_record[rel_info['source_col']]
                        if fk_value not in events_by_table_fk[table_name]:
                            events_by_table_fk[table_name][fk_value] = []
                        events_by_table_fk[table_name][fk_value].append(ev)
        
        # Merge events from related tables using FK relationships
        merged_events = []
        processed = set()
        
        for ev in events:
            ev_id = id(ev)
            if ev_id in processed:
                continue
            
            table_name = ev.get("table_name", "")
            raw_record = ev.get("raw_record", {})
            
            # Find related events through FK relationships
            related_events = [ev]
            if table_name in rel_map:
                for rel_info in rel_map[table_name]:
                    if rel_info['is_fk'] and rel_info['source_col'] in raw_record:
                        fk_value = raw_record[rel_info['source_col']]
                        target_table = rel_info['target']
                        
                        # Find events in target table with matching PK value
                        if target_table in events_by_table_fk:
                            # Check if target table has events with this FK value as PK
                            target_df = table_to_df.get(target_table)
                            if target_df is not None and rel_info['target_col'] in target_df.columns:
                                matching_rows = target_df[target_df[rel_info['target_col']] == fk_value]
                                if not matching_rows.empty:
                                    # Find events from target table
                                    for target_ev in events:
                                        if (target_ev.get("table_name") == target_table and
                                            target_ev.get("raw_record", {}).get(rel_info['target_col']) == fk_value and
                                            id(target_ev) not in processed):
                                            related_events.append(target_ev)
                                            processed.add(id(target_ev))
            
            # Mark as processed
            processed.add(ev_id)
            
            # Merge related events by adding FK link metadata
            for related_ev in related_events:
                if related_ev not in merged_events:
                    related_ev['_fk_links'] = related_ev.get('_fk_links', [])
                    related_ev['_merged_from_tables'] = related_ev.get('_merged_from_tables', [])
                    if table_name not in related_ev['_merged_from_tables']:
                        related_ev['_merged_from_tables'].append(table_name)
                    merged_events.append(related_ev)
        
        return merged_events

    def _detect_rejoining(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Detect employee re-joining scenarios and mark them appropriately."""
        # Group by user_id
        by_user: Dict[str, List[Dict[str, Any]]] = {}
        for ev in events:
            uid = ev.get("user_id", "unknown")
            by_user.setdefault(uid, []).append(ev)
        
        # Check for Exit -> Onboarding pattern (re-joining)
        for uid, user_events in by_user.items():
            user_events.sort(key=lambda x: x.get("timestamp") or pd.Timestamp.min)
            
            for i in range(len(user_events) - 1):
                curr = user_events[i]
                next_ev = user_events[i + 1]
                
                curr_event = curr.get("event", "")
                next_event = next_ev.get("event", "")
                
                # Check if Exit is followed by Onboarding (re-joining)
                if (curr_event in ["Employee status closed", "Full & Final settlement completed"] and
                    next_event in ["Employee profile creation", "Employee ID generated", "Joining date confirmation"]):
                    # Mark as re-joining
                    next_ev['_is_rejoining'] = True
                    next_ev['_rejoining_from'] = curr_event
                    # Calculate gap
                    curr_ts = curr.get("timestamp")
                    next_ts = next_ev.get("timestamp")
                    if curr_ts and next_ts:
                        gap_days = (next_ts - curr_ts).days
                        next_ev['_rejoining_gap_days'] = gap_days
        
        return events

    def _table_to_events(self, table: TableAnalysis, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Convert table rows to events. Event from event_name col (row data) or time column."""
        cols = self._find_datetime_columns(df)
        if not cols:
            return []
        date_col, time_col = cols
        event_time_col = time_col if time_col else date_col
        df = df.copy()

        # Try parsing date column - handle numeric formats (YYYYMMDD, Unix timestamps)
        def parse_date_value(val):
            """Parse various date formats including numeric ones."""
            if pd.isna(val):
                return pd.NaT
            
            val_str = str(val).strip()
            
            # Try YYYYMMDD format (8 digits)
            if val_str.isdigit() and len(val_str) == 8:
                try:
                    return pd.to_datetime(val_str, format="%Y%m%d", errors="coerce")
                except:
                    pass
            
            # Try YYYYMM format (6 digits)
            if val_str.isdigit() and len(val_str) == 6:
                try:
                    return pd.to_datetime(val_str, format="%Y%m", errors="coerce")
                except:
                    pass
            
            # Try Unix timestamp (10-13 digits)
            if val_str.isdigit() and 10 <= len(val_str) <= 13:
                try:
                    ts = int(val_str)
                    if 946684800 <= ts <= 4102444800:  # Reasonable range
                        return pd.to_datetime(ts, unit='s', errors="coerce")
                except:
                    pass
            
            # Try standard pandas parsing
            return pd.to_datetime(val_str, errors="coerce")
        
        df["__dt"] = df[date_col].apply(parse_date_value)
        
        if time_col and time_col in df.columns:
            df["__date_str"] = df[date_col].astype(str).str.split().str[0]
            df["__time_str"] = df[time_col].astype(str)
            combined = df["__date_str"] + " " + df["__time_str"]
            df["__dt"] = pd.to_datetime(combined, errors="coerce").fillna(df["__dt"])

        df = df.dropna(subset=["__dt"])
        if df.empty:
            return []
        df = df.sort_values("__dt", ascending=True)

        # STEP 1: Learn patterns from actual data (data-driven approach)
        self._learn_patterns_from_data(df)

        user_col = self._find_user_col(df)
        event_name_col = self._find_event_name_col(df)
        case_id_col = self._find_case_id_col(df)

        events: List[Dict[str, Any]] = []
        file_name = getattr(table, "file_name", "") or f"{table.table_name}.csv"

        for idx, row in df.iterrows():
            ts = row["__dt"]
            if pd.isna(ts):
                continue
            # Build raw_record first (needed for confidence calculation)
            raw_record = {}
            for c in df.columns:
                if c.startswith("__"):
                    continue
                v = row.get(c)
                raw_record[c] = "" if (v is None or (isinstance(v, float) and pd.isna(v))) else str(v)
            
            # Dynamic event detection: Try multiple sources in order
            source = "unknown"
            event_name = None
            
            # 1) Check explicit event_name column
            if event_name_col and event_name_col in row.index and pd.notna(row[event_name_col]):
                event_name = self._normalize_event_from_data(row[event_name_col])
                if event_name and event_name in HR_EVENT_DISPLAY:
                    source = "event_column"
            
            # 2) Scan ALL row data for event patterns (most thorough)
            if not event_name or event_name == "Employee profile creation":
                scanned = self._scan_row_for_event(row, list(df.columns), df)
                if scanned:
                    event_name = scanned
                    source = "row_scan"
            
            # 3) Derive from column name patterns + row values
            if not event_name or event_name == "Employee profile creation":
                # Try to derive event from column names and their values
                derived = self._derive_event_from_columns(row, list(df.columns), df)
                if derived:
                    event_name = derived
                    source = "column_pattern"
            
            # 4) Derive from timestamp column name (fallback)
            if not event_name or event_name == "Employee profile creation":
                event_name = self._time_column_to_hr_event(event_time_col)
                source = "column_name"
            
            # 5) Last resort: Use first non-date, non-ID column value as event hint
            if not event_name or event_name == "Employee profile creation":
                for col in df.columns:
                    if col.startswith("__"):
                        continue
                    col_lower = col.lower()
                    if any(k in col_lower for k in ["date", "time", "timestamp", "_id", "_code"]):
                        continue
                    val = row.get(col)
                    if val is not None and not (isinstance(val, float) and pd.isna(val)):
                        val_str = str(val).strip()
                        if val_str and not val_str.isdigit() and len(val_str) < 50:
                            # Try to normalize this value
                            normalized = self._normalize_event_from_data(val_str)
                            if normalized and normalized in HR_EVENT_DISPLAY:
                                event_name = normalized
                                source = "value_fallback"
                                break
            
            # Final fallback: Use table name or generic event
            if not event_name:
                # Try to infer from table name
                table_name_lower = table.table_name.lower()
                if "attendance" in table_name_lower:
                    event_name = "Clock-in"
                elif "payroll" in table_name_lower:
                    event_name = "Monthly payroll initiated"
                elif "training" in table_name_lower:
                    event_name = "Training started"
                elif "performance" in table_name_lower:
                    event_name = "Goal creation"
                elif "exit" in table_name_lower or "resignation" in table_name_lower:
                    event_name = "Resignation submitted"
                else:
                    event_name = "Employee profile creation"  # Last resort
                source = "table_name_fallback"
            
            # Calculate confidence
            confidence = self._calculate_event_confidence(event_name, source, event_name_col, event_time_col, raw_record)

            user_id = None
            if user_col and user_col in row.index and pd.notna(row[user_col]):
                user_id = str(row[user_col]).strip()
            case_id_val = None
            if case_id_col and case_id_col in row.index and pd.notna(row[case_id_col]):
                case_id_val = str(row[case_id_col]).strip()
            if not user_id and case_id_val:
                user_id = case_id_val

            ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")
            source_row_display = str(int(idx) + 1) if str(idx).isdigit() else ""
            event_story = self._build_event_story(
                event_name=event_name,
                user_id=user_id or "unknown",
                ts_str=ts_str,
                table_name=table.table_name,
                file_name=file_name,
                source_row_display=source_row_display,
                raw_record=raw_record,
            )

            events.append({
                "user_id": user_id or case_id_val or "unknown",
                "_case_id": case_id_val,
                "event": event_name,
                "_event_time_column": event_time_col,
                "timestamp": ts,
                "timestamp_str": ts_str,
                "table_name": table.table_name,
                "file_name": file_name,
                "source_row": int(idx) if str(idx).isdigit() else str(idx),
                "raw_record": raw_record,
                "event_story": event_story,
                "confidence": confidence,
                "_detection_source": source,
            })
        return events

    def _split_cases(self, all_events: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        """Group by case_id when present, else by user_id. Split by time gaps to support multiple journeys per employee.
        Does NOT split on repeated events - allows natural HR journeys with recurring activities."""
        has_case_id = any(ev.get("_case_id") for ev in all_events)
        if has_case_id:
            by_case: Dict[str, List[Dict[str, Any]]] = {}
            for ev in all_events:
                cid = ev.get("_case_id") or ev.get("user_id") or "unknown"
                by_case.setdefault(cid, []).append(ev)
            all_cases = []
            for cid, evs in by_case.items():
                evs_sorted = sorted(evs, key=lambda x: x.get("timestamp") or pd.Timestamp.min)
                all_cases.append(evs_sorted)
        else:
            by_user: Dict[str, List[Dict[str, Any]]] = {}
            for ev in all_events:
                uid = ev.get("user_id") or "unknown"
                by_user.setdefault(uid, []).append(ev)
            all_cases = []
            # HR process start events that indicate a new journey
            hr_start = {"Employee profile creation", "Employee ID generated", "Job posting / advertisement"}
            for uid, evs in by_user.items():
                evs_sorted = sorted(evs, key=lambda x: x.get("timestamp") or pd.Timestamp.min)
                if not evs_sorted:
                    continue
                
                cases: List[List[Dict]] = []
                current: List[Dict] = []
                
                for i, ev in enumerate(evs_sorted):
                    ts = ev.get("timestamp")
                    if ts is None or pd.isna(ts):
                        if current:
                            current.append(ev)
                        continue
                    
                    # Start a new case if:
                    # 1. This is the first event
                    # 2. This is a start event and we have a gap > HR_CASE_GAP_HOURS hours from last event
                    # 3. Re-joining detected (Exit followed by Onboarding)
                    if not current:
                        current.append(ev)
                    else:
                        prev_ts = current[-1].get("timestamp")
                        if prev_ts is None or pd.isna(prev_ts):
                            current.append(ev)
                        else:
                            time_gap_hours = (ts - prev_ts).total_seconds() / 3600.0
                            event_name = ev.get("event", "")
                            prev_event = current[-1].get("event", "")
                            
                            # Check for re-joining (Exit -> Onboarding)
                            is_rejoining = (prev_event in ["Employee status closed", "Full & Final settlement completed"] and
                                          event_name in ["Employee profile creation", "Employee ID generated", "Joining date confirmation"])
                            
                            # If it's a start event and gap is significant, start new case
                            # OR if re-joining is detected, start new case
                            if (event_name in hr_start and time_gap_hours > HR_CASE_GAP_HOURS) or is_rejoining:
                                if current:
                                    cases.append(current)
                                current = [ev]
                                if is_rejoining:
                                    ev['_is_rejoining'] = True
                            else:
                                # Allow repeated events in same case - they're part of the journey
                                # Do NOT split on repeated events
                                current.append(ev)
                
                if current:
                    cases.append(current)
                all_cases.extend(cases if cases else [evs_sorted])

        all_cases.sort(key=lambda c: c[0]["timestamp"] if c and c[0].get("timestamp") else pd.Timestamp.min)
        return all_cases

    def _event_phrase(self, step: str) -> str:
        return step.lower().replace("_", " ") if step else ""

    def _build_case_explanation(
        self,
        case_id: int,
        user_id: str,
        events: List[Dict[str, Any]],
    ) -> str:
        if not events:
            return f"Employee {user_id} · No steps."
        first, last = events[0], events[-1]
        
        # Get timestamp strings - prioritize timestamp_str, then format from timestamp object
        start_str = first.get("timestamp_str", "")
        end_str = last.get("timestamp_str", "")
        
        # If timestamp_str is empty or not properly formatted, try to get from timestamp object
        if not start_str or start_str == "":
            ts_obj = first.get("timestamp")
            if ts_obj is not None and hasattr(ts_obj, "strftime"):
                start_str = ts_obj.strftime("%Y-%m-%d %H:%M")
            elif ts_obj is not None:
                try:
                    start_str = pd.to_datetime(ts_obj).strftime("%Y-%m-%d %H:%M")
                except Exception:
                    start_str = str(ts_obj)[:16] if ts_obj else ""
        
        if not end_str or end_str == "":
            ts_obj = last.get("timestamp")
            if ts_obj is not None and hasattr(ts_obj, "strftime"):
                end_str = ts_obj.strftime("%Y-%m-%d %H:%M")
            elif ts_obj is not None:
                try:
                    end_str = pd.to_datetime(ts_obj).strftime("%Y-%m-%d %H:%M")
                except Exception:
                    end_str = str(ts_obj)[:16] if ts_obj else ""
        
        steps = [ev.get("event", "") for ev in events]
        seq = " → ".join(self._event_phrase(s) for s in steps) or " → ".join(steps)
        return f"{user_id} · {seq} · {start_str} → {end_str}"

    def _assign_case_ids(self, cases: List[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        case_details = []
        for idx, events in enumerate(cases):
            if not events:
                continue
            case_id = idx + 1
            user_id = events[0].get("user_id") or "unknown"
            activities = []
            for ev in events:
                ts = ev.get("timestamp")
                ts_str = ev.get("timestamp_str", "")
                # Only regenerate if timestamp_str is missing or invalid
                if not ts_str or ts_str == "":
                    if ts is not None and hasattr(ts, "strftime"):
                        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")
                    elif ts is not None:
                        try:
                            ts_str = pd.to_datetime(ts).strftime("%Y-%m-%d %H:%M:%S")
                        except Exception:
                            ts_str = str(ts)[:19] if ts else ""
                raw = ev.get("raw_record") or {}
                activities.append({
                    "event": ev.get("event"),
                    "timestamp_str": ts_str,
                    "user_id": user_id,
                    "table_name": ev.get("table_name"),
                    "file_name": ev.get("file_name"),
                    "source_row": ev.get("source_row"),
                    "event_story": ev.get("event_story"),
                    "raw_record": {k: ("" if v is None else str(v)) for k, v in raw.items()},
                })
            case_details.append({
                "case_id": case_id,
                "user_id": user_id,
                "employee_id": user_id,
                "first_activity_timestamp": activities[0]["timestamp_str"],
                "last_activity_timestamp": activities[-1]["timestamp_str"],
                "activity_count": len(activities),
                "activities": activities,
                "event_sequence": [a["event"] for a in activities],
                "explanation": self._build_case_explanation(case_id, user_id, events),
            })
        return case_details

    @staticmethod
    def _compute_same_time_groups(case_paths: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Find events where multiple case IDs have the same timestamp. Include all events, not just middle ones."""
        by_key: Dict[Tuple[str, str], List[int]] = {}
        for p in case_paths:
            seq = p.get("path_sequence", [])
            timings = p.get("timings", [])
            case_id = p.get("case_id")
            # Include all events, not just middle ones (range from 0 to len(seq))
            for j in range(len(seq)):
                event = seq[j]
                if event in ("Process", "End"):
                    continue
                # For first event, use start_datetime; for others, use end_datetime or start_datetime
                if j == 0:
                    # First event after Process - check first timing
                    t = timings[0] if timings else {}
                    ts_str = t.get("start_datetime") or t.get("end_datetime") or ""
                elif j - 1 < len(timings):
                    t = timings[j - 1]
                    ts_str = t.get("end_datetime") or t.get("start_datetime") or ""
                else:
                    # Fallback for last event
                    t = timings[-1] if timings else {}
                    ts_str = t.get("end_datetime") or t.get("start_datetime") or ""
                
                if not ts_str:
                    continue
                key = (event, ts_str)
                by_key.setdefault(key, []).append(case_id)
        out = []
        for (event, ts_str), case_ids in by_key.items():
            if len(case_ids) > 1:
                out.append({"event": event, "timestamp_str": ts_str, "case_ids": sorted(set(case_ids))})
        return out

    def _generate_unified_flow_data(self, case_details: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Unified flow for diagram. Case IDs different colors, dynamic event names."""
        colors = [
            "#4F46E5", "#EC4899", "#F59E0B", "#10B981", "#3B82F6",
            "#6366F1", "#14B8A6", "#F97316", "#EF4444", "#84CC16",
        ]
        case_paths = []
        for idx, case in enumerate(case_details):
            activities = case.get("activities", [])
            if not activities:
                continue
            case_color = colors[idx % len(colors)]
            path_sequence = ["Process"]
            timings = []
            prev_ts = None
            prev_event_name = "Process"
            for act in activities:
                event_display = act.get("event", "Step")
                ts_str = act.get("timestamp_str", "")
                try:
                    ts = pd.to_datetime(ts_str)
                except Exception:
                    ts = None
                if prev_ts is not None and ts is not None:
                    dur = max(0, int((ts - prev_ts).total_seconds()))
                    d, h = dur // 86400, (dur % 86400) // 3600
                    m, s = (dur % 3600) // 60, dur % 60
                    time_label = f"{d} day{'s' if d != 1 else ''} {h} hr" if d else (f"{h} hr {m} min" if h else (f"{m} min" if m else f"{s} sec"))
                else:
                    dur = 0
                    time_label = "Start" if prev_event_name == "Process" else "0 sec"
                path_sequence.append(event_display)
                timings.append({
                    "from": prev_event_name, "to": event_display,
                    "duration_seconds": dur, "label": time_label,
                    "start_time": prev_ts.strftime("%H:%M:%S") if prev_ts else "",
                    "end_time": ts.strftime("%H:%M:%S") if ts else "",
                    "start_datetime": prev_ts.strftime("%Y-%m-%d %H:%M:%S") if prev_ts else "",
                    "end_datetime": ts.strftime("%Y-%m-%d %H:%M:%S") if ts else "",
                })
                prev_ts, prev_event_name = ts, event_display
            path_sequence.append("End")
            last_ts = prev_ts
            timings.append({
                "from": prev_event_name, "to": "End", "duration_seconds": 0, "label": "End",
                "start_time": last_ts.strftime("%H:%M:%S") if last_ts else "",
                "end_time": last_ts.strftime("%H:%M:%S") if last_ts else "",
                "start_datetime": last_ts.strftime("%Y-%m-%d %H:%M:%S") if last_ts else "",
                "end_datetime": last_ts.strftime("%Y-%m-%d %H:%M:%S") if last_ts else "",
            })
            case_paths.append({
                "case_id": case.get("case_id"),
                "user_id": case.get("user_id"),
                "color": case_color,
                "path_sequence": path_sequence,
                "timings": timings,
                "total_duration": sum(t["duration_seconds"] for t in timings),
            })
        seen = {"Process", "End"}
        all_event_types = ["Process"]
        for p in case_paths:
            for s in p.get("path_sequence", []):
                if s not in seen and s not in ("Process", "End"):
                    seen.add(s)
                    all_event_types.append(s)
        all_event_types.append("End")
        same_time_groups = self._compute_same_time_groups(case_paths)
        # Sankey pattern: count (from, to) transitions across all case paths (no hardcoding)
        transition_counts = {}
        for p in case_paths:
            seq = p.get("path_sequence") or []
            for i in range(len(seq) - 1):
                f, t = seq[i], seq[i + 1]
                if f and t:
                    key = (f, t)
                    transition_counts[key] = transition_counts.get(key, 0) + 1
        transition_counts_list = [{"from": f, "to": t, "count": c} for (f, t), c in transition_counts.items()]
        return {
            "all_event_types": all_event_types,
            "case_paths": case_paths,
            "total_cases": len(case_paths),
            "same_time_groups": same_time_groups,
            "transition_counts": transition_counts_list,
        }

    def analyze_cluster(
        self,
        tables: List[TableAnalysis],
        dataframes: Dict[str, pd.DataFrame],
        relationships: List[Any],
    ) -> Dict[str, Any]:
        # Reset learned patterns for this analysis (learn fresh from data)
        self.observed_patterns = {}
        self.value_to_event_map = {}
        
        all_events = []
        tables_without_dates = []
        tables_checked = []
        
        for table in tables:
            df = dataframes.get(table.table_name)
            if df is None or df.empty:
                continue
            
            tables_checked.append(table.table_name)
            events = self._table_to_events(table, df)
            
            if not events:
                # Check why no events were found
                cols = self._find_datetime_columns(df)
                if not cols:
                    tables_without_dates.append({
                        "table": table.table_name,
                        "columns": list(df.columns),
                        "reason": "No parseable date/time columns found"
                    })
            else:
                all_events.extend(events)

        if not all_events:
            error_msg = "No HR events with usable timestamps found across tables."
            if tables_without_dates:
                error_msg += "\n\nTables checked: " + ", ".join(tables_checked)
                error_msg += "\n\nIssues found:"
                for issue in tables_without_dates:
                    error_msg += f"\n- {issue['table']}: {issue['reason']}"
                    error_msg += f"\n  Available columns: {', '.join(issue['columns'][:10])}"
                    if len(issue['columns']) > 10:
                        error_msg += f" ... ({len(issue['columns'])} total)"
                error_msg += "\n\nTip: Ensure your files have date/time columns with names like 'date', 'time', 'timestamp', 'created_at', etc."
            
            return {
                "success": False,
                "error": error_msg,
                "tables_checked": tables_checked,
                "diagnostics": tables_without_dates,
            }

        # Use PK-FK relationships to group events
        all_events = self._group_events_by_relationships(all_events, relationships, tables, dataframes)
        
        # Remove duplicate events
        all_events = self._remove_duplicate_events(all_events)
        
        # Detect re-joining scenarios
        all_events = self._detect_rejoining(all_events)
        
        # Validate event sequences and filter low-confidence events
        validated_events = []
        sequence_warnings = []
        low_confidence_count = 0
        
        for i, ev in enumerate(all_events):
            event_name = ev.get("event", "")
            confidence = ev.get("confidence", 0.5)
            
            # Filter out very low confidence events (< 0.2)
            if confidence < 0.2:
                low_confidence_count += 1
                continue
            
            # Validate sequence (only validate within same user's events)
            # Group by user for sequence validation
            if validated_events:
                # Check if same user
                prev_user = validated_events[-1].get("user_id")
                curr_user = ev.get("user_id")
                if prev_user == curr_user:
                    prev_event = validated_events[-1].get("event")
                    is_valid, reason = self._validate_event_sequence(prev_event, event_name)
                    
                    if not is_valid:
                        sequence_warnings.append(f"Event {event_name} at {ev.get('timestamp_str')}: {reason}")
                        # Still include but mark as problematic
                        ev['_sequence_warning'] = reason
                    elif reason:
                        ev['_sequence_note'] = reason
            
            validated_events.append(ev)
        
        all_events = validated_events
        
        # Correct invalid sequences by adjusting timestamps
        all_events = self._correct_invalid_sequences(all_events)
        
        # Separate events by HR domain (Attendance, Payroll, Training, etc.)
        # These can run in parallel and should be separated
        domain_separated_events = self._separate_by_domain(all_events)
        # Domain separation already sorted events
        all_events = domain_separated_events
        cases = self._split_cases(all_events)
        case_details = self._assign_case_ids(cases)
        unified_flow_data = self._generate_unified_flow_data(case_details)

        first_ts = all_events[0]["timestamp"]
        last_ts = all_events[-1]["timestamp"]
        observed = list(dict.fromkeys(a.get("event", "") for c in case_details for a in c.get("activities", [])))

        return {
            "success": True,
            "sorted_timeline": [{k: v for k, v in e.items() if k not in ("timestamp", "_case_id", "_event_time_column")} for e in all_events],
            "first_datetime": first_ts.strftime("%Y-%m-%d %H:%M:%S") if hasattr(first_ts, "strftime") else "",
            "last_datetime": last_ts.strftime("%Y-%m-%d %H:%M:%S") if hasattr(last_ts, "strftime") else "",
            "total_events": len(all_events),
            "total_activities": len(all_events),
            "case_ids": [c["case_id"] for c in case_details],
            "case_details": case_details,
            "total_cases": len(case_details),
            "total_employees": len(set(c["user_id"] for c in case_details)),
            "employees": list(dict.fromkeys(c["user_id"] for c in case_details)),
            "explanations": [
                f"We found {len(case_details)} case(s). Each case represents one HR process instance (recruitment/onboarding/attendance/payroll/performance/training/exit).",
                "Case IDs are numbered in order of first event time (ascending).",
                "Events are grouped by employee using PK-FK relationships and split into cases based on time gaps (>24 hours) or process start events. Multiple cases per employee are supported for recurring processes like attendance, payroll, and training.",
                "Repeated events within the same time window are part of the same case, allowing natural HR journeys with recurring activities. Duplicate events (same time, same type) are automatically removed.",
                "Event sequences are validated to ensure logical HR process flow. Employee re-joining scenarios are detected and handled appropriately.",
                "Events are observed from your uploaded columns and row data across all tables. Each event has a confidence score based on detection method.",
                f"Event types found: {', '.join(observed) or '—'}.",
                f"Sequence validation warnings: {len(sequence_warnings)}" if sequence_warnings else "All event sequences validated successfully.",
            ],
            "sequence_warnings": sequence_warnings,
            "quality_metrics": {
                "total_events_detected": len(all_events),
                "events_removed_low_confidence": low_confidence_count,
                "average_confidence": sum(e.get("confidence", 0.5) for e in all_events) / len(all_events) if all_events else 0.0,
                "rejoining_detected": len([e for e in all_events if e.get("_is_rejoining")]),
                "sequence_warnings_count": len(sequence_warnings),
            },
            "unified_flow_data": unified_flow_data,
        }

