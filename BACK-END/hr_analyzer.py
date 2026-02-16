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

    def _time_column_to_hr_event(self, col_name: str) -> str:
        """Derive event from timestamp column name. User file observed."""
        if not col_name or not str(col_name).strip():
            return "Employee profile creation"
        c = str(col_name).lower().replace("-", "_").replace(" ", "_")
        for suf in ["_time", "_date", "_timestamp", "_at", "_datetime"]:
            if c.endswith(suf):
                c = c[: -len(suf)]
                break
        
        # Map column names to HR events based on patterns (not hardcoded - observed from data)
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
        
        # Category-based fallback (observe data patterns)
        if "recruitment" in c or "job" in c or "candidate" in c or "interview" in c or "offer" in c:
            if "offer" in c:
                return "Offer letter sent" if "sent" in c else "Offer letter generated"
            if "interview" in c:
                return "Interview conducted" if "conducted" in c else ("Interview feedback recorded" if "feedback" in c else "Interview scheduled")
            return "Job posting / advertisement"
        
        if "onboarding" in c or "employee_profile" in c or "joining" in c or "induction" in c:
            if "joining" in c:
                return "Joining date confirmation"
            if "induction" in c or "orientation" in c:
                return "Employee induction"
            return "Employee profile creation"
        
        if "attendance" in c or "clock" in c or "shift" in c or "leave" in c:
            if "clock" in c:
                return "Clock-out" if "out" in c else "Clock-in"
            if "leave" in c:
                return "Leave approved" if "approved" in c else ("Leave rejected" if "rejected" in c else "Leave application submitted")
            return "Shift assigned"
        
        if "payroll" in c or "salary" in c or "payslip" in c:
            if "payslip" in c:
                return "Payslip published" if "published" in c else "Payslip generated"
            if "salary" in c:
                return "Salary processed" if "processed" in c else ("Salary calculated" if "calculated" in c else "Salary structure created")
            return "Monthly payroll initiated"
        
        if "performance" in c or "goal" in c or "review" in c or "rating" in c or "promotion" in c:
            if "promotion" in c:
                return "Promotion approved" if "approved" in c else "Promotion initiated"
            if "goal" in c:
                return "Goal updated" if "updated" in c else ("Goal assigned" if "assigned" in c else "Goal creation")
            return "Rating assigned"
        
        if "training" in c or "certification" in c:
            if "certification" in c:
                return "Certification issued"
            return "Training started" if "started" in c else ("Training completed" if "completed" in c else "Training enrollment")
        
        if "exit" in c or "resignation" in c or "settlement" in c:
            if "resignation" in c:
                return "Resignation acknowledged" if "acknowledged" in c else "Resignation submitted"
            if "settlement" in c:
                return "Full & Final settlement completed" if "completed" in c else "Full & Final settlement initiated"
            return "Employee status closed"
        
        return "Employee profile creation"

    def _find_user_col(self, df: pd.DataFrame) -> Optional[str]:
        """Detect user/employee column."""
        candidates = [
            "employee_id", "emp_id", "employeeid", "empid", "empcode", "emp_code",
            "user_id", "staff_id", "personnel_id",
        ]
        cols_lower = {c.lower(): c for c in df.columns}
        for cand in candidates:
            if cand in cols_lower:
                return cols_lower[cand]
        for col in df.columns:
            cl = col.lower()
            if any(k in cl for k in ["employee", "emp", "staff", "personnel"]) and "id" in cl:
                return col
        return None

    def _find_event_name_col(self, df: pd.DataFrame) -> Optional[str]:
        """Detect column with event type as DATA (row values). Check row values match HR event patterns."""
        candidates = ["event_name", "event_type", "action", "event", "step_name", "activity", "status", "activity_type"]
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
                if valid >= min(2, len(vals)):
                    return col
        # Prefer event_type, action, activity over event_id
        prefer = ["event_type", "event_name", "action", "activity", "step_name", "activity_type", "status"]
        for p in prefer:
            if p in cols_lower:
                col = cols_lower[p]
                sample = df[col].dropna().astype(str).head(20)
                if len(sample) > 0:
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
        """Normalize event name from row data to match HR event display names."""
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return "Employee profile creation"
        s = str(val).strip()
        if not s:
            return "Employee profile creation"
        normalized = s.replace("_", " ").replace("-", " ").title()
        
        # Map common variants to exact display name
        low = s.lower().replace(" ", "_").replace("-", "_")
        for disp in HR_EVENT_DISPLAY:
            key = disp.lower().replace(" ", "_").replace("/", "_")
            if key == low or key in low or low in key:
                return disp
        
        return normalized

    def _find_datetime_columns(self, df: pd.DataFrame) -> Optional[Tuple[str, Optional[str]]]:
        """Find (date_col, time_col)."""

        def is_parseable(col: str) -> bool:
            try:
                sample = df[col].dropna().head(10)
                if len(sample) == 0:
                    return False
                parsed = pd.to_datetime(sample, errors="coerce")
                return parsed.notna().sum() >= len(sample) * 0.5
            except Exception:
                return False

        for col in df.columns:
            cl = col.lower().replace("-", "_").replace(" ", "_")
            if any(pat in cl for pat in HR_EVENT_COLUMN_PATTERNS) and is_parseable(col):
                return (col, None)
        preferred = [
            "event_time", "event_timestamp", "created_at", "timestamp", "updated_at",
            "activity_date", "activity_time", "record_date", "record_time",
            "clock_in", "clock_out", "clockin_time", "clockout_time",
            "joining_date", "joining_time", "interview_date", "interview_time",
            "leave_date", "leave_time", "payroll_date", "payroll_time",
            "review_date", "review_time", "training_date", "training_time",
            "resignation_date", "resignation_time",
        ]
        for col in df.columns:
            cl = col.lower()
            if any(k in cl for k in preferred) and is_parseable(col):
                return (col, None)
        date_candidates = [c for c in df.columns if any(k in c.lower() for k in ["date", "time", "timestamp"]) and is_parseable(c)]
        if date_candidates:
            return (date_candidates[0], None)
        for col in df.columns:
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

    def _scan_row_for_event(self, row: Any, columns: List[str]) -> Optional[str]:
        """Scan ALL column values in row for event pattern match. Not column-only—observe data."""
        for col in columns:
            if col.startswith("__"):
                continue
            val = row.get(col)
            if val is None or (isinstance(val, float) and pd.isna(val)):
                continue
            s = str(val).strip()
            if not s:
                continue
            normalized = self._normalize_event_from_data(s)
            if normalized in HR_EVENT_DISPLAY:
                return normalized
            low = s.lower().replace(" ", "_").replace("-", "_")
            for disp in HR_EVENT_DISPLAY:
                key = disp.lower().replace(" ", "_").replace("/", "_")
                if key == low or key in low or low in key:
                    return disp
        return None

    def _table_to_events(self, table: TableAnalysis, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Convert table rows to events. Event from event_name col (row data) or time column."""
        cols = self._find_datetime_columns(df)
        if not cols:
            return []
        date_col, time_col = cols
        event_time_col = time_col if time_col else date_col
        df = df.copy()

        df["__dt"] = pd.to_datetime(df[date_col], errors="coerce")
        if time_col and time_col in df.columns:
            df["__date_str"] = df[date_col].astype(str).str.split().str[0]
            df["__time_str"] = df[time_col].astype(str)
            df["__dt"] = pd.to_datetime(
                df["__date_str"] + " " + df["__time_str"], errors="coerce"
            ).fillna(df["__dt"])

        df = df.dropna(subset=["__dt"])
        if df.empty:
            return []
        df = df.sort_values("__dt", ascending=True)

        user_col = self._find_user_col(df)
        event_name_col = self._find_event_name_col(df)
        case_id_col = self._find_case_id_col(df)

        events: List[Dict[str, Any]] = []
        file_name = getattr(table, "file_name", "") or f"{table.table_name}.csv"

        for idx, row in df.iterrows():
            ts = row["__dt"]
            if pd.isna(ts):
                continue
            # 1) event_name column value; 2) scan ALL row data for event pattern (not column-only)
            if event_name_col and event_name_col in row.index and pd.notna(row[event_name_col]):
                event_name = self._normalize_event_from_data(row[event_name_col])
            else:
                scanned = self._scan_row_for_event(row, list(df.columns))
                event_name = scanned if scanned else self._time_column_to_hr_event(event_time_col)

            user_id = None
            if user_col and user_col in row.index and pd.notna(row[user_col]):
                user_id = str(row[user_col]).strip()
            case_id_val = None
            if case_id_col and case_id_col in row.index and pd.notna(row[case_id_col]):
                case_id_val = str(row[case_id_col]).strip()
            if not user_id and case_id_val:
                user_id = case_id_val

            raw_record = {}
            for c in df.columns:
                if c.startswith("__"):
                    continue
                v = row.get(c)
                raw_record[c] = "" if (v is None or (isinstance(v, float) and pd.isna(v))) else str(v)

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
            })
        return events

    def _split_cases(self, all_events: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        """Group by case_id when present, else by user_id. Split by Employee profile creation when no case_id."""
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
            hr_start = {"Employee profile creation", "Employee ID generated", "Job posting / advertisement"}
            for uid, evs in by_user.items():
                evs_sorted = sorted(evs, key=lambda x: x.get("timestamp") or pd.Timestamp.min)
                cases: List[List[Dict]] = []
                current: List[Dict] = []
                has_start = False
                for ev in evs_sorted:
                    name = ev.get("event", "")
                    events_in_current = {e.get("event", "") for e in current}
                    if name in events_in_current:
                        if current:
                            cases.append(current)
                            current = []
                            has_start = False
                        current.append(ev)
                        has_start = name in hr_start
                        continue
                    if name in hr_start:
                        if current and has_start:
                            cases.append(current)
                            current = []
                            has_start = False
                        current.append(ev)
                        has_start = True
                    else:
                        if not current:
                            current = []
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
                if hasattr(ts, "strftime"):
                    ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")
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
        """Find events where multiple case IDs have the same timestamp."""
        by_key: Dict[Tuple[str, str], List[int]] = {}
        for p in case_paths:
            seq = p.get("path_sequence", [])
            timings = p.get("timings", [])
            case_id = p.get("case_id")
            for j in range(1, len(seq) - 1):
                event = seq[j]
                if event in ("Process", "End"):
                    continue
                t = timings[j - 1] if j - 1 < len(timings) else {}
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
        all_events = []
        for table in tables:
            df = dataframes.get(table.table_name)
            if df is None or df.empty:
                continue
            events = self._table_to_events(table, df)
            all_events.extend(events)

        if not all_events:
            return {
                "success": False,
                "error": "No HR events with usable timestamps found across tables.",
                "tables_checked": [t.table_name for t in tables],
            }

        all_events.sort(key=lambda e: e["timestamp"])
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
                f"We found {len(case_details)} case(s). Each case is one HR journey (recruitment/onboarding/attendance/payroll/performance/training/exit).",
                "Case IDs are numbered in order of first event time (ascending).",
                "Events are grouped by employee and sorted by timestamp. Same activity meaning again (duplicate or different source) starts a new Case ID so each case is one clean process flow.",
                "Events are observed from your uploaded columns and row data across all tables.",
                f"Event types found: {', '.join(observed) or '—'}.",
            ],
            "unified_flow_data": unified_flow_data,
        }

