"""
Insurance Process Timeline Analyzer
----------------------------------
For Insurance domain only. Same logic as Banking, Healthcare, Retail.

Goal:
- Find user column (customer_id, policyholder_id, insured_id).
- Case IDs: sorted by datetime (date, time, PM/AM, years, seconds).
- Each user can have multiple case IDs (timestamp-wise split).
- Each case = sequence of steps/events.
- Events derived from user uploaded file columns (observed, no hardcode).
- Diagram: dynamic event names, case IDs with different colors.

Valid insurance events (column-observed):
Customer_Registered, KYC_Completed, Policy_Quoted, Policy_Purchased,
Policy_Activated, Premium_Due, Premium_Paid, Payment_Failed, Policy_Renewed,
Policy_Expired, Claim_Requested, Claim_Registered, Claim_Verified, Claim_Assessed,
Claim_Approved, Claim_Rejected, Claim_Paid, Nominee_Updated, Policy_Cancelled, Policy_Closed.
"""

from typing import Dict, List, Any, Optional, Tuple

import pandas as pd

from models import TableAnalysis


INSURANCE_CASE_GAP_HOURS = 24.0

# Insurance event column patterns - user file observed (40 events)
INSURANCE_EVENT_COLUMN_PATTERNS = [
    "customer_registered", "kyc_completed", "policy_quoted", "policy_purchased",
    "policy_activated", "premium_due", "premium_paid", "payment_failed",
    "policy_renewed", "policy_expired", "claim_requested", "claim_registered",
    "claim_verified", "claim_assessed", "claim_approved", "claim_rejected",
    "claim_paid", "nominee_updated", "policy_cancelled", "policy_closed",
    "document_submitted", "document_verified", "medical_test_scheduled", "medical_test_completed",
    "risk_assessed", "underwriting_started", "underwriting_completed", "premium_calculated",
    "auto_debit_enabled", "auto_debit_disabled", "reminder_sent", "grace_period_started",
    "grace_period_ended", "policy_suspended", "reinstatement_requested", "policy_reinstated",
    "payout_initiated", "payout_completed", "fraud_check_started", "fraud_check_cleared",
    "event_time", "created_at", "timestamp",
]


class InsuranceTimelineAnalyzer:
    """
    Analyzes insurance tables. Same logic as retail/healthcare/banking.
    Events from event_name column (data) or timestamp column name. No hardcode.
    """

    def __init__(self) -> None:
        self.step_order = [
            "Customer Registered", "KYC Completed", "Policy Quoted", "Policy Purchased",
            "Policy Activated", "Premium Due", "Premium Paid", "Payment Failed",
            "Policy Renewed", "Policy Expired", "Claim Requested", "Claim Registered",
            "Claim Verified", "Claim Assessed", "Claim Approved", "Claim Rejected",
            "Claim Paid", "Nominee Updated", "Policy Cancelled", "Policy Closed",
            "Document Submitted", "Document Verified", "Medical Test Scheduled", "Medical Test Completed",
            "Risk Assessed", "Underwriting Started", "Underwriting Completed", "Premium Calculated",
            "Auto Debit Enabled", "Auto Debit Disabled", "Reminder Sent", "Grace Period Started",
            "Grace Period Ended", "Policy Suspended", "Reinstatement Requested", "Policy Reinstated",
            "Payout Initiated", "Payout Completed", "Fraud Check Started", "Fraud Check Cleared",
        ]

    def _time_column_to_insurance_event(self, col_name: str) -> str:
        """Derive event from timestamp column name. User file observed."""
        if not col_name or not str(col_name).strip():
            return "Policy Purchased"
        c = str(col_name).lower().replace("-", "_").replace(" ", "_")
        for suf in ["_time", "_date", "_timestamp", "_at", "_datetime"]:
            if c.endswith(suf):
                c = c[: -len(suf)]
                break
        m = {
            "customer_registered": "Customer Registered",
            "kyc_completed": "KYC Completed",
            "policy_quoted": "Policy Quoted",
            "policy_purchased": "Policy Purchased",
            "policy_activated": "Policy Activated",
            "premium_due": "Premium Due",
            "premium_paid": "Premium Paid",
            "payment_failed": "Payment Failed",
            "policy_renewed": "Policy Renewed",
            "policy_expired": "Policy Expired",
            "claim_requested": "Claim Requested",
            "claim_registered": "Claim Registered",
            "claim_verified": "Claim Verified",
            "claim_assessed": "Claim Assessed",
            "claim_approved": "Claim Approved",
            "claim_rejected": "Claim Rejected",
            "claim_paid": "Claim Paid",
            "nominee_updated": "Nominee Updated",
            "policy_cancelled": "Policy Cancelled",
            "policy_closed": "Policy Closed",
            "document_submitted": "Document Submitted",
            "document_verified": "Document Verified",
            "medical_test_scheduled": "Medical Test Scheduled",
            "medical_test_completed": "Medical Test Completed",
            "risk_assessed": "Risk Assessed",
            "underwriting_started": "Underwriting Started",
            "underwriting_completed": "Underwriting Completed",
            "premium_calculated": "Premium Calculated",
            "auto_debit_enabled": "Auto Debit Enabled",
            "auto_debit_disabled": "Auto Debit Disabled",
            "reminder_sent": "Reminder Sent",
            "grace_period_started": "Grace Period Started",
            "grace_period_ended": "Grace Period Ended",
            "policy_suspended": "Policy Suspended",
            "reinstatement_requested": "Reinstatement Requested",
            "policy_reinstated": "Policy Reinstated",
            "payout_initiated": "Payout Initiated",
            "payout_completed": "Payout Completed",
            "fraud_check_started": "Fraud Check Started",
            "fraud_check_cleared": "Fraud Check Cleared",
        }
        if c in m:
            return m[c]
        for key, ev in m.items():
            if key in c:
                return ev
        if "claim" in c:
            return "Claim Registered" if "paid" not in c else "Claim Paid"
        if "policy" in c:
            return "Policy Purchased"
        if "premium" in c:
            return "Premium Paid" if "due" not in c else "Premium Due"
        if "kyc" in c:
            return "KYC Completed"
        return "Policy Purchased"

    def _find_user_col(self, df: pd.DataFrame) -> Optional[str]:
        """Detect user/customer/policyholder column."""
        candidates = [
            "customer_id", "policyholder_id", "insured_id", "user_id",
            "client_id", "member_id", "applicant_id",
        ]
        cols_lower = {c.lower(): c for c in df.columns}
        for cand in candidates:
            if cand in cols_lower:
                return cols_lower[cand]
        for col in df.columns:
            cl = col.lower()
            if any(k in cl for k in ["customer", "policyholder", "insured", "applicant"]) and "id" in cl:
                return col
        return None

    def _find_policy_col(self, df: pd.DataFrame) -> Optional[str]:
        """Detect policy identifier."""
        candidates = ["policy_id", "policy_no", "policy_number"]
        cols_lower = {c.lower(): c for c in df.columns}
        for cand in candidates:
            if cand in cols_lower:
                return cols_lower[cand]
        return None

    def _find_event_name_col(self, df: pd.DataFrame) -> Optional[str]:
        """Detect column with event type as DATA (event_name, event_type, etc.)."""
        candidates = ["event_name", "event_type", "action", "event", "step_name", "activity", "status"]
        cols_lower = {c.lower(): c for c in df.columns}
        for cand in candidates:
            if cand in cols_lower:
                col = cols_lower[cand]
                sample = df[col].dropna().astype(str).head(20)
                if len(sample) == 0:
                    continue
                vals = set(s.strip() for v in sample for s in str(v).split(",") if s.strip())
                if not vals:
                    continue
                valid = sum(1 for v in vals if (
                    "_" in v or " " in v or
                    any(tok in v.lower() for tok in [
                        "policy", "claim", "premium", "kyc", "customer", "payment", "nominee",
                        "document", "medical", "underwriting", "risk", "payout", "fraud", "grace", "reinstat"
                    ])
                ))
                if valid >= min(2, len(vals)):
                    return col
        for col in df.columns:
            cl = col.lower()
            if "event" in cl and "time" not in cl and "date" not in cl:
                return col
            if "action" in cl or "activity" in cl:
                return col
        return None

    def _find_case_id_col(self, df: pd.DataFrame) -> Optional[str]:
        """Detect case_id for grouping."""
        cols_lower = {c.lower(): c for c in df.columns}
        for cand in ["case_id", "caseid", "policy_id", "policyid", "session_id", "journey_id"]:
            if cand in cols_lower:
                return cols_lower[cand]
        return None

    def _normalize_event_from_data(self, val: Any) -> str:
        """Customer_Registered -> Customer Registered, KYC_Completed -> KYC Completed."""
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return "Policy Purchased"
        s = str(val).strip()
        if not s:
            return "Policy Purchased"
        normalized = s.replace("_", " ").replace("-", " ").title()
        if "Kyc" in normalized and "KYC" not in normalized:
            normalized = normalized.replace("Kyc", "KYC")
        return normalized

    def _find_datetime_columns(self, df: pd.DataFrame) -> Optional[Tuple[str, Optional[str]]]:
        """Find (date_col, time_col). Supports date, time, AM/PM, years, seconds."""

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
            if any(pat in cl for pat in INSURANCE_EVENT_COLUMN_PATTERNS) and is_parseable(col):
                return (col, None)
        preferred = [
            "event_time", "event_timestamp", "created_at", "policy_date", "claim_date",
            "premium_due_date", "premium_paid_date", "kyc_date", "effective_date",
            "expiry_date", "timestamp", "updated_at",
            "document_submitted", "document_verified", "medical_test", "underwriting",
            "premium_calculated", "grace_period", "payout", "fraud_check",
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
        policy_id: Optional[str],
        ts_str: str,
        table_name: str,
        file_name: str,
        source_row_display: str,
        raw_record: Dict[str, Any],
    ) -> str:
        """Build explanation for each event step."""
        mapping = {
            "Customer Registered": "Customer registered for insurance",
            "KYC Completed": "KYC verification completed",
            "Policy Quoted": "Policy quote generated",
            "Policy Purchased": "Policy was purchased",
            "Policy Activated": "Policy became active",
            "Premium Due": "Premium payment due",
            "Premium Paid": "Premium payment received",
            "Payment Failed": "Payment failed",
            "Policy Renewed": "Policy was renewed",
            "Policy Expired": "Policy expired",
            "Claim Requested": "Claim was requested",
            "Claim Registered": "Claim registered in system",
            "Claim Verified": "Claim verified",
            "Claim Assessed": "Claim assessed",
            "Claim Approved": "Claim approved",
            "Claim Rejected": "Claim rejected",
            "Claim Paid": "Claim payment disbursed",
            "Nominee Updated": "Nominee details updated",
            "Policy Cancelled": "Policy cancelled",
            "Policy Closed": "Policy closed",
            "Document Submitted": "Document submitted for verification",
            "Document Verified": "Document verification completed",
            "Medical Test Scheduled": "Medical test scheduled",
            "Medical Test Completed": "Medical test completed",
            "Risk Assessed": "Risk assessment completed",
            "Underwriting Started": "Underwriting process started",
            "Underwriting Completed": "Underwriting completed",
            "Premium Calculated": "Premium amount calculated",
            "Auto Debit Enabled": "Auto debit enabled for premium",
            "Auto Debit Disabled": "Auto debit disabled",
            "Reminder Sent": "Premium reminder sent",
            "Grace Period Started": "Grace period started",
            "Grace Period Ended": "Grace period ended",
            "Policy Suspended": "Policy suspended",
            "Reinstatement Requested": "Reinstatement requested",
            "Policy Reinstated": "Policy reinstated",
            "Payout Initiated": "Payout initiated",
            "Payout Completed": "Payout completed",
            "Fraud Check Started": "Fraud check started",
            "Fraud Check Cleared": "Fraud check cleared",
        }
        core = mapping.get(event_name, event_name.replace("_", " "))
        parts = [p for p in [table_name or "", file_name or "", f"row {source_row_display}" if source_row_display else ""] if p]
        origin = f" [{' · '.join(parts)}]" if parts else ""
        return f"{core}{origin}"

    def _table_to_events(self, table: TableAnalysis, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Convert table rows to events. Event from event_name col (data) or time column."""
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
        policy_col = self._find_policy_col(df)
        event_name_col = self._find_event_name_col(df)
        case_id_col = self._find_case_id_col(df)

        events: List[Dict[str, Any]] = []
        file_name = getattr(table, "file_name", "") or f"{table.table_name}.csv"

        for idx, row in df.iterrows():
            ts = row["__dt"]
            if pd.isna(ts):
                continue
            if event_name_col and event_name_col in row.index and pd.notna(row[event_name_col]):
                event_name = self._normalize_event_from_data(row[event_name_col])
            else:
                event_name = self._time_column_to_insurance_event(event_time_col)

            user_id = None
            if user_col and user_col in row.index and pd.notna(row[user_col]):
                user_id = str(row[user_col]).strip()
            case_id_val = None
            if case_id_col and case_id_col in row.index and pd.notna(row[case_id_col]):
                case_id_val = str(row[case_id_col]).strip()
            policy_id = None
            if policy_col and policy_col in row.index and pd.notna(row[policy_col]):
                policy_id = str(row[policy_col]).strip()
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
                policy_id=policy_id,
                ts_str=ts_str,
                table_name=table.table_name,
                file_name=file_name,
                source_row_display=source_row_display,
                raw_record=raw_record,
            )

            events.append({
                "user_id": user_id or case_id_val or "unknown",
                "_case_id": case_id_val,
                "policy_id": policy_id or "",
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
        """Group by case_id when present, else by user_id. Split by Policy Purchased when no case_id."""
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
            policy_start = {"Policy Purchased", "Policy Quoted"}
            for uid, evs in by_user.items():
                evs_sorted = sorted(evs, key=lambda x: x.get("timestamp") or pd.Timestamp.min)
                cases: List[List[Dict]] = []
                current: List[Dict] = []
                has_policy = False
                for ev in evs_sorted:
                    name = ev.get("event", "")
                    if name in policy_start:
                        if current and has_policy:
                            cases.append(current)
                            current = []
                            has_policy = False
                        current.append(ev)
                        has_policy = True
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
            return f"Customer {user_id} · No steps."
        first, last = events[0], events[-1]
        start_str = first.get("timestamp_str", "")
        end_str = last.get("timestamp_str", "")
        if hasattr(first.get("timestamp"), "strftime"):
            start_str = first["timestamp"].strftime("%Y-%m-%d %H:%M")
        if hasattr(last.get("timestamp"), "strftime"):
            end_str = last["timestamp"].strftime("%Y-%m-%d %H:%M")
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
                    "policy_id": ev.get("policy_id", ""),
                    "table_name": ev.get("table_name"),
                    "file_name": ev.get("file_name"),
                    "source_row": ev.get("source_row"),
                    "event_story": ev.get("event_story"),
                    "raw_record": {k: ("" if v is None else str(v)) for k, v in raw.items()},
                })
            case_details.append({
                "case_id": case_id,
                "user_id": user_id,
                "customer_id": user_id,
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
        """Find events where multiple case IDs have the same timestamp. Returns list of {event, timestamp_str, case_ids}."""
        by_key: Dict[Tuple[str, str], List[int]] = {}
        for p in case_paths:
            seq = p.get("path_sequence", [])
            timings = p.get("timings", [])
            case_id = p.get("case_id")
            for j in range(1, len(seq) - 1):
                event = seq[j]
                if event in ("Process", "End"):
                    continue
                # timings[j-1] is segment ending at seq[j]
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
            "#8B5CF6", "#EC4899", "#F59E0B", "#10B981", "#3B82F6",
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
        return {
            "all_event_types": all_event_types,
            "case_paths": case_paths,
            "total_cases": len(case_paths),
            "same_time_groups": same_time_groups,
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
                "error": "No insurance events with usable timestamps found.",
                "tables_checked": [t.table_name for t in tables],
            }

        all_events.sort(key=lambda e: e["timestamp"])
        cases = self._split_cases(all_events)
        case_details = self._assign_case_ids(cases)
        unified_flow_data = self._generate_unified_flow_data(case_details)

        skip_keys = {"timestamp", "_case_id", "_event_time_column"}
        sanitized = [{k: v for k, v in e.items() if k not in skip_keys} for e in all_events]

        first_ts = all_events[0]["timestamp"]
        last_ts = all_events[-1]["timestamp"]
        observed = list(dict.fromkeys(a.get("event", "") for c in case_details for a in c.get("activities", [])))

        return {
            "success": True,
            "sorted_timeline": sanitized,
            "first_datetime": first_ts.strftime("%Y-%m-%d %H:%M:%S") if hasattr(first_ts, "strftime") else "",
            "last_datetime": last_ts.strftime("%Y-%m-%d %H:%M:%S") if hasattr(last_ts, "strftime") else "",
            "total_events": len(sanitized),
            "total_activities": len(sanitized),
            "case_ids": [c["case_id"] for c in case_details],
            "case_details": case_details,
            "total_cases": len(case_details),
            "total_customers": len(set(c["user_id"] for c in case_details)),
            "customers": list(dict.fromkeys(c["user_id"] for c in case_details)),
            "explanations": [
                f"We found {len(case_details)} case(s). Each case is one insurance journey (policy lifecycle or claim).",
                "Case IDs are numbered in order of first event time.",
                "Events derived from your uploaded columns (observed, no hardcode).",
                f"Event types: {', '.join(observed) or '—'}.",
            ],
            "unified_flow_data": unified_flow_data,
        }
