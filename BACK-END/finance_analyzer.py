"""
Finance Process Timeline Analyzer
---------------------------------
For Finance domain only. Events observed from columns and row data across DB/tables.

Goal:
- Find user column (customer_id, user_id, account_holder_id).
- Case IDs: sorted by datetime ascending.
- Each user can have multiple case IDs (timestamp-wise split).
- Events from event_name column (row data) OR timestamp column name OR row value patterns.
- 20 events: Customer_Registered, KYC_Completed, Account_Opened, Account_Closed,
  Login, Logout, Deposit, Withdrawal, Transfer_Initiated, Transfer_Completed,
  Payment_Initiated, Payment_Success, Payment_Failed, Loan_Applied, Loan_Approved,
  Loan_Disbursed, Policy_Purchased, Premium_Paid, Claim_Requested, Claim_Paid.
"""

from typing import Dict, List, Any, Optional, Tuple

import pandas as pd

from models import TableAnalysis


FINANCE_CASE_GAP_HOURS = 24.0

# Finance event column patterns - observed from data (columns + row values)
FINANCE_EVENT_COLUMN_PATTERNS = [
    "customer_registered", "kyc_completed", "account_opened", "account_closed",
    "login", "logout", "deposit", "withdrawal",
    "transfer_initiated", "transfer_completed",
    "payment_initiated", "payment_success", "payment_failed",
    "loan_applied", "loan_approved", "loan_disbursed",
    "policy_purchased", "premium_paid", "claim_requested", "claim_paid",
    "application_submitted", "application_reviewed", "proposal_generated", "proposal_accepted",
    "identity_verified", "address_verified", "income_verified",
    "beneficiary_added", "beneficiary_updated",
    "coverage_activated", "coverage_changed",
    "installment_generated", "installment_paid",
    "penalty_applied", "discount_applied",
    "case_escalated", "case_resolved",
    "support_ticket_created", "support_ticket_closed",
    "account_frozen",
    "event_time", "created_at", "timestamp",
]

# Valid display names (40 events) - original 20 + new 20
FINANCE_EVENT_DISPLAY = [
    "Customer Registered", "KYC Completed", "Account Opened", "Account Closed",
    "Login", "Logout", "Deposit", "Withdrawal",
    "Transfer Initiated", "Transfer Completed",
    "Payment Initiated", "Payment Success", "Payment Failed",
    "Loan Applied", "Loan Approved", "Loan Disbursed",
    "Policy Purchased", "Premium Paid", "Claim Requested", "Claim Paid",
    "Application Submitted", "Application Reviewed", "Proposal Generated", "Proposal Accepted",
    "Identity Verified", "Address Verified", "Income Verified",
    "Beneficiary Added", "Beneficiary Updated",
    "Coverage Activated", "Coverage Changed",
    "Installment Generated", "Installment Paid",
    "Penalty Applied", "Discount Applied",
    "Case Escalated", "Case Resolved",
    "Support Ticket Created", "Support Ticket Closed",
    "Account Frozen",
]


class FinanceTimelineAnalyzer:
    """
    Analyzes finance tables. Events from event_name column (row data),
    timestamp column name, or row value pattern match. Case ID sorted by time.
    """

    def __init__(self) -> None:
        self.step_order = list(FINANCE_EVENT_DISPLAY)

    def _time_column_to_finance_event(self, col_name: str) -> str:
        """Derive event from timestamp column name. User file observed."""
        if not col_name or not str(col_name).strip():
            return "Account Opened"
        c = str(col_name).lower().replace("-", "_").replace(" ", "_")
        for suf in ["_time", "_date", "_timestamp", "_at", "_datetime"]:
            if c.endswith(suf):
                c = c[: -len(suf)]
                break
        m = {
            "customer_registered": "Customer Registered",
            "kyc_completed": "KYC Completed",
            "account_opened": "Account Opened",
            "account_closed": "Account Closed",
            "login": "Login",
            "logout": "Logout",
            "deposit": "Deposit",
            "withdrawal": "Withdrawal",
            "transfer_initiated": "Transfer Initiated",
            "transfer_completed": "Transfer Completed",
            "payment_initiated": "Payment Initiated",
            "payment_success": "Payment Success",
            "payment_failed": "Payment Failed",
            "loan_applied": "Loan Applied",
            "loan_approved": "Loan Approved",
            "loan_disbursed": "Loan Disbursed",
            "policy_purchased": "Policy Purchased",
            "premium_paid": "Premium Paid",
            "claim_requested": "Claim Requested",
            "claim_paid": "Claim Paid",
            "application_submitted": "Application Submitted",
            "application_reviewed": "Application Reviewed",
            "proposal_generated": "Proposal Generated",
            "proposal_accepted": "Proposal Accepted",
            "identity_verified": "Identity Verified",
            "address_verified": "Address Verified",
            "income_verified": "Income Verified",
            "beneficiary_added": "Beneficiary Added",
            "beneficiary_updated": "Beneficiary Updated",
            "coverage_activated": "Coverage Activated",
            "coverage_changed": "Coverage Changed",
            "installment_generated": "Installment Generated",
            "installment_paid": "Installment Paid",
            "penalty_applied": "Penalty Applied",
            "discount_applied": "Discount Applied",
            "case_escalated": "Case Escalated",
            "case_resolved": "Case Resolved",
            "support_ticket_created": "Support Ticket Created",
            "support_ticket_closed": "Support Ticket Closed",
            "account_frozen": "Account Frozen",
        }
        if c in m:
            return m[c]
        for key, ev in m.items():
            if key in c:
                return ev
        if "claim" in c:
            return "Claim Paid" if "paid" in c else "Claim Requested"
        if "loan" in c:
            return "Loan Disbursed" if "disbursed" in c else ("Loan Approved" if "approved" in c else "Loan Applied")
        if "transfer" in c:
            return "Transfer Completed" if "completed" in c else "Transfer Initiated"
        if "payment" in c:
            return "Payment Success" if "success" in c else ("Payment Failed" if "failed" in c else "Payment Initiated")
        if "account" in c:
            return "Account Closed" if "closed" in c else "Account Opened"
        return "Account Opened"

    def _find_user_col(self, df: pd.DataFrame) -> Optional[str]:
        """Detect user/customer/account holder column."""
        candidates = [
            "customer_id", "user_id", "account_holder_id", "client_id",
            "member_id", "account_id",
        ]
        cols_lower = {c.lower(): c for c in df.columns}
        for cand in candidates:
            if cand in cols_lower:
                return cols_lower[cand]
        for col in df.columns:
            cl = col.lower()
            if any(k in cl for k in ["customer", "user", "account_holder", "client"]) and "id" in cl:
                return col
        return None

    def _find_event_name_col(self, df: pd.DataFrame) -> Optional[str]:
        """Detect column with event type as DATA (row values). Check row values match finance event patterns."""
        candidates = ["event_name", "event_type", "action", "event", "step_name", "activity", "status", "transaction_type"]
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
                # Row data pattern: values that look like our 40 events (with underscore/space)
                valid = sum(
                    1 for v in vals
                    if self._normalize_event_from_data(v) in FINANCE_EVENT_DISPLAY
                    or any(tok in v.lower().replace(" ", "_") for tok in [
                        "customer_registered", "kyc", "account_opened", "account_closed",
                        "login", "logout", "deposit", "withdrawal", "transfer", "payment",
                        "loan", "policy", "premium", "claim",
                        "application_submitted", "application_reviewed", "proposal_generated",
                        "identity_verified", "address_verified", "income_verified",
                        "beneficiary_added", "beneficiary_updated",
                        "coverage_activated", "coverage_changed",
                        "installment_generated", "installment_paid",
                        "penalty_applied", "discount_applied",
                        "case_escalated", "case_resolved",
                        "support_ticket_created", "support_ticket_closed",
                        "account_frozen"
                    ])
                )
                if valid >= min(2, len(vals)):
                    return col
        for col in df.columns:
            cl = col.lower()
            if "event" in cl and "time" not in cl and "date" not in cl:
                return col
            if "action" in cl or "activity" in cl or "transaction_type" in cl:
                return col
        return None

    def _find_case_id_col(self, df: pd.DataFrame) -> Optional[str]:
        """Detect case_id for grouping."""
        cols_lower = {c.lower(): c for c in df.columns}
        for cand in ["case_id", "caseid", "session_id", "journey_id", "account_id"]:
            if cand in cols_lower:
                return cols_lower[cand]
        return None

    def _normalize_event_from_data(self, val: Any) -> str:
        """Customer_Registered -> Customer Registered; map to one of 20 finance events."""
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return "Account Opened"
        s = str(val).strip()
        if not s:
            return "Account Opened"
        normalized = s.replace("_", " ").replace("-", " ").title()
        if "Kyc" in normalized and "KYC" not in normalized:
            normalized = normalized.replace("Kyc", "KYC")
        # Map common variants to exact display name
        low = s.lower().replace(" ", "_").replace("-", "_")
        for disp in FINANCE_EVENT_DISPLAY:
            key = disp.lower().replace(" ", "_")
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
            if any(pat in cl for pat in FINANCE_EVENT_COLUMN_PATTERNS) and is_parseable(col):
                return (col, None)
        preferred = [
            "event_time", "event_timestamp", "created_at", "transaction_date",
            "login_time", "logout_time", "timestamp", "updated_at",
            "account_opened", "kyc_completed", "payment_date", "transfer_date",
            "application_submitted", "application_reviewed", "proposal_generated",
            "identity_verified", "address_verified", "income_verified",
            "beneficiary_added", "coverage_activated", "installment_generated",
            "installment_paid", "case_escalated", "support_ticket_created",
            "account_frozen",
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
        mapping = {
            "Customer Registered": "Customer registered",
            "KYC Completed": "KYC verification completed",
            "Account Opened": "Account opened",
            "Account Closed": "Account closed",
            "Login": "User logged in",
            "Logout": "User logged out",
            "Deposit": "Deposit recorded",
            "Withdrawal": "Withdrawal recorded",
            "Transfer Initiated": "Transfer initiated",
            "Transfer Completed": "Transfer completed",
            "Payment Initiated": "Payment initiated",
            "Payment Success": "Payment successful",
            "Payment Failed": "Payment failed",
            "Loan Applied": "Loan application submitted",
            "Loan Approved": "Loan approved",
            "Loan Disbursed": "Loan disbursed",
            "Policy Purchased": "Policy purchased",
            "Premium Paid": "Premium paid",
            "Claim Requested": "Claim requested",
            "Claim Paid": "Claim paid",
            "Application Submitted": "Application submitted",
            "Application Reviewed": "Application reviewed",
            "Proposal Generated": "Proposal generated",
            "Proposal Accepted": "Proposal accepted",
            "Identity Verified": "Identity verified",
            "Address Verified": "Address verified",
            "Income Verified": "Income verified",
            "Beneficiary Added": "Beneficiary added",
            "Beneficiary Updated": "Beneficiary updated",
            "Coverage Activated": "Coverage activated",
            "Coverage Changed": "Coverage changed",
            "Installment Generated": "Installment generated",
            "Installment Paid": "Installment paid",
            "Penalty Applied": "Penalty applied",
            "Discount Applied": "Discount applied",
            "Case Escalated": "Case escalated",
            "Case Resolved": "Case resolved",
            "Support Ticket Created": "Support ticket created",
            "Support Ticket Closed": "Support ticket closed",
            "Account Frozen": "Account frozen",
        }
        core = mapping.get(event_name, event_name.replace("_", " "))
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
            if normalized in FINANCE_EVENT_DISPLAY:
                return normalized
            low = s.lower().replace(" ", "_").replace("-", "_")
            for disp in FINANCE_EVENT_DISPLAY:
                key = disp.lower().replace(" ", "_")
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
                event_name = scanned if scanned else self._time_column_to_finance_event(event_time_col)

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
        """Group by case_id when present, else by user_id. Split by Account Opened when no case_id."""
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
            account_start = {"Account Opened", "Customer Registered"}
            for uid, evs in by_user.items():
                evs_sorted = sorted(evs, key=lambda x: x.get("timestamp") or pd.Timestamp.min)
                cases: List[List[Dict]] = []
                current: List[Dict] = []
                has_start = False
                for ev in evs_sorted:
                    name = ev.get("event", "")
                    if name in account_start:
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
                "error": "No finance events with usable timestamps found across tables.",
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
            "total_customers": len(set(c["user_id"] for c in case_details)),
            "customers": list(dict.fromkeys(c["user_id"] for c in case_details)),
            "explanations": [
                f"We found {len(case_details)} case(s). Each case is one finance journey (account/transaction/loan/claim).",
                "Case IDs are numbered in order of first event time (ascending).",
                "Events are observed from your uploaded columns and row data across all tables.",
                f"Event types found: {', '.join(observed) or '—'}.",
            ],
            "unified_flow_data": unified_flow_data,
        }
