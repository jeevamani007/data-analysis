"""
Retail Process Timeline Analyzer
--------------------------------
For Retail / E‑commerce domain only.

Goal:
- Group all records by user_id (customer_id).
- For each user, sort all events by timestamp ascending.
- Identify process runs (cases) representing one order journey:
  User Signed Up → Login → Product Viewed → Added to Cart →
  Order Created → Payment → Delivery → Return/Refund.
- When a new order starts for the same user, start a NEW case_id.
- Do NOT merge repeated patterns; each order run = new Case_N.
- Finally, sort all cases globally by their first timestamp.

Output:
- case_details: list of cases with activities, event_sequence, explanation.
- unified_flow_data: Start ----|----|---- End diagram-friendly structure.
"""

from typing import Dict, List, Any, Optional, Tuple

import pandas as pd

from models import TableAnalysis


RETAIL_CASE_GAP_HOURS = 24.0  # fallback gap-based split if needed


class RetailTimelineAnalyzer:
    """
    Analyzes retail / e‑commerce tables and builds customer order journeys
    as Case IDs. Logic is pattern-based (table / column names, simple status
    values) – no hardcoded schema or SQL.
    """

    def __init__(self) -> None:
        # Canonical retail steps (used for ordering / display)
        # USER SIDE: Signup, Login, Logout
        # SHOPPING: Product Viewed, Added to Cart, Removed from Cart
        # ORDER & PAYMENT: Order Created, Payment Initiated/Completed/Failed
        # DELIVERY: Order Packed, Shipped, Out for Delivery, Delivered
        self.step_order = [
            "User Signed Up",
            "User Logged In",
            "User Logged Out",
            "Product Viewed",
            "Added to Cart",
            "Removed from Cart",
            "Order Created",
            "Payment Initiated",
            "Payment Completed",
            "Payment Failed",
            "Invoice Generated",
            "Order Packed",
            "Order Shipped",
            "Out for Delivery",
            "Order Delivered",
            "Return Requested",
            "Product Returned",
            "Refund Initiated",
            "Refund Completed",
        ]

    # ------------------------------------------------------------------
    # Basic column detection helpers
    # ------------------------------------------------------------------

    def _find_user_col(self, df: pd.DataFrame) -> Optional[str]:
        """Detect user/customer ID column."""
        candidates = [
            "customer_id",
            "user_id",
            "client_id",
            "member_id",
            "shopper_id",
            "buyer_id",
        ]
        cols_lower = {c.lower(): c for c in df.columns}
        for cand in candidates:
            if cand in cols_lower:
                return cols_lower[cand]
        for col in df.columns:
            cl = col.lower()
            if "customer" in cl or "shopper" in cl or "buyer" in cl or "user" in cl:
                if "id" in cl or cl in ("customer", "user", "buyer", "shopper"):
                    return col
        return None

    def _find_order_col(self, df: pd.DataFrame) -> Optional[str]:
        """Detect order identifier (if present)."""
        candidates = ["order_id", "order_no", "order_number", "invoice_id", "invoice_no", "bill_no"]
        cols_lower = {c.lower(): c for c in df.columns}
        for cand in candidates:
            if cand in cols_lower:
                return cols_lower[cand]
        for col in df.columns:
            cl = col.lower()
            if "order" in cl and "id" in cl:
                return col
            if "invoice" in cl and "id" in cl:
                return col
        return None

    def _find_status_col(self, df: pd.DataFrame) -> Optional[str]:
        """Detect generic status column (order_status, payment_status, etc.)."""
        for col in df.columns:
            cl = col.lower()
            if "status" in cl or "state" in cl or "stage" in cl:
                return col
        return None

    def _find_datetime_columns(self, df: pd.DataFrame) -> Optional[Tuple[str, Optional[str]]]:
        """
        Find (date_col, time_col) for EVENT TIME – when the retail action happened.
        Prefers columns with date/time/timestamp/created/order/payment semantics.
        """

        def is_parseable(col: str) -> bool:
            try:
                sample = df[col].dropna().head(10)
                if len(sample) == 0:
                    return False
                parsed = pd.to_datetime(sample, errors="coerce")
                return parsed.notna().sum() >= len(sample) * 0.5
            except Exception:
                return False

        # 1) Look for explicit timestamp-like columns
        preferred_keywords = [
            "event_time",
            "event_timestamp",
            "created_at",
            "created_time",
            "created_timestamp",
            "order_timestamp",
            "order_time",
            "payment_time",
            "payment_timestamp",
            "shipment_time",
            "delivery_time",
            "updated_at",
        ]
        for col in df.columns:
            cl = col.lower()
            if any(k in cl for k in preferred_keywords) and is_parseable(col):
                return (col, None)

        # 2) Generic date + optional time columns
        date_candidates: List[str] = []
        time_candidates: List[str] = []
        for col in df.columns:
            cl = col.lower()
            if any(k in cl for k in ["date", "timestamp", "created", "ordered"]) and is_parseable(col):
                date_candidates.append(col)
            elif "time" in cl and "stamp" not in cl:
                time_candidates.append(col)

        if date_candidates:
            date_col = date_candidates[0]
            # try to pair with any time column
            time_col = time_candidates[0] if time_candidates else None
            return (date_col, time_col if time_col in df.columns else None)

        # 3) Fallback: any parseable column
        for col in df.columns:
            if is_parseable(col):
                return (col, None)
        return None

    # ------------------------------------------------------------------
    # Event inference
    # ------------------------------------------------------------------

    def _get_status_value(self, row: pd.Series, status_col: Optional[str]) -> str:
        if status_col and status_col in row.index:
            val = row[status_col]
            if pd.isna(val):
                return ""
            return str(val).strip().lower()
        return ""

    def _infer_event_for_row(
        self,
        table_name: str,
        df: pd.DataFrame,
        row: pd.Series,
        status_col: Optional[str],
    ) -> str:
        """
        Infer a human-friendly retail event name from table / column context and row status.
        The output is one of the canonical step names (or a generic fallback).
        """
        t = table_name.lower().replace("_", " ")
        status = self._get_status_value(row, status_col)

        # Customer master / registration (User Signed Up)
        if any(k in t for k in ["customer", "user", "shopper", "buyer"]) and not any(
            k in t for k in ["order", "invoice", "payment", "transaction", "login", "session"]
        ):
            return "User Signed Up"

        # Login / session tables
        if any(k in t for k in ["login", "signin", "session", "auth"]):
            if any(x in status for x in ["logout", "exit", "signout"]):
                return "User Logged Out"
            return "User Logged In"

        # Browsing / product views (Product Viewed)
        if any(k in t for k in ["pageview", "page_view", "product_view", "view", "browse"]):
            return "Product Viewed"

        # Cart
        if "cart" in t or "basket" in t:
            if any(x in status for x in ["remove", "delete", "removed"]):
                return "Removed from Cart"
            return "Added to Cart"

        # Order table (most important)
        if "order" in t and not any(k in t for k in ["return", "refund", "shipment", "shipping", "delivery"]):
            if any(x in status for x in ["fail", "reject", "error", "declined"]):
                return "Payment Failed"
            if any(x in status for x in ["initiat", "pending", "created", "placed", "new"]):
                return "Order Created"
            if any(x in status for x in ["paid", "complete", "success", "captur"]):
                return "Payment Completed"
            return "Order Created"

        # Payment
        if "payment" in t or "pg_" in t or "gateway" in t:
            if any(x in status for x in ["fail", "reject", "error", "declined"]):
                return "Payment Failed"
            if any(x in status for x in ["initiat", "pending", "started"]):
                return "Payment Initiated"
            if any(x in status for x in ["paid", "success", "complete", "captur"]):
                return "Payment Completed"
            return "Payment Completed"

        # Invoice / bill
        if "invoice" in t or "bill" in t:
            return "Invoice Generated"

        # Fulfilment & logistics
        if "pack" in t:
            return "Order Packed"
        if "ship" in t or "dispatch" in t or "courier" in t:
            return "Order Shipped"
        if "out_for_delivery" in t or "out for delivery" in t.replace("_", " "):
            return "Out for Delivery"
        if "deliver" in t:
            return "Order Delivered"

        # Returns / refunds
        if "return" in t:
            if any(x in status for x in ["request", "initiated", "pending"]):
                return "Return Requested"
            return "Product Returned"
        if "refund" in t:
            if any(x in status for x in ["initiated", "pending", "processing"]):
                return "Refund Initiated"
            return "Refund Completed"

        # Fallback: map table name to generic step
        # This still keeps the case structure but with understandable labels.
        if "transaction" in t or "payment" in t:
            return "Payment Completed"
        if "shipment" in t or "shipping" in t:
            return "Order Shipped"

        # Fallback: check columns if table name matching was ambiguous
        cols_lower = set(c.lower() for c in df.columns)
        if "signup_date" in cols_lower:
            return "User Signed Up"
        if "order_date" in cols_lower or ("order_id" in cols_lower and "total_amount" in cols_lower):
            # Check status again to be precise if possible
            if any(x in status for x in ["paid", "complete"]):
                return "Payment Completed"
            return "Order Created"

        # Default
        return (table_name or "Retail Event").replace("_", " ").title()

    def _build_event_story(
        self,
        event_name: str,
        user_id: Optional[str],
        order_id: Optional[str],
        ts_str: str,
        table_name: str,
        file_name: str,
        source_row_display: str,
        raw_record: Dict[str, Any],
    ) -> str:
        """
        Build a concise English explanation for each step in a case.
        Format: "Explanation [table · file · row]" – user data across DB.
        """
        # English explanations per event (user/customer side, shopping, order, delivery)
        mapping = {
            "User Signed Up": "Customer created account for the first time",
            "User Logged In": "Customer opened app/website and logged in",
            "User Logged Out": "Customer signed out / exited",
            "Product Viewed": "Customer viewed a product",
            "Added to Cart": "Customer added product to cart",
            "Removed from Cart": "Customer removed product from cart",
            "Order Created": "Customer clicked Place Order",
            "Payment Initiated": "Customer started payment",
            "Payment Completed": "Payment succeeded",
            "Payment Failed": "Payment failed",
            "Invoice Generated": "Invoice was generated for the order",
            "Order Packed": "Shop packed the product",
            "Order Shipped": "Handed over to courier",
            "Out for Delivery": "Courier on the way to customer",
            "Order Delivered": "Product reached customer",
            "Return Requested": "Customer requested a return",
            "Product Returned": "Product was returned",
            "Refund Initiated": "Refund process started",
            "Refund Completed": "Refund completed",
            # Legacy names
            "Customer Registered": "Customer created account for the first time",
            "Customer Login": "Customer opened app/website and logged in",
            "Customer Browsed Product": "Customer viewed a product",
        }
        core = mapping.get(event_name, event_name.replace("_", " "))
        parts = [p for p in [table_name or "", file_name or "", f"row {source_row_display}" if source_row_display else ""] if p]
        origin = f" [{' · '.join(parts)}]" if parts else ""
        return f"{core}{origin}"

    # ------------------------------------------------------------------
    # Convert tables to per-row events
    # ------------------------------------------------------------------

    def _table_to_events(
        self,
        table: TableAnalysis,
        df: pd.DataFrame,
    ) -> List[Dict[str, Any]]:
        cols = self._find_datetime_columns(df)
        if not cols:
            return []
        date_col, time_col = cols
        df = df.copy()

        # Build unified datetime column
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
        order_col = self._find_order_col(df)
        status_col = self._find_status_col(df)

        events: List[Dict[str, Any]] = []
        file_name = getattr(table, "file_name", "") or f"{table.table_name}.csv"

        for idx, row in df.iterrows():
            ts = row["__dt"]
            event_name = self._infer_event_for_row(table.table_name, df, row, status_col)
            if pd.isna(ts):
                continue
            user_id = None
            if user_col and user_col in row.index and pd.notna(row[user_col]):
                user_id = str(row[user_col]).strip()
            order_id = None
            if order_col and order_col in row.index and pd.notna(row[order_col]):
                order_id = str(row[order_col]).strip()

            raw_record = {}
            for c in df.columns:
                if c.startswith("__"):
                    continue
                v = row.get(c)
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    raw_record[c] = ""
                else:
                    raw_record[c] = str(v)

            ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")
            # Display row number as 1-based when possible
            source_row_index = int(idx) if str(idx).isdigit() else None
            source_row_display = str(source_row_index + 1) if source_row_index is not None else ""
            event_story = self._build_event_story(
                event_name=event_name,
                user_id=user_id or "unknown",
                order_id=order_id,
                ts_str=ts_str,
                table_name=table.table_name,
                file_name=file_name,
                source_row_display=source_row_display,
                raw_record=raw_record,
            )

            events.append(
                {
                    "user_id": user_id or "unknown",
                    "order_id": order_id or "",
                    "event": event_name,
                    "timestamp": ts,
                    "timestamp_str": ts_str,
                    "table_name": table.table_name,
                    "file_name": file_name,
                    "source_row": int(idx) if str(idx).isdigit() else str(idx),
                    "raw_record": raw_record,
                    "event_story": event_story,
                }
            )
        return events

    # ------------------------------------------------------------------
    # Case detection
    # ------------------------------------------------------------------

    def _identify_cases_for_user(self, events: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        """
        For one user: events are already time-sorted.

        Rules:
        - Every time we see "Order Created", we start a NEW process run (case),
          except the very first one which attaches any earlier events (registration,
          login, browse, cart) as the start of Case_1.
        - If user never creates an order, all events form a single case.
        """
        if not events:
            return []

        cases: List[List[Dict[str, Any]]] = []
        pending_pre_order: List[Dict[str, Any]] = []
        current: List[Dict[str, Any]] = []
        has_order_in_current = False

        for ev in events:
            name = ev.get("event", "")
            if name == "Order Created":
                # Close existing case (if it already has an order)
                if current and has_order_in_current:
                    cases.append(current)
                    current = []
                    has_order_in_current = False

                # First order for this user: attach any pre-order context
                if not current and pending_pre_order:
                    current = pending_pre_order.copy()
                    pending_pre_order = []

                current.append(ev)
                has_order_in_current = True
            else:
                if current:
                    current.append(ev)
                else:
                    # Before first order – keep as context
                    pending_pre_order.append(ev)

        if current:
            cases.append(current)
        elif pending_pre_order:
            # No orders at all: still create one case for this customer's activity
            cases.append(pending_pre_order)

        return cases

    def _split_cases(self, all_events: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        """Group by user, then split into cases using order-based logic."""
        by_user: Dict[str, List[Dict[str, Any]]] = {}
        for ev in all_events:
            uid = ev.get("user_id") or "unknown"
            by_user.setdefault(uid, []).append(ev)

        all_cases: List[List[Dict[str, Any]]] = []
        for uid, events in by_user.items():
            events_sorted = sorted(events, key=lambda x: x["timestamp"])
            user_cases = self._identify_cases_for_user(events_sorted)
            all_cases.extend(user_cases)

        # Sort all cases globally by first timestamp (ascending)
        all_cases.sort(key=lambda c: c[0]["timestamp"] if c and c[0].get("timestamp") is not None else pd.Timestamp.min)
        return all_cases

    # ------------------------------------------------------------------
    # Case details + explanations
    # ------------------------------------------------------------------

    def _event_phrase(self, step: str) -> str:
        """Convert canonical step label into short phrase for case explanation."""
        mapping = {
            "User Signed Up": "signed up",
            "User Logged In": "logged in",
            "User Logged Out": "logged out",
            "Product Viewed": "viewed product",
            "Added to Cart": "added to cart",
            "Removed from Cart": "removed from cart",
            "Order Created": "placed order",
            "Payment Initiated": "payment started",
            "Payment Completed": "payment completed",
            "Payment Failed": "payment failed",
            "Invoice Generated": "invoice generated",
            "Order Packed": "order packed",
            "Order Shipped": "order shipped",
            "Out for Delivery": "out for delivery",
            "Order Delivered": "order delivered",
            "Return Requested": "return requested",
            "Product Returned": "product returned",
            "Refund Initiated": "refund started",
            "Refund Completed": "refund completed",
            "Customer Registered": "signed up",
            "Customer Login": "logged in",
            "Customer Browsed Product": "viewed product",
        }
        return mapping.get(step, step.lower().replace("_", " "))

    def _build_case_explanation(
        self,
        case_id: int,
        user_id: str,
        events: List[Dict[str, Any]],
    ) -> str:
        """Concise case summary: user, steps, time range."""
        if not events:
            return f"Customer {user_id} · No steps."

        first = events[0]
        last = events[-1]
        start_ts = first.get("timestamp")
        end_ts = last.get("timestamp")
        start_str = start_ts.strftime("%Y-%m-%d %H:%M") if hasattr(start_ts, "strftime") else first.get("timestamp_str", "")
        end_str = end_ts.strftime("%Y-%m-%d %H:%M") if hasattr(end_ts, "strftime") else last.get("timestamp_str", "")

        steps = [ev.get("event", "") for ev in events]
        canonical = [s for s in steps if s in self.step_order] or steps
        phrases = [self._event_phrase(s) for s in canonical]
        seq_text = " → ".join(phrases) if phrases else " → ".join(steps)

        return f"{user_id} · {seq_text} · {start_str} → {end_str}"

    def _assign_case_ids(self, cases: List[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """Attach case_id and build activities structure per case."""
        case_details: List[Dict[str, Any]] = []
        for idx, events in enumerate(cases):
            if not events:
                continue
            case_id = idx + 1
            user_id = events[0].get("user_id") or "unknown"
            activities: List[Dict[str, Any]] = []
            for ev in events:
                ts = ev.get("timestamp")
                ts_str = ev.get("timestamp_str", "")
                if hasattr(ts, "strftime"):
                    ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")
                raw = ev.get("raw_record") or {}
                activities.append(
                    {
                        "event": ev.get("event"),
                        "timestamp_str": ts_str,
                        "user_id": user_id,
                        "order_id": ev.get("order_id", ""),
                        "table_name": ev.get("table_name"),
                        "file_name": ev.get("file_name"),
                        "source_row": ev.get("source_row"),
                        "event_story": ev.get("event_story"),
                        "raw_record": {
                            k: ("" if v is None else str(v)) for k, v in raw.items()
                        },
                    }
                )

            first_ts = activities[0]["timestamp_str"]
            last_ts = activities[-1]["timestamp_str"]
            event_sequence = [a["event"] for a in activities]
            explanation = self._build_case_explanation(case_id, user_id, events)

            case_details.append(
                {
                    "case_id": case_id,
                    "user_id": user_id,
                    "customer_id": user_id,
                    "first_activity_timestamp": first_ts,
                    "last_activity_timestamp": last_ts,
                    "activity_count": len(activities),
                    "activities": activities,
                    "event_sequence": event_sequence,
                    "explanation": explanation,
                }
            )
        return case_details

    # ------------------------------------------------------------------
    # Unified flow data for diagram
    # ------------------------------------------------------------------

    def _generate_unified_flow_data(self, case_details: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Unified Process → steps → End with timings, same shape as banking/healthcare."""
        colors = [
            "#F97316",
            "#0EA5E9",
            "#22C55E",
            "#A855F7",
            "#EC4899",
            "#FACC15",
            "#14B8A6",
            "#6366F1",
            "#EF4444",
            "#3B82F6",
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
                    duration_seconds = max(0, int((ts - prev_ts).total_seconds()))
                    days = duration_seconds // 86400
                    hours = (duration_seconds % 86400) // 3600
                    minutes = (duration_seconds % 3600) // 60
                    seconds = duration_seconds % 60
                    if days > 0:
                        time_label = f"{days} day{'s' if days != 1 else ''} {hours} hr"
                    elif hours > 0:
                        time_label = (
                            f"{hours} hr {minutes} min" if minutes else f"{hours} hr"
                        )
                    elif minutes > 0:
                        time_label = (
                            f"{minutes} min {seconds} sec"
                            if seconds
                            else f"{minutes} min"
                        )
                    else:
                        time_label = f"{seconds} sec"
                else:
                    duration_seconds = 0
                    time_label = "Start" if prev_event_name == "Process" else "0 sec"

                path_sequence.append(event_display)
                timings.append(
                    {
                        "from": prev_event_name,
                        "to": event_display,
                        "duration_seconds": duration_seconds,
                        "label": time_label,
                        "start_time": prev_ts.strftime("%H:%M:%S") if prev_ts else "",
                        "end_time": ts.strftime("%H:%M:%S") if ts else "",
                        "start_datetime": prev_ts.strftime("%Y-%m-%d %H:%M:%S")
                        if prev_ts
                        else "",
                        "end_datetime": ts.strftime("%Y-%m-%d %H:%M:%S")
                        if ts
                        else "",
                    }
                )
                prev_ts = ts
                prev_event_name = event_display

            # Add End node
            path_sequence.append("End")
            last_ts = prev_ts
            timings.append(
                {
                    "from": prev_event_name,
                    "to": "End",
                    "duration_seconds": 0,
                    "label": "End",
                    "start_time": last_ts.strftime("%H:%M:%S") if last_ts else "",
                    "end_time": last_ts.strftime("%H:%M:%S") if last_ts else "",
                    "start_datetime": last_ts.strftime("%Y-%m-%d %H:%M:%S")
                    if last_ts
                    else "",
                    "end_datetime": last_ts.strftime("%Y-%m-%d %H:%M:%S")
                    if last_ts
                    else "",
                }
            )

            case_paths.append(
                {
                    "case_id": case.get("case_id"),
                    "user_id": case.get("user_id"),
                    "color": case_color,
                    "path_sequence": path_sequence,
                    "timings": timings,
                    "total_duration": sum(t["duration_seconds"] for t in timings),
                }
            )

        # Event types for legend (canonical order)
        all_event_types = ["Process"] + [
            s for s in self.step_order if any(s in p["path_sequence"] for p in case_paths)
        ] + ["End"]

        return {
            "all_event_types": all_event_types,
            "case_paths": case_paths,
            "total_cases": len(case_paths),
        }

    # ------------------------------------------------------------------
    # Public entrypoint
    # ------------------------------------------------------------------

    def analyze_cluster(
        self,
        tables: List[TableAnalysis],
        dataframes: Dict[str, pd.DataFrame],
        relationships: List[Any],
    ) -> Dict[str, Any]:
        """
        Main entrypoint for Retail Process Timeline:
        - Collects row-level events from all tables.
        - Builds Case IDs per customer (each order run).
        - Returns diagram-ready and explanation-friendly structures.
        """
        all_events: List[Dict[str, Any]] = []

        for table in tables:
            df = dataframes.get(table.table_name)
            if df is None or df.empty:
                continue
            events = self._table_to_events(table, df)
            all_events.extend(events)

        if not all_events:
            return {
                "success": False,
                "error": "No retail events with usable timestamps found.",
                "tables_checked": [t.table_name for t in tables],
            }

        # Sort all events and strip internal timestamp objects later
        all_events.sort(key=lambda e: e["timestamp"])

        cases = self._split_cases(all_events)
        case_details = self._assign_case_ids(cases)
        unified_flow_data = self._generate_unified_flow_data(case_details)

        case_ids_asc = [c["case_id"] for c in case_details]
        users_with_cases = list(dict.fromkeys(c["user_id"] for c in case_details))

        explanations = [
            f"We found {len(case_details)} case(s). Each case represents one retail process run (typically one order journey) for a customer.",
            "Case IDs are numbered in order of the first event time across all customers.",
            "If the same customer starts a new order again, we create a NEW Case ID (patterns are never merged).",
            "Each case lists the full sequence of steps: User Signed Up, Login, Product Viewed, Added to Cart, Order Created, Payment, Delivery, Returns and Refunds.",
        ]

        # Remove raw Timestamp objects to keep JSON output safe
        sanitized_events: List[Dict[str, Any]] = []
        for ev in all_events:
            ev_copy = {k: v for k, v in ev.items() if k != "timestamp"}
            sanitized_events.append(ev_copy)

        first_ts = all_events[0]["timestamp"]
        last_ts = all_events[-1]["timestamp"]
        first_datetime = (
            first_ts.strftime("%Y-%m-%d %H:%M:%S") if hasattr(first_ts, "strftime") else ""
        )
        last_datetime = (
            last_ts.strftime("%Y-%m-%d %H:%M:%S") if hasattr(last_ts, "strftime") else ""
        )

        total_events = len(sanitized_events)

        return {
            "success": True,
            "sorted_timeline": sanitized_events,
            "first_datetime": first_datetime,
            "last_datetime": last_datetime,
            "total_events": total_events,
            "total_activities": total_events,
            "case_ids": case_ids_asc,
            "case_details": case_details,
            "total_cases": len(case_details),
            "total_customers": len(users_with_cases),
            "customers": users_with_cases,
            "explanations": explanations,
            "unified_flow_data": unified_flow_data,
        }

