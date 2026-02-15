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

# Valid retail events - derived from column names (user file observed). No Other/Unknown.
RETAIL_EVENT_COLUMN_PATTERNS = [
    "customer_visit", "product_view", "product_search", "add_to_cart", "remove_from_cart",
    "apply_coupon", "checkout_started", "address_entered", "payment_selected",
    "payment_success", "payment_failed", "order_placed", "order_confirmed",
    "invoice_generated", "order_packed", "order_shipped", "out_for_delivery",
    "order_delivered", "order_cancelled", "return_initiated", "return_received",
    "refund_processed", "signup", "login", "logout", "created_at", "event_time",
]


class RetailTimelineAnalyzer:
    """
    Analyzes retail / e‑commerce tables and builds customer order journeys
    as Case IDs. Logic is pattern-based (table / column names, simple status
    values) – no hardcoded schema or SQL.
    """

    def __init__(self) -> None:
        # Canonical retail steps - matches column-observed events (no hardcode)
        self.step_order = [
            "Customer Visit", "Product View", "Product Search", "Add To Cart",
            "Remove From Cart", "Apply Coupon", "Checkout Started", "Address Entered",
            "Payment Selected", "Payment Success", "Payment Failed", "Order Placed",
            "Order Confirmed", "Invoice Generated", "Order Packed", "Order Shipped",
            "Out For Delivery", "Order Delivered", "Order Cancelled", "Return Initiated",
            "Return Received", "Refund Processed",
            "User Signed Up", "User Logged In", "User Logged Out",
        ]

    def _time_column_to_retail_event(self, col_name: str) -> str:
        """
        Derive event name from timestamp column name. NO hardcode - user file observed.
        Valid events only. Never return Other/Unknown.
        """
        if not col_name or not str(col_name).strip():
            return "Order Placed"
        c = str(col_name).lower().replace("-", "_").replace(" ", "_")
        # Strip common suffixes
        for suf in ["_time", "_date", "_timestamp", "_at", "_datetime"]:
            if c.endswith(suf):
                c = c[: -len(suf)]
                break
        # Explicit mappings (column -> event)
        m = {
            "customer_visit": "Customer Visit",
            "product_view": "Product View",
            "product_search": "Product Search",
            "add_to_cart": "Add To Cart",
            "remove_from_cart": "Remove From Cart",
            "apply_coupon": "Apply Coupon",
            "checkout_started": "Checkout Started",
            "address_entered": "Address Entered",
            "payment_selected": "Payment Selected",
            "payment_success": "Payment Success",
            "payment_failed": "Payment Failed",
            "order_placed": "Order Placed",
            "order_confirmed": "Order Confirmed",
            "invoice_generated": "Invoice Generated",
            "order_packed": "Order Packed",
            "order_shipped": "Order Shipped",
            "out_for_delivery": "Out For Delivery",
            "order_delivered": "Order Delivered",
            "order_cancelled": "Order Cancelled",
            "return_initiated": "Return Initiated",
            "return_received": "Return Received",
            "refund_processed": "Refund Processed",
            "signup": "User Signed Up", "sign_up": "User Signed Up", "registration": "User Signed Up",
            "login": "User Logged In", "signin": "User Logged In",
            "logout": "User Logged Out", "signout": "User Logged Out",
        }
        if c in m:
            return m[c]
        # Partial match
        for key, ev in m.items():
            if key in c:
                return ev
        # Infer from tokens
        if "visit" in c or "browse" in c:
            return "Customer Visit"
        if "view" in c or "product" in c:
            return "Product View"
        if "cart" in c:
            return "Add To Cart" if "remove" not in c else "Remove From Cart"
        if "checkout" in c:
            return "Checkout Started"
        if "payment" in c:
            return "Payment Success" if "fail" not in c else "Payment Failed"
        if "order" in c:
            if "cancel" in c:
                return "Order Cancelled"
            if "pack" in c:
                return "Order Packed"
            if "ship" in c:
                return "Order Shipped"
            if "deliver" in c:
                return "Order Delivered"
            return "Order Placed"
        if "return" in c:
            return "Return Initiated" if "receiv" in c else "Return Initiated"
        if "refund" in c:
            return "Refund Processed"
        if "invoice" in c:
            return "Invoice Generated"
        return "Order Placed"

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

    def _find_event_name_col(self, df: pd.DataFrame) -> Optional[str]:
        """
        Detect column that contains event type as DATA (not column name).
        e.g. event_name with values: Customer_Visit, Product_View, Add_To_Cart.
        Uses data pattern: values look like event names (underscores, known tokens).
        """
        candidates = ["event_name", "event_type", "action", "event", "step_name", "activity", "event_type_name"]
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
                    any(tok in v.lower() for tok in ["visit", "view", "cart", "order", "payment", "checkout", "deliver", "return", "refund", "add", "remove"])
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
        """Detect case_id column for grouping (user file observed)."""
        cols_lower = {c.lower(): c for c in df.columns}
        for cand in ["case_id", "caseid", "session_id", "sessionid", "journey_id"]:
            if cand in cols_lower:
                return cols_lower[cand]
        return None

    def _normalize_event_from_data(self, val: Any) -> str:
        """
        Convert event value from data to canonical format.
        Examples: Customer_Visit -> Customer Visit, Add_To_Cart -> Add To Cart, 
                  "order placed" -> Order Placed, "payment_success" -> Payment Success
        """
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return "Order Placed"
        s = str(val).strip()
        if not s:
            return "Order Placed"
        
        # Normalize format: replace underscores/dashes with spaces, title case
        normalized = s.replace("_", " ").replace("-", " ").strip()
        
        # Map common variations to canonical names (case-insensitive)
        normalized_lower = normalized.lower()
        canonical_map = {
            # User events
            "user signed up": "User Signed Up",
            "signup": "User Signed Up",
            "sign up": "User Signed Up",
            "registration": "User Signed Up",
            "register": "User Signed Up",
            "user logged in": "User Logged In",
            "login": "User Logged In",
            "log in": "User Logged In",
            "signin": "User Logged In",
            "user logged out": "User Logged Out",
            "logout": "User Logged Out",
            "log out": "User Logged Out",
            "signout": "User Logged Out",
            # Customer actions
            "customer visit": "Customer Visit",
            "visit": "Customer Visit",
            "product view": "Product View",
            "product viewed": "Product View",
            "view": "Product View",
            "product search": "Product Search",
            "search": "Product Search",
            "add to cart": "Add To Cart",
            "added to cart": "Add To Cart",
            "add cart": "Add To Cart",
            "remove from cart": "Remove From Cart",
            "removed from cart": "Remove From Cart",
            "remove cart": "Remove From Cart",
            "apply coupon": "Apply Coupon",
            "coupon": "Apply Coupon",
            # Checkout & Payment
            "checkout started": "Checkout Started",
            "checkout": "Checkout Started",
            "address entered": "Address Entered",
            "address": "Address Entered",
            "payment selected": "Payment Selected",
            "payment initiated": "Payment Selected",
            "payment success": "Payment Success",
            "payment completed": "Payment Success",
            "payment successful": "Payment Success",
            "paid": "Payment Success",
            "payment failed": "Payment Failed",
            "payment fail": "Payment Failed",
            # Order events
            "order placed": "Order Placed",
            "order created": "Order Placed",
            "order": "Order Placed",
            "order confirmed": "Order Confirmed",
            "confirmed": "Order Confirmed",
            "invoice generated": "Invoice Generated",
            "invoice": "Invoice Generated",
            # Fulfillment
            "order packed": "Order Packed",
            "packed": "Order Packed",
            "order shipped": "Order Shipped",
            "shipped": "Order Shipped",
            "out for delivery": "Out For Delivery",
            "order delivered": "Order Delivered",
            "delivered": "Order Delivered",
            "order cancelled": "Order Cancelled",
            "cancelled": "Order Cancelled",
            "canceled": "Order Cancelled",
            # Returns & Refunds
            "return initiated": "Return Initiated",
            "return requested": "Return Initiated",
            "return received": "Return Received",
            "returned": "Return Received",
            "refund processed": "Refund Processed",
            "refund": "Refund Processed",
            "refunded": "Refund Processed",
        }
        
        # Check for exact match first
        if normalized_lower in canonical_map:
            return canonical_map[normalized_lower]
        
        # Check for partial matches
        for key, canonical in canonical_map.items():
            if key in normalized_lower or normalized_lower in key:
                return canonical
        
        # If no match, format as title case
        return normalized.title()

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

        # 1) Prefer columns matching retail event patterns (user file observed)
        for col in df.columns:
            cl = col.lower().replace("-", "_").replace(" ", "_")
            if any(pat in cl for pat in RETAIL_EVENT_COLUMN_PATTERNS) and is_parseable(col):
                return (col, None)
        # 2) Explicit timestamp-like columns
        preferred_keywords = [
            "event_time", "event_timestamp", "created_at", "created_time", "created_timestamp",
            "order_timestamp", "order_time", "order_date", "order_placed", "order_confirmed",
            "payment_time", "payment_timestamp", "shipment_time", "delivery_time",
            "customer_visit", "product_view", "checkout", "invoice", "refund", "return",
            "updated_at", "timestamp",
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

    def _normalize_legacy_event(self, name: str) -> str:
        """Map legacy event names to canonical (column-observed) format."""
        m = {
            "User Signed Up": "User Signed Up",
            "User Logged In": "User Logged In",
            "User Logged Out": "User Logged Out",
            "Product Viewed": "Product View",
            "Added to Cart": "Add To Cart",
            "Removed from Cart": "Remove From Cart",
            "Order Created": "Order Placed",
            "Payment Initiated": "Payment Selected",
            "Payment Completed": "Payment Success",
            "Payment Failed": "Payment Failed",
            "Invoice Generated": "Invoice Generated",
            "Order Packed": "Order Packed",
            "Order Shipped": "Order Shipped",
            "Out for Delivery": "Out For Delivery",
            "Order Delivered": "Order Delivered",
            "Return Requested": "Return Initiated",
            "Product Returned": "Return Received",
            "Refund Initiated": "Refund Processed",
            "Refund Completed": "Refund Processed",
        }
        return m.get(name, name) if name else "Order Placed"

    def _get_status_value(self, row: pd.Series, status_col: Optional[str]) -> str:
        if status_col and status_col in row.index:
            val = row[status_col]
            if pd.isna(val):
                return ""
            return str(val).strip().lower()
        return ""

    def _scan_row_for_event_pattern(self, row: Any, columns: List[str]) -> Optional[str]:
        """
        Scan ALL column values in row for retail event pattern match.
        Observes actual data values, not just column names.
        Returns canonical event name if pattern found, None otherwise.
        """
        # Retail event patterns to look for in data values
        event_patterns = {
            'User Signed Up': ['signup', 'sign up', 'sign-up', 'registered', 'registration', 'register', 'new customer', 'account created', 'user created'],
            'User Logged In': ['login', 'log in', 'log-in', 'signin', 'sign in', 'sign-in', 'logged in', 'authenticated'],
            'User Logged Out': ['logout', 'log out', 'log-out', 'signout', 'sign out', 'sign-out', 'logged out'],
            'Customer Visit': ['visit', 'visited', 'browse', 'browsed', 'customer visit', 'store visit', 'website visit'],
            'Product View': ['view', 'viewed', 'product view', 'viewed product', 'see product', 'saw product'],
            'Product Search': ['search', 'searched', 'product search', 'search product', 'find product', 'looking for'],
            'Add To Cart': ['add to cart', 'added to cart', 'add cart', 'cart add', 'add item', 'added item', 'add product'],
            'Remove From Cart': ['remove from cart', 'removed from cart', 'remove cart', 'cart remove', 'remove item', 'removed item', 'delete from cart'],
            'Checkout Started': ['checkout', 'checkout started', 'started checkout', 'begin checkout', 'checkout begin'],
            'Address Entered': ['address', 'address entered', 'delivery address', 'shipping address', 'address added'],
            'Payment Selected': ['payment selected', 'payment method', 'select payment', 'payment choose', 'payment option'],
            'Payment Success': ['payment success', 'payment successful', 'paid', 'payment completed', 'payment succeed', 'payment success', 'transaction success'],
            'Payment Failed': ['payment failed', 'payment fail', 'payment error', 'transaction failed', 'payment declined', 'payment reject'],
            'Order Placed': ['order placed', 'order created', 'order', 'placed order', 'new order', 'order new', 'order submit'],
            'Order Confirmed': ['order confirmed', 'confirmed', 'order confirmation', 'confirm order'],
            'Invoice Generated': ['invoice', 'invoice generated', 'invoice created', 'bill generated', 'bill created'],
            'Order Packed': ['packed', 'order packed', 'packing', 'pack order', 'order packing'],
            'Order Shipped': ['shipped', 'order shipped', 'shipping', 'ship order', 'dispatch', 'dispatched'],
            'Out For Delivery': ['out for delivery', 'out for deliver', 'on the way', 'on way', 'delivery started', 'courier'],
            'Order Delivered': ['delivered', 'order delivered', 'delivery', 'delivery completed', 'received', 'order received'],
            'Order Cancelled': ['cancelled', 'canceled', 'order cancelled', 'order canceled', 'cancel order', 'order cancel'],
            'Return Initiated': ['return initiated', 'return request', 'return requested', 'initiate return', 'request return'],
            'Return Received': ['return received', 'returned', 'return complete', 'return finished', 'product returned'],
            'Refund Processed': ['refund', 'refund processed', 'refunded', 'refund complete', 'refund issued', 'refund processed'],
            'Apply Coupon': ['coupon', 'apply coupon', 'coupon applied', 'discount', 'discount applied', 'promo code', 'promotion'],
        }
        
        # Scan all column values for event patterns
        for col in columns:
            if col.startswith("__"):
                continue
            val = row.get(col)
            if val is None or (isinstance(val, float) and pd.isna(val)):
                continue
            
            val_str = str(val).lower().strip()
            if not val_str:
                continue
            
            # Check each event pattern
            for event_name, patterns in event_patterns.items():
                for pattern in patterns:
                    if pattern in val_str:
                        return event_name
        
        return None

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
        # English explanations per event (column-observed, user file)
        mapping = {
            "Customer Visit": "Customer visited the store or site",
            "Product View": "Customer viewed a product",
            "Product Search": "Customer searched for products",
            "Add To Cart": "Customer added product to cart",
            "Remove From Cart": "Customer removed product from cart",
            "Apply Coupon": "Customer applied a coupon or discount",
            "Checkout Started": "Customer started checkout",
            "Address Entered": "Customer entered delivery address",
            "Payment Selected": "Customer selected payment method",
            "Payment Success": "Payment succeeded",
            "Payment Failed": "Payment failed",
            "Order Placed": "Customer placed order",
            "Order Confirmed": "Order was confirmed",
            "Invoice Generated": "Invoice was generated for the order",
            "Order Packed": "Shop packed the product",
            "Order Shipped": "Order handed over to courier",
            "Out For Delivery": "Courier on the way to customer",
            "Order Delivered": "Product reached customer",
            "Order Cancelled": "Order was cancelled",
            "Return Initiated": "Customer requested a return",
            "Return Received": "Returned product was received",
            "Refund Processed": "Refund was processed",
            "User Signed Up": "Customer created account",
            "User Logged In": "Customer logged in",
            "User Logged Out": "Customer signed out",
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
        event_time_col = time_col if time_col else date_col
        df = df.copy()

        # Build unified datetime column (supports date, time, AM/PM, seconds, years)
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
        event_name_col = self._find_event_name_col(df)
        case_id_col = self._find_case_id_col(df)

        events: List[Dict[str, Any]] = []
        file_name = getattr(table, "file_name", "") or f"{table.table_name}.csv"

        for idx, row in df.iterrows():
            ts = row["__dt"]
            if pd.isna(ts):
                continue
            # 1) FIRST: Scan row data values for event patterns (actual data, not column names)
            #    This catches events that might be in status columns, description fields, etc.
            scanned_event = self._scan_row_for_event_pattern(row, list(df.columns))
            if scanned_event:
                event_name = scanned_event  # Already in canonical format
            # 2) SECOND: Check event_name column if available (data value)
            elif event_name_col and event_name_col in row.index and pd.notna(row[event_name_col]):
                event_name = self._normalize_event_from_data(row[event_name_col])
                # Normalize to canonical if needed
                if event_name and event_name not in self.step_order:
                    event_name = self._normalize_legacy_event(event_name)
            # 3) THIRD: Derive from time column name
            else:
                event_name = self._time_column_to_retail_event(event_time_col)
                if not event_name or event_name in ("Other", "Unknown"):
                    event_name = self._infer_event_for_row(table.table_name, df, row, status_col)
                    event_name = self._normalize_legacy_event(event_name)
            
            # Ensure event_name is never None or empty and is in canonical format - critical for Sankey diagram
            if not event_name or not str(event_name).strip() or event_name in ("Other", "Unknown", "None"):
                # Final fallback: use table name or generic retail event
                event_name = self._infer_event_for_row(table.table_name, df, row, status_col)
                event_name = self._normalize_legacy_event(event_name)
                if not event_name or not str(event_name).strip():
                    event_name = "Order Placed"  # Safe default for retail
            
            # Final validation: ensure event is in canonical step_order list
            # This ensures consistency across all events for Sankey diagram
            if event_name not in self.step_order:
                # Try to find closest match or use default
                event_name_lower = event_name.lower()
                for canonical_event in self.step_order:
                    if canonical_event.lower() == event_name_lower or canonical_event.lower() in event_name_lower:
                        event_name = canonical_event
                        break
                else:
                    # If still no match, keep the event_name as-is (it might be a valid user-specific event)
                    pass
            user_id = None
            if user_col and user_col in row.index and pd.notna(row[user_col]):
                user_id = str(row[user_col]).strip()
            case_id_val = None
            if case_id_col and case_id_col in row.index and pd.notna(row[case_id_col]):
                case_id_val = str(row[case_id_col]).strip()
            order_id = None
            if order_col and order_col in row.index and pd.notna(row[order_col]):
                order_id = str(row[order_col]).strip()
            if not user_id and case_id_val:
                user_id = case_id_val

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
                    "user_id": user_id or case_id_val or "unknown",
                    "_case_id": case_id_val,
                    "order_id": order_id or "",
                    "event": event_name,
                    "_event_time_column": event_time_col,
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

        order_start_events = {"Order Created", "Order Placed"}
        for ev in events:
            name = ev.get("event", "")
            events_in_current = {e.get("event", "") for e in current}
            if name in events_in_current:
                # Same activity meaning again → new Case ID (one clean process flow per case).
                if current:
                    cases.append(current)
                    current = []
                    has_order_in_current = False
                current.append(ev)
                has_order_in_current = name in order_start_events
                continue
            if name in order_start_events:
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
        """
        Group events into cases.
        When case_id is present in data: group by case_id (each case_id = one case).
        Otherwise: group by user_id, split by Order Placed.
        """
        has_case_id = any(ev.get("_case_id") for ev in all_events)
        if has_case_id:
            by_case: Dict[str, List[Dict[str, Any]]] = {}
            for ev in all_events:
                cid = ev.get("_case_id") or ev.get("user_id") or "unknown"
                by_case.setdefault(cid, []).append(ev)
            all_cases = []
            for cid, events in by_case.items():
                events_sorted = sorted(events, key=lambda x: x.get("timestamp") or pd.Timestamp.min)
                all_cases.append(events_sorted)
        else:
            by_user: Dict[str, List[Dict[str, Any]]] = {}
            for ev in all_events:
                uid = ev.get("user_id") or "unknown"
                by_user.setdefault(uid, []).append(ev)
            all_cases = []
            for uid, events in by_user.items():
                events_sorted = sorted(events, key=lambda x: x.get("timestamp") or pd.Timestamp.min)
                user_cases = self._identify_cases_for_user(events_sorted)
                all_cases.extend(user_cases)

        all_cases.sort(key=lambda c: c[0]["timestamp"] if c and c[0].get("timestamp") is not None else pd.Timestamp.min)
        return all_cases

    # ------------------------------------------------------------------
    # Case details + explanations
    # ------------------------------------------------------------------

    def _event_phrase(self, step: str) -> str:
        """Convert canonical step label into short phrase for case explanation."""
        mapping = {
            "Customer Visit": "customer visit",
            "Product View": "product view",
            "Product Search": "product search",
            "Add To Cart": "added to cart",
            "Remove From Cart": "removed from cart",
            "Apply Coupon": "applied coupon",
            "Checkout Started": "checkout started",
            "Address Entered": "address entered",
            "Payment Selected": "payment selected",
            "Payment Success": "payment success",
            "Payment Failed": "payment failed",
            "Order Placed": "order placed",
            "Order Confirmed": "order confirmed",
            "Invoice Generated": "invoice generated",
            "Order Packed": "order packed",
            "Order Shipped": "order shipped",
            "Out For Delivery": "out for delivery",
            "Order Delivered": "order delivered",
            "Order Cancelled": "order cancelled",
            "Return Initiated": "return initiated",
            "Return Received": "return received",
            "Refund Processed": "refund processed",
            "User Signed Up": "signed up",
            "User Logged In": "logged in",
            "User Logged Out": "logged out",
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
                # Ensure event is never None or empty - critical for Sankey diagram
                event_val = ev.get("event")
                if not event_val or not str(event_val).strip() or str(event_val).strip().lower() in ("none", "null", "unknown", "other"):
                    # Try to infer from table name or use safe default
                    event_val = "Order Placed"  # Safe default for retail
                
                activities.append(
                    {
                        "event": str(event_val).strip() if event_val else "Order Placed",
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
            # Build event_sequence, filtering out None/empty events - critical for Sankey diagram
            event_sequence = []
            for a in activities:
                event_val = a.get("event")
                if event_val and str(event_val).strip() and str(event_val).strip().lower() not in ("none", "null", "unknown", "other"):
                    event_sequence.append(str(event_val).strip())
            # If no valid events found, try to extract from raw events
            if not event_sequence:
                for ev in events:
                    event_val = ev.get("event")
                    if event_val and str(event_val).strip() and str(event_val).strip().lower() not in ("none", "null", "unknown", "other"):
                        event_sequence.append(str(event_val).strip())
            # Final fallback: if still no events, create generic sequence
            if not event_sequence:
                event_sequence = ["Order Placed"]  # At least one event for Sankey
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

        # Event types from actual case paths (observed from user files), preserve order
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
        for path in case_paths:
            seq = path.get("path_sequence") or []
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

        observed_events = list(dict.fromkeys(
            a.get("event", "") for c in case_details for a in c.get("activities", [])
        ))
        explanations = [
            f"We found {len(case_details)} case(s). Each case represents one retail process run (typically one order journey) for a customer.",
            "Case IDs are numbered in order of the first event time across all customers.",
            "Events are grouped by customer and sorted by timestamp. Same activity meaning again (duplicate or different source) starts a new Case ID so each case is one clean process flow.",
            "If the same customer starts a new order again, we create a NEW Case ID.",
            f"Event types derived from your uploaded columns: {', '.join(observed_events) or '—'}.",
        ]

        # Remove raw Timestamp and internal fields to keep JSON output safe
        sanitized_events: List[Dict[str, Any]] = []
        skip_keys = {"timestamp", "_case_id", "_event_time_column"}
        for ev in all_events:
            ev_copy = {k: v for k, v in ev.items() if k not in skip_keys}
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

