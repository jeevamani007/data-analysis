"""
Banking Case ID Analyzer
Processes uploaded banking activity files and generates Case IDs per user.
Sessions: login to logout. Case IDs are ascending by first activity time. Activities in each case are time-sorted.
Column detection is pattern-based (login_time, logout_time, created_time, open_time, etc.). No hardcoded column names.
Timestamps support AM/PM and multiple formats. Explanations are in simple English.
"""

from typing import Dict, List, Optional, Tuple, Any
import re
import pandas as pd
from models import TableAnalysis

SESSION_START = {'login', 'session_start'}
SESSION_END = {'logout', 'session_end'}
PRE_LOGIN_EVENTS = {'open', 'created'}
MIDDLE_EVENTS = {
    'credit', 'deposit', 'withdraw', 'debit', 'refund',
    'invalid_balance', 'negative_balance', 'invalid', 'negative',
    'check_balance', 'balance_check', 'balance_inquiry', 'inquiry'
}

EVENT_MAP = {
    'login': 'login', 'log in': 'login', 'session_start': 'login', 'open': 'login',
    'logout': 'logout', 'log out': 'logout', 'session_end': 'logout',
    'credit': 'credit', 'deposit': 'deposit', 'withdraw': 'withdraw', 'withdrawal': 'withdraw',
    'debit': 'debit', 'refund': 'refund',
    'invalid_balance': 'invalid_balance', 'invalid': 'invalid_balance',
    'negative_balance': 'negative_balance', 'negative': 'negative_balance',
    'declined': 'invalid_balance', 'blocked': 'invalid_balance',
    'check_balance': 'check_balance', 'balance_check': 'check_balance',
    'balance_inquiry': 'check_balance', 'inquiry': 'check_balance',
}

# Time formats to try (with and without AM/PM)
_DATETIME_FORMATS = [
    '%Y-%m-%d %H:%M:%S',
    '%Y-%m-%d %H:%M',
    '%Y-%m-%d %I:%M:%S %p',
    '%Y-%m-%d %I:%M %p',
    '%m/%d/%Y %H:%M:%S',
    '%m/%d/%Y %H:%M',
    '%m/%d/%Y %I:%M %p',
    '%d-%m-%Y %H:%M:%S',
    '%d-%m-%Y %H:%M',
    '%d/%m/%Y %I:%M %p',
    '%Y-%m-%d',
    '%m/%d/%Y',
]


def _normalize_event(val: Any) -> Optional[str]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip().lower()
    if not s:
        return None

    # 1) Direct exact match first (keeps existing behavior for known values)
    if s in EVENT_MAP:
        return EVENT_MAP[s]

    # 2) Normalize separators (dash/underscore) and try again
    s_norm = re.sub(r'[\s\-_]+', ' ', s).strip()
    if s_norm in EVENT_MAP:
        return EVENT_MAP[s_norm]

    # 3) Pattern-based detection so we can handle short forms and
    #    descriptive phrases from real banking data (e.g. "Login Success",
    #    "CR", "DR", "Balance Inquiry", "WDL ATM", etc.).
    text = re.sub(r'[^a-z0-9]+', ' ', s_norm).strip()
    if not text:
        return None
    tokens = text.split()
    token_set = set(tokens)

    def has_token(*candidates: str) -> bool:
        return any(c in token_set for c in candidates)

    def has_phrase(*phrases: str) -> bool:
        return any(p in text for p in phrases)

    # --- Session boundaries: login / logout ---
    if has_phrase('login', 'log in', 'sign in', 'signin', 'logon'):
        return 'login'
    if has_phrase('logout', 'log out', 'sign out', 'signoff', 'sign off'):
        return 'logout'

    # --- Account creation / open ---
    if has_phrase('account open', 'acct open', 'account opening', 'account opened'):
        return 'open'
    if has_phrase('created', 'creation', 'account creation'):
        return 'created'

    # --- Credit / deposit (money coming in) ---
    if has_token('cr', 'crd', 'credit') or (has_phrase('credit') and not has_phrase('card')):
        return 'credit'
    if has_token('dep', 'deposit', 'cashdep', 'cashdeposit') or has_phrase('cash deposit', 'salary credit'):
        return 'deposit'

    # --- Debit / withdrawal (money going out) ---
    if has_token('dr', 'db', 'debit'):
        return 'debit'
    if has_token('wd', 'wdl', 'withd', 'withdraw', 'withdrawal') or has_phrase('atm withdraw', 'atm withdrawal'):
        return 'withdraw'

    # --- Refunds / reversals ---
    if has_phrase('refund', 'reversal', 'chargeback', 'cashback'):
        return 'refund'

    # --- Balance / inquiry / failed / invalid ---
    if has_phrase(
        'check balance', 'balance inquiry', 'balance check', 'bal inq', 'mini statement', 'balance enquiry'
    ) or has_token('bal'):
        return 'check_balance'
    if has_phrase('declined', 'blocked', 'invalid', 'failed', 'failure', 'error'):
        return 'invalid_balance'
    if has_phrase('negative balance', 'overdrawn', 'overdraft'):
        return 'negative_balance'

    # 4) If already a canonical internal event name, keep it
    if s in SESSION_START | SESSION_END | MIDDLE_EVENTS:
        return s

    # 5) Otherwise we don't confidently know this event
    return None


def _parse_datetime_cell(val: Any) -> Optional[pd.Timestamp]:
    """Parse one value to datetime. Tries multiple formats including AM/PM. No hardcoded column names."""
    if val is None or (isinstance(val, float) and pd.isna(val)) or str(val).strip() == '':
        return None
    s = str(val).strip()
    # pandas first - normalize timezone-aware values to UTC to keep ordering correct
    try:
        dt = pd.to_datetime(s, errors='coerce', utc=True)
    except TypeError:
        # Fallback for very old pandas versions
        dt = pd.to_datetime(s, errors='coerce')
    if pd.notna(dt):
        # If this is tz-aware, convert to UTC then drop tz info so all comparisons are consistent
        try:
            if getattr(dt, 'tzinfo', None) is not None:
                dt = dt.tz_convert('UTC').tz_localize(None)
        except Exception:
            try:
                dt = dt.tz_localize(None)
            except Exception:
                pass
        return dt
    for fmt in _DATETIME_FORMATS:
        try:
            dt = pd.to_datetime(s, format=fmt, errors='coerce', utc=True)
            if pd.notna(dt):
                try:
                    if getattr(dt, 'tzinfo', None) is not None:
                        dt = dt.tz_convert('UTC').tz_localize(None)
                except Exception:
                    try:
                        dt = dt.tz_localize(None)
                    except Exception:
                        pass
                return dt
        except Exception:
            continue
    return None


def _parse_timestamp(row, date_col: str, time_col: Optional[str], df: pd.DataFrame) -> Optional[pd.Timestamp]:
    """Parse datetime from row. Uses date_col and optional time_col. Handles AM/PM via _parse_datetime_cell."""
    try:
        date_val = row.get(date_col, row[date_col]) if date_col in row.index else None
        if pd.isna(date_val) or str(date_val).strip() == '':
            return None
        if time_col and time_col in row.index:
            t_val = row[time_col]
            if pd.notna(t_val) and str(t_val).strip():
                date_part = str(date_val).split()[0] if hasattr(date_val, 'split') else str(pd.to_datetime(date_val, errors='coerce').date())
                combined = f"{date_part} {str(t_val).strip()}"
                return _parse_datetime_cell(combined)
        return _parse_datetime_cell(date_val)
    except Exception:
        return None


def _timestamp_to_display(ts: pd.Timestamp) -> str:
    """Format timestamp for user-facing text (12-hour AM/PM, simple English)."""
    try:
        return ts.strftime('%b %d, %Y %I:%M %p').replace(' 0', ' ').lstrip('0')
    except Exception:
        return str(ts)


class BankingAnalyzer:
    """
    Process banking activity files and generate Case IDs per user.
    Sessions: login → (credit|deposit|withdraw|debit|refund|invalid_balance|negative_balance)* → logout.
    """

    def _find_user_col(self, df: pd.DataFrame) -> Optional[str]:
        """Find user/customer ID column."""
        candidates = ['user_id', 'customer_id', 'user', 'customer', 'cust_id']
        cols_lower = {c.lower(): c for c in df.columns}
        for c in candidates:
            if c in cols_lower:
                return cols_lower[c]
        for col in df.columns:
            if 'user' in col.lower() or 'customer' in col.lower():
                if 'id' in col.lower() or col.lower() in ('user', 'customer'):
                    return col
        return None

    def _find_account_col(self, df: pd.DataFrame) -> Optional[str]:
        """Find account ID column."""
        candidates = ['account_id', 'account_number', 'account']
        cols_lower = {c.lower(): c for c in df.columns}
        for c in candidates:
            if c in cols_lower:
                return cols_lower[c]
        for col in df.columns:
            if 'account' in col.lower():
                return col
        return None

    def _find_event_col(self, df: pd.DataFrame) -> Optional[str]:
        """Find event or transaction type column."""
        candidates = ['event', 'transaction_type', 'txn_type', 'type', 'activity']
        cols_lower = {c.lower(): c for c in df.columns}
        for c in candidates:
            if c in cols_lower:
                return cols_lower[c]
        for col in df.columns:
            cl = col.lower()
            if 'event' in cl or 'transaction_type' in cl or ('type' in cl and 'txn' in cl):
                return col
        return None

    def _find_amount_col(self, df: pd.DataFrame) -> Optional[str]:
        """Find transaction amount column (credit/debit value)."""
        candidates = ['amount', 'transaction_amount', 'txn_amount', 'amt', 'value']
        cols_lower = {str(c).lower(): c for c in df.columns}
        for c in candidates:
            if c in cols_lower:
                return cols_lower[c]
        for col in df.columns:
            cl = str(col).lower()
            if 'amount' in cl or re.search(r'\bamt\b', cl):
                return col
        return None

    def _infer_time_purpose(self, col_name: str) -> Optional[str]:
        """Infer purpose from column name. login, logout, created, open are distinct. No hardcoding."""
        c = col_name.lower().replace('-', '_').replace(' ', '_')
        if re.search(r'\blogout\b', c) or 'session_end' in c:
            return 'logout'
        if re.search(r'\blogin\b', c):
            return 'login'
        # Account / profile created or joined
        if re.search(r'\bcreated\b', c) or 'created_at' in c:
            return 'created'
        if 'join' in c or 'joined' in c or 'signup' in c or 'sign_up' in c or 'register' in c or 'registered' in c:
            return 'created'
        if re.search(r'\bopen\b', c) and re.search(r'date|time|stamp', c):
            return 'open'
        if re.search(r'txn|transaction', c) and re.search(r'date|time|stamp', c):
            return 'transaction'
        if 'event' in c and re.search(r'date|time|stamp', c):
            return 'event'
        if re.search(r'date|time|stamp', c) and 'dob' not in c and 'birth' not in c:
            return 'event'
        if 'balance' in c and (re.search(r'check|inquiry', c) or c == 'balance'):
             return 'check_balance'
        return None

    def _find_date_time_pair(self, df: pd.DataFrame, base: str) -> Tuple[Optional[str], Optional[str]]:
        """Find (date_col, time_col) pair for base like open, created, transaction. Date+time or single timestamp."""
        cols_lower = {c.lower().replace('-', '_').replace(' ', '_'): c for c in df.columns}
        date_col = None
        time_col = None
        for key, col in cols_lower.items():
            if base not in key or 'dob' in key or 'birth' in key:
                continue
            if re.search(rf'{re.escape(base)}_(date|timestamp)\b', key) or key == f'{base}_date':
                date_col = col
            if re.search(rf'{re.escape(base)}_time\b', key) or key == f'{base}_time':
                time_col = col
        return (date_col, time_col)

    def _find_timestamp_cols(self, df: pd.DataFrame) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
        """Find (date_col, time_col, purpose, alt_logout_col). Distinguishes open, created, login, logout."""
        cols = list(df.columns)
        login_col = None
        logout_col = None
        for col in cols:
            pur = self._infer_time_purpose(col)
            if pur == 'login':
                sample = df[col].dropna().head(1)
                if not sample.empty and pd.notna(_parse_datetime_cell(sample.iloc[0])):
                    login_col = col
            elif pur == 'logout':
                sample = df[col].dropna().head(1)
                if not sample.empty and pd.notna(_parse_datetime_cell(sample.iloc[0])):
                    logout_col = col
        if login_col and logout_col:
            return (login_col, None, 'login', logout_col)
        if login_col:
            return (login_col, None, 'login', None)
        if logout_col:
            return (logout_col, None, 'logout', None)

        for base in ('open', 'created', 'txn', 'transaction', 'event'):
            date_col, time_col = self._find_date_time_pair(df, base)
            if date_col or time_col:
                pur = 'open' if base == 'open' else ('created' if base == 'created' else 'transaction')
                return (date_col or time_col, time_col if date_col else None, pur, None)

        best_combined = None
        best_purpose = None
        date_col = None
        time_col = None
        for col in cols:
            cl = col.lower().replace('-', '_')
            if 'dob' in cl or 'birth' in cl:
                continue
            pur = self._infer_time_purpose(col)
            sample = df[col].dropna().head(3)
            if sample.empty:
                if re.search(r'date|stamp', cl) and not re.search(r'\btime\b', cl):
                    date_col = date_col or col
                elif re.search(r'\btime\b', cl) or 'stamp' in cl:
                    time_col = time_col or col
                continue
            parsed_ok = all(pd.notna(_parse_datetime_cell(sample.iloc[i])) for i in range(len(sample)))
            if parsed_ok and pur:
                if best_combined is None or pur in ('login', 'logout', 'created', 'open'):
                    best_combined = col
                    best_purpose = pur
            else:
                if re.search(r'date|stamp', cl):
                    date_col = date_col or col
                if re.search(r'\btime\b', cl) and 'stamp' not in cl:
                    time_col = time_col or col
        if best_combined:
            return (best_combined, None, best_purpose or 'event', None)
        for col in cols:
            cl = col.lower()
            if 'dob' in cl or 'birth' in cl:
                continue
            if re.search(r'date|stamp', cl) and date_col is None:
                date_col = col
            if re.search(r'\btime\b', cl) and time_col is None and col != date_col:
                time_col = col
        if date_col:
            return (date_col, time_col, best_purpose or 'event', None)
        return (None, None, None, None)

    def _detect_event_columns_for_table(self, df: pd.DataFrame, table_name: str) -> Dict[str, List[str]]:
        """
        Detect event-related columns for a single table using only column names and simple patterns.
        This is used to build the Event Columns Blueprint for the UI.
        It does not read any row values.
        Enhanced to require banking context and exclude healthcare patterns.
        """
        events: Dict[str, List[str]] = {
            'account_open': [],
            'login': [],
            'logout': [],
            'deposit': [],
            'withdraw': [],
            'refund': [],
            'failed': [],
            'check_balance': [],
        }

        # Helper function to check if column has healthcare context
        def has_healthcare_context(col_name: str) -> bool:
            c = col_name.lower().replace('-', '_').replace(' ', '_')
            healthcare_keywords = [
                'patient', 'doctor', 'diagnosis', 'treatment', 'admission', 'discharge',
                'hospital', 'medical', 'prescription', 'lab', 'test', 'appointment',
                'ward', 'bed', 'clinic', 'blood', 'donor', 'medication', 'surgery'
            ]
            return any(kw in c for kw in healthcare_keywords)

        # Helper function to check if column has banking context
        def has_banking_context(col_name: str) -> bool:
            c = col_name.lower().replace('-', '_').replace(' ', '_')
            banking_keywords = [
                'account', 'transaction', 'banking', 'payment', 'transfer',
                'loan', 'card', 'branch', 'customer', 'user', 'session'
            ]
            return any(kw in c for kw in banking_keywords)

        for col in df.columns:
            name = str(col)
            c = name.lower().replace('-', '_').replace(' ', '_')

            # EXCLUDE healthcare columns immediately
            if has_healthcare_context(name):
                continue

            # Time-based purpose (login / logout / open / created / transaction)
            purpose = self._infer_time_purpose(name)
            if purpose == 'login':
                events['login'].append(name)
            elif purpose == 'logout':
                events['logout'].append(name)
            elif purpose in ('open', 'created'):
                events['account_open'].append(name)

            # Deposit / credit columns - require banking context OR explicit timestamp keywords
            if 'deposit' in c or ('credit' in c and 'card' not in c):
                # Only add if it has timestamp keywords OR banking context
                if any(k in c for k in ('date', 'time', 'timestamp', 'at', 'on')) or has_banking_context(name):
                    if name not in events['deposit']:
                        events['deposit'].append(name)

            # Withdraw / debit columns - require banking context OR explicit timestamp keywords
            if ('withdraw' in c or 'withdrawal' in c or 'debit' in c):
                if any(k in c for k in ('date', 'time', 'timestamp', 'at', 'on')) or has_banking_context(name):
                    if name not in events['withdraw']:
                        events['withdraw'].append(name)

            # Refund columns - require banking context OR explicit timestamp keywords
            if 'refund' in c:
                if any(k in c for k in ('date', 'time', 'timestamp', 'at', 'on')) or has_banking_context(name):
                    if name not in events['refund']:
                        events['refund'].append(name)

            # Balance columns - ONLY if banking context (avoid healthcare "patient balance" etc.)
            if 'balance' in c and has_banking_context(name):
                if name not in events['check_balance']:
                    events['check_balance'].append(name)
            
            # REMOVED: Generic status/result/invalid/negative matching
            # These are too broad and capture healthcare columns
            
            # Special handling for event/transaction_type columns
            # Only add if it has clear banking context
            if c in ('transaction_type', 'txn_type', 'banking_activity', 'activity_type'):
                # Add to all relevant event types since this column can indicate any event
                for event_type in ['login', 'logout', 'deposit', 'withdraw', 'refund']:
                    if name not in events[event_type]:
                        events[event_type].append(name)

        # Build mapping event_type -> [Table.Column] and remove duplicates
        result: Dict[str, List[str]] = {}
        for ev_type, cols in events.items():
            if not cols:
                continue
            full_names: List[str] = []
            for col_name in cols:
                full = f"{table_name}.{col_name}"
                if full not in full_names:
                    full_names.append(full)
            if full_names:
                result[ev_type] = full_names
        return result

    def _extract_activities(
        self,
        df: pd.DataFrame,
        table_name: str,
        file_name: str,
        account_to_user: Dict[str, str]
    ) -> Tuple[List[Dict[str, Any]], Dict[str, List[str]]]:
        """
        Extract activities and identifying the columns actually used for each event type.
        Returns (activities_list, used_columns_map).
        """
        user_col = self._find_user_col(df)
        account_col = self._find_account_col(df)
        event_col = self._find_event_col(df)
        amount_col = self._find_amount_col(df)
        date_col, time_col, ts_purpose, alt_logout_col = self._find_timestamp_cols(df)

        if not date_col:
            return [], {}

        # Track which event types are actually found
        found_event_types = set()
        
        def get_ts(row, dcol, tcol):
            if tcol and tcol in row.index and pd.notna(row.get(tcol)) and str(row[tcol]).strip():
                date_part = str(row.get(dcol, '')).split()[0] if dcol in row.index else ''
                combined = f"{date_part} {str(row[tcol]).strip()}"
                return _parse_datetime_cell(combined)
            return _parse_datetime_cell(row.get(dcol, row[dcol]) if dcol in row.index else None)

        def make_act(row, ts, ev, idx):
            uid = None
            if user_col and user_col in row.index and pd.notna(row[user_col]):
                uid = str(row[user_col]).strip()
            acc = None
            if account_col and account_col in row.index and pd.notna(row[account_col]):
                acc = str(row[account_col]).strip()
            if acc and not uid and acc in account_to_user:
                uid = account_to_user[acc]
            amt_raw = None
            amt_val: Optional[float] = None
            if amount_col and amount_col in row.index:
                amt_raw = row[amount_col]
                if pd.notna(amt_raw) and str(amt_raw).strip() != '':
                    try:
                        amt_val = float(str(amt_raw).replace(',', ''))
                    except Exception:
                        amt_val = None
            return {
                'user_id': uid or f"user_{acc or idx}",
                'account_id': acc or '',
                'event': ev,
                'timestamp': ts,
                'timestamp_str': ts.strftime('%Y-%m-%d %H:%M:%S'),
                'amount': amt_val,
                'amount_raw': str(amt_raw).strip() if amt_raw is not None and not (isinstance(amt_raw, float) and pd.isna(amt_raw)) else '',
                'table_name': table_name,
                'file_name': file_name,
                'source_row': int(idx) + 1 if isinstance(idx, (int, float)) else None,
                'raw_record': {c: str(row[c]) if pd.notna(row.get(c)) else '' for c in df.columns}
            }

        # If this table clearly has explicit login and logout timestamps
        # (e.g. login_time + logout_time), we treat it as a pure
        # "session table": each row becomes a login event plus a logout
        # event, and we ignore any generic "credit/debit" logic here.
        # In addition to the inferred purpose from _find_timestamp_cols,
        # we also fall back to simple column-name detection so that files
        # like banking/three.csv (user_id, username, login_time, logout_time)
        # are always handled as login/logout tables.
        has_login_col_name = any('login' in str(c).lower() for c in df.columns)
        has_logout_col_name = any('logout' in str(c).lower() for c in df.columns)
        is_login_logout_table = (
            (ts_purpose == 'login' and alt_logout_col is not None)
            or (has_login_col_name and has_logout_col_name)
        )

        # Determine which column to use for logout events in session tables.
        # Prefer the explicit alt_logout_col detected by _find_timestamp_cols;
        # otherwise, fall back to the first column whose name contains "logout".
        logout_event_col = alt_logout_col
        if is_login_logout_table and logout_event_col is None:
            for c in df.columns:
                if 'logout' in str(c).lower():
                    logout_event_col = c
                    break

        activities = []
        for idx, row in df.iterrows():
            ts = get_ts(row, date_col, time_col)
            if pd.isna(ts):
                continue

            # Pure login/logout session rows: emit login + logout only.
            if is_login_logout_table:
                # Login at the main timestamp column
                found_event_types.add('login')
                activities.append(make_act(row, ts, 'login', idx))

                # Logout from the dedicated logout column (if present and valid)
                if logout_event_col and logout_event_col in row.index:
                    logout_ts = _parse_datetime_cell(row[logout_event_col])
                    if pd.notna(logout_ts):
                        found_event_types.add('logout')
                        activities.append(make_act(row, logout_ts, 'logout', idx))
                continue

            event = None
            if event_col and event_col in row.index:
                event = _normalize_event(row[event_col])
                raw = str(row[event_col] or '').strip().upper()
                if raw in ('CREDIT', 'DEPOSIT'):
                    event = 'credit' if 'credit' in raw.lower() else 'deposit'
                elif raw in ('DEBIT', 'WITHDRAW', 'WITHDRAWAL'):
                    event = 'debit' if 'debit' in raw.lower() else 'withdraw'
                elif raw == 'REFUND':
                    event = 'refund'
                elif raw in ('DECLINED', 'BLOCKED', 'INVALID'):
                    event = 'invalid_balance'
                elif raw in ('CHECK_BALANCE', 'BALANCE_INQUIRY', 'BALANCE_CHECK', 'CHECK BALANCE', 'BALANCE'):
                    event = 'check_balance'
            if not event:
                # Fallback when there is no explicit event/transaction-type column.
                # Only trust clear purposes; do NOT default everything to "credit"
                # for generic timestamps like "DateJoined" or "StartDate".
                if ts_purpose in ('login', 'logout', 'open', 'created'):
                    event = ts_purpose
                elif ts_purpose == 'transaction':
                    # Generic transaction timestamp without type: treat as credit by default
                    # so that pure transaction tables still produce sessions.
                    event = 'credit'
                else:
                    # Unknown / generic "event" timestamps are ignored to avoid
                    # fabricating static "credit" steps from master/profile tables.
                    event = None

            if event:
                found_event_types.add(event)
                activities.append(make_act(row, ts, event, idx))

            if alt_logout_col and alt_logout_col in row.index:
                logout_ts = _parse_datetime_cell(row[alt_logout_col])
                if pd.notna(logout_ts):
                    found_event_types.add('logout')
                    activities.append(make_act(row, logout_ts, 'logout', idx))

        # Build map of event_type -> [columns used]
        # This reflects strictly what was used to generate the Case ID data
        used_cols_map: Dict[str, List[str]] = {}
        
        # Helper to add columns uniquely
        def add_cols(ev, cols):
            if ev not in used_cols_map:
                used_cols_map[ev] = []
            for c in cols:
                full_c = f"{table_name}.{c}"
                if full_c not in used_cols_map[ev]:
                    used_cols_map[ev].append(full_c)

        for ev in found_event_types:
            # If this is a logout event derived from a specific logout column, 
            # we handle it separately below (do not add the main date/time cols which are usually login time)
            if ev == 'logout' and logout_event_col:
                continue

            # All events use the date/time columns
            cols_to_add = []
            if date_col: cols_to_add.append(date_col)
            if time_col: cols_to_add.append(time_col)
            
            # If we used an event column to find this event, add it
            # But ONLY if it's not the generic time purpose
            # (e.g. if purpose is 'login', we didn't use event_col for it)
            if ev != ts_purpose and ev != 'logout' and event_col: 
                 cols_to_add.append(event_col)
            
            # Detect amount column usage inferred for this event type
            # (We didn't explicitly use it for the event *name*, but it's part of the "event data")
            # We can optionally add it for completeness if the user wants "columns used"
            
            # Map internal event names to UI buckets
            ui_bucket = ev
            if ev == 'created': ui_bucket = 'account_open'
            if ev == 'open': ui_bucket = 'account_open'
            if ev == 'invalid_balance': ui_bucket = 'failed'
            if ev == 'negative_balance': ui_bucket = 'failed'
            
            add_cols(ui_bucket, cols_to_add)

        # Handle logout_event_col separately so the blueprint knows which column
        # actually supplied real logout timestamps (login_time vs logout_time).
        if logout_event_col and 'logout' in found_event_types:
             add_cols('logout', [logout_event_col])

        return activities, used_cols_map

    def _build_account_to_user_map(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Build account_id -> user_id mapping from account/customer tables."""
        mapping = {}
        user_col_candidates = ['customer_id', 'user_id', 'cust_id']
        account_col_candidates = ['account_id', 'account_number']
        for _, df in dataframes.items():
            user_col = self._find_user_col(df)
            account_col = self._find_account_col(df)
            if user_col and account_col:
                for _, row in df.iterrows():
                    acc = row.get(account_col)
                    usr = row.get(user_col)
                    if pd.notna(acc) and pd.notna(usr):
                        mapping[str(acc).strip()] = str(usr).strip()
        return mapping

    def _identify_sessions(
        self,
        user_activities: List[Dict],
        session_gap_hours: float = 4.0,
        has_explicit_session_events: bool = False
    ) -> List[List[Dict]]:
        """
        Split user activities into sessions. Only login starts a new case.
        Created and open do NOT separate cases - they merge into the next login session.
        Duplicate sequences (same user, same pattern again) = new case.
        """
        if not user_activities:
            return []

        sorted_acts = sorted(user_activities, key=lambda x: x['timestamp'])

        # If this stream has no explicit login/logout events at all (typical for ATM / UPI
        # transaction logs), treat each contiguous block of activity (by time gap) as one
        # transaction session. Do NOT fabricate synthetic login/logout events.
        if not has_explicit_session_events:
            sessions: List[List[Dict]] = []
            current: List[Dict] = []
            last_ts = None
            for act in sorted_acts:
                ts = act['timestamp']
                if last_ts is None:
                    current = [act]
                    last_ts = ts
                    continue
                gap_hours = (ts - last_ts).total_seconds() / 3600.0
                if gap_hours >= session_gap_hours:
                    if current:
                        sessions.append(current)
                    current = [act]
                else:
                    current.append(act)
                last_ts = ts
            if current:
                sessions.append(current)
            return sessions

        sessions = []
        current = []
        pending = []
        last_ts = None

        for act in sorted_acts:
            ev = act.get('event', '')
            ts = act['timestamp']

            if ev in SESSION_START:
                if current:
                    sessions.append(current)
                current = pending + [act]
                pending = []
                last_ts = ts
                continue

            if ev in PRE_LOGIN_EVENTS:
                if current:
                    current.append(act)
                    last_ts = ts
                else:
                    pending.append(act)
                continue

            if ev in SESSION_END:
                current.append(act)
                sessions.append(current)
                current = []
                pending = []
                last_ts = None
                continue

            if ev in MIDDLE_EVENTS or ev in ('credit', 'debit', 'refund', 'deposit', 'withdraw',
                                             'invalid_balance', 'negative_balance'):
                # For the first middle-event in a new or gap-separated session,
                # create an implicit login event *before* the actual event.
                # Include any pending pre-login events (created/open) at the start
                # of this new session so they are part of the same case.
                if not current:
                    login_act = {
                        **act,
                        'event': 'login',
                        'raw_record': {**(act.get('raw_record', {})), 'inferred': 'implicit_session_start'}
                    }
                    current = pending + [login_act, act]
                    pending = []
                    last_ts = ts
                    continue
                if current:
                    if last_ts is not None:
                        gap_hours = (ts - last_ts).total_seconds() / 3600
                        if gap_hours >= session_gap_hours:
                            sessions.append(current)
                            login_act = {
                                **act,
                                'event': 'login',
                                'raw_record': {**(act.get('raw_record', {})), 'inferred': 'implicit_session_start'}
                            }
                            current = [login_act, act]
                            last_ts = ts
                            continue
                    current.append(act)
                    last_ts = ts
                else:
                    pending.append(act)
                continue

            if current:
                current.append(act)
                last_ts = ts
            else:
                pending.append(act)

        # Do NOT fabricate implicit logout events at the time of the last
        # transaction. Logout steps must come only from real logout columns
        # (e.g. a logout_time field) that were parsed into activities earlier.
        if current:
            sessions.append(current)
        elif pending:
            sessions.append(pending)

        return sessions

    def _assign_case_ids(
        self,
        all_sessions: Dict[Any, List[List[Dict]]]
    ) -> List[Dict[str, Any]]:
        """
        Assign ascending Case IDs to all sessions.
        Order by first activity timestamp globally.
        Same sequence repeating = new Case ID (each session gets unique ID).
        """
        flat = []
        for key, sessions in all_sessions.items():
            for session in sessions:
                if not session:
                    continue
                # Use the first activity to recover user/account context
                first_act = session[0]
                user_id = first_act.get('user_id')
                account_id = first_act.get('account_id')
                first_ts = min(a['timestamp'] for a in session)
                flat.append({
                    'user_id': user_id,
                    'account_id': account_id,
                    'session': session,
                    'first_timestamp': first_ts
                })

        flat.sort(key=lambda x: x['first_timestamp'])
        case_ids = []
        for i, item in enumerate(flat):
            case_id = i + 1
            activities = sorted(item['session'], key=lambda a: a['timestamp'])

            # Basic anomaly / fraud-style flags (purely analytical, do not change sessions)
            flags: List[str] = []
            if activities:
                start_ts = activities[0]['timestamp']
                end_ts = activities[-1]['timestamp']
                try:
                    duration_seconds = int((end_ts - start_ts).total_seconds())
                except Exception:
                    duration_seconds = 0

                if duration_seconds < 5:
                    flags.append('very_short_session')
                if duration_seconds > 8 * 3600:
                    flags.append('very_long_session')

                event_seq = [a.get('event') for a in activities]

                # Logout without any login in the same session
                if 'logout' in event_seq and 'login' not in event_seq:
                    flags.append('logout_without_login')

                # Login without any transaction in between
                if event_seq == ['login']:
                    flags.append('login_without_activity')

                # Abnormal flow: login → withdraw/debit → withdraw/debit
                if len(event_seq) >= 3 and event_seq[0] == 'login':
                    if event_seq[1] in ('withdraw', 'debit') and event_seq[2] in ('withdraw', 'debit'):
                        flags.append('login_with_multiple_immediate_withdrawals')

                # Any back-to-back withdrawals/debits in the session
                for j in range(len(event_seq) - 1):
                    if event_seq[j] in ('withdraw', 'debit') and event_seq[j + 1] in ('withdraw', 'debit'):
                        flags.append('multiple_withdrawals_same_session')
                        break

            # Ensure JSON-serializable: use timestamp_str, sanitize raw_record
            activities_serializable = []
            for a in activities:
                ac = {
                    'event': a.get('event'),
                    'timestamp_str': a.get('timestamp_str') or (a['timestamp'].strftime('%Y-%m-%d %H:%M:%S') if hasattr(a.get('timestamp'), 'strftime') else ''),
                    'account_id': a.get('account_id'),
                    'user_id': a.get('user_id'),
                    'table_name': a.get('table_name'),
                    'file_name': a.get('file_name'),
                    'source_row': a.get('source_row'),
                    'amount': a.get('amount'),
                    'amount_raw': a.get('amount_raw'),
                    'raw_record': {k: str(v) if v is not None and not (isinstance(v, float) and pd.isna(v)) else '' for k, v in (a.get('raw_record') or {}).items()}
                }
                activities_serializable.append(ac)
            case_ids.append({
                'case_id': case_id,
                'user_id': item['user_id'],
                'account_id': item.get('account_id'),
                'first_activity_timestamp': item['first_timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                'last_activity_timestamp': activities[-1]['timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                'activity_count': len(activities),
                'activities': activities_serializable,
                'event_sequence': [a['event'] for a in activities],
                'explanation': self._build_case_explanation(case_id, item['user_id'], activities),
                'flags': flags
            })
        return case_ids

    def _event_display_name(self, ev: str) -> str:
        """Simple English label for event. No jargon."""
        if ev == 'login':
            return 'login'
        if ev == 'logout':
            return 'logout'
        if ev == 'open':
            return 'account open'
        if ev == 'created':
            return 'created'
        if ev in ('credit', 'deposit'):
            return 'credit'
        if ev in ('debit', 'withdraw'):
            return 'debit'
        if ev == 'refund':
            return 'refund'
        if ev in ('invalid_balance', 'negative_balance'):
            return 'issue'
        return ev

    def _build_case_explanation(
        self,
        case_id: int,
        user_id: str,
        activities: List[Dict]
    ) -> str:
        """Short explanation in plain English. Times in AM/PM."""
        if not activities:
            return f"Case {case_id}: User {user_id}. No activities."
        start_ts = activities[0].get('timestamp')
        end_ts = activities[-1].get('timestamp')
        start_str = _timestamp_to_display(start_ts) if hasattr(start_ts, 'strftime') else activities[0].get('timestamp_str', '')
        end_str = _timestamp_to_display(end_ts) if hasattr(end_ts, 'strftime') else activities[-1].get('timestamp_str', '')
        steps = [self._event_display_name(a['event']) for a in activities]
        return f"Case {case_id}: User {user_id}. From {start_str} to {end_str}. Steps: {', '.join(steps)}."

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
        """Generate unified flow data for all Case IDs to be displayed in a single diagram."""
        # Predefined color palette for Case IDs
        colors = [
            '#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A', '#98D8C8',
            '#F7DC6F', '#BB8FCE', '#85C1E2', '#F8B739', '#52B788',
            '#E63946', '#F1FAEE', '#A8DADC', '#457B9D', '#1D3557'
        ]
        
        # Collect all unique events across all cases
        all_events = set()
        for case in case_details:
            for event in case.get('event_sequence', []):
                all_events.add(event)
        
        # Map event names to display names
        event_display_map = {
            'login': 'Login',
            'logout': 'Logout',
            'created': 'Created Account',
            'open': 'Created Account',
            'credit': 'Credit',
            'deposit': 'Deposit',
            'debit': 'Withdrawal Transaction',
            'withdraw': 'Withdrawal Transaction',
            'refund': 'Refund',
            'invalid_balance': 'Failed Transaction',
            'negative_balance': 'Failed Transaction',
        }
        
        # Build case paths with timing information
        case_paths = []
        for idx, case in enumerate(case_details):
            activities = case.get('activities', [])
            if not activities:
                continue
                
            case_color = colors[idx % len(colors)]
            path_sequence = ['Process']  # Start with Process
            timings = []
            
            prev_ts = None
            prev_event_name = 'Process'
            
            for i, activity in enumerate(activities):
                event = activity.get('event', '')
                event_display = event_display_map.get(event, event.title())
                ts_str = activity.get('timestamp_str', '')
                
                # Parse timestamp
                try:
                    ts = pd.to_datetime(ts_str)
                except:
                    ts = None
                
                # Calculate duration from previous event (user-friendly: hours, minutes, seconds)
                if prev_ts is not None and ts is not None:
                    duration_seconds = int((ts - prev_ts).total_seconds())
                    duration_seconds = max(0, duration_seconds)
                    days = duration_seconds // 86400
                    hours = (duration_seconds % 86400) // 3600
                    minutes = (duration_seconds % 3600) // 60
                    seconds = duration_seconds % 60
                    if days > 0:
                        time_label = f"{days} day{'s' if days != 1 else ''} {hours} hr"
                    elif hours > 0:
                        time_label = f"{hours} hr {minutes} min" if minutes else f"{hours} hr"
                    elif minutes > 0:
                        time_label = f"{minutes} min {seconds} sec" if seconds else f"{minutes} min"
                    else:
                        time_label = f"{seconds} sec"
                else:
                    duration_seconds = 0
                    time_label = 'Start' if prev_event_name == 'Process' else '0 sec'
                
                path_sequence.append(event_display)
                timings.append({
                    'from': prev_event_name,
                    'to': event_display,
                    'duration_seconds': duration_seconds,
                    'label': time_label,
                    'start_time': prev_ts.strftime('%H:%M:%S') if prev_ts else '',
                    'end_time': ts.strftime('%H:%M:%S') if ts else '',
                    'start_datetime': prev_ts.strftime('%Y-%m-%d %H:%M:%S') if prev_ts else '',
                    'end_datetime': ts.strftime('%Y-%m-%d %H:%M:%S') if ts else ''
                })
                
                prev_ts = ts
                prev_event_name = event_display
            
            # Add End node with last event datetime so UI can show "End" + full date-time
            path_sequence.append('End')
            last_ts = prev_ts
            timings.append({
                'from': prev_event_name,
                'to': 'End',
                'duration_seconds': 0,
                'label': 'End',
                'start_time': last_ts.strftime('%H:%M:%S') if last_ts else '',
                'end_time': last_ts.strftime('%H:%M:%S') if last_ts else '',
                'start_datetime': last_ts.strftime('%Y-%m-%d %H:%M:%S') if last_ts else '',
                'end_datetime': last_ts.strftime('%Y-%m-%d %H:%M:%S') if last_ts else ''
            })
            
            case_paths.append({
                'case_id': case.get('case_id'),
                'user_id': case.get('user_id'),
                'color': case_color,
                'path_sequence': path_sequence,
                'timings': timings,
                'total_duration': sum(t['duration_seconds'] for t in timings)
            })
        
        # Define all possible event types in order
        all_event_types = [
            'Process',
            'Created Account',
            'Login',
            'Withdrawal Transaction',
            'Credit',
            'Deposit',
            'Refund',
            'Check Balance',
            'Logout',
            'End'
        ]
        
        # Filter to only include events that actually appear in the data
        used_events = set()
        for path in case_paths:
            used_events.update(path['path_sequence'])
        
        filtered_event_types = [e for e in all_event_types if e in used_events]
        same_time_groups = self._compute_same_time_groups(case_paths)

        # Sankey pattern: count (from, to) transitions across all case paths (no hardcoding)
        transition_counts = {}
        for path in case_paths:
            seq = path.get('path_sequence') or []
            for i in range(len(seq) - 1):
                f, t = seq[i], seq[i + 1]
                if f and t:
                    key = (f, t)
                    transition_counts[key] = transition_counts.get(key, 0) + 1
        transition_counts_list = [{'from': f, 'to': t, 'count': c} for (f, t), c in transition_counts.items()]

        return {
            'all_event_types': filtered_event_types,
            'case_paths': case_paths,
            'total_cases': len(case_paths),
            'same_time_groups': same_time_groups,
            'transition_counts': transition_counts_list,
        }

    def analyze_cluster(
        self,
        tables: List[TableAnalysis],
        dataframes: Dict[str, pd.DataFrame],
        relationships: List[Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Process banking activity files and generate Case IDs per user.
        Returns case_ids (ascending), case_details, explanations.
        """
        account_to_user = self._build_account_to_user_map(dataframes)

        all_activities = []
        # Aggregate event columns across all tables for the blueprint UI
        # We now use the DYNAMIC filtered columns from data extraction, not static pattern matching
        event_columns: Dict[str, List[str]] = {}

        for table in tables:
            df = dataframes.get(table.table_name)
            if df is None or df.empty:
                continue

            # Extract per-row activities for Case IDs
            # This also returns the columns that were ACTUALLY used for the analysis
            file_name = getattr(table, 'file_name', '') or f"{table.table_name}.csv"
            activities, used_cols = self._extract_activities(df, table.table_name, file_name, account_to_user)
            all_activities.extend(activities)

            # Merge used columns into the global blueprint
            for ev_type, cols in used_cols.items():
                if ev_type not in event_columns:
                    event_columns[ev_type] = []
                for c in cols:
                     if c not in event_columns[ev_type]:
                         event_columns[ev_type].append(c)
        
        # If no activities found, fallback to static detection so we can at least show
        # "No columns detected" based on names, or hint what we found
        if not all_activities:
             # Repopulate event_columns statically as fallback so UI shows SOMETHING
             for table in tables:
                df = dataframes.get(table.table_name)
                if df is not None:
                     static_events = self._detect_event_columns_for_table(df, table.table_name)
                     for ev, cols in static_events.items():
                         if ev not in event_columns: event_columns[ev] = []
                         for c in cols:
                             if c not in event_columns[ev]: event_columns[ev].append(c)

             return {
                'success': False,
                'error': 'No activities found. We look for columns that contain login time, logout time, created time, or open time (and user or account).',
                'tables_checked': [t.table_name for t in tables],
                'event_columns': event_columns
            }

        # Group by (user, account) so multiple accounts for the same user are not mixed
        by_key: Dict[Tuple[str, str], List[Dict]] = {}
        for act in all_activities:
            uid = act.get('user_id') or 'unknown'
            acc = act.get('account_id') or ''
            key = (uid, acc)
            if key not in by_key:
                by_key[key] = []
            by_key[key].append(act)

        # Identify sessions per (user, account)
        sessions_by_key: Dict[Tuple[str, str], List[List[Dict]]] = {}
        for (uid, acc), acts in by_key.items():
            has_session_markers = any(a.get('event') in (SESSION_START | SESSION_END) for a in acts)
            sessions = self._identify_sessions(acts, has_explicit_session_events=has_session_markers)
            if sessions:
                sessions_by_key[(uid, acc)] = sessions

        if not sessions_by_key:
            return {
                'success': False,
                'error': 'No sessions found. Sessions need a start (e.g. login or open time) and steps (e.g. credit, debit).',
                'total_activities': len(all_activities)
            }

        case_details = self._assign_case_ids(sessions_by_key)

        case_ids_asc = [c['case_id'] for c in case_details]
        users_with_cases = sorted({c['user_id'] for c in case_details if c.get('user_id')})
        total_users = len(users_with_cases)
        total_cases = len(case_details)

        explanations = [
            f"We found {total_cases} session(s). Each session has one Case ID.",
            f"Case IDs are numbered 1 to {total_cases} in order of start time.",
            "Events are grouped by user and sorted by timestamp. Same activity meaning again (duplicate or different source) starts a new Case ID so each case is one clean process flow.",
            "Each case lists steps in time order (login, then credit or debit, then logout).",
            "Times use the columns we detected (login time, logout time, created time, or open time) from your files."
        ]

        # Generate unified flow data for visualization
        unified_flow_data = self._generate_unified_flow_data(case_details)
        
        return {
            'success': True,
            'case_ids': case_ids_asc,
            'case_details': case_details,
            'total_cases': total_cases,
            'total_users': total_users,
            'users': users_with_cases,
            'explanations': explanations,
            'total_activities': len(all_activities),
            'event_columns': event_columns,
            'unified_flow_data': unified_flow_data,
        }
