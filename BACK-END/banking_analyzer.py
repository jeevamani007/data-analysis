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
    'invalid_balance', 'negative_balance', 'invalid', 'negative'
}

EVENT_MAP = {
    'login': 'login', 'log in': 'login', 'session_start': 'login', 'open': 'login',
    'logout': 'logout', 'log out': 'logout', 'session_end': 'logout',
    'credit': 'credit', 'deposit': 'deposit', 'withdraw': 'withdraw', 'withdrawal': 'withdraw',
    'debit': 'debit', 'refund': 'refund',
    'invalid_balance': 'invalid_balance', 'invalid': 'invalid_balance',
    'negative_balance': 'negative_balance', 'negative': 'negative_balance',
    'declined': 'invalid_balance', 'blocked': 'invalid_balance',
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
    return EVENT_MAP.get(s) or (s if s in SESSION_START | SESSION_END | MIDDLE_EVENTS else None)


def _parse_datetime_cell(val: Any) -> Optional[pd.Timestamp]:
    """Parse one value to datetime. Tries multiple formats including AM/PM. No hardcoded column names."""
    if val is None or (isinstance(val, float) and pd.isna(val)) or str(val).strip() == '':
        return None
    s = str(val).strip()
    # pandas first
    dt = pd.to_datetime(s, errors='coerce')
    if pd.notna(dt):
        return dt
    for fmt in _DATETIME_FORMATS:
        try:
            dt = pd.to_datetime(s, format=fmt, errors='coerce')
            if pd.notna(dt):
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

    def _infer_time_purpose(self, col_name: str) -> Optional[str]:
        """Infer purpose from column name. login, logout, created, open are distinct. No hardcoding."""
        c = col_name.lower().replace('-', '_').replace(' ', '_')
        if re.search(r'\blogout\b', c) or 'session_end' in c:
            return 'logout'
        if re.search(r'\blogin\b', c):
            return 'login'
        if re.search(r'\bcreated\b', c):
            return 'created'
        if re.search(r'\bopen\b', c) and re.search(r'date|time|stamp', c):
            return 'open'
        if re.search(r'txn|transaction', c) and re.search(r'date|time|stamp', c):
            return 'transaction'
        if 'event' in c and re.search(r'date|time|stamp', c):
            return 'event'
        if re.search(r'date|time|stamp', c) and 'dob' not in c and 'birth' not in c:
            return 'event'
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
        Enhanced to detect more column name variations and support columns without strict date/time suffixes.
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

        for col in df.columns:
            name = str(col)
            c = name.lower().replace('-', '_').replace(' ', '_')

            # Time-based purpose (login / logout / open / created / transaction)
            purpose = self._infer_time_purpose(name)
            if purpose == 'login':
                events['login'].append(name)
            elif purpose == 'logout':
                events['logout'].append(name)
            elif purpose in ('open', 'created'):
                events['account_open'].append(name)

            # Deposit / credit columns - broader matching
            # Support columns with date/time OR just the event name
            if 'deposit' in c or ('credit' in c and 'card' not in c):
                # Already added by time purpose above, or add if has timestamp keywords or is event-type column
                if any(k in c for k in ('date', 'time', 'timestamp', 'at', 'on')) or c in ('deposit', 'credit', 'deposit_amount', 'credit_amount'):
                    if name not in events['deposit']:
                        events['deposit'].append(name)

            # Withdraw / debit columns - broader matching
            if ('withdraw' in c or 'withdrawal' in c or 'debit' in c):
                if any(k in c for k in ('date', 'time', 'timestamp', 'at', 'on')) or c in ('withdraw', 'withdrawal', 'debit', 'withdraw_amount', 'debit_amount'):
                    if name not in events['withdraw']:
                        events['withdraw'].append(name)

            # Refund columns - broader matching
            if 'refund' in c:
                if any(k in c for k in ('date', 'time', 'timestamp', 'at', 'on')) or c in ('refund', 'refund_amount'):
                    if name not in events['refund']:
                        events['refund'].append(name)

            # Failed / declined / blocked / invalid indicators - add status columns
            if any(k in c for k in ('failed', 'declined', 'blocked', 'invalid', 'negative', 'status', 'result')):
                # Also check for status-related event detection
                if 'status' in c or 'result' in c or any(k in c for k in ('failed', 'declined', 'blocked', 'invalid', 'negative')):
                    if name not in events['failed']:
                        events['failed'].append(name)

            # Balance columns (used for Check Balance)
            if 'balance' in c:
                if name not in events['check_balance']:
                    events['check_balance'].append(name)
            
            # Special handling for event/transaction_type columns
            # These columns contain the event type as values, not in the column name
            if c in ('event', 'transaction_type', 'txn_type', 'type', 'activity', 'action'):
                # Add to all relevant event types since this column can indicate any event
                # We'll mark it specially so the UI knows it's a multi-purpose event indicator
                for event_type in ['login', 'logout', 'deposit', 'withdraw', 'refund', 'failed']:
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
            return {
                'user_id': uid or f"user_{acc or idx}",
                'account_id': acc or '',
                'event': ev,
                'timestamp': ts,
                'timestamp_str': ts.strftime('%Y-%m-%d %H:%M:%S'),
                'table_name': table_name,
                'file_name': file_name,
                'source_row': int(idx) + 1 if isinstance(idx, (int, float)) else None,
                'raw_record': {c: str(row[c]) if pd.notna(row.get(c)) else '' for c in df.columns}
            }

        activities = []
        for idx, row in df.iterrows():
            ts = get_ts(row, date_col, time_col)
            if pd.isna(ts):
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
            if not event:
                event = ts_purpose if ts_purpose in ('login', 'logout', 'open', 'created', 'transaction') else 'credit'
                if event == 'transaction':
                    event = 'credit'
            
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
            if ev == 'logout' and alt_logout_col:
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

        # Handle alt_logout_col separately
        if alt_logout_col and 'logout' in found_event_types:
             add_cols('logout', [alt_logout_col])

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
        session_gap_hours: float = 4.0
    ) -> List[List[Dict]]:
        """
        Split user activities into sessions. Only login starts a new case.
        Created and open do NOT separate cases - they merge into the next login session.
        Duplicate sequences (same user, same pattern again) = new case.
        """
        if not user_activities:
            return []

        sorted_acts = sorted(user_activities, key=lambda x: x['timestamp'])
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
                if not current and not pending:
                    current = [{
                        **act, 'event': 'login',
                        'raw_record': {**(act.get('raw_record', {})), 'inferred': 'implicit_session_start'}
                    }]
                    last_ts = ts
                    continue
                if current:
                    if last_ts is not None:
                        gap_hours = (ts - last_ts).total_seconds() / 3600
                        if gap_hours >= session_gap_hours:
                            sessions.append(current)
                            current = [{
                                **act, 'event': 'login',
                                'raw_record': {**(act.get('raw_record', {})), 'inferred': 'implicit_session_start'}
                            }]
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

        if current:
            if current[-1].get('event') not in SESSION_END:
                last_act = current[-1]
                current.append({
                    **last_act,
                    'event': 'logout',
                    'raw_record': {**(last_act.get('raw_record', {})), 'inferred': 'implicit_session_end'}
                })
            sessions.append(current)
        elif pending:
            sessions.append(pending)

        return sessions

    def _assign_case_ids(
        self,
        all_sessions_by_user: Dict[str, List[List[Dict]]]
    ) -> List[Dict[str, Any]]:
        """
        Assign ascending Case IDs to all sessions.
        Order by first activity timestamp globally.
        Same sequence repeating = new Case ID (each session gets unique ID).
        """
        flat = []
        for user_id, sessions in all_sessions_by_user.items():
            for session in sessions:
                if not session:
                    continue
                first_ts = min(a['timestamp'] for a in session)
                flat.append({
                    'user_id': user_id,
                    'session': session,
                    'first_timestamp': first_ts
                })

        flat.sort(key=lambda x: x['first_timestamp'])
        case_ids = []
        for i, item in enumerate(flat):
            case_id = i + 1
            activities = sorted(item['session'], key=lambda a: a['timestamp'])
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
                    'raw_record': {k: str(v) if v is not None and not (isinstance(v, float) and pd.isna(v)) else '' for k, v in (a.get('raw_record') or {}).items()}
                }
                activities_serializable.append(ac)
            case_ids.append({
                'case_id': case_id,
                'user_id': item['user_id'],
                'first_activity_timestamp': item['first_timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                'last_activity_timestamp': activities[-1]['timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                'activity_count': len(activities),
                'activities': activities_serializable,
                'event_sequence': [a['event'] for a in activities],
                'explanation': self._build_case_explanation(case_id, item['user_id'], activities)
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

        # Group by user
        by_user: Dict[str, List[Dict]] = {}

        for act in all_activities:
            uid = act.get('user_id') or 'unknown'
            if uid not in by_user:
                by_user[uid] = []
            by_user[uid].append(act)

        # Identify sessions per user
        sessions_by_user: Dict[str, List[List[Dict]]] = {}
        for uid, acts in by_user.items():
            sessions = self._identify_sessions(acts)
            if sessions:
                sessions_by_user[uid] = sessions

        if not sessions_by_user:
            return {
                'success': False,
                'error': 'No sessions found. Sessions need a start (e.g. login or open time) and steps (e.g. credit, debit).',
                'total_activities': len(all_activities)
            }

        case_details = self._assign_case_ids(sessions_by_user)

        case_ids_asc = [c['case_id'] for c in case_details]
        users_with_cases = list(sessions_by_user.keys())
        total_users = len(users_with_cases)
        total_cases = len(case_details)

        explanations = [
            f"We found {total_cases} session(s). Each session has one Case ID.",
            f"Case IDs are numbered 1 to {total_cases} in order of start time.",
            "Each case lists steps in time order (login, then credit or debit, then logout).",
            "Times use the columns we detected (login time, logout time, created time, or open time) from your files."
        ]

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
        }
