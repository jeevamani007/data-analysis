"""
Banking Timeline Analyzer
Finds timestamp columns across all tables in a banking DB cluster,
sorts all records ascending (date, time, sec, AM/PM), returns timeline for Start----|----|----End diagram.
"""

from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
import re
from models import TableAnalysis


class BankingAnalyzer:
    """Analyze banking cluster: find timestamps, sort ascending, build timeline diagram."""

    def _find_date_timestamp_columns(self, df: pd.DataFrame) -> List[Tuple[str, Optional[str]]]:
        """
        Find (date_col, time_col) for banking event time.
        Patterns: open_date, open_timestamp, txn_date, login_date, created_date, etc.
        """
        event_patterns = [
            'open_timestamp', 'created_timestamp', 'login_timestamp', 'timestamp',
            'transaction_date', 'txn_date', 'transaction_time', 'txn_time',
        ]
        date_time_pairs = [
            ('open_date', 'open_time'),
            ('login_date', 'login_time'),
            ('created_date', 'created_time'),
            ('transaction_date', 'transaction_time'),
            ('txn_date', 'txn_time'),
            ('account_date', 'account_time'),
            ('signup_date', 'signup_time'),
        ]

        def is_parseable(col: str) -> bool:
            try:
                sample = df[col].dropna().head(10)
                if len(sample) == 0:
                    return False
                parsed = pd.to_datetime(sample, errors='coerce')
                return parsed.notna().sum() >= len(sample) * 0.5
            except Exception:
                return False

        cols_lower = [c.lower() for c in df.columns]

        # 1. Single full datetime column
        for col in df.columns:
            col_lower = col.lower()
            if any(pat in col_lower for pat in event_patterns) and is_parseable(col):
                sample = df[col].dropna().iloc[0] if len(df) > 0 else None
                if sample is not None:
                    parsed = pd.to_datetime(sample, errors='coerce')
                    if pd.notna(parsed):
                        return [(col, None)]

        # 2. date + time pairs
        for date_part, time_part in date_time_pairs:
            date_col = next((c for c in df.columns if c.lower() == date_part or date_part in c.lower()), None)
            if date_col and is_parseable(date_col):
                time_col = next((c for c in df.columns if c != date_col and (c.lower() == time_part or time_part in c.lower())), None)
                return [(date_col, time_col if time_col and time_col in df.columns else None)]

        # 3. Any *date or *timestamp column
        for col in df.columns:
            col_lower = col.lower()
            if ('date' in col_lower or 'timestamp' in col_lower or 'time' in col_lower):
                if 'dob' in col_lower or 'birth' in col_lower:
                    continue
                if is_parseable(col):
                    time_col = next((c for c in df.columns if c != col and 'time' in c.lower() and 'date' not in c.lower() and 'stamp' not in c.lower()), None)
                    return [(col, time_col if time_col and time_col in df.columns else None)]

        return []

    def _infer_column_purpose(self, col_name: str, series: pd.Series) -> Dict[str, str]:
        """Infer banking column purpose from name patterns."""
        col_lower = col_name.lower().replace('-', '_')
        if 'account_id' in col_lower or 'account_number' in col_lower:
            return {'purpose': 'Account identifier'}
        if 'customer_id' in col_lower or 'customer' in col_lower and 'id' in col_lower:
            return {'purpose': 'Customer identifier'}
        if 'open_date' in col_lower or 'open_timestamp' in col_lower:
            return {'purpose': 'Account opening date/time'}
        if 'login' in col_lower and ('date' in col_lower or 'time' in col_lower):
            return {'purpose': 'Login date/time'}
        if 'txn' in col_lower or 'transaction' in col_lower:
            if 'date' in col_lower or 'time' in col_lower:
                return {'purpose': 'Transaction date/time'}
            if 'type' in col_lower:
                return {'purpose': 'Transaction type (credit/debit)'}
            if 'amount' in col_lower or 'id' in col_lower:
                return {'purpose': 'Transaction amount' if 'amount' in col_lower else 'Transaction identifier'}
        if 'amount' in col_lower or 'balance' in col_lower:
            return {'purpose': 'Amount or balance'}
        if 'branch' in col_lower:
            return {'purpose': 'Branch or location'}
        if 'ip' in col_lower or 'address' in col_lower:
            return {'purpose': 'IP address' if 'ip' in col_lower else 'Address'}
        if 'created' in col_lower:
            return {'purpose': 'Creation date/time'}
        if 'name' in col_lower:
            return {'purpose': 'Name'}
        if 'type' in col_lower and 'account' in col_lower:
            return {'purpose': 'Account type'}
        return {'purpose': col_name.replace('_', ' ').title()}

    def _build_work_summary(self, table_name: str, raw_record: Dict, column_purposes: Dict, file_name: str) -> str:
        """Build short event summary for banking record."""
        cols_lower = " ".join(c.lower() for c in raw_record.keys())
        tbl_lower = table_name.lower()

        account_id = None
        customer_id = None
        for col, purp in column_purposes.items():
            p = (purp.get('purpose') if isinstance(purp, dict) else str(purp)).lower()
            if 'account' in p and 'identifier' in p:
                account_id = raw_record.get(col, '')
                break
        if not account_id:
            for col in raw_record:
                if 'account_id' in col.lower() or 'account_number' in col.lower():
                    account_id = raw_record.get(col, '')
                    break

        if 'open' in tbl_lower or 'open' in cols_lower:
            return f"Account {account_id or '—'} opened" if account_id else "Account opened"
        if 'login' in tbl_lower or 'login' in cols_lower:
            return f"Login for account {account_id or '—'}" if account_id else "Login event"
        if 'txn' in tbl_lower or 'transaction' in tbl_lower or 'txn' in cols_lower or 'transaction' in cols_lower:
            txn_type = raw_record.get('Txn_Type') or raw_record.get('transaction_type') or raw_record.get('Transaction_Type') or ''
            amt = raw_record.get('Amount') or raw_record.get('amount') or ''
            return f"Transaction {txn_type} {amt}" if (txn_type or amt) else f"Transaction for account {account_id or '—'}"
        if 'created' in cols_lower:
            return f"Record created for account {account_id or '—'}" if account_id else "Record created"
        return f"Banking record from {table_name}"

    def _extract_datetime(self, row, date_col: str, time_col: Optional[str], df: pd.DataFrame) -> Optional[pd.Timestamp]:
        """Extract combined datetime from row, handling AM/PM."""
        try:
            date_val = row.get(date_col, row[date_col]) if date_col in row.index else None
            if pd.isna(date_val):
                return None
            dt = pd.to_datetime(date_val, errors='coerce')
            if pd.isna(dt):
                return None
            if time_col and time_col in row.index:
                t_val = row[time_col]
                if pd.notna(t_val) and str(t_val).strip():
                    date_part = str(date_val).split()[0] if hasattr(date_val, 'split') else str(dt.date())
                    combined = f"{date_part} {str(t_val).strip()}"
                    parsed = pd.to_datetime(combined, errors='coerce')
                    if pd.notna(parsed):
                        return parsed
                    try:
                        return pd.to_datetime(combined, format="%Y-%m-%d %I:%M %p", errors='coerce')
                    except Exception:
                        pass
            return dt
        except Exception:
            return None

    def _table_to_sorted_records(
        self,
        df: pd.DataFrame,
        table_name: str,
        file_name: str = ""
    ) -> List[Dict[str, Any]]:
        """For one table: find date/timestamp, sort ascending, return records with event datetime."""
        cols = self._find_date_timestamp_columns(df)
        if not cols:
            return []
        date_col, time_col = cols[0]
        df = df.copy()
        df['__dt'] = pd.to_datetime(df[date_col], errors='coerce')
        if time_col and time_col in df.columns:
            df['__date_str'] = df[date_col].astype(str).str.split().str[0]
            df['__time_str'] = df[time_col].astype(str)
            df['__dt'] = pd.to_datetime(df['__date_str'] + ' ' + df['__time_str'], errors='coerce')
            if df['__dt'].isna().any():
                def _try_parse(row):
                    if pd.notna(row['__dt']):
                        return row['__dt']
                    date_part = str(row['__date_str'])
                    time_part = str(row['__time_str'])
                    try:
                        return pd.to_datetime(f"{date_part} {time_part}", format="%Y-%m-%d %I:%M %p", errors='coerce')
                    except Exception:
                        return pd.NaT
                df['__dt'] = df.apply(_try_parse, axis=1)
            df['__dt'] = df['__dt'].fillna(pd.to_datetime(df[date_col], errors='coerce'))
        df = df.dropna(subset=['__dt'])
        df = df.sort_values('__dt', ascending=True)

        skip_cols = {'__dt', '__date_str', '__time_str'}
        data_cols = [c for c in df.columns if c not in skip_cols and not str(c).startswith('__')]
        column_purposes = {c: self._infer_column_purpose(c, df[c]) for c in data_cols}

        records = []
        for row_idx, row in df.iterrows():
            dt = row['__dt']
            raw_record = {c: ('' if (row.get(c) is None or (isinstance(row.get(c), float) and pd.isna(row.get(c)))) else str(row.get(c)).strip()) for c in data_cols}
            work_summary = self._build_work_summary(table_name, raw_record, column_purposes, file_name)
            event_date = dt.strftime('%Y-%m-%d')
            event_time = dt.strftime('%H:%M:%S')
            time_log = f"Event: {event_date} {event_time}"

            records.append({
                'table_name': table_name,
                'file_name': file_name or f"{table_name}.csv",
                'source_row_number': int(row_idx) + 1 if isinstance(row_idx, (int, float)) else None,
                'date': event_date,
                'time': event_time,
                'event_datetime': f"{event_date} {event_time}",
                'datetime_sort': dt,
                'record': raw_record,
                'column_purposes': column_purposes,
                'work_summary': work_summary,
                'row_event_story': work_summary,
                'time_log_explanation': time_log,
                'table_workflow_role': {'role': 'banking', 'role_explanation': f'Banking table: {table_name}'},
            })
        return records

    def analyze_cluster(
        self,
        tables: List[TableAnalysis],
        dataframes: Dict[str, pd.DataFrame],
        relationships: List[Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Analyze banking cluster: find timestamps across all tables,
        merge and sort ascending (date, time, sec, AM/PM), return Start----|----|----End diagram.
        """
        all_records: List[Dict[str, Any]] = []
        tables_summary = []

        for table in tables:
            table_name = table.table_name
            file_name = getattr(table, 'file_name', '') or f"{table_name}.csv"
            df = dataframes.get(table_name)
            if df is None or df.empty:
                continue
            records = self._table_to_sorted_records(df, table_name, file_name)
            if not records:
                continue
            all_records.extend(records)
            col_purposes = records[0].get('column_purposes', {}) if records else {}
            tables_summary.append({
                'table_name': table_name,
                'file_name': file_name,
                'row_count': len(records),
                'first_date': records[0]['date'],
                'last_date': records[-1]['date'],
                'column_purposes': col_purposes,
            })

        if not all_records:
            return {
                'success': False,
                'error': 'No date/timestamp columns found in banking tables.',
                'tables_checked': [t.table_name for t in tables]
            }

        all_records.sort(key=lambda r: r['datetime_sort'])

        for r in all_records:
            del r['datetime_sort']

        first_date = all_records[0]['date']
        last_date = all_records[-1]['date']
        first_time = all_records[0].get('time', '')
        last_time = all_records[-1].get('time', '')

        node_map = {}
        for r in all_records:
            dt_key = (r['date'], r.get('time', ''))
            if dt_key not in node_map:
                node_map[dt_key] = {
                    'date': r['date'],
                    'time': r.get('time', ''),
                    'records': [],
                    'sort_key': f"{r['date']} {r.get('time', '') or '00:00:00'}",
                }
            node_map[dt_key]['records'].append(r)

        diagram_nodes = []
        for dt_key in sorted(node_map.keys(), key=lambda k: node_map[k]['sort_key']):
            n = node_map[dt_key]
            diagram_nodes.append({
                'date': n['date'],
                'time': n['time'],
                'count': len(n['records']),
                'records': n['records'],
                'table_names': list(set(x['table_name'] for x in n['records']))
            })

        return {
            'success': True,
            'tables_summary': tables_summary,
            'sorted_timeline': all_records,
            'diagram_nodes': diagram_nodes,
            'first_date': first_date,
            'last_date': last_date,
            'first_datetime': f"{first_date} {first_time}".strip(),
            'last_datetime': f"{last_date} {last_time}".strip(),
            'total_records': len(all_records),
            'total_tables_with_dates': len(tables_summary),
        }
