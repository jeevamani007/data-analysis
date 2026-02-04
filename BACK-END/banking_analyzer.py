"""
Banking Timeline Analyzer
Finds timestamp columns across all tables in a banking DB cluster,
sorts all records ascending (date, time, sec, AM/PM), returns timeline for Start----|----|----End diagram.
All explanations built from observed column names and data — no hardcoding.
"""

from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
import re
from models import TableAnalysis


def _safe_float(v) -> Optional[float]:
    try:
        if v is None or (isinstance(v, float) and pd.isna(v)) or str(v).strip() == '':
            return None
        return float(str(v).replace(',', ''))
    except (ValueError, TypeError):
        return None


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
        """Infer banking column purpose from name patterns. No hardcoded column names."""
        col_lower = col_name.lower().replace('-', '_')
        if 'account' in col_lower and ('id' in col_lower or 'number' in col_lower):
            return {'purpose': 'Account identifier'}
        if 'customer' in col_lower and 'id' in col_lower:
            return {'purpose': 'Customer identifier'}
        if 'open' in col_lower and ('date' in col_lower or 'timestamp' in col_lower):
            return {'purpose': 'Account opening date/time'}
        if 'login' in col_lower and ('date' in col_lower or 'time' in col_lower):
            return {'purpose': 'Login date/time'}
        if 'logout' in col_lower or 'session_end' in col_lower:
            return {'purpose': 'Logout or session end time'}
        if 'updated' in col_lower or 'modified' in col_lower:
            return {'purpose': 'Record last updated date/time'}
        if 'txn' in col_lower or 'transaction' in col_lower:
            if 'date' in col_lower or 'time' in col_lower:
                return {'purpose': 'Transaction date/time'}
            if 'type' in col_lower:
                return {'purpose': 'Transaction type (credit/debit/refund)'}
            if 'amount' in col_lower:
                return {'purpose': 'Transaction amount'}
            if 'id' in col_lower:
                return {'purpose': 'Transaction identifier'}
        if 'balance' in col_lower:
            if 'before' in col_lower or 'prior' in col_lower:
                return {'purpose': 'Balance before transaction'}
            if 'after' in col_lower:
                return {'purpose': 'Balance after transaction'}
            return {'purpose': 'Account balance'}
        if 'amount' in col_lower:
            return {'purpose': 'Amount or balance'}
        if 'branch' in col_lower:
            return {'purpose': 'Branch or location'}
        if 'ip' in col_lower:
            return {'purpose': 'IP address'}
        if 'created' in col_lower:
            return {'purpose': 'Creation date/time'}
        if 'name' in col_lower:
            return {'purpose': 'Name'}
        if 'type' in col_lower and 'account' in col_lower:
            return {'purpose': 'Account type'}
        if 'refund' in col_lower:
            return {'purpose': 'Refund indicator or amount'}
        return {'purpose': col_name.replace('_', ' ').title()}

    def _get_user_account_id(self, raw_record: Dict, column_purposes: Dict) -> Tuple[Optional[str], Optional[str]]:
        """Extract account ID and user/customer ID from record using purpose patterns."""
        account_id, user_id = None, None
        for col, purp in column_purposes.items():
            p = (purp.get('purpose') if isinstance(purp, dict) else str(purp)).lower()
            v = raw_record.get(col, '')
            if not v or str(v).strip() == '':
                continue
            v = str(v).strip()
            if 'account' in p and ('identifier' in p or 'id' in p or 'number' in p):
                account_id = v
            elif 'customer' in p and 'identifier' in p:
                user_id = v
        if not account_id:
            for col in raw_record:
                cl = col.lower()
                if ('account_id' in cl or 'account_number' in cl) and raw_record.get(col):
                    account_id = str(raw_record[col]).strip()
                    break
        if not user_id:
            for col, purp in column_purposes.items():
                p = (purp.get('purpose') if isinstance(purp, dict) else str(purp)).lower()
                if 'name' in p and raw_record.get(col):
                    user_id = str(raw_record[col]).strip()
                    break
        return (account_id, user_id)

    def _find_col_by_purpose(self, column_purposes: Dict, *substrings: str) -> Optional[str]:
        """Find column whose purpose contains any of the substrings."""
        for col, purp in column_purposes.items():
            p = (purp.get('purpose') if isinstance(purp, dict) else str(purp)).lower()
            if any(s in p for s in substrings):
                return col
        return None

    def _build_data_flow_explanation(
        self,
        table_name: str,
        file_name: str,
        raw_record: Dict,
        column_purposes: Dict,
        event_datetime: str,
        work_summary: str,
    ) -> str:
        """
        Build clear English explanation from observed data.
        Example: "At this time (2026-01-01 09:10:00), account ACC001 was created. User: Karthik. Source: One (one.csv). Details: Account type Savings, Balance 5000."
        """
        parts = []
        account_id, user_id = self._get_user_account_id(raw_record, column_purposes)
        parts.append(f"At this time ({event_datetime}), {work_summary}.")
        if user_id:
            parts.append(f"User: {user_id}.")
        parts.append(f"Source: {table_name}" + (f" ({file_name})" if file_name else "") + ".")
        detail_parts = []
        include_patterns = ('balance', 'amount', 'type', 'name', 'branch', 'ip')
        for col, purp in column_purposes.items():
            v = raw_record.get(col, '')
            if not v or str(v).strip() == '':
                continue
            v = str(v).strip()
            cl = col.lower()
            p = (purp.get('purpose') if isinstance(purp, dict) else str(purp)).lower()
            if any(pat in p or pat in cl for pat in include_patterns):
                detail_parts.append(f"{purp.get('purpose', col)}: {v}")
            elif 'id' not in cl or 'account' in cl or 'customer' in cl:
                if 'date' not in cl and 'time' not in cl:
                    detail_parts.append(f"{purp.get('purpose', col)}: {v}")
        if detail_parts:
            parts.append("Details: " + ", ".join(detail_parts[:10]) + ".")
        return " ".join(parts)

    def _build_transaction_explanation(
        self, raw_record: Dict, column_purposes: Dict
    ) -> Optional[Dict[str, Any]]:
        """
        Build transaction explanation: credit/debit/refund, before/after balance, increase/decrease, negative balance.
        All from observed columns — no hardcoding.
        """
        txn_type_col = self._find_col_by_purpose(column_purposes, 'transaction type', 'credit', 'debit', 'refund')
        amount_col = self._find_col_by_purpose(column_purposes, 'amount', 'balance') or next((c for c in raw_record if 'amount' in c.lower() or ('balance' in c.lower() and 'after' in c.lower())), None)
        before_col = self._find_col_by_purpose(column_purposes, 'balance before', 'prior') or next((c for c in raw_record if 'before' in c.lower() or 'prior' in c.lower()), None)
        after_col = self._find_col_by_purpose(column_purposes, 'balance after') or next((c for c in raw_record if 'after' in c.lower() and 'balance' in c.lower()), None)

        txn_type = str(raw_record.get(txn_type_col or '', '')).strip().upper() if txn_type_col else ''
        amount = _safe_float(raw_record.get(amount_col, 0)) if amount_col else None
        before = _safe_float(raw_record.get(before_col, '')) if before_col else None
        after = _safe_float(raw_record.get(after_col, '')) if after_col else None

        if not txn_type and amount is None:
            return None

        parts = []
        type_val = (txn_type_col and raw_record.get(txn_type_col)) or ''
        if not type_val:
            for k, v in raw_record.items():
                if 'type' in k.lower() and 'txn' in k.lower() or ('transaction' in k.lower() and 'type' in k.lower()):
                    type_val = str(v or '')
                    break
        type_lower = str(type_val).lower()
        is_credit = 'credit' in type_lower
        is_debit = 'debit' in type_lower
        is_refund = 'refund' in type_lower

        if amount is not None and amount != 0:
            if is_credit or is_refund:
                parts.append(f"Balance increased by {amount} ({txn_type or 'credit/refund'}).")
            elif is_debit:
                parts.append(f"Balance decreased by {amount} (debit).")
            else:
                parts.append(f"Amount: {amount} ({txn_type or 'transaction'}).")

        if before is not None and after is not None:
            parts.append(f"Before: {before}. After: {after}.")
            if after < before:
                parts.append("Decrease.")
            elif after > before:
                parts.append("Increase.")
        elif before is not None:
            parts.append(f"Balance before: {before}.")
        elif after is not None:
            parts.append(f"Balance after: {after}.")

        negative_block = None
        if after is not None and after < 0:
            negative_block = f"Negative balance at this time: {after}. Account is overdrawn."
        elif before is not None and before < 0:
            negative_block = f"Negative balance before this transaction: {before}."

        return {
            'explanation': " ".join(parts) if parts else f"Transaction: {txn_type} {amount or ''}",
            'is_credit': is_credit or is_refund,
            'is_debit': is_debit,
            'is_refund': is_refund,
            'amount': amount,
            'balance_before': before,
            'balance_after': after,
            'negative_balance_block': negative_block,
        }

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
            event_datetime = f"{event_date} {event_time}"
            time_log = f"Event: {event_datetime}"

            data_flow = self._build_data_flow_explanation(table_name, file_name or '', raw_record, column_purposes, event_datetime, work_summary)

            update_explanation = None
            update_col = self._find_col_by_purpose(column_purposes, 'updated', 'modified')
            if update_col and raw_record.get(update_col):
                update_explanation = f"This record was last updated at {raw_record[update_col]}."

            login_explanation = None
            cols_lower = " ".join(c.lower() for c in raw_record.keys())
            if 'login' in cols_lower:
                account_id, _ = self._get_user_account_id(raw_record, column_purposes)
                ip_col = self._find_col_by_purpose(column_purposes, 'ip')
                ip_val = raw_record.get(ip_col, '') if ip_col else ''
                login_explanation = f"User logged in at this time." + (f" IP: {ip_val}." if ip_val else "")

            transaction_explanation = None
            if 'txn' in cols_lower or 'transaction' in cols_lower:
                tx = self._build_transaction_explanation(raw_record, column_purposes)
                if tx:
                    transaction_explanation = tx

            rec = {
                'table_name': table_name,
                'file_name': file_name or f"{table_name}.csv",
                'source_row_number': int(row_idx) + 1 if isinstance(row_idx, (int, float)) else None,
                'date': event_date,
                'time': event_time,
                'event_datetime': event_datetime,
                'datetime_sort': dt,
                'record': raw_record,
                'column_purposes': column_purposes,
                'work_summary': work_summary,
                'row_event_story': work_summary,
                'time_log_explanation': time_log,
                'data_flow_explanation': data_flow,
                'update_explanation': update_explanation,
                'login_explanation': login_explanation,
                'transaction_explanation': transaction_explanation,
                'table_workflow_role': {'role': 'banking', 'role_explanation': f'Banking table: {table_name}'},
                '_account_id': self._get_user_account_id(raw_record, column_purposes)[0],
            }
            records.append(rec)
        return records

    def _compute_running_balance(self, all_records: List[Dict[str, Any]]) -> None:
        """
        Compute before/after balance for transactions when not in data.
        Uses account Balance from account tables and runs forward per account.
        """
        running: Dict[str, float] = {}
        # First pass: load initial balances from all account records (any order)
        for r in all_records:
            rec = r.get('record', {}) or {}
            cols_lower = " ".join(c.lower() for c in rec.keys())
            tbl = r.get('table_name', '').lower()
            if 'balance' not in cols_lower:
                continue
            account_id = r.get('_account_id') or rec.get('Account_ID') or rec.get('account_id', '')
            if not account_id:
                continue
            acc = str(account_id).strip()
            for c in rec:
                if 'balance' in c.lower() and 'before' not in c.lower() and 'after' not in c.lower():
                    bal = _safe_float(rec.get(c))
                    if bal is not None and (acc not in running or running[acc] == 0):
                        running[acc] = bal
                    break

        for r in all_records:
            rec = r.get('record', {}) or {}
            cols_lower = " ".join(c.lower() for c in rec.keys())
            tbl = r.get('table_name', '').lower()
            account_id = r.get('_account_id') or rec.get('Account_ID') or rec.get('account_id', '')
            if not account_id:
                continue
            acc = str(account_id).strip()

            if 'txn' in tbl or 'transaction' in tbl or 'txn' in cols_lower or 'transaction' in cols_lower:
                tx = r.get('transaction_explanation')
                if not tx or (tx.get('balance_before') is not None and tx.get('balance_after') is not None):
                    continue
                amount = tx.get('amount') or _safe_float(rec.get('Amount') or rec.get('amount', 0)) or 0
                is_credit = tx.get('is_credit', False)
                is_debit = tx.get('is_debit', False)
                is_refund = tx.get('is_refund', False)
                before = running.get(acc, 0)
                if is_credit or is_refund:
                    after = before + amount
                elif is_debit:
                    after = before - amount
                else:
                    after = before
                running[acc] = after
                tx['balance_before'] = before
                tx['balance_after'] = after
                tx['explanation'] = (tx.get('explanation', '') or '') + f" Before balance: {before}. After balance: {after}."

    def _add_explanation_blocks(self, all_records: List[Dict[str, Any]]) -> None:
        """
        Add explanation blocks from observed data:
        - Same-day multiple accounts (one user created 2+ accounts this day)
        - Same-day multiple transactions (one account had 2+ transactions this day)
        - Negative balance block
        """
        by_user_date: Dict[Tuple[str, str], int] = {}
        by_account_date: Dict[Tuple[str, str], int] = {}

        for r in all_records:
            r['explanation_blocks'] = []
            account_id = r.get('_account_id', '')
            user_id = self._get_user_account_id(r.get('record', {}), r.get('column_purposes', {}))[1]
            date = r.get('date', '')
            cols_lower = " ".join(c.lower() for c in (r.get('record', {}) or {}).keys())
            tbl = r.get('table_name', '').lower()

            if 'open' in tbl or 'open' in cols_lower or ('created' in cols_lower and 'account' in cols_lower):
                key = (user_id or account_id or str(r.get('record', {}).get('Account_ID', r.get('record', {}).get('account_id', ''))), date)
                if key[0]:
                    by_user_date[key] = by_user_date.get(key, 0) + 1

            if 'txn' in tbl or 'transaction' in tbl or 'txn' in cols_lower or 'transaction' in cols_lower:
                acc = account_id or (r.get('record', {}) or {}).get('Account_ID') or (r.get('record', {}) or {}).get('account_id', '')
                if acc:
                    key = (str(acc), date)
                    by_account_date[key] = by_account_date.get(key, 0) + 1

        for r in all_records:
            account_id = r.get('_account_id', '')
            user_id = self._get_user_account_id(r.get('record', {}), r.get('column_purposes', {}))[1]
            date = r.get('date', '')
            cols_lower = " ".join(c.lower() for c in (r.get('record', {}) or {}).keys())
            tbl = r.get('table_name', '').lower()

            if 'open' in tbl or 'open' in cols_lower or ('created' in cols_lower and 'account' in cols_lower):
                key = (user_id or account_id or str((r.get('record', {}) or {}).get('Account_ID', (r.get('record', {}) or {}).get('account_id', ''))), date)
                if key[0] and by_user_date.get(key, 0) >= 2:
                    r['explanation_blocks'].append({
                        'type': 'same_day_multiple_accounts',
                        'explanation': 'Same day: This user created 2 or more accounts on this date.',
                    })

            if 'txn' in tbl or 'transaction' in tbl or 'txn' in cols_lower or 'transaction' in cols_lower:
                acc = account_id or (r.get('record', {}) or {}).get('Account_ID') or (r.get('record', {}) or {}).get('account_id', '')
                if acc and by_account_date.get((str(acc), date), 0) >= 2:
                    r['explanation_blocks'].append({
                        'type': 'same_day_multiple_transactions',
                        'explanation': 'This account had 2 or more transactions on this day.',
                    })

            tx = r.get('transaction_explanation')
            if tx:
                before = tx.get('balance_before')
                after = tx.get('balance_after')
                if before is not None and after is not None:
                    if after > before:
                        r['explanation_blocks'].append({
                            'type': 'balance_increase',
                            'explanation': f"Before balance: {before}. After balance: {after}. Balance increased.",
                        })
                    elif after < before:
                        r['explanation_blocks'].append({
                            'type': 'balance_decrease',
                            'explanation': f"Before balance: {before}. After balance: {after}. Balance decreased.",
                        })
                if tx.get('is_credit'):
                    r['explanation_blocks'].append({
                        'type': 'credit',
                        'explanation': f"Credit: Balance increased by {tx.get('amount', '—')}.",
                    })
                if tx.get('is_debit'):
                    r['explanation_blocks'].append({
                        'type': 'debit',
                        'explanation': f"Debit: Balance decreased by {tx.get('amount', '—')}.",
                    })
                if tx.get('is_refund'):
                    r['explanation_blocks'].append({
                        'type': 'refund',
                        'explanation': f"Refund: Amount {tx.get('amount', '—')} credited back.",
                    })
                if tx.get('negative_balance_block'):
                    r['explanation_blocks'].append({
                        'type': 'negative_balance',
                        'explanation': tx['negative_balance_block'],
                    })

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

        self._compute_running_balance(all_records)
        self._add_explanation_blocks(all_records)

        for r in all_records:
            del r['datetime_sort']
            r.pop('_account_id', None)

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
