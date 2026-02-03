"""
Login Workflow Analyzer
Analyzes login patterns and correlates them with financial transactions.
"""

import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Optional
import numpy as np

class LoginWorkflowAnalyzer:
    """Analyzes user login patterns and correlates with transaction activity"""
    
    LOGIN_SYNONYMS = ['login', 'auth', 'access', 'session', 'signin', 'entry', 'enroll', 'signup', 'register', 'activity', 'visit']
    TIME_SYNONYMS = ['time', 'date', 'created_at', 'timestamp', 'logintime', 'login_date', 'datetime', 'enrollment_date', 'activity_date']
    ID_SYNONYMS = ['user_id', 'account_id', 'customer_id', 'cust_id', 'account_number', 'uid', 'id', 'account']

    def _find_best_column(self, df: pd.DataFrame, synonyms: List[str]) -> Optional[str]:
        for col in df.columns:
            col_lower = col.lower()
            if any(syn in col_lower for syn in synonyms):
                return col
        return None

    def _ids_match(self, id1: Any, id2: Any) -> bool:
        """Robust ID matching that handles leading zeros and different types"""
        if pd.isna(id1) or pd.isna(id2):
            return False
            
        s1 = str(id1).strip().lower()
        s2 = str(id2).strip().lower()
        
        # Exact match
        if s1 == s2:
            return True
            
        # Try numeric normalized match (ignores leading zeros)
        try:
            # If both can be converted to ints and are equal, it's a match (e.g. "001" == "1")
            return int(float(s1)) == int(float(s2))
        except (ValueError, TypeError):
            return False
    
    def detect_first_login(self, login_df: pd.DataFrame, login_col: str, id_col: str) -> pd.DataFrame:
        """Find the earliest login timestamp for each customer."""
        if login_df is None or login_df.empty:
            return pd.DataFrame()
            
        working = login_df.copy()
        working[login_col] = pd.to_datetime(working[login_col], errors='coerce')
        working = working.dropna(subset=[login_col, id_col])
        
        if working.empty:
            return pd.DataFrame()
        
        # Group by customer and find earliest login
        first_logins = working.groupby(id_col)[login_col].min().reset_index()
        first_logins.columns = [id_col, 'first_login_time']
        
        return first_logins
    
    def calculate_login_delay(self, accounts_df: pd.DataFrame, logins_df: Optional[pd.DataFrame], 
                              open_col: str, login_col: str, id_col: str) -> Dict[str, Any]:
        """Calculate delay between account creation and first login."""
        if accounts_df is None or accounts_df.empty:
            return {'has_login_data': False, 'message': 'No account data'}
        
        # Prepare accounts
        accounts = accounts_df.copy()
        accounts[open_col] = pd.to_datetime(accounts[open_col], errors='coerce')
        accounts = accounts.dropna(subset=[open_col, id_col])
        
        if logins_df is None or logins_df.empty:
            return {
                'has_login_data': False,
                'same_day_logins': 0,
                'delayed_logins': 0,
                'never_logged_in': len(accounts),
                'avg_delay_days': None,
                'engagement_score': 'No Login Data',
                'engagement_story': f'{len(accounts)} accounts created but no login data available.'
            }
        
        # Get first logins
        first_logins = self.detect_first_login(logins_df, login_col, id_col)
        
        if first_logins.empty:
            return {
                'has_login_data': False,
                'same_day_logins': 0,
                'delayed_logins': 0,
                'never_logged_in': len(accounts),
                'engagement_story': 'No valid login timestamps found.'
            }
        
        # Merge
        merged = accounts.merge(first_logins, on=id_col, how='left')
        merged['login_delay_days'] = (merged['first_login_time'] - merged[open_col]).dt.days
        
        # Classifications
        same_day = len(merged[merged['login_delay_days'] == 0])
        delayed = len(merged[(merged['login_delay_days'] > 0) & (merged['login_delay_days'].notna())])
        never = len(merged[merged['login_delay_days'].isna()])
        total = len(merged)
        
        # Stats
        valid_delays = merged['login_delay_days'].dropna()
        avg_delay = valid_delays.mean() if len(valid_delays) > 0 else 0
        
        # Score
        same_day_pct = (same_day / total * 100) if total > 0 else 0
        if same_day_pct > 60:
            score = "Excellent Engagement"
        elif same_day_pct > 40:
            score = "Good Engagement"
        elif same_day_pct > 20:
            score = "Moderate Engagement"
        else:
            score = "Low Engagement"
        
        # Story
        story = f"Out of {total} accounts: <strong>{same_day} ({same_day_pct:.0f}%)</strong> logged in same day, "
        story += f"<strong>{delayed}</strong> logged in later (avg {avg_delay:.1f} days), "
        story += f"<strong>{never}</strong> never logged in."
        
        return {
            'has_login_data': True,
            'same_day_logins': same_day,
            'delayed_logins': delayed,
            'never_logged_in': never,
            'avg_delay_days': round(avg_delay, 1),
            'engagement_score': score,
            'engagement_story': story,
            'total_accounts': total,
            'login_column': login_col
        }
    
    def analyze_daily_logins_with_account_age(
        self,
        accounts_df: pd.DataFrame,
        logins_df: pd.DataFrame,
        open_col: str,
        login_col: str,
        link_col: str,
    ) -> Dict[str, Any]:
        """
        Sort logins by timestamp, group by date, count per day.
        Join with accounts to get created_at. For each login day: New (≤30 days) vs Old (>365 days) account logins.
        Returns table + story for non-technical users.
        """
        if accounts_df is None or accounts_df.empty or logins_df is None or logins_df.empty:
            return {"has_data": False, "daily": [], "table": [], "story": "No login data available.", "brief": "No daily login analysis."}

        acc = accounts_df.copy()
        acc[open_col] = pd.to_datetime(acc[open_col], errors="coerce")
        acc = acc.dropna(subset=[open_col, link_col])

        log = logins_df.copy()
        log[login_col] = pd.to_datetime(log[login_col], errors="coerce")
        log = log.dropna(subset=[login_col, link_col])
        log = log.sort_values(login_col, ascending=True)

        acc_sub = acc[[link_col, open_col]].drop_duplicates(subset=[link_col]).rename(columns={open_col: "_created_at"})
        merged = log.merge(acc_sub, on=link_col, how="left")
        merged["_open_dt"] = pd.to_datetime(merged["_created_at"], errors="coerce")
        merged = merged.dropna(subset=["_open_dt"])
        merged["login_date"] = merged[login_col].dt.date
        merged["age_at_login_days"] = (merged[login_col] - merged["_open_dt"]).dt.days
        merged["is_new"] = merged["age_at_login_days"] <= 30
        merged["is_old"] = merged["age_at_login_days"] > 365

        by_date = merged.groupby("login_date").agg(
            login_count=("age_at_login_days", "count"),
            new_logins=("is_new", "sum"),
            old_logins=("is_old", "sum"),
        ).reset_index()
        by_date["login_date_str"] = by_date["login_date"].astype(str)
        by_date = by_date.sort_values("login_date").reset_index(drop=True)

        def _time_of_day(hour: int) -> str:
            if 5 <= hour < 12: return "Morning"
            if 12 <= hour < 17: return "Afternoon"
            if 17 <= hour < 21: return "Evening"
            return "Night"

        daily = []
        for _, row in by_date.iterrows():
            d = row["login_date_str"]
            total = int(row["login_count"])
            new = int(row["new_logins"])
            old = int(row["old_logins"])
            active = total - new - old
            day_merged = merged[merged["login_date"].astype(str) == d].sort_values(login_col)
            acc_counts = {}
            for _, r in day_merged.iterrows():
                aid = str(r[link_col]) if link_col in r else ""
                acc_counts[aid] = acc_counts.get(aid, 0) + 1
            multi_login_accounts = [a for a, c in acc_counts.items() if c > 1]
            multi_login_same_day = len(multi_login_accounts) > 0

            logins_list = []
            for _, r in day_merged.iterrows():
                login_dt = r[login_col]
                hour = login_dt.hour if pd.notna(login_dt) and hasattr(login_dt, 'hour') else 12
                time_str = login_dt.strftime("%H:%M") if pd.notna(login_dt) and hasattr(login_dt, 'strftime') else ""
                time_of_day = _time_of_day(hour)
                created_str = r["_open_dt"].strftime("%Y-%m-%d") if pd.notna(r["_open_dt"]) else ""
                login_at_str = login_dt.strftime("%Y-%m-%d %H:%M") if pd.notna(login_dt) and hasattr(login_dt, 'strftime') else ""
                aid = str(r[link_col]) if link_col in r else ""
                is_new = bool(r.get("is_new", r["age_at_login_days"] <= 30))
                logins_list.append({
                    "account_id": aid,
                    "login_at": login_at_str,
                    "time_str": time_str,
                    "time_of_day": time_of_day,
                    "created_at": created_str,
                    "is_new": is_new,
                })
            brief = f"On {d}: {total} login(s). New: {new}, Old: {old}."
            if multi_login_same_day:
                brief += f" One user logged in 2+ times: {', '.join(multi_login_accounts)}."
            full = f"Date {d}: {total} logins. {new} new accounts (≤30 days), {old} old (>1 year)."
            if multi_login_same_day:
                full += f" Same day multi-login: {', '.join(multi_login_accounts)} logged in 2+ times on this day."
            full += " Each login shows: this user, this time, morning/afternoon/evening/night."
            daily.append({
                "date": d,
                "login_count": total,
                "new_account_logins": new,
                "old_account_logins": old,
                "active_account_logins": active,
                "multi_login_same_day": multi_login_same_day,
                "multi_login_accounts": multi_login_accounts,
                "brief_explanation": brief,
                "full_explanation": full,
                "logins": logins_list,
            })

        table = [{"Date": d["date"], "Login Count": d["login_count"], "New Accounts Logins": d["new_account_logins"], "Old Accounts Logins": d["old_account_logins"]} for d in daily]
        table_detail = []
        for d in daily:
            for lg in d.get("logins", []):
                table_detail.append({
                    "Date": d["date"],
                    "Account": lg.get("account_id", ""),
                    "Login Time": lg.get("login_at", ""),
                    "Time of Day": lg.get("time_of_day", ""),
                    "Account Created": lg.get("created_at", ""),
                })

        total_logins = merged.shape[0]
        total_new = int(merged["is_new"].sum())
        total_old = int(merged["is_old"].sum())
        first_date = by_date["login_date_str"].iloc[0] if len(by_date) > 0 else ""
        last_date = by_date["login_date_str"].iloc[-1] if len(by_date) > 0 else ""

        story = (
            f"📖 <strong>Simple story:</strong> We took the <strong>{login_col}</strong> column (when users logged in) and the <strong>{open_col}</strong> column (when accounts were created). "
            f"From {first_date} to {last_date}, there were <strong>{total_logins}</strong> total logins. "
            f"<strong>{total_new}</strong> came from new users (account created in last 30 days), <strong>{total_old}</strong> from old users (>1 year). "
            f"This tells you: are people logging in right after signing up, or are long-term customers more active?"
        )

        brief = f"From {first_date} to {last_date}: {total_logins} logins total. Column {login_col} = login time, {open_col} = when account was created. Each day: New vs Old."

        full_explanation = (
            f"<strong>For beginners:</strong> We use two columns — <strong>{login_col}</strong> (when the user logged in) and <strong>{open_col}</strong> (when the account was created). "
            f"We sorted all logins by time, grouped by day, and counted. For each login we found the account and checked: was it created recently (≤30 days) or long ago (>1 year)? "
            f"<strong>Stats:</strong> {total_logins} logins from {first_date} to {last_date}. {total_new} from new accounts, {total_old} from old. "
            "This helps you understand: are new signups engaging, or are existing customers driving activity?"
        )

        return {
            "has_data": True,
            "daily": daily,
            "table": table,
            "table_detail": table_detail,
            "story": story,
            "brief": brief,
            "full_explanation": full_explanation,
            "open_column": open_col,
            "login_column": login_col,
            "first_date": first_date,
            "last_date": last_date,
            "total_logins": total_logins,
            "total_new_logins": total_new,
            "total_old_logins": total_old,
        }

    def classify_user_engagement(self, account_age_days: float, login_delay_days: Optional[float]) -> Dict[str, str]:
        """Classify user based on account age and login behavior."""
        # Never logged in
        if pd.isna(login_delay_days):
            if account_age_days <= 7:
                return {'category': 'Inactive New User', 'meaning': 'Created recently, never logged in', 'action': 'Send activation reminder', 'color': '#EF4444'}
            else:
                return {'category': 'Dormant Account', 'meaning': 'Old account, no login activity', 'action': 'Re-engagement campaign', 'color': '#F59E0B'}
        
        # Same day login
        if login_delay_days == 0:
            if account_age_days > 365:
                return {'category': 'Trusted Active User', 'meaning': 'Long-term user, immediate adoption', 'action': 'Offer premium features', 'color': '#10B981'}
            else:
                return {'category': 'High Intent User', 'meaning': 'Logged in immediately', 'action': 'Guide through features', 'color': '#3B82F6'}
        
        # Delayed login
        if login_delay_days <= 7:
            return {'category': 'Moderate Engagement', 'meaning': 'Logged in within a week', 'action': 'Monitor activity', 'color': '#8B5CF6'}
        elif login_delay_days <= 30:
            return {'category': 'Low Engagement', 'meaning': 'Delayed first login', 'action': 'Send engagement content', 'color': '#F59E0B'}
        else:
            return {'category': 'Very Low Engagement', 'meaning': 'Very late first login', 'action': 'Understand barriers', 'color': '#EF4444'}

    def analyze_cluster(self, tables: List[Any], dataframes: Dict[str, pd.DataFrame], 
                        relationships: List[Any], credit_analysis: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """Find login data in cluster and correlate with existing transaction analysis"""
        
        login_df = None
        time_col = None
        id_col = None
        table_name = ""

        # Find a table with login keywords
        for table in tables:
            df = dataframes.get(table.table_name)
            if df is None: continue
            
            # Heuristic: Table name or columns must suggest logins
            is_login_table = any(syn in table.table_name.lower() for syn in self.LOGIN_SYNONYMS)
            
            t_col = self._find_best_column(df, self.TIME_SYNONYMS)
            i_col = self._find_best_column(df, self.ID_SYNONYMS)
            
            if t_col and (is_login_table or 'login' in t_col.lower()):
                login_df = df
                time_col = t_col
                id_col = i_col # Optional but good
                table_name = table.table_name
                break
        
        if login_df is None:
            pass # We will attempt fallback after finding transactions below

        # Find transaction dataframe for correlation
        trans_df = None
        trans_cols = {}
        for table in tables:
             # Basic check for transaction table characteristics
             t_name = table.table_name.lower()
             if any(k in t_name for k in ['trans', 'trx', 'ledg', 'bill', 'state', 'depo', 'withdr', 'activity', 'finance']):
                 df = dataframes.get(table.table_name)
                 if df is not None:
                     # Find key columns
                     amount_col = self._find_best_column(df, ['amount', 'price', 'value'])
                     date_col = self._find_best_column(df, ['date', 'time', 'timestamp'])
                     
                     # Find ALL potential ID columns in transaction table
                     # We want to match against WHATEVER ID we have in the login table
                     possible_ids = []
                     for col in df.columns:
                         if any(syn in col.lower() for syn in self.ID_SYNONYMS):
                             possible_ids.append(col)

                     if amount_col and date_col:
                         trans_df = df
                         trans_cols = {'amount': amount_col, 'date': date_col, 'ids': possible_ids}
                         break

        is_fallback = False
        if login_df is None:
            # FALLBACK: If no explicit login table found, use the transaction table as the event source
            # This ensures the "Login Workflow" page (unified view) still appears if there is data.
            if trans_df is not None:
                login_df = trans_df
                time_col = trans_cols['date']
                id_col = trans_cols['ids'][0] if trans_cols['ids'] else None
                table_name = "Transaction Logs (Activity)"
                is_fallback = True
            else:
                return None

        return self.perform_analysis(login_df, time_col, id_col, table_name, trans_df, trans_cols, is_fallback)

    def perform_analysis(self, df: pd.DataFrame, time_col: str, id_col: str, 
                         table_name: str, trans_df: pd.DataFrame = None, trans_cols: Dict = None,
                         is_fallback: bool = False) -> Dict[str, Any]:
        """Perform granular timeline analysis and correlation with extended window"""
        
        working_df = df.copy()
        
        # Parse time
        working_df['__parsed_time'] = pd.to_datetime(working_df[time_col], errors='coerce')
        valid_df = working_df[working_df['__parsed_time'].notna()].copy()
        
        if len(valid_df) == 0:
            return None
            
        # Parse transaction time if available
        if trans_df is not None:
            trans_df['__parsed_time'] = pd.to_datetime(trans_df[trans_cols['date']], errors='coerce')
        
        # Build Timeline
        timeline = []
        
        # 1. Pre-calculate enriched transaction data using FuzzyAnalyzer (Once per analysis)
        enriched_txns = []
        if trans_df is not None:
            from fuzzy_analyzer import FuzzyAnalyzer
            f_analyzer = FuzzyAnalyzer()
            analyzed_trans = f_analyzer.analyze_transactions(trans_df)
            if analyzed_trans.get('success'):
                enriched_txns = analyzed_trans['transactions']

        # Build Timeline
        timeline = []
        valid_df = valid_df.sort_values('__parsed_time', ascending=True)
        
        for idx, row in valid_df.iterrows():
            login_time = row['__parsed_time']
            user_id = str(row[id_col]) if id_col and pd.notna(row[id_col]) else "Unknown User"
            
            formatted_date = login_time.strftime('%Y-%m-%d')
            formatted_time = login_time.strftime('%H:%M:%S')
            
            matched_txns_text = []
            if trans_df is not None:
                # Window: -1h to +24h
                window_start = login_time - pd.Timedelta(hours=1)
                window_end = login_time + pd.Timedelta(hours=24)
                
                # Filter trans_df slice for this login to get the time window correct
                time_mask = (trans_df['__parsed_time'] >= window_start) & (trans_df['__parsed_time'] <= window_end)
                id_mask = pd.Series([False] * len(trans_df))
                
                if id_col and trans_cols.get('ids'):
                    for t_id_col in trans_cols['ids']:
                         # Use robust ID matching
                         match_condition = trans_df[t_id_col].apply(lambda x: self._ids_match(x, user_id))
                         id_mask = id_mask | match_condition
                
                day_txns = trans_df[time_mask & id_mask]
                
                # Match these raw transactions to our enriched records to get Balance/Type info
                session_events = []
                for _, raw_txn in day_txns.iterrows():
                    raw_val = pd.to_numeric(raw_txn[trans_cols['amount']], errors='coerce') or 0
                    raw_date_str = raw_txn['__parsed_time'].strftime('%Y-%m-%d')
                    
                    found_enriched = None
                    for e_txn in enriched_txns:
                        # Match: ID + Date + Amount (approx)
                        # Use 'true_amount' for precise matching even if impact was 0 (Declined)
                        enriched_val = float(e_txn.get('true_amount', 0))
                        
                        if (self._ids_match(e_txn['account'], user_id) and 
                            e_txn['date'] == raw_date_str and 
                            abs(abs(enriched_val) - abs(raw_val)) < 0.01):
                            found_enriched = e_txn
                            session_events.append(e_txn)
                            break
                    
                    if found_enriched:
                        t_type = found_enriched['type'].title()
                        val_str = found_enriched['amount']
                        bal_msg = f"(Bal: <b>{found_enriched['balance_before']}</b> <span style='opacity:0.5'>→</span> <b>{found_enriched['balance_after']}</b>)"
                        emoji = "💰" if "Credit" in t_type else ("⬇️" if "Debit" in t_type else ("🚫" if "Declined" in t_type else "♻️"))

                        # Beginner-Friendly Narrative Construction
                        narrative = ""
                        if "Credit" in t_type or "Deposit" in t_type:
                            narrative = "Money In (Balance Increased)"
                        elif "Debit" in t_type or "Withdrawal" in t_type:
                            narrative = "Money Out (Balance Decreased)"
                        elif "Refund" in t_type:
                            narrative = "Money Returned (Balance Restored)"
                        elif "Declined" in t_type:
                            narrative = "Blocked: Insufficient Funds. Transaction stopped to prevent account from going below zero."

                        # Include status meaning or rule if interesting
                        rule_text = found_enriched.get('rule', '')
                        status_suffix = ""
                        if "Declined" in t_type:
                            status_suffix = f" [{found_enriched['meaning']}]"
                        elif rule_text and rule_text != "Normal transaction":
                            status_suffix = f" [{rule_text}]"
                            
                        # Final Label: narrative + tech details
                        badge_label = f"{t_type} of {val_str} {bal_msg}{status_suffix} {emoji}"
                        matched_txns_text.append(f"<strong>{narrative}</strong><br>{badge_label}")
                    else:
                        matched_txns_text.append(f"<strong>Transaction Detected</strong><br>Value: {abs(raw_val):,.2f} 📝")
            
            if matched_txns_text:
                 txn_count = len(matched_txns_text)
                 display_txns = matched_txns_text[:5]
                 if txn_count > 5:
                     display_txns.append(f"+{txn_count-5} more")
                 
                # --- ENHANCED: Transaction Stats & detailed Theory (Using Structured Data) ---
                 credit_count = 0
                 debit_count = 0
                 refund_count = 0
                 declined_count = 0
                 total_credit = 0.0
                 total_debit = 0.0
                 
                 refund_explanations = []
                 negative_rule_explanations = []

                 # Process the captured session events (enriched objects)
                 for txn in session_events:
                     t_type = txn.get('type', '').upper()
                     amt = 0.0
                     try:
                         amt = float(txn.get('true_amount', 0))
                     except: pass

                     if 'CREDIT' in t_type or 'DEPOSIT' in t_type:
                         credit_count += 1
                         total_credit += amt
                     elif 'DEBIT' in t_type or 'WITHDRAWAL' in t_type:
                         debit_count += 1
                         total_debit += amt
                     elif 'REFUND' in t_type:
                         refund_count += 1
                         total_credit += amt
                         # Capture detailed refund info
                         # txn['date'] is just YYYY-MM-DD, see if we can get time from original match or just use date
                         # The enriched object doesn't have time, but we are inside the loop where we matched it.
                         # Actually, we are outside the loop now. 
                         # Let's rely on what we have. enriched 'explanation' usually has date.
                         # We'll formatting: "Refund of X detected. Balance restored to Y."
                         refund_explanations.append(f"• Refund of <strong>{txn.get('amount', '0')}</strong> received (Balance before: {txn.get('balance_before', 'N/A')}).")
                     
                     elif 'DECLINED' in t_type:
                         declined_count += 1
                         # Capture negative balance rule info
                         rule_text = txn.get('rule_effect', 'Rule triggered')
                         negative_rule_explanations.append(f"• {rule_text}")

                 net_flow = total_credit - total_debit
                 
                 # Theory Formulation
                 theories = []
                 balance_reason = ""
                 
                 # 1. Behavior Theory
                 if declined_count > 0:
                     theories.append(f"🔴 <strong>High Risk / Distress:</strong> {declined_count} transaction(s) declined.<br>" + "<br>".join(negative_rule_explanations))
                 
                 if refund_count > 0:
                     theories.append(f"↺ <strong>Correction Phase:</strong><br>" + "<br>".join(refund_explanations))
                 
                 if credit_count > 0 and debit_count > 0:
                     theories.append("🔄 <strong>Active Management:</strong> User is actively rotating funds (both income and spending detected).")
                 
                 # 2. Balance Impact Reason
                 if net_flow > 0:
                     balance_reason = f"Balance <strong>INCREASED</strong> by {net_flow:,.2f} due to deposits/refunds exceeding outflows."
                 elif net_flow < 0:
                     balance_reason = f"Balance <strong>DECREASED</strong> by {abs(net_flow):,.2f} due to spending exceeding income."
                 else:
                     balance_reason = "Balance remained <strong>NEUTRAL</strong> (No net change or exact offset)."

                 # 3. Construct Narrative
                 theory_html = "<br>".join(theories) if theories else "👁️ <strong>Monitoring Only:</strong> No significant patterns detected."
                 
                 explanation = (
                     f"<strong>Session Insights:</strong> "
                     f"<span style='color:#10b981'>{credit_count} Credits</span>, "
                     f"<span style='color:#ef4444'>{debit_count} Debits</span>, "
                     f"<span style='color:#f59e0b'>{refund_count} Refunds</span>, "
                     f"<span style='color:#fe8a6a'>{declined_count} Declined</span>.<br>"
                     f"<strong>Data Theory Observed:</strong><br>{theory_html}<br>"
                     f"<strong>Net Flow:</strong> {balance_reason}<br>"
                     f"<hr style='border:0; border-top:1px solid rgba(255,255,255,0.1); margin:0.5rem 0;'>"
                     f"Events: " + " | ".join(display_txns)
                 )
            else:
                 explanation = (
                     "<strong>Session Activity:</strong> Standard account access.<br>"
                     "<strong>Data Theory Observed:</strong> 👁️ <strong>Monitoring Only:</strong> User logged in but performed no financial actions.<br>"
                     "<strong>Net Flow:</strong> No change."
                 )
            
            source_info = f"Source: {table_name}.{time_col}"


            timeline.append({
                'date': formatted_date,
                'time': formatted_time,
                'user': user_id,
                'event': 'Financial Activity' if is_fallback else 'Login / Authentication',
                'explanation': explanation,
                'source': source_info
            })
            
        return {
            'timeline': timeline,
            'analyzed_table': table_name,
            'time_column': time_col,
            'is_fallback': is_fallback,
            'summary': f"Processed {len(timeline)} events. " + 
                       ("This timeline uses Transaction data as the primary event source." if is_fallback else "Timeline correlates login sessions with financial transactions.")
        }
