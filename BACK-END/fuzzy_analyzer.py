import pandas as pd
from typing import Dict, Any, List, Optional, Tuple

class FuzzyAnalyzer:
    """
    Analyzes numerical distributions using fuzzy logic thresholds 
    to create business-relevant segments.
    """
    
    def __init__(self):
        # Define thresholds for 'balance' (could be configurable in future)
        self.THRESHOLD_LOW = 5000
        self.THRESHOLD_HIGH = 20000

    def analyze_balance_distribution(self, df: pd.DataFrame, balance_col: str, id_col: str) -> Dict[str, Any]:
        """
        Segment customers based on balance column.
        
        Returns:
            Dict containing counts, segments (sample data), and insights.
        """
        # Ensure numeric
        df[balance_col] = pd.to_numeric(df[balance_col], errors='coerce').fillna(0)
        
        # Segments
        low_segment = df[df[balance_col] < self.THRESHOLD_LOW]
        medium_segment = df[(df[balance_col] >= self.THRESHOLD_LOW) & (df[balance_col] < self.THRESHOLD_HIGH)]
        high_segment = df[df[balance_col] >= self.THRESHOLD_HIGH]
        
        # Prepare response data (limit rows for UI performance)
        def inspect_segment(segment_df, label):
            records = segment_df[[id_col, balance_col]].head(50).to_dict('records')
            for r in records:
                r['segment'] = label
            return records

        return {
            "counts": {
                "LOW": len(low_segment),
                "MEDIUM": len(medium_segment),
                "HIGH": len(high_segment)
            },
            "segments": {
                "LOW": inspect_segment(low_segment, "LOW"),
                "MEDIUM": inspect_segment(medium_segment, "MEDIUM"),
                "HIGH": inspect_segment(high_segment, "HIGH")
            },
            "insights": {
                "LOW": {
                    "label": "Low Balance",
                    "action": "Send Low Balance Alert",
                    "description": "Customers with < 5,000 balance. Monitor for potential churn or overdraft risks."
                },
                "MEDIUM": {
                    "label": "Medium Balance",
                    "action": "Standard Engagement",
                    "description": "Customers with 5,000 - 20,000 balance. Target for savings plans and credit card offers."
                },
                "HIGH": {
                    "label": "High Balance",
                    "action": "Premium Cross-Sell",
                    "description": "Customers with > 20,000 balance. VIP segment suitable for investment products and premium services."
                }
            }
        }

    def analyze_account_age(
        self, 
        df: pd.DataFrame, 
        date_col: str, 
        id_col: str,
        login_metrics: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Segment accounts based on age (Time since open_date) and provide a narrative.
        """
        # Ensure datetime
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        now = pd.Timestamp.now()
        
        # 1. Sort by Date Ascending
        df = df.sort_values(by=date_col, ascending=True)
        
        # Calculate Age in Days
        df['__age_days'] = (now - df[date_col]).dt.days.fillna(-1)
        
        # Segments
        new_segment = df[(df['__age_days'] >= 0) & (df['__age_days'] <= 30)]
        active_segment = df[(df['__age_days'] > 30) & (df['__age_days'] <= 365)]
        trusted_segment = df[df['__age_days'] > 365]
        
        # 2. Daily Trends & Stats
        daily_counts = df.groupby(df[date_col].dt.date).size().reset_index(name='count')
        daily_counts = daily_counts.sort_values(by=date_col)
        
        total_accounts = len(df)
        first_date = df[date_col].min().strftime('%d %B %Y') if not df.empty else "N/A"
        last_date = df[date_col].max().strftime('%d %B %Y') if not df.empty else "N/A"
        
        peak_day = daily_counts.loc[daily_counts['count'].idxmax()] if not daily_counts.empty else None
        peak_date_str = peak_day[date_col].strftime('%d %B %Y') if peak_day is not None else "N/A"
        peak_count = int(peak_day['count']) if peak_day is not None else 0
        
        # Daily / monthly growth: count accounts per day and per month
        total_days = (df[date_col].max() - df[date_col].min()).days if not df.empty and len(df) > 1 else 1
        total_days = max(1, total_days)
        accounts_per_day = round(total_accounts / total_days, 2) if total_days else 0
        monthly_counts = df.groupby(df[date_col].dt.to_period('M')).size()
        total_months = max(1, len(monthly_counts))
        accounts_per_month = round(total_accounts / total_months, 2) if total_months else 0
        
        # Summary of daily counts for UI (first 31 days or all if fewer)
        daily_counts_list = []
        if not daily_counts.empty:
            date_key = daily_counts.columns[0]
            for _, r in daily_counts.head(31).iterrows():
                d = r[date_key]
                daily_counts_list.append({
                    "date": d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d),
                    "count": int(r['count'])
                })
        
        # 3. Multi-Account Detection
        id_counts = df[id_col].value_counts()
        multi_account_holders = id_counts[id_counts > 1]
        multi_acc_count = len(multi_account_holders)
        
        # 4. Narrative Generation (The "Story")
        narrative_steps = []
        
        # Step 1: Volume
        narrative_steps.append({
            "title": "Total Volume",
            "icon": "📊",
            "text": f"We found a total of <strong>{total_accounts}</strong> accounts in this dataset."
        })
        
        # Step 2: Timeline
        narrative_steps.append({
            "title": "Timeline",
            "icon": "📅",
            "text": f"The first account was opened on <strong>{first_date}</strong>. The most recent one was on <strong>{last_date}</strong>."
        })
        
        # Step 3: Peak Indication
        if peak_count > 1:
            narrative_steps.append({
                "title": "Peak Activity",
                "icon": "🚀",
                "text": f"We noticed a spike on <strong>{peak_date_str}</strong>, where <strong>{peak_count}</strong> accounts were opened in a single day."
            })
            
        # Step 4: Multi-Account Insight
        if multi_acc_count > 0:
            sample_ids = ", ".join([str(x) for x in multi_account_holders.head(3).index.tolist()])
            narrative_steps.append({
                "title": "Multiple Accounts",
                "icon": "👥",
                "text": f"<strong>{multi_acc_count}</strong> customers have more than one account (e.g., {sample_ids}). This usually indicates users managing different pools of money (Savings vs Expenses)."
            })
        else:
             narrative_steps.append({
                "title": "Single Accounts",
                "icon": "👤",
                "text": "Every customer currently has exactly one account. No duplicates were found."
            })
            
        # Step 5: Login / Engagement Insight (NEW)
        if login_metrics and login_metrics.get('has_login_data'):
            score = login_metrics.get('engagement_score', 'N/A')
            story = login_metrics.get('engagement_story', '')
            
            # Determine icon based on score
            icon = "⚡" if "Excellent" in score else ("📉" if "Low" in score else "📱")
            
            narrative_steps.append({
                "title": f"Engagement: {score}",
                "icon": icon,
                "text": story
            })
            
        def inspect_segment(segment_df, label):
            data = segment_df[[id_col, date_col, '__age_days']].head(100).copy() # increase limit for sorting
            data[date_col] = data[date_col].dt.strftime('%Y-%m-%d')
            records = data.to_dict('records')
            # Inject meaning and ensure JSON-serializable (no numpy types)
            for r in records:
                if '__age_days' in r and r['__age_days'] is not None:
                    r['__age_days'] = int(r['__age_days'])
                r['group'] = label
                if label == 'NEW':
                    r['meaning'] = "New customer – monitor closely"
                    r['action'] = "Welcome Onboarding"
                elif label == 'ACTIVE':
                    r['meaning'] = "Regular customer – normal operations"
                    r['action'] = "Regular Engagement"
                else:
                    r['meaning'] = "Trusted long-term customer"
                    r['action'] = "Loyalty Rewards"
            return records

        # Return comprehensive structure (include date strings for frontend explanations)
        return {
            "counts": {
                "NEW": len(new_segment),
                "ACTIVE": len(active_segment),
                "TRUSTED": len(trusted_segment)
            },
            "segments": {
                "NEW": inspect_segment(new_segment, 'NEW'),
                "ACTIVE": inspect_segment(active_segment, 'ACTIVE'),
                "TRUSTED": inspect_segment(trusted_segment, 'TRUSTED')
            },
            "narrative_steps": narrative_steps,
            "insights": {
                "NEW": "Recent accounts opened in the last 30 days.",
                "ACTIVE": "Established accounts active for 1 month to 1 year.",
                "TRUSTED": "Loyal accounts open for more than 1 year."
            },
            "first_date_str": first_date,
            "last_date_str": last_date,
            "peak_date_str": peak_date_str,
            "peak_count": int(peak_count),
            "total_accounts": int(total_accounts),
            "growth_summary": {
                "total_days": int(total_days),
                "total_months": int(total_months),
                "accounts_per_day": float(accounts_per_day),
                "accounts_per_month": float(accounts_per_month),
                "daily_counts": daily_counts_list
            }
        }
    
    def detect_same_day_accounts(
        self,
        df: pd.DataFrame,
        date_col: str,
        id_col: str,
        include_timestamps: bool = True
    ) -> Dict[str, Any]:
        """
        Find customers who created multiple accounts on the same day.
        If timestamp exists, sort by time to show order of creation.
        
        Returns:
            Dict with:
                - same_day_customers: List of {customer_id, date, count, timestamps}
                - total_affected: int
                - explanation: str
        """
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        df = df.dropna(subset=[date_col, id_col])
        
        # Extract date only (no time)
        df['__date_only'] = df[date_col].dt.date
        
        # Group by customer ID and date
        grouped = df.groupby([id_col, '__date_only']).agg({
            date_col: ['count', 'min', 'max', list]
        }).reset_index()
        
        grouped.columns = [id_col, 'date', 'account_count', 'first_time', 'last_time', 'all_timestamps']
        
        # Filter only those with multiple accounts on same day
        same_day = grouped[grouped['account_count'] > 1].copy()
        
        if same_day.empty:
            return {
                'same_day_customers': [],
                'total_affected': 0,
                'explanation': "No customers created multiple accounts on the same day. Each customer opened one account per day.",
                'insight': "✓ Clean data - one account per customer per day",
                'brief_explanation': "No same-day multi-account users found.",
                'full_explanation': "We checked each customer and each date. No customer created more than one account on the same day. This means one account per person per day — clean data.",
            }
        
        # Sort by account count (descending) then by date
        same_day = same_day.sort_values(['account_count', 'date'], ascending=[False, False])
        
        results = []
        for _, row in same_day.head(50).iterrows():  # Limit to top 50
            timestamps = [pd.Timestamp(ts) for ts in row['all_timestamps']]
            timestamps_sorted = sorted(timestamps)
            
            results.append({
                'customer_id': str(row[id_col]),
                'date': str(row['date']),
                'account_count': int(row['account_count']),
                'timestamps': [ts.strftime('%H:%M:%S') for ts in timestamps_sorted] if include_timestamps else [],
                'explanation': f"Created {int(row['account_count'])} accounts on {row['date']}",
                'suspicious': int(row['account_count']) > 3  # Flag if more than 3 accounts
            })
        
        total = len(same_day)
        
        return {
            'same_day_customers': results,
            'total_affected': total,
            'explanation': f"Found {total} customers who created multiple accounts on the same day. This could indicate data issues or legitimate business needs (e.g., savings + current account).",
            'insight': f"⚠️ {total} customers with multiple same-day accounts",
            'brief_explanation': f"{total} customer(s) opened 2+ accounts on the same day.",
            'full_explanation': (
                f"We grouped your data by customer and date. {total} customer(s) created multiple accounts on the same calendar day. "
                "This can be legitimate (e.g. savings + current account) or a data quality issue. "
                "Each node with 👥 on the timeline marks a date where at least one such customer exists. "
                "Review the dates and customer IDs to understand the pattern."
            ),
        }
    
    def detect_inactive_customers(
        self,
        df: pd.DataFrame,
        date_col: str,
        id_col: str,
        age_threshold_days: int = 365,
        linked_activity: Optional[pd.DataFrame] = None
    ) -> Dict[str, Any]:
        """
        Detect inactive customers: accounts older than threshold with no activity.
        
        Args:
            df: Main DataFrame with accounts
            date_col: Account creation date column
            id_col: Customer ID column
            age_threshold_days: Minimum age for consideration (default 365)
            linked_activity: Optional DataFrame with activity flags from CustomerLinker
            
        Returns:
            Dict with inactive customer list and insights
        """
        # Parse dates
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        now = pd.Timestamp.now()
        
        # Calculate age
        df['__age_days'] = (now - df[date_col]).dt.days
        
        # Filter old accounts
        old_accounts = df[df['__age_days'] > age_threshold_days].copy()
        
        if old_accounts.empty:
            return {
                'count': 0,
                'customers': [],
                'insight': f"No accounts older than {age_threshold_days} days found."
            }
        
        # If we have linked activity data, use it
        inactive_list = []
        
        if linked_activity is not None and 'has_login_activity' in linked_activity.columns:
            # Use activity flags
            for _, row in old_accounts.iterrows():
                cust_id = row[id_col]
                
                # Find in linked data
                activity_row = linked_activity[linked_activity[id_col] == cust_id]
                
                if activity_row.empty:
                    # No activity record found
                    inactive_list.append({
                        'customer_id': cust_id,
                        'account_created': row[date_col].strftime('%Y-%m-%d') if pd.notna(row[date_col]) else 'Unknown',
                        'age_days': int(row['__age_days']),
                        'reason': 'No activity records found',
                        'action': 'Re-engagement campaign'
                    })
                else:
                    activity_row = activity_row.iloc[0]
                    
                    # Check if they lack activity
                    has_login = activity_row.get('has_login_activity', False)
                    has_transaction = activity_row.get('has_transaction_activity', False)
                    
                    if not has_login and not has_transaction:
                        inactive_list.append({
                            'customer_id': cust_id,
                            'account_created': row[date_col].strftime('%Y-%m-%d') if pd.notna(row[date_col]) else 'Unknown',
                            'age_days': int(row['__age_days']),
                            'reason': 'No login or transaction activity',
                            'action': 'Priority re-engagement'
                        })
        else:
            # No activity data available - just mark old accounts as potentially inactive
            for _, row in old_accounts.head(50).iterrows():  # Limit to 50 for performance
                inactive_list.append({
                    'customer_id': row[id_col],
                    'account_created': row[date_col].strftime('%Y-%m-%d') if pd.notna(row[date_col]) else 'Unknown',
                    'age_days': int(row['__age_days']),
                    'reason': 'Account age > 1 year (activity data not linked)',
                    'action': 'Review recommended'
                })
        
        # Generate insight
        if len(inactive_list) > 0:
            pct = (len(inactive_list) / len(old_accounts)) * 100
            insight = f"Found {len(inactive_list)} potentially inactive customers out of {len(old_accounts)} old accounts ({pct:.1f}%)."
        else:
            insight = "All old accounts show activity. Great engagement!"
        
        return {
            'count': len(inactive_list),
            'customers': inactive_list,
            'total_old_accounts': len(old_accounts),
            'insight': insight
        }

    def analyze_transactions(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze transactions with fuzzy logic to detect columns, 
        group by account/day, and calculate balances.
        """
        # 1. Fuzzy Column Detection
        cols = {
            'account': None,
            'amount': None,
            'type': None,
            'date': None
        }
        
        # Helper to find column by keywords
        def find_col(keywords):
            for col in df.columns:
                if any(k in col.lower() for k in keywords):
                    return col
            return None

        cols['account'] = find_col(['account', 'cust', 'id_', 'number'])
        cols['amount'] = find_col(['amount', 'value', 'price', 'sum'])
        cols['type'] = find_col(['type', 'mode', 'method', 'category', 'desc'])
        cols['date'] = find_col(['date', 'timestamp', 'created', 'posted', 'day'])
        if not cols['date']:
            cols['date'] = find_col(['time'])
        cols['time_sep'] = None
        for c in df.columns:
            cl = c.lower()
            if c != cols['date'] and ('time' in cl or 'hour' in cl) and 'stamp' not in cl:
                cols['time_sep'] = c
                break
        cols['status'] = find_col(['status', 'result', 'outcome'])

        if not cols.get('account') or not cols.get('amount') or not cols.get('type') or not cols.get('date'):
            return {"success": False, "error": "Could not autodetect required columns (Account, Amount, Type, Date)"}

        # 2. Data Preparation
        data = df.copy()
        
        # Ensure correct types
        data[cols['amount']] = pd.to_numeric(data[cols['amount']], errors='coerce').fillna(0)
        data[cols['date']] = pd.to_datetime(data[cols['date']], errors='coerce')
        if cols['time_sep']:
            date_str = data[cols['date']].dt.strftime('%Y-%m-%d')
            time_str = data[cols['time_sep']].astype(str).str.strip()
            data['__combined_dt'] = pd.to_datetime(date_str + ' ' + time_str, errors='coerce')
            data['__combined_dt'] = data['__combined_dt'].fillna(pd.to_datetime(data[cols['date']], errors='coerce'))
            data = data.sort_values(by=[cols['account'], '__combined_dt'])
        else:
            data['__combined_dt'] = data[cols['date']]
            data = data.sort_values(by=[cols['account'], cols['date']])
        
        # Extract just the date part for grouping
        data['__date_only'] = data['__combined_dt'].dt.date

        # 3. Processing & Logic
        results = {}
        
        grouped = data.groupby(cols['account'])
        
        formatted_transactions = []
        dt_col = '__combined_dt' if '__combined_dt' in data.columns else cols['date']
        
        for account_id, group in grouped:
            running_balance = 0.0
            
            for index, row in group.iterrows():
                amount = float(row[cols['amount']])
                txn_type = str(row[cols['type']]).upper() if pd.notna(row[cols['type']]) else "UNKNOWN"
                
                # Logic: Determine impact
                impact = 0
                business_meaning = ""
                
                if 'CREDIT' in txn_type or 'DEPOSIT' in txn_type:
                    impact = amount
                    business_meaning = "Balance increased 💰"
                    txn_type = "CREDIT" # Normalize
                elif 'DEBIT' in txn_type or 'WITHDRAWAL' in txn_type:
                    impact = -amount
                    business_meaning = "Balance decreased ⬇️"
                    txn_type = "DEBIT" # Normalize
                elif 'REFUND' in txn_type:
                    impact = amount
                    business_meaning = "Refund received, balance restored ♻️"
                    txn_type = "REFUND"
                else:
                    # Fallback logic if type is unclear but amount is signed
                    if amount < 0:
                        impact = amount
                        business_meaning = "Balance decreased ⬇️"
                        txn_type = "DEBIT"
                    else:
                        impact = amount
                        business_meaning = "Balance increased 💰"
                        txn_type = "CREDIT"


                
                # Business Rules / Key Insights
                rule_insight = "Normal transaction"
                
                if txn_type == "REFUND":
                    if abs(amount) <= 2000:
                        rule_insight = "Low impact refund"
                    elif abs(amount) <= 10000:
                        rule_insight = "Medium impact refund"
                    else:
                        rule_insight = "High impact refund"
                elif abs(amount) > 10000:
                    rule_insight = "High impact 💰"

                # --- NEW: Negative Balance Rule ---
                balance_before = running_balance
                potential_balance = running_balance + impact
                rule_created_declined = False
                rule_effect_text = "No rule triggered"

                if potential_balance < 0:
                     # RULE TRIGGERED: Do not apply transaction
                     rule_created_declined = True
                     balance_after = running_balance # Balance stays same
                     rule_effect_text = f"⚠ Rule Alert: {txn_type.title()} of {abs(amount):,.0f} not applied. Would make balance negative."
                     impact = 0 # Nullify impact
                     business_meaning = "Transaction declined 🚫 (Insufficient Funds)"
                     txn_type = "DECLINED"
                else:
                     # Normal application
                     running_balance += impact
                     balance_after = running_balance

                # Dynamic Explanation construction
                if txn_type == "DECLINED":
                    explanation = "Debit attempt failed because balance was insufficient. Balance remains unchanged."
                else:
                    action_verb = "credited" if txn_type == "CREDIT" else ("debited" if txn_type == "DEBIT" else "refunded")
                    balance_direction_text = f"{balance_before:,.0f} → {balance_after:,.0f}"
                    
                    context_text = "This is a normal transaction."
                    if txn_type == "REFUND":
                        # "Why this refund is low/medium/high impact"
                        impact_level = rule_insight.replace(" refund", "") # "Low impact"
                        context_text = f"This is a {impact_level} refund."
                    elif "High impact" in rule_insight:
                        context_text = f"High impact because {action_verb} > 10,000."
                    
                    explanation = (
                        f"{account_id} {action_verb} {abs(amount):,.0f} on {row['__date_only']}. "
                        f"Balance went from {balance_direction_text}. {context_text}"
                    )
                    
                # Time part: use time_sep column (HH:MM or HH:MM:SS) or datetime
                if cols.get('time_sep') and cols['time_sep'] in row:
                    tv = row[cols['time_sep']]
                    if pd.notna(tv):
                        tv_str = str(tv).strip()
                        if ':' in tv_str and len(tv_str) <= 8:
                            time_str = tv_str if len(tv_str) >= 5 else (tv_str + ":00")
                        else:
                            time_str = "—"
                    else:
                        time_str = "—"
                else:
                    dt_val = row.get(dt_col) or row.get(cols['date'])
                    time_str = dt_val.strftime('%H:%M:%S') if pd.notna(dt_val) and hasattr(dt_val, 'strftime') else "—"

                # Status: SUCCESS → PASS; FAILED / DECLINED / BLOCKED → FAIL (use column if present)
                if cols.get('status') and cols['status'] in row:
                    raw_s = str(row[cols['status']]).upper().strip()
                    status = "FAIL" if raw_s in ('FAILED', 'DECLINED', 'BLOCKED', 'REJECTED') else "PASS"
                else:
                    status = "FAIL" if txn_type == "DECLINED" else "PASS"
                status_explanation = "FAIL — Transaction declined or blocked (insufficient balance)" if status == "FAIL" else "PASS — Transaction completed successfully"
                formatted_transactions.append({
                    "date": row['__date_only'].strftime('%Y-%m-%d'),
                    "time": time_str,
                    "account": str(account_id),
                    "type": txn_type,
                    "amount": f"{abs(amount):,.2f}",
                    "balance_before": f"{balance_before:,.2f}",
                    "balance_after": f"{balance_after:,.2f}",
                    "meaning": business_meaning,
                    "rule": rule_insight,
                    "explanation": explanation,
                    "rule_effect": rule_effect_text,
                    "raw_amount": impact,
                    "true_amount": amount,
                    "status": status,
                    "status_explanation": status_explanation,
                })

        # 4. Final Packaging
        # We group by Account -> Day for the neat nested UI view requested
        
        # Group by Account then Date for easier UI rendering
        ui_structure = {}
        for txn in formatted_transactions:
            acc = txn['account']
            day = txn['date']
            
            if acc not in ui_structure:
                ui_structure[acc] = {}
            if day not in ui_structure[acc]:
                ui_structure[acc][day] = []
                
            ui_structure[acc][day].append(txn)

        return {
            "success": True,
            "columns_used": cols,
            "transactions": formatted_transactions, 
            "grouped_structure": ui_structure
        }

    def analyze_transaction_timeline(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Transaction timeline: START ----|-----|-----| END by date.
        Group by date, show credits/debits/refunds/blocked, per-transaction details.
        Simple banking story for non-technical users.
        """
        res = self.analyze_transactions(df)
        if not res.get("success") or not res.get("transactions"):
            return {"has_data": False, "daily": [], "table": [], "brief": "No transaction data found.", "first_date": "", "last_date": ""}

        txns = res["transactions"]
        cols = res["columns_used"]
        date_col = cols.get("date") or "date"

        # Sort by date ascending, then by time for same-day order
        def sort_key(t):
            tm = t.get("time", "—")
            if tm == "—" or not tm:
                return (t["date"], "99:99", t.get("account", ""))
            return (t["date"], tm, t.get("account", ""))
        txns_sorted = sorted(txns, key=sort_key)

        # Group by date
        by_date = {}
        for t in txns_sorted:
            d = t["date"]
            if d not in by_date:
                by_date[d] = {"transactions": [], "credits": 0, "debits": 0, "refunds": 0, "declined": 0}
            by_date[d]["transactions"].append(t)
            typ = t.get("type", "").upper()
            if typ == "CREDIT":
                by_date[d]["credits"] += 1
            elif typ == "DEBIT":
                by_date[d]["debits"] += 1
            elif typ == "REFUND":
                by_date[d]["refunds"] += 1
            elif typ == "DECLINED":
                by_date[d]["declined"] += 1

        dates_sorted = sorted(by_date.keys())
        first_date = dates_sorted[0] if dates_sorted else ""
        last_date = dates_sorted[-1] if dates_sorted else ""

        daily = []
        for d in dates_sorted:
            day_data = by_date[d]
            txn_list = day_data["transactions"]
            credits = day_data["credits"]
            debits = day_data["debits"]
            refunds = day_data["refunds"]
            declined = day_data["declined"]
            pass_count = sum(1 for t in txn_list if t.get("status") == "PASS")
            fail_count = sum(1 for t in txn_list if t.get("status") == "FAIL")

            # Same user multiple transactions on this date?
            acc_counts = {}
            for t in txn_list:
                acc = t.get("account", "")
                acc_counts[acc] = acc_counts.get(acc, 0) + 1
            multi_user_day = any(c > 1 for c in acc_counts.values())
            multi_accounts = [a for a, c in acc_counts.items() if c > 1]

            # Per-user full explanation: "Account X at time Y had balance Z. CREDIT/DEBIT amount. Balance became W."
            user_stories = []
            for t in txn_list:
                acc = t.get("account", "?")
                tm = t.get("time", "—")
                typ = t.get("type", "")
                amt = t.get("amount", "")
                bal_before = t.get("balance_before", "0")
                bal_after = t.get("balance_after", "0")
                if typ == "DECLINED":
                    story = f"<strong>Account {acc}</strong> at <strong>{tm}</strong>: Balance was {bal_before}. Debit blocked. Balance stayed {bal_before}."
                    story_plain = f"Account {acc} at {tm}: Balance was {bal_before}. Debit blocked. Balance stayed {bal_before}."
                elif typ == "CREDIT":
                    story = f"<strong>Account {acc}</strong> at <strong>{tm}</strong>: Balance {bal_before}. CREDIT {amt}. Balance became {bal_after}."
                    story_plain = f"Account {acc} at {tm}: Balance {bal_before}. CREDIT {amt}. Balance became {bal_after}."
                elif typ == "DEBIT":
                    story = f"<strong>Account {acc}</strong> at <strong>{tm}</strong>: Balance {bal_before}. DEBIT {amt}. Balance became {bal_after}."
                    story_plain = f"Account {acc} at {tm}: Balance {bal_before}. DEBIT {amt}. Balance became {bal_after}."
                elif typ == "REFUND":
                    story = f"<strong>Account {acc}</strong> at <strong>{tm}</strong>: Balance {bal_before}. REFUND {amt}. Balance became {bal_after}."
                    story_plain = f"Account {acc} at {tm}: Balance {bal_before}. REFUND {amt}. Balance became {bal_after}."
                else:
                    story = f"<strong>Account {acc}</strong> at <strong>{tm}</strong>: {t.get('explanation', '')}"
                    story_plain = f"Account {acc} at {tm}: {t.get('explanation', '')}"
                user_stories.append(story)
                t["explanation_line"] = story
                t["explanation_plain"] = story_plain

            # Build table rows with Explanation (same as diagram - per-transaction story)
            rows = []
            for t in txn_list:
                expl = t.get("explanation_plain", "") or t.get("explanation", "")
                rows.append({
                    "Date": d,
                    "Account": t.get("account", ""),
                    "Time": t.get("time", "—"),
                    "Type": t.get("type", ""),
                    "Amount": t.get("amount", ""),
                    "Balance": f"{t.get('balance_before', '')} → {t.get('balance_after', '')}",
                    "Meaning": t.get("meaning", ""),
                    "Explanation": expl,
                })

            # Full explanation: each user's story + key meanings
            line1 = f"<strong>Date {d}:</strong> {len(txn_list)} transaction(s). Credits: {credits}, Debits: {debits}, Refunds: {refunds}, Blocked: {declined}."
            line2 = "<strong>CREDIT</strong> = balance increased. <strong>DEBIT</strong> = balance decreased. <strong>REFUND</strong> = money returned. <strong>DECLINED</strong> = blocked (insufficient balance)."
            line3 = "<br><br>".join([f"• {s}" for s in user_stories])
            brief = f"On {d}: {len(txn_list)} txns. C:{credits} D:{debits} R:{refunds} B:{declined}."
            full = f"{line1}<br><br>{line2}<br><br><strong>Each transaction this date:</strong><br>{line3}"

            daily.append({
                "date": d,
                "transaction_count": len(txn_list),
                "credits": credits,
                "debits": debits,
                "refunds": refunds,
                "declined": declined,
                "pass_count": pass_count,
                "fail_count": fail_count,
                "transactions": txn_list,
                "table_rows": rows,
                "brief_explanation": brief,
                "full_explanation": full,
                "multi_user_same_day": multi_user_day,
                "multi_accounts": multi_accounts,
            })

        table = []
        for d in daily:
            for r in d["table_rows"]:
                table.append(r)

        brief = f"Transaction timeline from {first_date} to {last_date}. Grouped by date. Credits, Debits, Refunds, Blocked."
        full_explanation = (
            f"We use columns: account, amount, type, and date. Sorted from {first_date} to {last_date}. "
            "Each node = one date. CREDIT = balance increased. DEBIT = decreased. REFUND = money returned. "
            "DECLINED = blocked (insufficient balance). High value = transaction over 10,000."
        )

        return {
            "has_data": True,
            "daily": daily,
            "table": table,
            "brief": brief,
            "full_explanation": full_explanation,
            "first_date": first_date,
            "last_date": last_date,
            "total_transactions": len(txns),
            "columns_used": cols,
        }
