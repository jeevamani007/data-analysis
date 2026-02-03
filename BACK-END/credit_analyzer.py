"""
Credit Amount Time Slot Analyzer
Analyzes credit transactions by time slots and provides business insights,
supporting multi-table relationships.
"""

import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Optional
import numpy as np

# Import RelationshipInfo from models (assuming it's available or we redefine for type hinting)
# To avoid circular imports if models imports this, we'll just use Any for the annotation
# from models import RelationshipInfo 

class CreditTimeSlotAnalyzer:
    """Analyzes credit amounts grouped by time slots, handling relationships"""
    
    TIME_SLOTS = {
        'Morning': (6, 12),      # 6:00 AM - 12:00 PM
        'Afternoon': (12, 18),   # 12:00 PM - 6:00 PM
        'Evening': (18, 22),     # 6:00 PM - 10:00 PM
        'Night': (22, 6)         # 10:00 PM - 6:00 AM (wraps around)
    }
    
    # Synonyms for column detection
    CREDIT_SYNONYMS = ['credit', 'deposit', 'amount', 'transaction_amount', 'amt', 'balance', 'debit', 'withdrawal']
    TIMESTAMP_SYNONYMS = ['time', 'date', 'created_at', 'timestamp', 'txn_date', 'datetime']
    # Exclude columns that are likely not transaction times
    EXCLUDED_TIMESTAMPS = ['dob', 'birth', 'open_date', 'expiry', 'valid_until', 'last_login']
    
    ACCOUNT_SYNONYMS = ['account_number', 'account_id', 'acct_no', 'customer_id', 'user_id', 'cust_id']
    TYPE_SYNONYMS = ['type', 'txn_type', 'transaction_type', 'dr_cr', 'category', 'operation']

    def categorize_time_slot(self, hour: int) -> str:
        """Categorize an hour (0-23) into a time slot"""
        if 6 <= hour < 12:
            return 'Morning'
        elif 12 <= hour < 18:
            return 'Afternoon'
        elif 18 <= hour < 22:
            return 'Evening'
        else:  # 22-23 and 0-5
            return 'Night'
    
    def parse_timestamp(self, timestamp_str: str) -> Optional[datetime]:
        """Parse timestamp string to datetime object with multiple format support"""
        if pd.isna(timestamp_str) or str(timestamp_str).lower() in ['nan', 'nat', 'none', '']:
            return None
            
        formats = [
            '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', 
            '%d/%m/%Y %H:%M:%S', '%d/%m/%Y %H:%M',
            '%m/%d/%Y %H:%M:%S', '%m/%d/%Y %H:%M',
            '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f',
            '%d-%m-%Y %H:%M:%S', '%d-%m-%Y'
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(str(timestamp_str), fmt)
            except (ValueError, TypeError):
                continue
        
        try:
            return pd.to_datetime(timestamp_str)
        except:
            return None

    def _find_best_column(self, df: pd.DataFrame, synonyms: List[str], exclude: List[str] = None) -> Optional[str]:
        """Find the best matching column name based on synonyms, avoiding excluded terms"""
        
        # Priority 1: Exact matches or strong synonyms
        for col in df.columns:
            col_lower = col.lower()
            
            # Skip excluded
            if exclude and any(ex in col_lower for ex in exclude):
                continue
                
            if any(syn in col_lower for syn in synonyms):
                return col
        return None

    def analyze_cluster(self, tables: List[Any], dataframes: Dict[str, pd.DataFrame], relationships: List[Any]) -> Optional[Dict[str, Any]]:
        """
        Analyze a cluster of related tables to find credit/timestamp data.
        """
        
        main_df = None
        credit_col = None
        timestamp_col = None
        account_col = None
        type_col = None
        main_table_name = ""

        # Step 1: Find the main "Transaction" table (must have credit/amount)
        for table in tables:
            df = dataframes.get(table.table_name)
            if df is None:
                continue
            
            # Look for credit column
            c_col = self._find_best_column(df, self.CREDIT_SYNONYMS)
            if c_col:
                # Check for timestamp - use EXLCUDED_TIMESTAMPS to avoid 'dob' etc.
                t_col = self._find_best_column(df, self.TIMESTAMP_SYNONYMS, self.EXCLUDED_TIMESTAMPS)
                
                if t_col:
                    # Best case: One table has both
                    main_df = df
                    credit_col = c_col
                    timestamp_col = t_col
                    main_table_name = table.table_name
                    account_col = self._find_best_column(df, self.ACCOUNT_SYNONYMS)
                    type_col = self._find_best_column(df, self.TYPE_SYNONYMS)
                    break
                else:
                    # Keep looking for a better table, or fall back to this one if we can join later
                    if main_df is None:
                         main_df = df
                         credit_col = c_col
                         main_table_name = table.table_name
        
        if main_df is None or credit_col is None:
             return None # No credit data found

        # Step 2: If we have main_df but no timestamp, try to find it via relationships
        if timestamp_col is None:
            # Look for a relationship connecting main_table_name to another table with a timestamp
            for rel in relationships:
                if rel.source_table == main_table_name:
                    target_df = dataframes.get(rel.target_table)
                    if target_df is not None:
                        t_col = self._find_best_column(target_df, self.TIMESTAMP_SYNONYMS, self.EXCLUDED_TIMESTAMPS)
                        if t_col:
                            try:
                                merged = pd.merge(
                                    main_df, 
                                    target_df, 
                                    left_on=rel.source_column, 
                                    right_on=rel.target_column,
                                    how='inner'
                                )
                                main_df = merged
                                timestamp_col = t_col
                                break
                            except:
                                pass
                elif rel.target_table == main_table_name:
                     source_df = dataframes.get(rel.source_table)
                     if source_df is not None:
                        t_col = self._find_best_column(source_df, self.TIMESTAMP_SYNONYMS, self.EXCLUDED_TIMESTAMPS)
                        if t_col:
                            try:
                                merged = pd.merge(
                                    main_df, 
                                    source_df, 
                                    left_on=rel.target_column, 
                                    right_on=rel.source_column, 
                                    how='inner'
                                )
                                main_df = merged
                                timestamp_col = t_col
                                break
                            except:
                                pass

        if timestamp_col is None:
            return None # Still no timestamp
            
        # Analyze using the (potentially merged) dataframe
        result = self.analyze_credit_by_timeslot(main_df, timestamp_col, credit_col, account_col, type_col)
        result['analyzed_table'] = main_table_name
        result['credit_column'] = credit_col
        result['timestamp_column'] = timestamp_col
        return result

    def analyze_credit_by_timeslot(self, df: pd.DataFrame, 
                                   timestamp_col: str, 
                                   credit_col: str,
                                   account_col: str = None,
                                   type_col: str = None) -> Dict[str, Any]:
        """
        Analyze credit amounts grouped by time slots
        """
        # Create a copy to avoid modifying original
        analysis_df = df.copy()
        
        # Parse timestamps and extract hour
        analysis_df['parsed_time'] = analysis_df[timestamp_col].apply(self.parse_timestamp)
        valid_df = analysis_df[analysis_df['parsed_time'].notna()].copy()
        
        if len(valid_df) == 0:
            raise ValueError("No valid timestamps found in the data")
        
        # Extract hour and categorize
        valid_df['hour'] = valid_df['parsed_time'].dt.hour
        valid_df['time_slot'] = valid_df['hour'].apply(self.categorize_time_slot)
        
        # Clean credit column
        valid_df[credit_col] = pd.to_numeric(valid_df[credit_col], errors='coerce')
        # Handle cases where amount is negative for withdrawals (though often stored as positive with type)
        # We will work with absolute values and rely on type for classification if available
        valid_df['__abs_amount'] = valid_df[credit_col].abs()
        credit_df = valid_df[valid_df['__abs_amount'] > 0].copy()
        
        time_slot_analysis = []
        
        # Determine strictness for rules based on total volume
        total_cluster_credit = credit_df['__abs_amount'].sum() if len(credit_df) > 0 else 0
        high_threshold = total_cluster_credit * 0.40 # 40% of total
        med_threshold = total_cluster_credit * 0.15 # 15% of total
        
        for slot_name in ['Morning', 'Afternoon', 'Evening', 'Night']:
            slot_data = credit_df[credit_df['time_slot'] == slot_name]
            
            total_credit = float(slot_data['__abs_amount'].sum()) if len(slot_data) > 0 else 0.0
            credit_count = int(len(slot_data))
            
            unique_accounts = 0
            if account_col and account_col in slot_data.columns:
                unique_accounts = int(slot_data[account_col].nunique())

            # Enhanced Business Explanation with dynamic thresholds
            explanation = self._generate_detailed_explanation(
                slot_name, total_credit, credit_count, unique_accounts, 
                high_threshold, med_threshold, credit_col
            )
            
            time_slot_analysis.append({
                'time_group': slot_name,
                'total_credit': round(total_credit, 2),
                'credit_count': credit_count,
                'unique_accounts': unique_accounts,
                'business_meaning': explanation['meaning'],
                'business_rule': explanation['rule'],
                'recommendation': explanation['recommendation']
            })
        
        # Identify highest and lowest
        sorted_by_amount = sorted(time_slot_analysis, key=lambda x: x['total_credit'], reverse=True)
        highest_slot = sorted_by_amount[0] if sorted_by_amount else None
        lowest_slot = sorted_by_amount[-1] if sorted_by_amount else None
        
        # --- Transaction Type Analysis (Inflow vs Outflow) ---
        type_analysis = None
        debit_pattern_analysis = None
        credit_pattern_analysis = None
        
        # Identification Logic
        valid_df['__type_lower'] = valid_df[type_col].astype(str).str.lower() if type_col else ""
        
        inflow_keywords = ['deposit', 'credit', 'income', 'interest', 'add', 'received', 'in']
        outflow_keywords = ['withdrawal', 'debit', 'expense', 'payment', 'fee', 'sent', 'out']

        if type_col:
            inflow_mask = valid_df['__type_lower'].apply(lambda x: any(k in x for k in inflow_keywords))
            outflow_mask = valid_df['__type_lower'].apply(lambda x: any(k in x for k in outflow_keywords))
            
            credit_df_subset = valid_df[inflow_mask].copy()
            debit_df_subset = valid_df[outflow_mask].copy()
            
            type_analysis = {
                'inflow': {'amount': float(credit_df_subset['__abs_amount'].sum()), 'count': len(credit_df_subset)},
                'outflow': {'amount': float(debit_df_subset['__abs_amount'].sum()), 'count': len(debit_df_subset)},
                'other': {'amount': 0.0, 'count': 0}
            }
        else:
            # Fallback if no type col, assume credit_df (the one passed into analysis) is the focus
            credit_df_subset = credit_df.copy()
            debit_df_subset = pd.DataFrame() # No way to know debits easily without type col
            
        # Perform Pattern Analysis (Date & Account)
        if not credit_df_subset.empty:
            credit_pattern_analysis = self._perform_pattern_analysis(credit_df_subset, timestamp_col, credit_col, account_col, "credit")
            
        if not debit_df_subset.empty:
            debit_pattern_analysis = self._perform_pattern_analysis(debit_df_subset, timestamp_col, credit_col, account_col, "debit")

        return {
            'time_slot_analysis': time_slot_analysis,
            'highest_credit_slot': highest_slot['time_group'] if highest_slot else 'N/A',
            'lowest_credit_slot': lowest_slot['time_group'] if lowest_slot else 'N/A',
            'total_credit_amount': round(sum(float(i['total_credit']) for i in time_slot_analysis), 2),
            'total_credit_transactions': sum(int(i['credit_count']) for i in time_slot_analysis),
            'analysis_summary': self._generate_summary(time_slot_analysis, highest_slot, lowest_slot),
            'type_analysis': type_analysis,
            'debit_pattern_analysis': debit_pattern_analysis,
            'credit_pattern_analysis': credit_pattern_analysis
        }

    def _perform_pattern_analysis(self, df: pd.DataFrame, time_col: str, amt_col: str, acct_col: str, flow_type: str = "debit") -> Dict[str, Any]:
        """
        Groups by Date and Account to analyze transaction patterns (Credit or Debit).
        """
        results = {'by_date': [], 'by_account': [], 'summary': ""}
        term = "credit" if flow_type == "credit" else "debit"
        action = "received" if flow_type == "credit" else "spent/transferred"
        
        # 1. Group by Date
        df['__date_str'] = df['parsed_time'].dt.strftime('%Y-%m-%d')
        date_groups = df.groupby('__date_str')
        
        for date, group in date_groups:
            total_amt = group['__abs_amount'].sum()
            count = len(group)
            
            if count == 1:
                meaning = f"A single {term} of <strong>${total_amt:,.2f}</strong> was recorded on this day."
            else:
                meaning = f"This day saw <strong>{count} separate {term}s</strong> totaling <strong>${total_amt:,.2f}</strong>."
            
            rule = "Normal Activity"
            if count > 5: rule = f"🚨 High Frequency: Multiple {term}s on a single day."
            elif total_amt > 5000: rule = f"💰 High Volume: Significant {term} movement."
            
            results['by_date'].append({
                'label': date,
                'total_amount': float(total_amt),
                'count': int(count),
                'meaning': meaning,
                'rule': rule
            })
            
        # 2. Group by Account
        repeated_count = 0
        if acct_col and acct_col in df.columns:
            acct_groups = df.groupby(acct_col)
            for acct, group in acct_groups:
                total_amt = group['__abs_amount'].sum()
                count = len(group)
                repeated = count > 1
                if repeated: repeated_count += 1
                
                if repeated:
                    meaning = f"Account {acct} is a <strong>frequent {term} source/target</strong> with {count} transactions totaling <strong>${total_amt:,.2f}</strong>."
                else:
                    meaning = f"Account {acct} made one {term} transaction of <strong>${total_amt:,.2f}</strong>."
                
                rule = "Standard"
                if repeated and total_amt > 2000:
                    rule = f"💎 High Value {term.capitalize()} Cluster"
                elif repeated:
                    rule = f"🔄 Recurring {term.capitalize()} Activity"

                results['by_account'].append({
                    'label': str(acct),
                    'total_amount': float(total_amt),
                    'count': int(count),
                    'meaning': meaning,
                    'rule': rule,
                    'is_repeated': repeated
                })
        
        # 3. Summary
        total_sum = df['__abs_amount'].sum()
        results['summary'] = (f"Analysis of <strong>{len(df)} {term} transactions</strong> reveals a total of <strong>${total_sum:,.2f}</strong> {action}. "
                             f"We identified <strong>{repeated_count} accounts</strong> with repeated {term} activity.")

        # Sort by Date (Chronological)
        results['by_date'] = sorted(results['by_date'], key=lambda x: x['label'])
        # Sort by Account (Value - already done or keep as is)
        results['by_account'] = sorted(results['by_account'], key=lambda x: x['total_amount'], reverse=True)
        
        return results

    def _generate_detailed_explanation(self, slot: str, amount: float, count: int, accounts: int, 
                                       high_thresh: float, med_thresh: float, credit_col_name: str) -> Dict[str, str]:
        """Generate detailed, beginner-friendly explanations with context-aware thresholds"""
        
        # Determine strictness for context terms
        money_term = "debit" if "debit" in credit_col_name.lower() else "credit"
        if "withdrawal" in credit_col_name.lower(): money_term = "withdrawal"
        
        if count == 0:
            return {
                "meaning": f"We observed <strong>0 {money_term} transactions</strong> in the {slot}. Total volume was $0.00.",
                "rule": "⚪ <strong>No Activity</strong>: Dormant period.",
                "recommendation": "Normal for non-business hours."
            }

        avg_txn = amount / count if count > 0 else 0
        
        # Narrative Style as requested: "3 debit transactions at this time..."
        # Example: "This slot had 3 debit transactions totaling $500. It involved 2 unique users."
        meaning = (f"This time slot had <strong>{count} {money_term} transactions</strong> totaling <strong>${amount:,.2f}</strong>. "
                   f"The average transaction size was <strong>${avg_txn:,.2f}</strong>.")
        
        if accounts > 0:
            user_label = "unique user" if accounts == 1 else "unique users"
            user_part = f" It involved <strong>{accounts} {user_label}</strong>."
            meaning += user_part
            
            # Add reasoning context
            if accounts < 3 and amount < med_thresh:
                meaning += f" The low volume is likely due to limited user participation ({accounts} users)."
            elif accounts < 3 and amount > high_thresh:
                meaning += f" High volume driven by high-value transactions from a small group ({accounts} users)."

        # Rule generation based on relative thresholds
        rule = ""
        rec = ""
        
        if amount > high_thresh and amount > 0:
             rule = f"🔥 <strong>High Traffic</strong>: {count} transactions generated {money_term} volume significantly above average."
             rec = "Critical period for revenue/liquidity monitoring."
        elif amount > med_thresh:
             rule = "✅ <strong>Moderate Traffic</strong>: Consistent activity levels observed."
             rec = "Standard monitoring recommended."
        else:
             rule = "📉 <strong>Low Traffic</strong>: Minimal financial movement relative to other times."
             rec = "Low priority for real-time monitoring."

        return {
            "meaning": meaning,
            "rule": rule,
            "recommendation": rec
        }
    
    def _generate_summary(self, analysis: List[Dict], highest: Optional[Dict], lowest: Optional[Dict]) -> str:
        if not highest:
            return "No credit data available to analyze."

        summary = [
            "<div class='credit-summary p-3 bg-light rounded'>",
            f"<h5 style='color: #2c3e50;'>✨ Executive Summary</h5>",
            f"<p>The analysis reveals distinct patterns in when money enters the system.</p>",
            f"<ul>"
            f"<li><strong>Peak Inflow:</strong> The <strong>{highest['time_group']}</strong> is your most critical period, generating <strong>${highest['total_credit']:,.2f}</strong>. This suggests that {( 'business operations' if highest['time_group'] in ['Morning', 'Afternoon'] else 'automated settlements or international transfers' )} are driving liquidity.</li>",
            f"<li><strong>Quiet Period:</strong> The <strong>{lowest['time_group']}</strong> sees the least activity.</li>",
            f"</ul>",
            f"<p><strong>Strategic Tip:</strong> Focus customer engagement campaigns or system scaling efforts during the <strong>{highest['time_group']}</strong> to maximize impact.</p>",
            "</div>"
        ]
        return "\n".join(summary)


def analyze_credit_from_file(file_path: str, timestamp_col: str = None, credit_col: str = None) -> Dict[str, Any]:
    """Convenience function to analyze credit from a CSV file (Legacy support)"""
    df = pd.read_csv(file_path)
    
    analyzer = CreditTimeSlotAnalyzer()
    
    # Auto-detect if not provided
    if not timestamp_col:
        timestamp_col = analyzer._find_best_column(df, analyzer.TIMESTAMP_SYNONYMS)
    if not credit_col:
        credit_col = analyzer._find_best_column(df, analyzer.CREDIT_SYNONYMS)
        
    if not timestamp_col or not credit_col:
         raise ValueError("Could not auto-detect necessary columns for credit analysis.")
         
    return analyzer.analyze_credit_by_timeslot(df, timestamp_col, credit_col)
