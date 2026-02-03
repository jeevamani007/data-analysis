"""
Customer Data Linker
Links customer data across multiple uploaded tables using fuzzy ID matching
"""

import pandas as pd
from typing import List, Dict, Any, Optional, Tuple


class CustomerLinker:
    """
    Links customer/account data across multiple CSV tables
    without using SQL joins - uses pandas merge logic
    """
    
    def __init__(self):
        self.ID_KEYWORDS = [
            'customer_id', 'user_id', 'client_id', 'account_id',
            'member_id', 'customer', 'user', 'account', 'id'
        ]
    
    def detect_id_columns(self, tables: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """
        Detect ID columns across all tables.
        
        Args:
            tables: List of table dicts with 'name' and 'dataframe' keys
            
        Returns:
            Dict mapping table names to their ID columns
        """
        id_mapping = {}
        
        for table in tables:
            table_name = table.get('name', 'unknown')
            df = table.get('dataframe')
            
            if df is None:
                continue
            
            found_ids = []
            for col in df.columns:
                col_lower = col.lower()
                
                # Check for exact matches first
                if col_lower in self.ID_KEYWORDS:
                    found_ids.append(col)
                # Then check for partial matches
                elif any(keyword in col_lower for keyword in ['id', 'customer', 'user', 'account']):
                    found_ids.append(col)
            
            if found_ids:
                id_mapping[table_name] = found_ids
        
        return id_mapping
    
    def find_common_id_column(self, df1: pd.DataFrame, df2: pd.DataFrame) -> Optional[Tuple[str, str]]:
        """
        Find a common ID column between two DataFrames.
        
        Returns:
            Tuple of (column_in_df1, column_in_df2) or None if no match found
        """
        # Try exact name matches first
        common_cols = set(df1.columns) & set(df2.columns)
        
        for col in common_cols:
            col_lower = col.lower()
            if any(keyword in col_lower for keyword in ['id', 'customer', 'user', 'account']):
                return (col, col)
        
        # Try fuzzy matching
        for col1 in df1.columns:
            col1_lower = col1.lower()
            if not any(keyword in col1_lower for keyword in ['id', 'customer', 'user', 'account']):
                continue
            
            for col2 in df2.columns:
                col2_lower = col2.lower()
                
                # Check if they share keywords
                if any(keyword in col1_lower and keyword in col2_lower 
                       for keyword in ['customer', 'user', 'account']):
                    return (col1, col2)
        
        return None
    
    def link_customer_activity(
        self,
        customer_df: pd.DataFrame,
        activity_tables: List[Dict[str, Any]],
        customer_id_col: str
    ) -> pd.DataFrame:
        """
        Link customer data with activity from other tables (logins, transactions).
        
        Args:
            customer_df: Main customer/account DataFrame
            activity_tables: List of dicts with 'name', 'dataframe', and optional 'id_column'
            customer_id_col: Column name in customer_df to use as primary key
            
        Returns:
            Enhanced DataFrame with activity indicators
        """
        # Start with customer data
        result = customer_df.copy()
        
        # Add activity flags
        result['has_login_activity'] = False
        result['has_transaction_activity'] = False
        result['last_login_date'] = pd.NaT
        result['last_transaction_date'] = pd.NaT
        
        for activity_table in activity_tables:
            table_name = activity_table.get('name', '').lower()
            activity_df = activity_table.get('dataframe')
            
            if activity_df is None or activity_df.empty:
                continue
            
            # Find matching ID column
            activity_id_col = activity_table.get('id_column')
            
            if not activity_id_col:
                # Try to find it automatically
                match = self.find_common_id_column(customer_df, activity_df)
                if match:
                    _, activity_id_col = match
                else:
                    continue
            
            # Determine activity type
            is_login = any(word in table_name for word in ['login', 'auth', 'access', 'session'])
            is_transaction = any(word in table_name for word in ['transaction', 'payment', 'transfer'])
            
            # Get unique customer IDs with activity
            active_customers = activity_df[activity_id_col].unique()
            
            # Mark customers as active
            if is_login:
                result.loc[result[customer_id_col].isin(active_customers), 'has_login_activity'] = True
                
                # Try to find last login date
                date_cols = [col for col in activity_df.columns 
                           if any(word in col.lower() for word in ['date', 'time', 'created'])]
                
                if date_cols:
                    date_col = date_cols[0]
                    activity_df[date_col] = pd.to_datetime(activity_df[date_col], errors='coerce')
                    
                    # Get last login per customer
                    last_logins = activity_df.groupby(activity_id_col)[date_col].max()
                    
                    for cust_id, last_date in last_logins.items():
                        mask = result[customer_id_col] == cust_id
                        result.loc[mask, 'last_login_date'] = last_date
            
            if is_transaction:
                result.loc[result[customer_id_col].isin(active_customers), 'has_transaction_activity'] = True
                
                # Try to find last transaction date
                date_cols = [col for col in activity_df.columns 
                           if any(word in col.lower() for word in ['date', 'time', 'created'])]
                
                if date_cols:
                    date_col = date_cols[0]
                    activity_df[date_col] = pd.to_datetime(activity_df[date_col], errors='coerce')
                    
                    # Get last transaction per customer
                    last_trans = activity_df.groupby(activity_id_col)[date_col].max()
                    
                    for cust_id, last_date in last_trans.items():
                        mask = result[customer_id_col] == cust_id
                        result.loc[mask, 'last_transaction_date'] = last_date
        
        return result
    
    def identify_inactive_users(
        self,
        linked_data: pd.DataFrame,
        date_col: str,
        customer_id_col: str,
        age_threshold_days: int = 365,
        activity_threshold_days: int = 180
    ) -> List[Dict[str, Any]]:
        """
        Identify inactive customers: old account but no recent activity.
        
        Args:
            linked_data: DataFrame with customer and activity data
            date_col: Column name for account creation date
            customer_id_col: Column name for customer ID
            age_threshold_days: Minimum account age to be considered (default 365)
            activity_threshold_days: No activity in last N days (default 180)
            
        Returns:
            List of inactive customer records
        """
        # Parse dates
        linked_data = linked_data.copy()
        linked_data[date_col] = pd.to_datetime(linked_data[date_col], errors='coerce')
        
        now = pd.Timestamp.now()
        
        # Calculate account age
        linked_data['account_age_days'] = (now - linked_data[date_col]).dt.days
        
        # Define inactive criteria
        # 1. Account is old (> age_threshold_days)
        # 2. No login activity OR last login was > activity_threshold_days ago
        # 3. No transaction activity OR last transaction was > activity_threshold_days ago
        
        inactive_mask = (
            (linked_data['account_age_days'] > age_threshold_days) &
            (
                (~linked_data['has_login_activity']) | 
                ((now - linked_data['last_login_date']).dt.days > activity_threshold_days)
            ) &
            (
                (~linked_data['has_transaction_activity']) |
                ((now - linked_data['last_transaction_date']).dt.days > activity_threshold_days)
            )
        )
        
        inactive_customers = linked_data[inactive_mask].copy()
        
        # Format results
        results = []
        for _, row in inactive_customers.iterrows():
            reason_parts = []
            
            if not row['has_login_activity']:
                reason_parts.append("No login records found")
            elif pd.notna(row['last_login_date']):
                days_since = (now - row['last_login_date']).days
                reason_parts.append(f"Last login was {days_since} days ago")
            
            if not row['has_transaction_activity']:
                reason_parts.append("No transaction records found")
            elif pd.notna(row['last_transaction_date']):
                days_since = (now - row['last_transaction_date']).days
                reason_parts.append(f"Last transaction was {days_since} days ago")
            
            results.append({
                'customer_id': row[customer_id_col],
                'account_created': row[date_col].strftime('%Y-%m-%d') if pd.notna(row[date_col]) else 'Unknown',
                'account_age_days': int(row['account_age_days']) if pd.notna(row['account_age_days']) else 0,
                'inactive_reason': ' | '.join(reason_parts),
                'business_action': 'Re-engagement campaign recommended'
            })
        
        return results
    
    def get_multi_account_holders(
        self,
        df: pd.DataFrame,
        customer_id_col: str,
        date_col: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Find customers with multiple accounts.
        
        Returns:
            List of multi-account holders with their account counts
        """
        # Count accounts per customer
        account_counts = df[customer_id_col].value_counts()
        multi_account = account_counts[account_counts > 1]
        
        results = []
        for customer_id, count in multi_account.items():
            customer_accounts = df[df[customer_id_col] == customer_id]
            
            result = {
                'customer_id': customer_id,
                'account_count': int(count),
                'business_meaning': 'Customer manages multiple accounts (likely Savings + Checking)',
                'action': 'Cross-sell opportunity'
            }
            
            # Add date info if available
            if date_col and date_col in df.columns:
                dates = pd.to_datetime(customer_accounts[date_col], errors='coerce').dropna()
                if len(dates) > 0:
                    result['first_account'] = dates.min().strftime('%Y-%m-%d')
                    result['latest_account'] = dates.max().strftime('%Y-%m-%d')
            
            results.append(result)
        
        return results
