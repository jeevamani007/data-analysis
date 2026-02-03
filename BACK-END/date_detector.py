"""
Date Column Detector using Fuzzy Logic
Intelligently detects date columns and generates confirmation questions
"""

import pandas as pd
from typing import List, Dict, Any, Tuple
from datetime import datetime


class DateColumnDetector:
    """
    Detects date columns in DataFrame using fuzzy matching
    and generates intelligent confirmation questions.
    """
    
    def __init__(self):
        # Date-related keywords for fuzzy matching
        self.date_keywords = [
            'date', 'time', 'timestamp', 'created', 'opened', 'start',
            'signup', 'register', 'enrolled', 'activation', 'opening',
            'account_date', 'open_date', 'open_time', 'creation_date', 'created_at',
            'account_open_date', 'account_created', 'signed_up'
        ]
        
        # Login/access keywords for detecting login timestamps (first access, NOT account creation)
        self.login_keywords = [
            'login', 'access', 'signin', 'auth', 'session', 'visit',
            'first_login', 'login_time', 'login_date', 'login_timestamp',
            'first_access', 'access_time', 'activation_time'
        ]
        # Columns to EXCLUDE from login candidates (these are account open/creation, not login)
        self.login_exclude_keywords = [
            'open_date', 'open_time', 'created_at', 'created', 'signup', 'register',
            'account_date', 'opening', 'enrollment_date', 'joined', 'account_created'
        ]

        
        # Customer ID keywords
        self.ID_SYNONYMS = [
            'customer', 'user', 'client', 'account', 'member', 
            'id', 'number', 'code', 'identifier'
        ]
    
    def find_date_columns(self, df: pd.DataFrame, table_name: str = "") -> List[Dict[str, Any]]:
        """
        Scan DataFrame for potential date columns using fuzzy matching.
        
        Args:
            df: Pandas DataFrame to analyze
            table_name: Name of the table for context
            
        Returns:
            List of candidate columns with confidence scores
        """
        candidates = []
        
        for col in df.columns:
            col_lower = col.lower()
            
            # Calculate confidence based on keyword matches
            confidence = 0
            matched_keywords = []
            
            for keyword in self.date_keywords:
                if keyword in col_lower:
                    confidence += 20
                    matched_keywords.append(keyword)
            
            # Boost confidence for account-creation patterns (open_time / open_date)
            if 'open' in col_lower and ('date' in col_lower or 'time' in col_lower):
                confidence += 35
            if 'created' in col_lower or 'signup' in col_lower:
                confidence += 25
            if col_lower in ['date', 'timestamp', 'created_at', 'open_date', 'open_time']:
                confidence += 40
            
            # Cap confidence at 100
            confidence = min(confidence, 100)
            
            # Check if column can be parsed as datetime
            is_parseable = False
            sample_values = []
            
            if confidence > 0:
                try:
                    # Try parsing a few non-null values
                    non_null = df[col].dropna()
                    if len(non_null) > 0:
                        sample = non_null.head(5)
                        parsed = pd.to_datetime(sample, errors='coerce')
                        
                        # If at least 80% of sample parses successfully
                        if parsed.notna().sum() >= len(sample) * 0.8:
                            is_parseable = True
                            sample_values = sample.head(3).tolist()
                            confidence += 20  # Bonus for being parseable
                except:
                    pass
            
            # Only include columns with some confidence
            if confidence >= 20:
                candidates.append({
                    'column_name': col,
                    'confidence': min(confidence, 100),
                    'keywords_matched': matched_keywords,
                    'is_parseable': is_parseable,
                    'sample_values': sample_values,
                    'table': table_name
                })
        
        # Fallback: try parsing EVERY column as datetime (find timestamp even without keyword match)
        existing_cols = {c['column_name'] for c in candidates}
        for col in df.columns:
            if col in existing_cols:
                continue
            try:
                non_null = df[col].dropna().astype(str)
                if len(non_null) == 0:
                    continue
                sample = non_null.head(20)
                parsed = pd.to_datetime(sample, errors='coerce')
                success_rate = parsed.notna().sum() / len(sample) if len(sample) else 0
                if success_rate >= 0.5:  # at least 50% parse as datetime
                    candidates.append({
                        'column_name': col,
                        'confidence': min(30 + int(success_rate * 40), 85),  # 30–85 for parse-only
                        'keywords_matched': ['parsed_as_datetime'],
                        'is_parseable': True,
                        'sample_values': non_null.head(3).tolist(),
                        'table': table_name
                    })
            except Exception:
                pass
        
        # Sort by confidence (highest first)
        candidates.sort(key=lambda x: x['confidence'], reverse=True)
        
        return candidates
    
    def find_id_columns(self, df: pd.DataFrame) -> List[str]:
        """
        Find potential customer/account ID columns.
        
        Returns:
            List of column names that might be IDs
        """
        id_columns = []
        
        for col in df.columns:
            col_lower = col.lower()
            
            for keyword in self.ID_SYNONYMS:
                if keyword in col_lower:
                    id_columns.append(col)
                    break
        
        return id_columns
    
    def find_login_timestamp_columns(self, dataframes: Dict[str, pd.DataFrame]) -> List[Dict[str, Any]]:
        """
        Search across ALL tables for login timestamp columns.
        
        Args:
            dataframes: Dictionary of {table_name: DataFrame}
            
        Returns:
            List of login column candidates with table info
        """
        login_candidates = []
        
        for table_name, df in dataframes.items():
            for col in df.columns:
                col_lower = col.lower()
                
                # Exclude account-creation columns so open_time and login_timestamp stay separate
                if any(ex in col_lower for ex in self.login_exclude_keywords):
                    continue
                
                # Calculate confidence for login columns
                confidence = 0
                matched_keywords = []
                
                for keyword in self.login_keywords:
                    if keyword in col_lower:
                        confidence += 25
                        matched_keywords.append(keyword)
                
                # Boost for exact matches (clear login, not open)
                if col_lower in ['login_timestamp', 'first_login', 'login_time', 'first_access']:
                    confidence += 40
                
                # Check if parseable
                is_parseable = False
                sample_values = []
                
                if confidence > 0:
                    try:
                        non_null = df[col].dropna()
                        if len(non_null) > 0:
                            sample = non_null.head(5)
                            parsed = pd.to_datetime(sample, errors='coerce')
                            
                            if parsed.notna().sum() >= len(sample) * 0.8:
                                is_parseable = True
                                sample_values = sample.head(3).tolist()
                                confidence += 20
                    except:
                        pass
                
                if confidence >= 25:
                    login_candidates.append({
                        'column_name': col,
                        'table_name': table_name,
                        'confidence': min(confidence, 100),
                        'keywords_matched': matched_keywords,
                        'is_parseable': is_parseable,
                        'sample_values': sample_values
                    })
        
        # Sort by confidence
        login_candidates.sort(key=lambda x: x['confidence'], reverse=True)
        return login_candidates
    
    def generate_confirmation_questions(
        self, 
        date_candidates: List[Dict],
        id_candidates: List[str],
        tables: List[str]
    ) -> List[Dict[str, str]]:
        """
        Generate 3 intelligent questions based on detected columns.
        
        Args:
            date_candidates: List of date column candidates
            id_candidates: List of ID column candidates
            tables: List of table names in the database
            
        Returns:
            List of question dictionaries with question text and context
        """
        questions = []
        
        # Question 1: Account Creation Date
        if date_candidates:
            date_options = [c['column_name'] for c in date_candidates[:3]]
            q1 = {
                'question_id': 'account_creation_date',
                'question': 'Which column represents the account creation date?',
                'context': f"We detected these potential date columns: {', '.join(date_options)}",
                'suggestions': date_options,
                'type': 'date_column'
            }
            questions.append(q1)
        else:
            questions.append({
                'question_id': 'account_creation_date',
                'question': 'Which column represents the account creation date?',
                'context': 'No date columns were automatically detected. Please specify manually.',
                'suggestions': [],
                'type': 'date_column'
            })
        
        # Question 2: Customer Identifier
        if id_candidates:
            q2 = {
                'question_id': 'customer_identifier',
                'question': 'Which column is the customer identifier?',
                'context': f"We found these ID-like columns: {', '.join(id_candidates[:3])}",
                'suggestions': id_candidates[:3],
                'type': 'id_column'
            }
            questions.append(q2)
        else:
            questions.append({
                'question_id': 'customer_identifier',
                'question': 'Which column is the customer identifier?',
                'context': 'Please specify the customer ID column.',
                'suggestions': [],
                'type': 'id_column'
            })
        
        # Question 3: Last Activity Column (optional)
        activity_candidates = [
            c for c in date_candidates 
            if any(word in c['column_name'].lower() for word in ['login', 'activity', 'transaction', 'last', 'access'])
        ]
        
        if activity_candidates:
            q3 = {
                'question_id': 'last_activity',
                'question': 'Which column indicates last activity (login/transaction)?',
                'context': f"Optional: Helps detect inactive customers. Found: {', '.join([c['column_name'] for c in activity_candidates[:2]])}",
                'suggestions': [c['column_name'] for c in activity_candidates[:2]],
                'type': 'activity_column',
                'optional': True
            }
            questions.append(q3)
        else:
            questions.append({
                'question_id': 'last_activity',
                'question': 'Which column indicates last activity (login/transaction)?',
                'context': 'Optional: Leave blank if not applicable.',
                'suggestions': [],
                'type': 'activity_column',
                'optional': True
            })
        
        return questions
    
    def validate_date_column(self, df: pd.DataFrame, column_name: str) -> Dict[str, Any]:
        """
        Validate that a column can be used as a date column.
        
        Returns:
            Validation result with success status and details
        """
        if column_name not in df.columns:
            return {
                'valid': False,
                'error': f"Column '{column_name}' not found in dataset"
            }
        
        try:
            # Try parsing the column
            parsed = pd.to_datetime(df[column_name], errors='coerce')
            valid_count = parsed.notna().sum()
            total_count = len(df)
            
            if valid_count == 0:
                return {
                    'valid': False,
                    'error': f"Column '{column_name}' contains no valid dates"
                }
            
            success_rate = (valid_count / total_count) * 100
            
            if success_rate < 50:
                return {
                    'valid': False,
                    'error': f"Only {success_rate:.1f}% of values in '{column_name}' are valid dates"
                }
            
            # Get date range
            min_date = parsed.min()
            max_date = parsed.max()
            
            return {
                'valid': True,
                'parsed_count': int(valid_count),
                'total_count': int(total_count),
                'success_rate': float(success_rate),
                'date_range': {
                    'min': min_date.strftime('%Y-%m-%d') if pd.notna(min_date) else None,
                    'max': max_date.strftime('%Y-%m-%d') if pd.notna(max_date) else None
                }
            }
        except Exception as e:
            return {
                'valid': False,
                'error': f"Error parsing column '{column_name}': {str(e)}"
            }
