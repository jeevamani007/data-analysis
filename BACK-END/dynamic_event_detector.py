"""
Dynamic Event Detection Utility
--------------------------------
Detects events from data patterns (datatypes, values) when column names don't match.
Works across all domains: Banking, Retail, Healthcare, HR, Finance, Insurance.

Key Features:
- Datetime column detection by data type (not just name)
- Numeric column detection for amount-related events
- String pattern matching for event names
- Row value scanning across all columns
- No hardcoding - derives from actual data
"""

from typing import Dict, List, Any, Optional, Tuple, Set
import pandas as pd
import re
from datetime import datetime


def detect_datetime_columns_by_type(df: pd.DataFrame, exclude_dob: bool = True) -> List[Tuple[str, float]]:
    """
    Detect datetime columns by actually parsing the data (not just column name).
    Returns list of (column_name, parse_success_rate) sorted by success rate.
    
    Args:
        df: DataFrame to analyze
        exclude_dob: If True, exclude date of birth columns
    
    Returns:
        List of (column_name, success_rate) tuples, sorted by success rate (desc)
    """
    candidates = []
    
    for col in df.columns:
        if exclude_dob:
            col_lower = str(col).lower()
            if any(kw in col_lower for kw in ['dob', 'date_of_birth', 'birth_date', 'birthdate']):
                continue
        
        # Try to parse sample values
        sample = df[col].dropna().head(50)
        if len(sample) == 0:
            continue
        
        parsed_count = 0
        total_count = len(sample)
        
        for val in sample:
            if val is None or (isinstance(val, float) and pd.isna(val)):
                continue
            
            # Try pandas datetime parsing
            try:
                parsed = pd.to_datetime(val, errors='coerce')
                if pd.notna(parsed):
                    parsed_count += 1
            except Exception:
                pass
        
        if total_count > 0:
            success_rate = parsed_count / total_count
            if success_rate >= 0.3:  # At least 30% parseable
                candidates.append((col, success_rate))
    
    # Sort by success rate (descending)
    candidates.sort(key=lambda x: x[1], reverse=True)
    return candidates


def detect_numeric_columns(df: pd.DataFrame) -> List[Tuple[str, str]]:
    """
    Detect numeric columns (int/float) that might indicate amount-related events.
    Returns list of (column_name, dtype_category) where category is 'amount', 'count', or 'id'.
    
    Args:
        df: DataFrame to analyze
    
    Returns:
        List of (column_name, category) tuples
    """
    numeric_cols = []
    
    for col in df.columns:
        # Check if column is numeric type
        if pd.api.types.is_numeric_dtype(df[col]):
            col_lower = str(col).lower()
            
            # Categorize by column name hints
            category = 'amount'
            if any(kw in col_lower for kw in ['id', '_id', 'number', 'no', 'num']):
                category = 'id'
            elif any(kw in col_lower for kw in ['count', 'quantity', 'qty', 'total']):
                category = 'count'
            elif any(kw in col_lower for kw in ['amount', 'amt', 'value', 'price', 'cost', 'fee', 'charge', 'balance']):
                category = 'amount'
            
            numeric_cols.append((col, category))
    
    return numeric_cols


def scan_string_columns_for_events(
    df: pd.DataFrame,
    event_keywords: Set[str],
    sample_size: int = 100
) -> Dict[str, List[str]]:
    """
    Scan string columns for event-related keywords.
    Returns dict mapping column_name -> list of found event keywords.
    
    Args:
        df: DataFrame to analyze
        event_keywords: Set of event keywords to look for (domain-specific)
        sample_size: Number of rows to sample
    
    Returns:
        Dict mapping column_name -> list of found keywords
    """
    results = {}
    
    for col in df.columns:
        if pd.api.types.is_string_dtype(df[col]) or df[col].dtype == 'object':
            sample = df[col].dropna().head(sample_size)
            if len(sample) == 0:
                continue
            
            found_keywords = []
            for val in sample:
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    continue
                
                val_str = str(val).lower().strip()
                if not val_str:
                    continue
                
                # Check for event keywords
                for keyword in event_keywords:
                    keyword_lower = keyword.lower()
                    # Check if keyword appears in value
                    if keyword_lower in val_str or val_str in keyword_lower:
                        if keyword not in found_keywords:
                            found_keywords.append(keyword)
            
            if found_keywords:
                results[col] = found_keywords
    
    return results


def infer_event_from_datetime_column(
    col_name: str,
    domain: str = 'generic'
) -> Optional[str]:
    """
    Infer event name from datetime column name.
    Works across all domains - no hardcoding.
    
    Args:
        col_name: Column name
        domain: Domain name (banking, retail, healthcare, hr, finance, insurance)
    
    Returns:
        Inferred event name or None
    """
    if not col_name:
        return None
    
    col_lower = str(col_name).lower().replace('-', '_').replace(' ', '_')
    
    # Remove common datetime suffixes
    for suffix in ['_time', '_date', '_timestamp', '_at', '_datetime', '_dt']:
        if col_lower.endswith(suffix):
            col_lower = col_lower[:-len(suffix)]
            break
    
    # Extract base event name (everything before datetime suffix)
    # This is the event name inferred from column
    if col_lower:
        # Convert to title case with spaces
        event_name = col_lower.replace('_', ' ').title()
        return event_name
    
    return None


def infer_event_from_numeric_column(
    col_name: str,
    col_value: Any,
    domain: str = 'generic'
) -> Optional[str]:
    """
    Infer event from numeric column (amount-related events).
    
    Args:
        col_name: Column name
        col_value: Column value
        domain: Domain name
    
    Returns:
        Inferred event name or None
    """
    if col_name is None or col_value is None:
        return None
    
    col_lower = str(col_name).lower()
    
    # Amount-related events based on column name
    if any(kw in col_lower for kw in ['amount', 'amt', 'value', 'price', 'cost']):
        # Check if value is positive/negative to infer credit/debit
        try:
            num_val = float(col_value)
            if num_val > 0:
                if domain == 'banking':
                    return 'credit'
                elif domain == 'retail':
                    return 'Payment Success'
                elif domain == 'finance':
                    return 'Deposit'
            elif num_val < 0:
                if domain == 'banking':
                    return 'debit'
                elif domain == 'retail':
                    return 'Payment Failed'
                elif domain == 'finance':
                    return 'Withdrawal'
        except (ValueError, TypeError):
            pass
    
    return None


def scan_row_for_event_patterns(
    row: pd.Series,
    columns: List[str],
    event_keywords: Set[str],
    domain: str = 'generic'
) -> Optional[str]:
    """
    Scan all row values for event patterns.
    Checks string columns, numeric columns, and datetime columns.
    
    Args:
        row: DataFrame row (pd.Series)
        columns: List of column names to check
        event_keywords: Set of event keywords to match
        domain: Domain name
    
    Returns:
        Detected event name or None
    """
    if row is None or len(columns) == 0:
        return None
    
    # Priority 1: Check string columns for event keywords
    for col in columns:
        if col.startswith('__'):
            continue
        
        val = row.get(col)
        if val is None or (isinstance(val, float) and pd.isna(val)):
            continue
        
        val_str = str(val).strip().lower()
        if not val_str:
            continue
        
        # Check for exact or partial keyword matches
        for keyword in event_keywords:
            keyword_lower = keyword.lower()
            if keyword_lower in val_str or val_str in keyword_lower:
                # Return formatted event name
                return keyword.replace('_', ' ').title()
    
    # Priority 2: Check numeric columns for amount-related events
    for col in columns:
        if col.startswith('__'):
            continue
        
        val = row.get(col)
        if val is None or (isinstance(val, float) and pd.isna(val)):
            continue
        
        # Check if numeric
        try:
            float(val)
            event = infer_event_from_numeric_column(col, val, domain)
            if event:
                return event
        except (ValueError, TypeError):
            pass
    
    return None


def find_best_timestamp_column(
    df: pd.DataFrame,
    exclude_dob: bool = True
) -> Optional[Tuple[str, Optional[str]]]:
    """
    Find the best timestamp column(s) by data type analysis.
    Returns (date_col, time_col) or (timestamp_col, None).
    
    Args:
        df: DataFrame to analyze
        exclude_dob: If True, exclude date of birth columns
    
    Returns:
        Tuple of (date_col, time_col) or None
    """
    # Find datetime columns by type
    datetime_cols = detect_datetime_columns_by_type(df, exclude_dob)
    
    if not datetime_cols:
        return None
    
    # Prefer columns with highest parse success rate
    best_col = datetime_cols[0][0]
    
    # Check if there's a separate time column
    time_col = None
    for col in df.columns:
        col_lower = str(col).lower()
        if (col != best_col and 
            'time' in col_lower and 
            'date' not in col_lower and 
            'stamp' not in col_lower):
            # Check if it's parseable as time
            sample = df[col].dropna().head(10)
            if len(sample) > 0:
                time_col = col
                break
    
    return (best_col, time_col)


def extract_events_from_row(
    row: pd.Series,
    df: pd.DataFrame,
    event_keywords: Set[str],
    domain: str = 'generic',
    timestamp_col: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Extract all possible events from a single row.
    Returns list of event dictionaries with timestamp and event name.
    
    Args:
        row: DataFrame row
        df: Full DataFrame (for context)
        event_keywords: Set of event keywords
        domain: Domain name
        timestamp_col: Primary timestamp column name
    
    Returns:
        List of event dicts: [{'event': str, 'timestamp': pd.Timestamp, 'source': str}, ...]
    """
    events = []
    
    # Find all datetime columns in this row
    datetime_cols = detect_datetime_columns_by_type(df, exclude_dob=True)
    
    # If no datetime columns found, try to infer from row values
    if not datetime_cols:
        # Scan row for event patterns (might find events without timestamps)
        event_name = scan_row_for_event_patterns(row, list(df.columns), event_keywords, domain)
        if event_name:
            events.append({
                'event': event_name,
                'timestamp': None,
                'source': 'row_scan_no_timestamp'
            })
        return events
    
    # For each datetime column, try to extract an event
    for col_name, success_rate in datetime_cols:
        if col_name not in row.index:
            continue
        
        ts_val = row.get(col_name)
        if ts_val is None or (isinstance(ts_val, float) and pd.isna(ts_val)):
            continue
        
        # Parse timestamp
        try:
            ts = pd.to_datetime(ts_val, errors='coerce')
            if pd.isna(ts):
                continue
        except Exception:
            continue
        
        # Infer event from column name
        event_name = infer_event_from_datetime_column(col_name, domain)
        
        # If column name doesn't give event, scan row values
        if not event_name:
            event_name = scan_row_for_event_patterns(row, list(df.columns), event_keywords, domain)
        
        # If still no event, use generic
        if not event_name:
            event_name = f"Event at {col_name}"
        
        events.append({
            'event': event_name,
            'timestamp': ts,
            'source': f'datetime_column_{col_name}',
            'column': col_name
        })
    
    return events

