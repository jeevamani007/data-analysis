"""
CSV Analysis Engine
Analyzes CSV files to detect column types, calculate statistics, identify patterns, and flag anomalies
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Any, Tuple
from datetime import datetime
from models import ColumnAnalysis, TableAnalysis, ColumnType, Pattern
import re


class CSVAnalyzer:
    """Analyzes CSV files and generates detailed profiles"""
    
    def __init__(self):
        self.date_patterns = [
            r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
            r'\d{2}/\d{2}/\d{4}',  # MM/DD/YYYY
            r'\d{2}-\d{2}-\d{4}',  # DD-MM-YYYY
        ]
    
    def analyze_table(self, file_path: str, table_name: str) -> TableAnalysis:
        """
        Analyze a CSV file and return complete table analysis
        
        Args:
            file_path: Path to CSV file
            table_name: Name to assign to this table
            
        Returns:
            TableAnalysis object with complete analysis
        """
        # Read CSV file
        df = pd.read_csv(file_path)
        
        # Analyze each column
        column_analyses = []
        for col in df.columns:
            analysis = self._analyze_column(df, col)
            column_analyses.append(analysis)
        
        # Identify primary key candidates
        pk_candidates = self._identify_primary_keys(df, column_analyses)
        
        # Determine table purpose
        purpose = self._infer_table_purpose(table_name, column_analyses)
        
        # Identify critical columns
        critical_cols = self._identify_critical_columns(column_analyses)
        
        # Calculate data quality score
        quality_score, quality_notes = self._calculate_data_quality(column_analyses)
        
        return TableAnalysis(
            table_name=table_name,
            file_name=file_path.split('\\')[-1],
            row_count=len(df),
            column_count=len(df.columns),
            columns=column_analyses,
            primary_key_candidates=pk_candidates,
            purpose=purpose,
            critical_columns=critical_cols,
            data_quality_score=quality_score,
            data_quality_notes=quality_notes
        )
    
    def _analyze_column(self, df: pd.DataFrame, column_name: str) -> ColumnAnalysis:
        """Analyze a single column"""
        col_data = df[column_name]
        total_count = len(col_data)
        
        # Calculate null percentage
        null_count = col_data.isnull().sum()
        null_pct = (null_count / total_count * 100) if total_count > 0 else 0
        
        # Get unique values
        unique_count = int(col_data.nunique())
        is_unique = bool(unique_count == total_count - null_count)
        
        # Detect column type
        col_type = self._detect_column_type(col_data)
        
        # Initialize stats
        min_val, max_val, avg_val = None, None, None
        min_len, max_len, avg_len = None, None, None
        
        # Calculate statistics based on type
        if col_type in [ColumnType.INTEGER, ColumnType.FLOAT]:
            numeric_data = pd.to_numeric(col_data.dropna(), errors='coerce')
            if len(numeric_data) > 0:
                min_val = float(numeric_data.min())
                max_val = float(numeric_data.max())
                avg_val = float(numeric_data.mean())
        
        if col_type == ColumnType.STRING:
            str_data = col_data.dropna().astype(str)
            if len(str_data) > 0:
                lengths = str_data.str.len()
                min_len = int(lengths.min())
                max_len = int(lengths.max())
                avg_len = float(lengths.mean())
        
        # Detect pattern
        pattern = self._detect_pattern(col_data, col_type)
        
        # Detect anomalies
        anomalies = self._detect_anomalies(col_data, col_type, null_pct, unique_count, total_count)
        
        # Generate notes
        notes = self._generate_column_notes(column_name, col_type, unique_count, total_count, pattern)
        
        # Get sample values
        sample_values = col_data.dropna().head(5).tolist()
        
        return ColumnAnalysis(
            column_name=column_name,
            column_type=col_type,
            null_percentage=round(null_pct, 2),
            unique_count=unique_count,
            is_unique=is_unique,
            total_count=total_count,
            min_value=min_val,
            max_value=max_val,
            avg_value=round(avg_val, 2) if avg_val else None,
            max_length=max_len,
            min_length=min_len,
            avg_length=round(avg_len, 2) if avg_len else None,
            pattern=pattern,
            anomalies=anomalies,
            notes=notes,
            sample_values=sample_values
        )
    
    def _detect_column_type(self, col_data: pd.Series) -> ColumnType:
        """Detect the type of a column"""
        # Remove nulls for type detection
        non_null = col_data.dropna()
        
        if len(non_null) == 0:
            return ColumnType.UNKNOWN
        
        # Check for boolean
        if non_null.dtype == bool or set(non_null.unique()).issubset({0, 1, True, False, 'true', 'false', 'True', 'False'}):
            return ColumnType.BOOLEAN
        
        # Check for integer
        if pd.api.types.is_integer_dtype(non_null):
            return ColumnType.INTEGER
        
        # Check for float
        if pd.api.types.is_float_dtype(non_null):
            return ColumnType.FLOAT
        
        # Check for date
        sample = non_null.head(100).astype(str)
        date_matches = sum(any(re.match(pattern, str(val)) for pattern in self.date_patterns) for val in sample)
        if date_matches / len(sample) > 0.8:
            return ColumnType.DATE
        
        # Try parsing as datetime
        try:
            pd.to_datetime(non_null.head(100))
            return ColumnType.DATE
        except:
            pass
        
        return ColumnType.STRING
    
    def _detect_pattern(self, col_data: pd.Series, col_type: ColumnType) -> Pattern:
        """Detect patterns in the data"""
        non_null = col_data.dropna()
        
        if len(non_null) < 2:
            return Pattern.RANDOM
        
        # Check for constant values
        if non_null.nunique() == 1:
            return Pattern.CONSTANT
        
        # Check for categorical (low cardinality)
        if non_null.nunique() / len(non_null) < 0.1:
            return Pattern.CATEGORICAL
        
        # For numeric columns, check for sequential patterns
        if col_type in [ColumnType.INTEGER, ColumnType.FLOAT]:
            numeric_data = pd.to_numeric(non_null, errors='coerce').dropna()
            if len(numeric_data) > 2:
                diffs = numeric_data.diff().dropna()
                
                # Check for ascending
                if (diffs > 0).sum() / len(diffs) > 0.9:
                    # Check if sequential (diff of 1)
                    if (diffs == 1).sum() / len(diffs) > 0.9:
                        return Pattern.SEQUENTIAL
                    return Pattern.ASCENDING
                
                # Check for descending
                if (diffs < 0).sum() / len(diffs) > 0.9:
                    return Pattern.DESCENDING
        
        return Pattern.RANDOM
    
    def _detect_anomalies(self, col_data: pd.Series, col_type: ColumnType, 
                          null_pct: float, unique_count: int, total_count: int) -> List[str]:
        """Detect anomalies in the column"""
        anomalies = []
        
        # High null percentage
        if null_pct > 50:
            anomalies.append(f"High missing data: {null_pct:.1f}% of values are null")
        
        # All unique values (might be ID)
        if unique_count == total_count and total_count > 1:
            anomalies.append("All values are unique - likely an identifier column")
        
        # Very low cardinality
        if unique_count == 1 and total_count > 1:
            anomalies.append("All values are identical - constant column")
        
        # For numeric columns, check for outliers
        if col_type in [ColumnType.INTEGER, ColumnType.FLOAT]:
            numeric_data = pd.to_numeric(col_data.dropna(), errors='coerce')
            if len(numeric_data) > 0:
                Q1 = numeric_data.quantile(0.25)
                Q3 = numeric_data.quantile(0.75)
                IQR = Q3 - Q1
                outliers = ((numeric_data < Q1 - 1.5 * IQR) | (numeric_data > Q3 + 1.5 * IQR)).sum()
                if outliers > 0:
                    outlier_pct = (outliers / len(numeric_data)) * 100
                    if outlier_pct > 5:
                        anomalies.append(f"Contains {outliers} outliers ({outlier_pct:.1f}%)")
        
        return anomalies
    
    def _generate_column_notes(self, col_name: str, col_type: ColumnType, 
                               unique_count: int, total_count: int, pattern: Pattern) -> str:
        """Generate human-readable notes about the column"""
        notes = []
        
        # Infer purpose from column name
        col_lower = col_name.lower()
        if 'id' in col_lower:
            notes.append("Appears to be an identifier column")
        elif 'name' in col_lower:
            notes.append("Contains name information")
        elif 'date' in col_lower or 'time' in col_lower:
            notes.append("Stores date/time information")
        elif 'amount' in col_lower or 'balance' in col_lower or 'price' in col_lower:
            notes.append("Contains financial/monetary values")
        elif 'email' in col_lower:
            notes.append("Stores email addresses")
        elif 'phone' in col_lower:
            notes.append("Contains phone numbers")
        elif 'address' in col_lower:
            notes.append("Location/address information")
        elif 'status' in col_lower or 'type' in col_lower or 'category' in col_lower:
            notes.append("Categorical classification field")
        
        # Add pattern info
        if pattern == Pattern.SEQUENTIAL:
            notes.append("Values follow sequential order")
        elif pattern == Pattern.CATEGORICAL:
            notes.append(f"Has {unique_count} distinct categories")
        
        return "; ".join(notes) if notes else f"{col_type.value} column with {unique_count} unique values"
    
    def _identify_primary_keys(self, df: pd.DataFrame, columns: List[ColumnAnalysis]) -> List[str]:
        """Identify potential primary key columns"""
        candidates = []
        
        for col in columns:
            # Primary key should be unique and have no nulls
            if col.is_unique and col.null_percentage == 0:
                # Prefer ID columns
                if 'id' in col.column_name.lower():
                    candidates.insert(0, col.column_name)
                else:
                    candidates.append(col.column_name)
        
        return candidates
    
    def _infer_table_purpose(self, table_name: str, columns: List[ColumnAnalysis]) -> str:
        """Infer the purpose of the table based on name and columns"""
        name_lower = table_name.lower()
        
        # Common table patterns
        if 'customer' in name_lower or 'client' in name_lower:
            return "Stores customer information and profiles"
        elif 'account' in name_lower:
            return "Contains account details and balances"
        elif 'transaction' in name_lower:
            return "Records financial transactions and activities"
        elif 'order' in name_lower:
            return "Tracks customer orders and purchases"
        elif 'product' in name_lower or 'item' in name_lower:
            return "Maintains product catalog and inventory"
        elif 'employee' in name_lower or 'staff' in name_lower:
            return "Stores employee information and records"
        elif 'payment' in name_lower:
            return "Records payment and billing information"
        
        # Analyze column names
        col_names = [c.column_name.lower() for c in columns]
        if any('balance' in c or 'amount' in c for c in col_names):
            return "Stores financial data and balances"
        elif any('email' in c or 'phone' in c for c in col_names):
            return "Contains contact and communication information"
        
        return f"Data table with {len(columns)} columns"
    
    def _identify_critical_columns(self, columns: List[ColumnAnalysis]) -> List[str]:
        """Identify the most important columns in the table"""
        critical = []
        
        for col in columns:
            col_lower = col.column_name.lower()
            
            # IDs are critical
            if 'id' in col_lower and col.is_unique:
                critical.append(col.column_name)
            # Financial columns
            elif any(keyword in col_lower for keyword in ['balance', 'amount', 'price', 'total']):
                critical.append(col.column_name)
            # Status columns
            elif 'status' in col_lower:
                critical.append(col.column_name)
            # Date columns
            elif col.column_type == ColumnType.DATE:
                critical.append(col.column_name)
        
        return critical[:5]  # Limit to top 5
    
    def _calculate_data_quality(self, columns: List[ColumnAnalysis]) -> Tuple[float, str]:
        """Calculate overall data quality score and notes"""
        total_score = 0
        issues = []
        
        for col in columns:
            col_score = 100
            
            # Deduct for nulls
            col_score -= col.null_percentage * 0.5
            
            # Deduct for anomalies
            col_score -= len(col.anomalies) * 10
            
            total_score += max(col_score, 0)
        
        avg_score = total_score / len(columns) if columns else 0
        
        # Generate notes
        high_null_cols = [c.column_name for c in columns if c.null_percentage > 20]
        if high_null_cols:
            issues.append(f"High missing data in: {', '.join(high_null_cols[:3])}")
        
        anomaly_cols = [c.column_name for c in columns if len(c.anomalies) > 0]
        if len(anomaly_cols) > 3:
            issues.append(f"{len(anomaly_cols)} columns have anomalies")
        
        if avg_score >= 90:
            quality_note = "Excellent data quality - clean and complete"
        elif avg_score >= 70:
            quality_note = "Good data quality with minor issues"
        elif avg_score >= 50:
            quality_note = "Moderate data quality - some cleaning recommended"
        else:
            quality_note = "Poor data quality - significant cleaning needed"
        
        if issues:
            quality_note += ". " + "; ".join(issues)
        
        return round(avg_score, 2), quality_note
