"""
Database Schema Analysis and Synthetic Data Generation
Analyzes uploaded CSV files to understand schema, data types, ranges, and relationships.
Generates synthetic data that maintains PK/FK relationships and follows observed patterns.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
import random
import string
from datetime import datetime, timedelta
import re
from faker import Faker


class SchemaAnalyzer:
    """Analyzes database schema to extract column types, ranges, and patterns"""
    
    def __init__(self):
        self.schema_info = {}
        self.faker = Faker()
    
    def analyze_schema(self, session_dir: Path, files: List[str]) -> Dict[str, Any]:
        """
        Analyze all uploaded files to extract schema information
        
        Args:
            session_dir: Directory containing uploaded CSV files
            files: List of CSV filenames
            
        Returns:
            Dictionary with schema information for each table
        """
        schema = {}
        
        for filename in files:
            file_path = session_dir / filename
            table_name = filename.replace('.csv', '').replace('_', ' ').title()
            
            try:
                df = pd.read_csv(file_path)
                table_schema = self._analyze_table_schema(df, table_name)
                schema[table_name] = table_schema
            except Exception as e:
                print(f"Error analyzing {filename}: {e}")
                continue
        
        return schema
    
    def _analyze_table_schema(self, df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
        """Analyze a single table's schema"""
        schema = {
            'table_name': table_name,
            'row_count': len(df),
            'columns': []
        }
        
        for col in df.columns:
            col_info = self._analyze_column(df, col)
            schema['columns'].append(col_info)
        
        # Identify primary key candidates (unique, non-null columns)
        pk_candidates = []
        for col_info in schema['columns']:
            if col_info['is_unique'] and col_info['null_percentage'] < 5:
                pk_candidates.append(col_info['column_name'])
        
        schema['primary_key_candidates'] = pk_candidates
        if pk_candidates:
            schema['primary_key'] = pk_candidates[0]  # Use first candidate
        
        return schema
    
    def _analyze_column(self, df: pd.DataFrame, col: str) -> Dict[str, Any]:
        """Analyze a single column to determine type, range, and patterns"""
        col_info = {
            'column_name': col,
            'data_type': None,
            'null_percentage': (df[col].isna().sum() / len(df)) * 100,
            'unique_count': df[col].nunique(),
            'is_unique': df[col].nunique() == len(df),
            'total_count': len(df),
            'pattern_type': None,  # email, account_number, amount, balance, temperature, etc.
            'pattern_details': {}
        }
        
        # Get sample values for pattern detection and reuse
        non_null_values = df[col].dropna().head(100).astype(str).tolist()
        sample_values = [str(v) for v in non_null_values[:50]]  # Store more samples for reuse
        col_info['sample_values'] = sample_values[:10]  # For display
        col_info['all_sample_values'] = sample_values  # For data generation (reuse observed values)
        
        # Determine data type
        dtype = df[col].dtype
        
        if pd.api.types.is_integer_dtype(dtype):
            col_info['data_type'] = 'integer'
            col_info['min_value'] = int(df[col].min()) if not df[col].isna().all() else None
            col_info['max_value'] = int(df[col].max()) if not df[col].isna().all() else None
            col_info['avg_value'] = float(df[col].mean()) if not df[col].isna().all() else None
            
            # Detect account number pattern (numeric, specific length range)
            if col_info['min_value'] is not None:
                num_digits = len(str(col_info['min_value']))
                if 8 <= num_digits <= 20 and col_info['is_unique']:
                    col_info['pattern_type'] = 'account_number'
                    col_info['pattern_details'] = {
                        'min_digits': num_digits,
                        'max_digits': len(str(col_info['max_value'])) if col_info['max_value'] else num_digits
                    }
            
        elif pd.api.types.is_float_dtype(dtype):
            col_info['data_type'] = 'float'
            col_info['min_value'] = float(df[col].min()) if not df[col].isna().all() else None
            col_info['max_value'] = float(df[col].max()) if not df[col].isna().all() else None
            col_info['avg_value'] = float(df[col].mean()) if not df[col].isna().all() else None
            
            # Detect amount/balance pattern (positive, decimal places)
            col_lower = col.lower()
            if any(keyword in col_lower for keyword in ['amount', 'balance', 'price', 'cost', 'value', 'total', 'sum']):
                col_info['pattern_type'] = 'amount'
                col_info['pattern_details'] = {
                    'has_decimals': True,
                    'min_value': col_info['min_value'],
                    'max_value': col_info['max_value'],
                    'avg_value': col_info['avg_value']
                }
            # Detect temperature pattern (typical range -50 to 150)
            elif any(keyword in col_lower for keyword in ['temp', 'temperature', 'celsius', 'fahrenheit']):
                col_info['pattern_type'] = 'temperature'
                col_info['pattern_details'] = {
                    'unit': 'celsius' if 'celsius' in col_lower or 'c' in col_lower else 'fahrenheit',
                    'min_value': col_info['min_value'],
                    'max_value': col_info['max_value']
                }
            elif col_info['min_value'] is not None and -50 <= col_info['min_value'] <= 150 and col_info['max_value'] is not None and -50 <= col_info['max_value'] <= 150:
                col_info['pattern_type'] = 'temperature'
                col_info['pattern_details'] = {
                    'unit': 'celsius',
                    'min_value': col_info['min_value'],
                    'max_value': col_info['max_value']
                }
            
        elif pd.api.types.is_bool_dtype(dtype):
            col_info['data_type'] = 'boolean'
            
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            col_info['data_type'] = 'date'
            if not df[col].isna().all():
                col_info['min_date'] = str(df[col].min())
                col_info['max_date'] = str(df[col].max())
        else:
            # String type - detect patterns
            col_info['data_type'] = 'string'
            lengths = df[col].astype(str).str.len()
            col_info['min_length'] = int(lengths.min()) if len(lengths) > 0 else 0
            col_info['max_length'] = int(lengths.max()) if len(lengths) > 0 else 0
            col_info['avg_length'] = float(lengths.mean()) if len(lengths) > 0 else 0
            
            # Pattern detection for strings
            col_lower = col.lower()
            
            # Email pattern detection
            email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
            email_matches = sum(1 for v in sample_values if email_pattern.match(v))
            if email_matches >= len(sample_values) * 0.7:  # 70% match rate
                col_info['pattern_type'] = 'email'
                # Extract unique domains from sample values
                domains = []
                for v in sample_values:
                    if '@' in v:
                        domain = v.split('@')[1]
                        if domain and domain not in domains:
                            domains.append(domain)
                col_info['pattern_details'] = {
                    'domain_patterns': domains[:5]
                }
            # Account number pattern (alphanumeric, specific format)
            elif any(keyword in col_lower for keyword in ['account', 'acc', 'id', 'number', 'num']):
                # Check if it's numeric string or alphanumeric
                if all(v.isdigit() or (v.replace('-', '').replace('_', '').isdigit()) for v in sample_values[:5] if v):
                    col_info['pattern_type'] = 'account_number'
                    col_info['pattern_details'] = {
                        'format': 'numeric',
                        'has_separators': any('-' in v or '_' in v for v in sample_values[:5])
                    }
                elif all(re.match(r'^[A-Z0-9-]+$', v.upper()) for v in sample_values[:5] if v):
                    col_info['pattern_type'] = 'account_number'
                    col_info['pattern_details'] = {
                        'format': 'alphanumeric',
                        'has_separators': any('-' in v or '_' in v for v in sample_values[:5])
                    }
            # Phone number pattern
            elif any(keyword in col_lower for keyword in ['phone', 'mobile', 'tel', 'contact']):
                phone_pattern = re.compile(r'^[\d\s\-\+\(\)]+$')
                if all(phone_pattern.match(v) for v in sample_values[:5] if v):
                    col_info['pattern_type'] = 'phone_number'
                    col_info['pattern_details'] = {
                        'format': 'standard'
                    }
            # Name pattern detection (for Faker usage)
            elif any(keyword in col_lower for keyword in ['name', 'first_name', 'last_name', 'user_name', 'username', 'full_name', 'customer_name', 'patient_name']):
                col_info['pattern_type'] = 'name'
                col_info['pattern_details'] = {
                    'name_type': 'first_name' if 'first' in col_lower else 
                                'last_name' if 'last' in col_lower else
                                'full_name' if 'full' in col_lower else 'name'
                }
        
        return col_info


class SyntheticDataGenerator:
    """Generates synthetic data based on schema analysis"""
    
    def __init__(self):
        self.schema = {}
        self.relationships = []
        self.generated_data = {}
        self.pk_values = {}  # Store generated PK values for FK relationships
        self.faker = Faker()
    
    def generate_synthetic_data(
        self, 
        schema: Dict[str, Any], 
        relationships: List[Dict[str, Any]],
        num_rows_per_table: Optional[Dict[str, int]] = None
    ) -> Dict[str, pd.DataFrame]:
        """
        Generate synthetic data for all tables maintaining relationships
        
        Args:
            schema: Schema information from SchemaAnalyzer
            relationships: List of relationship dictionaries
            num_rows_per_table: Optional dict specifying rows per table
            
        Returns:
            Dictionary of table_name -> DataFrame with synthetic data
        """
        self.schema = schema
        self.relationships = relationships
        self.generated_data = {}
        self.pk_values = {}
        
        # Determine row counts
        if num_rows_per_table is None:
            num_rows_per_table = {}
            for table_name, table_schema in schema.items():
                # Generate 50-200 rows by default, or match original if small
                original_rows = table_schema.get('row_count', 100)
                num_rows_per_table[table_name] = min(max(50, original_rows), 200)
        
        # Sort tables by dependency (tables with FKs come after their referenced tables)
        sorted_tables = self._topological_sort(schema, relationships)
        
        # Generate data for each table in dependency order
        for table_name in sorted_tables:
            if table_name not in schema:
                continue
            
            num_rows = num_rows_per_table.get(table_name, 100)
            df = self._generate_table_data(table_name, schema[table_name], num_rows, relationships)
            self.generated_data[table_name] = df
        
        return self.generated_data
    
    def _topological_sort(self, schema: Dict[str, Any], relationships: List[Dict[str, Any]]) -> List[str]:
        """Sort tables so parent tables (referenced by FKs) come before child tables"""
        # Build dependency graph
        dependencies = {}
        all_tables = set(schema.keys())
        
        for rel in relationships:
            source = rel.get('source_table')
            target = rel.get('target_table')
            if source in all_tables and target in all_tables:
                if target not in dependencies:
                    dependencies[target] = []
                dependencies[target].append(source)
        
        # Topological sort
        sorted_list = []
        visited = set()
        temp_visited = set()
        
        def visit(table):
            if table in temp_visited:
                return  # Cycle detected, skip
            if table in visited:
                return
            
            temp_visited.add(table)
            for dep in dependencies.get(table, []):
                if dep in all_tables:
                    visit(dep)
            temp_visited.remove(table)
            visited.add(table)
            sorted_list.append(table)
        
        for table in all_tables:
            if table not in visited:
                visit(table)
        
        return sorted_list
    
    def _generate_table_data(
        self, 
        table_name: str, 
        table_schema: Dict[str, Any], 
        num_rows: int,
        relationships: List[Dict[str, Any]]
    ) -> pd.DataFrame:
        """Generate synthetic data for a single table"""
        data = {}
        pk_column = table_schema.get('primary_key')
        pk_values = []
        
        # Generate data for each column
        for col_info in table_schema['columns']:
            col_name = col_info['column_name']
            data_type = col_info['data_type']
            
            # Check if this column is a foreign key
            fk_info = self._find_foreign_key(table_name, col_name, relationships)
            
            if fk_info:
                # Generate FK values from referenced table
                ref_table = fk_info['target_table']
                ref_col = fk_info['target_column']
                if ref_table in self.pk_values:
                    values = random.choices(self.pk_values[ref_table], k=num_rows)
                else:
                    # Generate random values matching the type
                    values = self._generate_column_values(col_info, num_rows)
            elif col_name == pk_column:
                # Generate unique PK values
                values = self._generate_primary_key_values(col_info, num_rows)
                pk_values = values
            else:
                # Generate regular column values
                values = self._generate_column_values(col_info, num_rows)
            
            data[col_name] = values
        
        # Store PK values for FK references
        if pk_column and pk_values:
            self.pk_values[table_name] = pk_values
        
        return pd.DataFrame(data)
    
    def _find_foreign_key(
        self, 
        table_name: str, 
        column_name: str, 
        relationships: List[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        """Find if a column is a foreign key"""
        for rel in relationships:
            if (rel.get('source_table') == table_name and 
                rel.get('source_column') == column_name):
                return rel
        return None
    
    def _generate_primary_key_values(self, col_info: Dict[str, Any], num_rows: int) -> List[Any]:
        """Generate unique primary key values"""
        data_type = col_info['data_type']
        
        if data_type == 'integer':
            start = col_info.get('min_value', 1) if col_info.get('min_value') is not None else 1
            return list(range(start, start + num_rows))
        elif data_type == 'string':
            prefix = col_info['column_name'][:3].upper()
            return [f"{prefix}{i:06d}" for i in range(1, num_rows + 1)]
        else:
            return self._generate_column_values(col_info, num_rows)
    
    def _generate_column_values(self, col_info: Dict[str, Any], num_rows: int) -> List[Any]:
        """Generate values for a column based on its type, observed range, and detected patterns"""
        data_type = col_info['data_type']
        pattern_type = col_info.get('pattern_type')
        null_percentage = col_info.get('null_percentage', 0)
        num_nulls = int((null_percentage / 100) * num_rows)
        
        # Generate based on detected pattern first
        if pattern_type == 'email':
            values = self._generate_email_values(col_info, num_rows - num_nulls)
        elif pattern_type == 'account_number':
            values = self._generate_account_number_values(col_info, num_rows - num_nulls)
        elif pattern_type == 'phone_number':
            values = self._generate_phone_number_values(col_info, num_rows - num_nulls)
        elif pattern_type == 'amount':
            values = self._generate_amount_values(col_info, num_rows - num_nulls)
        elif pattern_type == 'temperature':
            values = self._generate_temperature_values(col_info, num_rows - num_nulls)
        elif pattern_type == 'name':
            values = self._generate_name_values(col_info, num_rows - num_nulls)
        elif data_type == 'integer':
            min_val = col_info.get('min_value', 0) if col_info.get('min_value') is not None else 0
            max_val = col_info.get('max_value', 1000) if col_info.get('max_value') is not None else 1000
            values = [random.randint(int(min_val), int(max_val)) for _ in range(num_rows - num_nulls)]
        elif data_type == 'float':
            min_val = col_info.get('min_value', 0.0) if col_info.get('min_value') is not None else 0.0
            max_val = col_info.get('max_value', 1000.0) if col_info.get('max_value') is not None else 1000.0
            values = [round(random.uniform(float(min_val), float(max_val)), 2) for _ in range(num_rows - num_nulls)]
        elif data_type == 'boolean':
            values = [random.choice([True, False]) for _ in range(num_rows - num_nulls)]
        elif data_type == 'date':
            min_date = col_info.get('min_date')
            max_date = col_info.get('max_date')
            
            if min_date and max_date:
                try:
                    min_dt = pd.to_datetime(min_date)
                    max_dt = pd.to_datetime(max_date)
                except:
                    min_dt = datetime.now() - timedelta(days=365)
                    max_dt = datetime.now()
            else:
                min_dt = datetime.now() - timedelta(days=365)
                max_dt = datetime.now()
            
            date_range = (max_dt - min_dt).days
            values = [
                (min_dt + timedelta(days=random.randint(0, date_range))).strftime('%Y-%m-%d')
                for _ in range(num_rows - num_nulls)
            ]
        else:  # string - USE OBSERVED VALUES, NOT RANDOM GENERATION
            # Reuse observed values from user's data
            observed_values = col_info.get('all_sample_values', [])
            if observed_values and len(observed_values) > 0:
                # Cycle through observed values, with some randomness
                values = []
                for _ in range(num_rows - num_nulls):
                    # Randomly select from observed values (with replacement)
                    values.append(random.choice(observed_values))
            else:
                # Fallback: if no observed values, use empty string
                values = ['' for _ in range(num_rows - num_nulls)]
        
        # Add nulls and shuffle
        values.extend([None] * num_nulls)
        random.shuffle(values)
        return values
    
    def _generate_email_values(self, col_info: Dict[str, Any], num_rows: int) -> List[str]:
        """Generate email addresses based on observed patterns"""
        pattern_details = col_info.get('pattern_details', {})
        domains = pattern_details.get('domain_patterns', ['example.com', 'test.com', 'sample.org'])
        
        # Common first names and last names for realistic emails
        first_names = ['john', 'jane', 'mike', 'sarah', 'david', 'emily', 'chris', 'lisa', 'robert', 'maria']
        last_names = ['smith', 'johnson', 'williams', 'brown', 'jones', 'garcia', 'miller', 'davis', 'rodriguez', 'martinez']
        
        values = []
        for i in range(num_rows):
            first = random.choice(first_names)
            last = random.choice(last_names)
            num = random.randint(1, 999) if random.random() < 0.3 else None
            domain = random.choice(domains) if domains else 'example.com'
            
            if num:
                email = f"{first}.{last}{num}@{domain}"
            else:
                email = f"{first}.{last}@{domain}"
            values.append(email)
        
        return values
    
    def _generate_account_number_values(self, col_info: Dict[str, Any], num_rows: int) -> List[str]:
        """Generate account numbers based on observed format"""
        pattern_details = col_info.get('pattern_details', {})
        data_type = col_info['data_type']
        has_separators = pattern_details.get('has_separators', False)
        
        if data_type == 'integer':
            min_val = col_info.get('min_value', 10000000) if col_info.get('min_value') is not None else 10000000
            max_val = col_info.get('max_value', 999999999999) if col_info.get('max_value') is not None else 999999999999
            values = [str(random.randint(int(min_val), int(max_val))) for _ in range(num_rows)]
        else:
            # String account numbers
            format_type = pattern_details.get('format', 'alphanumeric')
            min_len = col_info.get('min_length', 8) if col_info.get('min_length') is not None else 8
            max_len = col_info.get('max_length', 16) if col_info.get('max_length') is not None else 16
            
            values = []
            for i in range(num_rows):
                length = random.randint(min_len, max_len)
                if format_type == 'numeric':
                    acc_num = ''.join(random.choices(string.digits, k=length))
                else:
                    acc_num = ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))
                
                if has_separators and random.random() < 0.5:
                    # Add separator every 4 characters
                    acc_num = '-'.join([acc_num[j:j+4] for j in range(0, len(acc_num), 4)])
                
                values.append(acc_num)
        
        return values
    
    def _generate_phone_number_values(self, col_info: Dict[str, Any], num_rows: int) -> List[str]:
        """Generate phone numbers"""
        values = []
        for i in range(num_rows):
            # Generate 10-digit phone number
            area = random.randint(200, 999)
            exchange = random.randint(200, 999)
            number = random.randint(1000, 9999)
            values.append(f"{area}-{exchange}-{number}")
        return values
    
    def _generate_amount_values(self, col_info: Dict[str, Any], num_rows: int) -> List[float]:
        """Generate amount/balance values based on observed range"""
        min_val = col_info.get('min_value', 0.0) if col_info.get('min_value') is not None else 0.0
        max_val = col_info.get('max_value', 10000.0) if col_info.get('max_value') is not None else 10000.0
        avg_val = col_info.get('avg_value', (min_val + max_val) / 2) if col_info.get('avg_value') is not None else (min_val + max_val) / 2
        
        # Use normal distribution around average for more realistic values
        std_dev = (max_val - min_val) / 4
        values = []
        for i in range(num_rows):
            val = np.random.normal(avg_val, std_dev)
            val = max(min_val, min(max_val, val))  # Clamp to range
            values.append(round(val, 2))
        
        return values
    
    def _generate_temperature_values(self, col_info: Dict[str, Any], num_rows: int) -> List[float]:
        """Generate temperature values based on observed range"""
        min_val = col_info.get('min_value', 0.0) if col_info.get('min_value') is not None else 0.0
        max_val = col_info.get('max_value', 100.0) if col_info.get('max_value') is not None else 100.0
        unit = col_info.get('pattern_details', {}).get('unit', 'celsius')
        
        values = []
        for i in range(num_rows):
            temp = random.uniform(float(min_val), float(max_val))
            values.append(round(temp, 1))
        
        return values
    
    def _generate_name_values(self, col_info: Dict[str, Any], num_rows: int) -> List[str]:
        """Generate name values using Faker library"""
        pattern_details = col_info.get('pattern_details', {})
        name_type = pattern_details.get('name_type', 'name')
        
        values = []
        for i in range(num_rows):
            if name_type == 'first_name':
                values.append(self.faker.first_name())
            elif name_type == 'last_name':
                values.append(self.faker.last_name())
            elif name_type == 'full_name':
                values.append(self.faker.name())
            else:
                # Default: use full name
                values.append(self.faker.name())
        
        return values


class DataSynthesisEngine:
    """Main engine for database synthesis"""
    
    def __init__(self):
        self.schema_analyzer = SchemaAnalyzer()
        self.data_generator = SyntheticDataGenerator()
    
    def synthesize_database(
        self, 
        session_dir: Path, 
        files: List[str],
        relationships: List[Dict[str, Any]],
        num_rows_per_table: Optional[Dict[str, int]] = None
    ) -> Dict[str, Any]:
        """
        Complete synthesis pipeline: analyze schema and generate synthetic data
        
        Args:
            session_dir: Directory containing CSV files
            files: List of CSV filenames
            relationships: List of relationship dictionaries
            num_rows_per_table: Optional row counts per table
            
        Returns:
            Dictionary with schema info and generated dataframes
        """
        # Step 1: Analyze schema
        print(f"Analyzing schema for {len(files)} files...")
        schema = self.schema_analyzer.analyze_schema(session_dir, files)
        
        # Step 2: Generate synthetic data
        print(f"Generating synthetic data...")
        synthetic_data = self.data_generator.generate_synthetic_data(
            schema, 
            relationships, 
            num_rows_per_table
        )
        
        # Convert DataFrames to dictionaries for JSON serialization
        synthetic_data_dict = {}
        for table_name, df in synthetic_data.items():
            synthetic_data_dict[table_name] = {
                'columns': df.columns.tolist(),
                'rows': df.values.tolist(),
                'row_count': len(df)
            }
        
        return {
            'schema': schema,
            'synthetic_data': synthetic_data_dict,
            'relationships': relationships
        }

