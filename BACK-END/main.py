"""
FastAPI Application for Database Profile Generator
Handles file uploads and generates comprehensive database profiles
"""

from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from typing import List, Any
import os
import pandas as pd
from pathlib import Path
import shutil
import uuid

try:
    import numpy as np
    _HAS_NUMPY = True
except ImportError:
    _HAS_NUMPY = False


def _to_native(x: Any) -> Any:
    """Convert numpy/pandas types to native Python for JSON serialization."""
    if _HAS_NUMPY and hasattr(np, 'integer') and isinstance(x, (np.integer, np.int64, np.int32)):
        return int(x)
    if _HAS_NUMPY and hasattr(np, 'floating') and isinstance(x, (np.floating, np.float64, np.float32)):
        return float(x)
    if _HAS_NUMPY and isinstance(x, np.bool_):
        return bool(x)
    if isinstance(x, dict):
        return {k: _to_native(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [_to_native(v) for v in x]
    return x

from models import DatabaseProfile, UploadResponse, AnalysisResponse, TableAnalysis
from csv_analyzer import CSVAnalyzer
from relationship_detector import RelationshipDetector

# Initialize FastAPI app
app = FastAPI(
    title="Database Profile Generator",
    description="Analyze CSV files and generate comprehensive database profiles",
    version="1.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create upload directory
UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)

from domain_classifier import DomainClassifier
from fuzzy_analyzer import FuzzyAnalyzer
from date_detector import DateColumnDetector
from customer_linker import CustomerLinker
from db_grouping_engine import DBGroupingEngine
from login_analyzer import LoginWorkflowAnalyzer

# Initialize analyzers
csv_analyzer = CSVAnalyzer()
relationship_detector = RelationshipDetector()
domain_classifier = DomainClassifier()
fuzzy_analyzer = FuzzyAnalyzer()
date_detector = DateColumnDetector()
customer_linker = CustomerLinker()
db_grouping_engine = DBGroupingEngine()
login_analyzer = LoginWorkflowAnalyzer()


# Store session data (in production, use proper session management)
sessions = {}





@app.post("/api/upload", response_model=UploadResponse)
async def upload_files(files: List[UploadFile] = File(...)):
    """
    Upload CSV files for analysis
    
    Args:
        files: List of CSV files
        
    Returns:
        UploadResponse with session ID and file list
    """
    if not files:
        raise HTTPException(status_code=400, detail="No files provided")
    
    # Create session
    session_id = str(uuid.uuid4())
    session_dir = UPLOAD_DIR / session_id
    session_dir.mkdir(exist_ok=True)
    
    uploaded_files = []
    
    # Save each file
    for file in files:
        if not file.filename.endswith('.csv'):
            raise HTTPException(
                status_code=400, 
                detail=f"File {file.filename} is not a CSV file"
            )
        
        file_path = session_dir / file.filename
        
        # Save file
        with file_path.open("wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        uploaded_files.append(file.filename)
    
    # Store session info
    sessions[session_id] = {
        "files": uploaded_files,
        "directory": str(session_dir)
    }
    
    # Return session ID in the message for frontend to extract
    return UploadResponse(
        success=True,
        message=f"SESSION_ID:{session_id}",
        file_count=len(uploaded_files),
        files=uploaded_files
    )


@app.post("/api/analyze/{session_id}", response_model=AnalysisResponse)
async def analyze_database(session_id: str):
    print(f"Analyzing session: {session_id}")
    """
    Analyze uploaded CSV files and generate database profile
    
    Args:
        session_id: Session ID from upload
        
    Returns:
        AnalysisResponse with complete database profile
    """
    # Get session
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session not found")
    
    session = sessions[session_id]
    session_dir = Path(session["directory"])
    
    try:
        # Analyze each table
        tables: List[TableAnalysis] = []
        dataframes: dict[str, pd.DataFrame] = {}
        
        for filename in session["files"]:
            file_path = session_dir / filename
            
            # Extract table name from filename
            table_name = filename.replace('.csv', '').replace('_', ' ').title()
            
            # Analyze the table
            table_analysis = csv_analyzer.analyze_table(str(file_path), table_name)
            
            # --- Logic Moved to Cluster Level ---
            # We no longer do fuzzy/age analysis per table here to avoid duplicates.
            
            tables.append(table_analysis)
            
            # Read dataframe for relationship detection
            input_df = pd.read_csv(file_path)
            dataframes[table_name] = input_df
        
        # Detect relationships across ALL tables
        all_relationships = relationship_detector.detect_relationships(tables, dataframes)
        
        # Group tables into clusters (logical databases)
        clusters = relationship_detector.group_tables_into_clusters(tables, all_relationships)
        
        profiles = []
        
        # Generate profile for each cluster
        for i, cluster_tables in enumerate(clusters):
            # Filter relationships relevant to this cluster
            cluster_table_names = set(t.table_name for t in cluster_tables)
            cluster_relationships = [
                r for r in all_relationships 
                if r.source_table in cluster_table_names and r.target_table in cluster_table_names
            ]
            
            # Generate database name
            db_name = _infer_database_name(cluster_tables)
            if len(clusters) > 1:
                db_name = f"{db_name} {i+1}"
            
            # Calculate database-level metrics
            total_rows = sum(t.row_count for t in cluster_tables)
            
            # Identify key columns
            key_columns = _identify_key_columns(cluster_tables, cluster_relationships)
            
            # Calculate overall quality
            overall_quality = sum(t.data_quality_score for t in cluster_tables) / len(cluster_tables) if cluster_tables else 0
            
            # Generate metrics and explanations
            quality_highlights = _generate_quality_highlights(cluster_tables)
            unusual_patterns = _detect_unusual_patterns(cluster_tables, cluster_relationships)
            db_explanation = _generate_database_explanation(cluster_tables, cluster_relationships, db_name)
            relationship_explanation = relationship_detector.generate_relationship_summary(cluster_relationships)
            
            # Domain Classification (ML) - Enhanced with split summary
            all_columns = []
            for t in cluster_tables:
                for c in t.columns:
                    all_columns.append(c.column_name)
            
            domain_result = domain_classifier.get_domain_split_summary(
                table_names=[t.table_name for t in cluster_tables],
                all_columns=all_columns
            )
            
            # Update DB Name if confident
            if domain_result.get("is_banking", False):
                db_name = f"Banking Database ({domain_result.get('confidence', 0):.1f}%)"
                if len(clusters) > 1:
                    db_name += f" {i+1}"
            
            # --- Single Pass Analysis per DB Cluster ---
            # Rules: Healthcare domain ONLY runs new healthcare analyzer. Others (Banking, General) do NOT run old features.
            segmentation_result = None
            age_result = None
            transaction_result = None
            credit_analysis_result = None
            login_analysis_result = None

            is_healthcare = domain_result.get('primary_domain') == 'Healthcare'

            # Banking/Other: Skip all old features (balance, age, transaction, credit, login)
            if not is_healthcare:
                pass  # No banking-specific analysis
            else:
                # Healthcare: Skip banking features; healthcare_analysis runs below
                pass

            # Healthcare Analysis (ONLY for Healthcare domain - domain classifier rules unchanged)
            healthcare_analysis_result = None
            if domain_result.get('primary_domain') == 'Healthcare':
                try:
                    from healthcare_analyzer import HealthcareAnalyzer
                    healthcare_analyzer = HealthcareAnalyzer()
                    healthcare_analysis_result = healthcare_analyzer.analyze_cluster(
                        tables=cluster_tables,
                        dataframes=dataframes,
                        relationships=cluster_relationships
                    )
                    if healthcare_analysis_result and healthcare_analysis_result.get('success'):
                        print(f"[Healthcare Analysis] Success for cluster {i+1}")
                    else:
                        print(f"[Healthcare Analysis] No healthcare data found in cluster {i+1}")
                except Exception as he:
                    print(f"[Healthcare Analysis] Error in cluster {i+1}: {str(he)}")



            # Create profile (sanitize dicts so numpy types serialize to JSON)
            profile = DatabaseProfile(
                database_name=db_name,
                tables=cluster_tables,
                relationships=cluster_relationships,
                total_tables=len(cluster_tables),
                total_rows=total_rows,
                key_columns=key_columns,
                overall_quality_score=round(overall_quality, 2),
                quality_highlights=quality_highlights,
                unusual_patterns=unusual_patterns,
                domain_analysis=_to_native(domain_result) if domain_result else None,
                credit_analysis=_to_native(credit_analysis_result) if credit_analysis_result else None,
                login_analysis=_to_native(login_analysis_result) if login_analysis_result else None,
                account_age_analysis=_to_native(age_result) if age_result else None,
                segmentation_analysis=_to_native(segmentation_result) if segmentation_result else None,
                transaction_analysis=_to_native(transaction_result) if transaction_result else None,
                healthcare_analysis=_to_native(healthcare_analysis_result) if healthcare_analysis_result else None,
                database_explanation=db_explanation,
                relationship_explanation=relationship_explanation
            )
            profiles.append(profile)
        
        return AnalysisResponse(
            success=True,
            message=f"Analysis completed successfully. Detected {len(profiles)} logical database(s).",
            profiles=profiles
        )
    
    except Exception as e:
        return AnalysisResponse(
            success=False,
            message="Analysis failed",
            error=str(e)
        )


def _infer_database_name(tables: List[TableAnalysis]) -> str:
    """Infer database name from table names"""
    table_names = [t.table_name.lower() for t in tables]
    
    # Check for common domains with expanded keywords
    if any(k in name for name in table_names for k in ['customer', 'client', 'account', 'transaction', 'bank', 'balance']):
        return "Banking Database"
    elif any(k in name for name in table_names for k in ['order', 'product', 'sale', 'cart', 'invoice', 'sku']):
        return "E-Commerce Database"
    elif any(k in name for name in table_names for k in ['employee', 'staff', 'salary', 'payroll', 'hr_', 'job']):
        return "HR Database"
    elif any(k in name for name in table_names for k in ['student', 'course', 'grade', 'school', 'class', 'teacher']):
        return "Education Database"
    elif any(k in name for name in table_names for k in ['patient', 'doctor', 'hospital', 'medical', 'diagnosis', 'treatment']):
        return "Healthcare Database"
    
    return "Custom Database"


def _identify_key_columns(tables: List[TableAnalysis], relationships) -> List[str]:
    """Identify key columns across the entire database"""
    key_cols = set()
    
    # Add primary key candidates
    for table in tables:
        for pk in table.primary_key_candidates[:1]:  # Just the first one
            key_cols.add(f"{table.table_name}.{pk}")
    
    # Add foreign key columns
    for rel in relationships:
        if rel.is_foreign_key:
            key_cols.add(f"{rel.target_table}.{rel.target_column}")
    
    return list(key_cols)[:10]  # Limit to 10


def _generate_quality_highlights(tables: List[TableAnalysis]) -> List[str]:
    """Generate highlights about data quality"""
    highlights = []
    
    # Check overall completeness
    avg_completeness = 100 - sum(
        sum(col.null_percentage for col in t.columns) / len(t.columns)
        for t in tables
    ) / len(tables)
    
    if avg_completeness > 95:
        highlights.append("✓ Excellent data completeness across all tables")
    elif avg_completeness < 70:
        highlights.append("⚠ Some tables have significant missing data")
    
    # Check for tables with primary keys
    tables_with_pk = sum(1 for t in tables if t.primary_key_candidates)
    if tables_with_pk == len(tables):
        highlights.append("✓ All tables have identifiable primary keys")
    elif tables_with_pk < len(tables) / 2:
        highlights.append("⚠ Some tables lack clear primary keys")
    
    # Check for critical columns
    total_critical = sum(len(t.critical_columns) for t in tables)
    if total_critical > 0:
        highlights.append(f"ℹ {total_critical} critical columns identified across database")
    
    return highlights


def _detect_unusual_patterns(tables: List[TableAnalysis], relationships) -> List[str]:
    """Detect unusual patterns in the database"""
    patterns = []
    
    # Check for orphan tables (no relationships)
    if len(tables) > 1 and len(relationships) == 0:
        patterns.append("⚠ No relationships detected - tables appear isolated")
    
    # Check for tables with many anomalies
    for table in tables:
        anomaly_count = sum(len(col.anomalies) for col in table.columns)
        if anomaly_count > len(table.columns):
            patterns.append(f"⚠ {table.table_name} has multiple column anomalies")
    
    # Check for very small tables
    small_tables = [t.table_name for t in tables if t.row_count < 10]
    if small_tables:
        patterns.append(f"ℹ Small datasets: {', '.join(small_tables)} (< 10 rows)")
    
    return patterns


def _generate_database_explanation(tables: List[TableAnalysis], 
                                   relationships, db_name: str) -> str:
    """Generate step-by-step explanation for beginners (HTML format)"""
    table_names = [t.table_name for t in tables]
    table_list_str = ", ".join(f"<strong>{name}</strong>" for name in table_names)
    
    html_parts = [
        f"<div style='margin-bottom: 1rem;'><strong>Welcome to your {db_name} Analysis!</strong></div>",
        f"<div style='margin-bottom: 0.5rem;'>We've grouped these specific files together because they form a cohesive data structure.</div>",
        
        f"<h5 style='color: #b4b4c5; margin: 1rem 0 0.5rem;'>📊 Database Overview</h5>",
        f"<ul style='list-style-type: none; padding-left: 0; margin-bottom: 1rem;'>",
        f"  <li style='margin-bottom: 0.3rem;'>• This logical database consists of <strong>{len(tables)}</strong> table(s): {table_list_str}.</li>",
        f"  <li style='margin-bottom: 0.3rem;'>• Total Data Volume: <strong>{sum(t.row_count for t in tables):,}</strong> rows.</li>",
        f"</ul>"
    ]
    
    if relationships:
        html_parts.extend([
            f"<h5 style='color: #b4b4c5; margin: 1rem 0 0.5rem;'>🔗 Relationship Logic (Why these are split together)</h5>",
            f"<ul style='list-style-type: none; padding-left: 0; margin-bottom: 1rem;'>"
        ])
        
        # Explain specific links that bind the cluster
        shown_count = 0
        for r in relationships:
            if shown_count >= 5: # Limit detailed breakdown
                break
                
            # Create a nice sentence for the relationship
            if r.is_foreign_key:
                sentence = f"Table <strong>{r.source_table}</strong> links to <strong>{r.target_table}</strong> via the <code>{r.source_column}</code> column."
            else:
                sentence = f"<strong>{r.source_table}</strong> and <strong>{r.target_table}</strong> share common data in <code>{r.source_column}</code>."
                
            html_parts.append(f"  <li style='margin-bottom: 0.4rem;'>• {sentence}</li>")
            shown_count += 1
            
        html_parts.append(f"  <li>• These connections are strong evidence that these files belong to the same system.</li>")
        html_parts.append("</ul>")
    else:
         html_parts.extend([
            f"<h5 style='color: #b4b4c5; margin: 1rem 0 0.5rem;'>🔗 Connectivity</h5>",
            f"<ul style='list-style-type: none; padding-left: 0; margin-bottom: 1rem;'>",
            f"  <li>• These tables appear to function independently as we didn't detect strong identifiable links.</li>",
            f"</ul>"
        ])
    
    html_parts.extend([
        "<h5 style='color: #b4b4c5; margin: 1rem 0 0.5rem;'>📈 Context & Purpose</h5>",
        "<div style='margin-bottom: 0.5rem;'>Based on the columns found, here is the role of each file:</div>",
        "<ul style='list-style-type: none; padding-left: 0; margin-bottom: 1rem;'>"
    ])
    
    for table in tables:
        purpose = table.purpose if table.purpose else "Stores structured data records."
        html_parts.append(f"  <li style='margin-bottom: 0.3rem;'>• <strong>{table.table_name}</strong>: {purpose}</li>")
    
    html_parts.append("</ul>")
    
    return "\n".join(html_parts)


@app.post("/api/detect-date-columns/{session_id}")
async def detect_date_columns(session_id: str):
    """
    Detect potential date columns in uploaded files using fuzzy logic.
    
    Args:
        session_id: Session ID from upload
        
    Returns:
        List of date column candidates with confidence scores and intelligent questions
    """
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session not found")
    
    session = sessions[session_id]
    session_dir = Path(session["directory"])
    
    try:
        all_candidates = []
        all_id_candidates = []
        table_names = []
        dataframes = {}
        
        for filename in session["files"]:
            file_path = session_dir / filename
            df = pd.read_csv(file_path)
            table_name = filename.replace('.csv', '').replace('_', ' ').title()
            table_names.append(table_name)
            dataframes[table_name] = df
            
            # Detect date columns
            candidates = date_detector.find_date_columns(df, table_name)
            all_candidates.extend(candidates)
            
            # Detect ID columns
            id_cols = date_detector.find_id_columns(df)
            for id_col in id_cols:
                all_id_candidates.append({'column': id_col, 'table': table_name})
        
        # Detect login timestamp columns (separate from open_date)
        login_candidates = date_detector.find_login_timestamp_columns(dataframes)
        
        # Generate intelligent questions
        questions = date_detector.generate_confirmation_questions(
            all_candidates,
            [c['column'] for c in all_id_candidates],
            table_names
        )
        
        return {
            'success': True,
            'date_candidates': all_candidates,
            'id_candidates': all_id_candidates,
            'login_candidates': login_candidates,
            'questions': questions,
            'message': f"Found {len(all_candidates)} potential date columns"
        }
    
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'message': 'Date column detection failed'
        }


@app.post("/api/analyze-accounts/{session_id}")
async def analyze_accounts(session_id: str, date_column: str = None, id_column: str = None, activity_column: str = None):
    """
    Analyze account age, growth trends, and inactive customers.
    
    Args:
        session_id: Session ID from upload
        date_column: Name of the date column (e.g., 'open_date', 'created_at')
        id_column: Name of the customer ID column
        activity_column: Optional activity column for inactive detection
        
    Returns:
        Comprehensive account analysis with age segments, growth, peaks, and inactive users
    """
    from fastapi import Query
    
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session not found")
    
    session = sessions[session_id]
    session_dir = Path(session["directory"])
    
    # If no columns specified, try auto-detection
    if not date_column or not id_column:
        return {
            'success': False,
            'error': 'Please specify date_column and id_column parameters',
            'message': 'Use /api/detect-date-columns first to find candidates'
        }
    
    try:
        # Find the file containing these columns
        account_df = None
        account_table_name = None
        all_dataframes = {}
        
        for filename in session["files"]:
            file_path = session_dir / filename
            df = pd.read_csv(file_path)
            table_name = filename.replace('.csv', '').replace('_', ' ').title()
            all_dataframes[table_name] = df
            
            if date_column in df.columns and id_column in df.columns:
                account_df = df
                account_table_name = table_name
        
        if account_df is None:
            return {
                'success': False,
                'error': f"Could not find table with both '{date_column}' and '{id_column}' columns",
                'message': 'Please verify column names'
            }
        
        # Validate date column
        validation = date_detector.validate_date_column(account_df, date_column)
        if not validation['valid']:
            return {
                'success': False,
                'error': validation['error'],
                'message': 'Date column validation failed'
            }
        
        # Perform age analysis
        # NEW: First perform login analysis if possible
        login_metrics = None
        has_login_data = False
        daily_login_analysis = None
        
        # 1. Detect Login Data across all tables
        login_candidates = date_detector.find_login_timestamp_columns(all_dataframes)
        
        if login_candidates:
            # Use the highest confidence candidate
            best_candidate = login_candidates[0]
            login_table_name = best_candidate['table_name']
            login_col = best_candidate['column_name']
            
            print(f"Detected Login Data: Table '{login_table_name}', Column '{login_col}'")
            
            login_df = all_dataframes.get(login_table_name)
            
            if login_df is not None:
                # We need an ID column to link. Try to find one.
                # If the main id_column exists in login table, perfect.
                target_id_col = None
                
                if id_column in login_df.columns:
                    target_id_col = id_column
                else:
                    # Look for other ID candidates
                    possible_ids = date_detector.find_id_columns(login_df)
                    if possible_ids:
                        target_id_col = possible_ids[0]
                
                if target_id_col:
                    try:
                        login_metrics = login_analyzer.calculate_login_delay(
                            accounts_df=account_df,
                            logins_df=login_df,
                            open_col=date_column,
                            login_col=login_col,
                            id_col=target_id_col # Note: Assumes values match even if column names differ
                        )
                        has_login_data = True
                    except Exception as e:
                        print(f"Login analysis failed: {str(e)}")
                    try:
                        daily_login_analysis = login_analyzer.analyze_daily_logins_with_account_age(
                            accounts_df=account_df,
                            logins_df=login_df,
                            open_col=date_column,
                            login_col=login_col,
                            link_col=target_id_col,
                        )
                    except Exception as e:
                        print(f"Daily login analysis failed: {str(e)}")
                        daily_login_analysis = None
                else:
                    daily_login_analysis = None
        else:
            daily_login_analysis = None

        age_analysis = fuzzy_analyzer.analyze_account_age(
            df=account_df, 
            date_col=date_column, 
            id_col=id_column,
            login_metrics=login_metrics
        )
        age_analysis['analyzed_table'] = account_table_name
        age_analysis['validation'] = validation
        
        # Link activity data from other tables (for inactive detection)
        activity_tables = []
        for table_name, df in all_dataframes.items():
            if table_name != account_table_name:
                # Check if this might be an activity table
                has_login_indicators = any('login' in col.lower() or 'auth' in col.lower() for col in df.columns)
                has_transaction_indicators = any('transaction' in col.lower() or 'amount' in col.lower() for col in df.columns)
                
                if has_login_indicators or has_transaction_indicators:
                    # Try to find matching ID column
                    common_id = customer_linker.find_common_id_column(account_df, df)
                    if common_id:
                        activity_tables.append({
                            'name': table_name,
                            'dataframe': df,
                            'id_column': common_id[1]  # Column name in activity table
                        })
        
        # Link customer activity
        linked_data = None
        if activity_tables:
            linked_data = customer_linker.link_customer_activity(
                account_df,
                activity_tables,
                id_column
            )
        
        # Detect inactive customers with cross-table data
        inactive_result = fuzzy_analyzer.detect_inactive_customers(
            df=account_df,
            date_col=date_column,
            id_col=id_column,
            age_threshold_days=365,
            linked_activity=linked_data
        )
        
        # Identify multi-account holders
        id_counts = account_df[id_column].value_counts()
        multi_account_holders = id_counts[id_counts > 1].index.tolist()
        
        # NEW: Detect same-day multiple account creations
        same_day_result = fuzzy_analyzer.detect_same_day_accounts(
            df=account_df,
            date_col=date_column,
            id_col=id_column,
            include_timestamps=True
        )
        
        # Open date timeline: one point per date, with creations (customer_id, time, Morning/Evening/Night)
        open_date_timeline = []
        open_date_has_time = False
        try:
            acc = account_df.copy()
            time_col = None
            for c in acc.columns:
                if c != date_column and 'time' in c.lower() and 'stamp' not in c.lower():
                    time_col = c
                    break
            if time_col and time_col in acc.columns:
                acc['_dt'] = pd.to_datetime(acc[date_column].astype(str) + ' ' + acc[time_col].astype(str), errors='coerce')
            else:
                acc['_dt'] = pd.to_datetime(acc[date_column], errors='coerce')
            valid = acc.dropna(subset=['_dt']).copy()
            if not valid.empty:
                valid = valid.sort_values('_dt')
                has_time = valid['_dt'].dt.time.apply(lambda x: x != pd.Timestamp(0).time()).any()
                open_date_has_time = bool(has_time)
                valid['_date_only'] = valid['_dt'].dt.strftime('%Y-%m-%d')
                valid['_time_str'] = valid['_dt'].dt.strftime('%H:%M') if has_time else ""
                def _time_of_day(h):
                    if 5 <= h < 12: return "Morning"
                    if 12 <= h < 17: return "Afternoon"
                    if 17 <= h < 21: return "Evening"
                    return "Night"
                valid['_time_of_day'] = valid['_dt'].dt.hour.apply(_time_of_day)
                by_date = valid.groupby('_date_only')
                dates_sorted = sorted(valid['_date_only'].unique())
                peak_date_only = valid.groupby('_date_only').size().idxmax() if len(valid) > 0 else None
                dates_with_multi = set(str(c.get('date', '')) for c in same_day_result.get('same_day_customers', []) if c.get('date'))
                for idx, d in enumerate(dates_sorted):
                    day_df = by_date.get_group(d)
                    is_first = (idx == 0)
                    is_last = (idx == len(dates_sorted) - 1)
                    is_peak_day = (peak_date_only == d)
                    creations = []
                    cust_counts = {}
                    for _, r in day_df.iterrows():
                        cid = str(r.get(id_column, r.get('customer_id', '')))
                        cust_counts[cid] = cust_counts.get(cid, 0) + 1
                        creations.append({
                            "customer_id": cid,
                            "account_id": str(r.get('account_id', r.get(id_column, ''))),
                            "time_str": r.get('_time_str', ''),
                            "time_of_day": r.get('_time_of_day', ''),
                            "created_at": r['_dt'].strftime('%Y-%m-%d %H:%M') if has_time and hasattr(r['_dt'], 'strftime') else d,
                        })
                    multi_create_customers = [k for k, v in cust_counts.items() if v > 1]
                    brief_expl = f"On {d}: {len(creations)} account(s) created."
                    full_expl = f"Date {d}: {len(creations)} account(s). " + (f"Customer(s) {', '.join(multi_create_customers)} created 2+ accounts this day. " if multi_create_customers else "") + "Morning 5-12, Afternoon 12-17, Evening 17-21, Night 21-5."
                    open_date_timeline.append({
                        "date": d,
                        "date_time": d,
                        "count": len(creations),
                        "creations": creations,
                        "multi_create_customers": multi_create_customers,
                        "multi_create_same_day": len(multi_create_customers) > 0,
                        "is_first": is_first,
                        "is_last": is_last,
                        "is_peak_day": is_peak_day,
                        "brief_explanation": brief_expl,
                        "full_explanation": full_expl,
                    })
        except Exception as e:
            print(f"[open_date_timeline] {e}")
        
        # Multi-table linking: we used customer_id to link activity from other files (logical join)
        used_multi_table_activity = linked_data is not None and not linked_data.empty

        detected_login_col = None
        detected_login_table = None
        if login_candidates:
             detected_login_col = login_candidates[0].get('column_name')
             detected_login_table = login_candidates[0].get('table_name')

        # Summary for timeline line diagram: active/inactive counts, peak date/time, brief + full explanations
        total_accounts = len(account_df)
        active_count = age_analysis.get("counts", {}).get("NEW", 0) + age_analysis.get("counts", {}).get("ACTIVE", 0) + age_analysis.get("counts", {}).get("TRUSTED", 0)
        inactive_count = inactive_result.get("count", 0) if inactive_result else 0
        peak_entry = next((e for e in open_date_timeline if e.get("is_peak_day")), None)
        same_day = same_day_result or {}
        timeline_diagram_summary = {
            "total_accounts": total_accounts,
            "active_count": active_count,
            "inactive_count": inactive_count,
            "peak_date_time": peak_entry.get("date_time") if peak_entry else age_analysis.get("peak_date_str"),
            "peak_count": peak_entry.get("count") if peak_entry else age_analysis.get("peak_count"),
            "peak_explanation": "Peak activity: most accounts opened on this date/time. Often linked to a campaign or promotion." if peak_entry else "No single peak day.",
            "peak_brief": f"On {peak_entry.get('date_time', '')}, {peak_entry.get('count', 0)} account(s) opened — the busiest day." if peak_entry else "No single peak day.",
            "peak_full": (
                f"<strong>For beginners:</strong> We used the <strong>{date_column}</strong> column and counted how many accounts opened each day. "
                f"The peak day is <strong>{peak_entry.get('date_time', '')}</strong> with <strong>{peak_entry.get('count', 0)}</strong> accounts created. "
                "This often links to a campaign or promotion. Use it to see what drove signups."
            ) if peak_entry else "No single day stood out with more opens than others.",
            "timeline_brief": f"We use the {date_column} column. Line shows when accounts were created: Start (first) to End (last). Each node = one date/time, number = how many opened.",
            "timeline_full": (
                f"<strong>For beginners:</strong> The <strong>{date_column}</strong> column tells us when each account was created. "
                f"We list every distinct date (and time if your data has hours) from earliest to latest. "
                f"<strong>Stats:</strong> {total_accounts} total accounts, {active_count} active, {inactive_count} inactive. "
                f"Each node shows: at this date/time, this many accounts were opened. "
                f"Green = Start (first created), Red = End (last created), Purple = Peak (most opens that day). "
                f"👥 = at least one user created multiple accounts on that date."
            ),
            "multi_account_brief": same_day.get("brief_explanation", ""),
            "multi_account_full": same_day.get("full_explanation", ""),
            "multi_account_exists": same_day.get("total_affected", 0) > 0,
            "open_column": date_column,
            "open_table": account_table_name,
        }

        # Transaction Timeline: only when a transaction table exists (has amount, type, date)
        transaction_timeline = None
        for tname, tdf in all_dataframes.items():
            try:
                tx_res = fuzzy_analyzer.analyze_transaction_timeline(tdf)
                if tx_res.get("has_data") and tx_res.get("daily"):
                    transaction_timeline = tx_res
                    break
            except Exception as e:
                continue

        out = {
            "success": True,
            "age_analysis": age_analysis,
            "inactive_customers": inactive_result,
            "multi_account_holders": multi_account_holders[:20],
            "same_day_accounts": same_day_result,
            "login_metrics": login_metrics,
            "has_login_data": has_login_data,
            "open_column": date_column,
            "open_table": account_table_name,
            "login_column": login_metrics.get("login_column") if login_metrics else detected_login_col,
            "login_table": detected_login_table,
            "open_date_timeline": open_date_timeline,
            "open_date_has_time": open_date_has_time,
            "used_multi_table_activity": used_multi_table_activity,
            "timeline_diagram_summary": timeline_diagram_summary,
            "daily_login_analysis": daily_login_analysis,
            "transaction_timeline": transaction_timeline,
        }
        return _to_native(out)
    
    except Exception as e:
        import traceback
        return {
            'success': False,
            'error': str(e),
            'traceback': traceback.format_exc(),
            'message': 'Account analysis failed'
        }


@app.post("/api/analyze_credit/{session_id}")
async def analyze_credit(session_id: str):
    """
    Analyze credit amounts by time slot
    
    Args:
        session_id: Session ID from upload
        
    Returns:
        Credit analysis results with time slot breakdown
    """
    from credit_analyzer import CreditTimeSlotAnalyzer
    
    # Get session
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session not found")
    
    session = sessions[session_id]
    session_dir = Path(session["directory"])
    
    try:
        # Find CSV file with credit/transaction data
        results = []
        
        for filename in session["files"]:
            file_path = session_dir / filename
            df = pd.read_csv(file_path)
            
            # Auto-detect timestamp and credit columns
            timestamp_col = None
            credit_col = None
            
            # Look for timestamp column
            for col in df.columns:
                if any(keyword in col.lower() for keyword in ['time', 'date', 'timestamp']):
                    timestamp_col = col
                    break
            
            # Look for credit amount column
            for col in df.columns:
                if any(keyword in col.lower() for keyword in ['credit', 'amount']):
                    credit_col = col
                    break
            
            # Skip files without required columns
            if not timestamp_col or not credit_col:
                continue
            
            # Analyze this file
            analyzer = CreditTimeSlotAnalyzer()
            analysis = analyzer.analyze_credit_by_timeslot(df, timestamp_col, credit_col)
            
            results.append({
                'filename': filename,
                'analysis': analysis
            })
        
        if not results:
            return {
                'success': False,
                'message': 'No files found with timestamp and credit_amount columns',
                'error': 'Please ensure your CSV has columns for timestamp and credit amounts'
            }
        
        return {
            'success': True,
            'message': f'Credit analysis completed for {len(results)} file(s)',
            'results': results
        }
    
    except Exception as e:
        return {
            'success': False,
            'message': 'Credit analysis failed',
            'error': str(e)
        }



# Mount static files at root to serve index.html, styles.css, and app.js
frontend_dir = Path(__file__).parent.parent / "FRONT-END"
if frontend_dir.exists():
    app.mount("/", StaticFiles(directory=str(frontend_dir), html=True), name="frontend")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
