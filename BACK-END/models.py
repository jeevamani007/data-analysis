"""
Pydantic models for Database Profile Application
Defines data structures for CSV analysis, table profiling, and database summaries
"""

from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from enum import Enum


class ColumnType(str, Enum):
    """Column data types"""
    INTEGER = "Integer"
    FLOAT = "Float"
    STRING = "String"
    DATE = "Date"
    BOOLEAN = "Boolean"
    UNKNOWN = "Unknown"


class Pattern(str, Enum):
    """Data patterns detected in columns"""
    ASCENDING = "Ascending"
    DESCENDING = "Descending"
    RANDOM = "Random"
    CATEGORICAL = "Categorical"
    SEQUENTIAL = "Sequential"
    CONSTANT = "Constant"


class ColumnAnalysis(BaseModel):
    """Detailed analysis for a single column"""
    column_name: str = Field(..., description="Name of the column")
    column_type: ColumnType = Field(..., description="Detected data type")
    null_percentage: float = Field(..., description="Percentage of null/missing values")
    unique_count: int = Field(..., description="Number of unique values")
    is_unique: bool = Field(..., description="Whether all values are unique")
    total_count: int = Field(..., description="Total number of rows")
    
    # Numeric statistics
    min_value: Optional[float] = Field(None, description="Minimum value for numeric columns")
    max_value: Optional[float] = Field(None, description="Maximum value for numeric columns")
    avg_value: Optional[float] = Field(None, description="Average value for numeric columns")
    
    # String statistics
    max_length: Optional[int] = Field(None, description="Maximum string length")
    min_length: Optional[int] = Field(None, description="Minimum string length")
    avg_length: Optional[float] = Field(None, description="Average string length")
    
    # Pattern detection
    pattern: Pattern = Field(..., description="Detected pattern in the data")
    
    # Anomalies and notes
    anomalies: List[str] = Field(default_factory=list, description="List of detected anomalies")
    notes: str = Field("", description="Human-readable explanation of the column")
    
    # Sample values
    sample_values: List[Any] = Field(default_factory=list, description="Sample values from the column")


class RelationshipInfo(BaseModel):
    """Information about relationships between tables"""
    source_table: str = Field(..., description="Source table name")
    target_table: str = Field(..., description="Target table name")
    source_column: str = Field(..., description="Column in source table")
    target_column: str = Field(..., description="Column in target table")
    relationship_type: str = Field(..., description="Type of relationship (1:1, 1:N, N:M)")
    confidence: float = Field(..., description="Confidence score (0-1)")
    is_primary_key: bool = Field(False, description="Whether source column is a primary key")
    is_foreign_key: bool = Field(False, description="Whether it's a foreign key relationship")
    explanation: str = Field("", description="Simple explanation of the relationship")


class TableAnalysis(BaseModel):
    """Complete analysis for a single table"""
    table_name: str = Field(..., description="Name of the table (from filename)")
    file_name: str = Field(..., description="Original CSV filename")
    row_count: int = Field(..., description="Total number of rows")
    column_count: int = Field(..., description="Total number of columns")
    
    # Column analyses
    columns: List[ColumnAnalysis] = Field(..., description="Analysis for each column")
    
    # Primary key candidates
    primary_key_candidates: List[str] = Field(default_factory=list, description="Potential primary key columns")
    
    # Table purpose and summary
    purpose: str = Field("", description="Simple explanation of what this table stores")
    critical_columns: List[str] = Field(default_factory=list, description="Most important columns")
    data_quality_score: float = Field(0.0, description="Overall data quality score (0-100)")
    data_quality_notes: str = Field("", description="Notes about data quality")
    
    # Optional analysis results
    segmentation_analysis: Optional[Dict[str, Any]] = Field(None, description="Fuzzy logic segmentation results")
    account_age_analysis: Optional[Dict[str, Any]] = Field(None, description="Account age fuzzy logic results")


class DatabaseProfile(BaseModel):
    """Complete database profile with all tables and relationships"""
    database_name: str = Field(..., description="Name of the database")
    
    # Tables
    tables: List[TableAnalysis] = Field(..., description="All analyzed tables")
    
    # Relationships
    relationships: List[RelationshipInfo] = Field(default_factory=list, description="Detected relationships")
    
    # Database-level summary
    total_tables: int = Field(..., description="Total number of tables")
    total_rows: int = Field(0, description="Total rows across all tables")
    key_columns: List[str] = Field(default_factory=list, description="Important columns across database")
    
    # Quality and patterns
    overall_quality_score: float = Field(0.0, description="Overall database quality (0-100)")
    quality_highlights: List[str] = Field(default_factory=list, description="Key quality observations")
    unusual_patterns: List[str] = Field(default_factory=list, description="Unusual patterns detected")
    
    # Domain Analysis (ML)
    domain_analysis: Optional[Dict[str, Any]] = Field(None, description="ML-based domain classification results")
    
    # Credit Time Slot Analysis (for banking domains)
    credit_analysis: Optional[Dict[str, Any]] = Field(None, description="Credit amount time slot analysis results")
    
    # Unified Analysis (to show only once per DB)
    segmentation_analysis: Optional[Dict[str, Any]] = Field(None, description="Fuzzy logic segmentation results (DB Level)")
    account_age_analysis: Optional[Dict[str, Any]] = Field(None, description="Account age fuzzy logic results (DB Level)")
    
    # Explanations for beginners
    database_explanation: str = Field("", description="Step-by-step explanation of the database")
    relationship_explanation: str = Field("", description="Simple explanation of how tables relate")
    
    # Login Workflow Analysis
    login_analysis: Optional[Dict[str, Any]] = Field(None, description="Login workflow analysis results")

    # Transaction Analysis (Fuzzy Logic)
    transaction_analysis: Optional[Dict[str, Any]] = Field(None, description="Detailed transaction analysis results")

    # Healthcare Analysis (for healthcare domains)
    healthcare_analysis: Optional[Dict[str, Any]] = Field(None, description="Healthcare visit and department analysis results")

    # Banking Timeline Analysis (for banking domains)
    banking_analysis: Optional[Dict[str, Any]] = Field(None, description="Banking timeline sorted ascending for Start----|----|----End diagram")



class UploadResponse(BaseModel):
    """Response after file upload"""
    success: bool
    message: str
    file_count: int
    files: List[str]


class AnalysisResponse(BaseModel):
    """Response containing complete database profile"""
    success: bool
    message: str
    profiles: List[DatabaseProfile] = Field(default_factory=list, description="List of detected database profiles")
    error: Optional[str] = None
