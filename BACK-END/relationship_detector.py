"""
Relationship Detection Engine
Detects relationships between tables by analyzing common columns and data patterns
"""

import pandas as pd
from typing import List, Dict, Tuple
from models import RelationshipInfo, TableAnalysis, ColumnAnalysis


class RelationshipDetector:
    """Detects and analyzes relationships between tables"""
    
    def __init__(self):
        self.common_fk_patterns = ['id', '_id', 'code', '_code', 'key', '_key']
    
    def detect_relationships(self, tables: List[TableAnalysis], 
                           dataframes: Dict[str, pd.DataFrame]) -> List[RelationshipInfo]:
        """
        Detect relationships between all tables
        
        Args:
            tables: List of analyzed tables
            dataframes: Dictionary mapping table names to their DataFrames
            
        Returns:
            List of detected relationships
        """
        relationships = []
        
        # Compare each pair of tables
        for i, table1 in enumerate(tables):
            for table2 in tables[i+1:]:
                rels = self._find_relationships_between_tables(
                    table1, table2, 
                    dataframes.get(table1.table_name),
                    dataframes.get(table2.table_name)
                )
                relationships.extend(rels)
        
        return relationships
    
    def _find_relationships_between_tables(self, table1: TableAnalysis, table2: TableAnalysis,
                                          df1: pd.DataFrame, df2: pd.DataFrame) -> List[RelationshipInfo]:
        """Find relationships between two specific tables"""
        relationships = []
        
        if df1 is None or df2 is None:
            return relationships
        
        # Get column names from both tables
        cols1 = {col.column_name: col for col in table1.columns}
        cols2 = {col.column_name: col for col in table2.columns}
        
        # Find common column names
        common_cols = set(cols1.keys()) & set(cols2.keys())
        
        # Check each common column
        for col_name in common_cols:
            rel = self._analyze_column_relationship(
                table1, table2, col_name,
                cols1[col_name], cols2[col_name],
                df1, df2
            )
            if rel:
                relationships.append(rel)
        
        # Check for ID-based relationships (customer_id in one table, id in another)
        relationships.extend(self._find_id_based_relationships(table1, table2, cols1, cols2, df1, df2))
        
        return relationships
    
    def _analyze_column_relationship(self, table1: TableAnalysis, table2: TableAnalysis,
                                    col_name: str, col1: ColumnAnalysis, col2: ColumnAnalysis,
                                    df1: pd.DataFrame, df2: pd.DataFrame) -> RelationshipInfo:
        """Analyze relationship for a common column"""
        
        # Get values from both columns
        values1 = set(df1[col_name].dropna().unique())
        values2 = set(df2[col_name].dropna().unique())
        
        if len(values1) == 0 or len(values2) == 0:
            return None
        
        # Calculate overlap
        overlap = values1 & values2
        overlap_pct = len(overlap) / max(len(values1), len(values2))
        
        # Need significant overlap to consider it a relationship
        if overlap_pct < 0.1:
            return None
        
        # Determine relationship type and direction
        is_pk1 = col_name in table1.primary_key_candidates
        is_pk2 = col_name in table2.primary_key_candidates
        
        # Determine which is parent and which is child
        source_table = table1.table_name
        target_table = table2.table_name
        is_fk = False
        
        # If one is a primary key, it's likely the parent
        if is_pk1 and not is_pk2:
            source_table = table1.table_name
            target_table = table2.table_name
            is_fk = True
        elif is_pk2 and not is_pk1:
            source_table = table2.table_name
            target_table = table1.table_name
            is_fk = True
        
        # Analyze cardinality
        card1 = len(values1)
        card2 = len(values2)
        unique1 = col1.is_unique
        unique2 = col2.is_unique
        
        if unique1 and unique2:
            rel_type = "1:1"
        elif unique1 or unique2:
            rel_type = "1:N"
        else:
            rel_type = "N:M"
        
        # Calculate confidence
        confidence = self._calculate_confidence(overlap_pct, is_pk1 or is_pk2, col_name)
        
        # Generate explanation
        explanation = self._generate_relationship_explanation(
            source_table, target_table, col_name, rel_type, overlap_pct
        )
        
        return RelationshipInfo(
            source_table=source_table,
            target_table=target_table,
            source_column=col_name,
            target_column=col_name,
            relationship_type=rel_type,
            confidence=confidence,
            is_primary_key=is_pk1 or is_pk2,
            is_foreign_key=is_fk,
            explanation=explanation
        )
    
    def _find_id_based_relationships(self, table1: TableAnalysis, table2: TableAnalysis,
                                    cols1: Dict[str, ColumnAnalysis], cols2: Dict[str, ColumnAnalysis],
                                    df1: pd.DataFrame, df2: pd.DataFrame) -> List[RelationshipInfo]:
        """Find relationships based on ID naming patterns (e.g., customer_id -> id)"""
        relationships = []
        
        # Check if table1 has a column like "table2_id"
        for col1_name, col1 in cols1.items():
            col1_lower = col1_name.lower()
            
            # Check if this looks like a foreign key
            if not any(pattern in col1_lower for pattern in self.common_fk_patterns):
                continue
            
            # Try to match with table2
            table2_lower = table2.table_name.lower()
            
            # Check if column name contains table name
            if table2_lower in col1_lower or any(
                table2_lower.startswith(word) for word in col1_lower.split('_')
            ):
                # Find potential matching column in table2
                for col2_name, col2 in cols2.items():
                    if col2_name in table2.primary_key_candidates:
                        # Check value overlap
                        values1 = set(df1[col1_name].dropna().unique())
                        values2 = set(df2[col2_name].dropna().unique())
                        
                        if len(values1) > 0 and len(values2) > 0:
                            overlap = values1 & values2
                            overlap_pct = len(overlap) / max(len(values1), len(values2))
                            
                            if overlap_pct > 0.5:
                                confidence = self._calculate_confidence(overlap_pct, True, col1_name)
                                
                                explanation = (
                                    f"{table1.table_name} references {table2.table_name} "
                                    f"through {col1_name} → {col2_name} "
                                    f"({overlap_pct*100:.1f}% match)"
                                )
                                
                                relationships.append(RelationshipInfo(
                                    source_table=table2.table_name,
                                    target_table=table1.table_name,
                                    source_column=col2_name,
                                    target_column=col1_name,
                                    relationship_type="1:N",
                                    confidence=confidence,
                                    is_primary_key=True,
                                    is_foreign_key=True,
                                    explanation=explanation
                                ))
        
        return relationships
    
    def _calculate_confidence(self, overlap_pct: float, is_pk: bool, col_name: str) -> float:
        """Calculate confidence score for a relationship"""
        confidence = overlap_pct * 0.6  # Base confidence from overlap
        
        # Boost if it's a primary key
        if is_pk:
            confidence += 0.2
        
        # Boost if column name suggests it's a key
        col_lower = col_name.lower()
        if any(pattern in col_lower for pattern in self.common_fk_patterns):
            confidence += 0.2
        
        return min(confidence, 1.0)
    
    def _generate_relationship_explanation(self, source: str, target: str, 
                                          col_name: str, rel_type: str, overlap_pct: float) -> str:
        """Generate beginner-friendly explanation of the relationship"""
        
        if rel_type == "1:1":
            explanation = (
                f"Each record in {source} has exactly one matching record in {target} "
                f"based on {col_name}. This is a one-to-one relationship."
            )
        elif rel_type == "1:N":
            explanation = (
                f"Each record in {source} can have multiple related records in {target} "
                f"linked by {col_name}. This is a one-to-many relationship."
            )
        else:
            explanation = (
                f"{source} and {target} have a many-to-many relationship "
                f"through {col_name}."
            )
        
        explanation += f" ({overlap_pct*100:.1f}% of values match between tables)"
        
        return explanation
    
    
    def group_tables_into_clusters(self, tables: List[TableAnalysis], relationships: List[RelationshipInfo]) -> List[List[TableAnalysis]]:
        """
        Group tables into clusters based on relationships (connected components)
        
        Args:
            tables: List of all analyzed tables
            relationships: List of all detected relationships
            
        Returns:
            List of table clusters (each cluster is a list of TableAnalysis)
        """
        if not tables:
            return []
            
        # Initialize graph adjacency list
        adj = {table.table_name: set() for table in tables}
        table_map = {table.table_name: table for table in tables}
        
        # Build graph from relationships
        for rel in relationships:
            adj[rel.source_table].add(rel.target_table)
            adj[rel.target_table].add(rel.source_table)
            
        # Find connected components
        visited = set()
        clusters = []
        
        for table_name in adj:
            if table_name not in visited:
                component = []
                stack = [table_name]
                visited.add(table_name)
                
                while stack:
                    node = stack.pop()
                    component.append(table_map[node])
                    
                    for neighbor in adj[node]:
                        if neighbor not in visited:
                            visited.add(neighbor)
                            stack.append(neighbor)
                
                clusters.append(component)
                
        return clusters

    def generate_relationship_summary(self, relationships: List[RelationshipInfo]) -> str:
        """Generate a summary of all relationships for beginners"""
        if not relationships:
            return "No relationships detected between tables. Each table appears to be independent."
        
        summary_parts = [
            f"Found {len(relationships)} relationship(s) between tables:",
            ""
        ]
        
        # Group by parent table
        by_parent: Dict[str, List[RelationshipInfo]] = {}
        for rel in relationships:
            if rel.source_table not in by_parent:
                by_parent[rel.source_table] = []
            by_parent[rel.source_table].append(rel)
        
        # Describe each parent-child relationship
        for parent, rels in by_parent.items():
            children = [f"{r.target_table} (via {r.target_column})" for r in rels]
            summary_parts.append(
                f"• {parent} is linked to: {', '.join(children)}"
            )
        
        summary_parts.append("")
        summary_parts.append(
            "These relationships allow you to join data across tables "
            "to get a complete picture of your information."
        )
        
        return "\n".join(summary_parts)
