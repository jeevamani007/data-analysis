from typing import Dict, List, Any
import pandas as pd
from domain_classifier import DomainClassifier

class DBGroupingEngine:
    """
    Groups tables/databases based on their domain classification.
    """
    
    def __init__(self):
        self.domain_classifier = DomainClassifier()
        
    def group_databases(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Group tables into Banking vs Other domains.
        
        Args:
            dataframes: Dictionary of table_name -> DataFrame
            
        Returns:
            Dictionary with grouping stats and details
        """
        banking_tables = []
        other_tables = []
        
        total_tables = len(dataframes)
        if total_tables == 0:
            return {
                "summary": {"banking_pct": 0, "other_pct": 0},
                "groups": {"banking": [], "other": []}
            }
            
        for name, df in dataframes.items():
            # Classify the table
            is_banking, confidence, evidence = self.domain_classifier.classify_table(df, name)
            
            table_info = {
                "name": name,
                "rows": len(df),
                "confidence": confidence,
                "evidence": evidence
            }
            
            if is_banking:
                banking_tables.append(table_info)
            else:
                other_tables.append(table_info)
                
        # Calculate percentages
        banking_pct = (len(banking_tables) / total_tables) * 100
        other_pct = 100 - banking_pct
        
        return {
            "summary": {
                "banking_pct": round(banking_pct, 1),
                "other_pct": round(other_pct, 1),
                "total_tables": total_tables
            },
            "groups": {
                "banking": banking_tables,
                "other": other_tables
            }
        }
