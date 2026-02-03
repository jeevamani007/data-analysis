
import numpy as np
import pandas as pd
from typing import List, Dict, Tuple, Any
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
import pickle
import os

class DomainClassifier:
    """
    Classifies database domains using Logistic Regression based on metadata.
    Focuses on identifying 'Banking' vs 'Other' domains.
    """
    
    def __init__(self):
        self.model = None
        self.vectorizer = None
        self._train_model()
        
    def _train_model(self):
        """
        Train a Logistic Regression model on synthetic data.
        The features are 'bag of words' from column names.
        """
        # Synthetic Training Data
        # Class 1: Banking
        banking_samples = [
            "customer_id first_name last_name dob kyc_status risk_score",
            "account_number account_type balance status open_date",
            "transaction_id account_number amount transaction_type date",
            "loan_id loan_type interest_rate tenure_months",
            "credit_limit card_number expiry_date ifsc_code branch_id branch_name",
            "customer_id savings_account checking_account overdraft_limit",
            "beneficiary_id transfer_amount swift_code routing_number"
        ]
        
        # Class 0: Other (E-commerce, HR, School, etc.)
        other_samples = [
            "product_id product_name price stock_quantity category",
            "order_id customer_id order_date total_amount shipping_address",
            "employee_id first_name last_name salary department hire_date",
            "student_id course_id grade semester enrollment_date",
            "patient_id diagnosis treatment_plan doctor_id admission_date",
            "ticket_id issue_description status priority created_at",
            "vehicle_id model make year vin_number"
        ]
        
        X = banking_samples + other_samples
        y = [1] * len(banking_samples) + [0] * len(other_samples)
        
        # Pipeline: Vectorizer (Unigrams) -> Logistic Regression
        self.pipeline = Pipeline([
            ('vectorizer', CountVectorizer(token_pattern=r'(?u)\b\w+\b')), # Simple word tokenization
            ('classifier', LogisticRegression(random_state=42))
        ])
        
        self.pipeline.fit(X, y)
        print("Domain Classifier trained successfully.")

    def predict(self, table_names: List[str], all_columns: List[str]) -> Dict:
        """
        Predict if the database belongs to the Banking domain.
        Returns probability, label, and evidence.
        """
        # Combine all metadata into a single string for classification
        # We emphasize column names as they are the strongest signal
        combined_text = " ".join(table_names + all_columns)
        
        # Predict probability
        proba = self.pipeline.predict_proba([combined_text])[0]
        banking_prob = proba[1]  # Probability of class 1 (Banking)
        
        is_banking = banking_prob > 0.6  # Threshold
        
        # Detect specific evidence (heuristic check for explanation)
        evidence = self._get_evidence(all_columns)
        
        return {
            "is_banking": bool(is_banking),
            "confidence": float(round(banking_prob * 100, 2)),
            "domain_label": "Banking" if is_banking else "General/Other",
            "evidence": evidence
        }
    
    def _get_evidence(self, columns: List[str]) -> List[str]:
        """Identify specific keywords that contributed to the decision"""
        core_triangle = {
            "customer_id", "first_name", "last_name", "dob", "kyc_status", "risk_score",
            "account_number", "account_type", "balance", "status", "open_date",
            "transaction_id", "amount", "transaction_type", "date"
        }
        
        strong_signals = {
            "loan_id", "loan_type", "interest_rate", "tenure_months",
            "credit_limit", "card_number", "expiry_date", "ifsc_code", "branch_id"
        }
        
        found_core = [col for col in columns if col.lower() in core_triangle]
        found_strong = [col for col in columns if col.lower() in strong_signals]
        
        evidence = []
        if found_core:
            evidence.append(f"Core identifiers: {', '.join(found_core[:5])}")
        if found_strong:
            evidence.append(f"Strong banking signals: {', '.join(found_strong[:5])}")
            
        return evidence
    
    def get_domain_split_summary(self, table_names: List[str], all_columns: List[str]) -> Dict[str, Any]:
        """
        Get domain classification with percentage breakdown for visualization.
        Used to create pie charts showing Banking vs Other domain split.
        
        Returns:
            Dict with percentages, labels, and visual data
        """
        prediction = self.predict(table_names, all_columns)
        
        banking_pct = prediction['confidence']
        other_pct = 100 - banking_pct
        
        return {
            'percentages': {
                'Banking': round(banking_pct, 1),
                'Other': round(other_pct, 1)
            },
            'primary_domain': prediction['domain_label'],
            'confidence': banking_pct,
            'is_banking': prediction['is_banking'],
            'evidence': prediction['evidence'],
            'chart_data': {
                'labels': ['Banking', 'Other'],
                'values': [round(banking_pct, 1), round(other_pct, 1)],
                'colors': ['#0F766E', '#64748B']
            },
            'explanation': self._generate_domain_explanation(banking_pct, prediction['evidence'])
        }
    
    def _generate_domain_explanation(self, banking_confidence: float, evidence: List[str]) -> str:
        """Generate human-readable explanation of domain classification"""
        if banking_confidence >= 80:
            intro = "This appears to be a <strong>Banking database</strong> with high confidence."
        elif banking_confidence >= 60:
            intro = "This looks like a <strong>Banking-related database</strong>."
        elif banking_confidence >= 40:
            intro = "This database has <strong>mixed characteristics</strong>."
        else:
            intro = "This appears to be a <strong>Non-Banking database</strong>."
        
        evidence_text = ""
        if evidence:
            evidence_text = " " + " ".join(evidence)
        
        return intro + evidence_text
