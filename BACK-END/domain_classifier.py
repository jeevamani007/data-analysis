
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
        Now supports tri-class classification: Banking, Healthcare, Other.
        """
        # Synthetic Training Data
        # Class 0: Banking
        banking_samples = [
            "customer_id first_name last_name dob kyc_status risk_score",
            "account_number account_type balance status open_date",
            "transaction_id account_number amount transaction_type date",
            "loan_id loan_type interest_rate tenure_months",
            "credit_limit card_number expiry_date ifsc_code branch_id branch_name",
            "customer_id savings_account checking_account overdraft_limit",
            "beneficiary_id transfer_amount swift_code routing_number"
        ]
        
        # Class 1: Healthcare
        healthcare_samples = [
            "patient_id first_name last_name dob blood_type insurance_id",
            "diagnosis_id patient_id icd_code diagnosis_name doctor_id admission_date",
            "treatment_plan_id medication dosage frequency prescription_date",
            "doctor_id specialty license_number department hospital_id",
            "insurance_claim_id patient_id claim_amount coverage_type approval_status",
            "appointment_id patient_id doctor_id appointment_date appointment_time status",
            "medical_record_id patient_id condition symptoms lab_results treatment_history"
        ]
        
        # Class 2: Other (E-commerce, HR, School, etc.)
        other_samples = [
            "product_id product_name price stock_quantity category",
            "order_id customer_id order_date total_amount shipping_address",
            "employee_id first_name last_name salary department hire_date",
            "student_id course_id grade semester enrollment_date",
            "ticket_id issue_description status priority created_at",
            "vehicle_id model make year vin_number"
        ]
        
        X = banking_samples + healthcare_samples + other_samples
        y = [0] * len(banking_samples) + [1] * len(healthcare_samples) + [2] * len(other_samples)
        
        # Pipeline: Vectorizer (Unigrams) -> Logistic Regression (multi-class)
        self.pipeline = Pipeline([
            ('vectorizer', CountVectorizer(token_pattern=r'(?u)\b\w+\b')), # Simple word tokenization
            ('classifier', LogisticRegression(random_state=42, max_iter=200))
        ])
        
        self.pipeline.fit(X, y)
        print("Domain Classifier trained successfully (Banking, Healthcare, Other).")

    def predict(self, table_names: List[str], all_columns: List[str]) -> Dict:
        """
        Predict domain classification across Banking, Healthcare, and Other.
        Returns probabilities, primary label, and evidence.
        """
        # Combine all metadata into a single string for classification
        # We emphasize column names as they are the strongest signal
        combined_text = " ".join(table_names + all_columns)
        
        # Predict probability for all three classes
        proba = self.pipeline.predict_proba([combined_text])[0]
        banking_prob = proba[0]      # Class 0: Banking
        healthcare_prob = proba[1]   # Class 1: Healthcare
        other_prob = proba[2]        # Class 2: Other
        
        # Determine primary domain (highest probability)
        max_prob = max(banking_prob, healthcare_prob, other_prob)
        if banking_prob == max_prob:
            primary_domain = "Banking"
            is_banking = True
        elif healthcare_prob == max_prob:
            primary_domain = "Healthcare"
            is_banking = False
        else:
            primary_domain = "General/Other"
            is_banking = False
        
        # Detect specific evidence (heuristic check for explanation)
        evidence = self._get_evidence(all_columns)
        
        return {
            "is_banking": bool(is_banking),
            "confidence": float(round(max_prob * 100, 2)),
            "domain_label": primary_domain,
            "percentages": {
                "Banking": float(round(banking_prob * 100, 2)),
                "Healthcare": float(round(healthcare_prob * 100, 2)),
                "Other": float(round(other_prob * 100, 2))
            },
            "evidence": evidence
        }
    
    def _get_evidence(self, columns: List[str]) -> List[str]:
        """Identify specific keywords that contributed to the decision"""
        # Banking keywords
        banking_core = {
            "customer_id", "first_name", "last_name", "dob", "kyc_status", "risk_score",
            "account_number", "account_type", "balance", "status", "open_date",
            "transaction_id", "amount", "transaction_type", "date"
        }
        
        banking_strong = {
            "loan_id", "loan_type", "interest_rate", "tenure_months",
            "credit_limit", "card_number", "expiry_date", "ifsc_code", "branch_id"
        }
        
        # Healthcare keywords
        healthcare_core = {
            "patient_id", "diagnosis", "treatment", "doctor_id", "admission_date",
            "medical_record", "prescription", "lab_results", "symptoms"
        }
        
        healthcare_strong = {
            "icd_code", "insurance_claim", "medication", "dosage", "blood_type",
            "appointment", "hospital_id", "specialty", "coverage"
        }
        
        found_banking_core = [col for col in columns if col.lower() in banking_core]
        found_banking_strong = [col for col in columns if col.lower() in banking_strong]
        found_healthcare_core = [col for col in columns if col.lower() in healthcare_core]
        found_healthcare_strong = [col for col in columns if col.lower() in healthcare_strong]
        
        evidence = []
        if found_banking_core:
            evidence.append(f"Banking core: {', '.join(found_banking_core[:5])}")
        if found_banking_strong:
            evidence.append(f"Banking signals: {', '.join(found_banking_strong[:5])}")
        if found_healthcare_core:
            evidence.append(f"Healthcare core: {', '.join(found_healthcare_core[:5])}")
        if found_healthcare_strong:
            evidence.append(f"Healthcare signals: {', '.join(found_healthcare_strong[:5])}")
            
        return evidence
    
    def get_domain_split_summary(self, table_names: List[str], all_columns: List[str]) -> Dict[str, Any]:
        """
        Get domain classification with percentage breakdown for visualization.
        Used to create pie charts showing Banking vs Healthcare vs Other domain split.
        
        Returns:
            Dict with percentages, labels, and visual data
        """
        prediction = self.predict(table_names, all_columns)
        
        banking_pct = prediction['percentages']['Banking']
        healthcare_pct = prediction['percentages']['Healthcare']
        other_pct = prediction['percentages']['Other']
        
        return {
            'percentages': {
                'Banking': round(banking_pct, 1),
                'Healthcare': round(healthcare_pct, 1),
                'Other': round(other_pct, 1)
            },
            'primary_domain': prediction['domain_label'],
            'confidence': prediction['confidence'],
            'is_banking': prediction['is_banking'],
            'evidence': prediction['evidence'],
            'chart_data': {
                'labels': ['Banking', 'Healthcare', 'Other'],
                'values': [round(banking_pct, 1), round(healthcare_pct, 1), round(other_pct, 1)],
                'colors': ['#0F766E', '#14B8A6', '#64748B']  # Teal for Banking, Turquoise for Healthcare, Gray for Other
            },
            'explanation': self._generate_domain_explanation(prediction['percentages'], prediction['domain_label'], prediction['evidence'])
        }
    
    def _generate_domain_explanation(self, percentages: dict, primary_domain: str, evidence: List[str]) -> str:
        """Generate human-readable explanation of domain classification"""
        banking_pct = percentages.get('Banking', 0)
        healthcare_pct = percentages.get('Healthcare', 0)
        other_pct = percentages.get('Other', 0)
        
        max_pct = max(banking_pct, healthcare_pct, other_pct)
        
        # Generate intro based on primary domain and confidence
        if primary_domain == "Banking":
            if banking_pct >= 70:
                intro = "This appears to be a <strong>Banking database</strong> with high confidence."
            else:
                intro = "This looks like a <strong>Banking-related database</strong>."
        elif primary_domain == "Healthcare":
            if healthcare_pct >= 70:
                intro = "This appears to be a <strong>Healthcare database</strong> with high confidence."
            else:
                intro = "This looks like a <strong>Healthcare-related database</strong>."
        else:
            if other_pct >= 60:
                intro = "This appears to be a <strong>General/Other domain database</strong>."
            else:
                intro = "This database has <strong>mixed characteristics</strong>."
        
        evidence_text = ""
        if evidence:
            evidence_text = " " + " ".join(evidence)
        
        return intro + evidence_text
