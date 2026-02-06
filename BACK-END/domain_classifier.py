
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
        Enhanced with more distinctive samples to prevent misclassification.
        """
        # Synthetic Training Data with STRONGER domain-specific features
        # Class 0: Banking - Enhanced with exclusive banking terminology
        banking_samples = [
            "customer_id first_name last_name dob kyc_status risk_score credit_score",
            "account_number account_type balance status open_date ifsc_code branch_id",
            "transaction_id account_number amount transaction_type date routing_number",
            "loan_id loan_type interest_rate tenure_months apr emi_amount principal",
            "credit_limit card_number expiry_date ifsc_code branch_id branch_name swift_code",
            "customer_id savings_account checking_account overdraft_limit minimum_balance",
            "beneficiary_id transfer_amount swift_code routing_number iban bic_code",
            "user_id account_id transaction_id credit_amount debit_amount transaction_date",
            "account_number balance credit_date debit_date withdraw_amount deposit_amount",
            "customer_id login_time logout_time session_id banking_activity transaction_count",
            "account_id payment_type payment_amount transfer_date transfer_status clearance_date",
            "user_id account_number transaction_type credit debit refund balance_check standing_instruction",
            "account_id loan_amount disbursement_date repayment_schedule interest_accrued",
            "customer_id account_balance available_balance overdraft_used credit_utilized",
            "transaction_id merchant_id pos_terminal card_type authorization_code settlement_date"
        ]
        
        # Class 1: Healthcare - Enhanced with exclusive medical terminology
        healthcare_samples = [
            "patient_id first_name last_name dob blood_type insurance_id allergies",
            "diagnosis_id patient_id icd_code diagnosis_name doctor_id admission_date vitals",
            "treatment_plan_id medication dosage frequency prescription_date pharmacy_notes",
            "doctor_id specialty license_number department hospital_id consultation_fee",
            "insurance_claim_id patient_id claim_amount coverage_type approval_status policy_number",
            "appointment_id patient_id doctor_id appointment_date appointment_time status clinic_room",
            "medical_record_id patient_id condition symptoms lab_results treatment_history vaccination_record",
            "patient_id admission_date discharge_date diagnosis ward doctor_id hospital_id bed_number",
            "patient_id doctor_id appointment_date visit_reason department_name treatment_type chief_complaint",
            "patient_id prescription_id medication_name dosage diagnosis treatment_plan drug_interaction",
            "patient_id lab_test_id test_name test_result test_date doctor_id specimen_type",
            "patient_id admission_id discharge_id bed_number ward treatment diagnosis surgery_date",
            "patient_id blood_group donation_date hospital_id volume_ml donor_status hemoglobin_level",
            "patient_id emergency_contact_name vital_signs triage_level chief_complaint examination_notes",
            "patient_id radiology_report scan_type imaging_date radiologist_id findings_summary"
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

    def predict(self, table_names: List[str], all_columns: List[str], sample_values: List[str] = None) -> Dict:
        """
        Predict domain classification across Banking, Healthcare, and Other.
        Now also checks sample data values for healthcare/banking keywords.
        Returns probabilities, primary label, and evidence.
        """
        # Combine all metadata into a single string for classification
        # We emphasize column names as they are the strongest signal
        combined_text = " ".join(table_names + all_columns)
        
        # Also include sample values if provided (to catch "Doctor", "Nurse" etc.)
        if sample_values:
            combined_text += " " + " ".join(sample_values[:50])  # Limit to first 50 values
        
        # CRITICAL: Check for EXCLUSIVE domain keywords FIRST
        # These keywords force classification regardless of ML model
        cols_lower = [col.lower().replace('_', '').replace('-', '').replace(' ', '') for col in all_columns]
        cols_original_lower = [col.lower() for col in all_columns]
        
        # Healthcare EXCLUSIVE patterns - if found, MUST be healthcare
        # Using flexible matching to catch variations like patient_id, patientid, patient_no, etc.
        healthcare_force_patterns = [
            'patient', 'doctor', 'physician', 'surgeon', 'nurse',
            'diagnosis', 'treatment', 'prescription', 'medication',
            'admission', 'discharge', 'hospital', 'clinic', 'ward',
            'labtest', 'bloodtest', 'xray', 'surgery', 'appointment',
            'medicalrecord', 'vitals', 'symptoms', 'allergies'
        ]
        
        # Banking EXCLUSIVE patterns - if found, MUST be banking  
        banking_force_patterns = [
            'accountnumber', 'accountno', 'accountid',
            'ifsccode', 'ifsc', 'swiftcode', 'swift', 
            'routingnumber', 'routing', 'iban', 'bic',
            'transactionid', 'transactionno', 'txnid',
            'loanid', 'loannumber', 'branchid', 'branchcode',
            'accountbalance', 'accounttype'
        ]
        
        # Check for healthcare patterns (more flexible)
        has_healthcare_exclusive = False
        for col_clean in cols_lower:
            for pattern in healthcare_force_patterns:
                if pattern in col_clean:
                    has_healthcare_exclusive = True
                    print(f"[DOMAIN CLASSIFIER] Healthcare pattern '{pattern}' found in column: {all_columns[cols_lower.index(col_clean)]}")
                    break
            if has_healthcare_exclusive:
                break
        
        # Also check original column names for exact matches (case-insensitive)
        if not has_healthcare_exclusive:
            healthcare_exact_keywords = ['patient_id', 'patient', 'doctor_id', 'doctor', 'diagnosis']
            for col_orig in cols_original_lower:
                if any(keyword in col_orig for keyword in healthcare_exact_keywords):
                    has_healthcare_exclusive = True
                    print(f"[DOMAIN CLASSIFIER] Healthcare keyword found in column: {all_columns[cols_original_lower.index(col_orig)]}")
                    break
        
        # Check for banking patterns
        has_banking_exclusive = False
        for col_clean in cols_lower:
            for pattern in banking_force_patterns:
                if pattern in col_clean:
                    has_banking_exclusive = True
                    print(f"[DOMAIN CLASSIFIER] Banking pattern '{pattern}' found in column: {all_columns[cols_lower.index(col_clean)]}")
                    break
            if has_banking_exclusive:
                break
        
        # Predict probability for all three classes
        proba = self.pipeline.predict_proba([combined_text])[0]
        banking_prob = proba[0]      # Class 0: Banking
        healthcare_prob = proba[1]   # Class 1: Healthcare
        other_prob = proba[2]        # Class 2: Other
        
        # APPLY MUTUAL EXCLUSION LOGIC (MORE AGGRESSIVE)
        # If healthcare exclusive keywords found, force healthcare and severely penalize banking
        if has_healthcare_exclusive and not has_banking_exclusive:
            healthcare_prob = 0.95  # Force very high healthcare probability
            banking_prob = 0.02     # Severely penalize banking
            other_prob = 0.03
            print(f"[DOMAIN CLASSIFIER] FORCING Healthcare: 95%, Banking: 2%")
        
        # If banking exclusive keywords found, force banking and penalize healthcare
        elif has_banking_exclusive and not has_healthcare_exclusive:
            banking_prob = 0.95     # Force very high banking probability
            healthcare_prob = 0.02  # Severely penalize healthcare
            other_prob = 0.03
            print(f"[DOMAIN CLASSIFIER] FORCING Banking: 95%, Healthcare: 2%")
        
        # If both found (very rare/mixed data), favor healthcare if patient/doctor columns exist
        elif has_healthcare_exclusive and has_banking_exclusive:
            print(f"[DOMAIN CLASSIFIER] WARNING: Both healthcare and banking keywords detected!")
            # Check which has stronger evidence
            strong_healthcare = any(p in ''.join(cols_lower) for p in ['patient', 'doctor', 'diagnosis'])
            if strong_healthcare:
                healthcare_prob = 0.80
                banking_prob = 0.15
                other_prob = 0.05
                print(f"[DOMAIN CLASSIFIER] Mixed data, favoring Healthcare: 80%")
            else:
                banking_prob = 0.80
                healthcare_prob = 0.15
                other_prob = 0.05
                print(f"[DOMAIN CLASSIFIER] Mixed data, favoring Banking: 80%")
        
        # Normalize probabilities to sum to 1.0
        total = banking_prob + healthcare_prob + other_prob
        if total > 0:
            banking_prob = banking_prob / total
            healthcare_prob = healthcare_prob / total
            other_prob = other_prob / total
        
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
        evidence = self._get_evidence(all_columns, sample_values)
        
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
    
    def _get_evidence(self, columns: List[str], sample_values: List[str] = None) -> List[str]:
        """Identify specific keywords that contributed to the decision from columns and values"""
        # Banking-EXCLUSIVE keywords (NEVER found in healthcare)
        banking_core = {
            "account_number", "account_type", "account_id", "account_balance",
            "transaction_id", "transaction_type", "transaction_date",
            "login_time", "logout_time", "session_id", "banking_activity",
            "ifsc_code", "branch_id", "branch_name", "routing_number", "swift_code"
        }
        
        banking_strong = {
            "loan_id", "loan_type", "loan_amount", "interest_rate", "tenure_months", "apr", "emi",
            "credit_limit", "card_number", "expiry_date", "overdraft", "minimum_balance",
            "credit_amount", "debit_amount", "credit_date", "debit_date",
            "withdraw_amount", "deposit_amount", "payment_type", "transfer_amount",
            "transfer_date", "iban", "bic", "bic_code", "kyc_status", "risk_score",
            "clearance_date", "settlement_date", "disbursement_date", "repayment",
            "available_balance", "standing_instruction", "merchant_id", "pos_terminal"
        }
        
        # Healthcare-EXCLUSIVE keywords (NEVER found in banking)
        healthcare_core = {
            "patient_id", "patient_name", "patient", "diagnosis", "treatment", 
            "doctor_id", "doctor_name", "doctor", "physician", "surgeon",
            "admission_date", "discharge_date", "admission", "discharge",
            "medical_record", "prescription", "lab_results", "symptoms", "vitals"
        }
        
        healthcare_strong = {
            "icd_code", "insurance_claim", "medication", "dosage", "blood_type", "blood_group",
            "appointment", "hospital_id", "hospital", "clinic", "specialty", "license_number",
            "coverage", "ward", "bed_number", "lab_test", "test_result", "test_name",
            "donor_status", "volume_ml", "visit_reason", "chief_complaint", "examination",
            "radiology", "imaging", "scan_type", "radiologist", "hemoglobin", "triage",
            "vaccination", "vaccine", "allergy", "allergies", "surgery", "surgical",
            "emergency_contact", "specimen", "drug_interaction", "pharmacy", "consultation_fee"
        }
        
        # Generic keywords that appear in BOTH domains (should NOT be strong evidence)
        generic_keywords = {
            "customer_id", "user_id", "first_name", "last_name", "dob", "status",
            "date", "amount", "balance", "open_date", "created_date", "id", "name"
        }
        
        cols_lower = [col.lower() for col in columns]
        
        # ENHANCED sample values check for healthcare/banking keywords
        healthcare_value_keywords = [
            'doctor', 'dr.', 'dr ', 'nurse', 'patient', 'physician', 'surgeon', 'therapist',
            'diagnosis', 'prescription', 'medication', 'treatment', 'hospital', 'clinic',
            'emergency', 'admission', 'discharge', 'ward', 'surgery', 'lab test', 'blood test',
            'x-ray', 'ct scan', 'mri', 'ultrasound', 'vaccine', 'immunization', 'symptom'
        ]
        banking_value_keywords = [
            'transaction', 'payment', 'transfer', 'deposit', 'withdrawal', 'account',
            'ifsc', 'branch', 'loan', 'emi', 'interest', 'credit card', 'debit card',
            'swift', 'routing', 'overdraft', 'balance', 'statement', 'cheque', 'check'
        ]
        
        found_healthcare_in_values = []
        found_banking_in_values = []
        
        if sample_values:
            for val in sample_values:
                val_lower = str(val).lower().strip()
                # Check healthcare keywords
                for kw in healthcare_value_keywords:
                    if kw in val_lower:
                        found_healthcare_in_values.append(f"value:{kw}")
                        break
                # Check banking keywords
                for kw in banking_value_keywords:
                    if kw in val_lower:
                        found_banking_in_values.append(f"value:{kw}")
                        break
        
        # Check for partial matches (e.g., "credit_amount" contains "credit")
        found_banking_core = []
        found_banking_strong = []
        found_healthcare_core = []
        found_healthcare_strong = []
        
        for col in columns:
            col_lower = col.lower()
            # Banking core
            if any(kw in col_lower for kw in banking_core):
                found_banking_core.append(col)
            # Banking strong
            elif any(kw in col_lower for kw in banking_strong):
                found_banking_strong.append(col)
            # Healthcare core
            if any(kw in col_lower for kw in healthcare_core):
                found_healthcare_core.append(col)
            # Healthcare strong
            elif any(kw in col_lower for kw in healthcare_strong):
                found_healthcare_strong.append(col)
        
        evidence = []
        if found_banking_core:
            evidence.append(f"Banking core: {', '.join(found_banking_core[:5])}")
        if found_banking_strong:
            evidence.append(f"Banking signals: {', '.join(found_banking_strong[:5])}")
        if found_healthcare_core:
            evidence.append(f"Healthcare core: {', '.join(found_healthcare_core[:5])}")
        if found_healthcare_strong:
            evidence.append(f"Healthcare signals: {', '.join(found_healthcare_strong[:5])}")
        if found_healthcare_in_values:
            evidence.append(f"Healthcare in data: {', '.join(set(found_healthcare_in_values[:5]))}")
        if found_banking_in_values:
            evidence.append(f"Banking in data: {', '.join(set(found_banking_in_values[:5]))}")
            
        return evidence
    
    def get_domain_split_summary(self, table_names: List[str], all_columns: List[str], sample_values: List[str] = None) -> Dict[str, Any]:
        """
        Get domain classification with percentage breakdown for visualization.
        Used to create pie charts showing Banking vs Healthcare vs Other domain split.
        
        Returns:
            Dict with percentages, labels, and visual data
        """
        prediction = self.predict(table_names, all_columns, sample_values)
        
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
