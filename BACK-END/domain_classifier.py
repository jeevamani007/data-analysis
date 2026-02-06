
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
        
        # Class 2: Retail / E‑commerce (explicitly modelled, not lumped into "Other")
        retail_samples = [
            # Customer + product + order structure
            "customer_id customer_name customer_phone customer_email customer_address "
            "product_id product_name quantity unit_price total_amount payment_method",
            "order_id order_date bill_no product_id product_name category quantity unit_price "
            "total_amount discount_amount tax_amount net_amount payment_method payment_status",
            "customer_id order_id order_date sales_channel store_id cashier_id shift_time "
            "product_id product_name brand unit_price stock_quantity",
            # Returns / invoices
            "invoice_type invoice_no invoice_date product_id product_name quantity unit_price "
            "total_amount tax_amount net_amount return_flag return_date",
            "transaction_id payment_method payment_status total_amount tax_amount discount_amount "
            "net_amount sales_channel",
            # Inventory / stock
            "product_id sku product_name category brand unit_price cost_price stock_quantity",
        ]
        
        # Class 3: Other (HR, School, Tickets, Vehicles, etc. – explicitly non‑retail)
        other_samples = [
            "employee_id first_name last_name salary department hire_date",
            "student_id course_id grade semester enrollment_date",
            "ticket_id issue_description status priority created_at",
            "vehicle_id model make year vin_number",
            "device_id device_type os_version last_seen_ip_address",
        ]
        
        X = banking_samples + healthcare_samples + retail_samples + other_samples
        y = (
            [0] * len(banking_samples)
            + [1] * len(healthcare_samples)
            + [2] * len(retail_samples)
            + [3] * len(other_samples)
        )
        
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
        # These keywords strongly steer/override the ML model so domains do not get mixed.
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
        # NOTE: transactionid excluded - POS/retail also uses transaction_id; use retail schema override instead
        banking_force_patterns = [
            'accountnumber', 'accountno', 'accountid',
            'ifsccode', 'ifsc', 'swiftcode', 'swift', 
            'routingnumber', 'routing', 'iban', 'bic',
            'loanid', 'loannumber', 'branchid', 'branchcode',
            'accountbalance', 'accounttype'
        ]
        
        # Retail EXCLUSIVE patterns - must NOT appear in pure banking/healthcare schemas
        # Includes: order_id, product_id, product_name, category, quantity, unit_price, discount, tax
        retail_force_patterns = [
            'productname', 'productid', 'sku',
            'unitprice', 'costprice', 'stockqty', 'stockquantity',
            'billno', 'billnumber', 'invoiceno', 'invoicenumber', 'invoicetype',
            'taxamount', 'discountamount', 'discount', 'netamount', 'tax',  # discount, tax common in retail line items
            'saleschannel', 'online', 'store',
            'returnflag', 'returndate',
            'cashierid', 'storeid',
            'category',  # product category is strong retail signal
        ]
        
        # Strong retail combo (columns that almost surely mean retail POS / order line)
        retail_strong_combo_cols = {
            'productname', 'quantity', 'unitprice', 'totalamount', 'paymentmethod',
            'orderid', 'productid', 'category'  # order_id + product_id + category = retail
        }
        
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
        # Check for retail patterns
        has_retail_exclusive = False
        retail_combo_hits = 0
        cols_joined_clean = ''.join(cols_lower)
        # Strong retail schema: typical order line / POS tables
        retail_schema_keys = {
            'orderid', 'order_no', 'ordernumber',
            'productid', 'productname', 'sku',
            'category', 'quantity',
            'unitprice', 'unit_price',
            'discount', 'discountamount',
            'totalamount', 'total_amount', 'netamount',
            'paymentmethod', 'payment_method',
            'orderstatus', 'order_status',
            'tax',  # tax on line items = retail
        }
        retail_schema_hits = set()
        # Observed retail columns: order_id, product_id, product_name, category, quantity, unit_price, discount, tax
        observed_retail_cols = {'orderid', 'productid', 'productname', 'category', 'quantity', 'unitprice', 'discount', 'tax'}
        observed_retail_count = sum(1 for oc in observed_retail_cols if any(oc in col for col in cols_lower))
        for col_clean in cols_lower:
            # force patterns
            for pattern in retail_force_patterns:
                if pattern in col_clean:
                    has_retail_exclusive = True
                    print(f"[DOMAIN CLASSIFIER] Retail pattern '{pattern}' found in column: {all_columns[cols_lower.index(col_clean)]}")
                    break
            # strong combo columns
            for strong_key in retail_strong_combo_cols:
                if strong_key in col_clean:
                    retail_combo_hits += 1
                    break
            # schema keys (more tolerant, used for strong retail override)
            for sk in retail_schema_keys:
                if sk in col_clean:
                    retail_schema_hits.add(sk)
            if has_retail_exclusive:
                break
        
        # If we see at least 3 of the 5 core retail columns together, treat as strong retail signal
        if not has_retail_exclusive and retail_combo_hits >= 3:
            has_retail_exclusive = True
            print(f"[DOMAIN CLASSIFIER] Strong Retail combo detected ({retail_combo_hits} core columns).")
        # Observed retail columns (order_id, product_id, product_name, category, quantity, unit_price, discount, tax)
        if not has_retail_exclusive and observed_retail_count >= 4:
            has_retail_exclusive = True
            print(f"[DOMAIN CLASSIFIER] Observed Retail columns confirmed ({observed_retail_count}/8: order_id, product_id, product_name, category, quantity, unit_price, discount, tax). Forcing Retail.")
        
        # Predict probability for all four classes
        proba = self.pipeline.predict_proba([combined_text])[0]
        banking_prob = proba[0]      # Class 0: Banking
        healthcare_prob = proba[1]   # Class 1: Healthcare
        retail_prob = proba[2]       # Class 2: Retail
        other_prob = proba[3]        # Class 3: Other
        
        # APPLY MUTUAL EXCLUSION LOGIC (MORE AGGRESSIVE, NOW WITH RETAIL)
        # 1) Pure strong domain signals
        if has_healthcare_exclusive and not (has_banking_exclusive or has_retail_exclusive):
            healthcare_prob = 0.95
            banking_prob = 0.02
            retail_prob = 0.01
            other_prob = 0.02
            print(f"[DOMAIN CLASSIFIER] FORCING Healthcare: 95% (exclusive patterns)")
        
        elif has_banking_exclusive and not (has_healthcare_exclusive or has_retail_exclusive):
            banking_prob = 0.95
            healthcare_prob = 0.02
            retail_prob = 0.01
            other_prob = 0.02
            print(f"[DOMAIN CLASSIFIER] FORCING Banking: 95% (exclusive patterns)")
        
        elif has_retail_exclusive and not (has_banking_exclusive or has_healthcare_exclusive):
            retail_prob = 0.95
            banking_prob = 0.02
            healthcare_prob = 0.01
            other_prob = 0.02
            print(f"[DOMAIN CLASSIFIER] FORCING Retail: 95% (exclusive patterns)")
        # Strong retail schema (order_id + product + qty + unit_price + total_amount + payment_method, etc.)
        elif len(retail_schema_hits) >= 4 and not (has_banking_exclusive or has_healthcare_exclusive):
            # Override towards Retail even if ML model was uncertain
            print(f"[DOMAIN CLASSIFIER] Strong Retail schema detected (columns: {', '.join(list(retail_schema_hits)[:6])}). Forcing Retail domain.")
            retail_prob = 0.97
            banking_prob = 0.01
            healthcare_prob = 0.01
            other_prob = 0.01
        
        # 2) Mixed exclusive signals – fall back to "who is stronger" heuristic
        elif (has_healthcare_exclusive + has_banking_exclusive + has_retail_exclusive) > 1:
            print(f"[DOMAIN CLASSIFIER] WARNING: Mixed domain-exclusive patterns detected!")
            # Prefer healthcare if strong patient/doctor signals
            strong_healthcare = any(p in cols_joined_clean for p in ['patient', 'doctor', 'diagnosis'])
            strong_banking = any(p in cols_joined_clean for p in ['accountnumber', 'ifsccode', 'loanid', 'branchid'])
            strong_retail = (observed_retail_count >= 4 or
                            any(p in cols_joined_clean for p in ['productname', 'unitprice', 'totalamount', 'paymentmethod']) or
                            len(retail_schema_hits) >= 3)
            
            # Observed retail columns (order_id, product_id, product_name, etc.) take precedence when 4+
            if observed_retail_count >= 4 and not strong_healthcare:
                retail_prob, banking_prob, healthcare_prob, other_prob = 0.95, 0.02, 0.02, 0.01
                print(f"[DOMAIN CLASSIFIER] Observed Retail columns ({observed_retail_count}/8) - confirming Retail domain, allowing Retail model.")
            elif strong_healthcare:
                healthcare_prob, banking_prob, retail_prob, other_prob = 0.8, 0.1, 0.05, 0.05
                print(f"[DOMAIN CLASSIFIER] Mixed data, favoring Healthcare: 80%")
            elif strong_retail:
                retail_prob, banking_prob, healthcare_prob, other_prob = 0.8, 0.1, 0.05, 0.05
                print(f"[DOMAIN CLASSIFIER] Mixed data, favoring Retail: 80%")
            elif strong_banking:
                banking_prob, healthcare_prob, retail_prob, other_prob = 0.8, 0.1, 0.05, 0.05
                print(f"[DOMAIN CLASSIFIER] Mixed data, favoring Banking: 80%")
        
        # Normalize probabilities to sum to 1.0
        total = banking_prob + healthcare_prob + retail_prob + other_prob
        if total > 0:
            banking_prob = banking_prob / total
            healthcare_prob = healthcare_prob / total
            retail_prob = retail_prob / total
            other_prob = other_prob / total
        
        # Determine primary domain (highest probability)
        max_prob = max(banking_prob, healthcare_prob, retail_prob, other_prob)
        if banking_prob == max_prob:
            primary_domain = "Banking"
            is_banking = True
        elif healthcare_prob == max_prob:
            primary_domain = "Healthcare"
            is_banking = False
        elif retail_prob == max_prob:
            primary_domain = "Retail"
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
                "Retail": float(round(retail_prob * 100, 2)),
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
        
        # Healthcare-EXCLUSIVE keywords (NEVER found in banking/retail)
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
        
        # Retail-EXCLUSIVE keywords (NEVER found in banking/healthcare by design)
        retail_core = {
            "product_id", "product_name", "sku", "category", "brand",
            "unit_price", "cost_price", "stock_quantity",
            "bill_no", "invoice_no", "invoice_type",
            "tax_amount", "discount_amount", "net_amount",
            "sales_channel", "return_flag", "return_date",
            "store_id", "cashier_id", "shift_time",
            "payment_method", "payment_status"
        }
        
        retail_strong = {
            "customer_name", "customer_phone", "customer_email", "customer_address",
            "order_id", "order_date", "bill_number", "invoice_number",
            "total_amount", "quantity", "qty", "pos_id", "terminal_id"
        }
        
        # Generic keywords that appear in MANY domains (should NOT be strong evidence)
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
        retail_value_keywords = [
            'invoice', 'bill', 'receipt', 'order', 'cart', 'checkout',
            'product', 'sku', 'brand', 'store', 'online', 'pos', 'cashier',
            'cash', 'card', 'upi', 'wallet'
        ]
        
        found_healthcare_in_values = []
        found_banking_in_values = []
        found_retail_in_values = []
        
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
                # Check retail keywords
                for kw in retail_value_keywords:
                    if kw in val_lower:
                        found_retail_in_values.append(f"value:{kw}")
                        break
        
        # Check for partial matches (e.g., "credit_amount" contains "credit")
        found_banking_core = []
        found_banking_strong = []
        found_healthcare_core = []
        found_healthcare_strong = []
        found_retail_core = []
        found_retail_strong = []
        
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
            # Retail core
            if any(kw in col_lower.replace(' ', '_') for kw in retail_core):
                found_retail_core.append(col)
            # Retail strong
            elif any(kw in col_lower.replace(' ', '_') for kw in retail_strong):
                found_retail_strong.append(col)
        
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
        if found_retail_core:
            evidence.append(f"Retail core: {', '.join(found_retail_core[:5])}")
        if found_retail_strong:
            evidence.append(f"Retail signals: {', '.join(found_retail_strong[:5])}")
        if found_retail_in_values:
            evidence.append(f"Retail in data: {', '.join(set(found_retail_in_values[:5]))}")
            
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
        retail_pct = prediction['percentages']['Retail']
        other_pct = prediction['percentages']['Other']
        
        return {
            'percentages': {
                'Banking': round(banking_pct, 1),
                'Healthcare': round(healthcare_pct, 1),
                'Retail': round(retail_pct, 1),
                'Other': round(other_pct, 1)
            },
            'primary_domain': prediction['domain_label'],
            'confidence': prediction['confidence'],
            'is_banking': prediction['is_banking'],
            'evidence': prediction['evidence'],
            'chart_data': {
                'labels': ['Banking', 'Healthcare', 'Retail', 'Other'],
                'values': [
                    round(banking_pct, 1),
                    round(healthcare_pct, 1),
                    round(retail_pct, 1),
                    round(other_pct, 1)
                ],
                # Teal for Banking, Turquoise for Healthcare, Amber for Retail, Gray for Other
                'colors': ['#0F766E', '#14B8A6', '#F59E0B', '#64748B']
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
        elif primary_domain == "Retail":
            if percentages.get('Retail', 0) >= 70:
                intro = "This appears to be a <strong>Retail / E‑commerce database</strong> with high confidence."
            else:
                intro = "This looks like a <strong>Retail / sales-related database</strong>."
        else:
            if other_pct >= 60:
                intro = "This appears to be a <strong>General/Other domain database</strong>."
            else:
                intro = "This database has <strong>mixed characteristics</strong>."
        
        evidence_text = ""
        if evidence:
            evidence_text = " " + " ".join(evidence)
        
        return intro + evidence_text
