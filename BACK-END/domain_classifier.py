
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
    Supports multi-class domain detection with keyword-pattern overrides.
    """
    
    def __init__(self):
        self.model = None
        self.vectorizer = None
        self._train_model()
        
    def _train_model(self):
        """
        Train a Logistic Regression model on synthetic data.
        The features are 'bag of words' from column names.
        Now supports multi-class classification:
        Banking, Finance, Insurance, Healthcare, Retail/E‑commerce, Other.
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
        
        # Class 1: Finance / Accounting / Payroll
        finance_samples = [
            "invoice_id invoice_no invoice_date customer_id payment_mode payment_status due_date",
            "gst gstin tax tax_amount cgst sgst igst taxable_value invoice_total net_amount",
            "salary employee_id payroll_month gross_salary net_salary deduction hra pf esi tds",
            "expense_id expense_type expense_amount expense_date cost_center vendor_name",
            "profit loss revenue cogs margin budget fiscal_year quarter",
            "ledger_id ledger_name journal_id journal_date debit credit voucher_no narration",
            "accounts_payable supplier_id bill_no bill_date amount_due paid_amount payment_date",
            "accounts_receivable client_id receipt_no receipt_date amount_received balance_due",
            "budget_id budget_amount planned_amount actual_amount variance department_id",
        ]

        # Class 2: Insurance
        insurance_samples = [
            "policy_no policy_id policy_type insured_name nominee beneficiary sum_insured premium",
            "claim_id policy_no claim_date claim_amount claim_status settlement_amount",
            "risk_score underwriting_score rider coverage_type deductible copay co_insurance",
            "policy_start_date policy_end_date renewal_date lapse_date grace_period",
            "agent_id broker_id insurer_name premium_due_date premium_paid_date payment_mode",
        ]

        # Class 1: Healthcare - Enhanced with exclusive medical terminology
        healthcare_samples = [
            "patient_id first_name last_name dob blood_type insurance_id allergies",
            "diagnosis_id patient_id icd_code diagnosis_name doctor_id admission_date vitals",
            "treatment_plan_id medication dosage frequency prescription_date pharmacy_notes",
            "doctor_id specialty license_number department hospital_id consultation_fee",
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
        
        # IMPORTANT: class order must match probability indices used later
        # 0 Banking, 1 Finance, 2 Insurance, 3 Healthcare, 4 Retail, 5 Other
        X = banking_samples + finance_samples + insurance_samples + healthcare_samples + retail_samples + other_samples
        y = (
            [0] * len(banking_samples)
            + [1] * len(finance_samples)
            + [2] * len(insurance_samples)
            + [3] * len(healthcare_samples)
            + [4] * len(retail_samples)
            + [5] * len(other_samples)
        )
        
        # Pipeline: Vectorizer (Unigrams) -> Logistic Regression (multi-class)
        self.pipeline = Pipeline([
            ('vectorizer', CountVectorizer(token_pattern=r'(?u)\b\w+\b')), # Simple word tokenization
            ('classifier', LogisticRegression(random_state=42, max_iter=200))
        ])
        
        self.pipeline.fit(X, y)
        print("Domain Classifier trained successfully (Banking, Finance, Insurance, Healthcare, Retail, Other).")

    def predict_domain_2step(self, all_columns: List[str], sample_values: List[str] = None) -> Dict[str, Any]:
        """
        2-STEP DOMAIN CLASSIFICATION (for multiple files combined):
        
        STEP 1 – Column Analysis:
        Analyze only column names to identify domain-specific keywords.
        
        STEP 2 – Row Data (Value) Analysis:
        Analyze actual data values inside rows for semantic clues.
        
        FINAL DECISION:
        Combine STEP 1 and STEP 2 results.
        Choose the domain that is supported by BOTH columns and row data.
        If conflict exists, give higher priority to ROW DATA over column names.
        
        Args:
            all_columns: List of all column names from all uploaded files
            sample_values: List of sample row values from all uploaded files
            
        Returns:
            Dict with step-by-step analysis and final prediction
        """
        
        # ===== STEP 1: COLUMN ANALYSIS =====
        print("\n" + "="*70)
        print("STEP 1 – COLUMN ANALYSIS")
        print("="*70)
        
        step1_result = self._analyze_columns(all_columns)
        
        print(f"\nColumn Analysis Result:")
        print(f"  Primary Domain: {step1_result['primary_domain']}")
        print(f"  Confidence: {step1_result['confidence']:.1f}%")
        print(f"  Evidence: {', '.join(step1_result['evidence'][:3]) if step1_result['evidence'] else 'None'}")
        
        # ===== STEP 2: ROW DATA ANALYSIS =====
        print("\n" + "="*70)
        print("STEP 2 – ROW DATA (VALUE) ANALYSIS")
        print("="*70)
        
        step2_result = self._analyze_row_data(sample_values if sample_values else [])
        
        print(f"\nRow Data Analysis Result:")
        print(f"  Primary Domain: {step2_result['primary_domain']}")
        print(f"  Confidence: {step2_result['confidence']:.1f}%")
        print(f"  Evidence: {', '.join(step2_result['evidence'][:3]) if step2_result['evidence'] else 'None'}")
        
        # ===== FINAL DECISION =====
        print("\n" + "="*70)
        print("FINAL DECISION")
        print("="*70)
        
        final_domain = self._combine_results(step1_result, step2_result)
        
        print(f"\nFinal Predicted Domain: {final_domain['domain']}")
        print(f"Reasoning: {final_domain['reasoning']}")
        print("="*70 + "\n")
        
        return {
            'step1_column_analysis': step1_result,
            'step2_row_analysis': step2_result,
            'final_prediction': final_domain
        }
    
    def _analyze_columns(self, columns: List[str]) -> Dict[str, Any]:
        """
        STEP 1: Analyze only column names for domain-specific keywords.
        """
        cols_lower = [col.lower().replace('_', '').replace('-', '').replace(' ', '') for col in columns]
        
        # Domain-specific keyword patterns
        domain_keywords = {
            'Banking': [
                'accountno', 'accountnumber', 'accountid', 'accountbalance', 'accounttype',
                'ifsc', 'ifsccode', 'swift', 'swiftcode', 'routing', 'iban', 'bic',
                'branch', 'branchid', 'deposit', 'withdraw', 'loanid', 'logintime', 'logouttime'
            ],
            'Finance': [
                'invoice', 'invoiceid', 'invoiceno',
                'gst', 'gstin', 'tax', 'salary', 'payroll',
                'expense', 'profit', 'loss', 'budget', 'ledger', 'journal', 'voucher',
                'tds', 'pf', 'esi', 'receivable', 'payable'
            ],
            'Insurance': [
                'policy', 'pol', 'policyid', 'polid',
                'claim', 'claimid', 'clmid',
                'premium', 'prem', 'premamt',
                'suminsured', 'insured', 'insurer', 'beneficiary', 'nominee',
                'underwriting', 'coverage', 'deductible'
            ],
            'Healthcare': [
                'patient', 'doctor', 'physician', 'diagnosis', 'treatment', 'prescription',
                'medication', 'admission', 'discharge', 'hospital', 'clinic', 'ward',
                'labtest', 'appointment', 'vitals', 'symptoms', 'allergies'
            ],
            'Retail': [
                'productname', 'productid', 'sku', 'category', 'brand',
                'orderid', 'quantity', 'unitprice', 'discount',
                'saleschannel', 'storeid', 'cashierid', 'returnflag'
            ]
        }
        
        # Count keyword matches for each domain
        domain_scores = {}
        domain_evidence = {}
        
        for domain, keywords in domain_keywords.items():
            matches = []
            for col_clean in cols_lower:
                for keyword in keywords:
                    if keyword in col_clean:
                        original_col = columns[cols_lower.index(col_clean)]
                        if original_col not in matches:
                            matches.append(original_col)
                        break
            
            domain_scores[domain] = len(matches)
            domain_evidence[domain] = matches
        
        # Determine primary domain from columns
        if sum(domain_scores.values()) == 0:
            return {
                'primary_domain': 'Other',
                'confidence': 50.0,
                'scores': domain_scores,
                'evidence': []
            }
        
        primary = max(domain_scores, key=domain_scores.get)
        total_matches = sum(domain_scores.values())
        confidence = (domain_scores[primary] / total_matches * 100) if total_matches > 0 else 0
        
        return {
            'primary_domain': primary,
            'confidence': confidence,
            'scores': domain_scores,
            'evidence': domain_evidence.get(primary, [])
        }
    
    def _analyze_row_data(self, sample_values: List[str]) -> Dict[str, Any]:
        """
        STEP 2: Analyze actual data values for semantic clues.
        """
        if not sample_values:
            return {
                'primary_domain': 'Unknown',
                'confidence': 0.0,
                'scores': {},
                'evidence': []
            }
        
        # Domain-specific value keywords
        value_keywords = {
            'Banking': [
                'deposit', 'withdraw', 'transfer', 'account', 'branch', 'ifsc',
                'transaction', 'balance', 'credit', 'debit', 'loan', 'emi'
            ],
            'Finance': [
                'invoice', 'gst', 'gstin', 'tax', 'salary', 'payroll',
                'expense', 'profit', 'loss', 'buy', 'sell', 'invest',
                'tcs', 'infosys', 'stock', 'fund', 'nav'
            ],
            'Insurance': [
                'policy', 'claim', 'premium', 'claim_approved', 'policy_issued',
                'premium_paid', 'sum insured', 'beneficiary', 'nominee'
            ],
            'Healthcare': [
                'appointment_booked', 'cardiology', 'doctor', 'dr.', 'patient',
                'diagnosis', 'treatment', 'prescription', 'hospital', 'clinic',
                'emergency', 'surgery', 'lab test'
            ],
            'Retail': [
                'order_placed', 'order_shipped', 'delivered', 'product',
                'shipped', 'cancelled', 'returned', 'refund'
            ]
        }
        
        # Count value matches for each domain
        domain_scores = {}
        domain_evidence = {}
        
        for domain, keywords in value_keywords.items():
            matches = []
            for value in sample_values:
                value_lower = str(value).lower().strip()
                for keyword in keywords:
                    if keyword in value_lower:
                        if value not in matches:
                            matches.append(value)
                        break
            
            domain_scores[domain] = len(matches)
            domain_evidence[domain] = matches[:5]  # Keep only first 5 examples
        
        # Determine primary domain from row data
        if sum(domain_scores.values()) == 0:
            return {
                'primary_domain': 'Other',
                'confidence': 50.0,
                'scores': domain_scores,
                'evidence': []
            }
        
        primary = max(domain_scores, key=domain_scores.get)
        total_matches = sum(domain_scores.values())
        confidence = (domain_scores[primary] / total_matches * 100) if total_matches > 0 else 0
        
        return {
            'primary_domain': primary,
            'confidence': confidence,
            'scores': domain_scores,
            'evidence': domain_evidence.get(primary, [])
        }
    
    def _combine_results(self, step1: Dict, step2: Dict) -> Dict[str, str]:
        """
        FINAL DECISION: Combine STEP 1 and STEP 2 results.
        Priority: ROW DATA > COLUMN NAMES if there's a conflict.
        """
        domain1 = step1['primary_domain']
        domain2 = step2['primary_domain']
        conf1 = step1['confidence']
        conf2 = step2['confidence']
        
        # Case 1: Both agree
        if domain1 == domain2:
            return {
                'domain': domain1,
                'reasoning': f"Both column analysis and row data analysis agree on {domain1} domain. "
                           f"Column confidence: {conf1:.1f}%, Row data confidence: {conf2:.1f}%."
            }
        
        # Case 2: Conflict - Row data has priority
        if domain2 != 'Other' and domain2 != 'Unknown':
            return {
                'domain': domain2,
                'reasoning': f"Conflict detected. Columns suggest {domain1} ({conf1:.1f}%), "
                           f"but row data clearly indicates {domain2} ({conf2:.1f}%). "
                           f"Row data has higher priority - Final decision: {domain2}."
            }
        
        # Case 3: Row data is unclear, use column analysis
        if domain1 != 'Other':
            return {
                'domain': domain1,
                'reasoning': f"Row data analysis is inconclusive ({domain2}). "
                           f"Using column analysis result: {domain1} ({conf1:.1f}%)."
            }
        
        # Case 4: Both unclear
        return {
            'domain': 'Other',
            'reasoning': "Both column and row data analysis are inconclusive. "
                       "Unable to determine specific domain with confidence."
        }

    def predict(self, table_names: List[str], all_columns: List[str], sample_values: List[str] = None) -> Dict:
        """
        Predict domain classification across Banking, Finance, Insurance, Healthcare, Retail, and Other.
        Also checks sample data values for extra keywords.
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
        
        # Healthcare patterns
        # Using flexible matching to catch variations like patient_id, patientid, patient_no, etc.
        healthcare_force_patterns = [
            'patient', 'doctor', 'physician', 'surgeon', 'nurse',
            'diagnosis', 'treatment', 'prescription', 'medication',
            'admission', 'discharge', 'hospital', 'clinic', 'ward',
            'labtest', 'bloodtest', 'xray', 'surgery', 'appointment',
            'medicalrecord', 'vitals', 'symptoms', 'allergies'
        ]
        
        # Banking patterns – ONLY terms that are truly banking-specific.
        # EXCLUDED: balance, risk_score, beneficiary (used in Insurance too).
        # Banking = bank accounts, branches, IFSC/SWIFT, loans, login/logout sessions.
        banking_force_patterns = [
            'accountno', 'accountnumber', 'accountid', 'accountbalance', 'accounttype',
            'ifsc', 'ifsccode', 'swiftcode', 'swift', 'routingnumber', 'routing', 'iban', 'bic',
            'branch', 'branchid', 'branchcode', 'branch_name',
            'deposit', 'withdraw', 'withdrawal',
            'login_time', 'logout_time', 'logintime', 'logouttime',
            'loanid', 'loannumber', 'loan_id', 'loan_type',
            'transaction_id', 'transactionid',  # only count as banking when with account/ifsc
        ]
        
        # Finance patterns (Accounting/Payroll/Tax)
        # NOTE: 'invoice' and 'tax' can appear in retail; we rely on combos to disambiguate.
        finance_force_patterns = [
            # user provided finance clues
            'invoiceid', 'invoiceno', 'invoice', 'paymentmode',
            'gst', 'gstin',
            'tax', 'salary', 'expense', 'profit', 'loss', 'budget',
            # common accounting words
            'ledger', 'journal', 'voucher', 'payroll', 'tds', 'pf', 'esi',
            'receivable', 'payable', 'costcenter', 'fiscal', 'revenue', 'cogs',
        ]

        # Insurance patterns (include abbreviated column names e.g. POL_ID, PREM_AMT, CLM_ID from demo)
        insurance_force_patterns = [
            # user provided insurance clues
            'policyno', 'policyid', 'policy', 'polid', 'pol_id',
            'claimid', 'claim', 'clmid', 'clm_id',
            'premium', 'premamt', 'prem_amt', 'totprem', 'tot_prem',
            'suminsured', 'insured', 'insurer',
            'beneficiary', 'nominee',
            'policystartdate', 'policyenddate', 'policy_start_date', 'policy_end_date',
            'effdt', 'expdt', 'eff_dt', 'exp_dt',  # effective/expiry date
            # insurance-ish
            'underwriting', 'coverage', 'deductible', 'dedamt', 'ded_amt',
            'renewal', 'lapse', 'limamt', 'lim_amt',  # limit amount
            'lob', 'lob_cd',  # line of business
            'cov_cd', 'covdesc', 'cov_desc',  # coverage
            'paidamt', 'rsvamt', 'paid_amt', 'rsv_amt',  # claim paid, reserve
            'agnt', 'agnt_id',  # agent (insurance agent)
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
        def _pattern_hits(patterns: List[str]) -> Tuple[int, List[str]]:
            hits = []
            for col_clean in cols_lower:
                for pattern in patterns:
                    if pattern and pattern in col_clean:
                        hits.append(pattern)
            # de-dup while preserving order
            seen = set()
            uniq = []
            for h in hits:
                if h not in seen:
                    seen.add(h)
                    uniq.append(h)
            return len(uniq), uniq

        healthcare_hit_count, healthcare_hits = _pattern_hits(healthcare_force_patterns)
        
        # Also check original column names for exact matches (case-insensitive)
        has_healthcare_exclusive = healthcare_hit_count > 0
        if not has_healthcare_exclusive:
            healthcare_exact_keywords = ['patient_id', 'patient', 'doctor_id', 'doctor', 'diagnosis']
            for col_orig in cols_original_lower:
                if any(keyword in col_orig for keyword in healthcare_exact_keywords):
                    has_healthcare_exclusive = True
                    print(f"[DOMAIN CLASSIFIER] Healthcare keyword found in column: {all_columns[cols_original_lower.index(col_orig)]}")
                    break
        if has_healthcare_exclusive and healthcare_hits:
            print(f"[DOMAIN CLASSIFIER] Healthcare patterns found: {', '.join(healthcare_hits[:4])}")
        
        # Banking: require at least one STRONG banking-only signal (not shared with Insurance/Finance/Healthcare).
        # NOTE: login_time, logout_time, branch are AMBIGUOUS - healthcare (patient portal, hospital branch) uses them too.
        # We exclude these when healthcare is present (see banking_strong_unambiguous below).
        banking_strong_only = [
            'ifsc', 'ifsccode', 'swift', 'swiftcode', 'iban', 'bic',
            'branch', 'branchid', 'branchcode',
            'loanid', 'loannumber', 'loan_id', 'loan_type',
            'login_time', 'logout_time', 'logintime', 'logouttime',
            'deposit', 'withdraw', 'withdrawal',
        ]
        # Truly banking-exclusive: NOT used in healthcare (patient portal has login/logout, hospitals have branches)
        banking_strong_unambiguous = [
            'ifsc', 'ifsccode', 'swift', 'swiftcode', 'iban', 'bic',
            'loanid', 'loannumber', 'loan_id', 'loan_type',
            'deposit', 'withdraw', 'withdrawal',
        ]
        banking_strong_hits = sum(1 for p in banking_strong_only if any(p in c for c in cols_lower))
        banking_strong_unambiguous_hits = sum(1 for p in banking_strong_unambiguous if any(p in c for c in cols_lower))
        banking_hit_count, banking_hits = _pattern_hits(banking_force_patterns)
        # Only treat as banking-exclusive when we have strong banking-only signals.
        # Don't treat as banking when we only have e.g. BRANCH_CD (insurance agents have branch too) and strong insurance.
        _insurance_combo_for_banking = (  # computed early for banking check
            (1 if any('policy' in c or 'polid' in c for c in cols_lower) else 0) +
            (2 if any('premium' in c or 'premamt' in c for c in cols_lower) else 0) +
            (2 if any('claim' in c or 'clmid' in c for c in cols_lower) else 0)
        )
        # CRITICAL: When healthcare is present, login_time/logout_time and branch are AMBIGUOUS
        # (patient portal, hospital branch). Require unambiguous banking signals (ifsc, swift, loan, deposit, etc.)
        _banking_effective_strong = (
            banking_strong_unambiguous_hits if has_healthcare_exclusive else banking_strong_hits
        )
        has_banking_exclusive = (
            _banking_effective_strong > 0 and banking_hit_count > 0 and
            not (_insurance_combo_for_banking >= 3 and banking_strong_hits <= 1)  # e.g. only BRANCH_CD
        )
        if has_banking_exclusive and banking_hits:
            print(f"[DOMAIN CLASSIFIER] Banking patterns found: {', '.join(banking_hits[:4])}")

        # Check for finance patterns
        finance_hit_count, finance_hits = _pattern_hits(finance_force_patterns)

        # Check for insurance patterns
        insurance_hit_count, insurance_hits = _pattern_hits(insurance_force_patterns)

        # Check for retail patterns
        has_retail_exclusive = False
        retail_force_hits = set()  # which retail patterns matched (to avoid 'tax' alone stealing Finance)
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
                    retail_force_hits.add(pattern)
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
        
        # --- Strong combo logic for Finance / Insurance ---
        # Finance strong: invoice + (gst/tax) + payment_mode OR payroll terms OR profit/loss/budget
        finance_combo_score = 0
        if any('invoice' in c for c in cols_lower):
            finance_combo_score += 1
        if any('gst' in c or 'gstin' in c for c in cols_lower):
            finance_combo_score += 1
        if any('tax' in c for c in cols_lower):
            finance_combo_score += 1
        if any('paymentmode' in c or 'payment_mode' in c for c in cols_lower):
            finance_combo_score += 1
        if any('salary' in c or 'payroll' in c for c in cols_lower):
            finance_combo_score += 2
        if any('expense' in c or 'budget' in c for c in cols_lower):
            finance_combo_score += 1
        if any('profit' in c or 'loss' in c for c in cols_lower):
            finance_combo_score += 1

        # Insurance strong: policy + (premium or sum_insured) OR claim + policy (cols_lower has no underscores)
        # Include abbreviated columns: pol_id, prem_amt, clm_id, eff_dt, exp_dt
        insurance_combo_score = 0
        if any('policy' in c or 'polid' in c or 'pol_id' in c for c in cols_lower):
            insurance_combo_score += 1
        if any('premium' in c or 'premamt' in c or 'prem_amt' in c or 'totprem' in c for c in cols_lower):
            insurance_combo_score += 2
        if any('suminsured' in c or 'limamt' in c or 'lim_amt' in c for c in cols_lower):
            insurance_combo_score += 2
        if any('claim' in c or 'clmid' in c or 'clm_id' in c for c in cols_lower):
            insurance_combo_score += 2
        if any('nominee' in c or 'beneficiary' in c for c in cols_lower):
            insurance_combo_score += 1
        if any('policyenddate' in c or 'policyend' in c or 'expdt' in c or 'exp_dt' in c for c in cols_lower):
            insurance_combo_score += 1
        if any('policystartdate' in c or 'policystart' in c or 'effdt' in c or 'eff_dt' in c for c in cols_lower):
            insurance_combo_score += 1
        if any('deductible' in c or 'dedamt' in c or 'ded_amt' in c for c in cols_lower):
            insurance_combo_score += 1
        if any('coverage' in c or 'cov_cd' in c or 'covdesc' in c for c in cols_lower):
            insurance_combo_score += 1

        has_finance_exclusive = (finance_hit_count >= 2) or (finance_combo_score >= 3)
        # Insurance: trigger on 1+ insurance keyword or combo (policy+premium, claim+policy, etc.)
        has_insurance_exclusive = (insurance_hit_count >= 1) or (insurance_combo_score >= 2)
        # Don't treat as Retail when only 'tax'/'taxamount' matched and we have strong Finance
        if has_retail_exclusive and has_finance_exclusive and retail_force_hits <= {'tax', 'taxamount'}:
            has_retail_exclusive = False

        if has_finance_exclusive and finance_hits:
            print(f"[DOMAIN CLASSIFIER] Finance patterns found: {', '.join(finance_hits[:5])} (combo={finance_combo_score})")
        if has_insurance_exclusive and insurance_hits:
            print(f"[DOMAIN CLASSIFIER] Insurance patterns found: {', '.join(insurance_hits[:5])} (combo={insurance_combo_score})")

        # Domain strength scores from keywords (used for multi-domain mix and fallback – never trust raw ML alone for 95% banking)
        insurance_score = max(0, insurance_hit_count * 2 + insurance_combo_score)
        finance_score = max(0, finance_hit_count * 2 + finance_combo_score)
        banking_score = (banking_strong_hits * 3 + banking_hit_count) if (banking_strong_hits > 0) else 0
        healthcare_score = max(0, healthcare_hit_count * 2)
        retail_score = (4 if has_retail_exclusive else 0) + retail_combo_hits + len(retail_schema_hits)

        # Predict probability for all six classes (used only when we blend with keyword scores)
        proba = self.pipeline.predict_proba([combined_text])[0]
        banking_prob = proba[0]      # Class 0: Banking
        finance_prob = proba[1]      # Class 1: Finance
        insurance_prob = proba[2]    # Class 2: Insurance
        healthcare_prob = proba[3]   # Class 3: Healthcare
        retail_prob = proba[4]       # Class 4: Retail
        other_prob = proba[5]        # Class 5: Other
        
        # APPLY MUTUAL EXCLUSION LOGIC – order matters: Insurance/Finance before Banking so they are not overridden.
        # 1) Pure strong domain signals. Check Insurance and Finance BEFORE Banking (user uploads were misclassified as Banking).
        if has_retail_exclusive and not (has_healthcare_exclusive or has_banking_exclusive or has_finance_exclusive or has_insurance_exclusive):
            retail_prob, banking_prob, finance_prob, insurance_prob, healthcare_prob, other_prob = 0.95, 0.01, 0.01, 0.01, 0.01, 0.01
            print(f"[DOMAIN CLASSIFIER] FORCING Retail: 95% (exclusive patterns)")

        elif has_healthcare_exclusive and not (has_banking_exclusive or has_retail_exclusive or has_finance_exclusive or has_insurance_exclusive):
            healthcare_prob, banking_prob, finance_prob, insurance_prob, retail_prob, other_prob = 0.95, 0.01, 0.01, 0.01, 0.01, 0.01
            print(f"[DOMAIN CLASSIFIER] FORCING Healthcare: 95% (exclusive patterns)")

        # Insurance BEFORE Banking – so policy_no, claim_id, premium, nominee, beneficiary, sum_insured win
        elif has_insurance_exclusive and not (has_banking_exclusive or has_retail_exclusive or has_healthcare_exclusive or has_finance_exclusive):
            insurance_prob, banking_prob, finance_prob, healthcare_prob, retail_prob, other_prob = 0.95, 0.01, 0.01, 0.01, 0.01, 0.01
            print(f"[DOMAIN CLASSIFIER] FORCING Insurance: 95% (exclusive patterns)")

        elif has_finance_exclusive and not (has_healthcare_exclusive or has_retail_exclusive or has_banking_exclusive or has_insurance_exclusive):
            finance_prob, banking_prob, insurance_prob, healthcare_prob, retail_prob, other_prob = 0.95, 0.01, 0.01, 0.01, 0.01, 0.01
            print(f"[DOMAIN CLASSIFIER] FORCING Finance: 95% (exclusive patterns)")

        # Banking only when strong banking-only signals (ifsc, branch, loan, login_time, etc.)
        elif has_banking_exclusive and not (has_healthcare_exclusive or has_retail_exclusive or has_finance_exclusive or has_insurance_exclusive):
            banking_prob, finance_prob, insurance_prob, healthcare_prob, retail_prob, other_prob = 0.95, 0.01, 0.01, 0.01, 0.01, 0.01
            print(f"[DOMAIN CLASSIFIER] FORCING Banking: 95% (exclusive patterns)")

        # Strong retail schema (order_id + product + qty + unit_price + total_amount + payment_method, etc.)
        elif len(retail_schema_hits) >= 4 and not (has_banking_exclusive or has_healthcare_exclusive or has_finance_exclusive or has_insurance_exclusive):
            # Override towards Retail even if ML model was uncertain
            print(f"[DOMAIN CLASSIFIER] Strong Retail schema detected (columns: {', '.join(list(retail_schema_hits)[:6])}). Forcing Retail domain.")
            retail_prob, banking_prob, finance_prob, insurance_prob, healthcare_prob, other_prob = 0.97, 0.005, 0.005, 0.005, 0.01, 0.005
        
        # 2) Mixed or multiple domains – use weighted scores so percentages reflect actual mix and add up to 100%
        elif (has_healthcare_exclusive + has_banking_exclusive + has_retail_exclusive + has_finance_exclusive + has_insurance_exclusive) > 1:
            print(f"[DOMAIN CLASSIFIER] Multiple domains detected – using keyword-weighted percentages.")
            # Weights from domain strength scores (min 0.1 so we don't get exact zeros in pie)
            w_bank = max(0.1, banking_score)
            w_fin = max(0.1, finance_score)
            w_ins = max(0.1, insurance_score)
            w_health = max(0.1, healthcare_score)
            w_retail = max(0.1, retail_score)
            w_other = 0.1
            total_w = w_bank + w_fin + w_ins + w_health + w_retail + w_other
            banking_prob = w_bank / total_w
            finance_prob = w_fin / total_w
            insurance_prob = w_ins / total_w
            healthcare_prob = w_health / total_w
            retail_prob = w_retail / total_w
            other_prob = w_other / total_w

        # 3) No exclusive branch hit – do NOT use raw ML (it biases to Banking). Use keyword scores only.
        elif (insurance_score + finance_score + banking_score + healthcare_score + retail_score) > 0:
            print(f"[DOMAIN CLASSIFIER] Using keyword-based distribution (no single domain forced).")
            w_bank = max(0.05, banking_score)
            w_fin = max(0.05, finance_score)
            w_ins = max(0.05, insurance_score)
            w_health = max(0.05, healthcare_score)
            w_retail = max(0.05, retail_score)
            w_other = 0.05
            total_w = w_bank + w_fin + w_ins + w_health + w_retail + w_other
            banking_prob = w_bank / total_w
            finance_prob = w_fin / total_w
            insurance_prob = w_ins / total_w
            healthcare_prob = w_health / total_w
            retail_prob = w_retail / total_w
            other_prob = w_other / total_w

        # (If all scores are 0 we keep raw ML proba)
        
        # Normalize probabilities to sum to 1.0
        total = banking_prob + finance_prob + insurance_prob + healthcare_prob + retail_prob + other_prob
        if total > 0:
            banking_prob = banking_prob / total
            finance_prob = finance_prob / total
            insurance_prob = insurance_prob / total
            healthcare_prob = healthcare_prob / total
            retail_prob = retail_prob / total
            other_prob = other_prob / total
        
        # Determine primary domain (highest probability)
        max_prob = max(banking_prob, finance_prob, insurance_prob, healthcare_prob, retail_prob, other_prob)
        if banking_prob == max_prob:
            primary_domain = "Banking"
            is_banking = True
        elif finance_prob == max_prob:
            primary_domain = "Finance"
            is_banking = False
        elif insurance_prob == max_prob:
            primary_domain = "Insurance"
            is_banking = False
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
        
        # Build column-level classification: which columns triggered which domain
        column_domain_matches = self._get_column_domain_matches(all_columns)
        
        # Round percentages to 2 decimals and force sum to 100%
        pct_bank = round(banking_prob * 100, 2)
        pct_fin = round(finance_prob * 100, 2)
        pct_ins = round(insurance_prob * 100, 2)
        pct_health = round(healthcare_prob * 100, 2)
        pct_retail = round(retail_prob * 100, 2)
        pct_other = round(other_prob * 100, 2)
        total_pct = pct_bank + pct_fin + pct_ins + pct_health + pct_retail + pct_other
        if total_pct != 100.0:
            # Adjust largest so sum is exactly 100
            idx_max = max(
                range(6),
                key=lambda i: [pct_bank, pct_fin, pct_ins, pct_health, pct_retail, pct_other][i]
            )
            diff = 100.0 - total_pct
            if idx_max == 0: pct_bank += diff
            elif idx_max == 1: pct_fin += diff
            elif idx_max == 2: pct_ins += diff
            elif idx_max == 3: pct_health += diff
            elif idx_max == 4: pct_retail += diff
            else: pct_other += diff
        
        return {
            "is_banking": bool(is_banking),
            "confidence": float(round(max_prob * 100, 2)),
            "domain_label": primary_domain,
            "percentages": {
                "Banking": float(pct_bank),
                "Finance": float(pct_fin),
                "Insurance": float(pct_ins),
                "Healthcare": float(pct_health),
                "Retail": float(pct_retail),
                "Other": float(pct_other)
            },
            "evidence": evidence,
            "column_domain_matches": column_domain_matches,
        }
    
    def _get_column_domain_matches(self, columns: List[str]) -> Dict[str, List[str]]:
        """
        Identify which columns triggered which domain classification.
        Returns dict: {"Healthcare": [...], "Banking": [...], ...}
        """
        cols_lower = [col.lower().replace('_', '').replace('-', '').replace(' ', '') for col in columns]
        result = {"Healthcare": [], "Banking": [], "Finance": [], "Insurance": [], "Retail": [], "Other": []}
        
        healthcare_patterns = ['patient', 'doctor', 'physician', 'diagnosis', 'treatment', 'prescription', 'medication',
            'admission', 'discharge', 'hospital', 'clinic', 'ward', 'labtest', 'bloodtest', 'appointment',
            'medicalrecord', 'vitals', 'symptoms', 'allergies', 'radiology', 'specimen', 'vaccination']
        banking_patterns = ['accountno', 'accountnumber', 'accountid', 'ifsc', 'swift', 'routing', 'iban', 'bic',
            'branchid', 'branchcode', 'deposit', 'withdraw', 'loanid', 'transactionid', 'logintime', 'logouttime']
        finance_patterns = ['invoice', 'gst', 'gstin', 'tax', 'salary', 'payroll', 'ledger', 'journal', 'voucher',
            'receivable', 'payable', 'expense', 'budget', 'profit', 'loss', 'tds', 'pf', 'esi']
        insurance_patterns = ['policy', 'polid', 'claim', 'clmid', 'premium', 'premamt', 'suminsured', 'insured',
            'beneficiary', 'nominee', 'underwriting', 'coverage', 'deductible', 'renewal', 'effdt', 'expdt']
        retail_patterns = ['productid', 'productname', 'sku', 'orderid', 'category', 'unitprice', 'quantity',
            'discount', 'taxamount', 'saleschannel', 'storeid', 'cashierid', 'returnflag']
        
        for i, col in enumerate(columns):
            col_clean = cols_lower[i]
            matched = False
            if any(p in col_clean for p in healthcare_patterns):
                if col not in result["Healthcare"]:
                    result["Healthcare"].append(col)
                matched = True
            if any(p in col_clean for p in banking_patterns):
                if col not in result["Banking"]:
                    result["Banking"].append(col)
                matched = True
            if any(p in col_clean for p in finance_patterns):
                if col not in result["Finance"]:
                    result["Finance"].append(col)
                matched = True
            if any(p in col_clean for p in insurance_patterns):
                if col not in result["Insurance"]:
                    result["Insurance"].append(col)
                matched = True
            if any(p in col_clean for p in retail_patterns):
                if col not in result["Retail"]:
                    result["Retail"].append(col)
                matched = True
            if not matched:
                if col not in result["Other"]:
                    result["Other"].append(col)
        
        return result

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

        # Finance keywords
        finance_core = {
            "invoice_id", "invoice_no", "invoice_date", "payment_mode", "paymentmethod",
            "gst", "gstin", "tax", "tax_amount",
            "salary", "payroll", "expense", "budget", "profit", "loss",
            "ledger", "journal", "voucher", "debit", "credit"
        }

        finance_strong = {
            "tds", "pf", "esi", "cgst", "sgst", "igst",
            "accounts_payable", "accounts_receivable", "receivable", "payable",
            "cost_center", "fiscal_year", "revenue", "cogs", "variance"
        }

        # Insurance keywords
        insurance_core = {
            "policy_no", "policy_id", "policy_start_date", "policy_end_date",
            "claim_id", "premium", "sum_insured", "nominee", "beneficiary"
        }

        insurance_strong = {
            "underwriting", "coverage", "insured", "insurer",
            "renewal_date", "lapse_date", "deductible", "settlement_amount",
            "risk_score", "underwriting_score"
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

        finance_value_keywords = [
            'invoice', 'gst', 'gstin', 'tax', 'salary', 'payroll', 'expense',
            'profit', 'loss', 'budget', 'ledger', 'journal', 'voucher', 'tds',
            'receivable', 'payable'
        ]

        insurance_value_keywords = [
            'policy', 'policy no', 'policy number', 'claim', 'premium',
            'sum insured', 'beneficiary', 'nominee', 'underwriting', 'coverage'
        ]
        
        found_healthcare_in_values = []
        found_banking_in_values = []
        found_retail_in_values = []
        found_finance_in_values = []
        found_insurance_in_values = []
        
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
                # Check finance keywords
                for kw in finance_value_keywords:
                    if kw in val_lower:
                        found_finance_in_values.append(f"value:{kw}")
                        break
                # Check insurance keywords
                for kw in insurance_value_keywords:
                    if kw in val_lower:
                        found_insurance_in_values.append(f"value:{kw}")
                        break
        
        # Check for partial matches (e.g., "credit_amount" contains "credit")
        found_banking_core = []
        found_banking_strong = []
        found_healthcare_core = []
        found_healthcare_strong = []
        found_retail_core = []
        found_retail_strong = []
        found_finance_core = []
        found_finance_strong = []
        found_insurance_core = []
        found_insurance_strong = []
        
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

            # Finance core/strong
            if any(kw in col_lower.replace(' ', '_') for kw in finance_core):
                found_finance_core.append(col)
            elif any(kw in col_lower.replace(' ', '_') for kw in finance_strong):
                found_finance_strong.append(col)

            # Insurance core/strong
            if any(kw in col_lower.replace(' ', '_') for kw in insurance_core):
                found_insurance_core.append(col)
            elif any(kw in col_lower.replace(' ', '_') for kw in insurance_strong):
                found_insurance_strong.append(col)
        
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

        if found_finance_core:
            evidence.append(f"Finance core: {', '.join(found_finance_core[:5])}")
        if found_finance_strong:
            evidence.append(f"Finance signals: {', '.join(found_finance_strong[:5])}")
        if found_finance_in_values:
            evidence.append(f"Finance in data: {', '.join(set(found_finance_in_values[:5]))}")

        if found_insurance_core:
            evidence.append(f"Insurance core: {', '.join(found_insurance_core[:5])}")
        if found_insurance_strong:
            evidence.append(f"Insurance signals: {', '.join(found_insurance_strong[:5])}")
        if found_insurance_in_values:
            evidence.append(f"Insurance in data: {', '.join(set(found_insurance_in_values[:5]))}")
            
        return evidence
    
    def get_domain_split_summary(self, table_names: List[str], all_columns: List[str], sample_values: List[str] = None) -> Dict[str, Any]:
        """
        Get domain classification with percentage breakdown for visualization.
        Used to create pie charts showing domain split.
        
        Returns:
            Dict with percentages, labels, and visual data
        """
        prediction = self.predict(table_names, all_columns, sample_values)
        
        banking_pct = prediction['percentages']['Banking']
        finance_pct = prediction['percentages']['Finance']
        insurance_pct = prediction['percentages']['Insurance']
        healthcare_pct = prediction['percentages']['Healthcare']
        retail_pct = prediction['percentages']['Retail']
        other_pct = prediction['percentages']['Other']
        
        return {
            'percentages': {
                'Banking': round(banking_pct, 1),
                'Finance': round(finance_pct, 1),
                'Insurance': round(insurance_pct, 1),
                'Healthcare': round(healthcare_pct, 1),
                'Retail': round(retail_pct, 1),
                'Other': round(other_pct, 1)
            },
            'primary_domain': prediction['domain_label'],
            'confidence': prediction['confidence'],
            'is_banking': prediction['is_banking'],
            'evidence': prediction['evidence'],
            'column_domain_matches': prediction.get('column_domain_matches', {}),
            'chart_data': {
                'labels': ['Banking', 'Finance', 'Insurance', 'Healthcare', 'Retail', 'Other'],
                'values': [
                    round(banking_pct, 1),
                    round(finance_pct, 1),
                    round(insurance_pct, 1),
                    round(healthcare_pct, 1),
                    round(retail_pct, 1),
                    round(other_pct, 1)
                ],
                # Teal Banking, Indigo Finance, Purple Insurance, Turquoise Healthcare, Amber Retail, Gray Other
                'colors': ['#0F766E', '#4F46E5', '#7C3AED', '#14B8A6', '#F59E0B', '#64748B']
            },
            'explanation': self._generate_domain_explanation(prediction['percentages'], prediction['domain_label'], prediction['evidence'])
        }
    
    def _generate_domain_explanation(self, percentages: dict, primary_domain: str, evidence: List[str]) -> str:
        """Generate human-readable explanation of domain classification"""
        banking_pct = percentages.get('Banking', 0)
        finance_pct = percentages.get('Finance', 0)
        insurance_pct = percentages.get('Insurance', 0)
        healthcare_pct = percentages.get('Healthcare', 0)
        retail_pct = percentages.get('Retail', 0)
        other_pct = percentages.get('Other', 0)
        
        max_pct = max(banking_pct, finance_pct, insurance_pct, healthcare_pct, retail_pct, other_pct)
        
        # Generate intro based on primary domain and confidence
        if primary_domain == "Banking":
            if banking_pct >= 70:
                intro = "This appears to be a <strong>Banking database</strong> with high confidence."
            else:
                intro = "This looks like a <strong>Banking-related database</strong>."
        elif primary_domain == "Finance":
            if finance_pct >= 70:
                intro = "This appears to be a <strong>Finance / Accounting database</strong> with high confidence."
            else:
                intro = "This looks like a <strong>Finance / accounting-related database</strong>."
        elif primary_domain == "Insurance":
            if insurance_pct >= 70:
                intro = "This appears to be an <strong>Insurance database</strong> with high confidence."
            else:
                intro = "This looks like an <strong>Insurance-related database</strong>."
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
    
    def classify_table(self, df: pd.DataFrame, table_name: str) -> Tuple[bool, float, List[str]]:
        """
        Backward-compatible method for db_grouping_engine.py
        Classifies a single table and returns (is_banking, confidence, evidence)
        
        Args:
            df: DataFrame to classify
            table_name: Name of the table
            
        Returns:
            Tuple of (is_banking: bool, confidence: float, evidence: List[str])
        """
        all_columns = list(df.columns)
        sample_values = []
        
        # Get sample values from first few rows
        for col in df.columns:
            sample_vals = df[col].dropna().head(10).astype(str).tolist()
            sample_values.extend(sample_vals)
        
        result = self.predict(
            table_names=[table_name],
            all_columns=all_columns,
            sample_values=sample_values
        )
        
        is_banking = result.get('is_banking', False)
        confidence = result.get('confidence', 0.0)
        evidence = result.get('evidence', [])
        
        return is_banking, confidence, evidence
