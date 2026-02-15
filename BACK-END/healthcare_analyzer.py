"""
Healthcare Analyzer - For Healthcare domain only.
Finds date/timestamp columns per table (excluding date of birth), sorts all data ascending,
and produces a diagram-ready timeline format: Start ----|----|---- End.
Column purposes and data flow explanations are inferred from observed column names and data patterns (no hardcoding).
"""

import pandas as pd
from typing import Dict, Any, List, Tuple, Optional
from models import TableAnalysis
import re


# Columns to EXCLUDE - these are date of birth, not event dates
DOB_EXCLUDE_KEYWORDS = [
    'dob', 'date_of_birth', 'birth_date', 'birthdate', 'birth',
    'dateofbirth', 'patient_dob', 'birth_day'
]

# Column classification tags for healthcare explanation
COLUMN_CLASS_PK = "Primary Key (PK)"
COLUMN_CLASS_FK = "Foreign Key (FK)"
COLUMN_CLASS_DATE = "Date column"
COLUMN_CLASS_TIMESTAMP = "Timestamp column"
COLUMN_CLASS_STATUS = "Status column"
COLUMN_CLASS_AMOUNT = "Amount column"
COLUMN_CLASS_DESCRIPTION = "Description column"
COLUMN_CLASS_OTHER = "Other"

# Case split: gap (hours) between events above this = new Case ID for same patient
HEALTHCARE_CASE_GAP_HOURS = 24.0


class HealthcareAnalyzer:
    """
    Analyzes healthcare tables: finds date/timestamp columns (excluding DOB),
    sorts each table ascending, merges into one timeline for diagram UI.
    """

    def __init__(self):
        self.date_keywords = [
            'date', 'time', 'timestamp', 'created', 'admission', 'discharge',
            'reg_date', 'appt_date', 'donation_date', 'visit', 'appointment',
            'recorded', 'scheduled', 'admitted', 'discharged'
        ]

    def _is_dob_column(self, col_name: str) -> bool:
        """Return True if column is date of birth (exclude from analysis)."""
        col_lower = col_name.lower().replace('_', '').replace(' ', '')
        for kw in DOB_EXCLUDE_KEYWORDS:
            if kw.replace('_', '') in col_lower or col_lower in kw.replace('_', ''):
                return True
        return False

    def _identify_table_workflow_role(self, table_name: str, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Identify what this table represents in hospital workflow.
        Returns: { role, role_explanation } e.g. register, appointment, treatment, lab, billing, login_logout.
        Order matters: check specific patterns (login, lab, treatment, etc.) BEFORE generic patient.
        Also scans sample row data values to detect event patterns (not just column names).
        """
        tbl_lower = table_name.lower().replace('-', '_')
        cols_lower = " ".join(c.lower() for c in df.columns)
        cols_list = [c.lower() for c in df.columns]
        
        # FIRST: Scan sample rows for event patterns in actual data values
        # This helps detect events even when column names don't indicate the event type
        if not df.empty:
            sample_rows = df.head(5)  # Check first 5 rows
            for _, row in sample_rows.iterrows():
                scanned_event = self._scan_row_for_event_pattern(row, list(df.columns))
                if scanned_event:
                    # Map scanned event to role
                    event_to_role = {
                        'PATIENT_REGISTERED': 'register',
                        'APPOINTMENT_BOOKED': 'appointment_booked',
                        'DOCTOR_ASSIGNED': 'doctor_assignment',
                        'LAB_TEST_ORDERED': 'lab_order',
                        'LAB_RESULT_GENERATED': 'lab_result',
                        'MEDICINE_PRESCRIBED': 'prescription',
                        'PHARMACY_DISPENSED': 'pharmacy',
                        'INSURANCE_VERIFIED': 'insurance',
                        'BILL_PAID': 'billing_paid',
                        'FOLLOWUP_VISIT_SCHEDULED': 'followup',
                    }
                    if scanned_event in event_to_role:
                        role = event_to_role[scanned_event]
                        role_explanations = {
                            'register': "Patient registration. When the patient was registered in the hospital.",
                            'appointment_booked': "Appointment booking. When the patient appointment was booked.",
                            'doctor_assignment': "Doctor assignment. When a doctor was assigned to treat the patient.",
                            'lab_order': "Lab test order. When a doctor ordered a lab test for the patient.",
                            'lab_result': "Lab test result. Investigation reports for the patient.",
                            'prescription': "Medicine prescription. When doctor prescribed medicine to the patient.",
                            'pharmacy': "Pharmacy dispensing record. When medicine was dispensed to the patient.",
                            'insurance': "Insurance verification. When patient insurance was verified.",
                            'billing_paid': "Bill payment record. When the patient paid the bill.",
                            'followup': "Followup visit scheduled. When a followup appointment was scheduled for the patient.",
                        }
                        return {
                            "role": role,
                            "role_explanation": role_explanations.get(role, "Healthcare record detected from data patterns.")
                        }

        # Login/Logout - has login_time and logout_time (check early, before generic patient)
        if 'login_time' in cols_list and 'logout_time' in cols_list:
            return {"role": "login_logout", "role_explanation": "Patient or user login/logout. Session start and end times."}
        if 'log' in tbl_lower and ('login' in cols_lower or 'logout' in cols_lower):
            return {"role": "login_logout", "role_explanation": "Login/logout record. When the user logged in and out."}

        # Appointment - has appointment_id and appointment_time/date
        if 'appt' in tbl_lower or 'appointment' in tbl_lower:
            # Check if this is booking (has booking/booked) vs just appointment record
            if 'book' in cols_lower or 'booked' in cols_lower or 'booking' in cols_lower:
                return {"role": "appointment_booked", "role_explanation": "Appointment booking. When the patient appointment was booked."}
            return {"role": "appointment", "role_explanation": "Appointment booking. When the patient was scheduled to come to the hospital."}
        if 'appointment_id' in cols_list and ('appointment_time' in cols_list or 'appt_date' in cols_lower):
            if 'book' in cols_lower or 'booked' in cols_lower:
                return {"role": "appointment_booked", "role_explanation": "Appointment booking. When the patient appointment was booked."}
            return {"role": "appointment", "role_explanation": "Appointment booking. When the patient was scheduled to come."}

        # Treatment - has treatment_id, treatment_type, or treatment_time
        if 'treatment' in tbl_lower or ('treatment' in cols_lower and ('treatment_type' in cols_list or 'treatment_time' in cols_list or 'treatment_id' in cols_list)):
            return {"role": "treatment", "role_explanation": "Treatment or procedure record. What was done to the patient (e.g. ECG, X-Ray)."}

        # Lab Order - test ordered (before result is generated)
        if ('lab' in tbl_lower or 'test' in tbl_lower) and ('order' in tbl_lower or 'ordered' in cols_lower or 'order_date' in cols_lower or 'order_time' in cols_lower):
            return {"role": "lab_order", "role_explanation": "Lab test order. When a doctor ordered a lab test for the patient."}
        if 'test_order' in cols_lower or 'lab_order' in cols_lower:
            return {"role": "lab_order", "role_explanation": "Lab test order. When a doctor ordered a lab test for the patient."}
        
        # Lab Result - test_name + result, or lab_id, or test_time (result generated)
        if 'lab' in tbl_lower or 'test' in tbl_lower or 'report' in tbl_lower:
            # Check if this is a result (has result value) vs order (has order status)
            if 'result' in cols_lower and ('result_value' in cols_list or 'test_result' in cols_list or 'lab_result' in cols_list):
                return {"role": "lab_result", "role_explanation": "Lab test result. Investigation reports for the patient."}
            # If no result column but has report/test, assume it's a result table
            if 'report' in tbl_lower or 'result' in cols_lower:
                return {"role": "lab_result", "role_explanation": "Lab test result. Investigation reports for the patient."}
        if ('test_name' in cols_list or 'test_result' in cols_list) and ('result' in cols_list or 'test_time' in cols_list):
            return {"role": "lab_result", "role_explanation": "Lab test result. Investigation reports for the patient."}
        if 'result' in cols_list and ('test' in cols_lower or 'lab' in cols_lower):
            return {"role": "lab_result", "role_explanation": "Lab test result. Investigation reports for the patient."}

        # Pharmacy - dispense, pharmacy, medicine dispensed
        if 'pharmacy' in tbl_lower or 'dispense' in tbl_lower or 'dispensing' in tbl_lower:
            return {"role": "pharmacy", "role_explanation": "Pharmacy dispensing record. When medicine was dispensed to the patient."}
        if 'dispense' in cols_lower or ('pharmacy' in cols_lower and ('dispense' in cols_lower or 'dispensed' in cols_lower)):
            return {"role": "pharmacy", "role_explanation": "Pharmacy dispensing record. When medicine was dispensed to the patient."}
        
        # Prescription/Medicine - prescription, medicine prescribed
        if 'prescription' in tbl_lower or 'medicine' in tbl_lower or 'medication' in tbl_lower:
            if 'prescribe' in cols_lower or 'prescribed' in cols_lower or 'prescription_date' in cols_lower:
                return {"role": "prescription", "role_explanation": "Medicine prescription. When doctor prescribed medicine to the patient."}
            return {"role": "prescription", "role_explanation": "Medicine prescription. When doctor prescribed medicine to the patient."}
        if 'prescribe' in cols_lower or ('medicine' in cols_lower and ('prescribe' in cols_lower or 'prescribed' in cols_lower)):
            return {"role": "prescription", "role_explanation": "Medicine prescription. When doctor prescribed medicine to the patient."}
        
        # Insurance - insurance verification
        if 'insurance' in tbl_lower:
            if 'verify' in cols_lower or 'verified' in cols_lower or 'verification' in cols_lower:
                return {"role": "insurance", "role_explanation": "Insurance verification. When patient insurance was verified."}
            return {"role": "insurance", "role_explanation": "Insurance record. Insurance verification or claim information."}
        if 'insurance' in cols_lower and ('verify' in cols_lower or 'verified' in cols_lower or 'verification' in cols_lower):
            return {"role": "insurance", "role_explanation": "Insurance verification. When patient insurance was verified."}
        
        # Billing - bill_id, bill_amount, bill_time
        if 'bill' in tbl_lower or 'billing' in tbl_lower:
            # Check if this is payment (paid) vs just bill generation
            if 'paid' in cols_lower or 'payment' in cols_lower or 'payment_date' in cols_lower:
                return {"role": "billing_paid", "role_explanation": "Bill payment record. When the patient paid the bill."}
            return {"role": "billing", "role_explanation": "Billing record. Amount to be paid for the stay or service."}
        if 'bill' in cols_lower and 'amount' in cols_lower:
            if 'paid' in cols_lower or 'payment' in cols_lower:
                return {"role": "billing_paid", "role_explanation": "Bill payment record. When the patient paid the bill."}
            return {"role": "billing", "role_explanation": "Billing record. Amount to be paid for the stay or service."}

        # Admission, Discharge, Doctor Assignment - specific patterns
        if 'admission' in tbl_lower or 'admit' in tbl_lower or ('admission' in cols_lower and ('admission_date' in cols_lower or 'admission_timestamp' in cols_lower)):
            return {"role": "admission", "role_explanation": "Patient admission. When the patient was admitted to the ward or unit."}
        if 'discharge' in tbl_lower or ('discharge' in cols_lower and ('discharge_date' in cols_lower or 'discharge_time' in cols_lower)):
            return {"role": "discharge", "role_explanation": "Discharge record. When the patient left the hospital."}
        
        # Doctor Assignment - when doctor is assigned to patient (different from doctor master table)
        if 'doctor' in tbl_lower and ('assign' in tbl_lower or 'assignment' in tbl_lower or 'assigned' in cols_lower):
            return {"role": "doctor_assignment", "role_explanation": "Doctor assignment. When a doctor was assigned to treat the patient."}
        if 'doctor' in cols_lower and ('assign' in cols_lower or 'assigned' in cols_lower or 'assignment' in cols_lower):
            return {"role": "doctor_assignment", "role_explanation": "Doctor assignment. When a doctor was assigned to treat the patient."}
        
        # Doctor master table (not assignment)
        if 'doctor' in tbl_lower and 'appointment' not in cols_lower and 'assign' not in cols_lower:
            return {"role": "doctor", "role_explanation": "Doctor or staff record. Links which doctor attended the patient."}
        if 'doctor' in cols_lower and 'doctor_id' in cols_list and 'appointment' not in cols_lower and 'assign' not in cols_lower:
            return {"role": "doctor", "role_explanation": "Doctor or staff record. Links which doctor attended the patient."}
        
        # Followup Visit - scheduled followup
        if 'followup' in tbl_lower or 'follow_up' in tbl_lower or 'follow-up' in tbl_lower:
            return {"role": "followup", "role_explanation": "Followup visit scheduled. When a followup appointment was scheduled for the patient."}
        if 'followup' in cols_lower or 'follow_up' in cols_lower or ('follow' in cols_lower and 'up' in cols_lower):
            if 'schedule' in cols_lower or 'scheduled' in cols_lower or 'date' in cols_lower:
                return {"role": "followup", "role_explanation": "Followup visit scheduled. When a followup appointment was scheduled for the patient."}

        # Patient/Register - patient master: has patient_id + (first_name/last_name/dob) - NOT test_name/result
        has_patient_id = 'patient_id' in cols_list
        has_patient_master_cols = any(k in cols_lower for k in ['first_name', 'last_name', 'dob', 'date_of_birth', 'gender', 'city'])
        if 'patient' in tbl_lower:
            # Check if this is registration (has registration date/time) vs just patient master
            if 'register' in cols_lower or 'registration' in cols_lower or 'reg_date' in cols_lower or 'reg_time' in cols_lower:
                return {"role": "register", "role_explanation": "Patient registration. When the patient was registered in the hospital."}
            return {"role": "register", "role_explanation": "Patient registration. Master record of each patient registered in the hospital."}
        if has_patient_id and has_patient_master_cols and 'appointment' not in cols_lower and 'bill' not in cols_lower and 'treatment' not in cols_lower and 'test' not in cols_lower:
            if 'register' in cols_lower or 'registration' in cols_lower:
                return {"role": "register", "role_explanation": "Patient registration. When the patient was registered."}
            return {"role": "register", "role_explanation": "Patient registration. Master record of each patient."}

        if 'registration' in tbl_lower or ('visit' in tbl_lower and 'admission' not in cols_lower):
            return {"role": "register", "role_explanation": "Registration or visit log. When the patient first came or was registered."}
        if 'donation' in tbl_lower:
            return {"role": "donation", "role_explanation": "Blood or organ donation record."}
        return {"role": "other", "role_explanation": "Healthcare-related table. Part of hospital workflow."}

    def _find_date_timestamp_columns(self, df: pd.DataFrame) -> List[Tuple[str, Optional[str]]]:
        """
        Find (date_col, time_col) for EVENT TIME - when the hospital action happened.
        Prefer: event_time, created_timestamp, event_timestamp, recorded_at (actual event time).
        Fallback: admission_date+time, appointment_date+time, reg_date, etc.
        Ensures chronological order matches when actions actually occurred.
        """
        # Event-time columns: when the action happened (preferred for sort order)
        # Dynamic detection - column name determines event type (no hardcoded table names)
        event_time_patterns = [
            'register_time', 'reg_time', 'registration_time', 'registration_timestamp',
            'visit_time', 'visit_date', 'appointment_time', 'appt_time', 'appointment_booked_time', 'booked_time',
            'procedure_time', 'treatment_time', 'treatment_timestamp',
            'dispense_time', 'pharmacy_time', 'pharmacy_dispense_time',
            'prescribe_time', 'prescription_time', 'medicine_prescribed_time',
            'test_time', 'lab_time', 'lab_order_time', 'test_order_time', 'lab_result_time', 'test_result_time',
            'bill_time', 'bill_timestamp', 'billing_time', 'payment_time', 'bill_paid_time',
            'insurance_verify_time', 'insurance_verification_time', 'verify_time',
            'doctor_assigned_time', 'assignment_time', 'doctor_assignment_time',
            'followup_time', 'follow_up_time', 'followup_scheduled_time',
            'login_time', 'logout_time',
            'event_time', 'event_timestamp', 'created_timestamp', 'created_at',
            'recorded_at', 'discharge_timestamp', 'admission_time',
            'activity_date', 'activity_time', 'service_date', 'record_date',
            'event_date', 'action_date', 'transaction_date', 'updated_at',
        ]
        # Date+time pairs for admission/registration/appointment
        date_time_patterns = [
            ('admission', 'admission_date', 'admission_time'),
            ('discharge', 'discharge_date', 'discharge_time'),
            ('reg', 'reg_date', None),
            ('registration', 'registration_date', 'registration_time'),
            ('appointment', 'appointment_date', 'appointment_time'),
            ('appt', 'appt_date', 'appt_time'),
        ]

        def is_parseable(col: str) -> bool:
            try:
                sample = df[col].dropna().head(10)
                if len(sample) == 0:
                    return False
                parsed = pd.to_datetime(sample, errors='coerce')
                return parsed.notna().sum() >= len(sample) * 0.5
            except Exception:
                return False

        # 1. Prefer full event-time column (single column with datetime)
        for col in df.columns:
            if self._is_dob_column(col):
                continue
            col_lower = col.lower()
            if any(pat in col_lower for pat in event_time_patterns) and is_parseable(col):
                sample = df[col].dropna().iloc[0] if len(df) > 0 else None
                if sample is not None:
                    parsed = pd.to_datetime(sample, errors='coerce')
                    if pd.notna(parsed):
                        return [(col, None)]

        # 2. Admission: admission_date + admission_time (event = when admitted)
        adm_col = next((c for c in df.columns if ('admission' in c.lower() or 'admit' in c.lower()) and ('date' in c.lower() or 'time' in c.lower()) and is_parseable(c)), None)
        if adm_col:
            adm_time = next((c for c in df.columns if c != adm_col and 'admission' in c.lower() and 'time' in c.lower() and 'date' not in c.lower()), None)
            if adm_time and adm_time in df.columns:
                return [(adm_col, adm_time)]
            return [(adm_col, None)]

        # 3. Registration: reg_date, registration_date, created_timestamp
        for col in df.columns:
            if self._is_dob_column(col):
                continue
            col_lower = col.lower()
            if ('reg_date' in col_lower or 'registration_date' in col_lower or 'visit_date' in col_lower) and is_parseable(col):
                time_col = next((c for c in df.columns if c != col and 'time' in c.lower() and 'date' not in c.lower() and 'stamp' not in c.lower()), None)
                return [(col, time_col if time_col and time_col in df.columns else None)]

        # 4. Appointment: prefer created_timestamp (event) over appointment_date (scheduled)
        if 'created_timestamp' in df.columns and is_parseable('created_timestamp'):
            return [('created_timestamp', None)]
        appt_col = self._find_appointment_column(df)
        if appt_col and appt_col in df.columns:
            time_col = next((c for c in df.columns if 'appointment_time' in c.lower() or 'appt_time' in c.lower()), None)
            return [(appt_col, time_col if time_col and time_col in df.columns else None)]

        # 5. Bill: bill_timestamp or bill_date
        for col in df.columns:
            if 'bill_timestamp' in col.lower() and is_parseable(col):
                return [(col, None)]
            if 'bill_date' in col.lower() and is_parseable(col):
                return [(col, None)]

        # 6. Fallback: any date/timestamp column
        candidates = []
        for col in df.columns:
            if self._is_dob_column(col):
                continue
            col_lower = col.lower()
            if any(k in col_lower for k in ['date', 'time', 'timestamp', 'created', 'recorded']):
                if is_parseable(col):
                    candidates.append(col)
        if not candidates:
            return []
        col = candidates[0]
        time_col = next((c for c in df.columns if c != col and 'time' in c.lower() and 'date' not in c.lower() and 'stamp' not in c.lower() and not self._is_dob_column(c)), None)
        return [(col, time_col if time_col and time_col in df.columns else None)]

    def _infer_column_purpose(self, col_name: str, series: pd.Series, df: pd.DataFrame = None) -> Dict[str, Any]:
        """
        Infer healthcare column purpose and classification from observed column name and data.
        Returns: purpose, work_explanation, null_explanation, column_classification, link_explanation (for FK).
        """
        col_lower = col_name.lower().replace('-', '_')
        tokens = re.split(r'[_\s]+', col_lower)

        null_count = series.isna().sum()
        total = len(series)
        null_pct = (null_count / total * 100) if total > 0 else 0
        non_null = series.dropna()
        sample_vals = non_null.head(5).astype(str).tolist() if len(non_null) > 0 else []
        is_unique = series.nunique() == len(series) and len(series) > 0

        purpose = None
        work_explanation = None
        column_classification = COLUMN_CLASS_OTHER
        link_explanation = None

        # Healthcare-purpose patterns (observed from column name)
        if tokens[-1] == 'id' or col_lower.endswith('_id'):
            base = '_'.join(tokens[:-1]) if len(tokens) > 1 else 'record'
            purpose = f"{base.replace('_', ' ').title()} identifier"
            is_first_col = df is not None and len(df.columns) > 0 and col_name == df.columns[0]
            if is_first_col and (col_lower.endswith('_id') or is_unique):
                column_classification = COLUMN_CLASS_PK
                work_explanation = "Unique ID for this row in the table (primary key)."
            elif not is_first_col and base in ('patient', 'doctor', 'appointment', 'admission', 'treatment', 'bill', 'discharge'):
                column_classification = COLUMN_CLASS_FK
                if base == 'patient':
                    link_explanation = "Links this row to the patient master record. Same ID appears in patient table."
                elif base == 'doctor':
                    link_explanation = "Links this row to the doctor who attended. Same ID in doctor table."
                elif base == 'appointment':
                    link_explanation = "Links this row to the appointment booking. Connects admission to scheduled time."
                elif base == 'admission':
                    link_explanation = "Links this row to the admission record. Connects billing or treatment to that admission."
                elif base == 'treatment':
                    link_explanation = "Links this row to the treatment or procedure record."
                elif base == 'bill':
                    link_explanation = "Links this row to the bill record for this stay."
                elif base == 'discharge':
                    link_explanation = "Links this row to the discharge record."
            elif is_unique:
                column_classification = COLUMN_CLASS_PK
                work_explanation = "Unique ID for this row in the table."
        elif 'admission' in col_lower or 'admit' in col_lower:
            if 'date' in tokens or 'time' in tokens or 'stamp' in col_lower:
                purpose = "Patient admission date/time"
                work_explanation = "When patient was admitted"
                column_classification = COLUMN_CLASS_TIMESTAMP if 'stamp' in col_lower or 'time' in col_lower else COLUMN_CLASS_DATE
            else:
                purpose = "Admission identifier"
        elif 'discharge' in col_lower:
            if 'date' in tokens or 'time' in tokens or 'stamp' in col_lower:
                purpose = "Patient discharge date/time"
                work_explanation = "When patient was discharged"
                column_classification = COLUMN_CLASS_TIMESTAMP if 'stamp' in col_lower or 'time' in col_lower else COLUMN_CLASS_DATE
            else:
                purpose = "Discharge information"
        elif 'appt' in col_lower or 'appointment' in col_lower:
            if 'date' in tokens or 'time' in tokens or 'stamp' in col_lower:
                purpose = "Patient appointment date/time"
                work_explanation = "When appointment was scheduled"
                column_classification = COLUMN_CLASS_TIMESTAMP if 'stamp' in col_lower or 'time' in col_lower else COLUMN_CLASS_DATE
            else:
                purpose = "Appointment identifier"
        elif 'reg' in col_lower or 'registration' in col_lower or 'visit' in col_lower:
            if 'date' in tokens or 'time' in tokens:
                purpose = "Patient visit/registration date/time"
                work_explanation = "When patient registered or visited"
                column_classification = COLUMN_CLASS_TIMESTAMP if 'stamp' in col_lower or 'time' in col_lower else COLUMN_CLASS_DATE
            else:
                purpose = "Registration identifier"
        elif 'donation' in col_lower:
            if 'date' in tokens:
                purpose = "Blood donation date"
                work_explanation = "When donation was made"
                column_classification = COLUMN_CLASS_DATE
            else:
                purpose = "Donation information"
        elif 'lab' in col_lower or 'test' in col_lower or 'report' in col_lower or 'result' in col_lower:
            purpose = "Lab test or report"
            work_explanation = "Test result or report value"
            column_classification = COLUMN_CLASS_DESCRIPTION
        elif 'date' in tokens or col_lower.endswith('_date'):
            idx = next((i for i, t in enumerate(tokens) if t == 'date'), -1)
            prefix = ' '.join(tokens[:idx]).replace('_', ' ').title() if idx > 0 else 'Event'
            purpose = f"{prefix} date" if prefix else "Date"
            column_classification = COLUMN_CLASS_DATE
        elif 'time' in tokens or col_lower.endswith('_time') or 'stamp' in col_lower or 'timestamp' in col_lower:
            idx = next((i for i, t in enumerate(tokens) if t in ('time', 'stamp')), -1)
            prefix = ' '.join(tokens[:idx]).replace('_', ' ').title() if idx > 0 else 'Event'
            purpose = f"{prefix} time" if prefix else "Event time"
            column_classification = COLUMN_CLASS_TIMESTAMP
        elif 'reason' in tokens or 'diagnosis' in tokens or 'symptom' in tokens:
            purpose = "Reason for visit or diagnosis"
            work_explanation = "Why patient came or condition"
            column_classification = COLUMN_CLASS_DESCRIPTION
        elif 'ward' in tokens or 'dept' in tokens or 'department' in tokens:
            purpose = "Department or care unit"
            work_explanation = "Where patient was treated"
            column_classification = COLUMN_CLASS_DESCRIPTION
        elif 'slot' in tokens:
            purpose = "Time slot"
            work_explanation = "Morning/Afternoon/Evening session"
        elif 'patient' in tokens:
            purpose = "Patient identifier"
            work_explanation = "Links to patient record"
            if 'id' in col_lower:
                column_classification = COLUMN_CLASS_FK
                link_explanation = "Links this row to the patient master record. Same ID in patient table."
        elif 'volume' in col_lower or 'ml' in col_lower or 'amount' in col_lower or 'bill_amount' in col_lower:
            purpose = "Quantity, volume or amount"
            work_explanation = "Amount (e.g. bill amount, blood volume)"
            column_classification = COLUMN_CLASS_AMOUNT
        elif 'blood' in col_lower and 'group' in col_lower:
            purpose = "Blood type"
            column_classification = COLUMN_CLASS_DESCRIPTION
        elif 'name' in tokens:
            purpose = "Name"
        elif 'status' in tokens or 'result' in tokens:
            purpose = "Status or outcome"
            column_classification = COLUMN_CLASS_STATUS
        elif 'type' in tokens and 'treatment' in col_lower:
            purpose = "Type of treatment or procedure"
            work_explanation = "What was done (e.g. ECG, X-Ray)"
            column_classification = COLUMN_CLASS_DESCRIPTION
        else:
            purpose = col_name.replace('_', ' ').title()

        return {
            "purpose": purpose,
            "work_explanation": work_explanation,
            "null_pct": float(round(null_pct, 1)),
            "null_explanation": "Not recorded or missing" if null_pct > 0 else None,
            "column_classification": column_classification,
            "link_explanation": link_explanation,
        }

    def _build_work_summary(self, table_name: str, raw_record: Dict, column_purposes: Dict, file_name: str = "") -> str:
        """
        Build one-line healthcare work explanation from observed columns. Infers type from column names.
        """
        tbl_lower = table_name.lower()
        file_lower = (file_name or "").lower()
        cols_lower = " ".join(c.lower() for c in raw_record.keys())
        parts = []
        patient_id = ward = reason = ''
        for col, val in raw_record.items():
            if not val or str(val).strip() == '':
                continue
            col_lower = col.lower()
            purp = (column_purposes.get(col) or {}).get('purpose', '').lower()
            if ('patient' in col_lower and ('id' in col_lower or col_lower.endswith('_id'))) or (purp and 'patient' in purp and 'identifier' in purp):
                patient_id = str(val)
            elif 'ward' in col_lower or 'dept' in col_lower or 'department' in col_lower or 'care unit' in purp:
                ward = str(val)
            elif 'reason' in col_lower or 'diagnosis' in col_lower or 'symptom' in col_lower or 'reason' in purp:
                reason = str(val)
        is_reg = 'reg' in tbl_lower or 'registration' in tbl_lower or 'visit' in tbl_lower or 'reg_' in cols_lower or 'reg_date' in cols_lower
        is_appt = 'appt' in tbl_lower or 'appointment' in tbl_lower or 'appt_' in cols_lower or 'appt_date' in cols_lower
        is_adm = 'adm' in tbl_lower or 'admission' in tbl_lower or 'admission' in cols_lower
        is_discharge = 'discharge' in tbl_lower or 'discharge' in cols_lower
        is_donation = 'donation' in tbl_lower or 'donation' in cols_lower
        is_lab = 'lab' in tbl_lower or 'test' in tbl_lower or 'report' in tbl_lower or 'lab' in cols_lower or 'test' in cols_lower or 'report' in cols_lower
        is_pharmacy = 'pharmacy' in tbl_lower or 'dispense' in tbl_lower or 'dispense' in cols_lower
        is_prescription = 'prescription' in tbl_lower or 'prescribe' in tbl_lower or 'medicine' in tbl_lower or 'prescribe' in cols_lower
        is_insurance = 'insurance' in tbl_lower or ('insurance' in cols_lower and 'verify' in cols_lower)
        is_billing_paid = ('bill' in tbl_lower or 'billing' in tbl_lower) and ('paid' in cols_lower or 'payment' in cols_lower)
        is_doctor_assignment = 'doctor' in tbl_lower and ('assign' in tbl_lower or 'assign' in cols_lower)
        is_followup = 'followup' in tbl_lower or 'follow_up' in tbl_lower or 'followup' in cols_lower

        if is_reg:
            if patient_id:
                parts.append(f"Patient {patient_id} registered")
            if ward:
                parts.append(f"at {ward}")
        elif is_appt:
            if patient_id:
                parts.append(f"Patient {patient_id} appointment")
            if reason:
                parts.append(f"for {reason}")
            if ward:
                parts.append(f"at {ward}")
        elif is_adm:
            if patient_id:
                parts.append(f"Patient {patient_id} admitted")
            if ward:
                parts.append(f"to {ward}")
        elif is_discharge:
            if patient_id:
                parts.append(f"Patient {patient_id} discharged")
            if ward:
                parts.append(f"from {ward}")
        elif is_donation:
            if patient_id:
                parts.append(f"Blood donation by patient {patient_id}")
        elif is_lab:
            if patient_id:
                parts.append(f"Lab/test report for patient {patient_id}")
            else:
                parts.append("Lab or test record")
        elif is_pharmacy:
            if patient_id:
                parts.append(f"Pharmacy dispensed medicine for patient {patient_id}")
            else:
                parts.append("Pharmacy dispensing record")
        elif is_prescription:
            if patient_id:
                parts.append(f"Medicine prescribed for patient {patient_id}")
            else:
                parts.append("Prescription record")
        elif is_insurance:
            if patient_id:
                parts.append(f"Insurance verified for patient {patient_id}")
            else:
                parts.append("Insurance verification record")
        elif is_billing_paid:
            if patient_id:
                parts.append(f"Bill paid by patient {patient_id}")
            else:
                parts.append("Bill payment record")
        elif is_doctor_assignment:
            if patient_id:
                parts.append(f"Doctor assigned to patient {patient_id}")
            else:
                parts.append("Doctor assignment record")
        elif is_followup:
            if patient_id:
                parts.append(f"Followup visit scheduled for patient {patient_id}")
            else:
                parts.append("Followup visit record")
        else:
            if patient_id:
                parts.append(f"Record for patient {patient_id}")
            if ward:
                parts.append(f"at {ward}")
        return " ".join(parts).strip() or "Healthcare record"

    def _extract_key_values_by_purpose(
        self, raw_record: Dict, column_purposes: Dict
    ) -> Dict[str, str]:
        """
        Dynamically extract key values from record using column purpose patterns.
        No hardcoded column names.
        """
        out = {}
        for col, val in raw_record.items():
            if not val or str(val).strip() == '':
                continue
            v = str(val).strip()
            purp = (column_purposes.get(col) or {}).get('purpose', '').lower()
            col_lower = col.lower()
            if 'patient' in purp and 'identifier' in purp:
                out['patient_id'] = v
            elif 'name' in purp or col_lower == 'name':
                out['name'] = v
            elif 'reason' in purp or 'diagnosis' in purp or 'symptom' in purp or 'reason' in col_lower:
                out['reason'] = v
            elif 'amount' in purp or 'volume' in purp or 'amount' in col_lower or 'bill' in col_lower and 'amount' in col_lower:
                out['amount'] = v
            elif 'ward' in col_lower or 'care unit' in purp or 'department' in purp:
                out['ward'] = v
            elif 'status' in purp or 'status' in col_lower or 'result' in col_lower:
                out['status'] = v
            elif 'treatment' in purp or 'procedure' in purp or 'treatment' in col_lower:
                out['treatment'] = v
        return out

    def _build_row_event_story(
        self,
        table_name: str,
        raw_record: Dict,
        column_purposes: Dict,
        file_name: str,
        event_date: str,
        event_time: str,
        table_workflow_role: Dict[str, Any],
    ) -> str:
        """
        Build healthcare event explanation dynamically from role and observed column values.
        No hardcoded column names. Neat, one-line format.
        """
        role_info = table_workflow_role or {}
        role = role_info.get("role", "other")
        role_expl = role_info.get("role_explanation", "Healthcare record.")
        kv = self._extract_key_values_by_purpose(raw_record, column_purposes)
        patient_id = kv.get('patient_id') or self._get_patient_id_from_record(raw_record, column_purposes)
        parts = []

        if role in ("patient", "register"):
            name = kv.get('name', 'Patient')
            parts.append(f"Patient {patient_id or name} registered")
        elif role == "login_logout":
            parts.append(f"Login/Logout" + (f" (patient {patient_id})" if patient_id else ""))
        elif role == "appointment" or role == "appointment_booked":
            parts.append(f"Appointment booked for patient {patient_id or '—'}")
            if kv.get('ward'):
                parts.append(f"at {kv['ward']}")
            if kv.get('reason'):
                parts.append(f"({kv['reason']})")
        elif role == "doctor_assignment":
            doctor_name = kv.get('name', 'Doctor')
            parts.append(f"Doctor {doctor_name} assigned to patient {patient_id or '—'}")
        elif role == "doctor":
            name = kv.get('name', 'Doctor')
            parts.append(f"{name} profile created" + (f" (ID: {kv.get('patient_id', '')})" if kv.get('patient_id') else ""))
        elif role == "admission":
            parts.append(f"Patient {patient_id or '—'} admitted" + (f" to {kv['ward']}" if kv.get('ward') else ""))
            if kv.get('reason'):
                parts.append(f"- {kv['reason']}")
        elif role == "treatment":
            t = kv.get('treatment', 'treatment')
            parts.append(f"Treatment: {t}" + (f" (patient {patient_id})" if patient_id else ""))
        elif role == "lab_order":
            test_name = kv.get('test', kv.get('test_name', 'test'))
            parts.append(f"Lab test ordered for patient {patient_id or '—'}" + (f": {test_name}" if test_name else ""))
        elif role == "lab_result" or role == "lab":
            parts.append(f"Lab test result generated for patient {patient_id or '—'}")
        elif role == "prescription":
            medicine = kv.get('medicine', kv.get('medication', 'medicine'))
            parts.append(f"Medicine prescribed for patient {patient_id or '—'}" + (f": {medicine}" if medicine else ""))
        elif role == "pharmacy":
            parts.append(f"Medicine dispensed from pharmacy for patient {patient_id or '—'}")
        elif role == "insurance":
            parts.append(f"Insurance verified for patient {patient_id or '—'}")
        elif role == "billing_paid":
            amt = kv.get('amount', '—')
            parts.append(f"Bill paid by patient {patient_id or '—'}" + (f", amount {amt}" if amt != '—' else ""))
        elif role == "billing":
            amt = kv.get('amount', '—')
            parts.append(f"Bill generated for patient {patient_id or '—'}" + (f", amount {amt}" if amt != '—' else ""))
        elif role == "discharge":
            parts.append(f"Patient {patient_id or '—'} discharged" + (f" ({kv['status']})" if kv.get('status') else ""))
        elif role == "followup":
            parts.append(f"Followup visit scheduled for patient {patient_id or '—'}")
        elif role == "donation":
            parts.append(f"Donation by patient {patient_id or '—'}" + (f", {kv.get('amount', '')}" if kv.get('amount') else ""))
        else:
            work_sum = self._build_work_summary(table_name, raw_record, column_purposes, file_name)
            parts.append(work_sum)

        return " ".join(parts).strip() or role_expl

    def _build_time_log_explanation(
        self,
        event_date: str,
        event_time: str,
        row_event_story: str,
        table_name: str,
    ) -> str:
        """Short, clear event time. Always include full timestamp (HH:MM:SS) when available."""
        at_time = f"{event_date} {event_time}".strip() if event_time else event_date
        return f"Event: {at_time}"

    def _build_cross_table_links_for_record(
        self,
        raw_record: Dict,
        column_purposes: Dict,
    ) -> List[Dict[str, str]]:
        """
        For columns that look like patient_id, doctor_id, appointment_id, admission_id,
        explain how they link this table to another hospital process.
        """
        links = []
        for col, val in raw_record.items():
            if not val or str(val).strip() == '':
                continue
            purp = column_purposes.get(col) or {}
            link_expl = purp.get("link_explanation")
            if link_expl:
                links.append({
                    "column": col,
                    "value": str(val).strip(),
                    "link_explanation": link_expl,
                })
        return links

    def _find_admission_discharge_columns(self, df: pd.DataFrame) -> Tuple[Optional[str], Optional[str]]:
        """Find admission and discharge date columns by name pattern. Returns (admission_col, discharge_col)."""
        adm_col = dis_col = None
        for col in df.columns:
            cl = col.lower()
            if 'admission' in cl or 'admit' in cl:
                if 'date' in cl or 'time' in cl or 'stamp' in cl:
                    adm_col = col
            elif 'discharge' in cl:
                if 'date' in cl or 'time' in cl or 'stamp' in cl:
                    dis_col = col
        return (adm_col, dis_col)

    def _find_appointment_column(self, df: pd.DataFrame) -> Optional[str]:
        """Find appointment date column by name pattern."""
        for col in df.columns:
            cl = col.lower()
            if ('appt' in cl or 'appointment' in cl) and ('date' in cl or 'time' in cl or 'stamp' in cl):
                return col
        return None

    def _get_patient_id_from_record(self, raw_record: Dict, column_purposes: Dict) -> Optional[str]:
        """Extract patient ID from record using observed column patterns."""
        for col, val in raw_record.items():
            if not val or str(val).strip() == '':
                continue
            col_lower = col.lower()
            purp = (column_purposes.get(col) or {}).get('purpose', '').lower()
            if ('patient' in col_lower and ('id' in col_lower or col_lower.endswith('_id'))) or (purp and 'patient' in purp and 'identifier' in purp):
                return str(val).strip()
        return None

    def _calculate_stay_duration_explanation(
        self,
        admit_val: Any,
        discharge_val: Any,
    ) -> Optional[Dict[str, Any]]:
        """
        Calculate stay duration between admission and discharge.
        Returns dict with days, hours, discharge_time, explanation.
        """
        if admit_val is None or discharge_val is None:
            return None
        admit_str = str(admit_val).strip()
        discharge_str = str(discharge_val).strip()
        if not admit_str or not discharge_str or admit_str.lower() in ('nan', 'none', 'null') or discharge_str.lower() in ('nan', 'none', 'null'):
            return None
        try:
            admit_dt = pd.to_datetime(admit_val, errors='coerce')
            discharge_dt = pd.to_datetime(discharge_val, errors='coerce')
            if pd.isna(admit_dt) or pd.isna(discharge_dt):
                return None
            if discharge_dt < admit_dt:
                return None
            delta = discharge_dt - admit_dt
            total_seconds = delta.total_seconds()
            days = int(total_seconds // 86400)
            hours = int((total_seconds % 86400) // 3600)
            minutes = int((total_seconds % 3600) // 60)
            admit_time_str = admit_dt.strftime('%H:%M') if (admit_dt.hour or admit_dt.minute) else admit_dt.strftime('%Y-%m-%d')
            discharge_time_str = discharge_dt.strftime('%H:%M') if (discharge_dt.hour or discharge_dt.minute) else discharge_dt.strftime('%Y-%m-%d')
            discharge_full = discharge_dt.strftime('%Y-%m-%d %H:%M') if (discharge_dt.hour or discharge_dt.minute) else discharge_dt.strftime('%Y-%m-%d')
            parts = []
            if days > 0:
                parts.append(f"{days} day{'s' if days != 1 else ''}")
            if hours > 0:
                parts.append(f"{hours} hour{'s' if hours != 1 else ''}")
            if minutes > 0 and days == 0:
                parts.append(f"{minutes} min")
            duration_text = ", ".join(parts) if parts else "same day"
            explanation = (
                f"Admitted at {admit_time_str}, discharged at {discharge_time_str}. "
                f"Stay duration: {duration_text}."
            )
            if days >= 30:
                months = days // 30
                explanation += f" (~{months} month{'s' if months != 1 else ''})"
            return {
                "days": days,
                "hours": hours,
                "minutes": minutes,
                "discharge_time": discharge_full,
                "discharge_time_short": discharge_time_str,
                "admission_time": admit_dt.strftime('%Y-%m-%d %H:%M') if (admit_dt.hour or admit_dt.minute) else admit_dt.strftime('%Y-%m-%d'),
                "duration_text": duration_text,
                "explanation": explanation,
            }
        except Exception:
            return None

    def _calculate_appointment_admission_gap(
        self,
        appt_dt: pd.Timestamp,
        adm_dt: pd.Timestamp,
        threshold_hours: float = 2.0,
    ) -> Optional[Dict[str, Any]]:
        """
        If admission - appointment > threshold hours, return hospital delay explanation.
        """
        if pd.isna(appt_dt) or pd.isna(adm_dt):
            return None
        if adm_dt <= appt_dt:
            return None
        gap = adm_dt - appt_dt
        gap_hours = gap.total_seconds() / 3600
        if gap_hours <= threshold_hours:
            return None
        hours = int(gap_hours)
        minutes = int((gap_hours - hours) * 60)
        duration_parts = []
        if hours > 0:
            duration_parts.append(f"{hours} hour{'s' if hours != 1 else ''}")
        if minutes > 0:
            duration_parts.append(f"{minutes} min")
        duration_text = " ".join(duration_parts)
        return {
            "is_hospital_delay": True,
            "gap_hours": round(gap_hours, 2),
            "gap_duration": duration_text,
            "appointment_time": appt_dt.strftime('%Y-%m-%d %H:%M'),
            "admission_time": adm_dt.strftime('%Y-%m-%d %H:%M'),
            "explanation": (
                f"Hospital delay: Patient waited {duration_text} from appointment ({appt_dt.strftime('%H:%M')}) "
                f"to admission ({adm_dt.strftime('%H:%M')}). Gap exceeds 2 hours."
            ),
        }

    def _add_appointment_admission_gap_analysis(self, all_records: List[Dict[str, Any]]) -> None:
        """
        For each admission record, find matching appointment. If gap > 2 hours, mark as hospital delay.
        """
        appts_by_patient: Dict[str, List[Tuple[str, pd.Timestamp]]] = {}
        for r in all_records:
            pid = r.get('_patient_id')
            appt_dt = r.get('_appointment_datetime')
            if pid and appt_dt is not None and pd.notna(appt_dt):
                date_key = appt_dt.strftime('%Y-%m-%d')
                if pid not in appts_by_patient:
                    appts_by_patient[pid] = []
                appts_by_patient[pid].append((date_key, appt_dt))

        for r in all_records:
            adm_dt = r.get('_admission_datetime')
            if adm_dt is None or pd.isna(adm_dt):
                continue
            pid = r.get('_patient_id')
            if not pid or pid not in appts_by_patient:
                continue
            adm_date = adm_dt.strftime('%Y-%m-%d')
            for date_key, appt_dt in appts_by_patient[pid]:
                if date_key == adm_date:
                    gap_result = self._calculate_appointment_admission_gap(appt_dt, adm_dt)
                    if gap_result:
                        r['hospital_delay'] = gap_result
                    break

    def _scan_row_for_event_pattern(self, row: Any, columns: List[str]) -> Optional[str]:
        """
        Scan ALL column values in row for healthcare event pattern match.
        Observes actual data values, not just column names.
        Returns event name if pattern found, None otherwise.
        """
        # Healthcare event patterns to look for in data values
        event_patterns = {
            'PATIENT_REGISTERED': ['register', 'registered', 'registration', 'patient registered', 'new patient'],
            'APPOINTMENT_BOOKED': ['appointment booked', 'booked', 'appointment scheduled', 'scheduled appointment'],
            'DOCTOR_ASSIGNED': ['doctor assigned', 'assigned doctor', 'doctor assigned to', 'physician assigned'],
            'LAB_TEST_ORDERED': ['test ordered', 'lab ordered', 'order test', 'order lab', 'test order', 'lab order'],
            'LAB_RESULT_GENERATED': ['test result', 'lab result', 'result generated', 'test completed', 'lab completed'],
            'MEDICINE_PRESCRIBED': ['prescribed', 'prescription', 'medicine prescribed', 'medication prescribed', 'prescribe'],
            'PHARMACY_DISPENSED': ['dispensed', 'pharmacy dispensed', 'dispense', 'medicine dispensed', 'medication dispensed'],
            'INSURANCE_VERIFIED': ['insurance verified', 'verified', 'verification', 'insurance verification', 'verify'],
            'BILL_PAID': ['paid', 'payment', 'bill paid', 'payment received', 'paid bill'],
            'FOLLOWUP_VISIT_SCHEDULED': ['followup', 'follow up', 'follow-up', 'followup scheduled', 'follow up visit'],
        }
        
        for col in columns:
            if col.startswith("__"):
                continue
            val = row.get(col)
            if val is None or (isinstance(val, float) and pd.isna(val)):
                continue
            s = str(val).strip().lower()
            if not s:
                continue
            
            # Check each event pattern
            for event_name, patterns in event_patterns.items():
                for pattern in patterns:
                    if pattern in s:
                        return event_name
        
        return None

    def _explain_value(self, value: Any, purpose_info: Dict, col_name: str) -> str:
        """Generate explanation for a single value based on observed patterns."""
        if value is None or (isinstance(value, float) and pd.isna(value)) or str(value).strip() == '':
            return purpose_info.get("null_explanation") or "Empty or not recorded"
        val_str = str(value).strip()
        if val_str.lower() in ('nan', 'none', 'null', 'na', 'n/a'):
            return "Not recorded or missing"
        return val_str

    def _normalize_tz_naive(self, ts: Any) -> Optional[pd.Timestamp]:
        """Convert timestamp to tz-naive for consistent sorting (avoids tz-naive vs tz-aware comparison errors)."""
        if ts is None or (isinstance(ts, float) and pd.isna(ts)):
            return None
        try:
            t = pd.Timestamp(ts)
            if pd.isna(t):
                return None
            if t.tz is not None:
                try:
                    t = t.tz_convert(None)
                except (TypeError, ValueError):
                    t = pd.Timestamp(t.value)
            return t
        except Exception:
            return None

    def _extract_datetime(self, row, date_col: str, time_col: Optional[str], df: pd.DataFrame) -> Optional[pd.Timestamp]:
        """Extract combined datetime from row."""
        try:
            date_val = row.get(date_col, row[date_col]) if date_col in row.index else None
            if pd.isna(date_val):
                return None
            dt = pd.to_datetime(date_val, errors='coerce')
            if pd.isna(dt):
                return None
            if time_col and time_col in row.index:
                t_val = row[time_col]
                if pd.notna(t_val):
                    if isinstance(t_val, str) and ':' in t_val:
                        from datetime import datetime
                        parts = str(date_val).split()[0] if hasattr(date_val, 'split') else str(dt.date())
                        combined = f"{parts} {t_val}"
                        dt = pd.to_datetime(combined, errors='coerce')
            return dt
        except Exception:
            return None

    def _table_to_sorted_records(
        self,
        df: pd.DataFrame,
        table_name: str,
        file_name: str = ""
    ) -> List[Dict[str, Any]]:
        """
        For one table: find date/timestamp col, sort ascending, return list of records
        with date, time, table_name, file_name, column purposes (observed), and value explanations.
        """
        cols = self._find_date_timestamp_columns(df)
        if not cols:
            return []
        date_col, time_col = cols[0]
        event_time_col = time_col if time_col else date_col
        df = df.copy()
        df['__dt'] = pd.to_datetime(df[date_col], errors='coerce')
        # Only combine date+time when we have SEPARATE columns (e.g. appointment_date + appointment_time)
        # When date_col == time_col, the column already has full datetime - don't double-parse
        if time_col and time_col in df.columns and time_col != date_col:
            df['__date_str'] = df[date_col].astype(str).str.split().str[0]
            df['__time_str'] = df[time_col].astype(str)
            df['__dt'] = pd.to_datetime(df['__date_str'] + ' ' + df['__time_str'], errors='coerce')
            # Fallback for 12-hour formats with AM/PM (e.g., "10:30 AM")
            if df['__dt'].isna().any():
                def _try_parse(row):
                    if pd.notna(row['__dt']):
                        return row['__dt']
                    date_part = str(row['__date_str'])
                    time_part = str(row['__time_str'])
                    try:
                        return pd.to_datetime(f"{date_part} {time_part}", format="%Y-%m-%d %I:%M %p", errors='coerce')
                    except Exception:
                        return pd.NaT
                df['__dt'] = df.apply(_try_parse, axis=1)
            df['__dt'] = df['__dt'].fillna(pd.to_datetime(df[date_col], errors='coerce'))
        df = df.dropna(subset=['__dt'])
        df = df.sort_values('__dt', ascending=True)

        # Observe and infer column purposes for all columns (no hardcoding)
        skip_cols = {'__dt', '__date_str', '__time_str'}
        data_cols = [c for c in df.columns if c not in skip_cols and not str(c).startswith('__')]
        column_purposes = {}
        for c in data_cols:
            column_purposes[c] = self._infer_column_purpose(c, df[c], df)

        adm_col, dis_col = self._find_admission_discharge_columns(df)
        appt_col = self._find_appointment_column(df)
        table_workflow_role = self._identify_table_workflow_role(table_name, df)

        records = []
        for row_idx, row in df.iterrows():
            dt = row['__dt']
            raw_record = {}
            explained_record = {}
            data_flow_parts = []

            for c in data_cols:
                v = row.get(c)
                is_null = v is None or (isinstance(v, float) and pd.isna(v)) or str(v).strip() == ''
                val_str = '' if is_null else str(v)
                raw_record[c] = val_str
                purpose_info = column_purposes[c]
                expl = self._explain_value(v, purpose_info, c)
                explained_record[c] = expl
                if expl and expl != "Empty or not recorded":
                    data_flow_parts.append(f"{purpose_info['purpose']}: {expl}")

            work_summary = self._build_work_summary(table_name, raw_record, column_purposes, file_name)
            event_date = dt.strftime('%Y-%m-%d')
            event_time = dt.strftime('%H:%M:%S')  # Always include full time for clarity
            row_event_story = self._build_row_event_story(
                table_name, raw_record, column_purposes, file_name,
                event_date, event_time, table_workflow_role,
            )
            time_log_explanation = self._build_time_log_explanation(
                event_date, event_time, row_event_story, table_name,
            )
            cross_table_links = self._build_cross_table_links_for_record(raw_record, column_purposes)

            stay_duration = None
            if adm_col and dis_col and adm_col in row.index and dis_col in row.index:
                stay_duration = self._calculate_stay_duration_explanation(row[adm_col], row[dis_col])

            patient_id = self._get_patient_id_from_record(raw_record, column_purposes)

            rec = {
                'table_name': table_name,
                'file_name': file_name or f"{table_name}.csv",
                '_event_time_column': event_time_col,
                # Which row this event came from (helps “which table / which row” trace)
                'source_row_index': int(row_idx) if str(row_idx).isdigit() else str(row_idx),
                'source_row_number': int(row_idx) + 1 if str(row_idx).isdigit() else None,
                'date': event_date,
                'time': event_time,
                'event_datetime': f"{event_date} {event_time}",
                'datetime_sort': dt,
                'record': raw_record,
                'column_purposes': column_purposes,
                'data_flow_explanation': " | ".join(data_flow_parts) if data_flow_parts else "Record at this date/time",
                'value_explanations': explained_record,
                'work_summary': work_summary,
                'row_event_story': row_event_story,
                'time_log_explanation': time_log_explanation,
                'cross_table_links': cross_table_links,
                'table_workflow_role': table_workflow_role,
                '_patient_id': patient_id,
                '_event_datetime': dt,
                'patient_id': patient_id or 'unknown',
            }
            # Normalize datetime_sort to tz-naive so cross-table sort doesn't fail (tz-naive vs tz-aware)
            if 'datetime_sort' in rec:
                rec['datetime_sort'] = self._normalize_tz_naive(rec['datetime_sort'])
            if adm_col and adm_col in row.index:
                rec['_admission_datetime'] = pd.to_datetime(row[adm_col], errors='coerce')
            if appt_col and appt_col in row.index:
                rec['_appointment_datetime'] = pd.to_datetime(row[appt_col], errors='coerce')
            if stay_duration:
                rec['stay_duration'] = stay_duration
            records.append(rec)
        return records

    def _time_column_to_event_name(self, col_name: str) -> str:
        """
        Derive event name dynamically from the time column name.
        NO hardcoding - maps to all healthcare events including missing ones.
        Never return Other, Unknown, or table names.
        """
        if not col_name or not str(col_name).strip():
            return 'Visit'
        c = str(col_name).lower().replace('-', '_')
        # Explicit mappings from time column to event (comprehensive healthcare events)
        if 'register' in c or 'reg_' in c or (c.startswith('reg') and 'time' in c):
            return 'PATIENT_REGISTERED'
        if 'appointment' in c or 'appt_' in c:
            if 'book' in c or 'booked' in c:
                return 'APPOINTMENT_BOOKED'
            return 'APPOINTMENT_BOOKED'  # Default appointment to booked
        if 'doctor' in c and ('assign' in c or 'assigned' in c):
            return 'DOCTOR_ASSIGNED'
        if ('test_order' in c or 'lab_order' in c) and 'result' not in c:
            return 'LAB_TEST_ORDERED'
        if ('test_result' in c or 'lab_result' in c) or (('test' in c or 'lab' in c) and 'result' in c):
            return 'LAB_RESULT_GENERATED'
        if 'prescribe' in c or 'prescription' in c or ('medicine' in c and 'prescribe' in c):
            return 'MEDICINE_PRESCRIBED'
        if 'dispense' in c or ('pharmacy' in c and 'dispense' in c):
            return 'PHARMACY_DISPENSED'
        if 'insurance' in c and ('verify' in c or 'verification' in c):
            return 'INSURANCE_VERIFIED'
        if 'payment' in c or 'paid' in c or ('bill' in c and ('paid' in c or 'payment' in c)):
            return 'BILL_PAID'
        if 'followup' in c or 'follow_up' in c or ('follow' in c and 'up' in c):
            return 'FOLLOWUP_VISIT_SCHEDULED'
        if 'visit' in c or 'admission' in c:
            return 'Visit'
        if 'procedure' in c or 'treatment' in c:
            return 'Procedure'
        if 'test_' in c or 'lab_' in c or '_test' in c or '_lab' in c:
            return 'LAB_RESULT_GENERATED'  # Default lab/test to result
        if 'bill' in c or 'billing' in c:
            return 'Billing'  # Generic billing (not paid yet)
        if 'login' in c and 'logout' not in c:
            return 'Login'
        if 'logout' in c:
            return 'Logout'
        if 'discharge' in c:
            return 'Discharge'
        # Fallback: infer from column name - still meaningful, never Other/Unknown
        if 'created' in c or 'recorded' in c or 'event' in c:
            return 'Visit'
        return 'Visit'

    # Generic date columns that don't indicate event type - prefer table role for these
    _GENERIC_DATE_COLUMNS = (
        'visit_date', 'visit_time', 'activity_date', 'activity_time',
        'event_date', 'event_time', 'created_at', 'recorded_at', 'service_date',
        'record_date', 'action_date', 'transaction_date', 'updated_at',
    )

    def _record_to_step_name(self, rec: Dict[str, Any]) -> str:
        """
        Event name for diagram steps.

        Strong rule: **always prefer the inferred table workflow role** (register, appointment,
        admission, treatment, lab, billing, discharge, etc.) whenever we can map it to a
        canonical event. Also scans row data values for event patterns (not just column names).
        Only fall back to the time column naming when role is unknown.
        """
        time_col = (rec.get('_event_time_column') or '').strip().lower().replace('-', '_')
        role_info = rec.get('table_workflow_role') or {}
        role = (role_info.get('role') or '').strip()
        raw = rec.get('record') or {}
        
        ROLE_TO_EVENT = {
            # Core patient journey events
            'register': 'PATIENT_REGISTERED', 'patient': 'PATIENT_REGISTERED', 'registration': 'PATIENT_REGISTERED',
            'appointment': 'APPOINTMENT_BOOKED', 'appointment_booked': 'APPOINTMENT_BOOKED',
            'doctor_assignment': 'DOCTOR_ASSIGNED', 'doctor': 'DOCTOR_ASSIGNED',  # Doctor assignment takes precedence
            'admission': 'Admission',
            'discharge': 'Discharge',
            'followup': 'FOLLOWUP_VISIT_SCHEDULED',
            
            # Lab events - distinguish order from result
            'lab_order': 'LAB_TEST_ORDERED',
            'lab_result': 'LAB_RESULT_GENERATED', 'lab': 'LAB_RESULT_GENERATED',  # Default lab to result if not specified
            
            # Treatment and procedures
            'treatment': 'Procedure', 'donation': 'Procedure',
            
            # Medicine and pharmacy
            'prescription': 'MEDICINE_PRESCRIBED',
            'pharmacy': 'PHARMACY_DISPENSED',
            
            # Insurance and billing
            'insurance': 'INSURANCE_VERIFIED',
            'billing_paid': 'BILL_PAID',
            'billing': 'Billing',  # Generic billing (not paid yet)
            
            # Login/logout
            'login_logout': 'Login',
        }

        # 1) FIRST: Scan row data values for event patterns (actual data, not column names)
        #    This catches events that might be in status columns, description fields, etc.
        if raw:
            columns_list = list(raw.keys())
            # Create a simple row-like object for scanning
            row_data = {col: raw.get(col, '') for col in columns_list}
            scanned_event = self._scan_row_for_event_pattern(row_data, columns_list)
            if scanned_event:
                return scanned_event

        # 2) If we know the table workflow role and can map it, use that.
        #    This ensures different tables (registration, lab, billing, admission, etc.)
        #    show different event types instead of everything collapsing to "Visit".
        if role and role in ROLE_TO_EVENT:
            return ROLE_TO_EVENT[role]

        # 3) Otherwise, try deriving from the time column name (register_time, bill_time, etc.).
        if time_col:
            event = self._time_column_to_event_name(time_col)
            if event and event not in ('Other', 'Unknown'):
                return event

        # 4) Fallback: inspect raw column names to infer event type.
        for col in raw:
            ev = self._time_column_to_event_name(str(col).lower())
            if ev and ev not in ('Other', 'Unknown'):
                return ev
        return 'Visit'

    def _identify_healthcare_cases(
        self,
        all_records: List[Dict[str, Any]],
        gap_hours: float = HEALTHCARE_CASE_GAP_HOURS,
    ) -> List[List[Dict[str, Any]]]:
        """
        Group by patient_id, sort by timestamp. New Case ID when: (1) gap >= gap_hours,
        (2) same activity meaning appears again (duplicate) or from different source — one clean process flow per case.
        """
        by_patient: Dict[str, List[Dict]] = {}
        for r in all_records:
            pid = r.get('patient_id') or r.get('_patient_id') or 'unknown'
            if pid not in by_patient:
                by_patient[pid] = []
            by_patient[pid].append(r)

        cases: List[List[Dict]] = []
        for pid, recs in by_patient.items():
            def _ts_key(x):
                t = x.get('datetime_sort')
                if t is None or (hasattr(t, '__len__') and pd.isna(t)):
                    return pd.Timestamp.max
                return t
            recs_sorted = sorted(recs, key=_ts_key)
            current: List[Dict] = []
            last_ts: Optional[pd.Timestamp] = None
            for r in recs_sorted:
                ts = r.get('datetime_sort')
                if ts is None:
                    try:
                        ts = pd.to_datetime(r.get('event_datetime', ''))
                    except Exception:
                        ts = pd.Timestamp.min
                step = self._record_to_step_name(r)
                events_in_current = {self._record_to_step_name(x) for x in current}
                if step in events_in_current:
                    if current:
                        cases.append(current)
                    current = [r]
                    last_ts = ts
                    continue
                if last_ts is not None and ts is not None:
                    gap_h = (ts - last_ts).total_seconds() / 3600
                    if gap_h >= gap_hours:
                        if current:
                            cases.append(current)
                        current = []
                current.append(r)
                last_ts = ts
            if current:
                cases.append(current)

        cases.sort(key=lambda c: (c[0].get('datetime_sort') or pd.Timestamp.min))
        return cases

    def _assign_healthcare_case_ids(
        self,
        cases: List[List[Dict[str, Any]]],
    ) -> List[Dict[str, Any]]:
        """Assign case_id to each case, build case_details (activities, event_sequence, explanation)."""
        case_details = []
        for i, case_recs in enumerate(cases):
            case_id = i + 1
            activities = []
            for r in case_recs:
                ts = r.get('datetime_sort')
                ts_str = r.get('event_datetime', '') or ''
                if hasattr(ts, 'strftime'):
                    ts_str = ts.strftime('%Y-%m-%d %H:%M:%S')
                step = self._record_to_step_name(r)
                raw = r.get('record') or {}
                # Per-event explanation for UI (row_event_story = human-readable e.g. "Bill generated for patient P001, amount 1500")
                event_explanation = r.get('row_event_story') or r.get('work_summary') or ''
                if not event_explanation and raw:
                    # Fallback: build brief description from record
                    parts = [f"{k}: {v}" for k, v in raw.items() if v and str(v).strip()]
                    event_explanation = " | ".join(parts[:4]) if parts else step
                # date_only: true when time is 00:00:00 (date column only, no time recorded)
                date_only = bool(ts_str and ts_str.strip().endswith('00:00:00'))
                activities.append({
                    'event': step,
                    'timestamp_str': ts_str,
                    'date_only': date_only,
                    'user_id': r.get('patient_id') or 'unknown',
                    'table_name': r.get('table_name'),
                    'file_name': r.get('file_name'),
                    'source_row': r.get('source_row_index'),
                    'raw_record': {k: str(v) if v is not None and not (isinstance(v, float) and pd.isna(v)) else '' for k, v in raw.items()},
                    'explanation': event_explanation,
                })
            user_id = (case_recs[0].get('patient_id') or case_recs[0].get('_patient_id') or 'unknown')
            event_sequence = [self._record_to_step_name(r) for r in case_recs]
            explanation = self._build_healthcare_case_explanation(case_id, user_id, case_recs)
            case_details.append({
                'case_id': case_id,
                'user_id': user_id,
                'patient_id': user_id,
                'first_activity_timestamp': activities[0]['timestamp_str'] if activities else '',
                'last_activity_timestamp': activities[-1]['timestamp_str'] if activities else '',
                'activity_count': len(activities),
                'activities': activities,
                'event_sequence': event_sequence,
                'explanation': explanation,
            })
        return case_details

    def _build_healthcare_case_explanation(
        self,
        case_id: int,
        patient_id: str,
        case_recs: List[Dict[str, Any]],
    ) -> str:
        """Short explanation for this case: patient, date range, steps."""
        if not case_recs:
            return f"Case {case_id}: Patient {patient_id}. No steps."
        first = case_recs[0]
        last = case_recs[-1]
        start_str = first.get('event_datetime', '') or f"{first.get('date', '')} {first.get('time', '')}".strip()
        end_str = last.get('event_datetime', '') or f"{last.get('date', '')} {last.get('time', '')}".strip()
        steps = [self._record_to_step_name(r) for r in case_recs]
        return f"Case {case_id}: Patient {patient_id}. From {start_str} to {end_str}. Steps: {', '.join(steps)}."

    @staticmethod
    def _compute_same_time_groups(case_paths: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Find events where multiple case IDs have the same timestamp."""
        by_key: Dict[Tuple[str, str], List[int]] = {}
        for p in case_paths:
            seq = p.get("path_sequence", [])
            timings = p.get("timings", [])
            case_id = p.get("case_id")
            for j in range(1, len(seq) - 1):
                event = seq[j]
                if event in ("Process", "End"):
                    continue
                t = timings[j - 1] if j - 1 < len(timings) else {}
                ts_str = t.get("end_datetime") or t.get("start_datetime") or ""
                if not ts_str:
                    continue
                key = (event, ts_str)
                by_key.setdefault(key, []).append(case_id)
        out = []
        for (event, ts_str), case_ids in by_key.items():
            if len(case_ids) > 1:
                out.append({"event": event, "timestamp_str": ts_str, "case_ids": sorted(set(case_ids))})
        return out

    def _generate_unified_flow_data_healthcare(
        self,
        case_details: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Unified flow for healthcare: Process → steps → End with timings (same structure as banking)."""
        colors = [
            '#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A', '#98D8C8',
            '#F7DC6F', '#BB8FCE', '#85C1E2', '#F8B739', '#52B788',
            '#E63946', '#F1FAEE', '#A8DADC', '#457B9D', '#1D3557'
        ]
        case_paths = []
        for idx, case in enumerate(case_details):
            activities = case.get('activities', [])
            if not activities:
                continue
            case_color = colors[idx % len(colors)]
            path_sequence = ['Process']
            timings = []
            prev_ts = None
            prev_event_name = 'Process'
            for i, act in enumerate(activities):
                event_display = act.get('event', 'Step')
                ts_str = act.get('timestamp_str', '')
                try:
                    ts = pd.to_datetime(ts_str)
                except Exception:
                    ts = None
                if prev_ts is not None and ts is not None:
                    duration_seconds = max(0, int((ts - prev_ts).total_seconds()))
                    days = duration_seconds // 86400
                    hours = (duration_seconds % 86400) // 3600
                    minutes = (duration_seconds % 3600) // 60
                    seconds = duration_seconds % 60
                    if days > 0:
                        time_label = f"{days} day{'s' if days != 1 else ''} {hours} hr"
                    elif hours > 0:
                        time_label = f"{hours} hr {minutes} min" if minutes else f"{hours} hr"
                    elif minutes > 0:
                        time_label = f"{minutes} min {seconds} sec" if seconds else f"{minutes} min"
                    else:
                        time_label = f"{seconds} sec"
                else:
                    duration_seconds = 0
                    time_label = 'Start' if prev_event_name == 'Process' else '0 sec'
                path_sequence.append(event_display)
                date_only = act.get('date_only', False)
                timings.append({
                    'from': prev_event_name,
                    'to': event_display,
                    'duration_seconds': duration_seconds,
                    'label': time_label,
                    'date_only': date_only,
                    'start_time': prev_ts.strftime('%H:%M:%S') if prev_ts else '',
                    'end_time': ts.strftime('%H:%M:%S') if ts else '',
                    'start_datetime': prev_ts.strftime('%Y-%m-%d %H:%M:%S') if prev_ts else '',
                    'end_datetime': ts.strftime('%Y-%m-%d %H:%M:%S') if ts else '',
                })
                prev_ts = ts
                prev_event_name = event_display
            path_sequence.append('End')
            last_ts = prev_ts
            timings.append({
                'from': prev_event_name,
                'to': 'End',
                'duration_seconds': 0,
                'label': 'End',
                'start_time': last_ts.strftime('%H:%M:%S') if last_ts else '',
                'end_time': last_ts.strftime('%H:%M:%S') if last_ts else '',
                'start_datetime': last_ts.strftime('%Y-%m-%d %H:%M:%S') if last_ts else '',
                'end_datetime': last_ts.strftime('%Y-%m-%d %H:%M:%S') if last_ts else '',
            })
            case_paths.append({
                'case_id': case.get('case_id'),
                'user_id': case.get('user_id'),
                'color': case_color,
                'path_sequence': path_sequence,
                'timings': timings,
                'total_duration': sum(t['duration_seconds'] for t in timings),
            })
        all_event_types = ['Process'] + list(dict.fromkeys(
            s for path in case_paths for s in path['path_sequence'] if s not in ('Process', 'End')
        )) + ['End']
        same_time_groups = self._compute_same_time_groups(case_paths)
        # Sankey pattern: count (from, to) transitions across all case paths (no hardcoding)
        transition_counts = {}
        for path in case_paths:
            seq = path.get('path_sequence') or []
            for i in range(len(seq) - 1):
                f, t = seq[i], seq[i + 1]
                if f and t:
                    key = (f, t)
                    transition_counts[key] = transition_counts.get(key, 0) + 1
        transition_counts_list = [{'from': f, 'to': t, 'count': c} for (f, t), c in transition_counts.items()]
        return {
            'all_event_types': all_event_types,
            'case_paths': case_paths,
            'total_cases': len(case_paths),
            'same_time_groups': same_time_groups,
            'transition_counts': transition_counts_list,
        }

    def _build_appointment_to_patient_lookup(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """
        Build lookup: appointment_id -> patient_id from tables that have both columns.
        Used to resolve patient_id for treatment/lab/billing records that only have appointment_id.
        """
        lookup = {}
        for _tname, df in dataframes.items():
            if df is None or df.empty:
                continue
            cols = [c.lower() for c in df.columns]
            if 'appointment_id' in cols and 'patient_id' in cols:
                try:
                    for _, row in df.iterrows():
                        appt_id = row.get('appointment_id')
                        pat_id = row.get('patient_id')
                        if pd.notna(appt_id) and pd.notna(pat_id) and str(appt_id).strip():
                            lookup[str(appt_id).strip()] = str(pat_id).strip()
                except Exception:
                    pass
        return lookup

    def _build_visit_to_patient_lookup(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """
        Build lookup: visit_id -> patient_id from tables that have both columns.
        Dynamic: any table with visit_id + patient_id (visits, appointments, etc.).
        """
        lookup = {}
        for _tname, df in dataframes.items():
            if df is None or df.empty:
                continue
            col_map = {c.lower(): c for c in df.columns}
            visit_col = next((col_map[k] for k in ('visit_id', 'visitid') if k in col_map), None)
            if not visit_col or 'patient_id' not in col_map:
                continue
            try:
                for _, row in df.iterrows():
                    vid = row.get(visit_col)
                    pid = row.get('patient_id')
                    if pd.notna(vid) and pd.notna(pid) and str(vid).strip():
                        lookup[str(vid).strip()] = str(pid).strip()
            except Exception:
                pass
        return lookup

    def _resolve_patient_id_through_joins(
        self,
        all_records: List[Dict[str, Any]],
        dataframes: Dict[str, pd.DataFrame],
    ) -> None:
        """
        Resolve patient_id for records that don't have it directly.
        Dynamic: patient_id exists -> use it; else visit_id -> visits -> patient_id;
        else appointment_id -> appointments -> patient_id.
        Updates records in-place.
        """
        appt_lookup = self._build_appointment_to_patient_lookup(dataframes)
        visit_lookup = self._build_visit_to_patient_lookup(dataframes)
        for rec in all_records:
            pid = rec.get('_patient_id') or rec.get('patient_id')
            if pid and str(pid).lower() not in ('unknown', 'none', 'nan', ''):
                continue
            raw = rec.get('record') or {}
            resolved = None
            for col, val in raw.items():
                if not val or str(val).strip() == '':
                    continue
                col_lower = col.lower()
                val_str = str(val).strip()
                if 'visit' in col_lower and ('id' in col_lower or col_lower.endswith('_id')):
                    if visit_lookup and val_str in visit_lookup:
                        resolved = visit_lookup[val_str]
                        break
                if 'appointment' in col_lower and ('id' in col_lower or col_lower.endswith('_id')):
                    if appt_lookup and val_str in appt_lookup:
                        resolved = appt_lookup[val_str]
                        break
            if resolved:
                rec['_patient_id'] = resolved
                rec['patient_id'] = resolved

    def analyze_cluster(
        self,
        tables: List[TableAnalysis],
        dataframes: Dict[str, pd.DataFrame],
        relationships: List[Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Analyze healthcare cluster: for each table find date/timestamp (excl. DOB),
        sort each table ascending, merge all into one global sorted timeline.
        Returns diagram-ready structure: Start ----|----|---- End
        """
        all_records: List[Dict[str, Any]] = []
        tables_summary = []

        for table in tables:
            table_name = table.table_name
            file_name = getattr(table, 'file_name', '') or f"{table_name}.csv"
            df = dataframes.get(table_name)
            if df is None or df.empty:
                continue
            records = self._table_to_sorted_records(df, table_name, file_name)
            if not records:
                continue
            all_records.extend(records)
            # Column purposes observed for this table (first record has them)
            col_purposes = records[0].get('column_purposes', {}) if records else {}
            col_explanations = []
            for c in col_purposes:
                purp = col_purposes[c]
                col_explanations.append({
                    'column': c,
                    'purpose': purp.get('purpose'),
                    'work_explanation': purp.get('work_explanation'),
                    'column_classification': purp.get('column_classification', COLUMN_CLASS_OTHER),
                    'link_explanation': purp.get('link_explanation'),
                })
            table_role = records[0].get('table_workflow_role', {}) if records else {}
            tables_summary.append({
                'table_name': table_name,
                'file_name': file_name,
                'row_count': len(records),
                'date_column': self._find_date_timestamp_columns(df)[0][0] if self._find_date_timestamp_columns(df) else None,
                'first_date': records[0]['date'],
                'last_date': records[-1]['date'],
                'column_purposes': col_purposes,
                'table_workflow_role': table_role,
                'column_explanations': col_explanations,
            })

        if not all_records:
            return {
                'success': False,
                'error': 'No date/timestamp columns found. Add a column like visit_date, activity_date, service_date, or created_at with parseable date/time values. Date-only columns (e.g. 2020-01-15) are accepted. Excludes date of birth.',
                'tables_checked': [t.table_name for t in tables]
            }

        # Resolve patient_id for records that only have appointment_id (e.g. treatments -> appointments -> patients)
        self._resolve_patient_id_through_joins(all_records, dataframes)

        # Sort globally by datetime ascending (use .value to avoid tz-naive vs tz-aware comparison errors)
        def _sort_key(r):
            ts = r.get('datetime_sort')
            if ts is None:
                return 0
            try:
                if pd.isna(ts):
                    return 0
                return pd.Timestamp(ts).value
            except Exception:
                return 0
        all_records.sort(key=_sort_key)

        # Appointment–admission gap: if gap > 2 hours, mark as hospital delay
        self._add_appointment_admission_gap_analysis(all_records)

        # Case ID logic (same idea as banking): split by patient + time gap, full sorted case list, explanation per case
        cases = self._identify_healthcare_cases(all_records)
        case_details = self._assign_healthcare_case_ids(cases)
        unified_flow_data = self._generate_unified_flow_data_healthcare(case_details)
        case_ids_asc = [c['case_id'] for c in case_details]
        users_with_cases = list(dict.fromkeys(c['user_id'] for c in case_details))
        explanations = [
            f"We found {len(case_details)} case(s). Each case is one patient journey (steps in time order).",
            f"Case IDs are numbered 1 to {len(case_details)} in order of first event time.",
            "Events are grouped by patient and sorted by timestamp. Same activity meaning again (duplicate or different source) starts a new Case ID so each case is one clean process flow.",
            "Same patient with a gap of 24 hours or more also starts a new Case ID.",
            "Each case lists steps (e.g. Registration, Appointment, Admission, Treatment, Discharge) from your files.",
        ]

        for r in all_records:
            del r['datetime_sort']
            for k in list(r.keys()):
                if k.startswith('_'):
                    del r[k]

        first_date = all_records[0]['date']
        last_date = all_records[-1]['date']
        first_time = all_records[0].get('time', '')
        last_time = all_records[-1].get('time', '')

        # Build diagram nodes: unique (date, time) points, sorted chronologically
        # Use full datetime for grouping to avoid date-only collisions
        node_map = {}
        for r in all_records:
            dt_key = (r['date'], r.get('time', ''))
            if dt_key not in node_map:
                node_map[dt_key] = {
                    'date': r['date'],
                    'time': r.get('time', ''),
                    'records': [],
                    'sort_key': f"{r['date']} {r.get('time', '') or '00:00:00'}",
                }
            node_map[dt_key]['records'].append(r)

        diagram_nodes = []
        for dt_key in sorted(node_map.keys(), key=lambda k: node_map[k]['sort_key']):
            n = node_map[dt_key]
            diagram_nodes.append({
                'date': n['date'],
                'time': n['time'],
                'count': len(n['records']),
                'records': n['records'],
                'table_names': list(set(x['table_name'] for x in n['records']))
            })

        return {
            'success': True,
            'tables_summary': tables_summary,
            'sorted_timeline': all_records,
            'diagram_nodes': diagram_nodes,
            'first_date': first_date,
            'last_date': last_date,
            'first_datetime': f"{first_date} {first_time}".strip(),
            'last_datetime': f"{last_date} {last_time}".strip(),
            'total_records': len(all_records),
            'total_tables_with_dates': len(tables_summary),
            'case_ids': case_ids_asc,
            'case_details': case_details,
            'total_cases': len(case_details),
            'total_users': len(users_with_cases),
            'users': users_with_cases,
            'explanations': explanations,
            'total_activities': len(all_records),
            'unified_flow_data': unified_flow_data,
        }
