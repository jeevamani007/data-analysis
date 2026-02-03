"""
Healthcare Data Analyzer
Analyzes healthcare datasets to extract time slot patterns, department statistics,
diagnosis trends, and age group distributions.
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional
from datetime import datetime, time


class HealthcareAnalyzer:
    """Analyzes healthcare data for patient visits, departments, and diagnoses."""
    
    def __init__(self):
        self.patient_col_keywords = ['patient', 'patient_id', 'patientid']
        self.visit_date_keywords = ['visit_date', 'visitdate', 'admission_date', 'appointment_date', 'registration_date', 'date']
        # More specific datetime hints for dedicated appointment tables
        self.appointment_date_keywords = ['appointment_date_time', 'appointment_datetime', 'appointment_date']
        self.department_keywords = ['department', 'dept', 'specialty', 'ward']
        self.diagnosis_keywords = ['diagnosis', 'disease', 'reason', 'reason_for_visit', 'complaint']
        self.age_keywords = ['age', 'patient_age']
        self.gender_keywords = ['gender', 'sex']
    
    def _fuzzy_find_column(self, df: pd.DataFrame, keywords: List[str]) -> Optional[str]:
        """Find a column that matches any of the keywords (case-insensitive)."""
        cols_lower = {col.lower(): col for col in df.columns}
        for keyword in keywords:
            if keyword in cols_lower:
                return cols_lower[keyword]
            # Partial match
            for col_lower, col_original in cols_lower.items():
                if keyword in col_lower:
                    return col_original
        return None

    @staticmethod
    def _to_str_safe(value: Any) -> str:
        """Safely convert a value to string for JSON-friendly output."""
        try:
            if value is None or (isinstance(value, float) and pd.isna(value)):
                return ''
        except Exception:
            pass
        return str(value)
    
    def analyze_cluster(self, tables: List[Any], dataframes: Dict[str, pd.DataFrame], relationships: List[Any]) -> Dict[str, Any]:
        """
        Analyze healthcare data cluster.
        
        Returns:
            Dictionary with time slot analysis, department stats, diagnosis patterns, and date timeline
        """
        # Find the best visit/registration table and (optionally) a separate appointment table
        visit_df = None
        visit_table_name = None
        best_visit_score = -1

        appointment_df = None
        appointment_table_name = None
        best_appointment_score = -1
        patient_df = None
        patient_table_name = None
        
        for table_name, df in dataframes.items():
            # Look for generic visit/registration indicators
            has_visit_date = self._fuzzy_find_column(df, self.visit_date_keywords) is not None
            has_department = self._fuzzy_find_column(df, self.department_keywords) is not None
            has_diagnosis = self._fuzzy_find_column(df, self.diagnosis_keywords) is not None
            
            if has_visit_date and (has_department or has_diagnosis):
                # Score this candidate: prefer tables that have both department and diagnosis
                score = 1  # base: has_visit_date
                if has_department:
                    score += 1
                if has_diagnosis:
                    score += 1
                # Prefer tables whose name hints at visits/appointments/admissions
                name_l = table_name.lower()
                if any(k in name_l for k in ['visit', 'appointment', 'opd', 'admission']):
                    score += 1
                
                if score > best_visit_score:
                    best_visit_score = score
                    visit_df = df
                    visit_table_name = table_name

            # Look for a dedicated appointment table (column contains 'appointment' + date/time)
            appointment_date_col_candidate = None
            for col in df.columns:
                col_l = col.lower()
                if 'appointment' in col_l and ('date' in col_l or 'time' in col_l or 'datetime' in col_l):
                    appointment_date_col_candidate = col
                    break

            if appointment_date_col_candidate is not None:
                appt_score = 1  # has appointment datetime
                if self._fuzzy_find_column(df, self.patient_col_keywords) is not None:
                    appt_score += 1
                if has_department:
                    appt_score += 1
                if has_diagnosis:
                    appt_score += 1
                name_l = table_name.lower()
                if 'appointment' in name_l:
                    appt_score += 1

                if appt_score > best_appointment_score:
                    best_appointment_score = appt_score
                    appointment_df = df
                    appointment_table_name = table_name
            
            # Look for patient table
            has_age = self._fuzzy_find_column(df, self.age_keywords) is not None
            has_patient_id = self._fuzzy_find_column(df, self.patient_col_keywords) is not None
            if has_patient_id and has_age:
                patient_df = df
                patient_table_name = table_name
        
        if visit_df is None and appointment_df is None:
            return {'success': False, 'error': 'No visit/appointment data found'}

        # If no separate visit_df found but we do have an appointment_df, treat that as visit_df for core stats
        if visit_df is None and appointment_df is not None:
            visit_df = appointment_df
            visit_table_name = appointment_table_name
        
        # Detect columns for the main visit/registration timeline
        date_col = self._fuzzy_find_column(visit_df, self.visit_date_keywords)
        dept_col = self._fuzzy_find_column(visit_df, self.department_keywords)
        diag_col = self._fuzzy_find_column(visit_df, self.diagnosis_keywords)
        patient_id_col = self._fuzzy_find_column(visit_df, self.patient_col_keywords)

        # Extra appointment-related timestamps (if present)
        admission_col = None
        discharge_col = None
        if date_col:
            for col in visit_df.columns:
                col_l = col.lower()
                if admission_col is None and 'admission' in col_l:
                    admission_col = col
                if discharge_col is None and ('discharge' in col_l or 'checkout' in col_l or 'discharge_date' in col_l):
                    discharge_col = col
        
        # Merge with patient data if available
        analysis_df = visit_df.copy()
        if patient_df is not None and patient_id_col:
            patient_id_col_patient = self._fuzzy_find_column(patient_df, self.patient_col_keywords)
            if patient_id_col_patient:
                analysis_df = analysis_df.merge(patient_df, left_on=patient_id_col, right_on=patient_id_col_patient, how='left', suffixes=('', '_patient'))
        
        age_col = self._fuzzy_find_column(analysis_df, self.age_keywords)
        gender_col = self._fuzzy_find_column(analysis_df, self.gender_keywords)
        
        result = {
            'success': True,
            'visit_table': visit_table_name,
            'appointment_table': appointment_table_name,
            'patient_table': patient_table_name,
            'total_visits': len(analysis_df)
        }
        
        # 1. Time Slot Analysis & per-date registration/visit timeline
        if date_col:
            result['time_slot_analysis'] = self._analyze_time_slots(analysis_df, date_col, dept_col, diag_col, age_col)
            result['date_timeline'] = self._create_date_timeline(
                analysis_df,
                date_col=date_col,
                dept_col=dept_col,
                diag_col=diag_col,
                age_col=age_col,
                gender_col=gender_col,
                patient_id_col=patient_id_col,
                admission_col=admission_col,
                discharge_col=discharge_col,
            )

        # 1b. Separate Appointment Timeline (if a dedicated appointment table exists)
        if appointment_df is not None:
            appt_date_col = self._fuzzy_find_column(appointment_df, self.appointment_date_keywords) or \
                            self._fuzzy_find_column(appointment_df, self.visit_date_keywords)
            if appt_date_col:
                appt_dept_col = self._fuzzy_find_column(appointment_df, self.department_keywords)
                appt_diag_col = self._fuzzy_find_column(appointment_df, self.diagnosis_keywords)
                appt_patient_id_col = self._fuzzy_find_column(appointment_df, self.patient_col_keywords)
                appt_age_col = self._fuzzy_find_column(appointment_df, self.age_keywords)
                appt_gender_col = self._fuzzy_find_column(appointment_df, self.gender_keywords)

                appt_admission_col = None
                appt_discharge_col = None
                for col in appointment_df.columns:
                    col_l = col.lower()
                    if appt_admission_col is None and 'admission' in col_l:
                        appt_admission_col = col
                    if appt_discharge_col is None and ('discharge' in col_l or 'checkout' in col_l or 'discharge_date' in col_l):
                        appt_discharge_col = col

                result['appointment_timeline'] = self._create_date_timeline(
                    appointment_df,
                    date_col=appt_date_col,
                    dept_col=appt_dept_col,
                    diag_col=appt_diag_col,
                    age_col=appt_age_col,
                    gender_col=appt_gender_col,
                    patient_id_col=appt_patient_id_col,
                    admission_col=appt_admission_col,
                    discharge_col=appt_discharge_col,
                )
        
        # 2. Department Analysis
        if dept_col:
            result['department_analysis'] = self._analyze_departments(analysis_df, dept_col, diag_col)
        
        # 3. Diagnosis Patterns
        if diag_col:
            result['diagnosis_analysis'] = self._analyze_diagnoses(analysis_df, diag_col, dept_col)
        
        # 4. Age Group Analysis
        if age_col:
            result['age_group_analysis'] = self._analyze_age_groups(analysis_df, age_col, diag_col)
        
        # 5. Weekly Trends (if enough data)
        if date_col:
            result['weekly_analysis'] = self._analyze_weekly_trends(analysis_df, date_col, diag_col, age_col)
        
        return result
    
    def _analyze_time_slots(self, df: pd.DataFrame, date_col: str, dept_col: Optional[str], diag_col: Optional[str], age_col: Optional[str]) -> Dict[str, Any]:
        """Analyze visits by time slots (Morning/Afternoon/Evening/Night)."""
        df_copy = df.copy()
        df_copy['_parsed_datetime'] = pd.to_datetime(df_copy[date_col], errors='coerce')
        df_copy = df_copy.dropna(subset=['_parsed_datetime'])
        
        def get_time_slot(dt):
            hour = dt.hour
            if 5 <= hour < 12:
                return 'Morning'
            elif 12 <= hour < 17:
                return 'Afternoon'
            elif 17 <= hour < 21:
                return 'Evening'
            else:
                return 'Night'
        
        df_copy['_time_slot'] = df_copy['_parsed_datetime'].apply(get_time_slot)
        
        slots = {}
        for slot_name in ['Morning', 'Afternoon', 'Evening', 'Night']:
            slot_df = df_copy[df_copy['_time_slot'] == slot_name]
            slot_info = {
                'visit_count': len(slot_df),
                'slot_name': slot_name
            }
            
            if dept_col and dept_col in slot_df.columns and len(slot_df) > 0:
                top_dept = slot_df[dept_col].value_counts().head(1)
                if not top_dept.empty:
                    slot_info['top_department'] = str(top_dept.index[0])
                    slot_info['top_department_count'] = int(top_dept.values[0])
            
            if diag_col and diag_col in slot_df.columns and len(slot_df) > 0:
                top_diag = slot_df[diag_col].value_counts().head(1)
                if not top_diag.empty:
                    slot_info['top_diagnosis'] = str(top_diag.index[0])
                    slot_info['top_diagnosis_count'] = int(top_diag.values[0])
            
            slots[slot_name] = slot_info
        
        return {
            'slots': slots,
            'total_with_time': len(df_copy)
        }
    
    def _create_date_timeline(
        self,
        df: pd.DataFrame,
        date_col: str,
        dept_col: Optional[str],
        diag_col: Optional[str],
        age_col: Optional[str],
        gender_col: Optional[str],
        patient_id_col: Optional[str],
        admission_col: Optional[str],
        discharge_col: Optional[str],
    ) -> Dict[str, Any]:
        """
        Create a timeline of visits by date with drill-down details.
        
        For each date we also calculate Morning / Afternoon / Evening / Night
        buckets so the frontend can show beginner‑friendly explanations like:
        
        📅 Date: 2026-02-01
        🕘 Morning: 45 patients – top reason, top department
        """
        df_copy = df.copy()
        df_copy['_parsed_datetime'] = pd.to_datetime(df_copy[date_col], errors='coerce')
        df_copy = df_copy.dropna(subset=['_parsed_datetime'])
        df_copy['_date_only'] = df_copy['_parsed_datetime'].dt.strftime('%Y-%m-%d')
        
        if len(df_copy) == 0:
            return {'dates': []}
        
        dates_sorted = sorted(df_copy['_date_only'].unique())
        timeline = []
        
        for date_str in dates_sorted:
            day_df = df_copy[df_copy['_date_only'] == date_str]
            
            date_info = {
                'date': date_str,
                'visit_count': len(day_df)
            }
            
            if dept_col and dept_col in day_df.columns:
                dept_counts = day_df[dept_col].value_counts()
                date_info['top_department'] = str(dept_counts.index[0]) if not dept_counts.empty else 'N/A'
                date_info['departments'] = dept_counts.head(5).to_dict()
            
            if diag_col and diag_col in day_df.columns:
                diag_counts = day_df[diag_col].value_counts()
                date_info['top_diagnosis'] = str(diag_counts.index[0]) if not diag_counts.empty else 'N/A'
                date_info['diagnoses'] = diag_counts.head(5).to_dict()
            
            # Time slot breakdown for this date
            def get_time_slot(dt):
                hour = dt.hour
                if 5 <= hour < 12:
                    return 'Morning'
                elif 12 <= hour < 17:
                    return 'Afternoon'
                elif 17 <= hour < 21:
                    return 'Evening'
                else:
                    return 'Night'
            
            day_df = day_df.copy()
            day_df['_time_slot'] = day_df['_parsed_datetime'].apply(get_time_slot)
            time_slot_counts = day_df['_time_slot'].value_counts().to_dict()
            date_info['time_slots'] = time_slot_counts

            # Rich per‑slot details for UI (no hard‑coded column names; uses detected dept/diagnosis)
            slot_details: Dict[str, Any] = {}
            for slot_name in ['Morning', 'Afternoon', 'Evening', 'Night']:
                slot_df = day_df[day_df['_time_slot'] == slot_name]
                slot_info: Dict[str, Any] = {
                    'slot_name': slot_name,
                    'visit_count': int(len(slot_df))
                }

                if dept_col and dept_col in slot_df.columns and len(slot_df) > 0:
                    slot_dept_counts = slot_df[dept_col].value_counts()
                    if not slot_dept_counts.empty:
                        slot_info['top_department'] = str(slot_dept_counts.index[0])
                        slot_info['top_department_count'] = int(slot_dept_counts.values[0])

                if diag_col and diag_col in slot_df.columns and len(slot_df) > 0:
                    slot_diag_counts = slot_df[diag_col].value_counts()
                    if not slot_diag_counts.empty:
                        slot_info['top_diagnosis'] = str(slot_diag_counts.index[0])
                        slot_info['top_diagnosis_count'] = int(slot_diag_counts.values[0])

                slot_details[slot_name] = slot_info

            date_info['slot_details'] = slot_details

            # Gender breakdown (Male / Female / Other) for this date if a gender column exists
            if gender_col and gender_col in day_df.columns:
                gender_series = day_df[gender_col].astype(str).str.strip().str.lower()
                male_mask = gender_series.isin(['m', 'male', 'man', 'boy'])
                female_mask = gender_series.isin(['f', 'female', 'woman', 'girl'])
                male_count = int(male_mask.sum())
                female_count = int(female_mask.sum())
                other_count = int(len(gender_series) - male_count - female_count)
                date_info['gender_breakdown'] = {
                    'male': male_count,
                    'female': female_count,
                    'other': other_count
                }

            # Per-visit / per-appointment timeline for this date with exact time and slot name
            visits = []
            for _, row in day_df.sort_values('_parsed_datetime').iterrows():
                dt_val = row['_parsed_datetime']
                if pd.isna(dt_val):
                    continue
                slot_name = get_time_slot(dt_val)
                visit_entry: Dict[str, Any] = {
                    'time': dt_val.strftime('%H:%M'),
                    'time_slot': slot_name
                }

                # Linked patient ID (appointment owner) if available
                patient_id_val = None
                if patient_id_col and patient_id_col in day_df.columns:
                    patient_id_val = self._to_str_safe(row.get(patient_id_col))
                    visit_entry['patient_id'] = patient_id_val
                if dept_col and dept_col in day_df.columns:
                    visit_entry['department'] = self._to_str_safe(row.get(dept_col))
                if diag_col and diag_col in day_df.columns:
                    visit_entry['reason'] = self._to_str_safe(row.get(diag_col))
                if age_col and age_col in day_df.columns:
                    try:
                        visit_entry['age'] = int(row.get(age_col)) if pd.notna(row.get(age_col)) else None
                    except Exception:
                        visit_entry['age'] = self._to_str_safe(row.get(age_col))
                if gender_col and gender_col in day_df.columns:
                    visit_entry['gender'] = self._to_str_safe(row.get(gender_col))

                # Admission / discharge timestamps, if present
                adm_str = None
                dis_str = None
                if admission_col and admission_col in day_df.columns:
                    adm_dt = pd.to_datetime(row.get(admission_col), errors='coerce')
                    if pd.notna(adm_dt):
                        adm_str = adm_dt.strftime('%Y-%m-%d %H:%M')
                        visit_entry['admission_time'] = adm_str
                if discharge_col and discharge_col in day_df.columns:
                    dis_dt = pd.to_datetime(row.get(discharge_col), errors='coerce')
                    if pd.notna(dis_dt):
                        dis_str = dis_dt.strftime('%Y-%m-%d %H:%M')
                        visit_entry['discharge_time'] = dis_str

                # Simple human explanation for UI (beginner-friendly)
                dept_txt = self._to_str_safe(row.get(dept_col)) if (dept_col and dept_col in day_df.columns) else ''
                reason_txt = self._to_str_safe(row.get(diag_col)) if (diag_col and diag_col in day_df.columns) else ''
                # Build explanation sentence
                who_part = f"Patient {patient_id_val}" if patient_id_val else "A patient"
                dept_part = f"to {dept_txt}" if dept_txt else "to the hospital"
                reason_part = ""
                if reason_txt:
                    reason_part = f" for {reason_txt}"
                explanation = f"At {visit_entry['time']} ({slot_name}), {who_part} came {dept_part}{reason_part}."
                if adm_str and dis_str:
                    explanation += f" They were admitted at {adm_str} and discharged at {dis_str}."
                elif adm_str and not dis_str:
                    explanation += f" They were admitted at {adm_str}."
                elif dis_str and not adm_str:
                    explanation += f" Discharge time recorded as {dis_str}."
                visit_entry['explanation'] = explanation
                visits.append(visit_entry)

            date_info['visits'] = visits
            
            timeline.append(date_info)
        
        return {
            'dates': timeline,
            'first_date': dates_sorted[0] if dates_sorted else None,
            'last_date': dates_sorted[-1] if dates_sorted else None,
            'peak_date': max(timeline, key=lambda x: x['visit_count'])['date'] if timeline else None
        }
    
    def _analyze_departments(self, df: pd.DataFrame, dept_col: str, diag_col: Optional[str]) -> Dict[str, Any]:
        """Analyze department frequency and common diagnoses per department."""
        dept_counts = df[dept_col].value_counts()
        total = len(df)
        
        top_departments = []
        for dept, count in dept_counts.head(10).items():
            dept_info = {
                'department': str(dept),
                'visit_count': int(count),
                'percentage': round((count / total) * 100, 1)
            }
            
            if diag_col and diag_col in df.columns:
                dept_df = df[df[dept_col] == dept]
                top_diag = dept_df[diag_col].value_counts().head(1)
                if not top_diag.empty:
                    dept_info['top_diagnosis'] = str(top_diag.index[0])
            
            top_departments.append(dept_info)
        
        return {
            'total_departments': int(dept_counts.nunique()),
            'top_departments': top_departments
        }
    
    def _analyze_diagnoses(self, df: pd.DataFrame, diag_col: str, dept_col: Optional[str]) -> Dict[str, Any]:
        """Analyze diagnosis patterns."""
        diag_counts = df[diag_col].value_counts()
        total = len(df)
        
        top_diagnoses = []
        for diag, count in diag_counts.head(10).items():
            diag_info = {
                'diagnosis': str(diag),
                'visit_count': int(count),
                'percentage': round((count / total) * 100, 1)
            }
            top_diagnoses.append(diag_info)
        
        return {
            'total_unique_diagnoses': int(diag_counts.nunique()),
            'top_diagnoses': top_diagnoses
        }
    
    def _analyze_age_groups(self, df: pd.DataFrame, age_col: str, diag_col: Optional[str]) -> Dict[str, Any]:
        """Analyze age group distribution."""
        df_copy = df.copy()
        df_copy[age_col] = pd.to_numeric(df_copy[age_col], errors='coerce')
        df_copy = df_copy.dropna(subset=[age_col])
        
        def categorize_age(age):
            if age < 18:
                return 'Child (0-17)'
            elif age < 60:
                return 'Adult (18-59)'
            else:
                return 'Senior (60+)'
        
        df_copy['_age_group'] = df_copy[age_col].apply(categorize_age)
        age_group_counts = df_copy['_age_group'].value_counts()
        
        groups = []
        for group, count in age_group_counts.items():
            group_info = {
                'age_group': str(group),
                'visit_count': int(count),
                'percentage': round((count / len(df_copy)) * 100, 1)
            }
            
            if diag_col and diag_col in df_copy.columns:
                group_df = df_copy[df_copy['_age_group'] == group]
                top_diag = group_df[diag_col].value_counts().head(1)
                if not top_diag.empty:
                    group_info['top_diagnosis'] = str(top_diag.index[0])
            
            groups.append(group_info)
        
        return {
            'age_groups': groups,
            'total_with_age': len(df_copy)
        }
    
    def _analyze_weekly_trends(self, df: pd.DataFrame, date_col: str, diag_col: Optional[str], age_col: Optional[str]) -> Dict[str, Any]:
        """Analyze weekly trends if enough data."""
        df_copy = df.copy()
        df_copy['_parsed_datetime'] = pd.to_datetime(df_copy[date_col], errors='coerce')
        df_copy = df_copy.dropna(subset=['_parsed_datetime'])
        
        if len(df_copy) < 7:
            return {'available': False, 'reason': 'Not enough data for weekly analysis'}
        
        df_copy['_week'] = df_copy['_parsed_datetime'].dt.isocalendar().week
        df_copy['_year'] = df_copy['_parsed_datetime'].dt.year
        df_copy['_week_year'] = df_copy['_year'].astype(str) + '-W' + df_copy['_week'].astype(str)
        
        weekly_counts = df_copy['_week_year'].value_counts().sort_index()
        
        return {
            'available': True,
            'total_weeks': len(weekly_counts),
            'peak_week': str(weekly_counts.idxmax()) if not weekly_counts.empty else None,
            'peak_week_visits': int(weekly_counts.max()) if not weekly_counts.empty else 0
        }
