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

    def _find_date_timestamp_columns(self, df: pd.DataFrame) -> List[Tuple[str, Optional[str]]]:
        """
        Find (date_col, time_col) for the table.
        Returns list of (date_col, time_col or None). Uses first valid pair.
        Excludes DOB columns.
        """
        candidates = []
        for col in df.columns:
            if self._is_dob_column(col):
                continue
            col_lower = col.lower()
            if any(k in col_lower for k in self.date_keywords):
                # Check if parseable as datetime
                try:
                    sample = df[col].dropna().head(10)
                    if len(sample) == 0:
                        continue
                    parsed = pd.to_datetime(sample, errors='coerce')
                    if parsed.notna().sum() >= len(sample) * 0.5:
                        candidates.append(col)
                except Exception:
                    pass

        # Also try columns with date-like names that parse as datetime (fallback)
        existing_cols = {c for c in candidates}
        for col in df.columns:
            if self._is_dob_column(col) or col in existing_cols:
                continue
            col_lower = col.lower()
            if not any(k in col_lower for k in ['date', 'time', 'timestamp', 'created', 'admission', 'discharge', 'reg', 'appt', 'donation', 'visit', 'recorded', 'scheduled']):
                continue
            try:
                sample = df[col].dropna().head(20)
                if len(sample) < 5:
                    continue
                parsed = pd.to_datetime(sample, errors='coerce')
                if parsed.notna().sum() / len(sample) >= 0.5:
                    sample_str = sample.astype(str)
                    if sample_str.str.match(r'^\d{1,3}$').all() and sample_str.str.len().max() <= 3:
                        continue
                    candidates.append(col)
            except Exception:
                pass

        if not candidates:
            return []

        # Pick primary date column - prefer combined date+time, or date + separate time
        col = candidates[0]
        sample_val = df[col].dropna().iloc[0] if len(df) > 0 else None
        if sample_val is not None:
            parsed = pd.to_datetime(sample_val, errors='coerce')
            if pd.notna(parsed):
                # Has time component?
                if parsed.hour != 0 or parsed.minute != 0 or parsed.second != 0:
                    return [(col, None)]
        # Look for separate time column
        for c2 in df.columns:
            if c2 == col or self._is_dob_column(c2):
                continue
            if 'time' in c2.lower() and 'date' not in c2.lower() and 'stamp' not in c2.lower():
                return [(col, c2)]
        return [(col, None)]

    def _infer_column_purpose(self, col_name: str, series: pd.Series) -> Dict[str, Any]:
        """
        Infer healthcare column purpose from observed column name and data. Pattern-based, no hardcoded columns.
        Returns: { purpose, work_explanation, null_explanation } for admission, appointment, discharge, lab, etc.
        """
        col_lower = col_name.lower().replace('-', '_')
        tokens = re.split(r'[_\s]+', col_lower)

        null_count = series.isna().sum()
        total = len(series)
        null_pct = (null_count / total * 100) if total > 0 else 0
        non_null = series.dropna()
        sample_vals = non_null.head(5).astype(str).tolist() if len(non_null) > 0 else []

        purpose = None
        work_explanation = None

        # Healthcare-purpose patterns (observed from column name)
        if tokens[-1] == 'id' or col_lower.endswith('_id'):
            base = '_'.join(tokens[:-1]) if len(tokens) > 1 else 'record'
            purpose = f"{base.replace('_', ' ').title()} identifier"
        elif 'admission' in col_lower or 'admit' in col_lower:
            if 'date' in tokens or 'time' in tokens:
                purpose = "Patient admission date/time"
                work_explanation = "When patient was admitted"
            else:
                purpose = "Admission identifier"
        elif 'discharge' in col_lower:
            if 'date' in tokens or 'time' in tokens:
                purpose = "Patient discharge date/time"
                work_explanation = "When patient was discharged"
            else:
                purpose = "Discharge information"
        elif 'appt' in col_lower or 'appointment' in col_lower:
            if 'date' in tokens or 'time' in tokens:
                purpose = "Patient appointment date/time"
                work_explanation = "When appointment was scheduled"
            else:
                purpose = "Appointment identifier"
        elif 'reg' in col_lower or 'registration' in col_lower or 'visit' in col_lower:
            if 'date' in tokens or 'time' in tokens:
                purpose = "Patient visit/registration date/time"
                work_explanation = "When patient registered or visited"
            else:
                purpose = "Registration identifier"
        elif 'donation' in col_lower:
            if 'date' in tokens:
                purpose = "Blood donation date"
                work_explanation = "When donation was made"
            else:
                purpose = "Donation information"
        elif 'lab' in col_lower or 'test' in col_lower or 'report' in col_lower or 'result' in col_lower:
            purpose = "Lab test or report"
            work_explanation = "Test result or report value"
        elif 'date' in tokens or col_lower.endswith('_date'):
            idx = next((i for i, t in enumerate(tokens) if t == 'date'), -1)
            prefix = ' '.join(tokens[:idx]).replace('_', ' ').title() if idx > 0 else 'Event'
            purpose = f"{prefix} date" if prefix else "Date"
        elif 'time' in tokens or col_lower.endswith('_time'):
            idx = next((i for i, t in enumerate(tokens) if t == 'time'), -1)
            prefix = ' '.join(tokens[:idx]).replace('_', ' ').title() if idx > 0 else 'Event'
            purpose = f"{prefix} time" if prefix else "Time"
        elif 'reason' in tokens or 'diagnosis' in tokens or 'symptom' in tokens:
            purpose = "Reason for visit or diagnosis"
            work_explanation = "Why patient came or condition"
        elif 'ward' in tokens or 'dept' in tokens or 'department' in tokens:
            purpose = "Department or care unit"
            work_explanation = "Where patient was treated"
        elif 'slot' in tokens:
            purpose = "Time slot"
            work_explanation = "Morning/Afternoon/Evening session"
        elif 'patient' in tokens:
            purpose = "Patient identifier"
            work_explanation = "Links to patient record"
        elif 'volume' in col_lower or 'ml' in col_lower:
            purpose = "Quantity or volume"
            work_explanation = "Amount (e.g. blood volume)"
        elif 'blood' in col_lower and 'group' in col_lower:
            purpose = "Blood type"
        elif 'name' in tokens:
            purpose = "Name"
        elif 'status' in tokens or 'result' in tokens:
            purpose = "Status or outcome"
        else:
            purpose = col_name.replace('_', ' ').title()

        return {
            "purpose": purpose,
            "work_explanation": work_explanation,
            "null_pct": float(round(null_pct, 1)),
            "null_explanation": "Not recorded or missing" if null_pct > 0 else None,
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
        else:
            if patient_id:
                parts.append(f"Record for patient {patient_id}")
            if ward:
                parts.append(f"at {ward}")
        return " ".join(parts).strip() or "Healthcare record"

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

    def _explain_value(self, value: Any, purpose_info: Dict, col_name: str) -> str:
        """Generate explanation for a single value based on observed patterns."""
        if value is None or (isinstance(value, float) and pd.isna(value)) or str(value).strip() == '':
            return purpose_info.get("null_explanation") or "Empty or not recorded"
        val_str = str(value).strip()
        if val_str.lower() in ('nan', 'none', 'null', 'na', 'n/a'):
            return "Not recorded or missing"
        return val_str

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
        df = df.copy()
        df['__dt'] = pd.to_datetime(df[date_col], errors='coerce')
        if time_col and time_col in df.columns:
            df['__date_str'] = df[date_col].astype(str).str.split().str[0]
            df['__time_str'] = df[time_col].astype(str)
            df['__dt'] = pd.to_datetime(df['__date_str'] + ' ' + df['__time_str'], errors='coerce')
            df['__dt'] = df['__dt'].fillna(pd.to_datetime(df[date_col], errors='coerce'))
        df = df.dropna(subset=['__dt'])
        df = df.sort_values('__dt', ascending=True)

        # Observe and infer column purposes for all columns (no hardcoding)
        skip_cols = {'__dt', '__date_str', '__time_str'}
        data_cols = [c for c in df.columns if c not in skip_cols and not str(c).startswith('__')]
        column_purposes = {}
        for c in data_cols:
            column_purposes[c] = self._infer_column_purpose(c, df[c])

        adm_col, dis_col = self._find_admission_discharge_columns(df)
        appt_col = self._find_appointment_column(df)

        records = []
        for _, row in df.iterrows():
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

            stay_duration = None
            if adm_col and dis_col and adm_col in row.index and dis_col in row.index:
                stay_duration = self._calculate_stay_duration_explanation(row[adm_col], row[dis_col])

            patient_id = self._get_patient_id_from_record(raw_record, column_purposes)

            rec = {
                'table_name': table_name,
                'file_name': file_name or f"{table_name}.csv",
                'date': dt.strftime('%Y-%m-%d'),
                'time': dt.strftime('%H:%M:%S') if dt.hour != 0 or dt.minute != 0 or dt.second != 0 else '',
                'datetime_sort': dt,
                'record': raw_record,
                'column_purposes': column_purposes,
                'data_flow_explanation': " | ".join(data_flow_parts) if data_flow_parts else "Record at this date/time",
                'value_explanations': explained_record,
                'work_summary': work_summary,
                '_patient_id': patient_id,
                '_event_datetime': dt,
            }
            if adm_col and adm_col in row.index:
                rec['_admission_datetime'] = pd.to_datetime(row[adm_col], errors='coerce')
            if appt_col and appt_col in row.index:
                rec['_appointment_datetime'] = pd.to_datetime(row[appt_col], errors='coerce')
            if stay_duration:
                rec['stay_duration'] = stay_duration
            records.append(rec)
        return records

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
            tables_summary.append({
                'table_name': table_name,
                'file_name': file_name,
                'row_count': len(records),
                'date_column': self._find_date_timestamp_columns(df)[0][0] if self._find_date_timestamp_columns(df) else None,
                'first_date': records[0]['date'],
                'last_date': records[-1]['date'],
                'column_purposes': col_purposes,
            })

        if not all_records:
            return {
                'success': False,
                'error': 'No date/timestamp columns found in healthcare tables (excluding date of birth).',
                'tables_checked': [t.table_name for t in tables]
            }

        # Sort globally by datetime ascending
        all_records.sort(key=lambda r: r['datetime_sort'])

        # Appointment–admission gap: if gap > 2 hours, mark as hospital delay
        self._add_appointment_admission_gap_analysis(all_records)

        for r in all_records:
            del r['datetime_sort']
            for k in list(r.keys()):
                if k.startswith('_'):
                    del r[k]

        first_date = all_records[0]['date']
        last_date = all_records[-1]['date']
        first_time = all_records[0].get('time', '')
        last_time = all_records[-1].get('time', '')

        # Build diagram nodes: unique (date, time) points, each with list of records
        diagram_nodes = []
        seen = set()
        for r in all_records:
            key = (r['date'], r.get('time', ''))
            if key not in seen:
                seen.add(key)
                nodes_for_key = [x for x in all_records if (x['date'], x.get('time', '')) == key]
                diagram_nodes.append({
                    'date': r['date'],
                    'time': r.get('time', ''),
                    'count': len(nodes_for_key),
                    'records': nodes_for_key,
                    'table_names': list(set(x['table_name'] for x in nodes_for_key))
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
        }
