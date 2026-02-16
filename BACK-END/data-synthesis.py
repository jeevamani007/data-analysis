

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
import random
import string
from datetime import datetime, timedelta
import re
from faker import Faker


# ---------------------------------------------------------------------------
# Curated vocabulary for categorical columns
# ---------------------------------------------------------------------------

ACCOUNT_TYPE_VOCAB = ["Savings", "Current", "Premium", "Business", "Student"]
DEVICE_TYPE_VOCAB = ["Mobile", "Desktop", "Tablet"]
PAYMENT_METHOD_VOCAB = [
    "Credit Card", "Debit Card", "Net Banking",
    "UPI", "Wallet", "Cash on Delivery",
]
CHURN_RISK_VOCAB = ["Low", "Medium", "High"]

# Credit-score band -> churn-risk weights  [Low, Medium, High]
CREDIT_SCORE_CHURN_MAP = {
    "excellent": [0.80, 0.15, 0.05],
    "good":      [0.60, 0.30, 0.10],
    "fair":      [0.30, 0.45, 0.25],
    "poor":      [0.10, 0.35, 0.55],
}


def _credit_score_band(score: float) -> str:
    if score >= 750:
        return "excellent"
    if score >= 670:
        return "good"
    if score >= 580:
        return "fair"
    return "poor"


class SchemaAnalyzer:
    """Analyzes database schema to extract column types, ranges, and patterns"""

    def __init__(self):
        self.schema_info = {}
        self.faker = Faker()

    def analyze_schema(self, session_dir: Path, files: List[str]) -> Dict[str, Any]:
        schema = {}
        # ── Pass 1: analyse each table ──────────────────────────────────────────
        for filename in files:
            file_path = session_dir / filename
            table_name = filename.replace(".csv", "").replace("_", " ").title()
            try:
                df = pd.read_csv(file_path)
                table_schema = self._analyze_table_schema(df, table_name)

                # Store the REAL PK values ONLY for pattern inference.
                # They are intentionally NOT written to pk_values at generation
                # time — the generator will create fresh synthetic IDs and store
                # those instead, ensuring FK references point to IDs that actually
                # exist in the synthetic parent table.
                pk_col = table_schema.get("primary_key")
                if pk_col and pk_col in df.columns:
                    table_schema["real_pk_values"] = (
                        df[pk_col].dropna().astype(str).tolist()
                    )
                else:
                    table_schema["real_pk_values"] = []

                schema[table_name] = table_schema
            except Exception as e:
                print(f"Error analyzing {filename}: {e}")

        # ── Pass 2: auto-detect FK relationships by column-name matching ─────────
        auto_relationships: List[Dict[str, Any]] = []
        pk_index: Dict[str, str] = {}
        for tname, tschema in schema.items():
            pk = tschema.get("primary_key", "")
            if pk:
                pk_index[pk.lower()] = tname

        for tname, tschema in schema.items():
            for col_info in tschema["columns"]:
                col_name  = col_info["column_name"]
                col_lower = col_name.lower()
                if col_name == tschema.get("primary_key"):
                    continue
                if col_lower in pk_index:
                    parent_table = pk_index[col_lower]
                    if parent_table != tname:
                        parent_schema = schema[parent_table]
                        parent_pk_name = parent_schema.get("primary_key", "")
                        parent_col_meta = next(
                            (c for c in parent_schema["columns"] if c["column_name"] == parent_pk_name),
                            None,
                        )

                        def _values_look_compatible(
                            parent_meta: Optional[Dict[str, Any]],
                            child_meta: Dict[str, Any],
                        ) -> bool:
                            if not parent_meta:
                                return True
                            parent_vals = set(str(v) for v in parent_meta.get("all_sample_values", []) if v is not None)
                            child_vals  = set(str(v) for v in child_meta.get("all_sample_values", []) if v is not None)
                            if not parent_vals or not child_vals:
                                return True
                            return len(parent_vals & child_vals) > 0

                        if _values_look_compatible(parent_col_meta, col_info):
                            auto_relationships.append({
                                "source_table":  tname,
                                "source_column": col_name,
                                "target_table":  parent_table,
                                "target_column": col_name,
                                "auto_detected": True,
                            })

        schema["__auto_relationships__"] = auto_relationships  # type: ignore[assignment]
        return schema

    def _analyze_table_schema(self, df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
        schema = {
            "table_name": table_name,
            "row_count": len(df),
            "columns": [],
        }
        for col in df.columns:
            col_info = self._analyze_column(df, col)
            schema["columns"].append(col_info)

        raw_candidates = [
            c for c in schema["columns"]
            if c["is_unique"] and c["null_percentage"] < 5
        ]

        def _pk_score(col_meta: Dict[str, Any]) -> int:
            name   = col_meta["column_name"]
            name_l = name.lower()
            score  = 0
            if name_l == "id":                score += 100
            if name_l.endswith("_id"):        score += 90
            if name_l.endswith("id"):         score += 80
            if "id" in name_l:                score += 60
            if any(k in name_l for k in ["key", "code", "number", "no", "acct", "account"]):
                score += 40
            score -= len(name_l) // 5
            return score

        sorted_candidates = sorted(raw_candidates, key=_pk_score, reverse=True)
        pk_candidates = [c["column_name"] for c in sorted_candidates]
        schema["primary_key_candidates"] = pk_candidates
        if pk_candidates:
            schema["primary_key"] = pk_candidates[0]
        return schema

    def _analyze_column(self, df: pd.DataFrame, col: str) -> Dict[str, Any]:
        col_info = {
            "column_name": col,
            "data_type": None,
            "null_percentage": (df[col].isna().sum() / len(df)) * 100,
            "unique_count": df[col].nunique(),
            "is_unique": df[col].nunique() == len(df),
            "total_count": len(df),
            "pattern_type": None,
            "pattern_details": {},
        }

        non_null_values = df[col].dropna().head(100).astype(str).tolist()
        sample_values = [str(v) for v in non_null_values[:50]]
        col_info["sample_values"] = sample_values[:10]
        col_info["all_sample_values"] = sample_values

        dtype = df[col].dtype
        col_lower = col.lower()

        if pd.api.types.is_integer_dtype(dtype):
            col_info["data_type"] = "integer"
            col_info["min_value"] = int(df[col].min()) if not df[col].isna().all() else None
            col_info["max_value"] = int(df[col].max()) if not df[col].isna().all() else None
            col_info["avg_value"] = float(df[col].mean()) if not df[col].isna().all() else None
            if col_info["min_value"] is not None:
                num_digits = len(str(col_info["min_value"]))
                if 8 <= num_digits <= 20 and col_info["is_unique"]:
                    col_info["pattern_type"] = "account_number"
                    col_info["pattern_details"] = {
                        "min_digits": num_digits,
                        "max_digits": len(str(col_info["max_value"])) if col_info["max_value"] else num_digits,
                    }

        elif pd.api.types.is_float_dtype(dtype):
            col_info["data_type"] = "float"
            col_info["min_value"] = float(df[col].min()) if not df[col].isna().all() else None
            col_info["max_value"] = float(df[col].max()) if not df[col].isna().all() else None
            col_info["avg_value"] = float(df[col].mean()) if not df[col].isna().all() else None

            if any(k in col_lower for k in ["amount", "balance", "price", "cost", "value", "total", "sum", "spend"]):
                col_info["pattern_type"] = "amount"
                col_info["pattern_details"] = {
                    "has_decimals": True,
                    "min_value": col_info["min_value"],
                    "max_value": col_info["max_value"],
                    "avg_value": col_info["avg_value"],
                }
            elif any(k in col_lower for k in ["temp", "temperature", "celsius", "fahrenheit"]):
                col_info["pattern_type"] = "temperature"
                col_info["pattern_details"] = {
                    "unit": "celsius" if "celsius" in col_lower else "fahrenheit",
                    "min_value": col_info["min_value"],
                    "max_value": col_info["max_value"],
                }
            elif (
                col_info["min_value"] is not None
                and -50 <= col_info["min_value"] <= 150
                and col_info["max_value"] is not None
                and -50 <= col_info["max_value"] <= 150
            ):
                col_info["pattern_type"] = "temperature"
                col_info["pattern_details"] = {
                    "unit": "celsius",
                    "min_value": col_info["min_value"],
                    "max_value": col_info["max_value"],
                }

        elif pd.api.types.is_bool_dtype(dtype):
            col_info["data_type"] = "boolean"

        elif pd.api.types.is_datetime64_any_dtype(dtype):
            col_info["data_type"] = "date"
            if not df[col].isna().all():
                col_info["min_date"] = str(df[col].min())
                col_info["max_date"] = str(df[col].max())
        else:
            col_info["data_type"] = "string"
            lengths = df[col].astype(str).str.len()
            col_info["min_length"] = int(lengths.min()) if len(lengths) > 0 else 0
            col_info["max_length"] = int(lengths.max()) if len(lengths) > 0 else 0
            col_info["avg_length"] = float(lengths.mean()) if len(lengths) > 0 else 0

            # Detect date/time/timestamp stored as strings (e.g., "eventtimestamp",
            # "eventstamp", etc.).
            # We use BOTH column name and sample values so we don't misclassify
            # random strings that happen to be parseable.
            looks_like_datetime_name = any(
                k in col_lower
                for k in ["date", "time", "timestamp", "eventtime", "event_time", "stamp"]
            )
            if looks_like_datetime_name and sample_values:
                try:
                    parsed = pd.to_datetime(sample_values, errors="coerce")
                    good = parsed.notna().sum()
                    if good >= max(3, int(len(sample_values) * 0.6)):
                        has_time = any(":" in v or "T" in v for v in sample_values if v)
                        col_info["pattern_type"] = "datetime_string"
                        col_info["pattern_details"] = {
                            "min_date": str(parsed.min()),
                            "max_date": str(parsed.max()),
                            "has_time": has_time,
                        }
                except Exception:
                    # Fallback to normal string handling if we cannot safely parse
                    pass

            email_pattern = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
            email_matches = sum(1 for v in sample_values if email_pattern.match(v))
            if email_matches >= len(sample_values) * 0.7:
                col_info["pattern_type"] = "email"
                domains = []
                for v in sample_values:
                    if "@" in v:
                        domain = v.split("@")[1]
                        if domain and domain not in domains:
                            domains.append(domain)
                col_info["pattern_details"] = {"domain_patterns": domains[:5]}

            elif any(k in col_lower for k in ["account_type", "account type", "acct_type"]):
                col_info["pattern_type"] = "account_type"

            elif any(k in col_lower for k in ["device", "device_type"]):
                col_info["pattern_type"] = "device_type"

            elif any(k in col_lower for k in ["payment", "payment_method"]):
                col_info["pattern_type"] = "payment_method"

            elif any(k in col_lower for k in ["churn", "churn_risk"]):
                col_info["pattern_type"] = "churn_risk"

            elif any(k in col_lower for k in ["account", "acc", "id", "number", "num"]):
                if all(
                    v.isdigit() or (v.replace("-", "").replace("_", "").isdigit())
                    for v in sample_values[:5] if v
                ):
                    col_info["pattern_type"] = "account_number"
                    col_info["pattern_details"] = {
                        "format": "numeric",
                        "has_separators": any("-" in v or "_" in v for v in sample_values[:5]),
                    }
                elif all(re.match(r"^[A-Z0-9-]+$", v.upper()) for v in sample_values[:5] if v):
                    col_info["pattern_type"] = "account_number"
                    col_info["pattern_details"] = {
                        "format": "alphanumeric",
                        "has_separators": any("-" in v or "_" in v for v in sample_values[:5]),
                    }

            elif any(k in col_lower for k in ["phone", "mobile", "tel", "contact"]):
                phone_pattern = re.compile(r"^[\d\s\-\+\(\)]+$")
                if all(phone_pattern.match(v) for v in sample_values[:5] if v):
                    col_info["pattern_type"] = "phone_number"
                    col_info["pattern_details"] = {"format": "standard"}

            elif any(
                k in col_lower
                for k in ["name", "first_name", "last_name", "user_name", "username",
                           "full_name", "customer_name", "patient_name"]
            ):
                col_info["pattern_type"] = "name"
                col_info["pattern_details"] = {
                    "name_type": (
                        "first_name" if "first" in col_lower
                        else "last_name" if "last" in col_lower
                        else "full_name" if "full" in col_lower
                        else "name"
                    )
                }

        return col_info


# ---------------------------------------------------------------------------
# Synthetic Data Generator
# ---------------------------------------------------------------------------

class SyntheticDataGenerator:
    """Generates synthetic data based on schema analysis"""

    def __init__(self):
        self.schema: Dict[str, Any] = {}
        self.relationships: List[Dict[str, Any]] = []
        self.generated_data: Dict[str, pd.DataFrame] = {}
        # ── FIX: pk_values now stores ONLY freshly-generated synthetic IDs. ──────
        # Real CSV IDs are NEVER placed here. This means every FK lookup resolves
        # to an ID that genuinely exists in the corresponding synthetic parent table.
        self.pk_values: Dict[str, List[Any]] = {}
        self.faker = Faker()
        self._row_genders: Dict[str, List[str]] = {}

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def generate_synthetic_data(
        self,
        schema: Dict[str, Any],
        relationships: List[Dict[str, Any]],
        num_rows_per_table: Optional[Dict[str, int]] = None,
    ) -> Dict[str, pd.DataFrame]:
        auto_rels: List[Dict[str, Any]] = schema.pop("__auto_relationships__", [])  # type: ignore[arg-type]

        explicit_keys = {
            (r.get("source_table"), r.get("source_column"))
            for r in relationships
        }
        merged_relationships: List[Dict[str, Any]] = list(relationships)
        for rel in auto_rels:
            key = (rel.get("source_table"), rel.get("source_column"))
            if key not in explicit_keys:
                merged_relationships.append(rel)
                # NOTE: Use ASCII-only arrow to avoid Windows 'charmap' codec errors
                # when writing logs to a non-UTF8 console.
                print(
                    f"[FK auto-detected] {rel['source_table']}.{rel['source_column']}"
                    f" -> {rel['target_table']}.{rel['target_column']}"
                )

        self.schema        = schema
        self.relationships = merged_relationships
        self.generated_data = {}
        # ── CRITICAL: start with an empty pk_values dict ─────────────────────────
        # Do NOT pre-seed from real_pk_values. Synthetic IDs are stored here
        # immediately after they are generated so child tables can reference them.
        self.pk_values = {}
        self._row_genders = {}

        if num_rows_per_table is None:
            num_rows_per_table = {}
            for table_name, table_schema in schema.items():
                original_rows = table_schema.get("row_count", 100)
                num_rows_per_table[table_name] = min(max(50, original_rows), 200)

        sorted_tables = self._topological_sort(schema, merged_relationships)

        for table_name in sorted_tables:
            if table_name not in schema:
                continue
            num_rows = num_rows_per_table.get(table_name, 100)
            df = self._generate_table_data(
                table_name, schema[table_name], num_rows, merged_relationships
            )
            self.generated_data[table_name] = df

        return self.generated_data

    # ------------------------------------------------------------------
    # Topological sort (unchanged)
    # ------------------------------------------------------------------

    def _topological_sort(
        self, schema: Dict[str, Any], relationships: List[Dict[str, Any]]
    ) -> List[str]:
        all_tables = set(schema.keys())
        depends_on: Dict[str, List[str]] = {t: [] for t in all_tables}

        for rel in relationships:
            child  = rel.get("source_table")
            parent = rel.get("target_table")
            if child in all_tables and parent in all_tables and child != parent:
                depends_on[child].append(parent)

        sorted_list: List[str] = []
        visited: set = set()
        temp_visited: set = set()

        def visit(table: str) -> None:
            if table in visited:
                return
            if table in temp_visited:
                return
            temp_visited.add(table)
            for parent in depends_on.get(table, []):
                visit(parent)
            temp_visited.discard(table)
            visited.add(table)
            sorted_list.append(table)

        for table in sorted(all_tables):
            if table not in visited:
                visit(table)

        return sorted_list

    # ------------------------------------------------------------------
    # Gender assignment
    # ------------------------------------------------------------------

    def _assign_row_genders(self, num_rows: int) -> List[str]:
        return [random.choice(["male", "female"]) for _ in range(num_rows)]

    # ------------------------------------------------------------------
    # Table data generation
    # ------------------------------------------------------------------

    def _generate_table_data(
        self,
        table_name: str,
        table_schema: Dict[str, Any],
        num_rows: int,
        relationships: List[Dict[str, Any]],
    ) -> pd.DataFrame:
        row_genders = self._assign_row_genders(num_rows)
        self._row_genders[table_name] = row_genders

        data: Dict[str, List[Any]] = {}
        pk_column = table_schema.get("primary_key")
        pk_values: List[Any] = []

        join_date_values: Optional[List[str]] = None
        join_date_col_name: Optional[str] = None

        for col_info in table_schema["columns"]:
            col_name_lower = col_info["column_name"].lower()
            if "join" in col_name_lower and "date" in col_name_lower:
                join_date_col_name = col_info["column_name"]
                break

        for col_info in table_schema["columns"]:
            col_name = col_info["column_name"]
            col_name_lower = col_name.lower()

            fk_info = self._find_foreign_key(table_name, col_name, relationships)

            if fk_info:
                ref_table = fk_info["target_table"]
                ref_col   = fk_info["target_column"]
                if ref_table in self.pk_values and self.pk_values[ref_table]:
                    # ── Parent synthetic IDs are available -> sample from them. ───
                    # Because pk_values is NEVER pre-seeded with real CSV IDs,
                    # every value here is guaranteed to exist in the synthetic
                    # parent table — referential integrity is maintained.
                    values = random.choices(self.pk_values[ref_table], k=num_rows)
                    print(
                        # ASCII-only arrow for Windows console compatibility
                        f"  [FK resolved] {table_name}.{col_name} -> "
                        f"{ref_table}.{ref_col}  "
                        f"({len(self.pk_values[ref_table])} synthetic parent IDs available)"
                    )
                else:
                    # ASCII-only arrow for Windows console compatibility
                    print(
                        f"  [FK warning] {table_name}.{col_name} -> {ref_table}.{ref_col}: "
                        f"parent PK pool not available; generating stand-alone values."
                    )
                    values = self._generate_column_values(col_info, num_rows)

            elif col_name == pk_column:
                # Generate brand-new synthetic IDs using the real CSV values
                # only as a FORMAT template (pattern inference), not as actual values.
                real_pks = table_schema.get("real_pk_values") or []
                values   = self._generate_primary_key_values(col_info, num_rows, real_pks)
                pk_values = values
                # ── Store synthetic IDs immediately so child tables processed
                # later in this same generation pass can reference them. ───────
                self.pk_values[table_name] = list(pk_values)
                print(
                    f"  [PK generated] {table_name}.{col_name}: "
                    f"{num_rows} synthetic IDs (e.g. {pk_values[:3]})"
                )

            elif "last" in col_name_lower and "login" in col_name_lower and join_date_values is not None:
                values = self._generate_last_login_values(join_date_values, num_rows)

            elif col_name_lower in ("gender", "sex"):
                values = [g.capitalize() for g in row_genders]

            elif col_info.get("pattern_type") == "name":
                values = self._generate_name_values_gendered(col_info, num_rows, row_genders)

            elif "avg" in col_name_lower and "order" in col_name_lower and "value" in col_name_lower:
                values = self._generate_amount_values(col_info, num_rows)

            elif col_info.get("pattern_type") == "device_type":
                age_col = self._find_col_data(data, ["age"])
                values = self._generate_device_type_values(num_rows, age_col)

            elif col_info.get("pattern_type") == "payment_method":
                age_col = self._find_col_data(data, ["age"])
                values = self._generate_payment_method_values(num_rows, age_col)

            elif col_info.get("pattern_type") == "churn_risk":
                credit_col = self._find_col_data(data, ["credit_score", "creditscore", "credit"])
                values = self._generate_churn_risk_values(num_rows, credit_col)

            else:
                values = self._generate_column_values(col_info, num_rows, row_genders=row_genders)

            data[col_name] = values

            if col_name == join_date_col_name:
                join_date_values = values

        # FIX 2 – enforce Total_Spend = Total_Orders × Avg_Order_Value exactly
        for col_info in table_schema["columns"]:
            col_name = col_info["column_name"]
            col_name_lower = col_name.lower()
            if "avg" in col_name_lower and "order" in col_name_lower and "value" in col_name_lower:
                spend_key = next(
                    (k for k in data if "total" in k.lower() and "spend" in k.lower()), None
                )
                orders_key = next(
                    (k for k in data
                     if "total" in k.lower() and "order" in k.lower()
                     and "value" not in k.lower()), None
                )
                if spend_key and orders_key:
                    avg_vals  = data[col_name]
                    ord_vals  = data[orders_key]
                    data[spend_key] = [
                        round(float(o) * float(a), 2) if o and a else 0.0
                        for o, a in zip(ord_vals, avg_vals)
                    ]
                    data[col_name] = [
                        round(float(s) / float(o), 2) if o and float(o) > 0 else 0.0
                        for s, o in zip(data[spend_key], ord_vals)
                    ]

        return pd.DataFrame(data)

    def _find_foreign_key(
        self,
        table_name: str,
        column_name: str,
        relationships: List[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        for rel in relationships:
            if rel.get("source_table") == table_name and rel.get("source_column") == column_name:
                return rel
        return None

    def _find_col_data(self, data: Dict[str, List[Any]], keywords: List[str]) -> Optional[List[Any]]:
        for key in data:
            key_lower = key.lower()
            if any(kw in key_lower for kw in keywords):
                return data[key]
        return None

    # ------------------------------------------------------------------
    # Primary key generation
    # ------------------------------------------------------------------

    def _infer_pk_pattern(self, real_pk_values: List[str]) -> Dict[str, Any]:
        """
        Infer format from existing real PK strings — used for formatting only.
        The actual numeric values are NOT reused in synthetic output.
        """
        sample = real_pk_values[0]

        if all(v.isdigit() for v in real_pk_values):
            max_num = max(int(v) for v in real_pk_values)
            width   = max(len(v) for v in real_pk_values)
            return {"kind": "numeric", "width": width, "max_num": max_num}

        m = re.match(r"^([A-Za-z]+)(\d+)$", sample)
        if m and all(re.match(rf"^{re.escape(m.group(1))}\d+$", v) for v in real_pk_values):
            prefix = m.group(1)
            max_num = 0
            width   = 0
            for v in real_pk_values:
                m2 = re.match(rf"^{re.escape(prefix)}(\d+)$", v)
                if not m2:
                    continue
                num_str = m2.group(1)
                max_num = max(max_num, int(num_str))
                width   = max(width, len(num_str))
            return {"kind": "prefix_numeric", "prefix": prefix, "width": width, "max_num": max_num}

        return {"kind": "generic", "length": len(sample)}

    def _generate_primary_key_values(
        self,
        col_info: Dict[str, Any],
        num_rows: int,
        real_pk_values: Optional[List[str]] = None,
    ) -> List[Any]:
        """
        Generate FRESH synthetic primary key values.

        The real_pk_values argument is used ONLY to infer the ID format
        (zero-padding width, alpha prefix, string length, etc.).
        New IDs start just beyond the observed range so:
          - They never collide with real uploaded IDs (no data leakage).
          - They form a self-consistent synthetic universe where every FK
            in a child table resolves to an ID that exists in the synthetic
            parent table.
        """
        if real_pk_values and len(real_pk_values) > 0:
            pattern  = self._infer_pk_pattern(real_pk_values)
            # We generate IDs strictly BEYOND the real range so they never
            # overlap with original data.
            real_set = set(real_pk_values)   # safety guard only

            synthetic: List[Any] = []

            if pattern["kind"] == "numeric":
                # Return plain integers (not strings) so the column dtype matches
                # the original integer PK and FK sampling produces compatible values.
                current = pattern["max_num"]
                width   = pattern["width"]
                while len(synthetic) < num_rows:
                    current += 1
                    candidate_str = str(current).zfill(width)
                    if candidate_str not in real_set:
                        # Store as int if no zero-padding needed, else as string
                        synthetic.append(current if not candidate_str.startswith("0") else candidate_str)

            elif pattern["kind"] == "prefix_numeric":
                current = pattern["max_num"]
                width   = pattern["width"]
                prefix  = pattern["prefix"]
                while len(synthetic) < num_rows:
                    current += 1
                    candidate = f"{prefix}{str(current).zfill(width)}"
                    if candidate not in real_set:
                        real_set.add(candidate)
                        synthetic.append(candidate)

            else:   # generic fixed-length
                length  = pattern["length"]
                charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
                while len(synthetic) < num_rows:
                    candidate = "".join(random.choices(charset, k=length))
                    if candidate not in real_set:
                        real_set.add(candidate)
                        synthetic.append(candidate)

            return synthetic

        # No real values — type-based generation
        data_type = col_info["data_type"]
        if data_type == "integer":
            start = col_info.get("min_value") or 1
            return list(range(start, start + num_rows))
        elif data_type == "string":
            prefix = col_info["column_name"][:3].upper()
            return [f"{prefix}{i:06d}" for i in range(1, num_rows + 1)]
        return self._generate_column_values(col_info, num_rows)

    # ------------------------------------------------------------------
    # General column value dispatch
    # ------------------------------------------------------------------

    def _generate_column_values(
        self,
        col_info: Dict[str, Any],
        num_rows: int,
        row_genders: Optional[List[str]] = None,
    ) -> List[Any]:
        data_type    = col_info["data_type"]
        pattern_type = col_info.get("pattern_type")
        null_pct     = col_info.get("null_percentage", 0)
        num_nulls    = int((null_pct / 100) * num_rows)
        n            = num_rows - num_nulls

        if pattern_type == "email":
            values = self._generate_email_values(col_info, n)
        elif pattern_type == "datetime_string":
            values = self._generate_datetime_string_values(col_info, n)
        elif pattern_type == "account_number":
            values = self._generate_account_number_values(col_info, n)
        elif pattern_type == "phone_number":
            values = self._generate_phone_number_values(col_info, n)
        elif pattern_type == "amount":
            values = self._generate_amount_values(col_info, n)
        elif pattern_type == "temperature":
            values = self._generate_temperature_values(col_info, n)
        elif pattern_type == "name":
            values = self._generate_name_values_gendered(col_info, n, row_genders)
        elif pattern_type == "account_type":
            values = [random.choice(ACCOUNT_TYPE_VOCAB) for _ in range(n)]
        elif pattern_type == "device_type":
            values = self._generate_device_type_values(n)
        elif pattern_type == "payment_method":
            values = self._generate_payment_method_values(n)
        elif pattern_type == "churn_risk":
            values = self._generate_churn_risk_values(n)
        elif data_type == "integer":
            min_val = col_info.get("min_value") or 0
            max_val = col_info.get("max_value") or 1000
            values = [random.randint(int(min_val), int(max_val)) for _ in range(n)]
        elif data_type == "float":
            min_val = float(col_info.get("min_value") or 0.0)
            max_val = float(col_info.get("max_value") or 1000.0)
            values = [round(random.uniform(min_val, max_val), 2) for _ in range(n)]
        elif data_type == "boolean":
            values = [random.choice([True, False]) for _ in range(n)]
        elif data_type == "date":
            min_date = col_info.get("min_date")
            max_date = col_info.get("max_date")
            try:
                min_dt = pd.to_datetime(min_date) if min_date else datetime.now() - timedelta(days=365)
                max_dt = pd.to_datetime(max_date) if max_date else datetime.now()
            except Exception:
                min_dt = datetime.now() - timedelta(days=365)
                max_dt = datetime.now()
            date_range_days = max((max_dt - min_dt).days, 1)
            values = [
                (min_dt + timedelta(days=random.randint(0, date_range_days))).strftime("%Y-%m-%d")
                for _ in range(n)
            ]
        else:
            observed = col_info.get("all_sample_values", [])
            if observed:
                values = [random.choice(observed) for _ in range(n)]
            else:
                values = [""] * n

        values.extend([None] * num_nulls)
        random.shuffle(values)
        return values

    # ------------------------------------------------------------------
    # Email generation (unique)
    # ------------------------------------------------------------------

    def _generate_email_values(self, col_info: Dict[str, Any], num_rows: int) -> List[str]:
        pattern_details = col_info.get("pattern_details", {})
        domains = pattern_details.get("domain_patterns") or [
            "gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "example.com"
        ]
        seen: set = set()
        values: List[str] = []
        attempt_limit = num_rows * 15
        patterns = ["dot", "underscore_year", "initial"]
        pattern_weights = [0.40, 0.35, 0.25]

        for _ in range(attempt_limit):
            if len(values) >= num_rows:
                break
            first  = self.faker.first_name().lower().replace(" ", "").replace("'", "")
            last   = self.faker.last_name().lower().replace(" ", "").replace("'", "")
            domain = random.choice(domains)
            style  = random.choices(patterns, weights=pattern_weights, k=1)[0]

            if style == "dot":
                local = f"{first}.{last}"
            elif style == "underscore_year":
                year_suffix = random.randint(60, 99)
                local = f"{first}_{last}{year_suffix}"
            else:
                num = random.randint(10, 999)
                local = f"{first[0]}{last}{num}"

            email = f"{local}@{domain}"
            if email not in seen:
                seen.add(email)
                values.append(email)

        while len(values) < num_rows:
            idx = len(values)
            first    = self.faker.first_name().lower().replace(" ", "")
            fallback = f"{first}{idx:04d}@{random.choice(domains)}"
            if fallback not in seen:
                seen.add(fallback)
                values.append(fallback)

        return values

    # ------------------------------------------------------------------
    # Account numbers
    # ------------------------------------------------------------------

    def _generate_account_number_values(self, col_info: Dict[str, Any], num_rows: int) -> List[str]:
        pattern_details = col_info.get("pattern_details", {})
        data_type       = col_info["data_type"]
        has_separators  = pattern_details.get("has_separators", False)

        if data_type == "integer":
            min_val = int(col_info.get("min_value") or 10000000)
            max_val = int(col_info.get("max_value") or 999999999999)
            return [str(random.randint(min_val, max_val)) for _ in range(num_rows)]

        fmt     = pattern_details.get("format", "alphanumeric")
        min_len = col_info.get("min_length") or 8
        max_len = col_info.get("max_length") or 16
        values  = []
        for _ in range(num_rows):
            length = random.randint(min_len, max_len)
            if fmt == "numeric":
                acc = "".join(random.choices(string.digits, k=length))
            else:
                acc = "".join(random.choices(string.ascii_uppercase + string.digits, k=length))
            if has_separators and random.random() < 0.5:
                acc = "-".join(acc[j : j + 4] for j in range(0, len(acc), 4))
            values.append(acc)
        return values

    # ------------------------------------------------------------------
    # Phone numbers (US format)
    # ------------------------------------------------------------------

    def _generate_phone_number_values(self, col_info: Dict[str, Any], num_rows: int) -> List[str]:
        values = []
        for _ in range(num_rows):
            npa  = random.randint(200, 999)
            nxx  = random.randint(200, 999)
            subs = random.randint(1000, 9999)
            values.append(f"({npa}) {nxx}-{subs}")
        return values

    # ------------------------------------------------------------------
    # Amount / temperature
    # ------------------------------------------------------------------

    def _generate_amount_values(self, col_info: Dict[str, Any], num_rows: int) -> List[float]:
        min_val = float(col_info.get("min_value") or 0.0)
        max_val = float(col_info.get("max_value") or 10000.0)
        avg_val = float(col_info.get("avg_value") or (min_val + max_val) / 2)
        std_dev = (max_val - min_val) / 4
        values  = []
        for _ in range(num_rows):
            val = np.random.normal(avg_val, std_dev)
            val = max(min_val, min(max_val, val))
            values.append(round(val, 2))
        return values

    def _generate_temperature_values(self, col_info: Dict[str, Any], num_rows: int) -> List[float]:
        min_val = float(col_info.get("min_value") or 0.0)
        max_val = float(col_info.get("max_value") or 100.0)
        return [round(random.uniform(min_val, max_val), 1) for _ in range(num_rows)]

    def _generate_datetime_string_values(self, col_info: Dict[str, Any], num_rows: int) -> List[str]:
        """
        Generate date/time strings for columns that looked like timestamps
        (e.g., 'eventtimestamp', 'event_time') but were stored as text.
        """
        details = col_info.get("pattern_details", {}) or {}
        min_date = details.get("min_date")
        max_date = details.get("max_date")
        has_time = bool(details.get("has_time", True))

        try:
            min_dt = pd.to_datetime(min_date) if min_date else datetime.now() - timedelta(days=365)
            max_dt = pd.to_datetime(max_date) if max_date else datetime.now()
        except Exception:
            min_dt = datetime.now() - timedelta(days=365)
            max_dt = datetime.now()

        if max_dt < min_dt:
            max_dt = min_dt + timedelta(days=1)

        total_seconds = int((max_dt - min_dt).total_seconds()) or 1
        fmt = "%Y-%m-%d %H:%M:%S" if has_time else "%Y-%m-%d"

        values: List[str] = []
        for _ in range(num_rows):
            offset = random.randint(0, total_seconds)
            dt = min_dt + timedelta(seconds=offset)
            values.append(dt.strftime(fmt))
        return values

    # ------------------------------------------------------------------
    # Name generation (gender-aware)
    # ------------------------------------------------------------------

    def _generate_name_values_gendered(
        self,
        col_info: Dict[str, Any],
        num_rows: int,
        row_genders: Optional[List[str]],
    ) -> List[str]:
        pattern_details = col_info.get("pattern_details", {})
        name_type       = pattern_details.get("name_type", "name")

        if row_genders is None or len(row_genders) != num_rows:
            row_genders = [random.choice(["male", "female"]) for _ in range(num_rows)]

        values = []
        for gender in row_genders:
            if name_type == "last_name":
                values.append(self.faker.last_name())
            elif name_type == "first_name":
                values.append(self.faker.first_name_male() if gender == "male" else self.faker.first_name_female())
            else:
                values.append(self.faker.name_male() if gender == "male" else self.faker.name_female())
        return values

    # ------------------------------------------------------------------
    # Last login (always ≥ join date)
    # ------------------------------------------------------------------

    def _generate_last_login_values(
        self, join_date_values: List[Optional[str]], num_rows: int
    ) -> List[str]:
        today  = datetime.now()
        values = []
        for jd in join_date_values:
            try:
                join_dt = pd.to_datetime(jd)
            except Exception:
                join_dt = today - timedelta(days=365)
            days_since_join = max((today - join_dt).days, 0)
            offset = random.randint(0, days_since_join)
            values.append((join_dt + timedelta(days=offset)).strftime("%Y-%m-%d"))
        return values

    # ------------------------------------------------------------------
    # Age-correlated device & payment
    # ------------------------------------------------------------------

    def _generate_device_type_values(
        self, num_rows: int, age_col: Optional[List[Any]] = None
    ) -> List[str]:
        values = []
        for i in range(num_rows):
            age = int(age_col[i]) if (age_col and i < len(age_col) and age_col[i] is not None) \
                  else random.randint(18, 65)
            if age < 30:   weights = [0.68, 0.22, 0.10]
            elif age < 40: weights = [0.55, 0.33, 0.12]
            elif age < 50: weights = [0.45, 0.42, 0.13]
            elif age < 60: weights = [0.33, 0.54, 0.13]
            else:          weights = [0.25, 0.63, 0.12]
            values.append(random.choices(DEVICE_TYPE_VOCAB, weights=weights, k=1)[0])
        return values

    def _generate_payment_method_values(
        self, num_rows: int, age_col: Optional[List[Any]] = None
    ) -> List[str]:
        values  = []
        methods = PAYMENT_METHOD_VOCAB
        for i in range(num_rows):
            age = int(age_col[i]) if (age_col and i < len(age_col) and age_col[i] is not None) \
                  else random.randint(18, 65)
            if age < 25:   weights = [0.08, 0.18, 0.07, 0.38, 0.24, 0.05]
            elif age < 35: weights = [0.18, 0.22, 0.10, 0.28, 0.17, 0.05]
            elif age < 45: weights = [0.28, 0.25, 0.14, 0.18, 0.10, 0.05]
            elif age < 55: weights = [0.32, 0.28, 0.18, 0.12, 0.06, 0.04]
            else:          weights = [0.35, 0.30, 0.22, 0.07, 0.03, 0.03]
            values.append(random.choices(methods, weights=weights, k=1)[0])
        return values

    # ------------------------------------------------------------------
    # Credit-score-correlated churn risk
    # ------------------------------------------------------------------

    def _generate_churn_risk_values(
        self, num_rows: int, credit_col: Optional[List[Any]] = None
    ) -> List[str]:
        values = []
        for i in range(num_rows):
            if credit_col and i < len(credit_col) and credit_col[i] is not None:
                score = float(credit_col[i])
            else:
                score = random.uniform(300, 850)
            band    = _credit_score_band(score)
            weights = CREDIT_SCORE_CHURN_MAP[band]
            values.append(random.choices(CHURN_RISK_VOCAB, weights=weights, k=1)[0])
        return values


# ---------------------------------------------------------------------------
# Main engine
# ---------------------------------------------------------------------------

class DataSynthesisEngine:
    """Main engine for database synthesis"""

    def __init__(self):
        self.schema_analyzer = SchemaAnalyzer()
        self.data_generator  = SyntheticDataGenerator()

    def synthesize_database(
        self,
        session_dir: Path,
        files: List[str],
        relationships: List[Dict[str, Any]],
        num_rows_per_table: Optional[Dict[str, int]] = None,
    ) -> Dict[str, Any]:
        print(f"Analyzing schema for {len(files)} files...")
        schema = self.schema_analyzer.analyze_schema(session_dir, files)

        print("Generating synthetic data...")
        synthetic_data = self.data_generator.generate_synthetic_data(
            schema, relationships, num_rows_per_table
        )

        synthetic_data_dict = {}
        for table_name, df in synthetic_data.items():
            synthetic_data_dict[table_name] = {
                "columns":   df.columns.tolist(),
                "rows":      df.values.tolist(),
                "row_count": len(df),
            }

        return {
            "schema":       schema,
            "synthetic_data": synthetic_data_dict,
            "relationships": self.data_generator.relationships,
        }
