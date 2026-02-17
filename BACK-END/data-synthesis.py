
"""
Data Synthesis Engine
--------------------
Generates fully synthetic CSV datasets that preserve:
- Schema (column names)
- Data types (int/float/string/date/timestamp)
- Numeric/date ranges
- Categorical value sets
- Primary key uniqueness
- Foreign key referential integrity (child FK values reference parent PK pool)

This module is loaded dynamically from `main.py` via importlib because the filename
contains a hyphen: `data-synthesis.py`.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
import random
import re
import uuid
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

try:
    from faker import Faker  # type: ignore
    _HAS_FAKER = True
except Exception:
    Faker = None  # type: ignore
    _HAS_FAKER = False

# -----------------------------
# Helpers
# -----------------------------

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_PHONE_RE = re.compile(r"^\+?[\d\-\s\(\)]{7,}$")
_UUID_RE = re.compile(r"^[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}$")
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_ISO_TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?$")
_ALNUM_CODE_RE = re.compile(r"^[A-Za-z0-9\-_]{6,}$")
_ONLY_DIGITS_RE = re.compile(r"^\d+$")


def _safe_table_name_from_filename(filename: str) -> str:
    # Must match `main.py` behavior for consistent relationship keys
    return filename.replace(".csv", "").replace("_", " ").title()


def _is_missing(x: Any) -> bool:
    if x is None:
        return True
    try:
        return bool(pd.isna(x))
    except Exception:
        return False


def _coerce_str(x: Any) -> str:
    if _is_missing(x):
        return ""
    return str(x)


def _try_parse_datetime_series(s: pd.Series) -> Tuple[pd.Series, float]:
    """
    Returns (parsed_series, success_rate).
    """
    if s.empty:
        return pd.to_datetime(pd.Series([], dtype="object"), errors="coerce"), 0.0
    # pandas default parsing is now strict/consistent; avoid deprecated args
    parsed = pd.to_datetime(s, errors="coerce")
    success = float(parsed.notna().mean()) if len(parsed) else 0.0
    return parsed, success


def _infer_type(s: pd.Series) -> str:
    """
    Infer one of: integer, float, date, timestamp, string
    """
    non_null = s.dropna()
    if non_null.empty:
        return "string"

    # Try datetime (date/timestamp)
    parsed_dt, success_rate = _try_parse_datetime_series(non_null.astype(str).head(200))
    if success_rate >= 0.8:
        # Date vs timestamp: if any has non-midnight time or includes time info
        # Use original string sample too
        sample_str = non_null.astype(str).head(50).tolist()
        has_time_hint = any(bool(_ISO_TS_RE.match(x)) for x in sample_str) or any(
            any(t in x for t in [":", "T"]) for x in sample_str
        )
        if has_time_hint:
            return "timestamp"
        return "date"

    # Try numeric
    coerced = pd.to_numeric(non_null, errors="coerce")
    num_rate = float(coerced.notna().mean())
    if num_rate >= 0.9:
        # integer vs float
        # If all non-null values are integers (or close), call integer
        as_float = coerced.dropna().astype(float)
        if as_float.empty:
            return "integer"
        is_int_like = ((as_float % 1) == 0).mean() >= 0.98
        return "integer" if is_int_like else "float"

    return "string"


def _detect_string_pattern(sample_values: List[str], column_name: str) -> Optional[str]:
    """
    Returns pattern_type for UI + generation hints.
    """
    col = column_name.lower()
    sv = [v for v in sample_values if v]
    if not sv:
        return None

    # Strong name-based hints
    if any(k in col for k in ["username", "user_name", "login", "handle"]):
        return "username"
    if "email" in col:
        return "email"
    if any(k in col for k in ["phone", "mobile", "tel"]):
        return "phone_number"
    if any(k in col for k in ["amount", "amt", "balance", "price", "cost", "salary", "premium"]):
        return "amount"
    if any(k in col for k in ["temp", "temperature"]):
        return "temperature"
    if any(k in col for k in ["name", "first", "last", "fullname"]):
        return "name"
    if any(k in col for k in ["account", "acct"]) and "id" not in col and "name" not in col:
        return "account_number"

    # Value-based hints
    email_hits = sum(1 for v in sv[:50] if _EMAIL_RE.match(v))
    if email_hits >= max(1, int(0.6 * min(50, len(sv)))):
        return "email"
    phone_hits = sum(1 for v in sv[:50] if _PHONE_RE.match(v))
    if phone_hits >= max(1, int(0.6 * min(50, len(sv)))):
        return "phone_number"
    uuid_hits = sum(1 for v in sv[:50] if _UUID_RE.match(v))
    if uuid_hits >= max(1, int(0.6 * min(50, len(sv)))):
        return "uuid"
    digit_code_hits = sum(1 for v in sv[:50] if _ONLY_DIGITS_RE.match(v) and len(v) >= 6)
    if digit_code_hits >= max(1, int(0.6 * min(50, len(sv)))):
        return "account_number"
    alnum_hits = sum(1 for v in sv[:50] if _ALNUM_CODE_RE.match(v))
    if alnum_hits >= max(1, int(0.6 * min(50, len(sv)))):
        return "code"

    return None


def _is_categorical(s: pd.Series, inferred_type: str) -> bool:
    """
    Categorical = low cardinality relative to row_count.
    For strings: <= 50 unique or unique_ratio <= 0.2
    For numbers: <= 30 unique and unique_ratio <= 0.2 (e.g., status codes)
    """
    non_null = s.dropna()
    if non_null.empty:
        return False
    nunique = int(non_null.nunique(dropna=True))
    n = int(len(non_null))
    if n == 0:
        return False
    unique_ratio = nunique / max(1, n)
    if inferred_type == "string":
        return nunique <= 50 or unique_ratio <= 0.2
    if inferred_type in ("integer", "float"):
        return nunique <= 30 and unique_ratio <= 0.2
    return False


def _random_date(min_d: date, max_d: date) -> date:
    if max_d < min_d:
        min_d, max_d = max_d, min_d
    delta = (max_d - min_d).days
    if delta <= 0:
        return min_d
    return min_d + timedelta(days=random.randint(0, delta))


def _random_datetime(min_dt: datetime, max_dt: datetime) -> datetime:
    if max_dt < min_dt:
        min_dt, max_dt = max_dt, min_dt
    delta = max_dt - min_dt
    seconds = int(delta.total_seconds())
    if seconds <= 0:
        return min_dt
    return min_dt + timedelta(seconds=random.randint(0, seconds))


def _max_decimal_places(values: Iterable[Any]) -> int:
    max_dp = 0
    for v in values:
        if _is_missing(v):
            continue
        try:
            s = str(v)
            if "." in s:
                max_dp = max(max_dp, len(s.split(".", 1)[1].rstrip("0")))
        except Exception:
            continue
    return max_dp


# -----------------------------
# Analysis structures
# -----------------------------


class ColumnProfile:
    """
    Lightweight struct (NOT a dataclass).
    Important: this file is loaded via importlib.exec_module() without inserting
    the module into sys.modules; Python 3.13 dataclasses can crash in that setup.
    """

    def __init__(
        self,
        column_name: str,
        inferred_type: str,
        null_percentage: float,
        is_categorical: bool,
        categorical_values: List[Any],
        numeric_min: Optional[float],
        numeric_max: Optional[float],
        date_min: Optional[str],
        date_max: Optional[str],
        pattern_type: Optional[str],
    ) -> None:
        self.column_name = column_name
        self.inferred_type = inferred_type
        self.null_percentage = null_percentage
        self.is_categorical = is_categorical
        self.categorical_values = categorical_values
        self.numeric_min = numeric_min
        self.numeric_max = numeric_max
        self.date_min = date_min
        self.date_max = date_max
        self.pattern_type = pattern_type


class TableProfile:
    def __init__(
        self,
        table_name: str,
        filename: str,
        columns: List[ColumnProfile],
        primary_key: Optional[str],
        foreign_keys: Dict[str, Dict[str, str]],
    ) -> None:
        self.table_name = table_name
        self.filename = filename
        self.columns = columns
        self.primary_key = primary_key
        self.foreign_keys = foreign_keys


# -----------------------------
# Synthesis Engine
# -----------------------------


class DataSynthesisEngine:
    """
    Main entry point used by `main.py`.
    """

    def __init__(self, seed: Optional[int] = None):
        if seed is not None:
            random.seed(seed)
        self._faker = None
        if _HAS_FAKER:
            try:
                self._faker = Faker()
                if seed is not None:
                    # Faker seeding for repeatable generation when desired
                    self._faker.seed_instance(seed)
            except Exception:
                self._faker = None

    def synthesize_database(
        self,
        session_dir: Path,
        filenames: Optional[List[str]],
        relationships: List[Dict[str, Any]],
        num_rows_per_table: Optional[Dict[str, int]] = None,
    ) -> Dict[str, Any]:
        """
        Args:
            session_dir: directory containing the uploaded CSVs for a session
            filenames: list of CSV filenames in that directory
            relationships: list of relationship dicts normalized as:
              child_table.child_fk_col -> parent_table.parent_pk_col
              {source_table, source_column, target_table, target_column, is_foreign_key: True}
            num_rows_per_table: mapping table_name -> number of rows to generate
        Returns:
            dict with keys: schema, synthetic_data, relationships, output_dir
        """
        session_dir = Path(session_dir)
        num_rows_per_table = num_rows_per_table or {}

        # 0) Discover CSVs if caller didn't pass explicit upload list
        if not filenames:
            # Only take top-level CSVs under the session directory.
            # Avoid picking up prior outputs (synthetic_output/*) or any *_synthetic.csv.
            filenames = []
            for p in sorted(Path(session_dir).glob("*.csv")):
                if not p.is_file():
                    continue
                if p.name.lower().endswith("_synthetic.csv"):
                    continue
                filenames.append(p.name)
            if not filenames:
                raise FileNotFoundError(f"No uploaded CSV files found in: {session_dir}")

        # 1) Load dataframes
        dataframes: Dict[str, pd.DataFrame] = {}
        original_rows: Dict[str, set] = {}
        for fn in filenames:
            fp = session_dir / fn
            if not fp.exists():
                raise FileNotFoundError(f"Missing CSV: {fp}")
            table_name = _safe_table_name_from_filename(fn)
            df = pd.read_csv(fp)
            dataframes[table_name] = df
            # For "no exact copy" guard in tables w/o PK
            try:
                original_rows[table_name] = set(tuple(map(_coerce_str, r)) for r in df.astype(object).itertuples(index=False, name=None))
            except Exception:
                original_rows[table_name] = set()

        # 2) Analyze schema + keys
        table_profiles = self._analyze_tables(dataframes, filenames, relationships)

        # 3) Pre-generate PK pools (ensures: new, unique, non-null; also guarantees no original row copies when PK exists)
        pk_pools = self._build_primary_key_pools(table_profiles, dataframes, num_rows_per_table)

        # 4) Generate synthetic rows per table
        synthetic_data: Dict[str, Any] = {}
        for tname, tprof in table_profiles.items():
            nrows = int(num_rows_per_table.get(tname, 100))
            df_in = dataframes[tname]
            syn = self._generate_table(
                tprof=tprof,
                df_in=df_in,
                nrows=nrows,
                pk_pool=pk_pools.get(tname, {}),
                pk_pools_all=pk_pools,
                original_row_set=original_rows.get(tname, set()),
            )
            synthetic_data[tname] = syn

        # 5) Write output CSV files
        out_dir = session_dir / "synthetic_output"
        out_dir.mkdir(parents=True, exist_ok=True)
        output_files = []
        for tname, syn in synthetic_data.items():
            out_fn = f"{tname.replace(' ', '_').lower()}_synthetic.csv"
            out_fp = out_dir / out_fn
            out_df = pd.DataFrame(syn["rows"], columns=syn["columns"])
            out_df.to_csv(out_fp, index=False)
            output_files.append(str(out_fp))

        # 6) Shape schema for the frontend UI (`db-synthesis.js`)
        schema_for_ui = {}
        for tname, tprof in table_profiles.items():
            schema_for_ui[tname] = {
                "table_name": tname,
                "primary_key": tprof.primary_key,
                "foreign_keys": tprof.foreign_keys,
                "columns": [
                    {
                        "column_name": c.column_name,
                        "data_type": c.inferred_type,
                        "null_percentage": c.null_percentage,
                        "is_categorical": c.is_categorical,
                        "categorical_values": c.categorical_values[:50],
                        "min": c.numeric_min,
                        "max": c.numeric_max,
                        "date_min": c.date_min,
                        "date_max": c.date_max,
                        "pattern_type": c.pattern_type,
                    }
                    for c in tprof.columns
                ],
            }

        # Also attach the (normalized) FK relationships for UI display
        relationships_out = []
        for r in relationships:
            if not r.get("is_foreign_key", False):
                continue
            relationships_out.append(
                {
                    "source_table": r["source_table"],      # child
                    "source_column": r["source_column"],    # fk
                    "target_table": r["target_table"],      # parent
                    "target_column": r["target_column"],    # pk
                    "relationship_type": r.get("relationship_type", "foreign_key"),
                    "is_foreign_key": True,
                    "confidence": float(r.get("confidence", 0.0)),
                }
            )

        return {
            "schema": schema_for_ui,
            "synthetic_data": synthetic_data,
            "relationships": relationships_out,
            "output_dir": str(out_dir),
            "output_files": output_files,
        }

    # -----------------------------
    # Analysis
    # -----------------------------

    def _analyze_tables(
        self,
        dataframes: Dict[str, pd.DataFrame],
        filenames: List[str],
        relationships: List[Dict[str, Any]],
    ) -> Dict[str, TableProfile]:
        filename_by_table = { _safe_table_name_from_filename(fn): fn for fn in filenames }

        # FK map: child -> {fk_col: (parent_table, parent_col)}
        fk_map: Dict[str, Dict[str, Tuple[str, str]]] = {}
        inbound_refs: Dict[str, Dict[str, int]] = {}  # parent_table -> {parent_col: count}
        for r in relationships:
            if not r.get("is_foreign_key", False):
                continue
            child = r["source_table"]
            fk_col = r["source_column"]
            parent = r["target_table"]
            pk_col = r["target_column"]
            fk_map.setdefault(child, {})[fk_col] = (parent, pk_col)
            inbound_refs.setdefault(parent, {})
            inbound_refs[parent][pk_col] = inbound_refs[parent].get(pk_col, 0) + 1

        profiles: Dict[str, TableProfile] = {}

        for tname, df in dataframes.items():
            cols: List[ColumnProfile] = []
            for col in df.columns:
                s = df[col]
                inferred = _infer_type(s)
                null_pct = float(s.isna().mean() * 100.0)

                # Categorical detection
                is_cat = _is_categorical(s, inferred)
                cat_vals: List[Any] = []
                if is_cat:
                    # Keep original values (including numeric categories)
                    non_null = s.dropna()
                    # Use stable ordering by frequency, then value
                    try:
                        vc = non_null.value_counts().head(200)
                        cat_vals = vc.index.tolist()
                    except Exception:
                        cat_vals = list(non_null.unique())[:200]

                num_min = num_max = None
                dt_min = dt_max = None
                pattern_type = None

                if inferred in ("integer", "float"):
                    coerced = pd.to_numeric(s, errors="coerce")
                    if coerced.notna().any():
                        num_min = float(coerced.min())
                        num_max = float(coerced.max())
                elif inferred in ("date", "timestamp"):
                    parsed, _ = _try_parse_datetime_series(s.astype(str))
                    if parsed.notna().any():
                        mn = parsed.min()
                        mx = parsed.max()
                        if inferred == "date":
                            dt_min = mn.date().isoformat()
                            dt_max = mx.date().isoformat()
                        else:
                            # keep seconds precision ISO-like
                            dt_min = mn.to_pydatetime().replace(microsecond=0).isoformat(sep=" ")
                            dt_max = mx.to_pydatetime().replace(microsecond=0).isoformat(sep=" ")
                else:
                    # String patterns
                    non_null = s.dropna().astype(str)
                    sample_vals = non_null.head(50).tolist()
                    pattern_type = _detect_string_pattern(sample_vals, col)

                cols.append(
                    ColumnProfile(
                        column_name=str(col),
                        inferred_type=inferred,
                        null_percentage=null_pct,
                        is_categorical=is_cat,
                        categorical_values=cat_vals,
                        numeric_min=num_min,
                        numeric_max=num_max,
                        date_min=dt_min,
                        date_max=dt_max,
                        pattern_type=pattern_type,
                    )
                )

            # Primary key selection:
            # - If inbound relationships indicate this table has a referenced PK column, prefer that column
            # - Else use heuristic: uniqueness + non-null + name contains id
            preferred_pk = None
            if tname in inbound_refs and inbound_refs[tname]:
                # Pick most-referenced; tie-break by uniqueness
                candidate_cols = sorted(inbound_refs[tname].items(), key=lambda kv: kv[1], reverse=True)
                best_col = candidate_cols[0][0]
                if best_col in df.columns:
                    preferred_pk = best_col

            primary_key = preferred_pk or self._infer_primary_key(df)

            foreign_keys: Dict[str, Dict[str, str]] = {}
            for fk_col, (pt, pc) in fk_map.get(tname, {}).items():
                foreign_keys[fk_col] = {"parent_table": pt, "parent_column": pc}

            profiles[tname] = TableProfile(
                table_name=tname,
                filename=filename_by_table.get(tname, f"{tname}.csv"),
                columns=cols,
                primary_key=primary_key,
                foreign_keys=foreign_keys,
            )

        return profiles

    def _infer_primary_key(self, df: pd.DataFrame) -> Optional[str]:
        if df.empty:
            return None
        best = None
        best_score = -1.0
        n = len(df)
        for col in df.columns:
            s = df[col]
            non_null = s.dropna()
            if non_null.empty:
                continue
            # uniqueness ratio
            try:
                uniq = float(non_null.nunique(dropna=True)) / max(1.0, float(len(non_null)))
            except Exception:
                continue
            null_rate = float(s.isna().mean())
            name = str(col).lower()
            name_bonus = 0.0
            if name == "id" or name.endswith("_id") or "id" in name:
                name_bonus += 0.2
            # Prefer mostly-unique and mostly-non-null
            score = (uniq * 0.7) + ((1.0 - null_rate) * 0.2) + name_bonus
            # Require "reasonably unique"
            if uniq < 0.85:
                continue
            if score > best_score:
                best_score = score
                best = str(col)
        return best

    # -----------------------------
    # Key pools
    # -----------------------------

    def _build_primary_key_pools(
        self,
        profiles: Dict[str, TableProfile],
        dataframes: Dict[str, pd.DataFrame],
        num_rows_per_table: Dict[str, int],
    ) -> Dict[str, Dict[str, List[Any]]]:
        """
        Returns: pk_pools[table_name][pk_col] = list of unique values (length = nrows).
        We generate pools for:
        - The chosen `primary_key` column
        - Any column referenced as a parent PK by some relationship (if present in DF)
        """
        referenced_parent_cols: Dict[str, set] = {}
        for tname, tprof in profiles.items():
            for fk_col, mapping in tprof.foreign_keys.items():
                parent_t = mapping["parent_table"]
                parent_c = mapping["parent_column"]
                referenced_parent_cols.setdefault(parent_t, set()).add(parent_c)

        pk_pools: Dict[str, Dict[str, List[Any]]] = {}
        for tname, tprof in profiles.items():
            df = dataframes[tname]
            nrows = int(num_rows_per_table.get(tname, 100))
            pools_for_table: Dict[str, List[Any]] = {}

            needed_cols = set()
            if tprof.primary_key:
                needed_cols.add(tprof.primary_key)
            for pc in referenced_parent_cols.get(tname, set()):
                needed_cols.add(pc)

            for pk_col in needed_cols:
                if pk_col not in df.columns:
                    continue
                col_profile = next((c for c in tprof.columns if c.column_name == pk_col), None)
                inferred = col_profile.inferred_type if col_profile else _infer_type(df[pk_col])

                orig_vals = set(_coerce_str(v) for v in df[pk_col].dropna().astype(str).tolist())
                pools_for_table[pk_col] = self._generate_unique_pk_values(
                    inferred_type=inferred,
                    n=nrows,
                    original_values=orig_vals,
                    col_name=pk_col,
                    df_series=df[pk_col],
                )
            pk_pools[tname] = pools_for_table

        return pk_pools

    def _generate_unique_pk_values(
        self,
        inferred_type: str,
        n: int,
        original_values: set,
        col_name: str,
        df_series: pd.Series,
    ) -> List[Any]:
        """
        Generate new unique, non-null PK values not present in the original column.
        """
        out: List[Any] = []
        used: set = set()

        # For numeric PKs, generate beyond observed max to avoid collisions.
        if inferred_type in ("integer", "float"):
            coerced = pd.to_numeric(df_series, errors="coerce")
            mx = float(coerced.max()) if coerced.notna().any() else 0.0
            start = int(mx) + 10_000
            for i in range(n):
                v = start + i
                # Ensure not present (string compare to cover mixed types)
                sv = str(v)
                while sv in original_values or sv in used:
                    v += 1
                    sv = str(v)
                used.add(sv)
                out.append(int(v) if inferred_type == "integer" else float(v))
            return out

        # For date/timestamp PKs (rare): use timestamp-like UUID suffix to ensure uniqueness.
        if inferred_type in ("date", "timestamp"):
            base = datetime(2020, 1, 1)
            for i in range(n):
                v = (base + timedelta(seconds=i)).isoformat(sep=" ") if inferred_type == "timestamp" else (base.date() + timedelta(days=i)).isoformat()
                sv = str(v)
                while sv in original_values or sv in used:
                    i += 1
                    v = (base + timedelta(seconds=i)).isoformat(sep=" ") if inferred_type == "timestamp" else (base.date() + timedelta(days=i)).isoformat()
                    sv = str(v)
                used.add(sv)
                out.append(v)
            return out

        # Strings: produce deterministic-looking IDs based on column name.
        prefix = re.sub(r"[^a-z0-9]+", "", col_name.lower())[:6] or "id"
        for _ in range(n):
            while True:
                v = f"{prefix}_{uuid.uuid4().hex[:10]}"
                if v not in original_values and v not in used:
                    used.add(v)
                    out.append(v)
                    break
        return out

    # -----------------------------
    # Generation
    # -----------------------------

    def _generate_table(
        self,
        tprof: TableProfile,
        df_in: pd.DataFrame,
        nrows: int,
        pk_pool: Dict[str, List[Any]],
        pk_pools_all: Dict[str, Dict[str, List[Any]]],
        original_row_set: set,
    ) -> Dict[str, Any]:
        columns = list(df_in.columns)
        col_profiles = {c.column_name: c for c in tprof.columns}

        # For floats, preserve max decimal places observed
        float_dp_by_col: Dict[str, int] = {}
        for c in tprof.columns:
            if c.inferred_type == "float":
                float_dp_by_col[c.column_name] = _max_decimal_places(df_in[c.column_name].dropna().head(500).tolist())

        # FK sampling pools
        fk_sources: Dict[str, List[Any]] = {}
        for fk_col, mapping in tprof.foreign_keys.items():
            parent_t = mapping["parent_table"]
            parent_c = mapping["parent_column"]
            parent_pool = pk_pools_all.get(parent_t, {}).get(parent_c)
            if not parent_pool:
                # Fallback: build from parent df unique values (not ideal, but avoids null/broken refs)
                parent_df = None
                # parent_df may not exist if detector mismatched table name; ignore
                if parent_t in pk_pools_all:
                    parent_df = None
                if parent_t in pk_pools_all and parent_df is None:
                    pass
            fk_sources[fk_col] = parent_pool or []

        rows: List[List[Any]] = []
        attempts_limit = 30

        for i in range(nrows):
            # Try regeneration if we accidentally match an original row (primarily for tables without a PK).
            for attempt in range(attempts_limit):
                row: List[Any] = []
                for col in columns:
                    # PK
                    if tprof.primary_key and col == tprof.primary_key and col in pk_pool:
                        val = pk_pool[col][i]
                        row.append(val)
                        continue
                    # FK
                    if col in tprof.foreign_keys:
                        parent_vals = fk_sources.get(col) or []
                        if not parent_vals:
                            # Last resort: generate a non-null placeholder that is still stable-ish
                            row.append(f"fk_{uuid.uuid4().hex[:10]}")
                        else:
                            v = random.choice(parent_vals)
                            # Try to match the child column's inferred type
                            cprof = col_profiles.get(col)
                            if cprof:
                                if cprof.inferred_type == "string":
                                    v = str(v)
                                elif cprof.inferred_type == "integer":
                                    try:
                                        v = int(v)
                                    except Exception:
                                        pass
                                elif cprof.inferred_type == "float":
                                    try:
                                        v = float(v)
                                    except Exception:
                                        pass
                            row.append(v)
                        continue

                    cprof = col_profiles.get(col)
                    if cprof is None:
                        # Unknown column: generate generic string
                        row.append(self._gen_generic_string(df_in[col]))
                        continue

                    # Preserve null distribution (except key columns already handled)
                    if cprof.null_percentage > 0 and random.random() < (cprof.null_percentage / 100.0):
                        row.append(None)
                        continue

                    if cprof.is_categorical and cprof.categorical_values:
                        row.append(random.choice(cprof.categorical_values))
                        continue

                    if cprof.inferred_type == "integer":
                        row.append(self._gen_int(cprof))
                    elif cprof.inferred_type == "float":
                        dp = float_dp_by_col.get(col, 2)
                        row.append(self._gen_float(cprof, dp))
                    elif cprof.inferred_type == "date":
                        row.append(self._gen_date(cprof))
                    elif cprof.inferred_type == "timestamp":
                        row.append(self._gen_timestamp(cprof))
                    else:
                        row.append(self._gen_string(cprof, df_in[col]))

                # Guard: do not copy any original row exactly
                if tprof.primary_key:
                    # PK is guaranteed new => cannot match any original row exactly
                    rows.append(row)
                    break

                # No PK: must check whole row equality
                if original_row_set:
                    row_key = tuple(map(_coerce_str, row))
                    if row_key in original_row_set:
                        continue
                rows.append(row)
                break

        return {
            "columns": [str(c) for c in columns],
            "rows": rows,
            "row_count": int(nrows),
        }

    # -----------------------------
    # Value generators
    # -----------------------------

    def _gen_int(self, cprof: ColumnProfile) -> int:
        mn = int(cprof.numeric_min) if cprof.numeric_min is not None else 0
        mx = int(cprof.numeric_max) if cprof.numeric_max is not None else max(10, mn + 10)
        if mx < mn:
            mn, mx = mx, mn
        return int(random.randint(mn, mx))

    def _gen_float(self, cprof: ColumnProfile, decimal_places: int = 2) -> float:
        mn = float(cprof.numeric_min) if cprof.numeric_min is not None else 0.0
        mx = float(cprof.numeric_max) if cprof.numeric_max is not None else mn + 100.0
        if mx < mn:
            mn, mx = mx, mn
        v = random.uniform(mn, mx)
        if decimal_places is None:
            return float(v)
        return float(round(v, int(decimal_places)))

    def _gen_date(self, cprof: ColumnProfile) -> str:
        if cprof.date_min and cprof.date_max:
            mn = datetime.fromisoformat(cprof.date_min).date()
            mx = datetime.fromisoformat(cprof.date_max).date()
            return _random_date(mn, mx).isoformat()
        # fallback: recent dates
        return _random_date(date(2018, 1, 1), date(2025, 12, 31)).isoformat()

    def _gen_timestamp(self, cprof: ColumnProfile) -> str:
        def _parse_ts(s: str) -> datetime:
            # fromisoformat accepts "YYYY-MM-DD HH:MM:SS"
            return datetime.fromisoformat(s.replace("T", " ").replace("Z", ""))

        if cprof.date_min and cprof.date_max:
            mn = _parse_ts(cprof.date_min)
            mx = _parse_ts(cprof.date_max)
            return _random_datetime(mn, mx).replace(microsecond=0).isoformat(sep=" ")
        return _random_datetime(datetime(2018, 1, 1), datetime(2025, 12, 31, 23, 59, 59)).replace(microsecond=0).isoformat(sep=" ")

    def _gen_string(self, cprof: ColumnProfile, s_in: pd.Series) -> str:
        p = cprof.pattern_type
        if p == "username":
            return self._gen_username()
        if p == "email":
            return self._gen_email()
        if p == "phone_number":
            return self._gen_phone()
        if p == "account_number":
            return self._gen_account_number(s_in)
        if p == "name":
            return self._gen_name()
        if p == "uuid":
            return str(uuid.uuid4())
        if p == "code":
            return self._gen_code_like(s_in)
        # generic
        return self._gen_generic_string(s_in)

    def _gen_email(self) -> str:
        if self._faker:
            try:
                return self._faker.email()
            except Exception:
                pass
        user = uuid.uuid4().hex[:8]
        domain = random.choice(["example.com", "sample.org", "synthetic.net", "demo.io"])
        return f"{user}@{domain}"

    def _gen_phone(self) -> str:
        if self._faker:
            try:
                # Normalize to a compact digits-ish string where possible
                v = str(self._faker.phone_number())
                v = re.sub(r"\s+", "", v)
                return v[:20]
            except Exception:
                pass
        # Simple E.164-ish
        country = random.choice(["+1", "+44", "+91", "+61"])
        num = "".join(str(random.randint(0, 9)) for _ in range(10))
        return f"{country}{num}"

    def _gen_name(self) -> str:
        if self._faker:
            try:
                return self._faker.name()
            except Exception:
                pass
        first = random.choice(["Alex", "Sam", "Jordan", "Taylor", "Casey", "Riley", "Morgan", "Avery", "Jamie", "Robin"])
        last = random.choice(["Patel", "Smith", "Johnson", "Lee", "Brown", "Garcia", "Nguyen", "Khan", "Chen", "Singh"])
        return f"{first} {last}"

    def _gen_username(self) -> str:
        if self._faker:
            try:
                # Faker usernames can contain '.', keep it simple for CSVs
                return re.sub(r"[^a-zA-Z0-9_]+", "_", self._faker.user_name())[:24]
            except Exception:
                pass
        return f"user_{uuid.uuid4().hex[:8]}"

    def _gen_account_number(self, s_in: pd.Series) -> str:
        # Match digit length distribution if possible
        non_null = s_in.dropna().astype(str)
        lens = [len(v) for v in non_null.head(200).tolist() if _ONLY_DIGITS_RE.match(v)]
        target_len = random.choice(lens) if lens else random.choice([8, 10, 12, 16])
        return "".join(str(random.randint(0, 9)) for _ in range(target_len))

    def _gen_code_like(self, s_in: pd.Series) -> str:
        non_null = s_in.dropna().astype(str)
        sample = non_null.head(200).tolist()
        lengths = [len(v) for v in sample if v]
        target_len = max(6, min(24, int(random.choice(lengths)) if lengths else 10))
        alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
        return "".join(random.choice(alphabet) for _ in range(target_len))

    def _gen_generic_string(self, s_in: pd.Series) -> str:
        non_null = s_in.dropna().astype(str)
        sample = non_null.head(200).tolist()
        lengths = [len(v) for v in sample if v]
        target_len = max(4, min(40, int(random.choice(lengths)) if lengths else 12))
        alphabet = "abcdefghijklmnopqrstuvwxyz"
        # make it word-like
        out = []
        while len("".join(out)) < target_len:
            wlen = random.randint(3, 8)
            word = "".join(random.choice(alphabet) for _ in range(wlen))
            out.append(word)
        s = " ".join(out)
        return s[:target_len].strip()


