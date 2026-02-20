"""
Banking Business Rules (Without KYC)

This module inspects the *user's uploaded CSV data* and produces a clean,
UI-friendly set of rule checks + one-line explanations.

Design goals:
- No hardcoded table names; detect relevant columns by name patterns.
- Prefer "UNKNOWN" over guessing when data needed for a rule is missing.
- Output is compact: one-line explanation, plus observed columns + small stats.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import re

import pandas as pd


RuleStatus = str  # "PASS" | "FAIL" | "UNKNOWN"


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (s or "").strip().lower()).strip("_")


def _is_nullish(x: Any) -> bool:
    try:
        return x is None or (isinstance(x, float) and pd.isna(x)) or str(x).strip() == ""
    except Exception:
        return x is None


def _find_columns(df: pd.DataFrame, keywords: List[str], require_all: bool = False) -> List[str]:
    """
    Find columns where normalized name contains any/all keyword tokens.
    Example keywords: ["account", "status"].
    """
    out: List[str] = []
    for c in df.columns:
        cn = _norm(str(c))
        if require_all:
            ok = all(k in cn for k in keywords)
        else:
            ok = any(k in cn for k in keywords)
        if ok:
            out.append(str(c))
    return out


def _pick_best(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """Pick a 'best' column from candidates: prefer fewer nulls, then more uniques."""
    if not candidates:
        return None
    best = None
    best_score = None
    for c in candidates:
        try:
            s = df[c]
            nulls = float(s.isna().mean())
            uniq = int(s.nunique(dropna=True))
            score = (nulls, -uniq)  # minimize nulls, maximize uniqueness
        except Exception:
            score = (1.0, 0)
        if best_score is None or score < best_score:
            best_score = score
            best = c
    return best


def _to_float_series(s: pd.Series) -> pd.Series:
    # tolerant parse: remove commas and currency symbols
    try:
        s2 = s.astype(str).str.replace(",", "", regex=False)
        s2 = s2.str.replace("₹", "", regex=False).str.replace("$", "", regex=False)
        return pd.to_numeric(s2, errors="coerce")
    except Exception:
        return pd.to_numeric(s, errors="coerce")


def _sample_stats_numeric(df: pd.DataFrame, col: str) -> Dict[str, Any]:
    try:
        ser = _to_float_series(df[col]).dropna()
        if ser.empty:
            return {"min": None, "max": None}
        return {"min": float(ser.min()), "max": float(ser.max())}
    except Exception:
        return {"min": None, "max": None}


def _sample_stats_categorical(df: pd.DataFrame, col: str, top_k: int = 5) -> Dict[str, Any]:
    try:
        ser = df[col].dropna().astype(str).map(lambda x: x.strip()).replace("", pd.NA).dropna()
        if ser.empty:
            return {"top_values": []}
        vc = ser.value_counts().head(top_k)
        return {"top_values": [{"value": str(k), "count": int(v)} for k, v in vc.items()]}
    except Exception:
        return {"top_values": []}


def _explain_columns(table_cols: List[Tuple[str, str]]) -> List[str]:
    """
    Convert [(table, col)] into stable 'Table.Column' strings.
    """
    out = []
    for t, c in table_cols:
        if not t:
            out.append(c)
        else:
            out.append(f"{t}.{c}")
    # unique preserve order
    seen = set()
    uniq = []
    for x in out:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq


@dataclass
class RuleResult:
    rule_id: str
    title: str
    importance: int  # lower = more important (shown first)
    status: RuleStatus
    # Banking-focused explanations (requested format)
    rule: str
    observation: str
    impact: str
    observed_columns: List[str]
    observed_pattern: Dict[str, Any]
    applicable: bool  # show in UI only when columns+data exist for this file


def _row_examples(
    df: pd.DataFrame,
    mask: pd.Series,
    columns: List[str],
    limit: int = 3,
) -> List[Dict[str, Any]]:
    """
    Return a few row examples for a failing mask with 1-based row numbers.
    Keeps payload small for UI.
    """
    try:
        if mask is None or len(mask) != len(df):
            return []
        cols = [c for c in columns if c in df.columns]
        if not cols:
            return []
        idxs = df.index[mask.fillna(False)].tolist()[: max(0, int(limit))]
        out: List[Dict[str, Any]] = []
        for i in idxs:
            row = df.loc[i, cols]
            # pandas can return Series; convert to plain dict of strings
            vals: Dict[str, Any] = {}
            for c in cols:
                v = row[c] if isinstance(row, dict) else row.get(c) if hasattr(row, "get") else None
                if _is_nullish(v):
                    vals[c] = ""
                else:
                    s = str(v)
                    vals[c] = s[:200] + ("…" if len(s) > 200 else "")
            out.append({"row_number": int(i) + 1 if isinstance(i, int) else str(i), "values": vals})
        return out
    except Exception:
        return []


def _table_rule_results(
    table_name: str,
    df: pd.DataFrame,
    daily_limit_amount: float,
    min_balance_amount: float,
) -> List[RuleResult]:
    """
    Evaluate rules within ONE file/table only.
    This matches the requirement: "each file, the column and data row observed".
    """
    rules: List[RuleResult] = []

    # 1) Account Status Rule
    status_candidates = _find_columns(df, ["account", "status"], require_all=True) or _find_columns(df, ["status"], require_all=False)
    status_col = _pick_best(df, status_candidates)
    if status_col:
        stats = _sample_stats_categorical(df, status_col)
        ser = df[status_col].astype(str).str.strip().str.lower()
        bad = ser.str.contains("closed") | ser.str.contains("frozen") | ser.str.contains("blocked") | ser.str.contains("suspend")
        active = ser.str.contains("active")
        if int(bad.sum()) > 0:
            st = "FAIL"
            rule_txt = "The account must be ACTIVE. If the account is CLOSED/FROZEN/BLOCKED, no transaction is allowed."
            obs = f"Column `{status_col}` contains {int(bad.sum())} row(s) with CLOSED/FROZEN/BLOCKED/SUSPEND-like values."
            imp = "Transactions against non-active accounts can cause policy breaches and operational risk (blocked accounts should not transact)."
        elif int(active.sum()) > 0:
            st = "PASS"
            rule_txt = "The account must be ACTIVE. If the account is CLOSED/FROZEN/BLOCKED, no transaction is allowed."
            obs = f"Column `{status_col}` contains ACTIVE-like values and no CLOSED/FROZEN/BLOCKED values were detected in the sampled data."
            imp = "Status-based controls can be applied consistently to stop transactions on inactive/blocked accounts."
        else:
            # Not conclusively pass/fail from observed values -> do not show
            st = "UNKNOWN"
            rule_txt = "The account must be ACTIVE. If the account is CLOSED/FROZEN/BLOCKED, no transaction is allowed."
            obs = f"Column `{status_col}` exists but values do not clearly map to ACTIVE/CLOSED/FROZEN/BLOCKED."
            imp = "If account status cannot be interpreted, transactions may be incorrectly allowed or blocked."
        rules.append(
            RuleResult(
                rule_id="account_status",
                title="Account Status Rule (ACTIVE required)",
                importance=1,
                status=st,
                rule=rule_txt,
                observation=obs,
                impact=imp,
                observed_columns=_explain_columns([(table_name, status_col)]),
                observed_pattern={
                    "top_status_values": stats.get("top_values", []),
                    "bad_status_row_examples": _row_examples(df, bad, [status_col]),
                },
                applicable=(st in ("PASS", "FAIL")),
            )
        )
    else:
        rules.append(
            RuleResult(
                rule_id="account_status",
                title="Account Status Rule (ACTIVE required)",
                importance=1,
                status="UNKNOWN",
                rule="The account must be ACTIVE. If the account is CLOSED/FROZEN/BLOCKED, no transaction is allowed.",
                observation="No account status column was detected in this file.",
                impact="Without a status field, blocked/frozen accounts may not be prevented from transacting based on file-level validation.",
                observed_columns=[],
                observed_pattern={"note": "Add a status column to validate ACTIVE vs CLOSED/FROZEN/BLOCKED."},
                applicable=False,
            )
        )

    # 2) Authentication Rule (OTP/password/auth/login columns)
    auth_cols = (
        _find_columns(df, ["otp"], False)
        + _find_columns(df, ["password"], False)
        + _find_columns(df, ["auth"], False)
        + _find_columns(df, ["login", "status"], True)
    )
    auth_cols = list(dict.fromkeys(auth_cols))  # unique preserve order
    if auth_cols:
        # Only show when we observe actual credential-like values (not 100% empty)
        non_empty = 0
        total = 0
        for c in auth_cols[:3]:
            if c in df.columns:
                s = df[c]
                total += len(s)
                try:
                    non_empty += int((~s.isna() & (s.astype(str).str.strip() != "")).sum())
                except Exception:
                    pass
        has_values = non_empty > 0
        st = "PASS" if has_values else "UNKNOWN"
        rules.append(
            RuleResult(
                rule_id="authentication",
                title="Authentication Rule (User ID + Password/OTP)",
                importance=2,
                status=st,
                rule="The user must provide the correct User ID and Password/OTP. If credentials are wrong, access is denied.",
                observation=(
                    f"Authentication-related columns were detected ({', '.join(auth_cols[:5])}). "
                    + (f"Observed {non_empty} non-empty credential-like value(s) in these columns." if has_values else "However, the columns appear empty in the observed rows.")
                ),
                impact="Weak or missing authentication controls increase unauthorized access risk and fraud exposure.",
                observed_columns=_explain_columns([(table_name, c) for c in auth_cols[:8]]),
                observed_pattern={"columns_detected": auth_cols[:12]},
                applicable=(st in ("PASS", "FAIL")),
            )
        )
    else:
        rules.append(
            RuleResult(
                rule_id="authentication",
                title="Authentication Rule (User ID + Password/OTP)",
                importance=2,
                status="UNKNOWN",
                rule="The user must provide the correct User ID and Password/OTP. If credentials are wrong, access is denied.",
                observation="No OTP/password/auth/login-status columns found in this file.",
                impact="This file alone cannot prove authentication controls; login/auth data may be in a separate file.",
                observed_columns=[],
                observed_pattern={"note": "If this is a transactions file, auth may be stored in a separate login file."},
                applicable=False,
            )
        )

    # 3) Sufficient Balance Rule (withdraw <= balance) within same file
    balance_col = _pick_best(df, _find_columns(df, ["balance"], require_all=False))
    amount_col = _pick_best(
        df,
        _find_columns(df, ["withdraw", "amount"], True)
        or _find_columns(df, ["debit", "amount"], True)
        or _find_columns(df, ["amount"], False),
    )
    if balance_col and amount_col:
        b = _to_float_series(df[balance_col])
        a = _to_float_series(df[amount_col])
        mask = b.notna() & a.notna()
        if int(mask.sum()) == 0:
            st = "UNKNOWN"
            rule_txt = "The withdrawal amount must be less than or equal to the available balance. If not, reject with “Insufficient Funds”."
            obs = f"Columns `{balance_col}` and `{amount_col}` exist, but no rows have both values populated to validate."
            imp = "If balance vs amount cannot be checked, overdrafts or unauthorized negative balances may occur."
            violations = 0
            bad_mask = pd.Series([False] * len(df))
        else:
            # Heuristic: treat positive amount as a withdrawal candidate
            bad_mask = (a > b) & (a > 0) & mask
            violations = int(bad_mask.sum())
            st = "FAIL" if violations else "PASS"
            rule_txt = "The withdrawal amount must be less than or equal to the available balance. If not, reject with “Insufficient Funds”."
            obs = (
                f"Compared `{amount_col}` to `{balance_col}` on {int(mask.sum())} row(s) where both exist; "
                + (f"found {violations} violation(s) where amount > balance." if violations else "found 0 violations (amount never exceeded balance).")
            )
            imp = "Allowing withdrawals above available balance causes overdrafts, losses, and customer disputes."
        rules.append(
            RuleResult(
                rule_id="sufficient_balance",
                title="Sufficient Balance Rule (withdraw <= available balance)",
                importance=3,
                status=st,
                rule=rule_txt,
                observation=obs,
                impact=imp,
                observed_columns=_explain_columns([(table_name, balance_col), (table_name, amount_col)]),
                observed_pattern={
                    "balance_range": _sample_stats_numeric(df, balance_col),
                    "amount_range": _sample_stats_numeric(df, amount_col),
                    "violations": violations,
                    "violation_row_examples": _row_examples(df, bad_mask, [balance_col, amount_col]),
                },
                applicable=(st in ("PASS", "FAIL")),
            )
        )
    else:
        rules.append(
            RuleResult(
                rule_id="sufficient_balance",
                title="Sufficient Balance Rule (withdraw <= available balance)",
                importance=3,
                status="UNKNOWN",
                rule="The withdrawal amount must be less than or equal to the available balance. If not, reject with “Insufficient Funds”.",
                observation="Balance and/or withdrawal amount columns were not detected in this file.",
                impact="Without balance and amount in the same file, insufficient-funds validation cannot be demonstrated here.",
                observed_columns=_explain_columns([(table_name, c) for c in [balance_col, amount_col] if c]),
                observed_pattern={"note": "Need both balance and withdrawal/debit amount in the same file to compare."},
                applicable=False,
            )
        )

    # 4) Daily Transaction Limit Rule within same file
    date_col = _pick_best(df, _find_columns(df, ["timestamp"], False) or _find_columns(df, ["date"], False) or _find_columns(df, ["time"], False))
    user_col = _pick_best(
        df,
        _find_columns(df, ["user", "id"], True)
        or _find_columns(df, ["customer", "id"], True)
        or _find_columns(df, ["account", "id"], True),
    )
    if date_col and amount_col and user_col:
        work = df[[user_col, date_col, amount_col]].copy()
        work["_amt"] = _to_float_series(work[amount_col]).fillna(0.0)
        try:
            work["_dt"] = pd.to_datetime(work[date_col], errors="coerce")
        except Exception:
            work["_dt"] = pd.to_datetime(work[date_col].astype(str), errors="coerce")
        work["_day"] = work["_dt"].dt.date
        ok = work[user_col].notna() & work["_day"].notna()
        if int(ok.sum()) == 0:
            st = "UNKNOWN"
            rule_txt = f"Transactions must be within the daily allowed limit (example: ≤ {daily_limit_amount:.0f} per day). If exceeded, reject."
            obs = f"Columns `{user_col}`, `{date_col}`, `{amount_col}` exist, but the date/time values could not be parsed reliably to compute daily totals."
            imp = "If daily limits cannot be computed, customers may exceed allowed limits, increasing fraud and compliance risk."
            max_daily = None
            worst_key = None
        else:
            daily = work.loc[ok].groupby([user_col, "_day"], dropna=True)["_amt"].sum()
            if len(daily) == 0:
                max_daily = 0.0
                worst_key = None
                st = "PASS"
                rule_txt = f"Transactions must be within the daily allowed limit (example: ≤ {daily_limit_amount:.0f} per day). If exceeded, reject."
                obs = "No daily totals could be computed after filtering to valid user+date rows; treated as within limit."
                imp = "Daily limit checks help reduce large rapid outflows and limit fraud exposure."
            else:
                worst_key = daily.idxmax()
                max_daily = float(daily.max())
                st = "FAIL" if max_daily > daily_limit_amount else "PASS"
                rule_txt = f"Transactions must be within the daily allowed limit (example: ≤ {daily_limit_amount:.0f} per day). If exceeded, reject."
                obs = f"Computed daily totals per `{user_col}`; maximum daily total observed is {max_daily:.2f}."
                imp = "If daily limits are exceeded and still allowed, it increases loss exposure and may breach product limits."
        # show a few rows for the worst user/day if we have it
        worst_rows_mask = pd.Series([False] * len(work))
        if worst_key is not None:
            try:
                w_user, w_day = worst_key
                worst_rows_mask = (work[user_col] == w_user) & (work["_day"] == w_day)
            except Exception:
                pass
        rules.append(
            RuleResult(
                rule_id="daily_limit",
                title=f"Daily Transaction Limit Rule (≤ {daily_limit_amount:.0f}/day)",
                importance=4,
                status=st,
                rule=rule_txt,
                observation=obs,
                impact=imp,
                observed_columns=_explain_columns([(table_name, user_col), (table_name, date_col), (table_name, amount_col)]),
                observed_pattern={
                    "max_daily_total": max_daily,
                    "worst_user_day_examples": _row_examples(work, worst_rows_mask, [user_col, date_col, amount_col]),
                },
                applicable=(st in ("PASS", "FAIL")),
            )
        )
    else:
        rules.append(
            RuleResult(
                rule_id="daily_limit",
                title=f"Daily Transaction Limit Rule (≤ {daily_limit_amount:.0f}/day)",
                importance=4,
                status="UNKNOWN",
                rule=f"Transactions must be within the daily allowed limit (example: ≤ {daily_limit_amount:.0f} per day). If exceeded, reject.",
                observation="User + date/time + amount columns were not all detected in this file, so daily totals cannot be computed here.",
                impact="Without daily totals, excessive daily volume may not be detected and blocked at the file-validation layer.",
                observed_columns=_explain_columns([(table_name, c) for c in [user_col, date_col, amount_col] if c]),
                observed_pattern={"note": "If amounts are in another file, daily limit must be computed there."},
                applicable=False,
            )
        )

    # 5) Valid Transaction Rule (amount > 0, type credit/debit)
    txn_type_col = _pick_best(df, _find_columns(df, ["transaction", "type"], True) or _find_columns(df, ["txn", "type"], True) or _find_columns(df, ["type"], False))
    if amount_col and txn_type_col:
        amt = _to_float_series(df[amount_col])
        typ = df[txn_type_col].astype(str).str.lower().fillna("")
        valid_type = typ.str.contains("credit") | typ.str.contains("debit") | typ.str.contains(r"\bcr\b") | typ.str.contains(r"\bdr\b")
        valid_amt = amt.notna() & (amt > 0)
        bad_mask = (~valid_amt | ~valid_type) & (amt.notna() | (typ != ""))
        bad_count = int(bad_mask.sum())
        st = "FAIL" if bad_count else "PASS"
        rule_txt = "The transaction amount must be > 0 and the transaction type must be valid (credit or debit)."
        obs = f"Validated `{amount_col}` and `{txn_type_col}`; found {bad_count} row(s) with invalid amount or invalid type."
        imp = "Invalid transactions (negative/zero/unknown type) can break balances, reporting, and downstream reconciliation."
        rules.append(
            RuleResult(
                rule_id="valid_transaction",
                title="Valid Transaction Rule (amount > 0, type is credit/debit)",
                importance=5,
                status=st,
                rule=rule_txt,
                observation=obs,
                impact=imp,
                observed_columns=_explain_columns([(table_name, amount_col), (table_name, txn_type_col)]),
                observed_pattern={
                    "amount_range": _sample_stats_numeric(df, amount_col),
                    "type_top_values": _sample_stats_categorical(df, txn_type_col).get("top_values", []),
                    "violations": bad_count,
                    "violation_row_examples": _row_examples(df, bad_mask, [amount_col, txn_type_col]),
                },
                applicable=True,
            )
        )
    else:
        rules.append(
            RuleResult(
                rule_id="valid_transaction",
                title="Valid Transaction Rule (amount > 0, type is credit/debit)",
                importance=5,
                status="UNKNOWN",
                rule="The transaction amount must be > 0 and the transaction type must be valid (credit or debit).",
                observation="Transaction amount and/or transaction type column was not detected in this file.",
                impact="If invalid transaction records are not caught, they can corrupt balances and downstream reconciliation.",
                observed_columns=_explain_columns([(table_name, c) for c in [amount_col, txn_type_col] if c]),
                observed_pattern={"note": "Need both amount and transaction_type in the same file."},
                applicable=False,
            )
        )

    # 6) Duplicate Transaction Rule
    txn_id_col = _pick_best(df, _find_columns(df, ["transaction", "id"], True) or _find_columns(df, ["txn", "id"], True) or _find_columns(df, ["reference", "id"], True))
    if txn_id_col:
        ser = df[txn_id_col].dropna().astype(str)
        dup_mask = ser.duplicated(keep=False)
        dup_count = int(dup_mask.sum())
        st = "FAIL" if dup_count else "PASS"
        rule_txt = "The same transaction ID (or same record) must not be processed twice. Duplicates must be rejected."
        obs = f"Column `{txn_id_col}` has {dup_count} duplicated transaction-id row(s) (counting all rows in duplicate groups)." if dup_count else f"Column `{txn_id_col}` has no duplicates."
        imp = "Duplicate processing causes double-debits/credits, reconciliation breaks, and customer disputes."
        # map dup_mask back to df rows (aligned index)
        full_dup_mask = pd.Series([False] * len(df))
        try:
            full_dup_mask.loc[ser.index] = dup_mask.values
        except Exception:
            pass
        rules.append(
            RuleResult(
                rule_id="duplicate_transaction",
                title="Duplicate Transaction Rule (no duplicate transaction_id)",
                importance=6,
                status=st,
                rule=rule_txt,
                observation=obs,
                impact=imp,
                observed_columns=_explain_columns([(table_name, txn_id_col)]),
                observed_pattern={
                    "duplicate_rows": dup_count,
                    "duplicate_row_examples": _row_examples(df, full_dup_mask, [txn_id_col]),
                },
                applicable=True,
            )
        )
    else:
        rules.append(
            RuleResult(
                rule_id="duplicate_transaction",
                title="Duplicate Transaction Rule (no duplicate transaction_id)",
                importance=6,
                status="UNKNOWN",
                rule="The same transaction ID (or same record) must not be processed twice. Duplicates must be rejected.",
                observation="No transaction_id/txn_id/reference_id column was detected in this file.",
                impact="Without transaction identifiers, duplicate processing risk increases and reconciliation becomes harder.",
                observed_columns=[],
                observed_pattern={"note": "Add a unique transaction id column to reject duplicates."},
                applicable=False,
            )
        )

    # 7) Minimum Balance Rule
    if balance_col:
        b = _to_float_series(df[balance_col])
        bad_mask = b.notna() & (b < min_balance_amount)
        below = int(bad_mask.sum())
        st = "FAIL" if below else "PASS"
        rule_txt = f"The account must maintain the minimum required balance (example: ≥ {min_balance_amount:.0f}). If not, apply penalty / restrict."
        obs = f"Column `{balance_col}`: {below} row(s) are below {min_balance_amount:.2f}." if below else f"Column `{balance_col}`: no rows below {min_balance_amount:.2f}."
        imp = "Minimum balance violations may require charges or restrictions; missing enforcement impacts revenue and policy compliance."
        rules.append(
            RuleResult(
                rule_id="minimum_balance",
                title=f"Minimum Balance Rule (≥ {min_balance_amount:.0f})",
                importance=7,
                status=st,
                rule=rule_txt,
                observation=obs,
                impact=imp,
                observed_columns=_explain_columns([(table_name, balance_col)]),
                observed_pattern={
                    "balance_range": _sample_stats_numeric(df, balance_col),
                    "below_minimum_rows": below,
                    "below_minimum_row_examples": _row_examples(df, bad_mask, [balance_col]),
                },
                applicable=True,
            )
        )
    else:
        rules.append(
            RuleResult(
                rule_id="minimum_balance",
                title=f"Minimum Balance Rule (≥ {min_balance_amount:.0f})",
                importance=7,
                status="UNKNOWN",
                rule=f"The account must maintain the minimum required balance (example: ≥ {min_balance_amount:.0f}). If not, apply penalty / restrict.",
                observation="No balance column was detected in this file.",
                impact="Minimum balance violations cannot be identified from this file alone.",
                observed_columns=[],
                observed_pattern={"note": "Need a balance column to check minimum balance rule."},
                applicable=False,
            )
        )

    # 8-10 (Interest / Charges / Audit) are intentionally omitted here because,
    # for most uploaded CSVs, we cannot conclude PASS/FAIL purely from observed data
    # without bank policy configuration and cross-table context.

    # Only return rules that truly match this file AND produce PASS/FAIL based on observed data
    applicable_rules = [r for r in rules if r.applicable and r.status in ("PASS", "FAIL")]
    return sorted(applicable_rules, key=lambda r: (r.importance, r.rule_id))


def evaluate_banking_business_rules(
    dataframes: Dict[str, pd.DataFrame],
    daily_limit_amount: float = 20000.0,
    min_balance_amount: float = 1000.0,
) -> Dict[str, Any]:
    """
    Evaluate "Top 10 Banking Business Rules (Without KYC)" from the user's uploaded CSVs.

    Returns a JSON-serializable dict.
    """
    tables_out: List[Dict[str, Any]] = []
    for tname, df in (dataframes or {}).items():
        if df is None or getattr(df, "empty", False):
            continue
        table_rules = _table_rule_results(
            table_name=tname,
            df=df,
            daily_limit_amount=daily_limit_amount,
            min_balance_amount=min_balance_amount,
        )
        tables_out.append(
            {
                "table_name": tname,
                "row_count": int(len(df)),
                "column_count": int(len(df.columns)),
                "rules": [
                    {
                        "rule_id": r.rule_id,
                        "title": r.title,
                        "importance": r.importance,
                        "status": r.status,
                        "rule": r.rule,
                        "observation": r.observation,
                        "impact": r.impact,
                        "observed_columns": r.observed_columns,
                        "observed_pattern": r.observed_pattern,
                        "validity": "VALID" if r.status == "PASS" else ("INVALID" if r.status == "FAIL" else "UNKNOWN"),
                    }
                    for r in table_rules
                ],
                "matched_rule_count": int(len(table_rules)),
                "valid_count": int(sum(1 for r in table_rules if r.status == "PASS")),
                "invalid_count": int(sum(1 for r in table_rules if r.status == "FAIL")),
            }
        )

    return {
        "success": True,
        "tables": tables_out,
        "config": {
            "daily_limit_amount": daily_limit_amount,
            "min_balance_amount": min_balance_amount,
        },
    }

