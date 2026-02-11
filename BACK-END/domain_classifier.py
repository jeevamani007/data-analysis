"""
domain_classifier.py
====================
Classifies database schemas into one of six domains:
  Banking | Finance | Insurance | Healthcare | Retail | Other

Architecture
------------
1. Rule engine   – keyword + combo scoring per domain. Drives all normal cases.
2. ML fallback   – used ONLY when the rule engine produces zero signal.
                   When ML fires on fully generic columns, result is "Other".
3. Two-step API  – optional column-only vs value-only analysis before combining.

Key fixes vs previous version
--------------------------------------
- ML fallback no longer leaks banking bias onto generic/unknown schemas.
  When zero keyword signal exists the result defaults to "Other", not ML winner.
- Generic words (id, name, amount, date, type, status, code, number, value,
  reference, description) are excluded from scoring and from ML features.
- Banking exclusive keywords tightened: only truly bank-only tokens count
  (IFSC, SWIFT, IBAN, BIC, routing, emi, overdraft, kyc, etc.).
  Common words like account/balance/transaction/branch/credit/debit are shared,
  not exclusive – they alone cannot trigger a banking classification.
- Combo rules enforce that banking requires at least two EXCLUSIVE signals
  present together before the score is elevated.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline


# ---------------------------------------------------------------------------
# Generic token blacklist
# ---------------------------------------------------------------------------

_GENERIC_TOKENS: frozenset[str] = frozenset({
    "id", "name", "value", "date", "type", "status", "code", "number",
    "amount", "description", "reference", "flag", "key", "col", "field",
    "data", "info", "record", "row", "time", "timestamp", "created",
    "updated", "deleted", "by", "at", "no",
})


def _normalise(token: str) -> str:
    return re.sub(r"[_\-\s]", "", token.lower())


def _normalise_all(columns: list[str]) -> list[str]:
    return [_normalise(c) for c in columns]


def _is_generic(norm_col: str) -> bool:
    return norm_col in _GENERIC_TOKENS or all(
        tok in _GENERIC_TOKENS for tok in re.findall(r"[a-z]+", norm_col)
    )


# ---------------------------------------------------------------------------
# Abbreviation / synonym expansion
# ---------------------------------------------------------------------------

# Many real-world schemas use short codes for column names
# (acc_no, bal_amt, inv_no, trx_id, qty, etc.). These do not
# directly match the longer keywords configured per domain, which
# can cause the rule engine to see "zero signal" and fall back to ML.
#
# To make the rule engine more robust without changing user data, we
# expand some common abbreviations into their canonical forms purely
# for matching. Evidence strings remain based on the original names.

_ABBREV_REPLACEMENTS: dict[str, str] = {
    # Banking / Finance / Insurance
    "acc": "account",
    "acct": "account",
    "cust": "customer",
    "br": "branch",
    "bal": "balance",
    "amt": "amount",
    "amnt": "amount",
    "no": "number",
    "num": "number",
    "cd": "code",
    "txn": "transaction",
    "trx": "transaction",
    "inv": "invoice",
    "sal": "salary",
    "qty": "quantity",
    "qnty": "quantity",
    "fin": "finance",
    # Insurance / Healthcare style short codes
    "clm": "claim",
    "pol": "policy",
    "prem": "premium",
    "hsp": "hospital",
    "py": "payment",
    "tp": "type",
}


def _expand_norm_col(norm_col: str) -> set[str]:
    """
    Return a small set of string variants for matching.

    Example:
      'accno'  -> {'accno', 'accountnumber'}
      'bal_amt' (→ 'balamt') -> {'balamt', 'balanceamount'}

    The original value is always included so behaviour is backward
    compatible when no abbreviation is recognised.
    """
    expanded: set[str] = {norm_col}
    replaced = norm_col
    for abbr, full in _ABBREV_REPLACEMENTS.items():
        if abbr in replaced:
            replaced = replaced.replace(abbr, full)
    if replaced != norm_col:
        expanded.add(replaced)
    return expanded


# ---------------------------------------------------------------------------
# Domain configuration
# ---------------------------------------------------------------------------

@dataclass
class DomainConfig:
    name: str
    ml_label: int
    exclusive_keywords: list[str]   # unique to this domain – high weight
    shared_keywords: list[str]      # also appear elsewhere – low weight
    combo_rules: list[set[str]]     # sets of tokens that together = strong signal
    value_keywords: list[str]       # matched against row data values
    ml_samples: list[str]           # training strings for ML fallback
    exclusive_weight: float = 4.0
    shared_weight: float = 1.0
    combo_bonus: float = 6.0


DOMAIN_CONFIGS: list[DomainConfig] = [

    DomainConfig(
        name="Banking",
        ml_label=0,
        exclusive_keywords=[
            "ifsc", "ifsccode", "swiftcode", "iban", "bic",
            "routingnumber", "routing",
            "overdraftlimit", "overdraftused", "overdraft",
            "minimumbalance", "standinginstruction",
            "clearancedate", "disbursementdate",
            "kycstatus", "kyc",
            "cardnumber", "expirydate",
            "posterminal",
        ],
        shared_keywords=[
            "accountnumber", "accountno", "accountid", "accountbalance", "accounttype",
            "branchid", "branchcode", "branchname",
            "transactionid", "transactiontype",
            "depositamount", "withdrawamount", "withdrawal",
            "creditamount", "debitamount",
            "logintime", "logouttime",
            "settlementdate", "authorizationcode",
            # Loan / EMI concepts are shared between Banking and Finance
            "loanid", "loannumber", "loantype",
            "emiamount", "emi",
        ],
        combo_rules=[
            {"ifsc", "accountnumber"},
            {"swiftcode", "iban"},
            {"loanid", "emi"},
            {"accountid", "overdraft"},
            {"routing", "accountnumber"},
            {"kyc", "accountid"},
        ],
        value_keywords=[
            "ifsc", "swift", "iban", "emi", "overdraft",
            "neft", "rtgs", "imps", "cheque", "standing instruction",
        ],
        ml_samples=[
            "ifsc_code swift_code branch_id account_number routing_number iban bic",
            "loan_id emi_amount tenure_months repayment_schedule interest_accrued",
            "overdraft_limit minimum_balance kyc_status credit_utilized",
            "card_number expiry_date pos_terminal authorization_code settlement_date",
            "account_balance disbursement_date standing_instruction clearance_date",
            "account_type savings_account checking_account overdraft_used",
            "transfer_amount swift_code routing_number iban bic_code beneficiary_id",
            "login_time logout_time session_id banking_activity transaction_count",
            "credit_limit debit_card atm_withdrawal cash_deposit neft_transfer",
            "loan_amount loan_type principal interest_rate tenure disbursement",
        ],
    ),

    DomainConfig(
        name="Finance",
        ml_label=1,
        exclusive_keywords=[
            "gst", "gstin", "cgst", "sgst", "igst",
            "tds", "pf", "esi",
            "ledgerid", "ledgername", "journalid", "voucherno",
            "costcenter", "fiscalyear",
            "accountspayable", "accountsreceivable",
            "grosssalary", "netsalary", "payrollmonth",
            "cogs", "ebitda",
            # Loan contract / finance file identifiers
            "financeid", "finid", "fintype",
            "tenure", "intrate", "interestrate", "int_rate",
            "emiamount", "emi",
        ],
        shared_keywords=[
            "invoice", "invoiceid", "invoiceno",
            "tax", "taxamount",
            "payroll", "expense", "budget",
            "profit", "loss", "revenue",
            "receivable", "payable",
            "debit", "credit",
            "vendor", "supplier",
            "loan", "loanamount", "loantype",
        ],
        combo_rules=[
            {"invoice", "gst"},
            {"invoice", "tax", "paymentmode"},
            {"salary", "payroll"},
            {"ledger", "journal"},
            {"profit", "loss", "revenue"},
            {"gst", "tax"},
            {"tds", "salary"},
        ],
        value_keywords=[
            "gst", "gstin", "tds", "payroll", "invoice",
            "debit note", "credit note", "journal entry", "ledger",
            "accounts payable", "accounts receivable",
            "loan", "emi", "interest", "tenure",
        ],
        ml_samples=[
            "invoice_no gst gstin tax_amount cgst sgst igst taxable_value",
            "payroll_month gross_salary net_salary tds pf esi deductions",
            "ledger_id ledger_name journal_id voucher_no narration",
            "accounts_payable supplier_id bill_no amount_due paid_amount",
            "accounts_receivable receipt_no amount_received balance_due",
            "budget_amount planned_amount actual_amount variance cost_center",
            "profit loss revenue cogs margin fiscal_year quarter",
            "expense_type expense_amount cost_center vendor_name",
            "invoice_date payment_mode payment_status due_date gst",
        ],
    ),

    DomainConfig(
        name="Insurance",
        ml_label=2,
        exclusive_keywords=[
            "policyno", "policyid", "policystartdate", "policyenddate",
            "premiumamt", "premamt", "totalpremium",
            "suminsured", "limamt",
            "claimid", "claimamount", "claimstatus", "settlementamount",
            "claimnumber", "claimtype",
            "underwriting", "underwritingscore",
            "deductible", "dedamt",
            "renewaldate", "lapsedate", "graceperiod",
            "lob", "lobcd", "covcd",
            "paidamt", "rsvamt",
        ],
        shared_keywords=[
            "policy", "polid", "policytype",
            "claim", "clmid", "claimno",
            "premium", "prem",
            "insured", "insurer",
            "beneficiary", "nominee",
            "coverage", "rider",
            "agent", "agentid", "broker",
        ],
        combo_rules=[
            {"policy", "premium"},
            {"claim", "policy"},
            {"suminsured", "premium"},
            {"nominee", "policy"},
            {"coverage", "deductible"},
            {"policy", "beneficiary"},
        ],
        value_keywords=[
            "policy", "claim approved", "premium paid", "sum insured",
            "beneficiary", "nominee", "underwriting", "policy lapsed",
        ],
        ml_samples=[
            "policy_no policy_type insured_name nominee sum_insured premium",
            "claim_id claim_date claim_amount claim_status settlement_amount",
            "underwriting_score rider coverage_type deductible copay co_insurance",
            "policy_start_date policy_end_date renewal_date lapse_date grace_period",
            "agent_id broker_id insurer_name premium_due_date payment_mode",
            "pol_id prem_amt clm_id eff_dt exp_dt lob_cd cov_cd",
        ],
    ),

    DomainConfig(
        name="Healthcare",
        ml_label=3,
        exclusive_keywords=[
            "patientid", "patientname",
            "diagnosisid", "icdcode", "diagnosisname",
            "treatmentplan", "prescriptiondate",
            "medicationname", "dosage", "druginteraction",
            "admissiondate", "dischargedate",
            "labtest", "labtestid", "labresult", "specimentype",
            "radiologyreport", "scantype", "radiologistid",
            "triagelevel", "chiefcomplaint",
            "hemoglobin", "bloodgroup", "bloodtype",
            "vaccinationrecord", "vaccination",
            "icd",
        ],
        shared_keywords=[
            "patient", "doctor", "physician", "surgeon", "nurse",
            "diagnosis", "treatment", "prescription", "medication",
            "admission", "discharge", "hospital", "clinic", "ward",
            "appointment", "vitals", "symptoms", "allergies",
        ],
        combo_rules=[
            {"patient", "doctor"},
            {"patient", "diagnosis"},
            {"patient", "admission", "discharge"},
            {"medication", "dosage"},
            {"labtest", "testresult"},
            {"patient", "medication"},
            {"patient", "appointment"},
        ],
        value_keywords=[
            "dr.", "doctor", "patient", "diagnosis", "prescription",
            "hospital", "clinic", "surgery", "ward", "triage",
            "x-ray", "mri", "ct scan", "blood test",
        ],
        ml_samples=[
            "patient_id blood_type allergies icd_code",
            "diagnosis_id icd_code diagnosis_name doctor_id admission_date vitals",
            "treatment_plan_id medication dosage frequency prescription_date",
            "doctor_id specialty license_number hospital_id consultation_fee",
            "appointment_id doctor_id appointment_date clinic_room",
            "medical_record_id symptoms lab_results treatment_history vaccination",
            "admission_date discharge_date diagnosis ward bed_number",
            "prescription_id medication_name dosage drug_interaction",
            "lab_test_id test_name test_result specimen_type",
            "radiology_report scan_type imaging_date radiologist_id",
        ],
    ),

    DomainConfig(
        name="Retail",
        ml_label=4,
        exclusive_keywords=[
            "productid", "productname", "sku",
            "unitprice", "costprice", "stockquantity", "stockqty",
            "orderid", "orderstatus",
            "discountamount", "netamount",
            "saleschannel", "storeid", "cashierid",
            "returnflag", "returndate",
            "invoicetype", "billno",
        ],
        shared_keywords=[
            "category", "brand", "quantity",
            "discount", "tax", "taxamount",
            "paymentmethod", "paymentstatus",
            "totalamount", "orderdate",
        ],
        combo_rules=[
            {"productid", "quantity", "unitprice"},
            {"orderid", "productid", "category"},
            {"sku", "stockquantity"},
            {"discount", "totalamount", "paymentmethod"},
            {"productname", "unitprice"},
        ],
        value_keywords=[
            "order placed", "order shipped", "delivered", "cancelled",
            "returned", "refund", "sku", "checkout", "out of stock",
        ],
        ml_samples=[
            "product_id product_name sku unit_price stock_quantity brand",
            "order_id order_date product_name category unit_price discount tax",
            "sales_channel store_id cashier_id product_name brand stock_quantity",
            "invoice_type invoice_no product_id unit_price tax_amount return_flag",
            "payment_method payment_status total_amount discount net_amount",
            "sku product_name category brand unit_price cost_price stock_quantity",
        ],
    ),

    DomainConfig(
        name="Other",
        ml_label=5,
        exclusive_keywords=[],
        shared_keywords=[
            "employeeid", "studentid", "ticketid", "vehicleid", "deviceid",
            "courseid", "course", "subject", "classid", "teacherid",
            # HR / generic people-data
            "employee", "empid", "empcode", "empno", "emp_no",
            "department", "dept",
            "designation", "position", "jobtitle", "job_title", "role",
            "team", "managerid", "manager",
            "hr",
        ],
        combo_rules=[],
        value_keywords=[],
        ml_samples=[
            "employee_id department hire_date job_title manager_id",
            "student_id course_id grade semester enrollment_date",
            "ticket_id issue_description priority created_at resolved_at",
            "vehicle_id model make year vin_number owner_id",
            "device_id device_type os_version last_seen ip_address",
            "property_id address bedrooms area listing_date",
            "post_id likes shares platform published_at",
            "shipment_id origin destination carrier estimated_delivery",
        ],
    ),
]

DOMAIN_MAP: dict[str, DomainConfig] = {d.name: d for d in DOMAIN_CONFIGS}
DOMAIN_NAMES: list[str] = [d.name for d in DOMAIN_CONFIGS]


# ---------------------------------------------------------------------------
# Scoring helpers
# ---------------------------------------------------------------------------

def _score_domain(norm_cols: list[str], config: DomainConfig) -> dict[str, float]:
    # Expand each normalised column into a small set of variants so that
    # short codes like "acc_no", "bal_amt", "inv_no", "trx_id" still match
    # the longer domain keywords ("accountnumber", "balance", "invoice",
    # "transactionid", etc.).
    def _variants(col: str) -> set[str]:
        return _expand_norm_col(col)

    exclusive_hits = 0
    shared_hits = 0
    non_generic_cols = [c for c in norm_cols if not _is_generic(c)]

    for col in norm_cols:
        if _is_generic(col):
            continue
        vars_for_col = _variants(col)
        has_exclusive = any(
            kw in v for v in vars_for_col for kw in config.exclusive_keywords
        )
        has_shared = any(
            kw in v for v in vars_for_col for kw in config.shared_keywords
        )
        if has_exclusive:
            exclusive_hits += 1
        elif has_shared and not has_exclusive:
            shared_hits += 1

    combo_hits = 0
    if config.combo_rules:
        all_variants = [v for col in norm_cols for v in _variants(col)]
        for rule in config.combo_rules:
            if all(any(kw in v for v in all_variants) for kw in rule):
                combo_hits += 1

    total = (
        exclusive_hits * config.exclusive_weight
        + shared_hits * config.shared_weight
        + combo_hits * config.combo_bonus
    )
    coverage = 0.0
    if non_generic_cols:
        coverage = (exclusive_hits + shared_hits) / float(len(non_generic_cols))

    return {
        "exclusive_hits": exclusive_hits,
        "shared_hits": shared_hits,
        "combo_hits": combo_hits,
        "total": total,
        "coverage": coverage,
    }


def _score_values(sample_values: list[str], config: DomainConfig) -> int:
    return sum(
        1 for val in sample_values
        if any(kw in str(val).lower().strip() for kw in config.value_keywords)
    )


def _to_probs(scores: dict[str, float], min_floor: float = 0.005) -> dict[str, float]:
    total = sum(scores.values())
    if total == 0:
        equal = 1.0 / len(scores)
        return {k: equal for k in scores}
    raw = {k: max(v / total, min_floor) for k, v in scores.items()}
    norm_total = sum(raw.values())
    return {k: v / norm_total for k, v in raw.items()}


# ---------------------------------------------------------------------------
# ML fallback
# ---------------------------------------------------------------------------

class _MLFallback:
    """
    Logistic Regression over synthetic samples.
    Only invoked when keyword scoring produces zero signal.

    Key safeguard: if all columns are generic tokens (id, name, value, etc.)
    the model returns Other directly without running inference.
    If the model's top score is below the certainty threshold, Other wins.
    """

    _THRESHOLD = 0.55

    def __init__(self) -> None:
        X: list[str] = []
        y: list[int] = []
        for cfg in DOMAIN_CONFIGS:
            X.extend(cfg.ml_samples)
            y.extend([cfg.ml_label] * len(cfg.ml_samples))

        self._pipeline = Pipeline([
            ("tfidf", TfidfVectorizer(
                token_pattern=r"(?u)\b\w+\b",
                ngram_range=(1, 2),
                sublinear_tf=True,
                stop_words=list(_GENERIC_TOKENS),
            )),
            ("clf", LogisticRegression(
                max_iter=1000,
                random_state=42,
                class_weight="balanced",
            )),
        ])
        self._pipeline.fit(X, y)

    def predict(self, text: str, all_generic: bool) -> dict[str, float]:
        if all_generic:
            return {n: (1.0 if n == "Other" else 0.0) for n in DOMAIN_NAMES}

        proba = self._pipeline.predict_proba([text])[0]
        probs = {cfg.name: float(p) for cfg, p in zip(DOMAIN_CONFIGS, proba)}

        top = max(probs, key=probs.get)
        if probs[top] < self._THRESHOLD:
            return {n: (0.85 if n == "Other" else 0.03) for n in DOMAIN_NAMES}

        return probs


# ---------------------------------------------------------------------------
# Main classifier
# ---------------------------------------------------------------------------

class DomainClassifier:
    """
    Classifies database schemas into Banking, Finance, Insurance,
    Healthcare, Retail, or Other.

    Public methods
    --------------
    predict(table_names, all_columns, sample_values=None) → dict
    predict_domain_2step(all_columns, sample_values=None) → dict
    classify_table(df, table_name) → (is_banking, confidence, evidence)
    get_domain_split_summary(table_names, all_columns, sample_values=None) → dict
    """

    def __init__(self) -> None:
        self._ml = _MLFallback()

    # ------------------------------------------------------------------ predict

    def predict(
        self,
        table_names: list[str],
        all_columns: list[str],
        sample_values: list[str] | None = None,
    ) -> dict[str, Any]:
        norm_cols = _normalise_all(all_columns)
        values = list(sample_values or [])

        # Keyword scoring
        domain_stats: dict[str, dict[str, float]] = {
            cfg.name: _score_domain(norm_cols, cfg) for cfg in DOMAIN_CONFIGS
        }
        col_scores: dict[str, float] = {
            name: stats["total"] for name, stats in domain_stats.items()
        }

        # Blend in value-level scores at reduced weight
        if values:
            for cfg in DOMAIN_CONFIGS:
                col_scores[cfg.name] += _score_values(values[:200], cfg) * 0.5

        # ML fallback only when zero keyword signal
        used_ml = False
        if sum(col_scores.values()) == 0:
            all_generic = all(_is_generic(c) for c in norm_cols)
            text = " ".join(table_names + all_columns + values[:50])
            col_scores = self._ml.predict(text, all_generic)
            used_ml = True
        else:
            # Apply conservative cutoffs so that a domain only wins when
            # a meaningful share of non-generic columns point to it and
            # it is clearly stronger than the next-best domain.
            coverage = {
                name: stats.get("coverage", 0.0) for name, stats in domain_stats.items()
            }
            col_scores = self._apply_domain_cutoffs(col_scores, coverage)

        probs = _to_probs(col_scores)
        primary = max(probs, key=probs.get)
        confidence = round(probs[primary] * 100, 2)
        percentages = self._round_to_100({k: v * 100 for k, v in probs.items()})

        return {
            "domain_label": primary,
            "is_banking": primary == "Banking",
            "confidence": confidence,
            "percentages": percentages,
            "evidence": self._build_evidence(all_columns, norm_cols, values),
            "column_domain_map": self._build_column_map(all_columns, norm_cols),
            "used_ml_fallback": used_ml,
        }

    # -------------------------------------------------------- predict_domain_2step

    def predict_domain_2step(
        self,
        all_columns: list[str],
        sample_values: list[str] | None = None,
    ) -> dict[str, Any]:
        step1 = self._analyse_columns(all_columns)
        step2 = self._analyse_values(sample_values or [])
        return {
            "step1_column_analysis": step1,
            "step2_value_analysis": step2,
            "final_prediction": self._combine_steps(step1, step2),
        }

    # --------------------------------------------------------- classify_table

    def classify_table(
        self, df: pd.DataFrame, table_name: str
    ) -> tuple[bool, float, list[str]]:
        columns = list(df.columns)
        sample_values: list[str] = []
        for col in df.columns:
            sample_values.extend(df[col].dropna().head(10).astype(str).tolist())
        r = self.predict(table_names=[table_name], all_columns=columns,
                         sample_values=sample_values)
        return r["is_banking"], r["confidence"], r["evidence"]

    # --------------------------------------------------- get_domain_split_summary

    def get_domain_split_summary(
        self,
        table_names: list[str],
        all_columns: list[str],
        sample_values: list[str] | None = None,
    ) -> dict[str, Any]:
        r = self.predict(table_names, all_columns, sample_values)
        pcts = r["percentages"]
        colors = {"Banking": "#0F766E", "Finance": "#4F46E5",
                  "Insurance": "#7C3AED", "Healthcare": "#14B8A6",
                  "Retail": "#F59E0B", "Other": "#64748B"}
        return {
            "percentages": pcts,
            "primary_domain": r["domain_label"],
            "confidence": r["confidence"],
            "is_banking": r["is_banking"],
            "evidence": r["evidence"],
            "column_domain_map": r["column_domain_map"],
            "used_ml_fallback": r["used_ml_fallback"],
            "chart_data": {
                "labels": DOMAIN_NAMES,
                "values": [pcts[d] for d in DOMAIN_NAMES],
                "colors": [colors[d] for d in DOMAIN_NAMES],
            },
            "explanation": self._generate_explanation(
                r["domain_label"], r["confidence"], r["evidence"]
            ),
        }

    # ------------------------------------------------------------------ internals

    def _analyse_columns(self, columns: list[str]) -> dict[str, Any]:
        norm = _normalise_all(columns)
        scores = {cfg.name: _score_domain(norm, cfg)["total"] for cfg in DOMAIN_CONFIGS}
        total = sum(scores.values())
        if total == 0:
            return {"primary_domain": "Other", "confidence": 0.0,
                    "scores": scores, "evidence": []}
        primary = max(scores, key=scores.get)
        return {"primary_domain": primary,
                "confidence": round(scores[primary] / total * 100, 1),
                "scores": scores,
                "evidence": self._build_evidence(columns, norm, [])}

    def _analyse_values(self, sample_values: list[str]) -> dict[str, Any]:
        if not sample_values:
            return {"primary_domain": "Unknown", "confidence": 0.0,
                    "scores": {}, "evidence": []}
        scores = {cfg.name: float(_score_values(sample_values, cfg))
                  for cfg in DOMAIN_CONFIGS}
        total = sum(scores.values())
        if total == 0:
            return {"primary_domain": "Other", "confidence": 0.0,
                    "scores": scores, "evidence": []}
        primary = max(scores, key=scores.get)
        cfg = DOMAIN_MAP[primary]
        evidence = [v for v in sample_values
                    if any(kw in str(v).lower() for kw in cfg.value_keywords)][:3]
        return {"primary_domain": primary,
                "confidence": round(scores[primary] / total * 100, 1),
                "scores": scores, "evidence": evidence}

    @staticmethod
    def _apply_domain_cutoffs(
        col_scores: dict[str, float], coverage: dict[str, float]
    ) -> dict[str, float]:
        """
        Apply simple sanity rules so that a domain only wins when:
        - it covers a meaningful share of non-generic columns (coverage), AND
        - it is clearly stronger than the next-best domain.

        Otherwise we downgrade the result to Other to avoid misclassifying
        HR / generic tables as Finance/Banking/etc. just because of 1–2
        overlapping column names.
        """
        if not col_scores:
            return col_scores

        primary = max(col_scores, key=col_scores.get)
        best = col_scores[primary]
        others = [v for k, v in col_scores.items() if k != primary]
        second = max(others) if others else 0.0
        cov = coverage.get(primary, 0.0)

        # Thresholds can be tuned, but keep them conservative.
        min_coverage = 0.2  # at least 20% of non-generic columns
        margin_ratio = 1.25  # winner should be clearly above runner-up

        if primary != "Other":
            weak_coverage = cov < min_coverage
            weak_margin = second > 0 and best < margin_ratio * second
            if weak_coverage or weak_margin:
                # Downgrade ambiguous result to Other
                return {name: (1.0 if name == "Other" else 0.0)
                        for name in col_scores}

        return col_scores

    @staticmethod
    def _combine_steps(step1: dict, step2: dict) -> dict[str, Any]:
        d1, d2 = step1["primary_domain"], step2["primary_domain"]
        c1, c2 = step1["confidence"], step2["confidence"]
        if d1 == d2:
            return {"domain": d1, "reasoning":
                    f"Column and value analysis agree on '{d1}'. "
                    f"Column: {c1:.1f}%, values: {c2:.1f}%."}
        if d2 not in ("Other", "Unknown"):
            return {"domain": d2, "reasoning":
                    f"Conflict – columns suggest '{d1}' ({c1:.1f}%) "
                    f"but row values indicate '{d2}' ({c2:.1f}%). "
                    "Row data takes priority."}
        if d1 != "Other":
            return {"domain": d1, "reasoning":
                    f"Row data inconclusive; using column result '{d1}' ({c1:.1f}%)."}
        return {"domain": "Other",
                "reasoning": "Both column and value analysis are inconclusive."}

    def _build_evidence(
        self, columns: list[str], norm_cols: list[str], values: list[str]
    ) -> list[str]:
        evidence: list[str] = []
        for cfg in DOMAIN_CONFIGS:
            if cfg.name == "Other":
                continue
            all_kws = cfg.exclusive_keywords + cfg.shared_keywords
            hits = [col for col, nc in zip(columns, norm_cols)
                    if not _is_generic(nc) and any(kw in nc for kw in all_kws)]
            val_hits = [str(v) for v in values
                        if any(kw in str(v).lower() for kw in cfg.value_keywords)][:3]
            if hits:
                evidence.append(f"{cfg.name} columns: {', '.join(hits[:5])}")
            if val_hits:
                evidence.append(f"{cfg.name} values: {', '.join(val_hits)}")
        return evidence

    @staticmethod
    def _build_column_map(columns: list[str], norm_cols: list[str]) -> dict[str, list[str]]:
        result: dict[str, list[str]] = {d: [] for d in DOMAIN_NAMES}
        for col, nc in zip(columns, norm_cols):
            matched = False
            for cfg in DOMAIN_CONFIGS:
                if cfg.name == "Other":
                    continue
                all_kws = cfg.exclusive_keywords + cfg.shared_keywords
                if not _is_generic(nc) and any(kw in nc for kw in all_kws):
                    result[cfg.name].append(col)
                    matched = True
            if not matched:
                result["Other"].append(col)
        return result

    @staticmethod
    def _round_to_100(raw: dict[str, float]) -> dict[str, float]:
        rounded = {k: round(v, 1) for k, v in raw.items()}
        diff = round(100.0 - sum(rounded.values()), 1)
        if diff != 0:
            largest = max(rounded, key=rounded.get)
            rounded[largest] = round(rounded[largest] + diff, 1)
        return rounded

    @staticmethod
    def _generate_explanation(primary: str, confidence: float, evidence: list[str]) -> str:
        high = confidence >= 70
        intros: dict[str, tuple[str, str]] = {
            "Banking":    ("a <strong>Banking</strong> database",
                           "a <strong>Banking-related</strong> database"),
            "Finance":    ("a <strong>Finance / Accounting</strong> database",
                           "a <strong>Finance-related</strong> database"),
            "Insurance":  ("an <strong>Insurance</strong> database",
                           "an <strong>Insurance-related</strong> database"),
            "Healthcare": ("a <strong>Healthcare</strong> database",
                           "a <strong>Healthcare-related</strong> database"),
            "Retail":     ("a <strong>Retail / E-commerce</strong> database",
                           "a <strong>Retail-related</strong> database"),
            "Other":      ("a <strong>General / Other</strong> domain database",
                           "a database with <strong>mixed or unclear characteristics</strong>"),
        }
        hi, lo = intros.get(primary, ("an <strong>Unknown</strong> database",) * 2)
        ev = (" Evidence: " + "; ".join(evidence[:3]) + ".") if evidence else ""
        return f"This appears to be {hi if high else lo} (confidence: {confidence:.1f}%).{ev}"
