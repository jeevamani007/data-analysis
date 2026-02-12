"""
domain_classifier.py
====================
Classifies database schemas into one of six domains:
  Banking | Finance | Insurance | Healthcare | Retail | Other

Key improvements in this version
---------------------------------
1. Full abbreviation/shortcut column name support for ALL domains
   (acc_no, txn_id, pol_no, prm_amt, pat_id, prod_nm, inv_no, etc.)
2. Domain-specific abbreviation alias maps that translate short forms
   before scoring, so abbreviated schemas classify as reliably as full names.
3. Tighter "Other" domain handling – generic/unknown schemas stay Other
   instead of leaking into Banking.
4. ML fallback only fires on zero-signal input; fully-generic columns
   return Other directly without model inference.
5. Combo rules extended to cover abbreviated column combos.
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
# Generic token blacklist  (never drive classification alone)
# ---------------------------------------------------------------------------

_GENERIC_TOKENS: frozenset[str] = frozenset({
    "id", "nm", "name", "val", "value", "dt", "date", "typ", "type",
    "sts", "status", "cd", "code", "no", "num", "number",
    "amt", "amount", "desc", "description", "ref", "reference",
    "flg", "flag", "key", "col", "fld", "field",
    "data", "info", "rec", "record", "row",
    "ts", "time", "timestamp", "created", "updated", "deleted",
    "by", "at", "seq", "idx",
})


def _normalise(token: str) -> str:
    """Lowercase + strip separators."""
    return re.sub(r"[_\-\s]", "", token.lower())


def _normalise_all(columns: list[str]) -> list[str]:
    return [_normalise(c) for c in columns]


def _is_generic(norm_col: str) -> bool:
    """True if every alphabetic chunk of the column is a generic token."""
    parts = re.findall(r"[a-z]+", norm_col)
    return not parts or all(p in _GENERIC_TOKENS for p in parts)


# ---------------------------------------------------------------------------
# Abbreviation alias maps
# Each map: normalised_abbreviation → canonical_keyword (must appear in config)
# ---------------------------------------------------------------------------

_BANKING_ALIASES: dict[str, str] = {
    # account
    "acno": "accountno", "accno": "accountno", "acnum": "accountno",
    "acctno": "accountno", "accnum": "accountno", "acnumber": "accountno",
    "acid": "accountid", "accid": "accountid", "acctid": "accountid",
    "acbal": "accountbalance", "accbal": "accountbalance", "acctbal": "accountbalance",
    "actype": "accounttype", "acctyp": "accounttype",
    # transaction
    "txnid": "transactionid", "trnid": "transactionid", "txid": "transactionid",
    "txnamt": "depositamount", "trnamt": "depositamount",
    "txndt": "transactionid", "trndt": "transactionid",
    "txntyp": "transactiontype", "trntyp": "transactiontype",
    # branch
    "brcd": "branchcode", "brid": "branchid", "brncd": "branchcode",
    "brnid": "branchid",
    # loan
    "lnid": "loanid", "loanno": "loanid", "lnno": "loanid",
    "lnamt": "loanid", "lnamount": "loanid",
    # emi / overdraft / kyc
    "emino": "emi", "eminum": "emi", "emiamt": "emiamount",
    "odlmt": "overdraftlimit", "odamt": "overdraftlimit",
    "kycflg": "kycstatus", "kycsts": "kycstatus",
    # deposit / withdrawal
    "depamt": "depositamount", "dep": "depositamount",
    "wdlamt": "withdrawamount", "wdl": "withdrawamount", "wthdrwl": "withdrawal",
    # ifsc / swift
    "ifsccd": "ifsc", "swftcd": "swiftcode", "swft": "swiftcode",
}

_FINANCE_ALIASES: dict[str, str] = {
    # invoice
    "invno": "invoiceno", "invid": "invoiceid", "invdt": "invoiceno",
    "invamt": "invoiceid", "invcno": "invoiceno",
    # gst / tax
    "gstno": "gstin", "gstnm": "gstin", "gstamt": "gst",
    "taxamt": "taxamount", "taxno": "gst",
    # salary / payroll
    "salno": "salary", "salamt": "salary", "salmth": "payrollmonth",
    "grssal": "grosssalary", "netsal": "netsalary", "grpay": "grosssalary",
    "pfamt": "pf", "esiamt": "esi", "tdsamt": "tds",
    "hramt": "salary", "daamt": "salary",
    # ledger / journal / voucher
    "ldgrid": "ledgerid", "ldgrnm": "ledgername",
    "jrnlid": "journalid", "jrnldt": "journalid",
    "vchrno": "voucherno", "vchrid": "voucherno",
    # debit / credit
    "dramt": "debitamount", "cramt": "creditamount",
    "drno": "debit", "crno": "credit",
    "acctpay": "accountspayable", "acctrec": "accountsreceivable",
}

_INSURANCE_ALIASES: dict[str, str] = {
    # policy
    "plyno": "policyno", "plyid": "policyid", "polno": "policyno",
    "poltyp": "policyno", "pltyp": "policyno",
    "polstdt": "policystartdate", "polenddt": "policyenddate",
    "polstrtdt": "policystartdate",
    # premium
    "prmamt": "premiumamt", "prm": "premiumamt", "premno": "premiumamt",
    "totprm": "totalpremium", "prmpd": "premiumamt",
    # sum insured / limit
    "siamt": "suminsured", "si": "suminsured", "limamt": "limamt",
    # claim
    "clmno": "claimid", "clmamt": "claimamt", "clmsts": "claimstatus",
    "clmdt": "claimid", "clmid": "claimid",
    # beneficiary / nominee
    "bnfnm": "beneficiary", "bnfid": "beneficiary", "bnf": "beneficiary",
    "nomnm": "nominee", "nomid": "nominee", "nom": "nominee",
    # insured
    "insrdnm": "insured", "insrdid": "insured", "insrnm": "insurer",
    # coverage / underwriting
    "cvgtyp": "coverage", "cvgcd": "covcd", "covtyp": "coverage",
    "undscr": "underwriting", "undwrt": "underwriting",
    # renewal / lapse
    "rendt": "renewaldate", "rennm": "renewaldate",
    "lpsdt": "lapsedate", "lpsfg": "lapsedate",
    "grprd": "graceperiod", "graceprd": "graceperiod",
    # deductible / rider
    "dedamt": "deductible", "ded": "deductible",
    "rdrcd": "rider", "rdrnm": "rider",
    # lob / coverage code
    "lobcd": "lob", "covcd": "covcd", "cov": "covcd",
    # paid / reserve
    "pdamt": "paidamt", "rsvamt": "rsvamt", "rsv": "rsvamt",
}

_HEALTHCARE_ALIASES: dict[str, str] = {
    # patient
    "ptid": "patientid", "patnm": "patientname", "patrec": "patientid",
    "pt": "patient", "patno": "patientid",
    # doctor
    "drid": "doctor", "drno": "doctor", "docid": "doctor", "docnm": "doctor",
    # diagnosis / ICD
    "diagcd": "diagnosisid", "diagid": "diagnosisid", "diagno": "diagnosisid",
    "icdcd": "icdcode", "icdno": "icdcode",
    # medication / dosage / prescription
    "mednm": "medicationname", "medid": "medicationname", "med": "medication",
    "dosqty": "dosage", "dosage": "dosage",
    "rxdt": "prescriptiondate", "rxno": "prescriptiondate", "rx": "prescriptiondate",
    # admission / discharge
    "admdt": "admissiondate", "admno": "admissiondate", "adm": "admission",
    "dscdt": "dischargedate", "discdt": "dischargedate", "dsc": "discharge",
    # lab / test / specimen
    "labid": "labtest", "labrec": "labtest",
    "tstnm": "labtest", "tstno": "labtest",
    "tstres": "labresult", "tstrst": "labresult",
    "spcmtp": "specimentype", "spcm": "specimentype", "spmtyp": "specimentype",
    # ward / bed
    "wardno": "ward", "bedid": "ward", "bedno": "ward",
    # vitals / symptoms
    "vitsgn": "vitals", "vtl": "vitals",
    "symcd": "symptoms", "symno": "symptoms",
}

_RETAIL_ALIASES: dict[str, str] = {
    # product
    "prodid": "productid", "prodnm": "productname", "prodcd": "productid",
    "prodno": "productid", "itmid": "productid", "itmnm": "productname",
    # order
    "ordid": "orderid", "ordno": "orderid", "ordnm": "orderid",
    "orddt": "orderdate", "ordsts": "orderstatus",
    # price / quantity
    "upr": "unitprice", "uprc": "unitprice", "unitprc": "unitprice",
    "qty": "quantity", "qtyno": "quantity",
    "discamt": "discountamount", "disamt": "discountamount",
    "netamt": "netamount", "totamt": "totalamount",
    # sku / category / brand
    "skucd": "sku", "skuno": "sku",
    "catnm": "category", "catcd": "category", "catid": "category",
    "brndn": "brand", "brndnm": "brand", "brndid": "brand",
    # stock
    "stkqty": "stockquantity", "stk": "stockquantity", "stqty": "stockqty",
    # return / payment
    "rtnflg": "returnflag", "retflg": "returnflag", "rtndt": "returndate",
    "paymth": "paymentmethod", "paysts": "paymentstatus",
    # store / cashier
    "strid": "storeid", "strnm": "storeid",
    "csrid": "cashierid", "casrid": "cashierid",
    # bill / invoice
    "billno": "billno", "bilno": "billno", "invtyp": "invoicetype",
    "taxamt": "taxamount",
}

# Combined alias lookup: normalised_abbrev → canonical keyword
_ALL_ALIASES: dict[str, dict[str, str]] = {
    "Banking": _BANKING_ALIASES,
    "Finance": _FINANCE_ALIASES,
    "Insurance": _INSURANCE_ALIASES,
    "Healthcare": _HEALTHCARE_ALIASES,
    "Retail": _RETAIL_ALIASES,
}


def _expand_aliases(norm_cols: list[str]) -> dict[str, list[str]]:
    """
    For each domain, produce an expanded list where abbreviated column
    names are replaced by their canonical keyword equivalents.
    Returns dict[domain_name → expanded_norm_cols].
    """
    expanded: dict[str, list[str]] = {}
    for domain, alias_map in _ALL_ALIASES.items():
        result = []
        for nc in norm_cols:
            if nc in alias_map:
                result.append(alias_map[nc])
            else:
                result.append(nc)
        expanded[domain] = result
    return expanded


# ---------------------------------------------------------------------------
# Domain configuration
# ---------------------------------------------------------------------------

@dataclass
class DomainConfig:
    name: str
    ml_label: int
    exclusive_keywords: list[str]
    shared_keywords: list[str]
    combo_rules: list[set[str]]
    value_keywords: list[str]
    ml_samples: list[str]
    exclusive_weight: float = 4.0
    shared_weight: float = 1.0
    combo_bonus: float = 6.0


DOMAIN_CONFIGS: list[DomainConfig] = [

    # ------------------------------------------------------------------
    DomainConfig(
        name="Banking",
        ml_label=0,
        exclusive_keywords=[
            # IFSC / SWIFT / international
            "ifsc", "ifsccode", "swiftcode", "iban", "bic",
            "routingnumber", "routing",
            # loan / EMI
            "emiamount", "emi", "loanid", "loannumber", "loantype",
            # overdraft / balance controls
            "overdraftlimit", "overdraftused", "overdraft",
            "minimumbalance", "standinginstruction",
            # dates specific to banking ops
            "clearancedate", "disbursementdate",
            # KYC / card
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
        ],
        combo_rules=[
            {"ifsc", "accountnumber"},
            {"ifsc", "accountid"},
            {"swiftcode", "iban"},
            {"loanid", "emi"},
            {"accountid", "overdraft"},
            {"routing", "accountnumber"},
            {"kyc", "accountid"},
            {"emi", "accountid"},
            {"loanid", "accountid"},
        ],
        value_keywords=[
            "ifsc", "swift", "iban", "emi", "overdraft",
            "neft", "rtgs", "imps", "cheque", "standing instruction",
            "loan disbursed", "emi deducted",
        ],
        ml_samples=[
            "ifsc_code swift_code branch_id account_number routing_number iban bic",
            "loan_id emi_amount tenure_months repayment_schedule interest_accrued",
            "overdraft_limit minimum_balance kyc_status credit_utilized",
            "card_number expiry_date pos_terminal authorization_code settlement_date",
            "account_balance disbursement_date standing_instruction clearance_date",
            "account_type savings_account checking_account overdraft_used",
            "transfer_amount swift_code routing_number iban bic_code beneficiary_id",
            "login_time logout_time banking_activity transaction_count",
            "credit_limit debit_card atm_withdrawal cash_deposit neft_transfer",
            "loan_amount loan_type principal interest_rate tenure disbursement",
        ],
    ),

    # ------------------------------------------------------------------
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
            # abbreviation expansions
            "debitamount", "creditamount",
        ],
        shared_keywords=[
            "invoice", "invoiceid", "invoiceno",
            "tax", "taxamount",
            "salary", "payroll", "expense", "budget",
            "profit", "loss", "revenue",
            "receivable", "payable",
            "debit", "credit",
            "vendor", "supplier",
        ],
        combo_rules=[
            {"invoice", "gst"},
            {"invoice", "tax"},
            {"salary", "payroll"},
            {"ledger", "journal"},
            {"profit", "loss", "revenue"},
            {"gst", "tax"},
            {"tds", "salary"},
            {"pf", "esi"},
            {"invoice", "paymentmode"},
        ],
        value_keywords=[
            "gst", "gstin", "tds", "payroll", "salary", "invoice",
            "debit note", "credit note", "journal entry", "ledger",
            "accounts payable", "accounts receivable",
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

    # ------------------------------------------------------------------
    DomainConfig(
        name="Insurance",
        ml_label=2,
        exclusive_keywords=[
            "policyno", "policyid", "policystartdate", "policyenddate",
            "premiumamt", "premamt", "totalpremium",
            "suminsured", "limamt",
            "claimid", "claimamount", "claimstatus", "settlementamount",
            "underwriting", "underwritingscore",
            "deductible", "dedamt",
            "renewaldate", "lapsedate", "graceperiod",
            "lob", "lobcd", "covcd",
            "paidamt", "rsvamt",
            # abbreviation expansions
            "claimamt",
        ],
        shared_keywords=[
            "policy", "polid",
            "claim", "clmid",
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
            {"policyno", "premiumamt"},
            {"claimid", "policyno"},
            {"insured", "premium"},
            {"nominee", "beneficiary"},
        ],
        value_keywords=[
            "policy", "claim approved", "premium paid", "sum insured",
            "beneficiary", "nominee", "underwriting", "policy lapsed",
            "policy issued", "coverage",
        ],
        ml_samples=[
            "policy_no policy_type insured_name nominee sum_insured premium",
            "claim_id claim_date claim_amount claim_status settlement_amount",
            "underwriting_score rider coverage_type deductible copay co_insurance",
            "policy_start_date policy_end_date renewal_date lapse_date grace_period",
            "agent_id broker_id insurer_name premium_due_date payment_mode",
            "pol_id prem_amt clm_id eff_dt exp_dt lob_cd cov_cd",
            "beneficiary_name nominee_name coverage_type underwriting_score",
            "sum_insured deductible renewal_date lapse_date rider",
        ],
    ),

    # ------------------------------------------------------------------
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
            # abbreviation expansions
            "prescriptiondt", "prescriptionno",
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
            {"labtest", "labresult"},
            {"patient", "medication"},
            {"patient", "appointment"},
            {"patientid", "doctor"},
            {"patientid", "medication"},
            {"patientid", "labtest"},
            {"diagnosis", "treatment"},
        ],
        value_keywords=[
            "dr.", "doctor", "patient", "diagnosis", "prescription",
            "hospital", "clinic", "surgery", "ward", "triage",
            "x-ray", "mri", "ct scan", "blood test", "lab test",
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

    # ------------------------------------------------------------------
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
            # abbreviation expansions
            "prodid", "prodnm", "ordid",
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
            # abbreviated combos
            {"prodid", "ordid"},
            {"prodid", "quantity"},
            {"ordid", "quantity", "unitprice"},
            {"sku", "quantity"},
        ],
        value_keywords=[
            "order placed", "order shipped", "delivered", "cancelled",
            "returned", "refund", "sku", "checkout", "out of stock",
            "upi", "pos terminal",
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

    # ------------------------------------------------------------------
    DomainConfig(
        name="Other",
        ml_label=5,
        exclusive_keywords=[],
        shared_keywords=[
            "employeeid", "studentid", "ticketid", "vehicleid", "deviceid",
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
# Scoring
# ---------------------------------------------------------------------------

def _score_domain(
    norm_cols: list[str],
    config: DomainConfig,
    expanded_cols: list[str] | None = None,
) -> dict[str, float]:
    """
    Score a domain against normalised columns.
    expanded_cols = alias-expanded version for this domain (optional).
    Scoring runs on BOTH raw and expanded to catch either style.
    """
    cols_to_score = norm_cols[:]
    if expanded_cols:
        # Union: include expanded aliases as additional virtual columns
        cols_to_score = list(set(norm_cols) | set(expanded_cols))

    exclusive_hits = sum(
        1 for col in cols_to_score
        if not _is_generic(col)
        and any(kw in col for kw in config.exclusive_keywords)
    )
    shared_hits = sum(
        1 for col in cols_to_score
        if not _is_generic(col)
        and any(kw in col for kw in config.shared_keywords)
        and not any(kw in col for kw in config.exclusive_keywords)
    )
    combo_hits = sum(
        1 for rule in config.combo_rules
        if all(any(kw in col for col in cols_to_score) for kw in rule)
    )

    total = (
        exclusive_hits * config.exclusive_weight
        + shared_hits * config.shared_weight
        + combo_hits * config.combo_bonus
    )
    return {
        "exclusive_hits": exclusive_hits,
        "shared_hits": shared_hits,
        "combo_hits": combo_hits,
        "total": total,
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
    Only fires when keyword scoring returns zero signal.
    Generic-only columns → Other directly, no inference.
    Low-certainty predictions → Other.
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

    Handles:
    - Full column names      (account_number, policy_no, patient_id …)
    - Abbreviated columns    (acno, pol_no, pat_id, inv_no, prod_nm …)
    - Mixed schemas          (banking + insurance overlap, etc.)
    - Fully generic schemas  (id, name, value, date → Other)

    Public methods
    --------------
    predict(table_names, all_columns, sample_values=None) → dict
    predict_domain_2step(all_columns, sample_values=None) → dict
    classify_table(df, table_name) → (is_banking, confidence, evidence)
    get_domain_split_summary(table_names, all_columns, sample_values=None) → dict
    """

    def __init__(self) -> None:
        self._ml = _MLFallback()

    # ------------------------------------------------------------------
    def predict(
        self,
        table_names: list[str],
        all_columns: list[str],
        sample_values: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Returns
        -------
        {
          domain_label      : str,
          is_banking        : bool,
          confidence        : float,        # 0–100
          percentages       : {domain: pct},
          evidence          : [str, ...],
          column_domain_map : {domain: [col, ...]},
          used_ml_fallback  : bool,
        }
        """
        norm_cols = _normalise_all(all_columns)
        values = list(sample_values or [])

        # Build alias-expanded column lists per domain
        expanded = _expand_aliases(norm_cols)

        # Keyword scoring (with alias expansion per domain)
        col_scores: dict[str, float] = {
            cfg.name: _score_domain(norm_cols, cfg, expanded.get(cfg.name))["total"]
            for cfg in DOMAIN_CONFIGS
        }

        # Blend value-level scores at reduced weight
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

        probs = _to_probs(col_scores)
        primary = max(probs, key=probs.get)
        confidence = round(probs[primary] * 100, 2)
        percentages = self._round_to_100({k: v * 100 for k, v in probs.items()})

        return {
            "domain_label": primary,
            "is_banking": primary == "Banking",
            "confidence": confidence,
            "percentages": percentages,
            "evidence": self._build_evidence(all_columns, norm_cols, values, expanded),
            "column_domain_map": self._build_column_map(all_columns, norm_cols, expanded),
            "used_ml_fallback": used_ml,
        }

    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    def classify_table(
        self, df: pd.DataFrame, table_name: str
    ) -> tuple[bool, float, list[str]]:
        columns = list(df.columns)
        sample_values: list[str] = []
        for col in df.columns:
            sample_values.extend(df[col].dropna().head(10).astype(str).tolist())
        r = self.predict(
            table_names=[table_name],
            all_columns=columns,
            sample_values=sample_values,
        )
        return r["is_banking"], r["confidence"], r["evidence"]

    # ------------------------------------------------------------------
    def get_domain_split_summary(
        self,
        table_names: list[str],
        all_columns: list[str],
        sample_values: list[str] | None = None,
    ) -> dict[str, Any]:
        r = self.predict(table_names, all_columns, sample_values)
        pcts = r["percentages"]
        colors = {
            "Banking": "#0F766E", "Finance": "#4F46E5",
            "Insurance": "#7C3AED", "Healthcare": "#14B8A6",
            "Retail": "#F59E0B", "Other": "#64748B",
        }
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

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _analyse_columns(self, columns: list[str]) -> dict[str, Any]:
        norm = _normalise_all(columns)
        expanded = _expand_aliases(norm)
        scores = {
            cfg.name: _score_domain(norm, cfg, expanded.get(cfg.name))["total"]
            for cfg in DOMAIN_CONFIGS
        }
        total = sum(scores.values())
        if total == 0:
            return {"primary_domain": "Other", "confidence": 0.0,
                    "scores": scores, "evidence": []}
        primary = max(scores, key=scores.get)
        return {
            "primary_domain": primary,
            "confidence": round(scores[primary] / total * 100, 1),
            "scores": scores,
            "evidence": self._build_evidence(columns, norm, [], expanded),
        }

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
        return {
            "primary_domain": primary,
            "confidence": round(scores[primary] / total * 100, 1),
            "scores": scores,
            "evidence": evidence,
        }

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
                    f"but row values indicate '{d2}' ({c2:.1f}%). Row data takes priority."}
        if d1 != "Other":
            return {"domain": d1, "reasoning":
                    f"Row data inconclusive; using column result '{d1}' ({c1:.1f}%)."}
        return {"domain": "Other",
                "reasoning": "Both column and value analysis are inconclusive."}

    def _build_evidence(
        self,
        columns: list[str],
        norm_cols: list[str],
        values: list[str],
        expanded: dict[str, list[str]] | None = None,
    ) -> list[str]:
        evidence: list[str] = []
        for cfg in DOMAIN_CONFIGS:
            if cfg.name == "Other":
                continue
            all_kws = cfg.exclusive_keywords + cfg.shared_keywords
            exp = (expanded or {}).get(cfg.name, norm_cols)
            hits = [
                col for col, nc, ec in zip(columns, norm_cols, exp)
                if not _is_generic(nc)
                and (any(kw in nc for kw in all_kws) or any(kw in ec for kw in all_kws))
            ]
            val_hits = [str(v) for v in values
                        if any(kw in str(v).lower() for kw in cfg.value_keywords)][:3]
            if hits:
                evidence.append(f"{cfg.name} columns: {', '.join(hits[:5])}")
            if val_hits:
                evidence.append(f"{cfg.name} values: {', '.join(val_hits)}")
        return evidence

    @staticmethod
    def _build_column_map(
        columns: list[str],
        norm_cols: list[str],
        expanded: dict[str, list[str]] | None = None,
    ) -> dict[str, list[str]]:
        result: dict[str, list[str]] = {d: [] for d in DOMAIN_NAMES}
        for i, (col, nc) in enumerate(zip(columns, norm_cols)):
            matched = False
            for cfg in DOMAIN_CONFIGS:
                if cfg.name == "Other":
                    continue
                all_kws = cfg.exclusive_keywords + cfg.shared_keywords
                ec = (expanded or {}).get(cfg.name, [nc] * len(norm_cols))[i]
                if not _is_generic(nc) and (
                    any(kw in nc for kw in all_kws) or any(kw in ec for kw in all_kws)
                ):
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
