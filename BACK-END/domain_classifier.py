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
4. Full detection layers – DB profile (dtype, length, pattern) + domain-specific
   pattern matching so classification uses data type, length, and value patterns
   (banking/finance/insurance/healthcare/retail) at prediction time.

Full detection analysis layers
------------------------------
- Layer 1: Column names – keyword + combo scoring (existing).
- Layer 2: Value keywords – match sample values to domain value_keywords (existing).
- Layer 3: DB profile – per-column data type, min/max/avg length, pattern category.
- Layer 4: Domain pattern match – regex and profile rules (IFSC, account, UPI/NEFT/RTGS,
  GSTIN, policy no, patient ID, SKU, etc.) to score when data matches domain patterns.
- Final: Combine layer scores and return domain + confidence + evidence.
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
    # Banking-specific abbreviations
    "pan": "pan",
    "aadhaar": "aadhaar",
    "aadhar": "aadhaar",
    "kyc": "kyc",
    "emi": "emi",
    "ifsc": "ifsc",
    "upi": "upi",
    "neft": "neft",
    "rtgs": "rtgs",
    "imps": "imps",
    "atm": "atm",
    "dd": "dd",
    "fd": "fixeddeposit",
    "cvv": "cvv",
    "ph": "phone",
    "phno": "phone",
    "dob": "dateofbirth",
    "nm": "name",
    "fn": "firstname",
    "ln": "lastname",
    "em": "email",
    "mob": "mobile",
    "addr": "address",
    "pin": "pincode",
    "sts": "status",
    "dt": "date",
    "ts": "timestamp",
    "tm": "time",
    "typ": "type",
    "val": "value",
    "link": "link",
    "kind": "kind",
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
            "cardnumber", "expirydate", "cardstatus", "cardtype",
            "posterminal",
            # Customer master specific
            "panno", "pannumber", "aadhaarno", "aadhaarnumber",
            "customertype", "retail", "corporate",
            # Account master specific
            "accounttype", "savings", "current", "fd", "fixeddeposit",
            "openingdate", "closingbalance",
            # Transaction specific
            "txnmode", "txnmode", "txntype", "txnamount",
            "merchantname", "closingbalance",
            # Card specific
            "cardno", "cardnumber", "issuedate", "expirydate", "cvv",
            "limitamount",
            # Loan specific
            "loantype", "tenuremonths", "emiamount", "emi",
            "outstandingamt", "outstandingamount", "loanstatus",
            "approvedby", "approveddate",
            # Branch specific
            "branchname", "managername",
        ],
        shared_keywords=[
            "accountnumber", "accountno", "accountid", "accountbalance", "accounttype",
            "branchid", "branchcode", "branchname",
            "transactionid", "transactiontype", "txnid", "txnno",
            "depositamount", "withdrawamount", "withdrawal",
            "creditamount", "debitamount",
            "logintime", "logouttime",
            "settlementdate", "authorizationcode",
            # Customer master shared
            "custid", "customerid", "firstname", "lastname",
            "emailid", "mobileno", "phno", "phonenumber",
            "dob", "dateofbirth", "age",
            "address", "city", "state", "country", "pincode",
            "createddate", "updateddate", "createdts", "updatedts",
            # Loan / EMI concepts are shared between Banking and Finance
            "loanid", "loannumber", "loantype",
            "emiamount", "emi",
            # Payment/UPI shared
            "paymentid", "upiid", "receivername", "receiverbank",
            "paymentstatus",
        ],
        combo_rules=[
            {"ifsc", "accountnumber"},
            {"swiftcode", "iban"},
            {"loanid", "emi"},
            {"accountid", "overdraft"},
            {"routing", "accountnumber"},
            {"kyc", "accountid"},
            # Customer + Account relationships
            {"custid", "accountno"},
            {"customerid", "accountnumber"},
            # Account + Transaction relationships
            {"accountno", "txnid"},
            {"accountnumber", "transactionid"},
            # Customer + Card relationships
            {"custid", "cardno"},
            {"customerid", "cardnumber"},
            # Customer + Loan relationships
            {"custid", "loanid"},
            {"customerid", "loannumber"},
            # Account + Branch relationships
            {"accountno", "branchid"},
            {"accountnumber", "ifsc"},
            # Transaction mode patterns
            {"txnmode", "txnamount"},
            {"transactionmode", "amount"},
            # KYC + Customer
            {"kyc", "custid"},
            {"kycstatus", "customerid"},
            # PAN + Customer
            {"panno", "custid"},
            {"aadhaarno", "custid"},
        ],
        value_keywords=[
            "ifsc", "swift", "iban", "emi", "overdraft",
            "neft", "rtgs", "imps", "cheque", "standing instruction",
            "upi", "atm", "card", "netbanking", "dd",
            "savings", "current", "fd", "fixed deposit",
            "retail", "corporate",
            "verified", "pending", "rejected", "kyc",
            "debit", "credit",
            "active", "closed", "inactive",
            "home loan", "car loan", "personal loan", "education loan",
            "success", "fail", "failed", "pending",
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
            # Customer master patterns
            "cust_id first_name last_name gender dob age email_id mobile_no",
            "kyc_status pan_no aadhaar_no customer_type retail corporate",
            "address city state country pincode created_date updated_date",
            # Account master patterns
            "account_no cust_id account_type savings current fd branch_id ifsc_code",
            "opening_date balance status active closed interest_rate min_balance",
            # Transaction patterns
            "txn_id account_no txn_date txn_time txn_amount txn_type debit credit",
            "txn_mode upi neft rtgs imps atm cheque merchant_name location",
            "status success fail closing_balance",
            # Card patterns
            "card_no cust_id account_no card_type debit credit issue_date expiry_date",
            "cvv card_status limit_amount",
            # Loan patterns
            "loan_id cust_id loan_type home car personal education loan_amount",
            "tenure_months emi_amount start_date end_date outstanding_amt loan_status",
            "approved_by approved_date",
            # Branch patterns
            "branch_id branch_name ifsc_code city state manager_name contact_no",
            # Payment/UPI patterns
            "payment_id txn_id upi_id receiver_name receiver_bank payment_status",
        ],
    ),

    DomainConfig(
        name="Finance",
        ml_label=1,
        exclusive_keywords=[
            # Investment specific
            "investment", "invamt", "invamount", "investmentamount",
            "mutualfund", "mf", "mfid", "mfscheme", "nav", "netassetvalue",
            "stock", "stockid", "stockcode", "stockname", "share", "shares",
            "trading", "tradeid", "tradeid", "tradetype", "buy", "sell",
            "portfolio", "portfolioid", "holdings",
            "roi", "returnoninvestment", "yield", "dividend",
            "fd", "fixeddeposit", "recurringdeposit", "rd",
            # Ledger & Accounting
            "ledgerid", "ledgername", "journalid", "voucherno",
            "accountspayable", "accountsreceivable",
            "costcenter", "fiscalyear",
            # Tax & Compliance
            "gst", "gstin", "cgst", "sgst", "igst",
            "tds", "pf", "esi",
            # Financial statements
            "grosssalary", "netsalary", "payrollmonth",
            "cogs", "ebitda", "profitloss", "profitandloss",
            # Finance file identifiers
            "financeid", "finid", "fintype",
        ],
        shared_keywords=[
            # Accounting & Bookkeeping
            "invoice", "invoiceid", "invoiceno",
            "tax", "taxamount",
            "payroll", "expense", "budget",
            "profit", "loss", "revenue",
            "receivable", "payable",
            "debit", "credit",
            "vendor", "supplier",
            # Investment related (shared with banking for FD)
            "interest", "interestrate", "intrate", "int_rate",
            "tenure", "maturity",
        ],
        combo_rules=[
            # Investment patterns
            {"investment", "roi"},
            {"stock", "trading"},
            {"mutualfund", "nav"},
            {"portfolio", "holdings"},
            {"tradeid", "buy"},
            {"tradeid", "sell"},
            # Ledger patterns
            {"invoice", "gst"},
            {"invoice", "tax", "paymentmode"},
            {"ledger", "journal"},
            {"profit", "loss", "revenue"},
            # Accounting patterns
            {"salary", "payroll"},
            {"gst", "tax"},
            {"tds", "salary"},
            {"accounts", "payable"},
            {"accounts", "receivable"},
        ],
        value_keywords=[
            # Investment keywords
            "investment", "mutual fund", "mf", "nav", "stock", "share",
            "trading", "buy", "sell", "portfolio", "roi", "yield", "dividend",
            "fd", "fixed deposit", "rd", "recurring deposit",
            # Accounting keywords
            "gst", "gstin", "tds", "payroll", "invoice",
            "debit note", "credit note", "journal entry", "ledger",
            "accounts payable", "accounts receivable",
            "profit", "loss", "revenue", "expense",
        ],
        ml_samples=[
            # Investment & Trading
            "investment_id investment_amount roi yield dividend portfolio",
            "stock_id stock_code stock_name shares buy_price sell_price trading",
            "mutual_fund_id mf_scheme nav units purchase_date redemption_date",
            "trade_id trade_type buy sell stock_code quantity price trade_date",
            "portfolio_id holdings stock_code shares current_value",
            "fd_id fixed_deposit amount interest_rate tenure maturity_date",
            # Ledger & Accounting
            "ledger_id ledger_name journal_id voucher_no narration debit credit",
            "accounts_payable supplier_id bill_no amount_due paid_amount",
            "accounts_receivable receipt_no amount_received balance_due",
            "invoice_no gst gstin tax_amount cgst sgst igst taxable_value",
            # Financial statements
            "profit loss revenue cogs margin fiscal_year quarter ebitda",
            "budget_amount planned_amount actual_amount variance cost_center",
            "expense_type expense_amount cost_center vendor_name",
            "payroll_month gross_salary net_salary tds pf esi deductions",
        ],
    ),

    DomainConfig(
        name="Insurance",
        ml_label=2,
        exclusive_keywords=[
            # Policy specific
            "policyno", "policyid", "policynumber", "policystartdate", "policyenddate",
            "policytype", "policystatus",
            # Premium specific
            "premiumamt", "premamt", "totalpremium", "premiumamount",
            "premiumduedate", "premiumpaid", "premiumstatus",
            # Sum Assured specific
            "suminsured", "sumassured", "limamt", "limitamount", "coverageamount",
            # Claim specific
            "claimid", "claimamount", "claimstatus", "settlementamount",
            "claimnumber", "claimtype", "claimdate",
            # Nominee specific
            "nominee", "nomineename", "nomineeid",
            # Maturity specific
            "maturity", "maturitydate", "maturityamount", "maturityvalue",
            # Underwriting
            "underwriting", "underwritingscore", "underwritingstatus",
            # Deductible & Coverage
            "deductible", "dedamt", "deductibleamount",
            "coverage", "coveragetype", "coverageamount",
            # Policy lifecycle
            "renewaldate", "lapsedate", "graceperiod", "lapsestatus",
            # Insurance codes
            "lob", "lobcd", "covcd", "lineofbusiness",
            # Reserves
            "paidamt", "rsvamt", "reserveamount",
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
            # Policy + Premium
            {"policy", "premium"},
            {"policyno", "premiumamt"},
            # Policy + Sum Assured
            {"policy", "suminsured"},
            {"policyno", "sumassured"},
            # Claim + Policy
            {"claim", "policy"},
            {"claimid", "policyno"},
            # Policy + Nominee
            {"nominee", "policy"},
            {"policyno", "nominee"},
            # Policy + Maturity
            {"policy", "maturity"},
            {"policyno", "maturitydate"},
            # Coverage + Deductible
            {"coverage", "deductible"},
            {"suminsured", "deductible"},
            # Policy + Beneficiary
            {"policy", "beneficiary"},
            # Premium + Claim
            {"premium", "claim"},
        ],
        value_keywords=[
            # Policy keywords
            "policy", "policy number", "policy type", "policy status",
            # Premium keywords
            "premium", "premium paid", "premium due", "premium amount",
            # Claim keywords
            "claim", "claim approved", "claim amount", "claim status", "settlement",
            # Sum Assured keywords
            "sum insured", "sum assured", "coverage amount", "limit amount",
            # Nominee keywords
            "nominee", "beneficiary",
            # Maturity keywords
            "maturity", "maturity date", "maturity amount", "maturity value",
            # Underwriting keywords
            "underwriting", "underwriting score", "underwriting status",
            # Policy lifecycle
            "policy lapsed", "renewal", "grace period",
        ],
        ml_samples=[
            # Policy & Premium
            "policy_no policy_type insured_name nominee sum_insured premium",
            "policy_id policy_start_date policy_end_date renewal_date premium_amount",
            "premium_due_date premium_paid premium_status total_premium",
            # Claim
            "claim_id claim_date claim_amount claim_status settlement_amount",
            "claim_number claim_type policy_no claim_approved claim_rejected",
            # Sum Assured & Coverage
            "sum_insured sum_assured coverage_amount limit_amount deductible",
            "coverage_type rider coverage_amount sum_insured",
            # Nominee & Beneficiary
            "nominee nominee_name nominee_id beneficiary policy_no",
            # Maturity
            "maturity_date maturity_amount maturity_value policy_no",
            # Underwriting
            "underwriting_score underwriting_status rider coverage_type deductible copay co_insurance",
            # Policy lifecycle
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
# Strict keyword sets for Banking / Finance / Insurance decision rules
# ---------------------------------------------------------------------------
# Split into EXCLUSIVE (unique to one domain) and SHARED (common across
# Banking and Finance).  Exclusive keywords get 3x weight; shared get 1x.
# This prevents Finance data with columns like 'credit', 'debit', 'account'
# from being misclassified as Banking.

# --- Banking: EXCLUSIVE (only Banking, not Finance) -----------------------
STRICT_BANKING_EXCLUSIVE: frozenset[str] = frozenset({
    "ifsc", "atm", "upi", "txn", "transaction",
})

# --- Finance: EXCLUSIVE (only Finance, not Banking) -----------------------
STRICT_FINANCE_EXCLUSIVE: frozenset[str] = frozenset({
    "investment", "mf", "roi", "trade", "profit", "loss", "ledger",
})

# --- Shared between Banking and Finance (count for BOTH equally) ----------
STRICT_BANKING_FINANCE_SHARED: frozenset[str] = frozenset({
    "acc", "account", "balance", "debit", "credit", "loan",
    "inv", "fd",
})

# --- Insurance: all keywords are exclusive --------------------------------
STRICT_INSURANCE_KEYWORDS: frozenset[str] = frozenset({
    "policy", "premium", "claim", "sumassured", "sum_assured",
    "nominee", "maturity",
})


def _strict_result(winner: str, hits: dict[str, int]) -> dict[str, Any]:
    """
    Build a result dict from the strict-rules layer.

    Produces ``percentages`` across all six DOMAIN_NAMES so the rest of
    the pipeline (UI, charts, evidence) keeps working unchanged.
    """
    total = sum(hits.values()) or 1
    pcts: dict[str, float] = {}
    for d in DOMAIN_NAMES:
        if d in hits:
            pcts[d] = round(hits[d] / total * 100, 1)
        else:
            pcts[d] = 0.0

    # Make sure they sum to 100
    diff = round(100.0 - sum(pcts.values()), 1)
    if diff != 0:
        pcts[winner] = round(pcts[winner] + diff, 1)

    return {
        "domain": winner,
        "percentages": pcts,
    }


# ---------------------------------------------------------------------------
# DB profile: data type, length, pattern per column
# ---------------------------------------------------------------------------

@dataclass
class ColumnProfile:
    """Per-column profile: inferred type, length stats, and pattern category."""
    column_name: str
    dtype: str          # "int" | "float" | "str" | "datetime" | "bool" | "mixed"
    min_len: int
    max_len: int
    avg_len: float
    pattern: str        # "id_like" | "amount_like" | "date_like" | "code_like" | "ifsc_like" | "account_like" | "phone_like" | "gstin_like" | "policy_like" | "sku_like" | "unknown"
    sample_count: int


def _infer_dtype(values: list[str]) -> str:
    """Infer dominant dtype from string values."""
    if not values:
        return "str"
    int_c, float_c, dt_c, bool_c = 0, 0, 0, 0
    for v in values[:500]:
        s = str(v).strip()
        if not s or s.lower() in ("nan", "none", ""):
            continue
        try:
            int(s.replace(",", ""))
            int_c += 1
            continue
        except ValueError:
            pass
        try:
            float(s.replace(",", ""))
            float_c += 1
            continue
        except ValueError:
            pass
        if re.match(r"^\d{4}-\d{2}-\d{2}([T\s]\d{2}:\d{2}:\d{2})?$", s) or re.match(r"^\d{2}/\d{2}/\d{4}", s):
            dt_c += 1
            continue
        if s.lower() in ("true", "false", "yes", "no", "1", "0"):
            bool_c += 1
            continue
    total = int_c + float_c + dt_c + bool_c
    if total == 0:
        return "str"
    if int_c >= total * 0.7:
        return "int"
    if float_c >= total * 0.7:
        return "float"
    if dt_c >= total * 0.5:
        return "datetime"
    if bool_c >= total * 0.7:
        return "bool"
    if int_c + float_c > total * 0.4:
        return "mixed"
    return "str"


def _str_len(s: str) -> int:
    return len(str(s).strip()) if s is not None and str(s).strip() else 0


def _infer_pattern(column_name: str, dtype: str, values: list[str], min_len: int, max_len: int) -> str:
    """Infer pattern category from column name, dtype, and value stats."""
    norm = _normalise(column_name)
    expanded = _expand_norm_col(norm)
    sample_vals = [str(v).strip() for v in values[:200] if v is not None and str(v).strip()]

    # IFSC: 11 char alphanumeric, 4 letter bank, 0, 6 alphanumeric
    if any("ifsc" in v for v in expanded) or (4 <= max_len <= 12 and sample_vals):
        for v in sample_vals:
            if re.match(r"^[A-Z]{4}0[A-Z0-9]{6}$", str(v).upper()):
                return "ifsc_like"
    # Account number: often numeric or alphanumeric (e.g. A1001), 4–24 chars
    # Observe pattern: starts with letter followed by digits (A1001, C101), or pure numeric
    if any(x in "".join(expanded) for x in ["account", "accno", "acct"]) and dtype in ("int", "str"):
        if 4 <= max_len <= 24:
            # Pattern 1: Pure numeric (8-18 digits typical)
            if dtype == "int" or (sample_vals and sum(1 for v in sample_vals[:20] if v and str(v).isdigit() and 8 <= len(str(v)) <= 18) >= len(sample_vals[:20]) * 0.7):
                return "account_like"
            # Pattern 2: Alphanumeric starting with letter (A1001, C101, etc.)
            if dtype == "str" and sample_vals:
                letter_digit_pattern = sum(1 for v in sample_vals[:20] if v and re.match(r"^[A-Z][0-9]{3,}$", str(v).upper()))
                alphanum_pattern = sum(1 for v in sample_vals[:20] if v and str(v).replace(" ", "").isalnum() and 4 <= len(str(v)) <= 24)
                if letter_digit_pattern >= len(sample_vals[:20]) * 0.5 or (alphanum_pattern >= len(sample_vals[:20]) * 0.7):
                    return "account_like"
    # Amount / numeric
    if dtype == "float" or (dtype == "int" and any(x in "".join(expanded) for x in ["amt", "amount", "balance", "value", "price", "sum"])):
        return "amount_like"
    # Date
    if dtype == "datetime" or any(x in "".join(expanded) for x in ["date", "dt", "ts", "time"]):
        return "date_like"
    # Customer ID pattern: C followed by digits (C101, C102) - observe from data
    if any(x in "".join(expanded) for x in ["custid", "customerid", "cust_id"]) and 4 <= max_len <= 10:
        if sample_vals:
            cust_pattern_match = sum(1 for v in sample_vals[:20] if v and re.match(r"^C\d{3,}$", str(v).upper()))
            if cust_pattern_match >= len(sample_vals[:20]) * 0.6:
                return "id_like"  # Strong indicator of banking customer IDs
    
    # Transaction ID pattern: T followed by digits (T9001, T9002) - observe from data
    if any(x in "".join(expanded) for x in ["txnid", "transactionid", "txn_id"]) and 4 <= max_len <= 10:
        if sample_vals:
            txn_pattern_match = sum(1 for v in sample_vals[:20] if v and re.match(r"^T\d{3,}$", str(v).upper()))
            if txn_pattern_match >= len(sample_vals[:20]) * 0.6:
                return "id_like"  # Strong indicator of banking transaction IDs
    
    # Loan ID pattern: L followed by digits (L5001, L5002) - observe from data
    if any(x in "".join(expanded) for x in ["loanid", "loannumber", "loan_id"]) and 4 <= max_len <= 10:
        if sample_vals:
            loan_pattern_match = sum(1 for v in sample_vals[:20] if v and re.match(r"^L\d{3,}$", str(v).upper()))
            if loan_pattern_match >= len(sample_vals[:20]) * 0.6:
                return "id_like"  # Strong indicator of banking loan IDs
    
    # ID-like: short codes, alphanumeric
    if any(x in "".join(expanded) for x in ["id", "no", "num", "code"]) and max_len <= 24:
        return "id_like"
    # Phone
    if any(x in "".join(expanded) for x in ["phone", "ph", "mobile", "contact"]) and 10 <= max_len <= 15:
        if sample_vals and sum(1 for v in sample_vals if re.match(r"^\+?[\d\s\-]{10,15}$", str(v))) > len(sample_vals) * 0.5:
            return "phone_like"
    # PAN: 10 char alphanumeric (5 letters, 4 digits, 1 letter)
    if any("pan" in v for v in expanded) and 9 <= max_len <= 11:
        for v in sample_vals:
            if re.match(r"^[A-Z]{5}[0-9]{4}[A-Z]$", str(v).upper()):
                return "pan_like"
    # Aadhaar: 12 digits
    if any("aadhaar" in v or "aadhar" in v for v in expanded) and 11 <= max_len <= 13:
        for v in sample_vals:
            if re.match(r"^\d{12}$", str(v).replace(" ", "").replace("-", "")):
                return "aadhaar_like"
    # UPI ID: typically contains @ symbol (e.g., user@paytm)
    if any("upi" in v for v in expanded) and 5 <= max_len <= 50:
        for v in sample_vals:
            if "@" in str(v) and len(str(v).split("@")) == 2:
                return "upi_like"
    # Transaction mode: UPI, NEFT, RTGS, IMPS, ATM, etc.
    if any(x in "".join(expanded) for x in ["txnmode", "txnmode", "transactionmode", "paymentmode"]) and 2 <= max_len <= 15:
        for v in sample_vals:
            if str(v).upper() in ["UPI", "NEFT", "RTGS", "IMPS", "ATM", "CHEQUE", "DD", "CARD", "NETBANKING"]:
                return "txn_mode_like"
    # Customer type: Retail/Corporate
    if any(x in "".join(expanded) for x in ["customertype", "custtype"]) and 3 <= max_len <= 15:
        for v in sample_vals:
            if str(v).upper() in ["RETAIL", "CORPORATE", "INDIVIDUAL", "BUSINESS"]:
                return "customer_type_like"
    # Account type: Savings/Current/FD
    if any(x in "".join(expanded) for x in ["accounttype", "accttype"]) and 3 <= max_len <= 15:
        for v in sample_vals:
            if str(v).upper() in ["SAVINGS", "CURRENT", "FD", "FIXED DEPOSIT", "SALARY"]:
                return "account_type_like"
    # KYC status: Verified/Pending/Rejected
    if any("kyc" in v for v in expanded) and 3 <= max_len <= 15:
        for v in sample_vals:
            if str(v).upper() in ["VERIFIED", "PENDING", "REJECTED", "IN PROGRESS", "COMPLETED"]:
                return "kyc_status_like"
    # Card type: Debit/Credit
    if any(x in "".join(expanded) for x in ["cardtype", "cardtype"]) and 3 <= max_len <= 10:
        for v in sample_vals:
            if str(v).upper() in ["DEBIT", "CREDIT", "PREPAID"]:
                return "card_type_like"
    # Loan type: Home/Car/Personal/Education
    if any(x in "".join(expanded) for x in ["loantype", "loantype"]) and 3 <= max_len <= 20:
        for v in sample_vals:
            if str(v).upper() in ["HOME", "CAR", "PERSONAL", "EDUCATION", "BUSINESS", "GOLD"]:
                return "loan_type_like"
    # GSTIN: 15 char alphanumeric
    if any("gst" in v for v in expanded) and 14 <= max_len <= 16:
        for v in sample_vals:
            if re.match(r"^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z][0-9A-Z]Z[0-9A-Z]$", str(v).upper()):
                return "gstin_like"
    # Finance: Investment patterns - MF codes, stock codes, trade IDs
    if any(x in "".join(expanded) for x in ["mf", "mutualfund", "investment"]) and 4 <= max_len <= 12:
        for v in sample_vals:
            if re.match(r"^[A-Z]{2,4}\d{4,6}$", str(v).upper()):  # MF codes like MF123456
                return "mf_code_like"
    if any(x in "".join(expanded) for x in ["stock", "share", "equity"]) and 2 <= max_len <= 8:
        for v in sample_vals:
            if re.match(r"^[A-Z]{2,6}$", str(v).upper()):  # Stock ticker codes like RELIANCE, TCS
                return "stock_code_like"
    if any(x in "".join(expanded) for x in ["trade", "trading"]) and 7 <= max_len <= 15:
        for v in sample_vals:
            if re.match(r"^TRD[A-Z0-9]{4,12}$", str(v).upper()):  # Trade IDs like TRD123456
                return "trade_id_like"
    # Finance: Ledger patterns
    if any(x in "".join(expanded) for x in ["ledger", "journal", "voucher"]) and 6 <= max_len <= 16:
        for v in sample_vals:
            if re.match(r"^(LED|JRN|VCH)[A-Z0-9]{4,12}$", str(v).upper()):  # Ledger/Journal/Voucher IDs
                return "ledger_like"
    # Finance: Portfolio patterns
    if any("portfolio" in v for v in expanded) and 8 <= max_len <= 16:
        for v in sample_vals:
            if re.match(r"^PORT[A-Z0-9]{4,12}$", str(v).upper()):  # Portfolio IDs
                return "portfolio_like"
    # Finance: ROI/Yield patterns (percentage values)
    if any(x in "".join(expanded) for x in ["roi", "yield", "return"]) and 1 <= max_len <= 10:
        for v in sample_vals:
            if re.match(r"^\d+\.?\d*\s*%?$", str(v)):  # Percentage values like "12.5%" or "12.5"
                return "roi_like"
    
    # Insurance: Policy patterns
    if any(x in "".join(expanded) for x in ["policy", "pol"]) and 6 <= max_len <= 20:
        for v in sample_vals:
            if re.match(r"^POL[A-Z0-9]{4,12}$|^[A-Z]{2,4}\d{6,12}$", str(v).upper()):  # POL123456 or AB12345678
                return "policy_like"
    # Insurance: Claim patterns
    if any(x in "".join(expanded) for x in ["claim", "clm"]) and 7 <= max_len <= 20:
        for v in sample_vals:
            if re.match(r"^CLM[A-Z0-9]{4,12}$|^CLM\d{6,12}$", str(v).upper()):  # CLM123456
                return "claim_like"
    # Insurance: Sum Assured patterns (typically large amounts, 5+ digits)
    if any(x in "".join(expanded) for x in ["suminsured", "sumassured", "coverageamount"]) and dtype in ("int", "float"):
        if max_len >= 5:  # Usually 5+ digits for sum assured
            return "sum_assured_like"
    # Insurance: Nominee patterns
    if any("nominee" in v for v in expanded) and 7 <= max_len <= 15:
        for v in sample_vals:
            if re.match(r"^NOM[A-Z0-9]{4,12}$", str(v).upper()):  # Nominee IDs
                return "nominee_like"
    # Insurance: Premium patterns (observe from column name and values)
    if any(x in "".join(expanded) for x in ["premium", "prem"]) and dtype in ("int", "float"):
        if 1 <= max_len <= 15:
            return "premium_like"
    # Insurance: Maturity patterns
    if any("maturity" in v for v in expanded) and dtype == "datetime":
        return "maturity_like"
    if any("maturity" in v for v in expanded) and dtype in ("int", "float") and max_len >= 5:
        return "maturity_amount_like"
    
    # Policy / claim ID (generic)
    if any(x in "".join(expanded) for x in ["policy", "pol", "claim", "clm"]) and 4 <= max_len <= 24:
        return "policy_like"
    # SKU / product code
    if any(x in "".join(expanded) for x in ["sku", "product", "item"]) and 3 <= max_len <= 32:
        return "sku_like"
    # Code-like (short string)
    if max_len <= 20 and dtype == "str":
        return "code_like"
    return "unknown"


def build_db_profile(
    column_names: list[str],
    column_values: dict[str, list[Any]] | None = None,
    df: pd.DataFrame | None = None,
) -> list[ColumnProfile]:
    """
    Build per-column DB profile: data type, min/max/avg length, pattern.
    Provide either column_values (dict of column name -> list of sample values) or df.
    """
    if df is not None:
        column_names = list(df.columns)
        column_values = {
            col: df[col].dropna().head(200).astype(str).tolist()
            for col in df.columns
        }
    if not column_values:
        return [
            ColumnProfile(
                column_name=c,
                dtype="str",
                min_len=0,
                max_len=0,
                avg_len=0.0,
                pattern="unknown",
                sample_count=0,
            )
            for c in column_names
        ]

    profiles: list[ColumnProfile] = []
    for col in column_names:
        values = column_values.get(col, [])
        str_vals = [str(v).strip() for v in values if v is not None and str(v).strip()]
        lens = [_str_len(v) for v in str_vals]
        dtype = _infer_dtype(str_vals)
        min_len = min(lens) if lens else 0
        max_len = max(lens) if lens else 0
        avg_len = sum(lens) / len(lens) if lens else 0.0
        pattern = _infer_pattern(col, dtype, str_vals, min_len, max_len)
        profiles.append(ColumnProfile(
            column_name=col,
            dtype=dtype,
            min_len=min_len,
            max_len=max_len,
            avg_len=round(avg_len, 2),
            pattern=pattern,
            sample_count=len(str_vals),
        ))
    return profiles


# ---------------------------------------------------------------------------
# Domain-specific value patterns (regex + profile expectations)
# ---------------------------------------------------------------------------

@dataclass
class DomainPatternRule:
    """One rule: regex to match values and optional profile (dtype/length) hint."""
    name: str
    regex: str
    expected_dtype: str | None = None   # optional
    expected_min_len: int | None = None
    expected_max_len: int | None = None
    weight: float = 1.0


# Banking & Finance: IFSC, account, UPI/NEFT/RTGS/IMPS, card, SWIFT/IBAN
# Finance: GSTIN, invoice number, amount patterns
# Insurance: policy number, claim ID
# Healthcare: patient ID, ICD code, MR number
# Retail: SKU, order ID, barcode
DOMAIN_PATTERN_RULES: dict[str, list[DomainPatternRule]] = {
    "Banking": [
        DomainPatternRule("ifsc", r"^[A-Z]{4}0[A-Z0-9]{6}$", "str", 11, 11, 3.0),
        DomainPatternRule("account_num", r"^[A-Z0-9]{4,24}$", "str", 4, 24, 2.0),  # More flexible: A1001, 1234567890, etc.
        DomainPatternRule("txn_mode", r"^(UPI|NEFT|RTGS|IMPS|ATM|CHEQUE|DD|CARD|NETBANKING)$", "str", 2, 15, 2.5),
        DomainPatternRule("neft_rtgs", r"^(NEFT|RTGS|IMPS)$", None, None, None, 2.0),
        DomainPatternRule("swift", r"^[A-Z]{6}[A-Z0-9]{2}([A-Z0-9]{3})?$", "str", 8, 11, 2.5),
        DomainPatternRule("iban", r"^[A-Z]{2}[0-9]{2}[A-Z0-9]{4}[0-9]{7}([A-Z0-9]{0,16})?$", "str", 15, 34, 2.5),
        # PAN: 10 char (5 letters, 4 digits, 1 letter)
        DomainPatternRule("pan", r"^[A-Z]{5}[0-9]{4}[A-Z]$", "str", 10, 10, 3.0),
        # Aadhaar: 12 digits (may have spaces/dashes)
        DomainPatternRule("aadhaar", r"^\d{4}[\s\-]?\d{4}[\s\-]?\d{4}$", "str", 12, 14, 3.0),
        # UPI ID: contains @ symbol
        DomainPatternRule("upi_id", r"^[a-zA-Z0-9._-]+@[a-zA-Z0-9.-]+$", "str", 5, 50, 2.5),
        # Customer ID: C followed by digits (C101, C102, etc.)
        DomainPatternRule("cust_id", r"^C\d{3,}$", "str", 4, 10, 1.5),
        # Account number: A followed by digits (A1001, A1002, etc.)
        DomainPatternRule("acct_num", r"^A\d{3,}$", "str", 4, 10, 1.5),
        # Transaction ID: T followed by digits (T9001, T9002, etc.)
        DomainPatternRule("txn_id", r"^T\d{3,}$", "str", 4, 10, 1.5),
        # Loan ID: L followed by digits (L5001, L5002, etc.)
        DomainPatternRule("loan_id", r"^L\d{3,}$", "str", 4, 10, 1.5),
        # Card number: 16 digits (may have spaces/dashes)
        DomainPatternRule("card_num", r"^\d{4}[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{4}$", "str", 16, 19, 2.0),
        # CVV: 3-4 digits
        DomainPatternRule("cvv", r"^\d{3,4}$", "str", 3, 4, 2.0),
        # KYC status values
        DomainPatternRule("kyc_status", r"^(VERIFIED|PENDING|REJECTED|IN PROGRESS|COMPLETED)$", "str", 3, 15, 1.5),
        # Customer type
        DomainPatternRule("customer_type", r"^(RETAIL|CORPORATE|INDIVIDUAL|BUSINESS)$", "str", 3, 15, 1.5),
        # Account type
        DomainPatternRule("account_type", r"^(SAVINGS|CURRENT|FD|FIXED DEPOSIT|SALARY)$", "str", 2, 15, 1.5),
        # Card type
        DomainPatternRule("card_type", r"^(DEBIT|CREDIT|PREPAID)$", "str", 3, 10, 1.5),
        # Loan type
        DomainPatternRule("loan_type", r"^(HOME|CAR|PERSONAL|EDUCATION|BUSINESS|GOLD)$", "str", 3, 20, 1.5),
        # Transaction type
        DomainPatternRule("txn_type", r"^(DEBIT|CREDIT)$", "str", 3, 10, 1.5),
        # Account status
        DomainPatternRule("account_status", r"^(ACTIVE|CLOSED|INACTIVE|SUSPENDED)$", "str", 3, 15, 1.5),
        # Transaction status
        DomainPatternRule("txn_status", r"^(SUCCESS|FAIL|FAILED|PENDING|COMPLETED)$", "str", 3, 15, 1.5),
    ],
    "Finance": [
        # GSTIN pattern
        DomainPatternRule("gstin", r"^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z][0-9A-Z]Z[0-9A-Z]$", "str", 15, 15, 3.0),
        # Invoice pattern
        DomainPatternRule("invoice_no", r"^INV[-_]?[A-Z0-9]{4,12}$", "str", 6, 20, 1.5),
        # Investment patterns
        DomainPatternRule("mf_code", r"^[A-Z]{2,4}\d{4,6}$", "str", 6, 10, 2.0),  # Mutual fund codes
        DomainPatternRule("stock_code", r"^[A-Z]{2,6}$", "str", 2, 6, 2.0),  # Stock ticker codes
        DomainPatternRule("trade_id", r"^TRD[A-Z0-9]{4,12}$", "str", 7, 15, 2.0),  # Trade IDs
        DomainPatternRule("portfolio_id", r"^PORT[A-Z0-9]{4,12}$", "str", 8, 16, 1.5),  # Portfolio IDs
        # Ledger patterns
        DomainPatternRule("voucher", r"^[A-Z]*\d{6,12}$", "str", 6, 16, 1.0),
        DomainPatternRule("journal_id", r"^JRN[A-Z0-9]{4,12}$", "str", 7, 15, 1.5),
        DomainPatternRule("ledger_id", r"^LED[A-Z0-9]{4,12}$", "str", 7, 15, 1.5),
        # Amount patterns
        DomainPatternRule("amount_numeric", r"^\d+(\.\d{2})?$", "float", 1, 20, 0.5),
        # ROI/Yield patterns (percentage values)
        DomainPatternRule("roi_yield", r"^\d+\.?\d*\s*%?$", "str", 1, 10, 1.5),
    ],
    "Insurance": [
        # Policy patterns
        DomainPatternRule("policy_no", r"^POL[A-Z0-9]{4,12}$|^[A-Z]{2,4}\d{6,12}$", "str", 6, 20, 3.0),  # POL123456 or AB12345678
        DomainPatternRule("policy_id", r"^POL[A-Z0-9]{4,12}$", "str", 7, 15, 2.5),
        # Claim patterns
        DomainPatternRule("claim_id", r"^CLM[A-Z0-9]{4,12}$", "str", 7, 20, 3.0),
        DomainPatternRule("claim_no", r"^CLM\d{6,12}$", "str", 9, 15, 2.5),
        # Premium patterns
        DomainPatternRule("premium_amt", r"^\d+(\.\d{2})?$", "float", 1, 15, 2.0),
        # Sum Assured patterns (typically large amounts)
        DomainPatternRule("sum_assured", r"^\d{5,}(\.\d{2})?$", "float", 5, 20, 2.5),  # Usually 5+ digits
        # Nominee ID patterns
        DomainPatternRule("nominee_id", r"^NOM[A-Z0-9]{4,12}$", "str", 7, 15, 2.0),
        # Maturity patterns
        DomainPatternRule("maturity_date", r"^\d{4}-\d{2}-\d{2}$", "str", 10, 10, 1.5),
        # Policy status values
        DomainPatternRule("policy_status", r"^(ACTIVE|LAPSED|EXPIRED|RENEWED|PENDING)$", "str", 3, 15, 1.5),
        # Claim status values
        DomainPatternRule("claim_status", r"^(APPROVED|REJECTED|PENDING|SETTLED|UNDER_REVIEW)$", "str", 3, 20, 1.5),
    ],
    "Healthcare": [
        DomainPatternRule("patient_id", r"^P[T]?[A-Z0-9]{4,12}$", "str", 5, 16, 2.0),
        DomainPatternRule("icd_code", r"^[A-Z]\d{2}(\.\d{2})?$", "str", 3, 8, 2.5),
        DomainPatternRule("mrn", r"^MRN?[0-9]{4,12}$", "str", 6, 16, 1.5),
    ],
    "Retail": [
        DomainPatternRule("sku", r"^(?=.*[A-Z])[A-Z0-9\-_]{6,24}$", "str", 6, 24, 2.0),  # 6+ chars, at least one letter (avoid txn/account ids)
        DomainPatternRule("order_id", r"^ORD[A-Z0-9\-]{4,16}$", "str", 7, 24, 1.5),
        DomainPatternRule("product_code", r"^[A-Z0-9]{6,14}$", "str", 6, 14, 1.0),
    ],
    "Other": [],
}


def _score_value_patterns(sample_values: list[str], domain: str) -> float:
    """Score how many sample values match domain-specific regex patterns."""
    rules = DOMAIN_PATTERN_RULES.get(domain, [])
    if not rules or not sample_values:
        return 0.0
    total = 0.0
    matched_patterns = set()  # Track which patterns matched to avoid double counting
    
    for val in sample_values[:300]:
        s = str(val).strip()
        if not s or s.lower() in ("nan", "none", ""):
            continue
        
        # Try each rule, but only count once per value
        for rule in rules:
            # Check expected length if specified
            if rule.expected_min_len is not None and len(s) < rule.expected_min_len:
                continue
            if rule.expected_max_len is not None and len(s) > rule.expected_max_len:
                continue
            
            if re.match(rule.regex, s, re.IGNORECASE):
                # For banking domain, give bonus for multiple pattern matches
                pattern_key = f"{domain}:{rule.name}"
                if pattern_key not in matched_patterns:
                    total += rule.weight
                    matched_patterns.add(pattern_key)
                else:
                    # Still count but with reduced weight for repeated patterns
                    total += rule.weight * 0.3
                break  # Only match one pattern per value
    
    # Bonus for banking: if we see multiple distinct banking patterns, increase score
    if domain == "Banking" and len(matched_patterns) >= 3:
        total *= 1.2
    
    return total


def _score_profile_against_domain(profiles: list[ColumnProfile], domain: str) -> float:
    """
    Score DB profile layer: how many columns match this domain's expected
    pattern types (ifsc_like, account_like, amount_like, gstin_like, etc.).
    """
    pattern_to_domain: dict[str, list[str]] = {
        "ifsc_like": ["Banking"],
        "account_like": ["Banking", "Finance"],
        "amount_like": ["Banking", "Finance", "Insurance", "Retail"],
        "date_like": ["Banking", "Finance", "Insurance", "Healthcare", "Retail"],
        "code_like": ["Finance", "Insurance", "Healthcare", "Retail"],
        "gstin_like": ["Finance"],
        "policy_like": ["Insurance"],
        "phone_like": ["Healthcare", "Retail", "Other"],
        "sku_like": ["Retail"],
        "id_like": ["Banking", "Finance", "Insurance", "Healthcare", "Retail"],
        # Banking-specific patterns
        "pan_like": ["Banking"],
        "aadhaar_like": ["Banking"],
        "upi_like": ["Banking"],
        "txn_mode_like": ["Banking"],
        "customer_type_like": ["Banking"],
        "account_type_like": ["Banking"],
        "kyc_status_like": ["Banking"],
        "card_type_like": ["Banking"],
        "loan_type_like": ["Banking"],
        # Finance-specific patterns
        "mf_code_like": ["Finance"],
        "stock_code_like": ["Finance"],
        "trade_id_like": ["Finance"],
        "ledger_like": ["Finance"],
        "portfolio_like": ["Finance"],
        "roi_like": ["Finance"],
        # Insurance-specific patterns
        "claim_like": ["Insurance"],
        "sum_assured_like": ["Insurance"],
        "nominee_like": ["Insurance"],
        "premium_like": ["Insurance"],
        "maturity_like": ["Insurance"],
        "maturity_amount_like": ["Insurance"],
    }
    score = 0.0
    for p in profiles:
        if p.pattern == "unknown" or p.sample_count == 0:
            continue
        domains_for_pattern = pattern_to_domain.get(p.pattern, [])
        if domain in domains_for_pattern:
            # Stronger weight for domain-specific patterns
            if (domain == "Banking" and p.pattern in ["ifsc_like", "pan_like", "aadhaar_like", "upi_like"]) or \
               (domain == "Finance" and p.pattern in ["gstin_like", "mf_code_like", "stock_code_like", "trade_id_like", "ledger_like"]) or \
               (domain == "Insurance" and p.pattern in ["policy_like", "claim_like", "sum_assured_like"]) or \
               (domain == "Retail" and p.pattern == "sku_like"):
                score += 3.0
            elif (domain == "Banking" and p.pattern in ["txn_mode_like", "customer_type_like", "account_type_like", 
                                                          "kyc_status_like", "card_type_like", "loan_type_like"]) or \
                 (domain == "Finance" and p.pattern in ["portfolio_like", "roi_like"]) or \
                 (domain == "Insurance" and p.pattern in ["nominee_like", "premium_like", "maturity_like", "maturity_amount_like"]):
                score += 2.0
            else:
                score += 1.0
    return score


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
    predict(table_names, all_columns, sample_values=None, column_values=None, df=None) → dict
        Optionally pass column_values (dict[col_name, list]) or df to enable
        DB profile and domain pattern layers (dtype, length, pattern match).
    predict_domain_2step(all_columns, sample_values=None) → dict
    classify_table(df, table_name) → (is_banking, confidence, evidence)
        Uses df to build DB profile and pattern scores.
    get_domain_split_summary(table_names, all_columns, sample_values=None) → dict
    get_full_detection_analysis(table_names, all_columns, sample_values=None, column_values=None, df=None) → dict
        Returns layer1 (column names), layer2 (value keywords), layer3 (db_profile:
        dtype, length, pattern per column), layer4 (pattern match), combined domain.
    """

    def __init__(self) -> None:
        self._ml = _MLFallback()

    # ------------------------------------------------------------------ predict

    def predict(
        self,
        table_names: list[str],
        all_columns: list[str],
        sample_values: list[str] | None = None,
        column_values: dict[str, list[Any]] | None = None,
        df: pd.DataFrame | None = None,
    ) -> dict[str, Any]:
        norm_cols = _normalise_all(all_columns)
        values = list(sample_values or [])

        # Gather all values from df / column_values for strict-rules + later layers
        all_vals_for_strict = list(values)
        if column_values:
            for vals in column_values.values():
                all_vals_for_strict.extend(str(v) for v in vals[:50])
        if df is not None:
            for c in df.columns:
                all_vals_for_strict.extend(df[c].dropna().head(30).astype(str).tolist())

        # ------------------------------------------------------------------
        # STRICT RULES LAYER  (Banking / Finance / Insurance)
        # Fires first.  If it returns a conclusive result the weighted
        # scoring engine is skipped for domain selection.
        # ------------------------------------------------------------------
        strict = self._apply_strict_rules(norm_cols, all_vals_for_strict)

        if strict is not None:
            primary = strict["domain"]
            percentages = strict["percentages"]
            confidence = percentages[primary]
            profiles: list[ColumnProfile] = []
            if column_values or df is not None:
                profiles = build_db_profile(all_columns, column_values=column_values, df=df)

            out: dict[str, Any] = {
                "domain_label": primary,
                "is_banking": primary == "Banking",
                "confidence": confidence,
                "percentages": percentages,
                "evidence": self._build_evidence(all_columns, norm_cols, values),
                "column_domain_map": self._build_column_map(all_columns, norm_cols),
                "used_ml_fallback": False,
            }
            if profiles:
                out["db_profile"] = [
                    {
                        "column_name": p.column_name,
                        "dtype": p.dtype,
                        "min_len": p.min_len,
                        "max_len": p.max_len,
                        "avg_len": p.avg_len,
                        "pattern": p.pattern,
                        "sample_count": p.sample_count,
                    }
                    for p in profiles
                ]
            return out

        # ------------------------------------------------------------------
        # WEIGHTED SCORING ENGINE  (fallback for Healthcare, Retail, Other
        # and when strict rules are inconclusive)
        # ------------------------------------------------------------------

        # Layer 1: Keyword scoring (column names)
        domain_stats: dict[str, dict[str, float]] = {
            cfg.name: _score_domain(norm_cols, cfg) for cfg in DOMAIN_CONFIGS
        }
        col_scores: dict[str, float] = {
            name: stats["total"] for name, stats in domain_stats.items()
        }

        # Layer 2: Value keywords
        if values:
            for cfg in DOMAIN_CONFIGS:
                col_scores[cfg.name] += _score_values(values[:200], cfg) * 0.5

        # Layers 3 & 4: DB profile + domain pattern match when column_values or df provided
        profiles = []
        pattern_scores_for_cutoff: dict[str, float] | None = None
        if column_values or df is not None:
            profiles = build_db_profile(all_columns, column_values=column_values, df=df)
            for cfg in DOMAIN_CONFIGS:
                if cfg.name == "Other":
                    continue
                profile_score = _score_profile_against_domain(profiles, cfg.name)
                col_scores[cfg.name] += profile_score * 1.5
            all_vals = values
            if not all_vals and column_values:
                all_vals = [v for vals in column_values.values() for v in vals[:50]]
            if df is not None:
                for c in df.columns:
                    all_vals.extend(df[c].dropna().head(30).astype(str).tolist())
            if all_vals:
                pattern_scores_for_cutoff = {
                    cfg.name: _score_value_patterns(all_vals, cfg.name)
                    for cfg in DOMAIN_CONFIGS if cfg.name != "Other"
                }
                for cfg in DOMAIN_CONFIGS:
                    if cfg.name == "Other":
                        continue
                    col_scores[cfg.name] += pattern_scores_for_cutoff.get(cfg.name, 0) * 1.2

        # ML fallback only when zero keyword signal
        used_ml = False
        if sum(col_scores.values()) == 0:
            all_generic = all(_is_generic(c) for c in norm_cols)
            text = " ".join(table_names + all_columns + values[:50])
            col_scores = self._ml.predict(text, all_generic)
            used_ml = True
        else:
            coverage = {
                name: stats.get("coverage", 0.0) for name, stats in domain_stats.items()
            }
            col_scores = self._apply_domain_cutoffs(
                col_scores, coverage, pattern_scores=pattern_scores_for_cutoff
            )

        probs = _to_probs(col_scores)
        primary = max(probs, key=probs.get)
        confidence = round(probs[primary] * 100, 2)
        percentages = self._round_to_100({k: v * 100 for k, v in probs.items()})

        out = {
            "domain_label": primary,
            "is_banking": primary == "Banking",
            "confidence": confidence,
            "percentages": percentages,
            "evidence": self._build_evidence(all_columns, norm_cols, values),
            "column_domain_map": self._build_column_map(all_columns, norm_cols),
            "used_ml_fallback": used_ml,
        }
        if profiles:
            out["db_profile"] = [
                {
                    "column_name": p.column_name,
                    "dtype": p.dtype,
                    "min_len": p.min_len,
                    "max_len": p.max_len,
                    "avg_len": p.avg_len,
                    "pattern": p.pattern,
                    "sample_count": p.sample_count,
                }
                for p in profiles
            ]
        return out

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
        r = self.predict(
            table_names=[table_name],
            all_columns=columns,
            sample_values=sample_values,
            df=df,
        )
        return r["is_banking"], r["confidence"], r["evidence"]

    # ----------------------------------------------- get_full_detection_analysis

    def get_full_detection_analysis(
        self,
        table_names: list[str],
        all_columns: list[str],
        sample_values: list[str] | None = None,
        column_values: dict[str, list[Any]] | None = None,
        df: pd.DataFrame | None = None,
    ) -> dict[str, Any]:
        """
        Full detection analysis with all layers: column names, value keywords,
        DB profile (dtype, length, pattern per column), and domain pattern match.
        Returns layer-by-layer scores and combined domain prediction.
        """
        norm_cols = _normalise_all(all_columns)
        values = list(sample_values or [])

        # Layer 1: Column names
        domain_stats = {cfg.name: _score_domain(norm_cols, cfg) for cfg in DOMAIN_CONFIGS}
        layer1_scores = {name: stats["total"] for name, stats in domain_stats.items()}
        layer1_evidence = self._build_evidence(all_columns, norm_cols, [])

        # Layer 2: Value keywords
        layer2_scores = {cfg.name: 0.0 for cfg in DOMAIN_CONFIGS}
        if values:
            for cfg in DOMAIN_CONFIGS:
                layer2_scores[cfg.name] = float(_score_values(values[:200], cfg))
        layer2_evidence: list[str] = []
        if values:
            for cfg in DOMAIN_CONFIGS:
                if cfg.name == "Other":
                    continue
                hits = [str(v) for v in values[:100]
                        if any(kw in str(v).lower() for kw in cfg.value_keywords)][:3]
                if hits:
                    layer2_evidence.append(f"{cfg.name} value hits: {', '.join(hits)}")

        # Layer 3: DB profile
        profiles = build_db_profile(all_columns, column_values=column_values, df=df)
        layer3_scores = {cfg.name: 0.0 for cfg in DOMAIN_CONFIGS}
        for cfg in DOMAIN_CONFIGS:
            if cfg.name != "Other":
                layer3_scores[cfg.name] = _score_profile_against_domain(profiles, cfg.name)
        layer3_profile = [
            {
                "column_name": p.column_name,
                "dtype": p.dtype,
                "min_len": p.min_len,
                "max_len": p.max_len,
                "avg_len": p.avg_len,
                "pattern": p.pattern,
                "sample_count": p.sample_count,
            }
            for p in profiles
        ]

        # Layer 4: Domain value pattern match (regex)
        all_vals = values
        if not all_vals and column_values:
            all_vals = [v for vals in column_values.values() for v in vals[:100]]
        if df is not None:
            for c in df.columns:
                all_vals.extend(df[c].dropna().head(50).astype(str).tolist())
        layer4_scores = {cfg.name: 0.0 for cfg in DOMAIN_CONFIGS}
        for cfg in DOMAIN_CONFIGS:
            if cfg.name != "Other":
                layer4_scores[cfg.name] = _score_value_patterns(all_vals or [], cfg.name)

        # Combine: weighted sum of layers (tune weights as needed)
        combined = {name: 0.0 for name in DOMAIN_NAMES}
        for name in DOMAIN_NAMES:
            combined[name] = (
                layer1_scores.get(name, 0) * 1.0
                + layer2_scores.get(name, 0) * 0.5
                + layer3_scores.get(name, 0) * 1.5
                + layer4_scores.get(name, 0) * 1.2
            )
        coverage = {name: domain_stats[name].get("coverage", 0.0) for name in DOMAIN_NAMES}
        # When pattern layer (L4) gives strong signal, allow domain to win despite low column-name coverage
        combined = self._apply_domain_cutoffs(
            combined, coverage, pattern_scores=layer4_scores
        )

        # ML fallback when zero combined signal
        if sum(combined.values()) == 0:
            all_generic = all(_is_generic(c) for c in norm_cols)
            text = " ".join(table_names + all_columns + (values[:50] or []))
            combined = self._ml.predict(text, all_generic)

        probs = _to_probs(combined)
        primary = max(probs, key=probs.get)
        confidence = round(probs[primary] * 100, 2)
        percentages = self._round_to_100({k: v * 100 for k, v in probs.items()})

        return {
            "layer1_column_names": {
                "scores": layer1_scores,
                "evidence": layer1_evidence,
            },
            "layer2_value_keywords": {
                "scores": layer2_scores,
                "evidence": layer2_evidence,
            },
            "layer3_db_profile": {
                "columns": layer3_profile,
                "scores": layer3_scores,
            },
            "layer4_pattern_match": {
                "scores": layer4_scores,
            },
            "combined_scores": combined,
            "domain_label": primary,
            "confidence": confidence,
            "percentages": percentages,
            "evidence": self._build_evidence(all_columns, norm_cols, values or []),
            "column_domain_map": self._build_column_map(all_columns, norm_cols),
        }

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
    def _apply_strict_rules(
        norm_cols: list[str],
        sample_values: list[str],
    ) -> dict[str, Any] | None:
        """
        Strict keyword decision rules for Banking / Finance / Insurance.

        Exclusive keywords (unique to one domain) get 3x weight.
        Shared keywords (common between Banking & Finance) get 1x weight
        and count equally for both domains — so they never tip the balance.

        Decision priority:
        1. If only one domain has exclusive keyword hits → that domain.
        2. If multiple domains present → highest weighted score wins.
        3. Banking wins ONLY when banking-exclusive keywords (ifsc, atm,
           upi, txn) are present AND finance-exclusive keywords are absent.
        4. No strict keywords → None (fall back to weighted engine).
        """

        def _has_kw(token: str, kw_set: frozenset[str]) -> bool:
            for kw in kw_set:
                if kw in token:
                    return True
            return False

        # --- scan columns ------------------------------------------------
        banking_excl_col = 0
        finance_excl_col = 0
        insurance_col = 0
        shared_col = 0            # counts for BOTH Banking and Finance

        for nc in norm_cols:
            if _is_generic(nc):
                continue
            variants = _expand_norm_col(nc)
            matched_banking_excl = any(_has_kw(v, STRICT_BANKING_EXCLUSIVE) for v in variants)
            matched_finance_excl = any(_has_kw(v, STRICT_FINANCE_EXCLUSIVE) for v in variants)
            matched_shared = any(_has_kw(v, STRICT_BANKING_FINANCE_SHARED) for v in variants)
            matched_insurance = any(_has_kw(v, STRICT_INSURANCE_KEYWORDS) for v in variants)

            if matched_banking_excl:
                banking_excl_col += 1
            if matched_finance_excl:
                finance_excl_col += 1
            if matched_shared:
                shared_col += 1
            if matched_insurance:
                insurance_col += 1

        # --- scan values -------------------------------------------------
        banking_excl_val = 0
        finance_excl_val = 0
        insurance_val = 0
        shared_val = 0

        for raw in sample_values[:300]:
            low = str(raw).lower().strip()
            if not low or low in ("nan", "none"):
                continue
            if any(kw in low for kw in STRICT_BANKING_EXCLUSIVE):
                banking_excl_val += 1
            if any(kw in low for kw in STRICT_FINANCE_EXCLUSIVE):
                finance_excl_val += 1
            if any(kw in low for kw in STRICT_BANKING_FINANCE_SHARED):
                shared_val += 1
            if any(kw in low for kw in STRICT_INSURANCE_KEYWORDS):
                insurance_val += 1

        # --- weighted scores (exclusive 3x, shared 1x) -------------------
        # Columns count 2x more than values
        EXCL_W = 3.0
        SHARED_W = 1.0

        banking_score = (
            (banking_excl_col * EXCL_W + shared_col * SHARED_W) * 2
            + banking_excl_val * EXCL_W + shared_val * SHARED_W
        )
        finance_score = (
            (finance_excl_col * EXCL_W + shared_col * SHARED_W) * 2
            + finance_excl_val * EXCL_W + shared_val * SHARED_W
        )
        insurance_score = (
            insurance_col * EXCL_W * 2
            + insurance_val * EXCL_W
        )

        hits = {
            "Banking":   banking_score,
            "Finance":   finance_score,
            "Insurance": insurance_score,
        }

        # Determine which domains have exclusive signal
        has_banking_excl = (banking_excl_col + banking_excl_val) > 0
        has_finance_excl = (finance_excl_col + finance_excl_val) > 0
        has_insurance = (insurance_col + insurance_val) > 0
        has_shared_only = (shared_col + shared_val) > 0

        present = {d for d, s in hits.items() if s > 0}

        # Nothing matched at all → fall back
        if not present:
            return None

        # --- Key rule: if ONLY shared keywords matched (no exclusive from  ---
        # --- either Banking or Finance), fall back to weighted engine.      ---
        if not has_banking_excl and not has_finance_excl and not has_insurance:
            return None

        # --- Only one exclusive domain present ---------------------------
        if has_finance_excl and not has_banking_excl and not has_insurance:
            return _strict_result("Finance", hits)
        if has_banking_excl and not has_finance_excl and not has_insurance:
            return _strict_result("Banking", hits)
        if has_insurance and not has_banking_excl and not has_finance_excl:
            return _strict_result("Insurance", hits)

        # --- Multiple exclusive domains present → weighted score wins ----
        # Finance exclusive keywords present ⇒ prefer Finance over Banking
        # (because shared keywords like credit/debit/account appear in both)
        if has_finance_excl and has_banking_excl:
            # Finance has exclusive keywords → Finance wins unless Banking
            # exclusive score is significantly higher
            if finance_score >= banking_score * 0.5:
                hits["Finance"] = max(finance_score, banking_score + 1)

        best_domain = max(hits, key=lambda d: hits[d])
        return _strict_result(best_domain, hits)

    @staticmethod
    def _apply_domain_cutoffs(
        col_scores: dict[str, float],
        coverage: dict[str, float],
        pattern_scores: dict[str, float] | None = None,
    ) -> dict[str, float]:
        """
        Apply simple sanity rules so that a domain only wins when:
        - it covers a meaningful share of non-generic columns (coverage), AND
        - it is clearly stronger than the next-best domain.
        When pattern_scores is provided and the primary has strong pattern signal
        (e.g. >= 5.0), do not downgrade to Other based on coverage alone.
        """
        if not col_scores:
            return col_scores

        primary = max(col_scores, key=col_scores.get)
        best = col_scores[primary]
        others = [v for k, v in col_scores.items() if k != primary]
        second = max(others) if others else 0.0
        cov = coverage.get(primary, 0.0)
        strong_pattern = (
            (pattern_scores or {}).get(primary, 0) >= 5.0
        )

        min_coverage = 0.2
        margin_ratio = 1.25

        if primary != "Other":
            weak_coverage = cov < min_coverage
            weak_margin = second > 0 and best < margin_ratio * second
            # When no domain has any value-pattern signal (all L4 = 0), prefer Other for generic schemas
            no_pattern_signal = (
                pattern_scores is not None and sum(pattern_scores.values()) == 0
            )
            if (weak_coverage or weak_margin or no_pattern_signal) and not strong_pattern:
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
