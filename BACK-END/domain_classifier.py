
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline


# ---------------------------------------------------------------------------
# Tuning constants
# ---------------------------------------------------------------------------

MIN_SCORE_FLOOR: float = 8.0        # FIX-D: raised from 4.0; needs >=2 hits or a combo
SECONDARY_DOMAIN_GAP: float = 20.0  # flag secondary domain if within this % of primary


# ---------------------------------------------------------------------------
# Generic token blacklist
# ---------------------------------------------------------------------------

_GENERIC_TOKENS: frozenset[str] = frozenset({
    # universal generic
    "id", "nm", "name", "val", "value", "dt", "date", "typ", "type",
    "sts", "status", "cd", "code", "no", "num", "number",
    "amt", "amount", "desc", "description", "ref", "reference",
    "flg", "flag", "key", "col", "fld", "field",
    "data", "info", "rec", "record", "row",
    "ts", "time", "timestamp", "created", "updated", "deleted",
    "by", "at", "seq", "idx",
    # FIX-A: HR tokens -- must NOT trigger Finance alone
    "employee", "emp", "staff", "personnel",
    "department", "dept", "division",
    "designation", "jobtitle", "title", "role", "grade", "band",
    "reportingto", "manager", "supervisor",
    "hiredate", "doj", "dol", "relievingdate", "exitdate",
    "probation", "notice", "confirmation",
    "location", "worksite", "shift", "attendance",
    # salary alone is an HR column — only Finance when payroll context exists
    "salary",
    # FIX-A: Government/civic tokens -- must NOT trigger Banking alone
    "citizen", "aadhaar", "pan", "voter", "ration",
    "scheme", "beneficiary", "district", "state", "taluk", "village",
    "ward", "block", "constituency", "panchayat",
    "subsidy", "entitlement", "welfare",
})

# Strong HR indicators used for negative rules (see FIX-G)
_HR_STRONG_TOKENS: frozenset[str] = frozenset({
    "employeeid", "employee", "empid", "empcode",
    "hrdept", "hrdepartment",
    "pfnumber", "pfaccount", "pf", "esi",
    "salary", "grosssalary", "netsalary",
})


def _normalise(token: str) -> str:
    return re.sub(r"[_\-\s]", "", token.lower())


def _normalise_all(columns: list[str]) -> list[str]:
    return [_normalise(c) for c in columns]


def _is_generic(norm_col: str) -> bool:
    parts = re.findall(r"[a-z]+", norm_col)
    return not parts or all(p in _GENERIC_TOKENS for p in parts)


def _kw_match(col: str, kw: str) -> bool:
    """
    FIX-C: Match keyword at proper boundary only (prefix or suffix).
    Stops 'designation' matching 'esi', 'district' matching 'ict', etc.
    """
    if col == kw:
        return True
    if col.startswith(kw):
        return True
    if col.endswith(kw):
        return True
    return False


# ---------------------------------------------------------------------------
# Abbreviation alias maps
# ---------------------------------------------------------------------------

_BANKING_ALIASES: dict[str, str] = {
    "acno": "accountno", "accno": "accountno", "acnum": "accountno",
    "acctno": "accountno", "accnum": "accountno", "acnumber": "accountno",
    "acid": "accountid", "accid": "accountid", "acctid": "accountid",
    "acbal": "accountbalance", "accbal": "accountbalance", "acctbal": "accountbalance",
    "actype": "accounttype", "acctyp": "accounttype",
    "txnid": "transactionid", "trnid": "transactionid", "txid": "transactionid",
    "txnamt": "depositamount", "trnamt": "depositamount",
    "txndt": "transactiondate", "trndt": "transactiondate",
    "txntyp": "transactiontype", "trntyp": "transactiontype",
    "brcd": "branchcode", "brid": "branchid", "brncd": "branchcode", "brnid": "branchid",
    "lnid": "loanid", "loanno": "loanid", "lnno": "loanid",
    "lnamt": "loanamount", "lnamount": "loanamount",
    "emino": "emi", "eminum": "emi", "emiamt": "emiamount",
    "odlmt": "overdraftlimit", "odamt": "overdraftlimit",
    "kycflg": "kycstatus", "kycsts": "kycstatus",
    "depamt": "depositamount", "dep": "depositamount",
    "wdlamt": "withdrawamount", "wdl": "withdrawamount", "wthdrwl": "withdrawal",
    "ifsccd": "ifsc", "swftcd": "swiftcode", "swft": "swiftcode",
}

_FINANCE_ALIASES: dict[str, str] = {
    "invno": "invoiceno", "invid": "invoiceid",
    "invdt": "invoicedate", "invamt": "invoiceamount", "invcno": "invoiceno",
    "gstno": "gstin", "gstnm": "gstin", "gstamt": "gst",
    "taxamt": "taxamount", "taxno": "gst",
    "salno": "salary", "salamt": "salary", "salmth": "payrollmonth",
    "grssal": "grosssalary", "netsal": "netsalary", "grpay": "grosssalary",
    "pfamt": "pf", "esiamt": "esi", "tdsamt": "tds",
    "hramt": "salary", "daamt": "salary",
    "ldgrid": "ledgerid", "ldgrnm": "ledgername",
    "jrnlid": "journalid", "jrnldt": "journalid",
    "vchrno": "voucherno", "vchrid": "voucherno",
    "dramt": "debitamount", "cramt": "creditamount",
    "drno": "debit", "crno": "credit",
    "acctpay": "accountspayable", "acctrec": "accountsreceivable",
}

_INSURANCE_ALIASES: dict[str, str] = {
    "plyno": "policyno", "plyid": "policyid", "polno": "policyno",
    "poltyp": "policyno", "pltyp": "policyno",
    "polstdt": "policystartdate", "polenddt": "policyenddate", "polstrtdt": "policystartdate",
    "prmamt": "premiumamt", "prm": "premiumamt", "premno": "premiumamt",
    "totprm": "totalpremium", "prmpd": "premiumamt",
    "siamt": "suminsured", "si": "suminsured", "limamt": "limamt",
    "clmno": "claimid", "clmamt": "claimamt", "clmsts": "claimstatus",
    "clmdt": "claimid", "clmid": "claimid",
    "bnfnm": "beneficiary", "bnfid": "beneficiary", "bnf": "beneficiary",
    "nomnm": "nominee", "nomid": "nominee", "nom": "nominee",
    "insrdnm": "insured", "insrdid": "insured", "insrnm": "insurer",
    "cvgtyp": "coverage", "cvgcd": "covcd", "covtyp": "coverage",
    "undscr": "underwriting", "undwrt": "underwriting",
    "rendt": "renewaldate", "rennm": "renewaldate",
    "lpsdt": "lapsedate", "lpsfg": "lapsedate",
    "grprd": "graceperiod", "graceprd": "graceperiod",
    "dedamt": "deductible", "ded": "deductible",
    "rdrcd": "rider", "rdrnm": "rider",
    "lobcd": "lob", "covcd": "covcd", "cov": "covcd",
    "pdamt": "paidamt", "rsvamt": "rsvamt", "rsv": "rsvamt",
}

_HEALTHCARE_ALIASES: dict[str, str] = {
    "ptid": "patientid", "patnm": "patientname", "patrec": "patientid",
    "pt": "patient", "patno": "patientid",
    "drid": "doctor", "drno": "doctor", "docid": "doctor", "docnm": "doctor",
    "diagcd": "diagnosisid", "diagid": "diagnosisid", "diagno": "diagnosisid",
    "icdcd": "icdcode", "icdno": "icdcode",
    "mednm": "medicationname", "medid": "medicationname", "med": "medication",
    "dosqty": "dosage",
    "rxdt": "prescriptiondate", "rxno": "prescriptiondate", "rx": "prescriptiondate",
    "admdt": "admissiondate", "admno": "admissiondate", "adm": "admission",
    "dscdt": "dischargedate", "discdt": "dischargedate", "dsc": "discharge",
    "labid": "labtest", "labrec": "labtest",
    "tstnm": "labtest", "tstno": "labtest",
    "tstres": "labresult", "tstrst": "labresult",
    "spcmtp": "specimentype", "spcm": "specimentype", "spmtyp": "specimentype",
    "wardno": "ward", "bedid": "ward", "bedno": "ward",
    "vitsgn": "vitals", "vtl": "vitals",
    "symcd": "symptoms", "symno": "symptoms",
}

_RETAIL_ALIASES: dict[str, str] = {
    "prodid": "productid", "prodnm": "productname", "prodcd": "productid",
    "prodno": "productid", "itmid": "productid", "itmnm": "productname",
    "ordid": "orderid", "ordno": "orderid", "ordnm": "orderid",
    "orddt": "orderdate", "ordsts": "orderstatus",
    "upr": "unitprice", "uprc": "unitprice", "unitprc": "unitprice",
    "qty": "quantity", "qtyno": "quantity",
    "discamt": "discountamount", "disamt": "discountamount",
    "netamt": "netamount", "totamt": "totalamount",
    "skucd": "sku", "skuno": "sku",
    "catnm": "category", "catcd": "category", "catid": "category",
    "brndn": "brand", "brndnm": "brand", "brndid": "brand",
    "stkqty": "stockquantity", "stk": "stockquantity", "stqty": "stockqty",
    "rtnflg": "returnflag", "retflg": "returnflag", "rtndt": "returndate",
    "paymth": "paymentmethod", "paysts": "paymentstatus",
    "strid": "storeid", "strnm": "storeid",
    "csrid": "cashierid", "casrid": "cashierid",
    "billno": "billno", "bilno": "billno", "invtyp": "invoicetype",
    "taxamt": "taxamount",
}

_ALL_ALIASES: dict[str, dict[str, str]] = {
    "Banking": _BANKING_ALIASES, "Finance": _FINANCE_ALIASES,
    "Insurance": _INSURANCE_ALIASES, "Healthcare": _HEALTHCARE_ALIASES,
    "Retail": _RETAIL_ALIASES,
}


def _expand_aliases(norm_cols: list[str]) -> dict[str, list[str]]:
    return {
        domain: [alias_map.get(nc, nc) for nc in norm_cols]
        for domain, alias_map in _ALL_ALIASES.items()
    }


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

    DomainConfig(
        name="Banking", ml_label=0,
        exclusive_keywords=[
            "ifsc", "ifsccode", "swiftcode", "iban", "bic",
            "routingnumber", "routing",
            "emiamount", "emi", "loanid", "loannumber", "loantype", "loanamount",
            "overdraftlimit", "overdraftused", "overdraft",
            "minimumbalance", "standinginstruction",
            "clearancedate", "disbursementdate", "transactiondate",
            "kycstatus", "kyc", "cardnumber", "expirydate", "posterminal",
        ],
        shared_keywords=[
            "accountnumber", "accountno", "accountid", "accountbalance", "accounttype",
            "branchid", "branchcode", "branchname",
            "transactionid", "transactiontype",
            "depositamount", "withdrawamount", "withdrawal",
            "creditamount", "debitamount",
            "logintime", "logouttime", "settlementdate", "authorizationcode",
        ],
        combo_rules=[
            {"ifsc", "accountnumber"}, {"ifsc", "accountid"},
            {"swiftcode", "iban"}, {"loanid", "emi"},
            {"accountid", "overdraft"}, {"routing", "accountnumber"},
            {"kyc", "accountid"}, {"emi", "accountid"}, {"loanid", "accountid"},
            {"loanamount", "accountid"}, {"loanamount", "emi"},
            # Strong banking context combos to separate from generic Finance
            {"accountid", "ifsc", "loanamount"},
            {"accountnumber", "ifsc", "loanamount"},
            {"accountid", "ifsc", "transactiondate"},
        ],
        value_keywords=[
            "ifsc", "swift", "iban", "emi", "emi due", "emi pay", "overdraft",
            "neft", "rtgs", "imps", "cheque", "standing instruction",
            "loan disbursed", "emi deducted",
            "upi", "netbanking", "atm", "atm withdrawal",
            "fund transfer", "balance check", "balance inquiry",
        ],
        ml_samples=[
            "ifsc_code swift_code branch_id account_number routing_number iban bic",
            "loan_id emi_amount tenure_months repayment_schedule interest_accrued",
            "overdraft_limit minimum_balance kyc_status credit_utilized",
            "card_number expiry_date pos_terminal authorization_code settlement_date",
            "account_balance disbursement_date standing_instruction clearance_date",
            "account_type savings_account checking_account overdraft_used",
            "transfer_amount swift_code routing_number iban bic_code",
            "credit_limit debit_card atm_withdrawal cash_deposit neft_transfer",
            "loan_amount loan_type principal interest_rate tenure disbursement",
            "acno txnid brcd lnid emiamt odlmt kycflg",
            "acbal actype depamt wdlamt ifsccd swftcd",
            "lnamt lnno txndt txntyp brnid brncd",
        ],
        exclusive_weight=4.5,
        shared_weight=1.2,
        combo_bonus=7.0,
    ),

    DomainConfig(
        name="Finance", ml_label=1,
        exclusive_keywords=[
            "gst", "gstin", "cgst", "sgst", "igst", "gstno",
            "tds", "tdsamt",
            "ledgerid", "ledgername", "journalid", "voucherno", "vchrno",
            "costcenter", "fiscalyear", "fiscal_year",
            "accountspayable", "accountsreceivable", "acctpay", "acctrec",
            "grosssalary", "netsalary", "payrollmonth", "payroll", "grssal", "netsal",
            "cogs", "ebitda", "ebit",
            "debitamount", "creditamount", "dramt", "cramt",
            "invoiceamount", "invoicedate", "invamt", "invdt",
            "taxablevalue", "taxable_value",
        ],
        shared_keywords=[
            "invoice", "invoiceid", "invoiceno", "invno", "invid",
            "tax", "taxamount", "taxamt",
            "expense", "budget",
            "profit", "loss", "revenue",
            "receivable", "payable",
            "debit", "credit",
            "vendor", "supplier",
            "pf", "esi", "pfamt", "esiamt",
        ],
        combo_rules=[
            # Strong Finance patterns - high bonus
            {"invoice", "gst"}, {"invoice", "gstin"}, {"invoice", "cgst"}, {"invoice", "sgst"},
            {"invoice", "tax"}, {"invoice", "taxamount"},
            {"gst", "tax"}, {"gstin", "tax"}, {"cgst", "sgst"},
            {"invoice", "vendor"}, {"invoice", "supplier"},
            {"invoice", "accountspayable"}, {"invoice", "accountsreceivable"},
            {"invoice", "ledger"}, {"invoice", "journal"},
            # Payroll patterns
            {"salary", "payroll"}, {"grosssalary", "netsalary"}, {"payroll", "tds"},
            {"tds", "payroll"}, {"pf", "payroll"}, {"esi", "payroll"},
            {"pf", "tds"}, {"esi", "tds"}, {"pf", "esi"},
            # Accounting patterns
            {"ledger", "journal"}, {"ledger", "voucher"}, {"journal", "voucher"},
            {"profit", "loss"}, {"profit", "revenue"}, {"loss", "revenue"},
            {"debit", "credit"}, {"debitamount", "creditamount"},
            {"accountspayable", "accountsreceivable"},
            # Invoice + payment patterns
            {"invoice", "paymentmode"}, {"invoice", "duedate"},
            {"invoiceamount", "gst"}, {"invoicedate", "taxamount"},
            {"invoice", "cgst", "sgst"}, {"invoice", "gst", "tax"},
        ],
        value_keywords=[
            "gst", "gstin", "cgst", "sgst", "igst", "gst number",
            "tds", "tds deducted", "tds amount",
            "payroll", "payroll processing", "payroll month",
            "salary", "gross salary", "net salary",
            "invoice", "invoice number", "invoice date",
            "debit note", "credit note", "journal entry", "ledger entry",
            "accounts payable", "accounts receivable", "ap", "ar",
            "vendor invoice", "supplier invoice", "tax invoice",
            "fiscal year", "cost center", "profit and loss",
        ],
        ml_samples=[
            # Invoice + GST patterns (strong Finance)
            "invoice_no gst gstin tax_amount cgst sgst igst taxable_value invoice_date",
            "invoice_id invoice_number gst_number cgst_amount sgst_amount igst_amount",
            "vendor_invoice gstin tax_invoice cgst sgst igst total_amount",
            "invoice_date invoice_amount gst gstin tax_amount vendor_name",
            "supplier_invoice invoice_no gst cgst sgst payment_status",
            # Payroll patterns
            "payroll_month gross_salary net_salary tds pf esi deductions",
            "payroll_id employee_id payroll_month gross_salary net_salary tds pf esi",
            "payroll_period gross_pay net_pay tds_deduction pf_contribution esi_contribution",
            "salary_month gross_salary net_salary tds pf esi other_deductions",
            # Accounting ledger patterns
            "ledger_id ledger_name journal_id voucher_no narration",
            "ledger_account journal_entry voucher_number debit_credit",
            "general_ledger journal_id voucher_id transaction_type amount",
            "chart_of_accounts ledger_code account_name debit_credit_balance",
            # Accounts Payable/Receivable
            "accounts_payable supplier_id bill_no amount_due paid_amount",
            "accounts_receivable receipt_no amount_received balance_due",
            "ap_vendor invoice_number due_date payment_status outstanding_amount",
            "ar_customer invoice_no received_amount pending_amount",
            # Budget and expense
            "budget_amount planned_amount actual_amount variance cost_center",
            "expense_type expense_amount cost_center vendor_name",
            "budget_code department budget_allocated budget_spent",
            # Profit/Loss/Revenue
            "profit loss revenue cogs margin fiscal_year quarter",
            "income_statement revenue expenses profit loss fiscal_period",
            "p_and_l revenue cost_of_goods gross_profit net_profit",
            # Invoice + payment
            "invoice_date payment_mode payment_status due_date gst",
            "invoice_number invoice_amount gst_amount payment_received",
            # Abbreviated forms
            "invno gstno taxamt tdsamt pfamt esiamt salmth",
            "ldgrid jrnlid vchrno dramt cramt acctpay acctrec",
            "grssal netsal grpay invamt invdt invcno",
            "invno invdt invamt gstno cgst sgst taxamt",
            # Mixed Finance patterns
            "invoice gst vendor payment accounts_payable ledger",
            "payroll salary tds pf esi employee gross net",
            "journal voucher ledger debit credit narration",
            "gst invoice tax vendor supplier accounts_payable",
        ],
        exclusive_weight=4.5,  # Increased from 3.8
        shared_weight=1.2,     # Increased from 1.0
        combo_bonus=8.0,       # Increased from 5.8 for stronger combo signals
    ),

    DomainConfig(
        name="Insurance", ml_label=2,
        exclusive_keywords=[
            "policyno", "policyid", "policystartdate", "policyenddate",
            "premiumamt", "premamt", "totalpremium",
            "suminsured", "limamt",
            "claimid", "claimamount", "claimstatus", "settlementamount",
            "underwriting", "underwritingscore",
            "deductible", "dedamt",
            "renewaldate", "lapsedate", "graceperiod",
            "lob", "lobcd", "covcd",
            "paidamt", "rsvamt", "claimamt",
        ],
        shared_keywords=[
            "policy", "polid", "claim", "clmid",
            "premium", "prem", "insured", "insurer",
            "beneficiary", "nominee", "coverage", "rider",
            "agent", "agentid", "broker",
        ],
        combo_rules=[
            {"policy", "premium"}, {"claim", "policy"},
            {"suminsured", "premium"}, {"nominee", "policy"},
            {"coverage", "deductible"}, {"policy", "beneficiary"},
            {"policyno", "premiumamt"}, {"claimid", "policyno"},
            {"insured", "premium"}, {"nominee", "beneficiary"},
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
            "plyno prmamt siamt clmno bnfnm nomnm insrdnm",
            "polstdt polenddt rendt lpsdt grprd dedamt rdrcd",
            "cvgtyp lobcd covcd undscr pdamt rsvamt clmsts",
        ],
    ),

    DomainConfig(
        name="Healthcare", ml_label=3,
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
            "vaccinationrecord", "vaccination", "icd",
            "prescriptiondt", "prescriptionno",
        ],
        shared_keywords=[
            "patient", "doctor", "physician", "surgeon", "nurse",
            "diagnosis", "treatment", "prescription", "medication",
            "admission", "discharge", "hospital", "clinic",
            "appointment", "vitals", "symptoms", "allergies",
        ],
        combo_rules=[
            {"patient", "doctor"}, {"patient", "diagnosis"},
            {"patient", "admission", "discharge"},
            {"medication", "dosage"}, {"labtest", "labresult"},
            {"patient", "medication"}, {"patient", "appointment"},
            {"patientid", "doctor"}, {"patientid", "medication"},
            {"patientid", "labtest"}, {"diagnosis", "treatment"},
        ],
        value_keywords=[
            "dr.", "doctor", "patient", "diagnosis", "prescription",
            "hospital", "clinic", "surgery", "triage",
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
            "ptid patnm drid diagcd icdcd mednm dosqty rxdt",
            "admdt dscdt labid tstnm tstres spcmtp wardno vtl",
            "symcd vitsgn patrec docnm diagid diagno rxno",
        ],
    ),

    DomainConfig(
        name="Retail", ml_label=4,
        exclusive_keywords=[
            "productid", "productname", "sku",
            "unitprice", "costprice", "stockquantity", "stockqty",
            "orderid", "orderstatus",
            "discountamount", "netamount",
            "saleschannel", "storeid", "cashierid",
            "returnflag", "returndate",
            "invoicetype", "billno",
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
            {"prodid", "ordid"}, {"prodid", "quantity"},
            {"ordid", "quantity", "unitprice"}, {"sku", "quantity"},
            # POS-style retail: UPI / card / terminal together with product or order
            {"orderid", "upi"},
            {"productid", "upi"},
            {"orderid", "paymentmethod", "pos terminal"},
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
            "prodid prodnm skucd ordid orddt qty upr discamt",
            "catnm brndn stkqty rtnflg paymth strid csrid",
            "netamt totamt ordsts paysts taxamt billno invtyp",
        ],
        exclusive_weight=3.8,
        shared_weight=1.0,
        combo_bonus=5.8,
    ),

    DomainConfig(
        name="Other", ml_label=5,
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
            # FIX-A: pure HR schemas
            "employee_id department designation job_title manager reporting_to doj",
            "emp_id salary pf_number esi_number designation grade band",
            # FIX-A: government/civic schemas
            "citizen_id aadhaar_no voter_id pan_no ration_card scheme_code",
            "beneficiary_id district_code state_code block_name panchayat",
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
    if expanded_cols:
        seen: set[str] = set(norm_cols)
        extras = [c for c in expanded_cols if c not in seen]
        cols_to_score = norm_cols + extras
    else:
        cols_to_score = norm_cols[:]

    exclusive_hits = sum(
        1 for col in cols_to_score
        if not _is_generic(col)
        and any(_kw_match(col, kw) for kw in config.exclusive_keywords)
    )
    shared_hits = sum(
        1 for col in cols_to_score
        if not _is_generic(col)
        and any(_kw_match(col, kw) for kw in config.shared_keywords)
        and not any(_kw_match(col, kw) for kw in config.exclusive_keywords)
    )
    combo_hits = sum(
        1 for rule in config.combo_rules
        if all(any(_kw_match(col, kw) for col in cols_to_score) for kw in rule)
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
    """
    Value-based scoring.
    - Ignores purely numeric values (dates/IDs already captured via columns).
    - Focuses on text phrases containing domain value keywords.
    - Normalises case and trims whitespace.
    - For Finance: gives extra weight to GST/invoice patterns.
    """
    score = 0
    for val in sample_values[:200]:
        s = str(val).strip().lower()
        if not s:
            continue
        # Pure numbers (or numbers with punctuation) add almost no domain signal
        if re.fullmatch(r"[0-9,.\-\/]+", s):
            continue
        
        # Check for keyword matches
        matched_keywords = [kw for kw in config.value_keywords if kw in s]
        if matched_keywords:
            score += 1
            # Extra weight for Finance-specific strong patterns
            if config.name == "Finance":
                # GST patterns are very strong Finance indicators
                if any(kw in s for kw in ["gst", "gstin", "cgst", "sgst", "igst", "gst number"]):
                    score += 1  # Bonus point for GST
                # Invoice + GST together in value is very strong
                if "invoice" in s and any(kw in s for kw in ["gst", "gstin", "cgst", "sgst"]):
                    score += 2  # Strong Finance pattern
                # Payroll patterns
                if "payroll" in s and any(kw in s for kw in ["tds", "pf", "esi", "salary"]):
                    score += 1  # Bonus for payroll context
    
    return score


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
                max_iter=1000, random_state=42, class_weight="balanced",
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

    Key behaviours (v3)
    -------------------
    - Pure HR schemas (employee, department, designation, pf_number)  -> Other
    - HR + Payroll schemas (payroll_month, gross_salary, tds)          -> Finance
    - Government schemas (aadhaar, voter_id + ifsc_code)               -> Other
    - Mixed schemas expose secondary_domain in all outputs
    """

    def __init__(self) -> None:
        self._ml = _MLFallback()

    def predict(
        self,
        table_names: list[str],
        all_columns: list[str],
        sample_values: list[str] | None = None,
    ) -> dict[str, Any]:
        norm_cols = _normalise_all(all_columns)
        values = list(sample_values or [])
        expanded = _expand_aliases(norm_cols)

        col_scores: dict[str, float] = {
            cfg.name: _score_domain(norm_cols, cfg, expanded.get(cfg.name))["total"]
            for cfg in DOMAIN_CONFIGS
        }

        if values:
            for cfg in DOMAIN_CONFIGS:
                col_scores[cfg.name] += _score_values(values[:200], cfg) * 0.5

        # For very short schemas (few columns), fall back more strongly to ML /
        # value-based signals so we do not overfit on a single ambiguous column.
        used_ml = False
        n_cols = len(norm_cols)
        if n_cols <= 3:
            text = " ".join(table_names + all_columns + values[:50])
            ml_scores = self._ml.predict(text, all_generic=all(_is_generic(c) for c in norm_cols))
            # Blend: keep relative ordering from column scores, but let ML dominate
            for name in col_scores:
                col_scores[name] = col_scores[name] * 0.4 + ml_scores.get(name, 0.0) * 10.0
            used_ml = True

        if sum(col_scores.values()) == 0:
            all_generic = all(_is_generic(c) for c in norm_cols)
            text = " ".join(table_names + all_columns + values[:50])
            col_scores = self._ml.predict(text, all_generic)
            used_ml = True

        # HR negative rule (FIX-G): if strong HR pattern and Finance is only
        # weakly indicated, down-rank Finance in favour of Other.
        hr_hits = sum(
            1 for nc in norm_cols
            if any(tok in nc for tok in _HR_STRONG_TOKENS)
        )
        if hr_hits >= 3 and col_scores.get("Finance", 0.0) < MIN_SCORE_FLOOR + 2:
            # Remove weak Finance bias, prefer Other for HR schemas
            col_scores["Other"] = col_scores.get("Other", 0.0) + 5.0
            col_scores["Finance"] *= 0.3

        # Finance hard override rules (must come before Banking hints)
        # If we see clear Finance patterns, strongly boost Finance
        joined_cols = " ".join(all_columns).lower()
        joined_vals = " ".join(values[:50]).lower()
        finance_hint = False
        
        # Pattern 1: Invoice + GST = Finance (not Retail, not Banking)
        if any(k in joined_cols for k in ("invoice", "invoiceno", "invoiceid", "invno")) and \
           any(k in joined_cols for k in ("gst", "gstin", "gstno", "cgst", "sgst", "igst")):
            finance_hint = True
            col_scores["Finance"] = col_scores.get("Finance", 0.0) * 2.0 + 10.0  # Strong boost
            col_scores["Retail"] *= 0.3  # Down-rank Retail
            col_scores["Banking"] *= 0.3  # Down-rank Banking
        
        # Pattern 2: Invoice + Tax + Vendor/Supplier = Finance (not Retail)
        if any(k in joined_cols for k in ("invoice", "invoiceno", "invoiceid")) and \
           any(k in joined_cols for k in ("tax", "taxamount", "taxamt")) and \
           any(k in joined_cols for k in ("vendor", "supplier", "vendorid", "supplierid")):
            finance_hint = True
            col_scores["Finance"] = col_scores.get("Finance", 0.0) * 1.8 + 8.0
            col_scores["Retail"] *= 0.4
        
        # Retail hard override: Product/Order + Invoice (without GST) = Retail (not Finance)
        retail_hint = False
        if any(k in joined_cols for k in ("product", "productid", "productname", "prodid", "prodnm")) and \
           any(k in joined_cols for k in ("order", "orderid", "orderno", "ordid")) and \
           any(k in joined_cols for k in ("invoice", "invoiceno", "invoiceid")) and \
           not any(k in joined_cols for k in ("gst", "gstin", "gstno", "cgst", "sgst", "igst", "vendor", "supplier")):
            retail_hint = True
            col_scores["Retail"] = col_scores.get("Retail", 0.0) * 1.6 + 7.0
            col_scores["Finance"] *= 0.4  # Down-rank Finance
        
        # Retail: SKU + Order + Invoice = Retail (not Finance)
        if any(k in joined_cols for k in ("sku", "skucd", "skuno")) and \
           any(k in joined_cols for k in ("order", "orderid", "orderno")) and \
           any(k in joined_cols for k in ("invoice", "invoiceno", "billno")):
            retail_hint = True
            col_scores["Retail"] = col_scores.get("Retail", 0.0) * 1.5 + 6.0
            col_scores["Finance"] *= 0.5
        
        # Pattern 3: Payroll + TDS/PF/ESI = Finance (not HR)
        if any(k in joined_cols for k in ("payroll", "payrollmonth", "payroll_month")) and \
           any(k in joined_cols for k in ("tds", "tdsamt", "pf", "pfamt", "esi", "esiamt")):
            finance_hint = True
            col_scores["Finance"] = col_scores.get("Finance", 0.0) * 1.8 + 8.0
            col_scores["Other"] *= 0.5  # Down-rank HR/Other
        
        # Pattern 4: Ledger + Journal + Voucher = Finance
        if any(k in joined_cols for k in ("ledger", "ledgerid", "ledgername")) and \
           any(k in joined_cols for k in ("journal", "journalid")) and \
           any(k in joined_cols for k in ("voucher", "voucherno", "vchrno")):
            finance_hint = True
            col_scores["Finance"] = col_scores.get("Finance", 0.0) * 1.6 + 7.0
        
        # Pattern 5: Accounts Payable/Receivable = Finance
        if any(k in joined_cols for k in ("accountspayable", "accountsreceivable", "acctpay", "acctrec", "ap", "ar")):
            finance_hint = True
            col_scores["Finance"] = col_scores.get("Finance", 0.0) * 1.5 + 6.0
        
        # Pattern 6: Value-based Finance detection (GST in values)
        if any(k in joined_vals for k in ("gst", "gstin", "cgst", "sgst", "igst", "gst number", "tax invoice")):
            if any(k in joined_cols for k in ("invoice", "invoiceno", "invoiceid", "invno")):
                finance_hint = True
                col_scores["Finance"] = col_scores.get("Finance", 0.0) * 1.4 + 5.0
        
        # Healthcare vs Insurance differentiation
        # Patient + Claim = Insurance (not Healthcare)
        if any(k in joined_cols for k in ("patient", "patientid", "patientname", "ptid", "patnm")) and \
           any(k in joined_cols for k in ("claim", "claimid", "claimno", "claimamount", "claimstatus", "clmno", "clmid")):
            col_scores["Insurance"] = col_scores.get("Insurance", 0.0) * 1.5 + 6.0
            col_scores["Healthcare"] *= 0.4  # Down-rank Healthcare
        
        # Insurance: Policy + Premium + Claim = Insurance (not Healthcare)
        if any(k in joined_cols for k in ("policy", "policyno", "policyid", "plyno", "plyid")) and \
           any(k in joined_cols for k in ("premium", "premiumamt", "premamt", "prmamt")) and \
           any(k in joined_cols for k in ("claim", "claimid", "claimamount", "clmno", "clmid")):
            col_scores["Insurance"] = col_scores.get("Insurance", 0.0) * 1.8 + 8.0
            col_scores["Healthcare"] *= 0.3
        
        # Banking priority hint: if table looks transaction-like (account/loan +
        # timestamp + amount + event) BUT NOT Finance patterns, boost Banking
        banking_hint = False
        if not finance_hint:  # Only if Finance patterns not detected
            if any(k in joined_cols for k in ("account_id", "accountid", "loan_account", "loanaccount")) and \
               any(k in joined_cols for k in ("timestamp", "time", "txndt", "transactiondate")) and \
               any(k in joined_cols for k in ("event_type", "eventtype", "status", "amount")) and \
               not any(k in joined_cols for k in ("gst", "gstin", "invoice", "vendor", "supplier")):
                banking_hint = True
            if any(k in joined_vals for k in ("upi", "netbanking", "atm", "emi pay", "emi_due", "emi due", "atm withdrawal")) and \
               not any(k in joined_vals for k in ("gst", "invoice", "vendor", "supplier")):
                banking_hint = True
            if banking_hint:
                col_scores["Banking"] = col_scores.get("Banking", 0.0) * 1.3 + 1.0

        # Finance priority for short schemas with Finance keywords
        # If we have 2-3 columns and see Finance keywords, boost Finance
        if n_cols <= 3 and not used_ml:
            finance_keywords_found = sum(
                1 for nc in norm_cols
                if any(kw in nc for kw in ["gst", "gstin", "invoice", "payroll", "ledger", "journal", "voucher", "tds"])
            )
            if finance_keywords_found >= 1:
                # Even with short schema, Finance keywords are strong signal
                col_scores["Finance"] = col_scores.get("Finance", 0.0) * 1.5 + 5.0
        
        # FIX-D: minimum score floor (only when we did not already decide via ML)
        if not used_ml and max(col_scores.values()) < MIN_SCORE_FLOOR:
            col_scores = {k: 0.0 for k in col_scores}
            col_scores["Other"] = 1.0

        probs = _to_probs(col_scores)
        primary = max(probs, key=probs.get)
        confidence = round(probs[primary] * 100, 2)
        percentages = self._round_to_100({k: v * 100 for k, v in probs.items()})
        secondary_domain = self._find_secondary(probs, primary, confidence)

        return {
            "domain_label": primary,
            "secondary_domain": secondary_domain,
            "is_banking": primary == "Banking",
            "confidence": confidence,
            "percentages": percentages,
            "evidence": self._build_evidence(all_columns, norm_cols, values, expanded),
            "column_domain_map": self._build_column_map(all_columns, norm_cols, expanded),
            "used_ml_fallback": used_ml,
        }

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
            "secondary_domain": r["secondary_domain"],
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
                r["domain_label"], r["secondary_domain"],
                r["confidence"], r["evidence"]
            ),
        }

    # ------------------------------------------------------------------

    @staticmethod
    def _find_secondary(
        probs: dict[str, float], primary: str, primary_pct: float
    ) -> str | None:
        candidates = {k: v * 100 for k, v in probs.items() if k != primary}
        if not candidates:
            return None
        runner_up = max(candidates, key=candidates.get)
        if primary_pct - candidates[runner_up] <= SECONDARY_DOMAIN_GAP:
            return runner_up
        return None

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
        capped = sample_values[:200]
        scores = {cfg.name: float(_score_values(capped, cfg)) for cfg in DOMAIN_CONFIGS}
        total = sum(scores.values())
        if total == 0:
            return {"primary_domain": "Other", "confidence": 0.0,
                    "scores": scores, "evidence": []}
        primary = max(scores, key=scores.get)
        cfg = DOMAIN_MAP[primary]
        evidence = [v for v in capped
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
                    f"Conflict: columns suggest '{d1}' ({c1:.1f}%) "
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
        capped_vals = values[:200]
        for cfg in DOMAIN_CONFIGS:
            if cfg.name == "Other":
                continue
            all_kws = cfg.exclusive_keywords + cfg.shared_keywords
            exp = (expanded or {}).get(cfg.name, norm_cols)
            hits = [
                col for col, nc, ec in zip(columns, norm_cols, exp)
                if not _is_generic(nc)
                and (
                    any(_kw_match(nc, kw) for kw in all_kws)
                    or any(_kw_match(ec, kw) for kw in all_kws)
                )
            ]
            val_hits = [str(v) for v in capped_vals
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
                domain_exp = (expanded or {}).get(cfg.name, [])
                ec = domain_exp[i] if i < len(domain_exp) else nc
                if not _is_generic(nc) and (
                    any(_kw_match(nc, kw) for kw in all_kws)
                    or any(_kw_match(ec, kw) for kw in all_kws)
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
    def _generate_explanation(
        primary: str,
        secondary: str | None,
        confidence: float,
        evidence: list[str],
    ) -> str:
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
        secondary_note = (
            f" Also shows <strong>{secondary}</strong> characteristics."
            if secondary else ""
        )
        return (
            f"This appears to be {hi if high else lo} "
            f"(confidence: {confidence:.1f}%).{ev}{secondary_note}"
        )