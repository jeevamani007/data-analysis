"""
Demo: Run domain classifier on Finance and Insurance datasets.
Run: python check_domain_demo.py
"""
import sys
sys.path.insert(0, '.')

from domain_classifier import DomainClassifier

# Demo 1: Finance columns (user clues: INVOICE_ID, PAYMENT_MODE, TAX, GST, SALARY, EXPENSE, PROFIT, LOSS, BUDGET)
FINANCE_COLUMNS = [
    "INVOICE_ID", "PAYMENT_MODE", "TAX", "GST", "SALARY", "EXPENSE", "PROFIT", "LOSS", "BUDGET",
    "invoice_date", "customer_id", "net_amount", "gstin", "ledger_id", "voucher_no"
]
FINANCE_TABLE_NAMES = ["invoices", "expenses"]

# Demo 2: Insurance columns (user clues: POLICY_NO, CLAIM_ID, PREMIUM, RISK_SCORE, BENEFICIARY, NOMINEE, SUM_INSURED, POLICY_START_DATE, POLICY_END_DATE)
INSURANCE_COLUMNS = [
    "POLICY_NO", "CLAIM_ID", "PREMIUM", "RISK_SCORE", "BENEFICIARY", "NOMINEE", "SUM_INSURED",
    "POLICY_START_DATE", "POLICY_END_DATE", "claim_date", "claim_status", "insured_name"
]
INSURANCE_TABLE_NAMES = ["policies", "claims"]

# Demo 3: Mixed (Finance + Insurance tables)
MIXED_COLUMNS = FINANCE_COLUMNS + INSURANCE_COLUMNS
MIXED_TABLE_NAMES = ["invoices", "policies", "claims"]

def run_demo(name: str, table_names: list, columns: list, sample_values: list = None):
    clf = DomainClassifier()
    result = clf.get_domain_split_summary(table_names=table_names, all_columns=columns, sample_values=sample_values)
    print(f"\n{'='*60}")
    print(f"DEMO: {name}")
    print(f"{'='*60}")
    print(f"Primary domain: {result['primary_domain']}")
    print(f"Confidence: {result['confidence']}%")
    print("Percentages:")
    for domain, pct in result['percentages'].items():
        print(f"  {domain}: {pct}%")
    print("Evidence:", result.get('evidence', [])[:5])
    total = sum(result['percentages'].values())
    print(f"Sum of percentages: {total}%")
    return result

if __name__ == "__main__":
    print("Domain classifier demo – Finance & Insurance")
    run_demo("Finance dataset", FINANCE_TABLE_NAMES, FINANCE_COLUMNS)
    run_demo("Insurance dataset", INSURANCE_TABLE_NAMES, INSURANCE_COLUMNS)
    run_demo("Mixed Finance + Insurance", MIXED_TABLE_NAMES, MIXED_COLUMNS)
    print("\nDone.")
