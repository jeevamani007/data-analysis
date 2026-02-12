"""Compact verification for strict domain rules with exclusive/shared split."""
import sys
sys.path.insert(0, ".")
from domain_classifier import DomainClassifier

clf = DomainClassifier()
results = []

def test(name, columns, values=None, expected=None):
    r = clf.predict(table_names=["t"], all_columns=columns, sample_values=values)
    ok = "PASS" if r["domain_label"] == expected else "FAIL"
    results.append(ok)
    print(f"[{ok}] {name}: exp={expected}, got={r['domain_label']} ({r['confidence']}%)")

# --- Core domain tests ---
test("Banking", ["ifsc_code","account_number","txn_type","balance","atm_id"], expected="Banking")
test("Finance", ["investment_id","roi","profit","loss","ledger_name","trade_id"], expected="Finance")
test("Insurance", ["policy_no","premium_amount","claim_id","nominee_name","maturity_date"], expected="Insurance")
test("Healthcare", ["patient_id","diagnosis","doctor_name","admission_date"], expected="Healthcare")
test("Retail", ["product_id","sku","unit_price","order_id","stock_quantity"], expected="Retail")

# --- Finance with shared keywords (the bug case!) ---
test("FinCredit", ["credit_amount","debit_amount","account_name","profit","ledger_id"], expected="Finance")
test("FinLoan", ["loan_amount","investment_id","roi","profit","trade_id"], expected="Finance")
test("FinAcct", ["account_id","balance","credit","profit_loss","investment"], expected="Finance")

# --- Banking with exclusive keywords ---
test("BankIFSC", ["ifsc_code","account_number","balance","credit","debit"], expected="Banking")
test("BankUPI", ["upi_id","txn_amount","account_no","debit_amt"], expected="Banking")

# --- Shared-only columns (should fall through to weighted engine) ---
test("SharedOnly", ["account_id","balance","credit_amount","debit_amount"], expected="Banking")

# --- Finance values ---
test("FinVals", ["col_a","col_b"], values=["investment","profit","loss","roi"], expected="Finance")

# --- Mixed Banking+Finance with Finance exclusive present ---
test("MixBF", ["ifsc_code","investment_id","roi","balance"], expected="Finance")

print(f"\n{results.count('PASS')}/{len(results)} passed")
