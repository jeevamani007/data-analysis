
import pandas as pd
from date_detector import DateColumnDetector

# Mock data based on observed headers
df_accounts = pd.DataFrame(columns=['account_id', 'customer_id', 'balance', 'login_timestamp'])
df_customers = pd.DataFrame(columns=['customer_id', 'customer_name', 'login_timestamp'])

detector = DateColumnDetector()

# Test detection on accounts
candidates_acc = detector.find_login_timestamp_columns({'accounts': df_accounts})
print("Accounts Login Candidates:", candidates_acc)

# Test detection on customers
candidates_cust = detector.find_login_timestamp_columns({'customers': df_customers})
print("Customers Login Candidates:", candidates_cust)

# Test combined
candidates_all = detector.find_login_timestamp_columns({'accounts': df_accounts, 'customers': df_customers})
print("Combined Candidates:", candidates_all)
