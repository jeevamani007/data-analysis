
import pandas as pd
from date_detector import DateColumnDetector
import json

# Mock data
df_accounts = pd.DataFrame(columns=['account_id', 'customer_id', 'balance', 'login_timestamp'])
df_customers = pd.DataFrame(columns=['customer_id', 'customer_name', 'login_timestamp'])

detector = DateColumnDetector()

# Test combined
candidates_all = detector.find_login_timestamp_columns({'accounts': df_accounts, 'customers': df_customers})
print(json.dumps(candidates_all, indent=2, default=str))
