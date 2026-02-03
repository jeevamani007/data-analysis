
import pandas as pd
from login_analyzer import LoginWorkflowAnalyzer

# Mock data based on sample files
data_cust = {
    'customer_id': ['CUST001', 'CUST002', 'CUST003', 'CUST004'],
    'customer_name': ['Amit', 'Priya', 'Rahul', 'Divya'],
    'login_timestamp': ['2026-02-01 09:15:00', '2026-02-01 10:30:00', '2026-01-30 14:20:00', '2026-01-30 16:50:00']
}
df_customers = pd.DataFrame(data_cust)

data_acc = {
    'account_id': ['ACC001', 'ACC002', 'ACC003', 'ACC004'],
    'customer_id': ['CUST001', 'CUST002', 'CUST003', 'CUST004'],
    'balance': [5000, 0, 10000, 1500],
    'open_date': ['2026-01-01', '2026-01-15', '2026-01-20', '2026-01-30']
}
df_accounts = pd.DataFrame(data_acc)

analyzer = LoginWorkflowAnalyzer()

# Test calculation
try:
    metrics = analyzer.calculate_login_delay(
        accounts_df=df_accounts,
        logins_df=df_customers,
        open_col='open_date',
        login_col='login_timestamp',
        id_col='customer_id'
    )
    print("Metrics:", metrics)
except Exception as e:
    print("Error:", e)
    import traceback
    traceback.print_exc()
