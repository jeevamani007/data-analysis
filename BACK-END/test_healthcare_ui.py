"""Test healthcare analyzer returns per-activity explanations."""
import pandas as pd
from pathlib import Path
from healthcare_analyzer import HealthcareAnalyzer
from csv_analyzer import CSVAnalyzer

demo_dir = Path(r'c:\Users\jeeva\micro_service\demo-2')
csvs = sorted(demo_dir.glob('*.csv'))
tables, dataframes = [], {}
analyzer = CSVAnalyzer()
for p in csvs:
    df = pd.read_csv(p)
    tn = p.stem.replace('_', ' ').title()
    tables.append(analyzer.analyze_table(str(p), tn))
    dataframes[tn] = df

result = HealthcareAnalyzer().analyze_cluster(tables, dataframes, [])
if result.get('success'):
    all_cd = result.get('case_details', [])
    print("Total cases: %d" % len(all_cd))
    for ci, cd in enumerate(all_cd):
        acts = cd.get('activities', [])
        print("Case %d (patient %s): %d activities" % (cd.get('case_id'), cd.get('user_id'), len(acts)))
        for i, a in enumerate(acts[:3]):
            ex = (a.get('explanation') or '(none)')[:50]
            print("    %d. %s: %s" % (i+1, a.get('event'), ex))
else:
    print("Error:", result.get('error'))
