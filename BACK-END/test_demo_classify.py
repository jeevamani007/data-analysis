"""
Test domain classifier on demo folder CSVs.
Run from BACK-END: python test_demo_classify.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pandas as pd
from domain_classifier import DomainClassifier

DEMO_DIR = Path(__file__).resolve().parent.parent.parent / "demo"

def test_demo_files():
    if not DEMO_DIR.exists():
        print(f"Demo dir not found: {DEMO_DIR}")
        return
    csvs = list(DEMO_DIR.glob("*.csv"))
    if not csvs:
        print("No CSV files in demo folder")
        return

    all_columns = []
    all_tables = []
    sample_values = []
    for p in sorted(csvs):
        df = pd.read_csv(p, nrows=100)
        all_columns.extend(df.columns.tolist())
        all_tables.append(p.stem)
        for col in df.columns:
            sample_values.extend(df[col].dropna().head(5).astype(str).tolist())

    clf = DomainClassifier()
    result = clf.get_domain_split_summary(
        table_names=all_tables,
        all_columns=all_columns,
        sample_values=sample_values,
    )
    print("\n" + "="*60)
    print("CLASSIFICATION RESULT FOR DEMO FOLDER (all CSVs combined)")
    print("="*60)
    print("Tables:", all_tables)
    print("Columns:", all_columns[:30], "..." if len(all_columns) > 30 else "")
    print("-"*60)
    print("Primary domain:", result["primary_domain"])
    print("Confidence:", result["confidence"], "%")
    print("Percentages:")
    for d, pct in result["percentages"].items():
        print(f"  {d}: {pct}%")
    print("Sum:", sum(result["percentages"].values()), "%")
    print("Evidence:", result.get("evidence", [])[:6])
    print("="*60)
    return result

if __name__ == "__main__":
    test_demo_files()
