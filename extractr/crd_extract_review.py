import pandas as pd

def analyze_crd_dump(csv_path):
    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(csv_path)

    # Total number of rows
    total_rows = len(df)

    # --- CRD - Company ---
    # Missing (NaN)
    company_missing_count = df['CRD - Company'].isna().sum()
    # "N/A"
    company_na_count = (df['CRD - Company'] == 'N/A').sum()
    # Has actual value (neither NaN nor "N/A")
    company_has_value_count = (
        df['CRD - Company'].notna() & (df['CRD - Company'] != 'N/A')
    ).sum()

    # Calculate percentages
    company_missing_percent = (company_missing_count / total_rows) * 100
    company_na_percent = (company_na_count / total_rows) * 100

    # --- CRD - Person ---
    # Missing (NaN)
    person_missing_count = df['CRD - Person'].isna().sum()
    # "N/A"
    person_na_count = (df['CRD - Person'] == 'N/A').sum()
    # Has actual value (neither NaN nor "N/A")
    person_has_value_count = (
        df['CRD - Person'].notna() & (df['CRD - Person'] != 'N/A')
    ).sum()

    # Calculate percentages
    person_missing_percent = (person_missing_count / total_rows) * 100
    person_na_percent = (person_na_count / total_rows) * 100

    # --- Missing Both CRD Fields ---
    # Count of rows that have NaN in *both* CRD - Company and CRD - Person
    both_missing_count = (
        df['CRD - Company'].isna() & df['CRD - Person'].isna()
    ).sum()
    both_missing_percent = (both_missing_count / total_rows) * 100

    # Generate a simple report
    report = f"""
CRD_Dump Analysis:
-----------------
Total rows: {total_rows}

-- CRD - Company --
Missing (NaN):
  Count: {company_missing_count}
  Percentage: {company_missing_percent:.2f}%
"N/A":
  Count: {company_na_count}
  Percentage: {company_na_percent:.2f}%
Has actual value (not missing, not "N/A"):
  Count: {company_has_value_count}

-- CRD - Person --
Missing (NaN):
  Count: {person_missing_count}
  Percentage: {person_missing_percent:.2f}%
"N/A":
  Count: {person_na_count}
  Percentage: {person_na_percent:.2f}%
Has actual value (not missing, not "N/A"):
  Count: {person_has_value_count}

-- Missing Both CRD - Company AND CRD - Person --
Count: {both_missing_count}
Percentage: {both_missing_percent:.2f}%
"""
    return report

if __name__ == "__main__":
    csv_file_path = "CRD_Dump.csv"  # Adjust path if needed
    result = analyze_crd_dump(csv_file_path)
    print(result)
