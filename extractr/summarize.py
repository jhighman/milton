import pandas as pd

# Load the aggregated CSV report
data = pd.read_csv('aggregated_report.csv')

# Calculate the total records
total_records = len(data)

# Calculate records found and not found based on compliance indicators
# Assuming 'search_compliance' column indicates whether a match was found (e.g., True for found, False for not found)
records_found = data['search_compliance'].sum()
records_not_found = total_records - records_found

# Group by search strategy to get breakdowns for each
search_strategy_totals = data.groupby('search_strategy')['search_compliance'].agg(
    found='sum',  # Count records with a successful match
    not_found=lambda x: (x == False).sum()  # Count records without a match
).reset_index()

# Display the summary
print("Compliance Summary:")
print(f"Total Records: {total_records}")
print(f"Total Records Found: {records_found}")
print(f"Total Records Not Found: {records_not_found}\n")

print("Breakdown by Search Strategy:")
print(search_strategy_totals)

# Add metrics to the summary for detailed reporting
metrics = {
    'Overall Compliance Rate': (data['overall_compliance'].sum() / total_records) * 100,
    'Name Match Compliance Rate': (data['name_match'].sum() / total_records) * 100,
    'License Compliance Rate': (data['license_compliance'].sum() / total_records) * 100,
    'Exam Compliance Rate': (data['exam_compliance'].sum() / total_records) * 100,
    'Disclosure Compliance Rate': (data['disclosure_compliance'].sum() / total_records) * 100,
    'High-Risk Alerts Rate': (data['highest_alert_severity'] == 'High').sum() / total_records * 100,
    'Missing Disclosure Rate': (data['total_disclosures'] == 0).sum() / total_records * 100
}

print("\nAdditional Compliance Metrics:")
for metric, value in metrics.items():
    print(f"{metric}: {value:.2f}%")
 