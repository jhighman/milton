import csv

# File paths
input_file = 'EnderaSampleList.csv'
output_file = 'crd_qualified.csv'

# Filter records with a non-empty CRD field and write them to a new file
with open(input_file, mode='r', encoding='utf-8') as infile, open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
    reader = csv.DictReader(infile)
    # Create a writer with the same fieldnames to maintain the structure
    writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
    writer.writeheader()  # Write the header to the output file

    # Count records with non-empty CRD values
    crd_count = 0
    for row in reader:
        if row['CRD'].strip():  # Check if CRD is not empty or whitespace
            writer.writerow(row)
            crd_count += 1

    # Output the total number of records with CRD values
    print(f"Total records with CRD values: {crd_count}")
