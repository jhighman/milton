import csv

def split_csv_rows(input_csv_path):
    with open(input_csv_path, 'r', encoding='utf-8-sig') as infile:
        reader = csv.DictReader(infile)
        headers = reader.fieldnames

        for row_number, row in enumerate(reader, start=1):
            output_csv_path = f"fc_test_{row_number}.csv"
            with open(output_csv_path, 'w', newline='', encoding='utf-8-sig') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=headers)
                writer.writeheader()
                writer.writerow(row)
            print(f"Created file: {output_csv_path}")

if __name__ == "__main__":
    input_csv_file = "EnderaSampleList.csv"  # Replace with your actual CSV file name
    split_csv_rows(input_csv_file)

