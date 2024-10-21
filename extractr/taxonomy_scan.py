import os
import json
from collections import defaultdict

def extract_metadata_fields(metadata, fields_set, parent_key=''):
    """
    Recursively extracts fields from metadata and adds them to the fields_set,
    without including indices for list items in the field names.

    Args:
        metadata (dict or list): The metadata to extract fields from.
        fields_set (set): The set to store field names.
        parent_key (str): The prefix for nested fields.
    """
    if isinstance(metadata, dict):
        for key, value in metadata.items():
            full_key = f"{parent_key}.{key}" if parent_key else key
            fields_set.add(full_key)
            extract_metadata_fields(value, fields_set, parent_key=full_key)
    elif isinstance(metadata, list):
        for item in metadata:
            # For lists, we process each item without adding indices
            extract_metadata_fields(item, fields_set, parent_key=parent_key)
    else:
        # Base case: metadata is neither a dict nor a list
        pass

def build_alert_taxonomy(output_folder: str):
    """
    Scans the output folder for JSON files and builds an alert taxonomy,
    recursively extracting all fields from alerts and their nested metadata
    without including list indices.

    Args:
        output_folder (str): Path to the directory containing JSON files.
    """
    taxonomy = defaultdict(lambda: {'alert_fields': set(), 'metadata_fields': set()})

    # Get a list of all JSON files in the output directory
    json_files = [file for file in os.listdir(output_folder) if file.endswith('.json')]

    if not json_files:
        print(f"No JSON files found in the output folder '{output_folder}'")
        return

    # Process each JSON file
    for json_file in json_files:
        json_file_path = os.path.join(output_folder, json_file)
        try:
            with open(json_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Extract the alerts from final_evaluation
            final_evaluation = data.get('final_evaluation', {})
            alerts = final_evaluation.get('alerts', [])

            # Process each alert
            for alert in alerts:
                alert_type = alert.get('alert_type', 'Unknown')
                # Update alert-level fields
                taxonomy[alert_type]['alert_fields'].update(alert.keys())
                # Recursively extract metadata fields
                metadata = alert.get('metadata', {})
                if isinstance(metadata, (dict, list)):
                    extract_metadata_fields(metadata, taxonomy[alert_type]['metadata_fields'])
                else:
                    taxonomy[alert_type]['metadata_fields'].add('Non-dictionary metadata')

        except json.JSONDecodeError as e:
            print(f"JSON decode error in file '{json_file}': {e}")
        except Exception as e:
            print(f"Error processing file '{json_file}': {e}")

    # Write the taxonomy to a markdown file
    markdown_file = 'alert_taxonomy.md'
    with open(markdown_file, 'w', encoding='utf-8') as md_file:
        md_file.write("# Alert Taxonomy\n")
        
        for alert_type, fields in taxonomy.items():
            md_file.write(f"\n## Alert Type: {alert_type}\n")
            md_file.write(f"\n### Alert Fields:\n")
            for field in sorted(fields['alert_fields']):
                md_file.write(f"- {field}\n")

            md_file.write(f"\n### Metadata Fields:\n")
            for field in sorted(fields['metadata_fields']):
                md_file.write(f"- {field}\n")

    print(f"Alert taxonomy has been saved to '{markdown_file}'")

if __name__ == "__main__":
    # Define the output directory
    folder_path = './'
    output_folder = os.path.join(folder_path, 'output')

    # Build the alert taxonomy
    build_alert_taxonomy(output_folder=output_folder)
