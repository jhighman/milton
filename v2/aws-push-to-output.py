"""
AWS S3 Output Push Script

This script uploads JSON files from a local directory to an S3 bucket,
archives the files locally, and creates a manifest of the operation.
"""

import os
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
from storage_providers import StorageProviderFactory

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config():
    """Load storage configuration from config.json."""
    try:
        with open('config.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        # Fallback to environment variables if config.json doesn't exist
        return {
            "storage": {
                "mode": "s3",
                "s3": {
                    "aws_region": "us-east-1",
                    "output_bucket": os.environ.get('S3_OUTPUT_BUCKET'),
                    "output_prefix": os.environ.get('S3_OUTPUT_FOLDER')
                },
                "local": {
                    "output_folder": os.environ.get('LOCAL_OUTPUT_FOLDER', './output'),
                    "archive_folder": os.path.join(
                        os.environ.get('LOCAL_OUTPUT_FOLDER', './output'),
                        os.environ.get('LOCAL_ARCHIVE_SUBFOLDER', 'archive')
                    )
                }
            }
        }

def upload_json_files_to_s3_with_local_archive(manifest_filename="manifest.txt"):
    """
    Scans the local output folder for .json files and uploads them to S3.
    - Checks if each file already exists in S3 destination; skips upload if it does.
    - Moves all processed files to a date-stamped local archive folder.
    - Appends to a manifest.txt detailing the upload activity with timestamps.
    - Uploads the manifest to S3 and also places it in the local archive.
    """
    # Load configuration
    config = load_config()
    
    # Create storage providers
    s3_provider = StorageProviderFactory.create_provider(config)
    local_provider = StorageProviderFactory.create_provider({
        "storage": {
            "mode": "local",
            "local": config["storage"]["local"]
        }
    })

    try:
        # Get paths from config
        output_folder = config["storage"]["local"]["output_folder"]
        archive_folder = config["storage"]["local"]["archive_folder"]
        output_prefix = config["storage"]["s3"]["output_prefix"]

        # Ensure the local output folder exists
        if not local_provider.file_exists(output_folder):
            logging.error(f"Local folder '{output_folder}' does not exist.")
            return False

        # Get list of .json files in the output folder
        files = local_provider.list_files(output_folder)
        json_files = [f for f in files if f.lower().endswith('.json')]
        total_files = len(json_files)

        if total_files == 0:
            logging.info("No .json files found in the output folder.")
            return True

        logging.info(f"Found {total_files} .json file(s) in the output folder.")

        # Prepare date-stamped archive folder
        today_str = datetime.now().strftime("%m-%d-%Y")
        local_archive_path = os.path.join(archive_folder, today_str)
        local_provider.create_directory(local_archive_path)
        logging.info(f"Archive folder created at '{local_archive_path}'.")

        # Initialize counters and lists for manifest
        uploaded_count = 0
        skipped_count = 0
        uploaded_files = []
        skipped_files = []

        # Iterate over all .json files
        for file_name in json_files:
            local_file_path = os.path.join(output_folder, file_name)
            s3_key = os.path.join(output_prefix, file_name).replace("\\", "/")

            # Check if the file already exists in S3
            if s3_provider.file_exists(s3_key):
                logging.info(f"File already exists in S3: {s3_key}. Skipping upload.")
                skipped_count += 1
                skipped_files.append(file_name)
            else:
                # Upload the file to S3
                try:
                    content = local_provider.read_file(local_file_path)
                    s3_provider.write_file(s3_key, content)
                    logging.info(f"Uploaded {file_name} to s3://{config['storage']['s3']['output_bucket']}/{s3_key}")
                    uploaded_count += 1
                    uploaded_files.append(file_name)
                except Exception as e:
                    logging.error(f"Failed to upload {file_name}: {e}")
                    continue

            # Move the file to the local archive folder
            try:
                archive_path = os.path.join(local_archive_path, file_name)
                local_provider.move_file(local_file_path, archive_path)
                logging.info(f"Archived {file_name} to {archive_path}")
            except Exception as e:
                logging.error(f"Failed to archive {file_name}: {e}")

        # Create and save manifest
        manifest_content = "\n".join([
            f"Date: {today_str}",
            f"Total files processed: {total_files}",
            f"Files uploaded to S3: {uploaded_count}",
            f"Files skipped (already exist): {skipped_count}",
            "",
            "Uploaded files:",
            *[f"- {f}" for f in uploaded_files],
            "",
            "Skipped files:",
            *[f"- {f}" for f in skipped_files]
        ])

        # Save manifest locally
        manifest_path = os.path.join(local_archive_path, manifest_filename)
        local_provider.write_file(manifest_path, manifest_content)
        logging.info(f"Created manifest at {manifest_path}")

        # Upload manifest to S3
        s3_manifest_key = os.path.join(output_prefix, manifest_filename).replace("\\", "/")
        s3_provider.write_file(s3_manifest_key, manifest_content)
        logging.info(f"Uploaded manifest to S3: {s3_manifest_key}")

        return True

    except Exception as e:
        logging.error(f"Error during file processing: {str(e)}")
        return False

if __name__ == "__main__":
    success = upload_json_files_to_s3_with_local_archive()
    exit(0 if success else 1)
