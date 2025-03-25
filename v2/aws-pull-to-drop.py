"""
AWS S3 to Local File Pull Script

This script downloads CSV files from an S3 bucket to a local directory,
archives the files in S3, and creates a manifest of the operation.
"""

import os
import json
from datetime import datetime
from dotenv import load_dotenv
from storage_providers import StorageProviderFactory

# Load environment variables
load_dotenv()

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
                    "input_bucket": os.environ.get('S3_INPUT_BUCKET'),
                    "input_prefix": os.environ.get('S3_INPUT_FOLDER'),
                    "archive_bucket": os.environ.get('S3_INPUT_BUCKET'),
                    "archive_prefix": os.environ.get('S3_INPUT_ARCHIVE_FOLDER')
                },
                "local": {
                    "input_folder": os.environ.get('LOCAL_INPUT_FOLDER', './drop')
                }
            }
        }

def download_and_archive_csv_files():
    """
    1) Clear out CSV files in local input folder
    2) Download all CSV files from S3 input prefix -> local input folder
    3) Move each file in S3 to an archive subfolder named by today's date
    4) Create a manifest with counts. Upload manifest to S3 and also place in local input folder
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
        # 1) Clear out CSV files in the local input folder
        removed_count = 0
        input_folder = config["storage"]["local"]["input_folder"]
        
        if local_provider.file_exists(input_folder):
            files = local_provider.list_files(input_folder)
            for file in files:
                if file.lower().endswith('.csv'):
                    local_provider.delete_file(os.path.join(input_folder, file))
                    removed_count += 1
        else:
            local_provider.create_directory(input_folder)

        # 2) Download CSV files from S3
        input_prefix = config["storage"]["s3"]["input_prefix"]
        files = s3_provider.list_files(input_prefix)
        downloaded_count = 0

        for file_key in files:
            if not file_key.lower().endswith('.csv'):
                continue

            file_name = os.path.basename(file_key)
            local_file_path = os.path.join(input_folder, file_name)
            
            print(f"Downloading {file_key} -> {local_file_path}")
            
            # Download the file
            content = s3_provider.read_file(file_key)
            local_provider.write_file(local_file_path, content)
            downloaded_count += 1

            # 3) Move (archive) each downloaded file in S3
            today_str = datetime.now().strftime("%m-%d-%Y")
            archive_key = f"{config['storage']['s3']['archive_prefix'].strip('/')}/{today_str}/{file_name}"
            
            print(f"Archiving {file_key} -> {archive_key}")
            
            # Copy to archive and delete original
            s3_provider.move_file(file_key, archive_key)

        # 4) Create and save manifest
        manifest_filename = "manifest.txt"
        manifest_filepath = os.path.join(input_folder, manifest_filename)
        
        today_str = datetime.now().strftime("%m-%d-%Y")
        manifest_content = "\n".join([
            f"Date: {today_str}",
            f"Removed local CSV files: {removed_count}",
            f"Downloaded CSV files from S3: {downloaded_count}",
            ""
        ])
        
        # Write manifest locally
        local_provider.write_file(manifest_filepath, manifest_content)
        
        # Upload manifest to S3 archive
        archive_manifest_key = f"{config['storage']['s3']['archive_prefix'].strip('/')}/{today_str}/{manifest_filename}"
        print(f"Uploading manifest to S3: {archive_manifest_key}")
        s3_provider.write_file(archive_manifest_key, manifest_content)

        print(f"Successfully processed {downloaded_count} files")
        return True

    except Exception as e:
        print(f"Error during file processing: {str(e)}")
        return False

if __name__ == "__main__":
    success = download_and_archive_csv_files()
    exit(0 if success else 1)
