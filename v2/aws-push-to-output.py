import os
import boto3
import logging
import shutil

from datetime import datetime
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from dotenv import load_dotenv

load_dotenv()

# Access environment variables
S3_OUTPUT_BUCKET = os.environ.get('S3_OUTPUT_BUCKET')
S3_OUTPUT_FOLDER = os.environ.get('S3_OUTPUT_FOLDER')
LOCAL_OUTPUT_FOLDER = os.environ.get('LOCAL_OUTPUT_FOLDER')
LOCAL_ARCHIVE_SUBFOLDER = os.environ.get('LOCAL_ARCHIVE_SUBFOLDER')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# S3 bucket details
#S3_BUCKET = "cenxtgen-dev-brokersearch-response"            
#S3_FOLDER = "fmrdb/output/"
#LOCAL_OUTPUT_FOLDER = "./output"
#LOCAL_ARCHIVE_SUBFOLDER = "archive"  # Local archive subfolder name

def upload_json_files_to_s3_with_local_archive(manifest_filename="manifest.txt"):
    """
    Scans the local /output folder for .json files and uploads them to S3.
    - Checks if each file already exists in S3 destination; skips upload if it does.
    - Moves all processed files to a date-stamped local archive folder.
    - Appends to a manifest.txt detailing the upload activity with timestamps.
    - Uploads the manifest to S3 and also places it in the local archive.
    """
    try:
        # Initialize S3 client
        s3_client = boto3.client('s3')
        
        # Create manifest file path
        manifest_path = os.path.join(LOCAL_OUTPUT_FOLDER, manifest_filename)
        
        # Ensure the local output folder exists
        if not os.path.exists(LOCAL_OUTPUT_FOLDER):
            logging.error(f"Local folder '{LOCAL_OUTPUT_FOLDER}' does not exist.")
            return

        # Get list of .json files in the output folder
        json_files = [f for f in os.listdir(LOCAL_OUTPUT_FOLDER) if f.lower().endswith('.json')]
        total_files = len(json_files)

        if total_files == 0:
            logging.info("No .json files found in the /output folder.")
            return

        logging.info(f"Found {total_files} .json file(s) in the /output folder.")

        # Prepare date-stamped archive folder
        today_str = datetime.now().strftime("%m-%d-%Y")
        local_archive_path = os.path.join(LOCAL_OUTPUT_FOLDER, LOCAL_ARCHIVE_SUBFOLDER, today_str)
        os.makedirs(local_archive_path, exist_ok=True)
        logging.info(f"Archive folder created at '{local_archive_path}'.")

        # Initialize counters and lists for manifest
        uploaded_count = 0
        skipped_count = 0
        uploaded_files = []
        skipped_files = []

        # Iterate over all .json files
        for file_name in json_files:
            local_file_path = os.path.join(LOCAL_OUTPUT_FOLDER, file_name)
            s3_key = os.path.join(S3_OUTPUT_FOLDER, file_name).replace("\\", "/")  # Ensure S3 key uses forward slashes

            # Check if the file already exists in S3
            try:
                s3_client.head_object(Bucket=S3_OUTPUT_BUCKET, Key=s3_key)
                file_exists = True
                logging.info(f"File already exists in S3: {s3_key}. Skipping upload.")
            except s3_client.exceptions.ClientError as e:
                if e.response['Error']['Code'] == '404':
                    file_exists = False
                else:
                    logging.error(f"Error checking existence of {s3_key} in S3: {e}")
                    continue  # Skip to next file on error

            if file_exists:
                skipped_count += 1
                skipped_files.append(file_name)
            else:
                # Upload the file to S3
                try:
                    s3_client.upload_file(local_file_path, S3_OUTPUT_BUCKET, s3_key)
                    logging.info(f"Uploaded {file_name} to s3://{S3_OUTPUT_BUCKET}/{s3_key}")
                    uploaded_count += 1
                    uploaded_files.append(file_name)
                except Exception as e:
                    logging.error(f"Failed to upload {file_name}: {e}")
                    continue  # Skip moving the file if upload failed

            # Move the file to the local archive folder
            try:
                shutil.move(local_file_path, os.path.join(local_archive_path, file_name))
                logging.info(f"Moved {file_name} to archive folder.")
            except Exception as e:
                logging.error(f"Failed to move {file_name} to archive: {e}")

        # 4) Append to the manifest.txt with the required info and timestamp
        manifest_path_local = os.path.join(local_archive_path, manifest_filename)
        timestamp = datetime.now().strftime("%m-%d-%Y %H:%M")

        manifest_entry = (
            f"Timestamp: {timestamp}\n"
            f"Total .json files processed: {uploaded_count + skipped_count}\n"
            f"Uploaded files: {uploaded_count}\n"
            f"Skipped files (already exist in S3): {skipped_count}\n"
            "----------------------------------------\n"
        )

        # Check if manifest exists
        if os.path.exists(manifest_path_local):
            # Append to existing manifest
            with open(manifest_path_local, 'a') as mf:
                mf.write(manifest_entry)
            logging.info(f"Appended to existing manifest at '{manifest_path_local}'.")
        else:
            # Create a new manifest
            with open(manifest_path_local, 'w') as mf:
                # Optional: Add a header
                mf.write("Manifest File\n")
                mf.write("========================================\n")
                mf.write(manifest_entry)
            logging.info(f"Created new manifest at '{manifest_path_local}'.")

        # 5) Upload the manifest to the S3 destination folder
        # Since S3 archiving is handled upstream, we'll upload the manifest directly to the S3_FOLDER
        s3_manifest_key = os.path.join(S3_OUTPUT_FOLDER, manifest_filename).replace("\\", "/")
        try:
            s3_client.upload_file(manifest_path_local, S3_OUTPUT_BUCKET, s3_manifest_key)
            logging.info(f"Uploaded manifest to s3://{S3_OUTPUT_BUCKET}/{s3_manifest_key}")
        except Exception as e:
            logging.error(f"Failed to upload manifest to S3: {e}")

        # Log the manifest content
        logging.info("---- MANIFEST CONTENT ----")
        logging.info("\n" + manifest_entry)
        logging.info("---- JOB COMPLETE ----")

    except Exception as e:
        logging.error(f"Error during S3 upload process: {e}")
    finally:
        logging.info("S3 upload process completed.")

if __name__ == "__main__":
    upload_json_files_to_s3_with_local_archive()
