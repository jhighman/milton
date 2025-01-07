import os
import boto3
import logging
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# S3 bucket details
S3_BUCKET = "cenxtgen-dev-brokersearch"
S3_FOLDER = "fmrdb/output/"
LOCAL_OUTPUT_FOLDER = "./output"

def upload_json_files_to_s3():
    """
    Scans the local /output folder for .json files and uploads them to S3.
    Counts the files and prints the count to the console.
    """
    try:
        # Initialize S3 client
        s3_client = boto3.client('s3')

        # Ensure the local output folder exists
        if not os.path.exists(LOCAL_OUTPUT_FOLDER):
            logging.error(f"Local folder '{LOCAL_OUTPUT_FOLDER}' does not exist.")
            return

        # Get list of .json files in the output folder
        json_files = [f for f in os.listdir(LOCAL_OUTPUT_FOLDER) if f.endswith('.json')]
        file_count = len(json_files)

        if file_count == 0:
            logging.info("No .json files found in the /output folder.")
            return

        logging.info(f"Found {file_count} .json file(s) in the /output folder.")

        # Upload each file to S3
        for file_name in json_files:
            local_file_path = os.path.join(LOCAL_OUTPUT_FOLDER, file_name)
            s3_key = os.path.join(S3_FOLDER, file_name)

            try:
                s3_client.upload_file(local_file_path, S3_BUCKET, s3_key)
                logging.info(f"Uploaded {file_name} to s3://{S3_BUCKET}/{s3_key}")
            except Exception as e:
                logging.error(f"Failed to upload {file_name}: {e}")

        # Print the count of files uploaded
        logging.info(f"Total files uploaded to S3: {file_count}")

    except NoCredentialsError:
        logging.error("AWS credentials not found. Please configure your credentials.")
    except PartialCredentialsError:
        logging.error("Incomplete AWS credentials. Please check your configuration.")
    except Exception as e:
        logging.exception(f"An error occurred: {e}")

if __name__ == "__main__":
    upload_json_files_to_s3()