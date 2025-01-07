import boto3
import os
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

def download_csv_files(bucket_name, prefix, local_dir):
    try:
        # Create an S3 client
        s3 = boto3.client('s3', region_name='us-east-1')

        # Ensure the local directory exists
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)

        # List objects in the specified S3 bucket and prefix
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        if 'Contents' not in response:
            print("No files found in the specified prefix.")
            return

        file_count = 0

        # Download each file with .csv extension
        for obj in response['Contents']:
            file_key = obj['Key']
            file_name = os.path.basename(file_key)

            # Skip directories and non-CSV files
            if not file_name.endswith('.csv'):
                continue

            local_file_path = os.path.join(local_dir, file_name)
            print(f"Downloading {file_key} to {local_file_path}")

            # Download the file
            s3.download_file(bucket_name, file_key, local_file_path)
            file_count += 1

        print(f"Download complete. {file_count} .csv file(s) downloaded to {local_dir}.")

    except NoCredentialsError:
        print("No AWS credentials were found. Please configure them.")
    except PartialCredentialsError:
        print("Incomplete AWS credentials configuration.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # Specify your bucket name and prefix
    bucket_name = "cenxtgen-dev-brokersearch"
    prefix = "fmrdb/input/"
    local_dir = "./drop"

    # Call the function to download CSV files
    download_csv_files(bucket_name, prefix, local_dir)
