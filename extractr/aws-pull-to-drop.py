import boto3
import os
from datetime import datetime
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

def download_and_archive_csv_files(bucket_name, prefix, local_dir, archive_subfolder):
    """
    1) Clear out CSV files in local_dir.
    2) Download all CSV files from S3 prefix -> local_dir.
    3) Move each file in S3 to an archive subfolder named by today's date.
    4) Create a manifest with counts. Upload manifest to S3 and also place in local_dir.
    """
    try:
        # Create an S3 client
        s3 = boto3.client('s3', region_name='us-east-1')

        # 1) Clear out CSV files in the local_dir, count how many were removed
        removed_count = 0
        if os.path.isdir(local_dir):
            for f in os.listdir(local_dir):
                if f.lower().endswith('.csv'):
                    os.remove(os.path.join(local_dir, f))
                    removed_count += 1
        else:
            # If the directory doesn't exist, create it
            os.makedirs(local_dir)

        # 2) Download CSV files from S3
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        if 'Contents' not in response:
            print("No files found in the specified prefix.")
            downloaded_count = 0
        else:
            downloaded_count = 0

            for obj in response['Contents']:
                file_key = obj['Key']
                file_name = os.path.basename(file_key)

                # Skip directories and non-CSV files
                if not file_name.endswith('.csv'):
                    continue

                local_file_path = os.path.join(local_dir, file_name)
                print(f"Downloading {file_key} -> {local_file_path}")

                # Download the file to local_dir
                s3.download_file(bucket_name, file_key, local_file_path)
                downloaded_count += 1

                # 3) Move (archive) each downloaded file in S3
                #    to a date-stamped subfolder in archive_subfolder
                today_str = datetime.now().strftime("%m-%d-%Y")
                archive_key = f"{archive_subfolder.strip('/')}/{today_str}/{file_name}"

                print(f"Archiving {file_key} -> {archive_key}")

                # Copy the file to the new archive location
                s3.copy_object(
                    Bucket=bucket_name,
                    CopySource={'Bucket': bucket_name, 'Key': file_key},
                    Key=archive_key
                )

                # Delete the original file from the source prefix
                print(f"Deleting source file {file_key}")
                s3.delete_object(Bucket=bucket_name, Key=file_key)

        # 4) Create a manifest.txt with the required info
        manifest_filename = "manifest.txt"
        manifest_filepath = os.path.join(local_dir, manifest_filename)

        # Build the manifest content
        today_str = datetime.now().strftime("%m-%d-%Y")
        manifest_lines = [
            f"Date: {today_str}",
            f"Removed local CSV files: {removed_count}",
            f"Downloaded CSV files from S3: {downloaded_count}",
            ""
        ]
        manifest_content = "\n".join(manifest_lines)

        # Write the manifest locally
        with open(manifest_filepath, 'w') as mf:
            mf.write(manifest_content)

        # 5) Upload the manifest to the correct archive folder in S3
        archive_manifest_key = f"{archive_subfolder.strip('/')}/{today_str}/{manifest_filename}"
        print(f"Uploading manifest to S3: {archive_manifest_key}")
        s3.upload_file(manifest_filepath, bucket_name, archive_manifest_key)

        print("---- JOB COMPLETE ----")
        print(manifest_content)

    except NoCredentialsError:
        print("No AWS credentials were found. Please configure them.")
    except PartialCredentialsError:
        print("Incomplete AWS credentials configuration.")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    # Update these variables as needed
    bucket_name = "cenxtgen-dev-brokersearch"
    prefix = "fmrdb/input/"
    local_dir = "./drop"
    archive_subfolder = "fmrdb/input_archive"  # Updated to correct archive location

    download_and_archive_csv_files(bucket_name, prefix, local_dir, archive_subfolder)
