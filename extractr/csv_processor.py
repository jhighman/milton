# csv_processor.py

import os
import csv
import json
from datetime import datetime
from exceptions import RateLimitExceeded
from evaluation_library import (
    evaluate_name, evaluate_license, evaluate_exams,
    evaluate_registration_status, evaluate_disclosures, get_passed_exams
)

class CsvProcessor:
    def __init__(self, api_client, config, logger, checkpoint_manager, input_folder, output_folder, archive_folder):
        self.api_client = api_client
        self.config = config
        self.logger = logger
        self.checkpoint_manager = checkpoint_manager
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.archive_folder = archive_folder
        self.current_csv_file = None
        self.last_processed_line = -1
        self.records_written = 0
        self.files_processed = 0

    def process_files(self):
        os.makedirs(self.output_folder, exist_ok=True)
        os.makedirs(self.archive_folder, exist_ok=True)

        checkpoint_data = self.checkpoint_manager.load_checkpoint()
        self.current_csv_file = checkpoint_data.get('current_csv_file')
        self.last_processed_line = checkpoint_data.get('last_processed_line', -1)

        csv_files = [f for f in os.listdir(self.input_folder) if f.endswith('.csv')]
        if not csv_files:
            self.logger.info("No CSV files found in the input folder.")
            return

        csv_files.sort()

        if self.current_csv_file:
            if self.current_csv_file in csv_files:
                csv_files = csv_files[csv_files.index(self.current_csv_file):]
            else:
                self.logger.warning(f"Checkpoint file {self.current_csv_file} not found in input folder.")
                self.last_processed_line = -1

        for csv_file in csv_files:
            self.current_csv_file = csv_file
            csv_file_path = os.path.join(self.input_folder, csv_file)
            self.process_csv(csv_file_path)
            self.last_processed_line = -1

            current_date = datetime.now().strftime("%m-%d-%Y")
            archive_subfolder = os.path.join(self.archive_folder, current_date)
            os.makedirs(archive_subfolder, exist_ok=True)
            dest_path = os.path.join(archive_subfolder, csv_file)
            os.replace(csv_file_path, dest_path)

            self.files_processed += 1
            self.checkpoint_manager.remove_checkpoint()

        self.logger.info(f"Processing complete! Files processed: {self.files_processed}, Records written: {self.records_written}")

    def process_csv(self, csv_file_path):
        with open(csv_file_path, 'r') as csvfile:
            csv_reader = csv.DictReader(csvfile)
            records = list(csv_reader)

            for index, row in enumerate(records):
                if index <= self.last_processed_line:
                    continue

                try:
                    crd_number = row['crd_number']
                    last_name = row['last_name']
                    first_name = row['first_name']
                    name = f"{first_name} {last_name}"
                    license_type = row.get('license_type', '')
                except KeyError as e:
                    missing_key = str(e).strip("'")
                    self.logger.warning(f"Missing key '{missing_key}' in row: {row}")
                    continue

                self.logger.info(f"Processing CRD {crd_number}")

                try:
                    basic_info = self.api_client.get_individual_basic_info(crd_number)
                    detailed_info = self.api_client.get_individual_detailed_info(crd_number)
                except RateLimitExceeded as e:
                    self.logger.error(str(e))
                    self.logger.info(f"Processed {self.records_written} records before rate limiting.")
                    self.checkpoint_manager.save_checkpoint(self.current_csv_file, index)
                    return

                if basic_info and detailed_info:
                    # Perform evaluations and save results...
                    self.records_written += 1

                self.checkpoint_manager.save_checkpoint(self.current_csv_file, index)
