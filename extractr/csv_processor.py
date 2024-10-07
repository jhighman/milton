import os
import csv
import shutil
from datetime import datetime

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
        self.last_processed_line = -1  # Default to processing from the start

    def process_files(self):
        """
        Process all CSV files in the input folder, starting from the last processed file and line if checkpoint is available.
        """
        # Load checkpoint if exists
        checkpoint_data = self.checkpoint_manager.load_checkpoint()
        self.current_csv_file = checkpoint_data.get('current_csv_file', None)
        self.last_processed_line = checkpoint_data.get('last_processed_line', -1)

        # Get the list of CSV files
        csv_files = [f for f in os.listdir(self.input_folder) if f.endswith('.csv')]
        if not csv_files:
            self.logger.info("No CSV files found in the input folder.")
            return

        # Sort the files and process
        csv_files.sort()

        # If there is a checkpoint, resume from that file and line
        if self.current_csv_file and self.current_csv_file in csv_files:
            csv_files = csv_files[csv_files.index(self.current_csv_file):]
        else:
            # If no valid checkpoint file, start fresh from the beginning
            self.last_processed_line = -1

        # Process each CSV file
        for csv_file in csv_files:
            self.current_csv_file = csv_file
            csv_file_path = os.path.join(self.input_folder, csv_file)
            self._process_csv_file(csv_file_path)

            # After processing the file, archive it
            self._archive_file(csv_file)

            # Reset checkpoint for next file
            self.last_processed_line = -1

            # Remove checkpoint after successfully processing the file
            self.checkpoint_manager.remove_checkpoint()

    def _process_csv_file(self, csv_file_path):
        """
        Process an individual CSV file, handling row processing, evaluations, and checkpoint saving.
        """
        with open(csv_file_path, 'r') as csvfile:
            csv_reader = csv.DictReader(csvfile)
            records = list(csv_reader)

            for index, row in enumerate(records):
                # Skip already processed lines based on the checkpoint
                if index <= self.last_processed_line:
                    continue

                # Process each row and perform evaluations
                self._process_row(row)

                # Update the last processed line and save the checkpoint
                self.last_processed_line = index
                self.checkpoint_manager.save_checkpoint({
                    'current_csv_file': self.current_csv_file,
                    'last_processed_line': self.last_processed_line
                })

    def _process_row(self, row):
        """
        Process an individual row from the CSV file and perform the necessary evaluations.
        """
        # Implement your row processing and evaluation logic here
        self.logger.info(f"Processing row: {row}")
        # Your evaluation logic here

    def _archive_file(self, csv_file):
        """
        Archive a processed CSV file to the archive folder with a timestamp.
        """
        current_date = datetime.now().strftime("%m-%d-%Y")
        archive_subfolder = os.path.join(self.archive_folder, current_date)
        os.makedirs(archive_subfolder, exist_ok=True)
        shutil.move(os.path.join(self.input_folder, csv_file), os.path.join(archive_subfolder, csv_file))
        self.logger.info(f"Archived file: {csv_file}")
