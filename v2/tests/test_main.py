import unittest
import os
import json
import csv
import logging
import shutil
import pytest
from datetime import datetime
from unittest.mock import Mock, patch, mock_open, MagicMock
from collections import OrderedDict

# Import main.py from the parent directory
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import main as cp
from main_config import DEFAULT_CONFIG
from main_file_utils import load_checkpoint, save_checkpoint, get_csv_files
from main_csv_processing import CSVProcessor

class TestComplianceProcessor(unittest.TestCase):

    def setUp(self):
        # Just set up paths and reset globals, no directory creation
        self.input_folder = "drop"
        self.output_folder = "output"
        self.archive_folder = "archive"
        self.logger = logging.getLogger('main')
        self.logger.handlers = []  # Clear handlers to avoid duplicate logging
        self.logger.addHandler(logging.NullHandler())

    def tearDown(self):
        # No cleanup needed
        pass

    @patch('builtins.open', new_callable=mock_open, read_data='{"evaluate_name": false}')
    def test_load_config_file_exists(self, mock_file):
        config = cp.load_config()
        self.assertEqual(config, {**DEFAULT_CONFIG, "evaluate_name": False})

    @patch('builtins.open', side_effect=FileNotFoundError)
    @patch('main_config.logger')
    def test_load_config_file_not_found(self, mock_logger, mock_file):
        config = cp.load_config()
        self.assertEqual(config, DEFAULT_CONFIG)
        mock_logger.warning.assert_called_once_with("Config file not found, using defaults")

    def test_generate_reference_id_with_crd(self):
        csv_processor = CSVProcessor()
        ref_id = csv_processor.generate_reference_id("12345")
        self.assertEqual(ref_id, "12345")

    def test_generate_reference_id_no_crd(self):
        csv_processor = CSVProcessor()
        ref_id = csv_processor.generate_reference_id()
        self.assertTrue(ref_id.startswith("DEF-"))
        self.assertTrue(len(ref_id) >= 14)  # "DEF-" + 12 digits

    def test_load_checkpoint(self):
        mock_storage_manager = MagicMock()
        mock_storage_manager.file_exists.return_value = True
        mock_storage_manager.read_file.return_value = '{"csv_file": "test.csv", "line_number": 5}'
        checkpoint = load_checkpoint(mock_storage_manager)
        self.assertEqual(checkpoint, ("test.csv", 5))

    def test_load_checkpoint_not_found(self):
        mock_storage_manager = MagicMock()
        mock_storage_manager.file_exists.return_value = False
        checkpoint = load_checkpoint(mock_storage_manager)
        self.assertEqual(checkpoint, ('', 0))

    @patch('storage_manager.StorageManager.write_file')
    def test_save_checkpoint(self, mock_write_file):
        mock_storage_manager = MagicMock()
        save_checkpoint("test.csv", 10, mock_storage_manager)
        mock_storage_manager.write_file.assert_called_once()

    def test_save_checkpoint_invalid(self):
        mock_storage_manager = MagicMock()
        # This should not raise an exception
        save_checkpoint(None, None, mock_storage_manager)
        # Verify that write_file was called with the None values
        mock_storage_manager.write_file.assert_called_once()

    def test_get_csv_files(self):
        mock_storage_manager = MagicMock()
        mock_storage_manager.list_files.return_value = ['file1.csv', 'file3.csv']
        files = get_csv_files(mock_storage_manager)
        self.assertEqual(files, ['file1.csv', 'file3.csv'])

    def test_resolve_headers(self):
        fieldnames = ['First Name', 'CRD Number', 'unknown_field']
        csv_processor = CSVProcessor()
        resolved = csv_processor.resolve_headers(fieldnames)
        self.assertEqual(resolved, {
            'First Name': 'first_name',
            'CRD Number': 'crd_number',
            'unknown_field': 'unknown_field'  # Unknown fields are kept as-is
        })

    @patch('main_csv_processing.CSVProcessor.process_row')
    @patch('main_file_utils.save_checkpoint')
    def test_process_csv(self, mock_save, mock_process_row):
        # Create a mock storage manager
        mock_storage_manager = MagicMock()
        mock_storage_manager.read_file.return_value = b"first_name,crd_number\nJohn,12345"
        
        # Create a CSV processor and set the storage manager
        csv_processor = CSVProcessor()
        csv_processor.set_storage_manager(mock_storage_manager)
        
        # Process the CSV
        facade = Mock()
        config = DEFAULT_CONFIG
        csv_processor.process_csv("test.csv", 0, facade, config, 0.1)
        
        # Verify the process_row method was called
        mock_process_row.assert_called_once()

    @patch('business.process_claim', return_value={"reference_id": "123"})
    def test_process_row_success(self, mock_process_claim):
        # Create a mock storage manager
        mock_storage_manager = MagicMock()
        
        # Create a CSV processor and set the storage manager
        csv_processor = CSVProcessor()
        csv_processor.set_storage_manager(mock_storage_manager)
        
        # Create test data
        row = {"first_name": "John", "last_name": "Doe", "employee_number": "EMP123", "reference_id": "123"}
        facade = Mock()
        facade.process_record.return_value = {"reference_id": "123"}
        config = DEFAULT_CONFIG
        
        # Process the row
        csv_processor.process_row(row, facade, config, 0)
        
        # Verify the facade was called
        facade.process_record.assert_called_once()

    @patch('main_csv_processing.logger')
    def test_process_row_error(self, mock_logger):
        # Create a mock storage manager
        mock_storage_manager = MagicMock()
        
        # Create a CSV processor and set the storage manager
        csv_processor = CSVProcessor()
        csv_processor.set_storage_manager(mock_storage_manager)
        
        # Create test data
        row = {"first_name": "John", "last_name": "Doe", "employee_number": "EMP123"}
        facade = Mock()
        facade.process_record.side_effect = Exception("Test error")
        config = DEFAULT_CONFIG
        
        # The process_row method doesn't catch exceptions, so we expect it to raise
        with self.assertRaises(Exception):
            csv_processor.process_row(row, facade, config, 0)

    @patch('main.load_config', return_value=DEFAULT_CONFIG)
    @patch('main.run_batch_processing')
    @patch('argparse.ArgumentParser.parse_args')
    def test_main_batch_processing(self, mock_parse_args, mock_run_batch, mock_config):
        # Mock the argparse.parse_args to return a mock object with the expected attributes
        mock_args = MagicMock()
        mock_args.diagnostic = False
        mock_args.wait_time = 0.1
        mock_args.skip_disciplinary = False
        mock_args.skip_arbitration = False
        mock_args.skip_regulatory = False
        mock_args.headless = False
        mock_parse_args.return_value = mock_args
        
        # Mock the input to select batch processing
        with patch('builtins.input', return_value="1"):
            with patch('main.FinancialServicesFacade', return_value=Mock()):
                cp.main(test_mode=True)
                # Verify that run_batch_processing was called
                mock_run_batch.assert_called_once()
    @patch('main_menu_helper.logger')
    @patch('argparse.ArgumentParser.parse_args')
    def test_main_exit(self, mock_parse_args, mock_logger):
        # Mock the argparse.parse_args to return a mock object with the expected attributes
        mock_args = MagicMock()
        mock_args.diagnostic = False
        mock_args.wait_time = 0.1
        mock_args.skip_disciplinary = False
        mock_args.skip_arbitration = False
        mock_args.skip_regulatory = False
        mock_args.headless = False
        mock_parse_args.return_value = mock_args
        
        with patch('builtins.input', return_value="11"):  # Changed to 11 which is the exit option in the new menu
            with patch('builtins.print') as mock_print:
                cp.main(test_mode=True)
                # Verify that the menu helper logged the exit
                mock_logger.info.assert_called_with("User chose to exit")

if __name__ == '__main__':
    unittest.main()