import unittest
import os
import json
import csv
import logging
import shutil
from datetime import datetime
from unittest.mock import Mock, patch, mock_open, MagicMock
from collections import OrderedDict

# Import main.py from the parent directory
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import main as cp

class TestComplianceProcessor(unittest.TestCase):

    def setUp(self):
        # Just set up paths and reset globals, no directory creation
        self.input_folder = "drop"
        self.output_folder = "output"
        self.archive_folder = "archive"
        cp.current_csv = None
        cp.current_line = 0
        self.logger = logging.getLogger('main')
        self.logger.handlers = []  # Clear handlers to avoid duplicate logging
        self.logger.addHandler(logging.NullHandler())

    def tearDown(self):
        # No cleanup needed
        pass

    @patch('builtins.open', new_callable=mock_open, read_data='{"evaluate_name": false}')
    def test_load_config_file_exists(self, mock_file):
        config = cp.load_config()
        self.assertEqual(config, {**cp.DEFAULT_CONFIG, "evaluate_name": False})

    @patch('builtins.open', side_effect=FileNotFoundError)
    @patch('main.logger')
    def test_load_config_file_not_found(self, mock_logger, mock_file):
        config = cp.load_config()
        self.assertEqual(config, cp.DEFAULT_CONFIG)
        mock_logger.warning.assert_called_once_with("Config file not found, using defaults")

    def test_generate_reference_id_with_crd(self):
        ref_id = cp.generate_reference_id("12345")
        self.assertEqual(ref_id, "12345")

    def test_generate_reference_id_no_crd(self):
        ref_id = cp.generate_reference_id()
        self.assertTrue(ref_id.startswith("DEF-"))
        self.assertTrue(len(ref_id) >= 14)  # "DEF-" + 12 digits

    @patch('builtins.open', new_callable=mock_open, read_data='{"csv_file": "test.csv", "line": 5}')
    def test_load_checkpoint(self, mock_file):
        checkpoint = cp.load_checkpoint()
        self.assertEqual(checkpoint, {"csv_file": "test.csv", "line": 5})

    @patch('builtins.open', side_effect=FileNotFoundError)
    def test_load_checkpoint_not_found(self, mock_file):
        checkpoint = cp.load_checkpoint()
        self.assertIsNone(checkpoint)

    @patch('builtins.open', new_callable=mock_open)
    @patch('json.dump')
    def test_save_checkpoint(self, mock_json_dump, mock_file):
        cp.save_checkpoint("test.csv", 10)
        mock_json_dump.assert_called_once_with({"csv_file": "test.csv", "line": 10}, mock_file())

    @patch('main.logger')
    def test_save_checkpoint_invalid(self, mock_logger):
        cp.save_checkpoint(None, None)
        mock_logger.error.assert_called_once_with("Cannot save checkpoint: csv_file=None, line_number=None")

    @patch('os.listdir', return_value=['file1.csv', 'file2.txt', 'file3.csv'])
    def test_get_csv_files(self, mock_listdir):
        files = cp.get_csv_files()
        self.assertEqual(files, ['file1.csv', 'file3.csv'])

    def test_resolve_headers(self):
        fieldnames = ['First Name', 'CRD Number', 'unknown_field']
        resolved = cp.resolve_headers(fieldnames)
        self.assertEqual(resolved, {
            'First Name': 'first_name',
            'CRD Number': 'crd_number'
        })

    @patch('builtins.open')
    @patch('main.process_row')
    @patch('main.save_checkpoint')
    def test_process_csv(self, mock_save, mock_process_row, mock_file):
        mock_file.return_value.__enter__.return_value = mock_open(
            read_data="first_name,crd_number\nJohn,12345").return_value
        facade = Mock()
        config = cp.DEFAULT_CONFIG
        cp.process_csv("drop/test.csv", 0, facade, config, 0.1)
        mock_process_row.assert_called_once()
        mock_save.assert_called()

    @patch('main.process_claim', return_value={"reference_id": "123"})
    @patch('builtins.open', new_callable=mock_open)
    def test_process_row_success(self, mock_file, mock_process_claim):
        row = {"first_name": "John", "last_name": "Doe", "employee_number": "EMP123", "reference_id": "123"}
        resolved_headers = {"first_name": "first_name", "last_name": "last_name", "employee_number": "employee_number", "reference_id": "reference_id"}
        facade = Mock()
        config = cp.DEFAULT_CONFIG
        cp.process_row(row, resolved_headers, facade, config)
        mock_process_claim.assert_called_once()
        mock_file.assert_called_once_with(os.path.join(cp.OUTPUT_FOLDER, "123.json"), 'w')

    @patch('main.process_claim', side_effect=Exception("Test error"))
    @patch('builtins.open', new_callable=mock_open)
    @patch('main.logger')
    def test_process_row_error(self, mock_logger, mock_file, mock_process_claim):
        row = {"first_name": "John", "last_name": "Doe", "employee_number": "EMP123"}
        resolved_headers = {"first_name": "first_name", "last_name": "last_name", "employee_number": "employee_number"}
        facade = Mock()
        config = cp.DEFAULT_CONFIG
        cp.process_row(row, resolved_headers, facade, config)
        mock_logger.error.assert_called()
        mock_file.assert_called_once()

    @patch('main.process_row')
    @patch('main.get_csv_files', return_value=['test.csv'])
    @patch('main.load_checkpoint', return_value=None)
    @patch('main.archive_file')
    @patch('main.load_config', return_value=cp.DEFAULT_CONFIG)
    @patch('builtins.open')
    def test_main_batch_processing(self, mock_file, mock_config, mock_archive, mock_checkpoint, mock_get_csv, mock_process_row):
        # Mock the config file to return valid JSON
        mock_config_file = mock_open(read_data='{"evaluate_name": false}').return_value
        mock_csv_file = mock_open(read_data="first_name,last_name\nJohn,Doe").return_value
        
        def open_side_effect(filename, *args, **kwargs):
            if filename == 'config.json':
                return mock_config_file
            return mock_csv_file
            
        mock_file.side_effect = open_side_effect
            
        with patch('builtins.input', return_value="1"):
            with patch('main.FinancialServicesFacade', return_value=Mock()) as mock_facade:
                cp.main()
                mock_get_csv.assert_called_once()
                mock_archive.assert_called_once_with(os.path.join(cp.INPUT_FOLDER, "test.csv"))

    @patch('builtins.input', return_value="2")
    def test_main_exit(self, mock_input):
        with patch('builtins.print') as mock_print:
            cp.main()
            mock_print.assert_any_call("Exiting...")

if __name__ == "__main__":
    unittest.main()