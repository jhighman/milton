# tests/test_checkpoint_manager.py

import unittest
import os
import json
from checkpoint_manager import CheckpointManager
from unittest.mock import MagicMock

class TestCheckpointManager(unittest.TestCase):

    def setUp(self):
        self.checkpoint_file = 'tests/test_checkpoint.json'
        self.logger = MagicMock()
        self.checkpoint_manager = CheckpointManager(self.checkpoint_file, self.logger)

    def tearDown(self):
        if os.path.exists(self.checkpoint_file):
            os.remove(self.checkpoint_file)

    def test_save_and_load_checkpoint(self):
        data = {'current_csv_file': 'test.csv', 'last_processed_line': 5}
        self.checkpoint_manager.save_checkpoint(data)
        loaded_data = self.checkpoint_manager.load_checkpoint()
        self.assertEqual(data, loaded_data)

    def test_remove_checkpoint(self):
        data = {'current_csv_file': 'test.csv', 'last_processed_line': 5}
        self.checkpoint_manager.save_checkpoint(data)
        self.checkpoint_manager.remove_checkpoint()
        self.assertFalse(os.path.exists(self.checkpoint_file))

if __name__ == '__main__':
    unittest.main()
