# tests/test_config_loader.py

import unittest
import os
import json
from config_loader import load_config

class TestConfigLoader(unittest.TestCase):

    def setUp(self):
        self.config_file = 'tests/test_config.json'
        self.config_data = {'evaluate_name': True}
        with open(self.config_file, 'w') as f:
            json.dump(self.config_data, f)

    def tearDown(self):
        if os.path.exists(self.config_file):
            os.remove(self.config_file)

    def test_load_config_success(self):
        config = load_config(self.config_file)
        self.assertEqual(config, self.config_data)

    def test_load_config_file_not_found(self):
        with self.assertRaises(FileNotFoundError):
            load_config('non_existent_config.json')

if __name__ == '__main__':
    unittest.main()
