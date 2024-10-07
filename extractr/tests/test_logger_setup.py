# tests/test_logger_setup.py

import unittest
from logger_setup import setup_logger
import logging

class TestLoggerSetup(unittest.TestCase):

    def test_setup_logger(self):
        logger = setup_logger()
        self.assertIsInstance(logger, logging.Logger)
        self.assertEqual(logger.level, logging.DEBUG)

if __name__ == '__main__':
    unittest.main()
