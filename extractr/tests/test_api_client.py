# tests/test_api_client.py

import unittest
from unittest.mock import patch, MagicMock
import os
import json
import requests
from api_client import ApiClient
from exceptions import RateLimitExceeded

class TestApiClient(unittest.TestCase):

    def setUp(self):
        # Set up an ApiClient instance with a test cache folder
        self.cache_folder = 'tests/test_cache'
        self.wait_time = 0  # Set to zero to avoid delays during testing
        self.logger = MagicMock()  # Mock logger
        self.api_client = ApiClient(self.cache_folder, self.wait_time, self.logger)

        # Ensure the cache folder is clean
        if not os.path.exists(self.cache_folder):
            os.makedirs(self.cache_folder)
        else:
            # Clean up cache folder before each test
            for f in os.listdir(self.cache_folder):
                os.remove(os.path.join(self.cache_folder, f))

    def tearDown(self):
        # Remove test cache folder after tests
        for f in os.listdir(self.cache_folder):
            os.remove(os.path.join(self.cache_folder, f))
        os.rmdir(self.cache_folder)

    @patch('api_client.requests.get')
    def test_get_individual_basic_info_success(self, mock_get):
        # Mock successful API response
        crd_number = '123456'
        expected_data = {'hits': {'hits': [{'_source': {'ind_firstname': 'John', 'ind_lastname': 'Doe'}}]}}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = expected_data
        mock_get.return_value = mock_response

        # Call the method
        result = self.api_client.get_individual_basic_info(crd_number)

        # Assertions
        self.assertEqual(result, expected_data)
        self.assertTrue(os.path.exists(os.path.join(self.cache_folder, f"{crd_number}_basic_info.json")))

    @patch('api_client.requests.get')
    def test_get_individual_basic_info_rate_limit(self, mock_get):
        # Mock rate limit error
        crd_number = '123456'
        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_get.return_value = mock_response

        # Call the method and expect RateLimitExceeded
        with self.assertRaises(RateLimitExceeded):
            self.api_client.get_individual_basic_info(crd_number)

    @patch('api_client.requests.get')
    def test_get_individual_basic_info_request_exception(self, mock_get):
        # Mock request exception
        crd_number = '123456'
        mock_get.side_effect = requests.exceptions.RequestException("Connection error")

        # Call the method and expect None
        result = self.api_client.get_individual_basic_info(crd_number)
        self.assertIsNone(result)

    def test_get_individual_basic_info_cached(self):
        # Write cached data
        crd_number = '123456'
        cached_data = {'cached': True}
        cache_file = os.path.join(self.cache_folder, f"{crd_number}_basic_info.json")
        with open(cache_file, 'w') as f:
            json.dump(cached_data, f)

        # Call the method
        result = self.api_client.get_individual_basic_info(crd_number)

        # Assertions
        self.assertEqual(result, cached_data)
        self.logger.info.assert_called_with(f"Retrieved basic info for CRD {crd_number} from cache.")

if __name__ == '__main__':
    unittest.main()
