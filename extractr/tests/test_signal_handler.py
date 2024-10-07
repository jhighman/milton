import unittest
from unittest.mock import patch, ANY
import signal
from signal_handler import register_signal_handlers
from unittest import mock

class TestSignalHandler(unittest.TestCase):

    @patch('signal.signal')
    def test_register_signal_handlers(self, mock_signal):
        mock_callback = mock.Mock()
        
        # Call the function with the mock callback
        register_signal_handlers(save_checkpoint_callback=mock_callback)
        
        # Check if signal.signal was called with SIGINT
        mock_signal.assert_any_call(signal.SIGINT, ANY)
        
        # Check if signal.signal was called with SIGTERM
        mock_signal.assert_any_call(signal.SIGTERM, ANY)

if __name__ == '__main__':
    unittest.main()
