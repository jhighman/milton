# test_evaluation_library.py
import unittest
from evaluation_library import evaluate_name, evaluate_license

class TestEvaluationFunctions(unittest.TestCase):
    
    def test_evaluate_name(self):
        result, alert = evaluate_name("John Doe", "John Doe", [])
        self.assertTrue(result)
        self.assertIsNone(alert)
        
        result, alert = evaluate_name("John Doe", "Jane Doe", [])
        self.assertFalse(result)
        self.assertIsNotNone(alert)

    def test_evaluate_license(self):
        result, alert = evaluate_license("B", "active", "active", "John Doe")
        self.assertTrue(result)
        self.assertIsNone(alert)

if __name__ == '__main__':
    unittest.main()
