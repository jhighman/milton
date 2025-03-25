import unittest
from unittest.mock import Mock, patch
import json
from datetime import datetime
from pathlib import Path
from agents.compliance_report_agent import (
    save_compliance_report,
    convert_to_serializable,
    has_significant_changes,
    get_storage_provider
)
from evaluation_processor import Alert, AlertSeverity

class TestComplianceReportAgent(unittest.TestCase):
    def setUp(self):
        """Set up test environment before each test."""
        # Create a mock storage provider
        self.mock_storage = Mock()
        self.mock_storage.create_directory.return_value = True
        self.mock_storage.list_files.return_value = []
        self.mock_storage.read_file.return_value = json.dumps({})
        self.mock_storage.write_file.return_value = True

        # Patch the storage provider
        self.storage_patcher = patch('agents.compliance_report_agent._storage_provider', self.mock_storage)
        self.storage_patcher.start()

        # Test data
        self.test_report = {
            "reference_id": "TEST-001",
            "claim": {
                "employee_number": "EMP001"
            },
            "final_evaluation": {
                "overall_compliance": True,
                "alerts": []
            }
        }

    def tearDown(self):
        """Clean up after each test."""
        self.storage_patcher.stop()

    def test_save_compliance_report_new_file(self):
        """Test saving a new compliance report when no previous version exists."""
        # Configure mock storage
        self.mock_storage.list_files.return_value = []
        
        # Save the report
        result = save_compliance_report(self.test_report)
        
        # Verify the result
        self.assertTrue(result)
        
        # Verify storage provider calls
        self.mock_storage.create_directory.assert_called_once_with("cache/EMP001")
        self.mock_storage.list_files.assert_called_once_with("cache/EMP001")
        
        # Verify write_file was called with correct data
        write_call = self.mock_storage.write_file.call_args
        self.assertIsNotNone(write_call)
        file_path = write_call[0][0]
        self.assertTrue(file_path.startswith("cache/EMP001/ComplianceReportAgent_TEST-001_v1_"))
        self.assertTrue(file_path.endswith(".json"))

    def test_save_compliance_report_with_existing_file(self):
        """Test saving a compliance report when a previous version exists."""
        # Configure mock storage to return an existing file
        date = datetime.now().strftime("%Y%m%d")
        existing_file = f"ComplianceReportAgent_TEST-001_v1_{date}.json"
        self.mock_storage.list_files.return_value = [existing_file]
        
        # Configure mock storage to return existing report content
        existing_report = {
            "reference_id": "TEST-001",
            "final_evaluation": {
                "overall_compliance": False,
                "alerts": []
            }
        }
        self.mock_storage.read_file.return_value = json.dumps(existing_report)
        
        # Save the report
        result = save_compliance_report(self.test_report)
        
        # Verify the result
        self.assertTrue(result)
        
        # Verify storage provider calls
        self.mock_storage.create_directory.assert_called_once_with("cache/EMP001")
        self.mock_storage.list_files.assert_called_once_with("cache/EMP001")
        self.mock_storage.read_file.assert_called_once_with(f"cache/EMP001/{existing_file}")
        
        # Verify write_file was called with correct data
        write_call = self.mock_storage.write_file.call_args
        self.assertIsNotNone(write_call)
        file_path = write_call[0][0]
        self.assertTrue(file_path.startswith("cache/EMP001/ComplianceReportAgent_TEST-001_v2_"))
        self.assertTrue(file_path.endswith(".json"))

    def test_save_compliance_report_no_changes(self):
        """Test saving a compliance report when no significant changes are detected."""
        # Configure mock storage to return an existing file
        date = datetime.now().strftime("%Y%m%d")
        existing_file = f"ComplianceReportAgent_TEST-001_v1_{date}.json"
        self.mock_storage.list_files.return_value = [existing_file]
        
        # Configure mock storage to return existing report content
        existing_report = {
            "reference_id": "TEST-001",
            "final_evaluation": {
                "overall_compliance": True,
                "alerts": []
            }
        }
        self.mock_storage.read_file.return_value = json.dumps(existing_report)
        
        # Save the report
        result = save_compliance_report(self.test_report)
        
        # Verify the result
        self.assertTrue(result)
        
        # Verify storage provider calls
        self.mock_storage.create_directory.assert_called_once_with("cache/EMP001")
        self.mock_storage.list_files.assert_called_once_with("cache/EMP001")
        self.mock_storage.read_file.assert_called_once_with(f"cache/EMP001/{existing_file}")
        
        # Verify write_file was not called (no changes detected)
        self.mock_storage.write_file.assert_not_called()

    def test_save_compliance_report_with_alerts(self):
        """Test saving a compliance report with alerts."""
        # Create a test alert
        test_alert = Alert(
            alert_type="Test Alert",
            severity=AlertSeverity.HIGH,
            metadata={"test": "data"},
            description="Test alert description",
            alert_category="test"
        )
        
        # Update test report with alerts
        self.test_report["final_evaluation"]["alerts"] = [test_alert]
        
        # Save the report
        result = save_compliance_report(self.test_report)
        
        # Verify the result
        self.assertTrue(result)
        
        # Verify write_file was called with correct data
        write_call = self.mock_storage.write_file.call_args
        self.assertIsNotNone(write_call)
        file_path = write_call[0][0]
        self.assertTrue(file_path.startswith("cache/EMP001/ComplianceReportAgent_TEST-001_v1_"))
        self.assertTrue(file_path.endswith(".json"))
        
        # Verify the saved content includes the alert
        saved_content = write_call[0][1]
        saved_data = json.loads(saved_content)
        self.assertEqual(len(saved_data["final_evaluation"]["alerts"]), 1)
        self.assertEqual(saved_data["final_evaluation"]["alerts"][0]["severity"], "HIGH")

    def test_save_compliance_report_invalid_data(self):
        """Test saving a compliance report with invalid data."""
        # Test with invalid report data
        result = save_compliance_report(None)
        self.assertFalse(result)
        
        # Test with missing reference_id
        invalid_report = {"claim": {"employee_number": "EMP001"}}
        result = save_compliance_report(invalid_report)
        self.assertFalse(result)

    def test_convert_to_serializable(self):
        """Test the convert_to_serializable function."""
        # Test with Alert object
        alert = Alert(
            alert_type="Test Alert",
            severity=AlertSeverity.HIGH,
            metadata={"test": "data"},
            description="Test alert description",
            alert_category="test"
        )
        result = convert_to_serializable(alert)
        self.assertIsInstance(result, dict)
        self.assertEqual(result["severity"], "HIGH")
        
        # Test with nested structure
        test_data = {
            "alerts": [alert],
            "nested": {
                "alert": alert
            }
        }
        result = convert_to_serializable(test_data)
        self.assertIsInstance(result["alerts"][0], dict)
        self.assertIsInstance(result["nested"]["alert"], dict)

    def test_has_significant_changes(self):
        """Test the has_significant_changes function."""
        # Test with no changes
        old_report = {
            "final_evaluation": {
                "overall_compliance": True,
                "alerts": []
            }
        }
        new_report = {
            "final_evaluation": {
                "overall_compliance": True,
                "alerts": []
            }
        }
        self.assertFalse(has_significant_changes(new_report, old_report))
        
        # Test with compliance change
        new_report["final_evaluation"]["overall_compliance"] = False
        self.assertTrue(has_significant_changes(new_report, old_report))
        
        # Test with alert count change
        new_report["final_evaluation"]["overall_compliance"] = True
        new_report["final_evaluation"]["alerts"] = [{"severity": "high"}]
        self.assertTrue(has_significant_changes(new_report, old_report))

if __name__ == '__main__':
    unittest.main() 