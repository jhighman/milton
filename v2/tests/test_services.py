import unittest
import json
import sys
import os

# Adjust the import path to reach services.py from the tests folder
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from services import FinancialServicesFacade
from normalizer import create_individual_record

class TestNormalizeIndividualRecord(unittest.TestCase):
    def setUp(self):
        """Set up test data - no need for facade instance anymore as we're testing normalizer directly"""
        # Sample data from your provided results
        self.iapd_basic = {
            "hits": {
                "total": 1,
                "hits": [
                    {
                        "_type": "_doc",
                        "_source": {
                            "ind_source_id": "2112848",
                            "ind_firstname": "MATTHEW",
                            "ind_middlename": "LEE",
                            "ind_lastname": "VETTO",
                            "ind_other_names": ["Matthew Lee Vetto"],
                            "ind_bc_scope": "NotInScope",
                            "ind_ia_scope": "Active",
                            "ind_ia_disclosure_fl": "N",
                            "ind_approved_finra_registration_count": 0,
                            "ind_employments_count": 1,
                            "ind_industry_cal_date_iapd": "2021-11-17",
                            "ind_ia_current_employments": [
                                {
                                    "firm_id": "282563",
                                    "firm_name": "DOUGLAS C. LANE & ASSOCIATES",
                                    "branch_city": "NEW YORK",
                                    "branch_state": "NY",
                                    "branch_zip": "10017",
                                    "ia_only": "Y"
                                }
                            ]
                        }
                    }
                ]
            }
        }

        self.iapd_detailed = {
            "hits": {
                "total": 1,
                "hits": [
                    {
                        "_source": {
                            "iacontent": json.dumps({
                                "basicInformation": {
                                    "individualId": 2112848,
                                    "firstName": "MATTHEW",
                                    "middleName": "LEE",
                                    "lastName": "VETTO",
                                    "otherNames": ["Matthew Lee Vetto"],
                                    "bcScope": "NotInScope",
                                    "iaScope": "Active",
                                    "daysInIndustryCalculatedDateIAPD": "11/17/2021"
                                },
                                "currentIAEmployments": [
                                    {
                                        "firmId": 282563,
                                        "firmName": "DOUGLAS C. LANE & ASSOCIATES",
                                        "iaOnly": "Y",
                                        "registrationBeginDate": "11/18/2021",
                                        "firmBCScope": "NOTINSCOPE",
                                        "firmIAScope": "ACTIVE",
                                        "branchOfficeLocations": [
                                            {
                                                "displayOrder": 1,
                                                "locatedAtFlag": "Y",
                                                "supervisedFromFlag": "N",
                                                "privateResidenceFlag": "N",
                                                "branchOfficeId": "626424",
                                                "street1": "ONE DAG HAMMARSKJOLD PLAZA",
                                                "street2": "885 SECOND AVENUE, 42 FLOOR",
                                                "city": "NEW YORK",
                                                "state": "NY",
                                                "country": "United States",
                                                "zipCode": "10017"
                                            }
                                        ]
                                    }
                                ],
                                "disclosures": [],
                                "stateExamCategory": [
                                    {"examCategory": "Series 63", "examTakenDate": "9/22/1997", "examScope": "BC"}
                                ],
                                "productExamCategory": [
                                    {"examCategory": "Series 87", "examTakenDate": "12/30/2004", "examScope": "BC"},
                                    {"examCategory": "Series 7", "examTakenDate": "9/15/1997", "examScope": "BC"}
                                ],
                                "principalExamCategory": []
                            })
                        }
                    }
                ]
            }
        }

        self.bc_basic = {
            "hits": {
                "total": 1,
                "hits": [
                    {
                        "_type": "_doc",
                        "_source": {
                            "ind_source_id": "2722375",
                            "ind_firstname": "KRISTIN",
                            "ind_middlename": "LYNN",
                            "ind_lastname": "FAFARD",
                            "ind_other_names": ["KRISTIN LYNN BUSHARD", "KRISTIN LYNN SLUSSER"],
                            "ind_bc_scope": "Active",
                            "ind_ia_scope": "InActive",
                            "ind_bc_disclosure_fl": "N",
                            "ind_approved_finra_registration_count": 1,
                            "ind_employments_count": 2,
                            "ind_industry_cal_date": "2002-03-01",
                            "ind_current_employments": [
                                {
                                    "firm_id": "323408",
                                    "firm_name": "PINE DISTRIBUTORS LLC",
                                    "branch_city": "FORT LAUDERDALE",
                                    "branch_state": "FL",
                                    "branch_zip": "33324",
                                    "ia_only": "N",
                                    "firm_bd_sec_number": "71004"
                                },
                                {
                                    "firm_id": "323408",
                                    "firm_name": "PINE DISTRIBUTORS LLC",
                                    "branch_city": "Milford",
                                    "branch_state": "MA",
                                    "ia_only": "N",
                                    "firm_bd_sec_number": "71004"
                                }
                            ]
                        }
                    }
                ]
            }
        }

        self.bc_detailed = {
            "hits": {
                "total": 1,
                "hits": [
                    {
                        "_source": {
                            "content": json.dumps({
                                "basicInformation": {
                                    "individualId": 2722375,
                                    "firstName": "KRISTIN",
                                    "middleName": "LYNN",
                                    "lastName": "FAFARD",
                                    "otherNames": ["KRISTIN LYNN BUSHARD", "KRISTIN LYNN SLUSSER"],
                                    "bcScope": "Active",
                                    "iaScope": "InActive",
                                    "daysInIndustryCalculatedDate": "3/1/2002"
                                },
                                "currentEmployments": [
                                    {
                                        "firmId": 323408,
                                        "firmName": "PINE DISTRIBUTORS LLC",
                                        "iaOnly": "N",
                                        "registrationBeginDate": "1/6/2025",
                                        "firmBCScope": "ACTIVE",
                                        "firmIAScope": "NOTINSCOPE",
                                        "bdSECNumber": "71004",
                                        "branchOfficeLocations": [
                                            {
                                                "street1": "261 NORTH UNIVERSITY DRIVE",
                                                "street2": "SUITE 250",
                                                "city": "FORT LAUDERDALE",
                                                "state": "FL",
                                                "zipCode": "33324"
                                            }
                                        ]
                                    }
                                ],
                                "disclosures": [],
                                "stateExamCategory": [
                                    {"examCategory": "Series 63", "examTakenDate": "7/12/2018", "examScope": "BC"}
                                ],
                                "principalExamCategory": [
                                    {"examCategory": "Series 24", "examTakenDate": "10/23/2007", "examScope": "BC"}
                                ],
                                "productExamCategory": [
                                    {"examCategory": "Series 7", "examTakenDate": "6/25/2007", "examScope": "BC"}
                                ]
                            })
                        }
                    }
                ]
            }
        }

    # Positive Invariants
    def test_iapd_normalization_with_basic_and_detailed(self):
        """Test IAPD normalization with both basic and detailed data."""
        result = create_individual_record("IAPD", self.iapd_basic, self.iapd_detailed)
        
        # Basic info checks
        self.assertEqual(result["crd_number"], "2112848")
        self.assertEqual(result["fetched_name"], "MATTHEW LEE VETTO")
        self.assertEqual(result["other_names"], ["Matthew Lee Vetto"])
        self.assertEqual(result["bc_scope"], "NotInScope")
        self.assertEqual(result["ia_scope"], "Active")
        
        # Employment checks
        self.assertEqual(len(result["current_ia_employments"]), 1)
        employment = result["current_ia_employments"][0]
        self.assertEqual(employment["firm_id"], "282563")
        self.assertEqual(employment["firm_name"], "DOUGLAS C. LANE & ASSOCIATES")
        self.assertEqual(employment["registration_begin_date"], "11/18/2021")
        
        # Branch office checks (from detailed data)
        branch = employment["branch_offices"][0]
        self.assertEqual(branch["street"], "ONE DAG HAMMARSKJOLD PLAZA")
        self.assertEqual(branch["city"], "NEW YORK")
        self.assertEqual(branch["state"], "NY")
        self.assertEqual(branch["zip_code"], "10017")
        
        # Exam and disclosure checks
        self.assertEqual(result["disclosures"], [])
        self.assertEqual(len(result["exams"]), 3)  # Series 63, 87, 7
        expected_exams = [
            {"examCategory": "Series 63", "examTakenDate": "9/22/1997", "examScope": "BC"},
            {"examCategory": "Series 87", "examTakenDate": "12/30/2004", "examScope": "BC"},
            {"examCategory": "Series 7", "examTakenDate": "9/15/1997", "examScope": "BC"}
        ]
        for exam in expected_exams:
            self.assertIn(exam, result["exams"])

    def test_iapd_normalization_with_basic_only(self):
        """Test IAPD normalization with only basic data."""
        result = create_individual_record("IAPD", self.iapd_basic)
        
        # Basic info checks
        self.assertEqual(result["crd_number"], "2112848")
        self.assertEqual(result["fetched_name"], "MATTHEW LEE VETTO")
        self.assertEqual(result["bc_scope"], "NotInScope")
        self.assertEqual(result["ia_scope"], "Active")
        
        # Employment checks from ind_ia_current_employments
        self.assertEqual(len(result["current_ia_employments"]), 1)
        employment = result["current_ia_employments"][0]
        self.assertEqual(employment["firm_id"], "282563")
        self.assertEqual(employment["firm_name"], "DOUGLAS C. LANE & ASSOCIATES")
        
        # Branch office checks from basic data
        branch = employment["branch_offices"][0]
        self.assertIsNone(branch.get("street"))
        self.assertEqual(branch["city"], "NEW YORK")
        self.assertEqual(branch["state"], "NY")
        self.assertEqual(branch["zip_code"], "10017")
        
        # Empty lists for detailed data
        self.assertEqual(result["disclosures"], [])
        self.assertEqual(result["exams"], [])

    def test_bc_normalization_with_basic_and_detailed(self):
        """Test BrokerCheck normalization with both basic and detailed data."""
        result = create_individual_record("BrokerCheck", self.bc_basic, self.bc_detailed)
        
        self.assertEqual(result["crd_number"], "2722375")
        self.assertEqual(result["fetched_name"], "KRISTIN LYNN FAFARD")
        self.assertEqual(result["other_names"], ["KRISTIN LYNN BUSHARD", "KRISTIN LYNN SLUSSER"])
        self.assertEqual(result["bc_scope"], "Active")
        self.assertEqual(result["ia_scope"], "InActive")
        self.assertEqual(len(result["current_ia_employments"]), 0)  # BrokerCheck doesn't use this
        self.assertEqual(result["disclosures"], [])
        self.assertEqual(len(result["exams"]), 3)  # Series 63, 24, 7
        self.assertIn({"examCategory": "Series 24", "examTakenDate": "10/23/2007", "examScope": "BC"}, result["exams"])

    def test_bc_normalization_with_basic_only(self):
        """Test BrokerCheck normalization with only basic data."""
        result = create_individual_record("BrokerCheck", self.bc_basic)
        
        self.assertEqual(result["fetched_name"], "KRISTIN LYNN FAFARD")
        self.assertEqual(result["bc_scope"], "Active")
        self.assertEqual(result["ia_scope"], "InActive")
        self.assertEqual(result["disclosures"], [])
        self.assertEqual(result["exams"], [])

    # Negative Invariants
    def test_invalid_data_source(self):
        """Test normalization with an invalid data source."""
        result = create_individual_record("InvalidSource", self.iapd_basic)
        
        self.assertEqual(result["fetched_name"], "")
        self.assertEqual(result["crd_number"], None)
        self.assertEqual(result["disclosures"], [])
        self.assertEqual(result["exams"], [])
        self.assertEqual(result["current_ia_employments"], [])

    def test_no_basic_info(self):
        """Test normalization with no basic info provided."""
        result = create_individual_record("IAPD", None)
        
        self.assertEqual(result["fetched_name"], "")
        self.assertEqual(result["crd_number"], None)
        self.assertEqual(result["disclosures"], [])
        self.assertEqual(result["exams"], [])
        self.assertEqual(result["current_ia_employments"], [])

    def test_empty_hits(self):
        """Test normalization with basic info but no hits."""
        empty_basic = {"hits": {"total": 0, "hits": []}}
        result = create_individual_record("BrokerCheck", empty_basic)
        
        self.assertEqual(result["fetched_name"], "")
        self.assertEqual(result["crd_number"], None)
        self.assertEqual(result["disclosures"], [])
        self.assertEqual(result["exams"], [])
        self.assertEqual(result["current_ia_employments"], [])

    def test_malformed_detailed(self):
        """Test normalization with malformed detailed data."""
        malformed_detailed = {"invalid": "data"}  # No hits structure
        result = create_individual_record("IAPD", self.iapd_basic, malformed_detailed)
        
        self.assertEqual(result["fetched_name"], "MATTHEW LEE VETTO")
        self.assertEqual(result["bc_scope"], "NotInScope")
        self.assertEqual(result["ia_scope"], "Active")
        self.assertEqual(result["disclosures"], [])  # No disclosures from malformed detailed
        self.assertEqual(result["exams"], [])  # No exams from malformed detailed

    def test_iapd_normalization_with_string_firm_crd(self):
        """Test IAPD normalization when firm_crd is a string."""
        modified_basic = self.iapd_basic.copy()
        modified_basic["hits"]["hits"][0]["_source"]["ind_ia_current_employments"][0]["firm_id"] = "282563"
        
        result = create_individual_record("IAPD", modified_basic)
        
        self.assertEqual(result["current_ia_employments"][0]["firm_id"], "282563")  # Should keep as string
        self.assertEqual(result["current_ia_employments"][0]["firm_name"], "DOUGLAS C. LANE & ASSOCIATES")

    def test_iapd_normalization_with_invalid_firm_crd(self):
        """Test IAPD normalization when firm_crd is invalid."""
        modified_basic = self.iapd_basic.copy()
        modified_basic["hits"]["hits"][0]["_source"]["ind_ia_current_employments"][0]["firm_id"] = "invalid"
        
        result = create_individual_record("IAPD", modified_basic)
        
        self.assertEqual(result["current_ia_employments"][0]["firm_id"], "invalid")  # Should keep as string
        self.assertEqual(result["current_ia_employments"][0]["firm_name"], "DOUGLAS C. LANE & ASSOCIATES")

    def test_iapd_normalization_with_none_firm_crd(self):
        """Test IAPD normalization when firm_crd is None."""
        modified_basic = self.iapd_basic.copy()
        modified_basic["hits"]["hits"][0]["_source"]["ind_ia_current_employments"][0]["firm_id"] = None
        
        result = create_individual_record("IAPD", modified_basic)
        
        self.assertIsNone(result["current_ia_employments"][0]["firm_id"])  # Should be None
        self.assertEqual(result["current_ia_employments"][0]["firm_name"], "DOUGLAS C. LANE & ASSOCIATES")

if __name__ == "__main__":
    unittest.main()