import pytest
from pytest_bdd import scenarios, given, when, then, parsers
from unittest.mock import MagicMock
from pathlib import Path

from business import process_claim
from services import FinancialServicesFacade

# Fix the path to be relative to the steps directory
scenarios('../features/due_diligence_correlated_search.feature')

# Fixtures
@pytest.fixture
def claim_fixture() -> dict:
    return {}

@pytest.fixture
def mock_facade(mocker) -> FinancialServicesFacade:
    facade = FinancialServicesFacade()
    mocker.patch.object(facade, 'search_finra_brokercheck_individual', return_value=None)
    mocker.patch.object(facade, 'search_finra_brokercheck_detailed', return_value=None)
    mocker.patch.object(facade, 'search_sec_iapd_individual', return_value=None)
    mocker.patch.object(facade, 'search_sec_iapd_detailed', return_value=None)
    mocker.patch.object(facade, 'get_organization_crd', return_value=None)
    mocker.patch.object(facade, 'search_sec_iapd_correlated', return_value=None)
    return facade

@pytest.fixture
def result_fixture() -> dict:
    return {}

# Given Steps
@given(parsers.parse('"crd_number" = "{crd_number}"'))
def given_claim_with_crd_number(claim_fixture, crd_number):
    claim_fixture['crd_number'] = crd_number

@given(parsers.parse('"organization_crd_number" = "{org_crd}"'))
def given_org_crd_number(claim_fixture, org_crd):
    claim_fixture['organization_crd_number'] = org_crd

@given(parsers.parse('"organization_name" = "{org_name}"'))
def given_org_name(claim_fixture, org_name):
    claim_fixture['organization_name'] = org_name

@given('no "crd_number"')
def given_no_crd_number(claim_fixture):
    claim_fixture.pop('crd_number', None)

@given('no "organization_crd_number"')
def given_no_org_crd_number(claim_fixture):
    claim_fixture.pop('organization_crd_number', None)

@given('no "organization_name"')
def given_no_org_name(claim_fixture):
    claim_fixture.pop('organization_name', None)

@given(parsers.parse('BrokerCheck indicates {outcome}'))
def given_brokercheck_outcome(mock_facade, outcome):
    if outcome == 'hit':
        mock_facade.search_finra_brokercheck_individual.return_value = {"hits": {"total": 1, "hits": [{"_source": {"ind_source_id": "BC12345"}}]}}
        mock_facade.search_finra_brokercheck_detailed.return_value = {"detail": "some detailed info"}
    else:
        mock_facade.search_finra_brokercheck_individual.return_value = {"hits": {"total": 0, "hits": []}}
        mock_facade.search_finra_brokercheck_detailed.return_value = None

@given(parsers.parse('SEC IAPD indicates {outcome}'))
def given_sec_iapd_outcome(mock_facade, outcome):
    if outcome == 'hit':
        mock_facade.search_sec_iapd_individual.return_value = {"hits": {"total": 1, "hits": [{"_source": {"ind_source_id": "SEC12345"}}]}}
        mock_facade.search_sec_iapd_detailed.return_value = {"detail": "some sec iapd detail"}
    else:
        mock_facade.search_sec_iapd_individual.return_value = {"hits": {"total": 0, "hits": []}}
        mock_facade.search_sec_iapd_detailed.return_value = None

@given(parsers.parse('the organization lookup for "{org_name}" returns "{org_lookup}"'))
def given_org_lookup(mock_facade, org_name, org_lookup):
    mock_facade.get_organization_crd.return_value = org_lookup if org_lookup != "NOT_FOUND" else "NOT_FOUND"

@given(parsers.parse('"individual_name" = "{name}"'))
def given_individual_name(claim_fixture, name):
    claim_fixture['individual_name'] = name

@given(parsers.parse('"organization_crd_number" = "{org_crd}"'))
def given_organization_crd_number(claim_fixture, org_crd):
    claim_fixture['organization_crd_number'] = org_crd

@given(parsers.parse('SEC IAPD correlated search indicates {outcome}'))
def given_sec_iapd_correlated_outcome(mock_facade, outcome):
    if outcome == 'hit':
        mock_facade.search_sec_iapd_correlated.return_value = {
            "hits": {
                "total": 1,
                "hits": [{
                    "_source": {
                        "ind_name": "Matthew Vetto",
                        "ind_current_employments": [{"firm_id": "282563"}],
                        "ind_source_id": "12345"
                    }
                }]
            }
        }
        mock_facade.search_sec_iapd_detailed.return_value = {
            "basicInformation": {
                "firstName": "Matthew",
                "lastName": "Vetto",
                "individualId": "12345"
            },
            "currentRegistrations": [
                {"firm": {"crd": "282563"}}
            ],
            "registeredStates": ["NY", "CA"],
            "disclosureFlag": "N"
        }
    else:
        mock_facade.search_sec_iapd_correlated.return_value = {
            "hits": {"total": 0, "hits": []}
        }
        mock_facade.search_sec_iapd_detailed.return_value = None

# When Steps
@when('I process the claim')
def when_process_claim(claim_fixture, mock_facade, result_fixture):
    result = process_claim(claim_fixture, mock_facade, employee_number="EMPTEST")
    result_fixture.update(result)

@when(parsers.parse('I look up an organization named "{org_name}"'))
def when_lookup_organization(mock_facade, result_fixture, org_name):
    result = mock_facade.get_organization_crd(org_name)
    result_fixture['org_lookup_result'] = result

# Then Steps
@then(parsers.parse('the system should choose "{strategy_name}"'))
def then_check_strategy_used(result_fixture, strategy_name):
    eval_data = result_fixture.get('search_evaluation', {})
    actual_strategy = eval_data.get('search_strategy')
    assert actual_strategy == strategy_name, f"Expected {strategy_name}, got {actual_strategy}"

@then(parsers.parse('the final source is "{source_name}"'))
def then_final_source(result_fixture, source_name):
    actual_source = result_fixture.get('source')
    assert actual_source == source_name, f"Expected source='{source_name}', got '{actual_source}'"

@then('compliance is true')
def then_compliance_true(result_fixture):
    eval_data = result_fixture.get('search_evaluation', {})
    assert eval_data.get('compliance') is True, f"Expected compliance to be True, got {eval_data.get('compliance')}"

@then('compliance is false')
def then_compliance_false(result_fixture):
    eval_data = result_fixture.get('search_evaluation', {})
    assert eval_data.get('compliance') is False, f"Expected compliance to be False, got {eval_data.get('compliance')}"

@then(parsers.parse('the explanation includes "{fragment}"'))
def then_explanation_includes(result_fixture, fragment):
    eval_data = result_fixture.get('search_evaluation', {})
    explanation = eval_data.get('compliance_explanation', '')
    print(f"Expected fragment: '{fragment}'")
    print(f"Actual explanation: '{explanation}'")
    assert fragment in explanation, f"Expected '{fragment}' in '{explanation}'"

@then(parsers.parse('the error "{error_text}" is returned'))
def then_error_returned(result_fixture, error_text):
    eval_data = result_fixture.get('search_evaluation', {})
    outcome = eval_data.get('search_outcome', '')
    print(f"Expected error: '{error_text}', Actual outcome: '{outcome}'")
    assert error_text in outcome, f"Expected error '{error_text}' in '{outcome}'"

@then(parsers.parse('the organization lookup returns "{expected_result}"'))
def then_verify_organization_lookup(result_fixture, expected_result):
    actual_result = str(result_fixture.get('org_lookup_result'))
    assert actual_result == expected_result, f"Expected {expected_result}, got {actual_result}"

@then("the organization lookup returns a valid CRD")
def then_verify_valid_crd_lookup(result_fixture):
    result = str(result_fixture.get('org_lookup_result'))
    assert result.isdigit(), f"Expected a numeric CRD, got {result}"

# Combined Then Steps with _pytest_bdd_example
@then(parsers.parse('when BrokerCheck is "hit" the final source is "{source_name}" and compliance is true with explanation "{fragment}"'))
def then_brokercheck_hit_combined(result_fixture, source_name, fragment, _pytest_bdd_example):
    brokercheck_result = _pytest_bdd_example["brokercheck_result"]
    if brokercheck_result == "hit":
        actual_source = result_fixture.get('source')
        eval_data = result_fixture.get('search_evaluation', {})
        explanation = eval_data.get('compliance_explanation', '')
        print(f"Expected source: '{source_name}', Actual source: '{actual_source}'")
        print(f"Expected compliance: True, Actual compliance: {eval_data.get('compliance')}")
        print(f"Expected fragment: '{fragment}', Actual explanation: '{explanation}'")
        assert actual_source == source_name, f"Expected source='{source_name}', got '{actual_source}'"
        assert eval_data.get('compliance') is True, f"Expected compliance to be True, got {eval_data.get('compliance')}"
        assert fragment in explanation, f"Expected '{fragment}' in '{explanation}'"

@then(parsers.parse('when BrokerCheck is "no hit" and org lookup is "NOT_FOUND" the final source is "{source_name}" and error "{error_text}" is returned with compliance false'))
def then_brokercheck_no_hit_org_not_found_combined(result_fixture, source_name, error_text, _pytest_bdd_example):
    brokercheck_result = _pytest_bdd_example["brokercheck_result"]
    org_lookup = _pytest_bdd_example["org_lookup"]
    if brokercheck_result == "no hit" and org_lookup == "NOT_FOUND":
        actual_source = result_fixture.get('source')
        eval_data = result_fixture.get('search_evaluation', {})
        outcome = eval_data.get('search_outcome', '')
        print(f"Expected source: '{source_name}', Actual source: '{actual_source}'")
        print(f"Expected error: '{error_text}', Actual outcome: '{outcome}'")
        print(f"Expected compliance: False, Actual compliance: {eval_data.get('compliance')}")
        assert actual_source == source_name, f"Expected source='{source_name}', got '{actual_source}'"
        assert error_text in outcome, f"Expected error '{error_text}' in '{outcome}'"
        assert eval_data.get('compliance') is False, f"Expected compliance to be False, got {eval_data.get('compliance')}"

@then(parsers.parse('when BrokerCheck is "no hit" and org lookup is not "NOT_FOUND" the final source is "{source_name}" and error "{error_text}" is returned with compliance false'))
def then_brokercheck_no_hit_org_found_combined(result_fixture, source_name, error_text, _pytest_bdd_example):
    brokercheck_result = _pytest_bdd_example["brokercheck_result"]
    org_lookup = _pytest_bdd_example["org_lookup"]
    if brokercheck_result == "no hit" and org_lookup != "NOT_FOUND":
        actual_source = result_fixture.get('source')
        eval_data = result_fixture.get('search_evaluation', {})
        outcome = eval_data.get('search_outcome', '')
        print(f"Expected source: '{source_name}', Actual source: '{actual_source}'")
        print(f"Expected error: '{error_text}', Actual outcome: '{outcome}'")
        print(f"Expected compliance: False, Actual compliance: {eval_data.get('compliance')}")
        assert actual_source == source_name, f"Expected source='{source_name}', got '{actual_source}'"
        assert error_text in outcome, f"Expected error '{error_text}' in '{outcome}'"
        assert eval_data.get('compliance') is False, f"Expected compliance to be False, got {eval_data.get('compliance')}"

@then(parsers.parse('when SEC IAPD is "hit" compliance is true and explanation includes "{fragment}"'))
def then_iapd_hit_combined(result_fixture, fragment, _pytest_bdd_example):
    iapd_result = _pytest_bdd_example["iapd_result"]
    if iapd_result == "hit":
        eval_data = result_fixture.get('search_evaluation', {})
        explanation = eval_data.get('compliance_explanation', '')
        print(f"Expected compliance: True, Actual compliance: {eval_data.get('compliance')}")
        print(f"Expected fragment: '{fragment}', Actual explanation: '{explanation}'")
        assert eval_data.get('compliance') is True, f"Expected compliance to be True, got {eval_data.get('compliance')}"
        assert fragment in explanation, f"Expected '{fragment}' in '{explanation}'"

@then(parsers.parse('when SEC IAPD is "no hit" compliance is false and explanation includes "{fragment}"'))
def then_iapd_no_hit_combined(result_fixture, fragment, _pytest_bdd_example):
    iapd_result = _pytest_bdd_example["iapd_result"]
    if iapd_result == "no hit":
        eval_data = result_fixture.get('search_evaluation', {})
        explanation = eval_data.get('compliance_explanation', '')
        print(f"Expected compliance: False, Actual compliance: {eval_data.get('compliance')}")
        print(f"Expected fragment: '{fragment}', Actual explanation: '{explanation}'")
        assert eval_data.get('compliance') is False, f"Expected compliance to be False, got {eval_data.get('compliance')}"
        assert fragment in explanation, f"Expected '{fragment}' in '{explanation}'"

@then(parsers.parse('when BrokerCheck is "no hit" and SEC IAPD is "hit" the final source is "{source_name}" and compliance is true with explanation "{fragment}"'))
def then_brokercheck_no_hit_sec_iapd_hit_combined(result_fixture, source_name, fragment, _pytest_bdd_example):
    brokercheck_result = _pytest_bdd_example["brokercheck_result"]
    iapd_result = _pytest_bdd_example["iapd_result"]
    if brokercheck_result == "no hit" and iapd_result == "hit":
        actual_source = result_fixture.get('source')
        eval_data = result_fixture.get('search_evaluation', {})
        explanation = eval_data.get('compliance_explanation', '')
        print(f"Expected source: '{source_name}', Actual source: '{actual_source}'")
        print(f"Expected compliance: True, Actual compliance: {eval_data.get('compliance')}")
        print(f"Expected fragment: '{fragment}', Actual explanation: '{explanation}'")
        assert actual_source == source_name, f"Expected source='{source_name}', got '{actual_source}'"
        assert eval_data.get('compliance') is True, f"Expected compliance to be True, got {eval_data.get('compliance')}"
        assert fragment in explanation, f"Expected '{fragment}' in '{explanation}'"

@then(parsers.parse('when BrokerCheck is "no hit" and SEC IAPD is "no hit" the final source is "{source_name}" and compliance is false with explanation "{fragment}"'))
def then_both_no_hit_combined(result_fixture, source_name, fragment, _pytest_bdd_example):
    brokercheck_result = _pytest_bdd_example["brokercheck_result"]
    iapd_result = _pytest_bdd_example["iapd_result"]
    if brokercheck_result == "no hit" and iapd_result == "no hit":
        actual_source = result_fixture.get('source')
        eval_data = result_fixture.get('search_evaluation', {})
        explanation = eval_data.get('compliance_explanation', '')
        print(f"Expected source: '{source_name}', Actual source: '{actual_source}'")
        print(f"Expected compliance: False, Actual compliance: {eval_data.get('compliance')}")
        print(f"Expected fragment: '{fragment}', Actual explanation: '{explanation}'")
        assert actual_source == source_name, f"Expected source='{source_name}', got '{actual_source}'"
        assert eval_data.get('compliance') is False, f"Expected compliance to be False, got {eval_data.get('compliance')}"
        assert fragment in explanation, f"Expected '{fragment}' in '{explanation}'"

@then(parsers.parse('if {outcome} = "hit"'))
def then_if_hit(result_fixture, outcome):
    """Handle the if hit condition - this is a pass-through step"""
    pass

@then(parsers.parse('if {outcome} = "no hit"'))
def then_if_no_hit(result_fixture, outcome):
    """Handle the if no hit condition - this is a pass-through step"""
    pass