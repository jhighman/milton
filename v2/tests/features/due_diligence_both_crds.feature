Feature: Due Diligence Search - Both CRDs

  Scenario Outline: Claim with both crd_number and organization_crd_number
    Given "crd_number" = "<crd_number>"
      And "organization_crd_number" = "<org_crd>"
      And no "organization_name"
      And SEC IAPD indicates <iapd_result>
    When I process the claim
    Then the system should choose "search_with_both_crds"
      And the final source is "SEC_IAPD"
      And when SEC IAPD is "hit" compliance is true and explanation includes "Record found via SEC_IAPD."
      # Matches log from business.py
      And when SEC IAPD is "no hit" compliance is false and explanation includes "No records found"
    Examples:
      | crd_number | org_crd | iapd_result |
      | 11111      | 99999   | hit         |
      | 22222      | 88888   | no hit      |