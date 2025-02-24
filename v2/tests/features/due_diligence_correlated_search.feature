Feature: 7: Due Diligence Search - Correlated Search

  Scenario Outline: Claim with individual name and firm CRD
    Given "individual_name" = "<name>"
      And "organization_crd_number" = "<org_crd>"
      And SEC IAPD correlated search indicates <search_outcome>
    When I process the claim
    Then the system should choose "search_with_correlated"
      And the final source is "SEC_IAPD"


    Examples:
      | name           | org_crd | search_outcome |
      | Matthew Vetto  | 282563  | hit           |
