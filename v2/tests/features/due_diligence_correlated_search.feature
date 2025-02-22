Feature: 7: Due Diligence Search - Correlated Search

  Scenario Outline: Claim with individual name and firm CRD
    Given "individual_name" = "<name>"
      And "firm_crd" = "<firm_crd>"
      And SEC IAPD correlated search indicates <search_outcome>
    When I process the claim
    Then the system should choose "search_with_correlated"
      And the final source is "SEC_IAPD"
      And if <search_outcome> = "hit"
        Then compliance is true
        And the explanation includes "Record found via SEC_IAPD."
      And if <search_outcome> = "no hit"
        Then compliance is false
        And the explanation includes "No records found"

    Examples:
      | name           | firm_crd | search_outcome |
      | Matthew Vetto  | 282563   | hit           |
      | Invalid Name   | 282563   | no hit        |
      | Matthew Vetto  | 999999   | no hit        | 