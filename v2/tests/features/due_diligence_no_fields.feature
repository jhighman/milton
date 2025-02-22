Feature: Due Diligence Search - No Identifiers

  Scenario: Claim with no identifiers
    Given no "crd_number"
      And no "organization_crd_number"
      And no "organization_name"
    When I process the claim
    Then the system should choose "search_default"
      And the final source is "Default"
      And the error "Insufficient identifiers to perform search" is returned
      And compliance is false