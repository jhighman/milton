Feature: 5: Due Diligence Search - Organization Name Only

  Scenario Outline: Claim with only organization_name__entity_search_not_supported
    Given "organization_name" = "<org_name>"
      And no "crd_number"
      And no "organization_crd_number"
    When I process the claim
    Then the system should choose "search_with_org_name_only"
      And the final source is "Entity_Search"
      And the error "Entity search using organization_name is not supported at this time. Please provide an individual crd_number." is returned
      And compliance is false

    Examples:
      | org_name          |
      | "ABC Securities"  |
      | "XYZ Advisors"    |