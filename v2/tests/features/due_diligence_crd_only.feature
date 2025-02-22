Feature: 3: Due Diligence Search Variant - Organization CRD Only

  Scenario Outline: Claim with only organization_crd_number__entity_search_not_supported
    Given "organization_crd_number" = "<org_crd>"
      And no "crd_number"
      And no "organization_name"
    When I process the claim
    Then the system should choose "search_with_entity"
      And the final source is "Entity_Search"
      And the error "Entity search using organization_crd_number is not supported at this time. Please provide an individual crd_number." is returned
      And compliance is false

    Examples:
      | org_crd |
      | 12345   |
      | 99999   |
      | 88888   |