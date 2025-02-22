Feature: Due Diligence with CRD and Organization Name

  Scenario Outline: CRD and org name with BrokerCheck or org lookup
    Given "crd_number" = "<crd_number>"
      And "organization_name" = "<org_name>"
      And no "organization_crd_number"
      And BrokerCheck indicates <brokercheck_result>
      And the organization lookup for "<org_name>" returns "<org_lookup>"
    When I process the claim
    Then the system should choose "search_with_crd_and_org_name"
      And when BrokerCheck is "hit" the final source is "BrokerCheck" and compliance is true with explanation "Record found via BrokerCheck."
      And when BrokerCheck is "no hit" and org lookup is "NOT_FOUND" the final source is "Entity_Search" and error "unknown organization by lookup" is returned with compliance false
      And when BrokerCheck is "no hit" and org lookup is not "NOT_FOUND" the final source is "Entity_Search" and error "Entity search using the derived org_crd_number is not supported even though we found one for this organization." is returned with compliance false
    Examples:
      | crd_number | org_name | brokercheck_result | org_lookup |
      | 11111      | Known    | hit                | 12345      |
      | 22222      | Unknown  | no hit             | NOT_FOUND  |
      | 33333      | Known    | no hit             | 12345      |