Feature: Due Diligence Correlated Search
  As a compliance officer
  I want to search for individuals using correlated data
  So that I can verify their credentials and compliance status

  Scenario: Search with individual name and firm CRD
    Given "individual_name" = "Matthew Vetto"
    And "organization_crd_number" = "282563"
    And SEC IAPD correlated search indicates hit
    When I process the claim
    Then the system should choose "search_with_correlated"
    And the final source is "SEC_IAPD"
