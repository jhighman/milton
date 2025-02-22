Feature: Due Diligence Search - CRD Only

  Scenario Outline: CRD-only claim with BrokerCheck or SEC IAPD
    Given "crd_number" = "<crd_number>"
      And no "organization_crd_number"
      And no "organization_name"
      And BrokerCheck indicates <brokercheck_result>
      And SEC IAPD indicates <iapd_result>
    When I process the claim
    Then the system should choose "search_with_crd_only"
      And when BrokerCheck is "hit" the final source is "BrokerCheck" and compliance is true with explanation "Record found via BrokerCheck."
      And when BrokerCheck is "no hit" and SEC IAPD is "hit" the final source is "SEC_IAPD" and compliance is true with explanation "Record found via SEC_IAPD."
      And when BrokerCheck is "no hit" and SEC IAPD is "no hit" the final source is "SEC_IAPD" and compliance is false with explanation "No records found"

    Examples:
      | crd_number | brokercheck_result | iapd_result |
      | 11111      | hit                | hit         |
      | 22222      | hit                | no hit      |
      | 33333      | no hit             | hit         |
      | 44444      | no hit             | no hit      |