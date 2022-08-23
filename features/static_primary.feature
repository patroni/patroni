Feature: static primary
  We should check that static primary behavior is safe

  Scenario: check static primary config in dcs blocks replica from starting
    Given I start postgres0 as static primary
    Then postgres0 is a leader after 10 seconds
    And there is a non empty initialize key in DCS after 15 seconds
    When I issue a PATCH request to http://127.0.0.1:8008/config with {"ttl": 20, "loop_wait": 2}
    Then I receive a response code 200
    When I start postgres1 with a configured static primary will not boot after 20 seconds
    And I start postgres2 with a configured static primary will not boot after 20 seconds
    And "sync" key not in DCS after waiting 20 seconds
    And "members/postgres1" is stopped and uninitialized after waiting 10 seconds
    # NOTE: no need to wait an additional 10 seconds here.
    And "members/postgres2" is stopped and uninitialized after waiting 1 seconds

  Scenario: check removing static primary config from dcs allows replica startup
    Given I issue a PATCH request to http://127.0.0.1:8008/config with {"static_primary": null}
    Then "sync" key in DCS has leader=postgres0 after 20 seconds
    And "members/postgres1" key in DCS has state=running after 10 seconds
    And "members/postgres2" key in DCS has state=running after 10 seconds
