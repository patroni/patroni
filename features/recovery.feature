Feature: recovery
  We want to check that crashed postgres is started back

  Scenario: check that timeline is not incremented when primary is started after crash
    Given I start postgres0
    Then postgres0 is a leader after 10 seconds
    And there is a non empty initialize key in DCS after 15 seconds
    When I start postgres1
    And I add the table foo to postgres0
    Then table foo is present on postgres1 after 20 seconds
    When I kill postmaster on postgres0
    Then postgres0 role is the primary after 10 seconds
    When I issue a GET request to http://127.0.0.1:8008/
    Then I receive a response code 200
    And I receive a response role master
    And I receive a response timeline 1
    And "members/postgres0" key in DCS has state=running after 12 seconds
    And replication works from postgres0 to postgres1 after 15 seconds

  Scenario: check immediate failover when master_start_timeout=0
    Given I issue a PATCH request to http://127.0.0.1:8008/config with {"master_start_timeout": 0}
    Then I receive a response code 200
    And Response on GET http://127.0.0.1:8008/config contains master_start_timeout after 10 seconds
    When I kill postmaster on postgres0
    Then postgres1 is a leader after 10 seconds
    And postgres1 role is the primary after 10 seconds
