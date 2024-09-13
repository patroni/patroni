Feature: recovery
  We want to check that crashed postgres is started back

  Scenario: check that timeline is not incremented when primary is started after crash
    Given I start Postgres-0
    Then Postgres-0 is a leader after 10 seconds
    And there is a non empty initialize key in DCS after 15 seconds
    When I start Postgres-1
    And I add the table foo to Postgres-0
    Then table foo is present on Postgres-1 after 20 seconds
    When I kill postmaster on Postgres-0
    Then Postgres-0 role is the primary after 10 seconds
    When I issue a GET request to http://127.0.0.1:8008/
    Then I receive a response code 200
    And I receive a response role primary
    And I receive a response timeline 1
    And "members/Postgres-0" key in DCS has state=running after 12 seconds
    And replication works from Postgres-0 to Postgres-1 after 15 seconds

  Scenario: check immediate failover when master_start_timeout=0
    Given I issue a PATCH request to http://127.0.0.1:8008/config with {"master_start_timeout": 0}
    Then I receive a response code 200
    And Response on GET http://127.0.0.1:8008/config contains master_start_timeout after 10 seconds
    When I kill postmaster on Postgres-0
    Then Postgres-1 is a leader after 10 seconds
    And Postgres-1 role is the primary after 10 seconds
