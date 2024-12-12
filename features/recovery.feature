Feature: recovery
  We want to check that crashed postgres is started back

  Scenario: check that timeline is not incremented when primary is started after crash
    Given I start postgres-0
    Then postgres-0 is a leader after 10 seconds
    And there is a non empty initialize key in DCS after 15 seconds
    When I start postgres-1
    And I add the table foo to postgres-0
    Then table foo is present on postgres-1 after 20 seconds
    When I kill postmaster on postgres-0
    Then postgres-0 role is the primary after 10 seconds
    When I issue a GET request to http://127.0.0.1:8008/
    Then I receive a response code 200
    And I receive a response role primary
    And I receive a response timeline 1
    And "members/postgres-0" key in DCS has state=running after 12 seconds
    And replication works from postgres-0 to postgres-1 after 15 seconds

  Scenario: check immediate failover when master_start_timeout=0
    Given I issue a PATCH request to http://127.0.0.1:8008/config with {"master_start_timeout": 0}
    Then I receive a response code 200
    And Response on GET http://127.0.0.1:8008/config contains master_start_timeout after 10 seconds
    When I kill postmaster on postgres-0
    Then postgres-1 is a leader after 10 seconds
    And postgres-1 role is the primary after 10 seconds

  Scenario: check crashed primary demotes after failed attempt to start
    Given I issue a PATCH request to http://127.0.0.1:8009/config with {"master_start_timeout": null}
    Then I receive a response code 200
    And postgres-0 role is the replica after 10 seconds
    When I ensure postgres-1 fails to start after a failure
    When I kill postmaster on postgres-1
    Then postgres-0 is a leader after 10 seconds
    And there is a postgres-1_cb.log with "on_role_change demoted batman" in postgres-1 data directory
