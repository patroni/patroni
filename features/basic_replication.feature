Feature: basic replication
  We should check that the basic bootstrapping, replication and failover works.

  Scenario: check replication of a single table
    Given I start postgres0
    Then postgres0 is a leader after 10 seconds
    And there is a non empty initialize key in DCS after 15 seconds
    When I issue a PATCH request to http://127.0.0.1:8008/config with {"ttl": 20, "loop_wait": 2, "synchronous_mode": true}
    Then I receive a response code 200
    When I start postgres1
    And I configure and start postgres2 with a tag replicatefrom postgres0
    And "sync" key in DCS has leader=postgres0 after 20 seconds
    And I add the table foo to postgres0
    Then table foo is present on postgres1 after 20 seconds
    Then table foo is present on postgres2 after 20 seconds

  Scenario: check restart of sync replica
    Given I shut down postgres2
    Then "sync" key in DCS has sync_standby=postgres1 after 5 seconds
    When I start postgres2
    And I shut down postgres1
    Then "sync" key in DCS has sync_standby=postgres2 after 10 seconds
    When I start postgres1
    And "members/postgres1" key in DCS has state=running after 10 seconds
    And I sleep for 2 seconds
    When I issue a GET request to http://127.0.0.1:8010/sync
    Then I receive a response code 200
    When I issue a GET request to http://127.0.0.1:8009/async
    Then I receive a response code 200

  Scenario: check the basic failover in synchronous mode
    Given I run patronictl.py pause batman
    Then I receive a response returncode 0
    When I sleep for 2 seconds
    And I shut down postgres0
    And I run patronictl.py resume batman
    Then I receive a response returncode 0
    And postgres2 role is the primary after 24 seconds
    And Response on GET http://127.0.0.1:8010/history contains recovery after 10 seconds
    When I issue a PATCH request to http://127.0.0.1:8010/config with {"synchronous_mode": null, "master_start_timeout": 0}
    Then I receive a response code 200
    When I add the table bar to postgres2
    Then table bar is present on postgres1 after 20 seconds
    And Response on GET http://127.0.0.1:8010/config contains master_start_timeout after 10 seconds

  Scenario: check immediate failover when master_start_timeout=0
    Given I kill postmaster on postgres2
    Then postgres1 is a leader after 10 seconds
    And postgres1 role is the primary after 10 seconds

  Scenario: check rejoin of the former master with pg_rewind
    Given I add the table splitbrain to postgres0
    And I start postgres0
    Then postgres0 role is the secondary after 20 seconds
    When I add the table buz to postgres1
    Then table buz is present on postgres0 after 20 seconds
