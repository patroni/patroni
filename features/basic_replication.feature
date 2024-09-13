Feature: basic replication
  We should check that the basic bootstrapping, replication and failover works.

  Scenario: check replication of a single table
    Given I start Postgres-0
    Then Postgres-0 is a leader after 10 seconds
    And there is a non empty initialize key in DCS after 15 seconds
    When I issue a PATCH request to http://127.0.0.1:8008/config with {"ttl": 20, "synchronous_mode": true}
    Then I receive a response code 200
    When I start Postgres-1
    And I configure and start Postgres-2 with a tag replicatefrom Postgres-0
    And "sync" key in DCS has leader=Postgres-0 after 20 seconds
    And I add the table foo to Postgres-0
    Then table foo is present on Postgres-1 after 20 seconds
    Then table foo is present on Postgres-2 after 20 seconds

  Scenario: check restart of sync replica
    Given I shut down Postgres-2
    Then "sync" key in DCS has sync_standby=Postgres-1 after 5 seconds
    When I start Postgres-2
    And I shut down Postgres-1
    Then "sync" key in DCS has sync_standby=Postgres-2 after 10 seconds
    When I start Postgres-1
    Then "members/Postgres-1" key in DCS has state=running after 10 seconds
    And Status code on GET http://127.0.0.1:8010/sync is 200 after 3 seconds
    And Status code on GET http://127.0.0.1:8009/async is 200 after 3 seconds

  Scenario: check stuck sync replica
    Given I issue a PATCH request to http://127.0.0.1:8008/config with {"pause": true, "maximum_lag_on_syncnode": 15000000, "postgresql": {"parameters": {"synchronous_commit": "remote_apply"}}}
    Then I receive a response code 200
    And I create table on Postgres-0
    And table mytest is present on Postgres-1 after 2 seconds
    And table mytest is present on Postgres-2 after 2 seconds
    When I pause wal replay on Postgres-2
    And I load data on Postgres-0
    Then "sync" key in DCS has sync_standby=Postgres-1 after 15 seconds
    And I resume wal replay on Postgres-2
    And Status code on GET http://127.0.0.1:8009/sync is 200 after 3 seconds
    And Status code on GET http://127.0.0.1:8010/async is 200 after 3 seconds
    When I issue a PATCH request to http://127.0.0.1:8008/config with {"pause": null, "maximum_lag_on_syncnode": -1, "postgresql": {"parameters": {"synchronous_commit": "on"}}}
    Then I receive a response code 200
    And I drop table on Postgres-0

  Scenario: check multi sync replication
    Given I issue a PATCH request to http://127.0.0.1:8008/config with {"synchronous_node_count": 2}
    Then I receive a response code 200
    Then "sync" key in DCS has sync_standby=Postgres-1,Postgres-2 after 10 seconds
    And Status code on GET http://127.0.0.1:8010/sync is 200 after 3 seconds
    And Status code on GET http://127.0.0.1:8009/sync is 200 after 3 seconds
    When I issue a PATCH request to http://127.0.0.1:8008/config with {"synchronous_node_count": 1}
    Then I receive a response code 200
    And I shut down Postgres-1
    Then "sync" key in DCS has sync_standby=Postgres-2 after 10 seconds
    When I start Postgres-1
    Then "members/Postgres-1" key in DCS has state=running after 10 seconds
    And Status code on GET http://127.0.0.1:8010/sync is 200 after 3 seconds
    And Status code on GET http://127.0.0.1:8009/async is 200 after 3 seconds

  Scenario: check the basic failover in synchronous mode
    Given I run patronictl.py pause batman
    Then I receive a response returncode 0
    When I sleep for 2 seconds
    And I shut down Postgres-0
    And I run patronictl.py resume batman
    Then I receive a response returncode 0
    And Postgres-2 role is the primary after 24 seconds
    And Response on GET http://127.0.0.1:8010/history contains recovery after 10 seconds
    And there is a Postgres-2_cb.log with "on_role_change primary batman" in Postgres-2 data directory
    When I issue a PATCH request to http://127.0.0.1:8010/config with {"synchronous_mode": null, "master_start_timeout": 0}
    Then I receive a response code 200
    When I add the table bar to Postgres-2
    Then table bar is present on Postgres-1 after 20 seconds
    And Response on GET http://127.0.0.1:8010/config contains master_start_timeout after 10 seconds

  Scenario: check rejoin of the former primary with pg_rewind
    Given I add the table splitbrain to Postgres-0
    And I start Postgres-0
    Then Postgres-0 role is the secondary after 20 seconds
    When I add the table buz to Postgres-2
    Then table buz is present on Postgres-0 after 20 seconds

  @reject-duplicate-name
  Scenario: check graceful rejection when two nodes have the same name
    Given I start duplicate Postgres-0 on port 8011
    Then there is one of ["Can't start; there is already a node named 'Postgres-0' running"] CRITICAL in the dup-Postgres-0 patroni log after 5 seconds
