Feature: standby cluster
  Scenario: prepare the cluster with logical slots
    Given I start postgres-1
    Then postgres-1 is a leader after 10 seconds
    And there is a non empty initialize key in DCS after 15 seconds
    When I issue a PATCH request to http://127.0.0.1:8009/config with {"slots": {"pm_1": {"type": "physical"}}, "postgresql": {"parameters": {"wal_level": "logical"}}}
    Then I receive a response code 200
    And Response on GET http://127.0.0.1:8009/config contains slots after 10 seconds
    And I sleep for 3 seconds
    When I issue a PATCH request to http://127.0.0.1:8009/config with {"slots": {"test_logical": {"type": "logical", "database": "postgres", "plugin": "test_decoding"}}}
    Then I receive a response code 200
    And I do a backup of postgres-1
    When I start postgres-0
    Then "members/postgres-0" key in DCS has state=running after 10 seconds
    And replication works from postgres-1 to postgres-0 after 15 seconds
    And Response on GET http://127.0.0.1:8008/patroni contains replication_state=streaming after 10 seconds
    And "members/postgres-0" key in DCS has replication_state=streaming after 10 seconds

  @pg110000
  Scenario: check permanent logical slots are synced to the replica
    Given I run patronictl.py restart batman postgres-1 --force
    Then Logical slot test_logical is in sync between postgres-0 and postgres-1 after 10 seconds

  Scenario: Detach exiting node from the cluster
    When I shut down postgres-1
    Then postgres-0 is a leader after 10 seconds
    And "members/postgres-0" key in DCS has role=primary after 5 seconds
    When I issue a GET request to http://127.0.0.1:8008/
    Then I receive a response code 200

  Scenario: check replication of a single table in a standby cluster
    Given I start postgres-1 in a standby cluster batman1 as a clone of postgres-0
    Then postgres-1 is a leader of batman1 after 10 seconds
    When I add the table foo to postgres-0
    Then table foo is present on postgres-1 after 20 seconds
    And Response on GET http://127.0.0.1:8009/patroni contains replication_state=streaming after 10 seconds
    And I sleep for 3 seconds
    When I issue a GET request to http://127.0.0.1:8009/primary
    Then I receive a response code 503
    When I issue a GET request to http://127.0.0.1:8009/standby_leader
    Then I receive a response code 200
    And I receive a response role standby_leader
    And there is a postgres-1_cb.log with "on_role_change standby_leader batman1" in postgres-1 data directory
    When I start postgres-2 in a cluster batman1
    Then postgres-2 role is the replica after 24 seconds
    And postgres-2 is replicating from postgres-1 after 10 seconds
    And table foo is present on postgres-2 after 20 seconds
    And Response on GET http://127.0.0.1:8010/patroni contains replication_state=streaming after 10 seconds
    And postgres-1 does not have a replication slot named test_logical

  Scenario: check switchover
    Given I run patronictl.py switchover batman1 --force
    Then Status code on GET http://127.0.0.1:8010/standby_leader is 200 after 10 seconds
    And postgres-1 is replicating from postgres-2 after 32 seconds
    And there is a postgres-2_cb.log with "on_start replica batman1\non_role_change standby_leader batman1" in postgres-2 data directory

  Scenario: check failover
    When I kill postgres-2
    And I kill postmaster on postgres-2
    Then postgres-1 is replicating from postgres-0 after 32 seconds
    And Status code on GET http://127.0.0.1:8009/standby_leader is 200 after 10 seconds
    When I issue a GET request to http://127.0.0.1:8009/primary
    Then I receive a response code 503
    And I receive a response role standby_leader
    And replication works from postgres-0 to postgres-1 after 15 seconds
    And there is a postgres-1_cb.log with "on_role_change replica batman1\non_role_change standby_leader batman1" in postgres-1 data directory

  Scenario: demote cluster
    When I switch standby cluster batman1 to archive recovery
    Then Response on GET http://127.0.0.1:8009/patroni contains replication_state=in archive recovery after 30 seconds
    When I demote cluster batman
    And "members/postgres-0" key in DCS has role=standby_leader after 20 seconds
    And "members/postgres-0" key in DCS has state=running after 10 seconds

  Scenario: promote cluster
    When I issue a PATCH request to http://127.0.0.1:8009/config with {"standby_cluster": null}
    Then I receive a response code 200
    And postgres-1 role is the primary after 10 seconds
    When I add the table foo2 to postgres-1
    Then table foo2 is present on postgres-0 after 20 seconds
