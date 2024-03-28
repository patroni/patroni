Feature: standby cluster
  Scenario: prepare the cluster with logical slots
    Given I start postgres1
    Then postgres1 is a leader after 10 seconds
    And there is a non empty initialize key in DCS after 15 seconds
    When I issue a PATCH request to http://127.0.0.1:8009/config with {"slots": {"pm_1": {"type": "physical"}}, "postgresql": {"parameters": {"wal_level": "logical"}}}
    Then I receive a response code 200
    And Response on GET http://127.0.0.1:8009/config contains slots after 10 seconds
    And I sleep for 3 seconds
    When I issue a PATCH request to http://127.0.0.1:8009/config with {"slots": {"test_logical": {"type": "logical", "database": "postgres", "plugin": "test_decoding"}}}
    Then I receive a response code 200
    And I do a backup of postgres1
    When I start postgres0
    Then "members/postgres0" key in DCS has state=running after 10 seconds
    And replication works from postgres1 to postgres0 after 15 seconds
    When I issue a GET request to http://127.0.0.1:8008/patroni
    Then I receive a response code 200
    And I receive a response replication_state streaming
    And "members/postgres0" key in DCS has replication_state=streaming after 10 seconds

  @slot-advance
  Scenario: check permanent logical slots are synced to the replica
    Given I run patronictl.py restart batman postgres1 --force
    Then Logical slot test_logical is in sync between postgres0 and postgres1 after 10 seconds

  Scenario: Detach exiting node from the cluster
    When I shut down postgres1
    Then postgres0 is a leader after 10 seconds
    And "members/postgres0" key in DCS has role=master after 5 seconds
    When I issue a GET request to http://127.0.0.1:8008/
    Then I receive a response code 200

  Scenario: check replication of a single table in a standby cluster
    Given I start postgres1 in a standby cluster batman1 as a clone of postgres0
    Then postgres1 is a leader of batman1 after 10 seconds
    When I add the table foo to postgres0
    Then table foo is present on postgres1 after 20 seconds
    When I issue a GET request to http://127.0.0.1:8009/patroni
    Then I receive a response code 200
    And I receive a response replication_state streaming
    And I sleep for 3 seconds
    When I issue a GET request to http://127.0.0.1:8009/primary
    Then I receive a response code 503
    When I issue a GET request to http://127.0.0.1:8009/standby_leader
    Then I receive a response code 200
    And I receive a response role standby_leader
    And there is a postgres1_cb.log with "on_role_change standby_leader batman1" in postgres1 data directory
    When I start postgres2 in a cluster batman1
    Then postgres2 role is the replica after 24 seconds
    And postgres2 is replicating from postgres1 after 10 seconds
    And table foo is present on postgres2 after 20 seconds
    When I issue a GET request to http://127.0.0.1:8010/patroni
    Then I receive a response code 200
    And I receive a response replication_state streaming
    And postgres1 does not have a replication slot named test_logical

  Scenario: check switchover
    Given I run patronictl.py switchover batman1 --force
    Then Status code on GET http://127.0.0.1:8010/standby_leader is 200 after 10 seconds
    And postgres1 is replicating from postgres2 after 32 seconds
    And there is a postgres2_cb.log with "on_start replica batman1\non_role_change standby_leader batman1" in postgres2 data directory

  Scenario: check failover
    When I kill postgres2
    And I kill postmaster on postgres2
    Then postgres1 is replicating from postgres0 after 32 seconds
    And Status code on GET http://127.0.0.1:8009/standby_leader is 200 after 10 seconds
    When I issue a GET request to http://127.0.0.1:8009/primary
    Then I receive a response code 503
    And I receive a response role standby_leader
    And replication works from postgres0 to postgres1 after 15 seconds
    And there is a postgres1_cb.log with "on_role_change replica batman1\non_role_change standby_leader batman1" in postgres1 data directory
