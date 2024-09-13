Feature: dcs failsafe mode
  We should check the basic dcs failsafe mode functioning

  Scenario: check failsafe mode can be successfully enabled
    Given I start Postgres-0
    And Postgres-0 is a leader after 10 seconds
    Then "config" key in DCS has ttl=30 after 10 seconds
    When I issue a PATCH request to http://127.0.0.1:8008/config with {"loop_wait": 2, "ttl": 20, "retry_timeout": 3, "failsafe_mode": true}
    Then I receive a response code 200
    And Response on GET http://127.0.0.1:8008/failsafe contains Postgres-0 after 10 seconds
    When I issue a GET request to http://127.0.0.1:8008/failsafe
    Then I receive a response code 200
    And I receive a response Postgres-0 http://127.0.0.1:8008/patroni
    When I issue a PATCH request to http://127.0.0.1:8008/config with {"postgresql": {"parameters": {"wal_level": "logical"}},"slots":{"dcs_slot_1": null,"postgres_0":null}}
    Then I receive a response code 200
    When I issue a PATCH request to http://127.0.0.1:8008/config with {"slots": {"dcs_slot_0": {"type": "logical", "database": "postgres", "plugin": "test_decoding"}}}
    Then I receive a response code 200

  @dcs-failsafe
  Scenario: check one-node cluster is functioning while DCS is down
    Given DCS is down
    Then Response on GET http://127.0.0.1:8008/primary contains failsafe_mode_is_active after 12 seconds
    And Postgres-0 role is the primary after 10 seconds

  @dcs-failsafe
  Scenario: check new replica isn't promoted when leader is down and DCS is up
    Given DCS is up
    When I do a backup of Postgres-0
    And I shut down Postgres-0
    When I start Postgres-1 in a cluster batman from backup with no_leader
    Then Postgres-1 role is the replica after 12 seconds

  Scenario: check leader and replica are both in /failsafe key after leader is back
    Given I start Postgres-0
    And I start Postgres-1
    Then "members/Postgres-0" key in DCS has state=running after 10 seconds
    And "members/Postgres-1" key in DCS has state=running after 2 seconds
    And Response on GET http://127.0.0.1:8009/failsafe contains Postgres-1 after 10 seconds
    When I issue a GET request to http://127.0.0.1:8009/failsafe
    Then I receive a response code 200
    And I receive a response Postgres-0 http://127.0.0.1:8008/patroni
    And I receive a response Postgres-1 http://127.0.0.1:8009/patroni

  @dcs-failsafe
  @slot-advance
  Scenario: check leader and replica are functioning while DCS is down
    Given I get all changes from physical slot dcs_slot_1 on Postgres-0
    Then physical slot dcs_slot_1 is in sync between Postgres-0 and Postgres-1 after 10 seconds
    And logical slot dcs_slot_0 is in sync between Postgres-0 and Postgres-1 after 10 seconds
    And DCS is down
    Then Response on GET http://127.0.0.1:8008/primary contains failsafe_mode_is_active after 12 seconds
    Then Postgres-0 role is the primary after 10 seconds
    And Postgres-1 role is the replica after 2 seconds
    And replication works from Postgres-0 to Postgres-1 after 10 seconds
    When I get all changes from logical slot dcs_slot_0 on Postgres-0
    And I get all changes from physical slot dcs_slot_1 on Postgres-0
    Then logical slot dcs_slot_0 is in sync between Postgres-0 and Postgres-1 after 20 seconds
    And physical slot dcs_slot_1 is in sync between Postgres-0 and Postgres-1 after 10 seconds

  @dcs-failsafe
  Scenario: check primary is demoted when one replica is shut down and DCS is down
    Given DCS is down
    And I kill Postgres-1
    And I kill postmaster on Postgres-1
    Then Postgres-0 role is the replica after 12 seconds

  @dcs-failsafe
  Scenario: check known replica is promoted when leader is down and DCS is up
    Given I kill Postgres-0
    And I shut down postmaster on Postgres-0
    And DCS is up
    When I start Postgres-1
    Then "members/Postgres-1" key in DCS has state=running after 10 seconds
    And Postgres-1 role is the primary after 25 seconds

  @dcs-failsafe
  Scenario: scale to three-node cluster
    Given I start Postgres-0
    And I configure and start Postgres-2 with a tag replicatefrom Postgres-0
    Then "members/Postgres-2" key in DCS has state=running after 10 seconds
    And "members/Postgres-0" key in DCS has state=running after 20 seconds
    And Response on GET http://127.0.0.1:8008/failsafe contains Postgres-2 after 10 seconds
    And replication works from Postgres-1 to Postgres-0 after 10 seconds
    And replication works from Postgres-1 to Postgres-2 after 10 seconds

  @dcs-failsafe
  @slot-advance
  Scenario: make sure permanent slots exist on replicas
    Given I issue a PATCH request to http://127.0.0.1:8009/config with {"slots":{"dcs_slot_0":null,"dcs_slot_2":{"type":"logical","database":"postgres","plugin":"test_decoding"}}}
    Then logical slot dcs_slot_2 is in sync between Postgres-1 and Postgres-0 after 20 seconds
    And logical slot dcs_slot_2 is in sync between Postgres-1 and Postgres-2 after 20 seconds
    When I get all changes from physical slot dcs_slot_1 on Postgres-1
    Then physical slot dcs_slot_1 is in sync between Postgres-1 and Postgres-0 after 10 seconds
    And physical slot dcs_slot_1 is in sync between Postgres-1 and Postgres-2 after 10 seconds
    And physical slot postgres_0 is in sync between Postgres-1 and Postgres-2 after 10 seconds
    And physical slot postgres_2 is in sync between Postgres-0 and Postgres-1 after 10 seconds

  @dcs-failsafe
  Scenario: check three-node cluster is functioning while DCS is down
    Given DCS is down
    Then Response on GET http://127.0.0.1:8009/primary contains failsafe_mode_is_active after 12 seconds
    Then Postgres-1 role is the primary after 10 seconds
    And Postgres-0 role is the replica after 2 seconds
    And Postgres-2 role is the replica after 2 seconds

  @dcs-failsafe
  @slot-advance
  Scenario: check that permanent slots are in sync between nodes while DCS is down
    Given replication works from Postgres-1 to Postgres-0 after 10 seconds
    And replication works from Postgres-1 to Postgres-2 after 10 seconds
    When I get all changes from logical slot dcs_slot_2 on Postgres-1
    And I get all changes from physical slot dcs_slot_1 on Postgres-1
    Then logical slot dcs_slot_2 is in sync between Postgres-1 and Postgres-0 after 20 seconds
    And logical slot dcs_slot_2 is in sync between Postgres-1 and Postgres-2 after 20 seconds
    And physical slot dcs_slot_1 is in sync between Postgres-1 and Postgres-0 after 10 seconds
    And physical slot dcs_slot_1 is in sync between Postgres-1 and Postgres-2 after 10 seconds
    And physical slot postgres_0 is in sync between Postgres-1 and Postgres-2 after 10 seconds
    And physical slot postgres_2 is in sync between Postgres-0 and Postgres-1 after 10 seconds
