Feature: dcs failsafe mode
  We should check the basic dcs failsafe mode functioning

  Scenario: check failsafe mode can be successfully enabled
    Given I start postgres-0
    And postgres-0 is a leader after 10 seconds
    Then "config" key in DCS has ttl=30 after 10 seconds
    When I issue a PATCH request to http://127.0.0.1:8008/config with {"loop_wait": 2, "ttl": 20, "retry_timeout": 3, "failsafe_mode": true}
    Then I receive a response code 200
    And Response on GET http://127.0.0.1:8008/failsafe contains postgres-0 after 10 seconds
    When I issue a GET request to http://127.0.0.1:8008/failsafe
    Then I receive a response code 200
    And I receive a response postgres-0 http://127.0.0.1:8008/patroni
    When I issue a PATCH request to http://127.0.0.1:8008/config with {"postgresql": {"parameters": {"wal_level": "logical"}},"slots":{"dcs_slot_1": null,"postgres_0":null}}
    Then I receive a response code 200
    When I issue a PATCH request to http://127.0.0.1:8008/config with {"slots": {"dcs_slot_0": {"type": "logical", "database": "postgres", "plugin": "test_decoding"}}}
    Then I receive a response code 200

  @dcs-failsafe
  Scenario: check one-node cluster is functioning while DCS is down
    Given DCS is down
    Then Response on GET http://127.0.0.1:8008/primary contains failsafe_mode_is_active after 12 seconds
    And postgres-0 role is the primary after 10 seconds

  @dcs-failsafe
  Scenario: check new replica isn't promoted when leader is down and DCS is up
    Given DCS is up
    When I do a backup of postgres-0
    And I shut down postgres-0
    When I start postgres-1 in a cluster batman from backup with no_leader
    Then postgres-1 role is the replica after 12 seconds

  Scenario: check leader and replica are both in /failsafe key after leader is back
    Given I start postgres-0
    And I start postgres-1
    Then "members/postgres-0" key in DCS has state=running after 10 seconds
    And "members/postgres-1" key in DCS has state=running after 2 seconds
    And Response on GET http://127.0.0.1:8009/failsafe contains postgres-1 after 10 seconds
    When I issue a GET request to http://127.0.0.1:8009/failsafe
    Then I receive a response code 200
    And I receive a response postgres-0 http://127.0.0.1:8008/patroni
    And I receive a response postgres-1 http://127.0.0.1:8009/patroni

  @dcs-failsafe
  @pg110000
  Scenario: check leader and replica are functioning while DCS is down
    Given I get all changes from physical slot dcs_slot_1 on postgres-0
    Then physical slot dcs_slot_1 is in sync between postgres-0 and postgres-1 after 10 seconds
    And logical slot dcs_slot_0 is in sync between postgres-0 and postgres-1 after 10 seconds
    And DCS is down
    Then Response on GET http://127.0.0.1:8008/primary contains failsafe_mode_is_active after 12 seconds
    Then postgres-0 role is the primary after 10 seconds
    And postgres-1 role is the replica after 2 seconds
    And replication works from postgres-0 to postgres-1 after 10 seconds
    When I get all changes from logical slot dcs_slot_0 on postgres-0
    And I get all changes from physical slot dcs_slot_1 on postgres-0
    Then logical slot dcs_slot_0 is in sync between postgres-0 and postgres-1 after 20 seconds
    And physical slot dcs_slot_1 is in sync between postgres-0 and postgres-1 after 10 seconds

  @dcs-failsafe
  Scenario: check primary is demoted when one replica is shut down and DCS is down
    Given DCS is down
    And I kill postgres-1
    And I kill postmaster on postgres-1
    Then postgres-0 role is the replica after 12 seconds

  @dcs-failsafe
  Scenario: check known replica is promoted when leader is down and DCS is up
    Given I kill postgres-0
    And I shut down postmaster on postgres-0
    And DCS is up
    When I start postgres-1
    Then "members/postgres-1" key in DCS has state=running after 10 seconds
    And postgres-1 role is the primary after 25 seconds

  @dcs-failsafe
  Scenario: scale to three-node cluster
    Given I start postgres-0
    And I configure and start postgres-2 with a tag replicatefrom postgres-0
    Then "members/postgres-2" key in DCS has state=running after 10 seconds
    And "members/postgres-0" key in DCS has state=running after 20 seconds
    And Response on GET http://127.0.0.1:8008/failsafe contains postgres-2 after 10 seconds
    And replication works from postgres-1 to postgres-0 after 10 seconds
    And replication works from postgres-1 to postgres-2 after 10 seconds

  @dcs-failsafe
  @pg110000
  Scenario: make sure permanent slots exist on replicas
    Given I issue a PATCH request to http://127.0.0.1:8009/config with {"slots":{"dcs_slot_0":null,"dcs_slot_2":{"type":"logical","database":"postgres","plugin":"test_decoding"}}}
    Then logical slot dcs_slot_2 is in sync between postgres-1 and postgres-0 after 20 seconds
    And logical slot dcs_slot_2 is in sync between postgres-1 and postgres-2 after 20 seconds
    When I get all changes from physical slot dcs_slot_1 on postgres-1
    Then physical slot dcs_slot_1 is in sync between postgres-1 and postgres-0 after 10 seconds
    And physical slot dcs_slot_1 is in sync between postgres-1 and postgres-2 after 10 seconds
    And physical slot postgres_0 is in sync between postgres-1 and postgres-2 after 10 seconds
    And physical slot postgres_2 is in sync between postgres-0 and postgres-1 after 10 seconds

  @dcs-failsafe
  Scenario: check three-node cluster is functioning while DCS is down
    Given DCS is down
    Then Response on GET http://127.0.0.1:8009/primary contains failsafe_mode_is_active after 12 seconds
    Then postgres-1 role is the primary after 10 seconds
    And postgres-0 role is the replica after 2 seconds
    And postgres-2 role is the replica after 2 seconds

  @dcs-failsafe
  @pg110000
  Scenario: check that permanent slots are in sync between nodes while DCS is down
    Given replication works from postgres-1 to postgres-0 after 10 seconds
    And replication works from postgres-1 to postgres-2 after 10 seconds
    When I get all changes from logical slot dcs_slot_2 on postgres-1
    And I get all changes from physical slot dcs_slot_1 on postgres-1
    Then logical slot dcs_slot_2 is in sync between postgres-1 and postgres-0 after 20 seconds
    And logical slot dcs_slot_2 is in sync between postgres-1 and postgres-2 after 20 seconds
    And physical slot dcs_slot_1 is in sync between postgres-1 and postgres-0 after 10 seconds
    And physical slot dcs_slot_1 is in sync between postgres-1 and postgres-2 after 10 seconds
    And physical slot postgres_0 is in sync between postgres-1 and postgres-2 after 10 seconds
    And physical slot postgres_2 is in sync between postgres-0 and postgres-1 after 10 seconds
