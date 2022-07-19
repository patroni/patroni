Feature: dcs failsafe mode
  We should check the basic dcs failsafe mode functioning

  Scenario: check failsafe mode can be successfully enabled
    Given I start postgres0
    And postgres0 is a leader after 10 seconds
    And I sleep for 3 seconds
    When I issue a PATCH request to http://127.0.0.1:8008/config with {"loop_wait": 2, "ttl": 20, "retry_timeout": 5, "failsafe_mode": true}
    Then I receive a response code 200
    And Response on GET http://127.0.0.1:8008/failsafe contains postgres0 after 10 seconds
    When I issue a GET request to http://127.0.0.1:8008/failsafe
    Then I receive a response code 200
    And I receive a response postgres0 http://127.0.0.1:8008/patroni
    When I issue a PATCH request to http://127.0.0.1:8008/config with {"postgresql": {"parameters": {"wal_level": "logical"}}}
    Then I receive a response code 200
    When I issue a PATCH request to http://127.0.0.1:8008/config with {"slots": {"dcs_slot_0": {"type": "logical", "database": "postgres", "plugin": "test_decoding"}}}
    Then I receive a response code 200

  @dcs-failsafe
  Scenario: check one-node cluster is functioning while DCS is down
    Given DCS is down
    When I sleep for 12 seconds
    Then postgres0 role is the primary after 10 seconds

  @dcs-failsafe
  Scenario: check new replica isn't promoted when leader is down and DCS is up
    When I do a backup of postgres0
    And I shut down postgres0
    And DCS is up
    When I start postgres1 in a cluster batman from backup with no_master
    And I sleep for 2 seconds
    Then postgres1 role is the replica after 12 seconds

  Scenario: check leader and replica are both in /failsafe key after leader is back
    Given I start postgres0
    And I start postgres1
    Then "members/postgres0" key in DCS has state=running after 10 seconds
    And "members/postgres1" key in DCS has state=running after 2 seconds
    And Response on GET http://127.0.0.1:8009/failsafe contains postgres1 after 10 seconds
    When I issue a GET request to http://127.0.0.1:8009/failsafe
    Then I receive a response code 200
    And I receive a response postgres0 http://127.0.0.1:8008/patroni
    And I receive a response postgres1 http://127.0.0.1:8009/patroni

  @dcs-failsafe
  @slot-advance
  Scenario: check leader and replica are functioning while DCS is down
    Given logical slot dcs_slot_0 is in sync between postgres0 and postgres1 after 10 seconds
    And DCS is down
    And I sleep for 12 seconds
    Then postgres0 role is the primary after 10 seconds
    And postgres1 role is the replica after 2 seconds
    And replication works from postgres0 to postgres1 after 10 seconds
    And I get all changes from logical slot dcs_slot_0 on postgres0
    And logical slot dcs_slot_0 is in sync between postgres0 and postgres1 after 10 seconds

  @dcs-failsafe
  Scenario: check master is demoted when one replica is shut down and DCS is down
    Given DCS is down
    And I shut down postgres1
    And I sleep for 2 seconds
    Then postgres0 role is the replica after 12 seconds

  @dcs-failsafe
  Scenario: check known replica is promoted when leader is down and DCS is up
    Given DCS is up
    Then postgres0 role is the primary after 22 seconds
    When I start postgres1
    Then "members/postgres1" key in DCS has state=running after 10 seconds
    And Response on GET http://127.0.0.1:8009/failsafe contains postgres1 after 10 seconds
    Given DCS is down
    And I shut down postgres0
    And DCS is up
    Then postgres1 role is the primary after 22 seconds

  @dcs-failsafe
  Scenario: check three-node cluster is functioning while DCS is down
    Given I start postgres0
    And I start postgres2
    Then "members/postgres0" key in DCS has state=running after 10 seconds
    And "members/postgres2" key in DCS has state=running after 10 seconds
    And Response on GET http://127.0.0.1:8008/failsafe contains postgres2 after 10 seconds
    Given DCS is down
    And I sleep for 12 seconds
    Then postgres1 role is the primary after 10 seconds
    And postgres0 role is the replica after 2 seconds
    And postgres2 role is the replica after 2 seconds
