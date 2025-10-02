Feature: permanent slots
  Scenario: check that physical permanent slots are created
    Given I start postgres-0
    Then postgres-0 is a leader after 10 seconds
    And there is a non empty initialize key in DCS after 15 seconds
    When I issue a PATCH request to http://127.0.0.1:8008/config with {"slots":{"test_physical":0,"postgres_3":0},"postgresql":{"parameters":{"wal_level":"logical"}}}
    Then I receive a response code 200
    And Response on GET http://127.0.0.1:8008/config contains slots after 10 seconds
    When I start postgres-1
    And I configure and start postgres-2 with a tag nofailover true
    And I configure and start postgres-3 with a tag replicatefrom postgres-2
    Then postgres-0 has a physical replication slot named test_physical after 10 seconds
    And postgres-0 has a physical replication slot named postgres_1 after 10 seconds
    And postgres-0 has a physical replication slot named postgres_2 after 10 seconds
    And postgres-2 has a physical replication slot named postgres_3 after 10 seconds
    And postgres-2 does not have a replication slot named test_physical

  @pg110000
  Scenario: check that logical permanent slots are created
    Given I run patronictl.py restart batman postgres-0 --force
    And I issue a PATCH request to http://127.0.0.1:8008/config with {"slots":{"test_logical":{"type":"logical","database":"postgres","plugin":"test_decoding"}}}
    Then postgres-0 has a logical replication slot named test_logical with the test_decoding plugin after 10 seconds

  @pg110000
  Scenario: check that permanent slots are created on replicas
    Given postgres-1 has a logical replication slot named test_logical with the test_decoding plugin after 10 seconds
    Then Logical slot test_logical is in sync between postgres-0 and postgres-1 after 10 seconds
    And Logical slot test_logical is in sync between postgres-0 and postgres-3 after 10 seconds
    And postgres-1 has a physical replication slot named test_physical after 2 seconds
    And postgres-2 does not have a replication slot named test_logical
    And postgres-3 has a physical replication slot named test_physical after 2 seconds

  @pg110000
  Scenario: check permanent physical slots that match with member names
    Given postgres-0 has a physical replication slot named postgres_3 after 2 seconds
    And postgres-1 has a physical replication slot named postgres_0 after 2 seconds
    And postgres-1 has a physical replication slot named postgres_2 after 2 seconds
    And postgres-1 has a physical replication slot named postgres_3 after 2 seconds
    And postgres-2 does not have a replication slot named postgres_0
    And postgres-2 does not have a replication slot named postgres_1
    And postgres-2 has a physical replication slot named postgres_3 after 2 seconds
    And postgres-3 has a physical replication slot named postgres_0 after 2 seconds
    And postgres-3 has a physical replication slot named postgres_1 after 2 seconds
    And postgres-3 has a physical replication slot named postgres_2 after 2 seconds

  @pg110000
  Scenario: check that permanent slots are advanced on replicas
    Given I add the table replicate_me to postgres-0
    When I get all changes from logical slot test_logical on postgres-0
    And I get all changes from physical slot test_physical on postgres-0
    Then Logical slot test_logical is in sync between postgres-0 and postgres-1 after 10 seconds
    And Physical slot test_physical is in sync between postgres-0 and postgres-1 after 10 seconds
    And Logical slot test_logical is in sync between postgres-0 and postgres-3 after 10 seconds
    And Physical slot test_physical is in sync between postgres-0 and postgres-3 after 10 seconds
    And Physical slot postgres_1 is in sync between postgres-0 and postgres-3 after 10 seconds
    And Physical slot postgres_3 is in sync between postgres-2 and postgres-0 after 20 seconds
    And Physical slot postgres_3 is in sync between postgres-2 and postgres-1 after 10 seconds

  @pg110000
  Scenario: check that permanent slots and member slots are written to the /status key
    Given "status" key in DCS has test_physical in slots
    And "status" key in DCS has postgres_0 in slots
    And "status" key in DCS has postgres_1 in slots
    And "status" key in DCS has postgres_2 in slots
    And "status" key in DCS has postgres_3 in slots

  @pg110000
  Scenario: check that only non-permanent member slots are written to the retain_slots in /status key
    Given "status" key in DCS has postgres_0 in retain_slots
    And "status" key in DCS has postgres_1 in retain_slots
    And "status" key in DCS has postgres_2 in retain_slots
    And "status" key in DCS does not have postgres_3 in retain_slots

  Scenario: check permanent physical replication slot after failover
    Given I shut down postgres-3
    And I shut down postgres-2
    And I shut down postgres-0
    Then postgres-1 has a physical replication slot named test_physical after 10 seconds
    And postgres-1 has a physical replication slot named postgres_0 after 10 seconds
    And postgres-1 has a physical replication slot named postgres_3 after 10 seconds

  @pg110000
  Scenario: check permanent physical replication slot on replica after failover
    Given I start postgres-0
    Then postgres-0 role is the replica after 20 seconds
    And physical replication slot named postgres_1 on postgres-0 has no xmin value after 10 seconds
    # postgres_2 and postgres_3 slots are retained, but postgres_2 will still have xmin value :(
    And postgres-0 has a physical replication slot named postgres_2 after 10 seconds
    And postgres-0 has a physical replication slot named postgres_3 after 10 seconds
