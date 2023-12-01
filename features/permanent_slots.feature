Feature: permanent slots
  Scenario: check that physical permanent slots are created
    Given I start postgres0
    Then postgres0 is a leader after 10 seconds
    And there is a non empty initialize key in DCS after 15 seconds
    When I issue a PATCH request to http://127.0.0.1:8008/config with {"slots":{"test_physical":0,"postgres0":0,"postgres1":0,"postgres3":0},"postgresql":{"parameters":{"wal_level":"logical"}}}
    Then I receive a response code 200
    And Response on GET http://127.0.0.1:8008/config contains slots after 10 seconds
    When I start postgres1
    And I start postgres2
    And I configure and start postgres3 with a tag replicatefrom postgres2
    Then postgres0 has a physical replication slot named test_physical after 10 seconds
    And postgres0 has a physical replication slot named postgres1 after 10 seconds
    And postgres0 has a physical replication slot named postgres2 after 10 seconds
    And postgres2 has a physical replication slot named postgres3 after 10 seconds

  @slot-advance
  Scenario: check that logical permanent slots are created
    Given I run patronictl.py restart batman postgres0 --force
    And I issue a PATCH request to http://127.0.0.1:8008/config with {"slots":{"test_logical":{"type":"logical","database":"postgres","plugin":"test_decoding"}}}
    Then postgres0 has a logical replication slot named test_logical with the test_decoding plugin after 10 seconds

  @slot-advance
  Scenario: check that permanent slots are created on replicas
    Given postgres1 has a logical replication slot named test_logical with the test_decoding plugin after 10 seconds
    Then Logical slot test_logical is in sync between postgres0 and postgres1 after 10 seconds
    And Logical slot test_logical is in sync between postgres0 and postgres2 after 10 seconds
    And Logical slot test_logical is in sync between postgres0 and postgres3 after 10 seconds
    And postgres1 has a physical replication slot named test_physical after 2 seconds
    And postgres2 has a physical replication slot named test_physical after 2 seconds
    And postgres3 has a physical replication slot named test_physical after 2 seconds

  @slot-advance
  Scenario: check permanent physical slots that match with member names
    Given postgres0 has a physical replication slot named postgres3 after 2 seconds
    And postgres1 has a physical replication slot named postgres0 after 2 seconds
    And postgres1 has a physical replication slot named postgres3 after 2 seconds
    And postgres2 has a physical replication slot named postgres0 after 2 seconds
    And postgres2 has a physical replication slot named postgres3 after 2 seconds
    And postgres2 has a physical replication slot named postgres1 after 2 seconds
    And postgres1 does not have a replication slot named postgres2
    And postgres3 does not have a replication slot named postgres2

  @slot-advance
  Scenario: check that permanent slots are advanced on replicas
    Given I add the table replicate_me to postgres0
    When I get all changes from logical slot test_logical on postgres0
    And I get all changes from physical slot test_physical on postgres0
    Then Logical slot test_logical is in sync between postgres0 and postgres1 after 10 seconds
    And Physical slot test_physical is in sync between postgres0 and postgres1 after 10 seconds
    And Logical slot test_logical is in sync between postgres0 and postgres2 after 10 seconds
    And Physical slot test_physical is in sync between postgres0 and postgres2 after 10 seconds
    And Logical slot test_logical is in sync between postgres0 and postgres3 after 10 seconds
    And Physical slot test_physical is in sync between postgres0 and postgres3 after 10 seconds
    And Physical slot postgres1 is in sync between postgres0 and postgres2 after 10 seconds
    And Physical slot postgres3 is in sync between postgres2 and postgres0 after 20 seconds
    And Physical slot postgres3 is in sync between postgres2 and postgres1 after 10 seconds
    And postgres1 does not have a replication slot named postgres2
    And postgres3 does not have a replication slot named postgres2

  @slot-advance
  Scenario: check that only permanent slots are written to the /status key
    Given "status" key in DCS has test_physical in slots
    And "status" key in DCS has postgres0 in slots
    And "status" key in DCS has postgres1 in slots
    And "status" key in DCS does not have postgres2 in slots
    And "status" key in DCS has postgres3 in slots

  Scenario: check permanent physical replication slot after failover
    Given I shut down postgres3
    And I shut down postgres2
    And I shut down postgres0
    Then postgres1 has a physical replication slot named test_physical after 10 seconds
    And postgres1 has a physical replication slot named postgres0 after 10 seconds
    And postgres1 has a physical replication slot named postgres3 after 10 seconds
