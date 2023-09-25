Feature: permanent slots
  Scenario: check that physical permanent slots are created
    Given I start postgres0
    Then postgres0 is a leader after 10 seconds
    And there is a non empty initialize key in DCS after 15 seconds
    When I issue a PATCH request to http://127.0.0.1:8008/config with {"slots": {"test_physical": {"type": "physical"}}, "postgresql": {"parameters": {"wal_level": "logical"}}}
    Then I receive a response code 200
    And Response on GET http://127.0.0.1:8008/config contains slots after 10 seconds
    Then postgres0 has a physical replication slot named test_physical after 10 seconds
    And I start postgres1

  @slot-advance
  Scenario: check that logical permanent slots are created
    Given I run patronictl.py restart batman postgres0 --force
    And I issue a PATCH request to http://127.0.0.1:8008/config with {"slots": {"test_logical": {"type": "logical", "database": "postgres", "plugin": "test_decoding"}}}
    Then postgres0 has a logical replication slot named test_logical with the test_decoding plugin after 10 seconds

  @slot-advance
  Scenario: check that permanent slots are created on the replica
    Given postgres1 has a logical replication slot named test_logical with the test_decoding plugin after 10 seconds
    Then Logical slot test_logical is in sync between postgres0 and postgres1 after 10 seconds
    And postgres1 has a physical replication slot named test_physical after 2 seconds

  @slot-advance
  Scenario: check that permanent slots are advanced on the replica
    Given I add the table replicate_me to postgres0
    And I get all changes from physical slot test_physical on postgres0
    When I get all changes from logical slot test_logical on postgres0
    Then Logical slot test_logical is in sync between postgres0 and postgres1 after 10 seconds
    And Physical slot test_physical is in sync between postgres0 and postgres1 after 10 seconds

  Scenario: check permanent physical replication slot after failover
    Given I shut down postgres0
    Then postgres1 has a physical replication slot named test_physical after 10 seconds
