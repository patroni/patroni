Feature: basic replication
  We should check that the basic bootstrapping, replication, failover and dyncamic configuration works.

  Scenario: check replication of a single table
    Given I start postgres0
    And postgres0 is a leader after 10 seconds
    And I start postgres1
    When I add the table foo to postgres0
    Then table foo is present on postgres1 after 20 seconds

  Scenario: check dynamic configuration change via DCS
    When I patch global configuration with {"ttl": 20, "loop_wait": 5, "postgresql": {"parameters": {"max_connections": 101}}}
    Then Response on GET http://127.0.0.1:8008/patroni contains restart_pending after 11 seconds
    And Response on GET http://127.0.0.1:8009/patroni contains restart_pending after 11 seconds

  Scenario: check the basic failover
    And I kill postgres0
    Then postgres1 role is the primary after 22 seconds
    When I start postgres0
    Then postgres0 role is the secondary after 20 seconds
    When I add the table bar to postgres1
    Then table bar is present on postgres0 after 10 seconds
