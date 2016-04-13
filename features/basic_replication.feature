Feature: basic replication
  We should check that the basic bootstrapping, replication and failover works.

  Scenario: check replication of a single table
    Given I start postgres0
    And postgres0 is a leader after 10 seconds
    And I start postgres1
    When I add the table foo to postgres0
    Then table foo is present on postgres1 after 20 seconds

  Scenario: check the basic failover
    When I kill postgres0
    Then postgres1 role is the primary after 32 seconds
    When I start postgres0
    Then postgres0 role is the secondary after 20 seconds
    When I add the table bar to postgres1
    Then table bar is present on postgres0 after 20 seconds
