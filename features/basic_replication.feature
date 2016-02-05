Feature: basic replication
  In order to check that basic replication is working
  As observers
  We'll start 2 nodes of a new cluster,
  add a table to the primary
  and check that it gets replicated to the other over time.

  Scenario: check replication of a single table
    Given I have started postgres0
    And I have started postgres1
    When I add the table foo to postgres0
    Then table foo is present on postgres1
