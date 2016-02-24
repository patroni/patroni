Feature: basic replication
  In order to check that basic replication works
  As observers
  We start the primary and the replica
  add a table on the primary
  and check that it gets replicated to the replica over time.

  Scenario: check replication of a single table
    Given I start postgres0
    And I start postgres1
    When I add the table foo to postgres0
    Then table foo is present on postgres1 after 10 seconds
