Feature: basic replication
  In order to check that basic replication works
  As observers
  We start the primary and the replica
  add a table on the primary
  and check that it gets replicated to the replica over time.
  We stop the primary and check that the replica promotes itself to primary
  We start the old primary and check that it rejoins as a replica.

  Scenario: check replication of a single table
    Given I start postgres0
    And I start postgres1
    When I add the table foo to postgres0
    Then table foo is present on postgres1 after 10 seconds

  Scenario: check the basic failover
    When I shut down postgres0
    Then postgres1 role is the primary after 10 seconds
    When I start postgres0
    Then postgres0 role is the secondary after 10 seconds
    When I add the table bar to postgres1
    Then table bar is present on postgres1 after 10 seconds