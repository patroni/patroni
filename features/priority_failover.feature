Feature: priority replication
  We should check that we can give nodes priority during failover

  Scenario: check failover priority 0 prevents leaderships
    Given I configure and start postgres0 with a tag failover_priority 1
    And I configure and start postgres1 with a tag failover_priority 0
    Then replication works from postgres0 to postgres1 after 20 seconds
    When I shut down postgres0
    And I sleep for 5 seconds
    Then postgres1 role is the secondary after 10 seconds
    And there is one of ["following a different leader because I am not allowed to promote"] INFO in the postgres1 patroni log after 5 seconds
    Given I start postgres0
    Then postgres0 role is the primary after 10 seconds

  Scenario: check higher failover priority is respected
    Given I configure and start postgres2 with a tag failover_priority 1
    And I configure and start postgres3 with a tag failover_priority 2
    Then replication works from postgres0 to postgres2 after 20 seconds
    And replication works from postgres0 to postgres3 after 20 seconds
    When I shut down postgres0
    And I sleep for 5 seconds
    Then postgres3 role is the primary after 10 seconds
    And there is one of ["postgres3 has equally tolerable WAL position and priority 2, while this node has priority 1","Wal position of postgres3 is ahead of my wal position"] INFO in the postgres2 patroni log after 5 seconds
