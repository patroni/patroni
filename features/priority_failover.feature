Feature: priority replication
  We should check that we can give nodes priority during failover

  Scenario: check failover priority 0 prevents leaderships
    Given I configure and start postgres-0 with a tag failover_priority 1
    And I configure and start postgres-1 with a tag failover_priority 0
    Then replication works from postgres-0 to postgres-1 after 20 seconds
    When I shut down postgres-0
    And there is one of ["following a different leader because I am not allowed to promote"] INFO in the postgres-1 patroni log after 5 seconds
    Then postgres-1 role is the secondary after 10 seconds
    When I start postgres-0
    Then postgres-0 role is the primary after 10 seconds

  Scenario: check higher failover priority is respected
    Given I configure and start postgres-2 with a tag failover_priority 1
    And I configure and start postgres-3 with a tag failover_priority 2
    Then replication works from postgres-0 to postgres-2 after 20 seconds
    And replication works from postgres-0 to postgres-3 after 20 seconds
    When I shut down postgres-0
    Then postgres-3 role is the primary after 10 seconds
    And there is one of ["postgres-3 has equally tolerable WAL position and priority 2, while this node has priority 1","Wal position of postgres-3 is ahead of my wal position"] INFO in the postgres-2 patroni log after 5 seconds

 Scenario: check conflicting configuration handling
    When I set nofailover tag in postgres-2 config
    And I issue an empty POST request to http://127.0.0.1:8010/reload
    Then I receive a response code 202
    And there is one of ["Conflicting configuration between nofailover: True and failover_priority: 1. Defaulting to nofailover: True"] WARNING in the postgres-2 patroni log after 5 seconds
    And "members/postgres-2" key in DCS has tags={'failover_priority': '1', 'nofailover': True} after 10 seconds
    When I issue a POST request to http://127.0.0.1:8010/failover with {"candidate": "postgres-2"}
    Then I receive a response code 412
    And I receive a response text "failover is not possible: no good candidates have been found"
    When I reset nofailover tag in postgres-1 config
    And I issue an empty POST request to http://127.0.0.1:8009/reload
    Then I receive a response code 202
    And there is one of ["Conflicting configuration between nofailover: False and failover_priority: 0. Defaulting to nofailover: False"] WARNING in the postgres-1 patroni log after 5 seconds
    And "members/postgres-1" key in DCS has tags={'failover_priority': '0', 'nofailover': False} after 10 seconds
    And I issue a POST request to http://127.0.0.1:8009/failover with {"candidate": "postgres-1"}
    Then I receive a response code 200
    And postgres-1 role is the primary after 10 seconds
