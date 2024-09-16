Feature: citus
  We should check that coordinator discovers and registers workers and clients don't have errors when worker cluster switches over

  Scenario: check that worker cluster is registered in the coordinator
    Given I start postgres-0 in citus group 0
    And I start postgres-2 in citus group 1
    Then postgres-0 is a leader in a group 0 after 10 seconds
    And postgres-2 is a leader in a group 1 after 10 seconds
    When I start postgres-1 in citus group 0
    And I start postgres-3 in citus group 1
    Then replication works from postgres-0 to postgres-1 after 15 seconds
    Then replication works from postgres-2 to postgres-3 after 15 seconds
    And postgres-0 is registered in the postgres-0 as the primary in group 0 after 5 seconds
    And postgres-1 is registered in the postgres-0 as the secondary in group 0 after 5 seconds
    And postgres-2 is registered in the postgres-0 as the primary in group 1 after 5 seconds
    And postgres-3 is registered in the postgres-0 as the secondary in group 1 after 5 seconds

  Scenario: coordinator failover updates pg_dist_node
    Given I run patronictl.py failover batman --group 0 --candidate postgres-1 --force
    Then postgres-1 role is the primary after 10 seconds
    And "members/postgres-0" key in a group 0 in DCS has state=running after 15 seconds
    And replication works from postgres-1 to postgres-0 after 15 seconds
    And postgres-1 is registered in the postgres-2 as the primary in group 0 after 5 seconds
    And postgres-0 is registered in the postgres-2 as the secondary in group 0 after 15 seconds
    And "sync" key in a group 0 in DCS has sync_standby=postgres-0 after 15 seconds
    When I run patronictl.py switchover batman --group 0 --candidate postgres-0 --force
    Then postgres-0 role is the primary after 10 seconds
    And replication works from postgres-0 to postgres-1 after 15 seconds
    And postgres-0 is registered in the postgres-2 as the primary in group 0 after 5 seconds
    And postgres-1 is registered in the postgres-2 as the secondary in group 0 after 15 seconds
    And "sync" key in a group 0 in DCS has sync_standby=postgres-1 after 15 seconds

  Scenario: worker switchover doesn't break client queries on the coordinator
    Given I create a distributed table on postgres-0
    And I start a thread inserting data on postgres-0
    When I run patronictl.py switchover batman --group 1 --force
    Then I receive a response returncode 0
    And postgres-3 role is the primary after 10 seconds
    And "members/postgres-2" key in a group 1 in DCS has state=running after 15 seconds
    And replication works from postgres-3 to postgres-2 after 15 seconds
    And postgres-3 is registered in the postgres-0 as the primary in group 1 after 5 seconds
    And postgres-2 is registered in the postgres-0 as the secondary in group 1 after 15 seconds
    And "sync" key in a group 1 in DCS has sync_standby=postgres-2 after 15 seconds
    And a thread is still alive
    When I run patronictl.py switchover batman --group 1 --force
    Then I receive a response returncode 0
    And postgres-2 role is the primary after 10 seconds
    And replication works from postgres-2 to postgres-3 after 15 seconds
    And postgres-2 is registered in the postgres-0 as the primary in group 1 after 5 seconds
    And postgres-3 is registered in the postgres-0 as the secondary in group 1 after 15 seconds
    And "sync" key in a group 1 in DCS has sync_standby=postgres-3 after 15 seconds
    And a thread is still alive
    When I stop a thread
    Then a distributed table on postgres-0 has expected rows

  Scenario: worker primary restart doesn't break client queries on the coordinator
    Given I cleanup a distributed table on postgres-0
    And I start a thread inserting data on postgres-0
    When I run patronictl.py restart batman postgres-2 --group 1 --force
    Then I receive a response returncode 0
    And postgres-2 role is the primary after 10 seconds
    And replication works from postgres-2 to postgres-3 after 15 seconds
    And postgres-2 is registered in the postgres-0 as the primary in group 1 after 5 seconds
    And postgres-3 is registered in the postgres-0 as the secondary in group 1 after 15 seconds
    And a thread is still alive
    When I stop a thread
    Then a distributed table on postgres-0 has expected rows

  Scenario: check that in-flight transaction is rolled back after timeout when other workers need to change pg_dist_node
    Given I start postgres-4 in citus group 2
    Then postgres-4 is a leader in a group 2 after 10 seconds
    And "members/postgres-4" key in a group 2 in DCS has role=primary after 3 seconds
    When I run patronictl.py edit-config batman --group 2 -s ttl=20 --force
    Then I receive a response returncode 0
    And I receive a response output "+ttl: 20"
    Then postgres-4 is registered in the postgres-2 as the primary in group 2 after 5 seconds
    When I shut down postgres-4
    Then there is a transaction in progress on postgres-0 changing pg_dist_node after 5 seconds
    When I run patronictl.py restart batman postgres-2 --group 1 --force
    Then a transaction finishes in 20 seconds
