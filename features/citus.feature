Feature: citus
  We should check that coordinator discovers and registers workers and clients don't have errors when worker cluster switches over

  Scenario: check that worker cluster is registered in the coordinator
    Given I start Postgres-0 in citus group 0
    And I start Postgres-2 in citus group 1
    Then Postgres-0 is a leader in a group 0 after 10 seconds
    And Postgres-2 is a leader in a group 1 after 10 seconds
    When I start Postgres-1 in citus group 0
    And I start Postgres-3 in citus group 1
    Then replication works from Postgres-0 to Postgres-1 after 15 seconds
    Then replication works from Postgres-2 to Postgres-3 after 15 seconds
    And Postgres-0 is registered in the Postgres-0 as the primary in group 0 after 5 seconds
    And Postgres-1 is registered in the Postgres-0 as the secondary in group 0 after 5 seconds
    And Postgres-2 is registered in the Postgres-0 as the primary in group 1 after 5 seconds
    And Postgres-3 is registered in the Postgres-0 as the secondary in group 1 after 5 seconds

  Scenario: coordinator failover updates pg_dist_node
    Given I run patronictl.py failover batman --group 0 --candidate Postgres-1 --force
    Then Postgres-1 role is the primary after 10 seconds
    And "members/Postgres-0" key in a group 0 in DCS has state=running after 15 seconds
    And replication works from Postgres-1 to Postgres-0 after 15 seconds
    And Postgres-1 is registered in the Postgres-2 as the primary in group 0 after 5 seconds
    And Postgres-0 is registered in the Postgres-2 as the secondary in group 0 after 15 seconds
    And "sync" key in a group 0 in DCS has sync_standby=Postgres-0 after 15 seconds
    When I run patronictl.py switchover batman --group 0 --candidate Postgres-0 --force
    Then Postgres-0 role is the primary after 10 seconds
    And replication works from Postgres-0 to Postgres-1 after 15 seconds
    And Postgres-0 is registered in the Postgres-2 as the primary in group 0 after 5 seconds
    And Postgres-1 is registered in the Postgres-2 as the secondary in group 0 after 15 seconds
    And "sync" key in a group 0 in DCS has sync_standby=Postgres-1 after 15 seconds

  Scenario: worker switchover doesn't break client queries on the coordinator
    Given I create a distributed table on Postgres-0
    And I start a thread inserting data on Postgres-0
    When I run patronictl.py switchover batman --group 1 --force
    Then I receive a response returncode 0
    And Postgres-3 role is the primary after 10 seconds
    And "members/Postgres-2" key in a group 1 in DCS has state=running after 15 seconds
    And replication works from Postgres-3 to Postgres-2 after 15 seconds
    And Postgres-3 is registered in the Postgres-0 as the primary in group 1 after 5 seconds
    And Postgres-2 is registered in the Postgres-0 as the secondary in group 1 after 15 seconds
    And "sync" key in a group 1 in DCS has sync_standby=Postgres-2 after 15 seconds
    And a thread is still alive
    When I run patronictl.py switchover batman --group 1 --force
    Then I receive a response returncode 0
    And Postgres-2 role is the primary after 10 seconds
    And replication works from Postgres-2 to Postgres-3 after 15 seconds
    And Postgres-2 is registered in the Postgres-0 as the primary in group 1 after 5 seconds
    And Postgres-3 is registered in the Postgres-0 as the secondary in group 1 after 15 seconds
    And "sync" key in a group 1 in DCS has sync_standby=Postgres-3 after 15 seconds
    And a thread is still alive
    When I stop a thread
    Then a distributed table on Postgres-0 has expected rows

  Scenario: worker primary restart doesn't break client queries on the coordinator
    Given I cleanup a distributed table on Postgres-0
    And I start a thread inserting data on Postgres-0
    When I run patronictl.py restart batman Postgres-2 --group 1 --force
    Then I receive a response returncode 0
    And Postgres-2 role is the primary after 10 seconds
    And replication works from Postgres-2 to Postgres-3 after 15 seconds
    And Postgres-2 is registered in the Postgres-0 as the primary in group 1 after 5 seconds
    And Postgres-3 is registered in the Postgres-0 as the secondary in group 1 after 15 seconds
    And a thread is still alive
    When I stop a thread
    Then a distributed table on Postgres-0 has expected rows

  Scenario: check that in-flight transaction is rolled back after timeout when other workers need to change pg_dist_node
    Given I start Postgres-4 in citus group 2
    Then Postgres-4 is a leader in a group 2 after 10 seconds
    And "members/Postgres-4" key in a group 2 in DCS has role=primary after 3 seconds
    When I run patronictl.py edit-config batman --group 2 -s ttl=20 --force
    Then I receive a response returncode 0
    And I receive a response output "+ttl: 20"
    Then Postgres-4 is registered in the Postgres-2 as the primary in group 2 after 5 seconds
    When I shut down Postgres-4
    Then there is a transaction in progress on Postgres-0 changing pg_dist_node after 5 seconds
    When I run patronictl.py restart batman Postgres-2 --group 1 --force
    Then a transaction finishes in 20 seconds
