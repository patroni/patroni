Feature: citus
  We should check that coordinator discovers and registers workers and clients don't have errors when worker cluster switches over

  Scenario: check that worker cluster is registered in the coordinator
    Given I start postgres0 in citus group 0
    And I start postgres2 in citus group 1
    Then postgres0 is a leader in a group 0 after 10 seconds
    And postgres2 is a leader in a group 1 after 10 seconds
    When I start postgres1 in citus group 0
    And I start postgres3 in citus group 1
    Then replication works from postgres0 to postgres1 after 15 seconds
    Then replication works from postgres2 to postgres3 after 15 seconds
    And postgres0 is registered in the postgres0 as the primary in group 0 after 5 seconds
    And postgres2 is registered in the postgres0 as the primary in group 1 after 5 seconds

  Scenario: coordinator failover updates pg_dist_node
    Given I run patronictl.py failover batman --group 0 --candidate postgres1 --force
    Then postgres1 role is the primary after 10 seconds
    And "members/postgres0" key in a group 0 in DCS has state=running after 15 seconds
    And replication works from postgres1 to postgres0 after 15 seconds
    And postgres1 is registered in the postgres2 as the primary in group 0 after 5 seconds
    And "sync" key in a group 0 in DCS has sync_standby=postgres0 after 15 seconds
    When I run patronictl.py switchover batman --group 0 --candidate postgres0 --force
    Then postgres0 role is the primary after 10 seconds
    And replication works from postgres0 to postgres1 after 15 seconds
    And postgres0 is registered in the postgres2 as the primary in group 0 after 5 seconds
    And "sync" key in a group 0 in DCS has sync_standby=postgres1 after 15 seconds

  Scenario: worker switchover doesn't break client queries on the coordinator
    Given I create a distributed table on postgres0
    And I start a thread inserting data on postgres0
    When I run patronictl.py switchover batman --group 1 --force
    Then I receive a response returncode 0
    And postgres3 role is the primary after 10 seconds
    And "members/postgres2" key in a group 1 in DCS has state=running after 15 seconds
    And replication works from postgres3 to postgres2 after 15 seconds
    And postgres3 is registered in the postgres0 as the primary in group 1 after 5 seconds
    And "sync" key in a group 1 in DCS has sync_standby=postgres2 after 15 seconds
    And a thread is still alive
    When I run patronictl.py switchover batman --group 1 --force
    Then I receive a response returncode 0
    And postgres2 role is the primary after 10 seconds
    And replication works from postgres2 to postgres3 after 15 seconds
    And postgres2 is registered in the postgres0 as the primary in group 1 after 5 seconds
    And "sync" key in a group 1 in DCS has sync_standby=postgres3 after 15 seconds
    And a thread is still alive
    When I stop a thread
    Then a distributed table on postgres0 has expected rows

  Scenario: worker primary restart doesn't break client queries on the coordinator
    Given I cleanup a distributed table on postgres0
    And I start a thread inserting data on postgres0
    When I run patronictl.py restart batman postgres2 --group 1 --force
    Then I receive a response returncode 0
    And postgres2 role is the primary after 10 seconds
    And replication works from postgres2 to postgres3 after 15 seconds
    And postgres2 is registered in the postgres0 as the primary in group 1 after 5 seconds
    And a thread is still alive
    When I stop a thread
    Then a distributed table on postgres0 has expected rows

  Scenario: check that in-flight transaction is rolled back after timeout when other workers need to change pg_dist_node
    Given I start postgres4 in citus group 2
    Then postgres4 is a leader in a group 2 after 10 seconds
    And "members/postgres4" key in a group 2 in DCS has role=master after 3 seconds
    When I run patronictl.py edit-config batman --group 2 -s ttl=20 --force
    Then I receive a response returncode 0
    And I receive a response output "+ttl: 20"
    Then postgres4 is registered in the postgres2 as the primary in group 2 after 5 seconds
    When I shut down postgres4
    Then there is a transaction in progress on postgres0 changing pg_dist_node after 5 seconds
    When I run patronictl.py restart batman postgres2 --group 1 --force
    Then a transaction finishes in 20 seconds
