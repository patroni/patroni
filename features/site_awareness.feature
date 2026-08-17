Feature: site awareness
  Scenario: setup a multisite cluster
    When I start postgres-0 in site dc1
    And postgres-0 is a leader after 10 seconds
    And there is a non empty initialize key in DCS after 15 seconds
    And "members/postgres-0" key in DCS has site=dc1 after 5 seconds
    And I start postgres-1 in site dc1 with failover priority 2, sync priority 2
    And I start postgres-2 in site dc2 with failover priority 3, sync priority 3
    And I start postgres-3 in site dc2 with failover priority 4, sync priority 4
    Then "members/postgres-1" key in DCS has site=dc1 after 5 seconds
    And "members/postgres-2" key in DCS has site=dc2 after 5 seconds
    And "members/postgres-3" key in DCS has site=dc2 after 5 seconds
    And "members/postgres-1" key in DCS has state=running after 15 seconds
    And "members/postgres-2" key in DCS has state=running after 15 seconds
    And "members/postgres-3" key in DCS has state=running after 15 seconds
    And "status" key in DCS has dc1 in current_site

  Scenario: test site-aware reinit
    When I run patronictl.py reinit batman postgres-2 --force
    Then I receive a response returncode 0
    And there is one of ["bootstrapped from replica 'postgres-3'"] INFO in the postgres-2 patroni log after 25 seconds
    And postgres-2 is in sync with primary after 30 seconds

  Scenario: test local failover
    When I shut down postgres-0
    Then "members/postgres-1" key in DCS has role=primary after 10 seconds
    And postgres-2 is in sync with primary after 30 seconds
    And postgres-3 is in sync with primary after 30 seconds

  Scenario: test site failover with failover_priority
    When I shut down postgres-1
    Then "members/postgres-3" key in DCS has role=primary after 10 seconds
    And postgres-2 is in sync with primary after 30 seconds

  Scenario: test synchronous_cross_site disabled
    When I start postgres-0
    And I start postgres-1
    And "members/postgres-0" key in DCS has replication_state=streaming after 10 seconds
    And "members/postgres-1" key in DCS has replication_state=streaming after 10 seconds
    And I issue a PATCH request to http://127.0.0.1:8011/config with {"synchronous_mode": true, "synchronous_node_count": 2}
    Then "sync" key in DCS has sync_standby=postgres-1,postgres-2 after 10 seconds
    And synchronous_standby_names on postgres-3 is set to '2 ("postgres-1","postgres-2")' after 10 seconds

  Scenario: test synchronous_cross_site balanced
    When I start postgres-4 in site dc3 with failover priority 5, sync priority 5
    Then "members/postgres-4" key in DCS has site=dc3 after 5 seconds
    And "members/postgres-4" key in DCS has replication_state=streaming after 10 seconds
    When I issue a PATCH request to http://127.0.0.1:8011/config with {"synchronous_cross_site": "balanced"}
    Then "sync" key in DCS has sync_standby=postgres-1,postgres-4 after 10 seconds
    And synchronous_standby_names on postgres-3 is set to '2 ("postgres-1","postgres-4")' after 10 seconds

  Scenario: test synchronous_cross_site local_only
    When I issue a PATCH request to http://127.0.0.1:8011/config with {"synchronous_cross_site": "local_only"}
    Then "sync" key in DCS has sync_standby=postgres-2 after 10 seconds
    And synchronous_standby_names on postgres-3 is set to '"postgres-2"' after 10 seconds

  Scenario: test synchronous_cross_site prefer_local and local_only with synchronous_mode_strict when no local nodes
    When I issue a PATCH request to http://127.0.0.1:8011/config with {"synchronous_cross_site": "prefer_local", "synchronous_mode_strict": "true"}
    Then "sync" key in DCS has sync_standby=postgres-2 after 10 seconds
    And synchronous_standby_names on postgres-3 is set to '2 ("postgres-2","postgres-4")' after 10 seconds
    When I shut down postgres-2
    Then "sync" key in DCS has sync_standby=postgres-1,postgres-4 after 10 seconds
    And synchronous_standby_names on postgres-3 is set to '2 ("postgres-1","postgres-4")' after 10 seconds
    When I issue a PATCH request to http://127.0.0.1:8011/config with {"synchronous_cross_site": "local_only"}
    Then "sync" key in DCS has sync_standby=None after 10 seconds
    And synchronous_standby_names on postgres-3 is set to '__patroni_strict_sync_replica_placeholder__' after 10 seconds

  Scenario: test synchronous_cross_site remote_only
    When I issue a PATCH request to http://127.0.0.1:8011/config with {"synchronous_cross_site": "remote_only", "synchronous_mode_strict": "false"}
    Then "sync" key in DCS has sync_standby=postgres-1,postgres-4 after 10 seconds
    And synchronous_standby_names on postgres-3 is set to '2 ("postgres-1","postgres-4")' after 10 seconds
    When I shut down postgres-1
    And I shut down postgres-4
    Then "sync" key in DCS has sync_standby=postgres-0 after 40 seconds
    And synchronous_standby_names on postgres-3 is set to '"postgres-0"' after 20 seconds
    When I start postgres-2
    And I start postgres-5 in site dc2 with failover priority 6, sync priority 6
    Then "members/postgres-2" key in DCS has replication_state=streaming after 10 seconds
    And "members/postgres-5" key in DCS has replication_state=streaming after 10 seconds
    When I shut down postgres-0
    Then "sync" key in DCS has sync_standby=None after 20 seconds
    And synchronous_standby_names on postgres-3 is set to '_empty_str_' after 20 seconds
