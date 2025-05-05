Feature: synchronous replicas priority
    We should check that we can give nodes priority for becoming synchronous replicas

  Scenario: check replica with sync_priority=0 does not become a synchronous replica
     Given I start postgres-0
     Then postgres-0 is a leader after 10 seconds
     And there is a non empty initialize key in DCS after 15 seconds
     When I issue a PATCH request to http://127.0.0.1:8008/config with {"ttl": 20, "synchronous_mode": true}
     Then I receive a response code 200
     When I configure and start postgres-1 with a tag sync_priority 0
     Then sync key in DCS has leader=postgres-0 after 20 seconds
     And sync key in DCS has sync_standby=None after 5 seconds

  Scenario: check higher synchronous replicas priority is respected
    Given I configure and start postgres-2 with a tag sync_priority 1
    And I configure and start postgres-3 with a tag sync_priority 2
    Then replication works from postgres-0 to postgres-2 after 20 seconds
    And replication works from postgres-0 to postgres-3 after 20 seconds
    When I issue a PATCH request to http://127.0.0.1:8008/config with {"synchronous_node_count": 1}
    Then I receive a response code 200
    And sync key in DCS has sync_standby=postgres-3 after 10 seconds


 Scenario: check conflicting configuration handling
    When I set nosync tag in postgres-3 config
    And I issue an empty POST request to http://127.0.0.1:8011/reload
    Then I receive a response code 202
    And there is one of ["Conflicting configuration between nosync: True and sync_priority: 2. Defaulting to nosync: True"] WARNING in the postgres-3 patroni log after 5 seconds
    And "members/postgres-3" key in DCS has tags={'nosync': True, 'sync_priority': '2'} after 10 seconds
    And "sync" key in DCS has sync_standby=postgres-2 after 10 seconds
    When I reset nosync tag in postgres-1 config
    And I issue an empty POST request to http://127.0.0.1:8009/reload
    Then I receive a response code 202
    And there is one of ["Conflicting configuration between nosync: False and sync_priority: 0. Defaulting to nosync: False"] WARNING in the postgres-1 patroni log after 5 seconds
    And "members/postgres-1" key in DCS has tags={'nosync': False, 'sync_priority': '0'} after 10 seconds
    When I shut down postgres-2
    And "sync" key in DCS has sync_standby=postgres-1 after 3 seconds

