Feature: dynamic synchronized_standby_slots
    We should check that the dynamic_synchronized_standby_slots feature correctly
    manages the synchronized_standby_slots GUC for PG17+ in both synchronous_mode
    and quorum mode, and properly handles toggling and failover scenarios.

  @pg170000
  Scenario: dynamic_synchronized_standby_slots is populated correctly in sync mode
    Given I start postgres-0
    Then postgres-0 is a leader after 10 seconds
    And there is a non empty initialize key in DCS after 15 seconds
    When I issue a PATCH request to http://127.0.0.1:8008/config with {"ttl": 20, "synchronous_mode": true, "dynamic_synchronized_standby_slots": true, "postgresql": {"parameters": {"wal_level": "logical", "sync_replication_slots": "on", "hot_standby_feedback": "on"}}}
    Then I receive a response code 200
    When I run patronictl.py restart batman postgres-0 --force
    Then postgres-0 role is the primary after 20 seconds
    When I start postgres-1
    Then sync key in DCS has sync_standby=postgres-1 after 20 seconds
    And synchronous_standby_names on postgres-0 is set to '"postgres-1"' after 5 seconds
    # Maithem regression: the value must be the proper slot name, NOT u0034postgres_1u0034
    And synchronized_standby_slots on postgres-0 is set to 'postgres_1' after 5 seconds
    And synchronized_standby_slots on postgres-0 matches existing physical slots

  @pg170000
  Scenario: synchronized_standby_slots tracks adding and removing sync members
    Given I start postgres-2
    And I issue a PATCH request to http://127.0.0.1:8008/config with {"synchronous_node_count": 2}
    Then I receive a response code 200
    And sync key in DCS has sync_standby=postgres-1,postgres-2 after 20 seconds
    And synchronized_standby_slots on postgres-0 is set to 'postgres_1,postgres_2' after 10 seconds
    And synchronized_standby_slots on postgres-0 matches existing physical slots
    When I shut down postgres-2
    Then sync key in DCS has sync_standby=postgres-1 after 20 seconds
    And synchronized_standby_slots on postgres-0 is set to 'postgres_1' after 10 seconds

  @pg170000
  Scenario: toggling the feature off restores user-configured value, on re-applies dynamic
    # User has explicitly configured a synchronized_standby_slots value of their own.
    When I issue a PATCH request to http://127.0.0.1:8008/config with {"dynamic_synchronized_standby_slots": false, "postgresql": {"parameters": {"synchronized_standby_slots": "user_managed_slot"}}}
    Then I receive a response code 200
    # After disabling, Patroni restores the user-configured value.
    And synchronized_standby_slots on postgres-0 is set to 'user_managed_slot' after 10 seconds
    # Re-enabling overrides with the dynamic value.
    When I issue a PATCH request to http://127.0.0.1:8008/config with {"dynamic_synchronized_standby_slots": true}
    Then I receive a response code 200
    And synchronized_standby_slots on postgres-0 is set to 'postgres_1' after 15 seconds
    # Clean up: drop the user-configured value so it doesn't interfere with later scenarios.
    When I issue a PATCH request to http://127.0.0.1:8008/config with {"postgresql": {"parameters": {"synchronized_standby_slots": null}}}
    Then I receive a response code 200

  @pg170000
  Scenario: failover - new primary populates synchronized_standby_slots
    Given I shut down postgres-0
    Then postgres-1 role is the primary after 30 seconds
    When I start postgres-0
    Then sync key in DCS has sync_standby=postgres-0 after 30 seconds
    And synchronous_standby_names on postgres-1 is set to '"postgres-0"' after 5 seconds
    # After failover, the new primary must populate synchronized_standby_slots correctly.
    And synchronized_standby_slots on postgres-1 is set to 'postgres_0' after 15 seconds
    And synchronized_standby_slots on postgres-1 matches existing physical slots

  @pg170000
  Scenario: dynamic_synchronized_standby_slots works in quorum mode
    When I issue a PATCH request to http://127.0.0.1:8009/config with {"synchronous_mode": "quorum", "synchronous_node_count": 1}
    Then I receive a response code 200
    And synchronous_standby_names on postgres-1 is set to 'ANY 1 ("postgres-0")' after 10 seconds
    And synchronized_standby_slots on postgres-1 is set to 'postgres_0' after 10 seconds
    And synchronized_standby_slots on postgres-1 matches existing physical slots

  @pg170000
  Scenario: quorum mode with multiple sync standbys lists all of them
    Given I start postgres-2
    When I issue a PATCH request to http://127.0.0.1:8009/config with {"synchronous_node_count": 2}
    Then I receive a response code 200
    And sync key in DCS has sync_standby=postgres-0,postgres-2 after 20 seconds
    And synchronous_standby_names on postgres-1 is set to 'ANY 2 ("postgres-0","postgres-2")' after 10 seconds
    And synchronized_standby_slots on postgres-1 is set to 'postgres_0,postgres_2' after 10 seconds
    And synchronized_standby_slots on postgres-1 matches existing physical slots
