Feature: ignored slots
  Scenario: check ignored slots aren't removed on failover/switchover
    Given I start postgres-1
    Then postgres-1 is a leader after 10 seconds
    And there is a non empty initialize key in DCS after 15 seconds
    When I issue a PATCH request to http://127.0.0.1:8009/config with {"ignore_slots": [{"name": "unmanaged_slot_0", "database": "postgres", "plugin": "test_decoding", "type": "logical"}, {"name": "unmanaged_slot_1", "database": "postgres", "plugin": "test_decoding"}, {"name": "unmanaged_slot_2", "database": "postgres"}, {"name": "unmanaged_slot_3"}], "postgresql": {"parameters": {"wal_level": "logical"}}}
    Then I receive a response code 200
    And Response on GET http://127.0.0.1:8009/config contains ignore_slots after 10 seconds
    And Response on GET http://127.0.0.1:8009/patroni contains pending_restart after 10 seconds
    # Make sure the wal_level has been changed.
    When I run patronictl.py restart batman postgres-1 --force
    Then "members/postgres-1" key in DCS has role=primary after 10 seconds
    # Make sure Patroni has finished telling Postgres it should be accepting writes.
    And postgres-1 role is the primary after 20 seconds
    # 1. Create our test logical replication slot.
    # Test that ny subset of attributes in the ignore slots matcher is enough to match a slot
    # by using 3 different slots.
    When I create a logical replication slot unmanaged_slot_0 on postgres-1 with the test_decoding plugin
    And I create a logical replication slot unmanaged_slot_1 on postgres-1 with the test_decoding plugin
    And I create a logical replication slot unmanaged_slot_2 on postgres-1 with the test_decoding plugin
    And I create a logical replication slot unmanaged_slot_3 on postgres-1 with the test_decoding plugin
    And I create a logical replication slot dummy_slot on postgres-1 with the test_decoding plugin
    # It seems like it'd be obvious that these slots exist since we just created them,
    # but Patroni can actually end up dropping them almost immediately, so it's helpful
    # to verify they exist before we begin testing whether they persist through failover
    # cycles.
    Then postgres-1 has a logical replication slot named unmanaged_slot_0 with the test_decoding plugin after 2 seconds
    And postgres-1 has a logical replication slot named unmanaged_slot_1 with the test_decoding plugin after 2 seconds
    And postgres-1 has a logical replication slot named unmanaged_slot_2 with the test_decoding plugin after 2 seconds
    And postgres-1 has a logical replication slot named unmanaged_slot_3 with the test_decoding plugin after 2 seconds

    When I start postgres-0
    Then "members/postgres-0" key in DCS has role=replica after 10 seconds
    And postgres-0 role is the secondary after 20 seconds
    # Verify that the replica has advanced beyond the point in the WAL
    # where we created the replication slot so that on the next failover
    # cycle we don't accidentally rewind to before the slot creation.
    And replication works from postgres-1 to postgres-0 after 20 seconds
    When I shut down postgres-1
    Then "members/postgres-0" key in DCS has role=primary after 10 seconds

    # 2. After a failover the server (now a replica) still has the slot.
    When I start postgres-1
    Then postgres-1 role is the secondary after 20 seconds
    And "members/postgres-1" key in DCS has role=replica after 10 seconds
    # give Patroni time to sync replication slots
    And I sleep for 2 seconds
    And postgres-1 has a logical replication slot named unmanaged_slot_0 with the test_decoding plugin after 2 seconds
    And postgres-1 has a logical replication slot named unmanaged_slot_1 with the test_decoding plugin after 2 seconds
    And postgres-1 has a logical replication slot named unmanaged_slot_2 with the test_decoding plugin after 2 seconds
    And postgres-1 has a logical replication slot named unmanaged_slot_3 with the test_decoding plugin after 2 seconds
    And postgres-1 does not have a replication slot named dummy_slot

    # 3. After a failover the server (now a primary) still has the slot.
    When I shut down postgres-0
    Then "members/postgres-1" key in DCS has role=primary after 10 seconds
    And postgres-1 has a logical replication slot named unmanaged_slot_0 with the test_decoding plugin after 2 seconds
    And postgres-1 has a logical replication slot named unmanaged_slot_1 with the test_decoding plugin after 2 seconds
    And postgres-1 has a logical replication slot named unmanaged_slot_2 with the test_decoding plugin after 2 seconds
    And postgres-1 has a logical replication slot named unmanaged_slot_3 with the test_decoding plugin after 2 seconds

  @pg170000
  Scenario: check that logical slots with failover are not removed by Patroni
    Given I create a logical failover slot test17 on postgres-1 with the test_decoding plugin
    When I run patronictl.py restart batman postgres-1 --force
    Then postgres-1 has a logical replication slot named test17 with the test_decoding plugin after 2 seconds

  @pg170000
  Scenario: check that logical slots with failover is dropped, recreated and synced after switchover
    Given I issue a PATCH request to http://127.0.0.1:8009/config with {"postgresql": {"parameters": {"hot_standby_feedback": "on", "sync_replication_slots": "on"}}}
    Then I receive a response code 200
    When I start postgres-0
    Then postgres-0 has a synced replication slot named test17 with the test_decoding plugin after 30 seconds
    When I add the table replicate1 to postgres-1
    And I get all changes from logical slot test17 on postgres-1
    Then logical slot test17 is in sync between postgres-1 and postgres-0 after 30 seconds
    When I run patronictl.py switchover batman --force
    Then I receive a response returncode 0
    And postgres-0 role is the primary after 5 seconds
    And replication works from postgres-0 to postgres-1 after 20 seconds
    And I get all changes from logical slot test17 on postgres-0
    # slot synchronization in 17+ is quite complex, the source slot and replication
    # must move forward a few times before synchronization completes.
    When I add the table replicate2 to postgres-0
    And I get all changes from logical slot test17 on postgres-0
    Then postgres-1 has a synced replication slot named test17 with the test_decoding plugin after 30 seconds
    When I add the table replicate3 to postgres-0
    And I get all changes from logical slot test17 on postgres-0
    Then postgres-1 has a synced replication slot named test17 with the test_decoding plugin after 30 seconds
    And replication works from postgres-0 to postgres-1 after 20 seconds
    And logical slot test17 is in sync between postgres-0 and postgres-1 after 30 seconds
