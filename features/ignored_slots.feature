Feature: ignored slots
  Scenario: check ignored slots aren't removed on failover/switchover
    Given I start postgres1
    Then postgres1 is a leader after 10 seconds
    And there is a non empty initialize key in DCS after 15 seconds
    When I issue a PATCH request to http://127.0.0.1:8009/config with {"ignore_slots": [{"name": "unmanaged_slot_0", "database": "postgres", "plugin": "test_decoding", "type": "logical"}, {"name": "unmanaged_slot_1", "database": "postgres", "plugin": "test_decoding"}, {"name": "unmanaged_slot_2", "database": "postgres"}, {"name": "unmanaged_slot_3"}], "postgresql": {"parameters": {"wal_level": "logical"}}}
    Then I receive a response code 200
    And Response on GET http://127.0.0.1:8009/config contains ignore_slots after 10 seconds
    # Make sure the wal_level has been changed.
    When I shut down postgres1
    And I start postgres1
    Then postgres1 is a leader after 10 seconds
    And "members/postgres1" key in DCS has role=master after 10 seconds
    # Make sure Patroni has finished telling Postgres it should be accepting writes.
    And postgres1 role is the primary after 20 seconds
    # 1. Create our test logical replication slot.
    # Test that ny subset of attributes in the ignore slots matcher is enough to match a slot
    # by using 3 different slots.
    When I create a logical replication slot unmanaged_slot_0 on postgres1 with the test_decoding plugin
    And I create a logical replication slot unmanaged_slot_1 on postgres1 with the test_decoding plugin
    And I create a logical replication slot unmanaged_slot_2 on postgres1 with the test_decoding plugin
    And I create a logical replication slot unmanaged_slot_3 on postgres1 with the test_decoding plugin
    And I create a logical replication slot dummy_slot on postgres1 with the test_decoding plugin
    # It seems like it'd be obvious that these slots exist since we just created them,
    # but Patroni can actually end up dropping them almost immediately, so it's helpful
    # to verify they exist before we begin testing whether they persist through failover
    # cycles.
    Then postgres1 has a logical replication slot named unmanaged_slot_0 with the test_decoding plugin after 2 seconds
    And postgres1 has a logical replication slot named unmanaged_slot_1 with the test_decoding plugin after 2 seconds
    And postgres1 has a logical replication slot named unmanaged_slot_2 with the test_decoding plugin after 2 seconds
    And postgres1 has a logical replication slot named unmanaged_slot_3 with the test_decoding plugin after 2 seconds

    When I start postgres0
    Then "members/postgres0" key in DCS has role=replica after 10 seconds
    And postgres0 role is the secondary after 20 seconds
    # Verify that the replica has advanced beyond the point in the WAL
    # where we created the replication slot so that on the next failover
    # cycle we don't accidentally rewind to before the slot creation.
    And replication works from postgres1 to postgres0 after 20 seconds
    When I shut down postgres1
    Then "members/postgres0" key in DCS has role=master after 10 seconds

    # 2. After a failover the server (now a replica) still has the slot.
    When I start postgres1
    Then postgres1 role is the secondary after 20 seconds
    And "members/postgres1" key in DCS has role=replica after 10 seconds
    # give Patroni time to sync replication slots
    And I sleep for 2 seconds
    And postgres1 has a logical replication slot named unmanaged_slot_0 with the test_decoding plugin after 2 seconds
    And postgres1 has a logical replication slot named unmanaged_slot_1 with the test_decoding plugin after 2 seconds
    And postgres1 has a logical replication slot named unmanaged_slot_2 with the test_decoding plugin after 2 seconds
    And postgres1 has a logical replication slot named unmanaged_slot_3 with the test_decoding plugin after 2 seconds
    And postgres1 does not have a replication slot named dummy_slot

    # 3. After a failover the server (now a primary) still has the slot.
    When I shut down postgres0
    Then "members/postgres1" key in DCS has role=master after 10 seconds
    And postgres1 has a logical replication slot named unmanaged_slot_0 with the test_decoding plugin after 2 seconds
    And postgres1 has a logical replication slot named unmanaged_slot_1 with the test_decoding plugin after 2 seconds
    And postgres1 has a logical replication slot named unmanaged_slot_2 with the test_decoding plugin after 2 seconds
    And postgres1 has a logical replication slot named unmanaged_slot_3 with the test_decoding plugin after 2 seconds
