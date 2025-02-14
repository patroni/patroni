Feature: nostream node

Scenario: check nostream node is recovering from archive
    When I start postgres-0
    And I configure and start postgres-1 with a tag nostream true
    Then "members/postgres-1" key in DCS has replication_state=in archive recovery after 10 seconds
    And replication works from postgres-0 to postgres-1 after 30 seconds

@pg110000
Scenario: check permanent logical replication slots are not copied
    When I issue a PATCH request to http://127.0.0.1:8008/config with {"postgresql": {"parameters": {"wal_level": "logical"}}, "slots":{"test_logical":{"type":"logical","database":"postgres","plugin":"test_decoding"}}}
    Then I receive a response code 200
    When I run patronictl.py restart batman postgres-0 --force
    Then postgres-0 has a logical replication slot named test_logical with the test_decoding plugin after 10 seconds
    When I configure and start postgres-2 with a tag replicatefrom postgres-1
    Then "members/postgres-2" key in DCS has replication_state=streaming after 10 seconds 
    And postgres-1 does not have a replication slot named test_logical
    And postgres-2 does not have a replication slot named test_logical

@pg110000
Scenario: check that slots are written to the /status key
    Given "status" key in DCS has postgres_0 in slots
    And "status" key in DCS has postgres_2 in slots
    And "status" key in DCS has test_logical in slots
    And "status" key in DCS has test_logical in slots
    And "status" key in DCS does not have postgres_1 in slots
