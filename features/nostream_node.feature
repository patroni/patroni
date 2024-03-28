Feature: nostream node

Scenario: check nostream node is recovering from archive
    When I start postgres0
    And I configure and start postgres1 with a tag nostream true
    Then "members/postgres1" key in DCS has replication_state=in archive recovery after 10 seconds
    And replication works from postgres0 to postgres1 after 30 seconds

@slot-advance
Scenario: check permanent logical replication slots are not copied
    When I issue a PATCH request to http://127.0.0.1:8008/config with {"postgresql": {"parameters": {"wal_level": "logical"}}, "slots":{"test_logical":{"type":"logical","database":"postgres","plugin":"test_decoding"}}}
    Then I receive a response code 200
    When I run patronictl.py restart batman postgres0 --force
    Then postgres0 has a logical replication slot named test_logical with the test_decoding plugin after 10 seconds
    When I configure and start postgres2 with a tag replicatefrom postgres1
    Then "members/postgres2" key in DCS has replication_state=streaming after 10 seconds 
    And postgres1 does not have a replication slot named test_logical
    And postgres2 does not have a replication slot named test_logical
