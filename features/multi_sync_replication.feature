Feature: multi sync replication
  We should check that the basic bootstrapping, multi sync replication and failover works.

  Scenario: check replication of a single table
    Given I start postgres0
    Then postgres0 is a leader after 10 seconds
    And there is a non empty initialize key in DCS after 15 seconds
    When I issue a PATCH request to http://127.0.0.1:8008/config with {"ttl": 20, "loop_wait": 2, "synchronous_mode": true}
    Then I receive a response code 200
    When I start postgres1
    And I configure and start postgres2 with a tag replicatefrom postgres0
    And "sync" key in DCS has leader=postgres0 after 20 seconds
    And I add the table foo to postgres0
    Then table foo is present on postgres1 after 20 seconds
    Then table foo is present on postgres2 after 20 seconds

  Scenario: check multi sync replication
    Given I issue a PATCH request to http://127.0.0.1:8008/config with {"synchronous_node_count": 2}
    Then I receive a response code 200
    And I sleep for 30 seconds
    Then "sync" sync key in DCS has sync_standby_list=postgres1 after 5 seconds
    Then "sync" sync key in DCS has sync_standby_list=postgres2 after 5 seconds
    When I issue a GET request to http://127.0.0.1:8010/sync
    Then I receive a response code 200
    When I issue a GET request to http://127.0.0.1:8009/sync
    Then I receive a response code 200
    When I issue a PATCH request to http://127.0.0.1:8008/config with {"synchronous_node_count": 1}
    Then I receive a response code 200
    And I shut down postgres1
    And I sleep for 40 seconds
    Then "sync" sync key in DCS has sync_standby_list=postgres2 after 10 seconds
    When I start postgres1
    And "members/postgres1" key in DCS has state=running after 10 seconds
    When I issue a GET request to http://127.0.0.1:8010/sync
    Then I receive a response code 200
    When I issue a GET request to http://127.0.0.1:8009/async
    Then I receive a response code 200

