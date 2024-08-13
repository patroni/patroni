Feature: quorum commit
  Check basic workfrlows when quorum commit is enabled

  Scenario: check enable quorum commit and that the only leader promotes after restart
    Given I start postgres0
    Then postgres0 is a leader after 10 seconds
    And there is a non empty initialize key in DCS after 15 seconds
    When I issue a PATCH request to http://127.0.0.1:8008/config with {"ttl": 20, "synchronous_mode": "quorum"}
    Then I receive a response code 200
    And sync key in DCS has leader=postgres0 after 20 seconds
    And sync key in DCS has quorum=0 after 2 seconds
    And synchronous_standby_names on postgres0 is set to "_empty_str_" after 2 seconds
    When I shut down postgres0
    And sync key in DCS has leader=postgres0 after 2 seconds
    When I start postgres0
    Then postgres0 role is the primary after 10 seconds
    When I issue a PATCH request to http://127.0.0.1:8008/config with {"synchronous_mode_strict": true}
    Then synchronous_standby_names on postgres0 is set to "ANY 1 (*)" after 10 seconds

  Scenario: check failover with one quorum standby
    Given I start postgres1
    Then sync key in DCS has sync_standby=postgres1 after 10 seconds
    And synchronous_standby_names on postgres0 is set to "ANY 1 (postgres1)" after 2 seconds
    When I shut down postgres0
    Then postgres1 role is the primary after 10 seconds
    And sync key in DCS has quorum=0 after 10 seconds
    Then synchronous_standby_names on postgres1 is set to "ANY 1 (*)" after 10 seconds
    When I start postgres0
    Then sync key in DCS has leader=postgres1 after 10 seconds
    Then sync key in DCS has sync_standby=postgres0 after 10 seconds
    And synchronous_standby_names on postgres1 is set to "ANY 1 (postgres0)" after 2 seconds

  Scenario: check behavior with three nodes and different replication factor
    Given I start postgres2
    Then sync key in DCS has sync_standby=postgres0,postgres2 after 10 seconds
    And sync key in DCS has quorum=1 after 2 seconds
    And synchronous_standby_names on postgres1 is set to "ANY 1 (postgres0,postgres2)" after 2 seconds
    When I issue a PATCH request to http://127.0.0.1:8009/config with {"synchronous_node_count": 2}
    Then sync key in DCS has quorum=0 after 10 seconds
    And synchronous_standby_names on postgres1 is set to "ANY 2 (postgres0,postgres2)" after 2 seconds

  Scenario: switch from quorum replication to good old multisync and back
    Given I issue a PATCH request to http://127.0.0.1:8009/config with {"synchronous_mode": true, "synchronous_node_count": 1}
    And I shut down postgres0
    Then synchronous_standby_names on postgres1 is set to "postgres2" after 10 seconds
    And sync key in DCS has sync_standby=postgres2 after 10 seconds
    Then sync key in DCS has quorum=0 after 2 seconds
    When I issue a PATCH request to http://127.0.0.1:8009/config with {"synchronous_mode": "quorum"}
    And I start postgres0
    Then synchronous_standby_names on postgres1 is set to "ANY 1 (postgres0,postgres2)" after 10 seconds
    And sync key in DCS has sync_standby=postgres0,postgres2 after 10 seconds
    Then sync key in DCS has quorum=1 after 2 seconds

  Scenario: REST API and patronictl
    Given I run patronictl.py list batman
    Then I receive a response returncode 0
    And I receive a response output "Quorum Standby"
    And Status code on GET http://127.0.0.1:8008/quorum is 200 after 3 seconds
    And Status code on GET http://127.0.0.1:8010/quorum is 200 after 3 seconds

  Scenario: nosync node is removed from voters and synchronous_standby_names
    Given I add tag nosync true to postgres2 config
    When I issue an empty POST request to http://127.0.0.1:8010/reload
    Then I receive a response code 202
    And sync key in DCS has quorum=0 after 10 seconds
    And sync key in DCS has sync_standby=postgres0 after 10 seconds
    And synchronous_standby_names on postgres1 is set to "ANY 1 (postgres0)" after 2 seconds
    And Status code on GET http://127.0.0.1:8010/quorum is 503 after 10 seconds
