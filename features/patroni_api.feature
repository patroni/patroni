Feature: patroni api
	We should check that patroni correctly responds to valid and not-valid API requests.

Scenario: check API requests on a stand-alone server
	Given I start postgres0
	And postgres0 is a leader after 10 seconds
	When I issue a GET request to http://127.0.0.1:8008/
	Then I receive a response code 200
	And I receive a response state running
	And I receive a response role master
	When I issue a GET request to http://127.0.0.1:8008/replica
	Then I receive a response code 503
	When I run patronictl.py reinit batman postgres0 --force
	Then I receive a response returncode 0
	And I receive a response output "Failed: reinitialize for member postgres0, status code=503, (I am the leader, can not reinitialize)"
	When I run patronictl.py failover batman --master postgres0 --force
	Then I receive a response returncode 1
	And I receive a response output "Error: No candidates found to failover to"
	When I issue a POST request to http://127.0.0.1:8008/failover with {"leader": "postgres0"}
	Then I receive a response code 500
	And I receive a response text failover is not possible: cluster does not have members except leader
	When I issue an empty POST request to http://127.0.0.1:8008/failover
	Then I receive a response code 400
	When I issue a POST request to http://127.0.0.1:8008/failover with {"foo": "bar"}
	Then I receive a response code 400
	And I receive a response text "No values given for required parameters leader and candidate"

Scenario: check local configuration reload
	Given I issue an empty POST request to http://127.0.0.1:8008/reload
	Then I receive a response code 200
	And I receive a response text nothing changed
	When I add tag new_tag new_value to postgres0 config
	And I issue an empty POST request to http://127.0.0.1:8008/reload
	Then I receive a response code 202

Scenario: check dynamic configuration change via DCS
	Given I issue a PATCH request to http://127.0.0.1:8008/config with {"ttl": 20, "loop_wait": 1, "postgresql": {"parameters": {"max_connections": 101}}}
	Then I receive a response code 200
	And I receive a response loop_wait 1
	And Response on GET http://127.0.0.1:8008/patroni contains pending_restart after 11 seconds
	When I issue a GET request to http://127.0.0.1:8008/config
	Then I receive a response code 200
	And I receive a response loop_wait 1
	When I issue a GET request to http://127.0.0.1:8008/patroni
	Then I receive a response code 200
	And I receive a response tags {'tag': 'new_value'}

Scenario: check API requests for the primary-replica pair
	Given I start postgres1
	And replication works from postgres0 to postgres1 after 20 seconds
	When I issue a GET request to http://127.0.0.1:8009/replica
	Then I receive a response code 200
	And I receive a response state running
	And I receive a response role replica
	When I run patronictl.py reinit batman postgres1 --force
	Then I receive a response returncode 0
	And I receive a response output "Success: reinitialize for member postgres1"
	When I run patronictl.py restart batman postgres0 --force
	Then I receive a response returncode 0
	And I receive a response output "Success: restart on member postgres0"
	And postgres0 role is the primary after 5 seconds
	When I sleep for 10 seconds
	Then postgres1 role is the secondary after 15 seconds

Scenario: check the failover via the API
	Given I run patronictl.py failover batman --master postgres0 --candidate postgres1 --force
	Then I receive a response returncode 0
	And postgres1 is a leader after 5 seconds
	And postgres1 role is the primary after 10 seconds
	And postgres0 role is the secondary after 10 seconds
	And replication works from postgres1 to postgres0 after 20 seconds

Scenario: check the scheduled failover
	Given I issue a scheduled failover from postgres1 to postgres0 in 1 seconds
	Then I receive a response returncode 0
	And postgres0 is a leader after 20 seconds
	And postgres0 role is the primary after 10 seconds
	And postgres1 role is the secondary after 10 seconds
	And replication works from postgres0 to postgres1 after 25 seconds

Scenario: check the scheduled restart
	Given I issue a PATCH request to http://127.0.0.1:8008/config with {"postgresql": {"parameters": {"superuser_reserved_connections": "6"}}}
	Then I receive a response code 200
		And Response on GET http://127.0.0.1:8008/patroni contains pending_restart after 5 seconds
	Given I issue a scheduled restart at http://127.0.0.1:8008 in 1 seconds with {"role": "replica"}
	Then I receive a response code 202
		And I sleep for 10 seconds
		And Response on GET http://127.0.0.1:8008/patroni contains pending_restart after 10 seconds
	Given I issue a scheduled restart at http://127.0.0.1:8008 in 1 seconds with {"restart_pending": "True"}
	Then I receive a response code 202
		And Response on GET http://127.0.0.1:8008/patroni does not contain pending_restart after 10 seconds

