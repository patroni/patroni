Feature: patroni api
	We should check that patroni correctly responds to valid and not-valid API requests.

Scenario: check API requests on a stand-alone server
	Given I start postgres0
	And postgres0 is a leader after 10 seconds
	When I issue a GET request to http://127.0.0.1:8008/
	Then I receive a response code 200
	And I receive a response state running
	And I receive a response role master
	When I issue a GET request to http://127.0.0.1:8008/standby_leader
	Then I receive a response code 503
	When I issue a GET request to http://127.0.0.1:8008/health
	Then I receive a response code 200
	When I issue a GET request to http://127.0.0.1:8008/replica
	Then I receive a response code 503
	When I issue a POST request to http://127.0.0.1:8008/reinitialize with {"force": true}
	Then I receive a response code 503
	And I receive a response text I am the leader, can not reinitialize
	When I run patronictl.py switchover batman --master postgres0 --force
	Then I receive a response returncode 1
	And I receive a response output "Error: No candidates found to switchover to"
	When I issue a POST request to http://127.0.0.1:8008/switchover with {"leader": "postgres0"}
	Then I receive a response code 412
	And I receive a response text switchover is not possible: cluster does not have members except leader
	When I issue an empty POST request to http://127.0.0.1:8008/failover
	Then I receive a response code 400
	When I issue a POST request to http://127.0.0.1:8008/failover with {"foo": "bar"}
	Then I receive a response code 400
	And I receive a response text "Failover could be performed only to a specific candidate"

Scenario: check local configuration reload
	Given I add tag new_tag new_value to postgres0 config
	And I issue an empty POST request to http://127.0.0.1:8008/reload
	Then I receive a response code 202

Scenario: check dynamic configuration change via DCS
	Given I issue a PATCH request to http://127.0.0.1:8008/config with {"ttl": 20, "postgresql": {"parameters": {"max_connections": "101"}}}
	Then I receive a response code 200
	And Response on GET http://127.0.0.1:8008/patroni contains pending_restart after 11 seconds
	When I issue a GET request to http://127.0.0.1:8008/config
	Then I receive a response code 200
	And I receive a response ttl 20
	When I issue a GET request to http://127.0.0.1:8008/patroni
	Then I receive a response code 200
	And I receive a response tags {'new_tag': 'new_value'}
	And I sleep for 4 seconds

Scenario: check the scheduled restart
	Given I run patronictl.py edit-config -p 'superuser_reserved_connections=6' --force batman
	Then I receive a response returncode 0
	And I receive a response output "+    superuser_reserved_connections: 6"
	And Response on GET http://127.0.0.1:8008/patroni contains pending_restart after 5 seconds
	Given I issue a scheduled restart at http://127.0.0.1:8008 in 5 seconds with {"role": "replica"}
	Then I receive a response code 202
	And I sleep for 8 seconds
	And Response on GET http://127.0.0.1:8008/patroni contains pending_restart after 10 seconds
	Given I issue a scheduled restart at http://127.0.0.1:8008 in 5 seconds with {"restart_pending": "True"}
	Then I receive a response code 202
	And Response on GET http://127.0.0.1:8008/patroni does not contain pending_restart after 10 seconds
	And postgres0 role is the primary after 10 seconds

Scenario: check API requests for the primary-replica pair in the pause mode
	Given I start postgres1
	Then replication works from postgres0 to postgres1 after 20 seconds
	When I run patronictl.py pause batman
	Then I receive a response returncode 0
	When I kill postmaster on postgres1
	And I issue a GET request to http://127.0.0.1:8009/replica
	Then I receive a response code 503
	And "members/postgres1" key in DCS has state=stopped after 10 seconds
	When I run patronictl.py restart batman postgres1 --force
	Then I receive a response returncode 0
	Then replication works from postgres0 to postgres1 after 20 seconds
	And I sleep for 2 seconds
	When I issue a GET request to http://127.0.0.1:8009/replica
	Then I receive a response code 200
	And I receive a response state running
	And I receive a response role replica
	When I run patronictl.py reinit batman postgres1 --force --wait
	Then I receive a response returncode 0
	And I receive a response output "Success: reinitialize for member postgres1"
	And postgres1 role is the secondary after 30 seconds
	And replication works from postgres0 to postgres1 after 20 seconds
	When I run patronictl.py restart batman postgres0 --force
	Then I receive a response returncode 0
	And I receive a response output "Success: restart on member postgres0"
	And postgres0 role is the primary after 5 seconds

Scenario: check the switchover via the API in the pause mode
	Given I issue a POST request to http://127.0.0.1:8008/switchover with {"leader": "postgres0", "candidate": "postgres1"}
	Then I receive a response code 200
	And postgres1 is a leader after 5 seconds
	And postgres1 role is the primary after 10 seconds
	And postgres0 role is the secondary after 10 seconds
	And replication works from postgres1 to postgres0 after 20 seconds
	And "members/postgres0" key in DCS has state=running after 10 seconds
	When I issue a GET request to http://127.0.0.1:8008/primary
	Then I receive a response code 503
	When I issue a GET request to http://127.0.0.1:8008/replica
	Then I receive a response code 200
	When I issue a GET request to http://127.0.0.1:8009/primary
	Then I receive a response code 200
	When I issue a GET request to http://127.0.0.1:8009/replica
	Then I receive a response code 503

Scenario: check the scheduled switchover
	Given I issue a scheduled switchover from postgres1 to postgres0 in 10 seconds
	Then I receive a response returncode 1
	And I receive a response output "Can't schedule switchover in the paused state"
	When I run patronictl.py resume batman
	Then I receive a response returncode 0
	Given I issue a scheduled switchover from postgres1 to postgres0 in 10 seconds
	Then I receive a response returncode 0
	And postgres0 is a leader after 20 seconds
	And postgres0 role is the primary after 10 seconds
	And postgres1 role is the secondary after 10 seconds
	And replication works from postgres0 to postgres1 after 25 seconds
	And "members/postgres1" key in DCS has state=running after 10 seconds
	When I issue a GET request to http://127.0.0.1:8008/primary
	Then I receive a response code 200
	When I issue a GET request to http://127.0.0.1:8008/replica
	Then I receive a response code 503
	When I issue a GET request to http://127.0.0.1:8009/primary
	Then I receive a response code 503
	When I issue a GET request to http://127.0.0.1:8009/replica
	Then I receive a response code 200
