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
	When I issue an empty POST request to http://127.0.0.1:8008/reinitialize
	Then I receive a response code 503
	And I receive a response text "I am the leader, can not reinitialize"
	When I issue a POST request to http://127.0.0.1:8008/failover with leader=postgres0
	Then I receive a response code 500
	And I receive a response text "failover is not possible: cluster does not have members except leader"
	When I issue an empty POST request to http://127.0.0.1:8008/failover
	Then I receive a response code 400
	And I receive a response text "No values given for required parameters leader and candidate"

Scenario: check API requests for the primary-replica pair
	Given I start postgres1
	And replication works from postgres0 to postgres1 after 20 seconds
	When I issue a GET request to http://127.0.0.1:8009/replica
	Then I receive a response code 200
	And I receive a response state running
	And I receive a response role replica
	When I issue an empty POST request to http://127.0.0.1:8009/reinitialize
	Then I receive a response code 200
	When I issue an empty POST request to http://127.0.0.1:8008/restart
	Then I receive a response code 200
        And postgres0 role is the primary after 5 seconds
        When I sleep for 10 seconds
        Then postgres1 role is the secondary after 15 seconds

Scenario: check the failover via the API
	Given I issue a POST request to http://127.0.0.1:8008/failover with leader=postgres0,candidate=postgres1
	Then I receive a response code 200
	And postgres1 is a leader after 5 seconds
        And postgres1 role is the primary after 5 seconds
        And postgres0 role is the secondary after 10 seconds
	And replication works from postgres1 to postgres0 after 20 seconds

Scenario: check the scheduled failover
	Given I issue a scheduled failover at http://127.0.0.1:8009 from postgres1 to postgres0 in 1 seconds
	Then I receive a response code 200
	And postgres0 is a leader after 20 seconds
        And postgres0 role is the primary after 5 seconds
        And postgres1 role is the secondary after 10 seconds
	And replication works from postgres0 to postgres1 after 25 seconds

