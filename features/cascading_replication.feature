Feature: cascading replication
	We should check that patroni can do base backup and streaming from the replica

Scenario: check a base backup and streaming replication from a replica
	Given I start postgres-0
	And postgres-0 is a leader after 10 seconds
	And I configure and start postgres-1 with a tag clonefrom true
	And replication works from postgres-0 to postgres-1 after 20 seconds
	And I create label with "postgres-0" in postgres-0 data directory
	And I create label with "postgres-1" in postgres-1 data directory
	And "members/postgres-1" key in DCS has state=running after 12 seconds
	And I configure and start postgres-2 with a tag replicatefrom postgres-1
	Then replication works from postgres-0 to postgres-2 after 30 seconds
	And there is a label with "postgres-1" in postgres-2 data directory
