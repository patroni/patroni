Feature: cascading replication
	We should check that patroni can do base backup and streaming from the replica

Scenario: check a base backup and streaming replication from a replica
	Given I start Postgres-0
	And Postgres-0 is a leader after 10 seconds
	And I configure and start Postgres-1 with a tag clonefrom true
	And replication works from Postgres-0 to Postgres-1 after 20 seconds
	And I create label with "Postgres-0" in Postgres-0 data directory
	And I create label with "Postgres-1" in Postgres-1 data directory
	And "members/Postgres-1" key in DCS has state=running after 12 seconds
	And I configure and start Postgres-2 with a tag replicatefrom Postgres-1
	Then replication works from Postgres-0 to Postgres-2 after 30 seconds
	And there is a label with "Postgres-1" in Postgres-2 data directory
