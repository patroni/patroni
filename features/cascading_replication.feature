Feature: cascading replication
	We should check that patroni can do base backup and streaming from the replica

Scenario: check a base backup and streaming replication from a replica
	Given I start postgres0
	And postgres0 is a leader after 10 seconds
	And I configure and start postgres1 with a tag clonefrom true
	And replication works from postgres0 to postgres1 after 20 seconds
	And I create label with "postgres0" in postgres0 data directory
	And I create label with "postgres1" in postgres1 data directory
	And I configure and start postgres2 with a tag replicatefrom postgres1
	Then replication works from postgres0 to postgres2 after 30 seconds
	And there is a label with "postgres1" in postgres2 data directory
