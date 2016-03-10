Feature: cascading replication
	We should check that patroni can do base backup and streaming from the replica

Scenario: check a base backup from the replica
	Given I start postgres0
	And I start postgres1
	And replication works from postgres0 to postgres1 after 15 seconds
	And I create label with "postgres0" in postgres0 data directory
	And I create label with "postgres1" in postgres1 data directory
	And I configure and start postgres2 with a tag clonefrom postgres1
	Then replication works from postgres0 to postgres2 after 30 seconds
	And there is a label with "postgres0" in postgres2 data directory
