Feature: basic failover
	In order to check that failover works
	As observers
	We start the primary and the replica,
	shut down the primary
	and check that the replica assumed the primary role.

Scenario: check the basic failover
	Given basic replication
	When I shut down postgres0
	Then postgres1 role is the primary after 10 seconds
	When I start postgres0
	Then postgres0 role is the secondary after 10 seconds