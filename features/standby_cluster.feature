Feature: standby cluster

  Scenario: check replication of a single table in a standby cluster
    Given I start postgres0
    And I add the table foo to postgres0
    And I start postgres1 in a standby cluster batman1 as a clone of postgres0
    Then postgres1 is a leader of batman1 after 10 seconds
    And postgres1 role is the standby_leader after 24 seconds
    And table foo is present on postgres1 after 20 seconds
    When I start postgres2 in a cluster batman1
    Then postgres2 role is the replica after 24 seconds
    And table foo is present on postgres2 after 20 seconds

  Scenario: check failover
    When I kill postmaster on postgres1
    Then postgres2 role is the standby_leader after 24 seconds
