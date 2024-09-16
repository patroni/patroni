Feature: custom bootstrap
    We should check that patroni can bootstrap a new cluster from a backup

Scenario: clone existing cluster using pg_basebackup
    Given I start postgres-0
    Then postgres-0 is a leader after 10 seconds
    When I add the table foo to postgres-0
    And I start postgres-1 in a cluster batman1 as a clone of postgres-0
    Then postgres-1 is a leader of batman1 after 10 seconds
    Then table foo is present on postgres-1 after 10 seconds

Scenario: make a backup and do a restore into a new cluster
    Given I add the table bar to postgres-1
    And I do a backup of postgres-1
    When I start postgres-2 in a cluster batman2 from backup
    Then postgres-2 is a leader of batman2 after 30 seconds
    And table bar is present on postgres-2 after 10 seconds
