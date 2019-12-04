Feature: custom bootstrap
    We should check that patroni can bootstrap a new cluster from a backup

Scenario: clone existing cluster using pg_basebackup
    Given I start postgres0
    Then postgres0 is a leader after 10 seconds
    When I add the table foo to postgres0
    And I start postgres1 in a cluster batman1 as a clone of postgres0
    Then postgres1 is a leader of batman1 after 10 seconds
    Then table foo is present on postgres1 after 10 seconds

Scenario: make a backup and do a restore into a new cluster
    Given I add the table bar to postgres1
    And I do a backup of postgres1
    When I start postgres2 in a cluster batman2 from backup
    Then postgres2 is a leader of batman2 after 30 seconds
    And table bar is present on postgres2 after 10 seconds
