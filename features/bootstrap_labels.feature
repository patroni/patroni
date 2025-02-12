Feature: bootstrap labels
  Check that user-configurable bootstrap labels are set and removed with state change

Scenario: check label for cluster bootstrap
    When I start postgres-0
    Then postgres-0 is a leader after 10 seconds
    When I start postgres-1 in a cluster batman1 as a long-running clone of postgres-0
    Then "members/postgres-1" key in DCS has state=running custom bootstrap script after 20 seconds
    And postgres-1 is labeled with "foo"
    And postgres-1 is a leader of batman1 after 20 seconds

Scenario: check label for replica bootstrap
    When I do a backup of postgres-1
    And I start postgres-2 in cluster batman1 using long-running backup_restore
    Then "members/postgres-2" key in DCS has state=creating replica after 20 seconds
    And postgres-2 is labeled with "foo"

Scenario: check bootstrap label is removed
    Given "members/postgres-1" key in DCS has state=running after 2 seconds
    And  "members/postgres-2" key in DCS has state=running after 20 seconds
    Then postgres-1 is not labeled with "foo"
    And postgres-2 is not labeled with "foo"
