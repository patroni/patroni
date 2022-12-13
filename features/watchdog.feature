Feature: watchdog
  Verify that watchdog gets pinged and triggered under appropriate circumstances.

  Scenario: watchdog is opened and pinged
    Given I start postgres0 with watchdog
    Then postgres0 is a leader after 10 seconds
    And postgres0 role is the primary after 10 seconds
    And postgres0 watchdog has been pinged after 10 seconds
    And postgres0 watchdog has a 15 second timeout

  Scenario: watchdog is reconfigured after global ttl changed
    Given I run patronictl.py edit-config batman -s ttl=30 --force
    Then I receive a response returncode 0
    And I receive a response output "+ttl: 30"
    When I sleep for 4 seconds
    Then postgres0 watchdog has a 25 second timeout

  Scenario: watchdog is disabled during pause
    Given I run patronictl.py pause batman
    Then I receive a response returncode 0
    When I sleep for 2 seconds
    Then postgres0 watchdog has been closed

  Scenario: watchdog is opened and pinged after resume
    Given I reset postgres0 watchdog state
    And I run patronictl.py resume batman
    Then I receive a response returncode 0
    And postgres0 watchdog has been pinged after 10 seconds

  Scenario: watchdog is disabled when shutting down
    Given I shut down postgres0
    Then postgres0 watchdog has been closed

  Scenario: watchdog is triggered if patroni stops responding
    Given I reset postgres0 watchdog state
    And I start postgres0 with watchdog
    Then postgres0 role is the primary after 10 seconds
    When postgres0 hangs for 30 seconds
    Then postgres0 watchdog is triggered after 30 seconds
