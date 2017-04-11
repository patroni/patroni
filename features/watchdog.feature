Feature: watchdog
  Verify that watchdog gets pinged and triggered under appropriate circumstances.

  Scenario: watchdog is opened, pinged and closed
    Given I start postgres0 with watchdog
    Then postgres0 is a leader after 10 seconds
    And postgres0 role is the primary after 10 seconds
    And postgres0 watchdog has been pinged after 10 seconds
    When I shut down postgres0
    Then postgres0 watchdog has been closed

  Scenario: watchdog is updated during pause
    Given I start postgres0 with watchdog
    Then postgres0 role is the primary after 10 seconds
    When I run patronictl.py pause batman
    And I wait for next postgres0 watchdog ping
    Then I receive a response returncode 0
    And postgres0 watchdog has been pinged after 10 seconds
    When I shut down postgres0
    Then postgres0 watchdog has been closed
    And postgres0 database is running

  Scenario: watchdog is updated during shutdown checkpoint
    Given I start postgres0 with watchdog
    Then postgres0 role is the primary after 10 seconds
    And Sleep for 10 seconds
    Given I run patronictl.py resume batman
    Then I receive a response returncode 0
    When I start postgres1
    Then postgres1 role is the secondary after 10 seconds
    When postgres0 checkpoint takes 30 seconds
    And I shut down postgres0
    Then postgres0 watchdog was not triggered

  Scenario: watchdog is triggered if postgres stops responding
    Given I start postgres0 with watchdog
    Then postgres0 role is the secondary after 10 seconds
    When I shut down postgres1
    Then postgres0 role is the primary after 10 seconds
    And I start postgres1
    When postgres0 hangs for 30 seconds
    And I terminate postgres0 user processes
    Then postgres0 watchdog is triggered after 30 seconds
