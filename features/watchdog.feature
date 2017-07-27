Feature: watchdog
  Verify that watchdog gets pinged and triggered under appropriate circumstances.

  Scenario: watchdog is opened, pinged and closed
    Given I start postgres0 with watchdog
    Then postgres0 is a leader after 10 seconds
    And postgres0 role is the primary after 10 seconds
    And postgres0 watchdog has been pinged after 10 seconds
    When I shut down postgres0
    Then postgres0 watchdog has been closed

  #TODO: test watchdog is disabled during pause
  #TODO: test watchdog is disabled properly when shutting down

  Scenario: watchdog is triggered if patroni stops responding
    Given I start postgres0 with watchdog
    Then postgres0 role is the primary after 10 seconds
    When postgres0 hangs for 30 seconds
    Then postgres0 watchdog is triggered after 30 seconds
