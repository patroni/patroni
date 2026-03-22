Feature: frozen patroni
  Verify reaction of watchdog and other Patroni nodes when primary is frozen.

  Scenario: watchdog is opened and pinged
    Given I start postgres-0 with watchdog
    Then postgres-0 is a leader after 10 seconds
    And postgres-0 role is the primary after 10 seconds
    And postgres-0 watchdog has been pinged after 10 seconds
    And postgres-0 watchdog has a 16 second timeout after 10 seconds

  Scenario: watchdog is reconfigured after global ttl changed
    Given I run patronictl.py edit-config batman -s ttl=20 --force
    Then I receive a response returncode 0
    And I receive a response output "+ttl: 20"
    And Response on GET http://127.0.0.1:8008/config contains ttl=20 after 10 seconds
    And postgres-0 watchdog has a 15 second timeout after 10 seconds

  Scenario: watchdog is disabled during pause
    Given I run patronictl.py pause batman
    Then I receive a response returncode 0
    Then postgres-0 watchdog has been closed after 2 seconds

  Scenario: watchdog is opened and pinged after resume
    Given I reset postgres-0 watchdog state
    And I run patronictl.py resume batman
    Then I receive a response returncode 0
    And postgres-0 watchdog has been pinged after 10 seconds

  Scenario: watchdog is disabled when shutting down
    Given I shut down postgres-0
    Then postgres-0 watchdog has been closed after 2 seconds

  Scenario: prepare two nodes cluster
    Given I reset postgres-0 watchdog state
    And I start postgres-0
    And I start postgres-1
    # we need to have loop_wait a few times bigger than polling interval=1s, otherwise test will be flaky
    When I issue a PATCH request to http://127.0.0.1:8008/config with {"primary_race_backoff": 6, "loop_wait": 3, "retry_timeout": 8}
    Then I receive a response code 200
    And postgres-0 role is the primary after 10 seconds
    Then replication works from postgres-0 to postgres-1 after 15 seconds

  Scenario: watchdog is triggered if patroni primary stops responding
    Given postgres-0 hangs for 60 seconds
    Then postgres-0 watchdog is triggered after 20 seconds

  Scenario: check primary race backoff
    Given primary race backoff is triggered on postgres-1 in 12 seconds because postgres-0 is active
    And postgres-1 is promoted to primary in 20 seconds despite postgres-0 being active
