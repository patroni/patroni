Feature: site awareness
  Scenario: setup a mutisite cluster
    When I start postgres-0 in site dc1
    And postgres-0 is a leader after 10 seconds
    And there is a non empty initialize key in DCS after 15 seconds
    And "members/postgres-0" key in DCS has site=dc1 after 5 seconds
    And I start postgres-1 in site dc1 with failover priority 2, sync priority 2
    And I start postgres-2 in site dc2 with failover priority 3, sync priority 3
    And I start postgres-3 in site dc2 with failover priority 4, sync priority 4
    Then "members/postgres-1" key in DCS has site=dc1 after 5 seconds
    And "members/postgres-2" key in DCS has site=dc2 after 5 seconds
    And "members/postgres-3" key in DCS has site=dc2 after 5 seconds
    And "status" key in DCS has dc1 in current_site

  Scenario: test site-aware reinit
    When I run patronictl.py reinit batman postgres-2 --force
    Then I receive a response returncode 0
    And there is one of ["bootstrapped from replica 'postgres-3'"] INFO in the postgres-2 patroni log after 25 seconds
