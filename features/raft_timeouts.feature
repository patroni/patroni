Feature: raft pysyncobj timeouts
  Custom pysyncobj timeout parameters configured under the `raft` section must be
  accepted at startup and must not prevent cluster formation or replication. This
  guards the configurable-timeouts feature (the DCS layer of issue #3550) against
  regressions that only surface with a real Raft DCS. It is intentionally
  latency-free so it stays deterministic in CI.

  Scenario: cluster forms and replicates with custom raft timeouts
    Given I start postgres-0 with custom raft timeouts
    Then postgres-0 is a leader after 20 seconds
    And there is one of ["Applying custom pysyncobj timeouts"] INFO in the postgres-0 patroni log after 10 seconds
    When I start postgres-1 with custom raft timeouts
    And I start postgres-2 with custom raft timeouts
    And I add the table foo to postgres-0
    Then table foo is present on postgres-1 after 30 seconds
    And table foo is present on postgres-2 after 30 seconds
