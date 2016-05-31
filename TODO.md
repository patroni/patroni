Failover
========
- When determining who should become master, include the minor version of PostgreSQL in the decision.

Configuration
==============
- Provide a way to change pg_hba.conf of a running cluster on the Patroni level, without changing individual nodes.
- Provide hooks to store and retrieve cluster-wide passwords without exposing them in a plain-text form to unauthorized users.

Documentation
==============
- Document how to run cascading replication and possibly initialize the cluster without an access to the master node.
