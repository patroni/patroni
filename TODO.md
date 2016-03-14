Failover
========
- When determining who should become master, include the minor version of PostgreSQL in the decision.
- Create a way to disable governance of a cluster, something like the existence of a "nogover" or "admin" file in PGDATA will stop patroni from changing the cluster state.

Configuration
==============
- Provide a way to change postgresql.conf and pg_hba.conf of a running cluster on the Patroni level, without changing individual nodes.
- Provide hooks to store and retrieve cluster-wide passwords without exposing them in a plain-text form to unauthorized users.
- Implement patronictl command to create initial configuration of the cluster with leader and member keys fixed to the user-supplied values in order to simplify migrations.
- Implement support for consul in addtion to etcd and zookeeper
- Complete zookeeper support in patronictl

Documentation
==============
- Document how to run cascading replication and possibly initialize the cluster without an access to the master node.
