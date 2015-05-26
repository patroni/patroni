Failover
========
- When determining who should become master, include the minor version of PostgreSQL in the decision
- Create a way to disable governance of a cluster, something like the existence of a "nogover" or "admin" file in PGDATA will stop governor from changing the cluster state
