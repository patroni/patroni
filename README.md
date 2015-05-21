[![Build Status](https://travis-ci.org/zalando/governor.svg?branch=master)](https://travis-ci.org/zalando/governor)
[![Coverage Status](https://coveralls.io/repos/zalando/governor/badge.svg?branch=master)](https://coveralls.io/r/zalando/governor?branch=master)
# Governor: A Template for PostgreSQL HA with etcd

*There are many ways to run high availability with PostgreSQL; here we present a template for you to create your own custom fit high availability solution using etcd and python for maximum accessibility.*

Compose runs a a [Postgresql as a service platform](https://www.compose.io/postgresql), which is highly-available from creation.  This is a coded example from our prior blog post: [High Availability for PostgreSQL, Batteries Not Included](https://blog.compose.io/high-availability-for-postgresql-batteries-not-included/).

## Getting Started
To get started, do the following from different terminals:

```
> etcd --data-dir=data/etcd
> ./governor.py postgres0.yml
> ./governor.py postgres1.yml
```

From there, you will see a high-availability cluster start up. Test
different settings in the YAML files to see how behavior changes.  Kill
some of the different components to see how the system behaves.

Add more `postgres*.yml` files to create an even larger cluster.

We provide a haproxy configuration, which will give your application a single endpoint for connecting to the cluster's leader.  To configure, run:

```
> haproxy -f haproxy.cfg
> sh haproxy_status.sh 127.0.0.1 5432 15432
> sh haproxy_status.sh 127.0.0.1 5433 15433
```

```
> psql --host 127.0.0.1 --port 5000 postgres
```

## How Governor works

For a diagram of the high availability decision loop, see the included a PDF: [postgres-ha.pdf](https://github.com/compose/template-etcd-based-postgres-ha/blob/master/postgres-ha.pdf)

## YAML Configuration

For an example file, see `postgres0.yml`.  Below is an explanation of settings:

* *loop_wait*: the number of seconds the loop will sleep

* *etcd*
  * *scope*: the relative path used on etcd's http api for this deployment, thus you can run multiple HA deployments from a single etcd
  * *ttl*: the TTL to acquire the leader lock.  Think of it as the length of time before automatic failover process is initiated.
  * *host*: the host:port for the etcd endpoint

* *postgresql*
  * *name*: the name of the Postgres host, must be unique for the cluster
  * *listen*: ip address + port that Postgres listening. Must be accessible from other nodes in the cluster if using streaming replication.
  * *data_dir*: file path to initialize and store Postgres data files
  * *maximum_lag_on_failover*: the maximum bytes a follower may lag before it is not eligible become leader
  * *replication*
    * *username*: replication username, user will be created during initialization
    * *password*: replication password, user will be created during initialization
    * *network*: network setting for replication in pg_hba.conf
  * *recovery_conf*: configuration settings written to recovery.conf when configuring follower
  * *parameters*: list of configuration settings for Postgres

## Replication choices

Governor uses Postgres' streaming replication.  By default, this replication is asynchronous.  For more information, see the [Postgres documentation on streaming replication](http://www.postgresql.org/docs/current/static/warm-standby.html#STREAMING-REPLICATION). 

Governor's asynchronous replication configuration allows for `maximum_lag_on_failover` settings. This setting ensures replication will not occur if a follower is more than a certain number of bytes behind the follower.  This setting should be increased or decreased based on business requirements.

When asynchronous replication is not best for your use-case, investigate how Postgres's [synchronous replication](http://www.postgresql.org/docs/current/static/warm-standby.html#SYNCHRONOUS-REPLICATION) works.  Synchronous replication ensures consistency across a cluster by confirming that writes are written to a secondary before returning to the connecting client with a success.  The cost of synchronous replication will be reduced throughput on writes.  This throughput will be entirely based on network performance.  In hosted datacenter environments (like AWS, Rackspace, or any network you do not control), synchrous replication increases the variability of write performance significantly.  If followers become inaccessible from the leader, the leader will becomes effectively readonly.

To enable a simple synchronous replication test, add the follow lines to the `parameters` section of your YAML configuration files.

```YAML
    synchronous_commit: "on"
    synchronous_standby_names: "*"
```

When using synchronous replication, use at least a 3-Postgres data nodes to ensure write availability if one host fails.

Choosing your replication schema is dependent on the many business decisions.  Investigate both async and sync replication, as well as other HA solutions, to determine which solution is best for you.

## Applications should not use superusers

When connecting from an application, always use a non-superuser. Governor requires access to the database to function properly.  By using a superuser from application, you can potentially use the entire connection pool, including the connections reserved for superusers with the `superuser_reserved_connections` setting. If Governor cannot access the Primary, because the connection pool is full, behavior will be undesireable.

## Requirements on a Mac

Run the following on a Mac to install requirements:

```
brew install postgresql etcd haproxy libyaml python
pip install psycopg2 pyyaml
```

## Notice

There are many different ways to do HA with PostgreSQL, see [the
PostgreSQL documentation](https://wiki.postgresql.org/wiki/Replication,_Clustering,_and_Connection_Pooling) for a complete list.

We call this project a "template" because it is far from a one-size fits
all, or a plug-and-play replication system.  It will have it's own
caveats.  Use wisely.
