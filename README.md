[![Build Status](https://travis-ci.org/zalando/patroni.svg?branch=master)](https://travis-ci.org/zalando/patroni)
[![Coverage Status](https://coveralls.io/repos/zalando/patroni/badge.svg?branch=master)](https://coveralls.io/r/zalando/patroni?branch=master)
# Patroni: A Template for PostgreSQL HA with ZooKeeper or etcd

*There are many ways to run high availability with PostgreSQL; here we present a template for you to create your own custom fit high availability solution using python and distributed configuration store (like ZooKeeper or etcd) for maximum accessibility.*

## Getting Started
To get started, do the following from different terminals:

```
> etcd --data-dir=data/etcd
> ./patroni.py postgres0.yml
> ./patroni.py postgres1.yml
```

From there, you will see a high-availability cluster start up. Test
different settings in the YAML files to see how behavior changes.  Kill
some of the different components to see how the system behaves.

Add more `postgres*.yml` files to create an even larger cluster.

We provide a haproxy configuration, which will give your application a single endpoint for connecting to the cluster's leader.  To configure, run:

```
> haproxy -f haproxy.cfg
```

```
> psql --host 127.0.0.1 --port 5000 postgres
```

## How Patroni works

For a diagram of the high availability decision loop, see the included a PDF: [postgres-ha.pdf](https://github.com/zalando/patroni/blob/master/postgres-ha.pdf)

## YAML Configuration

For an example file, see `postgres0.yml`.  Below is an explanation of settings:

* *ttl*: the TTL to acquire the leader lock.  Think of it as the length of time before automatic failover process is initiated.
* *loop_wait*: the number of seconds the loop will sleep

* *restapi*
  * *listen*: ip address + port that Patroni will listen to provide health-check information for haproxy.
  * *connect_address*: ip address + port through which restapi is accessible.

* *etcd*
  * *scope*: the relative path used on etcd's http api for this deployment, thus you can run multiple HA deployments from a single etcd
  * *ttl*: the TTL to acquire the leader lock.  Think of it as the length of time before automatic failover process is initiated.
  * *host*: the host:port for the etcd endpoint

* *zookeeper*
  * *scope*: the relative path used on etcd's http api for this deployment, thus you can run multiple HA deployments from a single etcd
  * *session_timeout*: the TTL to acquire the leader lock. Think of it as the length of time before automatic failover process is initiated.
  * *reconnects_timeout*: how long we should try to reconnect to ZooKeeper after connection loss. After this timeout we assume that we don't have lock anymore and will restart in read-only mode.
  * *hosts*: List of ZooKeeper cluster members in format: 'host1:port1,host2:port2,..etc...'


* *postgresql*
  * *name*: the name of the Postgres host, must be unique for the cluster
  * *listen*: ip address + port that Postgres listening. Must be accessible from other nodes in the cluster if using streaming replication.
  * *connect_address*: ip address + port through which Postgres is accessible from other nodes and applications.
  * *data_dir*: file path to initialize and store Postgres data files
  * *maximum_lag_on_failover*: the maximum bytes a follower may lag before it is not eligible become leader
  * *replication*
    * *username*: replication username, user will be created during initialization
    * *password*: replication password, user will be created during initialization
    * *network*: network setting for replication in pg_hba.conf
  * *recovery_conf*: configuration settings written to recovery.conf when configuring follower
  * *parameters*: list of configuration settings for Postgres

## Replication choices

Patroni uses Postgres' streaming replication.  By default, this replication is asynchronous.  For more information, see the [Postgres documentation on streaming replication](http://www.postgresql.org/docs/current/static/warm-standby.html#STREAMING-REPLICATION). 

Patroni's asynchronous replication configuration allows for `maximum_lag_on_failover` settings. This setting ensures failover will not occur if a follower is more than a certain number of bytes behind the follower.  This setting should be increased or decreased based on business requirements.

When asynchronous replication is not best for your use-case, investigate how Postgres's [synchronous replication](http://www.postgresql.org/docs/current/static/warm-standby.html#SYNCHRONOUS-REPLICATION) works.  Synchronous replication ensures consistency across a cluster by confirming that writes are written to a secondary before returning to the connecting client with a success.  The cost of synchronous replication will be reduced throughput on writes.  This throughput will be entirely based on network performance.  In hosted datacenter environments (like AWS, Rackspace, or any network you do not control), synchrous replication increases the variability of write performance significantly.  If followers become inaccessible from the leader, the leader will becomes effectively readonly.

To enable a simple synchronous replication test, add the follow lines to the `parameters` section of your YAML configuration files.

```YAML
    synchronous_commit: "on"
    synchronous_standby_names: "*"
```

When using synchronous replication, use at least a 3-Postgres data nodes to ensure write availability if one host fails.

Choosing your replication schema is dependent on the many business decisions.  Investigate both async and sync replication, as well as other HA solutions, to determine which solution is best for you.

## Applications should not use superusers

When connecting from an application, always use a non-superuser. Patroni requires access to the database to function properly.  By using a superuser from application, you can potentially use the entire connection pool, including the connections reserved for superusers with the `superuser_reserved_connections` setting. If Patroni cannot access the Primary, because the connection pool is full, behavior will be undesireable.

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
