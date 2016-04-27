|Build Status| |Coverage Status|

Patroni: A Template for PostgreSQL HA with ZooKeeper, etcd or Consul
------------------------------------------------------------

Patroni was previously known as Governor.

*There are many ways to run high availability with PostgreSQL. Here, we
present a template for you to create your own customized, high-availability 
solution using Python and — for maximum accessibility — a distributed 
configuration store like ZooKeeper, etcd or Consul.*

Getting Started
---------------

To get started, do the following from different terminals:

::

    > etcd --data-dir=data/etcd
    > ./patroni.py postgres0.yml
    > ./patroni.py postgres1.yml

From there, you will see a high-availability cluster start up. Test
different settings in the YAML files to see how its behavior changes. Kill
some of the components to see how the system behaves.

Add more ``postgres*.yml`` files to create an even larger cluster.

We provide a haproxy configuration, which will give your application a
single endpoint for connecting to the cluster's leader. To configure,
run:

::

    > haproxy -f haproxy.cfg

::

    > psql --host 127.0.0.1 --port 5000 postgres

How Patroni Works
-----------------

For a diagram of the high availability decision loop, review this PDF:
`postgres-ha.pdf <https://github.com/zalando/patroni/blob/master/postgres-ha.pdf>`__

YAML Configuration
------------------

For an example file, see ``postgres0.yml``. Regarding settings:

-  *ttl*: the TTL to acquire the leader lock. Think of it as the length of time before initiation of the automatic failover process.
-  *loop\_wait*: the number of seconds the loop will sleep

-  *restapi*:
    -  *listen*: IP address + port that Patroni will listen to, to provide health-check information for haproxy.
    -  *connect\_address*: IP address + port through which restapi is accessible.
    -  *auth*: (optional) 'username:password' to protect dangerous REST API endpoints.
    -  *certfile*: (optional) Specifies a file with the certificate in the PEM format. If the certfile is not specified or is left empty, the API server will work without SSL.
    -  *keyfile*: (optional) Specifies a file with the secret key in the PEM format.

-  *etcd*:
    -  *scope*: the relative path used on etcd's HTTP API for this deployment; makes it possible to run multiple HA deployments from a single etcd cluster.
    -  *ttl*: the TTL to acquire the leader lock. Think of it as the length of time before initiation of the automatic failover process.
    -  *host*: the host:port for the etcd endpoint.

-  *consul*:
    -  *scope*: the relative path used on Consul's HTTP API for this deployment; makes it possible to run multiple HA deployments from a single Consul cluster.
    -  *ttl*: the TTL to acquire the leader lock. Think of it as the length of time before initiation of the automatic failover process.
    -  *host*: the host:port for the Consul endpoint.

-  *zookeeper*:
    -  *scope*: the relative path used on ZooKeeper for this deployment; makes it possible to run multiple HA deployments from a single ZooKeeper cluster.
    -  *session\_timeout*: the TTL to acquire the leader lock. Think of it as the length of time before initiation of the automatic failover process.
    -  *reconnect\_timeout*: how long we should try to reconnect to ZooKeeper after a connection loss. After this timeout, assume that you no longer have a lock and restart in read-only mode.
    -  *hosts*: list of ZooKeeper cluster members in format: ['host1:port1', 'host2:port2', 'etc...']
    -  *exhibitor*: if you are running a ZooKeeper cluster under the Exhibitor supervisory, the following section might interest you:
        -  *poll\_interval*: how often the list of ZooKeeper and Exhibitor nodes should be updated from Exhibitor
        -  *port*: Exhibitor port.
        -  *hosts*: initial list of Exhibitor (ZooKeeper) nodes in format: ['host1', 'host2', 'etc...' ]. This list updates automatically whenever the Exhibitor (ZooKeeper) cluster topology changes.

-  *postgresql*:
    -  *name*: the name of the Postgres host. Must be unique for the cluster.
    -  *listen*: IP address + port that Postgres listens to; must be accessible from other nodes in the cluster, if you're using streaming replication. Multiple comma-separated addresses are permitted, as long as the port component is appended after to the last one with a colon, i.e. ``listen: 127.0.0.1,127.0.0.2:5432``. The first address from this list will be used by Patroni to establish local connections to the PostgreSQL node. 
    
    -  *connect\_address*: IP address + port through which Postgres is accessible from other nodes and applications.
    -  *data\_dir*: file path to initialize and store Postgres data files.
    -  *maximum\_lag\_on\_failover*: the maximum bytes a follower may lag.
    -  *use\_slots*: whether or not to use replication_slots. Must be False for PostgreSQL 9.3. You should comment out max_replication_slots before it becomes ineligible for leader status.

    -  *initdb*:  List options to be passed on to initdb
        -  *encoding*: default encoding for new databases
        -  *locale*: default locale for new databases
        -  *data-checksums*  # When pg_rewind is needed on 9.3, this needs to be enabled

    -  *pg\_hba*: list of lines which should be added to pg\_hba.conf.
        -  *- host all all 0.0.0.0/0 md5*.
        -  *- host replication replicator 127.0.0.1/32 md5* # A line like this is required for replication

    -  *replication*:
        -  *username*: replication username; user will be created during initialization.
        -  *password*: replication password; user will be created during initialization.

    -  *callbacks* callback scripts to run on certain actions. Patroni will pass the action, role and cluster name. See scripts/aws.py as an example on how to write them.
        -  *on\_start*: a script to run when the cluster starts.
        -  *on\_stop*: a script to run when the cluster stops.
        -  *on\_restart*: a script to run when the cluster restarts.
        -  *on\_reload*: a script to run when configuration reload is triggered.
        -  *on\_role\_change*: a script to run when the cluster is being promoted or demoted.

    -  *superuser*:
        -  *password*: password for the Postgres user, set during initialization.

    -  *admin*:
        -  *username*: admin username; user is created during initialization. It will have CREATEDB and CREATEROLE privileges.
        -  *password*: admin password; user is created during initialization.

    -  *recovery\_conf*: additional configuration settings written to recovery.conf when configuring follower.
        -  *parameters*: list of configuration settings for Postgres.  Many of these are required for replication to work.

    -  *create\_replica\_methods*: an ordered list of the create methods for turning a patroni node into a new replica.
       "basebackup" is the default method; other methods are assumed to refer to scripts, each of which is configured
       as its own config item.

    -  *replica\_method* for each create_replica_method other than basebackup, you would add a configuration section
       of the same name.  At a minimum, this should include "command" with a full path to the actual script to be
       executed.  Other configuration parameters will be passed along to the script in the form "parameter=value".

Replication Choices
-------------------

Patroni uses Postgres' streaming replication. By default, this
replication is asynchronous. For more information, see the `Postgres
documentation on streaming
replication <http://www.postgresql.org/docs/current/static/warm-standby.html#STREAMING-REPLICATION>`__.

Patroni's asynchronous replication configuration allows for
``maximum_lag_on_failover`` settings. This setting ensures failover will
not occur if a follower is more than a certain number of bytes behind
the follower. This setting should be increased or decreased based on
business requirements.

When asynchronous replication is not optimal for your use case, investigate
how Postgres's `synchronous
replication <http://www.postgresql.org/docs/current/static/warm-standby.html#SYNCHRONOUS-REPLICATION>`__
works. Synchronous replication ensures consistency across a cluster by
confirming that writes are written to a secondary before returning to
the connecting client with a success. The cost of synchronous
replication: reduced throughput on writes. This throughput will
be entirely based on network performance. In hosted datacenter
environments (like AWS, Rackspace, or any network you do not control),
synchrous replication significantly increases the variability of write 
performance. If followers become inaccessible from the leader, the
leader effectively becomes readonly.

To enable a simple synchronous replication test, add the follow lines to
the ``parameters`` section of your YAML configuration files:

.. code:: YAML

        synchronous_commit: "on"
        synchronous_standby_names: "*"

When using synchronous replication, use at least three Postgres data nodes
to ensure write availability if one host fails.

Choosing your replication schema is dependent on your business
considerations. Investigate both async and sync replication, as well as other
HA solutions, to determine which solution is best for you.

Applications Should Not Use Superusers
--------------------------------------

When connecting from an application, always use a non-superuser. Patroni
requires access to the database to function properly. By using a
superuser from an application, you can potentially use the entire
connection pool, including the connections reserved for superusers with
the ``superuser_reserved_connections`` setting. If Patroni cannot access
the Primary because the connection pool is full, behavior will be
undesireable.

Requirements on a Mac
---------------------

Run the following on a Mac to install requirements:

::

    brew install postgresql etcd haproxy libyaml python
    pip install psycopg2 pyyaml

Notice
------

There are many different ways to do HA with PostgreSQL: See `the
PostgreSQL
documentation <https://wiki.postgresql.org/wiki/Replication,_Clustering,_and_Connection_Pooling>`__
for a complete list.

We call Patroni a "template" because it is far from being a one-size-fits-all
or plug-and-play replication system. It will have its own caveats. Use wisely.

.. |Build Status| image:: https://travis-ci.org/zalando/patroni.svg?branch=master
   :target: https://travis-ci.org/zalando/patroni
.. |Coverage Status| image:: https://coveralls.io/repos/zalando/patroni/badge.svg?branch=master
   :target: https://coveralls.io/r/zalando/patroni?branch=master
