|Build Status| |Coverage Status|

Patroni: A Template for PostgreSQL HA with ZooKeeper, etcd or Consul
------------------------------------------------------------
There are many ways to run high availability with PostgreSQL; for a list, see the `PostgreSQL Documentation <https://wiki.postgresql.org/wiki/Replication,_Clustering,_and_Connection_Pooling>`__.

Patroni is a template for you to create your own customized, high-availability solution using Python and — for maximum accessibility — a distributed configuration store like `ZooKeeper <https://zookeeper.apache.org/>`__, `etcd <https://github.com/coreos/etcd>`__ or `Consul <https://github.com/hashicorp/consul>`__. Database engineers, DBAs, DevOps engineers, and SREs who are looking to quickly deploy HA PostgreSQL in the datacenter—or anywhere else—will hopefully find it useful.

We call Patroni a "template" because it is far from being a one-size-fits-all or plug-and-play replication system. It will have its own caveats. Use wisely.

**Note to Kubernetes users**: We're currently developing Patroni to be as useful as possible for teams running Kubernetes on top of Google Compute Engine; Patroni can be the HA solution for Postgres in such an environment. Please contact us via our Issues Tracker if this describes your team's current setup, and we'll follow up.

.. contents::
    :local:
    :depth: 1
    :backlinks: none

==============
How Patroni Works
==============

Patroni originated as a fork of `Governor <https://github.com/compose/governor>`__, the project from Compose. It includes plenty of new features. 

For an example of a Docker-based deployment with Patroni, see `Spilo <https://github.com/zalando/spilo>`__, currently in use at Zalando.

For additional background info, see:

* `PostgreSQL HA with Kubernetes and Patroni <https://www.youtube.com/watch?v=iruaCgeG7qs>`__, talk by Josh Berkus at KubeCon 2016 (video)
* `Feb. 2016 Zalando Tech blog post <https://tech.zalando.de/blog/zalandos-patroni-a-template-for-high-availability-postgresql/>`__

================
Development Status
================

Patroni is in active development and accepts contributions. See our `Contributing <https://github.com/zalando/patroni/blob/master/README.rst#contributing>`__ section below for more details. 

===========================
Technical Requirements/Installation
===========================

**For Mac**

To install requirements on a Mac, run the following:

::

    brew install postgresql etcd haproxy libyaml python
    pip install psycopg2 pyyaml

===================
Running and Configuring
===================

To get started, do the following from different terminals:
::

    > etcd --data-dir=data/etcd
    > ./patroni.py postgres0.yml
    > ./patroni.py postgres1.yml

You will then see a high-availability cluster start up. Test different settings in the YAML files to see how the cluster’s behavior changes. Kill some of the components to see how the system behaves.

Add more ``postgres*.yml`` files to create an even larger cluster.

Patroni provides an `HAProxy <http://www.haproxy.org/>`__ configuration, which will give your application a single endpoint for connecting to the cluster's leader. To configure,
run:

::

    > haproxy -f haproxy.cfg

::

    > psql --host 127.0.0.1 --port 5000 postgres

===============
YAML Configuration
===============

Go `here <https://github.com/zalando/patroni/blob/master/docs/SETTINGS.rst>`__ for comprehensive information about settings for etcd, consul, and ZooKeeper. And for an example, see `postgres0.yml <https://github.com/zalando/patroni/blob/master/postgres0.yml>`__. 

=========================
Environment Configuration
=========================

Go `here <https://github.com/zalando/patroni/blob/master/docs/ENVIRONMENT.rst>`__ for comprehensive information about configuring(overriding) settings via environment variables.

===============
Replication Choices
===============

Patroni uses Postgres' streaming replication, which is asynchronous by default. For more information, see the `Postgres documentation on streaming replication <http://www.postgresql.org/docs/current/static/warm-standby.html#STREAMING-REPLICATION>`__.

Patroni's asynchronous replication configuration allows for ``maximum_lag_on_failover`` settings. This setting ensures failover will not occur if a follower is more than a certain number of bytes behind the follower. This setting should be increased or decreased based on business requirements.

When asynchronous replication is not optimal for your use case, investigate Postgres's `synchronous replication <http://www.postgresql.org/docs/current/static/warm-standby.html#SYNCHRONOUS-REPLICATION>`__. Synchronous replication ensures consistency across a cluster by confirming that writes are written to a secondary before returning to the connecting client with a success. The cost of synchronous replication: reduced throughput on writes. This throughput will be entirely based on network performance. 

In hosted datacenter environments (like AWS, Rackspace, or any network you do not control), synchronous replication significantly increases the variability of write performance. If followers become inaccessible from the leader, the leader effectively becomes read-only.

To enable a simple synchronous replication test, add the follow lines to the ``parameters`` section of your YAML configuration files:

.. code:: YAML

        synchronous_commit: "on"
        synchronous_standby_names: "*"

When using synchronous replication, use at least three Postgres data nodes to ensure write availability if one host fails.

Choosing your replication schema is dependent on your business considerations. Investigate both async and sync replication, as well as other HA solutions, to determine which solution is best for you.

===============================
Applications Should Not Use Superusers
===============================

When connecting from an application, always use a non-superuser. Patroni requires access to the database to function properly. By using a superuser from an application, you can potentially use the entire connection pool, including the connections reserved for superusers, with the ``superuser_reserved_connections`` setting. If Patroni cannot access the Primary because the connection pool is full, behavior will be undesirable.

================
Contributing
================
Patroni accepts contributions from the open-source community; see the `Issues Tracker <https://github.com/zalando/patroni/issues>`__ for current needs. 

Before making a contribution, please let us know by posting a comment to the relevant issue. 
If you would like to propose a new feature, please first file a new issue explaining the feature you’d like to create.

.. |Build Status| image:: https://travis-ci.org/zalando/patroni.svg?branch=master
   :target: https://travis-ci.org/zalando/patroni
.. |Coverage Status| image:: https://coveralls.io/repos/zalando/patroni/badge.svg?branch=master
   :target: https://coveralls.io/r/zalando/patroni?branch=master
