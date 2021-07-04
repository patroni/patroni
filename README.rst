|Tests Status| |Coverage Status|

Patroni: A Template for PostgreSQL HA with ZooKeeper, etcd or Consul
--------------------------------------------------------------------

You can find a version of this documentation that is searchable and also easier to navigate at `patroni.readthedocs.io <https://patroni.readthedocs.io>`__.


There are many ways to run high availability with PostgreSQL; for a list, see the `PostgreSQL Documentation <https://wiki.postgresql.org/wiki/Replication,_Clustering,_and_Connection_Pooling>`__.

Patroni is a template for you to create your own customized, high-availability solution using Python and - for maximum accessibility - a distributed configuration store like `ZooKeeper <https://zookeeper.apache.org/>`__, `etcd <https://github.com/coreos/etcd>`__, `Consul <https://github.com/hashicorp/consul>`__ or `Kubernetes <https://kubernetes.io>`__. Database engineers, DBAs, DevOps engineers, and SREs who are looking to quickly deploy HA PostgreSQL in the datacenter-or anywhere else-will hopefully find it useful.

We call Patroni a "template" because it is far from being a one-size-fits-all or plug-and-play replication system. It will have its own caveats. Use wisely.

Currently supported PostgreSQL versions: 9.3 to 13.

**Note to Kubernetes users**: Patroni can run natively on top of Kubernetes. Take a look at the `Kubernetes <https://github.com/zalando/patroni/blob/master/docs/kubernetes.rst>`__ chapter of the Patroni documentation.

.. contents::
    :local:
    :depth: 1
    :backlinks: none

=================
How Patroni Works
=================

Patroni originated as a fork of `Governor <https://github.com/compose/governor>`__, the project from Compose. It includes plenty of new features.

For an example of a Docker-based deployment with Patroni, see `Spilo <https://github.com/zalando/spilo>`__, currently in use at Zalando.

For additional background info, see:

* `Elephants on Automatic: HA Clustered PostgreSQL with Helm <https://www.youtube.com/watch?v=CftcVhFMGSY>`_, talk by Josh Berkus and Oleksii Kliukin at KubeCon Berlin 2017
* `PostgreSQL HA with Kubernetes and Patroni <https://www.youtube.com/watch?v=iruaCgeG7qs>`__, talk by Josh Berkus at KubeCon 2016 (video)
* `Feb. 2016 Zalando Tech blog post <https://tech.zalando.de/blog/zalandos-patroni-a-template-for-high-availability-postgresql/>`__

==================
Development Status
==================

Patroni is in active development and accepts contributions. See our `Contributing <https://github.com/zalando/patroni/blob/master/docs/CONTRIBUTING.rst>`__ section below for more details.

We report new releases information `here <https://github.com/zalando/patroni/releases>`__.

=========
Community
=========

There are two places to connect with the Patroni community: `on github <https://github.com/zalando/patroni>`__, via Issues and PRs, and on channel #patroni in the `PostgreSQL Slack <https://postgres-slack.herokuapp.com/>`__.  If you're using Patroni, or just interested, please join us.

===================================
Technical Requirements/Installation
===================================

**Pre-requirements for Mac OS**

To install requirements on a Mac, run the following:

::

    brew install postgresql etcd haproxy libyaml python

**Psycopg2**

Starting from `psycopg2-2.8 <http://initd.org/psycopg/articles/2019/04/04/psycopg-28-released/>`__ the binary version of psycopg2 will no longer be installed by default. Installing it from the source code requires C compiler and postgres+python dev packages.
Since in the python world it is not possible to specify dependency as ``psycopg2 OR psycopg2-binary`` you will have to decide how to install it.

There are a few options available:

1. Use the package manager from your distro

::

    sudo apt-get install python-psycopg2   # install python2 psycopg2 module on Debian/Ubuntu
    sudo apt-get install python3-psycopg2  # install python3 psycopg2 module on Debian/Ubuntu
    sudo yum install python-psycopg2       # install python2 psycopg2 on RedHat/Fedora/CentOS

2. Install psycopg2 from the binary package

::

    pip install psycopg2-binary

3. Install psycopg2 from source

::

    pip install psycopg2>=2.5.4

**General installation for pip**

Patroni can be installed with pip:

::

    pip install patroni[dependencies]

where dependencies can be either empty, or consist of one or more of the following:

etcd or etcd3
    `python-etcd` module in order to use Etcd as DCS
consul
    `python-consul` module in order to use Consul as DCS
zookeeper
    `kazoo` module in order to use Zookeeper as DCS
exhibitor
    `kazoo` module in order to use Exhibitor as DCS (same dependencies as for Zookeeper)
kubernetes
    `kubernetes` module in order to use Kubernetes as DCS in Patroni
raft
    `pysyncobj` module in order to use python Raft implementation as DCS
aws
    `boto` in order to use AWS callbacks

For example, the command in order to install Patroni together with dependencies for Etcd as a DCS and AWS callbacks is:

::

    pip install patroni[etcd,aws]

Note that external tools to call in the replica creation or custom bootstrap scripts (i.e. WAL-E) should be installed independently of Patroni.

=======================
Running and Configuring
=======================

To get started, do the following from different terminals:
::

    > etcd --data-dir=data/etcd --enable-v2=true
    > ./patroni.py postgres0.yml
    > ./patroni.py postgres1.yml

You will then see a high-availability cluster start up. Test different settings in the YAML files to see how the cluster's behavior changes. Kill some of the components to see how the system behaves.

Add more ``postgres*.yml`` files to create an even larger cluster.

Patroni provides an `HAProxy <http://www.haproxy.org/>`__ configuration, which will give your application a single endpoint for connecting to the cluster's leader. To configure,
run:

::

    > haproxy -f haproxy.cfg

::

    > psql --host 127.0.0.1 --port 5000 postgres

==================
YAML Configuration
==================

Go `here <https://github.com/zalando/patroni/blob/master/docs/SETTINGS.rst>`__ for comprehensive information about settings for etcd, consul, and ZooKeeper. And for an example, see `postgres0.yml <https://github.com/zalando/patroni/blob/master/postgres0.yml>`__.

=========================
Environment Configuration
=========================

Go `here <https://github.com/zalando/patroni/blob/master/docs/ENVIRONMENT.rst>`__ for comprehensive information about configuring(overriding) settings via environment variables.

===================
Replication Choices
===================

Patroni uses Postgres' streaming replication, which is asynchronous by default. Patroni's asynchronous replication configuration allows for ``maximum_lag_on_failover`` settings. This setting ensures failover will not occur if a follower is more than a certain number of bytes behind the leader. This setting should be increased or decreased based on business requirements. It's also possible to use synchronous replication for better durability guarantees. See `replication modes documentation <https://github.com/zalando/patroni/blob/master/docs/replication_modes.rst>`__ for details.

======================================
Applications Should Not Use Superusers
======================================

When connecting from an application, always use a non-superuser. Patroni requires access to the database to function properly. By using a superuser from an application, you can potentially use the entire connection pool, including the connections reserved for superusers, with the ``superuser_reserved_connections`` setting. If Patroni cannot access the Primary because the connection pool is full, behavior will be undesirable.

.. |Tests Status| image:: https://github.com/zalando/patroni/actions/workflows/tests.yaml/badge.svg
   :target: https://github.com/zalando/patroni/actions/workflows/tests.yaml?query=branch%3Amaster
.. |Coverage Status| image:: https://coveralls.io/repos/zalando/patroni/badge.svg?branch=master
   :target: https://coveralls.io/github/zalando/patroni?branch=master
