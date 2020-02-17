.. _readme:

============
Introduction
============

Patroni originated as a fork of `Governor <https://github.com/compose/governor>`__, the project from Compose. It includes plenty of new features.

For an example of a Docker-based deployment with Patroni, see `Spilo <https://github.com/zalando/spilo>`__, currently in use at Zalando.

For additional background info, see:

* `PostgreSQL HA with Kubernetes and Patroni <https://www.youtube.com/watch?v=iruaCgeG7qs>`__, talk by Josh Berkus at KubeCon 2016 (video)
* `Feb. 2016 Zalando Tech blog post <https://tech.zalando.de/blog/zalandos-patroni-a-template-for-high-availability-postgresql/>`__


Development Status
------------------

Patroni is in active development and accepts contributions. See our :ref:`Contributing <contributing>` section below for more details.

We report new releases information :ref:`here <releases>`.


Technical Requirements/Installation
-----------------------------------

**Pre-requirements for Mac OS**

To install requirements on a Mac, run the following:

::

    brew install postgresql etcd haproxy libyaml python

.. _psycopg2_install_options:

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

etcd
    `python-etcd` module in order to use Etcd as DCS
consul
    `python-consul` module in order to use Consul as DCS
zookeeper
    `kazoo` module in order to use Zookeeper as DCS
exhibitor
    `kazoo` module in order to use Exhibitor as DCS (same dependencies as for Zookeeper)
kubernetes
    `kubernetes` module in order to use Kubernetes as DCS in Patroni
aws
    `boto` in order to use AWS callbacks

For example, the command in order to install Patroni together with dependencies for Etcd as a DCS and AWS callbacks is:

::

    pip install patroni[etcd,aws]

Note that external tools to call in the replica creation or custom bootstrap scripts (i.e. WAL-E) should be installed
independently of Patroni.


.. _running_configuring:

Running and Configuring
-----------------------

The following section assumes Patroni repository as being cloned from https://github.com/zalando/patroni. Namely, you
will need example configuration files `postgres0.yml` and `postgres1.yml`. If you installed Patroni with pip, you can
obtain those files from the git repository and replace `./patroni.py` below with `patroni` command.

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


YAML Configuration
------------------

Go :ref:`here <settings>` for comprehensive information about settings for etcd, consul, and ZooKeeper. And for an example, see `postgres0.yml <https://github.com/zalando/patroni/blob/master/postgres0.yml>`__.


Environment Configuration
-------------------------

Go :ref:`here <environment>` for comprehensive information about configuring(overriding) settings via environment variables.


Replication Choices
-------------------

Patroni uses Postgres' streaming replication, which is asynchronous by default. Patroni's asynchronous replication configuration allows for ``maximum_lag_on_failover`` settings. This setting ensures failover will not occur if a follower is more than a certain number of bytes behind the leader. This setting should be increased or decreased based on business requirements. It's also possible to use synchronous replication for better durability guarantees. See :ref:`replication modes documentation <replication_modes>` for details.


Applications Should Not Use Superusers
--------------------------------------

When connecting from an application, always use a non-superuser. Patroni requires access to the database to function properly. By using a superuser from an application, you can potentially use the entire connection pool, including the connections reserved for superusers, with the ``superuser_reserved_connections`` setting. If Patroni cannot access the Primary because the connection pool is full, behavior will be undesirable.

.. |Build Status| image:: https://travis-ci.org/zalando/patroni.svg?branch=master
   :target: https://travis-ci.org/zalando/patroni
.. |Coverage Status| image:: https://coveralls.io/repos/zalando/patroni/badge.svg?branch=master
   :target: https://coveralls.io/r/zalando/patroni?branch=master
