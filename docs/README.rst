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

**For Mac**

To install requirements on a Mac, run the following:

::

    brew install postgresql etcd haproxy libyaml python
    pip install psycopg2 pyyaml


Running and Configuring
-----------------------

To get started, do the following from different terminals:
::

    > etcd --data-dir=data/etcd
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
