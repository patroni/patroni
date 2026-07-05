|Tests Status| |Coverage Status|

.. image:: docs/_static/patroni-logo.svg
   :height: 128px
   :width: 128px

Patroni: A Template for PostgreSQL HA with ZooKeeper, etcd or Consul
====================================================================

You can find a searchable version of this documentation at
`patroni.readthedocs.io <https://patroni.readthedocs.io>`__.

Patroni is a Python template for building PostgreSQL high availability (HA) clusters.
It supports several distributed configuration stores, including `ZooKeeper <https://zookeeper.apache.org/>`__,
`etcd <https://github.com/coreos/etcd>`__, `Consul <https://github.com/hashicorp/consul>`__, and `Kubernetes <https://kubernetes.io>`__.

Supported PostgreSQL versions: 9.3 to 18.

**Note to Citus users**: Since version 3.0, Patroni integrates with the `Citus <https://github.com/citusdata/citus>`__ extension.
See the `Citus support page <https://github.com/patroni/patroni/blob/master/docs/citus.rst>`__ for details.

**Note to Kubernetes users**: Patroni runs natively on Kubernetes.
See the `Kubernetes guide <https://github.com/patroni/patroni/blob/master/docs/kubernetes.rst>`__ for more information.

.. contents::
    :local:
    :depth: 2
    :backlinks: none


How Patroni Works
-----------------

Patroni (formerly known as Zalando's Patroni) started as a fork of `Governor <https://github.com/compose/governor>`__ and adds support for modern HA patterns.

For additional background info, see:

* `Elephants on Automatic: HA Clustered PostgreSQL with Helm <https://www.youtube.com/watch?v=CftcVhFMGSY>`_
* `PostgreSQL HA with Kubernetes and Patroni <https://www.youtube.com/watch?v=iruaCgeG7qs>`__
* `Zalando Tech blog post from Feb. 2016 <https://engineering.zalando.com/posts/2016/02/zalandos-patroni-a-template-for-high-availability-postgresql.html>`__

Development Status
------------------

Patroni is actively developed and welcomes contributions.

- `Contributing guidelines <https://github.com/patroni/patroni/blob/master/docs/contributing_guidelines.rst>`__
- `Release notes <https://github.com/patroni/patroni/releases>`__

Community
---------

Connect with the Patroni community on GitHub or Slack:

- `Issues, discussions and PRs in the GitHub repository <https://github.com/patroni/patroni>`__
- `#patroni channel <https://postgresteam.slack.com/archives/C9XPYG92A>`__ on the `PostgreSQL Slack <https://pgtreats.info/slack-invite>`__

Requirements and Installation
-----------------------------

Pre-requirements for macOS
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Install the requirements on macOS with Homebrew:

::

    brew install postgresql etcd haproxy libyaml python

Psycopg choices
^^^^^^^^^^^^^^^

Patroni requires a PostgreSQL Python driver. Recent versions of `psycopg2 <http://initd.org/psycopg/articles/2019/04/04/psycopg-28-released/>`__
no longer install a binary package by default, which means building from source may require a C compiler and development libraries.

Options:

1. Install using the package manager from your Linux distribution:

::

    sudo apt-get install python3-psycopg2
    sudo yum install python3-psycopg2

2. Install one of the supported Python packages with pip:

- `psycopg`
- `psycopg2`
- `psycopg2-binary`

Installing with pip
^^^^^^^^^^^^^^^^^^

Install Patroni with optional dependency groups:

::

    pip install patroni[dependencies]

Available dependency extras:

- ``etcd`` or ``etcd3``: `python-etcd` for Etcd as DCS
- ``consul``: `py-consul` for Consul as DCS
- ``zookeeper``: `kazoo` for ZooKeeper as DCS
- ``exhibitor``: `kazoo` for Exhibitor as DCS
- ``kubernetes``: `kubernetes` for Kubernetes as DCS
- ``raft``: `pysyncobj` for the python Raft DCS
- ``aws``: `boto3` for AWS callbacks
- ``systemd``: `systemd-python` for sd_notify integration
- ``all``: all of the above (except psycopg family)
- ``psycopg3``: `psycopg[binary]>=3.0.0`
- ``psycopg2``: `psycopg2>=2.5.4`
- ``psycopg2-binary``: `psycopg2-binary`

For example:

::

    pip install patroni[psycopg3,etcd3,aws]

Note: external tools used by bootstrap or replica creation scripts (for example WAL-G) must be installed separately.

Running and Configuring
-----------------------

A minimal cluster can be started from different terminals:

::

    > etcd --data-dir=data/etcd --enable-v2=true
    > ./patroni.py postgres0.yml
    > ./patroni.py postgres1.yml

Then verify cluster behavior and experiment with the YAML configuration files.

Add more ``postgres*.yml`` files to scale the cluster.

Memory issue on Python 3.11+
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you run Patroni on a system with strict memory limits, for example with ``vm.overcommit_memory=2`` (recommended for PostgreSQL), and use Python 3.11 or newer, you may observe unexpected behavior:

- Patroni appears healthy
- PostgreSQL continues to run
- Patroni **REST API becomes unresponsive**
- the operating system reports that Patroni is listening on the REST API port
- Patroni logs look normal; however, following messages may appear once: ``Exception ignored in thread started by: <object repr() failed>``, ``MemoryError``
- kernel logs may contain messages such as   ``not enough memory for the allocation``

This is caused by a `Python 3.11+ issue <https://github.com/python/cpython/issues/140746>`__.
Under strict memory conditions, starting a new thread may hang indefinitely when there is not enough free memory.

Recommended solution
""""""""""""""""""""

Recent Patroni releases (4.1.1+, 4.0.8+) reduce the impact of this issue by starting all required threads early in startup before memory pressure builds.

Additional recommendations (Linux, glibc)
"""""""""""""""""""""""""""""""""""""""""

When running with ``vm.overcommit_memory=2`` (recommended for PostgreSQL), we also recommend starting Patroni with the following environment variables configured:

- ``MALLOC_ARENA_MAX=1`` - reduces the amount of virtual memory allocated by glibc for multi-threaded
  applications
- ``PG_MALLOC_ARENA_MAX=`` - resets the value of ``MALLOC_ARENA_MAX`` for PostgreSQL processes started by Patroni.

In addition, you may tune the following Patroni configuration parameters:

- ``thread_stack_size`` - stack size used for threads started by Patroni. Lowering this value reduces memory usage of the Patroni process. The default value set by Patroni is ``512kB``. Increase ``thread_stack_size`` if Patroni experience stack-related crashes; otherwise the default value is sufficient.
- ``thread_pool_size`` - size of the thread pool used by Patroni for asynchronous tasks and REST API communication with other members during leader race or failsafe checks. The default value is ``5``, which is sufficient for three-node clusters.
- ``restapi.thread_pool_size`` - size of the thread pool used to process REST API requests. The default value is ``5``, allowing up to five parallel REST API requests. Note that requests involving SQL queries are effectively serialized because a single database connection is used, so increasing this value typically provides no benefit.

HAProxy support
^^^^^^^^^^^^^^^

Patroni includes an `HAProxy <http://www.haproxy.org/>`__ configuration for a single application endpoint.
Start it with:

::

    > haproxy -f haproxy.cfg

Then connect with:

::

    > psql --host 127.0.0.1 --port 5000 postgres

Configuration References
------------------------

YAML configuration
^^^^^^^^^^^^^^^^^^

For complete YAML options, see
`docs/dynamic_configuration.rst <https://github.com/patroni/patroni/blob/master/docs/dynamic_configuration.rst>`__
and the example file `postgres0.yml <https://github.com/patroni/patroni/blob/master/postgres0.yml>`__.

Environment configuration
^^^^^^^^^^^^^^^^^^^^^^^^^

For environment variable configuration, see
`docs/ENVIRONMENT.rst <https://github.com/patroni/patroni/blob/master/docs/ENVIRONMENT.rst>`__.

Replication choices
^^^^^^^^^^^^^^^^^^^

Patroni uses PostgreSQL streaming replication. It supports:

- asynchronous replication with ``maximum_lag_on_failover``
- synchronous replication for stronger durability guarantees

See the `replication modes documentation <https://github.com/patroni/patroni/blob/master/docs/replication_modes.rst>`__ for details.

Application connections
^^^^^^^^^^^^^^^^^^^^^^^

Applications should connect with a non-superuser. Using a superuser can consume reserved connections
for Patroni and cause undesirable behavior if the leader becomes unavailable.

.. |Tests Status| image:: https://github.com/patroni/patroni/actions/workflows/tests.yaml/badge.svg
    :target: https://github.com/patroni/patroni/actions/workflows/tests.yaml?query=branch%3Amaster
.. |Coverage Status| image:: https://codecov.io/gh/patroni/patroni/graph/badge.svg?token=qWNJyFTeul 
    :target: https://codecov.io/gh/patroni/patroni
