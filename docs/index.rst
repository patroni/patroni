.. Patroni documentation master file, created by
   sphinx-quickstart on Mon Dec 19 16:54:09 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. warning::

  Running Patroni on **memory-restricted systems with Python 3.11+**

----

If you run Patroni on a system with strict memory limits, for example with ``vm.overcommit_memory=2`` (recommended for PostgreSQL), and use Python 3.11 or newer, you may observe unexpected behavior:

- Patroni appears healthy
- PostgreSQL continues to run
- Patroni **REST API becomes unresponsive**
- The operating system reports that Patroni is listening on the REST API port
- Patroni logs look normal; however, following messages may appear once: ``Exception ignored in thread started by: <object repr() failed>``, ``MemoryError``
- Kernel logs may contain messages such as   ``not enough memory for the allocation``

This behavior is caused by a `bug in Python 3.11+ <https://github.com/python/cpython/issues/140746>`__.
Under strict memory conditions, starting a new thread may hang indefinitely when there is not enough free memory.

Recommended solution
====================

Recent Patroni releases (4.1.1+, 4.0.8+) reduce the impact of this issue by starting all required threads early during startup, before the system is under memory pressure.

Additional recommendations (Linux, glibc)
=========================================

When running with ``vm.overcommit_memory=2`` (recommended for PostgreSQL), we also recommend starting Patroni with the following environment variables configured:

- ``MALLOC_ARENA_MAX=1`` - reduces the amount of virtual memory allocated by glibc for multi-threaded
  applications
- ``PG_MALLOC_ARENA_MAX=`` - resets the value of ``MALLOC_ARENA_MAX`` for PostgreSQL processes started by Patroni.

In addition, you may tune the following Patroni configuration parameters:

- ``thread_stack_size`` - stack size used for threads started by Patroni. Lowering this value reduces memory usage of the Patroni process. The default value set by Patroni is ``512kB``. Increase ``thread_stack_size`` if Patroni experience stack-related crashes; otherwise the default value is sufficient.
- ``thread_pool_size`` - size of the thread pool used by Patroni for asynchronous tasks and REST API communication with other members during leader race or failsafe checks. The default value is ``5``, which is sufficient for three-node clusters.
- ``restapi.thread_pool_size`` - size of the thread pool used to process REST API requests. The default value is ``5``, allowing up to five parallel REST API requests. Note that requests involving SQL queries are effectively serialized because a single database connection is used, so increasing this value typically provides no benefit.

----

Introduction
============

Patroni is a template for high availability (HA) PostgreSQL solutions using Python. For maximum accessibility, Patroni supports a variety of distributed configuration stores like `ZooKeeper <https://zookeeper.apache.org/>`__, `etcd <https://github.com/coreos/etcd>`__, `Consul <https://github.com/hashicorp/consul>`__ or `Kubernetes <https://kubernetes.io>`__. Database engineers, DBAs, DevOps engineers, and SREs who are looking to quickly deploy HA PostgreSQL in datacenters — or anywhere else — will hopefully find it useful.

We call Patroni a "template" because it is far from being a one-size-fits-all or plug-and-play replication system. It will have its own caveats. Use wisely. There are many ways to run high availability with PostgreSQL; for a list, see the `PostgreSQL Documentation <https://wiki.postgresql.org/wiki/Replication,_Clustering,_and_Connection_Pooling>`__.

Currently supported PostgreSQL versions: 9.3 to 18.

**Note to Citus users**: Starting from 3.0 Patroni nicely integrates with the `Citus <https://github.com/citusdata/citus>`__ database extension to Postgres. Please check the :ref:`Citus support page <citus>` in the Patroni documentation for more info about how to use Patroni high availability together with a Citus distributed cluster.

**Note to Kubernetes users**: Patroni can run natively on top of Kubernetes. Take a look at the :ref:`Kubernetes <kubernetes>` chapter of the Patroni documentation.


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   README
   installation
   patroni_configuration
   rest_api
   patronictl
   replica_bootstrap
   replication_modes
   standby_cluster
   watchdog
   pause
   dcs_failsafe_mode
   kubernetes
   citus
   existing_data
   tools_integration
   security
   ha_multi_dc
   faq
   releases
   CONTRIBUTING

Indices and tables
==================

.. ifconfig:: builder == 'html'

  * :ref:`genindex`
  * :ref:`modindex`
  * :ref:`search`

.. ifconfig:: builder != 'html'

  * :ref:`genindex`
  * :ref:`search`
