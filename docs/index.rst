.. Patroni documentation master file, created by
   sphinx-quickstart on Mon Dec 19 16:54:09 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Introduction
============

Patroni is a template for high availability (HA) PostgreSQL solutions using Python. For maximum accessibility, Patroni supports a variety of distributed configuration stores like `ZooKeeper <https://zookeeper.apache.org/>`__, `etcd <https://github.com/coreos/etcd>`__, `Consul <https://github.com/hashicorp/consul>`__ or `Kubernetes <https://kubernetes.io>`__. Database engineers, DBAs, DevOps engineers, and SREs who are looking to quickly deploy HA PostgreSQL in datacenters — or anywhere else — will hopefully find it useful.

We call Patroni a "template" because it is far from being a one-size-fits-all or plug-and-play replication system. It will have its own caveats. Use wisely. There are many ways to run high availability with PostgreSQL; for a list, see the `PostgreSQL Documentation <https://wiki.postgresql.org/wiki/Replication,_Clustering,_and_Connection_Pooling>`__.

Currently supported PostgreSQL versions: 9.3 to 16.

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
   watchdog
   pause
   dcs_failsafe_mode
   kubernetes
   citus
   existing_data
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
