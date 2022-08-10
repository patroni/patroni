.. Patroni documentation master file, created by
   sphinx-quickstart on Mon Dec 19 16:54:09 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Introduction
============

Patroni is a template for you to create your own customized, high-availability solution using Python and - for maximum accessibility - a distributed configuration store like `ZooKeeper <https://zookeeper.apache.org/>`__, `etcd <https://github.com/coreos/etcd>`__, `Consul <https://github.com/hashicorp/consul>`__ or `Kubernetes <https://kubernetes.io>`__. Database engineers, DBAs, DevOps engineers, and SREs who are looking to quickly deploy HA PostgreSQL in the datacenter-or anywhere else-will hopefully find it useful.

We call Patroni a "template" because it is far from being a one-size-fits-all or plug-and-play replication system. It will have its own caveats. Use wisely. There are many ways to run high availability with PostgreSQL; for a list, see the `PostgreSQL Documentation <https://wiki.postgresql.org/wiki/Replication,_Clustering,_and_Connection_Pooling>`__.

Currently supported PostgreSQL versions: 9.3 to 14.

**Note to Kubernetes users**: Patroni can run natively on top of Kubernetes. Take a look at the :ref:`Kubernetes <kubernetes>` chapter of the Patroni documentation.


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   README
   dynamic_configuration
   dcs_failsafe_mode
   rest_api
   existing_data
   ENVIRONMENT
   SETTINGS
   security
   replica_bootstrap
   replication_modes
   pause
   kubernetes
   watchdog
   releases
   CONTRIBUTING

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
