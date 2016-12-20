.. Patroni documentation master file, created by
   sphinx-quickstart on Mon Dec 19 16:54:09 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Introduction
============

Patroni is a template for you to create your own customized, high-availability solution using Python and - for maximum accessibility - a distributed configuration store like `ZooKeeper <https://zookeeper.apache.org/>`__, `etcd <https://github.com/coreos/etcd>`__ or `Consul <https://github.com/hashicorp/consul>`__. Database engineers, DBAs, DevOps engineers, and SREs who are looking to quickly deploy HA PostgreSQL in the datacenter-or anywhere else-will hopefully find it useful.

We call Patroni a "template" because it is far from being a one-size-fits-all or plug-and-play replication system. It will have its own caveats. Use wisely. There are many ways to run high availability with PostgreSQL; for a list, see the `PostgreSQL Documentation <https://wiki.postgresql.org/wiki/Replication,_Clustering,_and_Connection_Pooling>`__.

**Note to Kubernetes users**: We're currently developing Patroni to be as useful as possible for teams running Kubernetes on top of Google Compute Engine; Patroni can be the HA solution for Postgres in such an environment. To this end, we've created a `Helm Chart <https://github.com/kubernetes/charts/tree/master/incubator/patroni>`__ that enables you to deploy a five-node Patroni cluster using a Kubernetes PetSet.


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   README
   dynamic_configuration
   ENVIRONMENT
   SETTINGS
   replication_modes
   pause
   releases

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


