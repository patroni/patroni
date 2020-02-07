.. _existing_data:

Convert a Standalone to a Patroni Cluster
=========================================

This section describes the process for converting a standalone PostgreSQL instance into a Patroni cluster.

To deploy a Patroni cluster without using a pre-existing PostgreSQL instance, see :ref:`Running and Configuring <running_configuring>` instead.

Procedure
---------

A Patroni cluster can be started with a data directory from a single-node PostgreSQL database. This is achieved by following closely these steps:

1. Manually start PostgreSQL daemon
2. Create Patroni superuser and replication users as defined in the :ref:`authentication <postgresql_settings>` section of the Patroni configuration. If this user is created in SQL, the following queries achieve this:

.. code-block:: sql

   CREATE USER $PATRONI_SUPERUSER_USERNAME WITH SUPERUSER ENCRYPTED PASSWORD '$PATRONI_SUPERUSER_PASSWORD';
   CREATE USER $PATRONI_REPLICATION_USERNAME WITH REPLICATION ENCRYPTED PASSWORD '$PATRONI_REPLICATION_PASSWORD';

3. Start Patroni (e.g. ``patroni /etc/patroni/patroni.yml``). It automatically detects that PostgreSQL daemon is already running but its configuration might be out-of-date.
4. Ask Patroni to restart the node with ``patronictl restart cluster-name node-name``. This step is only required if PostgreSQL configuration is out-of-date.


FAQ
---

- During Patroni startup, Patroni complains that it cannot bind to the PostgreSQL port.

  You need to verify ``listen_addresses`` and ``port`` in ``postgresql.conf`` and ``postgresql.listen`` in ``patroni.yml``. Don't forget that ``pg_hba.conf`` should allow such access.

- After asking Patroni to restart the node, PostgreSQL displays the error message ``could not open configuration file "/etc/postgresql/10/main/pg_hba.conf": No such file or directory``

  It can mean various things depending on how you manage PostgreSQL configuration. If you specified `postgresql.config_dir`, Patroni generates the ``pg_hba.conf`` based on the settings in the :ref:`bootstrap <bootstrap_settings>` section only when it bootstraps a new cluster. In this scenario the ``PGDATA`` was not empty, therefore no bootstrap happened. This file must exist beforehand.
