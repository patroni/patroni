.. _existing_data:

Convert a Standalone to a Patroni Cluster
=========================================

This section describes the process for converting a standalone PostgreSQL instance into a Patroni cluster.

To deploy a Patroni cluster without using a pre-existing PostgreSQL instance, see :ref:`text <link>` instead.

Procedure
---------

A Patroni cluster can be started with a data directory from a single-node PostgreSQL database. This is achieved by following closely these steps:

#. Manually start PostgreSQL daemon
#. Create Patroni super-user as defined in the :ref:`authentication <postgresql_settings>` section of the Patroni configuration. If this user is created in SQL, the following queries achieve this:

.. code-block:: sql
  CREATE USER $PATRONI_SUPERUSER_USERNAME SUPERUSER CREATEDB CREATEROLE REPLICATION BYPASSRLS;
  ALTER USER $PATRONI_SUPERUSER_USERNAME ENCRYPTED PASSWORD '$PATRONI_SUPERUSER_PASSWORD';

#. Start Patroni (e.g. ``patroni /etc/patroni/patroni.yml``). It automatically detects that PostgreSQL daemon is already running but its configuration is out-of-date.
#. Ask Patroni to restart the node with ``patronictl restart cluster-name node-name``.


FAQ
---

#. During Patroni startup, Patroni complains that it cannot bind to the PostgreSQL port.

You need to verify ``listen_addresses`` and ``port`` in ``postgresql.conf`` and ``postgresql.listen`` in ``patroni.yml``. Don't forget that ``pg_hba.conf`` should allow such access.

#. After asking Patroni to restart the node, PostgreSQL displays the error message ``could not open configuration file "/etc/postgresql/10/main/pg_hba.conf": No such file or directory``

Patroni generates the ``pg_hba.conf`` based on the settings in the :ref:`bootstrap <bootstrap_settings>` section only when it bootstraps a new cluster. In this scenarion the ``PGDATA`` was not empty, therefore no bootstrap happened. This file must exist beforehand.
