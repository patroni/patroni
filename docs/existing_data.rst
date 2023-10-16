.. _existing_data:

Convert a Standalone to a Patroni Cluster
=========================================

This section describes the process for converting a standalone PostgreSQL instance into a Patroni cluster.

To deploy a Patroni cluster without using a pre-existing PostgreSQL instance, see :ref:`Running and Configuring <running_configuring>` instead.

Procedure
---------

You can find below an overview of steps for converting an existing Postgres cluster to a Patroni managed cluster. In the steps we assume all nodes that are part of the existing cluster are currently up and running, and that you *do not* intend to change Postgres configuration while the migration is ongoing. The steps:

#. Create the Postgres users as explained for :ref:`authentication <postgresql_settings>` section of the Patroni configuration. You can find sample SQL commands to create the users in the code block below, in which you need to replace the usernames and passwords as per your environment. If you already have the relevant users, then you can skip this step.

   .. code-block:: sql

      -- Patroni superuser
      -- Replace PATRONI_SUPERUSER_USERNAME and PATRONI_SUPERUSER_PASSWORD accordingly
      CREATE USER PATRONI_SUPERUSER_USERNAME WITH SUPERUSER ENCRYPTED PASSWORD 'PATRONI_SUPERUSER_PASSWORD';

      -- Patroni replication user
      -- Replace PATRONI_REPLICATION_USERNAME and PATRONI_REPLICATION_PASSWORD accordingly
      CREATE USER PATRONI_REPLICATION_USERNAME WITH REPLICATION ENCRYPTED PASSWORD 'PATRONI_REPLICATION_PASSWORD';

      -- Patroni rewind user, if you intend to enable use_pg_rewind in your Patroni configuration
      -- Replace PATRONI_REWIND_USERNAME and PATRONI_REWIND_PASSWORD accordingly
      CREATE USER PATRONI_REWIND_USERNAME WITH ENCRYPTED PASSWORD 'PATRONI_REWIND_PASSWORD';
      GRANT EXECUTE ON function pg_catalog.pg_ls_dir(text, boolean, boolean) TO PATRONI_REWIND_USERNAME;
      GRANT EXECUTE ON function pg_catalog.pg_stat_file(text, boolean) TO PATRONI_REWIND_USERNAME;
      GRANT EXECUTE ON function pg_catalog.pg_read_binary_file(text) TO PATRONI_REWIND_USERNAME;
      GRANT EXECUTE ON function pg_catalog.pg_read_binary_file(text, bigint, bigint, boolean) TO PATRONI_REWIND_USERNAME;

#. Perform the following steps on all Postgres nodes. Perform all steps on one node before proceeding with the next node. Start with the primary node, then proceed with each standby node:

   #. If you are running Postgres through systemd, then disable the Postgres systemd unit. This is performed as Patroni manages starting and stopping the Postgres daemon.

   #. Create a YAML configuration file for Patroni.

      * **Note (specific for the primary node):** If you have replication slots being used for replication between cluster members, then it is recommended that you enable ``use_slots`` and configure the existing replication slots as permanent via the ``slots`` configuration item. Be aware that Patroni automatically creates replication slots for replication between members, and drops replication slots that it does not recognize, when ``use_slots`` is enabled. The idea of using permanent slots here is to allow your existing slots to persist while the migration to Patroni is in progress. See :ref:`YAML Configuration Settings <yaml_configuration>` for details.

   #. Start Patroni using the ``patroni`` systemd service unit. It automatically detects that Postgres is already running and starts monitoring the instance.

#. Hand over Postgres "start up procedure" to Patroni. In order to do that you need to restart the cluster members through :ref:`patronictl restart cluster-name member-name <patronictl_restart_parameters>` command. For minimal downtime you might want to split this step into:

   #. Immediate restart of the standby nodes.
   #. Scheduled restart of the primary node within a maintenance window.

#. If you configured permanent slots in step ``1.2.``, then you should remove them from ``slots`` configuration through :ref:`patronictl edit-config cluster-name member-name <patronictl_edit_config_parameters>` command once the ``restart_lsn`` of the slots created by Patroni is able to catch up with the ``restart_lsn`` of the original slots for the corresponding members. By removing the slots from ``slots`` configuration you will allow Patroni to drop the original slots from your cluster once they are not needed anymore. You can find below an example query to check the ``restart_lsn`` of a couple slots, so you can compare them:

   .. code-block:: sql

      -- Assume original_slot_for_member_x is the name of the slot in your original
      -- cluster for replicating changes to member X, and slot_for_member_x is the
      -- slot created by Patroni for that purpose. You need restart_lsn of
      -- slot_for_member_x to be >= restart_lsn of original_slot_for_member_x
      SELECT slot_name,
             restart_lsn
      FROM pg_replication_slots
      WHERE slot_name IN (
          'original_slot_for_member_x',
          'slot_for_member_x'
      )

.. _major_upgrade:

Major Upgrade of PostgreSQL Version
===================================

The only possible way to do a major upgrade currently is:

#. Stop Patroni
#. Upgrade PostgreSQL binaries and perform `pg_upgrade <https://www.postgresql.org/docs/current/pgupgrade.html>`_ on the primary node
#. Update patroni.yml
#. Remove the initialize key from DCS or wipe complete cluster state from DCS. The second one could be achieved by running :ref:`patronictl remove cluster-name <patronictl_remove_parameters>` . It is necessary because pg_upgrade runs initdb which actually creates a new database with a new PostgreSQL system identifier.
#. If you wiped the cluster state in the previous step, you may wish to copy patroni.dynamic.json from old data dir to the new one.  It will help you to retain some PostgreSQL parameters you had set before.
#. Start Patroni on the primary node.
#. Upgrade PostgreSQL binaries, update patroni.yml and wipe the data_dir on standby nodes.
#. Start Patroni on the standby nodes and wait for the replication to complete.

Running pg_upgrade on standby nodes is not supported by PostgreSQL. If you know what you are doing, you can try the rsync procedure described in https://www.postgresql.org/docs/current/pgupgrade.html instead of wiping data_dir on standby nodes. The safest way is however to let Patroni replicate the data for you.

FAQ
---

- During Patroni startup, Patroni complains that it cannot bind to the PostgreSQL port.

  You need to verify ``listen_addresses`` and ``port`` in ``postgresql.conf`` and ``postgresql.listen`` in ``patroni.yml``. Don't forget that ``pg_hba.conf`` should allow such access.

- After asking Patroni to restart the node, PostgreSQL displays the error message ``could not open configuration file "/etc/postgresql/10/main/pg_hba.conf": No such file or directory``

  It can mean various things depending on how you manage PostgreSQL configuration. If you specified `postgresql.config_dir`, Patroni generates the ``pg_hba.conf`` based on the settings in the :ref:`bootstrap <bootstrap_settings>` section only when it bootstraps a new cluster. In this scenario the ``PGDATA`` was not empty, therefore no bootstrap happened. This file must exist beforehand.
