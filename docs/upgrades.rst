.. _upgrades:

Major version upgrades
======================

PostgreSQL major version upgrades require special handling. There are a few different ways to achieve major version upgrades. The options are ``pg_dump``, ``pg_upgrade`` and logical replication. The ``pg_dump`` method, a full export and import of the database, is unsuitable for use in larger databases  because of the large outage it would cause. The logical replication based solution allows for versions to be switched with minimal downtime and allows for clusters of mixed versions. There are limitations to this method, replication throughput is limited, and database schema updates and some objects like large objects cannot be replicated. Currently Patroni does not directly support logical replication based method.

The upgrade method supported by Patroni is based on running ``pg_upgrade`` with ``--link`` option. This method does a schema-only database export and import and then transplant data files directly from old version to the new version using filesystem level capabilities. Copying metadata does not depend on size of the database, just the number of tables, and is relatively quick. Typical inaccessibility to writes using this method is less than a minute. Depending on specifics of the database schema and hardware performance the process might take longer. It is advised to test the upgrade on a restored backup ahead of time to verify successful upgrade capability and performance.

Configuring major version upgrades
----------------------------------

Traditional Patroni configuration cannot be directly used for upgrades because they hard-code the location of binaries and data directories. Automatically changing the configuration while Patroni is running the cluster would not for robustness reasons. Therefore it is necessary to set up Patroni to generate those paths based on the currently active version. Which parameters need to be changed depends on the filesystem layout conventions used for the installation.

Single data directory setup
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The simplest filesystem layout is where there is a version independent location for PostgreSQL data directory. During the upgrade the new version data directory will be created with a ``_new`` suffix. After a successful upgrade the data directories will be swapped with the old version ending up with ``_old`` suffix until it is deleted after a successful upgrade.

.. note::
    For pg_upgrade --link to work the source and target data directories must be on the same filesystem. This means that upgrades will not work in systems where the primary data directory is stored directly on the mount point. Always use a subdirectory for PostgreSQL data directory.

Example configuration:

.. code:: YAML

    bootstrap:
        version: 15  # This is only used to initialize the data directory

    postgresql:
        data_dir: /mnt/db/pgdata
        bin_dir_template: /opt/builds/pgsql-{major_version}/bin
        state_file: /mnt/db/patroni.state  # Optional: increases robustness for failures during upgrade

Version specific data directory setup
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Many systems set up PostgreSQL using a convention where the major version of the database is a part of the data directory path. This means that upgrades become simpler because there is no need to shuffle data directories around. But because there is no longer a canonical location for the data directory the currently active data directory location needs to be stored in a well known location. This is achieved by specifying a ``postgresql.state_file`` parameter.

Example configuration:

.. code:: YAML

    bootstrap:
        version: 15

    postgresql:
        data_dir_tempalte: /var/lib/pgsql/{major_version/data/
        bin_dir_template: /usr/pgsql-{major_version}/bin
        state_file: /var/lib/pgsql/patroni.state  # Required to find the correct data dir location

Running the upgrade
-------------------

Upgrading a cluster is a simple process.

#. Install the desired target version and all used extensions on all nodes in the cluster.

#. Make sure that the cluster is healthy and your backup systems are operating properly.

#. Trigger the upgrade process using ``patronictl upgrade``

   Example:

   .. code:: YAML

       $ patronictl upgrade --new-version=18

#. Confirm that you want to start the upgrade and observe that the upgrade completes.


Configuring the upgrade process
-------------------------------

Some PostgreSQL databases may need some extra steps to let major version upgrades proceed smoothly. For example incompatible extensions may need to be dropped, unlogged tables should be truncated to avoid copying them during the outage window, views and functions referencing system catalog schema need to be recreated, and some extensions may need to be put into upgrade mode. Patroni provides customization points to allow for automation of such tasks.

The upgrade process can be customized using the ``upgrade`` configuration key in the global configuration. For example:

.. code:: YAML

    ttl: 30
    loop_wait: 10
    retry_timeout: 10
    upgrade:
      prepare:
      - truncate_unlogged
      replica_upgrade_method: rsync
      rsync:
        conf_dir: /var/lib/pgsql/rsync
        port: 5432
      parameters:
        timescaledb.restoring: "on"
        archive_mode: "off"
      post_upgrade:
      - cmd:
          shell: "pgbackrest stanza-upgrade --stanza=db"



Things that do not work
-----------------------

-  Upgrading standby clusters using pg_upgrade is not supported. Standby cluster need to be reinitialized after the source cluster has been upgraded.
- Upgrading clusters with tablespaces is not yet supported.
- Storing WAL outside of the data directory is not yet supported.
- Data directory layout suffix must match between primaries and replicas. Version independent prefix can differ.
- Automatic reinitialization of replicas that failed during the upgrade process is not yet implemented. These must be reinitialized manually.