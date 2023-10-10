.. _patroni_configuration:

Patroni configuration
=====================

.. toctree::
   :hidden:

   dynamic_configuration
   yaml_configuration
   ENVIRONMENT


There are 3 types of Patroni configuration:

- Global :ref:`dynamic configuration <dynamic_configuration>`.
	These options are stored in the DCS (Distributed Configuration Store) and applied on all cluster nodes.
	Dynamic configuration can be set at any time using :ref:`patronictl_edit_config` tool or Patroni :ref:`REST API <rest_api>`.
	If the options changed are not part of the startup configuration, they are applied asynchronously (upon the next wake up cycle)
	to every node, which gets subsequently reloaded.
	If the node requires a restart to apply the configuration (for `PostgreSQL parameters <https://www.postgresql.org/docs/current/view-pg-settings.html>`__ with context postmaster, if their values
	have changed), a special flag ``pending_restart`` indicating this is set in the members.data JSON.
	Additionally, the node status indicates this by showing ``"restart_pending": true``.

- Local :ref:`configuration file <yaml_configuration>` (patroni.yml).
	These options are defined in the configuration file and take precedence over dynamic configuration.
	``patroni.yml`` can be changed and reloaded at runtime (without restart of Patroni) by sending SIGHUP to the Patroni process, performing ``POST /reload`` REST-API request or executing :ref:`patronictl_reload`. Local configuration can be either a single YAML file or a directory. When it is a directory, all YAML files in that directory are loaded one by one in sorted order. In case a key is defined in multiple files, the occurrence in the last file takes precedence.

- :ref:`Environment configuration <environment>`.
	It is possible to set/override some of the "Local" configuration parameters with environment variables.
	Environment configuration is very useful when you are running in a dynamic environment and you don't know some of the parameters in advance (for example it's not possible to know your external IP address when you are running inside ``docker``).


Important rules
---------------

PostgreSQL parameters controlled by Patroni
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Some of the PostgreSQL parameters **must hold the same values on the primary and the replicas**. For those, **values set either in the local patroni configuration files or via the environment variables take no effect**. To alter or set their values one must change the shared configuration in the DCS. Below is the actual list of such parameters together with the default values:

- **max_connections**: 100
- **max_locks_per_transaction**: 64
- **max_worker_processes**: 8
- **max_prepared_transactions**: 0
- **wal_level**: hot_standby
- **track_commit_timestamp**: off

For the parameters below, PostgreSQL does not require equal values among the primary and all the replicas. However, considering the possibility of a replica to become the primary at any time, it doesn't really make sense to set them differently; therefore, **Patroni restricts setting their values to the** :ref:`dynamic configuration <dynamic_configuration>`.

- **max_wal_senders**: 5
- **max_replication_slots**: 5
- **wal_keep_segments**: 8
- **wal_keep_size**: 128MB

These parameters are validated to ensure they are sane, or meet a minimum value.

There are some other Postgres parameters controlled by Patroni:

- **listen_addresses** - is set either from ``postgresql.listen`` or from ``PATRONI_POSTGRESQL_LISTEN`` environment variable
- **port** - is set either from ``postgresql.listen`` or from ``PATRONI_POSTGRESQL_LISTEN`` environment variable
- **cluster_name** - is set either from ``scope`` or from ``PATRONI_SCOPE`` environment variable
- **hot_standby: on**
- **wal_log_hints: on** - for Postgres 9.4 and newer.

To be on the safe side parameters from the above lists are not written into ``postgresql.conf``, but passed as a list of arguments to the ``pg_ctl start`` which gives them the highest precedence, even above `ALTER SYSTEM <https://www.postgresql.org/docs/current/static/sql-altersystem.html>`__

There also are some parameters like **postgresql.listen**, **postgresql.data_dir** that **can be set only locally**, i.e. in the Patroni :ref:`config file <yaml_configuration>` or via :ref:`configuration <environment>` variable. In most cases the local configuration will override the dynamic configuration.


When applying the local or dynamic configuration options, the following actions are taken:

- The node first checks if there is a `postgresql.base.conf` or if the ``custom_conf`` parameter is set.
- If the ``custom_conf`` parameter is set, it will take the file specified on it as a base configuration, ignoring `postgresql.base.conf` and `postgresql.conf`.
- If the ``custom_conf`` parameter is not set and `postgresql.base.conf` exists, it contains the renamed "original" configuration and it will be used as a base configuration.
- If there is no ``custom_conf``` nor `postgresql.base.conf`, the original `postgresql.conf`` is taken and renamed to postgresql.base.conf.
- The dynamic options (with the exceptions above) are dumped into the `postgresql.conf`` and an include is set in
  postgresql.conf to the used base configuration (either `postgresql.base.conf` or what is on ``custom_conf``). Therefore, we would be able to apply new options without re-reading the configuration file to check if the include is present not.
- Some parameters that are essential for Patroni to manage the cluster are overridden using the command line.
- If some of the options that require restart are changed (we should look at the context in pg_settings and at the actual
  values of those options), a pending_restart flag of a given node is set. This flag is reset on any restart.

The parameters would be applied in the following order (run-time are given the highest priority):

1. load parameters from file `postgresql.base.conf` (or from a ``custom_conf`` file, if set)
2. load parameters from file `postgresql.conf`
3. load parameters from file `postgresql.auto.conf`
4. run-time parameter using `-o --name=value`

This allows configuration for all the nodes (2), configuration for a specific node using ``ALTER SYSTEM`` (3) and ensures that parameters essential to the running of Patroni are enforced (4), as well as leaves room for configuration tools that manage `postgresql.conf` directly without involving Patroni (1).


PostgreSQL parameters that touch shared memory
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

PostgreSQL has some parameters that determine the size of the shared memory used by them:

- **max_connections**
- **max_prepared_transactions**
- **max_locks_per_transaction**
- **max_wal_senders**
- **max_worker_processes**

Changing these parameters require a PostgreSQL restart to take effect, and their shared memory structures cannot be smaller on the standby nodes than on the primary node.

As explained before, Patroni restrict changing their values through :ref:`dynamic configuration <dynamic_configuration>`, which usually consists of:

1. Applying changes through :ref:`patronictl_edit_config` (or via REST API ``/config`` endpoint)
2. Restarting nodes through :ref:`patronictl_restart` (or via REST API ``/restart`` endpoint)

**Note:** please keep in mind that you should perform a restart of the PostgreSQL nodes through :ref:`patronictl_restart` command, or via REST API ``/restart`` endpoint. An attempt to restart PostgreSQL by restarting the Patroni daemon, e.g. by executing ``systemctl restart patroni``, can cause a failover to occur in the cluster, if you are restarting the primary node.

However, as those settings manage shared memory, some extra care should be taken when restarting the nodes:

* If you want to **increase** the value of any of those settings:

   1. Restart all standbys first
   2. Restart the primary after that

* If you want to **decrease** the value of any of those settings:

   1. Restart the primary first
   2. Restart all standbys after that

**Note:** if you attempt to restart all nodes in one go after **decreasing** the value of any of those settings, Patroni will ignore the change and restart the standby with the original setting value, thus requiring that you restart the standbys again later. Patroni does that to prevent the standby to enter in an infinite crash loop, because PostgreSQL quits with a `FATAL` message if you attempt to set any of those parameters to a value lower than what is visible in ``pg_controldata`` on the Standby node. In other words, we can only decrease the setting on the standby once its ``pg_controldata`` is up-to-date with the primary in regards to these changes on the primary.

More information about that can be found at `PostgreSQL Administrator's Overview <https://www.postgresql.org/docs/current/hot-standby.html#HOT-STANDBY-ADMIN>`__.

Patroni configuration parameters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Also the following Patroni configuration options **can be changed only dynamically**:

- **ttl**: 30
- **loop_wait**: 10
- **retry_timeouts**: 10
- **maximum_lag_on_failover**: 1048576
- **max_timelines_history**: 0
- **check_timeline**: false
- **postgresql.use_slots**: true

Upon changing these options, Patroni will read the relevant section of the configuration stored in DCS and change its run-time values.

Patroni nodes are dumping the state of the DCS options to disk upon for every change of the configuration into the file ``patroni.dynamic.json`` located in the Postgres data directory. Only the leader is allowed to restore these options from the on-disk dump if these are completely absent from the DCS or if they are invalid.
