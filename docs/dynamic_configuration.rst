.. _dynamic_configuration:

==============================
Dynamic Configuration Settings
==============================

Dynamic configuration is stored in the DCS (Distributed Configuration Store) and applied on all cluster nodes.

In order to change the dynamic configuration you can use either ``patronictl edit-config`` tool or Patroni :ref:`REST API <rest_api>`.

-  **loop\_wait**: the number of seconds the loop will sleep. Default value: 10
-  **ttl**: the TTL to acquire the leader lock (in seconds). Think of it as the length of time before initiation of the automatic failover process. Default value: 30
-  **retry\_timeout**: timeout for DCS and PostgreSQL operation retries (in seconds). DCS or network issues shorter than this will not cause Patroni to demote the leader. Default value: 10
-  **maximum\_lag\_on\_failover**: the maximum bytes a follower may lag to be able to participate in leader election.
-  **maximum\_lag\_on\_syncnode**: the maximum bytes a synchronous follower may lag before it is considered as an unhealthy candidate and swapped by healthy asynchronous follower. Patroni utilize the max replica lsn if there is more than one follower, otherwise it will use leader's current wal lsn. Default is -1, Patroni will not take action to swap synchronous unhealthy follower when the value is set to 0 or below. Please set the value high enough so Patroni won't swap synchrounous follower fequently during high transaction volume.
-  **max\_timelines\_history**: maximum number of timeline history items kept in DCS.  Default value: 0. When set to 0, it keeps the full history in DCS.
-  **primary\_start\_timeout**: the amount of time a primary is allowed to recover from failures before failover is triggered (in seconds). Default is 300 seconds. When set to 0 failover is done immediately after a crash is detected if possible. When using asynchronous replication a failover can cause lost transactions. Worst case failover time for primary failure is: loop\_wait + primary\_start\_timeout + loop\_wait, unless primary\_start\_timeout is zero, in which case it's just loop\_wait. Set the value according to your durability/availability tradeoff.
-  **primary\_stop\_timeout**: The number of seconds Patroni is allowed to wait when stopping Postgres and effective only when synchronous_mode is enabled. When set to > 0 and the synchronous_mode is enabled, Patroni sends SIGKILL to the postmaster if the stop operation is running for more than the value set by primary\_stop\_timeout. Set the value according to your durability/availability tradeoff. If the parameter is not set or set <= 0, primary\_stop\_timeout does not apply.
-  **synchronous\_mode**: turns on synchronous replication mode. In this mode a replica will be chosen as synchronous and only the latest leader and synchronous replica are able to participate in leader election. Synchronous mode makes sure that successfully committed transactions will not be lost at failover, at the cost of losing availability for writes when Patroni cannot ensure transaction durability. See :ref:`replication modes documentation <replication_modes>` for details.
-  **synchronous\_mode\_strict**: prevents disabling synchronous replication if no synchronous replicas are available, blocking all client writes to the primary. See :ref:`replication modes documentation <replication_modes>` for details.
-  **failsafe\_mode**: Enables :ref:`DCS Failsafe Mode <dcs_failsafe_mode>`. Defaults to `false`.
-  **postgresql**:

   -  **use\_pg\_rewind**: whether or not to use pg_rewind. Defaults to `false`.
   -  **use\_slots**: whether or not to use replication slots. Defaults to `true` on PostgreSQL 9.4+.
   -  **recovery\_conf**: additional configuration settings written to recovery.conf when configuring follower. There is no recovery.conf anymore in PostgreSQL 12, but you may continue using this section, because Patroni handles it transparently.
   -  **parameters**: list of configuration settings for Postgres.

-  **standby\_cluster**: if this section is defined, we want to bootstrap a standby cluster.

   -  **host**: an address of remote node
   -  **port**: a port of remote node
   -  **primary\_slot\_name**: which slot on the remote node to use for replication. This parameter is optional, the default value is derived from the instance name (see function `slot_name_from_member_name`).
   -  **create\_replica\_methods**: an ordered list of methods that can be used to bootstrap standby leader from the remote primary, can be different from the list defined in :ref:`postgresql_settings`
   -  **restore\_command**: command to restore WAL records from the remote primary to nodes in a standby cluster, can be different from the list defined in :ref:`postgresql_settings`
   -  **archive\_cleanup\_command**: cleanup command for standby leader
   -  **recovery\_min\_apply\_delay**: how long to wait before actually apply WAL records on a standby leader

-  **slots**: define permanent replication slots. These slots will be preserved during switchover/failover. The logical slots are copied from the primary to a standby with restart, and after that their position advanced every **loop_wait** seconds (if necessary). Copying logical slot files performed via ``libpq`` connection and using either rewind or superuser credentials (see **postgresql.authentication** section). There is always a chance that the logical slot position on the replica is a bit behind the former primary, therefore application should be prepared that some messages could be received the second time after the failover. The easiest way of doing so - tracking ``confirmed_flush_lsn``. Enabling permanent logical replication slots requires **postgresql.use_slots** to be set and will also automatically enable the ``hot_standby_feedback``. Since the failover of logical replication slots is unsafe on PostgreSQL 9.6 and older and PostgreSQL version 10 is missing some important functions, the feature only works with PostgreSQL 11+.

   -  **my\_slot\_name**: the name of replication slot. If the permanent slot name matches with the name of the current primary it will not be created. Everything else is the responsibility of the operator to make sure that there are no clashes in names between replication slots automatically created by Patroni for members and permanent replication slots.

      -  **type**: slot type. Could be ``physical`` or ``logical``. If the slot is logical, you have to additionally define ``database`` and ``plugin``.
      -  **database**: the database name where logical slots should be created.
      -  **plugin**: the plugin name for the logical slot.

-  **ignore\_slots**: list of sets of replication slot properties for which Patroni should ignore matching slots. This configuration/feature/etc. is useful when some replication slots are managed outside of Patroni. Any subset of matching properties will cause a slot to be ignored.

   -  **name**: the name of the replication slot.
   -  **type**: slot type. Can be ``physical`` or ``logical``. If the slot is logical, you may additionally define ``database`` and/or ``plugin``.
   -  **database**: the database name (when matching a ``logical`` slot).
   -  **plugin**: the logical decoding plugin (when matching a ``logical`` slot).

Note: **slots** is a hashmap while **ignore_slots** is an array. For example:

.. code:: YAML

        slots:
          permanent_logical_slot_name:
            type: logical
            database: my_db
            plugin: test_decoding
          permanent_physical_slot_name:
            type: physical
          ...
        ignore_slots:
          - name: ignored_logical_slot_name
            type: logical
            database: my_db
            plugin: test_decoding
          - name: ignored_physical_slot_name
            type: physical
          ...