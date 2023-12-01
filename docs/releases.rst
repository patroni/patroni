.. _releases:

Release notes
=============

Version 3.2.1
-------------

**Bugfixes**

- Limit accepted values for ``--format`` argument in ``patronictl`` (Alexander Kukushkin)

  It used to accept any arbitrary string and produce no output if the value wasn't recognized.

- Verify that replica nodes received checkpoint LSN on shutdown before releasing the leader key (Alexander Kukushkin)

  Previously in some cases, we were using LSN of the SWITCH record that is followed by CHECKPOINT (if archiving mode is enabled). As a result the former primary sometimes had to do ``pg_rewind``, but there would be no data loss involved.

- Do a real HTTP request when performing node name uniqueness check (Alexander Kukushkin)

  When running Patroni in containers it is possible that the traffic is routed using ``docker-proxy``, which listens on the port and accepts incoming connections. It was causing false positives.

- Fixed Citus support with Etcd v2 (Alexander Kukushkin)

  Patroni was failing to deploy a new Citus cluster with Etcd v2.

- Fixed ``pg_rewind`` behavior with Postgres v16+ (Alexander Kukushkin)

  The error message format of ``pg_waldump`` changed in v16 which caused ``pg_rewind`` to be called by Patroni even when it was not necessary.

- Fixed bug with custom bootstrap (Alexander Kukushkin)

  Patroni was falsely applying ``--command`` argument, which is a bootstrap command itself.

- Fixed the issue with REST API health check endpoints (Sophia Ruan)

  There were chances that after Postgres restart it could return ``unknown`` state for Postgres because connections were not properly closed.

- Cache ``postgres --describe-config`` output results (Waynerv)

  They are used to figure out which GUCs are available to validate PostgreSQL configuration and we don't expect this list to change while Patroni is running.


Version 3.2.0
-------------

**Deprecation notice**

- The ``bootstrap.users`` support will be removed in version 4.0.0. If you need to create users after deploying a new cluster please use the ``bootstrap.post_bootstrap`` hook for that.


**Breaking changes**

- Enforce ``loop_wait + 2*retry_timeout <= ttl`` rule and hard-code minimal possible values (Alexander Kukushkin)

  Minimal values: ``loop_wait=2``, ``retry_timeout=3``, ``ttl=20``. In case values are smaller or violate the rule they are adjusted and a warning is written to Patroni logs.


**New features**

- Failover priority (Mark Pekala)

  With the help of ``tags.failover_priority`` it's now possible to make a node more preferred during the leader race. More details in the documentation (ref tags).

- Implemented ``patroni --generate-config [--dsn DSN]`` and ``patroni --generate-sample-config`` (Polina Bungina)

  It allows to generate a config file for the running PostgreSQL cluster or a sample config file for the new Patroni cluster.

- Use a dedicated connection to Postgres for Patroni REST API (Alexander Kukushkin)

  It helps to avoid blocking the main heartbeat loop if the system is under stress.

- Enrich some endpoints with the ``name`` of the node (sskserk)

  For the monitoring endpoint ``name`` is added next to the ``scope`` and for metrics endpoint the ``name`` is added to tags.

- Ensure strict failover/switchover difference (Polina Bungina)

  Be more precise in log messages and allow failing over to an asynchronous node in a healthy synchronous cluster.

- Make permanent physical replication slots behave similarly to permanent logical slots (Alexander Kukushkin)

  Create permanent physical replication slots on all nodes that are allowed to become the leader and use ``pg_replication_slot_advance()`` function to advance ``restart_lsn`` for slots on standby nodes.

- Add capability of specifying namespace through ``--dcs`` argument in ``patronictl`` (Israel Barth Rubio)

  It could be handy if ``patronictl`` is used without a configuration file.

- Add support for additional parameters in custom bootstrap configuration (Israel Barth Rubio)

  Previously it was only possible to add custom arguments to the ``command`` and now one could list them as a mapping.


**Improvements**

- Set ``citus.local_hostname`` GUC to the same value which is used by Patroni to connect to the Postgres (Alexander Kukushkin)

  There are cases when Citus wants to have a connection to the local Postgres. By default it uses ``localhost``, which is not always available.


**Bugfixes**

- Ignore ``synchronous_mode`` setting in a standby cluster (Polina Bungina)

  Postgres doesn't support cascading synchronous replication and not ignoring ``synchronous_mode`` was breaking a switchover in a standby cluster.

- Handle SIGCHLD for ``on_reload`` callback (Alexander Kukushkin)

  Not doing so results in a zombie process, which is reaped only when the next ``on_reload`` is executed.

- Handle ``AuthOldRevision`` error when working with Etcd v3 (Alexander Kukushkin, Kenny Do)

  The error is raised if Etcd is configured to use JWT and when the user database in Etcd is updated.


Version 3.1.2
-------------

**Bugfixes**

- Fixed bug with ``wal_keep_size`` checks (Alexander Kukushkin)

  The ``wal_keep_size`` is a GUC that normally has a unit and Patroni was failing to cast its value to ``int``. As a result the value of ``bootstrap.dcs`` was not written to the ``/config`` key afterwards.

- Detect and resolve inconsistencies between ``/sync`` key and ``synchronous_standby_names`` (Alexander Kukushkin)

  Normally, Patroni updates ``/sync`` and ``synchronous_standby_names`` in a very specific order, but in case of a bug or when someone manually reset ``synchronous_standby_names``, Patroni was getting into an inconsistent state. As a result it was possible that the failover happens to an asynchronous node.

- Read GUC's values when joining running Postgres (Alexander Kukushkin)

  When restarted in ``pause``, Patroni was discarding the ``synchronous_standby_names`` GUC from the ``postgresql.conf``. To solve it and avoid similar issues, Patroni will read GUC's value if it is joining an already running Postgres.

- Silenced annoying warnings when checking for node uniqueness (Alexander Kukushkin)

  ``WARNING`` messages are produced by ``urllib3`` if Patroni is quickly restarted.


Version 3.1.1
-------------

**Bugfixes**

- Reset failsafe state on promote (ChenChangAo)

  If switchover/failover happened shortly after failsafe mode had been activated, the newly promoted primary was demoting itself after failsafe becomes inactive.

- Silence useless warnings in ``patronictl`` (Alexander Kukushkin)

  If ``patronictl`` uses the same patroni.yaml file as Patroni and can access ``PGDATA`` directory it might have been showing annoying warnings about incorrect values in the global configuration.

- Explicitly enable synchronous mode for a corner case (Alexander Kukushkin)

  Synchronous mode effectively was never activated if there are no replicas streaming from the primary.

- Fixed bug with ``0`` integer values validation (Israel Barth Rubio)

  In most cases, it didn't cause any issues, just warnings.

- Don't return logical slots for standby cluster (Alexander Kukushkin)

  Patroni can't create logical replication slots in the standby cluster, thus they should be ignored if they are defined in the global configuration.

- Avoid showing docstring in ``patronictl --help`` output (Israel Barth Rubio)

  The ``click`` module needs to get a special hint for that.

- Fixed bug with ``kubernetes.standby_leader_label_value`` (Alexander Kukushkin)

  This feature effectively never worked.

- Returned cluster system identifier to the ``patronictl list`` output (Polina Bungina)

  The problem was introduced while implementing the support for Citus, where we need to hide the identifier because it is different for coordinator and all workers.

- Override ``write_leader_optime`` method in Kubernetes implementation (Alexander Kukushkin)

  The method is supposed to write shutdown LSN to the leader Endpoint/ConfigMap when there are no healthy replicas available to become the new primary.

- Don't start stopped postgres in pause (Alexander Kukushkin)

  Due to a race condition, Patroni was falsely assuming that the standby should be restarted because some recovery parameters (``primary_conninfo`` or similar) were changed.

- Fixed bug in ``patronictl query`` command (Israel Barth Rubio)

  It didn't work when only ``-m`` argument was provided or when none of ``-r`` or ``-m`` were provided.

- Properly treat integer parameters that are used in the command line to start postgres (Polina Bungina)

  If values are supplied as strings and not casted to integer it was resulting in an incorrect calculation of ``max_prepared_transactions`` based on ``max_connections`` for Citus clusters.

- Don't rely on ``pg_stat_wal_receiver`` when deciding on ``pg_rewind`` (Alexander Kukushkin)

  It could happen that ``received_tli`` reported by ``pg_stat_wal_recevier`` is ahead of the actual replayed timeline, while the timeline reported by ``DENTIFY_SYSTEM`` via replication connection is always correct.


Version 3.1.0
-------------

**Breaking changes**

- Changed semantic of ``restapi.keyfile`` and ``restapi.certfile`` (Alexander Kukushkin)

  Previously Patroni was using ``restapi.keyfile`` and ``restapi.certfile`` as client certificates as a fallback if there were no respective configuration parameters in the ``ctl`` section.

.. warning::
    If you enabled client certificates validation (``restapi.verify_client`` is set to ``required``), you also **must** provide **valid client certificates** in the ``ctl.certfile``, ``ctl.keyfile``, ``ctl.keyfile_password``. If not provided, Patroni will not work correctly.


**New features**

- Make Pod role label configurable (Waynerv)

  Values could be customized using ``kubernetes.leader_label_value``, ``kubernetes.follower_label_value`` and ``kubernetes.standby_leader_label_value`` parameters. This feature will be very useful when we change the ``master`` role to the ``primary``. You can read more about the feature and migration steps :ref:`here <kubernetes_role_values>`.


**Improvements**

- Various improvements of ``patroni --validate-config`` (Alexander Kukushkin)

  Improved parameter validation for different DCS, ``bootstrap.dcs`` , ``ctl``, ``restapi``, and ``watchdog`` sections.

- Start Postgres not in recovery if it crashed during recovery while Patroni is running (Alexander Kukushkin)

  It may reduce recovery time and will help to prevent unnecessary timeline increments.

- Avoid unnecessary updates of ``/status`` key (Alexander Kukushkin)

  When there are no permanent logical slots Patroni was updating the ``/status`` on every heartbeat loop even when LSN on the primary didn't move forward.

- Don't allow stale primary to win the leader race (Alexander Kukushkin)

  If Patroni was hanging during a significant time due to lack of resources it will additionally check that no other nodes promoted Postgres before acquiring the leader lock.

- Implemented visibility of certain PostgreSQL parameters validation (Alexander Kukushkin, Feike Steenbergen)

  If validation of ``max_connections``, ``max_wal_senders``, ``max_prepared_transactions``, ``max_locks_per_transaction``, ``max_replication_slots``, or ``max_worker_processes`` failed Patroni was using some sane default value. Now in addition to that it will also show a warning.

- Set permissions for files and directories created in ``PGDATA`` (Alexander Kukushkin)

  All files created by Patroni had only owner read/write permissions. This behaviour was breaking backup tools that run under a different user and relying on group read permissions. Now Patroni honors permissions on ``PGDATA`` and correctly sets permissions on all directories and files it creates inside ``PGDATA``.


**Bugfixes**

- Run ``archive_command`` through shell (Waynerv)

  Patroni might archive some WAL segments before doing crash recovery in a single-user mode or before ``pg_rewind``. If the archive_command contains some shell operators, like ``&&`` it didn't work with Patroni.

- Fixed "on switchover" shutdown checks (Polina Bungina)

  It was possible that specified candidate is still streaming and didn't received shut down checking but the leader key was removed because some other nodes were healthy.

- Fixed "is primary" check (Alexander Kukushkin)

  During the leader race replicas were not able to recognize that Postgres on the old leader is still running as a primary.

- Fixed ``patronictl list`` (Alexander Kukushkin)

  The Cluster name field was missing in ``tsv``, ``json``, and ``yaml`` output formats.

- Fixed ``pg_rewind`` behaviour after pause (Alexander Kukushkin)

  Under certain conditions, Patroni wasn't able to join the false primary back to the cluster with ``pg_rewind`` after coming out of maintenance mode.

- Fixed bug in Etcd v3 implementation (Alexander Kukushkin)

  Invalidate internal KV cache if key update performed using ``create_revision``/``mod_revision`` field due to revision mismatch.

- Fixed behaviour of replicas in standby cluster in pause (Alexander Kukushkin)

  When the leader key expires replicas in standby cluster will not follow the remote node but keep ``primary_conninfo`` as it is.


Version 3.0.4
-------------

**New features**

- Make the replication status of standby nodes visible (Alexander Kukushkin)

  For PostgreSQL 9.6+ Patroni will report the replication state as ``streaming`` when the standby is streaming from the other node or ``in archive recovery`` when there is no replication connection and ``restore_command`` is set. The state is visible in ``member`` keys in DCS, in the REST API, and in ``patronictl list`` output.


**Improvements**

- Improved error messages with Etcd v3 (Alexander Kukushkin)

  When Etcd v3 cluster isn't accessible Patroni was reporting that it can't access ``/v2`` endpoints.

- Use quorum read in ``patronictl`` if it is possible (Alexander Kukushkin)

  Etcd or Consul clusters could be degraded to read-only, but from the ``patronictl`` view everything was fine. Now it will fail with the error.

- Prevent splitbrain from duplicate names in configuration (Mark Pekala)

  When starting Patroni will check if node with the same name is registered in DCS, and try to query its REST API. If REST API is accessible Patroni exits with an error. It will help to protect from the human error.

- Start Postgres not in recovery if it crashed while Patroni is running (Alexander Kukushkin)

  It may reduce recovery time and will help from unnecessary timeline increments.


**Bugfixes**

- REST API SSL certificate were not reloaded upon receiving a SIGHUP (Israel Barth Rubio)

  Regression was introduced in 3.0.3.

- Fixed integer GUCs validation for parameters like ``max_connections`` (Feike Steenbergen)

  Patroni didn't like quoted numeric values. Regression was introduced in 3.0.3.

- Fix issue with ``synchronous_mode`` (Alexander Kukushkin)

  Execute ``txid_current()`` with ``synchronous_commit=off`` so it doesn't accidentally wait for absent synchronous standbys when ``synchronous_mode_strict`` is enabled.


Version 3.0.3
-------------

**New features**

- Compatibility with PostgreSQL 16 beta1 (Alexander Kukushkin)

  Extended GUC's validator rules.

- Make PostgreSQL GUC's validator extensible (Israel Barth Rubio)

  Validator rules are loaded from YAML files located in ``patroni/postgresql/available_parameters/`` directory. Files are ordered in alphabetical order and applied one after another. It makes possible to have custom validators for non-standard Postgres distributions.

- Added ``restapi.request_queue_size`` option (Andrey Zhidenkov, Aleksei Sukhov)

  Sets request queue size for TCP socket used by Patroni REST API. Once the queue is full, further requests get a "Connection denied" error. The default value is 5.

- Call ``initdb`` directly when initializing a new cluster (Matt Baker)

  Previously it was called via ``pg_ctl``, what required a special quoting of parameters passed to ``initdb``.

- Added before stop hook (Le Duane)

  The hook could be configured via ``postgresql.before_stop`` and is executed right before ``pg_ctl stop``. The exit code doesn't impact shutdown process.

- Added support for custom Postgres binary names (Israel Barth Rubio, Polina Bungina)

  When using a custom Postgres distribution it may be the case that the Postgres binaries are compiled with different names other than the ones used by the community Postgres distribution. Custom binary names could be configured using ``postgresql.bin_name.*`` and ``PATRONI_POSTGRESQL_BIN_*`` environment variables.


**Improvements**

- Various improvements of ``patroni --validate-config`` (Polina Bungina)

  -  Make ``bootstrap.initdb`` optional. It is only required for new clusters, but ``patroni --validate-config`` was complaining if it was missing in the config.
  -  Don't error out when ``postgresql.bin_dir`` is empty or not set. Try to first find Postgres binaries in the default PATH instead.
  -  Make ``postgresql.authentication.rewind`` section optional. If it is missing, Patroni is using the superuser.

- Improved error reporting in ``patronictl`` (Israel Barth Rubio)

  The ``\n`` symbol was rendered as it is, instead of the actual newline symbol.


**Bugfixes**

- Fixed issue in Citus support (Alexander Kukushkin)

  If the REST API call from the promoted worker to the coordinator failed during switchover it was leaving the given Citus group blocked during indefinite time.

- Allow `etcd3` URL in `--dcs-url` option of `patronictl` (Israel Barth Rubio)

  If users attempted to pass a `etcd3` URL through `--dcs-url` option of `patronictl` they would face an exception.


Version 3.0.2
-------------

.. warning::
    Version 3.0.2 dropped support of Python older than 3.6.


**New features**

- Added sync standby replica status to ``/metrics`` endpoint (Thomas von Dein, Alexander Kukushkin)

  Before were only reporting ``primary``/``standby_leader``/``replica``.

- User-friendly handling of ``PAGER`` in ``patronictl`` (Israel Barth Rubio)

  It makes pager configurable via ``PAGER`` environment variable, which overrides default ``less`` and ``more``.

- Make K8s retriable HTTP status code configurable (Alexander Kukushkin)

  On some managed platforms it is possible to get status code ``401 Unauthorized``, which sometimes gets resolved after a few retries.


**Improvements**

- Set ``hot_standby`` to ``off`` during custom bootstrap only if ``recovery_target_action`` is set to ``promote`` (Alexander Kukushkin)

  It was necessary to make ``recovery_target_action=pause`` work correctly.

- Don't allow ``on_reload`` callback to kill other callbacks (Alexander Kukushkin)

  ``on_start``/``on_stop``/``on_role_change`` are usually used to add/remove Virtual IP and ``on_reload`` should not interfere with them.

- Switched to ``IMDSFetcher`` in aws callback example script (Polina Bungina)

  The ``IMDSv2`` requires a token to work with and the ``IMDSFetcher`` handles it transparently.


**Bugfixes**

- Fixed ``patronictl switchover`` on Citus cluster running on Kubernetes (Lukáš Lalinský)

  It didn't work for namespaces different from ``default``.

- Don't write to ``PGDATA`` if major version is not known (Alexander Kukushkin)

  If right after the start ``PGDATA`` was empty (maybe wasn't yet mounted), Patroni was making a false assumption about PostgreSQL version and falsely creating ``recovery.conf`` file even if the actual major version is v10+.

- Fixed bug with Citus metadata after coordinator failover (Alexander Kukushkin)

  The ``citus_set_coordinator_host()`` call doesn't cause metadata sync and the change was invisible on worker nodes. The issue is solved by switching to ``citus_update_node()``.

- Use etcd hosts listed in the config file as a fallback when all etcd nodes "failed" (Alexander Kukushkin)

  The etcd cluster may change topology over time and Patroni tries to follow it. If at some point all nodes became unreachable Patroni will use a combination of nodes from the config plus the last known topology when trying to reconnect.


Version 3.0.1
-------------

**Bugfixes**

- Pass proper role name to an ``on_role_change`` callback script'. (Alexander Kukushkin, Polina Bungina)

  Patroni used to erroneously pass ``promoted`` role to an ``on_role_change`` callback script on promotion. The passed role name changed back to ``master``. This regression was introduced in 3.0.0.


Version 3.0.0
-------------

This version adds integration with `Citus <https://www.citusdata.com>`__ and makes it possible to survive temporary DCS outages without demoting primary.

.. warning::
   - Version 3.0.0 is the last release supporting Python 2.7. Upcoming release will drop support of Python versions older than 3.7.

   - The RAFT support is deprecated. We will do our best to maintain it, but take neither guarantee nor responsibility for possible issues.

   - This version is the first step in getting rid of the "master", in favor of "primary". Upgrading to the next major release will work reliably only if you run at least 3.0.0.


**New features**

- DCS failsafe mode (Alexander Kukushkin, Polina Bungina)

  If the feature is enabled it will allow Patroni cluster to survive temporary DCS outages. You can find more details in the :ref:`documentation <dcs_failsafe_mode>`.

- Citus support (Alexander Kukushkin, Polina Bungina, Jelte Fennema)

  Patroni enables easy deployment and management of `Citus <https://www.citusdata.com>`__ clusters with HA. Please check :ref:`here <citus>` page for more information.


**Improvements**

- Suppress recurring errors when dropping unknown but active replication slots (Michael Banck)

  Patroni will still write these logs, but only in DEBUG.

- Run only one monitoring query per HA loop (Alexander Kukushkin)

  It wasn't the case if synchronous replication is enabled.

- Keep only latest failed data directory (William Albertus Dembo)

  If bootstrap failed Patroni used to rename $PGDATA folder with timestamp suffix. From now on the suffix will be ``.failed`` and if such folder exists it is removed before renaming.

- Improved check of synchronous replication connections (Alexander Kukushkin)

  When the new host is added to the ``synchronous_standby_names`` it will be set as synchronous in DCS only when it managed to catch up with the primary in addition to ``pg_stat_replication.sync_state = 'sync'``.


**Removed functionality**

- Remove ``patronictl scaffold`` (Alexander Kukushkin)

  The only reason for having it was a hacky way of running standby clusters.


Version 2.1.7
-------------

**Bugfixes**

- Fixed little incompatibilities with legacy python modules (Alexander Kukushkin)

  They prevented from building/running Patroni on Debian buster/Ubuntu bionic.


Version 2.1.6
-------------

**Improvements**

- Fix annoying exceptions on ssl socket shutdown (Alexander Kukushkin)

  The HAProxy is closing connections as soon as it got the HTTP Status code leaving no time for Patroni to properly shutdown SSL connection.

- Adjust example Dockerfile for arm64 (Polina Bungina)

  Remove explicit ``amd64`` and ``x86_64``, don't remove ``libnss_files.so.*``.


**Security improvements**

- Enforce ``search_path=pg_catalog`` for non-replication connections (Alexander Kukushkin)

  Since Patroni is heavily relying on superuser connections, we want to protect it from the possible attacks carried out using user-defined functions and/or operators in ``public`` schema with the same name and signature as the corresponding objects in ``pg_catalog``. For that, ``search_path=pg_catalog`` is enforced for all connections created by Patroni (except replication connections).

- Prevent passwords from being recorded in ``pg_stat_statements`` (Feike Steenbergen)

  It is achieved by setting ``pg_stat_statements.track_utility=off`` when creating users.


**Bugfixes**

- Declare ``proxy_address`` as optional (Denis Laxalde)

  As it is effectively a non-required option.

- Improve behaviour of the insecure option (Alexander Kukushkin)

  Ctl's ``insecure`` option didn't work properly when client certificates were used for REST API requests.

- Take watchdog configuration from ``bootstrap.dcs`` when the new cluster is bootstrapped (Matt Baker)

  Patroni used to initially configure watchdog with defaults when bootstrapping a new cluster rather than taking configuration used to bootstrap the DCS.

- Fix the way file extensions are treated while finding executables in WIN32 (Martín Marqués)

  Only add ``.exe`` to a file name if it has no extension yet.

- Fix Consul TTL setup (Alexander Kukushkin)

  We used ``ttl/2.0`` when setting the value on the HTTPClient, but forgot to multiply the current value by 2 in the class' property. It was resulting in Consul TTL off by twice.


**Removed functionality**

- Remove ``patronictl configure`` (Polina Bungina)

  There is no more need for a separate ``patronictl`` config creation.


Version 2.1.5
-------------

This version enhances compatibility with PostgreSQL 15 and declares Etcd v3 support as production ready. The Patroni on Raft remains in Beta.

**New features**

- Improve ``patroni --validate-config`` (Denis Laxalde)

  Exit with code 1 if config is invalid and print errors to stderr.

- Don't drop replication slots in pause (Alexander Kukushkin)

  Patroni is automatically creating/removing physical replication slots when members are joining/leaving the cluster. In pause slots will no longer be removed.

- Support the ``HEAD`` request method for monitoring endpoints (Robert Cutajar)

  If used instead of ``GET`` Patroni will return only the HTTP Status Code.

- Support behave tests on Windows (Alexander Kukushkin)

  Emulate graceful Patroni shutdown (``SIGTERM``) on Windows by introduce the new REST API endpoint ``POST /sigterm``.

- Introduce ``postgresql.proxy_address`` (Alexander Kukushkin)

  It will be written to the member key in DCS as the ``proxy_url`` and could be used/useful for service discovery.


**Stability improvements**

- Call ``pg_replication_slot_advance()`` from a thread (Alexander Kukushkin)

  On busy clusters with many logical replication slots the ``pg_replication_slot_advance()`` call was affecting the main HA loop and could result in the member key expiration.

- Archive possibly missing WALs before calling ``pg_rewind`` on the old primary (Polina Bungina)

  If the primary crashed and was down during considerable time, some WAL files could be missing from archive and from the new primary. There is a chance that ``pg_rewind`` could remove these WAL files from the old primary making it impossible to start it as a standby. By archiving ``ready`` WAL files we not only mitigate this problem but in general improving continues archiving experience.

- Ignore ``403`` errors when trying to create Kubernetes Service (Nick Hudson, Polina Bungina)

  Patroni was spamming logs by unsuccessful attempts to create the service, which in fact could already exist.

- Improve liveness probe (Alexander Kukushkin)

  The liveness problem will start failing if the heartbeat loop is running longer than `ttl` on the primary or `2*ttl` on the replica. That will allow us to use it as an alternative for :ref:`watchdog <watchdog>` on Kubernetes.

- Make sure only sync node tries to grab the lock when switchover (Alexander Kukushkin, Polina Bungina)

  Previously there was a slim chance that up-to-date async member could become the leader if the manual switchover was performed without specifying the target.

- Avoid cloning while bootstrap is running (Ants Aasma)

  Do not allow a create replica method that does not require a leader to be triggered while the cluster bootstrap is running.

- Compatibility with kazoo-2.9.0 (Alexander Kukushkin)

  Depending on python version the ``SequentialThreadingHandler.select()`` method may raise ``TypeError`` and ``IOError`` exceptions if ``select()`` is called on the closed socket.

- Explicitly shut down SSL connection before socket shutdown (Alexander Kukushkin)

  Not doing it resulted in ``unexpected eof while reading`` errors with OpenSSL 3.0.

- Compatibility with `prettytable>=2.2.0` (Alexander Kukushkin)

  Due to the internal API changes the cluster name header was shown on the incorrect line.


**Bugfixes**

- Handle expired token for Etcd lease_grant (monsterxx03)

  In case of error get the new token and retry request.

- Fix bug in the ``GET /read-only-sync`` endpoint (Alexander Kukushkin)

  It was introduced in previous release and effectively never worked.

- Handle the case when data dir storage disappeared (Alexander Kukushkin)

  Patroni is periodically checking that the PGDATA is there and not empty, but in case of issues with storage the ``os.listdir()`` is raising the ``OSError`` exception, breaking the heart-beat loop.

- Apply ``master_stop_timeout`` when waiting for user backends to close (Alexander Kukushkin)

  Something that looks like user backend could be in fact a background worker (e.g., Citus Maintenance Daemon) that is failing to stop.

- Accept ``*:<port>`` for ``postgresql.listen`` (Denis Laxalde)

  The ``patroni --validate-config`` was complaining about it being invalid.

- Timeouts fixes in Raft (Alexander Kukushkin)

  When Patroni or patronictl are starting they try to get Raft cluster topology from known members. These calls were made without proper timeouts.

- Forcefully update consul service if token was changed (John A. Lotoski)

  Not doing so results in errors "rpc error making call: rpc error making call: ACL not found".


Version 2.1.4
-------------

**New features**

- Improve ``pg_rewind`` behavior on typical Debian/Ubuntu systems (Gunnar "Nick" Bluth)

  On Postgres setups that keep `postgresql.conf` outside of the data directory (e.g. Ubuntu/Debian packages), ``pg_rewind --restore-target-wal``  fails to figure out the value of the ``restore_command``.

- Allow setting ``TLSServerName`` on Consul service checks (Michael Gmelin)

  Useful when checks are performed by IP and the Consul ``node_name`` is not a FQDN.

- Added ``ppc64le`` support in watchdog (Jean-Michel Scheiwiler)

  And fixed watchdog support on some non-x86 platforms.

- Switched aws.py callback from ``boto`` to ``boto3`` (Alexander Kukushkin)

 ``boto``  2.x is abandoned since 2018 and fails with python 3.9.

- Periodically refresh service account token on K8s (Haitao Li)

  Since Kubernetes v1.21 service account tokens expire in 1 hour.

- Added ``/read-only-sync`` monitoring endpoint (Dennis4b)

  It is similar to the ``/read-only`` but includes only synchronous replicas.


**Stability improvements**

- Don't copy the logical replication slot to a replica if there is a configuration mismatch in the logical decoding setup with the primary (Alexander Kukushkin)

  A replica won't copy a logical replication slot from the primary anymore if the slot doesn't match the ``plugin`` or ``database`` configuration options. Previously, the check for whether the slot matches those configuration options was not performed until after the replica copied the slot and started with it, resulting in unnecessary and repeated restarts.

- Special handling of recovery configuration parameters for PostgreSQL v12+ (Alexander Kukushkin)

  While starting as replica Patroni should be able to update ``postgresql.conf`` and restart/reload if the leader address has changed by caching current parameters values instead of querying them from ``pg_settings``.

- Better handling of IPv6 addresses in the ``postgresql.listen`` parameters (Alexander Kukushkin)

  Since the ``listen`` parameter has a port, people try to put IPv6 addresses into square brackets, which were not correctly stripped when there is more than one IP in the list.

- Use ``replication`` credentials when performing divergence check only on PostgreSQL v10 and older (Alexander Kukushkin)

  If ``rewind`` is enabled, Patroni will again use either ``superuser`` or ``rewind`` credentials on newer Postgres versions.


**Bugfixes**

- Fixed missing import of ``dateutil.parser`` (Wesley Mendes)

  Tests weren't failing only because it was also imported from other modules.

- Ensure that ``optime`` annotation is a string (Sebastian Hasler)

  In certain cases Patroni was trying to pass it as numeric.

- Better handling of failed ``pg_rewind`` attempt (Alexander Kukushkin)

  If the primary becomes unavailable during ``pg_rewind``, ``$PGDATA`` will be left in a broken state. Following that,  Patroni will remove the data directory even if this is not allowed by the configuration.

- Don't remove ``slots`` annotations from the leader ``ConfigMap``/``Endpoint`` when PostgreSQL isn't ready (Alexander Kukushkin)

  If ``slots`` value isn't passed the annotation will keep the current value.

- Handle concurrency problem with K8s API watchers (Alexander Kukushkin)

  Under certain (unknown) conditions watchers might become stale; as a result, ``attempt_to_acquire_leader()`` method could fail due to the HTTP status code 409. In that case we reset watchers connections and restart from scratch.


Version 2.1.3
-------------

**New features**

- Added support for encrypted TLS keys for ``patronictl`` (Alexander Kukushkin)

  It could be configured via ``ctl.keyfile_password`` or the ``PATRONI_CTL_KEYFILE_PASSWORD`` environment variable.

- Added more metrics to the /metrics endpoint (Alexandre Pereira)

  Specifically, ``patroni_pending_restart`` and ``patroni_is_paused``.

- Make it possible to specify multiple hosts in the standby cluster configuration (Michael Banck)

  If the standby cluster is replicating from the Patroni cluster it might be nice to rely on client-side failover which is available in ``libpq`` since PostgreSQL v10. That is, the ``primary_conninfo`` on the standby leader and ``pg_rewind`` setting ``target_session_attrs=read-write`` in the connection string. The ``pgpass`` file will be generated with multiple lines (one line per host), and instead of calling ``CHECKPOINT`` on the primary cluster nodes the standby cluster will wait for ``pg_control`` to be updated.

**Stability improvements**

- Compatibility with legacy ``psycopg2`` (Alexander Kukushkin)

  For example, the ``psycopg2`` installed from Ubuntu 18.04 packages doesn't have the ``UndefinedFile`` exception yet.

- Restart ``etcd3`` watcher if all Etcd nodes don't respond (Alexander Kukushkin)

  If the watcher is alive the ``get_cluster()`` method continues returning stale information even if all Etcd nodes are failing.

- Don't remove the leader lock in the standby cluster while paused (Alexander Kukushkin)

  Previously the lock was maintained only by the node that was running as a primary and not a standby leader.

**Bugfixes**

- Fixed bug in the standby-leader bootstrap (Alexander Kukushkin)

  Patroni was considering bootstrap as failed if Postgres didn't start accepting connections after 60 seconds. The bug was introduced in the 2.1.2 release.

- Fixed bug with failover to a cascading standby (Alexander Kukushkin)

  When figuring out which slots should be created on cascading standby we forgot to take into account that the leader might be absent.

- Fixed small issues in Postgres config validator (Alexander Kukushkin)

  Integer parameters introduced in PostgreSQL v14 were failing to validate because min and max values were quoted in the validator.py

- Use replication credentials when checking leader status (Alexander Kukushkin)

  It could be that the ``remove_data_directory_on_diverged_timelines`` is set, but there is no ``rewind_credentials`` defined and superuser access between nodes is not allowed.

- Fixed "port in use" error on REST API certificate replacement (Ants Aasma)

  When switching certificates there was a race condition with a concurrent API request. If there is one active during the replacement period then the replacement will error out with a port in use error and Patroni gets stuck in a state without an active API server.

- Fixed a bug in cluster bootstrap if passwords contain ``%`` characters (Bastien Wirtz)

  The bootstrap method executes the ``DO`` block, with all parameters properly quoted, but the ``cursor.execute()`` method didn't like an empty list with parameters passed.

- Fixed the "AttributeError: no attribute 'leader'" exception (Hrvoje Milković)

  It could happen if the synchronous mode is enabled and the DCS content was wiped out.

- Fix bug in divergence timeline check (Alexander Kukushkin)

  Patroni was falsely assuming that timelines have diverged. For pg_rewind it didn't create any problem, but if pg_rewind is not allowed and the ``remove_data_directory_on_diverged_timelines`` is set, it resulted in reinitializing the former leader.


Version 2.1.2
-------------

**New features**

- Compatibility with ``psycopg>=3.0`` (Alexander Kukushkin)

  By default ``psycopg2`` is preferred. `psycopg>=3.0` will be used only if ``psycopg2`` is not available or its version is too old.

- Add ``dcs_last_seen`` field to the REST API (Michael Banck)

  This field notes the last time (as unix epoch) a cluster member has successfully communicated with the DCS. This is useful to identify and/or analyze network partitions.

- Release the leader lock when ``pg_controldata`` reports "shut down" (Alexander Kukushkin)

  To solve the problem of slow switchover/shutdown in case ``archive_command`` is slow/failing, Patroni will remove the leader key immediately after ``pg_controldata`` started reporting PGDATA as ``shut down`` cleanly and it verified that there is at least one replica that received all changes. If there are no replicas that fulfill this condition the leader key is not removed and the old behavior is retained, i.e. Patroni will keep updating the lock.

- Add ``sslcrldir`` connection parameter support (Kostiantyn Nemchenko)

  The new connection parameter was introduced in the PostgreSQL v14.

- Allow setting ACLs for ZNodes in Zookeeper (Alwyn Davis)

  Introduce a new configuration option ``zookeeper.set_acls`` so that Kazoo will apply a default ACL for each ZNode that it creates.


**Stability improvements**

- Delay the next attempt of recovery till next HA loop (Alexander Kukushkin)

  If Postgres crashed due to out of disk space (for example) and fails to start because of that Patroni is too eagerly trying to recover it flooding logs.

- Add log before demoting, which can take some time (Michael Banck)

  It can take some time for the demote to finish and it might not be obvious from looking at the logs what exactly is going on.

- Improve "I am" status messages (Michael Banck)

  ``no action. I am a secondary ({0})`` vs ``no action. I am ({0}), a secondary``

- Cast to int ``wal_keep_segments`` when converting to ``wal_keep_size`` (Jorge Solórzano)

  It is possible to specify ``wal_keep_segments`` as a string in the global :ref:`dynamic configuration <dynamic_configuration>` and due to Python being a dynamically typed language the string was simply multiplied. Example: ``wal_keep_segments: "100"`` was converted to ``100100100100100100100100100100100100100100100100MB``.

- Allow switchover only to sync nodes when synchronous replication is enabled (Alexander Kukushkin)

  In addition to that do the leader race only against known synchronous nodes.

- Use cached role as a fallback when Postgres is slow (Alexander Kukushkin)

  In some extreme cases Postgres could be so slow that the normal monitoring query does not finish in a few seconds. The ``statement_timeout`` exception not being properly handled could lead to the situation where Postgres was not demoted on time when the leader key expired or the update failed. In case of such exception Patroni will use the cached ``role`` to determine whether Postgres is running as a primary.

- Avoid unnecessary updates of the member ZNode (Alexander Kukushkin)

  If no values have changed in the members data, the update should not happen.

- Optimize checkpoint after promote (Alexander Kukushkin)

  Avoid doing ``CHECKPOINT`` if the latest timeline is already stored in ``pg_control``. It helps to avoid unnecessary ``CHECKPOINT`` right after initializing the new cluster with ``initdb``.

- Prefer members without ``nofailover`` when picking sync nodes (Alexander Kukushkin)

  Previously sync nodes were selected only based on the replication lag, hence the node with ``nofailover`` tag had the same chances to become synchronous as any other node. That behavior was confusing and dangerous at the same time because in case of a failed primary the failover could not happen automatically.

- Remove duplicate hosts from the etcd machine cache (Michael Banck)

  Advertised client URLs in the etcd cluster could be misconfigured. Removing duplicates in Patroni in this case is a low-hanging fruit.


**Bugfixes**

- Skip temporary replication slots while doing slot management (Alexander Kukushkin)

  Starting from v10 ``pg_basebackup`` creates a temporary replication slot for WAL streaming and Patroni was trying to drop it because the slot name looks unknown. In order to fix it, we skip all temporary slots when querying ``pg_stat_replication_slots`` view.

- Ensure ``pg_replication_slot_advance()`` doesn't timeout (Alexander Kukushkin)

  Patroni was using the default ``statement_timeout`` in this case and once the call failed there are very high chances that it will never recover, resulting in increased size of ``pg_wal`` and ``pg_catalog`` bloat.

- The ``/status`` wasn't updated on demote (Alexander Kukushkin)

  After demoting PostgreSQL the old leader updates the last LSN in DCS. Starting from ``2.1.0`` the new ``/status`` key was introduced, but the optime was still written to the ``/optime/leader``.

- Handle DCS exceptions when demoting (Alexander Kukushkin)

  While demoting the master due to failure to update the leader lock it could happen that DCS goes completely down and the ``get_cluster()`` call raises an exception. Not being handled properly it results in Postgres remaining stopped until DCS recovers.

- The ``use_unix_socket_repl`` didn't work is some cases (Alexander Kukushkin)

  Specifically, if ``postgresql.unix_socket_directories`` is not set. In this case Patroni is supposed to use the default value from ``libpq``.

- Fix a few issues with Patroni REST API (Alexander Kukushkin)

  The ``clusters_unlocked`` sometimes could be not defined, what resulted in exceptions in the ``GET /metrics`` endpoint. In addition to that the error handling method was assuming that the ``connect_address`` tuple always has two elements, while in fact there could be more in case of IPv6.

- Wait for newly promoted node to finish recovery before deciding to rewind (Alexander Kukushkin)

  It could take some time before the actual promote happens and the new timeline is created. Without waiting replicas could come to the conclusion that rewind isn't required.

- Handle missing timelines in a history file when deciding to rewind (Alexander Kukushkin)

  If the current replica timeline is missing in the history file on the primary the replica was falsely assuming that rewind isn't required.


Version 2.1.1
-------------

**New features**

- Support for ETCD SRV name suffix (David Pavlicek)

  Etcd allows to differentiate between multiple Etcd clusters under the same domain and from now on Patroni also supports it.

- Enrich history with the new leader (huiyalin525)

  It adds the new column to the ``patronictl history`` output.

- Make the CA bundle configurable for in-cluster Kubernetes config (Aron Parsons)

  By default Patroni is using ``/var/run/secrets/kubernetes.io/serviceaccount/ca.crt`` and this new feature allows specifying the custom ``kubernetes.cacert``.

- Support dynamically registering/deregistering as a Consul service and changing tags (Tommy Li)

  Previously it required Patroni restart.

**Bugfixes**

- Avoid unnecessary reload of REST API (Alexander Kukushkin)

  The previous release added a feature of reloading REST API certificates if changed on disk. Unfortunately, the reload was happening unconditionally right after the start.

- Don't resolve cluster members when ``etcd.use_proxies`` is set (Alexander Kukushkin)

  When starting up Patroni checks the healthiness of Etcd cluster by querying the list of members. In addition to that, it also tried to resolve their hostnames, which is not necessary when working with Etcd via proxy and was causing unnecessary warnings.

- Skip rows with NULL values in the ``pg_stat_replication`` (Alexander Kukushkin)

  It seems that the ``pg_stat_replication`` view could contain NULL values in the ``replay_lsn``, ``flush_lsn``, or ``write_lsn`` fields even when ``state = 'streaming'``.


Version 2.1.0
-------------

This version adds compatibility with PostgreSQL v14, makes logical replication slots to survive failover/switchover, implements support of allowlist for REST API, and also reducing the number of logs to one line per heart-beat.

**New features**

- Compatibility with PostgreSQL v14 (Alexander Kukushkin)

  Unpause WAL replay if Patroni is not in a "pause" mode itself. It could be "paused" due to the change of certain parameters like for example ``max_connections`` on the primary.

- Failover logical slots (Alexander Kukushkin)

  Make logical replication slots survive failover/switchover on PostgreSQL v11+. The replication slot if copied from the primary to the replica with restart and later the `pg_replication_slot_advance() <https://www.postgresql.org/docs/11/functions-admin.html#id-1.5.8.31.8.5.2.2.8.1.1>`__ function is used to move it forward. As a result, the slot will already exist before the failover and no events should be lost, but, there is a chance that some events could be delivered more than once.

- Implemented allowlist for Patroni REST API (Alexander Kukushkin)

  If configured, only IP's that matching rules would be allowed to call unsafe endpoints. In addition to that, it is possible to automatically include IP's of members of the cluster to the list.

- Added support of replication connections via unix socket (Mohamad El-Rifai)

  Previously Patroni was always using TCP for replication connection what could cause some issues with SSL verification. Using unix sockets allows exempt replication user from SSL verification.

- Health check on user-defined tags (Arman Jafari Tehrani)

  Along with :ref:`predefined tags: <tags_settings>` it is possible to specify any number of custom tags that become visible in the ``patronictl list`` output and in the REST API. From now on it is possible to use custom tags in health checks.

- Added Prometheus ``/metrics`` endpoint (Mark Mercado, Michael Banck)

  The endpoint exposing the same metrics as ``/patroni``.

- Reduced chattiness of Patroni logs (Alexander Kukushkin)

  When everything goes normal, only one line will be written for every run of HA loop.


**Breaking changes**

- The old ``permanent logical replication slots`` feature will no longer work with PostgreSQL v10 and older (Alexander Kukushkin)

  The strategy of creating the logical slots after performing a promotion can't guaranty that no logical events are lost and therefore disabled.

- The ``/leader`` endpoint always returns 200 if the node holds the lock (Alexander Kukushkin)

  Promoting the standby cluster requires updating load-balancer health checks, which is not very convenient and easy to forget. To solve it, we change the behavior of the ``/leader`` health check endpoint. It will return 200 without taking into account whether the cluster is normal or the ``standby_cluster``.


**Improvements in Raft support**

- Reliable support of Raft traffic encryption (Alexander Kukushkin)

  Due to the different issues in the ``PySyncObj`` the encryption support was very unstable

- Handle DNS issues in Raft implementation (Alexander Kukushkin)

  If ``self_addr`` and/or ``partner_addrs`` are configured using the DNS name instead of IP's the ``PySyncObj`` was effectively doing resolve only once when the object is created. It was causing problems when the same node was coming back online with a different IP.


**Stability improvements**

- Compatibility with ``psycopg2-2.9+`` (Alexander Kukushkin)

  In ``psycopg2`` the ``autocommit = True`` is ignored in the ``with connection`` block, which breaks replication protocol connections.

- Fix excessive HA loop runs with Zookeeper (Alexander Kukushkin)

  Update of member ZNodes was causing a chain reaction and resulted in running the HA loops multiple times in a row.

- Reload if REST API certificate is changed on disk (Michael Todorovic)

  If the REST API certificate file was updated in place Patroni didn't perform a reload.

- Don't create pgpass dir if kerberos auth is used (Kostiantyn Nemchenko)

  Kerberos and password authentication are mutually exclusive.

- Fixed little issues with custom bootstrap (Alexander Kukushkin)

  Start Postgres with ``hot_standby=off`` only when we do a PITR and restart it after PITR is done.


**Bugfixes**

- Compatibility with ``kazoo-2.7+`` (Alexander Kukushkin)

  Since Patroni is handling retries on its own, it is relying on the old behavior of ``kazoo`` that requests to a Zookeeper cluster are immediately discarded when there are no connections available.

- Explicitly request the version of Etcd v3 cluster when it is known that we are connecting via proxy (Alexander Kukushkin)

  Patroni is working with Etcd v3 cluster via gPRC-gateway and it depending on the cluster version different endpoints (``/v3``, ``/v3beta``, or ``/v3alpha``) must be used. The version was resolved only together with the cluster topology, but since the latter was never done when connecting via proxy.


Version 2.0.2
-------------

**New features**

- Ability to ignore externally managed replication slots (James Coleman)

  Patroni is trying to remove any replication slot which is unknown to it, but there are certainly cases when replication slots should be managed externally. From now on it is possible to configure slots that should not be removed.

- Added support for cipher suite limitation for REST API (Gunnar "Nick" Bluth)

  It could be configured via ``restapi.ciphers`` or the ``PATRONI_RESTAPI_CIPHERS`` environment variable.

- Added support for encrypted TLS keys for REST API (Jonathan S. Katz)

  It could be configured via ``restapi.keyfile_password`` or the ``PATRONI_RESTAPI_KEYFILE_PASSWORD`` environment variable.

- Constant time comparison of REST API authentication credentials (Alex Brasetvik)

  Use ``hmac.compare_digest()`` instead of ``==``, which is vulnerable to timing attack.

- Choose synchronous nodes based on replication lag (Krishna Sarabu)

  If the replication lag on the synchronous node starts exceeding the configured threshold it could be demoted to asynchronous and/or replaced by the other node. Behaviour is controlled with ``maximum_lag_on_syncnode``.


**Stability improvements**

- Start postgres with ``hot_standby = off`` when doing custom bootstrap (Igor Yanchenko)

  During custom bootstrap Patroni is restoring the basebackup, starting Postgres up, and waiting until recovery finishes. Some PostgreSQL parameters on the standby can't be smaller than on the primary and if the new value (restored from WAL) is higher than the configured one, Postgres panics and stops. In order to avoid such behavior we will do custom bootstrap without ``hot_standby`` mode.

- Warn the user if the required watchdog is not healthy (Nicolas Thauvin)

  When the watchdog device is not writable or missing in required mode, the member cannot be promoted. Added a warning to show the user where to search for this misconfiguration.

- Better verbosity for single-user mode recovery (Alexander Kukushkin)

  If Patroni notices that PostgreSQL wasn't shutdown clearly, in certain cases the crash-recovery is executed by starting Postgres in single-user mode. It could happen that the recovery failed (for example due to the lack of space on disk) but errors were swallowed.

- Added compatibility with ``python-consul2`` module (Alexander Kukushkin, Wilfried Roset)

  The good old ``python-consul`` is not maintained since a few years, therefore someone created a fork with new features and bug-fixes.

- Don't use ``bypass_api_service`` when running ``patronictl`` (Alexander Kukushkin)

  When a K8s pod is running in a non-``default`` namespace it does not necessarily have enough permissions to query the ``kubernetes`` endpoint. In this case Patroni shows the warning and ignores the ``bypass_api_service`` setting. In case of ``patronictl`` the warning was a bit annoying.

- Create ``raft.data_dir`` if it doesn't exists or make sure that it is writable (Mark Mercado)

  Improves user-friendliness and usability.


**Bugfixes**

- Don't interrupt restart or promote if lost leader lock in pause (Alexander Kukushkin)

  In pause it is allowed to run postgres as primary without lock.

- Fixed issue with ``shutdown_request()`` in the REST API (Nicolas Limage)

  In order to improve handling of SSL connections and delay the handshake until thread is started Patroni overrides a few methods in the ``HTTPServer``. The ``shutdown_request()`` method was forgotten.

- Fixed issue with sleep time when using Zookeeper (Alexander Kukushkin)

  There were chances that Patroni was sleeping up to twice longer between running HA code.

- Fixed invalid ``os.symlink()`` calls when moving data directory after failed bootstrap (Andrew L'Ecuyer)

  If the bootstrap failed Patroni is renaming data directory, pg_wal, and all tablespaces. After that it updates symlinks so filesystem remains consistent. The symlink creation was failing due to the ``src`` and ``dst`` arguments being swapped.

- Fixed bug in the post_bootstrap() method (Alexander Kukushkin)

  If the superuser password wasn't configured Patroni was failing to call the ``post_init`` script and therefore the whole bootstrap was failing.

- Fixed an issues with pg_rewind in the standby cluster (Alexander Kukushkin)

  If the superuser name is different from Postgres, the ``pg_rewind`` in the standby cluster was failing because the connection string didn't contain the database name.

- Exit only if authentication with Etcd v3 explicitly failed (Alexander Kukushkin)

  On start Patroni performs discovery of Etcd cluster topology and authenticates if it is necessarily. It could happen that one of etcd servers is not accessible, Patroni was trying to perform authentication on this server and failing instead of retrying with the next node.

- Handle case with psutil cmdline() returning empty list (Alexander Kukushkin)

  Zombie processes are still postmasters children, but they don't have cmdline()

- Treat ``PATRONI_KUBERNETES_USE_ENDPOINTS`` environment variable as boolean (Alexander Kukushkin)

  Not doing so was making impossible disabling ``kubernetes.use_endpoints`` via environment.

- Improve handling of concurrent endpoint update errors (Alexander Kukushkin)

  Patroni will explicitly query the current endpoint object, verify that the current pod still holds the leader lock and repeat the update.


Version 2.0.1
-------------

**New features**

- Use ``more`` as pager in ``patronictl edit-config`` if ``less`` is not available (Pavel Golub)

  On Windows it would be the ``more.com``. In addition to that, ``cdiff`` was changed to ``ydiff`` in ``requirements.txt``, but ``patronictl`` still supports both for compatibility.

- Added support of ``raft`` ``bind_addr`` and ``password`` (Alexander Kukushkin)

  ``raft.bind_addr`` might be useful when running behind NAT. ``raft.password`` enables traffic encryption (requires the ``cryptography`` module).

- Added ``sslpassword`` connection parameter support (Kostiantyn Nemchenko)

  The connection parameter was introduced in PostgreSQL 13.

**Stability improvements**

- Changed the behavior in pause (Alexander Kukushkin)

  1. Patroni will not call the ``bootstrap`` method if the ``PGDATA`` directory is missing/empty.
  2. Patroni will not exit on sysid mismatch in pause, only log a warning.
  3. The node will not try to grab the leader key in pause mode if Postgres is running not in recovery (accepting writes) but the sysid doesn't match with the initialize key.

- Apply ``master_start_timeout`` when executing crash recovery (Alexander Kukushkin)

  If Postgres crashed on the leader node, Patroni does a crash-recovery by starting Postgres in single-user mode. During the crash-recovery the leader lock is being updated. If the crash-recovery didn't finish in ``master_start_timeout`` seconds, Patroni will stop it forcefully and release the leader lock.

- Removed the ``secure`` extra from the ``urllib3`` requirements (Alexander Kukushkin)

  The only reason for adding it there was the ``ipaddress`` dependency for python 2.7.

**Bugfixes**

- Fixed a bug in the ``Kubernetes.update_leader()`` (Alexander Kukushkin)

  An unhandled exception was preventing demoting the primary when the update of the leader object failed.

- Fixed hanging ``patronictl`` when RAFT is being used (Alexander Kukushkin)

  When using ``patronictl`` with Patroni config, ``self_addr`` should be added to the ``partner_addrs``.

- Fixed bug in ``get_guc_value()`` (Alexander Kukushkin)

  Patroni was failing to get the value of ``restore_command`` on PostgreSQL 12, therefore fetching missing WALs for ``pg_rewind`` didn't work.


Version 2.0.0
-------------

This version enhances compatibility with PostgreSQL 13, adds support of multiple synchronous standbys, has significant improvements in handling of ``pg_rewind``, adds support of Etcd v3 and Patroni on pure RAFT (without Etcd, Consul, or Zookeeper), and makes it possible to optionally call the ``pre_promote`` (fencing) script.

**PostgreSQL 13 support**

- Don't fire ``on_reload`` when promoting to ``standby_leader`` on PostgreSQL 13+ (Alexander Kukushkin)

  When promoting to ``standby_leader`` we change ``primary_conninfo``, update the role and reload Postgres. Since ``on_role_change`` and ``on_reload`` effectively duplicate each other, Patroni will call only ``on_role_change``.

- Added support for ``gssencmode`` and ``channel_binding`` connection parameters (Alexander Kukushkin)

  PostgreSQL 12 introduced ``gssencmode`` and 13 ``channel_binding`` connection parameters and now they can be used if defined in the ``postgresql.authentication`` section.

- Handle renaming of ``wal_keep_segments`` to ``wal_keep_size`` (Alexander Kukushkin)

  In case of misconfiguration (``wal_keep_segments`` on 13 and ``wal_keep_size`` on older versions) Patroni will automatically adjust the configuration.

- Use ``pg_rewind`` with ``--restore-target-wal`` on 13 if possible (Alexander Kukushkin)

  On PostgreSQL 13 Patroni checks if ``restore_command`` is configured and tells ``pg_rewind`` to use it.


**New features**

- [BETA] Implemented support of Patroni on pure RAFT (Alexander Kukushkin)

  This makes it possible to run Patroni without 3rd party dependencies, like Etcd, Consul, or Zookeeper. For HA you will have to run either three Patroni nodes or two nodes with Patroni and one node with ``patroni_raft_controller``. For more information please check the :ref:`documentation <raft_settings>`.

- [BETA] Implemented support for Etcd v3 protocol via gPRC-gateway (Alexander Kukushkin)

  Etcd 3.0 was released more than four years ago and Etcd 3.4 has v2 disabled by default. There are also chances that v2 will be completely removed from Etcd, therefore we implemented support of Etcd v3 in Patroni. In order to start using it you have to explicitly create the ``etcd3`` section is the Patroni configuration file.

- Supporting multiple synchronous standbys (Krishna Sarabu)

  It allows running a cluster with more than one synchronous replicas. The maximum number of synchronous replicas is controlled by the new parameter ``synchronous_node_count``. It is set to 1 by default and has no effect when the ``synchronous_mode`` is set to ``off``.

- Added possibility to call the ``pre_promote`` script (Sergey Dudoladov)

  Unlike callbacks, the ``pre_promote`` script is called synchronously after acquiring the leader lock, but before promoting Postgres. If the script fails or exits with a non-zero exitcode, the current node will release the leader lock.

- Added support for configuration directories (Floris van Nee)

  YAML files in the directory loaded and applied in alphabetical order.

- Advanced validation of PostgreSQL parameters (Alexander Kukushkin)

  In case the specific parameter is not supported by the current PostgreSQL version or when its value is incorrect, Patroni will remove the parameter completely or try to fix the value.

- Wake up the main thread when the forced checkpoint after promote completed (Alexander Kukushkin)

  Replicas are waiting for checkpoint indication via member key of the leader in DCS. The key is normally updated only once per HA loop. Without waking the main thread up, replicas will have to wait up to ``loop_wait`` seconds longer than necessary.

- Use of ``pg_stat_wal_recevier`` view on 9.6+ (Alexander Kukushkin)

  The view contains up-to-date values of ``primary_conninfo`` and ``primary_slot_name``, while the contents of ``recovery.conf`` could be stale.

- Improved handing of IPv6 addresses in the Patroni config file (Mateusz Kowalski)

  The IPv6 address is supposed to be enclosed into square brackets, but Patroni was expecting to get it plain. Now both formats are supported.

- Added Consul ``service_tags`` configuration parameter (Robert Edström)

  They are useful for dynamic service discovery, for example by load balancers.

- Implemented SSL support for Zookeeper (Kostiantyn Nemchenko)

  It requires ``kazoo>=2.6.0``.

- Implemented ``no_params`` option for custom bootstrap method (Kostiantyn Nemchenko)

  It allows calling ``wal-g``, ``pgBackRest`` and other backup tools without wrapping them into shell scripts.

- Move WAL and tablespaces after a failed init (Feike Steenbergen)

  When doing ``reinit``, Patroni was already removing not only ``PGDATA`` but also the symlinked WAL directory and tablespaces. Now the ``move_data_directory()`` method will do a similar job, i.e. rename WAL directory and tablespaces and update symlinks in PGDATA.


**Improved in pg_rewind support**

- Improved timeline divergence check (Alexander Kukushkin)

  We don't need to rewind when the replayed location on the replica is not ahead of the switchpoint or the end of the checkpoint record on the former primary is the same as the switchpoint. In order to get the end of the checkpoint record we use ``pg_waldump`` and parse its output.

- Try to fetch missing WAL if ``pg_rewind`` complains about it (Alexander Kukushkin)

  It could happen that the WAL segment required for ``pg_rewind`` doesn't exist in the ``pg_wal`` directory anymore and therefore ``pg_rewind`` can't find the checkpoint location before the divergence point. Starting from PostgreSQL 13 ``pg_rewind`` could use ``restore_command`` for fetching missing WALs. For older PostgreSQL versions Patroni parses the errors of a failed rewind attempt and tries to fetch the missing WAL by calling the ``restore_command`` on its own.

- Detect a new timeline in the standby cluster and trigger rewind/reinitialize if necessary (Alexander Kukushkin)

  The ``standby_cluster`` is decoupled from the primary cluster and therefore doesn't immediately know about leader elections and timeline switches. In order to detect the fact, the ``standby_leader`` periodically checks for new history files in ``pg_wal``.

- Shorten and beautify history log output (Alexander Kukushkin)

  When Patroni is trying to figure out the necessity of ``pg_rewind``, it could write the content of the history file from the primary into the log. The history file is growing with every failover/switchover and eventually starts taking up too many lines, most of which are not so useful. Instead of showing the raw data, Patroni will show only 3 lines before the current replica timeline and 2 lines after.


**Improvements on K8s**

- Get rid of ``kubernetes`` python module (Alexander Kukushkin)

  The official python kubernetes client contains a lot of auto-generated code and therefore very heavy. Patroni uses only a small fraction of K8s API endpoints and implementing support for them wasn't hard.

- Make it possible to bypass the ``kubernetes`` service (Alexander Kukushkin)

  When running on K8s, Patroni is usually communicating with the K8s API via the ``kubernetes`` service, the address of which is exposed in the ``KUBERNETES_SERVICE_HOST`` environment variable. Like any other service, the ``kubernetes`` service is handled by ``kube-proxy``, which in turn, depending on the configuration, is either relying on a userspace program or ``iptables`` for traffic routing. Skipping the intermediate component and connecting directly to the K8s master nodes allows us to implement a better retry strategy and mitigate risks of demoting Postgres when K8s master nodes are upgraded.

- Sync HA loops of all pods of a Patroni cluster (Alexander Kukushkin)

  Not doing so was increasing failure detection time from ``ttl`` to ``ttl + loop_wait``.

- Populate ``references`` and ``nodename`` in the subsets addresses on K8s (Alexander Kukushkin)

  Some load-balancers are relying on this information.

- Fix possible race conditions in the ``update_leader()`` (Alexander Kukushkin)

  The concurrent update of the leader configmap or endpoint happening outside of Patroni might cause the ``update_leader()`` call to fail. In this case Patroni rechecks that the current node is still owning the leader lock and repeats the update.

- Explicitly disallow patching non-existent config (Alexander Kukushkin)

  For DCS other than ``kubernetes`` the PATCH call is failing with an exception due to ``cluster.config`` being ``None``, but on Kubernetes it was happily creating the config annotation and preventing writing bootstrap configuration after the bootstrap finished.

- Fix bug in ``pause`` (Alexander Kukushkin)

  Replicas were removing ``primary_conninfo`` and restarting Postgres when the leader key was absent, but they should do nothing.


**Improvements in REST API**

- Defer TLS handshake until worker thread has started (Alexander Kukushkin, Ben Harris)

  If the TLS handshake was done in the API thread and the client-side didn't send any data, the API thread was blocked (risking DoS).

- Check ``basic-auth`` independently from client certificate in REST API (Alexander Kukushkin)

  Previously only the client certificate was validated. Doing two checks independently is an absolutely valid use-case.

- Write double ``CRLF`` after HTTP headers of the ``OPTIONS`` request (Sergey Burladyan)

  HAProxy was happy with a single ``CRLF``, while Consul health-check complained about broken connection and unexpected EOF.

- ``GET /cluster`` was showing stale members info for Zookeeper (Alexander Kukushkin)

  The endpoint was using the Patroni internal cluster view. For Patroni itself it didn't cause any issues, but when exposed to the outside world we need to show up-to-date information, especially replication lag.

- Fixed health-checks for standby cluster (Alexander Kukushkin)

  The ``GET /standby-leader`` for a master and ``GET /master`` for a ``standby_leader`` were incorrectly responding with 200.

- Implemented ``DELETE /switchover`` (Alexander Kukushkin)

  The REST API call deletes the scheduled switchover.

- Created ``/readiness`` and ``/liveness`` endpoints (Alexander Kukushkin)

  They could be useful to eliminate "unhealthy" pods from subsets addresses when the K8s service is used with label selectors.

- Enhanced ``GET /replica`` and ``GET /async`` REST API health-checks (Krishna Sarabu, Alexander Kukushkin)

  Checks now support optional keyword ``?lag=<max-lag>`` and will respond with 200 only if the lag is smaller than the supplied value. If relying on this feature please keep in mind that information about WAL position on the leader is updated only every ``loop_wait`` seconds!

- Added support for user defined HTTP headers in the REST API response (Yogesh Sharma)

  This feature might be useful if requests are made from a browser.


**Improvements in patronictl**

- Don't try to call non-existing leader in ``patronictl pause`` (Alexander Kukushkin)

  While pausing a cluster without a leader on K8s, ``patronictl`` was showing warnings that member "None" could not be accessed.

- Handle the case when member ``conn_url`` is missing (Alexander Kukushkin)

  On K8s it is possible that the pod doesn't have the necessary annotations because Patroni is not yet running. It was making ``patronictl`` to fail.

- Added ability to print ASCII cluster topology (Maxim Fedotov, Alexander Kukushkin)

  It is very useful to get overview of the cluster with cascading replication.

- Implement ``patronictl flush switchover`` (Alexander Kukushkin)

  Before that ``patronictl flush`` only supported cancelling scheduled restarts.


**Bugfixes**

- Attribute error during bootstrap of the cluster with existing PGDATA (Krishna Sarabu)

  When trying to create/update the ``/history`` key, Patroni was accessing the ``ClusterConfig`` object which wasn't created in DCS yet.

- Improved exception handling in Consul (Alexander Kukushkin)

  Unhandled exception in the ``touch_member()`` method caused the whole Patroni process to crash.

- Enforce ``synchronous_commit=local`` for the ``post_init`` script (Alexander Kukushkin)

  Patroni was already doing that when creating users (``replication``, ``rewind``), but missing it in the case of ``post_init`` was an oversight. As a result, if the script wasn't doing it internally on it's own the bootstrap in ``synchronous_mode`` wasn't able to finish.

- Increased ``maxsize`` in the Consul pool manager (ponvenkates)

  With the default ``size=1`` some warnings were generated.

- Patroni was wrongly reporting Postgres as running (Alexander Kukushkin)

  The state wasn't updated when for example Postgres crashed due to an out-of-disk error.

- Put ``*`` into ``pgpass`` instead of missing or empty values (Alexander Kukushkin)

  If for example the ``standby_cluster.port`` is not specified, the ``pgpass`` file was incorrectly generated.

- Skip physical replication slot creation on the leader node with special characters (Krishna Sarabu)

  Patroni appeared to be creating a dormant slot (when ``slots`` defined) for the leader node when the name contained special chars such as '-'  (for e.g. "abc-us-1").

- Avoid removing non-existent ``pg_hba.conf`` in the custom bootstrap (Krishna Sarabu)

  Patroni was failing if ``pg_hba.conf`` happened to be located outside of the ``pgdata`` dir after custom bootstrap.


Version 1.6.5
-------------

**New features**

- Master stop timeout (Krishna Sarabu)

  The number of seconds Patroni is allowed to wait when stopping Postgres. Effective only when ``synchronous_mode`` is enabled. When set to value greater than 0 and the ``synchronous_mode`` is enabled, Patroni sends ``SIGKILL`` to the postmaster if the stop operation is running for more than the value set by ``master_stop_timeout``. Set the value according to your durability/availability tradeoff. If the parameter is not set or set to non-positive value, ``master_stop_timeout`` does not have an effect.

- Don't create permanent physical slot with name of the primary (Alexander Kukushkin)

  It is a common problem that the primary recycles WAL segments while the replica is down. Now we have a good solution for static clusters, with a fixed number of nodes and names that never change. You just need to list the names of all nodes in the ``slots`` so the primary will not remove the slot when the node is down (not registered in DCS).

- First draft of Config Validator (Igor Yanchenko)

  Use ``patroni --validate-config patroni.yaml`` in order to validate Patroni configuration.

- Possibility to configure max length of timelines history (Krishna Sarabu)

  Patroni writes the history of failovers/switchovers into the ``/history`` key in DCS. Over time the size of this key becomes big, but in most cases only the last few lines are interesting. The ``max_timelines_history`` parameter allows to specify the maximum number of timeline history items to be kept in DCS.

- Kazoo 2.7.0 compatibility (Danyal Prout)

  Some non-public methods in Kazoo changed their signatures, but Patroni was relying on them.


**Improvements in patronictl**

- Show member tags (Kostiantyn Nemchenko, Alexander Kukushkin)

  Tags are configured individually for every node and there was no easy way to get an overview of them

- Improve members output (Alexander Kukushkin)

  The redundant cluster name won't be shown anymore on every line, only in the table header.

.. code-block:: bash

    $ patronictl list
    + Cluster: batman (6813309862653668387) +---------+----+-----------+---------------------+
    |    Member   |      Host      |  Role  |  State  | TL | Lag in MB | Tags                |
    +-------------+----------------+--------+---------+----+-----------+---------------------+
    | postgresql0 | 127.0.0.1:5432 | Leader | running |  3 |           | clonefrom: true     |
    |             |                |        |         |    |           | noloadbalance: true |
    |             |                |        |         |    |           | nosync: true        |
    +-------------+----------------+--------+---------+----+-----------+---------------------+
    | postgresql1 | 127.0.0.1:5433 |        | running |  3 |       0.0 |                     |
    +-------------+----------------+--------+---------+----+-----------+---------------------+

- Fail if a config file is specified explicitly but not found (Kaarel Moppel)

  Previously ``patronictl`` was only reporting a ``DEBUG`` message.

- Solved the problem of not initialized K8s pod breaking patronictl (Alexander Kukushkin)

  Patroni is relying on certain pod annotations on K8s. When one of the Patroni pods is stopping or starting there is no valid annotation yet and ``patronictl`` was failing with an exception.


**Stability improvements**

- Apply 1 second backoff if LIST call to K8s API server failed (Alexander Kukushkin)

  It is mostly necessary to avoid flooding logs, but also helps to prevent starvation of the main thread.

- Retry if the ``retry-after`` HTTP header is returned by K8s API (Alexander Kukushkin)

  If the K8s API server is overwhelmed with requests it might ask to retry.

- Scrub ``KUBERNETES_`` environment from the postmaster (Feike Steenbergen)

  The ``KUBERNETES_`` environment variables are not required for PostgreSQL, yet having them exposed to the postmaster will also expose them to backends and to regular database users (using pl/perl for example).

- Clean up tablespaces on reinitialize (Krishna Sarabu)

  During reinit, Patroni was removing only ``PGDATA`` and leaving user-defined tablespace directories. This is causing Patroni to loop in reinit. The previous workarond for the problem was implementing the :ref:`custom bootstrap <custom_bootstrap>` script.

- Explicitly execute ``CHECKPOINT`` after promote happened (Alexander Kukushkin)

  It helps to reduce the time before the new primary is usable for ``pg_rewind``.

- Smart refresh of Etcd members (Alexander Kukushkin)

  In case Patroni failed to execute a request on all members of the Etcd cluster, Patroni will re-check ``A`` or ``SRV`` records for changes of IPs/hosts before retrying the next time.

- Skip missing values from ``pg_controldata`` (Feike Steenbergen)

  Values are missing when trying to use binaries of a version that doesn't match PGDATA. Patroni will try to start Postgres anyway, and Postgres will complain that the major version doesn't match and abort with an error.


**Bugfixes**

- Disable SSL verification for Consul when required (Julien Riou)

  Starting from a certain version of ``urllib3``, the ``cert_reqs`` must be explicitly set to ``ssl.CERT_NONE`` in order to effectively disable SSL verification.

- Avoid opening replication connection on every cycle of HA loop (Alexander Kukushkin)

  Regression was introduced in 1.6.4.

- Call ``on_role_change`` callback on failed primary (Alexander Kukushkin)

  In certain cases it could lead to the virtual IP remaining attached to the old primary. Regression was introduced in 1.4.5.

- Reset rewind state if postgres started after successful pg_rewind (Alexander Kukushkin)

  As a result of this bug Patroni was starting up manually shut down postgres in the pause mode.

- Convert ``recovery_min_apply_delay`` to ``ms`` when checking ``recovery.conf``

  Patroni was indefinitely restarting replica if ``recovery_min_apply_delay`` was configured on PostgreSQL older than 12.

- PyInstaller compatibility (Alexander Kukushkin)

  PyInstaller freezes (packages) Python applications into stand-alone executables. The compatibility was broken when we switched to the ``spawn`` method instead of ``fork`` for ``multiprocessing``.


Version 1.6.4
-------------

**New features**

- Implemented ``--wait`` option for ``patronictl reinit`` (Igor Yanchenko)

  Patronictl will wait for ``reinit`` to finish is the ``--wait`` option is used.

- Further improvements of Windows support (Igor Yanchenko, Alexander Kukushkin)

  1. All shell scripts which are used for integration testing are rewritten in python
  2. The ``pg_ctl kill`` will be used to stop postgres on non posix systems
  3. Don't try to use unix-domain sockets


**Stability improvements**

- Make sure ``unix_socket_directories`` and ``stats_temp_directory`` exist (Igor Yanchenko)

  Upon the start of Patroni and Postgres make sure that ``unix_socket_directories`` and ``stats_temp_directory`` exist or try to create them. Patroni will exit if failed to create them.

- Make sure ``postgresql.pgpass`` is located in the place where Patroni has write access (Igor Yanchenko)

  In case if it doesn't have a write access Patroni will exit with exception.

- Disable Consul ``serfHealth`` check by default (Kostiantyn Nemchenko)

  Even in case of little network problems the failing ``serfHealth`` leads to invalidation of all sessions associated with the node. Therefore, the leader key is lost much earlier than ``ttl`` which causes unwanted restarts of replicas and maybe demotion of the primary.

- Configure tcp keepalives for connections to K8s API (Alexander Kukushkin)

  In case if we get nothing from the socket after TTL seconds it can be considered dead.

- Avoid logging of passwords on user creation (Alexander Kukushkin)

  If the password is rejected or logging is configured to verbose or not configured at all it might happen that the password is written into postgres logs. In order to avoid it Patroni will change ``log_statement``, ``log_min_duration_statement``, and ``log_min_error_statement`` to some safe values before doing the attempt to create/update user.


**Bugfixes**

- Use ``restore_command`` from the ``standby_cluster`` config on cascading replicas (Alexander Kukushkin)

  The ``standby_leader`` was already doing it from the beginning the feature existed. Not doing the same on replicas might prevent them from catching up with standby leader.

- Update timeline reported by the standby cluster (Alexander Kukushkin)

  In case of timeline switch the standby cluster was correctly replicating from the primary but ``patronictl`` was reporting the old timeline.

- Allow certain recovery parameters be defined in the custom_conf (Alexander Kukushkin)

  When doing validation of recovery parameters on replica Patroni will skip ``archive_cleanup_command``, ``promote_trigger_file``, ``recovery_end_command``, ``recovery_min_apply_delay``, and ``restore_command`` if they are not defined in the patroni config but in files other than ``postgresql.auto.conf`` or ``postgresql.conf``.

- Improve handling of postgresql parameters with period in its name (Alexander Kukushkin)

  Such parameters could be defined by extensions where the unit is not necessarily a string. Changing the value might require a restart (for example ``pg_stat_statements.max``).

- Improve exception handling during shutdown (Alexander Kukushkin)

  During shutdown Patroni is trying to update its status in the DCS. If the DCS is inaccessible an exception might be raised. Lack of exception handling was preventing logger thread from stopping.


Version 1.6.3
-------------

**Bugfixes**

- Don't expose password when running ``pg_rewind`` (Alexander Kukushkin)

  Bug was introduced in the `#1301 <https://github.com/zalando/patroni/pull/1301>`__

- Apply connection parameters specified in the ``postgresql.authentication`` to ``pg_basebackup`` and custom replica creation methods (Alexander Kukushkin)

  They were relying on url-like connection string and therefore parameters never applied.


Version 1.6.2
-------------

**New features**

- Implemented ``patroni --version`` (Igor Yanchenko)

  It prints the current version of Patroni and exits.

- Set the ``user-agent`` http header for all http requests (Alexander Kukushkin)

  Patroni is communicating with Consul, Etcd, and Kubernetes API via the http protocol. Having a specifically crafted ``user-agent`` (example: ``Patroni/1.6.2 Python/3.6.8 Linux``) might be useful for debugging and monitoring.

- Make it possible to configure log level for exception tracebacks (Igor Yanchenko)

  If you set ``log.traceback_level=DEBUG`` the tracebacks will be visible only when ``log.level=DEBUG``. The default behavior remains the same.


**Stability improvements**

- Avoid importing all DCS modules when searching for the module required by the config file (Alexander Kukushkin)

  There is no need to import modules for Etcd, Consul, and Kubernetes if we need only e.g. Zookeeper. It helps to reduce memory usage and solves the problem of having INFO messages ``Failed to import smth``.

- Removed python ``requests`` module from explicit requirements (Alexander Kukushkin)

  It wasn't used for anything critical, but causing a lot of problems when the new version of ``urllib3`` is released.

- Improve handling of ``etcd.hosts`` written as a comma-separated string instead of YAML array (Igor Yanchenko)

  Previously it was failing when written in format ``host1:port1, host2:port2`` (the space character after the comma).


**Usability improvements**

- Don't force users to choose members from an empty list in ``patronictl`` (Igor Yanchenko)

  If the user provides a wrong cluster name, we will raise an exception rather than ask to choose a member from an empty list.

- Make the error message more helpful if the REST API cannot bind (Igor Yanchenko)

  For an inexperienced user it might be hard to figure out what is wrong from the Python stacktrace.


**Bugfixes**

- Fix calculation of ``wal_buffers`` (Alexander Kukushkin)

  The base unit has been changed from 8 kB blocks to bytes in PostgreSQL 11.

- Use ``passfile`` in ``primary_conninfo`` only on PostgreSQL 10+ (Alexander Kukushkin)

  On older versions there is no guarantee that ``passfile`` will work, unless the latest version of ``libpq`` is installed.


Version 1.6.1
-------------

**New features**

- Added ``PATRONICTL_CONFIG_FILE`` environment variable (msvechla)

  It allows configuring the ``--config-file`` argument for ``patronictl`` from the environment.

- Implement ``patronictl history`` (Alexander Kukushkin)

  It shows the history of failovers/switchovers.

- Pass ``-c statement_timeout=0`` in ``PGOPTIONS`` when doing ``pg_rewind`` (Alexander Kukushkin)

  It protects from the case when ``statement_timeout`` on the server is set to some small value and one of the statements executed by pg_rewind is canceled.

- Allow lower values for PostgreSQL configuration (Soulou)

  Patroni didn't allow some of the PostgreSQL configuration parameters be set smaller than some hardcoded values. Now the minimal allowed values are smaller, default values have not been changed.

- Allow for certificate-based authentication (Jonathan S. Katz)

  This feature enables certificate-based authentication for superuser, replication, rewind accounts and allows the user to specify the ``sslmode`` they wish to connect with.

- Use the ``passfile`` in the ``primary_conninfo`` instead of password (Alexander Kukushkin)

  It allows to avoid setting ``600`` permissions on postgresql.conf

- Perform ``pg_ctl reload`` regardless of config changes (Alexander Kukushkin)

  It is possible that some config files are not controlled by Patroni. When somebody is doing a reload via the REST API or by sending SIGHUP to the Patroni process, the usual expectation is that Postgres will also be reloaded. Previously it didn't happen when there were no changes in the ``postgresql`` section of Patroni config.

- Compare all recovery parameters, not only ``primary_conninfo`` (Alexander Kukushkin)

  Previously the ``check_recovery_conf()`` method was only checking whether ``primary_conninfo`` has changed, never taking into account all other recovery parameters.

- Make it possible to apply some recovery parameters without restart (Alexander Kukushkin)

  Starting from PostgreSQL 12 the following recovery parameters could be changed without restart: ``archive_cleanup_command``, ``promote_trigger_file``, ``recovery_end_command``, and ``recovery_min_apply_delay``. In future Postgres releases this list will be extended and Patroni will support it automatically.

- Make it possible to change ``use_slots`` online (Alexander Kukushkin)

  Previously it required restarting Patroni and removing slots manually.

- Remove only ``PATRONI_`` prefixed environment variables when starting up Postgres (Cody Coons)

  It will solve a lot of problems with running different Foreign Data Wrappers.


**Stability improvements**

- Use LIST + WATCH when working with K8s API (Alexander Kukushkin)

  It allows to efficiently receive object changes (pods, endpoints/configmaps) and makes less stress on K8s master nodes.

- Improve the workflow when PGDATA is not empty during bootstrap (Alexander Kukushkin)

  According to the ``initdb`` source code it might consider a PGDATA empty when there are only ``lost+found`` and ``.dotfiles`` in it. Now Patroni does the same. If ``PGDATA`` happens to be non-empty, and at the same time not valid from the ``pg_controldata`` point of view, Patroni will complain and exit.

- Avoid calling expensive ``os.listdir()`` on every HA loop (Alexander Kukushkin)

  When the system is under IO stress, ``os.listdir()`` could take a few seconds (or even minutes) to execute, badly affecting the HA loop of Patroni. This could even cause the leader key to disappear from DCS due to the lack of updates. There is a better and less expensive way to check that the PGDATA is not empty. Now we check the presence of the ``global/pg_control`` file in the PGDATA.

- Some improvements in logging infrastructure (Alexander Kukushkin)

  Previously there was a possibility to loose the last few log lines on shutdown because the logging thread was a ``daemon`` thread.

- Use ``spawn`` multiprocessing start method on python 3.4+ (Maciej Kowalczyk)

  It is a known `issue <https://bugs.python.org/issue6721>`__ in Python that threading and multiprocessing do not mix well. Switching from the default method ``fork`` to the ``spawn`` is a recommended workaround. Not doing so might result in the Postmaster starting process hanging and Patroni indefinitely reporting ``INFO: restarting after failure in progress``, while  Postgres is actually up and running.

**Improvements in REST API**

- Make it possible to check client certificates in the REST API (Alexander Kukushkin)

  If the ``verify_client`` is set to ``required``, Patroni will check client certificates for all REST API calls. When it is set to ``optional``, client certificates are checked for all unsafe REST API endpoints.

- Return the response code 503 for the ``GET /replica`` health check request if Postgres is not running (Alexander Anikin)

  Postgres might spend significant time in recovery before it starts accepting client connections.

- Implement ``/history`` and ``/cluster`` endpoints (Alexander Kukushkin)

  The ``/history`` endpoint shows the content of the ``history`` key in DCS. The ``/cluster`` endpoint shows all cluster members and some service info like pending and scheduled restarts or switchovers.


**Improvements in Etcd support**

- Retry on Etcd RAFT internal error (Alexander Kukushkin)

  When the Etcd node is being shut down, it sends ``response code=300, data='etcdserver: server stopped'``, which was causing Patroni to demote the primary.

- Don't give up on Etcd request retry too early (Alexander Kukushkin)

  When there were some network problems, Patroni was quickly exhausting the list of Etcd nodes and giving up without using the whole ``retry_timeout``, potentially resulting in demoting the primary.


**Bugfixes**

- Disable ``synchronous_commit`` when granting execute permissions to the ``pg_rewind`` user (kremius)

  If the bootstrap is done with ``synchronous_mode_strict: true`` the `GRANT EXECUTE` statement was waiting indefinitely due to the non-synchronous nodes being available.

- Fix memory leak on python 3.7 (Alexander Kukushkin)

  Patroni is using ``ThreadingMixIn`` to process REST API requests and python 3.7 made threads spawn for every request non-daemon by default.

- Fix race conditions in asynchronous actions (Alexander Kukushkin)

  There was a chance that ``patronictl reinit --force`` could be overwritten by the attempt to recover stopped Postgres. This ended up in a situation when Patroni was trying to start Postgres while basebackup was running.

- Fix race condition in ``postmaster_start_time()`` method (Alexander Kukushkin)

  If the method is executed from the REST API thread, it requires a separate cursor object to be created.

- Fix the problem of not promoting the sync standby that had a name containing upper case letters (Alexander Kukushkin)

  We converted the name to the lower case because Postgres was doing the same while comparing the ``application_name`` with the value in ``synchronous_standby_names``.

- Kill all children along with the callback process before starting the new one (Alexander Kukushkin)

  Not doing so makes it hard to implement callbacks in bash and eventually can lead to the situation when two callbacks are running at the same time.

- Fix 'start failed' issue (Alexander Kukushkin)

  Under certain conditions the Postgres state might be set to 'start failed' despite Postgres being up and running.


Version 1.6.0
-------------

This version adds compatibility with PostgreSQL 12, makes is possible to run pg_rewind without superuser on PostgreSQL 11 and newer, and enables IPv6 support.


**New features**

- Psycopg2 was removed from requirements and must be installed independently (Alexander Kukushkin)

  Starting from 2.8.0 ``psycopg2`` was split into two different packages, ``psycopg2``, and ``psycopg2-binary``, which could be installed at the same time into the same place on the filesystem. In order to decrease dependency hell problem, we let a user choose how to install it. There are a few options available, please consult the :ref:`documentation <psycopg2_install_options>`.

- Compatibility with PostgreSQL 12 (Alexander Kukushkin)

  Starting from PostgreSQL 12 there is no ``recovery.conf`` anymore and all former recovery parameters are converted into `GUC <https://www.enterprisedb.com/blog/what-is-a-guc-variable>`_. In order to protect from ``ALTER SYSTEM SET primary_conninfo`` or similar, Patroni will parse ``postgresql.auto.conf`` and remove all standby and recovery parameters from there. Patroni config remains backward compatible. For example despite ``restore_command`` being a GUC, one can still specify it in the ``postgresql.recovery_conf.restore_command`` section and Patroni will write it into ``postgresql.conf`` for PostgreSQL 12.

- Make it possible to use ``pg_rewind`` without superuser on PostgreSQL 11 and newer (Alexander Kukushkin)

  If you want to use this feature please define ``username`` and ``password`` in the ``postgresql.authentication.rewind`` section of Patroni configuration file. For an already existing cluster you will have to create the user manually and ``GRANT EXECUTE`` permission on a few functions. You can find more details in the PostgreSQL `documentation <https://www.postgresql.org/docs/11/app-pgrewind.html#id-1.9.5.8.8>`__.

- Do a smart comparison of actual and desired ``primary_conninfo`` values on replicas (Alexander Kukushkin)

  It might help to avoid replica restart when you are converting an already existing primary-standby cluster to one managed by Patroni

- IPv6 support (Alexander Kukushkin)

  There were two major issues. Patroni REST API service was listening only on ``0.0.0.0`` and IPv6 IP addresses used in the ``api_url`` and ``conn_url`` were not properly quoted.

- Kerberos support (Ajith Vilas, Alexander Kukushkin)

  It makes possible using Kerberos authentication between Postgres nodes instead of defining passwords in Patroni configuration file

- Manage ``pg_ident.conf`` (Alexander Kukushkin)

  This functionality works similarly to ``pg_hba.conf``: if the ``postgresql.pg_ident`` is defined in the config file or DCS, Patroni will write its value to ``pg_ident.conf``, however, if ``postgresql.parameters.ident_file`` is defined, Patroni will assume that ``pg_ident`` is managed from outside and not update the file.


**Improvements in REST API**

- Added ``/health`` endpoint (Wilfried Roset)

  It will return an HTTP status code only if PostgreSQL is running

- Added ``/read-only`` and ``/read-write`` endpoints (Julien Riou)

  The ``/read-only`` endpoint enables reads balanced across replicas and the primary. The ``/read-write`` endpoint is an alias for ``/primary``, ``/leader`` and ``/master``.

- Use ``SSLContext`` to wrap the REST API socket (Julien Riou)

  Usage of ``ssl.wrap_socket()`` is deprecated and was still allowing soon-to-be-deprecated protocols like TLS 1.1.


**Logging improvements**

- Two-step logging (Alexander Kukushkin)

  All log messages are first written into the in-memory queue and later they are asynchronously flushed into the stderr or file from a separate thread. The maximum queue size is limited (configurable). If the limit is reached, Patroni will start losing logs, which is still better than blocking the HA loop.

- Enable debug logging for GET/OPTIONS API calls together with latency (Jan Tomsa)

  It will help with debugging of health-checks performed by HAProxy, Consul or other tooling that decides which node is the primary/replica.

- Log exceptions caught in Retry (Daniel Kucera)

  Log the final exception when either the number of attempts or the timeout were reached. It will hopefully help to debug some issues when communication to DCS fails.


**Improvements in patronictl**

- Enhance dialogues for scheduled switchover and restart (Rafia Sabih)

  Previously dialogues did not take into account scheduled actions and therefore were misleading.

- Check if config file exists (Wilfried Roset)

  Be verbose about configuration file when the given filename does not exists, instead of ignoring silently (which can lead to misunderstanding).

- Add fallback value for ``EDITOR`` (Wilfried Roset)

  When the ``EDITOR`` environment variable was not defined, ``patronictl edit-config`` was failing with `PatroniCtlException`. The new strategy is to try ``editor`` and than ``vi``, which should be available on most systems.


**Improvements in Consul support**

- Allow to specify Consul consistency mode (Jan Tomsa)

  You can read more about consistency mode `here <https://www.consul.io/api/features/consistency.html>`__.

- Reload Consul config on SIGHUP (Cameron Daniel Kucera, Alexander Kukushkin)

  It is especially useful when somebody is changing the value of ``token``.


**Bugfixes**

- Fix corner case in switchover/failover (Sharoon Thomas)

  The variable ``scheduled_at`` may be undefined if REST API is not accessible and we are using DCS as a fallback.

- Open trust to localhost in ``pg_hba.conf`` during custom bootstrap (Alexander Kukushkin)

  Previously it was open only to unix_socket, which was causing a lot of errors: ``FATAL:  no pg_hba.conf entry for replication connection from host "127.0.0.1", user "replicator"``

- Consider synchronous node as healthy even when the former leader is ahead (Alexander Kukushkin)

  If the primary loses access to the DCS, it restarts Postgres in read-only, but it might happen that other nodes can still access the old primary via the REST API. Such a situation was causing the synchronous standby not to promote because the old primary was reporting WAL position ahead of the synchronous standby.

- Standby cluster bugfixes (Alexander Kukushkin)

  Make it possible to bootstrap a replica in a standby cluster when the standby_leader is not accessible and a few other minor fixes.


Version 1.5.6
-------------

**New features**

- Support work with etcd cluster via set of proxies (Alexander Kukushkin)

  It might happen that etcd cluster is not accessible directly but via set of proxies. In this case Patroni will not perform etcd topology discovery but just round-robin via proxy hosts. Behavior is controlled by `etcd.use_proxies`.

- Changed callbacks behavior when role on the node is changed (Alexander Kukushkin)

  If the role was changed from `master` or `standby_leader` to `replica` or from `replica` to `standby_leader`, `on_restart` callback will not be called anymore in favor of `on_role_change` callback.

- Change the way how we start postgres (Alexander Kukushkin)

  Use `multiprocessing.Process` instead of executing itself and `multiprocessing.Pipe` to transmit the postmaster pid to the Patroni process. Before that we were using pipes, what was leaving postmaster process with stdin closed.

**Bug fixes**

- Fix role returned by REST API for the standby leader (Alexander Kukushkin)

  It was incorrectly returning `replica` instead of `standby_leader`

- Wait for callback end if it could not be killed (Julien Tachoires)

  Patroni doesn't have enough privileges to terminate the callback script running under `sudo` what was cancelling the new callback. If the running script could not be killed, Patroni will wait until it finishes and then run the next callback.

- Reduce lock time taken by dcs.get_cluster method (Alexander Kukushkin)

  Due to the lock being held DCS slowness was affecting the REST API health checks causing false positives.

- Improve cleaning of PGDATA when `pg_wal`/`pg_xlog` is a symlink (Julien Tachoires)

  In this case Patroni will explicitly remove files from the target directory.

- Remove unnecessary usage of os.path.relpath (Ants Aasma)

  It depends on being able to resolve the working directory, what will fail if Patroni is started in a directory that is later unlinked from the filesystem.

- Do not enforce ssl version when communicating with Etcd (Alexander Kukushkin)

  For some unknown reason python3-etcd on debian and ubuntu are not based on the latest version of the package and therefore it enforces TLSv1 which is not supported by Etcd v3. We solved this problem on Patroni side.

Version 1.5.5
-------------

This version introduces the possibility of automatic reinit of the former master, improves patronictl list output and fixes a number of bugs.

**New features**

- Add support of `PATRONI_ETCD_PROTOCOL`, `PATRONI_ETCD_USERNAME` and `PATRONI_ETCD_PASSWORD` environment variables (Étienne M)

  Before it was possible to configure them only in the config file or as a part of `PATRONI_ETCD_URL`, which is not always convenient.

- Make it possible to automatically reinit the former master (Alexander Kukushkin)

  If the pg_rewind is disabled or can't be used, the former master could fail to start as a new replica due to diverged timelines. In this case, the only way to fix it is wiping the data directory and reinitializing. This behavior could be changed by setting `postgresql.remove_data_directory_on_diverged_timelines`. When it is set, Patroni will wipe the data directory and reinitialize the former master automatically.

- Show information about timelines in patronictl list (Alexander Kukushkin)

  It helps to detect stale replicas. In addition to that, `Host` will include ':{port}' if the port value isn't default or there is more than one member running on the same host.

- Create a headless service associated with the $SCOPE-config endpoint (Alexander Kukushkin)

  The "config" endpoint keeps information about the cluster-wide Patroni and Postgres configuration, history file, and last but the most important, it holds the `initialize` key. When the Kubernetes master node is restarted or upgraded, it removes endpoints without services. The headless service will prevent it from being removed.

**Bug fixes**

- Adjust the read timeout for the leader watch blocking query (Alexander Kukushkin)

  According to the Consul documentation, the actual response timeout is increased by a small random amount of additional wait time added to the supplied maximum wait time to spread out the wake up time of any concurrent requests. It adds up to `wait / 16` additional time to the maximum duration. In our case we are adding `wait / 15` or 1 second depending on what is bigger.

- Always use replication=1 when connecting via replication protocol to the postgres (Alexander Kukushkin)

  Starting from Postgres 10 the line in the pg_hba.conf with database=replication doesn't accept connections with the parameter replication=database.

- Don't write primary_conninfo into recovery.conf for wal-only standby cluster (Alexander Kukushkin)

  Despite not having neither `host` nor `port` defined in the `standby_cluster` config, Patroni was putting the `primary_conninfo` into the `recovery.conf`, which is useless and generating a lot of errors.


Version 1.5.4
-------------

This version implements flexible logging and fixes a number of bugs.

**New features**

- Improvements in logging infrastructure (Alexander Kukushkin, Lucas Capistrant, Alexander Anikin)

  Logging configuration could be configured not only from environment variables but also from Patroni config file. It makes it possible to change logging configuration in runtime by updating config and doing reload or sending SIGHUP to the Patroni process. By default Patroni writes logs to stderr, but now it becomes possible to write logs directly into the file and rotate when it reaches a certain size. In addition to that added support of custom dateformat and the possibility to fine-tune log level for each python module.

- Make it possible to take into account the current timeline during leader elections (Alexander Kukushkin)

  It could happen that the node is considering itself as a healthiest one although it is currently not on the latest known timeline. In some cases we want to avoid promoting of such node, which could be achieved by setting `check_timeline` parameter to `true` (default behavior remains unchanged).

- Relaxed requirements on superuser credentials

  Libpq allows opening connections without explicitly specifying neither username nor password. Depending on situation it relies either on pgpass file or trust authentication method in pg_hba.conf. Since pg_rewind is also using libpq, it will work the same way.

- Implemented possibility to configure Consul Service registration and check interval via environment variables (Alexander Kukushkin)

  Registration of service in Consul was added in the 1.5.0, but so far it was only possible to turn it on via patroni.yaml.

**Stability Improvements**

- Set archive_mode to off during the custom bootstrap (Alexander Kukushkin)

  We want to avoid archiving wals and history files until the cluster is fully functional.  It really helps if the custom bootstrap involves pg_upgrade.

- Apply five seconds backoff when loading global config on start (Alexander Kukushkin)

  It helps to avoid hammering DCS when Patroni just starting up.

- Reduce amount of error messages generated on shutdown (Alexander Kukushkin)

  They were harmless but rather annoying and sometimes scary.

- Explicitly secure rw perms for recovery.conf at creation time (Lucas Capistrant)

  We don't want anybody except patroni/postgres user reading this file, because it contains replication user and password.

- Redirect HTTPServer exceptions to logger (Julien Riou)

  By default, such exceptions were logged on standard output messing with regular logs.

**Bug fixes**

- Removed stderr pipe to stdout on pg_ctl process (Cody Coons)

  Inheriting stderr from the main Patroni process allows all Postgres logs to be seen along with all patroni logs. This is very useful in a container environment as Patroni and Postgres logs may be consumed using standard tools (docker logs, kubectl, etc). In addition to that, this change fixes a bug with Patroni not being able to catch postmaster pid when postgres writing some warnings into stderr.

- Set Consul service check deregister timeout in Go time format (Pavel Kirillov)

  Without explicitly mentioned time unit registration was failing.

- Relax checks of standby_cluster cluster configuration (Dmitry Dolgov, Alexander Kukushkin)

  It was accepting only strings as valid values and therefore it was not possible to specify the port as integer and create_replica_methods as a list.

Version 1.5.3
-------------

Compatibility and bugfix release.

- Improve stability when running with python3 against zookeeper (Alexander Kukushkin)

  Change of `loop_wait` was causing Patroni to disconnect from zookeeper and never reconnect back.

- Fix broken compatibility with postgres 9.3 (Alexander Kukushkin)

  When opening a replication connection we should specify replication=1, because 9.3 does not understand replication='database'

- Make sure we refresh Consul session at least once per HA loop and improve handling of consul sessions exceptions (Alexander Kukushkin)

  Restart of local consul agent invalidates all sessions related to the node. Not calling session refresh on time and not doing proper handling of session errors was causing demote of the primary.

Version 1.5.2
-------------

Compatibility and bugfix release.

- Compatibility with kazoo-2.6.0 (Alexander Kukushkin)

  In order to make sure that requests are performed with an appropriate timeout, Patroni redefines create_connection method from python-kazoo module. The last release of kazoo slightly changed the way how create_connection method is called.

- Fix Patroni crash when Consul cluster loses the leader (Alexander Kukushkin)

  The crash was happening due to incorrect implementation of touch_member method, it should return boolean and not raise any exceptions.

Version 1.5.1
-------------

This version implements support of permanent replication slots, adds support of pgBackRest and fixes number of bugs.

**New features**

- Permanent replication slots (Alexander Kukushkin)

  Permanent replication slots are preserved on failover/switchover, that is, Patroni on the new primary will create configured replication slots right after doing promote. Slots could be configured with the help of `patronictl edit-config`. The initial configuration could be also done in the :ref:`bootstrap.dcs <yaml_configuration>`.

- Add pgbackrest support (Yogesh Sharma)

  pgBackrest can restore in existing $PGDATA folder, this allows speedy restore as files which have not changed since last backup are skipped, to support this feature new parameter `keep_data` has been introduced. See :ref:`replica creation method <custom_replica_creation>` section for additional examples.

**Bug fixes**

- A few bugfixes in the "standby cluster" workflow (Alexander Kukushkin)

  Please see https://github.com/zalando/patroni/pull/823 for more details.

- Fix REST API health check when cluster management is paused and DCS is not accessible (Alexander Kukushkin)

  Regression was introduced in https://github.com/zalando/patroni/commit/90cf930036a9d5249265af15d2b787ec7517cf57

Version 1.5.0
-------------

This version enables Patroni HA cluster to operate in a standby mode, introduces experimental support for running on Windows, and provides a new configuration parameter to register PostgreSQL service in Consul.

**New features**

- Standby cluster (Dmitry Dolgov)

  One or more Patroni nodes can form a standby cluster that runs alongside the primary one (i.e. in another datacenter) and consists of standby nodes that replicate from the master in the primary cluster. All PostgreSQL nodes in the standby cluster are replicas; one of those replicas elects itself to replicate directly from the remote master, while the others replicate from it in a cascading manner. More detailed description of this feature and some configuration examples can be found at :ref:`here <standby_cluster>`.

- Register Services in Consul (Pavel Kirillov, Alexander Kukushkin)

  If `register_service` parameter in the consul :ref:`configuration <consul_settings>` is enabled, the node will register a service with the name `scope` and the tag `master`, `replica` or `standby-leader`.

- Experimental Windows support (Pavel Golub)

  From now on it is possible to run Patroni on Windows, although Windows support is brand-new and hasn't received as much real-world testing as its Linux counterpart. We welcome your feedback!

**Improvements in patronictl**

- Add patronictl -k/--insecure flag and support for restapi cert (Wilfried Roset)

  In the past if the REST API was protected by the self-signed certificates `patronictl` would fail to verify them. There was no way to  disable that verification. It is now possible to configure `patronictl` to skip the certificate verification altogether or provide CA and client certificates in the :ref:`ctl: <patronictl_settings>` section of configuration.

- Exclude members with nofailover tag from patronictl switchover/failover output (Alexander Anikin)

  Previously, those members were incorrectly proposed as candidates when performing interactive switchover or failover via patronictl.

**Stability improvements**

- Avoid parsing non-key-value output lines of pg_controldata (Alexander Anikin)

  Under certain circuimstances pg_controldata outputs lines without a colon character. That would trigger an error in Patroni code that parsed pg_controldata output, hiding the actual problem; often such lines are emitted in a warning shown by pg_controldata before the regular output, i.e. when the binary major version does not match the one of the PostgreSQL data directory.

- Add member name to the error message during the leader election (Jan Mussler)

  During the leader election, Patroni connects to all known members of the cluster and requests their status. Such status is written to the Patroni log and includes the name of the member. Previously, if the member was not accessible, the error message did not indicate its name, containing only the URL.

- Immediately reserve the WAL position upon creation of the replication slot (Alexander Kukushkin)

  Starting from 9.6, `pg_create_physical_replication_slot` function provides an additional boolean parameter `immediately_reserve`. When it is set to `false`, which is also the default, the slot doesn't reserve the WAL position until it receives the first client connection, potentially losing some segments required by the client in a time window between the slot creation and the initial client connection.

- Fix bug in strict synchronous replication (Alexander Kukushkin)

  When running with `synchronous_mode_strict: true`, in some cases Patroni puts `*` into the `synchronous_standby_names`, changing the sync state for most of the replication connections to `potential`. Previously, Patroni couldn't pick a synchronous candidate under such curcuimstances, as it only considered those with the state `async`.


Version 1.4.6
-------------

**Bug fixes and stability improvements**

This release fixes a critical issue with Patroni API /master endpoint returning 200 for the non-master node. This is a
reporting issue, no actual split-brain, but under certain circumstances clients might be directed to the read-only node.

- Reset is_leader status on demote (Alexander Kukushkin, Oleksii Kliukin)

  Make sure demoted cluster member stops responding with code 200 on the /master API call.

- Add new "cluster_unlocked" field to the API output (Dmitry Dolgov)

  This field indicates whether the cluster has the master running. It can be used when it is not possible to query any
  other node but one of the replicas.

Version 1.4.5
-------------

**New features**

- Improve logging when applying new postgres configuration (Don Seiler)

  Patroni logs changed parameter names and values.

- Python 3.7 compatibility (Christoph Berg)

  async is a reserved keyword in python3.7

- Set state to "stopped" in the DCS when a member is shut down (Tony Sorrentino)

  This shows the member state as "stopped" in "patronictl list" command.

- Improve the message logged when stale postmaster.pid matches a running process (Ants Aasma)

  The previous one was beyond confusing.

- Implement patronictl reload functionality (Don Seiler)

  Before that it was only possible to reload configuration by either calling REST API or by sending SIGHUP signal to the Patroni process.

- Take and apply some parameters from controldata when starting as a replica (Alexander Kukushkin)

  The value of `max_connections` and some other parameters set in the global configuration may be lower than the one actually used by the primary; when this happens, the replica cannot start and should be fixed manually. Patroni takes care of that now by reading and applying the value from  `pg_controldata`, starting postgres and setting `pending_restart` flag.

- If set, use LD_LIBRARY_PATH when starting postgres (Chris Fraser)

  When starting up Postgres, Patroni was passing along PATH, LC_ALL and LANG env vars if they are set. Now it is doing the same with LD_LIBRARY_PATH. It should help if somebody installed PostgreSQL to non-standard place.

- Rename create_replica_method to create_replica_methods (Dmitry Dolgov)

  To make it clear that it's actually an array. The old name is still supported for backward compatibility.

**Bug fixes and stability improvements**

- Fix condition for the replica start due to pg_rewind in paused state (Oleksii Kliukin)

  Avoid starting the replica that had already executed pg_rewind before.

- Respond 200 to the master health-check only if update_lock has been successful (Alexander Kukushkin)

  Prevent Patroni from reporting itself a master on the former (demoted) master if DCS is partitioned.

- Fix compatibility with the new consul module (Alexander Kukushkin)

  Starting from v1.1.0 python-consul changed internal API and started using `list` instead of `dict` to pass query parameters.

- Catch exceptions from Patroni REST API thread during shutdown (Alexander Kukushkin)

  Those uncaught exceptions kept PostgreSQL running at shutdown.

- Do crash recovery only when Postgres runs as the master (Alexander Kukushkin)

  Require `pg_controldata` to report  'in production' or 'shutting down' or 'in crash recovery'. In all other cases no crash recovery is necessary.

- Improve handling of configuration errors (Henning Jacobs, Alexander Kukushkin)

  It is possible to change a lot of parameters in runtime (including `restapi.listen`) by updating Patroni config file and sending SIGHUP to Patroni process. This fix eliminates obscure exceptions from the 'restapi' thread when some of the parameters receive invalid values.


Version 1.4.4
-------------

**Stability improvements**

- Fix race condition in poll_failover_result (Alexander Kukushkin)

  It didn't affect directly neither failover nor switchover, but in some rare cases it was reporting success too early, when the former leader released the lock, producing a 'Failed over to "None"' instead of 'Failed over to "desired-node"' message.

- Treat Postgres parameter names as case insensitive (Alexander Kukushkin)

  Most of the Postgres parameters have snake_case names, but there are three exceptions from this rule: DateStyle, IntervalStyle and TimeZone. Postgres accepts those parameters when written in a different case (e.g. timezone = 'some/tzn'); however, Patroni was unable to find case-insensitive matches of those parameter names in pg_settings and ignored such parameters as a result.

- Abort start if attaching to running postgres and cluster not initialized (Alexander Kukushkin)

  Patroni can attach itself to an already running Postgres instance. It is imperative to start running Patroni on the master node before getting to the replicas.

- Fix behavior of patronictl scaffold (Alexander Kukushkin)

  Pass dict object to touch_member instead of json encoded string, DCS implementation will take care of encoding it.

- Don't demote master if failed to update leader key in pause (Alexander Kukushkin)

  During maintenance a DCS may start failing write requests while continuing to responds to read ones. In that case, Patroni used to put the Postgres master node to a read-only mode after failing to update the leader lock in DCS.

- Sync replication slots when Patroni notices a new postmaster process (Alexander Kukushkin)

  If Postgres has been restarted, Patroni has to make sure that list of replication slots matches its expectations.

- Verify sysid and sync replication slots after coming out of pause (Alexander Kukushkin)

  During the `maintenance` mode it may happen that data directory was completely rewritten and therefore we have to make sure that `Database system identifier` still belongs to our cluster and replication slots are in sync with Patroni expectations.

- Fix a possible failure to start not running Postgres on a data directory with postmaster lock file present (Alexander Kukushkin)

  Detect reuse of PID from the postmaster lock file. More likely to hit such problem if you run Patroni and Postgres in the docker container.

- Improve protection of DCS being accidentally wiped (Alexander Kukushkin)

  Patroni has a lot of logic in place to prevent failover in such case; it can also restore all keys back; however, until this change an accidental removal of /config key was switching off pause mode for 1 cycle of HA loop.

- Do not exit when encountering invalid system ID (Oleksii Kliukin)

  Do not exit when the cluster system ID is empty or the one that doesn't pass the validation check. In that case, the cluster most likely needs a reinit; mention it in the result message. Avoid terminating Patroni, as otherwise reinit cannot happen.

**Compatibility with Kubernetes 1.10+**

- Added check for empty subsets (Cody Coons)

  Kubernetes 1.10.0+ started returning `Endpoints.subsets` set to `None` instead of `[]`.

**Bootstrap improvements**

- Make deleting recovery.conf optional (Brad Nicholson)

  If `bootstrap.<custom_bootstrap_method_name>.keep_existing_recovery_conf` is defined and set to ``True``, Patroni will not remove the existing ``recovery.conf`` file. This is useful when bootstrapping from a backup with tools like pgBackRest that generate the appropriate `recovery.conf` for you.

- Allow options to the basebackup built-in method (Oleksii Kliukin)

  It is now possible to supply options to the built-in basebackup method by defining the `basebackup` section in the configuration, similar to how those are defined for custom replica creation methods. The difference is in the format accepted by the `basebackup` section: since pg_basebackup accepts both `--key=value` and `--key` options, the contents of the section could be either a dictionary of key-value pairs, or a list of either one-element dictionaries or just keys (for the options that don't accept values). See :ref:`replica creation method <custom_replica_creation>` section for additional examples.


Version 1.4.3
-------------

**Improvements in logging**

- Make log level configurable from environment variables (Andy Newton, Keyvan Hedayati)

  `PATRONI_LOGLEVEL` - sets the general logging level
  `PATRONI_REQUESTS_LOGLEVEL` - sets the logging level for all HTTP requests e.g. Kubernetes API calls
  See `the docs for Python logging <https://docs.python.org/3.6/library/logging.html#levels>` to get the names of possible log levels

**Stability improvements and bug fixes**

- Don't rediscover etcd cluster topology when watch timed out (Alexander Kukushkin)

  If we have only one host in etcd configuration and exactly this host is not accessible, Patroni was starting discovery of cluster topology and never succeeding. Instead it should just switch to the next available node.

- Write content of bootstrap.pg_hba into a pg_hba.conf after custom bootstrap (Alexander Kukushkin)

  Now it behaves similarly to the usual bootstrap with `initdb`

- Single user mode was waiting for user input and never finish (Alexander Kukushkin)

  Regression was introduced in https://github.com/zalando/patroni/pull/576


Version 1.4.2
-------------

**Improvements in patronictl**

- Rename scheduled failover to scheduled switchover (Alexander Kukushkin)

  Failover and switchover functions were separated in version 1.4, but `patronictl list` was still reporting `Scheduled failover` instead of `Scheduled switchover`.

- Show information about pending restarts (Alexander Kukushkin)

  In order to apply some configuration changes sometimes it is necessary to restart postgres. Patroni was already giving a hint about that in the REST API and when writing node status into DCS, but there were no easy way to display it.

- Make show-config to work with cluster_name from config file (Alexander Kukushkin)

  It works similar to the `patronictl edit-config`

**Stability improvements**

- Avoid calling pg_controldata during bootstrap (Alexander Kukushkin)

  During initdb or custom bootstrap there is a time window when pgdata is not empty but pg_controldata has not been written yet. In such case pg_controldata call was failing with error messages.

- Handle exceptions raised from psutil (Alexander Kukushkin)

  cmdline is read and parsed every time when `cmdline()` method is called. It could happen that the process being examined
  has already disappeared, in that case `NoSuchProcess` is raised.

**Kubernetes support improvements**

- Don't swallow errors from k8s API (Alexander Kukushkin)

  A call to Kubernetes API could fail for a different number of reasons. In some cases such call should be retried, in some other cases we should log the error message and the exception stack trace. The change here will help debug Kubernetes permission issues.

- Update Kubernetes example Dockerfile to install Patroni from the master branch (Maciej Szulik)

  Before that it was using `feature/k8s`, which became outdated.

- Add proper RBAC to run patroni on k8s (Maciej Szulik)

  Add the Service account that is assigned to the pods of the cluster, the role that holds only the necessary permissions, and the rolebinding that connects the Service account and the Role.


Version 1.4.1
-------------

**Fixes in patronictl**

- Don't show current leader in suggested list of members to failover to. (Alexander Kukushkin)

  patronictl failover could still work when there is leader in the cluster and it should be excluded from the list of member where it is possible to failover to.

- Make patronictl switchover compatible with the old Patroni api (Alexander Kukushkin)

  In case if POST /switchover REST API call has failed with status code 501 it will do it once again, but to /failover endpoint.


Version 1.4
-----------

This version adds support for using Kubernetes as a DCS, allowing to run Patroni as a cloud-native agent in Kubernetes without any additional deployments of Etcd, Zookeeper or Consul.

**Upgrade notice**

Installing Patroni via pip will no longer bring in dependencies for (such as libraries for Etcd, Zookeper, Consul or Kubernetes, or support for AWS). In order to enable them one need to list them in pip install command explicitly, for instance `pip install patroni[etcd,kubernetes]`.

**Kubernetes support**

Implement Kubernetes-based DCS. The endpoints meta-data is used in order to store the configuration and the leader key. The meta-data field inside the pods definition is used to store the member-related data.
In addition to using Endpoints, Patroni supports ConfigMaps. You can find more information about this feature in the :ref:`Kubernetes chapter of the documentation <kubernetes>`

**Stability improvements**

- Factor out postmaster process into a separate object (Ants Aasma)

  This object identifies a running postmaster process via pid and start time and simplifies detection (and resolution) of situations when the postmaster was restarted behind our back or when postgres directory disappeared from the file system.

- Minimize the amount of SELECT's issued by Patroni on every loop of HA cylce (Alexander Kukushkin)

  On every iteration of HA loop Patroni needs to know recovery status and absolute wal position. From now on Patroni will run only single SELECT to get this information instead of two on the replica and three on the master.

- Remove leader key on shutdown only when we have the lock (Ants Aasma)

  Unconditional removal was generating unnecessary and misleading exceptions.

**Improvements in patronictl**

- Add version command to patronictl (Ants Aasma)

  It will show the version of installed Patroni and versions of running Patroni instances (if the cluster name is specified).

- Make optional specifying cluster_name argument for some of patronictl commands (Alexander Kukushkin, Ants Aasma)

  It will work if patronictl is using usual Patroni configuration file with the ``scope`` defined.

- Show information about scheduled switchover and maintenance mode (Alexander Kukushkin)

  Before that it was possible to get this information only from Patroni logs or directly from DCS.

- Improve ``patronictl reinit`` (Alexander Kukushkin)

  Sometimes ``patronictl reinit`` refused to proceed when Patroni was busy with other actions, namely trying to start postgres. `patronictl` didn't provide any commands to cancel such long running actions and the only (dangerous) workarond was removing a data directory manually. The new implementation of `reinit` forcefully cancells other long-running actions before proceeding with reinit.

- Implement ``--wait`` flag in ``patronictl pause`` and ``patronictl resume`` (Alexander Kukushkin)

  It will make ``patronictl`` wait until the requested action is acknowledged by all nodes in the cluster.
  Such behaviour is achieved by exposing the ``pause`` flag for every node in DCS and via the REST API.

- Rename ``patronictl failover`` into ``patronictl switchover`` (Alexander Kukushkin)

  The previous ``failover`` was actually only capable of doing a switchover; it refused to proceed in a cluster without the leader.

- Alter the behavior of ``patronictl failover`` (Alexander Kukushkin)

  It will work even if there is no leader, but in that case you will have to explicitly specify a node which should become the new leader.

**Expose information about timeline and history**

- Expose current timeline in DCS and via API (Alexander Kukushkin)

  Store information about the current timeline for each member of the cluster. This information is accessible via the API and is stored in the DCS

- Store promotion history in the /history key in DCS (Alexander Kukushkin)

  In addition, store the timeline history enriched with the timestamp of the corresponding promotion in the /history key in DCS and update it with each promote.

**Add endpoints for getting synchronous and asynchronous replicas**

- Add new /sync and /async endpoints (Alexander Kukushkin, Oleksii Kliukin)

 Those endpoints (also accessible as /synchronous and /asynchronous) return 200 only for synchronous and asynchronous replicas correspondingly (exclusing those marked as `noloadbalance`).

**Allow multiple hosts for Etcd**

- Add a new `hosts` parameter to Etcd configuration (Alexander Kukushkin)

  This parameter should contain the initial list of hosts that will be used to discover and populate the list of the running etcd cluster members. If for some reason during work this list of discovered hosts is exhausted (no available hosts from that list), Patroni will return to the initial list from the `hosts` parameter.


Version 1.3.6
-------------

**Stability improvements**

- Verify process start time when checking if postgres is running. (Ants Aasma)

  After a crash that doesn't clean up postmaster.pid there could be a new process with the same pid, resulting in a false positive for is_running(), which will lead to all kinds of bad behavior.

- Shutdown postgresql before bootstrap when we lost data directory (ainlolcat)

  When data directory on the master is forcefully removed, postgres process can still stay alive for some time and prevent the replica created in place of that former master from starting or replicating.
  The fix makes Patroni cache the postmaster pid and its start time and let it terminate the old postmaster in case it is still running after the corresponding data directory has been removed.

- Perform crash recovery in a single user mode if postgres master dies (Alexander Kukushkin)

  It is unsafe to start immediately as a standby and not possible to run ``pg_rewind`` if postgres hasn't been shut down cleanly.
  The single user crash recovery only kicks in if ``pg_rewind`` is enabled or there is no master at the moment.

**Consul improvements**

- Make it possible to provide datacenter configuration for Consul (Vilius Okockis, Alexander Kukushkin)

  Before that Patroni was always communicating with datacenter of the host it runs on.

- Always send a token in X-Consul-Token http header (Alexander Kukushkin)

  If ``consul.token`` is defined in Patroni configuration, we will always send it in the 'X-Consul-Token' http header.
  python-consul module tries to be "consistent" with Consul REST API, which doesn't accept token as a query parameter for `session API <https://www.consul.io/api/session.html>`__, but it still works with 'X-Consul-Token' header.

- Adjust session TTL if supplied value is smaller than the minimum possible (Stas Fomin, Alexander Kukushkin)

  It could happen that the TTL provided in the Patroni configuration is smaller than the minimum one supported by Consul. In that case, Consul agent fails to create a new session.
  Without a session Patroni cannot create member and leader keys in the Consul KV store, resulting in an unhealthy cluster.

**Other improvements**

- Define custom log format via environment variable ``PATRONI_LOGFORMAT`` (Stas Fomin)

  Allow disabling timestamps and other similar fields in Patroni logs if they are already added by the system logger (usually when Patroni runs as a service).

Version 1.3.5
-------------

**Bugfix**

- Set role to 'uninitialized' if data directory was removed (Alexander Kukushkin)

  If the node was running as a master it was preventing from failover.

**Stability improvement**

- Try to run postmaster in a single-user mode if we tried and failed to start postgres (Alexander Kukushkin)

  Usually such problem happens when node running as a master was terminated and timelines were diverged.
  If ``recovery.conf`` has ``restore_command`` defined, there are really high chances that postgres will abort startup and leave controldata unchanged.
  It makes impossible to use ``pg_rewind``, which requires a clean shutdown.

**Consul improvements**

- Make it possible to specify health checks when creating session (Alexander Kukushkin)

  If not specified, Consul will use "serfHealth". From one side it allows fast detection of isolated master, but from another side it makes it impossible for Patroni to tolerate short network lags.

**Bugfix**

- Fix watchdog on Python 3 (Ants Aasma)

  A misunderstanding of the ioctl() call interface. If mutable=False then fcntl.ioctl() actually returns the arg buffer back.
  This accidentally worked on Python2 because int and str comparison did not return an error.
  Error reporting is actually done by raising IOError on Python2 and OSError on Python3.

Version 1.3.4
-------------

**Different Consul improvements**

- Pass the consul token as a header (Andrew Colin Kissa)

  Headers are now the preferred way to pass the token to the consul `API <https://www.consul.io/api/index.html#authentication>`__.


- Advanced configuration for Consul (Alexander Kukushkin)

  possibility to specify ``scheme``, ``token``, client and ca certificates :ref:`details <consul_settings>`.

- compatibility with python-consul-0.7.1 and above (Alexander Kukushkin)

  new python-consul module has changed signature of some methods

- "Could not take out TTL lock" message was never logged (Alexander Kukushkin)

  Not a critical bug, but lack of proper logging complicates investigation in case of problems.


**Quote synchronous_standby_names using quote_ident**

- When writing ``synchronous_standby_names`` into the ``postgresql.conf`` its value must be quoted (Alexander Kukushkin)

  If it is not quoted properly, PostgreSQL will effectively disable synchronous replication and continue to work.


**Different bugfixes around pause state, mostly related to watchdog** (Alexander Kukushkin)

- Do not send keepalives if watchdog is not active
- Avoid activating watchdog in a pause mode
- Set correct postgres state in pause mode
- Do not try to run queries from API if postgres is stopped


Version 1.3.3
-------------

**Bugfixes**

- synchronous replication was disabled shortly after promotion even when synchronous_mode_strict was turned on (Alexander Kukushkin)
- create empty ``pg_ident.conf`` file if it is missing after restoring from the backup (Alexander Kukushkin)
- open access in ``pg_hba.conf`` to all databases, not only postgres (Franco Bellagamba)


Version 1.3.2
-------------

**Bugfix**

- patronictl edit-config didn't work with ZooKeeper (Alexander Kukushkin)


Version 1.3.1
-------------

**Bugfix**

- failover via API was broken due to change in ``_MemberStatus`` (Alexander Kukushkin)


Version 1.3
-----------

Version 1.3 adds custom bootstrap possibility, significantly improves support for pg_rewind, enhances the
synchronous mode support, adds configuration editing to patronictl and implements watchdog support on Linux.
In addition, this is the first version to work correctly with PostgreSQL 10.

**Upgrade notice**

There are no known compatibility issues with the new version of Patroni. Configuration from version 1.2 should work
without any changes. It is possible to upgrade by installing new packages and either  restarting Patroni (will cause
PostgreSQL restart), or by putting Patroni into a :ref:`pause mode <pause>` first and then restarting Patroni on all
nodes in the cluster (Patroni in a pause mode will not attempt to stop/start PostgreSQL), resuming from the pause mode
at the end.

**Custom bootstrap**

- Make the process of bootstrapping the cluster configurable (Alexander Kukushkin)

  Allow custom bootstrap scripts instead of ``initdb`` when initializing the very first node in the cluster.
  The bootstrap command receives the name of the cluster and the path to the data directory. The resulting cluster can
  be configured to perform recovery, making it possible to bootstrap from a backup and do point in time recovery. Refer
  to the :ref:`documentaton page <custom_bootstrap>` for more detailed description of this feature.

**Smarter pg_rewind support**

-  Decide on whether to run pg_rewind by looking at the timeline differences from the current master (Alexander Kukushkin)

   Previously, Patroni had a fixed set of conditions to trigger pg_rewind, namely when starting a former master, when
   doing a switchover to the designated node for every other node in the cluster or when there is a replica with the
   nofailover tag. All those cases have in common a chance that some replica may be ahead of the new master. In some cases,
   pg_rewind did nothing, in some other ones it was not running when necessary. Instead of relying on this limited list
   of rules make Patroni compare the master and the replica WAL positions (using the streaming replication protocol)
   in order to reliably decide if rewind is necessary for the replica.

**Synchronous replication mode strict**

-  Enhance synchronous replication support by adding the strict mode (James Sewell, Alexander Kukushkin)

   Normally, when ``synchronous_mode`` is enabled and there are no replicas attached to the master, Patroni will disable
   synchronous replication in order to keep the master available for writes. The ``synchronous_mode_strict`` option
   changes that, when it is set Patroni will not disable the synchronous replication in a lack of replicas, effectively
   blocking all clients writing data to the master. In addition to the synchronous mode guarantee of preventing any data
   loss due to automatic failover, the strict mode ensures that each write is either durably stored on two nodes or not
   happening altogether if there is only one node in the cluster.

**Configuration editing with patronictl**

- Add configuration editing to patronictl (Ants Aasma, Alexander Kukushkin)

  Add the ability to patronictl of editing dynamic cluster configuration stored in DCS. Support either specifying the
  parameter/values from the command-line, invoking the $EDITOR, or applying configuration from the yaml file.

**Linux watchdog support**

- Implement watchdog support for Linux (Ants Aasma)

  Support Linux software watchdog in order to reboot the node where Patroni is not running or not responding (e.g because
  of the high load) The Linux software watchdog reboots the non-responsive node. It is possible to configure the watchdog
  device to use (`/dev/watchdog` by default) and the mode (on, automatic, off) from the watchdog section of the Patroni
  configuration. You can get more information from the :ref:`watchdog documentation <watchdog>`.

**Add support for PostgreSQL 10**

- Patroni is compatible with all beta versions of PostgreSQL 10 released so far and we expect it to be compatible with
  the PostgreSQL 10 when it will be released.

**PostgreSQL-related minor improvements**

- Define pg_hba.conf via the Patroni configuration file or the dynamic configuration in DCS (Alexander Kukushkin)

  Allow to define the contents of ``pg_hba.conf`` in the ``pg_hba`` sub-section of the ``postgresql`` section of the
  configuration. This simplifies managing ``pg_hba.conf`` on multiple nodes, as one needs to define it only ones in DCS
  instead of logging to every node, changing it manually and reload the configuration.

  When defined, the contents of this section will replace the current ``pg_hba.conf`` completely. Patroni ignores it
  if ``hba_file`` PostgreSQL parameter is set.

- Support connecting via a UNIX socket to the local PostgreSQL cluster (Alexander Kukushkin)

  Add the ``use_unix_socket`` option to the ``postgresql`` section of Patroni configuration. When set to true and the
  PostgreSQL ``unix_socket_directories`` option is not empty, enables Patroni to use the first value from it to connect
  to the local PostgreSQL cluster. If ``unix_socket_directories`` is not defined, Patroni will assume its default value
  and omit the ``host`` parameter in the PostgreSQL connection string altogether.

- Support change of superuser and replication credentials on reload (Alexander Kukushkin)

- Support storing of configuration files outside of PostgreSQL data directory (@jouir)

  Add the new configuration ``postgresql`` configuration directive ``config_dir``.
  It defaults to the data directory and must be writable by Patroni.

**Bug fixes and stability improvements**

- Handle EtcdEventIndexCleared and EtcdWatcherCleared exceptions (Alexander Kukushkin)

  Faster recovery when the watch operation is ended by Etcd by avoiding useless retries.

- Remove error spinning on Etcd failure and reduce log spam (Ants Aasma)

  Avoid immediate retrying and emitting stack traces in the log on the second and subsequent Etcd connection failures.

- Export locale variables when forking PostgreSQL processes (Oleksii Kliukin)

  Avoid the `postmaster became multithreaded during startup` fatal error on non-English locales for PostgreSQL built with NLS.

- Extra checks when dropping the replication slot (Alexander Kukushkin)

  In some cases Patroni is prevented from dropping the replication slot by the WAL sender.

- Truncate the replication slot name to 63  (NAMEDATALEN - 1) characters to comply with PostgreSQL naming rules (Nick Scott)

- Fix a race condition resulting in extra connections being opened to the PostgreSQL cluster from Patroni (Alexander Kukushkin)

- Release the leader key when the node restarts with an empty data directory (Alex Kerney)

- Set asynchronous executor busy when running bootstrap without a leader (Alexander Kukushkin)

  Failure to do so could have resulted in errors stating the node belonged to a different cluster, as Patroni proceeded with
  the normal business while being bootstrapped by a bootstrap method that doesn't require a leader to be present in the
  cluster.

- Improve WAL-E replica creation method (Joar Wandborg, Alexander Kukushkin).

  - Use csv.DictReader when parsing WAL-E base backup, accepting ISO dates with space-delimited date and time.
  - Support fetching current WAL position from the replica to estimate the amount of WAL to restore. Previously, the code used to call system information functions that were available only on the master node.


Version 1.2
-----------

This version introduces significant improvements over the handling of synchronous replication, makes the startup process and failover more reliable, adds PostgreSQL 9.6 support and fixes plenty of bugs.
In addition, the documentation, including these release notes, has been moved to https://patroni.readthedocs.io.

**Synchronous replication**

- Add synchronous replication support. (Ants Aasma)

  Adds a new configuration variable ``synchronous_mode``. When enabled, Patroni will manage ``synchronous_standby_names`` to enable synchronous replication whenever there are healthy standbys available. When synchronous mode is enabled, Patroni will automatically fail over only to a standby that was synchronously replicating at the time of the master failure. This effectively means that no user visible transaction gets lost in such a case. See the
  :ref:`feature documentation <synchronous_mode>` for the detailed description and implementation details.

**Reliability improvements**

- Do not try to update the leader position stored in the ``leader optime`` key when PostgreSQL is not 100% healthy. Demote immediately when the update of the leader key failed. (Alexander Kukushkin)

- Exclude unhealthy nodes from the list of targets to clone the new replica from. (Alexander Kukushkin)

- Implement retry and timeout strategy for Consul similar to how it is done for Etcd. (Alexander Kukushkin)

- Make ``--dcs`` and ``--config-file`` apply to all options in ``patronictl``. (Alexander Kukushkin)

- Write all postgres parameters into postgresql.conf. (Alexander Kukushkin)

  It allows starting PostgreSQL configured by Patroni with just ``pg_ctl``.

- Avoid exceptions when there are no users in the config. (Kirill Pushkin)

- Allow pausing an unhealthy cluster. Before this fix, ``patronictl`` would bail out if the node it tries to execute pause on is unhealthy. (Alexander Kukushkin)

- Improve the leader watch functionality. (Alexander Kukushkin)

  Previously the replicas were always watching the leader key (sleeping until the timeout or the leader key changes). With this change, they only watch
  when the replica's PostgreSQL is in the ``running`` state and not when it is stopped/starting or restarting PostgreSQL.

- Avoid running into race conditions when handling SIGCHILD as a PID 1. (Alexander Kukushkin)

  Previously a race condition could occur when running inside the Docker containers, since the same process inside Patroni both spawned new processes
  and handled SIGCHILD from them. This change uses fork/execs for Patroni and leaves the original PID 1 process responsible for handling signals from children.

- Fix WAL-E restore. (Oleksii Kliukin)

  Previously WAL-E restore used the ``no_master`` flag to avoid consulting with the master altogether, making Patroni always choose restoring
  from WAL over the ``pg_basebackup``. This change reverts it to the original meaning of ``no_master``, namely Patroni WAL-E restore may be selected as a replication method if the master is not running.
  The latter is checked by examining the connection string passed to the method. In addition, it makes the retry mechanism more robust and handles other minutia.

- Implement asynchronous DNS resolver cache. (Alexander Kukushkin)

  Avoid failing when DNS is temporary unavailable (for instance, due to an excessive traffic received by the node).

- Implement starting state and master start timeout. (Ants Aasma, Alexander Kukushkin)

  Previously ``pg_ctl`` waited for a timeout and then happily trodded on considering PostgreSQL to be running. This caused PostgreSQL to show up in listings as running when it was actually not and caused a race condition that   resulted in either a failover, or a crash recovery, or a crash recovery interrupted by failover and a missed rewind.
  This change adds a ``master_start_timeout`` parameter and introduces a new state for the main HA loop: ``starting``. When ``master_start_timeout`` is 0 we will failover immediately when the master crashes as soon as there is a failover candidate. Otherwise, Patroni will wait after attempting to start PostgreSQL on the master for the duration of the timeout; when it expires, it will failover if possible. Manual failover requests will be honored during the crash of the master even before the timeout expiration.

  Introduce the ``timeout`` parameter to the ``restart`` API endpoint and ``patronictl``. When it is set and restart takes longer than the timeout, PostgreSQL is considered unhealthy and the other nodes becomes eligible to take the leader lock.

- Fix ``pg_rewind`` behavior in a pause mode. (Ants Aasma)

  Avoid unnecessary restart in a pause mode when Patroni thinks it needs to rewind but rewind is not possible (i.e. ``pg_rewind`` is not present). Fallback to default ``libpq`` values for the ``superuser`` (default OS user) if ``superuser`` authentication is missing from the ``pg_rewind`` related Patroni configuration section.

- Serialize callback execution. Kill the previous callback of the same type when the new one is about to run. Fix the issue of spawning zombie processes when running callbacks. (Alexander Kukushkin)

- Avoid promoting a former master when the leader key is set in DCS but update to this leader key fails. (Alexander Kukushkin)

  This avoids the issue of a current master continuing to keep its role when it is partitioned together with the minority of nodes in Etcd and other DCSs that allow "inconsistent reads".

**Miscellaneous**

- Add ``post_init`` configuration option on bootstrap. (Alejandro Martínez)

  Patroni will call the script argument of this option right after running ``initdb`` and starting up PostgreSQL for a new cluster. The script receives a connection URL with ``superuser``
  and sets ``PGPASSFILE`` to point to the ``.pgpass`` file containing the password. If the script fails, Patroni initialization fails as well. It is useful for adding
  new users or creating extensions in the new cluster.

- Implement PostgreSQL 9.6 support. (Alexander Kukushkin)

  Use ``wal_level = replica`` as a synonym for ``hot_standby``, avoiding pending_restart flag when it changes from one to another. (Alexander Kukushkin)

**Documentation improvements**

- Add a Patroni main `loop workflow diagram <https://raw.githubusercontent.com/zalando/patroni/master/docs/ha_loop_diagram.png>`__. (Alejandro Martínez, Alexander Kukushkin)

- Improve README, adding the Helm chart and links to release notes. (Lauri Apple)

- Move Patroni documentation to ``Read the Docs``. The up-to-date documentation is available at https://patroni.readthedocs.io. (Oleksii Kliukin)

  Makes the documentation easily viewable from different devices (including smartphones) and searchable.

- Move the package to the semantic versioning. (Oleksii Kliukin)

  Patroni will follow the major.minor.patch version schema to avoid releasing the new minor version on small but critical bugfixes. We will only publish the release notes for the minor version, which will include all patches.


Version 1.1
-----------

This release improves management of Patroni cluster by bring in pause mode, improves maintenance with scheduled and conditional restarts, makes Patroni interaction with Etcd or Zookeeper more resilient and greatly enhances patronictl.

**Upgrade notice**

When upgrading from releases below 1.0 read about changing of credentials and configuration format at 1.0 release notes.

**Pause mode**

- Introduce pause mode to temporary detach Patroni from managing PostgreSQL instance (Murat Kabilov, Alexander Kukushkin, Oleksii Kliukin).

  Previously, one had to send SIGKILL signal to Patroni to stop it without terminating PostgreSQL. The new pause mode detaches Patroni from PostgreSQL cluster-wide without terminating Patroni. It is similar to the maintenance mode in Pacemaker. Patroni is still responsible for updating member and leader keys in DCS, but it will not start, stop or restart PostgreSQL server in the process. There are a few exceptions, for instance, manual failovers, reinitializes and restarts are still allowed. You can read :ref:`a detailed description of this feature <pause>`.

In addition, patronictl supports new ``pause`` and ``resume`` commands to toggle the pause mode.

**Scheduled and conditional restarts**

- Add conditions to the restart API command (Oleksii Kliukin)

  This change enhances Patroni restarts by adding a couple of conditions that can be verified in order to do the restart. Among the conditions are restarting when PostgreSQL role is either a master or a replica, checking the PostgreSQL version number or restarting only when restart is necessary in order to apply configuration changes.

- Add scheduled restarts (Oleksii Kliukin)

  It is now possible to schedule a restart in the future. Only one scheduled restart per node is supported. It is possible to clear the scheduled restart if it is not needed anymore. A combination of scheduled and conditional restarts is supported, making it possible, for instance, to scheduled minor PostgreSQL upgrades in the night, restarting only the instances that are running the outdated minor version without adding postgres-specific logic to administration scripts.

- Add support for conditional and scheduled restarts to patronictl (Murat Kabilov).

  patronictl restart supports several new options. There is also patronictl flush command to clean the scheduled actions.

**Robust DCS interaction**

- Set Kazoo timeouts depending on the loop_wait (Alexander Kukushkin)

  Originally, ping_timeout and connect_timeout values were calculated from the negotiated session timeout. Patroni loop_wait was not taken into account. As
  a result, a single retry could take more time than the session timeout, forcing Patroni to release the lock and demote.

  This change set ping and connect timeout to half of the value of loop_wait, speeding up detection of connection issues and  leaving enough time to retry the connection attempt before losing the lock.

- Update Etcd topology only after original request succeed (Alexander Kukushkin)

  Postpone updating the Etcd topology known to the client until after the original request. When retrieving the cluster topology, implement the retry timeouts depending on the known number of nodes in the Etcd cluster. This makes our client prefer to get the results of the request to having the up-to-date list of nodes.

  Both changes make Patroni connections to DCS more robust in the face of network issues.

**Patronictl, monitoring and configuration**

- Return information about streaming replicas via the API (Feike Steenbergen)

Previously, there was no reliable way to query Patroni about PostgreSQL instances that fail to stream changes (for instance, due to connection issues). This change exposes the contents of pg_stat_replication via the /patroni endpoint.

- Add patronictl scaffold command (Oleksii Kliukin)

  Add a command to create cluster structure in Etcd. The cluster is created with user-specified sysid and leader, and both leader and member keys are made persistent. This command is useful to create so-called master-less configurations, where Patroni cluster consisting of only replicas replicate  from the external master node that is unaware of Patroni. Subsequently, one
  may remove the leader key, promoting one of the Patroni nodes and replacing
  the original master with the Patroni-based HA cluster.

- Add configuration option ``bin_dir`` to locate PostgreSQL binaries (Ants Aasma)

  It is useful to be able to specify the location of PostgreSQL binaries explicitly when Linux distros that support installing multiple PostgreSQL versions at the same time.

- Allow configuration file path to be overridden using ``custom_conf`` of (Alejandro Martínez)

  Allows for custom configuration file paths, which will be unmanaged by Patroni, :ref:`details <postgresql_settings>`.

**Bug fixes and code improvements**

- Make Patroni compatible with new version schema in PostgreSQL 10 and above (Feike Steenbergen)

  Make sure that Patroni understand 2-digits version numbers when doing conditional restarts based on the PostgreSQL version.

- Use pkgutil to find DCS modules (Alexander Kukushkin)

  Use the dedicated python module instead of traversing directories manually in order to find DCS modules.

- Always call on_start callback when starting Patroni (Alexander Kukushkin)

  Previously, Patroni did not call any callbacks when attaching to the already running node with the correct role. Since callbacks are often used to route
  client connections that could result in the failure to register the running
  node in the connection routing scheme. With this fix, Patroni calls on_start
  callback even when attaching to the already running node.

- Do not drop active replication slots (Murat Kabilov, Oleksii Kliukin)

  Avoid dropping active physical replication slots on master. PostgreSQL cannot
  drop such slots anyway. This change makes possible to run non-Patroni managed
  replicas/consumers on the master.

- Close Patroni connections during start of the PostgreSQL instance (Alexander Kukushkin)

  Forces Patroni to close all former connections when PostgreSQL node is started. Avoids the trap of reusing former connections if postmaster was killed with SIGKILL.

- Replace invalid characters when constructing slot names from member names (Ants Aasma)

  Make sure that standby names that do not comply with the slot naming rules don't cause the slot creation and standby startup to fail. Replace the dashes in the slot names with underscores and all other characters not allowed in slot names with their unicode codepoints.

Version 1.0
-----------

This release introduces the global dynamic configuration that allows dynamic changes of the PostgreSQL and Patroni configuration parameters for the entire HA cluster. It also delivers numerous bugfixes.

**Upgrade notice**

When upgrading from v0.90 or below, always upgrade all replicas before the master. Since we don't store replication credentials in DCS anymore, an old replica won't be able to connect to the new master.

**Dynamic Configuration**

- Implement the dynamic global configuration (Alexander Kukushkin)

  Introduce new REST API endpoint /config to provide PostgreSQL and Patroni configuration parameters that should be set globally for the entire HA cluster (master and all the replicas). Those parameters are set in DCS and in many cases can be applied without disrupting PostgreSQL or Patroni. Patroni sets a special flag called "pending restart" visible via the API when some of the values require the PostgreSQL restart. In that case, restart should be issued manually via the API.

  Patroni SIGHUP or POST to /reload will make it re-read the configuration file.

  See the :ref:`Patroni configuration <patroni_configuration>` for the details on which parameters can be changed and the order of processing difference configuration sources.

  The configuration file format *has changed* since the v0.90. Patroni is still compatible with the old configuration files, but in order to take advantage of the bootstrap parameters one needs to change it. Users are encourage to update them by referring to the :ref:`dynamic configuration documentation page <dynamic_configuration>`.

**More flexible configuration***

- Make postgresql configuration and database name Patroni connects to configurable (Misja Hoebe)

  Introduce `database` and `config_base_name` configuration parameters. Among others, it makes possible to run Patroni with PipelineDB and other PostgreSQL forks.

- Implement possibility to configure some Patroni configuration parameters via environment (Alexander Kukushkin)

  Those include the scope, the node name and the namespace, as well as the secrets and makes it easier to run Patroni in a dynamic environment, i.e. Kubernetes  Please, refer to the :ref:`supported environment variables <environment>` for further details.

- Update the built-in Patroni docker container  to take advantage of environment-based configuration (Feike Steenbergen).

- Add Zookeeper support to Patroni docker image (Alexander Kukushkin)

- Split the Zookeeper and Exhibitor configuration options (Alexander Kukushkin)

- Make patronictl reuse the code from Patroni to read configuration (Alexander Kukushkin)

  This allows patronictl to take advantage of environment-based configuration.

- Set application name to node name in primary_conninfo (Alexander Kukushkin)

  This simplifies identification and configuration of synchronous replication for a given node.

**Stability, security and usability improvements**

- Reset sysid and do not call pg_controldata when restore of backup in progress (Alexander Kukushkin)

  This change reduces the amount of noise generated by Patroni API health checks during the lengthy initialization of this node from the backup.

- Fix a bunch of pg_rewind corner-cases (Alexander Kukushkin)

  Avoid running pg_rewind if the source cluster is not the master.

  In addition, avoid removing the data directory on an unsuccessful rewind, unless the new parameter *remove_data_directory_on_rewind_failure* is set to true. By default it is false.

- Remove passwords from the replication connection string in DCS (Alexander Kukushkin)

  Previously, Patroni always used the replication credentials from the Postgres URL in DCS. That is now changed to take the credentials from the patroni configuration. The secrets (replication username and password) and no longer exposed in DCS.

- Fix the asynchronous machinery around the demote call (Alexander Kukushkin)

  Demote now runs totally asynchronously without blocking the DCS interactions.

- Make patronictl always send the authorization header if it is configured (Alexander Kukushkin)

  This allows patronictl to issue "protected" requests, i.e. restart or reinitialize, when Patroni is configured to require authorization on those.

- Handle the SystemExit exception correctly (Alexander Kukushkin)

  Avoids the issues of Patroni not stopping properly when receiving the SIGTERM

- Sample haproxy templates for confd (Alexander Kukushkin)

  Generates and dynamically changes haproxy configuration from the patroni state in the DCS using confide

- Improve and restructure the documentation to make it more friendly to the new users (Lauri Apple)

- API must report role=master during pg_ctl stop (Alexander Kukushkin)

  Makes the callback calls more reliable, particularly in the cluster stop case. In addition, introduce the `pg_ctl_timeout` option to set the timeout for the start, stop and restart calls via the `pg_ctl`.

- Fix the retry logic in etcd (Alexander Kukushkin)

  Make retries more predictable and robust.

- Make Zookeeper code more resilient against short network hiccups (Alexander Kukushkin)

  Reduce the connection timeouts to make Zookeeper connection attempts more frequent.

Version 0.90
------------

This releases adds support for Consul, includes a new *noloadbalance* tag, changes the behavior of the *clonefrom* tag, improves *pg_rewind* handling and improves *patronictl* control program.

**Consul support**

- Implement Consul support (Alexander Kukushkin)

  Patroni runs against Consul, in addition to Etcd and Zookeeper. the connection parameters can be configured in the YAML file.

**New and improved tags**

- Implement *noloadbalance* tag (Alexander Kukushkin)

  This tag makes Patroni always return that the replica is not available to the load balancer.

- Change the implementation of the *clonefrom* tag (Alexander Kukushkin)

  Previously, a node name had to be supplied to the *clonefrom*, forcing a tagged replica to clone from the specific node. The new implementation makes *clonefrom* a boolean tag: if it is set to true, the replica becomes a candidate for other replicas to clone from it. When multiple candidates are present, the replicas picks one randomly.

**Stability and security improvements**

- Numerous reliability improvements (Alexander Kukushkin)

  Removes some spurious error messages, improves the stability of the failover, addresses some corner cases with reading data from DCS, shutdown, demote and reattaching of the former leader.

- Improve systems script to avoid killing Patroni children on stop (Jan Keirse, Alexander Kukushkin)

  Previously, when stopping Patroni, *systemd* also sent a signal to PostgreSQL. Since Patroni also tried to stop PostgreSQL by itself, it resulted in sending to different shutdown requests (the smart shutdown, followed by the fast shutdown). That resulted in replicas disconnecting too early and a former master not being able to rejoin after demote. Fix by Jan with prior research by Alexander.

- Eliminate some cases where the former master was unable to call pg_rewind before rejoining as a replica (Oleksii Kliukin)

  Previously, we only called *pg_rewind* if the former master had crashed. Change this to always run pg_rewind for the former master as long as pg_rewind is present in the system. This fixes the case when the master is shut down before the replicas managed to get the latest changes (i.e. during the "smart" shutdown).

- Numerous improvements to unit- and acceptance- tests, in particular, enable support for Zookeeper and Consul (Alexander Kukushkin).

- Make Travis CI faster and implement support for running tests against Zookeeper (Exhibitor) and Consul (Alexander Kukushkin)

  Both unit and acceptance tests run automatically against Etcd, Zookeeper and Consul on each commit or pull-request.

- Clear environment variables before calling PostgreSQL commands from Patroni (Feike Steenbergen)

  This prevents  a possibility of reading system environment variables by connecting to the PostgreSQL cluster managed by Patroni.

**Configuration and control changes**

- Unify patronictl and Patroni configuration (Feike Steenbergen)

  patronictl can use the same configuration file as Patroni itself.

- Enable Patroni to read the configuration from the environment variables (Oleksii Kliukin)

  This simplifies generating configuration for Patroni automatically, or merging a single configuration from different sources.

- Include database system identifier in the information returned by the API (Feike Steenbergen)

- Implement *delete_cluster* for all available DCSs (Alexander Kukushkin)

  Enables support for DCSs other than Etcd in patronictl.


Version 0.80
------------

This release adds support for *cascading replication* and simplifies Patroni management by providing *scheduled failovers*. One may use older versions of Patroni (in particular, 0.78) combined with this one in order to migrate to the new release. Note that the scheduled failover and cascading replication related features will only work with Patroni 0.80 and above.

**Cascading replication**

 - Add support for the *replicatefrom* and *clonefrom* tags for the patroni node (Oleksii Kliukin).

 The tag *replicatefrom*  allows a replica to use an arbitrary node a source, not necessary the master. The *clonefrom* does the same for the initial backup. Together, they enable Patroni to fully support cascading replication.

- Add support for running replication methods to initialize the replica even without a running replication connection (Oleksii Kliukin).

 This is useful in order to create replicas from the snapshots stored on S3 or FTP.  A replication method that does not require a running replication connection should supply *no_master: true* in the yaml configuration. Those scripts will still be called in order if the replication connection is present.

**Patronictl, API and DCS improvements**

- Implement scheduled failovers (Feike Steenbergen).

  Failovers can be scheduled to happen at a certain time in the future, using either patronictl, or API calls.

- Add support for *dbuser* and *password* parameters in patronictl (Feike Steenbergen).

- Add PostgreSQL version to the health check output (Feike Steenbergen).

- Improve Zookeeper support in patronictl (Oleksandr Shulgin)

- Migrate to python-etcd 0.43 (Alexander Kukushkin)

**Configuration**

- Add a sample systems configuration script for Patroni (Jan Keirse).

- Fix the problem of Patroni ignoring the superuser name specified in the configuration file for DB connections  (Alexander Kukushkin).

- Fix the handling of CTRL-C by creating a separate session ID and process group for the postmaster launched by Patroni (Alexander Kukushkin).

**Tests**

- Add acceptance tests with *behave* in order to check real-world scenarios of running Patroni (Alexander Kukushkin, Oleksii Kliukin).

  The tests can be launched manually using the *behave* command. They are also launched automatically for pull requests and after commits.

  Release notes for some older versions can be found on `project's github page <https://github.com/zalando/patroni/releases>`__.
