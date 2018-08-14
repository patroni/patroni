.. _releases:

Release notes
=============

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

- Fix condition for the replica start due to pg_rewind in paused state (Oleksii  Kliukin)

  Avoid starting the replica that had already executed pg_rewind before.

- Respond 200 to the master health-check only if update_lock has been successful (Alexander)

  Prevent Patroni from reporting itself a master on the former (demoted) master if DCS is partitioned.

- Fix compatibility with the new consul module (Alexander)

  Starting from v1.1.0 python-consul changed internal API and started using `list` instead of `dict` to pass query parameters.

- Catch exceptions from Patroni REST API thread during shutdown (Alexander)

  Those uncaught exceptions kept PostgreSQL running at shutdown.

- Do crash recovery only when Postgres runs as the master (Alexander)

  Require `pg_controldata` to report  'in production' or 'shutting down' or 'in crash recovery'. In all other cases no crash recovery is necessary.

- Improve handling of configuration errors (Henning Jacobs, Alexander)

  It is possible to change a lot of parameters in runtime (including `restapi.listen`) by updating Patroni config file and sending SIGHUP to Patroni process. This fix eliminates obscure exceptions from the 'restapi' thread when some of the parameters receive invalid values.


Version 1.4.4
-------------

**Stability improvements**

- Fix race condition in poll_failover_result (Alexander Kukushkin)

  It didn't affect directly neither failover nor switchover, but in some rare cases it was reporting success too early, when the former leader released the lock, producing a 'Failed over to "None"' instead of 'Failed over to "desired-node"' message.

- Treat Postgres parameter names as case insensitive (Alexander)

  Most of the Postgres parameters have snake_case names, but there are three exceptions from this rule: DateStyle, IntervalStyle and TimeZone. Postgres accepts those parameters when written in a different case (e.g. timezone = 'some/tzn'); however, Patroni was unable to find case-insensitive matches of those parameter names in pg_settings and ignored such parameters as a result.

- Abort start if attaching to running postgres and cluster not initialized (Alexander)

  Patroni can attach itself to an already running Postgres instance. It is imperative to start running Patroni on the master node before getting to the replicas.

- Fix behavior of patronictl scaffold (Alexander)

  Pass dict object to touch_member instead of json encoded string, DCS implementation will take care of encoding it.

- Don't demote master if failed to update leader key in pause (Alexander)

  During maintenance a DCS may start failing write requests while continuing to responds to read ones. In that case, Patroni used to put the Postgres master node to a read-only mode after failing to update the leader lock in DCS.

- Sync replication slots when Patroni notices a new postmaster process (Alexander)

  If Postgres has been restarted, Patroni has to make sure that list of replication slots matches its expectations.

- Verify sysid and sync replication slots after coming out of pause (Alexander)

  During the `maintenance` mode it may happen that data directory was completely rewritten and therefore we have to make sure that `Database system identifier` still belongs to our cluster and replication slots are in sync with Patroni expectations.

- Fix a possible failure to start not running Postgres on a data directory with postmaster lock file present (Alexander)

  Detect reuse of PID from the postmaster lock file. More likely to hit such problem if you run Patroni and Postgres in the docker container.

- Improve protection of DCS being accidentally wiped (Alexander)

  Patroni has a lot of logic in place to prevent failover in such case; it can also restore all keys back; however, until this change an accidental removal of /config key was switching off pause mode for 1 cycle of HA loop.

- Do not exit when encountering invalid system ID (Oleksii Kliukin)

  Do not exit when the cluster system ID is empty or the one that doesn't pass the validation check. In that case, the cluster most likely needs a reinit; mention it in the result message. Avoid terminating Patroni, as otherwise reinit cannot happen.

**Compatibility with Kubernetes 1.10+**

- Added check for empty subsets (Cody Coons)

  Kubernetes 1.10.0+ started returning `Endpoints.subsets` set to `None` instead of `[]`.

**Bootstrap improvements**

- Make deleting recovery.conf optional (Brad Nicholson)

  If `bootstrap.<custom_bootstrap_method_name>.keep_existing_recovery_conf` is defined and set to ``True``, Patroni will not remove the existing ``recovery.conf`` file. This is useful when bootstrapping from a backup with tools like pgBackRest that generate the appropriate `recovery.conf` for you.

- Allow options to the basebackup built-in method (Oleksii)

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

- Write content of bootstrap.pg_hba into a pg_hba.conf after custom bootstrap (Alexander)

  Now it behaves similarly to the usual bootstrap with `initdb`

- Single user mode was waiting for user input and never finish (Alexander)

  Regression was introduced in https://github.com/zalando/patroni/pull/576


Version 1.4.2
-------------

**Improvements in patronictl**

- Rename scheduled failover to scheduled switchover (Alexander Kukushkin)

  Failover and switchover functions were separated in version 1.4, but `patronictl list` was still reporting `Scheduled failover` instead of `Scheduled switchover`.

- Show information about pending restarts (Alexander)

  In order to apply some configuration changes sometimes it is necessary to restart postgres. Patroni was already giving a hint about that in the REST API and when writing node status into DCS, but there were no easy way to display it.

- Make show-config to work with cluster_name from config file (Alexander)

  It works similar to the `patronictl edit-config`

**Stability improvements**

- Avoid calling pg_controldata during bootstrap (Alexander)

  During initdb or custom bootstrap there is a time window when pgdata is not empty but pg_controldata has not been written yet. In such case pg_controldata call was failing with error messages.

- Handle exceptions raised from psutil (Alexander)

  cmdline is read and parsed every time when `cmdline()` method is called. It could happen that the process being examined
  has already disappeared, in that case `NoSuchProcess` is raised.

**Kubernetes support improvements**

- Don't swallow errors from k8s API (Alexander)

  A call to Kubernetes API could fail for a different number of reasons. In some cases such call should be retried, in some other cases we should log the error message and the exception stack trace. The change here will help debug Kubernetes permission issues.

- Update Kubernetes example Dockerfile to install Patroni from the master branch (Maciej Szulik)

  Before that it was using `feature/k8s`, which became outdated.

- Add proper RBAC to run patroni on k8s (Maciej)

  Add the Service account that is assigned to the pods of the cluster, the role that holds only the necessary permissions, and the rolebinding that connects the Service account and the Role.


Version 1.4.1
-------------

**Fixes in patronictl**

- Don't show current leader in suggested list of members to failover to. (Alexander Kukushkin)

  patronictl failover could still work when there is leader in the cluster and it should be excluded from the list of member where it is possible to failover to.

- Make patronictl switchover compatible with the old Patroni api (Alexander)

  In case if POST /switchover REST API call has failed with status code 501 it will do it once again, but to /failover endpoint.


Version 1.4
-----------

This version adds support for using Kubernetes as a DCS, allowing to run Patroni as a cloud-native agent in Kubernetes without any additional deployments of Etcd, Zookeeper or Consul.

**Upgrade notice**

Installing Patroni via pip will no longer bring in dependencies for (such as libraries for Etcd, Zookeper, Consul or Kubernetes, or support for AWS). In order to enable them one need to list them in pip install command explicitely, for instance `pip install patroni[etcd,kubernetes]`.

**Kubernetes support**

Implement Kubernetes-based DCS. The endpoints meta-data is used in order to store the configuration and the leader key. The meta-data field inside the pods definition is used to store the member-related data.
In addition to using Endpoints, Patroni supports ConfigMaps. You can find more information about this feature in the :ref:`Kubernetes chapter of the documentation <kubernetes>`

**Stability improvements**

- Factor out postmaster process into a separate object (Ants Aasma)

  This object identifies a running postmaster process via pid and start time and simplifies detection (and resolution) of situations when the postmaster was restarted behind our back or when postgres directory disappeared from the file system.

- Minimize the amount of SELECT's issued by Patroni on every loop of HA cylce (Alexander Kukushkin)

  On every iteration of HA loop Patroni needs to know recovery status and absolute wal position. From now on Patroni will run only single SELECT to get this information instead of two on the replica and three on the master. 
  
- Remove leader key on shutdown only when we have the lock (Ants)

  Unconditional removal was generating unnecessary and missleading exceptions.

**Improvements in patronictl**

- Add version command to patronictl (Ants)

  It will show the version of installed Patroni and versions of running Patroni instances (if the cluster name is specified).
  
- Make optional specifying cluster_name argument for some of patronictl commands (Alexander, Ants)

  It will work if patronictl is using usual Patroni configuration file with the ``scope`` defined.

- Show information about scheduled switchover and maintenance mode (Alexander)

  Before that it was possible to get this information only from Patroni logs or directly from DCS.
  
- Improve ``patronictl reinit`` (Alexander)

  Sometimes ``patronictl reinit`` refused to proceed when Patroni was busy with other actions, namely trying to start postgres. `patronictl` didn't provide any commands to cancel such long running actions and the only (dangerous) workarond was removing a data directory manually. The new implementation of `reinit` forcefully cancells other long-running actions before proceeding with reinit.
  
- Implement ``--wait`` flag in ``patronictl pause`` and ``patronictl resume`` (Alexander)

  It will make ``patronictl`` wait until the requested action is acknowledged by all nodes in the cluster.
  Such behaviour is achieved by exposing the ``pause`` flag for every node in DCS and via the REST API.

- Rename ``patronictl failover`` into ``patronictl switchover`` (Alexander)

  The previous ``failover`` was actually only capable of doing a switchover; it refused to proceed in a cluster without the leader.

- Alter the behavior of ``patronictl failover`` (Alexander)

  It will work even if there is no leader, but in that case you will have to explicitely specify a node which should become the new leader.

**Expose information about timeline and history**

- Expose current timeline in DCS and via API (Alexander)

  Store information about the current timeline for each member of the cluster. This information is accessible via the API and is stored in the DCS

- Store promotion history in the /history key in DCS (Alexander)

  In addition, store the timeline history enriched with the timestamp of the corresponding promotion in the /history key in DCS and update it with each promote.

**Add endpoints for getting synchronous and asynchronous replicas**

- Add new /sync and /async endpoints (Alexander, Oleksii Kliukin)

 Those endpoints (also accessible as /synchronous and /asynchronous) return 200 only for synchronous and asynchornous replicas correspondingly (exclusing those marked as `noloadbalance`).

**Allow multiple hosts for Etcd**

- Add a new `hosts` parameter to Etcd configuration (Alexander)

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

- Make it possible to provide datacenter configuration for Consul (Vilius Okockis, Alexander)

  Before that Patroni was always communicating with datacenter of the host it runs on.

- Always send a token in X-Consul-Token http header (Alexander)

  If ``consul.token`` is defined in Patroni configuration, we will always send it in the 'X-Consul-Token' http header.
  python-consul module tries to be "consistent" with Consul REST API, which doesn't accept token as a query parameter for `session API <https://www.consul.io/api/session.html>`__, but it still works with 'X-Consul-Token' header.

- Adjust session TTL if supplied value is smaller than the minimum possible (Stas Fomin, Alexander)

  It could happen that the TTL provided in the Patroni configuration is smaller than the minimum one supported by Consul. In that case, Consul agent fails to create a new session.
  Without a session Patroni cannot create member and leader keys in the Consul KV store, resulting in an unhealthy cluster.

**Other improvements**

- Define custom log format via environment variable ``PATRONI_LOGFORMAT`` (Stas)

  Allow disabling timestamps and other similar fields in Patroni logs if they are already added by the system logger (usually when Patroni runs as a service).

Version 1.3.5
-------------

**Bugfix**

- Set role to 'uninitialized' if data directory was removed (Alexander Kukushkin)

  If the node was running as a master it was preventing from failover.

**Stability improvement**

- Try to run postmaster in a single-user mode if we tried and failed to start postgres (Alexander)

  Usually such problem happens when node running as a master was terminated and timelines were diverged.
  If ``recovery.conf`` has ``restore_command`` defined, there are really high chances that postgres will abort startup and leave controldata unchanged.
  It makes impossible to use ``pg_rewind``, which requires a clean shutdown.

**Consul improvements**

- Make it possible to specify health checks when creating session (Alexander)

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

  Headers are now the prefered way to pass the token to the consul `API <https://www.consul.io/api/index.html#authentication>`__.
  

- Advanced configuration for Consul (Alexander Kukushkin)

  possibility to specify ``scheme``, ``token``, client and ca certificates :ref:`details <consul_settings>`.

- compatibility with python-consul-0.7.1 and above (Alexander)

  new python-consul module has changed signature of some methods

- "Could not take out TTL lock" message was never logged (Alexander)

  Not a critical bug, but lack of proper logging complicates investigation in case of problems.


**Quote synchronous_standby_names using quote_ident**

- When writing ``synchronous_standby_names`` into the ``postgresql.conf`` its value must be quoted (Alexander)

  If it is not quoted properly, PostgreSQL will effectively disable synchronous replication and continue to work.


**Different bugfixes around pause state, mostly related to watchdog** (Alexander)

- Do not send keepalives if watchdog is not active
- Avoid activating watchdog in a pause mode
- Set correct postgres state in pause mode
- Do not try to run queries from API if postgres is stopped


Version 1.3.3
-------------

**Bugfixes**

- synchronous replication was disabled shortly after promotion even when synchronous_mode_strict was turned on (Alexander Kukushkin)
- create empty ``pg_ident.conf`` file if it is missing after restoring from the backup (Alexander)
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

-  Decide on whether to run pg_rewind by looking at the timeline differences from the current master (Alexander)

   Previously, Patroni had a fixed set of conditions to trigger pg_rewind, namely when starting a former master, when
   doing a switchover to the designated node for every other node in the cluster or when there is a replica with the
   nofailover tag. All those cases have in common a chance that some replica may be ahead of the new master. In some cases,
   pg_rewind did nothing, in some other ones it was not running when necessary. Instead of relying on this limited list
   of rules make Patroni compare the master and the replica WAL positions (using the streaming replication protocol)
   in order to reliably decide if rewind is necessary for the replica.

**Synchronous replication mode strict**

-  Enhance synchronous replication support by adding the strict mode (James Sewell, Alexander)

   Normally, when ``synchronous_mode`` is enabled and there are no replicas attached to the master, Patroni will disable
   synchronous replication in order to keep the master available for writes. The ``synchronous_mode_strict`` option
   changes that, when it is set Patroni will not disable the synchronous replication in a lack of replicas, effectively
   blocking all clients writing data to the master. In addition to the synchronous mode guarantee of preventing any data
   loss due to automatic failover, the strict mode ensures that each write is either durably stored on two nodes or not
   happening altogether if there is only one node in the cluster.

**Configuration editing with patronictl**

- Add configuration editing to patronictl (Ants Aasma, Alexander)

  Add the ability to patronictl of editing dynamic cluster configuration stored in DCS. Support either specifying the
  parameter/values from the command-line, invoking the $EDITOR, or applying configuration from the yaml file.

**Linux watchdog support**

- Implement watchdog support for Linux (Ants)

  Support Linux software watchdog in order to reboot the node where Patroni is not running or not responding (e.g because
  of the high load) The Linux software watchdog reboots the non-responsive node. It is possible to configure the watchdog
  device to use (`/dev/watchdog` by default) and the mode (on, automatic, off) from the watchdog section of the Patroni
  configuration. You can get more information from the :ref:`watchdog documentation <watchdog>`.

**Add support for PostgreSQL 10**

- Patroni is compatible with all beta versions of PostgreSQL 10 released so far and we expect it to be compatible with
  the PostgreSQL 10 when it will be released.

**PostgreSQL-related minor improvements**

- Define pg_hba.conf via the Patroni configuration file or the dynamic configuration in DCS (Alexander)

  Allow to define the contents of ``pg_hba.conf`` in the ``pg_hba`` sub-section of the ``postgresql`` section of the
  configuration. This simplifies managing ``pg_hba.conf`` on multiple nodes, as one needs to define it only ones in DCS
  instead of logging to every node, changing it manually and reload the configuration.

  When defined, the contents of this section will replace the current ``pg_hba.conf`` completely. Patroni ignores it
  if ``hba_file`` PostgreSQL parameter is set.

- Support connecting via a UNIX socket to the local PostgreSQL cluster (Alexander)

  Add the ``use_unix_socket`` option to the ``postgresql`` section of Patroni configuration. When set to true and the
  PostgreSQL ``unix_socket_directories`` option is not empty, enables Patroni to use the first value from it to connect
  to the local PostgreSQL cluster. If ``unix_socket_directories`` is not defined, Patroni will assume its default value
  and omit the ``host`` parameter in the PostgreSQL connection string altogether.

- Support change of superuser and replication credentials on reload (Alexander)

- Support storing of configuration files outside of PostgreSQL data directory (@jouir)

  Add the new configuration ``postgresql`` configuration directive ``config_dir``.
  It defaults to the data directory and must be writable by Patroni.

**Bug fixes and stability improvements**

- Handle EtcdEventIndexCleared and EtcdWatcherCleared exceptions (Alexander)

  Faster recovery when the watch operation is ended by Etcd by avoiding useless retries.

- Remove error spinning on Etcd failure and reduce log spam (Ants)

  Avoid immediate retrying and emitting stack traces in the log on the second and subsequent Etcd connection failures.

- Export locale variables when forking PostgreSQL processes (Oleksii Kliukin)

  Avoid the `postmaster became multithreaded during startup` fatal error on non-English locales for PostgreSQL built with NLS.

- Extra checks when dropping the replication slot (Alexander)

  In some cases Patroni is prevented from dropping the replication slot by the WAL sender.

- Truncate the replication slot name to 63  (NAMEDATALEN - 1) characters to comply with PostgreSQL naming rules (Nick Scott)

- Fix a race condition resulting in extra connections being opened to the PostgreSQL cluster from Patroni (Alexander)

- Release the leader key when the node restarts with an empty data directory (Alex Kerney)

- Set asynchronous executor busy when running bootstrap without a leader (Alexander)

  Failure to do so could have resulted in errors stating the node belonged to a different cluster, as Patroni proceeded with
  the normal business while being bootstrapped by a bootstrap method that doesn't require a leader to be present in the
  cluster.

- Improve WAL-E replica creation method (Joar Wandborg, Alexander).

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

- Exclude unhealthy nodes from the list of targets to clone the new replica from. (Alexander)

- Implement retry and timeout strategy for Consul similar to how it is done for Etcd. (Alexander)

- Make ``--dcs`` and ``--config-file`` apply to all options in ``patronictl``. (Alexander)

- Write all postgres parameters into postgresql.conf. (Alexander)

  It allows starting PostgreSQL configured by Patroni with just ``pg_ctl``.

- Avoid exceptions when there are no users in the config. (Kirill Pushkin)

- Allow pausing an unhealthy cluster. Before this fix, ``patronictl`` would bail out if the node it tries to execute pause on is unhealthy. (Alexander)

- Improve the leader watch functionality. (Alexander)

  Previously the replicas were always watching the leader key (sleeping until the timeout or the leader key changes). With this change, they only watch
  when the replica's PostgreSQL is in the ``running`` state and not when it is stopped/starting or restarting PostgreSQL.

- Avoid running into race conditions when handling SIGCHILD as a PID 1. (Alexander)

  Previously a race condition could occur when running inside the Docker containers, since the same process inside Patroni both spawned new processes
  and handled SIGCHILD from them. This change uses fork/execs for Patroni and leaves the original PID 1 process responsible for handling signals from children.

- Fix WAL-E restore. (Oleksii Kliukin)

  Previously WAL-E restore used the ``no_master`` flag to avoid consulting with the master altogether, making Patroni always choose restoring
  from WAL over the ``pg_basebackup``. This change reverts it to the original meaning of ``no_master``, namely Patroni WAL-E restore may be selected as a replication method if the master is not running.
  The latter is checked by examining the connection string passed to the method. In addition, it makes the retry mechanism more robust and handles other minutia.

- Implement asynchronous DNS resolver cache. (Alexander)

  Avoid failing when DNS is temporary unavailable (for instance, due to an excessive traffic received by the node).

- Implement starting state and master start timeout. (Ants, Alexander)

  Previously ``pg_ctl`` waited for a timeout and then happily trodded on considering PostgreSQL to be running. This caused PostgreSQL to show up in listings as running when it was actually not and caused a race condition that   resulted in either a failover, or a crash recovery, or a crash recovery interrupted by failover and a missed rewind.
  This change adds a ``master_start_timeout`` parameter and introduces a new state for the main HA loop: ``starting``. When ``master_start_timeout`` is 0 we will failover immediately when the master crashes as soon as there is a failover candidate. Otherwise, Patroni will wait after attempting to start PostgreSQL on the master for the duration of the timeout; when it expires, it will failover if possible. Manual failover requests will be honored during the crash of the master even before the timeout expiration.

  Introduce the ``timeout`` parameter to the ``restart`` API endpoint and ``patronictl``. When it is set and restart takes longer than the timeout, PostgreSQL is considered unhealthy and the other nodes becomes eligible to take the leader lock.

- Fix ``pg_rewind`` behavior in a pause mode. (Ants)

  Avoid unnecessary restart in a pause mode when Patroni thinks it needs to rewind but rewind is not possible (i.e. ``pg_rewind`` is not present). Fallback to default ``libpq`` values for the ``superuser`` (default OS user) if ``superuser`` authentication is missing from the ``pg_rewind`` related Patroni configuration section.

- Serialize callback execution. Kill the previous callback of the same type when the new one is about to run. Fix the issue of spawning zombie processes when running callbacks. (Alexander)

- Avoid promoting a former master when the leader key is set in DCS but update to this leader key fails. (Alexander)

  This avoids the issue of a current master continuing to keep its role when it is partitioned together with the minority of nodes in Etcd and other DCSs that allow "inconsistent reads".

**Miscellaneous**

- Add ``post_init`` configuration option on bootstrap. (Alejandro Martínez)

  Patroni will call the script argument of this option right after running ``initdb`` and starting up PostgreSQL for a new cluster. The script receives a connection URL with ``superuser``
  and sets ``PGPASSFILE`` to point to the ``.pgpass`` file containing the password. If the script fails, Patroni initialization fails as well. It is useful for adding
  new users or creating extensions in the new cluster.

- Implement PostgreSQL 9.6 support. (Alexander)

  Use ``wal_level = replica`` as a synonym for ``hot_standby``, avoiding pending_restart flag when it changes from one to another. (Alexander)

**Documentation improvements**

- Add a Patroni main `loop workflow diagram <https://raw.githubusercontent.com/zalando/patroni/master/docs/ha_loop_diagram.png>`__. (Alejandro, Alexander)

- Improve README, adding the Helm chart and links to release notes. (Lauri Apple)

- Move Patroni documentation to ``Read the Docs``. The up-to-date documentation is available at https://patroni.readthedocs.io. (Oleksii)

  Makes the documentation easily viewable from different devices (including smartphones) and searchable.

- Move the package to the semantic versioning. (Oleksii)

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

- Add conditions to the restart API command (Oleksii)

  This change enhances Patroni restarts by adding a couple of conditions that can be verified in order to do the restart. Among the conditions are restarting when PostgreSQL role is either a master or a replica, checking the PostgreSQL version number or restarting only when restart is necessary in order to apply configuration changes.

- Add scheduled restarts (Oleksii)

  It is now possible to schedule a restart in the future. Only one scheduled restart per node is supported. It is possible to clear the scheduled restart if it is not needed anymore. A combination of scheduled and conditional restarts is supported, making it possible, for instance, to scheduled minor PostgreSQL upgrades in the night, restarting only the instances that are running the outdated minor version without adding postgres-specific logic to administration scripts.

- Add support for conditional and scheduled restarts to patronictl (Murat).

  patronictl restart supports several new options. There is also patronictl flush command to clean the scheduled actions.

**Robust DCS interaction**

- Set Kazoo timeouts depending on the loop_wait (Alexander)

  Originally, ping_timeout and connect_timeout values were calculated from the negotiated session timeout. Patroni loop_wait was not taken into account. As
  a result, a single retry could take more time than the session timeout, forcing Patroni to release the lock and demote.

  This change set ping and connect timeout to half of the value of loop_wait, speeding up detection of connection issues and  leaving enough time to retry the connection attempt before loosing the lock.

- Update Etcd topology only after original request succeed (Alexander)

  Postpone updating the Etcd topology known to the client until after the original request. When retrieving the cluster topology, implement the retry timeouts depending on the known number of nodes in the Etcd cluster. This makes our client prefer to get the results of the request to having the up-to-date list of nodes.

  Both changes make Patroni connections to DCS more robust in the face of network issues.

**Patronictl, monitoring and configuration**

- Return information about streaming replicas via the API (Feike Steenbergen)

Previously, there was no reliable way to query Patroni about PostgreSQL instances that fail to stream changes (for instance, due to connection issues). This change exposes the contents of pg_stat_replication via the /patroni endpoint.

- Add patronictl scaffold command (Oleksii)

  Add a command to create cluster structure in Etcd. The cluster is created with user-specified sysid and leader, and both leader and member keys are made persistent. This command is useful to create so-called master-less configurations, where Patroni cluster consisting of only replicas replicate  from the external master node that is unaware of Patroni. Subsequently, one
  may remove the leader key, promoting one of the Patroni nodes and replacing
  the original master with the Patroni-based HA cluster.

- Add configuration option ``bin_dir`` to locate PostgreSQL binaries (Ants Aasma)

  It is useful to be able to specify the location of PostgreSQL binaries explicitly when Linux distros that support installing multiple PostgreSQL versions at the same time.

- Allow configuration file path to be overridden using ``custom_conf`` of (Alejandro Martínez)

  Allows for custom configuration file paths, which will be unmanaged by Patroni, :ref:`details <postgresql_settings>`.

**Bug fixes and code improvements**

- Make Patroni compatible with new version schema in PostgreSQL 10 and above (Feike)

  Make sure that Patroni understand 2-digits version numbers when doing conditional restarts based on the PostgreSQL version.

- Use pkgutil to find DCS modules (Alexander)

  Use the dedicated python module instead of traversing directories manually in order to find DCS modules.

- Always call on_start callback when starting Patroni (Alexander)

  Previously, Patroni did not call any callbacks when attaching to the already running node with the correct role. Since callbacks are often used to route
  client connections that could result in the failure to register the running
  node in the connection routing scheme. With this fix, Patroni calls on_start
  callback even when attaching to the already running node.

- Do not drop active replication slots (Murat, Oleksii)

  Avoid dropping active physical replication slots on master. PostgreSQL cannot
  drop such slots anyway. This change makes possible to run non-Patroni managed
  replicas/consumers on the master.

- Close Patroni connections during start of the PostgreSQL instance (Alexander)

  Forces Patroni to close all former connections when PostgreSQL node is started. Avoids the trap of reusing former connections if postmaster was killed with SIGKILL.

- Replace invalid characters when constructing slot names from member names (Ants)

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

  See the :ref:`dynamic configuration <dynamic_configuration>`  for the details on which parameters can be changed and the order of processing difference configuration sources.

  The configuration file format *has changed* since the v0.90. Patroni is still compatible with the old configuration files, but in order to take advantage of the bootstrap parameters one needs to change it. Users are encourage to update them by referring to the :ref:`dynamic configuraton documentation page <dynamic_configuration>`.

**More flexible configuration***

- Make postgresql configuration and database name Patroni connects to configurable (Misja Hoebe)

  Introduce `database` and `config_base_name` configuration parameters. Among others, it makes possible to run Patroni with PipelineDB and other PostgreSQL forks.

- Implement possibility to configure some Patroni configuration parameters via environment (Alexander)

  Those include the scope, the node name and the namespace, as well as the secrets and makes it easier to run Patroni in a dynamic environment, i.e. Kubernetes  Please, refer to the :ref:`supported environment variables <environment>` for further details.

- Update the built-in Patroni docker container  to take advantage of environment-based configuration (Feike Steenbergen).

- Add Zookeeper support to Patroni docker image (Alexander)

- Split the Zookeeper and Exhibitor configuration options (Alexander)

- Make patronictl reuse the code from Patroni to read configuration (Alexander)

  This allows patronictl to take advantage of environment-based configuration.

- Set application name to node name in primary_conninfo (Alexander)

  This simplifies identification and configuration of synchronous replication for a given node.

**Stability, security and usability improvements**

- Reset sysid and do not call pg_controldata when restore of backup in progress (Alexander)

  This change reduces the amount of noise generated by Patroni API health checks during the lengthy initialization of this node from the backup.

- Fix a bunch of pg_rewind corner-cases (Alexander)

  Avoid running pg_rewind if the source cluster is not the master.

  In addition, avoid removing the data directory on an unsuccessful rewind, unless the new parameter *remove_data_directory_on_rewind_failure* is set to true. By default it is false.

- Remove passwords from the replication connection string in DCS (Alexander)

  Previously, Patroni always used the replication credentials from the Postgres URL in DCS. That is now changed to take the credentials from the patroni configuration. The secrets (replication username and password) and no longer exposed in DCS.

- Fix the asynchronous machinery around the demote call (Alexander)

  Demote now runs totally asynchronously without blocking the DCS interactions.

- Make patronictl always send the authorization header if it is configured (Alexander)

  This allows patronictl to issue "protected" requests, i.e. restart or reinitialize, when Patroni is configured to require authorization on those.

- Handle the SystemExit exception correctly (Alexander)

  Avoids the issues of Patroni not stopping properly when receiving the SIGTERM

- Sample haproxy templates for confd (Alexander)

  Generates and dynamically changes haproxy configuration from the patroni state in the DCS using confide

- Improve and restructure the documentation to make it more friendly to the new users (Lauri Apple)

- API must report role=master during pg_ctl stop (Alexander)

  Makes the callback calls more reliable, particularly in the cluster stop case. In addition, introduce the `pg_ctl_timeout` option to set the timeout for the start, stop and restart calls via the `pg_ctl`.

- Fix the retry logic in etcd (Alexander)

  Make retries more predictable and robust.

- Make Zookeeper code more resilient against short network hiccups (Alexander)

  Reduce the connection timeouts to make Zookeeper connection attempts more frequent.

Version 0.90
------------

This releases adds support for Consul, includes a new *noloadbalance* tag, changes the behavior of the *clonefrom* tag, improves *pg_rewind* handling and improves *patronictl* control program.

**Consul support**

- Implement Consul support (Alexander Kukushkin)

  Patroni runs against Consul, in addition to Etcd and Zookeeper. the connection parameters can be configured in the YAML file.

**New and improved tags**

- Implement *noloadbalance* tag (Alexander)

  This tag makes Patroni always return that the replica is not available to the load balancer.

- Change the implementation of the *clonefrom* tag (Alexander)

  Previously, a node name had to be supplied to the *clonefrom*, forcing a tagged replica to clone from the specific node. The new implementation makes *clonefrom* a boolean tag: if it is set to true, the replica becomes a candidate for other replicas to clone from it. When multiple candidates are present, the replicas picks one randomly.

**Stability and security improvements**

- Numerous reliability improvements (Alexander)

  Removes some spurious error messages, improves the stability of the failover, addresses some corner cases with reading data from DCS, shutdown, demote and reattaching of the former leader.

- Improve systems script to avoid killing Patroni children on stop (Jan Keirse, Alexander Kukushkin)

  Previously, when stopping Patroni, *systemd* also sent a signal to PostgreSQL. Since Patroni also tried to stop PostgreSQL by itself, it resulted in sending to different shutdown requests (the smart shutdown, followed by the fast shutdown). That resulted in replicas disconnecting too early and a former master not being able to rejoin after demote. Fix by Jan with prior research by Alexander.

- Eliminate some cases where the former master was unable to call pg_rewind before rejoining as a replica (Oleksii Kliukin)

  Previously, we only called *pg_rewind* if the former master had crashed. Change this to always run pg_rewind for the former master as long as pg_rewind is present in the system. This fixes the case when the master is shut down before the replicas managed to get the latest changes (i.e. during the "smart" shutdown).

- Numerous improvements to unit- and acceptance- tests, in particular, enable support for Zookeeper and Consul (Alexander).

- Make Travis CI faster and implement support for running tests against Zookeeper (Exhibitor) and Consul (Alexander)

  Both unit and acceptance tests run automatically against Etcd, Zookeeper and Consul on each commit or pull-request.

- Clear environment variables before calling PostgreSQL commands from Patroni (Feike Steenbergen)

  This prevents  a possibility of reading system environment variables by connecting to the PostgreSQL cluster managed by Patroni.

**Configuration and control changes**

- Unify patronictl and Patroni configuration (Feike)

  patronictl can use the same configuration file as Patroni itself.

- Enable Patroni to read the configuration from the environment variables (Oleksii)

  This simplifies generating configuration for Patroni automatically, or merging a single configuration from different sources.

- Include database system identifier in the information returned by the API (Feike)

- Implement *delete_cluster* for all available DCSs (Alexander)

  Enables support for DCSs other than Etcd in patronictl.


Version 0.80
------------

This release adds support for *cascading replication* and simplifies Patroni management by providing *scheduled failovers*. One may use older versions of Patroni (in particular, 0.78) combined with this one in order to migrate to the new release. Note that the scheduled failover and cascading replication related features will only work with Patroni 0.80 and above.

**Cascading replication**

 - Add support for the *replicatefrom* and *clonefrom* tags for the patroni node (Oleksii Kliukin).

 The tag *replicatefrom*  allows a replica to use an arbitrary node a source, not necessary the master. The *clonefrom* does the same for the initial backup. Together, they enable Patroni to fully support cascading replication.

- Add support for running replication methods to initialize the replica even without a running replication connection (Oleksii).

 This is useful in order to create replicas from the snapshots stored on S3 or FTP.  A replication method that does not require a running replication connection should supply *no_master: true* in the yaml configuration. Those scripts will still be called in order if the replication connection is present.

**Patronictl, API and DCS improvements**

- Implement scheduled failovers (Feike Steenbergen).

  Failovers can be scheduled to happen at a certain time in the future, using either patronictl, or API calls.

- Add support for *dbuser* and *password* parameters in patronictl (Feike).

- Add PostgreSQL version to the health check output (Feike).

- Improve Zookeeper support in patronictl (Oleksandr Shulgin)

- Migrate to python-etcd 0.43 (Alexander Kukushkin)

**Configuration**

- Add a sample systems configuration script for Patroni (Jan Keirse).

- Fix the problem of Patroni ignoring the superuser name specified in the configuration file for DB connections  (Alexander).

- Fix the handling of CTRL-C by creating a separate session ID and process group for the postmaster launched by Patroni (Alexander).

**Tests**

- Add acceptance tests with *behave* in order to check real-world scenarios of running Patroni (Alexander, Oleksii).

  The tests can be launched manually using the *behave* command. They are also launched automatically for pull requests and after commits.

  Release notes for some older versions can be found on `project's github page <https://github.com/zalando/patroni/releases>`__.
