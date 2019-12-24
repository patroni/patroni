.. _releases:

Release notes
=============

Version 1.6.3
-------------

**Bugfixes**

- Don't expose password when running ``pg_rewind`` (Alexander Kukushkin)

  Bug was introduced in the `#1301 <https://github.com/zalando/patroni/pull/1301>`__

- Apply connection parameters specified in the ``postgresql.authentication`` to ``pg_basebackup`` and custom replica creation methods (Alexander)

  They were relying on url-like connection string and therefore parameters never applied.


Version 1.6.2
-------------

**New features**

- Implemented ``patroni --version`` (Igor Yanchenko)

  It prints the current version of Patroni and exits.

- Set the ``user-agent`` http header for all http requests (Alexander Kukushkin)

  Patroni is communicating with Consul, Etcd, and Kubernetes API via the http protocol. Having a specifically crafted ``user-agent`` (example: ``Patroni/1.6.2 Python/3.6.8 Linux``) might be useful for debugging and monitoring.

- Make it possible to configure log level for exception tracebacks (Igor)

  If you set ``log.traceback_level=DEBUG`` the tracebacks will be visible only when ``log.level=DEBUG``. The default behavior remains the same.


**Stability improvements**

- Avoid importing all DCS modules when searching for the module required by the config file (Alexander)

  There is no need to import modules for Etcd, Consul, and Kubernetes if we need only e.g. Zookeeper. It helps to reduce memory usage and solves the problem of having INFO messages ``Failed to import smth``.

- Removed python ``requests`` module from explicit requirements (Alexander)

  It wasn't used for anything critical, but causing a lot of problems when the new version of ``urllib3`` is released.

- Improve handling of ``etcd.hosts`` written as a comma-separated string instead of YAML array (Igor)

  Previously it was failing when written in format ``host1:port1, host2:port2`` (the space character after the comma).


**Usability improvements**

- Don't force users to choose members from an empty list in ``patronictl`` (Igor)

  If the user provides a wrong cluster name, we will raise an exception rather than ask to choose a member from an empty list.

- Make the error message more helpful if the REST API cannot bind (Igor)

  For an inexperienced user it might be hard to figure out what is wrong from the Python stacktrace.


**Bugfixes**

- Fix calculation of ``wal_buffers`` (Alexander)

  The base unit has been changed from 8 kB blocks to bytes in PostgreSQL 11.

- Use ``passfile`` in ``primary_conninfo`` only on PostgreSQL 10+ (Alexander)

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

  Previously threre was a possibility to loose the last few log lines on shutdown because the logging thread was a ``daemon`` thread.

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

- Fix the problem of not promoting the sync standby that had a name contaning upper case letters (Alexander Kukushkin)

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

- Reload Consul config on SIGHUP (Cameron Daniel, Alexander Kukushkin)

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

- Changed callbacks behavior when role on the node is changed (Alexander)

  If the role was changed from `master` or `standby_leader` to `replica` or from `replica` to `standby_leader`, `on_restart` callback will not be called anymore in favor of `on_role_change` callback.

- Change the way how we start postgres (Alexander)

  Use `multiprocessing.Process` instead of executing itself and `multiprocessing.Pipe` to transmit the postmaster pid to the Patroni process. Before that we were using pipes, what was leaving postmaster process with stdin closed.

**Bug fixes**

- Fix role returned by REST API for the standby leader (Alexander)

  It was incorrectly returning `replica` instead of `standby_leader`

- Wait for callback end if it could not be killed (Julien Tachoires)

  Patroni doesn't have enough privileges to terminate the callback script running under `sudo` what was cancelling the new callback. If the running script could not be killed, Patroni will wait until it finishes and then run the next callback.

- Reduce lock time taken by dcs.get_cluster method (Alexander)

  Due to the lock being held DCS slowness was affecting the REST API health checks causing false positives.

- Improve cleaning of PGDATA when `pg_wal`/`pg_xlog` is a symlink (Julien)

  In this case Patroni will explicitly remove files from the target directory.

- Remove unnecessary usage of os.path.relpath (Ants Aasma)

  It depends on being able to resolve the working directory, what will fail if Patroni is started in a directory that is later unlinked from the filesystem.

- Do not enforce ssl version when communicating with Etcd (Alexander)

  For some unknown reason python3-etcd on debian and ubuntu are not based on the latest version of the package and therefore it enforces TLSv1 which is not supported by Etcd v3. We solved this problem on Patroni side.

Version 1.5.5
-------------

This version introduces the possibility of automatic reinit of the former master, improves patronictl list output and fixes a number of bugs.

**New features**

- Add support of `PATRONI_ETCD_PROTOCOL`, `PATRONI_ETCD_USERNAME` and `PATRONI_ETCD_PASSWORD` environment variables (Étienne M)

  Before it was possible to configure them only in the config file or as a part of `PATRONI_ETCD_URL`, which is not always convenient.

- Make it possible to automatically reinit the former master (Alexander Kukushkin)

  If the pg_rewind is disabled or can't be used, the former master could fail to start as a new replica due to diverged timelines. In this case, the only way to fix it is wiping the data directory and reinitializing. This behavior could be changed by setting `postgresql.remove_data_directory_on_diverged_timelines`. When it is set, Patroni will wipe the data directory and reinitialize the former master automatically.

- Show information about timelines in patronictl list (Alexander)

  It helps to detect stale replicas. In addition to that, `Host` will include ':{port}' if the port value isn't default or there is more than one member running on the same host.

- Create a headless service associated with the $SCOPE-config endpoint (Alexander)

  The "config" endpoint keeps information about the cluster-wide Patroni and Postgres configuration, history file, and last but the most important, it holds the `initialize` key. When the Kubernetes master node is restarted or upgraded, it removes endpoints without services. The headless service will prevent it from being removed.

**Bug fixes**

- Adjust the read timeout for the leader watch blocking query (Alexander)

  According to the Consul documentation, the actual response timeout is increased by a small random amount of additional wait time added to the supplied maximum wait time to spread out the wake up time of any concurrent requests. It adds up to `wait / 16` additional time to the maximum duration. In our case we are adding `wait / 15` or 1 second depending on what is bigger.

- Always use replication=1 when connecting via replication protocol to the postgres (Alexander)

  Starting from Postgres 10 the line in the pg_hba.conf with database=replication doesn't accept connections with the parameter replication=database.

- Don't write primary_conninfo into recovery.conf for wal-only standby cluster (Alexander)

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

- Explicitly secure rw perms for recovery.conf at creation time (Lucas)

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

- Fix broken compatibility with postgres 9.3 (Alexander)

  When opening a replication connection we should specify replication=1, beacuse 9.3 does not understand replication='database'

- Make sure we refresh Consul session at least once per HA loop and improve handling of consul sessions exceptions (Alexander)

  Restart of local consul agent invalidates all sessions related to the node. Not calling session refresh on time and not doing proper handling of session errors was causing demote of the primary.

Version 1.5.2
-------------

Compatibility and bugfix release.

- Compatibility with kazoo-2.6.0 (Alexander Kukushkin)

  In order to make sure that requests are performed with an appropriate timeout, Patroni redefines create_connection method from python-kazoo module. The last release of kazoo slightly changed the way how create_connection method is called.

- Fix Patroni crash when Consul cluster loses the leader (Alexander)

  The crash was happening due to incorrect implementation of touch_member method, it should return boolean and not raise any exceptions.

Version 1.5.1
-------------

This version implements support of permanent replication slots, adds support of pgBackRest and fixes number of bugs.

**New features**

- Permanent replication slots (Alexander Kukushkin)

  Permanent replication slots are preserved on failover/switchover, that is, Patroni on the new primary will create configured replication slots right after doing promote. Slots could be configured with the help of `patronictl edit-config`. The initial configuration could be also done in the :ref:`bootstrap.dcs <settings>`.

- Add pgbackrest support (Yogesh Sharma)

  pgBackrest can restore in existing $PGDATA folder, this allows speedy restore as files which have not changed since last backup are skipped, to support this feature new parameter `keep_data` has been introduced. See :ref:`replica creation method <custom_replica_creation>` section for additional examples.

**Bug fixes**

- A few bugfixes in the "standby cluster" workflow (Alexander)

  Please see https://github.com/zalando/patroni/pull/823 for more details.

- Fix REST API health check when cluster management is paused and DCS is not accessible (Alexander)

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

  Starting from 9.6, `pg_create_physical_replication_slot` function provides an additional boolean parameter `immediately_reserve`. When it is set to `false`, which is also the default, the slot doesn't reserve the WAL position until it receives the first client connection, potentially losing some segments required by the client in a time window between the slot creation and the intiial client connection.

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
