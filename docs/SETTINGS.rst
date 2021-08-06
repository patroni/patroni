.. _settings:

===========================
YAML Configuration Settings
===========================

.. _dynamic_configuration_settings:

Dynamic configuration settings
------------------------------

Dynamic configuration is stored in the DCS (Distributed Configuration Store) and applied on all cluster nodes. Some parameters, like **loop_wait**, **ttl**, **postgresql.parameters.max_connections**, **postgresql.parameters.max_worker_processes** and so on could be set only in the dynamic configuration. Some other parameters like **postgresql.listen**, **postgresql.data_dir** could be set only locally, i.e. in the Patroni config file or via :ref:`configuration <environment>` variable. In most cases the local configuration will override the dynamic configuration. In order to change the dynamic configuration you can use either ``patronictl edit-config`` tool or Patroni :ref:`REST API <rest_api>`.

-  **loop\_wait**: the number of seconds the loop will sleep. Default value: 10
-  **ttl**: the TTL to acquire the leader lock (in seconds). Think of it as the length of time before initiation of the automatic failover process. Default value: 30
-  **retry\_timeout**: timeout for DCS and PostgreSQL operation retries (in seconds). DCS or network issues shorter than this will not cause Patroni to demote the leader. Default value: 10
-  **maximum\_lag\_on\_failover**: the maximum bytes a follower may lag to be able to participate in leader election.
-  **maximum\_lag\_on\_syncnode**: the maximum bytes a synchronous follower may lag before it is considered as an unhealthy candidate and swapped by healthy asynchronous follower. Patroni utilize the max replica lsn if there is more than one follower, otherwise it will use leader's current wal lsn. Default is -1, Patroni will not take action to swap synchronous unhealthy follower when the value is set to 0 or below. Please set the value high enough so Patroni won't swap synchrounous follower fequently during high transaction volume.
-  **max\_timelines\_history**: maximum number of timeline history items kept in DCS.  Default value: 0. When set to 0, it keeps the full history in DCS.
-  **master\_start\_timeout**: the amount of time a master is allowed to recover from failures before failover is triggered (in seconds). Default is 300 seconds. When set to 0 failover is done immediately after a crash is detected if possible. When using asynchronous replication a failover can cause lost transactions. Worst case failover time for master failure is: loop\_wait + master\_start\_timeout + loop\_wait, unless master\_start\_timeout is zero, in which case it's just loop\_wait. Set the value according to your durability/availability tradeoff.
- **master\_stop\_timeout**: The number of seconds Patroni is allowed to wait when stopping Postgres and effective only when synchronous_mode is enabled. When set to > 0 and the synchronous_mode is enabled, Patroni sends SIGKILL to the postmaster if the stop operation is running for more than the value set by master_stop_timeout. Set the value according to your durability/availability tradeoff. If the parameter is not set or set <= 0, master_stop_timeout does not apply.
-  **synchronous\_mode**: turns on synchronous replication mode. In this mode a replica will be chosen as synchronous and only the latest leader and synchronous replica are able to participate in leader election. Synchronous mode makes sure that successfully committed transactions will not be lost at failover, at the cost of losing availability for writes when Patroni cannot ensure transaction durability. See :ref:`replication modes documentation <replication_modes>` for details.
-  **synchronous\_mode\_strict**: prevents disabling synchronous replication if no synchronous replicas are available, blocking all client writes to the master. See :ref:`replication modes documentation <replication_modes>` for details.
-  **postgresql**:
    -  **use\_pg\_rewind**: whether or not to use pg_rewind. Defaults to `false`.
    -  **use\_slots**: whether or not to use replication slots. Defaults to `true` on PostgreSQL 9.4+.
    -  **recovery\_conf**: additional configuration settings written to recovery.conf when configuring follower. There is no recovery.conf anymore in PostgreSQL 12, but you may continue using this section, because Patroni handles it transparently.
    -  **parameters**: list of configuration settings for Postgres.
-  **standby\_cluster**: if this section is defined, we want to bootstrap a standby cluster.
    -  **host**: an address of remote master
    -  **port**: a port of remote master
    -  **primary\_slot\_name**: which slot on the remote master to use for replication. This parameter is optional, the default value is derived from the instance name (see function `slot_name_from_member_name`).
    -  **create\_replica\_methods**: an ordered list of methods that can be used to bootstrap standby leader from the remote master, can be different from the list defined in :ref:`postgresql_settings`
    -  **restore\_command**: command to restore WAL records from the remote master to standby leader, can be different from the list defined in :ref:`postgresql_settings`
    -  **archive\_cleanup\_command**: cleanup command for standby leader
    -  **recovery\_min\_apply\_delay**: how long to wait before actually apply WAL records on a standby leader
-  **slots**: define permanent replication slots. These slots will be preserved during switchover/failover. The logical slots are copied from the primary to a standby with restart, and after that their position advanced every **loop_wait** seconds (if necessary). Copying logical slot files performed via ``libpq`` connection and using either rewind or superuser credentials (see **postgresql.authentication** section). There is always a chance that the logical slot position on the replica is a bit behind the former primary, therefore application should be prepared that some messages could be received the second time after the failover. The easiest way of doing so - tracking ``confirmed_flush_lsn``. Enabling permanent logical replication slots requires **postgresql.use_slots** to be set and will also automatically enable the ``hot_standby_feedback``. Since the failover of logical replication slots is unsafe on PostgreSQL 9.6 and older and PostgreSQL version 10 is missing some important functions, the feature only works with PostgreSQL 11+.
    -  **my_slot_name**: the name of replication slot. If the permanent slot name matches with the name of the current primary it will not be created. Everything else is the responsibility of the operator to make sure that there are no clashes in names between replication slots automatically created by Patroni for members and permanent replication slots.
        -  **type**: slot type. Could be ``physical`` or ``logical``. If the slot is logical, you have to additionally define ``database`` and ``plugin``.
        -  **database**: the database name where logical slots should be created.
        -  **plugin**: the plugin name for the logical slot.
-  **ignore_slots**: list of sets of replication slot properties for which Patroni should ignore matching slots. This configuration/feature/etc. is useful when some replication slots are managed outside of Patroni. Any subset of matching properties will cause a slot to be ignored.
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

Global/Universal
----------------
-  **name**: the name of the host. Must be unique for the cluster.
-  **namespace**: path within the configuration store where Patroni will keep information about the cluster. Default value: "/service"
-  **scope**: cluster name

Log
---
-  **level**: sets the general logging level. Default value is **INFO** (see `the docs for Python logging <https://docs.python.org/3.6/library/logging.html#levels>`_)
-  **traceback\_level**: sets the level where tracebacks will be visible. Default value is **ERROR**. Set it to **DEBUG** if you want to see tracebacks only if you enable **log.level=DEBUG**.
-  **format**: sets the log formatting string. Default value is **%(asctime)s %(levelname)s: %(message)s** (see `the LogRecord attributes <https://docs.python.org/3.6/library/logging.html#logrecord-attributes>`_)
-  **dateformat**: sets the datetime formatting string. (see the `formatTime() documentation <https://docs.python.org/3.6/library/logging.html#logging.Formatter.formatTime>`_)
-  **max\_queue\_size**: Patroni is using two-step logging. Log records are written into the in-memory queue and there is a separate thread which pulls them from the queue and writes to stderr or file. The maximum size of the internal queue is limited by default by **1000** records, which is enough to keep logs for the past 1h20m.
-  **dir**: Directory to write application logs to. The directory must exist and be writable by the user executing Patroni. If you set this value, the application will retain 4 25MB logs by default. You can tune those retention values with `file_num` and `file_size` (see below).
-  **file\_num**: The number of application logs to retain.
-  **file\_size**: Size of patroni.log file (in bytes) that triggers a log rolling.
-  **loggers**: This section allows redefining logging level per python module
    -  **patroni.postmaster: WARNING**
    -  **urllib3: DEBUG**

.. _bootstrap_settings:

Bootstrap configuration
-----------------------
-  **bootstrap**:
    -  **dcs**: This section will be written into `/<namespace>/<scope>/config` of the given configuration store after initializing of new cluster. The global dynamic configuration for the cluster. Under the ``bootstrap.dcs`` you can put any of the parameters described in the :ref:`Dynamic Configuration settings <dynamic_configuration_settings>` and after Patroni initialized (bootstrapped) the new cluster, it will write this section into `/<namespace>/<scope>/config` of the configuration store. All later changes of ``bootstrap.dcs`` will not take any effect! If you want to change them please use either ``patronictl edit-config`` or Patroni :ref:`REST API <rest_api>`.
    -  **method**: custom script to use for bootstrapping this cluster.
       See :ref:`custom bootstrap methods documentation <custom_bootstrap>` for details.
       When ``initdb`` is specified revert to the default ``initdb`` command. ``initdb`` is also triggered when no ``method``
       parameter is present in the configuration file.
    -  **initdb**: List options to be passed on to initdb.
            -  **- data-checksums**: Must be enabled when pg_rewind is needed on 9.3.
            -  **- encoding: UTF8**: default encoding for new databases.
            -  **- locale: UTF8**: default locale for new databases.
    -  **pg\_hba**: list of lines that you should add to pg\_hba.conf.
            -  **- host all all 0.0.0.0/0 md5**.
            -  **- host replication replicator 127.0.0.1/32 md5**: A line like this is required for replication.
    -  **users**: Some additional users which need to be created after initializing new cluster
        -  **admin**: the name of user
            -  **password: zalando**:
            -  **options**: list of options for CREATE USER statement
                -  **- createrole**
                -  **- createdb**
    -  **post\_bootstrap** or **post\_init**: An additional script that will be executed after initializing the cluster. The script receives a connection string URL (with the cluster superuser as a user name). The PGPASSFILE variable is set to the location of pgpass file.

.. _consul_settings:

Consul
------
Most of the parameters are optional, but you have to specify one of the **host** or **url**

-  **host**: the host:port for the Consul local agent.
-  **url**: url for the Consul local agent, in format: http(s)://host:port.
-  **port**: (optional) Consul port.
-  **scheme**: (optional) **http** or **https**, defaults to **http**.
-  **token**: (optional) ACL token.
-  **verify**: (optional) whether to verify the SSL certificate for HTTPS requests.
-  **cacert**: (optional) The ca certificate. If present it will enable validation.
-  **cert**: (optional) file with the client certificate.
-  **key**: (optional) file with the client key. Can be empty if the key is part of **cert**.
-  **dc**: (optional) Datacenter to communicate with. By default the datacenter of the host is used.
-  **consistency**: (optional) Select consul consistency mode. Possible values are ``default``, ``consistent``, or ``stale`` (more details in `consul API reference <https://www.consul.io/api/features/consistency.html/>`__)
-  **checks**: (optional) list of Consul health checks used for the session. By default an empty list is used.
-  **register\_service**: (optional) whether or not to register a service with the name defined by the scope parameter and the tag master, replica or standby-leader depending on the node's role. Defaults to **false**.
-  **service\_tags**: (optional) additional static tags to add to the Consul service apart from the role (``master``/``replica``/``standby-leader``).  By default an empty list is used.
-  **service\_check\_interval**: (optional) how often to perform health check against registered url.

The ``token`` needs to have the following ACL permissions:

::

    service_prefix "${scope}" {
        policy = "write"
    }
    key_prefix "${namespace}/${scope}" {
        policy = "write"
    }
    session_prefix "" {
        policy = "write"
    }

Etcd
----
Most of the parameters are optional, but you have to specify one of the **host**, **hosts**, **url**, **proxy** or **srv**

-  **host**: the host:port for the etcd endpoint.
-  **hosts**: list of etcd endpoint in format host1:port1,host2:port2,etc... Could be a comma separated string or an actual yaml list.
-  **use\_proxies**: If this parameter is set to true, Patroni will consider **hosts** as a list of proxies and will not perform a topology discovery of etcd cluster.
-  **url**: url for the etcd.
-  **proxy**: proxy url for the etcd. If you are connecting to the etcd using proxy, use this parameter instead of **url**.
-  **srv**: Domain to search the SRV record(s) for cluster autodiscovery.
-  **protocol**: (optional) http or https, if not specified http is used. If the **url** or **proxy** is specified - will take protocol from them.
-  **username**: (optional) username for etcd authentication.
-  **password**: (optional) password for etcd authentication.
-  **cacert**: (optional) The ca certificate. If present it will enable validation.
-  **cert**: (optional) file with the client certificate.
-  **key**: (optional) file with the client key. Can be empty if the key is part of **cert**.

Etcdv3
------
If you want that Patroni works with Etcd cluster via protocol version 3, you need to use the ``etcd3`` section in the Patroni configuration file. All configuration parameters are the same as for ``etcd``.

.. warning::
    Keys created with protocol version 2 are not visible with protocol version 3 and the other way around, therefore it is not possible to switch from ``etcd`` to ``etcd3`` just by updating Patroni config file.


ZooKeeper
----------
-  **hosts**: List of ZooKeeper cluster members in format: ['host1:port1', 'host2:port2', 'etc...'].
-  **use_ssl**: (optional) Whether SSL is used or not. Defaults to ``false``. If set to ``false``, all SSL specific parameters are ignored.
-  **cacert**: (optional) The CA certificate. If present it will enable validation.
-  **cert**: (optional) File with the client certificate.
-  **key**: (optional) File with the client key.
-  **key_password**: (optional) The client key password.
-  **verify**: (optional) Whether to verify certificate or not. Defaults to ``true``.

.. note::
    It is required to install ``kazoo>=2.6.0`` to support SSL.


Exhibitor
---------
-  **hosts**: initial list of Exhibitor (ZooKeeper) nodes in format: 'host1,host2,etc...'. This list updates automatically whenever the Exhibitor (ZooKeeper) cluster topology changes.
-  **poll\_interval**: how often the list of ZooKeeper and Exhibitor nodes should be updated from Exhibitor.
-  **port**: Exhibitor port.

.. _kubernetes_settings:

Kubernetes
----------
-  **bypass\_api\_service**: (optional) When communicating with the Kubernetes API, Patroni is usually relying on the `kubernetes` service, the address of which is exposed in the pods via the `KUBERNETES_SERVICE_HOST` environment variable. If `bypass_api_service` is set to ``true``, Patroni will resolve the list of API nodes behind the service and connect directly to them.
-  **namespace**: (optional) Kubernetes namespace where Patroni pod is running. Default value is `default`.
-  **labels**: Labels in format ``{label1: value1, label2: value2}``. These labels will be used to find existing objects (Pods and either Endpoints or ConfigMaps) associated with the current cluster. Also Patroni will set them on every object (Endpoint or ConfigMap) it creates.
-  **scope\_label**: (optional) name of the label containing cluster name. Default value is `cluster-name`.
-  **role\_label**: (optional) name of the label containing role (master or replica). Patroni will set this label on the pod it runs in. Default value is ``role``.
-  **use\_endpoints**: (optional) if set to true, Patroni will use Endpoints instead of ConfigMaps to run leader elections and keep cluster state.
-  **pod\_ip**: (optional) IP address of the pod Patroni is running in. This value is required when `use_endpoints` is enabled and is used to populate the leader endpoint subsets when the pod's PostgreSQL is promoted.
-  **ports**: (optional) if the Service object has the name for the port, the same name must appear in the Endpoint object, otherwise service won't work. For example, if your service is defined as ``{Kind: Service, spec: {ports: [{name: postgresql, port: 5432, targetPort: 5432}]}}``, then you have to set ``kubernetes.ports: [{"name": "postgresql", "port": 5432}]`` and Patroni will use it for updating subsets of the leader Endpoint. This parameter is used only if `kubernetes.use_endpoints` is set.
-  **cacert**: (optional) Specifies the file with the CA_BUNDLE file with certificates of trusted CAs to use while verifying Kubernetes API SSL certs. If not provided, patroni will use the value provided by the ServiceAccount secret.


.. _raft_settings:

Raft
----
-  **self\_addr**: ``ip:port`` to listen on for Raft connections. The ``self_addr`` must be accessible from other nodes of the cluster. If not set, the node will not participate in consensus.
-  **bind\_addr**: (optional) ``ip:port`` to listen on for Raft connections. If not specified the ``self_addr`` will be used.
-  **partner\_addrs**: list of other Patroni nodes in the cluster in format: ['ip1:port', 'ip2:port', 'etc...']
-  **data\_dir**: directory where to store Raft log and snapshot. If not specified the current working directory is used.
-  **password**: (optional) Encrypt Raft traffic with a specified password, requires ``cryptography`` python module.

  Short FAQ about Raft implementation

  - Q: How to list all the nodes providing consensus?

    A: ``syncobj_admin -conn host:port`` -status where the host:port is the address of one of the cluster nodes

  - Q: Node that was a part of consensus and has gone and I can't reuse the same IP for other node. How to remove this node from the consensus?

    A: ``syncobj_admin -conn host:port -remove host2:port2`` where the ``host2:port2`` is the address of the node you want to remove from consensus.

  - Q: Where to get the ``syncobj_admin`` utility?

    A: It is installed together with ``pysyncobj`` module (python RAFT implementation), which is Patroni dependency.

  - Q: it is possible to run Patroni node without adding in to the consensus?

    A: Yes, just comment out or remove ``raft.self_addr`` from Patroni configuration.

  - Q: It is possible to run Patroni and PostgreSQL only on two nodes?

    A: Yes, on the third node you can run ``patroni_raft_controller`` (without Patroni and PostgreSQL). In such a setup, one can temporarily lose one node without affecting the primary.


.. _postgresql_settings:

PostgreSQL
----------
-  **postgresql**:
    -  **authentication**:
        -  **superuser**:
            -  **username**: name for the superuser, set during initialization (initdb) and later used by Patroni to connect to the postgres.
            -  **password**: password for the superuser, set during initialization (initdb).
            -  **sslmode**: (optional) maps to the `sslmode <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLMODE>`__ connection parameter, which allows a client to specify the type of TLS negotiation mode with the server. For more information on how each mode works, please visit the `PostgreSQL documentation <https://www.postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-SSLMODE-STATEMENTS>`__. The default mode is ``prefer``.
            -  **sslkey**: (optional) maps to the `sslkey <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLKEY>`__ connection parameter, which specifies the location of the secret key used with the client's certificate.
            -  **sslpassword**: (optional) maps to the `sslpassword <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLPASSWORD>`__ connection parameter, which specifies the password for the secret key specified in ``sslkey``.
            -  **sslcert**: (optional) maps to the `sslcert <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLCERT>`__ connection parameter, which specifies the location of the client certificate.
            -  **sslrootcert**: (optional) maps to the `sslrootcert <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLROOTCERT>`__ connection parameter, which specifies the location of a file containing one ore more certificate authorities (CA) certificates that the client will use to verify a server's certificate.
            -  **sslcrl**: (optional) maps to the `sslcrl <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLCRL>`__ connection parameter, which specifies the location of a file containing a certificate revocation list. A client will reject connecting to any server that has a certificate present in this list.
            -  **gssencmode**: (optional) maps to the `gssencmode <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-GSSENCMODE>`__ connection parameter, which determines whether or with what priority a secure GSS TCP/IP connection will be negotiated with the server
            -  **channel_binding**: (optional) maps to the `channel_binding <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-CHANNEL-BINDING>`__ connection parameter, which controls the client's use of channel binding.
        -  **replication**:
            -  **username**: replication username; the user will be created during initialization. Replicas will use this user to access master via streaming replication
            -  **password**: replication password; the user will be created during initialization.
            -  **sslmode**: (optional) maps to the `sslmode <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLMODE>`__ connection parameter, which allows a client to specify the type of TLS negotiation mode with the server. For more information on how each mode works, please visit the `PostgreSQL documentation <https://www.postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-SSLMODE-STATEMENTS>`__. The default mode is ``prefer``.
            -  **sslkey**: (optional) maps to the `sslkey <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLKEY>`__ connection parameter, which specifies the location of the secret key used with the client's certificate.
            -  **sslpassword**: (optional) maps to the `sslpassword <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLPASSWORD>`__ connection parameter, which specifies the password for the secret key specified in ``sslkey``.
            -  **sslcert**: (optional) maps to the `sslcert <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLCERT>`__ connection parameter, which specifies the location of the client certificate.
            -  **sslrootcert**: (optional) maps to the `sslrootcert <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLROOTCERT>`__ connection parameter, which specifies the location of a file containing one ore more certificate authorities (CA) certificates that the client will use to verify a server's certificate.
            -  **sslcrl**: (optional) maps to the `sslcrl <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLCRL>`__ connection parameter, which specifies the location of a file containing a certificate revocation list. A client will reject connecting to any server that has a certificate present in this list.
            -  **gssencmode**: (optional) maps to the `gssencmode <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-GSSENCMODE>`__ connection parameter, which determines whether or with what priority a secure GSS TCP/IP connection will be negotiated with the server
            -  **channel_binding**: (optional) maps to the `channel_binding <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-CHANNEL-BINDING>`__ connection parameter, which controls the client's use of channel binding.
        -  **rewind**:
            -  **username**: name for the user for ``pg_rewind``; the user will be created during initialization of postgres 11+ and all necessary `permissions <https://www.postgresql.org/docs/11/app-pgrewind.html#id-1.9.5.8.8>`__ will be granted.
            -  **password**: password for the user for ``pg_rewind``; the user will be created during initialization.
            -  **sslmode**: (optional) maps to the `sslmode <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLMODE>`__ connection parameter, which allows a client to specify the type of TLS negotiation mode with the server. For more information on how each mode works, please visit the `PostgreSQL documentation <https://www.postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-SSLMODE-STATEMENTS>`__. The default mode is ``prefer``.
            -  **sslkey**: (optional) maps to the `sslkey <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLKEY>`__ connection parameter, which specifies the location of the secret key used with the client's certificate.
            -  **sslpassword**: (optional) maps to the `sslpassword <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLPASSWORD>`__ connection parameter, which specifies the password for the secret key specified in ``sslkey``.
            -  **sslcert**: (optional) maps to the `sslcert <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLCERT>`__ connection parameter, which specifies the location of the client certificate.
            -  **sslrootcert**: (optional) maps to the `sslrootcert <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLROOTCERT>`__ connection parameter, which specifies the location of a file containing one ore more certificate authorities (CA) certificates that the client will use to verify a server's certificate.
            -  **sslcrl**: (optional) maps to the `sslcrl <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLCRL>`__ connection parameter, which specifies the location of a file containing a certificate revocation list. A client will reject connecting to any server that has a certificate present in this list.
            -  **gssencmode**: (optional) maps to the `gssencmode <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-GSSENCMODE>`__ connection parameter, which determines whether or with what priority a secure GSS TCP/IP connection will be negotiated with the server
            -  **channel_binding**: (optional) maps to the `channel_binding <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-CHANNEL-BINDING>`__ connection parameter, which controls the client's use of channel binding.
    -  **callbacks**: callback scripts to run on certain actions. Patroni will pass the action, role and cluster name. (See scripts/aws.py as an example of how to write them.)
            -  **on\_reload**: run this script when configuration reload is triggered.
            -  **on\_restart**: run this script when the postgres restarts (without changing role).
            -  **on\_role\_change**: run this script when the postgres is being promoted or demoted.
            -  **on\_start**: run this script when the postgres starts.
            -  **on\_stop**: run this script when the postgres stops.
    -  **connect\_address**: IP address + port through which Postgres is accessible from other nodes and applications.
    -  **create\_replica\_methods**: an ordered list of the create methods for turning a Patroni node into a new replica.
       "basebackup" is the default method; other methods are assumed to refer to scripts, each of which is configured as its
       own config item. See :ref:`custom replica creation methods documentation <custom_replica_creation>` for further explanation.
    -  **data\_dir**: The location of the Postgres data directory, either :ref:`existing <existing_data>` or to be initialized by Patroni.
    -  **config\_dir**: The location of the Postgres configuration directory, defaults to the data directory. Must be writable by Patroni.
    -  **bin\_dir**: Path to PostgreSQL binaries (pg_ctl, pg_rewind, pg_basebackup, postgres). The default value is an empty string meaning that PATH environment variable will be used to find the executables.
    -  **listen**: IP address + port that Postgres listens to; must be accessible from other nodes in the cluster, if you're using streaming replication. Multiple comma-separated addresses are permitted, as long as the port component is appended after to the last one with a colon, i.e. ``listen: 127.0.0.1,127.0.0.2:5432``. Patroni will use the first address from this list to establish local connections to the PostgreSQL node.
    -  **use\_unix\_socket**: specifies that Patroni should prefer to use unix sockets to connect to the cluster. Default value is ``false``. If ``unix_socket_directories`` is defined, Patroni will use the first suitable value from it to connect to the cluster and fallback to tcp if nothing is suitable. If ``unix_socket_directories`` is not specified in ``postgresql.parameters``, Patroni will assume that the default value should be used and omit ``host`` from the connection parameters.
    -  **use\_unix\_socket\_repl**: specifies that Patroni should prefer to use unix sockets for replication user cluster connection. Default value is ``false``. If ``unix_socket_directories`` is defined, Patroni will use the first suitable value from it to connect to the cluster and fallback to tcp if nothing is suitable. If ``unix_socket_directories`` is not specified in ``postgresql.parameters``, Patroni will assume that the default value should be used and omit ``host`` from the connection parameters.
    -  **pgpass**: path to the `.pgpass <https://www.postgresql.org/docs/current/static/libpq-pgpass.html>`__ password file. Patroni creates this file before executing pg\_basebackup, the post_init script and under some other circumstances. The location must be writable by Patroni.
    -  **recovery\_conf**: additional configuration settings written to recovery.conf when configuring follower.
    -  **custom\_conf** : path to an optional custom ``postgresql.conf`` file, that will be used in place of ``postgresql.base.conf``. The file must exist on all cluster nodes, be readable by PostgreSQL and will be included from its location on the real ``postgresql.conf``. Note that Patroni will not monitor this file for changes, nor backup it. However, its settings can still be overridden by Patroni's own configuration facilities - see :ref:`dynamic configuration <dynamic_configuration>` for details.
    -  **parameters**: list of configuration settings for Postgres. Many of these are required for replication to work.
    -  **pg\_hba**: list of lines that Patroni will use to generate ``pg_hba.conf``. This parameter has higher priority than ``bootstrap.pg_hba``. Together with :ref:`dynamic configuration <dynamic_configuration>` it simplifies management of ``pg_hba.conf``.
            -  **- host all all 0.0.0.0/0 md5**.
            -  **- host replication replicator 127.0.0.1/32 md5**: A line like this is required for replication.
    -  **pg\_ident**: list of lines that Patroni will use to generate ``pg_ident.conf``. Together with :ref:`dynamic configuration <dynamic_configuration>` it simplifies management of ``pg_ident.conf``.
            -  **- mapname1 systemname1 pguser1**.
            -  **- mapname1 systemname2 pguser2**.
    -  **pg\_ctl\_timeout**: How long should pg_ctl wait when doing ``start``, ``stop`` or ``restart``. Default value is 60 seconds.
    -  **use\_pg\_rewind**: try to use pg\_rewind on the former leader when it joins cluster as a replica.
    -  **remove\_data\_directory\_on\_rewind\_failure**: If this option is enabled, Patroni will remove the PostgreSQL data directory and recreate the replica. Otherwise it will try to follow the new leader. Default value is **false**.
    -  **remove\_data\_directory\_on\_diverged\_timelines**: Patroni will remove the PostgreSQL data directory and recreate the replica if it notices that timelines are diverging and the former master can not start streaming from the new master. This option is useful when ``pg_rewind`` can not be used. Default value is **false**.
    -  **replica\_method**: for each create_replica_methods other than basebackup, you would add a configuration section of the same name. At a minimum, this should include "command" with a full path to the actual script to be executed. Other configuration parameters will be passed along to the script in the form "parameter=value".
    -  **pre\_promote**: a fencing script that executes during a failover after acquiring the leader lock but before promoting the replica. If the script exits with a non-zero code, Patroni does not promote the replica and removes the leader key from DCS.

REST API
--------
-  **restapi**:
        -  **connect\_address**: IP address (or hostname) and port, to access the Patroni's :ref:`REST API <rest_api>`. All the members of the cluster must be able to connect to this address, so unless the Patroni setup is intended for a demo inside the localhost, this address must be a non "localhost" or loopback address (ie: "localhost" or "127.0.0.1"). It can serve as an endpoint for HTTP health checks (read below about the "listen" REST API parameter), and also for user queries (either directly or via the REST API), as well as for the health checks done by the cluster members during leader elections (for example, to determine whether the master is still running, or if there is a node which has a WAL position that is ahead of the one doing the query; etc.) The connect_address is put in the member key in DCS, making it possible to translate the member name into the address to connect to its REST API.

        -  **listen**: IP address (or hostname) and port that Patroni will listen to for the REST API - to provide also the same health checks and cluster messaging between the participating nodes, as described above. to provide health-check information for HAProxy (or any other load balancer capable of doing a HTTP "OPTION" or "GET" checks).

        -  **authentication**: (optional)
            -  **username**: Basic-auth username to protect unsafe REST API endpoints.
            -  **password**: Basic-auth password to protect unsafe REST API endpoints.
        -  **certfile**: (optional): Specifies the file with the certificate in the PEM format. If the certfile is not specified or is left empty, the API server will work without SSL.
        -  **keyfile**: (optional): Specifies the file with the secret key in the PEM format.
        -  **keyfile_password**: (optional): Specifies a password for decrypting the keyfile.
        -  **cafile**: (optional): Specifies the file with the CA_BUNDLE with certificates of trusted CAs to use while verifying client certs.
        -  **ciphers**: (optional): Specifies the permitted cipher suites (e.g. "ECDHE-RSA-AES256-GCM-SHA384:DHE-RSA-AES256-GCM-SHA384:ECDHE-RSA-AES128-GCM-SHA256:DHE-RSA-AES128-GCM-SHA256:!SSLv1:!SSLv2:!SSLv3:!TLSv1:!TLSv1.1")
        -  **verify\_client**: (optional): ``none`` (default), ``optional`` or ``required``. When ``none`` REST API will not check client certificates. When ``required`` client certificates are required for all REST API calls. When ``optional`` client certificates are required for all unsafe REST API endpoints. When ``required`` is used, then client authentication succeeds, if the certificate signature verification succeeds.  For ``optional`` the client cert will only be checked for ``PUT``, ``POST``, ``PATCH``, and ``DELETE`` requests.
        -  **allowlist**: (optional): Specifies the set of hosts that are allowed to call unsafe REST API endpoints. The single element could be a host name, an IP address or a network address using CIDR notation. By default ``allow all`` is used. In case if ``allowlist`` or ``allowlist_include_members`` are set, anything that is not included is rejected.
        -  **allowlist\_include\_members**: (optional): If set to ``true`` it allows accessing unsafe REST API endpoints from other cluster members registered in DCS (IP address or hostname is taken from the members ``api_url``). Be careful, it might happen that OS will use a different IP for outgoing connections.
        -  **http\_extra\_headers**: (optional): HTTP headers let the REST API server pass additional information with an HTTP response.
        -  **https\_extra\_headers**: (optional): HTTPS headers let the REST API server pass additional information with an HTTP response when TLS is enabled. This will also pass additional information set in ``http_extra_headers``.

Here is an example of both **http_extra_headers** and **https_extra_headers**:

.. code:: YAML

        restapi:
          listen: <listen>
          connect_address: <connect_address>
          authentication:
            username: <username>
            password: <password>
          http_extra_headers:
            'X-Frame-Options': 'SAMEORIGIN'
            'X-XSS-Protection': '1; mode=block'
            'X-Content-Type-Options': 'nosniff'
          cafile: <ca file>
          certfile: <cert>
          keyfile: <key>
          https_extra_headers:
            'Strict-Transport-Security': 'max-age=31536000; includeSubDomains'

.. _patronictl_settings:

CTL
---
- **ctl**: (optional)
    -  **insecure**: Allow connections to REST API without verifying SSL certs.
    -  **cacert**: Specifies the file with the CA_BUNDLE file or directory with certificates of trusted CAs to use while verifying REST API SSL certs. If not provided patronictl will use the value provided for REST API "cafile" parameter.
    -  **certfile**: Specifies the file with the client certificate in the PEM format. If not provided patronictl will use the value provided for REST API "certfile" parameter.
    -  **keyfile**: Specifies the file with the client secret key in the PEM format. If not provided patronictl will use the value provided for REST API "keyfile" parameter.

Watchdog
--------
- **mode**: ``off``, ``automatic`` or ``required``. When ``off`` watchdog is disabled. When ``automatic`` watchdog will be used if available, but ignored if it is not. When ``required`` the node will not become a leader unless watchdog can be successfully enabled.
- **device**: Path to watchdog device. Defaults to ``/dev/watchdog``.
- **safety_margin**: Number of seconds of safety margin between watchdog triggering and leader key expiration.

.. _tags_settings:

Tags
----
- **nofailover**: ``true`` or ``false``, controls whether this node is allowed to participate in the leader race and become a leader. Defaults to ``false``
- **clonefrom**: ``true`` or ``false``. If set to ``true`` other nodes might prefer to use this node for bootstrap (take ``pg_basebackup`` from). If there are several nodes with ``clonefrom`` tag set to ``true`` the node to bootstrap from will be chosen randomly. The default value is ``false``.
- **noloadbalance**: ``true`` or ``false``. If set to ``true`` the node will return HTTP Status Code 503 for the ``GET /replica`` REST API health-check and therefore will be excluded from the load-balancing. Defaults to ``false``.
- **replicatefrom**: The IP address/hostname of another replica. Used to support cascading replication.
- **nosync**: ``true`` or ``false``. If set to ``true`` the node will never be selected as a synchronous replica.

In addition to these predefined tags, you can also add your own ones:

- **key1**: ``true``
- **key2**: ``false``
- **key3**: ``1.4``
- **key4**: ``"RandomString"``

Tags are visible in the :ref:`REST API <rest_api>` and ``patronictl list`` You can also check for an instance health using these tags. If the tag isn't defined for an instance, or if the respective value doesn't match the querying value, it will return HTTP Status Code 503.
