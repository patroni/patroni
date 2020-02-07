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
-  **master\_start\_timeout**: the amount of time a master is allowed to recover from failures before failover is triggered (in seconds). Default is 300 seconds. When set to 0 failover is done immediately after a crash is detected if possible. When using asynchronous replication a failover can cause lost transactions. Worst case failover time for master failure is: loop\_wait + master\_start\_timeout + loop\_wait, unless master\_start\_timeout is zero, in which case it's just loop\_wait. Set the value according to your durability/availability tradeoff.
-  **synchronous\_mode**: turns on synchronous replication mode. In this mode a replica will be chosen as synchronous and only the latest leader and synchronous replica are able to participate in leader election. Synchronous mode makes sure that successfully committed transactions will not be lost at failover, at the cost of losing availability for writes when Patroni cannot ensure transaction durability. See :ref:`replication modes documentation <replication_modes>` for details.
-  **synchronous\_mode\_strict**: prevents disabling synchronous replication if no synchronous replicas are available, blocking all client writes to the master. See :ref:`replication modes documentation <replication_modes>` for details.
-  **postgresql**:
    -  **use\_pg\_rewind**: whether or not to use pg_rewind. Defaults to `false`.
    -  **use\_slots**: whether or not to use replication_slots. Defaults to `true` on PostgreSQL 9.4+.
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
-  **slots**: define permanent replication slots. These slots will be preserved during switchover/failover. Patroni will try to create slots before opening connections to the cluster.
    -  **my_slot_name**: the name of replication slot. It is the responsibility of the operator to make sure that there are no clashes in names between replication slots automatically created by Patroni for members and permanent replication slots.
        -  **type**: slot type. Could be ``physical`` or ``logical``. If the slot is logical, you have to additionally define ``database`` and ``plugin``.
        -  **database**: the database name where logical slots should be created.
        -  **plugin**: the plugin name for the logical slot.

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

-  **host**: the host:port for the Consul endpoint, in format: http(s)://host:port
-  **url**: url for the Consul endpoint
-  **port**: (optional) Consul port
-  **scheme**: (optional) **http** or **https**, defaults to **http**
-  **token**: (optional) ACL token
-  **verify**: (optional) whether to verify the SSL certificate for HTTPS requests
-  **cacert**: (optional) The ca certificate. If present it will enable validation.
-  **cert**: (optional) file with the client certificate
-  **key**: (optional) file with the client key. Can be empty if the key is part of **cert**.
-  **dc**: (optional) Datacenter to communicate with. By default the datacenter of the host is used.
-  **consistency**: (optional) Select consul consistency mode. Possible values are ``default``, ``consistent``, or ``stale`` (more details in `consul API reference <https://www.consul.io/api/features/consistency.html/>`__)
-  **checks**: (optional) list of Consul health checks used for the session. By default an empty list is used.
-  **register\_service**: (optional) whether or not to register a service with the name defined by the scope parameter and the tag master, replica or standby-leader depending on the node's role. Defaults to **false**
-  **service\_check\_interval**: (optional) how often to perform health check against registered url

Etcd
----
Most of the parameters are optional, but you have to specify one of the **host**, **hosts**, **url**, **proxy** or **srv**

-  **host**: the host:port for the etcd endpoint.
-  **hosts**: list of etcd endpoint in format host1:port1,host2:port2,etc... Could be a comma separated string or an actual yaml list.
-  **use\_proxies**: If this parameter is set to true, Patroni will consider **hosts** as a list of proxies and will not perform a topology discovery of etcd cluster.
-  **url**: url for the etcd
-  **proxy**: proxy url for the etcd. If you are connecting to the etcd using proxy, use this parameter instead of **url**
-  **srv**: Domain to search the SRV record(s) for cluster autodiscovery.
-  **protocol**: (optional) http or https, if not specified http is used. If the **url** or **proxy** is specified - will take protocol from them.
-  **username**: (optional) username for etcd authentication.
-  **password**: (optional) password for etcd authentication.
-  **cacert**: (optional) The ca certificate. If present it will enable validation.
-  **cert**: (optional) file with the client certificate.
-  **key**: (optional) file with the client key. Can be empty if the key is part of **cert**.

ZooKeeper
----------
-  **hosts**: list of ZooKeeper cluster members in format: ['host1:port1', 'host2:port2', 'etc...'].

Exhibitor
---------
-  **hosts**: initial list of Exhibitor (ZooKeeper) nodes in format: 'host1,host2,etc...'. This list updates automatically whenever the Exhibitor (ZooKeeper) cluster topology changes.
-  **poll\_interval**: how often the list of ZooKeeper and Exhibitor nodes should be updated from Exhibitor
-  **port**: Exhibitor port.

.. _kubernetes_settings:

Kubernetes
----------
-  **namespace**: (optional) Kubernetes namespace where Patroni pod is running. Default value is `default`.
-  **labels**: Labels in format ``{label1: value1, label2: value2}``. These labels will be used to find existing objects (Pods and either Endpoints or ConfigMaps) associated with the current cluster. Also Patroni will set them on every object (Endpoint or ConfigMap) it creates.
-  **scope\_label**: (optional) name of the label containing cluster name. Default value is `cluster-name`.
-  **role\_label**: (optional) name of the label containing role (master or replica). Patroni will set this label on the pod it runs in. Default value is ``role``.
-  **use\_endpoints**: (optional) if set to true, Patroni will use Endpoints instead of ConfigMaps to run leader elections and keep cluster state.
-  **pod\_ip**: (optional) IP address of the pod Patroni is running in. This value is required when `use_endpoints` is enabled and is used to populate the leader endpoint subsets when the pod's PostgreSQL is promoted.
-  **ports**: (optional) if the Service object has the name for the port, the same name must appear in the Endpoint object, otherwise service won't work. For example, if your service is defined as ``{Kind: Service, spec: {ports: [{name: postgresql, port: 5432, targetPort: 5432}]}}``, then you have to set ``kubernetes.ports: [{"name": "postgresql", "port": 5432}]`` and Patroni will use it for updating subsets of the leader Endpoint. This parameter is used only if `kubernetes.use_endpoints` is set.

.. _postgresql_settings:

PostgreSQL
----------
-  **authentication**:
    -  **superuser**:
        -  **username**: name for the superuser, set during initialization (initdb) and later used by Patroni to connect to the postgres.
        -  **password**: password for the superuser, set during initialization (initdb).
        -  **sslmode**: (optional) maps to the `sslmode <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLMODE>`__ connection parameter, which allows a client to specify the type of TLS negotiation mode with the server. For more information on how each mode works, please visit the `PostgreSQL documentation <https://www.postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-SSLMODE-STATEMENTS>`__. The default mode is ``prefer``.
        -  **sslkey**: (optional) maps to the `sslkey <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLKEY>`__ connection parameter, which specifies the location of the secret key used with the client's certificate.
        -  **sslcert**: (optional) maps to the `sslcert <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLCERT>`__ connection parameter, which specifies the location of the client certificate.
        -  **sslrootcert**: (optional) maps to the `sslrootcert <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLROOTCERT>`__ connection parameter, which specifies the location of a file containing one ore more certificate authorities (CA) certificates that the client will use to verify a server's certificate.
        -  **sslcrl**: (optional) maps to the `sslcrl <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLCRL>`__ connection parameter, which specifies the location of a file containing a certificate revocation list. A client will reject connecting to any server that has a certificate present in this list.
    -  **replication**:
        -  **username**: replication username; the user will be created during initialization. Replicas will use this user to access master via streaming replication
        -  **password**: replication password; the user will be created during initialization.
        -  **sslmode**: (optional) maps to the `sslmode <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLMODE>`__ connection parameter, which allows a client to specify the type of TLS negotiation mode with the server. For more information on how each mode works, please visit the `PostgreSQL documentation <https://www.postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-SSLMODE-STATEMENTS>`__. The default mode is ``prefer``.
        -  **sslkey**: (optional) maps to the `sslkey <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLKEY>`__ connection parameter, which specifies the location of the secret key used with the client's certificate.
        -  **sslcert**: (optional) maps to the `sslcert <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLCERT>`__ connection parameter, which specifies the location of the client certificate.
        -  **sslrootcert**: (optional) maps to the `sslrootcert <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLROOTCERT>`__ connection parameter, which specifies the location of a file containing one ore more certificate authorities (CA) certificates that the client will use to verify a server's certificate.
        -  **sslcrl**: (optional) maps to the `sslcrl <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLCRL>`__ connection parameter, which specifies the location of a file containing a certificate revocation list. A client will reject connecting to any server that has a certificate present in this list.
    -  **rewind**:
        -  **username**: name for the user for ``pg_rewind``; the user will be created during initialization of postgres 11+ and all necessary `permissions <https://www.postgresql.org/docs/11/app-pgrewind.html#id-1.9.5.8.8>`__ will be granted.
        -  **password**: password for the user for ``pg_rewind``; the user will be created during initialization.
        -  **sslmode**: (optional) maps to the `sslmode <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLMODE>`__ connection parameter, which allows a client to specify the type of TLS negotiation mode with the server. For more information on how each mode works, please visit the `PostgreSQL documentation <https://www.postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-SSLMODE-STATEMENTS>`__. The default mode is ``prefer``.
        -  **sslkey**: (optional) maps to the `sslkey <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLKEY>`__ connection parameter, which specifies the location of the secret key used with the client's certificate.
        -  **sslcert**: (optional) maps to the `sslcert <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLCERT>`__ connection parameter, which specifies the location of the client certificate.
        -  **sslrootcert**: (optional) maps to the `sslrootcert <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLROOTCERT>`__ connection parameter, which specifies the location of a file containing one ore more certificate authorities (CA) certificates that the client will use to verify a server's certificate.
        -  **sslcrl**: (optional) maps to the `sslcrl <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLCRL>`__ connection parameter, which specifies the location of a file containing a certificate revocation list. A client will reject connecting to any server that has a certificate present in this list.
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

REST API
--------
-  **connect\_address**: IP address (or hostname) and port, to access the Patroni's :ref:`REST API <rest_api>`. All the members of the cluster must be able to connect to this address, so unless the Patroni setup is intended for a demo inside the localhost, this address must be a non "localhost" or loopback address (ie: "localhost" or "127.0.0.1"). It can serve as an endpoint for HTTP health checks (read below about the "listen" REST API parameter), and also for user queries (either directly or via the REST API), as well as for the health checks done by the cluster members during leader elections (for example, to determine whether the master is still running, or if there is a node which has a WAL position that is ahead of the one doing the query; etc.) The connect_address is put in the member key in DCS, making it possible to translate the member name into the address to connect to its REST API.

-  **listen**: IP address (or hostname) and port that Patroni will listen to for the REST API - to provide also the same health checks and cluster messaging between the participating nodes, as described above. to provide health-check information for HAProxy (or any other load balancer capable of doing a HTTP "OPTION" or "GET" checks).

-  **Optional**:
        -  **authentication**:
            -  **username**: Basic-auth username to protect unsafe REST API endpoints.
            -  **password**: Basic-auth password to protect unsafe REST API endpoints.

        -  **certfile**: Specifies the file with the certificate in the PEM format. If the certfile is not specified or is left empty, the API server will work without SSL.
        -  **keyfile**: Specifies the file with the secret key in the PEM format.
        -  **cafile**: Specifies the file with the CA_BUNDLE with certificates of trusted CAs to use while verifying client certs.
        -  **verify\_client**: ``none``, ``optional`` or ``required``. When ``none`` REST API will not check client certificates. When ``required`` client certificates are required for all REST API calls. When ``optional`` client certificates are required for all unsafe REST API endpoints. If ``verify_client`` is set to ``optional`` or ``required`` basic-auth is not checked.

.. _patronictl_settings:

CTL
---
-  **Optional**:
    -  **insecure**: Allow connections to REST API without verifying SSL certs.
    -  **cacert**: Specifies the file with the CA_BUNDLE file or directory with certificates of trusted CAs to use while verifying REST API SSL certs. If not provided patronictl will use the value provided for REST API "cafile" parameter.
    -  **certfile**: Specifies the file with the client certificate in the PEM format. If not provided patronictl will use the value provided for REST API "certfile" parameter.
    -  **keyfile**: Specifies the file with the client secret key in the PEM format. If not provided patronictl will use the value provided for REST API "keyfile" parameter.

Watchdog
--------
- **mode**: ``off``, ``automatic`` or ``required``. When ``off`` watchdog is disabled. When ``automatic`` watchdog will be used if available, but ignored if it is not. When ``required`` the node will not become a leader unless watchdog can be successfully enabled.
- **device**: Path to watchdog device. Defaults to ``/dev/watchdog``.
- **safety_margin**: Number of seconds of safety margin between watchdog triggering and leader key expiration.

Tags
----
- **nofailover**: ``true`` or ``false``, controls whether this node is allowed to participate in the leader race and become a leader. Defaults to ``false``
- **clonefrom**: ``true`` or ``false``. If set to ``true`` other nodes might prefer to use this node for bootstrap (take ``pg_basebackup`` from). If there are several nodes with ``clonefrom`` tag set to ``true`` the node to bootstrap from will be chosen randomly. The default value is ``false``.
- **noloadbalance**: ``true`` or ``false``. If set to ``true`` the node will return HTTP Status Code 503 for the ``GET /replica`` REST API health-check and therefore will be excluded from the load-balancing. Defaults to ``false``.
- **replicatefrom**: The IP address/hostname of another replica. Used to support cascading replication.
- **nosync**: ``true`` or ``false``. If set to ``true`` the node will never be selected as a synchronous replica.
