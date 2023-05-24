.. _yaml_configuration:

============================
YAML Configuration Settings
============================


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

   -  **dcs**: This section will be written into `/<namespace>/<scope>/config` of the given configuration store after initializing of new cluster. The global dynamic configuration for the cluster. Under the ``bootstrap.dcs`` you can put any of the parameters described in the :ref:`Dynamic Configuration settings <dynamic_configuration>` and after Patroni initialized (bootstrapped) the new cluster, it will write this section into `/<namespace>/<scope>/config` of the configuration store. All later changes of ``bootstrap.dcs`` will not take any effect! If you want to change them please use either ``patronictl edit-config`` or Patroni :ref:`REST API <rest_api>`.
   -  **method**: custom script to use for bootstrapping this cluster.

      See :ref:`custom bootstrap methods documentation <custom_bootstrap>` for details.
      When ``initdb`` is specified revert to the default ``initdb`` command. ``initdb`` is also triggered when no ``method``
      parameter is present in the configuration file.
   -  **initdb**: (optional) list options to be passed on to initdb.

      -  **- data-checksums**: Must be enabled when pg_rewind is needed on 9.3.
      -  **- encoding: UTF8**: default encoding for new databases.
      -  **- locale: UTF8**: default locale for new databases.
   -  **users**: Some additional users which need to be created after initializing new cluster

      -  **admin**: the name of user

         -  **password**: (optional) password for the user
         -  **options**: list of options for CREATE USER statement

            -  **- createrole**
            -  **- createdb**
   -  **post\_bootstrap** or **post\_init**: An additional script that will be executed after initializing the cluster. The script receives a connection string URL (with the cluster superuser as a user name). The PGPASSFILE variable is set to the location of pgpass file.

.. _citus_settings:

Citus
-----
Enables integration Patroni with `Citus <https://docs.citusdata.com>`__. If configured, Patroni will take care of registering Citus worker nodes on the coordinator. You can find more information about Citus support :ref:`here <citus>`.

-  **group**: the Citus group id, integer. Use ``0`` for coordinator and ``1``, ``2``, etc... for workers
-  **database**: the database where ``citus`` extension should be created. Must be the same on the coordinator and all workers. Currently only one database is supported.

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
-  **register\_service**: (optional) whether or not to register a service with the name defined by the scope parameter and the tag master, primary, replica, or standby-leader depending on the node's role. Defaults to **false**.
-  **service\_tags**: (optional) additional static tags to add to the Consul service apart from the role (``master``/``primary``/``replica``/``standby-leader``). By default an empty list is used.
-  **service\_check\_interval**: (optional) how often to perform health check against registered url. Defaults to '5s'.
-  **service\_check\_tls\_server\_name**: (optional) overide SNI host when connecting via TLS, see also `consul agent check API reference <https://www.consul.io/api-docs/agent/check#tlsservername>`__.

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
-  **srv**: Domain to search the SRV record(s) for cluster autodiscovery. Patroni will try to query these SRV service names for specified domain (in that order until first success): ``_etcd-client-ssl``, ``_etcd-client``, ``_etcd-ssl``, ``_etcd``, ``_etcd-server-ssl``, ``_etcd-server``. If SRV records for ``_etcd-server-ssl`` or ``_etcd-server`` are retrieved then ETCD peer protocol is used do query ETCD for available members. Otherwise hosts from SRV records will be used.
-  **srv\_suffix**: Configures a suffix to the SRV name that is queried during discovery. Use this flag to differentiate between multiple etcd clusters under the same domain. Works only with conjunction with **srv**. For example, if ``srv_suffix: foo`` and ``srv: example.org`` are set, the following DNS SRV query is made:``_etcd-client-ssl-foo._tcp.example.com`` (and so on for every possible ETCD SRV service name).
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
-  **set_acls**: (optional) If set, configure Kazoo to apply a default ACL to each ZNode that it creates. ACLs will assume 'x509' schema and should be specified as a dictionary with the principal as the key and one or more permissions as a list in the value.  Permissions may be one of ``CREATE``, ``READ``, ``WRITE``, ``DELETE`` or ``ADMIN``.  For example, ``set_acls: {CN=principal1: [CREATE, READ], CN=principal2: [ALL]}``.

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
-  **retriable\_http\_codes**: (optional) list of HTTP status codes from K8s API to retry on. By default Patroni is retrying on ``500``, ``503``, and ``504``, or if K8s API response has ``retry-after`` HTTP header.


.. _raft_settings:

Raft (deprecated)
-----------------
-  **self\_addr**: ``ip:port`` to listen on for Raft connections. The ``self_addr`` must be accessible from other nodes of the cluster. If not set, the node will not participate in consensus.
-  **bind\_addr**: (optional) ``ip:port`` to listen on for Raft connections. If not specified the ``self_addr`` will be used.
-  **partner\_addrs**: list of other Patroni nodes in the cluster in format: ['ip1:port', 'ip2:port', 'etc...']
-  **data\_dir**: directory where to store Raft log and snapshot. If not specified the current working directory is used.
-  **password**: (optional) Encrypt Raft traffic with a specified password, requires ``cryptography`` python module.

   Short FAQ about Raft implementation

   - Q: How to list all the nodes providing consensus?

     A: ``syncobj_admin -conn host:port -status`` where the host:port is the address of one of the cluster nodes

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
         -  **sslcrldir**: (optional) maps to the `sslcrldir <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLCRLDIR>`__ connection parameter, which specifies the location of a directory with files containing a certificate revocation list. A client will reject connecting to any server that has a certificate present in this list.
         -  **gssencmode**: (optional) maps to the `gssencmode <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-GSSENCMODE>`__ connection parameter, which determines whether or with what priority a secure GSS TCP/IP connection will be negotiated with the server
         -  **channel_binding**: (optional) maps to the `channel_binding <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-CHANNEL-BINDING>`__ connection parameter, which controls the client's use of channel binding.
      -  **replication**:

         -  **username**: replication username; the user will be created during initialization. Replicas will use this user to access the replication source via streaming replication
         -  **password**: replication password; the user will be created during initialization.
         -  **sslmode**: (optional) maps to the `sslmode <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLMODE>`__ connection parameter, which allows a client to specify the type of TLS negotiation mode with the server. For more information on how each mode works, please visit the `PostgreSQL documentation <https://www.postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-SSLMODE-STATEMENTS>`__. The default mode is ``prefer``.
         -  **sslkey**: (optional) maps to the `sslkey <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLKEY>`__ connection parameter, which specifies the location of the secret key used with the client's certificate.
         -  **sslpassword**: (optional) maps to the `sslpassword <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLPASSWORD>`__ connection parameter, which specifies the password for the secret key specified in ``sslkey``.
         -  **sslcert**: (optional) maps to the `sslcert <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLCERT>`__ connection parameter, which specifies the location of the client certificate.
         -  **sslrootcert**: (optional) maps to the `sslrootcert <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLROOTCERT>`__ connection parameter, which specifies the location of a file containing one ore more certificate authorities (CA) certificates that the client will use to verify a server's certificate.
         -  **sslcrl**: (optional) maps to the `sslcrl <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLCRL>`__ connection parameter, which specifies the location of a file containing a certificate revocation list. A client will reject connecting to any server that has a certificate present in this list.
         -  **sslcrldir**: (optional) maps to the `sslcrldir <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLCRLDIR>`__ connection parameter, which specifies the location of a directory with files containing a certificate revocation list. A client will reject connecting to any server that has a certificate present in this list.
         -  **gssencmode**: (optional) maps to the `gssencmode <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-GSSENCMODE>`__ connection parameter, which determines whether or with what priority a secure GSS TCP/IP connection will be negotiated with the server
         -  **channel_binding**: (optional) maps to the `channel_binding <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-CHANNEL-BINDING>`__ connection parameter, which controls the client's use of channel binding.
      -  **rewind**:

         -  **username**: (optional) name for the user for ``pg_rewind``; the user will be created during initialization of postgres 11+ and all necessary `permissions <https://www.postgresql.org/docs/11/app-pgrewind.html#id-1.9.5.8.8>`__ will be granted.
         -  **password**: (optional) password for the user for ``pg_rewind``; the user will be created during initialization.
         -  **sslmode**: (optional) maps to the `sslmode <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLMODE>`__ connection parameter, which allows a client to specify the type of TLS negotiation mode with the server. For more information on how each mode works, please visit the `PostgreSQL documentation <https://www.postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-SSLMODE-STATEMENTS>`__. The default mode is ``prefer``.
         -  **sslkey**: (optional) maps to the `sslkey <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLKEY>`__ connection parameter, which specifies the location of the secret key used with the client's certificate.
         -  **sslpassword**: (optional) maps to the `sslpassword <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLPASSWORD>`__ connection parameter, which specifies the password for the secret key specified in ``sslkey``.
         -  **sslcert**: (optional) maps to the `sslcert <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLCERT>`__ connection parameter, which specifies the location of the client certificate.
         -  **sslrootcert**: (optional) maps to the `sslrootcert <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLROOTCERT>`__ connection parameter, which specifies the location of a file containing one ore more certificate authorities (CA) certificates that the client will use to verify a server's certificate.
         -  **sslcrl**: (optional) maps to the `sslcrl <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLCRL>`__ connection parameter, which specifies the location of a file containing a certificate revocation list. A client will reject connecting to any server that has a certificate present in this list.
         -  **sslcrldir**: (optional) maps to the `sslcrldir <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLCRLDIR>`__ connection parameter, which specifies the location of a directory with files containing a certificate revocation list. A client will reject connecting to any server that has a certificate present in this list.
         -  **gssencmode**: (optional) maps to the `gssencmode <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-GSSENCMODE>`__ connection parameter, which determines whether or with what priority a secure GSS TCP/IP connection will be negotiated with the server
         -  **channel_binding**: (optional) maps to the `channel_binding <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-CHANNEL-BINDING>`__ connection parameter, which controls the client's use of channel binding.

   -  **callbacks**: callback scripts to run on certain actions. Patroni will pass the action, role and cluster name. (See scripts/aws.py as an example of how to write them.)

      -  **on\_reload**: run this script when configuration reload is triggered.
      -  **on\_restart**: run this script when the postgres restarts (without changing role).
      -  **on\_role\_change**: run this script when the postgres is being promoted or demoted.
      -  **on\_start**: run this script when the postgres starts.
      -  **on\_stop**: run this script when the postgres stops.
   -  **connect\_address**: IP address + port through which Postgres is accessible from other nodes and applications.
   -  **proxy\_address**: IP address + port through which a connection pool (e.g. pgbouncer) running next to Postgres is accessible. The value is written to the member key in DCS as ``proxy_url`` and could be used/useful for service discovery.
   -  **create\_replica\_methods**: an ordered list of the create methods for turning a Patroni node into a new replica.
      "basebackup" is the default method; other methods are assumed to refer to scripts, each of which is configured as its
      own config item. See :ref:`custom replica creation methods documentation <custom_replica_creation>` for further explanation.
   -  **data\_dir**: The location of the Postgres data directory, either :ref:`existing <existing_data>` or to be initialized by Patroni.
   -  **config\_dir**: The location of the Postgres configuration directory, defaults to the data directory. Must be writable by Patroni.
   -  **bin\_dir**: (optional) Path to PostgreSQL binaries (pg_ctl, initdb, pg_controldata, pg_basebackup, postgres, pg_isready, pg_rewind). If not provided or is an empty string, PATH environment variable will be used to find the executables.
   -  **bin\_name**: (optional) Make it possible to override Postgres binary names, if you are using a custom Postgres distribution:

      - **pg\_ctl**: (optional) Custom name for ``pg_ctl`` binary.
      - **initdb**: (optional) Custom name for ``initdb`` binary.
      - **pg\controldata**: (optional) Custom name for ``pg_controldata`` binary.
      - **pg\_basebackup**: (optional) Custom name for ``pg_basebackup`` binary.
      - **postgres**: (optional) Custom name for ``postgres`` binary.
      - **pg\_isready**: (optional) Custom name for ``pg_isready`` binary.
      - **pg\_rewind**: (optional) Custom name for ``pg_rewind`` binary.
   -  **listen**: IP address + port that Postgres listens to; must be accessible from other nodes in the cluster, if you're using streaming replication. Multiple comma-separated addresses are permitted, as long as the port component is appended after to the last one with a colon, i.e. ``listen: 127.0.0.1,127.0.0.2:5432``. Patroni will use the first address from this list to establish local connections to the PostgreSQL node.
   -  **use\_unix\_socket**: specifies that Patroni should prefer to use unix sockets to connect to the cluster. Default value is ``false``. If ``unix_socket_directories`` is defined, Patroni will use the first suitable value from it to connect to the cluster and fallback to tcp if nothing is suitable. If ``unix_socket_directories`` is not specified in ``postgresql.parameters``, Patroni will assume that the default value should be used and omit ``host`` from the connection parameters.
   -  **use\_unix\_socket\_repl**: specifies that Patroni should prefer to use unix sockets for replication user cluster connection. Default value is ``false``. If ``unix_socket_directories`` is defined, Patroni will use the first suitable value from it to connect to the cluster and fallback to tcp if nothing is suitable. If ``unix_socket_directories`` is not specified in ``postgresql.parameters``, Patroni will assume that the default value should be used and omit ``host`` from the connection parameters.
   -  **pgpass**: path to the `.pgpass <https://www.postgresql.org/docs/current/static/libpq-pgpass.html>`__ password file. Patroni creates this file before executing pg\_basebackup, the post_init script and under some other circumstances. The location must be writable by Patroni.
   -  **recovery\_conf**: additional configuration settings written to recovery.conf when configuring follower.
   -  **custom\_conf** : path to an optional custom ``postgresql.conf`` file, that will be used in place of ``postgresql.base.conf``. The file must exist on all cluster nodes, be readable by PostgreSQL and will be included from its location on the real ``postgresql.conf``. Note that Patroni will not monitor this file for changes, nor backup it. However, its settings can still be overridden by Patroni's own configuration facilities - see :ref:`dynamic configuration <patroni_configuration>` for details.
   -  **parameters**: list of configuration settings for Postgres. Many of these are required for replication to work.
   -  **pg\_hba**: list of lines that Patroni will use to generate ``pg_hba.conf``. Patroni ignores this parameter if ``hba_file`` PostgreSQL parameter is set to a non-default value. Together with :ref:`dynamic configuration <dynamic_configuration>` this parameter simplifies management of ``pg_hba.conf``.

      -  **- host all all 0.0.0.0/0 md5**
      -  **- host replication replicator 127.0.0.1/32 md5**: A line like this is required for replication.
   -  **pg\_ident**: list of lines that Patroni will use to generate ``pg_ident.conf``. Patroni ignores this parameter if ``ident_file`` PostgreSQL parameter is set to a non-default value. Together with :ref:`dynamic configuration <dynamic_configuration>` this parameter simplifies management of ``pg_ident.conf``.

      -  **- mapname1 systemname1 pguser1**
      -  **- mapname1 systemname2 pguser2**
   -  **pg\_ctl\_timeout**: How long should pg_ctl wait when doing ``start``, ``stop`` or ``restart``. Default value is 60 seconds.
   -  **use\_pg\_rewind**: try to use pg\_rewind on the former leader when it joins cluster as a replica.
   -  **remove\_data\_directory\_on\_rewind\_failure**: If this option is enabled, Patroni will remove the PostgreSQL data directory and recreate the replica. Otherwise it will try to follow the new leader. Default value is **false**.
   -  **remove\_data\_directory\_on\_diverged\_timelines**: Patroni will remove the PostgreSQL data directory and recreate the replica if it notices that timelines are diverging and the former primary can not start streaming from the new primary. This option is useful when ``pg_rewind`` can not be used. While performing timelines divergence check on PostgreSQL v10 and older Patroni will try to connect with replication credential to the "postgres" database. Hence, such access should be allowed in the pg_hba.conf. Default value is **false**.
   -  **replica\_method**: for each create_replica_methods other than basebackup, you would add a configuration section of the same name. At a minimum, this should include "command" with a full path to the actual script to be executed. Other configuration parameters will be passed along to the script in the form "parameter=value".
   -  **pre\_promote**: a fencing script that executes during a failover after acquiring the leader lock but before promoting the replica. If the script exits with a non-zero code, Patroni does not promote the replica and removes the leader key from DCS.
   -  **before\_stop**: a script that executes immediately prior to stopping postgres. As opposed to a callback, this script runs synchronously, blocking shutdown until it has completed. The return code of this script does not impact whether shutdown proceeds afterwards.

.. _restapi_settings:

REST API
--------
-  **restapi**:

   -  **connect\_address**: IP address (or hostname) and port, to access the Patroni's :ref:`REST API <rest_api>`. All the members of the cluster must be able to connect to this address, so unless the Patroni setup is intended for a demo inside the localhost, this address must be a non "localhost" or loopback address (ie: "localhost" or "127.0.0.1"). It can serve as an endpoint for HTTP health checks (read below about the "listen" REST API parameter), and also for user queries (either directly or via the REST API), as well as for the health checks done by the cluster members during leader elections (for example, to determine whether the leader is still running, or if there is a node which has a WAL position that is ahead of the one doing the query; etc.) The connect_address is put in the member key in DCS, making it possible to translate the member name into the address to connect to its REST API.
   -  **listen**: IP address (or hostname) and port that Patroni will listen to for the REST API - to provide also the same health checks and cluster messaging between the participating nodes, as described above. to provide health-check information for HAProxy (or any other load balancer capable of doing a HTTP "OPTION" or "GET" checks).
   -  **authentication**: (optional)

      -  **username**: Basic-auth username to protect unsafe REST API endpoints.
      -  **password**: Basic-auth password to protect unsafe REST API endpoints.
   -  **certfile**: (optional): Specifies the file with the certificate in the PEM format. If the certfile is not specified or is left empty, the API server will work without SSL.
   -  **keyfile**: (optional): Specifies the file with the secret key in the PEM format.
   -  **keyfile\_password**: (optional): Specifies a password for decrypting the keyfile.
   -  **cafile**: (optional): Specifies the file with the CA_BUNDLE with certificates of trusted CAs to use while verifying client certs.
   -  **ciphers**: (optional): Specifies the permitted cipher suites (e.g. "ECDHE-RSA-AES256-GCM-SHA384:DHE-RSA-AES256-GCM-SHA384:ECDHE-RSA-AES128-GCM-SHA256:DHE-RSA-AES128-GCM-SHA256:!SSLv1:!SSLv2:!SSLv3:!TLSv1:!TLSv1.1")
   -  **verify\_client**: (optional): ``none`` (default), ``optional`` or ``required``. When ``none`` REST API will not check client certificates. When ``required`` client certificates are required for all REST API calls. When ``optional`` client certificates are required for all unsafe REST API endpoints. When ``required`` is used, then client authentication succeeds, if the certificate signature verification succeeds.  For ``optional`` the client cert will only be checked for ``PUT``, ``POST``, ``PATCH``, and ``DELETE`` requests.
   -  **allowlist**: (optional): Specifies the set of hosts that are allowed to call unsafe REST API endpoints. The single element could be a host name, an IP address or a network address using CIDR notation. By default ``allow all`` is used. In case if ``allowlist`` or ``allowlist_include_members`` are set, anything that is not included is rejected.
   -  **allowlist\_include\_members**: (optional): If set to ``true`` it allows accessing unsafe REST API endpoints from other cluster members registered in DCS (IP address or hostname is taken from the members ``api_url``). Be careful, it might happen that OS will use a different IP for outgoing connections.
   -  **http\_extra\_headers**: (optional): HTTP headers let the REST API server pass additional information with an HTTP response.
   -  **https\_extra\_headers**: (optional): HTTPS headers let the REST API server pass additional information with an HTTP response when TLS is enabled. This will also pass additional information set in ``http_extra_headers``.
   -  **request_queue_size**: (optional): Sets request queue size for TCP socket used by Patroni REST API.  Once the queue is full, further requests get a "Connection denied" error. The default value is 5.

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
-  **ctl**: (optional)

   -  **insecure**: Allow connections to REST API without verifying SSL certs.
   -  **cacert**: Specifies the file with the CA_BUNDLE file or directory with certificates of trusted CAs to use while verifying REST API SSL certs. If not provided patronictl will use the value provided for REST API "cafile" parameter.
   -  **certfile**: Specifies the file with the client certificate in the PEM format. If not provided patronictl will use the value provided for REST API "certfile" parameter.
   -  **keyfile**: Specifies the file with the client secret key in the PEM format. If not provided patronictl will use the value provided for REST API "keyfile" parameter.
   -  **keyfile\_password**: Specifies a password for decrypting the keyfile. If not provided patronictl will use the value provided for REST API "keyfile\_password" parameter.

Watchdog
--------
-  **mode**: ``off``, ``automatic`` or ``required``. When ``off`` watchdog is disabled. When ``automatic`` watchdog will be used if available, but ignored if it is not. When ``required`` the node will not become a leader unless watchdog can be successfully enabled.
-  **device**: Path to watchdog device. Defaults to ``/dev/watchdog``.
-  **safety_margin**: Number of seconds of safety margin between watchdog triggering and leader key expiration.

.. _tags_settings:

Tags
----
-  **nofailover**: ``true`` or ``false``, controls whether this node is allowed to participate in the leader race and become a leader. Defaults to ``false``
-  **clonefrom**: ``true`` or ``false``. If set to ``true`` other nodes might prefer to use this node for bootstrap (take ``pg_basebackup`` from). If there are several nodes with ``clonefrom`` tag set to ``true`` the node to bootstrap from will be chosen randomly. The default value is ``false``.
-  **noloadbalance**: ``true`` or ``false``. If set to ``true`` the node will return HTTP Status Code 503 for the ``GET /replica`` REST API health-check and therefore will be excluded from the load-balancing. Defaults to ``false``.
-  **replicatefrom**: The IP address/hostname of another replica. Used to support cascading replication.
-  **nosync**: ``true`` or ``false``. If set to ``true`` the node will never be selected as a synchronous replica.

In addition to these predefined tags, you can also add your own ones:

-  **key1**: ``true``
-  **key2**: ``false``
-  **key3**: ``1.4``
-  **key4**: ``"RandomString"``

Tags are visible in the :ref:`REST API <rest_api>` and ``patronictl list`` You can also check for an instance health using these tags. If the tag isn't defined for an instance, or if the respective value doesn't match the querying value, it will return HTTP Status Code 503.
