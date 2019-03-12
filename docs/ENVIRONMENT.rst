.. _environment:

Environment Configuration Settings
==================================

It is possible to override some of the configuration parameters defined in the Patroni configuration file using the system environment variables. This document lists all environment variables handled by Patroni. The values set via those variables always take precedence over the ones set in the Patroni configuration file.

Global/Universal
----------------
-  **PATRONI\_CONFIGURATION**: it is possible to set the entire configuration for the Patroni via ``PATRONI_CONFIGURATION`` environment variable. In this case any other environment variables will not be considered!
-  **PATRONI\_NAME**: name of the node where the current instance of Patroni is running. Must be unique for the cluster.
-  **PATRONI\_NAMESPACE**: path within the configuration store where Patroni will keep information about the cluster. Default value: "/service"
-  **PATRONI\_SCOPE**: cluster name
-  **PATRONI\_LOG\_LEVEL**: sets the general logging level. Default value is **INFO** (see `the docs for Python logging <https://docs.python.org/3.6/library/logging.html#levels>`_)
-  **PATRONI\_LOG\_FORMAT**: sets the log formatting string. Default value is **%(asctime)s %(levelname)s: %(message)s** (see `the LogRecord attributes <https://docs.python.org/3.6/library/logging.html#logrecord-attributes>`_)
-  **PATRONI\_LOG\_DATEFORMAT**: sets the datetime formatting string. (see the `formatTime() documentation <https://docs.python.org/3.6/library/logging.html#logging.Formatter.formatTime>`_)
-  **PATRONI\_LOG\_DIR**: Directory to write application logs to. The directory must exist and be writable by the user executing Patroni. If you set this env variable, the application will retain 4 25MB logs by default. You can tune those retention values with `PATRONI_LOG_FILE_NUM` and `PATRONI_LOG_FILE_SIZE` (see below).
-  **PATRONI\_LOG\_FILE\_NUM**: The number of application logs to retain.
-  **PATRONI\_LOG\_FILE\_SIZE**: Size of patroni.log file (in bytes) that triggers a log rolling.
-  **PATRONI\_LOG\_LOGGERS**: Redefine logging level per python module. Example ``PATRONI_LOG_LOGGERS="{patroni.postmaster: WARNING, urllib3: DEBUG}"``

Bootstrap configuration
-----------------------
It is possible to create new database users right after the successful initialization of a new cluster. This process is defined by the following variables:

-  **PATRONI\_<username>\_PASSWORD='<password>'**
-  **PATRONI\_<username>\_OPTIONS='list,of,options'**

Example: defining ``PATRONI_admin_PASSWORD=strongpasswd`` and ``PATRONI_admin_OPTIONS='createrole,createdb'`` will cause creation of the user **admin** with the password **strongpasswd** that is allowed to create other users and databases.

Consul
------
-  **PATRONI\_CONSUL\_HOST**: the host:port for the Consul endpoint.
-  **PATRONI\_CONSUL\_URL**: url for the Consul, in format: http(s)://host:port
-  **PATRONI\_CONSUL\_PORT**: (optional) Consul port
-  **PATRONI\_CONSUL\_SCHEME**: (optional) **http** or **https**, defaults to **http**
-  **PATRONI\_CONSUL\_TOKEN**: (optional) ACL token
-  **PATRONI\_CONSUL\_VERIFY**: (optional) whether to verify the SSL certificate for HTTPS requests
-  **PATRONI\_CONSUL\_CACERT**: (optional) The ca certificate. If present it will enable validation.
-  **PATRONI\_CONSUL\_CERT**: (optional) File with the client certificate
-  **PATRONI\_CONSUL\_KEY**: (optional) File with the client key. Can be empty if the key is part of certificate.
-  **PATRONI\_CONSUL\_DC**: (optional) Datacenter to communicate with. By default the datacenter of the host is used.
-  **PATRONI\_CONSUL\_CHECKS**: (optional) list of Consul health checks used for the session. If not specified Consul will use "serfHealth" in additional to the TTL based check created by Patroni. Additional checks, in particular the "serfHealth", may cause the leader lock to expire faster than in `ttl` seconds when the leader instance becomes unavailable.
-  **PATRONI\_CONSUL\_REGISTER\_SERVICE**: (optional) whether or not to register a service with the name defined by the scope parameter and the tag master, replica or standby-leader depending on the node's role. Defaults to **false**
-  **PATRONI\_CONSUL\_SERVICE\_CHECK\_INTERVAL**: (optional) how often to perform health check against registered url

Etcd
----

-  **PATRONI\_ETCD\_PROXY**: proxy url for the etcd. If you are connecting to the etcd using proxy, use this parameter instead of **PATRONI\_ETCD\_URL**
-  **PATRONI\_ETCD\_URL**: url for the etcd, in format: http(s)://(username:password@)host:port
-  **PATRONI\_ETCD\_HOSTS**: list of etcd endpoints in format 'host1:port1','host2:port2',etc...
-  **PATRONI\_ETCD\_USE\_PROXIES**: If this parameter is set to true, Patroni will consider **hosts** as a list of proxies and will not perform a topology discovery of etcd cluster but stick to a fixed list of **hosts**.
-  **PATRONI\_ETCD\_PROTOCOL**: http or https, if not specified http is used. If the **url** or **proxy** is specified - will take protocol from them.
-  **PATRONI\_ETCD\_HOST**: the host:port for the etcd endpoint.
-  **PATRONI\_ETCD\_SRV**: Domain to search the SRV record(s) for cluster autodiscovery.
-  **PATRONI\_ETCD\_USERNAME**: username for etcd authentication.
-  **PATRONI\_ETCD\_PASSWORD**: password for etcd authentication.
-  **PATRONI\_ETCD\_CACERT**: The ca certificate. If present it will enable validation.
-  **PATRONI\_ETCD\_CERT**: File with the client certificate.
-  **PATRONI\_ETCD\_KEY**: File with the client key. Can be empty if the key is part of certificate.

Exhibitor
---------
-  **PATRONI\_EXHIBITOR\_HOSTS**: initial list of Exhibitor (ZooKeeper) nodes in format: 'host1,host2,etc...'. This list updates automatically whenever the Exhibitor (ZooKeeper) cluster topology changes.
-  **PATRONI\_EXHIBITOR\_PORT**: Exhibitor port.

.. _kubernetes_environment:

Kubernetes
----------
-  **PATRONI\_KUBERNETES\_NAMESPACE**: (optional) Kubernetes namespace where the Patroni pod is running. Default value is `default`.
-  **PATRONI\_KUBERNETES\_LABELS**: Labels in format ``{label1: value1, label2: value2}``. These labels will be used to find existing objects (Pods and either Endpoints or ConfigMaps) associated with the current cluster. Also Patroni will set them on every object (Endpoint or ConfigMap) it creates.
-  **PATRONI\_KUBERNETES\_SCOPE\_LABEL**: (optional) name of the label containing cluster name. Default value is `cluster-name`.
-  **PATRONI\_KUBERNETES\_ROLE\_LABEL**: (optional) name of the label containing Postgres role (`master` or `replica`). Patroni will set this label on the pod it is running in. Default value is `role`.
-  **PATRONI\_KUBERNETES\_USE\_ENDPOINTS**: (optional) if set to true, Patroni will use Endpoints instead of ConfigMaps to run leader elections and keep cluster state.
-  **PATRONI\_KUBERNETES\_POD\_IP**: (optional) IP address of the pod Patroni is running in. This value is required when `PATRONI_KUBERNETES_USE_ENDPOINTS` is enabled and is used to populate the leader endpoint subsets when the pod's PostgreSQL is promoted.
-  **PATRONI\_KUBERNETES\_PORTS**: (optional) if the Service object has the name for the port, the same name must appear in the Endpoint object, otherwise service won't work. For example, if your service is defined as ``{Kind: Service, spec: {ports: [{name: postgresql, port: 5432, targetPort: 5432}]}}``, then you have to set ``PATRONI_KUBERNETES_PORTS='{[{"name": "postgresql", "port": 5432}]}'`` and Patroni will use it for updating subsets of the leader Endpoint. This parameter is used only if `PATRONI_KUBERNETES_USE_ENDPOINTS` is set.

PostgreSQL
----------
-  **PATRONI\_POSTGRESQL\_LISTEN**: IP address + port that Postgres listens to. Multiple comma-separated addresses are permitted, as long as the port component is appended after to the last one with a colon, i.e. ``listen: 127.0.0.1,127.0.0.2:5432``. Patroni will use the first address from this list to establish local connections to the PostgreSQL node.
-  **PATRONI\_POSTGRESQL\_CONNECT\_ADDRESS**: IP address + port through which Postgres is accessible from other nodes and applications.
-  **PATRONI\_POSTGRESQL\_DATA\_DIR**: The location of the Postgres data directory, either existing or to be initialized by Patroni.
-  **PATRONI\_POSTGRESQL\_CONFIG\_DIR**: The location of the Postgres configuration directory, defaults to the data directory. Must be writable by Patroni.
-  **PATRONI\_POSTGRESQL\_BIN_DIR**: Path to PostgreSQL binaries. (pg_ctl, pg_rewind, pg_basebackup, postgres) The  default value is an empty string meaning that PATH environment variable will be used to find the executables.
-  **PATRONI\_POSTGRESQL\_PGPASS**: path to the `.pgpass <https://www.postgresql.org/docs/current/static/libpq-pgpass.html>`__ password file. Patroni creates this file before executing pg\_basebackup and under some other circumstances. The location must be writable by Patroni.
-  **PATRONI\_REPLICATION\_USERNAME**: replication username; the user will be created during initialization. Replicas will use this user to access master via streaming replication
-  **PATRONI\_REPLICATION\_PASSWORD**: replication password; the user will be created during initialization.
-  **PATRONI\_SUPERUSER\_USERNAME**: name for the superuser, set during initialization (initdb) and later used by Patroni to connect to the postgres. Also this user is used by pg_rewind.
-  **PATRONI\_SUPERUSER\_PASSWORD**: password for the superuser, set during initialization (initdb).

REST API
--------
-  **PATRONI\_RESTAPI\_CONNECT\_ADDRESS**: IP address and port to access the REST API.
-  **PATRONI\_RESTAPI\_LISTEN**: IP address and port that Patroni will listen to, to provide health-check information for HAProxy.
-  **PATRONI\_RESTAPI\_USERNAME**: Basic-auth username to protect unsafe REST API endpoints.
-  **PATRONI\_RESTAPI\_PASSWORD**: Basic-auth password to protect unsafe REST API endpoints.
-  **PATRONI\_RESTAPI\_CERTFILE**: Specifies the file with the certificate in the PEM format. If the certfile is not specified or is left empty, the API server will work without SSL.
-  **PATRONI\_RESTAPI\_KEYFILE**: Specifies the file with the secret key in the PEM format.

ZooKeeper
---------
-  **PATRONI\_ZOOKEEPER\_HOSTS**: comma separated list of ZooKeeper cluster members: "'host1:port1','host2:port2','etc...'". It is important to quote every single entity!
