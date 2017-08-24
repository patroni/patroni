.. _environment:

==================================
Environment Configuration Settings
==================================

It is possible to override some of the configuration parameters defined in the Patroni configuration file using the system environment variables. This document lists all environment variables handled by Patroni. The values set via those variables always take precedence over the ones set in the Patroni configuration file.

Global/Universal
----------------
-  **PATRONI\_CONFIGURATION**: it is possible to set the entire configuration for the Patroni via ``PATRONI_CONFIGURATION`` environment variable. In this case any other environment variables will not be considered!
-  **PATRONI\_NAME**: name of the node where the current instance of Patroni is running. Must be unique for the cluster.
-  **PATRONI\_NAMESPACE**: path within the configuration store where Patroni will keep information about the cluster. Default value: "/service"
-  **PATRONI\_SCOPE**: cluster name

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
-  **PATRONI\_CONSUL\_CACERT**: (optional) The ca certificate. If pressent it will enable validation.
-  **PATRONI\_CONSUL\_CERT**: (optional) File with the client certificate
-  **PATRONI\_CONSUL\_KEY**: (optional) File with the client key. Can be empty if the key is part of certificate.

Etcd
----
-  **PATRONI\_ETCD\_HOST**: the host:port for the etcd endpoint.
-  **PATRONI\_ETCD\_URL**: url for the etcd, in format: http(s)://(username:password@)host:port
-  **PATRONI\_ETCD\_PROXY**: proxy url for the etcd. If you are connecting to the etcd using proxy, use this parameter instead of **PATRONI\_ETCD\_URL**
-  **PATRONI\_ETCD\_SRV**: Domain to search the SRV record(s) for cluster autodiscovery.
-  **PATRONI\_ETCD\_CACERT**: The ca certificate. If pressent it will enable validation.
-  **PATRONI\_ETCD\_CERT**: File with the client certificate
-  **PATRONI\_ETCD\_KEY**: File with the client key. Can be empty if the key is part of certificate.

Exhibitor
---------
-  **PATRONI\_EXHIBITOR\_HOSTS**: initial list of Exhibitor (ZooKeeper) nodes in format: 'host1,host2,etc...'. This list updates automatically whenever the Exhibitor (ZooKeeper) cluster topology changes.
-  **PATRONI\_EXHIBITOR\_PORT**: Exhibitor port.

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
