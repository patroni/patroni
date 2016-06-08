==================================
Environment Configuration Settings
==================================

Some of configuration parameters defined in the configuration file is possible to override via Environment variables
This document list all possible environment variables handled by Patroni.
Environment variable always takes precedence on configuration file.

Global/Universal
----------------
-  **PATRONI\_NAME**: name of the node where the current instance of Patroni is running. Must be unique for the cluster.
-  **PATRONI\_NAMESPACE**: path within configuration store where Patroni will keep information about cluster. Default value: "/service"
-  **PATRONI\_SCOPE**: cluster name

Bootstrap configuration
-----------------------
It is possible to define users which will be created right after initializing of a new cluster by defining following environment variables:
-  **PATRONI\_<username>\_PASSWORD='<password>'**
-  **PATRONI\_<username>\_OPTIONS='list,of,options'**
Example: defining of `PATRONI\_admin\_PASSWORD=admin` `PATRONI\_admin\_OPTIONS='createrole,createdb'` will cause creation of `admin` user which is allowed to create other users and databases

Consul
------
-  **PATRONI\_CONSUL\_HOST**: the host:port for the Consul endpoint.

Etcd
----
-  **PATRONI\_ETCD\_HOST**: the host:port for the etcd endpoint.

PostgreSQL
----------
-  **PATRONI\_POSTGRESQL\_LISTEN**: IP address + port that Postgres listens to. Multiple comma-separated addresses are permitted, as long as the port component is appended after to the last one with a colon, i.e. ``listen: 127.0.0.1,127.0.0.2:5432``. Patroni will use the first address from this list to establish local connections to the PostgreSQL node.
-  **PATRONI\_POSTGRESQL\_CONNECT\_ADDRESS**: IP address + port through which Postgres is accessible from other nodes and applications.
-  **PATRONI\_POSTGRESQL\_DATA\_DIR**: file path to initialize and store Postgres data files.
-  **PATRONI\_POSTGRESQL\_PGPASS**: path to `pgpass` file which would be created by Patroni when it is necessary (for example, before executing pg\_basebackup). This locations must be accessible for writing by Patroni.
-  **PATRONI\_REPLICATION\_USERNAME**: replication username; user will be created during initialization. Replicas will use this user to access master via streaming replication
-  **PATRONI\_REPLICATION\_PASSWORD**: replication password; user will be created during initialization.
-  **PATRONI\_SUPERUSER\_USERNAME**: name for the superuser, set during initialization (initdb) and later used by Patroni to connect to the postgres. Also this user is used by pg_rewind.
-  **PATRONI\_SUPERUSER\_PASSWORD**: password for the superuser, set during initialization (initdb).

REST API
-------- 
-  **PATRONI\_RESTAPI\_CONNECT\_ADDRESS**: IP address and port through which restapi is accessible.
-  **PATRONI\_RESTAPI\_LISTEN**: IP address and port that Patroni will listen to, to provide health-check information for HAProxy.
-  **PATRONI\_RESTAPI\_USERNAME**: Basic-auth username to protect dangerous REST API endpoints.
-  **PATRONI\_RESTAPI\_PASSWORD**: Basic-auth password to protect dangerous REST API endpoints.
-  **PATRONI\_RESTAPI\_CERTFILE**: Specifies a file with the certificate in the PEM format. If the certfile is not specified or is left empty, the API server will work without SSL.
-  **PATRONI\_RESTAPI\_KEYFILE**: Specifies a file with the secret key in the PEM format.

ZooKeeper
---------
-  **PATRONI\_ZOOKEEPER\_HOSTS**: comma separated list of ZooKeeper cluster members: 'host1:port1,host2:port2,etc...'
