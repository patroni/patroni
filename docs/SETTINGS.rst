===========================
YAML Configuration Settings
===========================

Global/Universal
----------------
-  **name**: the name of the host. Must be unique for the cluster.
-  **namespace**: path within the configuration store where Patroni will keep information about the cluster. Default value: "/service"
-  **scope**: cluster name

Bootstrap configuration
-----------------------
-  **dcs**: This section will be written into `/<namespace>/<scope>/config` of a given configuration store after initializing of new cluster. This is the global configuration for the cluster. If you want to change some parameters for all cluster nodes - just do it in DCS (or via Patroni API) and all nodes will apply this configuration.
    -  **loop\_wait**: the number of seconds the loop will sleep. Default value: 10
    -  **ttl**: the TTL to acquire the leader lock. Think of it as the length of time before initiation of the automatic failover process. Default value: 30
    -  **maximum\_lag\_on\_failover**: the maximum bytes a follower may lag to be able to participate in leader election.
    -  **postgresql**:
        -  **use\_pg\_rewind**:whether or not to use pg_rewind
        -  **use\_slots**: whether or not to use replication_slots. Must be False for PostgreSQL 9.3. You should comment out max_replication_slots before it becomes ineligible for leader status.
        -  **recovery\_conf**: additional configuration settings written to recovery.conf when configuring follower. 
        -  **parameters**: list of configuration settings for Postgres. Many of these are required for replication to work.
-  **initdb**: List options to be passed on to initdb.
        -  **- data-checksums**: Must be enabled when pg_rewind is needed on 9.3.
        -  **- encoding: UTF8**: default encoding for new databases.
        -  **- locale: UTF8**: default locale for new databases.
-  **pg\_hba**: list of lines that you should add to pg\_hba.conf.
        -  **- host all all 0.0.0.0/0 md5**.
        -  **- host replication replicator 127.0.0.1/32 md5**: A line like this is required for replication.
-  **users**: Some additional users users which needs to be created after initializing new cluster
    -  **admin**: the name of user
        -  **password: zalando**:
        -  **options**: list of options for CREATE USER statement
            -  **- createrole**
            -  **- createdb**

Consul
------
-  **host**: the host:port for the Consul endpoint.

Etcd
----
-  **host**: the host:port for the etcd endpoint.

Exhibitor
---------
-  **hosts**: initial list of Exhibitor (ZooKeeper) nodes in format: ['host1', 'host2', 'etc...' ]. This list updates automatically whenever the Exhibitor (ZooKeeper) cluster topology changes.
-  **poll\_interval**: how often the list of ZooKeeper and Exhibitor nodes should be updated from Exhibitor
-  **port**: Exhibitor port.

PostgreSQL
----------
-  **authentication**:
    -  **superuser**:
        -  **username**: name for the superuser, set during initialization (initdb) and later used by Patroni to connect to the postgres.
        -  **password**: password for the superuser, set during initialization (initdb).
    -  **replication**:
        -  **username**: replication username; the user will be created during initialization. Replicas will use this user to access master via streaming replication
        -  **password**: replication password; the user will be created during initialization.
-  **callbacks**: callback scripts to run on certain actions. Patroni will pass the action, role and cluster name. (See scripts/aws.py as an example of how to write them.)
        -  **on\_reload**: run this script when configuration reload is triggered.
        -  **on\_restart**: run this script when the cluster restarts.
        -  **on\_role\_change**: run this script when the cluster is being promoted or demoted.
        -  **on\_start**: run this script when the cluster starts.
        -  **on\_stop**: run this script when the cluster stops.
-  **connect\_address**: IP address + port through which Postgres is accessible from other nodes and applications.
-  **create\_replica\_methods**: an ordered list of the create methods for turning a Patroni node into a new replica. "basebackup" is the default method; other methods are assumed to refer to scripts, each of which is configured as its own config item.
-  **data\_dir**: The location of the Postgres data directory, either existing or to be initialized by Patroni.
-  **listen**: IP address + port that Postgres listens to; must be accessible from other nodes in the cluster, if you're using streaming replication. Multiple comma-separated addresses are permitted, as long as the port component is appended after to the last one with a colon, i.e. ``listen: 127.0.0.1,127.0.0.2:5432``. Patroni will use the first address from this list to establish local connections to the PostgreSQL node.
-  **pgpass**: path to the `.pgpass <https://www.postgresql.org/docs/current/static/libpq-pgpass.html>`__ password file. Patroni creates this file before executing pg\_basebackup and under some other circumstances. The location must be writable by Patroni.
-  **recovery\_conf**: additional configuration settings written to recovery.conf when configuring follower.
-  **parameters**: list of configuration settings for Postgres. Many of these are required for replication to work.
-  **replica\_method** for each create_replica_method other than basebackup, you would add a configuration section of the same name. At a minimum, this should include "command" with a full path to the actual script to be executed.  Other configuration parameters will be passed along to the script in the form "parameter=value".

REST API
-------- 
-  **connect\_address**: IP address and port to access the REST API.
-  **listen**: IP address and port that Patroni will listen to, to provide health-check information for HAProxy.
-  **Optional**:
        -  **authentication**:
            -  **username**: Basic-auth username to protect unsafe REST API endpoints.
            -  **password**: Basic-auth password to protect unsafe REST API endpoints.

        -  **certfile**: Specifies the file with the certificate in the PEM format. If the certfile is not specified or is left empty, the API server will work without SSL.
        -  **keyfile**: Specifies the file with the secret key in the PEM format.

ZooKeeper
----------
-  **hosts**: list of ZooKeeper cluster members in format: ['host1:port1', 'host2:port2', 'etc...'].
