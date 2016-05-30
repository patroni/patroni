YAML Configuration Settings

-  *loop\_wait*: the number of seconds the loop will sleep.
-  *ttl*: the TTL to acquire the leader lock. Think of it as the length of time before initiation of the automatic failover process.

Consul
---------------
-  *host*: the host:port for the Consul endpoint.
-  *scope*: the relative path used on Consul's HTTP API for this deployment; makes it possible to run multiple HA deployments from a single Consul cluster.
-  *ttl*: the TTL to acquire the leader lock. Think of it as the length of time before initiation of the automatic failover process.

etcd
---------------
-  *host*: the host:port for the etcd endpoint.
-  *scope*: the relative path used on etcd's HTTP API for this deployment. Makes it possible to run multiple HA deployments from a single etcd cluster.
-  *ttl*: the TTL to acquire the leader lock. Think of it as the length of time before initiation of the automatic failover process.


PostgreSQL
---------------
-  *admin*:
        -  *password*: admin password; user is created during initialization.
        -  *username*: admin username; user is created during initialization. It will have CREATEDB and CREATEROLE privileges.
-  *callbacks*: callback scripts to run on certain actions. Patroni will pass the action, role and cluster name. (See scripts/aws.py as an example of how to write them.)
        -  *on\_reload*: run this script when configuration reload is triggered.
        -  *on\_restart*: run this script when the cluster restarts.
        -  *on\_role\_change*: run this script when the cluster is being promoted or demoted.
        -  *on\_start*: run this script when the cluster starts.
        -  *on\_stop*: run this script when the cluster stops.
-  *connect\_address*: IP address + port through which Postgres is accessible from other nodes and applications.
-  *create\_replica\_methods*: an ordered list of the create methods for turning a Patroni node into a new replica. "basebackup" is the default method; other methods are assumed to refer to scripts, each of which is configured as its own config item.
-  *data\_dir*: file path to initialize and store Postgres data files.
-  *initdb*:  List options to be passed on to initdb.
-  *data-checksums*: Must be enabled when pg_rewind is needed on 9.3.
-  *encoding*: default encoding for new databases.
-  *locale*: default locale for new databases.
-  *listen*: IP address + port that Postgres listens to; must be accessible from other nodes in the cluster, if you're using streaming replication. Multiple comma-separated addresses are permitted, as long as the port component is appended after to the last one with a colon, i.e. ``listen: 127.0.0.1,127.0.0.2:5432``. Patroni will use the first address from this list to establish local connections to the PostgreSQL node.
-  *maximum\_lag\_on\_failover*: the maximum bytes a follower may lag.
-  *name*: the name of the Postgres host. Must be unique for the cluster.
-  *pg\_hba*: list of lines that you should add to pg\_hba.conf.
        -  *- host all all 0.0.0.0/0 md5*.
        -  *- host replication replicator 127.0.0.1/32 md5* # A line like this is required for replication.
-  *recovery\_conf*: additional configuration settings written to recovery.conf when configuring follower.
        -  *parameters*: list of configuration settings for Postgres. Many of these are required for replication to work.
-  *replica\_method* for each create_replica_method other than basebackup, you would add a configuration section of the same name. At a minimum, this should include "command" with a full path to the actual script to be executed.  Other configuration parameters will be passed along to the script in the form "parameter=value".
-  *replication*:
        -  *username*: replication username; user will be created during initialization.
        -  *password*: replication password; user will be created during initialization.
-  *use\_slots*: whether or not to use replication_slots. Must be False for PostgreSQL 9.3. You should comment out max_replication_slots before it becomes ineligible for leader status.
-  *superuser*:
        -  *password*: password for the Postgres user, set during initialization.

REST API
---------------
-  *connect\_address*: IP address and port through which restapi is accessible.
-  *listen*: IP address and port that Patroni will listen to, to provide health-check information for HAProxy.
-* Optional*:
-  *auth*: 'username:password' to protect dangerous REST API endpoints.
    -  *certfile*: Specifies a file with the certificate in the PEM format. If the certfile is not specified or is left empty, the API server will work without SSL.
    -  *keyfile*: Specifies a file with the secret key in the PEM format.


 ZooKeeper
---------------
-  *hosts*: list of ZooKeeper cluster members in format: ['host1:port1', 'host2:port2', 'etc...'].
-  *reconnect\_timeout*: how long you should try to reconnect to ZooKeeper after a connection loss. After this timeout, assume that you no longer have a lock and restart in read-only mode.
-  *scope*: the relative path used on ZooKeeper for this deployment. Makes it possible to run multiple HA deployments from a single ZooKeeper cluster.
-  *session\_timeout*: the TTL to acquire the leader lock. Think of it as the length of time before initiation of the automatic failover process.

(subsection)ZooKeeper Exhibitor
If you are running a ZooKeeper cluster under the Exhibitor supervisory, this section might interest you:
-  *hosts*: initial list of Exhibitor (ZooKeeper) nodes in format: ['host1', 'host2', 'etc...' ]. This list updates automatically whenever the Exhibitor (ZooKeeper) cluster topology changes.
-  *poll\_interval*: how often the list of ZooKeeper and Exhibitor nodes should be updated from Exhibitor
-  *port*: Exhibitor port.
