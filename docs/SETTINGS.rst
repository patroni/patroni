.. _settings:

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
    -  **retry\_timeout**: timeout for DCS and PostgreSQL operation retries. DCS or network issues shorter than this will not cause Patroni to demote the leader. Default value: 10
    -  **maximum\_lag\_on\_failover**: the maximum bytes a follower may lag to be able to participate in leader election.
    -  **master\_start\_timeout**: the amount of time a master is allowed to recover from failures before failover is triggered. Default is 300 seconds. When set to 0 failover is done immediately after a crash is detected if possible. When using asynchronous replication a failover can cause lost transactions. Best worst case failover time for master failure is: loop\_wait + master\_start\_timeout + loop\_wait, unless master\_start\_timeout is zero, in which case it's just loop\_wait. Set the value according to your durability/availability tradeoff.
    -  **synchronous\_mode**: turns on synchronous replication mode. In this mode a replica will be chosen as synchronous and only the latest leader and synchronous replica are able to participate in leader election. Synchronous mode makes sure that successfully committed transactions will not be lost at failover, at the cost of losing availability for writes when Patroni cannot ensure transaction durability. See :ref:`replication modes documentation <replication_modes>` for details.
    -  **postgresql**:
        -  **use\_pg\_rewind**: whether or not to use pg_rewind
        -  **use\_slots**: whether or not to use replication_slots. Must be False for PostgreSQL 9.3. You should comment out max_replication_slots before it becomes ineligible for leader status.
        -  **recovery\_conf**: additional configuration settings written to recovery.conf when configuring follower. 
        -  **parameters**: list of configuration settings for Postgres. Many of these are required for replication to work.
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
-  **checks**: (optional) list of Consul health checks used for the session. If not specified Consul will use "serfHealth" in additional to the TTL based check created by Patroni. Additional checks, in particular the "serfHealth", may cause the leader lock to expire faster than in `ttl` seconds when the leader instance becomes unavailable

Etcd
----
Most of the parameters are optional, but you have to specify one of the **host**, **hosts**, **url**, **proxy** or **srv**

-  **host**: the host:port for the etcd endpoint.
-  **hosts**: list of etcd endpoint in format host1:port1,host2:port2,etc... Could be a comma separated string or an actual yaml list.
-  **url**: url for the etcd
-  **proxy**: proxy url for the etcd. If you are connecting to the etcd using proxy, use this parameter instead of **url**
-  **srv**: Domain to search the SRV record(s) for cluster autodiscovery.
-  **protocol**: (optional) http or https, if not specified http is used. If the **url** or **proxy** is specified - will take protocol from them.
-  **username**: (optional) username for etcd authentication
-  **password**: (optional) password for etcd authentication.
-  **cacert**: (optional) The ca certificate. If present it will enable validation.
-  **cert**: (optional) file with the client certificate
-  **key**: (optional) file with the client key. Can be empty if the key is part of **cert**.

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
-  **ports**: (optional) if the Service object has the name for the port, the same name must appear in the Endpoint object, otherwise service won't work. For example, if your service is defined as ``{Kind: Service, spec: {ports: [{name: postgresql, port: 5432, targetPort: 5432}]}}``, then you have to set ``kubernetes.ports: {[{"name": "postgresql", "port": 5432}]}`` and Patroni will use it for updating subsets of the leader Endpoint. This parameter is used only if `kubernetes.use_endpoints` is set.

.. _postgresql_settings:

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
-  **create\_replica\_methods**: an ordered list of the create methods for turning a Patroni node into a new replica.
   "basebackup" is the default method; other methods are assumed to refer to scripts, each of which is configured as its
   own config item. See :ref:`custom replica creation methods documentation <custom_replica_creation>` for further explanation.
-  **data\_dir**: The location of the Postgres data directory, either existing or to be initialized by Patroni.
-  **config\_dir**: The location of the Postgres configuration directory, defaults to the data directory. Must be writable by Patroni.
-  **bin\_dir**: Path to PostgreSQL binaries (pg_ctl, pg_rewind, pg_basebackup, postgres). The default value is an empty string meaning that PATH environment variable will be used to find the executables.
-  **listen**: IP address + port that Postgres listens to; must be accessible from other nodes in the cluster, if you're using streaming replication. Multiple comma-separated addresses are permitted, as long as the port component is appended after to the last one with a colon, i.e. ``listen: 127.0.0.1,127.0.0.2:5432``. Patroni will use the first address from this list to establish local connections to the PostgreSQL node.
-  **use\_unix\_socket**: specifies that Patroni should prefer to use unix sockets to connect to the cluster. Default value is ``false``. If ``unix_socket_directories`` is definded, Patroni will use first suitable value from it to connect to the cluster and fallback to tcp if nothing is suitable. If ``unix_socket_directories`` is not specified in ``postgresql.parameters``, Patroni will assume that default value should be used and omit ``host`` from connection parameters.
-  **pgpass**: path to the `.pgpass <https://www.postgresql.org/docs/current/static/libpq-pgpass.html>`__ password file. Patroni creates this file before executing pg\_basebackup, the post_init script and under some other circumstances. The location must be writable by Patroni.
-  **recovery\_conf**: additional configuration settings written to recovery.conf when configuring follower.
-  **custom\_conf** : path to an optional custom ``postgresql.conf`` file, that will be used in place of ``postgresql.base.conf``. The file must exist on all cluster nodes, be readable by PostgreSQL and will be included from its location on the real ``postgresql.conf``. Note that Patroni will not monitor this file for changes, nor backup it. However, its settings can still be overridden by Patroni's own configuration facilities - see :ref:`dynamic configuration <dynamic_configuration>` for details.
-  **parameters**: list of configuration settings for Postgres. Many of these are required for replication to work.
-  **pg\_hba**: list of lines that Patroni will use to generate ``pg_hba.conf``. This parameter has higher priority than ``bootstrap.pg_hba``. Together with :ref:`dynamic configuration <dynamic_configuration>` it simplifies management of ``pg_hba.conf``.
        -  **- host all all 0.0.0.0/0 md5**.
        -  **- host replication replicator 127.0.0.1/32 md5**: A line like this is required for replication.
-  **pg\_ctl\_timeout**: How long should pg_ctl wait when doing ``start``, ``stop`` or ``restart``. Default value is 60 seconds.
-  **use\_pg\_rewind**: try to use pg\_rewind on the former leader when it joins cluster as a replica.
-  **remove\_data\_directory\_on\_rewind\_failure**: If this option is enabled, Patroni will remove postgres data directory and recreate replica. Otherwise it will try to follow the new leader. Default value is **false**.
-  **replica\_method**: for each create_replica_methods other than basebackup, you would add a configuration section of the same name. At a minimum, this should include "command" with a full path to the actual script to be executed. Other configuration parameters will be passed along to the script in the form "parameter=value".

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

Watchdog
--------
- **mode**: ``off``, ``automatic`` or ``required``. When ``off`` watchdog is disabled. When ``automatic`` watchdog will be used if available, but ignored if it is not. When ``required`` the node will not become a leader unless watchdog can be successfully enabled.
- **device**: Path to watchdog device. Defaults to ``/dev/watchdog``.
- **safety_margin**: Number of seconds of safety margin between watchdog triggering and leader key expiration.
