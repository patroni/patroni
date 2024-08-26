# Dockerfile and Dockerfile.citus
You can run Patroni in a docker container using these Dockerfiles

They are meant in aiding development of Patroni and quick testing of features and not a production-worthy!

    docker build -t patroni .
    docker build -f Dockerfile.citus -t patroni-citus .

# Examples

## Standalone Patroni

    docker run -d patroni

## Three-node Patroni cluster

In addition to three Patroni containers the stack starts three containers with etcd (forming a three-node cluster), and one container with haproxy.
The haproxy listens on ports 5000 (connects to the primary) and 5001 (does load-balancing between healthy standbys).

Example session:

    $ docker compose up -d
    ✔ Network patroni_demo     Created
    ✔ Container demo-etcd1     Started
    ✔ Container demo-haproxy   Started
    ✔ Container demo-patroni1  Started
    ✔ Container demo-patroni2  Started
    ✔ Container demo-patroni3  Started
    ✔ Container demo-etcd2     Started
    ✔ Container demo-etcd3     Started

    $ docker ps
    CONTAINER ID   IMAGE     COMMAND                  CREATED          STATUS          PORTS                                                           NAMES
    a37bcec56726   patroni   "/bin/sh /entrypoint…"   15 minutes ago   Up 15 minutes                                                                   demo-etcd3
    034ab73868a8   patroni   "/bin/sh /entrypoint…"   15 minutes ago   Up 15 minutes                                                                   demo-patroni2
    03837736f710   patroni   "/bin/sh /entrypoint…"   15 minutes ago   Up 15 minutes                                                                   demo-patroni3
    22815c3d85b3   patroni   "/bin/sh /entrypoint…"   15 minutes ago   Up 15 minutes                                                                   demo-etcd2
    814b4304d132   patroni   "/bin/sh /entrypoint…"   15 minutes ago   Up 15 minutes   0.0.0.0:5000-5001->5000-5001/tcp, :::5000-5001->5000-5001/tcp   demo-haproxy
    6375b0ba2d0a   patroni   "/bin/sh /entrypoint…"   15 minutes ago   Up 15 minutes                                                                   demo-patroni1
    aef8bf3ee91f   patroni   "/bin/sh /entrypoint…"   15 minutes ago   Up 15 minutes                                                                   demo-etcd1

    $ docker logs demo-patroni1
    2024-08-26 09:04:33,547 INFO: Selected new etcd server http://172.29.0.3:2379
    2024-08-26 09:04:33,605 INFO: Lock owner: None; I am patroni1
    2024-08-26 09:04:33,693 INFO: trying to bootstrap a new cluster
    ...
    2024-08-26 09:04:34.920 UTC [43] LOG:  starting PostgreSQL 16.4 (Debian 16.4-1.pgdg120+1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 12.2.0-14) 12.2.0, 64-bit
    2024-08-26 09:04:34.921 UTC [43] LOG:  listening on IPv4 address "0.0.0.0", port 5432
    2024-08-26 09:04:34,922 INFO: postmaster pid=43
    2024-08-26 09:04:34.922 UTC [43] LOG:  listening on Unix socket "/var/run/postgresql/.s.PGSQL.5432"
    2024-08-26 09:04:34.925 UTC [47] LOG:  database system was shut down at 2024-08-26 09:04:34 UTC
    2024-08-26 09:04:34.928 UTC [43] LOG:  database system is ready to accept connections
    localhost:5432 - accepting connections
    localhost:5432 - accepting connections
    2024-08-26 09:04:34,938 INFO: establishing a new patroni heartbeat connection to postgres
    2024-08-26 09:04:34,992 INFO: running post_bootstrap
    2024-08-26 09:04:35,004 WARNING: User creation via "bootstrap.users" will be removed in v4.0.0
    2024-08-26 09:04:35,009 WARNING: Could not activate Linux watchdog device: Can't open watchdog device: [Errno 2] No such file or directory: '/dev/watchdog'
    2024-08-26 09:04:35,189 INFO: initialized a new cluster
    2024-08-26 09:04:35,328 INFO: no action. I am (patroni1), the leader with the lock
    2024-08-26 09:04:43,824 INFO: establishing a new patroni restapi connection to postgres
    2024-08-26 09:04:45,322 INFO: no action. I am (patroni1), the leader with the lock
    2024-08-26 09:04:55,320 INFO: no action. I am (patroni1), the leader with the lock
    ...

    $ docker exec -ti demo-patroni1 bash
    postgres@patroni1:~$ patronictl list
    + Cluster: demo (7303838734793224214) --------+----+-----------+
    | Member   | Host       | Role    | State     | TL | Lag in MB |
    +----------+------------+---------+-----------+----+-----------+
    | patroni1 | 172.29.0.2 | Leader  | running   |  1 |           |
    | patroni2 | 172.29.0.6 | Replica | streaming |  1 |         0 |
    | patroni3 | 172.29.0.5 | Replica | streaming |  1 |         0 |
    +----------+------------+---------+-----------+----+-----------+

    postgres@patroni1:~$ etcdctl get --keys-only --prefix /service/demo
    /service/demo/config
    /service/demo/initialize
    /service/demo/leader
    /service/demo/members/patroni1
    /service/demo/members/patroni2
    /service/demo/members/patroni3
    /service/demo/status

    postgres@patroni1:~$ etcdctl member list
    2bf3e2ceda5d5960, started, etcd2, http://etcd2:2380, http://172.29.0.3:2379
    55b3264e129c7005, started, etcd3, http://etcd3:2380, http://172.29.0.7:2379
    acce7233f8ec127e, started, etcd1, http://etcd1:2380, http://172.29.0.8:2379


    postgres@patroni1:~$ exit

    $ docker exec -ti demo-haproxy bash
    postgres@haproxy:~$ psql -h localhost -p 5000 -U postgres -W
    Password: postgres
    psql (16.4 (Debian 16.4-1.pgdg120+1))
    Type "help" for help.

    postgres=# SELECT pg_is_in_recovery();
     pg_is_in_recovery
    ───────────────────
     f
    (1 row)

    postgres=# \q

    postgres@haproxy:~$ psql -h localhost -p 5001 -U postgres -W
    Password: postgres
    psql (16.4 (Debian 16.4-1.pgdg120+1))
    Type "help" for help.

    postgres=# SELECT pg_is_in_recovery();
     pg_is_in_recovery
    ───────────────────
     t
    (1 row)

## Citus cluster

The stack starts three containers with etcd (forming a three-node etcd cluster), seven containers with Patroni+PostgreSQL+Citus (three coordinator nodes, and two worker clusters with two nodes each), and one container with haproxy.
The haproxy listens on ports 5000 (connects to the coordinator primary) and 5001 (does load-balancing between worker primary nodes).

Example session:

    $ docker-compose -f docker-compose-citus.yml up -d
    ✔ Network patroni_demo    Created
    ✔ Container demo-coord2   Started
    ✔ Container demo-work2-2  Started
    ✔ Container demo-etcd1    Started
    ✔ Container demo-haproxy  Started
    ✔ Container demo-work1-1  Started
    ✔ Container demo-work2-1  Started
    ✔ Container demo-work1-2  Started
    ✔ Container demo-coord1   Started
    ✔ Container demo-etcd3    Started
    ✔ Container demo-coord3   Started
    ✔ Container demo-etcd2    Started


    $ docker ps
    CONTAINER ID   IMAGE           COMMAND                  CREATED          STATUS          PORTS                                                           NAMES
    79c95492fac9   patroni-citus   "/bin/sh /entrypoint…"   11 minutes ago   Up 11 minutes                                                                   demo-etcd3
    77eb82d0f0c1   patroni-citus   "/bin/sh /entrypoint…"   11 minutes ago   Up 11 minutes                                                                   demo-work2-1
    03dacd7267ef   patroni-citus   "/bin/sh /entrypoint…"   11 minutes ago   Up 11 minutes                                                                   demo-etcd1
    db9206c66f85   patroni-citus   "/bin/sh /entrypoint…"   11 minutes ago   Up 11 minutes                                                                   demo-etcd2
    9a0fef7b7dd4   patroni-citus   "/bin/sh /entrypoint…"   11 minutes ago   Up 11 minutes                                                                   demo-work1-2
    f06b031d99dc   patroni-citus   "/bin/sh /entrypoint…"   11 minutes ago   Up 11 minutes                                                                   demo-work2-2
    f7c58545f314   patroni-citus   "/bin/sh /entrypoint…"   11 minutes ago   Up 11 minutes                                                                   demo-coord2
    383f9e7e188a   patroni-citus   "/bin/sh /entrypoint…"   11 minutes ago   Up 11 minutes                                                                   demo-work1-1
    f02e96dcc9d6   patroni-citus   "/bin/sh /entrypoint…"   11 minutes ago   Up 11 minutes                                                                   demo-coord3
    6945834b7056   patroni-citus   "/bin/sh /entrypoint…"   11 minutes ago   Up 11 minutes                                                                   demo-coord1
    b96ca42f785d   patroni-citus   "/bin/sh /entrypoint…"   11 minutes ago   Up 11 minutes   0.0.0.0:5000-5001->5000-5001/tcp, :::5000-5001->5000-5001/tcp   demo-haproxy


    $ docker logs demo-coord1
    2024-08-26 08:21:05,323 INFO: Selected new etcd server http://172.19.0.5:2379
    2024-08-26 08:21:05,339 INFO: No PostgreSQL configuration items changed, nothing to reload.
    2024-08-26 08:21:05,388 INFO: Lock owner: None; I am coord1
    2024-08-26 08:21:05,480 INFO: trying to bootstrap a new cluster
    ...
    2024-08-26 08:21:17,115 INFO: postmaster pid=35
    localhost:5432 - no response
    2024-08-26 08:21:17.127 UTC [35] LOG:  starting PostgreSQL 16.4 (Debian 16.4-1.pgdg120+1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 12.2.0-14) 12.2.0, 64-bit
    2024-08-26 08:21:17.127 UTC [35] LOG:  listening on IPv4 address "0.0.0.0", port 5432
    2024-08-26 08:21:17.141 UTC [35] LOG:  listening on Unix socket "/var/run/postgresql/.s.PGSQL.5432"
    2024-08-26 08:21:17.155 UTC [39] LOG:  database system was shut down at 2024-08-26 08:21:05 UTC
    2024-08-26 08:21:17.182 UTC [35] LOG:  database system is ready to accept connections
    2024-08-26 08:21:17,683 INFO: establishing a new patroni heartbeat connection to postgres
    2024-08-26 08:21:17,704 INFO: establishing a new patroni restapi connection to postgres
    localhost:5432 - accepting connections
    localhost:5432 - accepting connections
    2024-08-26 08:21:18,202 INFO: running post_bootstrap
    2024-08-26 08:21:19.048 UTC [53] LOG:  starting maintenance daemon on database 16385 user 10
    2024-08-26 08:21:19.048 UTC [53] CONTEXT:  Citus maintenance daemon for database 16385 user 10
    2024-08-26 08:21:19,058 WARNING: Could not activate Linux watchdog device: Can't open watchdog device: [Errno 2] No such file or directory: '/dev/watchdog'
    2024-08-26 08:21:19.250 UTC [37] LOG:  checkpoint starting: immediate force wait
    2024-08-26 08:21:19,275 INFO: initialized a new cluster
    2024-08-26 08:21:22.946 UTC [37] LOG:  checkpoint starting: immediate force wait
    2024-08-26 08:21:29,059 INFO: Lock owner: coord1; I am coord1
    2024-08-26 08:21:29,205 INFO: Enabled synchronous replication
    2024-08-26 08:21:29,206 DEBUG: query(SELECT groupid, nodename, nodeport, noderole, nodeid FROM pg_catalog.pg_dist_node, ())
    2024-08-26 08:21:29,206 INFO: establishing a new patroni citus connection to postgres
    2024-08-26 08:21:29,206 DEBUG: Adding the new task: PgDistTask({PgDistNode(nodeid=None,host=172.19.0.8,port=5432,role=primary)})
    2024-08-26 08:21:29,206 DEBUG: Adding the new task: PgDistTask({PgDistNode(nodeid=None,host=172.19.0.2,port=5432,role=primary)})
    2024-08-26 08:21:29,206 DEBUG: Adding the new task: PgDistTask({PgDistNode(nodeid=None,host=172.19.0.9,port=5432,role=primary)})
    2024-08-26 08:21:29,219 DEBUG: query(SELECT pg_catalog.citus_add_node(%s, %s, %s, %s, 'default'), ('172.19.0.2', 5432, 1, 'primary'))
    2024-08-26 08:21:29,256 DEBUG: query(SELECT pg_catalog.citus_add_node(%s, %s, %s, %s, 'default'), ('172.19.0.9', 5432, 2, 'primary'))
    2024-08-26 08:21:29,474 INFO: no action. I am (coord1), the leader with the lock
    2024-08-26 08:21:39,060 INFO: Lock owner: coord1; I am coord1
    2024-08-26 08:21:39,159 DEBUG: Adding the new task: PgDistTask({PgDistNode(nodeid=None,host=172.19.0.8,port=5432,role=primary), PgDistNode(nodeid=None,host=172.19.0.11,port=5432,role=secondary), PgDistNode(nodeid=None,host=172.19.0.7,port=5432,role=secondary)})
    2024-08-26 08:21:39,159 DEBUG: Adding the new task: PgDistTask({PgDistNode(nodeid=None,host=172.19.0.2,port=5432,role=primary), PgDistNode(nodeid=None,host=172.19.0.12,port=5432,role=secondary)})
    2024-08-26 08:21:39,159 DEBUG: Adding the new task: PgDistTask({PgDistNode(nodeid=None,host=172.19.0.6,port=5432,role=secondary), PgDistNode(nodeid=None,host=172.19.0.9,port=5432,role=primary)})
    2024-08-26 08:21:39,160 DEBUG: query(BEGIN, ())
    2024-08-26 08:21:39,160 DEBUG: query(SELECT pg_catalog.citus_add_node(%s, %s, %s, %s, 'default'), ('172.19.0.11', 5432, 0, 'secondary'))
    2024-08-26 08:21:39,164 DEBUG: query(SELECT pg_catalog.citus_add_node(%s, %s, %s, %s, 'default'), ('172.19.0.7', 5432, 0, 'secondary'))
    2024-08-26 08:21:39,166 DEBUG: query(COMMIT, ())
    2024-08-26 08:21:39,176 DEBUG: query(SELECT pg_catalog.citus_add_node(%s, %s, %s, %s, 'default'), ('172.19.0.12', 5432, 1, 'secondary'))
    2024-08-26 08:21:39,191 DEBUG: query(SELECT pg_catalog.citus_add_node(%s, %s, %s, %s, 'default'), ('172.19.0.6', 5432, 2, 'secondary'))
    2024-08-26 08:21:39,211 INFO: no action. I am (coord1), the leader with the lock
    2024-08-26 08:21:49,060 INFO: Lock owner: coord1; I am coord1
    2024-08-26 08:21:49,166 INFO: Setting synchronous replication to 1 of 2 (coord2, coord3)
    server signaled
    2024-08-26 08:21:49.170 UTC [35] LOG:  received SIGHUP, reloading configuration files
    2024-08-26 08:21:49.171 UTC [35] LOG:  parameter "synchronous_standby_names" changed to "ANY 1 (coord2,coord3)"
    2024-08-26 08:21:49.377 UTC [68] LOG:  standby "coord2" is now a candidate for quorum synchronous standby
    2024-08-26 08:21:49.377 UTC [68] STATEMENT:  START_REPLICATION SLOT "coord2" 0/3000000 TIMELINE 1
    2024-08-26 08:21:49.377 UTC [69] LOG:  standby "coord3" is now a candidate for quorum synchronous standby
    2024-08-26 08:21:49.377 UTC [69] STATEMENT:  START_REPLICATION SLOT "coord3" 0/4000000 TIMELINE 1
    2024-08-26 08:21:50,278 INFO: Setting leader to coord1, quorum to 1 of 2 (coord2, coord3)
    2024-08-26 08:21:50,390 INFO: no action. I am (coord1), the leader with the lock
    2024-08-26 08:21:59,159 INFO: no action. I am (coord1), the leader with the lock
    ...

    $ docker exec -ti demo-haproxy bash
    postgres@haproxy:~$ etcdctl member list
    2b28411e74c0c281, started, etcd3, http://etcd3:2380, http://172.30.0.4:2379
    6c70137d27cfa6c1, started, etcd2, http://etcd2:2380, http://172.30.0.5:2379
    a28f9a70ebf21304, started, etcd1, http://etcd1:2380, http://172.30.0.6:2379

    postgres@haproxy:~$ etcdctl get --keys-only --prefix /service/demo
    /service/demo/0/config
    /service/demo/0/initialize
    /service/demo/0/leader
    /service/demo/0/members/coord1
    /service/demo/0/members/coord2
    /service/demo/0/members/coord3
    /service/demo/0/status
    /service/demo/0/sync
    /service/demo/1/config
    /service/demo/1/initialize
    /service/demo/1/leader
    /service/demo/1/members/work1-1
    /service/demo/1/members/work1-2
    /service/demo/1/status
    /service/demo/1/sync
    /service/demo/2/config
    /service/demo/2/initialize
    /service/demo/2/leader
    /service/demo/2/members/work2-1
    /service/demo/2/members/work2-2
    /service/demo/2/status
    /service/demo/2/sync

    postgres@haproxy:~$ psql -h localhost -p 5000 -U postgres -d citus
    Password for user postgres: postgres
    psql (16.4 (Debian 16.4-1.pgdg120+1))
    SSL connection (protocol: TLSv1.3, cipher: TLS_AES_256_GCM_SHA384, compression: off)
    Type "help" for help.

    citus=# select pg_is_in_recovery();
     pg_is_in_recovery
    -------------------
     f
    (1 row)

    citus=# table pg_dist_node;
     nodeid | groupid |  nodename   | nodeport | noderack | hasmetadata | isactive | noderole  | nodecluster | metadatasynced | shouldhaveshards
    --------+---------+-------------+----------+----------+-------------+----------+-----------+-------------+----------------+------------------
          1 |       0 | 172.19.0.8  |     5432 | default  | t           | t        | primary   | default     | t              | f
          2 |       1 | 172.19.0.2  |     5432 | default  | t           | t        | primary   | default     | t              | t
          3 |       2 | 172.19.0.9  |     5432 | default  | t           | t        | primary   | default     | t              | t
          4 |       0 | 172.19.0.11 |     5432 | default  | t           | t        | secondary | default     | t              | f
          5 |       0 | 172.19.0.7  |     5432 | default  | t           | t        | secondary | default     | t              | f
          6 |       1 | 172.19.0.12 |     5432 | default  | f           | t        | secondary | default     | f              | t
          7 |       2 | 172.19.0.6  |     5432 | default  | f           | t        | secondary | default     | f              | t
    (7 rows)

    citus=# \q

    postgres@haproxy:~$ patronictl list
    + Citus cluster: demo ----------+----------------+-----------+----+-----------+
    | Group | Member  | Host        | Role           | State     | TL | Lag in MB |
    +-------+---------+-------------+----------------+-----------+----+-----------+
    |     0 | coord1  | 172.19.0.8  | Leader         | running   |  1 |           |
    |     0 | coord2  | 172.19.0.7  | Quorum Standby | streaming |  1 |         0 |
    |     0 | coord3  | 172.19.0.11 | Quorum Standby | streaming |  1 |         0 |
    |     1 | work1-1 | 172.19.0.12 | Quorum Standby | streaming |  1 |         0 |
    |     1 | work1-2 | 172.19.0.2  | Leader         | running   |  1 |           |
    |     2 | work2-1 | 172.19.0.6  | Quorum Standby | streaming |  1 |         0 |
    |     2 | work2-2 | 172.19.0.9  | Leader         | running   |  1 |           |
    +-------+---------+-------------+----------------+-----------+----+-----------+


    postgres@haproxy:~$ patronictl switchover --group 2 --force
    Current cluster topology
    + Citus cluster: demo (group: 2, 7407360296219029527) ---+-----------+
    | Member  | Host       | Role           | State     | TL | Lag in MB |
    +---------+------------+----------------+-----------+----+-----------+
    | work2-1 | 172.19.0.6 | Quorum Standby | streaming |  1 |         0 |
    | work2-2 | 172.19.0.9 | Leader         | running   |  1 |           |
    +---------+------------+----------------+-----------+----+-----------+
    2024-08-26 08:31:45.92277 Successfully switched over to "work2-1"
    + Citus cluster: demo (group: 2, 7407360296219029527) ------+
    | Member  | Host       | Role    | State   | TL | Lag in MB |
    +---------+------------+---------+---------+----+-----------+
    | work2-1 | 172.19.0.6 | Leader  | running |  1 |           |
    | work2-2 | 172.19.0.9 | Replica | stopped |    |   unknown |
    +---------+------------+---------+---------+----+-----------+

    postgres@haproxy:~$ patronictl list
    + Citus cluster: demo ----------+----------------+-----------+----+-----------+
    | Group | Member  | Host        | Role           | State     | TL | Lag in MB |
    +-------+---------+-------------+----------------+-----------+----+-----------+
    |     0 | coord1  | 172.19.0.8  | Leader         | running   |  1 |           |
    |     0 | coord2  | 172.19.0.7  | Quorum Standby | streaming |  1 |         0 |
    |     0 | coord3  | 172.19.0.11 | Quorum Standby | streaming |  1 |         0 |
    |     1 | work1-1 | 172.19.0.12 | Quorum Standby | streaming |  1 |         0 |
    |     1 | work1-2 | 172.19.0.2  | Leader         | running   |  1 |           |
    |     2 | work2-1 | 172.19.0.6  | Leader         | running   |  2 |           |
    |     2 | work2-2 | 172.19.0.9  | Quorum Standby | streaming |  2 |         0 |
    +-------+---------+-------------+----------------+-----------+----+-----------+

    postgres@haproxy:~$ psql -h localhost -p 5000 -U postgres -d citus
    Password for user postgres: postgres
    psql (16.4 (Debian 16.4-1.pgdg120+1))
    SSL connection (protocol: TLSv1.3, cipher: TLS_AES_256_GCM_SHA384, compression: off)
    Type "help" for help.

    citus=# table pg_dist_node;
     nodeid | groupid |  nodename   | nodeport | noderack | hasmetadata | isactive | noderole  | nodecluster | metadatasynced | shouldhaveshards
    --------+---------+-------------+----------+----------+-------------+----------+-----------+-------------+----------------+------------------
          1 |       0 | 172.19.0.8  |     5432 | default  | t           | t        | primary   | default     | t              | f
          4 |       0 | 172.19.0.11 |     5432 | default  | t           | t        | secondary | default     | t              | f
          5 |       0 | 172.19.0.7  |     5432 | default  | t           | t        | secondary | default     | t              | f
          6 |       1 | 172.19.0.12 |     5432 | default  | f           | t        | secondary | default     | f              | t
          3 |       2 | 172.19.0.6  |     5432 | default  | t           | t        | primary   | default     | t              | t
          2 |       1 | 172.19.0.2  |     5432 | default  | t           | t        | primary   | default     | t              | t
          8 |       2 | 172.19.0.9  |     5432 | default  | f           | t        | secondary | default     | f              | t
    (7 rows)
