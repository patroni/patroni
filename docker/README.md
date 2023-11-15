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
     ✔ Container demo-patroni2  Started
     ✔ Container demo-patroni1  Started
     ✔ Container demo-patroni3  Started
     ✔ Container demo-etcd2     Started
     ✔ Container demo-haproxy   Started
     ✔ Container demo-etcd3     Started
     ✔ Container demo-etcd1     Started

    $ docker ps
    CONTAINER ID   IMAGE           COMMAND                  CREATED          STATUS          PORTS                                                           NAMES
    302aee008512   patroni         "/bin/sh /entrypoint…"   4 minutes ago    Up 4 minutes                                                                    demo-etcd3
    89e38ef02268   patroni         "/bin/sh /entrypoint…"   4 minutes ago    Up 4 minutes                                                                    demo-etcd1
    998b2e0405bb   patroni         "/bin/sh /entrypoint…"   4 minutes ago    Up 4 minutes    0.0.0.0:5000-5001->5000-5001/tcp, :::5000-5001->5000-5001/tcp   demo-haproxy
    17717a187445   patroni         "/bin/sh /entrypoint…"   4 minutes ago    Up 4 minutes                                                                    demo-patroni1
    5f18c20896d5   patroni         "/bin/sh /entrypoint…"   4 minutes ago    Up 4 minutes                                                                    demo-etcd2
    8d5677f6b16d   patroni         "/bin/sh /entrypoint…"   4 minutes ago    Up 4 minutes                                                                    demo-patroni3
    051514b17f6a   patroni         "/bin/sh /entrypoint…"   4 minutes ago    Up 4 minutes                                                                    demo-patroni2

    $ docker logs demo-patroni1
    2023-11-15 16:37:12,784 ERROR: Failed to get list of machines from http://etcd2:2379/v3beta: ValueError("invalid literal for int() with base 10: 'not_decided'")
    2023-11-15 16:37:12,832 INFO: Selected new etcd server http://172.18.0.5:2379
    2023-11-15 16:37:12,872 INFO: No PostgreSQL configuration items changed, nothing to reload.
    2023-11-15 16:37:12,969 ERROR: watchprefix failed: ProtocolError("Connection broken: InvalidChunkLength(got length b'', 0 bytes read)", InvalidChunkLength(got length b'', 0 bytes read))
    2023-11-15 16:37:12,924 INFO: Lock owner: None; I am patroni1
    2023-11-15 16:37:13,015 INFO: failed to acquire initialize lock
    2023-11-15 16:37:14,856 INFO: Lock owner: patroni2; I am patroni1
    2023-11-15 16:37:14,856 INFO: trying to bootstrap from leader 'patroni2'
    2023-11-15 16:37:14,984 INFO: Lock owner: patroni2; I am patroni1
    2023-11-15 16:37:15,027 INFO: bootstrap from leader 'patroni2' in progress
    2023-11-15 16:37:15,970 INFO: replica has been created using basebackup
    2023-11-15 16:37:15,971 INFO: bootstrapped from leader 'patroni2'
    2023-11-15 16:37:16.466 UTC [41] LOG:  starting PostgreSQL 15.5 (Debian 15.5-1.pgdg120+1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 12.2.0-14) 12.2.0, 64-bit
    2023-11-15 16:37:16.466 UTC [41] LOG:  listening on IPv4 address "0.0.0.0", port 5432
    2023-11-15 16:37:16,468 INFO: postmaster pid=41
    2023-11-15 16:37:16.470 UTC [41] LOG:  listening on Unix socket "/var/run/postgresql/.s.PGSQL.5432"
    2023-11-15 16:37:16.473 UTC [45] LOG:  database system was interrupted; last known up at 2023-11-15 16:37:15 UTC
    2023-11-15 16:37:16.477 UTC [46] FATAL:  the database system is starting up
    localhost:5432 - rejecting connections
    2023-11-15 16:37:16.483 UTC [48] FATAL:  the database system is starting up
    localhost:5432 - rejecting connections
    2023-11-15 16:37:16.549 UTC [45] LOG:  entering standby mode
    2023-11-15 16:37:16.551 UTC [45] LOG:  redo starts at 0/20000D8
    2023-11-15 16:37:16.552 UTC [45] LOG:  consistent recovery state reached at 0/20001B0
    2023-11-15 16:37:16.552 UTC [41] LOG:  database system is ready to accept read-only connections
    2023-11-15 16:37:16.561 UTC [49] LOG:  started streaming WAL from primary at 0/3000000 on timeline 1
    localhost:5432 - accepting connections
    2023-11-15 16:37:17,493 INFO: Reaped pid=52, exit status=0
    2023-11-15 16:37:17,494 INFO: Lock owner: patroni2; I am patroni1
    2023-11-15 16:37:17,494 INFO: establishing a new patroni heartbeat connection to postgres
    2023-11-15 16:37:17,557 INFO: no action. I am (patroni1), a secondary, and following a leader (patroni2)
    2023-11-15 16:37:23,164 INFO: establishing a new patroni restapi connection to postgres
    2023-11-15 16:37:25,075 INFO: no action. I am (patroni1), a secondary, and following a leader (patroni2)

    $ docker exec -ti demo-patroni1 bash
    postgres@patroni1:~$ patronictl list
    + Cluster: demo (7301728872342061079) --------+----+-----------+
    | Member   | Host       | Role    | State     | TL | Lag in MB |
    +----------+------------+---------+-----------+----+-----------+
    | patroni1 | 172.18.0.3 | Replica | streaming |  1 |         0 |
    | patroni2 | 172.18.0.8 | Leader  | running   |  1 |           |
    | patroni3 | 172.18.0.4 | Replica | streaming |  1 |         0 |
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
    1bab629f01fa9065, started, etcd3, http://etcd3:2380, http://172.18.0.5:2379
    8ecb6af518d241cc, started, etcd2, http://etcd2:2380, http://172.18.0.2:2379
    b2e169fcb8a34028, started, etcd1, http://etcd1:2380, http://172.18.0.7:2379


    postgres@patroni1:~$ exit

    $ docker exec -ti demo-haproxy bash
    postgres@haproxy:~$ psql -h localhost -p 5000 -U postgres -W
    Password: postgres
    psql (15.5 (Debian 15.5-1.pgdg120+1))
    Type "help" for help.

    postgres=# SELECT pg_is_in_recovery();
     pg_is_in_recovery
    ───────────────────
     f
    (1 row)

    postgres=# \q

    postgres@haproxy:~$ psql -h localhost -p 5001 -U postgres -W
    Password: postgres
    psql (15.5 (Debian 15.5-1.pgdg120+1))
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

    $ docker compose -f docker-compose-citus.yml up -d
    ✔ Network patroni_demo    Created
    ✔ Container demo-etcd3    Started
    ✔ Container demo-etcd2    Started
    ✔ Container demo-work1-1  Started
    ✔ Container demo-work2-1  Started
    ✔ Container demo-coord1   Started
    ✔ Container demo-work1-2  Started
    ✔ Container demo-coord2   Started
    ✔ Container demo-haproxy  Started
    ✔ Container demo-etcd1    Started
    ✔ Container demo-work2-2  Started
    ✔ Container demo-coord3   Started

    $ docker ps
    CONTAINER ID   IMAGE           COMMAND                  CREATED         STATUS         PORTS                                                           NAMES
    b8924834836e   patroni-citus   "/bin/sh /entrypoint…"   4 minutes ago   Up 4 minutes                                                                   demo-work1-1
    b45f8daaa173   patroni-citus   "/bin/sh /entrypoint…"   4 minutes ago   Up 4 minutes                                                                   demo-coord3
    2ae2ee63737a   patroni-citus   "/bin/sh /entrypoint…"   4 minutes ago   Up 4 minutes                                                                   demo-work1-2
    6f3645abd0b3   patroni-citus   "/bin/sh /entrypoint…"   4 minutes ago   Up 4 minutes                                                                   demo-etcd2
    4ef625cca07a   patroni-citus   "/bin/sh /entrypoint…"   4 minutes ago   Up 4 minutes                                                                   demo-etcd3
    8e4a50e542fa   patroni-citus   "/bin/sh /entrypoint…"   4 minutes ago   Up 4 minutes                                                                   demo-coord1
    da7ae11caec4   patroni-citus   "/bin/sh /entrypoint…"   4 minutes ago   Up 4 minutes                                                                   demo-coord2
    43998188468b   patroni-citus   "/bin/sh /entrypoint…"   4 minutes ago   Up 4 minutes   0.0.0.0:5000-5001->5000-5001/tcp, :::5000-5001->5000-5001/tcp   demo-haproxy
    cf69f12dc472   patroni-citus   "/bin/sh /entrypoint…"   4 minutes ago   Up 4 minutes                                                                   demo-work2-1
    ea0e32ca68f5   patroni-citus   "/bin/sh /entrypoint…"   4 minutes ago   Up 4 minutes                                                                   demo-work2-2
    52376efe98d2   patroni-citus   "/bin/sh /entrypoint…"   4 minutes ago   Up 4 minutes                                                                   demo-etcd1


    $ docker logs demo-coord1
    2023-11-15 15:53:01,467 INFO: Selected new etcd server http://172.20.0.5:2379
    2023-11-15 15:53:01,504 INFO: No PostgreSQL configuration items changed, nothing to reload.
    2023-11-15 15:53:01,566 INFO: Lock owner: None; I am coord1
    2023-11-15 15:53:01,609 INFO: waiting for leader to bootstrap
    2023-11-15 15:53:05,160 INFO: Lock owner: coord3; I am coord1
    2023-11-15 15:53:05,160 INFO: trying to bootstrap from leader 'coord3'
    2023-11-15 15:53:05,308 INFO: Lock owner: coord3; I am coord1
    2023-11-15 15:53:05,358 INFO: bootstrap from leader 'coord3' in progress
    2023-11-15 15:53:06,754 INFO: replica has been created using basebackup
    2023-11-15 15:53:06,755 INFO: bootstrapped from leader 'coord3'
    2023-11-15 15:53:08,112 INFO: postmaster pid=47
    2023-11-15 15:53:08.127 UTC [47] LOG:  starting PostgreSQL 15.5 (Debian 15.5-1.pgdg120+1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 12.2.0-14) 12.2.0, 64-bit
    localhost:5432 - no response
    2023-11-15 15:53:08.128 UTC [47] LOG:  listening on IPv4 address "0.0.0.0", port 5432
    2023-11-15 15:53:08.137 UTC [47] LOG:  listening on Unix socket "/var/run/postgresql/.s.PGSQL.5432"
    2023-11-15 15:53:08.147 UTC [51] LOG:  database system was interrupted; last known up at 2023-11-15 15:53:05 UTC
    2023-11-15 15:53:08.264 UTC [51] LOG:  entering standby mode
    2023-11-15 15:53:08.267 UTC [51] LOG:  redo starts at 0/2015BC8
    2023-11-15 15:53:08.268 UTC [51] LOG:  consistent recovery state reached at 0/3000050
    2023-11-15 15:53:08.268 UTC [47] LOG:  database system is ready to accept read-only connections
    2023-11-15 15:53:08.284 UTC [52] LOG:  started streaming WAL from primary at 0/4000000 on timeline 1
    localhost:5432 - accepting connections
    localhost:5432 - accepting connections
    2023-11-15 15:53:09,172 INFO: Lock owner: coord3; I am coord1
    2023-11-15 15:53:09,173 INFO: establishing a new patroni heartbeat connection to postgres
    2023-11-15 15:53:09,264 INFO: no action. I am (coord1), a secondary, and following a leader (coord3)
    2023-11-15 15:53:11,827 INFO: establishing a new patroni restapi connection to postgres
    2023-11-15 15:53:15,398 INFO: no action. I am (coord1), a secondary, and following a leader (coord3)
    2023-11-15 15:53:25,352 INFO: no action. I am (coord1), a secondary, and following a leader (coord3)
    2023-11-15 15:53:35,396 INFO: no action. I am (coord1), a secondary, and following a leader (coord3)
    2023-11-15 15:53:45,355 INFO: no action. I am (coord1), a secondary, and following a leader (coord3)
    2023-11-15 15:53:55,400 INFO: no action. I am (coord1), a secondary, and following a leader (coord3)
    2023-11-15 15:54:05,899 INFO: no action. I am (coord1), a secondary, and following a leader (coord3)
    2023-11-15 15:54:15,356 INFO: no action. I am (coord1), a secondary, and following a leader (coord3)
    2023-11-15 15:54:25,398 INFO: no action. I am (coord1), a secondary, and following a leader (coord3)
    2023-11-15 15:54:35,354 INFO: no action. I am (coord1), a secondary, and following a leader (coord3)


    $ docker exec -ti demo-haproxy bash
    postgres@haproxy:~$ etcdctl member list
    1bab629f01fa9065, started, etcd3, http://etcd3:2380, http://172.20.0.5:2379
    8ecb6af518d241cc, started, etcd2, http://etcd2:2380, http://172.20.0.9:2379
    b2e169fcb8a34028, started, etcd1, http://etcd1:2380, http://172.20.0.3:2379

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
    psql (15.5 (Debian 15.5-1.pgdg120+1))
    SSL connection (protocol: TLSv1.3, cipher: TLS_AES_256_GCM_SHA384, compression: off)
    Type "help" for help.

    citus=# select pg_is_in_recovery();
     pg_is_in_recovery
    -------------------
     f
    (1 row)

    citus=# table pg_dist_node;
     nodeid | groupid |  nodename  | nodeport | noderack | hasmetadata | isactive | noderole | nodecluster | metadatasynced | shouldhaveshards 
    --------+---------+------------+----------+----------+-------------+----------+----------+-------------+----------------+------------------
        1 |       0 | 172.20.0.4 |     5432 | default  | t           | t        | primary  | default     | t              | f
        2 |       2 | 172.20.0.2 |     5432 | default  | t           | t        | primary  | default     | t              | t
        3 |       1 | 172.20.0.8 |     5432 | default  | t           | t        | primary  | default     | t              | t
    (3 rows)


    citus=# \q

    postgres@haproxy:~$ patronictl list
    + Citus cluster: demo ----------+--------------+-----------+----+-----------+
    | Group | Member  | Host        | Role         | State     | TL | Lag in MB |
    +-------+---------+-------------+--------------+-----------+----+-----------+
    |     0 | coord1  | 172.20.0.10 | Sync Standby | streaming |  1 |         0 |
    |     0 | coord2  | 172.20.0.6  | Replica      | streaming |  1 |         0 |
    |     0 | coord3  | 172.20.0.4  | Leader       | running   |  1 |           |
    |     1 | work1-1 | 172.20.0.11 | Sync Standby | streaming |  1 |         0 |
    |     1 | work1-2 | 172.20.0.8  | Leader       | running   |  1 |           |
    |     2 | work2-1 | 172.20.0.7  | Sync Standby | streaming |  1 |         0 |
    |     2 | work2-2 | 172.20.0.2  | Leader       | running   |  1 |           |
    +-------+---------+-------------+--------------+-----------+----+-----------+


    postgres@haproxy:~$ patronictl switchover --group 2 --force
    Current cluster topology
    + Citus cluster: demo (group: 2, 7301717483933835287) -+-----------+
    | Member  | Host       | Role         | State     | TL | Lag in MB |
    +---------+------------+--------------+-----------+----+-----------+
    | work2-1 | 172.20.0.7 | Leader       | running   |  2 |           |
    | work2-2 | 172.20.0.2 | Sync Standby | streaming |  2 |         0 |
    +---------+------------+--------------+-----------+----+-----------+
    2023-11-15 16:06:38.00256 Successfully switched over to "work2-2"
    + Citus cluster: demo (group: 2, 7301717483933835287) ------+
    | Member  | Host       | Role    | State   | TL | Lag in MB |
    +---------+------------+---------+---------+----+-----------+
    | work2-1 | 172.20.0.7 | Replica | stopped |    |   unknown |
    | work2-2 | 172.20.0.2 | Leader  | running |  2 |           |
    +---------+------------+---------+---------+----+-----------+


    postgres@haproxy:~$ patronictl list
    + Citus cluster: demo ----------+--------------+-----------+----+-----------+
    | Group | Member  | Host        | Role         | State     | TL | Lag in MB |
    +-------+---------+-------------+--------------+-----------+----+-----------+
    |     0 | coord1  | 172.20.0.10 | Sync Standby | streaming |  1 |         0 |
    |     0 | coord2  | 172.20.0.6  | Replica      | streaming |  1 |         0 |
    |     0 | coord3  | 172.20.0.4  | Leader       | running   |  1 |           |
    |     1 | work1-1 | 172.20.0.11 | Sync Standby | streaming |  1 |         0 |
    |     1 | work1-2 | 172.20.0.8  | Leader       | running   |  1 |           |
    |     2 | work2-1 | 172.20.0.7  | Sync Standby | streaming |  3 |         0 |
    |     2 | work2-2 | 172.20.0.2  | Leader       | running   |  3 |           |
    +-------+---------+-------------+--------------+-----------+----+-----------+

    postgres@haproxy:~$ psql -h localhost -p 5000 -U postgres -d citus
    psql (15.5 (Debian 15.5-1.pgdg120+1))
    SSL connection (protocol: TLSv1.3, cipher: TLS_AES_256_GCM_SHA384, compression: off)
    Type "help" for help.


    citus=# table pg_dist_node;
     nodeid | groupid |  nodename  | nodeport | noderack | hasmetadata | isactive | noderole | nodecluster | metadatasynced | shouldhaveshards 
    --------+---------+------------+----------+----------+-------------+----------+----------+-------------+----------------+------------------
        1 |       0 | 172.20.0.4 |     5432 | default  | t           | t        | primary  | default     | t              | f
        2 |       2 | 172.20.0.2 |     5432 | default  | t           | t        | primary  | default     | t              | t
        3 |       1 | 172.20.0.8 |     5432 | default  | t           | t        | primary  | default     | t              | t
    (3 rows)
