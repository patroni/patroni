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

    $ docker-compose up -d
    Creating demo-haproxy ...
    Creating demo-patroni2 ...
    Creating demo-patroni1 ...
    Creating demo-patroni3 ...
    Creating demo-etcd2 ...
    Creating demo-etcd1 ...
    Creating demo-etcd3 ...
    Creating demo-haproxy
    Creating demo-patroni2
    Creating demo-patroni1
    Creating demo-patroni3
    Creating demo-etcd1
    Creating demo-etcd2
    Creating demo-etcd2 ... done

    $ docker ps
    CONTAINER ID        IMAGE               COMMAND                  CREATED             STATUS              PORTS                              NAMES
    5b7a90b4cfbf        patroni             "/bin/sh /entrypoint…"   29 seconds ago      Up 27 seconds                                          demo-etcd2
    e30eea5222f2        patroni             "/bin/sh /entrypoint…"   29 seconds ago      Up 27 seconds                                          demo-etcd1
    83bcf3cb208f        patroni             "/bin/sh /entrypoint…"   29 seconds ago      Up 27 seconds                                          demo-etcd3
    922532c56e7d        patroni             "/bin/sh /entrypoint…"   29 seconds ago      Up 28 seconds                                          demo-patroni3
    14f875e445f3        patroni             "/bin/sh /entrypoint…"   29 seconds ago      Up 28 seconds                                          demo-patroni2
    110d1073b383        patroni             "/bin/sh /entrypoint…"   29 seconds ago      Up 28 seconds                                          demo-patroni1
    5af5e6e36028        patroni             "/bin/sh /entrypoint…"   29 seconds ago      Up 28 seconds       0.0.0.0:5000-5001->5000-5001/tcp   demo-haproxy

    $ docker logs demo-patroni1
    2019-02-20 08:19:32,714 INFO: Failed to import patroni.dcs.consul
    2019-02-20 08:19:32,737 INFO: Selected new etcd server http://etcd3:2379
    2019-02-20 08:19:35,140 INFO: Lock owner: None; I am patroni1
    2019-02-20 08:19:35,174 INFO: trying to bootstrap a new cluster
    ...
    2019-02-20 08:19:39,310 INFO: postmaster pid=37
    2019-02-20 08:19:39.314 UTC [37] LOG:  listening on IPv4 address "0.0.0.0", port 5432
    2019-02-20 08:19:39.321 UTC [37] LOG:  listening on Unix socket "/var/run/postgresql/.s.PGSQL.5432"
    2019-02-20 08:19:39.353 UTC [39] LOG:  database system was shut down at 2019-02-20 08:19:36 UTC
    2019-02-20 08:19:39.354 UTC [40] FATAL:  the database system is starting up
    localhost:5432 - rejecting connections
    2019-02-20 08:19:39.369 UTC [37] LOG:  database system is ready to accept connections
    localhost:5432 - accepting connections
    2019-02-20 08:19:39,383 INFO: establishing a new patroni connection to the postgres cluster
    2019-02-20 08:19:39,408 INFO: running post_bootstrap
    2019-02-20 08:19:39,432 WARNING: Could not activate Linux watchdog device: "Can't open watchdog device: [Errno 2] No such file or directory: '/dev/watchdog'"
    2019-02-20 08:19:39,515 INFO: initialized a new cluster
    2019-02-20 08:19:49,424 INFO: Lock owner: patroni1; I am patroni1
    2019-02-20 08:19:49,447 INFO: Lock owner: patroni1; I am patroni1
    2019-02-20 08:19:49,480 INFO: no action.  i am the leader with the lock
    2019-02-20 08:19:59,422 INFO: Lock owner: patroni1; I am patroni1

    $ docker exec -ti demo-patroni1 bash
    postgres@patroni1:~$ patronictl list
    +---------+----------+------------+--------+---------+----+-----------+
    | Cluster |  Member  |    Host    |  Role  |  State  | TL | Lag in MB |
    +---------+----------+------------+--------+---------+----+-----------+
    |   demo  | patroni1 | 172.22.0.3 | Leader | running |  1 |         0 |
    |   demo  | patroni2 | 172.22.0.7 |        | running |  1 |         0 |
    |   demo  | patroni3 | 172.22.0.4 |        | running |  1 |         0 |
    +---------+----------+------------+--------+---------+----+-----------+

    postgres@patroni1:~$ etcdctl get --keys-only --prefix /service/demo
    /service/demo/config
    /service/demo/initialize
    /service/demo/leader
    /service/demo/members/
    /service/demo/members/patroni1
    /service/demo/members/patroni2
    /service/demo/members/patroni3
    /service/demo/optime/
    /service/demo/optime/leader

    postgres@patroni1:~$ etcdctl member list
    1bab629f01fa9065: name=etcd3 peerURLs=http://etcd3:2380 clientURLs=http://etcd3:2379 isLeader=false
    8ecb6af518d241cc: name=etcd2 peerURLs=http://etcd2:2380 clientURLs=http://etcd2:2379 isLeader=true
    b2e169fcb8a34028: name=etcd1 peerURLs=http://etcd1:2380 clientURLs=http://etcd1:2379 isLeader=false
    postgres@patroni1:~$ exit

    $ docker exec -ti demo-haproxy bash
    postgres@haproxy:~$ psql -h localhost -p 5000 -U postgres -W
    Password: postgres
    psql (11.2 (Ubuntu 11.2-1.pgdg18.04+1), server 10.7 (Debian 10.7-1.pgdg90+1))
    Type "help" for help.

    localhost/postgres=# select pg_is_in_recovery();
     pg_is_in_recovery
    ───────────────────
     f
    (1 row)

    localhost/postgres=# \q

    $postgres@haproxy:~ psql -h localhost -p 5001 -U postgres -W
    Password: postgres
    psql (11.2 (Ubuntu 11.2-1.pgdg18.04+1), server 10.7 (Debian 10.7-1.pgdg90+1))
    Type "help" for help.

    localhost/postgres=# select pg_is_in_recovery();
     pg_is_in_recovery
    ───────────────────
     t
    (1 row)

## Citus cluster

The stack starts three containers with etcd (forming a three-node etcd cluster), seven containers with Patroni+PostgreSQL+Citus (three coordinator nodes, and two worker clusters with two nodes each), and one container with haproxy.
The haproxy listens on ports 5000 (connects to the coordinator primary) and 5001 (does load-balancing between worker primary nodes).

Example session:

    $ docker-compose -f docker-compose-citus.yml up -d
    Creating demo-work2-1 ... done
    Creating demo-work1-1 ... done
    Creating demo-etcd2   ... done
    Creating demo-etcd1   ... done
    Creating demo-coord3  ... done
    Creating demo-etcd3   ... done
    Creating demo-coord1  ... done
    Creating demo-haproxy ... done
    Creating demo-work2-2 ... done
    Creating demo-coord2  ... done
    Creating demo-work1-2 ... done

    $ docker ps
    CONTAINER ID   IMAGE                  COMMAND                  CREATED         STATUS         PORTS                              NAMES
    852d8885a612   patroni-citus          "/bin/sh /entrypoint…"   6 seconds ago   Up 3 seconds                                      demo-coord3
    cdd692f947ab   patroni-citus          "/bin/sh /entrypoint…"   6 seconds ago   Up 3 seconds                                      demo-work1-2
    9f4e340b36da   patroni-citus          "/bin/sh /entrypoint…"   6 seconds ago   Up 3 seconds                                      demo-etcd3
    d69c129a960a   patroni-citus          "/bin/sh /entrypoint…"   6 seconds ago   Up 4 seconds                                      demo-etcd1
    c5849689b8cd   patroni-citus          "/bin/sh /entrypoint…"   6 seconds ago   Up 4 seconds                                      demo-coord1
    c9d72bd6217d   patroni-citus          "/bin/sh /entrypoint…"   6 seconds ago   Up 3 seconds                                      demo-work2-1
    24b1b43efa05   patroni-citus          "/bin/sh /entrypoint…"   6 seconds ago   Up 4 seconds                                      demo-coord2
    cb0cc2b4ca0a   patroni-citus          "/bin/sh /entrypoint…"   6 seconds ago   Up 3 seconds                                      demo-work2-2
    9796c6b8aad5   patroni-citus          "/bin/sh /entrypoint…"   6 seconds ago   Up 5 seconds                                      demo-work1-1
    8baccd74dcae   patroni-citus          "/bin/sh /entrypoint…"   6 seconds ago   Up 4 seconds                                      demo-etcd2
    353ec62a0187   patroni-citus          "/bin/sh /entrypoint…"   6 seconds ago   Up 4 seconds   0.0.0.0:5000-5001->5000-5001/tcp   demo-haproxy

    $ docker logs demo-coord1
    2023-01-05 15:09:31,295 INFO: Selected new etcd server http://172.27.0.4:2379
    2023-01-05 15:09:31,388 INFO: Lock owner: None; I am coord1
    2023-01-05 15:09:31,501 INFO: trying to bootstrap a new cluster
    ...
    2023-01-05 15:09:45,096 INFO: postmaster pid=39
    localhost:5432 - no response
    2023-01-05 15:09:45.137 UTC [39] LOG:  starting PostgreSQL 15.1 (Debian 15.1-1.pgdg110+1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 10.2.1-6) 10.2.1 20210110, 64-bit
    2023-01-05 15:09:45.137 UTC [39] LOG:  listening on IPv4 address "0.0.0.0", port 5432
    2023-01-05 15:09:45.152 UTC [39] LOG:  listening on Unix socket "/var/run/postgresql/.s.PGSQL.5432"
    2023-01-05 15:09:45.177 UTC [43] LOG:  database system was shut down at 2023-01-05 15:09:32 UTC
    2023-01-05 15:09:45.193 UTC [39] LOG:  database system is ready to accept connections
    localhost:5432 - accepting connections
    localhost:5432 - accepting connections
    2023-01-05 15:09:46,139 INFO: establishing a new patroni connection to the postgres cluster
    2023-01-05 15:09:46,208 INFO: running post_bootstrap
    2023-01-05 15:09:47.209 UTC [55] LOG:  starting maintenance daemon on database 16386 user 10
    2023-01-05 15:09:47.209 UTC [55] CONTEXT:  Citus maintenance daemon for database 16386 user 10
    2023-01-05 15:09:47,215 WARNING: Could not activate Linux watchdog device: "Can't open watchdog device: [Errno 2] No such file or directory: '/dev/watchdog'"
    2023-01-05 15:09:47.446 UTC [41] LOG:  checkpoint starting: immediate force wait
    2023-01-05 15:09:47,466 INFO: initialized a new cluster
    2023-01-05 15:09:47,594 DEBUG: query(SELECT nodeid, groupid, nodename, nodeport, noderole FROM pg_catalog.pg_dist_node WHERE noderole = 'primary', ())
    2023-01-05 15:09:47,594 INFO: establishing a new patroni connection to the postgres cluster
    2023-01-05 15:09:47,467 INFO: Lock owner: coord1; I am coord1
    2023-01-05 15:09:47,613 DEBUG: query(SELECT pg_catalog.citus_set_coordinator_host(%s, %s, 'primary', 'default'), ('172.27.0.6', 5432))
    2023-01-05 15:09:47,924 INFO: no action. I am (coord1), the leader with the lock
    2023-01-05 15:09:51.282 UTC [41] LOG:  checkpoint complete: wrote 1086 buffers (53.0%); 0 WAL file(s) added, 0 removed, 0 recycled; write=0.029 s, sync=3.746 s, total=3.837 s; sync files=280, longest=0.028 s, average=0.014 s; distance=8965 kB, estimate=8965 kB
    2023-01-05 15:09:51.283 UTC [41] LOG:  checkpoint starting: immediate force wait
    2023-01-05 15:09:51.495 UTC [41] LOG:  checkpoint complete: wrote 18 buffers (0.9%); 0 WAL file(s) added, 0 removed, 0 recycled; write=0.044 s, sync=0.091 s, total=0.212 s; sync files=15, longest=0.015 s, average=0.007 s; distance=67 kB, estimate=8076 kB
    2023-01-05 15:09:57,467 INFO: Lock owner: coord1; I am coord1
    2023-01-05 15:09:57,569 INFO: Assigning synchronous standby status to ['coord3']
    server signaled
    2023-01-05 15:09:57.574 UTC [39] LOG:  received SIGHUP, reloading configuration files
    2023-01-05 15:09:57.580 UTC [39] LOG:  parameter "synchronous_standby_names" changed to "coord3"
    2023-01-05 15:09:59,637 INFO: Synchronous standby status assigned to ['coord3']
    2023-01-05 15:09:59,638 DEBUG: query(SELECT pg_catalog.citus_add_node(%s, %s, %s, 'primary', 'default'), ('172.27.0.2', 5432, 1))
    2023-01-05 15:09:59.690 UTC [67] LOG:  standby "coord3" is now a synchronous standby with priority 1
    2023-01-05 15:09:59.690 UTC [67] STATEMENT:  START_REPLICATION SLOT "coord3" 0/3000000 TIMELINE 1
    2023-01-05 15:09:59,694 INFO: no action. I am (coord1), the leader with the lock
    2023-01-05 15:09:59,704 DEBUG: query(SELECT pg_catalog.citus_add_node(%s, %s, %s, 'primary', 'default'), ('172.27.0.8', 5432, 2))
    2023-01-05 15:10:07,625 INFO: no action. I am (coord1), the leader with the lock
    2023-01-05 15:10:17,579 INFO: no action. I am (coord1), the leader with the lock

    $ docker exec -ti demo-haproxy bash
    postgres@haproxy:~$ etcdctl member list
    1bab629f01fa9065, started, etcd3, http://etcd3:2380, http://172.27.0.10:2379
    8ecb6af518d241cc, started, etcd2, http://etcd2:2380, http://172.27.0.4:2379
    b2e169fcb8a34028, started, etcd1, http://etcd1:2380, http://172.27.0.7:2379

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
    psql (15.1 (Debian 15.1-1.pgdg110+1))
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
          1 |       0 | 172.27.0.6 |     5432 | default  | t           | t        | primary  | default     | t              | f
          2 |       1 | 172.27.0.2 |     5432 | default  | t           | t        | primary  | default     | t              | t
          3 |       2 | 172.27.0.8 |     5432 | default  | t           | t        | primary  | default     | t              | t
    (3 rows)

    citus=# \q

    postgres@haproxy:~$ patronictl list
    + Citus cluster: demo ----------+--------------+---------+----+-----------+
    | Group | Member  | Host        | Role         | State   | TL | Lag in MB |
    +-------+---------+-------------+--------------+---------+----+-----------+
    |     0 | coord1  | 172.27.0.6  | Leader       | running |  1 |           |
    |     0 | coord2  | 172.27.0.5  | Replica      | running |  1 |         0 |
    |     0 | coord3  | 172.27.0.9  | Sync Standby | running |  1 |         0 |
    |     1 | work1-1 | 172.27.0.2  | Leader       | running |  1 |           |
    |     1 | work1-2 | 172.27.0.12 | Sync Standby | running |  1 |         0 |
    |     2 | work2-1 | 172.27.0.11 | Sync Standby | running |  1 |         0 |
    |     2 | work2-2 | 172.27.0.8  | Leader       | running |  1 |           |
    +-------+---------+-------------+--------------+---------+----+-----------+

    postgres@haproxy:~$ patronictl switchover --group 2 --force
    Current cluster topology
    + Citus cluster: demo (group: 2, 7185185529556963355) +-----------+
    | Member  | Host        | Role         | State   | TL | Lag in MB |
    +---------+-------------+--------------+---------+----+-----------+
    | work2-1 | 172.27.0.11 | Sync Standby | running |  1 |         0 |
    | work2-2 | 172.27.0.8  | Leader       | running |  1 |           |
    +---------+-------------+--------------+---------+----+-----------+
    2023-01-05 15:29:29.54204 Successfully switched over to "work2-1"
    + Citus cluster: demo (group: 2, 7185185529556963355) -------+
    | Member  | Host        | Role    | State   | TL | Lag in MB |
    +---------+-------------+---------+---------+----+-----------+
    | work2-1 | 172.27.0.11 | Leader  | running |  1 |           |
    | work2-2 | 172.27.0.8  | Replica | stopped |    |   unknown |
    +---------+-------------+---------+---------+----+-----------+

    postgres@haproxy:~$ patronictl list
    + Citus cluster: demo ----------+--------------+---------+----+-----------+
    | Group | Member  | Host        | Role         | State   | TL | Lag in MB |
    +-------+---------+-------------+--------------+---------+----+-----------+
    |     0 | coord1  | 172.27.0.6  | Leader       | running |  1 |           |
    |     0 | coord2  | 172.27.0.5  | Replica      | running |  1 |         0 |
    |     0 | coord3  | 172.27.0.9  | Sync Standby | running |  1 |         0 |
    |     1 | work1-1 | 172.27.0.2  | Leader       | running |  1 |           |
    |     1 | work1-2 | 172.27.0.12 | Sync Standby | running |  1 |         0 |
    |     2 | work2-1 | 172.27.0.11 | Leader       | running |  2 |           |
    |     2 | work2-2 | 172.27.0.8  | Sync Standby | running |  2 |         0 |
    +-------+---------+-------------+--------------+---------+----+-----------+

    postgres@haproxy:~$ psql -h localhost -p 5000 -U postgres -d citus
    Password for user postgres: postgres
    psql (15.1 (Debian 15.1-1.pgdg110+1))
    SSL connection (protocol: TLSv1.3, cipher: TLS_AES_256_GCM_SHA384, compression: off)
    Type "help" for help.

    citus=# table pg_dist_node;
     nodeid | groupid |  nodename   | nodeport | noderack | hasmetadata | isactive | noderole | nodecluster | metadatasynced | shouldhaveshards
    --------+---------+-------------+----------+----------+-------------+----------+----------+-------------+----------------+------------------
          1 |       0 | 172.27.0.6  |     5432 | default  | t           | t        | primary  | default     | t              | f
          3 |       2 | 172.27.0.11 |     5432 | default  | t           | t        | primary  | default     | t              | t
          2 |       1 | 172.27.0.2  |     5432 | default  | t           | t        | primary  | default     | t              | t
    (3 rows)
