# Patroni Dockerfile
You can run Patroni in a docker container using this Dockerfile

This Dockerfile is meant in aiding development of Patroni and quick testing of features. It is not a production-worthy
Dockerfile

    docker build -t patroni .

# Examples

## Standalone Patroni

    docker run -d patroni

## Three-node Patroni cluster with three-node etcd cluster and one haproxy container using docker-compose

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

    postgres@patroni1:~$ etcdctl ls --recursive --sort -p /service/demo
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

    $ psql -h localhost -p 5000 -U postgres -W
    Password: postgres
    psql (11.2 (Ubuntu 11.2-1.pgdg18.04+1), server 10.7 (Debian 10.7-1.pgdg90+1))
    Type "help" for help.

    localhost/postgres=# select pg_is_in_recovery();
     pg_is_in_recovery
    ───────────────────
     f
    (1 row)

    localhost/postgres=# \q

    $ psql -h localhost -p 5001 -U postgres -W
    Password: postgres
    psql (11.2 (Ubuntu 11.2-1.pgdg18.04+1), server 10.7 (Debian 10.7-1.pgdg90+1))
    Type "help" for help.

    localhost/postgres=# select pg_is_in_recovery();
     pg_is_in_recovery
    ───────────────────
     t
    (1 row)
