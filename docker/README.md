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
    2023-11-21 09:04:33,497 ERROR: Failed to get list of machines from http://etcd1:2379/v3beta: ValueError("invalid literal for int() with base 10: 'not_decided'")
    2023-11-21 09:04:33,501 ERROR: Failed to get list of machines from http://etcd3:2379/v3beta: ValueError("invalid literal for int() with base 10: 'not_decided'")
    2023-11-21 09:04:33,547 INFO: Selected new etcd server http://172.29.0.3:2379
    2023-11-21 09:04:33,560 INFO: No PostgreSQL configuration items changed, nothing to reload.
    2023-11-21 09:04:33,605 INFO: Lock owner: None; I am patroni1
    2023-11-21 09:04:33,693 INFO: trying to bootstrap a new cluster
    The files belonging to this database system will be owned by user "postgres".
    This user must also own the server process.

    The database cluster will be initialized with locale "en_US.UTF-8".
    The default text search configuration will be set to "english".

    Data page checksums are enabled.

    creating directory /home/postgres/data ... ok
    creating subdirectories ... ok
    selecting dynamic shared memory implementation ... posix
    selecting default max_connections ... 100
    selecting default shared_buffers ... 128MB
    selecting default time zone ... Etc/UTC
    creating configuration files ... ok
    running bootstrap script ... ok
    performing post-bootstrap initialization ... ok
    initdb: warning: enabling "trust" authentication for local connections
    initdb: hint: You can change this by editing pg_hba.conf or using the option -A, or --auth-local and --auth-host, the next time you run initdb.
    syncing data to disk ... ok


    Success. You can now start the database server using:

        pg_ctl -D /home/postgres/data -l logfile start

    2023-11-21 09:04:34.920 UTC [43] LOG:  starting PostgreSQL 15.5 (Debian 15.5-1.pgdg120+1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 12.2.0-14) 12.2.0, 64-bit
    2023-11-21 09:04:34.921 UTC [43] LOG:  listening on IPv4 address "0.0.0.0", port 5432
    2023-11-21 09:04:34,922 INFO: postmaster pid=43
    2023-11-21 09:04:34.922 UTC [43] LOG:  listening on Unix socket "/var/run/postgresql/.s.PGSQL.5432"
    2023-11-21 09:04:34.925 UTC [47] LOG:  database system was shut down at 2023-11-21 09:04:34 UTC
    2023-11-21 09:04:34.928 UTC [43] LOG:  database system is ready to accept connections
    localhost:5432 - accepting connections
    localhost:5432 - accepting connections
    2023-11-21 09:04:34,938 INFO: establishing a new patroni heartbeat connection to postgres
    2023-11-21 09:04:34,992 INFO: running post_bootstrap
    2023-11-21 09:04:35,004 WARNING: User creation via "bootstrap.users" will be removed in v4.0.0
    2023-11-21 09:04:35,009 WARNING: Could not activate Linux watchdog device: Can't open watchdog device: [Errno 2] No such file or directory: '/dev/watchdog'
    2023-11-21 09:04:35,010 INFO: Reaped pid=56, exit status=0
    2023-11-21 09:04:35,011 INFO: Reaped pid=57, exit status=0
    2023-11-21 09:04:35.164 UTC [45] LOG:  checkpoint starting: force wait
    2023-11-21 09:04:35,189 INFO: initialized a new cluster
    2023-11-21 09:04:35,328 INFO: no action. I am (patroni1), the leader with the lock
    2023-11-21 09:04:35.587 UTC [45] LOG:  checkpoint complete: wrote 7 buffers (0.0%); 0 WAL file(s) added, 0 removed, 0 recycled; write=0.402 s, sync=0.002 s, total=0.423 s; sync files=6, longest=0.001 s, average=0.001 s; distance=8951 kB, estimate=8951 kB
    2023-11-21 09:04:35.587 UTC [45] LOG:  checkpoint starting: force wait
    2023-11-21 09:04:35.589 UTC [45] LOG:  checkpoint complete: wrote 0 buffers (0.0%); 0 WAL file(s) added, 0 removed, 0 recycled; write=0.001 s, sync=0.001 s, total=0.003 s; sync files=0, longest=0.000 s, average=0.000 s; distance=0 kB, estimate=8056 kB
    2023-11-21 09:04:43,824 INFO: establishing a new patroni restapi connection to postgres
    2023-11-21 09:04:45,322 INFO: no action. I am (patroni1), the leader with the lock
    2023-11-21 09:04:55,320 INFO: no action. I am (patroni1), the leader with the lock
    2023-11-21 09:05:05,233 INFO: no action. I am (patroni1), the leader with the lock
    2023-11-21 09:05:15,232 INFO: no action. I am (patroni1), the leader with the lock
    2023-11-21 09:05:25,233 INFO: no action. I am (patroni1), the leader with the lock
    2023-11-21 09:05:35,233 INFO: no action. I am (patroni1), the leader with the lock
    2023-11-21 09:05:45,233 INFO: no action. I am (patroni1), the leader with the lock
    2023-11-21 09:05:55,234 INFO: no action. I am (patroni1), the leader with the lock
    2023-11-21 09:06:05,320 INFO: no action. I am (patroni1), the leader with the lock
    2023-11-21 09:06:15,232 INFO: no action. I am (patroni1), the leader with the lock
    2023-11-21 09:06:25,324 INFO: no action. I am (patroni1), the leader with the lock


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
    2023-11-21 09:36:14,244 ERROR: Failed to get list of machines from http://etcd3:2379/v3beta: ValueError("invalid literal for int() with base 10: 'not_decided'")
    2023-11-21 09:36:14,251 ERROR: Failed to get list of machines from http://etcd1:2379/v3beta: ValueError("invalid literal for int() with base 10: 'not_decided'")
    2023-11-21 09:36:14,293 INFO: Selected new etcd server http://172.30.0.4:2379
    2023-11-21 09:36:14,316 INFO: No PostgreSQL configuration items changed, nothing to reload.
    2023-11-21 09:36:14,390 INFO: Lock owner: None; I am coord1
    2023-11-21 09:36:14,478 INFO: trying to bootstrap a new cluster
    The files belonging to this database system will be owned by user "postgres".
    This user must also own the server process.

    The database cluster will be initialized with locale "en_US.UTF-8".
    The default text search configuration will be set to "english".

    Data page checksums are enabled.

    creating directory /home/postgres/data ... ok
    creating subdirectories ... ok
    selecting dynamic shared memory implementation ... posix
    selecting default max_connections ... 100
    selecting default shared_buffers ... 128MB
    selecting default time zone ... Etc/UTC
    creating configuration files ... ok
    running bootstrap script ... ok
    performing post-bootstrap initialization ... ok
    syncing data to disk ... ok

    initdb: warning: enabling "trust" authentication for local connections
    initdb: hint: You can change this by editing pg_hba.conf or using the option -A, or --auth-local and --auth-host, the next time you run initdb.

    Success. You can now start the database server using:

        pg_ctl -D /home/postgres/data -l logfile start

    2023-11-21 09:36:16,475 INFO: postmaster pid=52
    localhost:5432 - no response
    2023-11-21 09:36:16.495 UTC [52] LOG:  starting PostgreSQL 15.5 (Debian 15.5-1.pgdg120+1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 12.2.0-14) 12.2.0, 64-bit
    2023-11-21 09:36:16.495 UTC [52] LOG:  listening on IPv4 address "0.0.0.0", port 5432
    2023-11-21 09:36:16.496 UTC [52] LOG:  listening on Unix socket "/var/run/postgresql/.s.PGSQL.5432"
    2023-11-21 09:36:16.498 UTC [56] LOG:  database system was shut down at 2023-11-21 09:36:15 UTC
    2023-11-21 09:36:16.501 UTC [52] LOG:  database system is ready to accept connections
    localhost:5432 - accepting connections
    localhost:5432 - accepting connections
    2023-11-21 09:36:17,509 INFO: establishing a new patroni heartbeat connection to postgres
    2023-11-21 09:36:17,569 INFO: running post_bootstrap
    2023-11-21 09:36:17,593 WARNING: User creation via "bootstrap.users" will be removed in v4.0.0
    2023-11-21 09:36:17,783 INFO: establishing a new patroni restapi connection to postgres
    2023-11-21 09:36:17,969 WARNING: Could not activate Linux watchdog device: Can't open watchdog device: [Errno 2] No such file or directory: '/dev/watchdog'
    2023-11-21 09:36:17.969 UTC [70] LOG:  starting maintenance daemon on database 16386 user 10
    2023-11-21 09:36:17.969 UTC [70] CONTEXT:  Citus maintenance daemon for database 16386 user 10
    2023-11-21 09:36:18.159 UTC [54] LOG:  checkpoint starting: immediate force wait
    2023-11-21 09:36:18,162 INFO: initialized a new cluster
    2023-11-21 09:36:18,164 INFO: Lock owner: coord1; I am coord1
    2023-11-21 09:36:18,297 INFO: Enabled synchronous replication
    2023-11-21 09:36:18,298 DEBUG: Adding the new task: PgDistNode(nodeid=None,group=0,host=172.30.0.3,port=5432,event=after_promote)
    2023-11-21 09:36:18,298 DEBUG: Adding the new task: PgDistNode(nodeid=None,group=1,host=172.30.0.7,port=5432,event=after_promote)
    2023-11-21 09:36:18,298 DEBUG: Adding the new task: PgDistNode(nodeid=None,group=2,host=172.30.0.8,port=5432,event=after_promote)
    2023-11-21 09:36:18,299 DEBUG: query(SELECT nodeid, groupid, nodename, nodeport, noderole FROM pg_catalog.pg_dist_node WHERE noderole = 'primary', ())
    2023-11-21 09:36:18,299 INFO: establishing a new patroni citus connection to postgres
    2023-11-21 09:36:18,323 DEBUG: query(SELECT pg_catalog.citus_add_node(%s, %s, %s, 'primary', 'default'), ('172.30.0.7', 5432, 1))
    2023-11-21 09:36:18,361 INFO: no action. I am (coord1), the leader with the lock
    2023-11-21 09:36:18,393 DEBUG: query(SELECT pg_catalog.citus_add_node(%s, %s, %s, 'primary', 'default'), ('172.30.0.8', 5432, 2))
    2023-11-21 09:36:18.429 UTC [54] LOG:  checkpoint complete: wrote 1000 buffers (48.8%); 0 WAL file(s) added, 0 removed, 0 recycled; write=0.026 s, sync=0.241 s, total=0.270 s; sync files=402, longest=0.010 s, average=0.001 s; distance=8951 kB, estimate=8951 kB
    2023-11-21 09:36:18.435 UTC [54] LOG:  checkpoint starting: immediate force wait
    2023-11-21 09:36:18.484 UTC [54] LOG:  checkpoint complete: wrote 22 buffers (1.1%); 0 WAL file(s) added, 0 removed, 0 recycled; write=0.010 s, sync=0.024 s, total=0.049 s; sync files=20, longest=0.009 s, average=0.002 s; distance=70 kB, estimate=8063 kB
    [root@Kube-Worker1 Patroni]# docker logs -f demo-coord1
    [root@Kube-Worker1 Patroni]# docker logs -f demo-coord1
    2023-11-21 09:36:14,244 ERROR: Failed to get list of machines from http://etcd3:2379/v3beta: ValueError("invalid literal for int() with base 10: 'not_decided'")
    2023-11-21 09:36:14,251 ERROR: Failed to get list of machines from http://etcd1:2379/v3beta: ValueError("invalid literal for int() with base 10: 'not_decided'")
    2023-11-21 09:36:14,293 INFO: Selected new etcd server http://172.30.0.4:2379
    2023-11-21 09:36:14,316 INFO: No PostgreSQL configuration items changed, nothing to reload.
    2023-11-21 09:36:14,390 INFO: Lock owner: None; I am coord1
    2023-11-21 09:36:14,478 INFO: trying to bootstrap a new cluster
    The files belonging to this database system will be owned by user "postgres".
    This user must also own the server process.

    The database cluster will be initialized with locale "en_US.UTF-8".
    The default text search configuration will be set to "english".

    Data page checksums are enabled.

    creating directory /home/postgres/data ... ok
    creating subdirectories ... ok
    selecting dynamic shared memory implementation ... posix
    selecting default max_connections ... 100
    selecting default shared_buffers ... 128MB
    selecting default time zone ... Etc/UTC
    creating configuration files ... ok
    running bootstrap script ... ok
    performing post-bootstrap initialization ... ok
    syncing data to disk ... ok

    initdb: warning: enabling "trust" authentication for local connections
    initdb: hint: You can change this by editing pg_hba.conf or using the option -A, or --auth-local and --auth-host, the next time you run initdb.

    Success. You can now start the database server using:

        pg_ctl -D /home/postgres/data -l logfile start

    2023-11-21 09:36:16,475 INFO: postmaster pid=52
    localhost:5432 - no response
    2023-11-21 09:36:16.495 UTC [52] LOG:  starting PostgreSQL 15.5 (Debian 15.5-1.pgdg120+1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 12.2.0-14) 12.2.0, 64-bit
    2023-11-21 09:36:16.495 UTC [52] LOG:  listening on IPv4 address "0.0.0.0", port 5432
    2023-11-21 09:36:16.496 UTC [52] LOG:  listening on Unix socket "/var/run/postgresql/.s.PGSQL.5432"
    2023-11-21 09:36:16.498 UTC [56] LOG:  database system was shut down at 2023-11-21 09:36:15 UTC
    2023-11-21 09:36:16.501 UTC [52] LOG:  database system is ready to accept connections
    localhost:5432 - accepting connections
    localhost:5432 - accepting connections
    2023-11-21 09:36:17,509 INFO: establishing a new patroni heartbeat connection to postgres
    2023-11-21 09:36:17,569 INFO: running post_bootstrap
    2023-11-21 09:36:17,593 WARNING: User creation via "bootstrap.users" will be removed in v4.0.0
    2023-11-21 09:36:17,783 INFO: establishing a new patroni restapi connection to postgres
    2023-11-21 09:36:17,969 WARNING: Could not activate Linux watchdog device: Can't open watchdog device: [Errno 2] No such file or directory: '/dev/watchdog'
    2023-11-21 09:36:17.969 UTC [70] LOG:  starting maintenance daemon on database 16386 user 10
    2023-11-21 09:36:17.969 UTC [70] CONTEXT:  Citus maintenance daemon for database 16386 user 10
    2023-11-21 09:36:18.159 UTC [54] LOG:  checkpoint starting: immediate force wait
    2023-11-21 09:36:18,162 INFO: initialized a new cluster
    2023-11-21 09:36:18,164 INFO: Lock owner: coord1; I am coord1
    2023-11-21 09:36:18,297 INFO: Enabled synchronous replication
    2023-11-21 09:36:18,298 DEBUG: Adding the new task: PgDistNode(nodeid=None,group=0,host=172.30.0.3,port=5432,event=after_promote)
    2023-11-21 09:36:18,298 DEBUG: Adding the new task: PgDistNode(nodeid=None,group=1,host=172.30.0.7,port=5432,event=after_promote)
    2023-11-21 09:36:18,298 DEBUG: Adding the new task: PgDistNode(nodeid=None,group=2,host=172.30.0.8,port=5432,event=after_promote)
    2023-11-21 09:36:18,299 DEBUG: query(SELECT nodeid, groupid, nodename, nodeport, noderole FROM pg_catalog.pg_dist_node WHERE noderole = 'primary', ())
    2023-11-21 09:36:18,299 INFO: establishing a new patroni citus connection to postgres
    2023-11-21 09:36:18,323 DEBUG: query(SELECT pg_catalog.citus_add_node(%s, %s, %s, 'primary', 'default'), ('172.30.0.7', 5432, 1))
    2023-11-21 09:36:18,361 INFO: no action. I am (coord1), the leader with the lock
    2023-11-21 09:36:18,393 DEBUG: query(SELECT pg_catalog.citus_add_node(%s, %s, %s, 'primary', 'default'), ('172.30.0.8', 5432, 2))
    2023-11-21 09:36:18.429 UTC [54] LOG:  checkpoint complete: wrote 1000 buffers (48.8%); 0 WAL file(s) added, 0 removed, 0 recycled; write=0.026 s, sync=0.241 s, total=0.270 s; sync files=402, longest=0.010 s, average=0.001 s; distance=8951 kB, estimate=8951 kB
    2023-11-21 09:36:18.435 UTC [54] LOG:  checkpoint starting: immediate force wait
    2023-11-21 09:36:18.484 UTC [54] LOG:  checkpoint complete: wrote 22 buffers (1.1%); 0 WAL file(s) added, 0 removed, 0 recycled; write=0.010 s, sync=0.024 s, total=0.049 s; sync files=20, longest=0.009 s, average=0.002 s; distance=70 kB, estimate=8063 kB
    2023-11-21 09:36:28,164 INFO: Lock owner: coord1; I am coord1
    2023-11-21 09:36:28,251 INFO: Assigning synchronous standby status to ['coord3']
    server signaled
    2023-11-21 09:36:28.435 UTC [52] LOG:  received SIGHUP, reloading configuration files
    2023-11-21 09:36:28.436 UTC [52] LOG:  parameter "synchronous_standby_names" changed to "coord3"
    2023-11-21 09:36:28.641 UTC [83] LOG:  standby "coord3" is now a synchronous standby with priority 1
    2023-11-21 09:36:28.641 UTC [83] STATEMENT:  START_REPLICATION SLOT "coord3" 0/3000000 TIMELINE 1
    2023-11-21 09:36:30,582 INFO: Synchronous standby status assigned to ['coord3']
    2023-11-21 09:36:30,626 INFO: no action. I am (coord1), the leader with the lock
    2023-11-21 09:36:38,250 INFO: no action. I am (coord1), the leader with the lock
    2023-11-21 09:37:08,292 INFO: no action. I am (coord1), the leader with the lock
    2023-11-21 09:37:18,206 INFO: no action. I am (coord1), the leader with the lock
    2023-11-21 09:37:28,294 INFO: no action. I am (coord1), the leader with the lock
    2023-11-21 09:37:38,292 INFO: no action. I am (coord1), the leader with the lock
    2023-11-21 09:37:48,292 INFO: no action. I am (coord1), the leader with the lock
    2023-11-21 09:37:58,292 INFO: no action. I am (coord1), the leader with the lock




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
        1 |       0 | 172.30.0.3 |     5432 | default  | t           | t        | primary  | default     | t              | f
        2 |       1 | 172.30.0.7 |     5432 | default  | t           | t        | primary  | default     | t              | t
        3 |       2 | 172.30.0.8 |     5432 | default  | t           | t        | primary  | default     | t              | t
    (3 rows)



    citus=# \q

    postgres@haproxy:~$ patronictl list
    + Citus cluster: demo ----------+--------------+-----------+----+-----------+
    | Group | Member  | Host        | Role         | State     | TL | Lag in MB |
    +-------+---------+-------------+--------------+-----------+----+-----------+
    |     0 | coord1  | 172.30.0.3  | Leader       | running   |  1 |           |
    |     0 | coord2  | 172.30.0.12 | Replica      | streaming |  1 |         0 |
    |     0 | coord3  | 172.30.0.2  | Sync Standby | streaming |  1 |         0 |
    |     1 | work1-1 | 172.30.0.7  | Leader       | running   |  1 |           |
    |     1 | work1-2 | 172.30.0.10 | Sync Standby | streaming |  1 |         0 |
    |     2 | work2-1 | 172.30.0.8  | Leader       | running   |  1 |           |
    |     2 | work2-2 | 172.30.0.11 | Sync Standby | streaming |  1 |         0 |
    +-------+---------+-------------+--------------+-----------+----+-----------+


    postgres@haproxy:~$ patronictl switchover --group 2 --force
    Current cluster topology
    + Citus cluster: demo (group: 2, 7303846899271086103) --+-----------+
    | Member  | Host        | Role         | State     | TL | Lag in MB |
    +---------+-------------+--------------+-----------+----+-----------+
    | work2-1 | 172.30.0.8  | Leader       | running   |  1 |           |
    | work2-2 | 172.30.0.11 | Sync Standby | streaming |  1 |         0 |
    +---------+-------------+--------------+-----------+----+-----------+
    2023-11-21 09:44:15.83849 Successfully switched over to "work2-2"
    + Citus cluster: demo (group: 2, 7303846899271086103) -------+
    | Member  | Host        | Role    | State   | TL | Lag in MB |
    +---------+-------------+---------+---------+----+-----------+
    | work2-1 | 172.30.0.8  | Replica | stopped |    |   unknown |
    | work2-2 | 172.30.0.11 | Leader  | running |  1 |           |
    +---------+-------------+---------+---------+----+-----------+



    postgres@haproxy:~$ patronictl list
    + Citus cluster: demo ----------+--------------+-----------+----+-----------+
    | Group | Member  | Host        | Role         | State     | TL | Lag in MB |
    +-------+---------+-------------+--------------+-----------+----+-----------+
    |     0 | coord1  | 172.30.0.3  | Leader       | running   |  1 |           |
    |     0 | coord2  | 172.30.0.12 | Replica      | streaming |  1 |         0 |
    |     0 | coord3  | 172.30.0.2  | Sync Standby | streaming |  1 |         0 |
    |     1 | work1-1 | 172.30.0.7  | Leader       | running   |  1 |           |
    |     1 | work1-2 | 172.30.0.10 | Sync Standby | streaming |  1 |         0 |
    |     2 | work2-1 | 172.30.0.8  | Sync Standby | streaming |  2 |         0 |
    |     2 | work2-2 | 172.30.0.11 | Leader       | running   |  2 |           |
    +-------+---------+-------------+--------------+-----------+----+-----------+


    postgres@haproxy:~$ psql -h localhost -p 5000 -U postgres -d citus
    psql (15.5 (Debian 15.5-1.pgdg120+1))
    SSL connection (protocol: TLSv1.3, cipher: TLS_AES_256_GCM_SHA384, compression: off)
    Type "help" for help.


    citus=# table pg_dist_node;
     nodeid | groupid |  nodename   | nodeport | noderack | hasmetadata | isactive | noderole | nodecluster | metadatasynced | shouldhaveshards 
    --------+---------+-------------+----------+----------+-------------+----------+----------+-------------+----------------+------------------
        1 |       0 | 172.30.0.3  |     5432 | default  | t           | t        | primary  | default     | t              | f
        3 |       2 | 172.30.0.11 |     5432 | default  | t           | t        | primary  | default     | t              | t
        2 |       1 | 172.30.0.7  |     5432 | default  | t           | t        | primary  | default     | t              | t
    (3 rows)
