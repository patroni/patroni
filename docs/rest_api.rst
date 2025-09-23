.. _rest_api:

Patroni REST API
================

Patroni has a rich REST API, which is used by Patroni itself during the leader race, by the :ref:`patronictl` tool in order to perform failovers/switchovers/reinitialize/restarts/reloads, by HAProxy or any other kind of load balancer to perform HTTP health checks, and of course could also be used for monitoring. Below you will find the list of Patroni REST API endpoints.

Health check endpoints
----------------------
For all health check ``GET`` requests Patroni returns a JSON document with the status of the node, along with the HTTP status code. If you don't want or don't need the JSON document, you might consider using the ``HEAD`` or ``OPTIONS`` method instead of ``GET``.

- The following requests to Patroni REST API will return HTTP status code **200** only when the Patroni node is running as the primary with leader lock:

  - ``GET /``
  - ``GET /primary``
  - ``GET /read-write``

- ``GET /standby-leader``: returns HTTP status code **200** only when the Patroni node is running as the leader in a :ref:`standby cluster <standby_cluster>`.

- ``GET /leader``: returns HTTP status code **200** when the Patroni node has the leader lock. The major difference from the two previous endpoints is that it doesn't take into account whether PostgreSQL is running as the ``primary`` or the ``standby_leader``.

- ``GET /replica``: replica health check endpoint. It returns HTTP status code **200** only when the Patroni node is in the state ``running``, the role is ``replica`` and ``noloadbalance`` tag is not set.

- ``GET /replica?lag=<max-lag>``: replica check endpoint. In addition to checks from ``replica``, it also checks replication latency and returns status code **200** only when it is below specified value. The key cluster.last_leader_operation from DCS is used for Leader wal position and compute latency on replica for performance reasons. max-lag can be specified in bytes (integer) or in human readable values, for e.g. 16kB, 64MB, 1GB.

  - ``GET /replica?lag=1048576``
  - ``GET /replica?lag=1024kB``
  - ``GET /replica?lag=10MB``
  - ``GET /replica?lag=1GB``

- ``GET /replica?tag_key1=value1&tag_key2=value2``: replica check endpoint. In addition, It will also check for user defined tags ``key1`` and ``key2`` and their respective values in the **tags** section of the yaml configuration management. If the tag isn't defined for an instance, or if the value in the yaml configuration doesn't match the querying value, it will return HTTP Status Code 503.

  In the following requests, since we are checking for the leader or standby-leader status, Patroni doesn't apply any of the user defined tags and they will be ignored.

  - ``GET /?tag_key1=value1&tag_key2=value2``
  - ``GET /leader?tag_key1=value1&tag_key2=value2``
  - ``GET /primary?tag_key1=value1&tag_key2=value2``
  - ``GET /read-write?tag_key1=value1&tag_key2=value2``
  - ``GET /standby_leader?tag_key1=value1&tag_key2=value2``
  - ``GET /standby-leader?tag_key1=value1&tag_key2=value2``

- ``GET /read-only``: like the above endpoint, but also includes the primary.

- ``GET /synchronous`` or ``GET /sync``: returns HTTP status code **200** only when the Patroni node is running as a synchronous standby.

- ``GET /read-only-sync``: like the above endpoint, but also includes the primary.

- ``GET /quorum``: returns HTTP status code **200** only when this Patroni node is listed as a quorum node in ``synchronous_standby_names`` on the primary.

- ``GET /read-only-quorum``: like the above endpoint, but also includes the primary.

- ``GET /asynchronous`` or ``GET /async``: returns HTTP status code **200** only when the Patroni node is running as an asynchronous standby.


- ``GET /asynchronous?lag=<max-lag>`` or ``GET /async?lag=<max-lag>``: asynchronous standby check endpoint. In addition to checks from ``asynchronous`` or ``async``, it also checks replication latency and returns status code **200** only when it is below specified value. The key cluster.last_leader_operation from DCS is used for Leader wal position and compute latency on replica for performance reasons. max-lag can be specified in bytes (integer) or in human readable values, for e.g. 16kB, 64MB, 1GB.

  - ``GET /async?lag=1048576``
  - ``GET /async?lag=1024kB``
  - ``GET /async?lag=10MB``
  - ``GET /async?lag=1GB``

- ``GET /health``: returns HTTP status code **200** only when PostgreSQL is up and running.

- ``GET /liveness``: returns HTTP status code **200** if Patroni heartbeat loop is properly running and **503** if the last run was more than ``ttl`` seconds ago on the primary or ``2*ttl`` on the replica. Could be used for ``livenessProbe``.

- ``GET /readiness?lag=<max-lag>&mode=apply|write``: returns HTTP status code **200** when the Patroni node is running as the leader or when PostgreSQL is up, replicating and not too far behind the leader. The lag parameter sets how far a standby is allowed to be behind, it defaults to ``maximum_lag_on_failover``. Lag can be specified in bytes or in human readable values, for e.g. 16kB, 64MB, 1GB. Mode sets whether the WAL needs to be replayed (apply) or just received (write). The default is apply.

  When used as Kubernetes ``readinessProbe`` it will make sure freshly started pods only become ready when they have caught up to the leader. This combined with a PodDisruptionBudget will protect against leader being terminated too early during a rolling restart of nodes. It will also make sure that replicas that cannot keep up with replication do not service read-only traffic. The endpoint could be used for ``readinessProbe`` when it is not possible to use Kubernetes endpoints for leader elections (OpenShift).

The ``liveness`` endpoint is very light-weight and not executing any SQL. Probes should be configured in such a way that they start failing about time when the leader key is expiring. With the default value of ``ttl``, which is ``30s`` example probes would look like:

.. code-block:: yaml

    readinessProbe:
      httpGet:
        scheme: HTTP
        path: /readiness
        port: 8008
      initialDelaySeconds: 3
      periodSeconds: 10
      timeoutSeconds: 5
      successThreshold: 1
      failureThreshold: 3
    livenessProbe:
      httpGet:
        scheme: HTTP
        path: /liveness
        port: 8008
      initialDelaySeconds: 3
      periodSeconds: 10
      timeoutSeconds: 5
      successThreshold: 1
      failureThreshold: 3


Monitoring endpoint
-------------------

The ``GET /patroni`` is used by Patroni during the leader race. It also could be used by your monitoring system. The JSON document produced by this endpoint has the same structure as the JSON produced by the health check endpoints.

**Example:** A healthy cluster

.. code-block:: bash

    $ curl -s http://localhost:8008/patroni | jq .
    {
      "state": "running",
      "postmaster_start_time": "2024-08-28 19:39:26.352526+00:00",
      "role": "primary",
      "server_version": 160004,
      "xlog": {
        "location": 67395656
      },
      "timeline": 1,
      "replication": [
        {
          "usename": "replicator",
          "application_name": "patroni2",
          "client_addr": "10.89.0.6",
          "state": "streaming",
          "sync_state": "async",
          "sync_priority": 0
        },
        {
          "usename": "replicator",
          "application_name": "patroni3",
          "client_addr": "10.89.0.2",
          "state": "streaming",
          "sync_state": "async",
          "sync_priority": 0
        }
      ],
      "dcs_last_seen": 1692356718,
      "tags": {
        "clonefrom": true
      },
      "database_system_identifier": "7268616322854375442",
      "patroni": {
        "version": "4.0.0",
        "scope": "demo",
        "name": "patroni1"
      }
    }

**Example:** An unlocked cluster

.. code-block:: bash

    $ curl -s http://localhost:8008/patroni  | jq .
    {
      "state": "running",
      "postmaster_start_time": "2024-08-28 19:39:26.352526+00:00",
      "role": "replica",
      "server_version": 160004,
      "xlog": {
        "received_location": 67419744,
        "replayed_location": 67419744,
        "replayed_timestamp": null,
        "paused": false
      },
      "timeline": 1,
      "replication": [
        {
          "usename": "replicator",
          "application_name": "patroni2",
          "client_addr": "10.89.0.6",
          "state": "streaming",
          "sync_state": "async",
          "sync_priority": 0
        },
        {
          "usename": "replicator",
          "application_name": "patroni3",
          "client_addr": "10.89.0.2",
          "state": "streaming",
          "sync_state": "async",
          "sync_priority": 0
        }
      ],
      "cluster_unlocked": true,
      "dcs_last_seen": 1692356928,
      "tags": {
        "clonefrom": true
      },
      "database_system_identifier": "7268616322854375442",
      "patroni": {
        "version": "4.0.0",
        "scope": "demo",
        "name": "patroni1"
      }
    }

**Example:** An unlocked cluster with :ref:`DCS failsafe mode <dcs_failsafe_mode>` enabled

.. code-block:: bash

    $ curl -s http://localhost:8008/patroni  | jq .
    {
      "state": "running",
      "postmaster_start_time": "2024-08-28 19:39:26.352526+00:00",
      "role": "replica",
      "server_version": 160004,
      "xlog": {
        "location": 67420024
      },
      "timeline": 1,
      "replication": [
        {
          "usename": "replicator",
          "application_name": "patroni2",
          "client_addr": "10.89.0.6",
          "state": "streaming",
          "sync_state": "async",
          "sync_priority": 0
        },
        {
          "usename": "replicator",
          "application_name": "patroni3",
          "client_addr": "10.89.0.2",
          "state": "streaming",
          "sync_state": "async",
          "sync_priority": 0
        }
      ],
      "cluster_unlocked": true,
      "failsafe_mode_is_active": true,
      "dcs_last_seen": 1692356928,
      "tags": {
        "clonefrom": true
      },
      "database_system_identifier": "7268616322854375442",
      "patroni": {
        "version": "4.0.0",
        "scope": "demo",
        "name": "patroni1"
      }
    }

**Example:** A cluster with the :ref:`pause mode <pause>` enabled

.. code-block:: bash

    $ curl -s http://localhost:8008/patroni  | jq .
    {
      "state": "running",
      "postmaster_start_time": "2024-08-28 19:39:26.352526+00:00",
      "role": "replica",
      "server_version": 160004,
      "xlog": {
        "location": 67420024
      },
      "timeline": 1,
      "replication": [
        {
          "usename": "replicator",
          "application_name": "patroni2",
          "client_addr": "10.89.0.6",
          "state": "streaming",
          "sync_state": "async",
          "sync_priority": 0
        },
        {
          "usename": "replicator",
          "application_name": "patroni3",
          "client_addr": "10.89.0.2",
          "state": "streaming",
          "sync_state": "async",
          "sync_priority": 0
        }
      ],
      "pause": true,
      "dcs_last_seen": 1724874295,
      "tags": {
        "clonefrom": true
      },
      "database_system_identifier": "7268616322854375442",
      "patroni": {
        "version": "4.0.0",
        "scope": "demo",
        "name": "patroni1"
      }
    }

Retrieve the Patroni metrics in Prometheus format through the ``GET /metrics`` endpoint.

.. code-block:: bash

	$ curl http://localhost:8008/metrics

	# HELP patroni_version Patroni semver without periods. \
	# TYPE patroni_version gauge
	patroni_version{scope="batman",name="patroni1"} 040000
	# HELP patroni_postgres_running Value is 1 if Postgres is running, 0 otherwise.
	# TYPE patroni_postgres_running gauge
	patroni_postgres_running{scope="batman",name="patroni1"} 1
	# HELP patroni_postmaster_start_time Epoch seconds since Postgres started.
	# TYPE patroni_postmaster_start_time gauge
	patroni_postmaster_start_time{scope="batman",name="patroni1"} 1724873966.352526
	# HELP patroni_primary Value is 1 if this node is the leader, 0 otherwise.
	# TYPE patroni_primary gauge
	patroni_primary{scope="batman",name="patroni1"} 1
	# HELP patroni_xlog_location Current location of the Postgres transaction log, 0 if this node is not the leader.
	# TYPE patroni_xlog_location counter
	patroni_xlog_location{scope="batman",name="patroni1"} 22320573386952
	# HELP patroni_standby_leader Value is 1 if this node is the standby_leader, 0 otherwise.
	# TYPE patroni_standby_leader gauge
	patroni_standby_leader{scope="batman",name="patroni1"} 0
	# HELP patroni_replica Value is 1 if this node is a replica, 0 otherwise.
	# TYPE patroni_replica gauge
	patroni_replica{scope="batman",name="patroni1"} 0
	# HELP patroni_sync_standby Value is 1 if this node is a sync standby replica, 0 otherwise.
	# TYPE patroni_sync_standby gauge
	patroni_sync_standby{scope="batman",name="patroni1"} 0
	# HELP patroni_quorum_standby Value is 1 if this node is a quorum standby replica, 0 otherwise.
	# TYPE patroni_quorum_standby gauge
	patroni_quorum_standby{scope="batman",name="patroni1"} 0
	# HELP patroni_xlog_received_location Current location of the received Postgres transaction log, 0 if this node is not a replica.
	# TYPE patroni_xlog_received_location counter
	patroni_xlog_received_location{scope="batman",name="patroni1"} 0
	# HELP patroni_xlog_replayed_location Current location of the replayed Postgres transaction log, 0 if this node is not a replica.
	# TYPE patroni_xlog_replayed_location counter
	patroni_xlog_replayed_location{scope="batman",name="patroni1"} 0
	# HELP patroni_xlog_replayed_timestamp Current timestamp of the replayed Postgres transaction log, 0 if null.
	# TYPE patroni_xlog_replayed_timestamp gauge
	patroni_xlog_replayed_timestamp{scope="batman",name="patroni1"} 0
	# HELP patroni_xlog_paused Value is 1 if the Postgres xlog is paused, 0 otherwise.
	# TYPE patroni_xlog_paused gauge
	patroni_xlog_paused{scope="batman",name="patroni1"} 0
	# HELP patroni_postgres_streaming Value is 1 if Postgres is streaming, 0 otherwise.
	# TYPE patroni_postgres_streaming gauge
	patroni_postgres_streaming{scope="batman",name="patroni1"} 1
	# HELP patroni_postgres_in_archive_recovery Value is 1 if Postgres is replicating from archive, 0 otherwise.
	# TYPE patroni_postgres_in_archive_recovery gauge
	patroni_postgres_in_archive_recovery{scope="batman",name="patroni1"} 0
	# HELP patroni_postgres_server_version Version of Postgres (if running), 0 otherwise.
	# TYPE patroni_postgres_server_version gauge
	patroni_postgres_server_version{scope="batman",name="patroni1"} 160004
	# HELP patroni_cluster_unlocked Value is 1 if the cluster is unlocked, 0 if locked.
	# TYPE patroni_cluster_unlocked gauge
	patroni_cluster_unlocked{scope="batman",name="patroni1"} 0
	# HELP patroni_postgres_timeline Postgres timeline of this node (if running), 0 otherwise.
	# TYPE patroni_postgres_timeline counter
	patroni_failsafe_mode_is_active{scope="batman",name="patroni1"} 0
	# HELP patroni_postgres_timeline Postgres timeline of this node (if running), 0 otherwise.
	# TYPE patroni_postgres_timeline counter
	patroni_postgres_timeline{scope="batman",name="patroni1"} 24
	# HELP patroni_dcs_last_seen Epoch timestamp when DCS was last contacted successfully by Patroni.
	# TYPE patroni_dcs_last_seen gauge
	patroni_dcs_last_seen{scope="batman",name="patroni1"} 1724874235
	# HELP patroni_pending_restart Value is 1 if the node needs a restart, 0 otherwise.
	# TYPE patroni_pending_restart gauge
	patroni_pending_restart{scope="batman",name="patroni1"} 1
	# HELP patroni_is_paused Value is 1 if auto failover is disabled, 0 otherwise.
	# TYPE patroni_is_paused gauge
	patroni_is_paused{scope="batman",name="patroni1"} 1
	# HELP patroni_postgres_state Numeric representation of Postgres state.
	# Values: 0=initdb, 1=initdb_failed, 2=custom_bootstrap, 3=custom_bootstrap_failed, 4=creating_replica, 5=running, 6=starting, 7=bootstrap_starting, 8=start_failed, 9=restarting, 10=restart_failed, 11=stopping, 12=stopped, 13=stop_failed, 14=crashed
	# TYPE patroni_postgres_state gauge
	patroni_postgres_state{scope="batman",name="patroni1"} 5

PostgreSQL State Values
^^^^^^^^^^^^^^^^^^^^^^^

The ``patroni_postgres_state`` metric provides a numeric representation of the current PostgreSQL instance state. This is useful for monitoring and alerting systems that need to track state changes over time. The numeric values are generated using the ``PostgresqlState.get_metrics_description()`` static method.

.. list-table:: PostgreSQL State Values
   :widths: 10 20 50
   :header-rows: 1

   * - Value
     - State Name
     - Description
   * - 0
     - initdb
     - Initializing new cluster
   * - 1
     - initdb_failed
     - Initialization of new cluster failed
   * - 2
     - custom_bootstrap
     - Running custom bootstrap script
   * - 3
     - custom_bootstrap_failed
     - Custom bootstrap script failed
   * - 4
     - creating_replica
     - Creating replica from primary
   * - 5
     - running
     - PostgreSQL is running normally
   * - 6
     - starting
     - PostgreSQL is starting up
   * - 7
     - bootstrap_starting
     - Starting after custom bootstrap
   * - 8
     - start_failed
     - PostgreSQL start failed
   * - 9
     - restarting
     - PostgreSQL is restarting
   * - 10
     - restart_failed
     - PostgreSQL restart failed
   * - 11
     - stopping
     - PostgreSQL is stopping
   * - 12
     - stopped
     - PostgreSQL is stopped
   * - 13
     - stop_failed
     - PostgreSQL stop failed
   * - 14
     - crashed
     - PostgreSQL has crashed

.. note::
   These numeric values are fixed and will never change to maintain backward compatibility with existing monitoring systems. If new states are added in the future, they will be assigned new numeric values without changing existing ones.


Cluster status endpoints
------------------------

- The ``GET /cluster`` endpoint generates a JSON document describing the current cluster topology and state:

.. code-block:: bash

    $ curl -s http://localhost:8008/cluster | jq .
    {
      "members": [
        {
          "name": "patroni1",
          "role": "leader",
          "state": "running",
          "api_url": "http://10.89.0.4:8008/patroni",
          "host": "10.89.0.4",
          "port": 5432,
          "timeline": 5,
          "tags": {
            "clonefrom": true
          }
        },
        {
          "name": "patroni2",
          "role": "replica",
          "state": "streaming",
          "api_url": "http://10.89.0.6:8008/patroni",
          "host": "10.89.0.6",
          "port": 5433,
          "timeline": 5,
          "tags": {
            "clonefrom": true
          },
          "receive_lag": 0,
          "receive_lsn": "0/4000060",
          "replay_lag": 0,
          "replay_lsn": "0/4000060",
          "lag": 0,
          "lsn": "0/4000060"
        }
      ],
      "scope": "demo",
      "scheduled_switchover": {
        "at": "2023-09-24T10:36:00+02:00",
        "from": "patroni1",
        "to": "patroni3"
      }
    }


- The ``GET /history`` endpoint provides a view on the history of cluster switchovers/failovers. The format is very similar to the content of history files in the ``pg_wal`` directory. The only difference is the timestamp field showing when the new timeline was created.

.. code-block:: bash

    $ curl -s http://localhost:8008/history | jq .
    [
      [
        1,
        25623960,
        "no recovery target specified",
        "2019-09-23T16:57:57+02:00"
      ],
      [
        2,
        25624344,
        "no recovery target specified",
        "2019-09-24T09:22:33+02:00"
      ],
      [
        3,
        25624752,
        "no recovery target specified",
        "2019-09-24T09:26:15+02:00"
      ],
      [
        4,
        50331856,
        "no recovery target specified",
        "2019-09-24T09:35:52+02:00"
      ]
    ]

.. _config_endpoint:

Config endpoint
---------------

``GET /config``: Get the current version of the dynamic configuration:

.. code-block:: bash

	$ curl -s http://localhost:8008/config | jq .
	{
	  "ttl": 30,
	  "loop_wait": 10,
	  "retry_timeout": 10,
	  "maximum_lag_on_failover": 1048576,
	  "postgresql": {
	    "use_slots": true,
	    "use_pg_rewind": true,
	    "parameters": {
	      "hot_standby": "on",
	      "wal_level": "hot_standby",
	      "max_wal_senders": 5,
	      "max_replication_slots": 5,
	      "max_connections": "100"
	    }
	  }
	}


``PATCH /config``: Change the existing configuration.

.. code-block:: bash

	$ curl -s -XPATCH -d \
		'{"loop_wait":5,"ttl":20,"postgresql":{"parameters":{"max_connections":"101"}}}' \
		http://localhost:8008/config | jq .
	{
	  "ttl": 20,
	  "loop_wait": 5,
	  "maximum_lag_on_failover": 1048576,
	  "retry_timeout": 10,
	  "postgresql": {
	    "use_slots": true,
	    "use_pg_rewind": true,
	    "parameters": {
	      "hot_standby": "on",
	      "wal_level": "hot_standby",
	      "max_wal_senders": 5,
	      "max_replication_slots": 5,
	      "max_connections": "101"
	    }
	  }
	}

The above REST API call patches the existing configuration and returns the new configuration.

Let's check that the node processed this configuration. First of all it should start printing log lines every 5 seconds (loop_wait=5). The change of "max_connections" requires a restart, so the "pending_restart" flag should be exposed:

.. code-block:: bash

	$ curl -s http://localhost:8008/patroni | jq .
	{
	  "database_system_identifier": "6287881213849985952",
	  "postmaster_start_time": "2024-08-28 19:39:26.352526+00:00",
	  "xlog": {
	    "location": 2197818976
	  },
	  "timeline": 1,
	  "dcs_last_seen": 1724874545,
	  "database_system_identifier": "7408277255830290455",
	  "pending_restart": true,
	  "pending_restart_reason": {
	    "max_connections": {
	      "old_value": "100",
	      "new_value": "101"
	    }
	  },
	  "patroni": {
	    "version": "4.0.0",
	    "scope": "batman",
	    "name": "patroni1"
	  },
	  "state": "running",
	  "role": "primary",
	  "server_version": 160004
	}

Removing parameters:

If you want to remove (reset) some setting just patch it with ``null``:

.. code-block:: bash

	$ curl -s -XPATCH -d \
		'{"postgresql":{"parameters":{"max_connections":null}}}' \
		http://localhost:8008/config | jq .
	{
	  "ttl": 20,
	  "loop_wait": 5,
	  "retry_timeout": 10,
	  "maximum_lag_on_failover": 1048576,
	  "postgresql": {
	    "use_slots": true,
	    "use_pg_rewind": true,
	    "parameters": {
	      "hot_standby": "on",
	      "unix_socket_directories": ".",
	      "wal_level": "hot_standby",
	      "max_wal_senders": 5,
	      "max_replication_slots": 5
	    }
	  }
	}

The above call removes ``postgresql.parameters.max_connections`` from the dynamic configuration.

``PUT /config``: It's also possible to perform the full rewrite of an existing dynamic configuration unconditionally:

.. code-block:: bash

	$ curl -s -XPUT -d \
		'{"maximum_lag_on_failover":1048576,"retry_timeout":10,"postgresql":{"use_slots":true,"use_pg_rewind":true,"parameters":{"hot_standby":"on","wal_level":"hot_standby","unix_socket_directories":".","max_wal_senders":5}},"loop_wait":3,"ttl":20}' \
		http://localhost:8008/config | jq .
	{
	  "ttl": 20,
	  "maximum_lag_on_failover": 1048576,
	  "retry_timeout": 10,
	  "postgresql": {
	    "use_slots": true,
	    "parameters": {
	      "hot_standby": "on",
	      "unix_socket_directories": ".",
	      "wal_level": "hot_standby",
	      "max_wal_senders": 5
	    },
	    "use_pg_rewind": true
	  },
	  "loop_wait": 3
	}


Switchover and failover endpoints
---------------------------------

.. _switchover_api:

Switchover
^^^^^^^^^^

``/switchover`` endpoint only works when the cluster is healthy (there is a leader). It also allows to schedule a switchover at a given time.

When calling ``/switchover`` endpoint a candidate can be specified but is not required, in contrast to ``/failover`` endpoint. If a candidate is not provided, all the eligible nodes of the cluster will participate in the leader race after the leader stepped down.

In the JSON body of the ``POST`` request you must specify the ``leader`` field. The ``candidate`` and the ``scheduled_at`` fields are optional and can be used to schedule a switchover at a specific time.

Depending on the situation, requests might return different HTTP status codes and bodies. Status code **200** is returned when the switchover or failover successfully completed. If the switchover was successfully scheduled, Patroni will return HTTP status code **202**. In case something went wrong, the error status code (one of **400**, **412**, or **503**) will be returned with some details in the response body.

``DELETE /switchover`` can be used to delete the currently scheduled switchover.

**Example:** perform a switchover to any healthy standby

.. code-block:: bash

	$ curl -s http://localhost:8008/switchover -XPOST -d '{"leader":"postgresql1"}'
	Successfully switched over to "postgresql2"


**Example:** perform a switchover to a specific node

.. code-block:: bash

	$ curl -s http://localhost:8008/switchover -XPOST -d \
		'{"leader":"postgresql1","candidate":"postgresql2"}'
	Successfully switched over to "postgresql2"


**Example:** schedule a switchover from the leader to any other healthy standby in the cluster at a specific time.

.. code-block:: bash

	$ curl -s http://localhost:8008/switchover -XPOST -d \
		'{"leader":"postgresql0","scheduled_at":"2019-09-24T12:00+00"}'
	Switchover scheduled


Failover
^^^^^^^^

``/failover`` endpoint can be used to perform a manual failover when there are no healthy nodes (e.g. to an asynchronous standby if all synchronous standbys are not healthy enough to promote). However there is no requirement for a cluster not to have leader - failover can also be run on a healthy cluster.

In the JSON body of the ``POST`` request you must specify the ``candidate`` field. If the ``leader`` field is specified, a switchover is triggered instead.

**Example:**

.. code-block:: bash

	$ curl -s http://localhost:8008/failover -XPOST -d '{"candidate":"postgresql1"}'
	Successfully failed over to "postgresql1"

.. warning::
	:ref:`Be very careful <failover_healthcheck>` when using this endpoint, as this can cause data loss in certain situations. In most cases, :ref:`the switchover endpoint <switchover_api>` satisfies the administrator's needs.


``POST /switchover`` and ``POST /failover`` endpoints are used by :ref:`patronictl_switchover` and :ref:`patronictl_failover`, respectively.

``DELETE /switchover`` is used by :ref:`patronictl flush cluster-name switchover <patronictl_flush_parameters>`.

.. list-table:: Failover/Switchover comparison
   :widths: 25 25 25
   :header-rows: 1

   * -
     - Failover
     - Switchover
   * - Requires leader specified
     - no
     - yes
   * - Requires candidate specified
     - yes
     - no
   * - Can be run in pause
     - yes
     - yes (only to a specific candidate)
   * - Can be scheduled
     - no
     - yes (if not in pause)

.. _failover_healthcheck:

Healthy standby
^^^^^^^^^^^^^^^

There are a couple of checks that a member of a cluster should pass to be able to participate in the leader race during a switchover or to become a leader as a failover/switchover candidate:

- be reachable via Patroni API;
- not have ``nofailover`` tag set to ``true``;
- have watchdog fully functional (if required by the configuration);
- in case of a switchover in a healthy cluster or an automatic failover, not exceed maximum replication lag (``maximum_lag_on_failover`` :ref:`configuration parameter <dynamic_configuration>`);
- in case of a switchover in a healthy cluster or an automatic failover, not have a timeline number smaller than the cluster timeline if ``check_timeline`` :ref:`configuration parameter <dynamic_configuration>` is set to ``true``;
- in :ref:`synchronous mode <synchronous_mode>`:

  - In case of a switchover (both with and without a candidate): be listed in the ``/sync`` key members;
  - For a failover in both healthy and unhealthy clusters, this check is omitted.

.. warning::
    In case of a manual failover in a cluster without a leader, a candidate will be allowed to promote even if:
	- it is not in the ``/sync`` key members when synchronous mode is enabled;
	- its lag exceeds the maximum replication lag allowed;
	- it has the timeline number smaller than the last known cluster timeline.

.. _restart_endpoint:

Restart endpoint
----------------

- ``POST /restart``: You can restart Postgres on the specific node by performing the ``POST /restart`` call. In the JSON body of ``POST`` request it is possible to optionally specify some restart conditions:

  - **restart_pending**: boolean, if set to ``true`` Patroni will restart PostgreSQL only when restart is pending in order to apply some changes in the PostgreSQL config.
  - **role**: perform restart only if the current role of the node matches with the role from the POST request.
  - **postgres_version**: perform restart only if the current version of postgres is smaller than specified in the POST request.
  - **timeout**: how long we should wait before PostgreSQL starts accepting connections. Overrides ``primary_start_timeout``.
  - **schedule**: timestamp with time zone, schedule the restart somewhere in the future.

- ``DELETE /restart``: delete the scheduled restart

``POST /restart`` and ``DELETE /restart`` endpoints are used by :ref:`patronictl_restart` and :ref:`patronictl flush cluster-name restart <patronictl_flush_parameters>` respectively.

.. _reload_endpoint:

Reload endpoint
---------------

The ``POST /reload`` call will order Patroni to re-read and apply the configuration file. This is the equivalent of sending the ``SIGHUP`` signal to the Patroni process. In case you changed some of the Postgres parameters which require a restart (like **shared_buffers**), you still have to explicitly do the restart of Postgres by either calling the ``POST /restart`` endpoint or with the help of :ref:`patronictl_restart`.

The reload endpoint is used by :ref:`patronictl_reload`.


Reinitialize endpoint
---------------------

``POST /reinitialize``: reinitialize the PostgreSQL data directory on the specified node. It is allowed to be executed only on replicas. Once called, it will remove the data directory and start ``pg_basebackup`` or some alternative :ref:`replica creation method <custom_replica_creation>`.

The call might fail if Patroni is in a loop trying to recover (restart) a failed Postgres. In order to overcome this problem one can specify ``{"force":true}`` in the request body.

You can specify {"from-leader":true} in the request body to directly get basebackup from leader node. This is useful when executing reinit during all replica nodes fail.

The reinitialize endpoint is used by :ref:`patronictl_reinit`.
