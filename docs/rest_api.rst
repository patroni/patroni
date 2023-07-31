.. _rest_api:

Patroni REST API
================

Patroni has a rich REST API, which is used by Patroni itself during the leader race, by the ``patronictl`` tool in order to perform failovers/switchovers/reinitialize/restarts/reloads, by HAProxy or any other kind of load balancer to perform HTTP health checks, and of course could also be used for monitoring. Below you will find the list of Patroni REST API endpoints.

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

- ``GET /asynchronous`` or ``GET /async``: returns HTTP status code **200** only when the Patroni node is running as an asynchronous standby.


- ``GET /asynchronous?lag=<max-lag>`` or ``GET /async?lag=<max-lag>``: asynchronous standby check endpoint. In addition to checks from ``asynchronous`` or ``async``, it also checks replication latency and returns status code **200** only when it is below specified value. The key cluster.last_leader_operation from DCS is used for Leader wal position and compute latency on replica for performance reasons. max-lag can be specified in bytes (integer) or in human readable values, for e.g. 16kB, 64MB, 1GB.

  - ``GET /async?lag=1048576``
  - ``GET /async?lag=1024kB``
  - ``GET /async?lag=10MB``
  - ``GET /async?lag=1GB``

- ``GET /health``: returns HTTP status code **200** only when PostgreSQL is up and running.

- ``GET /liveness``: returns HTTP status code **200** if Patroni heartbeat loop is properly running and **503** if the last run was more than ``ttl`` seconds ago on the primary or ``2*ttl`` on the replica. Could be used for ``livenessProbe``.

- ``GET /readiness``: returns HTTP status code **200** when the Patroni node is running as the leader or when PostgreSQL is up and running. The endpoint could be used for ``readinessProbe`` when it is not possible to use Kubernetes endpoints for leader elections (OpenShift).

Both, ``readiness`` and ``liveness`` endpoints are very light-weight and not executing any SQL. Probes should be configured in such a way that they start failing about time when the leader key is expiring. With the default value of ``ttl``, which is ``30s`` example probes would look like:

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

.. code-block:: bash

    $ curl -s http://localhost:8008/patroni | jq .
    {
      "state": "running",
      "postmaster_start_time": "2019-09-24 09:22:32.555 CEST",
      "role": "master",
      "server_version": 110005,
      "cluster_unlocked": false,
      "xlog": {
        "location": 25624640
      },
      "timeline": 3,
      "database_system_identifier": "6739877027151648096",
      "patroni": {
        "version": "1.6.0",
        "scope": "batman"
      }
    }


Retrieve the Patroni metrics in Prometheus format through the ``GET /metrics`` endpoint.

.. code-block:: bash

	$ curl http://localhost:8008/metrics
	
	# HELP patroni_version Patroni semver without periods. \
	# TYPE patroni_version gauge
	patroni_version{scope="batman"} 020103
	# HELP patroni_postgres_running Value is 1 if Postgres is running, 0 otherwise.
	# TYPE patroni_postgres_running gauge
	patroni_postgres_running{scope="batman"} 1
	# HELP patroni_postmaster_start_time Epoch seconds since Postgres started.
	# TYPE patroni_postmaster_start_time gauge
	patroni_postmaster_start_time{scope="batman"} 1657656955.179243
	# HELP patroni_master Value is 1 if this node is the leader, 0 otherwise.
	# TYPE patroni_master gauge
	patroni_master{scope="batman"} 1
	# HELP patroni_xlog_location Current location of the Postgres transaction log, 0 if this node is not the leader.
	# TYPE patroni_xlog_location counter
	patroni_xlog_location{scope="batman"} 22320573386952
	# HELP patroni_standby_leader Value is 1 if this node is the standby_leader, 0 otherwise.
	# TYPE patroni_standby_leader gauge
	patroni_standby_leader{scope="batman"} 0
	# HELP patroni_replica Value is 1 if this node is a replica, 0 otherwise.
	# TYPE patroni_replica gauge
	patroni_replica{scope="batman"} 0
	# HELP patroni_sync_standby Value is 1 if this node is a sync standby replica, 0 otherwise.
	# TYPE patroni_sync_standby gauge
	patroni_sync_standby{scope="batman"} 0
	# HELP patroni_xlog_received_location Current location of the received Postgres transaction log, 0 if this node is not a replica.
	# TYPE patroni_xlog_received_location counter
	patroni_xlog_received_location{scope="batman"} 0
	# HELP patroni_xlog_replayed_location Current location of the replayed Postgres transaction log, 0 if this node is not a replica.
	# TYPE patroni_xlog_replayed_location counter
	patroni_xlog_replayed_location{scope="batman"} 0
	# HELP patroni_xlog_replayed_timestamp Current timestamp of the replayed Postgres transaction log, 0 if null.
	# TYPE patroni_xlog_replayed_timestamp gauge
	patroni_xlog_replayed_timestamp{scope="batman"} 0
	# HELP patroni_xlog_paused Value is 1 if the Postgres xlog is paused, 0 otherwise.
	# TYPE patroni_xlog_paused gauge
	patroni_xlog_paused{scope="batman"} 0
	# HELP patroni_postgres_streaming Value is 1 if Postgres is streaming, 0 otherwise.
	# TYPE patroni_postgres_streaming gauge
	patroni_postgres_streaming{scope="batman"} 1
	# HELP patroni_postgres_in_archive_recovery Value is 1 if Postgres is replicating from archive, 0 otherwise.
	# TYPE patroni_postgres_in_archive_recovery gauge
	patroni_postgres_in_archive_recovery{scope="batman"} 0
	# HELP patroni_postgres_server_version Version of Postgres (if running), 0 otherwise.
	# TYPE patroni_postgres_server_version gauge
	patroni_postgres_server_version {scope="batman"} 140004
	# HELP patroni_cluster_unlocked Value is 1 if the cluster is unlocked, 0 if locked.
	# TYPE patroni_cluster_unlocked gauge
	patroni_cluster_unlocked{scope="batman"} 0
	# HELP patroni_postgres_timeline Postgres timeline of this node (if running), 0 otherwise.
	# TYPE patroni_postgres_timeline counter
	patroni_postgres_timeline{scope="batman"} 24
	# HELP patroni_dcs_last_seen Epoch timestamp when DCS was last contacted successfully by Patroni.
	# TYPE patroni_dcs_last_seen gauge
	patroni_dcs_last_seen{scope="batman"} 1677658321
	# HELP patroni_pending_restart Value is 1 if the node needs a restart, 0 otherwise.
	# TYPE patroni_pending_restart gauge
	patroni_pending_restart{scope="batman"} 1
	# HELP patroni_is_paused Value is 1 if auto failover is disabled, 0 otherwise.
	# TYPE patroni_is_paused gauge
	patroni_is_paused{scope="batman"} 1


Cluster status endpoints
------------------------

- The ``GET /cluster`` endpoint generates a JSON document describing the current cluster topology and state:

.. code-block:: bash

    $ curl -s http://localhost:8008/cluster | jq .
    {
      "members": [
        {
          "name": "postgresql0",
          "host": "127.0.0.1",
          "port": 5432,
          "role": "leader",
          "state": "running",
          "api_url": "http://127.0.0.1:8008/patroni",
          "timeline": 5,
          "tags": {
            "clonefrom": true
          }
        },
        {
          "name": "postgresql1",
          "host": "127.0.0.1",
          "port": 5433,
          "role": "replica",
          "state": "running",
          "api_url": "http://127.0.0.1:8009/patroni",
          "timeline": 5,
          "tags": {
            "clonefrom": true
          },
          "lag": 0
        }
      ],
      "scheduled_switchover": {
        "at": "2019-09-24T10:36:00+02:00",
        "from": "postgresql0"
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


Config endpoint
---------------

``GET /config``: Get the current version of the dynamic configuration:

.. code-block:: bash

	$ curl -s localhost:8008/config | jq .
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
	      "wal_log_hints": "on",
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
	      "wal_log_hints": "on",
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
	  "pending_restart": true,
	  "database_system_identifier": "6287881213849985952",
	  "postmaster_start_time": "2016-06-13 13:13:05.211 CEST",
	  "xlog": {
	    "location": 2197818976
	  },
	  "patroni": {
	    "scope": "batman",
	    "version": "1.0"
	  },
	  "state": "running",
	  "role": "master",
	  "server_version": 90503
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
	      "wal_log_hints": "on",
	      "max_wal_senders": 5,
	      "max_replication_slots": 5
	    }
	  }
	}

The above call removes ``postgresql.parameters.max_connections`` from the dynamic configuration.

``PUT /config``: It's also possible to perform the full rewrite of an existing dynamic configuration unconditionally:

.. code-block:: bash

	$ curl -s -XPUT -d \
		'{"maximum_lag_on_failover":1048576,"retry_timeout":10,"postgresql":{"use_slots":true,"use_pg_rewind":true,"parameters":{"hot_standby":"on","wal_log_hints":"on","wal_level":"hot_standby","unix_socket_directories":".","max_wal_senders":5}},"loop_wait":3,"ttl":20}' \
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
	      "wal_log_hints": "on",
	      "max_wal_senders": 5
	    },
	    "use_pg_rewind": true
	  },
	  "loop_wait": 3
	}


Switchover and failover endpoints
---------------------------------

``POST /switchover`` or ``POST /failover``. These endpoints are very similar to each other. There are a couple of minor differences though:

1. The failover endpoint allows to perform a manual failover when there are no healthy nodes, but at the same time it will not allow you to schedule a switchover.

2. The switchover endpoint is the opposite. It works only when the cluster is healthy (there is a leader) and allows to schedule a switchover at a given time.


In the JSON body of the ``POST`` request you must specify at least the ``leader`` or ``candidate`` fields and optionally the ``scheduled_at`` field if you want to schedule a switchover at a specific time.


Example: perform a failover to the specific node:

.. code-block:: bash

    $ curl -s http://localhost:8009/failover -XPOST -d '{"candidate":"postgresql1"}'
    Successfully failed over to "postgresql1"


Example: schedule a switchover from the leader to any other healthy replica in the cluster at a specific time:

.. code-block:: bash

    $ curl -s http://localhost:8008/switchover -XPOST -d \
	    '{"leader":"postgresql0","scheduled_at":"2019-09-24T12:00+00"}'
    Switchover scheduled


Depending on the situation the request might finish with a different HTTP status code and body. The status code **200** is returned when the switchover or failover successfully completed. If the switchover was successfully scheduled, Patroni will return HTTP status code **202**. In case something went wrong, the error status code (one of **400**, **412** or **503**) will be returned with some details in the response body. For more information please check the source code of ``patroni/api.py:do_POST_failover()`` method.

- ``DELETE /switchover``: delete the scheduled switchover

The ``POST /switchover`` and ``POST failover`` endpoints are used by ``patronictl switchover`` and ``patronictl failover``, respectively.
The ``DELETE /switchover`` is used by ``patronictl flush <cluster-name> switchover``.


Restart endpoint
----------------

- ``POST /restart``: You can restart Postgres on the specific node by performing the ``POST /restart`` call. In the JSON body of ``POST`` request it is possible to optionally specify some restart conditions:

  - **restart_pending**: boolean, if set to ``true`` Patroni will restart PostgreSQL only when restart is pending in order to apply some changes in the PostgreSQL config.
  - **role**: perform restart only if the current role of the node matches with the role from the POST request.
  - **postgres_version**: perform restart only if the current version of postgres is smaller than specified in the POST request.
  - **timeout**: how long we should wait before PostgreSQL starts accepting connections. Overrides ``primary_start_timeout``.
  - **schedule**: timestamp with time zone, schedule the restart somewhere in the future.

- ``DELETE /restart``: delete the scheduled restart

``POST /restart`` and ``DELETE /restart`` endpoints are used by ``patronictl restart`` and ``patronictl flush <cluster-name> restart`` respectively.


Reload endpoint
---------------

The ``POST /reload`` call will order Patroni to re-read and apply the configuration file. This is the equivalent of sending the ``SIGHUP`` signal to the Patroni process. In case you changed some of the Postgres parameters which require a restart (like **shared_buffers**), you still have to explicitly do the restart of Postgres by either calling the ``POST /restart`` endpoint or with the help of ``patronictl restart``.

The reload endpoint is used by ``patronictl reload``.


Reinitialize endpoint
---------------------

``POST /reinitialize``: reinitialize the PostgreSQL data directory on the specified node. It is allowed to be executed only on replicas. Once called, it will remove the data directory and start ``pg_basebackup`` or some alternative :ref:`replica creation method <custom_replica_creation>`.

The call might fail if Patroni is in a loop trying to recover (restart) a failed Postgres. In order to overcome this problem one can specify ``{"force":true}`` in the request body.

The reinitialize endpoint is used by ``patronictl reinit``.
