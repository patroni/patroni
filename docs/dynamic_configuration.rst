Patroni configuration
=====================

Patroni configuration is stored in the DCS (Distributed Configuration Store). There are 3 types of configuration:

- Dynamic configuration.
	Those options can be set in DCS at any time. If the options changed are not part of the startup configuration,
	they are applied asynchronously (upon the next wake up cycle) to every node, which gets subsequently reloaded.
	If the node requires a restart to apply the configuration (for options with context postmaster, if their values
	have changed), a special flag, ``pending_restart`` indicating this, is set in the members.data JSON.
	Additionally, the node status also indicates this, by showing ``"restart_pending": true``.

- Local `configuration <https://github.com/zalando/patroni/blob/master/docs/SETTINGS.rst>`__ (patroni.yml).
	Those options are defined in the configuration file and take precedence over dynamic configuration.
	patroni.yml could be changed and reload in runtime (without restart of Patroni) by sending SIGHUP to the Patroni process or by performing ``POST /reload`` REST-API request.

- Environment `configuration <https://github.com/zalando/patroni/blob/master/docs/ENVIRONMENT.rst>`
	It is possible to set/override some of the "Local" configuration parameters with environment variables.
	Environment configuration is very useful when you are running in dynamic environment and you don't know some of the parameters in advance (for example it's not possible to know you external IP address when you are running inside ``docker``).

Some of PostgreSQL parameters must be set to the same value on master and replicas and therefore controlled only via "Dynamic configuration". Any attemt to change them via "Local configuration" are blocked:

- max_connections: 100
- max_locks_per_transaction: 64
- max_worker_processes: 8
- max_prepared_transactions: 0
- wal_level: hot_standby
- wal_log_hints: on
- track_commit_timestamp: off

Although following parameters are not necessarily must be the same on master and replicas, but it is better to have them eqals, therefore they are also controlled by Patroni and can be set only via Dynamic configuration.
- max_wal_senders: 5
- max_replication_slots: 5
- wal_keep_segments: 8

Important! Patroni does some simple checks of new values obtained from DCS before applying them. Above lists containing some sane default values and it is not allowed to set new value smaller.

There are some other PostgreSQL parameters controlled by Patroni:
- listen_addresses - is set either from ``postgresql.listen`` or from ``PATRONI_POSTGRESQL_LISTEN`` environment variable
- port - is set either from ``postgresql.listen`` or from ``PATRONI_POSTGRESQL_LISTEN`` environment variable
- cluster_name - is set either from ``scope`` or from ``PATRRONI_SCOPE`` environment variable
- hot_standby: on

To be on the safe side parameters from the above lists are not written into ``postgresql.conf``, but passed as a list of arguments to the ``pg_ctl start`` what makes it not possible to change them even with 'ALTER SYSTEM'


When applying the local or dynamic configuration options, the following actions are taken:

- The node first checks if there is a postgresql.base.conf.
- If it exists, it contains the renamed "original" configuration.
- If it doesn't, the original postgresql.conf is taken and renamed to postgresql.base.conf.
- The dynamic options (with the exceptions above) are dumped into the postgresql.conf and an include is set in
  postgresql.conf to postgresql.base.conf. Therefore, we would be able to apply new options without re-reading the configuration file to check if the include is present not.
- Some parameters that are essential for Patroni to manage the cluster are overridden using the command line.
- If some of the options that require restart are changed (we should look at the context in pg_settings and at the actual
  values of those options), a pending_restart flag of a given node is set. This flag is reset on any restart.

Parameters would be applied in the following order (run-time are given the highest priority):

1. load parameters from file `postgresql.base.conf`
2. load parameters from file `postgresql.conf`
3. load parameters from file `postgresql.auto.conf`
4. run-time parameter using `-o --name=value`

This allows configuration for all the nodes (2), configuration for a specific node using `ALTER SYSTEM` (3) and ensures that parameters essential to the running of Patroni are enforced. (4)


Also, the following patroni configuration options can be changed only dynamically:

- ttl: 30
- loop_wait: 10
- retry_timeouts: 10
- maximum_lag_on_failover: 1048576
- postgresql.use_slots: true

Upon changing those options, Patroni should read the relevant section of the configuration stored in DCS and change their
run-time values.

Patroni nodes are dumping the state of the DCS options to disk upon every change of the configuration into file ``patroni.dynamic.json`` located in the postgres data directory. Only master is allowed to restore those options from the on-disk dump if those are completely absent from the DCS or invalid.

REST API
========

We are providing REST API endpoint for working with dynamic configuration.

GET /config
-----------
Get current version of dynamic configuration.

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
	      "wal_keep_segments": 8,
	      "wal_level": "hot_standby",
	      "max_wal_senders": 5,
	      "max_replication_slots": 5,
	      "max_connections": "100"
	    }
	  }
	}

PATCH /config
-------------
Patch existing configuration.

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
	      "wal_keep_segments": 8,
	      "wal_level": "hot_standby",
	      "max_wal_senders": 5,
	      "max_replication_slots": 5,
	      "max_connections": "101"
	    }
	  }
	}

Above REST API call patches existing configuration and returns the new configuration.

Let's check that node processed this configuration. First of all it should start printing logs lines every 5 seconds (loop_wait=5). Change of "max_connections" requires restart, so "restart_pending" flag should be exposed:

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
	      "wal_keep_segments": 8,
	      "wal_level": "hot_standby",
	      "wal_log_hints": "on",
	      "max_wal_senders": 5,
	      "max_replication_slots": 5
	    }
	  }
	}

Above call removes ``postgresql.parameters.max_connections`` from dynaminc configuration.

PUT /config
-----------

It's also possible to perform the full rewrite of existing dynamic configuration unconditionally:

.. code-block:: bash

	$ curl -s -XPUT -d \
		'{"maximum_lag_on_failover":1048576,"retry_timeout":10,"postgresql":{"use_slots":true,"use_pg_rewind":true,"parameters":{"hot_standby":"on","wal_log_hints":"on","wal_keep_segments":8,"wal_level":"hot_standby","unix_socket_directories":".","max_wal_senders":5}},"loop_wait":3,"ttl":20}' \
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
	      "wal_keep_segments": 8,
	      "wal_level": "hot_standby",
	      "wal_log_hints": "on",
	      "max_wal_senders": 5
	    },
	    "use_pg_rewind": true
	  },
	  "loop_wait": 3
	}
