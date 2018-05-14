Replica imaging and bootstrap
=============================

Patroni allows customizing creation of a new replica. It also supports defining what happens when the new empty cluster
is being bootstrapped. The distinction between two is well defined: Patroni creates replicas only if the ``initialize``
key is present in DCS for the cluster. If there is no ``initialize`` key - Patroni calls bootstrap exclusively on the
first node that takes the initialize key lock.

.. _custom_bootstrap:

Bootstrap
---------

PostgreSQL provides ``initdb`` command to initialize a new cluster and Patroni calls it by default. In certain cases,
particularly when creating a new cluster as a copy of an existing one, it is necessary to replace a built-in method with
custom actions. Patroni supports executing user-defined scripts to bootstrap new clusters, supplying some required
arguments to them, i.e. the name of the cluster and the path to the data directory. This is configured in the
``bootstrap`` section of the Patroni configuration. For example:

.. code:: YAML

    bootstrap:
        method: <custom_bootstrap_method_name>
        <custom_bootstrap_method_name>:
            command: <path_to_custom_bootstrap_script> [param1 [, ...]]
            keep_existing_recovery_conf: False
            recovery_conf:
                recovery_target_action: promote
                recovery_target_timeline: latest
                restore_command: <method_specific_restore_command>


Each bootstrap method must define at least a ``name`` and a ``command``. A special ``initdb`` method is available to trigger
the default behavior, in which case ``method`` parameter can be omitted altogether. The ``command`` can be specified using either
an absolute path, or the one relative to the ``patroni`` command location. In addition to the fixed parameters defined
in the configuration files, Patroni supplies two cluster-specific ones:

--scope
    Name of the cluster to be bootstrapped
--datadir
    Path to the data directory of the cluster instance to be bootstrapped

If the bootstrap script returns 0, Patroni tries to configure and start the PostgreSQL instance produced by it. If any
of the intermediate steps fail, or the script returns a non-zero value, Patroni assumes that the bootstrap has failed,
cleans up after itself and releases the initialize lock to give another node the opportunity to bootstrap.

If a ``recovery_conf`` block is defined in the same section as the custom bootstrap method, Patroni will generate a
``recovery.conf`` before starting the newly bootstrapped instance. Typically, such recovery.conf should contain at least
one of the ``recovery_target_*`` parameters, together with the ``recovery_target_timeline`` set to ``promote``.

If ``keep_existing_recovery_conf`` is defined and set to ``True``, Patroni will not remove the existing ``recovery.conf`` file if it exists.
This is useful when bootstrapping from a backup with tools like pgBackRest that generate the appropriate ``recovery.conf`` for you.

 .. note:: Bootstrap methods are neither chained, nor fallen-back to the default one in case the primary one fails


.. _custom_replica_creation:

Building replicas
-----------------

Patroni uses tried and proven ``pg_basebackup`` in order to create new replicas. One downside of it is that it requires
a running master node. Another one is the lack of 'on-the-fly' compression for the backup data and no built-in cleanup
for outdated backup files. Some people prefer other backup solutions, such as ``WAL-E``, ``pgBackRest``, ``Barman`` and
others, or simply roll their own scripts. In order to accommodate all those use-cases Patroni supports running custom
scripts to clone a new replica. Those are configured in the ``postgresql`` configuration block:

.. code:: YAML

    postgresql:
        create_replica_method:
            - wal_e
            - basebackup
        wal_e:
            command: patroni_wale_restore
            no_master: 1
            envdir: {{WALE_ENV_DIR}}
            use_iam: 1
        basebackup:
            max-rate: '100M'


The ``create_replica_method`` defines available replica creation methods and the order of executing them. Patroni will
stop on the first one that returns 0. Each method should a separate section in the configuration file, listing the command
to execute and any custom parameters that should be passed to that command. All parameters will be passed in a
``--name=value`` format. Besides user-defined parameters, Patroni supplies a couple of cluster-specific ones:

--scope
    Which cluster this replica belongs to
--datadir
    Path to the data directory of the replica
--role
    Always 'replica'
--connstring
    Connection string to connect to the cluster member to clone from (master or other replica). The user in the
    connection string can execute SQL and replication protocol commands.

A special ``no_master`` parameter, if defined, allows Patroni to call the replica creation method even if there is no
running master or replicas. In that case, an empty string will be passed in a connection string. This is useful for
restoring the formerly running cluster from the binary backup.

A ``basebackup`` method is a special case: it will be used if ``create_replica_method`` is empty, although it is possible
to list it explicitly among the ``create_replica_method`` methods. ``Basebackup`` initializes a new replica with the
pg_basebackup, the base backup is taken from the master unless there are replicas with ``clonefrom`` tag, in which case one
of such replicas will be used as an origin for pg_basebackup. It works without any configuration; however, it is
possible to specify a ``basebackup`` configuration section. Same rules as with the other method configuration apply,
namely, only long (with --) options should be specified there. Not all parameters make sense, if you override a connection
string or provide an option to created tar-ed or compressed base backups, patroni won't be able to make a replica out
of it. There is no validation performed on the names or values of the parameters passed to the ``basebackup`` section.

If all replica creation methods fail, Patroni will try again all methods in order during the next event loop cycle.
