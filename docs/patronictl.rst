.. _patronictl:

Patronictl
==========

Patroni has a command-line interface named ``patronictl``, which is used basically to interact with Patroni's REST API and with the DCS. It is intended to make it easier to perform operations in the cluster, and can easily be used by humans or scripts.

Configuration
-------------

``patronictl`` uses 3 sections of the configuration:

- **restapi**: where the REST API server is serving requests. ``patronictl`` is mainly interested in ``restapi.connect_address`` setting;
- **ctl**: how to authenticate against the REST API server, and how to validate the server identity;
- DCS (e.g. **etcd**): how to contact and authenticate against the DCS used by Patroni.

Those configuration options can come either from environment variables or from a configuration file. Look for the above sections in :ref:`Environment Configuration Settings <environment>` or :ref:`YAML Configuration Settings <yaml_configuration>` to understand how you can set the options for them through environment variables or through a configuration file.

If you opt for using environment variables, it's a straight forward approach. Patroni will read the environment variables and use their values.

If you opt for using a configuration file, you have different ways to inform ``patronictl`` about the file to be used. By default ``patronictl`` will attempt to load a configuration file named ``patronictl.yaml``, which is expected to be found under either of these paths, according to your system:

- Mac OS X: ``~/Library/Application Support/Foo Bar/patroni``
- Mac OS X (POSIX): ``~/.foo-bar/patroni``
- Unix: ``~/.config/foo-bar/patroni``
- Unix (POSIX): ``~/.foo-bar/patroni``
- Windows (roaming): ``C:\Users\<user>\AppData\Roaming\Foo Bar/patroni``
- Windows (not roaming): ``C:\Users\<user>\AppData\Local\Foo Bar/patroni``

You can override that behavior either by:

- Setting the environment variable ``PATRONICTL_CONFIG_FILE`` with the path to a custom configuration file;
- Using the ``-c`` / ``--config-file`` command-line argument of ``patronictl`` with the path to a custom configuration file.

.. note::
    If you are running ``patronictl`` in the same host as ``patroni`` daemon is running, you may just use the same configuration file if it contains all the configuration sections required by ``patronictl``.

Usage
--------------------

``patronictl`` exposes several handy operations. This section is intended to describe each of them.

Before jumping into each of the sub-commands of ``patronictl``, be aware that ``patronictl`` itself has the following command-line arguments:

- ``-c`` / ``--config-file``: as explained before, used to provide a path to a configuration file for ``patronictl``;
- ``-d`` / ``--dcs-url`` / ``--dcs``: provide a connection string to the DCS used by Patroni. This argument can be used either to override the DCS settings from the ``patronictl`` configuration, or to define it if it's missing in the configuration. The value should be in the format ``DCS://HOST:PORT``, e.g. ``etcd3://localhost:2379`` to connect to etcd v3 running on ``localhost``;
- ``-k`` / ``--insecure``: bypass validation of REST API server SSL certificate.

This is the synopsis for running a command from the ``patronictl``:

.. code:: text

    patronictl [ { -c | --config-file } CONFIG_FILE ]
      [ { -d | --dcs-url | --dcs } DCS_URL ] 
      [ { -k | --insecure } ]
      SUBCOMMAND

.. note::

    This is the syntax for the synopsis:

    - Options between square brackets are optional;
    - Options between curly brackets represent a "chose one of set" operation;
    - Options with ``[, ... ]`` can be specified multiple times;
    - Things written in uppercase represent a literal that should be given a value to.

    We will use this same syntax when describing ``patronictl`` sub-commands in the following sub-sections. Also, when describing sub-commands in the following sub-sections, the commands' synposis should replace the ``SUBCOMMAND`` in the above synopsis.

In the following sub-sections you can find a description of each command implemented by ``patronictl``. For sake of example, we will use the configuration files present in the GitHub repository of Patroni (files ``postgres0.yml``, ``postgres1.yml`` and ``postgres2.yml``).

patronictl dsn
^^^^^^^^^^^^^^

Synopsis
""""""""

.. code:: text

    dsn
      [ { { -r | --role } { leader | primary | standby-leader | replica | standby | any | master } | { -m | --member } MEMBER_NAME } ]
      [ --group CITUS_GROUP ]
      [ CLUSTER_NAME ]

Description
"""""""""""

``patronictl dsn`` will get the connection string to one member of the Patroni cluster.

If multiple members match the parameters of this command, one of them will be chosen, prioritizing the primary node.

Parameters
""""""""""

- ``-r`` / ``--role``: chose a member that has the given role:

    - ``leader``: the leader of either a regular Patroni cluster or a standby Patroni cluster; or
    - ``primary``: the leader of a regular Patroni cluster; or
    - ``standby-leader``: the leader of a standby Patroni cluster; or
    - ``replica``: a replica of a Patroni cluster; or
    - ``standby``: same as ``replica``; or
    - ``any``: any role. Same as omitting this parameter; or
    - ``master``: same as ``primary``.

- ``-m`` / ``--member``: chose a member of the cluster with the given name:

    - ``MEMBER_NAME``: name of the member;

- ``--group``: chose a member that is part of the given Citus group:

    - ``CITUS_GROUP``: the ID of the Citus group;

- ``CLUSTER_NAME``: name of the Patroni cluster. If not given, ``patronictl`` will attempt to fetch that from ``scope`` configuration, if it exists.

Examples
""""""""

Get DSN of the primary node:

.. code:: text

    patronictl -c postgres0.yml dsn batman -r primary
    host=127.0.0.1 port=5432

Get DSN of the standby node named ``postgresql1``:

.. code:: text

    patronictl -c postgres0.yml dsn batman --member postgresql1
    host=127.0.0.1 port=5433

patronictl edit-config
^^^^^^^^^^^^^^

Synopsis
""""""""

.. code:: text

    edit-config
      [ --group CITUS_GROUP ]
      [ { -q | --quiet } ]
      [ { -s | --set } CONFIG="VALUE" [, ... ] ]
      [ { -p | --pg } PG_CONFIG="PG_VALUE" [, ... ] ]
      [ { --apply | --replace } CONFIG_FILE ]
      [ --force ]
      [ CLUSTER_NAME ]

Description
"""""""""""

``patronictl edit-config`` changes the dynamic configuration of the cluster and updates the DCS with that.

**Note:** when invoked through a TTY the command attempts to show a diff of the dynamic configuration through a pager. By default it attempts to use either ``less`` or ``more``. If you want to use a different pager, set ``PAGER`` environment variable with the desired pager.

Parameters
""""""""""

- ``--group``: change dynamic configuration of the given Citus group:

    - ``CITUS_GROUP``: the ID of the Citus group;

- ``-q`` / ``--quiet``: flag to skip showing the configuration diff;

- ``-s`` / ``--set``: set a given dynamic configuration option with a given value:

    - ``CONFIG``: name of the dynamic configuration path in the YAML tree, with levels joined by ``.`` ;
    - ``VALUE``: value for ``CONFIG``. If it is ``null``, then ``CONFIG`` will be removed from the dynamic configuration.

- ``-p`` / ``--pg``: set a given dynamic Postgres configuration option with the given value. It is essentially a shorthand for ``--s`` / ``--set`` with ``CONFIG`` prepended with ``postgresql.parameters.``:

    - ``PG_CONFIG``: name of the Postgres configuration;
    - ``PG_VALUE``: value for ``PG_CONFIG``. If it is ``nulll``, then ``PG_CONFIG`` will be removed from the dynamic configuration.

- ``--apply``: apply dynamic configuration from a given file. It is similar to specifying multiple ``-s`` / ``--set``, with each configuration from ``CONFIG_FILE``:

    - ``CONFIG_FILE``: path to a file containing the dynamic configuration to be applied, in YAML format. Use ``-`` if you want to read from ``stdin``.

- ``--replace``: replace the dynamic configuration in the DCS with the dynamic configuration specified in a given file:

    - ``CONFIG_FILE``: path to a file containing the new dynamic configuration to take effect, in YAML format. Use ``-`` if you want to read from ``stdin``.

- ``--force``: skip confirmation prompts when changing the dynamic configuration. Useful for scripts.

- ``CLUSTER_NAME``: name of the Patroni cluster. If not given, ``patronictl`` will attempt to fetch that from ``scope`` configuration, if it exists.

Examples
""""""""

Change ``max_connections`` Postgres GUC:

.. code:: text

    patronictl -c postgres0.yml edit-config batman --pg max_connections="150" --force
    ---
    +++
    @@ -1,6 +1,8 @@
    loop_wait: 10
    maximum_lag_on_failover: 1048576
    postgresql:
    +  parameters:
    +    max_connections: 150
    pg_hba:
    - host replication replicator 127.0.0.1/32 md5
    - host all all 0.0.0.0/0 md5

    Configuration changed

Change ``loop_wait`` and ``ttl`` settings:

.. code:: text

    patronictl -c postgres0.yml edit-config batman --set loop_wait="15" --set ttl="45" --force
    ---
    +++
    @@ -1,4 +1,4 @@
    -loop_wait: 10
    +loop_wait: 15
    maximum_lag_on_failover: 1048576
    postgresql:
    pg_hba:
    @@ -6,4 +6,4 @@
    - host all all 0.0.0.0/0 md5
    use_pg_rewind: true
    retry_timeout: 10
    -ttl: 30
    +ttl: 45

    Configuration changed

Remove ``maximum_lag_on_failover`` setting from dynamic configuration:

.. code:: text

    patronictl -c postgres0.yml edit-config batman --set maximum_lag_on_failover="null" --force
    ---
    +++
    @@ -1,5 +1,4 @@
    loop_wait: 10
    -maximum_lag_on_failover: 1048576
    postgresql:
    pg_hba:
    - host replication replicator 127.0.0.1/32 md5

    Configuration changed
