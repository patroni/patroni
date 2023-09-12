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
-----

``patronictl`` exposes several handy operations. This section is intended to describe each of them.

Before jumping into each of the sub-commands of ``patronictl``, be aware that ``patronictl`` itself has the following command-line arguments:

``-c`` / ``--config-file``
    As explained before, used to provide a path to a configuration file for ``patronictl``.

``-d`` / ``--dcs-url`` / ``--dcs``
    Provide a connection string to the DCS used by Patroni.

    This argument can be used either to override the DCS settings from the ``patronictl`` configuration, or to define it if it's missing in the configuration.

    The value should be in the format ``DCS://HOST:PORT``, e.g. ``etcd3://localhost:2379`` to connect to etcd v3 running on ``localhost``.

``-k`` / ``--insecure``
    Flag to bypass validation of REST API server SSL certificate.

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

    We will use this same syntax when describing ``patronictl`` sub-commands in the following sub-sections.
    Also, when describing sub-commands in the following sub-sections, the commands' synposis should be seen as a replacement for the ``SUBCOMMAND`` in the above synopsis.

In the following sub-sections you can find a description of each command implemented by ``patronictl``. For sake of example, we will use the configuration files present in the GitHub repository of Patroni (files ``postgres0.yml``, ``postgres1.yml`` and ``postgres2.yml``).

patronictl dsn
^^^^^^^^^^^^^^

Synopsis
""""""""

.. code:: text

    dsn
      [ CLUSTER_NAME ]
      [ { { -r | --role } { leader | primary | standby-leader | replica | standby | any | master } | { -m | --member } MEMBER_NAME } ]
      [ --group CITUS_GROUP ]

Description
"""""""""""

``patronictl dsn`` gets the connection string to one member of the Patroni cluster.

If multiple members match the parameters of this command, one of them will be chosen, prioritizing the primary node.

Parameters
""""""""""

``CLUSTER_NAME``
    Name of the Patroni cluster.

    If not given, ``patronictl`` will attempt to fetch that from ``scope`` configuration, if it exists.

``-r`` / ``--role``
    Choose a member that has the given role.

    Role can be one of:

    - ``leader``: the leader of either a regular Patroni cluster or a standby Patroni cluster; or
    - ``primary``: the leader of a regular Patroni cluster; or
    - ``standby-leader``: the leader of a standby Patroni cluster; or
    - ``replica``: a replica of a Patroni cluster; or
    - ``standby``: same as ``replica``; or
    - ``any``: any role. Same as omitting this parameter; or
    - ``master``: same as ``primary``.

``-m`` / ``--member``
    Choose a member of the cluster with the given name.

    ``MEMBER_NAME`` is the name of the member.

``--group``
    Choose a member that is part of the given Citus group.

    ``CITUS_GROUP`` is the ID of the Citus group.

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
^^^^^^^^^^^^^^^^^^^^^^

Synopsis
""""""""

.. code:: text

    edit-config
      [ CLUSTER_NAME ]
      [ --group CITUS_GROUP ]
      [ { -q | --quiet } ]
      [ { -s | --set } CONFIG="VALUE" [, ... ] ]
      [ { -p | --pg } PG_CONFIG="PG_VALUE" [, ... ] ]
      [ { --apply | --replace } CONFIG_FILE ]
      [ --force ]

Description
"""""""""""

``patronictl edit-config`` changes the dynamic configuration of the cluster and updates the DCS with that.

.. note::
    When invoked through a TTY the command attempts to show a diff of the dynamic configuration through a pager. By default it attempts to use either ``less`` or ``more``. If you want to use a different pager, set ``PAGER`` environment variable with the desired pager.

Parameters
""""""""""

``CLUSTER_NAME``
    Name of the Patroni cluster.

    If not given, ``patronictl`` will attempt to fetch that from ``scope`` configuration, if it exists.

``--group``
    Change dynamic configuration of the given Citus group.

    ``CITUS_GROUP`` is the ID of the Citus group.

``-q`` / ``--quiet``
    Flag to skip showing the configuration diff.

``-s`` / ``--set``
    Set a given dynamic configuration option with a given value.

    ``CONFIG`` is the name of the dynamic configuration path in the YAML tree, with levels joined by ``.`` .

    ``VALUE`` is the value for ``CONFIG``. If it is ``null``, then ``CONFIG`` will be removed from the dynamic configuration.

``-p`` / ``--pg``
    Set a given dynamic Postgres configuration option with the given value.

    It is essentially a shorthand for ``--s`` / ``--set`` with ``CONFIG`` prepended with ``postgresql.parameters.``.

    ``PG_CONFIG`` is the name of the Postgres configuration to be set.

    ``PG_VALUE`` is the value for ``PG_CONFIG``. If it is ``nulll``, then ``PG_CONFIG`` will be removed from the dynamic configuration.

``--apply``
    Apply dynamic configuration from the given file.

    It is similar to specifying multiple ``-s`` / ``--set`` options, one for each configuration from ``CONFIG_FILE``.

    ``CONFIG_FILE`` is the path to a file containing the dynamic configuration to be applied, in YAML format. Use ``-`` if you want to read from ``stdin``.

``--replace``
    Replace the dynamic configuration in the DCS with the dynamic configuration specified in the given file.

    ``CONFIG_FILE`` is the path to a file containing the new dynamic configuration to take effect, in YAML format. Use ``-`` if you want to read from ``stdin``.

``--force``
    Flag to skip confirmation prompts when changing the dynamic configuration.

    Useful for scripts.

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

patronictl failover
^^^^^^^^^^^^^^^^^^^

Synopsis
""""""""

.. code:: text

    failover
      [ CLUSTER_NAME ]
      [ --group CITUS_GROUP ]
      [ { --leader | --primary | --master } LEADER_NAME ]
      --candidate CANDIDATE_NAME
      [ --force ]

Description
"""""""""""

``patronictl failover`` performs a manual failover in the cluster.

It is designed to be used when the cluster is not healthy, e.g.:

- There is no leader; or
- There is no synchronous standby available in a synchronous cluster.

.. note::
    Nothing prevents you from running ``patronictl failover`` in a healthy cluster. However, we recommend using ``patronictl switchover`` in that case.

Parameters
""""""""""

``CLUSTER_NAME``
    Name of the Patroni cluster.

    If not given, ``patronictl`` will attempt to fetch that from ``scope`` configuration, if it exists.

``--group``
    Perform a failover in the given Citus group.

    ``CITUS_GROUP`` is the ID of the Citus group.

``--leader`` / ``--primary`` / ``--master``
    Indicate who is the expected leader at failover time.

    If given, a switchover is performed instead of a failover.

    ``LEADER_NAME`` should match the name of the current leader in the cluster.

``--candidate``
    The node to be promoted on failover.

    ``CANDIDATE_NAME`` is the name of the node to be promoted.

``--force``
    Flag to skip confirmation prompts when performing the failover.

    Useful for scripts.

Examples
""""""""

Failover to node ``postgresql2``:

.. code:: text

    patronictl -c postgres0.yml failover batman --candidate postgresql2 --force
    Current cluster topology
    + Cluster: batman (7277694203142172922) -+-----------+----+-----------+
    | Member      | Host           | Role    | State     | TL | Lag in MB |
    +-------------+----------------+---------+-----------+----+-----------+
    | postgresql0 | 127.0.0.1:5432 | Leader  | running   |  3 |           |
    | postgresql1 | 127.0.0.1:5433 | Replica | streaming |  3 |         0 |
    | postgresql2 | 127.0.0.1:5434 | Replica | streaming |  3 |         0 |
    +-------------+----------------+---------+-----------+----+-----------+
    2023-09-12 11:52:27.50978 Successfully failed over to "postgresql2"
    + Cluster: batman (7277694203142172922) -+---------+----+-----------+
    | Member      | Host           | Role    | State   | TL | Lag in MB |
    +-------------+----------------+---------+---------+----+-----------+
    | postgresql0 | 127.0.0.1:5432 | Replica | stopped |    |   unknown |
    | postgresql1 | 127.0.0.1:5433 | Replica | running |  3 |         0 |
    | postgresql2 | 127.0.0.1:5434 | Leader  | running |  3 |           |
    +-------------+----------------+---------+---------+----+-----------+


patronictl flush
^^^^^^^^^^^^^^^^

Synopsis
""""""""

.. code:: text

    flush
      CLUSTER_NAME
      [ MEMBER_NAME [, ... ] ]
      { restart | switchover }
      [ --group CITUS_GROUP ]
      [ { -r | --role } { leader | primary | standby-leader | replica | standby | any | master } ]
      [ --force ]

Description
"""""""""""

``patronictl flush`` discards scheduled events, if any.

Parameters
""""""""""

``CLUSTER_NAME``
    Name of the Patroni cluster.

``MEMBER_NAME``
    Discard scheduled events for the given Patroni member(s).

    .. note::
        Only used if discarding scheduled restart events.

``restart``
    Discard scheduled restart events.

``switchover``
    Discard scheduled switchover event.

``--group``
    Discard scheduled events from the given Citus group.

    ``CITUS_GROUP`` is the ID of the Citus group.

``-r`` / ``--role``
    Discard scheduled events for members that have the given role.

    Role can be one of:

    - ``leader``: the leader of either a regular Patroni cluster or a standby Patroni cluster; or
    - ``primary``: the leader of a regular Patroni cluster; or
    - ``standby-leader``: the leader of a standby Patroni cluster; or
    - ``replica``: a replica of a Patroni cluster; or
    - ``standby``: same as ``replica``; or
    - ``any``: any role. Same as omitting this parameter; or
    - ``master``: same as ``primary``.

    .. note::
        Only used if discarding scheduled restart events.

``--force``
    Flag to skip confirmation prompts when performing the flush.

    Useful for scripts.

Examples
""""""""

Discard a scheduled switchover event:

.. code:: text

    patronictl -c postgres0.yml flush batman switchover --force
    Success: scheduled switchover deleted

Discard scheduled restart of all standby nodes:

.. code:: text

    patronictl -c postgres0.yml flush batman restart -r replica --force
    + Cluster: batman (7277694203142172922) -+-----------+----+-----------+---------------------------+
    | Member      | Host           | Role    | State     | TL | Lag in MB | Scheduled restart         |
    +-------------+----------------+---------+-----------+----+-----------+---------------------------+
    | postgresql0 | 127.0.0.1:5432 | Leader  | running   |  5 |           | 2023-09-12T17:17:00+00:00 |
    | postgresql1 | 127.0.0.1:5433 | Replica | streaming |  5 |         0 | 2023-09-12T17:17:00+00:00 |
    | postgresql2 | 127.0.0.1:5434 | Replica | streaming |  5 |         0 | 2023-09-12T17:17:00+00:00 |
    +-------------+----------------+---------+-----------+----+-----------+---------------------------+
    Success: flush scheduled restart for member postgresql1
    Success: flush scheduled restart for member postgresql2

Discard scheduled restart of nodes ``postgresql0`` and ``postgresql1``:

.. code:: text

    patronictl -c postgres0.yml flush batman postgresql0 postgresql1 restart --force
    + Cluster: batman (7277694203142172922) -+-----------+----+-----------+---------------------------+
    | Member      | Host           | Role    | State     | TL | Lag in MB | Scheduled restart         |
    +-------------+----------------+---------+-----------+----+-----------+---------------------------+
    | postgresql0 | 127.0.0.1:5432 | Leader  | running   |  5 |           | 2023-09-12T17:17:00+00:00 |
    | postgresql1 | 127.0.0.1:5433 | Replica | streaming |  5 |         0 | 2023-09-12T17:17:00+00:00 |
    | postgresql2 | 127.0.0.1:5434 | Replica | streaming |  5 |         0 | 2023-09-12T17:17:00+00:00 |
    +-------------+----------------+---------+-----------+----+-----------+---------------------------+
    Success: flush scheduled restart for member postgresql0
    Success: flush scheduled restart for member postgresql1

patronictl history
^^^^^^^^^^^^^^^^^^

Synopsis
""""""""

.. code:: text

    history
      [ CLUSTER_NAME ]
      [ --group CITUS_GROUP ]
      [ { -f | --format } { pretty | tsv | json | yaml } ]

Description
"""""""""""

``patronictl history`` shows a history of failover and/or switchover events from the cluster, if any.

The following information is included in the output:

``TL``
    Postgres timeline at which the event occurred.

``LSN``
    Postgres LSN at which the event occurred.

``Reason``
    Reason fetched from the Postgres ``.history`` file.

``Timestamp``
    Time when the event occurred.

``New Leader``
    Patroni member that has been promoted during the event.

Parameters
""""""""""

``CLUSTER_NAME``
    Name of the Patroni cluster.

    If not given, ``patronictl`` will attempt to fetch that from ``scope`` configuration, if it exists.

``--group``
    Show history of events from the given Citus group.

    ``CITUS_GROUP`` is the ID of the Citus group.

``-f`` / ``--format``
    How to format the list of events in the output.

    Format can be one of:

    - ``pretty``: prints history as a pretty table; or
    - ``tsv``: prints history as tabular information, with columns delimited by ``\t``; or
    - ``json``: prints history in JSON format; or
    - ``yaml``: prints history in YAML format.

    The default is ``pretty``.

``--force``
    Flag to skip confirmation prompts when performing the flush.

    Useful for scripts.

Examples
""""""""

Show the history of events:

.. code:: text

    patronictl -c postgres0.yml history batman
    +----+----------+------------------------------+----------------------------------+-------------+
    | TL |      LSN | Reason                       | Timestamp                        | New Leader  |
    +----+----------+------------------------------+----------------------------------+-------------+
    |  1 | 24392648 | no recovery target specified | 2023-09-11T22:11:27.125527+00:00 | postgresql0 |
    |  2 | 50331864 | no recovery target specified | 2023-09-12T11:34:03.148097+00:00 | postgresql0 |
    |  3 | 83886704 | no recovery target specified | 2023-09-12T11:52:26.948134+00:00 | postgresql2 |
    |  4 | 83887280 | no recovery target specified | 2023-09-12T11:53:09.620136+00:00 | postgresql0 |
    +----+----------+------------------------------+----------------------------------+-------------+

Show the history of events in YAML format:

.. code:: text

    patronictl -c postgres0.yml history batman -f yaml
    - LSN: 24392648
      New Leader: postgresql0
      Reason: no recovery target specified
      TL: 1
      Timestamp: '2023-09-11T22:11:27.125527+00:00'
    - LSN: 50331864
      New Leader: postgresql0
      Reason: no recovery target specified
      TL: 2
      Timestamp: '2023-09-12T11:34:03.148097+00:00'
    - LSN: 83886704
      New Leader: postgresql2
      Reason: no recovery target specified
      TL: 3
      Timestamp: '2023-09-12T11:52:26.948134+00:00'
    - LSN: 83887280
      New Leader: postgresql0
      Reason: no recovery target specified
      TL: 4
      Timestamp: '2023-09-12T11:53:09.620136+00:00'

patronictl list
^^^^^^^^^^^^^^^

Synopsis
""""""""

.. code:: text

    list
      [ CLUSTER_NAME [, ... ] ]
      [ --group CITUS_GROUP ]
      [ { -e | --extended } ]
      [ { -t | --timestamp } ]
      [ { -f | --format } { pretty | tsv | json | yaml } ]
      [ { -W | { -w | --watch } TIME } ]

Description
"""""""""""

``patronictl list`` shows information about Patroni cluster and its members.

The following information is included in the output:

``Cluster``
    Name of the Patroni cluster.

``Member``
    Name of the Patroni member.

``Host``
    Host where the member is located.

``Role``
    Current role of the member.

    Can be one among:

    * ``Leader``: the current leader of a regular Patroni cluster; or
    * ``Standby Leader``: the current leader of a Patroni standby cluster; or
    * ``Sync Standby``: a synchronous standby of a Patroni cluster with synchronous mode enabled; or
    * ``Replica``: a regular standby of a Patroni cluster.

``State``
    Current state of Postgres in the Patroni member.

    Some examples among the possible states:

    * ``running``: if Postgres is currently up and running;
    * ``streaming``: if a replica and Postgres is currently streaming WALs from the primary node;
    * ``in archive recovery``: if a replica and Postgres is currently fetching WALs from the archive;
    * ``stopped``: if Postgres had been shut down;
    * ``crashed``: if Postgres has crashed.

``TL``
    Current Postgres timeline in the Patroni member.

``Lag in MB``
    Amount worth of replication lag in megabytes between the Patroni member and its upstream.

Besides that, the following information may be included in the output:

``System identifier``
    Postgres system identifier.

    .. note::
        Shown in the table header.

        Only shown if output format is ``pretty``.

``Group``
    Citus group ID.

    .. note::
        Shown in the table header.

        Only shown if a Citus cluster.

``Pending restart``
    ``*`` indicates the node needs a restart for some Postgres configuration to take effect. An empty value indicates the node does not require a restart.

    .. note::
        Shown as a member attribute.

        Shown if:

        - Printing in ``pretty`` or ``tsv`` format and with extended output enabled; or
        - If node requires a restart.

``Scheduled restart``
    Timestamp at which a restart has been scheduled for the Postgres instance managed by the Patroni member. An empty value indicates there is no scheduled restart for the member.

    .. note::
        Shown as a member attribute.

        Shown if:

        - Printing in ``pretty`` or ``tsv`` format and with extended output enabled; or
        - If node has a scheduled restart.

``Tags``
    Contains tags set for the Patroni member. An empty value indicates that either no tags have been configured, or that they have been configured with default values.

    .. note::
        Shown as a member attribute.

        Shown if:

        - Printing in ``pretty`` or ``tsv`` format and with extended output enabled; or
        - If node has any custom tags, or any default tags with non-default values.

``Scheduled switchover``
    Timestamp at which a switchover has been scheduled for the Patroni cluster, if any.

    .. note::
        Shown in the table footer.

        Only shown if there is a scheduled switchover, and output format is ``pretty``.

``Maintenance mode``

    If the cluster monitoring is currently paused.

    .. note::
        Shown in the table footer.

        Only shown if the cluster is paused, and output format is ``pretty``.

Parameters
""""""""""

``CLUSTER_NAME``
    Name of the Patroni cluster.

    If not given, ``patronictl`` will attempt to fetch that from ``scope`` configuration, if it exists.

``--group``
    Show information about members from the given Citus group.

    ``CITUS_GROUP`` is the ID of the Citus group.

``-e`` / ``--extended``
    Show extended information.

    Force showing ``Pending restart``, ``Scheduled restart`` and ``Tags`` attributes, even if their value is empty.

    .. note::
        Only applies to ``pretty`` and ``tsv`` output formats.

``-t`` / ``--timestamp``
    Print timestamp before printing information about the cluster and its members.

``-f`` / ``--format``
    How to format the list of events in the output.

    Format can be one of:

    - ``pretty``: prints history as a pretty table; or
    - ``tsv``: prints history as tabular information, with columns delimited by ``\t``; or
    - ``json``: prints history in JSON format; or
    - ``yaml``: prints history in YAML format.

    The default is ``pretty``.

``-W``
    Automatically refresh information every 2 seconds.

``-w`` / ``--watch``
    Automatically refresh information at the specified interval.

    ``TIME`` is the interval between refreshes, in seconds.

Examples
""""""""

Show information about the cluster in pretty format:

.. code:: text

    patronictl -c postgres0.yml list batman
    + Cluster: batman (7277694203142172922) -+-----------+----+-----------+
    | Member      | Host           | Role    | State     | TL | Lag in MB |
    +-------------+----------------+---------+-----------+----+-----------+
    | postgresql0 | 127.0.0.1:5432 | Leader  | running   |  5 |           |
    | postgresql1 | 127.0.0.1:5433 | Replica | streaming |  5 |         0 |
    | postgresql2 | 127.0.0.1:5434 | Replica | streaming |  5 |         0 |
    +-------------+----------------+---------+-----------+----+-----------+

Show information about the cluster in pretty format with extended columns:

.. code:: text

    patronictl -c postgres0.yml list batman -e
    + Cluster: batman (7277694203142172922) -+-----------+----+-----------+-----------------+-------------------+------+
    | Member      | Host           | Role    | State     | TL | Lag in MB | Pending restart | Scheduled restart | Tags |
    +-------------+----------------+---------+-----------+----+-----------+-----------------+-------------------+------+
    | postgresql0 | 127.0.0.1:5432 | Leader  | running   |  5 |           |                 |                   |      |
    | postgresql1 | 127.0.0.1:5433 | Replica | streaming |  5 |         0 |                 |                   |      |
    | postgresql2 | 127.0.0.1:5434 | Replica | streaming |  5 |         0 |                 |                   |      |
    +-------------+----------------+---------+-----------+----+-----------+-----------------+-------------------+------+

Show information about the cluster in YAML format, with timestamp of execution:

.. code:: text

    patronictl -c postgres0.yml list batman -f yaml -t
    2023-09-12 13:30:48
    - Cluster: batman
      Host: 127.0.0.1:5432
      Member: postgresql0
      Role: Leader
      State: running
      TL: 5
    - Cluster: batman
      Host: 127.0.0.1:5433
      Lag in MB: 0
      Member: postgresql1
      Role: Replica
      State: streaming
      TL: 5
    - Cluster: batman
      Host: 127.0.0.1:5434
      Lag in MB: 0
      Member: postgresql2
      Role: Replica
      State: streaming
      TL: 5

patronictl pause
^^^^^^^^^^^^^^^^

Synopsis
""""""""

.. code:: text

    pause
      [ CLUSTER_NAME ]
      [ --group CITUS_GROUP ]
      [ --wait ]

Description
"""""""""""

``patronictl pause`` temporarily puts the Patroni cluster in maintenance mode and disables automatic failover.

Parameters
""""""""""

``CLUSTER_NAME``
    Name of the Patroni cluster.

    If not given, ``patronictl`` will attempt to fetch that from ``scope`` configuration, if it exists.

``--group``
    Pause the given Citus group.

    ``CITUS_GROUP`` is the ID of the Citus group.

``--wait``
    Wait until all Patroni members are paused before returning control the the caller.

Examples
""""""""

Put the cluster in maintenance mode:

.. code:: text

    patronictl -c postgres0.yml pause batman --wait
    'pause' request sent, waiting until it is recognized by all nodes
    Success: cluster management is paused
