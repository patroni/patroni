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
      COMMAND

.. note::

    This is the syntax for the synopsis:

    - Options between square brackets are optional;
    - Options between curly brackets represent a "chose one of set" operation;
    - Things written in uppercase represent a literal that should be given a value to.

    We will use this same syntax when describing ``patronictl`` commands later. Also, when describing commands in the following sub-sections, the commands' synposis should be replace the ``COMMAND`` in the above synopsis.

In the following sub-sections you can find a description of each command implemented by ``patronictl``. For sake of example, we will use the configuration files present in the GitHub repository of Patroni (files ``postgres0.yml``, ``postgres1.yml`` and ``postgres2.yml``).

patronictl dsn
^^^^^^^^^^^^^^

Synopsis
""""""""

.. code:: text

    dsn [ { { -r | --role } { leader | primary | standby-leader | replica | standby | any | master } | { -m | --member } MEMBER_NAME } ]
      [ --group CITUS_GROUP ]
      [ CLUSTER_NAME ]

Description
"""""""""""

``patronictl dsn`` will get the connection string to one member of the Patroni cluster.

If multiple members match the parameters of this command, one of them will be chosen, prioritizing the primary node.

Parameters
""""""""""

- ``-r`` / ``--role``: chose a member that has the given role:

    - ``leader``: the leader of either a regular Patroni cluster or a standby Patroni cluster;
    - ``primary``: the leader of a regular Patroni cluster;
    - ``standby-leader``: the leader of a standby Patroni cluster;
    - ``replica``: a replica of a Patroni cluster;
    - ``standby``: same as ``replica``;
    - ``any``: any role. Same as omitting this parameter;
    - ``master``: same as ``primary``.

- ``-m`` / ``--member``: chose a member of the cluster with the given name:

    - ``MEMBER_NAME``: name of the member;

- ``--group``: chose a member that is part of the given Citus group:

    - ``CITUS_GROUP``: the ID of the Citus group;

- ``CLUSTER_NAME``: name of the Patroni cluster. If not given, ``patronictl`` will fetch that from ``scope`` configuration.

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
