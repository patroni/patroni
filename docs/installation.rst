.. _installation:

Installation
============

Pre-requirements for Mac OS
---------------------------

To install requirements on a Mac, run the following:

.. code-block:: shell

    brew install postgresql etcd haproxy libyaml python

.. _psycopg2_install_options:

Psycopg
-------

Starting from `psycopg2-2.8`_ the binary version of psycopg2 will no longer be installed by default. Installing it from
the source code requires C compiler and postgres+python dev packages. Since in the python world it is not possible to
specify dependency as ``psycopg2 OR psycopg2-binary`` you will have to decide how to install it.

There are a few options available:

1. Use the package manager from your distro

.. code-block:: shell

    sudo apt-get install python3-psycopg2  # install psycopg2 module on Debian/Ubuntu
    sudo yum install python3-psycopg2      # install psycopg2 on RedHat/Fedora/CentOS

2. Specify one of `psycopg`, `psycopg2`, or `psycopg2-binary` in the :ref:`list of dependencies <extras>` when installing Patroni with pip.


.. _extras:

General installation for pip
----------------------------

Patroni can be installed with pip:

.. code-block:: shell

    pip install patroni[dependencies]

where ``dependencies`` can be either empty, or consist of one or more of the following:

etcd or etcd3
    `python-etcd` module in order to use Etcd as Distributed Configuration Store (DCS)
consul
    `py-consul` module in order to use Consul as DCS
zookeeper
    `kazoo` module in order to use Zookeeper as DCS
exhibitor
    `kazoo` module in order to use Exhibitor as DCS (same dependencies as for Zookeeper)
kubernetes
    `kubernetes` module in order to use Kubernetes as DCS in Patroni
raft
    `pysyncobj` module in order to use python Raft implementation as DCS
aws
    `boto3` in order to use AWS callbacks
jsonlogger
    `python-json-logger` module in order to enable :ref:`logging <log_settings>` in json format
systemd
    `systemd-python` in order to use sd_notify integration
all
    all of the above (except psycopg family)
psycopg3
    `psycopg[binary]>=3.0.0` module
psycopg2
    `psycopg2>=2.5.4` module
psycopg2-binary
    `psycopg2-binary` module

For example, the command in order to install Patroni together with psycopg3, dependencies for Etcd as a DCS, and AWS callbacks is:

.. code-block:: shell

    pip install patroni[psycopg3,etcd3,aws]

Note that external tools to call in the replica creation or custom bootstrap scripts (i.e. WAL-E) should be installed
independently of Patroni.

.. _package_installation:

Package installation on Linux
-----------------------------

Patroni packages may be available for your operating system, produced by the Postgres community for:

* RHEL, RockyLinux, AlmaLinux;
* Debian and Ubuntu;
* SUSE Enterprise Linux.

You can also find packages for direct dependencies of Patroni, like python modules that might not be available in
the official operating system repositories.

For more information see the `PGDG repository`_ documentation.

If you are on a RedHat Enterprise Linux derivative operating system you may also require packages from EPEL, see
`EPEL repository`_ documentation.

Once you have installed the PGDG repository for your OS you can install patroni.

.. note::

    Patroni packages are not maintained by the Patroni developers, but rather by the Postgres community. If you
    require support please first try connecting on `Postgres slack`_.

Installing on Debian derivatives
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

With PGDG repo installed, see :ref:`above <package_installation>`, install Patroni via apt run:

.. code-block:: shell

    apt-get install patroni

Installing on RedHat derivatives
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

With PGDG repo installed, see :ref:`above <package_installation>`, install patroni with an etcd DCS via dnf on RHEL 9
(and derivatives) run:

.. code-block:: shell

    dnf install patroni patroni-etcd

You can install etcd from PGDG if your RedHat derivative distribution does not provide packages. On the nodes that will
host the DCS run:

.. code-block:: shell

    dnf install 'dnf-command(config-manager)'
    dnf config-manager --enable pgdg-rhel9-extras
    dnf install etcd

You can replace the version of RHEL with `8` in the repo to make `pgdg-rhel8-extras` if needed. The repo name is still
`pgdg-rhelN-extras` on RockyLinux, AlmaLinux, Oracle Linux, etc...

Installing on SUSE Enterprise Linux
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You might need to enable the SUSE PackageHub repositories for some dependencies. see `SUSE PackageHub`_ documentation.

For SLES 15 with PGDG repo installed, see :ref:`above <package_installation>`, you can install patroni using:

.. code-block:: shell

    zypper install patroni patroni-etcd

With the SUSE PackageHub repo enabled you can also install etcd:

.. code-block:: shell

    SUSEConnect -p PackageHub/15.5/x86_64
    zypper install etcd

Upgrading
---------

Upgrading patroni is a very simple process, just update the software installation and restart the Patroni daemon on
each node in the cluster.

However, restarting the Patroni daemon will result in a Postgres database restart. In some situations this may cause
a failover of the primary node in your cluster, therefore it is recommended to put the cluster into maintenance mode
until the Patroni daemon restart has been completed.

To put the cluster in maintenance mode, run the following command on one of the patroni nodes:

.. code-block:: shell

    patronictl pause --wait

Then on each node in the cluster, perform the package upgrade required for your OS:

.. code-block:: shell

    apt-get update && apt-get install patroni patroni-etcd

Restart the patroni daemon process on each node:

.. code-block:: shell

    systemctl restart patroni

Then finally resume monitoring of Postgres with patroni to take it out of maintenance mode:

.. code-block:: shell

    patronictl resume --wait

The cluster will now be full operational with the new version of Patroni.

.. _psycopg2-2.8: http://initd.org/psycopg/articles/2019/04/04/psycopg-28-released/
.. _PGDG repository: https://www.postgresql.org/download/linux/
.. _EPEL repository: https://docs.fedoraproject.org/en-US/epel/
.. _SUSE PackageHub: https://packagehub.suse.com/how-to-use/
.. _Postgres slack: http://pgtreats.info/slack-invite
