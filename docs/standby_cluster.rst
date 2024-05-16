.. _standby_cluster:

Standby cluster
---------------

Patroni also support running cascading replication to a remote datacenter
(region) using a feature that is called "standby cluster". This type of
clusters has:

* "standby leader", that behaves pretty much like a regular cluster leader,
  except it replicates from a remote node.

* cascade replicas, that are replicating from standby leader.

Standby leader holds and updates a leader lock in DCS. If the leader lock
expires, cascade replicas will perform an election to choose another leader
from the standbys.

There is no further relationship between the standby cluster and the primary
cluster it replicates from, in particular, they must not share the same DCS
scope if they use the same DCS. They do not know anything else from each other
apart from replication information. Also, the standby cluster is not being
displayed in :ref:`patronictl_list` or :ref:`patronictl_topology` output on the
primary cluster.

For the sake of flexibility, you can specify methods of creating a replica and
recovery WAL records when a cluster is in the "standby mode" by providing
:ref:`create_replica_methods <custom_replica_creation>` key in
`standby_cluster` section. It is distinct from creating replicas, when cluster
is detached and functions as a normal cluster, which is controlled by
`create_replica_methods` in `postgresql` section. Both "standby" and "normal"
`create_replica_methods` reference  keys in `postgresql` section.

To configure such cluster you need to specify the section ``standby_cluster``
in a patroni configuration:

.. code:: YAML

    bootstrap:
        dcs:
            standby_cluster:
                host: 1.2.3.4
                port: 5432
                primary_slot_name: patroni
                create_replica_methods:
                - basebackup

Note, that these options will be applied only once during cluster bootstrap,
and the only way to change them afterwards is through DCS.

Patroni expects to find `postgresql.conf` or `postgresql.conf.backup` in PGDATA
of the remote primary and will not start if it does not find it after a
basebackup. If the remote primary keeps its `postgresql.conf` elsewhere, it is
your responsibility to copy it to PGDATA.

If you use replication slots on the standby cluster, you must also create the
corresponding replication slot on the primary cluster.  It will not be done
automatically by the standby cluster implementation.  You can use Patroni's
permanent replication slots feature on the primary cluster to maintain a
replication slot with the same name as ``primary_slot_name``, or its default
value if ``primary_slot_name`` is not provided.

In case the remote site doesn't provide a single endpoint that connects to a
primary, one could list all hosts of the source cluster in the
``standby_cluster.host`` section.  When ``standby_cluster.host`` contains
multiple hosts separated by commas, Patroni will:

* add ``target_session_attrs=read-write`` to the ``primary_conninfo`` on the
  standby leader node.
* use ``target_session_attrs=read-write`` when trying to determine whether we
  need to run ``pg_rewind`` or when executing ``pg_rewind`` on all nodes of the
  standby cluster.
* It is important to note that for ``pg_rewind`` to operate successfully, 
  either the cluster must be initialized with ``data page checksums`` 
  (``--data-checksums`` option for ``initdb``) and/or ``wal_log_hints`` must be set to ``on``.
  Otherwise, ``pg_rewind`` will not function properly.
  
There is also a possibility to replicate the standby cluster from another
standby cluster or from a standby member of the primary cluster: for that, you
need to define a single host in the ``standby_cluster.host`` section. However,
you need to beware that in this case ``pg_rewind`` will fail to execute on the
standby cluster.
