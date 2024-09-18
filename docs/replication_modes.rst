.. _replication_modes:

=================
Replication modes
=================

Patroni uses PostgreSQL streaming replication. For more information about streaming replication, see the `Postgres documentation <http://www.postgresql.org/docs/current/static/warm-standby.html#STREAMING-REPLICATION>`__. By default Patroni configures PostgreSQL for asynchronous replication. Choosing your replication schema is dependent on your business considerations. Investigate both async and sync replication, as well as other HA solutions, to determine which solution is best for you.


Asynchronous mode durability
============================

In asynchronous mode the cluster is allowed to lose some committed transactions to ensure availability. When the primary server fails or becomes unavailable for any other reason Patroni will automatically promote a sufficiently healthy standby to primary. Any transactions that have not been replicated to that standby remain in a "forked timeline" on the primary, and are effectively unrecoverable [1]_.

The amount of transactions that can be lost is controlled via ``maximum_lag_on_failover`` parameter. Because the primary transaction log position is not sampled in real time, in reality the amount of lost data on failover is worst case bounded by  ``maximum_lag_on_failover`` bytes of transaction log plus the amount that is written in the last ``ttl`` seconds (``loop_wait``/2 seconds in the average case). However typical steady state replication delay is well under a second.

By default, when running leader elections, Patroni does not take into account the current timeline of replicas, what in some cases could be undesirable behavior. You can prevent the node not having the same timeline as a former primary become the new leader by changing the value of ``check_timeline`` parameter to ``true``.


PostgreSQL synchronous replication
==================================

You can use Postgres's `synchronous replication <http://www.postgresql.org/docs/current/static/warm-standby.html#SYNCHRONOUS-REPLICATION>`__ with Patroni. Synchronous replication ensures consistency across a cluster by confirming that writes are written to a secondary before returning to the connecting client with a success. The cost of synchronous replication: increased latency and reduced throughput on writes. This throughput will be entirely based on network performance.

In hosted datacenter environments (like AWS, Rackspace, or any network you do not control), synchronous replication significantly increases the variability of write performance. If followers become inaccessible from the leader, the leader effectively becomes read-only.

To enable a simple synchronous replication test, add the following lines to the ``parameters`` section of your YAML configuration files:

.. code:: YAML

        synchronous_commit: "on"
        synchronous_standby_names: "*"

When using PostgreSQL synchronous replication, use at least three Postgres data nodes to ensure write availability if one host fails.

Using PostgreSQL synchronous replication does not guarantee zero lost transactions under all circumstances. When the primary and the secondary that is currently acting as a synchronous replica fail simultaneously a third node that might not contain all transactions will be promoted.


.. _synchronous_mode:

Synchronous mode
================

For use cases where losing committed transactions is not permissible you can turn on Patroni's ``synchronous_mode``. When ``synchronous_mode`` is turned on Patroni will not promote a standby unless it is certain that the standby contains all transactions that may have returned a successful commit status to client [2]_. This means that the system may be unavailable for writes even though some servers are available. System administrators can still use manual failover commands to promote a standby even if it results in transaction loss.

Turning on ``synchronous_mode`` does not guarantee multi node durability of commits under all circumstances. When no suitable standby is available, primary server will still accept writes, but does not guarantee their replication. When the primary fails in this mode no standby will be promoted. When the host that used to be the primary comes back it will get promoted automatically, unless system administrator performed a manual failover. This behavior makes synchronous mode usable with 2 node clusters.

When ``synchronous_mode`` is on and a standby crashes, commits will block until next iteration of Patroni runs and switches the primary to standalone mode (worst case delay for writes ``ttl`` seconds, average case ``loop_wait``/2 seconds). Manually shutting down or restarting a standby will not cause a commit service interruption. Standby will signal the primary to release itself from synchronous standby duties before PostgreSQL shutdown is initiated.

When it is absolutely necessary to guarantee that each write is stored durably
on at least two nodes, enable ``synchronous_mode_strict`` in addition to the
``synchronous_mode``. This parameter prevents Patroni from switching off the
synchronous replication on the primary when no synchronous standby candidates
are available. As a downside, the primary is not be available for writes
(unless the Postgres transaction explicitly turns off ``synchronous_mode``),
blocking all client write requests until at least one synchronous replica comes
up.

You can ensure that a standby never becomes the synchronous standby by setting ``nosync`` tag to true. This is recommended to set for standbys that are behind slow network connections and would cause performance degradation when becoming a synchronous standby. Setting tag ``nostream`` to true will also have the same effect.

Synchronous mode can be switched on and off using ``patronictl edit-config`` command or via Patroni REST interface. See :ref:`dynamic configuration <dynamic_configuration>` for instructions.

Note: Because of the way synchronous replication is implemented in PostgreSQL it is still possible to lose transactions even when using ``synchronous_mode_strict``. If the PostgreSQL backend is cancelled while waiting to acknowledge replication (as a result of packet cancellation due to client timeout or backend failure) transaction changes become visible for other backends. Such changes are not yet replicated and may be lost in case of standby promotion.


Synchronous Replication Factor
==============================

The parameter ``synchronous_node_count`` is used by Patroni to manage the number of synchronous standby databases. It is set to ``1`` by default. It has no effect when ``synchronous_mode`` is set to ``off``. When enabled, Patroni manages the precise number of synchronous standby databases based on parameter ``synchronous_node_count`` and adjusts the state in DCS & ``synchronous_standby_names`` in PostgreSQL as members join and leave. If the parameter is set to a value higher than the number of eligible nodes it will be automatically reduced by Patroni.


Maximum lag on synchronous node
===============================

By default Patroni sticks to nodes that are declared as ``synchronous``, according to the ``pg_stat_replication`` view, even when there are other nodes ahead of it. This is done to minimize the number of changes of ``synchronous_standby_names``. To change this behavior one may use ``maximum_lag_on_syncnode`` parameter. It controls how much lag the replica can have to still be considered as "synchronous".

Patroni utilizes the max replica LSN if there is more than one standby, otherwise it will use leader's current wal LSN. The default is ``-1``, and Patroni will not take action to swap a synchronous unhealthy standby when the value is set to ``0`` or less. Please set the value high enough so that Patroni won't swap synchronous standbys frequently during high transaction volume.


Synchronous mode implementation
===============================

When in synchronous mode Patroni maintains synchronization state in the DCS (``/sync`` key), containing the latest primary and current synchronous standby databases. This state is updated with strict ordering constraints to ensure the following invariants:

- A node must be marked as the latest leader whenever it can accept write transactions. Patroni crashing or PostgreSQL not shutting down can cause violations of this invariant.

- A node must be set as the synchronous standby in PostgreSQL as long as it is published as the synchronous standby in the ``/sync`` key in DCS..

- A node that is not the leader or current synchronous standby is not allowed to promote itself automatically.

Patroni will only assign one or more synchronous standby nodes based on ``synchronous_node_count`` parameter to ``synchronous_standby_names``.

On each HA loop iteration Patroni re-evaluates synchronous standby nodes choice. If the current list of synchronous standby nodes are connected and has not requested its synchronous status to be removed it remains picked. Otherwise the cluster members available for sync that are furthest ahead in replication are picked.

Example:
---------

``/config`` key in DCS
^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: YAML

    synchronous_mode: on
    synchronous_node_count: 2
    ...

``/sync`` key in DCS
^^^^^^^^^^^^^^^^^^^^

.. code-block:: JSON

    {
        "leader": "node0",
        "sync_standby": "node1,node2"
    }

postgresql.conf
^^^^^^^^^^^^^^^

.. code-block:: INI

    synchronous_standby_names = 'FIRST 2 (node1,node2)'


In the above examples only nodes ``node1`` and ``node2`` are known to be synchronous and allowed to be automatically promoted if the primary (``node0``) fails.


.. _quorum_mode:

Quorum commit mode
==================

Starting from PostgreSQL v10 Patroni supports quorum-based synchronous replication.

In this mode, Patroni maintains synchronization state in the DCS, containing the latest known primary, the number of nodes required for quorum, and the nodes currently eligible to vote on quorum. In steady state, the nodes voting on quorum are the leader and all synchronous standbys. This state is updated with strict ordering constraints, with regards to node promotion and ``synchronous_standby_names``, to ensure that at all times any subset of voters that can achieve quorum includes at least one node with the latest successful commit.

On each iteration of HA loop, Patroni re-evaluates synchronous standby choices and quorum, based on node availability and requested cluster configuration. In PostgreSQL versions above 9.6 all eligible nodes are added as synchronous standbys as soon as their replication catches up to leader.

Quorum commit helps to reduce worst case latencies, even during normal operation, as a higher latency of replicating to one standby can be compensated by other standbys.

The quorum-based synchronous mode could be enabled by setting ``synchronous_mode`` to ``quorum`` using ``patronictl edit-config`` command or via Patroni REST interface. See :ref:`dynamic configuration <dynamic_configuration>` for instructions.

Other parameters, like ``synchronous_node_count``, ``maximum_lag_on_syncnode``, and ``synchronous_mode_strict`` continue to work the same way as with ``synchronous_mode=on``.

Example:
---------

``/config`` key in DCS
^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: YAML

    synchronous_mode: quorum
    synchronous_node_count: 2
    ...

``/sync`` key in DCS
^^^^^^^^^^^^^^^^^^^^

.. code-block:: JSON

    {
        "leader": "node0",
        "sync_standby": "node1,node2,node3",
        "quorum": 1
    }

postgresql.conf
^^^^^^^^^^^^^^^

.. code-block:: INI

    synchronous_standby_names = 'ANY 2 (node1,node2,node3)'


If the primary (``node0``) failed, in the above example two of the ``node1``, ``node2``, ``node3`` will have the latest transaction received, but we don't know which ones. To figure out whether the node ``node1`` has received the latest transaction, we need to compare its LSN with the LSN on **at least** one node (``quorum=1`` in the ``/sync`` key) among ``node2`` and ``node3``. If ``node1`` isn't behind of at least one of them, we can guarantee that there will be no user visible data loss if ``node1`` is promoted.


.. [1] The data is still there, but recovering it requires a manual recovery effort by data recovery specialists. When Patroni is allowed to rewind with ``use_pg_rewind`` the forked timeline will be automatically erased to rejoin the failed primary with the cluster. However, for ``use_pg_rewind`` to function properly, either the cluster must be initialized with ``data page checksums`` (``--data-checksums`` option for ``initdb``) and/or ``wal_log_hints`` must be set to ``on``.

.. [2] Clients can change the behavior per transaction using PostgreSQL's ``synchronous_commit`` setting. Transactions with ``synchronous_commit`` values of ``off`` and ``local`` may be lost on fail over, but will not be blocked by replication delays.
