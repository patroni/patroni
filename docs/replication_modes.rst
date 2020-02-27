.. _replication_modes:

=================
Replication modes
=================

Patroni uses PostgreSQL streaming replication. For more information about streaming replication, see the `Postgres documentation <http://www.postgresql.org/docs/current/static/warm-standby.html#STREAMING-REPLICATION>`__. By default Patroni configures PostgreSQL for asynchronous replication. Choosing your replication schema is dependent on your business considerations. Investigate both async and sync replication, as well as other HA solutions, to determine which solution is best for you.

Asynchronous mode durability
----------------------------

In asynchronous mode the cluster is allowed to lose some committed transactions to ensure availability. When the primary server fails or becomes unavailable for any other reason Patroni will automatically promote a sufficiently healthy standby to primary. Any transactions that have not been replicated to that standby remain in a "forked timeline" on the primary, and are effectively unrecoverable [1]_.

The amount of transactions that can be lost is controlled via ``maximum_lag_on_failover`` parameter. Because the primary transaction log position is not sampled in real time, in reality the amount of lost data on failover is worst case bounded by  ``maximum_lag_on_failover`` bytes of transaction log plus the amount that is written in the last ``ttl`` seconds (``loop_wait``/2 seconds in the average case). However typical steady state replication delay is well under a second.

By default, when running leader elections, Patroni does not take into account the current timeline of replicas, what in some cases could be undesirable behavior. You can prevent the node not having the same timeline as a former master become the new leader by changing the value of ``check_timeline`` parameter to ``true``.

PostgreSQL synchronous replication
----------------------------------

You can use Postgres's `synchronous replication <http://www.postgresql.org/docs/current/static/warm-standby.html#SYNCHRONOUS-REPLICATION>`__ with Patroni. Synchronous replication ensures consistency across a cluster by confirming that writes are written to a secondary before returning to the connecting client with a success. The cost of synchronous replication: reduced throughput on writes. This throughput will be entirely based on network performance.

In hosted datacenter environments (like AWS, Rackspace, or any network you do not control), synchronous replication significantly increases the variability of write performance. If followers become inaccessible from the leader, the leader effectively becomes read-only.

To enable a simple synchronous replication test, add the following lines to the ``parameters`` section of your YAML configuration files:

.. code:: YAML

        synchronous_commit: "on"
        synchronous_standby_names: "*"

When using PostgreSQL synchronous replication, use at least three Postgres data nodes to ensure write availability if one host fails.

Using PostgreSQL synchronous replication does not guarantee zero lost transactions under all circumstances. When the primary and the secondary that is currently acting as a synchronous replica fail simultaneously a third node that might not contain all transactions will be promoted.

.. _synchronous_mode:

Synchronous mode
----------------

For use cases where losing committed transactions is not permissible you can turn on Patroni's ``synchronous_mode``. When ``synchronous_mode`` is turned on Patroni will not promote a standby unless it is certain that the standby contains all transactions that may have returned a successful commit status to client [2]_. This means that the system may be unavailable for writes even though some servers are available. System administrators can still use manual failover commands to promote a standby even if it results in transaction loss.

Turning on ``synchronous_mode`` does not guarantee multi node durability of commits under all circumstances. When no suitable standby is available, primary server will still accept writes, but does not guarantee their replication. When the primary fails in this mode no standby will be promoted. When the host that used to be the primary comes back it will get promoted automatically, unless system administrator performed a manual failover. This behavior makes synchronous mode usable with 2 node clusters.

When ``synchronous_mode`` is on and a standby crashes, commits will block until next iteration of Patroni runs and switches the primary to standalone mode (worst case delay for writes ``ttl`` seconds, average case ``loop_wait``/2 seconds). Manually shutting down or restarting a standby will not cause a commit service interruption. Standby will signal the primary to release itself from synchronous standby duties before PostgreSQL shutdown is initiated.

When it is absolutely necessary to guarantee that each write is stored durably
on at least two nodes, enable ``synchronous_mode_strict`` in addition to the
``synchronous_mode``. This parameter prevents Patroni from switching off the
synchronous replication on the primary when no synchronous standby candidates
are available. As a downside, the primary is not be available for writes
(unless the Postgres transaction explicitly turns of ``synchronous_mode``),
blocking all client write requests until at least one synchronous replica comes
up.

You can ensure that a standby never becomes the synchronous standby by setting ``nosync`` tag to true. This is recommended to set for standbys that are behind slow network connections and would cause performance degradation when becoming a synchronous standby.

Synchronous mode can be switched on and off via Patroni REST interface. See :ref:`dynamic configuration <dynamic_configuration>` for instructions.

Note: Because of the way synchronous replication is implemented in PostgreSQL it is still possible to lose transactions even when using ``synchronous_mode_strict``. If the PostgreSQL backend is cancelled while waiting to acknowledge replication (as a result of packet cancellation due to client timeout or backend failure) transaction changes become visible for other backends. Such changes are not yet replicated and may be lost in case of standby promotion.


Synchronous mode implementation
-------------------------------

When in synchronous mode Patroni maintains synchronization state in the DCS, containing the latest primary and current synchronous standby. This state is updated with strict ordering constraints to ensure the following invariants:

- A node must be marked as the latest leader whenever it can accept write transactions. Patroni crashing or PostgreSQL not shutting down can cause violations of this invariant.

- A node must be set as the synchronous standby in PostgreSQL as long as it is published as the synchronous standby.

- A node that is not the leader or current synchronous standby is not allowed to promote itself automatically.

Patroni will only ever assign one standby to ``synchronous_standby_names`` because with multiple candidates it is not possible to know which node was acting as synchronous during the failure.

On each HA loop iteration Patroni re-evaluates synchronous standby choice. If the current synchronous standby is connected and has not requested its synchronous status to be removed it remains picked. Otherwise the cluster member available for sync that is furthest ahead in replication is picked.


.. [1] The data is still there, but recovering it requires a manual recovery effort by data recovery specialists. When Patroni is allowed to rewind with ``use_pg_rewind`` the forked timeline will be automatically erased to rejoin the failed primary with the cluster.

.. [2] Clients can change the behavior per transaction using PostgreSQL's ``synchronous_commit`` setting. Transactions with ``synchronous_commit`` values of ``off`` and ``local`` may be lost on fail over, but will not be blocked by replication delays.
