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
----------------

For use cases where losing committed transactions is not permissible you can turn on Patroni's ``synchronous_mode``. When ``synchronous_mode`` is turned on Patroni will not promote a standby unless it is certain that the standby contains all transactions that may have returned a successful commit status to client [2]_. This means that the system may be unavailable for writes even though some servers are available. System administrators can still use manual failover commands to promote a standby even if it results in transaction loss.

When ``synchronous_mode`` is enabled it is possible to set a balance between durability and availability using ``replication_factor`` and ``minimum_replication_factor`` configuration variables. ``replication_facor`` sets the target level of replication to configure given enough replicating standby nodes are available. A cluster can perform an automatic failover when ``replication_factor`` - 1 members go offline at once before the system has time to re-evaluate cluster membership. Typically the time window for reconfiguring is one ``loop_wait`` cycle.

Turning on ``synchronous_mode`` does not guarantee multi node durability of commits under all circumstances. When not enough suitable standbys are available, primary server will still accept writes, but runs with reduced replication factor. When the primary fails while effective replication factor is 1 no standby will be promoted. Automatic promotion is similarly disabled when primary and effective replication factor - 1 standbys fail at once. If the node that used to be the primary or enough standbys to achieve quorum rejoin the cluster a new leader can be automatically determined, unless system administrator performed a manual failover. This behavior makes synchronous mode usable with 2 node clusters.

When it is absolutely necessary to guarantee that each write is stored durably
on at least two nodes, users can set ``minimum_replication_factor`` to a higher
value. This makes sure that no commit can succeed unless it's replicated to at
least this number of nodes. If not enough synchronous standby candidates are
available all client write requests will block until enough synchronous replicas
come online (unless replication is explicitly disabled for the transaction using
``synchronous_commit=off``.

For PostgreSQL versions before 9.6 maximum supported ``replication_factor`` is 2. For PostgreSQL versions 9.6 and up higher levels can be used. Starting from PostgreSQL version 10 quorum commit is supported. This means that any combination of standbys can acknowledge the commit. In earlier versions PostgreSQL picks specific standbys for synchronization. On standby failure the replication connection has to time out or the standbys cluster membership has to expire before synchronization is switched to another standby or switched off and commits can succeed. This can take up to ``ttl`` + ``loop_wait`` seconds. Quorum commit helps to reduce worst case latencies even during normal operation as a higher latency of replicating to one standby can be compensated by other standbys. Manually shutting down or restarting a standby will not cause a commit service interruption. Standby will signal the primary to release itself from synchronous standby duties before PostgreSQL shutdown is initiated.

You can ensure that a standby never becomes a synchronous standby by setting ``nosync`` tag to true. This is recommended to set for standbys that are behind slow network connections and would cause performance degradation when becoming a synchronous standby.

Synchronous mode can be switched on and off using ``patronictl edit-config`` command. See :ref:`dynamic configuration <dynamic_configuration>` for instructions.


Synchronous mode implementation
-------------------------------

When in synchronous mode Patroni maintains synchronization state in the DCS, containing the latest primary, number of nodes required for quorum and nodes currently eligible to vote on quorum. In steady state the nodes voting on quorum are the leader and all synchronous standbys. This state is updated with strict ordering constraints with regards to node promotion and ``synchronous_standby_names`` to ensure that at all times any subset of voters that can achieve quorum is contained to have at least one node having the latest successful commit.

On each HA loop iteration Patroni re-evaluates synchronous standby choices and quorum based on node availability and requested cluster configuration. In PostgreSQL versions above 9.6 all eligible nodes are added as synchronous standbys as soon as their replication catches up to leader. In older PostgreSQL versions if the current synchronous standby is connected and has not requested its synchronous status to be removed it remains picked. Otherwise the cluster member available for sync that is furthest ahead in replication is picked.


.. [1] The data is still there, but recovering it requires a manual recovery effort by data recovery specialists. When Patroni is allowed to rewind with ``use_pg_rewind`` the forked timeline will be automatically erased to rejoin the failed primary with the cluster.

.. [2] Clients can change the behavior per transaction using PostgreSQL's ``synchronous_commit`` setting. Transactions with ``synchronous_commit`` values of ``off`` and ``local`` may be lost on fail over, but will not be blocked by replication delays.
