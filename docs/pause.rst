Pause/Resume mode for the cluster
=================================

The goal
--------

Under certain circumstances Patroni needs to temporary step down from managing the cluster, while still retaining the cluster state in DCS. Possible use cases are uncommon activities on the cluster, such as major version upgrades or corruption recovery. During those activities nodes are often started and stopped for the reason unknown to Patroni, some nodes can be even temporary promoted, violating the assumption of running only one master.


The implementation
------------------

When Patroni runs in a paused mode, it does not change the state of PostgreSQL, except for the following cases:

- For each node, the member key in DCS is updated with the current information about the cluster. This causes Patroni to run read-only queries on a member node if the member is running.

- For the leader node, Patroni maintains the leader key and promotes the node holding the leader key if it is not the master.

- Manual unscheduled restart and manual failover are allowed. Manual failover is only allowed if the node to failover to is specified. In the paused mode, manual failover does not require a running master node.

- If 'parallel' masters are detected by Patroni, it emits a warning, but does not demote the masters without the leader lock.

- If there is no leader lock in the cluster, Patroni promotes a running master. If there is more than one master node, then the first master node to try to acquire the lock wins. There is an exception in this rule: if there is no leader lock because the old master has demoted itself due to the manual promotion, than only the node mentioned in the promotion may take the leader lock.

When the new leader is promoted, Patroni makes sure the replicas that were streaming from the previous leader will switch to the new one.


User guide
----------

``patronictl`` supports ``pause`` and ``resume`` commands.

One can also issue a ``PATCH`` request to the ``{namespace}/{cluster}/config`` key with ``{'pause': True/False}``
