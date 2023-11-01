.. _pause:

Pause/Resume mode for the cluster
=================================

The goal
--------

Under certain circumstances Patroni needs to temporarily step down from managing the cluster, while still retaining the cluster state in DCS. Possible use cases are uncommon activities on the cluster, such as major version upgrades or corruption recovery. During those activities nodes are often started and stopped for reasons unknown to Patroni, some nodes can be even temporarily promoted, violating the assumption of running only one primary. Therefore, Patroni needs to be able to "detach" from the running cluster, implementing an equivalent of the maintenance mode in Pacemaker.



The implementation
------------------

When Patroni runs in a paused mode, it does not change the state of PostgreSQL, except for the following cases:

- For each node, the member key in DCS is updated with the current information about the cluster. This causes Patroni to run read-only queries on a member node if the member is running.

- For the Postgres primary with the leader lock Patroni updates the lock. If the node with the leader lock stops being the primary (i.e. is demoted manually), Patroni will release the lock instead of promoting the node back.

- Manual unscheduled restart, manual unscheduled failover/switchover and reinitialize are allowed. No scheduled action is allowed. Manual switchover is only allowed if the node to switch over to is specified.

- If 'parallel' primaries are detected by Patroni, it emits a warning, but does not demote the primary without the leader lock.

- If there is no leader lock in the cluster, the running primary acquires the lock. If there is more than one primary node, then the first primary to acquire the lock wins. If there are no primary altogether, Patroni does not try to promote any replicas. There is an exception in this rule: if there is no leader lock because the old primary has demoted itself due to the manual promotion, then only the candidate node mentioned in the promotion request may take the leader lock. When the new leader lock is granted (i.e. after promoting a replica manually), Patroni makes sure the replicas that were streaming from the previous leader will switch to the new one.

- When Postgres is stopped, Patroni does not try to start it. When Patroni is stopped, it does not try to stop the Postgres instance it is managing.

- Patroni will not try to remove replication slots that don't represent the other cluster member or are not listed in the configuration of the permanent slots.

User guide
----------

``patronictl`` supports :ref:`pause <patronictl_pause>` and :ref:`resume <patronictl_resume>` commands.

One can also issue a ``PATCH`` request to the ``{namespace}/{cluster}/config`` key with ``{"pause": true/false/null}``
