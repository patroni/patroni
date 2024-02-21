.. _dcs_failsafe_mode:

DCS Failsafe Mode
=================

The problem
-----------

Patroni is heavily relying on Distributed Configuration Store (DCS) to solve the task of leader elections and detect network partitioning. That is, the node is allowed to run Postgres as the primary only if it can update the leader lock in DCS. In case the update of the leader lock fails, Postgres is immediately demoted and started as read-only. Depending on which DCS is used, the chances of hitting the "problem" differ. For example, with Etcd which is only used for Patroni, chances are close to zero, while with K8s API (backed by Etcd) it could be observed more frequently.


Reasons for the current implementation
---------------------------------------

The leader lock update failure could be caused by two main reasons:

1. Network partitioning
2. DCS being down

In general, it is impossible to distinguish between these two from a single node, and therefore Patroni assumes the worst case - network partitioning. In the case of a partitioned network, other nodes of the Patroni cluster may successfully grab the leader lock and promote Postgres to primary. In order to avoid a split-brain, the old primary is demoted before the leader lock expires.


DCS Failsafe Mode
-----------------

We introduce a new special option, the ``failsafe_mode``. It could be enabled only via global :ref:`dynamic configuration <dynamic_configuration>` stored in the DCS ``/config`` key. If the failsafe mode is enabled and the leader lock update in DCS failed due to reasons different from the version/value/index mismatch, Postgres may continue to run as a primary if it can access all known members of the cluster via Patroni REST API.


Low-level implementation details
--------------------------------

- We introduce a new, permanent key in DCS, named ``/failsafe``.
- The ``/failsafe`` key contains all known members of the given Patroni cluster at a given time.
- The current leader maintains the ``/failsafe`` key.
- The member is allowed to participate in the leader race and become the new leader only if it is present in the ``/failsafe`` key.
- If the cluster consists of a single node the ``/failsafe`` key will contain a single member.
- In the case of DCS "outage" the existing primary connects to all members presented in the ``/failsafe`` key via the ``POST /failsafe`` REST API and may continue to run as the primary if all replicas acknowledge it.
- If one of the members doesn't respond, the primary is demoted.
- Replicas are using incoming ``POST /failsafe`` REST API requests as an indicator that the primary is still alive. This information is cached for ``ttl`` seconds.


F.A.Q.
------

- Why MUST the current primary see ALL other members? Can’t we rely on quorum here?

  This is a great question! The problem is that the view on the quorum might be different from the perspective of DCS and Patroni. While DCS nodes must be evenly distributed across availability zones, there is no such rule for Patroni, and more importantly, there is no mechanism for introducing and enforcing such a rule. If the majority of Patroni nodes ends up in the losing part of the partitioned network (including primary) while minority nodes are in the winning part, the primary must be demoted. Only checking ALL other members allows detecting such a situation.

- What if node/pod gets terminated while DCS is down?

  If DCS isn’t accessible, the check “are ALL other cluster members accessible?” is executed every cycle of the heartbeat loop (every ``loop_wait`` seconds). If pod/node is terminated, the check will fail and Postgres will be demoted to a read-only and will not recover until DCS is restored.

- What if all members of the Patroni cluster are lost while DCS is down?

  Patroni could be configured to create the new replica from the backup even when the cluster doesn't have a leader. But, if the new member isn't present in the ``/failsafe`` key, it will not be able to grab the leader lock and promote.

- What will happen if the primary lost access to DCS while replicas didn't?

  The primary will execute the failsafe code and contact all known replicas. These replicas will use this information as an indicator that the primary is alive and will not start the leader race even if the leader lock in DCS has expired.

- How to enable the Failsafe Mode?

  Before enabling the ``failsafe_mode`` please make sure that Patroni version on all members is up-to-date. After that, you can use either the ``PATCH /config`` :ref:`REST API <rest_api>` or :ref:`patronictl edit-config -s failsafe_mode=true <patronictl_edit_config_parameters>`
