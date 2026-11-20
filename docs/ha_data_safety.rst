.. _ha_data_safety:

Recommended setup for data safety
=================================

PostgreSQL can remain writable when Patroni stops making progress. Possible causes include a hung boot disk, a suspended Patroni process, or a network partition. The leader key can then expire while the old primary still accepts writes, and a replica can be promoted next to it. Three things protect the data in that situation, and they do different jobs: synchronous replication keeps acknowledged transactions on a replica, the watchdog stops a primary whose Patroni cannot stop it, and careful handling of the former primary keeps its divergent data until you decide what to do with it.

This page gives a starting configuration for a three-node cluster and explains each part. Fencing and the preservation of a former primary need deployment-specific pieces that Patroni does not provide; those are named as such. Test these settings and the external recovery procedures together before relying on them.

.. contents::
   :local:
   :depth: 1

Configuration
-------------

Dynamic configuration (``patronictl edit-config``):

.. code-block:: yaml

   ttl: 120
   loop_wait: 10
   retry_timeout: 40
   synchronous_mode: quorum
   synchronous_node_count: 1
   synchronous_mode_strict: true
   postgresql:
     parameters:
       synchronous_commit: on

Local configuration (``patroni.yml``) on every member that may become primary:

.. code-block:: yaml

   watchdog:
     mode: required
     safety_margin: -1

Merge these into your existing configuration, and keep ``fsync`` and ``full_page_writes`` enabled on every node. The reasons for each value follow.

Synchronous replication
-----------------------

With asynchronous replication, an isolated primary acknowledges transactions that no replica has, and they are lost when a replica is promoted. With these settings, a write transaction normally waits until at least one replica has flushed its WAL to disk, and Patroni's automatic promotion checks protect these synchronously replicated transactions. Cancelling the synchronous wait can leave a transaction committed locally without replication. Keep ``synchronous_commit`` at ``on`` (or ``remote_apply``) for the sessions whose data matters; check application, role and database settings for overrides.

Strict mode decides what happens when no suitable replica is available: without it Patroni falls back to asynchronous replication and the protection is gone until a replica returns; with it, commits that need a synchronous standby wait. That is the trade this setup makes: it prefers a write outage to unprotected writes. In strict mode, Patroni keeps the known standby names or uses a reserved placeholder instead of the ``*`` fallback. A manual failover to a named candidate can bypass the replication-safety checks and lose transactions. See :ref:`replication_modes`.

Watchdog and DCS
----------------

The watchdog resets the host when Patroni stops sending keepalives. It is the protection for the case where Patroni itself is blocked, for example on a hung boot disk. ``mode: required`` makes Patroni refuse to promote a member that cannot activate a watchdog (outside paused mode, where the watchdog is disabled). See :ref:`watchdog` for the device setup.

Patroni feeds the watchdog after each successful leader-key update. With the default ``safety_margin`` of 5 seconds, a delay between the update and the keepalive can move the reset past the key expiry; ``safety_margin: -1`` requests a timeout of ``ttl // 2`` instead, which closes that gap. The timeout must still leave room for one HA loop with a DCS retry, so the timings above are chosen as a set. They satisfy Patroni's rule: ``10 + 2 × 40 = 90 ≤ 120``. The requested watchdog timeout is 60 seconds. This leaves 10 seconds beyond one loop interval and one retry budget. That is a planning allowance, not a guaranteed shutdown time. A longer retry budget can avoid demotion during short DCS outages, but delays the response to longer outages. With the default ``30/10/10`` timings, the 15-second watchdog is shorter than one loop plus one retry, so set the timings before enabling the half-TTL watchdog. Check the effective timeout of your device, and test it on your platform: a software watchdog needs a running kernel and does not help when the whole VM is paused.

After a host reset, Patroni starts an existing stopped PostgreSQL instance in recovery, even if the node still owns the leader key. It may promote it in a later HA loop, after renewing the key and passing the promotion checks. When you enable a previously disabled watchdog, restart Patroni on each replica and verify its eligibility; then switch over and restart Patroni on the former primary.

The watchdog relies on the leader key expiring only when Patroni stops updating it. With Consul, keep ``consul.checks`` empty (the default): a session tied to node health checks can be invalidated before the watchdog fires, while PostgreSQL is still running. With :ref:`DCS failsafe mode <dcs_failsafe_mode>` enabled, a primary can continue without the key during a DCS outage, so routing and alerts that look only at the key must take that state into account.

Fencing
-------

The watchdog cannot fence another node on demand. External fencing can be integrated through ``postgresql.pre_promote``. Patroni runs this command on the candidate after it has acquired the leader key and before promoting PostgreSQL. A non-zero exit prevents the promotion; outside paused mode, Patroni then releases the key. For example, an integration with a reliable record of possible writers could stop those machines through the cloud API and return non-zero if their identity or stop status is uncertain. This needs provider guarantees that the reported state means execution has stopped, plus controls against a concurrent restart. Patroni provides the hook, not this integration. See the ``pre_promote`` entry in :ref:`postgresql_settings`.

Former primary
--------------

A former primary can hold the only copy of the transactions that did not reach the new primary. With ``use_pg_rewind`` enabled, Patroni can rewind it to rejoin the current leader: a successful rewind replaces the divergent data, and a failed one can leave the directory unusable. Before a rewind or a restart of an uncleanly stopped former primary, Patroni may also run crash recovery in single-user mode. Either removal flag, ``remove_data_directory_on_rewind_failure`` or ``remove_data_directory_on_diverged_timelines``, can allow Patroni to delete the directory after a failed rewind; the second also allows re-initialization when timelines diverge and rewind cannot be used. Both default to ``false``. These defaults prevent those removal paths; they do not prevent recovery or rewind from changing the files.

If you may need that data, preserve it before the node rejoins: keep PostgreSQL stopped, prevent Patroni from starting recovery, rewind or re-initialization on that node, and take a consistent snapshot or copy of PGDATA, WAL and all tablespaces. Only then let it rejoin. Patroni has no hook for this step.

Operations and limits
---------------------

Health checks route traffic; they do not stop a writer. ``GET /primary`` returns 200 as soon as Patroni considers the node leader, which can be before PostgreSQL has left recovery, so a check that must wait for the promotion should also require ``"state": "running"`` and ``"role": "primary"`` in the response body. Clients and proxies may not detect a fenced or reset node immediately; configure connection timeouts or proxy session termination to bound that delay. See :ref:`rest_api`.

Monitor commit progress, not only Patroni state: a small transaction committed at a fixed interval, with an alert when a commit is still outstanding after a deadline. The watchdog checks Patroni's keepalives, not commits. A stalled data volume can block WAL-flushing commits while Patroni continues renewing the leader key and feeding the watchdog. Automatic failover is not guaranteed in that case. The write probe detects it; if a failover is needed, fence the affected node and assess the replicas before choosing a candidate.

For planned maintenance use ``patronictl switchover``. Before an emergency failover, fence any old primary that may still accept writes and confirm that the fencing completed. Compare the replicas' positions on compatible timeline histories. Promote through Patroni, verify the new primary, and only then reopen application traffic. If a failover may have discarded transactions, remember that applications can issue the same IDs again; account for that before writes resume. Unknown replication lag is not zero lag.
