.. _ha_data_safety:

Recommended setup for data safety
=================================

Use this setup for a three-node Linux cluster that must protect acknowledged writes during a *partial* failure of the primary: a hung boot disk while PostgreSQL keeps running, a suspended Patroni process, or a network partition. Such failures can leave PostgreSQL accepting writes after the leader key has expired. If another node becomes primary, the transactions that exist only on the former primary can be missing from the new one.

The setup uses existing Patroni features and a few external components. Each part has a *Set*, *Why* and *Limits* section. The lab tests behind it used Consul, Patroni 3.3.4 and a 4.1.5 build, with PostgreSQL 17; they covered specific failure cases and did not test this exact configuration as a whole.

**Version requirement.** In strict synchronous mode, Patroni 3.3.x and 4.1.5 set ``synchronous_standby_names = '*'`` when no suitable replica is available. The wildcard lets any connected standby, including the external WAL receiver below, satisfy a synchronous commit, so an acknowledged transaction is not guaranteed to be on a promotable replica. Patroni master replaces the wildcard with a placeholder that matches no node (see :ref:`replication_modes`). Use the complete setup only with a release that has this change, that is, a release newer than 4.1.5.

.. contents::
   :local:
   :depth: 1

The setup at a glance
---------------------

Merge these fragments into your existing Patroni configuration; they are not a complete ``patroni.yml``. Install and test the external components before you rely on the setup. Keep PostgreSQL's durability settings (``fsync``, ``full_page_writes``) enabled, and check the prerequisites for ``pg_rewind``.

Dynamic configuration (``patronictl edit-config``):

.. code-block:: yaml

   ttl: 60
   loop_wait: 5
   retry_timeout: 10
   synchronous_mode: quorum
   synchronous_node_count: 1
   synchronous_mode_strict: true
   postgresql:
     use_slots: true
     parameters:
       synchronous_commit: on
   slots:
     wal_receiver:                   # slot for the external WAL receiver
       type: physical

Local configuration (``patroni.yml``) on every member that can become primary:

.. code-block:: yaml

   watchdog:
     mode: required
     safety_margin: -1
   postgresql:
     use_pg_rewind: true          # only with a preservation guard, see Former primary
     remove_data_directory_on_rewind_failure: false
     remove_data_directory_on_diverged_timelines: false
     pre_promote: /usr/local/bin/fence-former-primary   # see Fencing
   consul:
     checks: []                                           # Consul only

External components:

- a fencing service called from ``pre_promote``;
- a WAL receiver (``pg_receivewal``) on a separate host;
- client routing that checks ``state`` and ``role``, not only the HTTP status;
- a step that preserves a former primary before it rejoins;
- a write probe that detects hung commits.

Watchdog
--------

**Set.** ``watchdog.mode: required`` and ``safety_margin: -1`` on every member that may become primary. On Linux, load ``softdog`` with ``nowayout=0`` and make ``/dev/watchdog`` writable for the Patroni user. See :ref:`watchdog`.

**Why.** A working watchdog resets the host if Patroni stops sending keepalives, for example because its process is blocked on a hung boot disk or is suspended. With ``safety_margin: -1`` Patroni requests a watchdog timeout of ``ttl // 2``. A delay between the key renewal and the watchdog keepalive then cannot move the reset past the key expiry; the default margin of 5 seconds allows that.

**Timing.** The dynamic settings must satisfy ``loop_wait + 2 * retry_timeout <= ttl``. For the half-TTL watchdog, also leave time for one loop interval and one DCS retry: ``ttl // 2 > loop_wait + retry_timeout``. With ``ttl: 60``, ``loop_wait: 5`` and ``retry_timeout: 10`` the requested timeout is 30 seconds, which leaves 15 seconds beyond ``loop_wait + retry_timeout``. With the default ``30/10/10`` settings, the requested 15-second timeout is shorter than ``loop_wait + retry_timeout``. Test renewal delays, scheduling stalls and shutdown time with your settings; the arithmetic alone does not prove the margin is enough. Also check the effective timeout of your device.

**Limits.**

- Outside paused mode, Patroni refuses promotion if it cannot activate a watchdog. In paused mode the watchdog is disabled.
- A software watchdog needs a running kernel. It does not help when the whole VM is paused or a hypervisor fault stops the guest.
- A shorter timeout can cause more resets during temporary stalls. A node that reboots within the TTL can take its leader key back; see `Former primary`_.
- When you change an inactive member from ``off`` to ``required``, restart Patroni and verify that the watchdog is available. A reload that changes only the mode can leave that member with the null watchdog implementation, and such a member cannot become leader. Roll out replica by replica, then switch over, then restart the former leader.
- The watchdog checks Patroni keepalives, not commits. See `What this setup does not cover`_.

Synchronous replication
-----------------------

**Set.** ``synchronous_mode: quorum`` with ``synchronous_node_count: 1`` and ``synchronous_mode_strict: true`` for a three-node cluster (quorum mode needs Patroni 4.0 or later and PostgreSQL 10 or later; the strict-mode requirement above applies). Keep ``synchronous_commit: on`` (or ``remote_apply``), and check application, role and database settings for overrides. See :ref:`replication_modes` and :ref:`quorum_mode`.

**Why.** With asynchronous replication, an isolated primary acknowledges writes that no replica has. With ``synchronous_commit: on``, a synchronous commit waits until the WAL is on durable storage on a selected standby, and Patroni's candidate checks protect those commits during an automatic failover, within the limits below.

**Limits.**

- Strict mode blocks commits that require synchronous replication while no suitable standby is available. This setup keeps synchronous replication enabled even when writes must wait. Without strict mode, Patroni falls back to asynchronous replication, and the protection is gone until a standby returns.
- ``wal_sender_timeout`` (default 60 seconds) closes inactive replication connections. Connection errors may be detected sooner, and another eligible synchronous standby may let commits continue. It is not a limit for the commit wait itself.
- A manual failover with a named candidate can bypass the candidate checks.
- Sessions with ``synchronous_commit`` set to ``local``, ``remote_write`` or ``off`` do not get this protection.

DCS session
-----------

**Set.** For Consul, keep ``consul.checks: []`` (the default). For other DCS types, check that nothing can invalidate the leader session before its TTL except Patroni itself.

**Why.** The watchdog protection depends on the order "watchdog reset first, key expiry second". A session that is also tied to node health checks can be invalidated early, while PostgreSQL is still running and before the watchdog fires. Another node can then acquire the leader key while the old primary is still writable.

**Limits.** During a DCS outage, ``failsafe_mode`` can keep the primary running if every member in the failsafe topology confirms it over the REST API. It does not override a confirmed leader-key ownership mismatch. Routing and alerts that use the leader key alone must take the failsafe state into account. See :ref:`dcs_failsafe_mode`.

Fencing
-------

**Set.** Configure ``postgresql.pre_promote`` to call a fencing script. The script stops every other node that may still accept writes, through the cloud API or another power or storage control, and returns success only after it has confirmed this. See :ref:`postgresql_settings`.

**Why.** The watchdog resets a host when keepalives stop. It does not confirm that every other possible primary is unable to write, and it does nothing on a manual failover of a primary whose Patroni still runs. The hook runs on the candidate after it has acquired the leader key and before ``pg_ctl promote``. A non-zero exit prevents that promotion; outside paused mode the HA loop then attempts to release the leader key.

The script should:

- keep a durable list of nodes that may still write, and record the candidate itself before it returns success. Register the existing primary before you enable the mechanism, and cover the initial bootstrap;
- identify nodes by immutable IDs (cloud instance IDs), not by names or IP addresses;
- wait for the stop operation to finish and verify the result;
- check that the candidate still holds the leader key before it returns success;
- refuse when the state of any possible writer is unknown;
- keep fenced nodes unable to restart as primaries until their data is preserved and they rejoin under control.

**Limits.**

- If fencing is not confirmed, return a non-zero exit code. Patroni then refuses that promotion. A script that returns success without stopping the node re-creates the split brain.
- Cluster history is not a complete list of possible writers: PostgreSQL can finish a promotion before Patroni writes the history entry.
- The candidate can lose the leader key after the hook returns. Its process can also be suspended right after the hook checks ownership.
- Test how the fencing service handles a restart of Patroni on the leader. For planned restarts, switch over first.

Client routing
--------------

**Set.** Route write traffic with ``GET /primary``, and require HTTP 200 **and** ``"state": "running"`` **and** ``"role": "primary"`` (``"master"`` in Patroni 3.x) in the response body. With HAProxy, use ``http-check expect`` on the body and ``on-marked-down shutdown-sessions``. Set TCP timeouts on pooler-to-server connections (for PgBouncer: ``tcp_user_timeout`` and keepalives). See :ref:`rest_api`.

**Why.** In a normal primary cluster, ``/primary`` can return 200 while PostgreSQL is still in recovery, because Patroni already considers the node leader; the body checks reject that state. Connections to an unreachable node can stay open for minutes unless a proxy or a timeout closes them. A DNS or service-discovery update does not end existing sessions.

**Limits.**

- Detection time depends on the check interval, the failure threshold and the check timeout. During that time, existing sessions can still write to the old primary. Fencing, not routing, is what stops them.
- These checks show server state. They do not prove that a commit can finish or that application-level recovery has finished.
- If a failover may have discarded transactions, applications that use sequences or other ID allocators can issue the same IDs again. Keep writes blocked until recovery has accounted for the IDs issued by the old primary and has invalidated cached allocations that could reuse them.

WAL receiver
------------

**Set.** ``pg_receivewal`` on a separate host, on a permanent slot declared in the ``slots`` section of the dynamic configuration. Connect it to the primary by address, and keep it on the old primary after a failover until that node is fenced; start another receiver for the new primary, with a separate output directory per source node, kept across receiver restarts. Set ``max_slot_wal_keep_size``, and alert on receiver lag, slot loss and disk space. Test full restores (base backup plus received WAL). See :ref:`dynamic_configuration`.

**Why.** Patroni can redirect replicas after a leadership change, and local archiving can stop if the failed disk holds the shell, the executable or the libraries that ``archive_command`` needs. A receiver that stays connected to the old primary can preserve the WAL it wrote after everything else stopped receiving it. With a compatible base backup and a complete WAL chain, that WAL supports recovery of the transactions missing from the new primary.

**Limits.**

- The receiver can miss the newest records. ``--synchronous`` makes it flush promptly; this option alone does not make commits wait for it. With the strict-mode wildcard of older releases (see the version requirement above), the receiver can be selected as a synchronous standby, which is not intended here.
- Redirecting the receiver to the new leader stops the collection from the old primary. It must stay on the old primary until that node is confirmed stopped.
- Do not rely on replicas to retain the old primary's WAL after a leadership change.
- ``max_slot_wal_keep_size`` can let the primary remove WAL that the receiver still needs.

Former primary
--------------

**Set.** Keep ``remove_data_directory_on_rewind_failure`` and ``remove_data_directory_on_diverged_timelines`` at ``false``. Before a former primary rejoins, keep PostgreSQL stopped and prevent recovery or rewind. Take a consistent snapshot or copy of PGDATA, WAL and all tablespaces. Allow the rejoin only after the copy is complete. A check in the service start (for example ``ExecStartPre=`` in systemd) can enforce this after a reboot or a restart. Enable ``use_pg_rewind`` only after this guard is in place, including for recovery inside a running Patroni process; if you cannot build such a guard, keep fenced former primaries stopped and preserve their data before you start Patroni on them.

**Why.** A fenced or reset former primary can hold the only copy of the transactions that did not reach the new primary. When the node rejoins, Patroni may run crash recovery or ``pg_rewind``. Recovery modifies the data directory, and a rewind can overwrite the divergent data.

**Limits.**

- The two settings control automatic removal of the data directory only. They do not stop a successful rewind, or the changes a failed rewind makes before it fails.
- A start-time check does not cover recovery inside a Patroni process that is already running. Patroni has no hook for that case.
- A node that reboots quickly after a watchdog reset can take its leader key back before the key expires. Decide whether Patroni may start automatically on such a node.

Monitoring
----------

**Set.**

- A write probe: a small transaction committed on the primary at a fixed interval, with an alert when a commit is still outstanding after a deadline.
- Alerts on: a node running as primary without the leader key (outside failsafe and pause), no leader for longer than the expected failover time, watchdog resets, fencing failures, receiver lag and slot loss, and ``pg_rewind`` on a former leader.

**Why.** Patroni does not test application commit progress, and it does not verify that the fencing service and the WAL receiver achieved their result. Monitor those outcomes separately.

**Limits.** A probe checks one transaction over one connection path. Alerts do not stop an unsafe writer; fencing and routing have to do that.

Manual failover
---------------

For planned maintenance, use ``patronictl switchover``. If an emergency failover is needed while the old primary may still accept writes, follow these steps in order:

#. Block application writes on every route and close existing write sessions.
#. Check whether an automatic failover has already chosen a new primary, and if so, identify the primary to keep. Make sure no other promotion or automatic rejoin can run at the same time.
#. Fence all other nodes that may still accept writes, and prevent them from restarting as primaries.
#. If the old primary's data must be preserved, keep PostgreSQL stopped and prevent recovery, rewind or reinitialization until the copy is complete.
#. Compare the replicas' WAL positions on compatible timeline histories. Assess the risk of losing acknowledged transactions before choosing a candidate. Record any uncertainty.
#. If a promotion is still needed, request it through Patroni.
#. Check the new leader and PostgreSQL's role. Finish application-level recovery, including ID allocators.
#. Reopen application writes. Keep application traffic away from the fenced nodes until their data is preserved and they have rejoined under control.

Unknown replication lag is not zero lag.

What this setup does not cover
------------------------------

**A hung data volume.** If the disk with PGDATA and ``pg_wal`` stops completing I/O while the boot disk is healthy, commits that flush WAL hang, while Patroni can keep renewing the leader key and feeding the watchdog. ``/primary`` can still return 200 with ``running`` and the primary role. Automatic failover is not guaranteed. Use the write probe to detect the outage, fence the affected node, and then run the manual failover procedure. Assess the replication state before you choose a candidate: the absence of new acknowledgements does not prove that a failover cannot lose earlier writes.

**The newest transactions under asynchronous fallback.** If you disable strict mode and lose the synchronous standbys, Patroni allows asynchronous commits. The newest transactions may then exist only on the primary; the receiver can lag too.

**Platform failures of the watchdog.** A software watchdog cannot run while its VM or guest kernel is stopped, and a device that fails to reset the host provides no protection. Fencing covers these cases only when a promotion happens.

**Application-level consistency after a loss.** Preventing ID reuse does not recover the lost transactions or repair references to them in other systems.
