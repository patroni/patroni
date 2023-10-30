.. _faq:

FAQ
===

In this section you will find answers for the most frequently asked questions about Patroni.
Each sub-section attempts to focus on different kinds of questions.

We hope that this helps you to clarify most of your questions.
If you still have further concerns or find yourself facing an unexpected issue, please refer to :ref:`chatting` and :ref:`reporting_bugs` for instructions on how to get help or report issues.

Comparison with other HA solutions
----------------------------------

Why does Patroni require a separate cluster of DCS nodes while other solutions like ``repmgr`` do not?
    There are different ways of implementing HA solutions, each of them with their pros and cons.

    Software like ``repmgr`` performs communication among the nodes to decide when actions should be taken.

    Patroni on the other hand relies on the state stored in the DCS. The DCS acts as a source of truth for Patroni to decide what it should do.

    While having a separate DCS cluster can make you bloat your architecture, this approach also makes it less likely for split-brain scenarios to happen in your Postgres cluster.

What is the difference between Patroni and other HA solutions in regards to Postgres management?
    Patroni does not just manage the high availability of the Postgres cluster but also manages Postgres itself.

    If Postgres nodes do not exist yet, it takes care of bootstrapping the primary and the standby nodes, and also manages Postgres configuration of the nodes. If the Postgres nodes already exist, Patroni will take over management of the cluster.

    Besides the above, Patroni also has self-healing capabilities. In other words, if a primary node fails, Patroni will not only fail over to a replica, but also attempt to rejoin the former primary as a replica of the new primary. Similarly, if a replica fails, Patroni will attempt to rejoin that replica.

    That is way we call Patroni as a "template for HA solutions". It goes further than just managing physical replication: it manages Postgres as a whole.

DCS
---

Can I use the same ``etcd`` cluster to store data from two or more Patroni clusters?
    Yes, you can!

    Information about a Patroni cluster is stored in the DCS under a path prefixed with the ``namespace`` and ``scope`` Patroni settings.

    As long as you do not have conflicting namespace and scope across different Patroni clusters, you should be able to use the same DCS cluster to store information from multiple Patroni clusters.

What occurs if I attempt to use the same combination of ``namespace`` and ``scope`` for different Patroni clusters that point to the same DCS cluster?
    The second Patroni cluster that attempts to use the same ``namespace`` and ``scope`` will not be able to manage Postgres because it will find information related with that same combination in the DCS, but with an incompatible Postgres system identifier.
    The mismatch on the system identifier causes Patroni to abort the management of the second cluster, as it assumes that refers to a different cluster and that the user has misconfigured Patroni.

    Make sure to use different ``namespace`` / ``scope`` when dealing with different Patroni clusters that share the same DCS cluster.

What occurs if I lose my DCS cluster?
    The DCS is used to store basically status and the dynamic configuration of the Patroni cluster.

    They very first consequence is that all the Patroni clusters that rely on that DCS will go to read-only mode -- unless :ref:`dcs_failsafe_mode` is enabled.

What should I do if I lose my DCS cluster?
    There are three possible outcomes upon losing your DCS cluster:

    1. The DCS cluster is fully recovered: this requires no action from the Patroni side. Once the DCS cluster is recovered, Patroni should be able to recover too;
    2. The DCS cluster is re-created in place, and the endpoints remain the same. No changes are required on the Patroni side;
    3. A new DCS cluster is created with different endpoints. You will need to update the DCS endpoints in the Patroni configuration of each Patroni node.

    If you face scenario ``2.`` or ``3.`` Patroni will take care of creating the status information again based on the current status of the cluster, and recreate the dynamic configuration on the DCS based on a backup file named ``patroni.dynamic.json`` which is stored inside the Postgres data directory of each member of the Patroni cluster.

What occurs if I lose majority in my DCS cluster?
    The DCS will become unresponsive, which will cause Patroni to demote the current read/write Postgres node.

    Remember: Patroni relies on the state of the DCS to take actions on the cluster.

    You can use the :ref:`dcs_failsafe_mode` to alleviate that situation.

patronictl
----------

Do I need to run :ref:`patronictl` in the Patroni host?
    No, you do not need to do that.

    Running :ref:`patronictl` in the Patroni host is handy if you have access to the Patroni host because you can use the very same configuration file from the ``patroni`` agent for the :ref:`patronictl` application.

    However, :ref:`patronictl` is basically a client and it can be executed from remote machines. You just need to provide it with enough configuration so it can reach the DCS and the REST API of the Patroni member(s).

Why did the information from one of my Patroni members disappear from the output of :ref:`patronictl_list` command?
    Information shown by :ref:`patronictl_list` is based on the contents of the DCS.

    If information about a member disappeared from the DCS it is very likely that the Patroni agent on that node is not running anymore, or it is not able to communicate with the DCS.

    As the member is not able to update the information, the information eventually expires from the DCS, and consequently the member is not shown anymore in the output of :ref:`patronictl_list`.

Why is the information about one of my Patroni members not up-to-date in the output of :ref:`patronictl_list` command?
    Information shown by :ref:`patronictl_list` is based on the contents of the DCS.

    By default, that information is updated by Patroni roughly every ``loop_wait`` seconds.
    In other words, even if everything is normally functional you may still see a "delay" of up to ``loop_wait`` seconds in the information stored in the DCS.

    Be aware that that is not a rule, though. Some operations performed by Patroni cause it to immediately update the DCS information.

Configuration
-------------

What is the difference between dynamic configuration and local configuration?
    Dynamic configuration (or global configuration) is the configuration stored in the DCS, and which is applied to all members of the Patroni cluster.
    This is primarily where you should store your configuration.

    Settings that are specific to a node, or settings that you would like to overwrite the global configuration with, you should set only on the desired Patroni member as a local configuration.
    That local configuration can be specified either through the configuration file or through environment variables.

    See more in :ref:`patroni_configuration`.

What are the types of configuration in Patroni, and what is the precedence?
    The types are:

    * Dynamic configuration: applied to all members;
    * Local configuration: applied to the local member, overrides dynamic configuration;
    * Environment configuration: applied to the local member, overrides both dynamic and local configuration.

    **Note:** some Postgres GUCs can only be set globally, i.e., through dynamic configuration. Besides that, there are GUCs which Patroni enforces a hard-coded value.

    See more in :ref:`patroni_configuration`.

Is there any facility to help me create my Patroni configuration file?
    Yes, there is.

    You can use ``patroni --generate-sample-config`` or ``patroni --generate-config`` commands to generate a sample Patroni configuration or a Patroni configuration based on an existing Postgres instance, respectively.

    Please refer to :ref:`generate_sample_config` and :ref:`generate_config` for more details.

I changed my parameters under ``bootstrap.dcs`` configuration but Patroni is not applying the changes to the cluster members. What is wrong?
    The values configured under ``bootstrap.dcs`` are only used when bootstrapping a fresh cluster. Those values will be written to the DCS during the bootstrap.

    After the bootstrap phase finishes, you will only be able to change the dynamic configuration through the DCS.

    Refer to the next question for more details.

How can I change my dynamic configuration?
    You need to change the configuration in the DCS. That is accomplished either through:

    * :ref:`patronictl_edit_config`; or
    * A ``PATCH`` request to :ref:`config_endpoint`.

How can I change my local configuration?
    You need to change the configuration file of the corresponding Patroni member and signal the Patroni agent with ``SIHGUP``. You can do that using either of these approaches:

    * Send a ``POST`` request to the REST API :ref:`reload_endpoint`; or
    * Run :ref:`patronictl_reload`; or
    * Locally signal the Patroni process with ``SIGHUP``:

        * If you started Patroni through systemd, you can use the command ``systemctl reload PATRONI_UNIT.service``, ``PATRONI_UNIT`` being the name of the Patroni service; or
        * If you started Patroni through other means, you will need to identify the ``patroni`` process and run ``kill -s HUP PID``, ``PID`` being the process ID of the ``patroni`` process.

    **Note:** there are cases where a reload through the :ref:`patronictl_reload` may not work:

    * Expired REST API certificates: you can mitigate that by using the ``-k`` option of the :ref:`patronictl`;
    * Wrong credentials: for example when changing ``restapi`` or ``ctl`` credentials in the configuration file, and using that same configuration file for Patroni and :ref:`patronictl`.

How can I change my environment configuration?
    The environment configuration is only read by Patroni during startup.

    With that in mind, if you change the environment configuration you will need to restart the corresponding Patroni agent.

    Take care to not cause a failover in the cluster! You might be interested in checking :ref:`patronictl_pause`.

What occurs if I change a Postgres GUC that requires a reload?
    When you change the dynamic or the local configuration as explained in the previous questions, Patroni will take care of reloading the Postgres configuration for you.

What occurs if I change a Postgres GUC that requires a restart?
    Patroni will mark the affected members with a flag of ``pending restart``.

    It is up to you to determine when and how to restart the members. That can be accomplished either through:

    * :ref:`patronictl_restart`; or
    * A ``POST`` request to :ref:`restart_endpoint`.

    **Note:** some Postgres GUCs require a special management in terms of the order for restarting the Postgres nodes. Refer to :ref:`shared_memory_gucs` for more details.

What is the difference between ``etcd`` and ``etcd3`` in Patroni configuration?
    ``etcd`` uses the API version 2 of ``etcd``, while ``etcd3`` uses the API version 3 of ``etcd``.

    Be aware that information stored by the API version 2 is not manageable by API version 3 and vice-versa.

    We recommend that you configure ``etcd3`` instead of ``etcd`` because:

    * API version 2 is disabled by default from Etcd v3.4 onward;
    * API version 2 will be completely removed on Etcd v3.6.

I have ``use_slots`` enabled in my Patroni configuration, but when a cluster member goes offline for some time, the replication slot used by that member is dropped on the upstream node. What can I do to avoid that issue?
    You can configure a permanent physical replication slot for the members.

    Since Patroni ``3.2.0`` it is now possible to have member slots as permanent slots managed by Patroni.

    Patroni will create the permanent physical slots on all nodes, and make sure to not remove the slots, as well as to advance the slots' LSN on all nodes according to the LSN that has been consumed by the member.

    Later, if you decide to remove the corresponding member, it's **your responsability** to adjust the permanent slots configuration, otherwise Patroni will keep the slots around forever.

    **Note:** on Patroni older than ``3.2.0`` you could still have member slots configured as permanent physical slots, however they would be managed only on the current leader. That is, in case of failover/switchover these slots would be created on the new leader, but that wouldn't guarantee that it had all WAL segments for the absent node.

    **Note:** even with Patroni ``3.2.0`` there might be a small race condition. In the very beginning, when the slot is created on the replica it could be ahead of the same slot on the leader and in case if nobody is consuming the slot there is still a chance that some files could be missing after failover. With that in mind, it is recommended that you configure continuous archiving, which makes it possible to restore required WALs or perform PITR.

What is the difference between ``loop_wait``, ``retry_timeout`` and ``ttl``?
    Patroni performs what we call a HA cycle from time to time. On each HA cycle it takes care of performing a series of checks on the cluster to determine its healthiness, and depending on the status it may take actions, like failing over to a standby.

    ``loop_wait`` determines for how long, in seconds, Patroni should sleep before performing a new cycle of HA checks.

    ``retry_timeout`` sets the timeout for retry operations on the DCS and on Postgres. For example: if the DCS is unresponsive for more than ``retry_timeout`` seconds, Patroni might demote the primary node as a security action.

    ``ttl`` sets the lease time on the ``leader`` lock in the DCS. If the current leader of the cluster is not able to renew the lease during its HA cycles for longer than ``ttl``, then the lease will expire and that will trigger a ``leader race`` in the cluster.

    **Note:** when modifying these settings, please keep in mind that Patroni enforces the rule and minimal values described in :ref:`dynamic_configuration` section of the docs.

Postgres management
-------------------

Can I change Postgres GUCs directly in Postgres configuration?
    You can, but you should avoid that.

    Postgres configuration is managed by Patroni, and attempts to edit the configuration files may end up being frustrated by Patroni as it may eventually overwrite them.

    There are a few options available to overcome the management performed by Patroni:

    * Change Postgres GUCs through ``$PGDATA/postgresql.base.conf``; or
    * Define a ``postgresql.custom_conf`` which will be used instead of ``postgresql.base.conf`` so you can manage that externally; or
    * Change GUCs using ``ALTER SYSTEM`` / ``ALTER DATABASE`` / ``ALTER USER``.

    You can find more information about that in the section :ref:`important_configuration_rules`.

    In any case we recommend that you manage all the Postgres configuration through Patroni. That will centralize the management and make it easier to debug Patroni when needed.

Can I restart Postgres nodes directly?
    No, you should **not** attempt to manage Postgres directly!

    Any attempt of bouncing the Postgres server without Patroni can lead your cluster to face failovers.

    If you need to manage the Postgres server, do that through the ways exposed by Patroni.

Is Patroni able to take over management of an already existing Postgres cluster?
    Yes, it can!

    Please refer to :ref:`existing_data` for detailed instructions.

How does Patroni manage Postgres?
    Patroni takes care of bringing Postgres up and down by running the Postgres binaries, like ``pg_ctl`` and ``postgres``.

    With that in mind you **MUST** disable any other sources that could manage the Postgres clusters, like the systemd units, e.g. ``postgresql.service``. Only Patroni should be able to start, stop and promote Postgres instances in the cluster. Not doing so may result in split-brain scenarios. For example: if the node running as a primary failed and the unit ``postgresql.service`` is enabled, it may bring Postgres back up and cause a split-brain.

Concepts and requirements
-------------------------

Which are the applications that make part of Patroni?
    Patroni basically ships a couple applications:

    * ``patroni``: This is the Patroni agent, which takes care of managing a Postgres node;
    * ``patronictl``: This is a command-line utility used to interact with a Patroni cluster (perform switchovers, restarts, changes in the configuration, etc.). Please find more information in :ref:`patronictl`.

What is a ``standby cluster`` in Patroni?
    It is a cluster that does not have any primary Postgres node running, i.e., there is no read/write member in the cluster.

    These kinds of clusters exist to replicate data from another cluster and are usually useful when you want to replicate data across data centers.

    There will be a leader in the cluster which will be a standby in charge of replicating changes from a remote Postgres node.
    Then, there will be a set of standbys configured with cascading replication from such leader member.

    **Note:** the standby cluster doesn't know anything about the source cluster which it is replicating from -- it can even use ``restore_command`` instead of WAL streaming, and may use an absolutely independent DCS cluster.

    Refer to :ref:`standby_cluster` for more details.

What is a ``leader`` in Patroni?
    A ``leader`` in Patroni is like a coordinator of the cluster.

    In a regular Patroni cluster, the ``leader`` will be the read/write node.

    In a standby Patroni cluster, the ``leader`` (AKA ``standby leader``) will be in charge of replicating from a remote Postgres node, and cascading those changes to the other members of the standby cluster.

Does Patroni require a minimum number of Postgres nodes in the cluster?
    No, you can run Patroni with any number of Postgres nodes.

    Remember: Patroni is decoupled from the DCS.

What does ``pause`` mean in Patroni?
    Pause is an operation exposed by Patroni so the user can ask Patroni to step back in regards to Postgres management.

    That is mainly useful when you want to perform maintenance on the cluster, and would like to avoid that Patroni takes decisions related with HA, like failing over to a standby when you stop the primary.

    You can find more information about that in :ref:`pause`.

Automatic failover
------------------

How does the automatic failover mechanism of Patroni work?
    Patroni automatic failover is based on what we call ``leader race``.

    Patroni stores the cluster's status in the DCS, among them a ``leader`` lock which holds the name of the Patroni member which is the current ``leader`` of the cluster.

    That ``leader`` lock has a time-to-live associated with it. If the leader node fails to update the lease of the ``leader`` lock in time, the key will eventually expire from the DCS.

    When the ``leader`` lock expires, it triggers what Patroni calls a ``leader race``: all nodes start performing checks to determine if they are the best candidates for taking over the ``leader`` role.
    Some of these checks include calls to the REST API of all other Patroni members.

    All Patroni members that find themselves as the best candidate for taking over the ``leader`` lock will attempt to do so.
    The first Patroni member that is able to take the ``leader`` lock will promote itself to a read/write node (or ``standby leader``), and the others will be configured to follow it.

Can I temporarily disable automatic failover in the Patroni cluster?
    Yes, you can!

    You can achieve that by temporarily pausing the cluster.
    This is typically useful for performing maintenance.

    When you want to resume the automatic failover of the cluster, you just need to unpause it.

    You can find more information about that in :ref:`pause`.

Bootstrapping and standbys creation
-----------------------------------

How does Patroni create a primary Postgres node? What about a standby Postgres node?
    By default Patroni will use ``initdb`` to bootstrap a fresh cluster, and ``pg_basebackup`` to create standby nodes from a copy of the ``leader`` member.

    You can customize that behavior by writing your custom bootstrap methods, and your custom replica creation methods.

    Custom methods are usually useful when you want to restore backups created by backup tools like pgBackRest or Barman, for example.

    For detailed information please refer to :ref:`custom_bootstrap` and :ref:`custom_replica_creation`.

Monitoring
----------

How can I monitor my Patroni cluster?
    Patroni exposes a couple handy endpoints in its :ref:`rest_api`:

    * ``/metrics``: exposes monitoring metrics in a format that can be consumed by Prometheus;
    * ``/patroni``: exposes the status of the cluster in a JSON format. The information shown here is very similar to what is shown by the ``/metrics`` endpoint.

    You can use those endpoints to implement monitoring checks.
