.. _citus:

Citus support
=============

Patroni makes it extremely simple to deploy `Multi-Node Citus`__ clusters.

__ https://docs.citusdata.com/en/stable/installation/multi_node.html

TL;DR
-----

There are only a few simple rules you need to follow:

1. Citus extension must be available on all nodes.
2. Cluster name (``scope``) must be the same for all Citus nodes!
3. Superuser credentials must be the same on coordinator and all worker
   nodes, and ``pg_hba.conf`` should allow superuser access between all nodes.
4. :ref:`REST API <restapi_settings>` access should be allowed from worker
   nodes to the coordinator. E.g., credentials should be the same and if
   configured, client certificates from worker nodes must be accepted by the
   coordinator.
5. Add the following section to the ``patroni.yaml``:

.. code:: YAML

        citus:
          group: X  # 0 for coordinator and 1, 2, 3, etc for workers
          database: citus  # must be the same on all nodes


After that you just need to start Patroni and it will handle the rest:

1. ``citus`` extension will be automatically added to ``shared_preload_libraries``.
2. If ``max_prepared_transactions`` isn't explicitly set in the global
   :ref:`dynamic configuration <dynamic_configuration>` Patroni will
   automatically set it to ``2*max_connections``.
3. The ``citus.database`` will be automatically created followed by ``CREATE EXTENSION citus``.
4. Current superuser :ref:`credentials <postgresql_settings>` will be added to the ``pg_dist_authinfo``
   table to allow cross-node communication. Don't forget to update them if
   later you decide to change superuser username/password/sslcert/sslkey!
5. The coordinator primary node will automatically discover worker primary
   nodes and add them to the ``pg_dist_node`` table using the
   ``citus_add_node()`` function.
6. Patroni will also maintain ``pg_dist_node`` in case failover/switchover
   on the coordinator or worker clusters occurs.

patronictl
----------

Coordinator and worker clusters are physically different PostgreSQL/Patroni
clusters that are just logically groupped together using Citus. Therefore in
most cases it is not possible to manage them as a single entity.

It results in two major differences in ``patronictl`` behaviour when
``patroni.yaml`` has the ``citus`` section comparing with the usual:

1. The ``list`` and the ``topology`` by default output all members of the Citus
   formation (coordinators and workers). The new column ``Group`` indicates
   which Citus group they belong to.
2. For all ``patronictl`` commands the new option is introduced, named
   ``--group``. For some commands the default value for the group might be
   taken from the ``patroni.yaml``. For example, ``patronictl pause`` will
   enable the maintenance mode by default for the ``group`` that is set in the
   ``citus`` section, but for example for ``patronictl  switchover`` or
   ``patronictl remove`` the group must be explicitly specified.

An example of ``patronictl list`` output for the Citus cluster::

    postgres@coord1:~$ patronictl list demo
    + Citus cluster: demo ----------+--------------+---------+----+-----------+
    | Group | Member  | Host        | Role         | State   | TL | Lag in MB |
    +-------+---------+-------------+--------------+---------+----+-----------+
    |     0 | coord1  | 172.27.0.10 | Replica      | running |  1 |         0 |
    |     0 | coord2  | 172.27.0.6  | Sync Standby | running |  1 |         0 |
    |     0 | coord3  | 172.27.0.4  | Leader       | running |  1 |           |
    |     1 | work1-1 | 172.27.0.8  | Sync Standby | running |  1 |         0 |
    |     1 | work1-2 | 172.27.0.2  | Leader       | running |  1 |           |
    |     2 | work2-1 | 172.27.0.5  | Sync Standby | running |  1 |         0 |
    |     2 | work2-2 | 172.27.0.7  | Leader       | running |  1 |           |
    +-------+---------+-------------+--------------+---------+----+-----------+

If we add the ``--group`` option, the output will change to::

    postgres@coord1:~$ patronictl list demo --group 0
    + Citus cluster: demo (group: 0, 7179854923829112860) -----------+
    | Member | Host        | Role         | State   | TL | Lag in MB |
    +--------+-------------+--------------+---------+----+-----------+
    | coord1 | 172.27.0.10 | Replica      | running |  1 |         0 |
    | coord2 | 172.27.0.6  | Sync Standby | running |  1 |         0 |
    | coord3 | 172.27.0.4  | Leader       | running |  1 |           |
    +--------+-------------+--------------+---------+----+-----------+

    postgres@coord1:~$ patronictl list demo --group 1
    + Citus cluster: demo (group: 1, 7179854923881963547) -----------+
    | Member  | Host       | Role         | State   | TL | Lag in MB |
    +---------+------------+--------------+---------+----+-----------+
    | work1-1 | 172.27.0.8 | Sync Standby | running |  1 |         0 |
    | work1-2 | 172.27.0.2 | Leader       | running |  1 |           |
    +---------+------------+--------------+---------+----+-----------+

Citus worker switchover
-----------------------

When a switchover is orchestrated for a Citus worker node, Citus offers the
opportunity to make the switchover close to transparent for an application.
Because the application connects to the coordinator, which in turn connects to
the worker nodes, then it is possible with Citus to `pause` the SQL traffic on
the coordinator for the shards hosted on a worker node. The switchover then
happens while the traffic is kept on the coordinator, and resumes as soon as a
new primary worker node is ready to accept read-write queries.

An example of ``patronictl switchover`` on the worker cluster::

    postgres@coord1:~$ patronictl switchover demo
    + Citus cluster: demo ----------+--------------+---------+----+-----------+
    | Group | Member  | Host        | Role         | State   | TL | Lag in MB |
    +-------+---------+-------------+--------------+---------+----+-----------+
    |     0 | coord1  | 172.27.0.10 | Replica      | running |  1 |         0 |
    |     0 | coord2  | 172.27.0.6  | Sync Standby | running |  1 |         0 |
    |     0 | coord3  | 172.27.0.4  | Leader       | running |  1 |           |
    |     1 | work1-1 | 172.27.0.8  | Leader       | running |  1 |           |
    |     1 | work1-2 | 172.27.0.2  | Sync Standby | running |  1 |         0 |
    |     2 | work2-1 | 172.27.0.5  | Sync Standby | running |  1 |         0 |
    |     2 | work2-2 | 172.27.0.7  | Leader       | running |  1 |           |
    +-------+---------+-------------+--------------+---------+----+-----------+
    Citus group: 2
    Master [work2-2]:
    Candidate ['work2-1'] []:
    When should the switchover take place (e.g. 2022-12-22T08:02 )  [now]:
    Current cluster topology
    + Citus cluster: demo (group: 2, 7179854924063375386) -----------+
    | Member  | Host       | Role         | State   | TL | Lag in MB |
    +---------+------------+--------------+---------+----+-----------+
    | work2-1 | 172.27.0.5 | Sync Standby | running |  1 |         0 |
    | work2-2 | 172.27.0.7 | Leader       | running |  1 |           |
    +---------+------------+--------------+---------+----+-----------+
    Are you sure you want to switchover cluster demo, demoting current master work2-2? [y/N]: y
    2022-12-22 07:02:40.33003 Successfully switched over to "work2-1"
    + Citus cluster: demo (group: 2, 7179854924063375386) ------+
    | Member  | Host       | Role    | State   | TL | Lag in MB |
    +---------+------------+---------+---------+----+-----------+
    | work2-1 | 172.27.0.5 | Leader  | running |  1 |           |
    | work2-2 | 172.27.0.7 | Replica | stopped |    |   unknown |
    +---------+------------+---------+---------+----+-----------+

    postgres@coord1:~$ patronictl list demo
    + Citus cluster: demo ----------+--------------+---------+----+-----------+
    | Group | Member  | Host        | Role         | State   | TL | Lag in MB |
    +-------+---------+-------------+--------------+---------+----+-----------+
    |     0 | coord1  | 172.27.0.10 | Replica      | running |  1 |         0 |
    |     0 | coord2  | 172.27.0.6  | Sync Standby | running |  1 |         0 |
    |     0 | coord3  | 172.27.0.4  | Leader       | running |  1 |           |
    |     1 | work1-1 | 172.27.0.8  | Leader       | running |  1 |           |
    |     1 | work1-2 | 172.27.0.2  | Sync Standby | running |  1 |         0 |
    |     2 | work2-1 | 172.27.0.5  | Leader       | running |  2 |           |
    |     2 | work2-2 | 172.27.0.7  | Sync Standby | running |  2 |         0 |
    +-------+---------+-------------+--------------+---------+----+-----------+

And this is how it looks on the coordinator side::

    # The worker primary notifies the coordinator that it is going to execute "pg_ctl stop".
    2022-12-22 07:02:38,636 DEBUG: query("BEGIN")
    2022-12-22 07:02:38,636 DEBUG: query("SELECT pg_catalog.citus_update_node(3, '172.27.0.7-demoted', 5432, true, 10000)")
    # From this moment all application traffic on the coordinator to the worker group 2 is paused.

    # The future worker primary notifies the coordinator that it acquired the leader lock in DCS and about to run "pg_ctl promote".
    2022-12-22 07:02:40,085 DEBUG: query("SELECT pg_catalog.citus_update_node(3, '172.27.0.5', 5432)")

    # The new worker primary just finished promote and notifies coordinator that it is ready to accept read-write traffic.
    2022-12-22 07:02:41,485 DEBUG: query("COMMIT")
    # From this moment the application traffic on the coordinator to the worker group 2 is unblocked.

Peek into DCS
-------------

The Citus cluster (coordinator and workers) are stored in DCS as a fleet of
Patroni clusters logically grouped together::

    /service/batman/              # scope=batman
    /service/batman/0/            # citus.group=0, coordinator
    /service/batman/0/initialize
    /service/batman/0/leader
    /service/batman/0/members/
    /service/batman/0/members/m1
    /service/batman/0/members/m2
    /service/batman/1/            # citus.group=1, worker
    /service/batman/1/initialize
    /service/batman/1/leader
    /service/batman/1/members/
    /service/batman/1/members/m3
    /service/batman/1/members/m4
    ...

Such an approach was chosen because for most DCS it becomes possible to fetch
the entire Citus cluster with a single recursive read request. Only Citus
coordinator nodes are reading the whole tree, because they have to discover
worker nodes. Worker nodes are reading only the subtree for their own group and
in some cases they could read the subtree of the coordinator group.

Citus on Kubernetes
-------------------

Since Kubernetes doesn't support hierarchical structures we had to include the
citus group to all K8s objects Patroni creates::

    batman-0-leader  # the leader config map for the coordinator
    batman-0-config  # the config map holding initialize, config, and history "keys"
    ...
    batman-1-leader  # the leader config map for worker group 1
    batman-1-config
    ...

I.e., the naming pattern is: ``${scope}-${citus.group}-${type}``.

All Kubernetes objects are discovered by Patroni using the `label selector`__,
therefore all Pods with Patroni&Citus and Endpoints/ConfigMaps must have
similar labels, and Patroni must be configured to use them using Kubernetes
:ref:`settings <kubernetes_settings>` or :ref:`environment variables
<kubernetes_environment>`.

__ https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors

A couple of examples of Patroni configuration using Pods environment variables:

1. for the coordinator cluster

.. code:: YAML

        apiVersion: v1
        kind: Pod
        metadata:
          labels:
            application: patroni
            citus-group: "0"
            citus-type: coordinator
            cluster-name: citusdemo
          name: citusdemo-0-0
          namespace: default
        spec:
          containers:
          - env:
            - name: PATRONI_SCOPE
              value: citusdemo
            - name: PATRONI_NAME
              valueFrom:
                fieldRef:
                  apiVersion: v1
                  fieldPath: metadata.name
            - name: PATRONI_KUBERNETES_POD_IP
              valueFrom:
                fieldRef:
                  apiVersion: v1
                  fieldPath: status.podIP
            - name: PATRONI_KUBERNETES_NAMESPACE
              valueFrom:
                fieldRef:
                  apiVersion: v1
                  fieldPath: metadata.namespace
            - name: PATRONI_KUBERNETES_LABELS
              value: '{application: patroni}'
            - name: PATRONI_CITUS_DATABASE
              value: citus
            - name: PATRONI_CITUS_GROUP
              value: "0"

2. for the worker cluster from the group 2

.. code:: YAML

        apiVersion: v1
        kind: Pod
        metadata:
          labels:
            application: patroni
            citus-group: "2"
            citus-type: worker
            cluster-name: citusdemo
          name: citusdemo-2-0
          namespace: default
        spec:
          containers:
          - env:
            - name: PATRONI_SCOPE
              value: citusdemo
            - name: PATRONI_NAME
              valueFrom:
                fieldRef:
                  apiVersion: v1
                  fieldPath: metadata.name
            - name: PATRONI_KUBERNETES_POD_IP
              valueFrom:
                fieldRef:
                  apiVersion: v1
                  fieldPath: status.podIP
            - name: PATRONI_KUBERNETES_NAMESPACE
              valueFrom:
                fieldRef:
                  apiVersion: v1
                  fieldPath: metadata.namespace
            - name: PATRONI_KUBERNETES_LABELS
              value: '{application: patroni}'
            - name: PATRONI_CITUS_DATABASE
              value: citus
            - name: PATRONI_CITUS_GROUP
              value: "2"

As you may noticed, both examples have ``citus-group`` label set. This label
allows Patroni to identify object as belonging to a certain Citus group. In
addition to that, there is also ``PATRONI_CITUS_GROUP`` environment variable,
which has the same value as the ``citus-group`` label. When Patroni creates
new Kubernetes objects ConfigMaps or Endpoints, it automatically puts the
``citus-group: ${env.PATRONI_CITUS_GROUP}`` label on them:

.. code:: YAML

        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: citusdemo-0-leader  # Is generated as ${env.PATRONI_SCOPE}-${env.PATRONI_CITUS_GROUP}-leader
          labels:
            application: patroni    # Is set from the ${env.PATRONI_KUBERNETES_LABELS}
            cluster-name: citusdemo # Is automatically set from the ${env.PATRONI_SCOPE}
            citus-group: '0'        # Is automatically set from the ${env.PATRONI_CITUS_GROUP}

You can find a complete example of Patroni deployment on Kubernetes with Citus
support in the `kubernetes`__ folder of the Patroni repository.

__ https://github.com/zalando/patroni/tree/master/kubernetes

There are two important files for you:

1. Dockerfile.citus
2. citus_k8s.yaml
