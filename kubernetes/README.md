# Kubernetes deployment examples
Below you will find examples of Patroni deployments using [kind](https://kind.sigs.k8s.io/).

# Patroni on K8s
The Patroni cluster deployment with a StatefulSet consisting of three Pods.

Example session:

    $ kind create cluster
    Creating cluster "kind" ...
     ✓ Ensuring node image (kindest/node:v1.25.3) 🖼
     ✓ Preparing nodes 📦
     ✓ Writing configuration 📜
     ✓ Starting control-plane 🕹️
     ✓ Installing CNI 🔌
     ✓ Installing StorageClass 💾
    Set kubectl context to "kind-kind"
    You can now use your cluster with:

    kubectl cluster-info --context kind-kind

    Thanks for using kind! 😊

    $ docker build -t patroni .
    Sending build context to Docker daemon  138.8kB
    Step 1/9 : FROM postgres:16
    ...
    Successfully built e9bfe69c5d2b
    Successfully tagged patroni:latest

    $ kind load docker-image patroni
    Image: "" with ID "sha256:e9bfe69c5d2b319dec0cf564fb895484537664775e18f37f9b707914cc5537e6" not yet present on node "kind-control-plane", loading...

    $ kubectl apply -f patroni_k8s.yaml
    service/patronidemo-config created
    statefulset.apps/patronidemo created
    endpoints/patronidemo created
    service/patronidemo created
    service/patronidemo-repl created
    secret/patronidemo created
    serviceaccount/patronidemo created
    role.rbac.authorization.k8s.io/patronidemo created
    rolebinding.rbac.authorization.k8s.io/patronidemo created
    clusterrole.rbac.authorization.k8s.io/patroni-k8s-ep-access created
    clusterrolebinding.rbac.authorization.k8s.io/patroni-k8s-ep-access created

    $ kubectl get pods -L role
    NAME            READY   STATUS    RESTARTS   AGE   ROLE
    patronidemo-0   1/1     Running   0          34s   primary
    patronidemo-1   1/1     Running   0          30s   replica
    patronidemo-2   1/1     Running   0          26s   replica

    $ kubectl exec -ti patronidemo-0 -- bash
    postgres@patronidemo-0:~$ patronictl list
    + Cluster: patronidemo (7186662553319358497) ----+----+-------------+-----+------------+-----+
    | Member        | Host       | Role    | State   | TL | Receive LSN | Lag | Replay LSN | Lag |
    +---------------+------------+---------+---------+----+-------------+-----+------------+-----+
    | patronidemo-0 | 10.244.0.5 | Leader  | running |  1 |             |     |            |     |
    | patronidemo-1 | 10.244.0.6 | Replica | running |  1 |   0/40004E8 |   0 |  0/40004E8 |   0 |
    | patronidemo-2 | 10.244.0.7 | Replica | running |  1 |   0/40004E8 |   0 |  0/40004E8 |   0 |
    +---------------+------------+---------+---------+----+-------------+-----+------------+-----+

# Citus on K8s
The Citus cluster with the StatefulSets, one coordinator with three Pods and two workers with two pods each.

Example session:

    $ kind create cluster
    Creating cluster "kind" ...
     ✓ Ensuring node image (kindest/node:v1.25.3) 🖼
     ✓ Preparing nodes 📦
     ✓ Writing configuration 📜
     ✓ Starting control-plane 🕹️
     ✓ Installing CNI 🔌
     ✓ Installing StorageClass 💾
    Set kubectl context to "kind-kind"
    You can now use your cluster with:

    kubectl cluster-info --context kind-kind

    Thanks for using kind! 😊

    demo@localhost:~/git/patroni/kubernetes$ docker build -f Dockerfile.citus -t patroni-citus-k8s .
    Sending build context to Docker daemon  138.8kB
    Step 1/11 : FROM postgres:16
    ...
    Successfully built 8cd73e325028
    Successfully tagged patroni-citus-k8s:latest

    $ kind load docker-image patroni-citus-k8s
    Image: "" with ID "sha256:8cd73e325028d7147672494965e53453f5540400928caac0305015eb2c7027c7" not yet present on node "kind-control-plane", loading...

    $ kubectl apply -f citus_k8s.yaml
    service/citusdemo-0-config created
    service/citusdemo-1-config created
    service/citusdemo-2-config created
    statefulset.apps/citusdemo-0 created
    statefulset.apps/citusdemo-1 created
    statefulset.apps/citusdemo-2 created
    endpoints/citusdemo-0 created
    service/citusdemo-0 created
    endpoints/citusdemo-1 created
    service/citusdemo-1 created
    endpoints/citusdemo-2 created
    service/citusdemo-2 created
    service/citusdemo-workers created
    secret/citusdemo created
    serviceaccount/citusdemo created
    role.rbac.authorization.k8s.io/citusdemo created
    rolebinding.rbac.authorization.k8s.io/citusdemo created
    clusterrole.rbac.authorization.k8s.io/patroni-k8s-ep-access created
    clusterrolebinding.rbac.authorization.k8s.io/patroni-k8s-ep-access created

    $ kubectl get sts
    NAME          READY   AGE
    citusdemo-0   1/3     6s  # coodinator (group=0)
    citusdemo-1   1/2     6s  # worker (group=1)
    citusdemo-2   1/2     6s  # worker (group=2)

    $ kubectl get pods -l cluster-name=citusdemo -L role
    NAME            READY   STATUS    RESTARTS   AGE    ROLE
    citusdemo-0-0   1/1     Running   0          105s   primary
    citusdemo-0-1   1/1     Running   0          101s   replica
    citusdemo-0-2   1/1     Running   0          96s    replica
    citusdemo-1-0   1/1     Running   0          105s   primary
    citusdemo-1-1   1/1     Running   0          101s   replica
    citusdemo-2-0   1/1     Running   0          105s   primary
    citusdemo-2-1   1/1     Running   0          101s   replica

    $ kubectl exec -ti citusdemo-0-0 -- bash
    postgres@citusdemo-0-0:~$ patronictl list
    + Citus cluster: citusdemo -----------+----------------+---------+----+-------------+-----+------------+-----+
    | Group | Member        | Host        | Role           | State   | TL | Receive LSN | Lag | Replay LSN | Lag |
    +-------+---------------+-------------+----------------+---------+----+-------------+-----+------------+-----+
    |     0 | citusdemo-0-0 | 10.244.0.10 | Leader         | running |  1 |             |     |            |     |
    |     0 | citusdemo-0-1 | 10.244.0.12 | Replica        | running |  1 |   0/40004E8 |   0 |  0/40004E8 |   0 |
    |     0 | citusdemo-0-2 | 10.244.0.14 | Quorum Standby | running |  1 |   0/40004E8 |   0 |  0/40004E8 |   0 |
    |     1 | citusdemo-1-0 | 10.244.0.8  | Leader         | running |  1 |             |     |            |     |
    |     1 | citusdemo-1-1 | 10.244.0.11 | Quorum Standby | running |  1 |   0/40004E8 |   0 |  0/40004E8 |   0 |
    |     2 | citusdemo-2-0 | 10.244.0.9  | Leader         | running |  1 |             |     |            |     |
    |     2 | citusdemo-2-1 | 10.244.0.13 | Quorum Standby | running |  1 |   0/40004E8 |   0 |  0/40004E8 |   0 |
    +-------+---------------+-------------+----------------+---------+----+-------------+-----+------------+-----+

    postgres@citusdemo-0-0:~$ psql citus
    psql (16.4 (Debian 16.4-1.pgdg120+1))
    Type "help" for help.

    citus=# table pg_dist_node;
     nodeid | groupid |  nodename   | nodeport | noderack | hasmetadata | isactive | noderole  | nodecluster | metadatasynced | shouldhaveshards
    --------+---------+-------------+----------+----------+-------------+----------+-----------+-------------+----------------+------------------
          1 |       0 | 10.244.0.10 |     5432 | default  | t           | t        | primary   | default     | t              | f
          2 |       1 | 10.244.0.8  |     5432 | default  | t           | t        | primary   | default     | t              | t
          3 |       2 | 10.244.0.9  |     5432 | default  | t           | t        | primary   | default     | t              | t
          4 |       0 | 10.244.0.14 |     5432 | default  | t           | t        | secondary | default     | t              | f
          5 |       0 | 10.244.0.12 |     5432 | default  | t           | t        | secondary | default     | t              | f
          6 |       1 | 10.244.0.11 |     5432 | default  | t           | t        | secondary | default     | t              | t
          7 |       2 | 10.244.0.13 |     5432 | default  | t           | t        | secondary | default     | t              | t
    (7 rows)
