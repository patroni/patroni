.. _kubernetes:

Using Patroni with Kubernetes
=============================

Patroni can use Kubernetes objects in order to store the state of the cluster and manage the leader key. That makes it
capable of operating Postgres in Kubernetes environment without any consistency store, namely, one doesn't
need to run an extra Etcd deployment. There are two different type of Kubernetes objects Patroni can use to store the
leader and the configuration keys, they are configured with the `kubernetes.use_endpoints` or `PATRONI_KUBERNETES_USE_ENDPOINTS`
environment variable.

Use Endpoints
-------------

Despite the fact that this is the recommended mode, it is turned off by default for compatibility reasons. When it is on, Patroni stores
the cluster configuration and the leader key in the `metadata: annotations` fields of the respective `Endpoints` it creates.
Changing the leader is safer than when using `ConfigMaps`, since both the annotations, containing the leader information, and the actual addresses
pointing to the running leader pod are updated simultaneously in one go.

Use ConfigMaps
--------------

In this mode, Patroni will create ConfigMaps instead of Endpoints and store keys inside meta-data of those ConfigMaps.
Changing the leader takes at least two updates, one to the leader ConfigMap and another to the respective Endpoint.

To direct the traffic to the Postgres leader you need to configure the Kubernetes Postgres service to use the label selector with the `role_label` (configured in patroni configuration).

Note that in some cases, for instance, when running on OpenShift, there is no alternative to using ConfigMaps.

Configuration
-------------

Patroni Kubernetes :ref:`settings <kubernetes_settings>` and :ref:`environment variables <kubernetes_environment>` are described in the general chapters of the documentation.

.. _kubernetes_role_values:

Customize role label
^^^^^^^^^^^^^^^^^^^^

By default, Patroni will set corresponding labels on the pod it runs in based on node's role, such as ``role=primary``.
The key and value of label can be customized by `kubernetes.role_label`, `kubernetes.leader_label_value`, `kubernetes.follower_label_value` and `kubernetes.standby_leader_label_value`.

Note that if you migrate from default role labels to custom ones, you can reduce downtime by following migration steps:

1. Add a temporary label using original role value for the pod with `kubernetes.tmp_role_label` (like ``tmp_role``). Once pods are restarted they will get following labels set by Patroni:

  .. code:: YAML

    labels:
      cluster-name: foo
      role: primary
      tmp_role: primary

2. After all pods have been updated, modify the service selector to select the temporary label.

  .. code:: YAML

    selector:
      cluster-name: foo
      tmp_role: primary

3. Add your custom role label (e.g., set `kubernetes.leader_label_value=primary`). Once pods are restarted they will get following new labels set by Patroni:

  .. code:: YAML

    labels:
      cluster-name: foo
      role: primary
      tmp_role: primary

4. After all pods have been updated again, modify the service selector to use new role value.

  .. code:: YAML

    selector:
      cluster-name: foo
      role: primary

5. Finally, remove the temporary label from your configuration and update all pods.

  .. code:: YAML

    labels:
      cluster-name: foo
      role: primary

Examples
--------

- The `kubernetes <https://github.com/patroni/patroni/tree/master/kubernetes>`__ folder of the Patroni repository contains
  examples of the Docker image, and the Kubernetes manifest to test Patroni Kubernetes setup.
  Note that in the current state it will not be able to use PersistentVolumes because of permission issues.

- You can find the full-featured Docker image that can use Persistent Volumes in the
  `Spilo Project <https://github.com/zalando/spilo>`_.

- There is also a `Helm chart <https://github.com/kubernetes/charts/tree/master/incubator/patroni>`_
  to deploy the Spilo image configured with Patroni running using Kubernetes.

- In order to run your database clusters at scale using Patroni and Spilo, take a look at the
  `postgres-operator <https://github.com/zalando/postgres-operator>`_ project. It implements the operator pattern
  to manage Spilo clusters.
