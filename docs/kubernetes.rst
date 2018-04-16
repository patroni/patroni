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

There are two ways to direct the traffic to the Postgres master:

- use the `callback script <https://github.com/zalando/patroni/blob/master/kubernetes/callback.py>`_ provided by Patroni
- configure the Kubernetes Postgres service to use the label selector with the `role_label` (configured in patroni configuration).

Note that in some cases, for instance, when running on OpenShift, there is no alternative to using ConfigMaps.

Configuration
-------------

Patroni Kubernetes :ref:`settings <kubernetes_settings>` and :ref:`environment variables <kubernetes_environment>` are described in the general chapters of the documentation.

Examples
--------

- The `kubernetes <https://github.com/zalando/patroni/tree/master/kubernetes>`__ folder of the Patroni repository contains
  examples of the Docker image, the Kubernetes manifest and the callback script in order to test Patroni Kubernetes setup.
  Note that in the current state it will not be able to use PersistentVolumes because of permission issues.

- You can find the full-featured Docker image that can use Persistent Volumes in the
  `Spilo Project <https://github.com/zalando/spilo>`_.

- There is also a `Helm chart <https://github.com/kubernetes/charts/tree/master/incubator/patroni>`_
  to deploy the Spilo image configured with Patroni running using Kubernetes.

- In order to run your database clusters at scale using Patroni and Spilo, take a look at the
  `postgres-operator <https://github.com/zalando-incubator/postgres-operator>`_ project. It implements the operator pattern
  to manage Spilo clusters.
