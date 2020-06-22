from __future__ import absolute_import
import datetime
import functools
import json
import logging
import socket
import sys
import time

from kubernetes import client as k8s_client, config as k8s_config, watch as k8s_watch
from urllib3 import Timeout
from urllib3.exceptions import HTTPError
from six.moves.http_client import HTTPException
from threading import Condition, Lock, Thread

from . import AbstractDCS, Cluster, ClusterConfig, Failover, Leader, Member, SyncState, TimelineHistory
from ..exceptions import DCSError
from ..utils import deep_compare, Retry, RetryFailedError, tzutc, USER_AGENT

logger = logging.getLogger(__name__)


class KubernetesError(DCSError):
    pass


class KubernetesRetriableException(k8s_client.rest.ApiException):

    def __init__(self, orig):
        super(KubernetesRetriableException, self).__init__(orig.status, orig.reason)
        self.body = orig.body
        self.headers = orig.headers

    @property
    def sleeptime(self):
        try:
            return int(self.headers['retry-after'])
        except Exception:
            return None


class CoreV1ApiProxy(object):

    def __init__(self, use_endpoints=False):
        self._api = k8s_client.CoreV1Api()
        self._api.api_client.user_agent = USER_AGENT
        self._api.api_client.rest_client.pool_manager.connection_pool_kw['maxsize'] = 10
        self._request_timeout = None
        self._use_endpoints = bool(use_endpoints)

    def configure_timeouts(self, loop_wait, retry_timeout, ttl):
        # Normally every loop_wait seconds we should have receive something from the socket.
        # If we didn't received anything after the loop_wait + retry_timeout it is a time
        # to start worrying (send keepalive messages). Finally, the connection should be
        # considered as dead if we received nothing from the socket after the ttl seconds.
        cnt = 3
        idle = int(loop_wait + retry_timeout)
        intvl = max(1, int(float(ttl - idle) / cnt))
        self._api.api_client.rest_client.pool_manager.connection_pool_kw['socket_options'] = [
            (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
            (socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, idle),
            (socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, intvl),
            (socket.IPPROTO_TCP, socket.TCP_KEEPCNT, cnt),
            (socket.IPPROTO_TCP, 18, int(ttl * 1000))  # TCP_USER_TIMEOUT
        ]
        self._request_timeout = (1, retry_timeout / 3.0)

    def __getattr__(self, func):
        if func.endswith('_kind'):
            func = func[:-4] + ('endpoints' if self._use_endpoints else 'config_map')

        def wrapper(*args, **kwargs):
            if '_request_timeout' not in kwargs:
                kwargs['_request_timeout'] = self._request_timeout
            try:
                return getattr(self._api, func)(*args, **kwargs)
            except k8s_client.rest.ApiException as e:
                if e.status in (500, 503, 504) or e.headers and 'retry-after' in e.headers:  # XXX
                    raise KubernetesRetriableException(e)
                raise
        return wrapper

    @property
    def use_endpoints(self):
        return self._use_endpoints


def catch_kubernetes_errors(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except k8s_client.rest.ApiException as e:
            if e.status == 403:
                logger.exception('Permission denied')
            elif e.status != 409:  # Object exists or conflict in resource_version
                logger.exception('Unexpected error from Kubernetes API')
            return False
        except (RetryFailedError, HTTPException, HTTPError, socket.error, socket.timeout):
            return False
    return wrapper


class ObjectCache(Thread):

    def __init__(self, dcs, func, retry, condition, name=None):
        Thread.__init__(self)
        self.daemon = True
        self._api_client = k8s_client.ApiClient()
        self._dcs = dcs
        self._func = func
        self._retry = retry
        self._condition = condition
        self._name = name  # name of this pod
        self._is_ready = False
        self._object_cache = {}
        self._object_cache_lock = Lock()
        self._annotations_map = {self._dcs.leader_path: self._dcs._LEADER, self._dcs.config_path: self._dcs._CONFIG}
        self.start()

    def _list(self):
        try:
            return self._func(_request_timeout=(self._retry.deadline, Timeout.DEFAULT_TIMEOUT))
        except Exception:
            time.sleep(1)
            raise

    def _watch(self, resource_version):
        return self._func(_request_timeout=(self._retry.deadline, Timeout.DEFAULT_TIMEOUT),
                          _preload_content=False, watch=True, resource_version=resource_version)

    def set(self, name, value):
        with self._object_cache_lock:
            old_value = self._object_cache.get(name)
            ret = not old_value or int(old_value.metadata.resource_version) < int(value.metadata.resource_version)
            if ret:
                self._object_cache[name] = value
        return ret, old_value

    def delete(self, name, resource_version):
        with self._object_cache_lock:
            old_value = self._object_cache.get(name)
            ret = old_value and int(old_value.metadata.resource_version) < int(resource_version)
            if ret:
                del self._object_cache[name]
        return not old_value or ret, old_value

    def copy(self):
        with self._object_cache_lock:
            return self._object_cache.copy()

    def get(self, name):
        with self._object_cache_lock:
            return self._object_cache.get(name)

    def _build_cache(self):
        objects = self._list()
        return_type = 'V1' + objects.kind[:-4]
        with self._object_cache_lock:
            self._object_cache = {item.metadata.name: item for item in objects.items}
        with self._condition:
            self._is_ready = True
            self._condition.notify()

        response = self._watch(objects.metadata.resource_version)
        try:
            for line in k8s_watch.watch.iter_resp_lines(response):
                event = json.loads(line)
                obj = event['object']
                if obj.get('code') == 410:
                    break

                ev_type = event['type']
                name = obj['metadata']['name']

                if ev_type in ('ADDED', 'MODIFIED'):
                    obj = k8s_watch.watch.SimpleNamespace(data=json.dumps(obj))
                    obj = self._api_client.deserialize(obj, return_type)
                    success, old_value = self.set(name, obj)
                    if success:
                        new_value = (obj.metadata.annotations or {}).get(self._annotations_map.get(name))
                elif ev_type == 'DELETED':
                    success, old_value = self.delete(name, obj['metadata']['resourceVersion'])
                    new_value = None
                else:
                    logger.warning('Unexpected event type: %s', ev_type)
                    continue

                if success and return_type != 'V1Pod':
                    if old_value:
                        old_value = (old_value.metadata.annotations or {}).get(self._annotations_map.get(name))

                    value_changed = old_value != new_value and \
                        (name != self._dcs.config_path or old_value is not None and new_value is not None)

                    if value_changed:
                        logger.debug('%s changed from %s to %s', name, old_value, new_value)

                    # Do not wake up HA loop if we run as leader and received leader object update event
                    if value_changed or name == self._dcs.leader_path and self._name != new_value:
                        self._dcs.event.set()
        finally:
            with self._condition:
                self._is_ready = False
            response.close()
            response.release_conn()

    def run(self):
        while True:
            try:
                self._build_cache()
            except Exception as e:
                with self._condition:
                    self._is_ready = False
                logger.error('ObjectCache.run %r', e)

    def is_ready(self):
        """Must be called only when holding the lock on `_condition`"""
        return self._is_ready


class Kubernetes(AbstractDCS):

    def __init__(self, config):
        self._labels = config['labels']
        self._labels[config.get('scope_label', 'cluster-name')] = config['scope']
        self._label_selector = ','.join('{0}={1}'.format(k, v) for k, v in self._labels.items())
        self._namespace = config.get('namespace') or 'default'
        self._role_label = config.get('role_label', 'role')
        config['namespace'] = ''
        super(Kubernetes, self).__init__(config)
        self._retry = Retry(deadline=config['retry_timeout'], max_delay=1, max_tries=-1,
                            retry_exceptions=(KubernetesRetriableException, HTTPException,
                                              HTTPError, socket.error, socket.timeout))
        self._ttl = None
        try:
            k8s_config.load_incluster_config()
        except k8s_config.ConfigException:
            k8s_config.load_kube_config(context=config.get('context', 'local'))

        self.__my_pod = None
        self.__ips = [] if config.get('patronictl') else [config.get('pod_ip')]
        self.__ports = []
        for p in config.get('ports', [{}]):
            port = {'port': int(p.get('port', '5432'))}
            port.update({n: p[n] for n in ('name', 'protocol') if p.get(n)})
            self.__ports.append(k8s_client.V1EndpointPort(**port))

        self._api = CoreV1ApiProxy(config.get('use_endpoints'))
        self._should_create_config_service = self._api.use_endpoints
        self.reload_config(config)
        # leader_observed_record, leader_resource_version, and leader_observed_time are used only for leader race!
        self._leader_observed_record = {}
        self._leader_observed_time = None
        self._leader_resource_version = None
        self.__do_not_watch = False

        self._condition = Condition()

        pods_func = functools.partial(self._api.list_namespaced_pod, self._namespace,
                                      label_selector=self._label_selector)
        self._pods = ObjectCache(self, pods_func, self._retry, self._condition)

        kinds_func = functools.partial(self._api.list_namespaced_kind, self._namespace,
                                       label_selector=self._label_selector)
        self._kinds = ObjectCache(self, kinds_func, self._retry, self._condition, self._name)

    def retry(self, *args, **kwargs):
        return self._retry.copy()(*args, **kwargs)

    def client_path(self, path):
        return super(Kubernetes, self).client_path(path)[1:].replace('/', '-')

    @property
    def leader_path(self):
        return self._base_path[1:] if self._api.use_endpoints else super(Kubernetes, self).leader_path

    def set_ttl(self, ttl):
        ttl = int(ttl)
        self.__do_not_watch = self._ttl != ttl
        self._ttl = ttl

    @property
    def ttl(self):
        return self._ttl

    def set_retry_timeout(self, retry_timeout):
        self._retry.deadline = retry_timeout

    def reload_config(self, config):
        super(Kubernetes, self).reload_config(config)
        self._api.configure_timeouts(self.loop_wait, self._retry.deadline, self.ttl)

    @staticmethod
    def member(pod):
        annotations = pod.metadata.annotations or {}
        member = Member.from_node(pod.metadata.resource_version, pod.metadata.name, None, annotations.get('status', ''))
        member.data['pod_labels'] = pod.metadata.labels
        return member

    def _wait_caches(self):
        stop_time = time.time() + self._retry.deadline
        while not (self._pods.is_ready() and self._kinds.is_ready()):
            timeout = stop_time - time.time()
            if timeout <= 0:
                raise RetryFailedError('Exceeded retry deadline')
            self._condition.wait(timeout)

    def _load_cluster(self):
        try:
            with self._condition:
                self._wait_caches()

                members = [self.member(pod) for pod in self._pods.copy().values()]
                nodes = self._kinds.copy()

            config = nodes.get(self.config_path)
            metadata = config and config.metadata
            annotations = metadata and metadata.annotations or {}

            # get initialize flag
            initialize = annotations.get(self._INITIALIZE)

            # get global dynamic configuration
            config = ClusterConfig.from_node(metadata and metadata.resource_version,
                                             annotations.get(self._CONFIG) or '{}',
                                             metadata.resource_version if self._CONFIG in annotations else 0)

            # get timeline history
            history = TimelineHistory.from_node(metadata and metadata.resource_version,
                                                annotations.get(self._HISTORY) or '[]')

            leader = nodes.get(self.leader_path)
            metadata = leader and leader.metadata
            self._leader_resource_version = metadata.resource_version if metadata else None
            annotations = metadata and metadata.annotations or {}

            # get last leader operation
            last_leader_operation = annotations.get(self._OPTIME)
            last_leader_operation = 0 if last_leader_operation is None else int(last_leader_operation)

            # get leader
            leader_record = {n: annotations.get(n) for n in (self._LEADER, 'acquireTime',
                             'ttl', 'renewTime', 'transitions') if n in annotations}
            if (leader_record or self._leader_observed_record) and leader_record != self._leader_observed_record:
                self._leader_observed_record = leader_record
                self._leader_observed_time = time.time()

            leader = leader_record.get(self._LEADER)
            try:
                ttl = int(leader_record.get('ttl')) or self._ttl
            except (TypeError, ValueError):
                ttl = self._ttl

            if not metadata or not self._leader_observed_time or self._leader_observed_time + ttl < time.time():
                leader = None

            if metadata:
                member = Member(-1, leader, None, {})
                member = ([m for m in members if m.name == leader] or [member])[0]
                leader = Leader(metadata.resource_version, None, member)

            # failover key
            failover = nodes.get(self.failover_path)
            metadata = failover and failover.metadata
            failover = Failover.from_node(metadata and metadata.resource_version,
                                          metadata and (metadata.annotations or {}).copy())

            # get synchronization state
            sync = nodes.get(self.sync_path)
            metadata = sync and sync.metadata
            sync = SyncState.from_node(metadata and metadata.resource_version,  metadata and metadata.annotations)

            return Cluster(initialize, config, leader, last_leader_operation, members, failover, sync, history)
        except Exception:
            logger.exception('get_cluster')
            raise KubernetesError('Kubernetes API is not responding properly')

    @staticmethod
    def compare_ports(p1, p2):
        return p1.name == p2.name and p1.port == p2.port and (p1.protocol or 'TCP') == (p2.protocol or 'TCP')

    @staticmethod
    def subsets_changed(last_observed_subsets, ip, ports):
        """
        >>> Kubernetes.subsets_changed([], None, [])
        True
        >>> ip = '1.2.3.4'
        >>> a = [k8s_client.V1EndpointAddress(ip=ip)]
        >>> s = [k8s_client.V1EndpointSubset(addresses=a)]
        >>> Kubernetes.subsets_changed(s, '1.2.3.5', [])
        True
        >>> s = [k8s_client.V1EndpointSubset(addresses=a, ports=[k8s_client.V1EndpointPort(protocol='TCP', port=1)])]
        >>> Kubernetes.subsets_changed(s, '1.2.3.4', [k8s_client.V1EndpointPort(port=5432)])
        True
        >>> p1 = k8s_client.V1EndpointPort(name='port1', port=1)
        >>> p2 = k8s_client.V1EndpointPort(name='port2', port=2)
        >>> p3 = k8s_client.V1EndpointPort(name='port3', port=3)
        >>> s = [k8s_client.V1EndpointSubset(addresses=a, ports=[p1, p2])]
        >>> Kubernetes.subsets_changed(s, ip, [p2, p3])
        True
        >>> s2 = [k8s_client.V1EndpointSubset(addresses=a, ports=[p2, p1])]
        >>> Kubernetes.subsets_changed(s, ip, [p2, p1])
        False
        """

        if len(last_observed_subsets) != 1:
            return True
        if len(last_observed_subsets[0].addresses or []) != 1 or \
                last_observed_subsets[0].addresses[0].ip != ip or \
                len(last_observed_subsets[0].ports) != len(ports):
            return True
        if len(ports) == 1:
            return not Kubernetes.compare_ports(last_observed_subsets[0].ports[0], ports[0])
        observed_ports = {p.name: p for p in last_observed_subsets[0].ports}
        for p in ports:
            if p.name not in observed_ports or not Kubernetes.compare_ports(p, observed_ports.pop(p.name)):
                return True
        return False

    def __target_ref(self, leader_ip, latest_subsets, pod):
        # we want to re-use existing target_ref if possible
        for subset in latest_subsets:
            for address in subset.addresses or []:
                if address.ip == leader_ip and address.target_ref and address.target_ref.name == self._name:
                    return address.target_ref
        return k8s_client.V1ObjectReference(kind='Pod', uid=pod.metadata.uid, namespace=self._namespace,
                                            name=self._name, resource_version=pod.metadata.resource_version)

    def _map_subsets(self, endpoints, ips):
        leader = self._kinds.get(self.leader_path)
        latest_subsets = leader and leader.subsets or []
        if not ips:
            # We want to have subsets empty
            if latest_subsets:
                endpoints['subsets'] = []
            return

        pod = self._pods.get(self._name)
        leader_ip = ips[0] or pod and pod.status.pod_ip
        # don't touch subsets if our (leader) ip is unknown or subsets is valid
        if leader_ip and self.subsets_changed(latest_subsets, leader_ip, self.__ports):
            kwargs = {'hostname': pod.spec.hostname, 'node_name': pod.spec.node_name,
                      'target_ref': self.__target_ref(leader_ip, latest_subsets, pod)} if pod else {}
            address = k8s_client.V1EndpointAddress(ip=leader_ip, **kwargs)
            endpoints['subsets'] = [k8s_client.V1EndpointSubset(addresses=[address], ports=self.__ports)]

    def _patch_or_create(self, name, annotations, resource_version=None, patch=False, retry=None, ips=None):
        metadata = {'namespace': self._namespace, 'name': name, 'labels': self._labels, 'annotations': annotations}
        if patch or resource_version:
            if resource_version is not None:
                metadata['resource_version'] = resource_version
            func = functools.partial(self._api.patch_namespaced_kind, name)
        else:
            func = functools.partial(self._api.create_namespaced_kind)
            # skip annotations with null values
            metadata['annotations'] = {k: v for k, v in metadata['annotations'].items() if v is not None}

        metadata = k8s_client.V1ObjectMeta(**metadata)
        if ips is not None and self._api.use_endpoints:
            endpoints = {'metadata': metadata}
            self._map_subsets(endpoints, ips)
            body = k8s_client.V1Endpoints(**endpoints)
        else:
            body = k8s_client.V1ConfigMap(metadata=metadata)
        ret = retry(func, self._namespace, body) if retry else func(self._namespace, body)
        if ret:
            self._kinds.set(name, ret)
        return ret

    @catch_kubernetes_errors
    def patch_or_create(self, name, annotations, resource_version=None, patch=False, retry=True, ips=None):
        if retry is True:
            retry = self.retry
        return self._patch_or_create(name, annotations, resource_version, patch, retry, ips)

    def patch_or_create_config(self, annotations, resource_version=None, patch=False, retry=True):
        # SCOPE-config endpoint requires corresponding service otherwise it might be "cleaned" by k8s master
        if self._api.use_endpoints and not patch and not resource_version:
            self._should_create_config_service = True
            self._create_config_service()
        return self.patch_or_create(self.config_path, annotations, resource_version, patch, retry)

    def _create_config_service(self):
        metadata = k8s_client.V1ObjectMeta(namespace=self._namespace, name=self.config_path, labels=self._labels)
        body = k8s_client.V1Service(metadata=metadata, spec=k8s_client.V1ServiceSpec(cluster_ip='None'))
        try:
            if not self._api.create_namespaced_service(self._namespace, body):
                return
        except Exception as e:
            if not isinstance(e, k8s_client.rest.ApiException) or e.status != 409:  # Service already exists
                return logger.exception('create_config_service failed')
        self._should_create_config_service = False

    def _write_leader_optime(self, last_operation):
        """Unused"""

    def _update_leader(self):
        """Unused"""

    def _update_leader_with_retry(self, annotations, resource_version, ips):
        retry = self._retry.copy()

        def _retry(*args, **kwargs):
            return retry(*args, **kwargs)

        try:
            return self._patch_or_create(self.leader_path, annotations, resource_version, ips=ips, retry=_retry)
        except k8s_client.rest.ApiException as e:
            if e.status == 409:
                logger.warning('Concurrent update of %s', self.leader_path)
            else:
                logger.exception('Permission denied' if e.status == 403 else 'Unexpected error from Kubernetes API')
                return False
        except RetryFailedError:
            return False

        deadline = retry.stoptime - time.time()
        if deadline < 2:
            return False

        retry.sleep_func(1)  # Give a chance for ObjectCache to receive the latest version

        kind = self._kinds.get(self.leader_path)
        kind_annotations = kind and kind.metadata.annotations or {}
        kind_resource_version = kind and kind.metadata.resource_version

        # There is different leader or resource_version in cache didn't change
        if kind and (kind_annotations.get(self._LEADER) != self._name or kind_resource_version == resource_version):
            return False

        retry.deadline = deadline - 1  # Update deadline and retry
        return self.patch_or_create(self.leader_path, annotations, kind_resource_version, ips=ips, retry=_retry)

    def update_leader(self, last_operation, access_is_restricted=False):
        kind = self._kinds.get(self.leader_path)
        kind_annotations = kind and kind.metadata.annotations or {}

        if kind and kind_annotations.get(self._LEADER) != self._name:
            return False

        now = datetime.datetime.now(tzutc).isoformat()
        leader_observed_record = kind_annotations or self._leader_observed_record
        annotations = {self._LEADER: self._name, 'ttl': str(self._ttl), 'renewTime': now,
                       'acquireTime': leader_observed_record.get('acquireTime') or now,
                       'transitions': leader_observed_record.get('transitions') or '0'}
        if last_operation:
            annotations[self._OPTIME] = last_operation

        resource_version = kind and kind.metadata.resource_version
        ips = [] if access_is_restricted else self.__ips
        return self._update_leader_with_retry(annotations, resource_version, ips)

    def attempt_to_acquire_leader(self, permanent=False):
        now = datetime.datetime.now(tzutc).isoformat()
        annotations = {self._LEADER: self._name, 'ttl': str(sys.maxsize if permanent else self._ttl),
                       'renewTime': now, 'acquireTime': now, 'transitions': '0'}
        if self._leader_observed_record:
            try:
                transitions = int(self._leader_observed_record.get('transitions'))
            except (TypeError, ValueError):
                transitions = 0

            if self._leader_observed_record.get(self._LEADER) != self._name:
                transitions += 1
            else:
                annotations['acquireTime'] = self._leader_observed_record.get('acquireTime') or now
            annotations['transitions'] = str(transitions)
        ips = [] if self._api.use_endpoints else None
        ret = self.patch_or_create(self.leader_path, annotations, self._leader_resource_version, ips=ips)
        if not ret:
            logger.info('Could not take out TTL lock')
        return ret

    def take_leader(self):
        return self.attempt_to_acquire_leader()

    def set_failover_value(self, value, index=None):
        """Unused"""

    def manual_failover(self, leader, candidate, scheduled_at=None, index=None):
        annotations = {'leader': leader or None, 'member': candidate or None,
                       'scheduled_at': scheduled_at and scheduled_at.isoformat()}
        patch = bool(self.cluster and isinstance(self.cluster.failover, Failover) and self.cluster.failover.index)
        return self.patch_or_create(self.failover_path, annotations, index, bool(index or patch), False)

    @property
    def _config_resource_version(self):
        config = self._kinds.get(self.config_path)
        return config and config.metadata.resource_version

    def set_config_value(self, value, index=None):
        return self.patch_or_create_config({self._CONFIG: value}, index, bool(self._config_resource_version), False)

    @catch_kubernetes_errors
    def touch_member(self, data, permanent=False):
        cluster = self.cluster
        if cluster and cluster.leader and cluster.leader.name == self._name:
            role = 'promoted' if data['role'] in ('replica', 'promoted') else 'master'
        elif data['state'] == 'running' and data['role'] != 'master':
            role = data['role']
        else:
            role = None

        member = cluster and cluster.get_member(self._name, fallback_to_leader=False)
        pod_labels = member and member.data.pop('pod_labels', None)
        ret = pod_labels is not None and pod_labels.get(self._role_label) == role and deep_compare(data, member.data)

        if not ret:
            metadata = {'namespace': self._namespace, 'name': self._name, 'labels': {self._role_label: role},
                        'annotations': {'status': json.dumps(data, separators=(',', ':'))}}
            body = k8s_client.V1Pod(metadata=k8s_client.V1ObjectMeta(**metadata))
            ret = self._api.patch_namespaced_pod(self._name, self._namespace, body)
            if ret:
                self._pods.set(self._name, ret)
        if self._should_create_config_service:
            self._create_config_service()
        return ret

    def initialize(self, create_new=True, sysid=""):
        cluster = self.cluster
        resource_version = cluster.config.index if cluster and cluster.config and cluster.config.index else None
        return self.patch_or_create_config({self._INITIALIZE: sysid}, resource_version)

    def _delete_leader(self):
        """Unused"""

    def delete_leader(self, last_operation=None):
        kind = self._kinds.get(self.leader_path)
        if kind and (kind.metadata.annotations or {}).get(self._LEADER) == self._name:
            annotations = {self._LEADER: None}
            if last_operation:
                annotations[self._OPTIME] = last_operation
            self.patch_or_create(self.leader_path, annotations, kind.metadata.resource_version, True, False, [])
            self.reset_cluster()

    def cancel_initialization(self):
        self.patch_or_create_config({self._INITIALIZE: None}, self._config_resource_version, True)

    @catch_kubernetes_errors
    def delete_cluster(self):
        self.retry(self._api.delete_collection_namespaced_kind, self._namespace, label_selector=self._label_selector)

    def set_history_value(self, value):
        return self.patch_or_create_config({self._HISTORY: value}, None, bool(self._config_resource_version), False)

    def set_sync_state_value(self, value, index=None):
        """Unused"""

    def write_sync_state(self, leader, sync_standby, index=None):
        return self.patch_or_create(self.sync_path, self.sync_state(leader, sync_standby), index, False)

    def delete_sync_state(self, index=None):
        return self.write_sync_state(None, None, index)

    def watch(self, leader_index, timeout):
        if self.__do_not_watch:
            self.__do_not_watch = False
            return True

        try:
            return super(Kubernetes, self).watch(None, timeout + 0.5)
        finally:
            self.event.clear()
