import json
import time
import unittest

from mock import Mock, patch
from patroni.dcs.kubernetes import Kubernetes, KubernetesError, k8s_client, RetryFailedError
from threading import Thread
from . import SleepException


def mock_list_namespaced_config_map(self, *args, **kwargs):
    metadata = {'resource_version': '1', 'labels': {'f': 'b'}, 'name': 'test-config',
                'annotations': {'initialize': '123', 'config': '{}'}}
    items = [k8s_client.V1ConfigMap(metadata=k8s_client.V1ObjectMeta(**metadata))]
    metadata.update({'name': 'test-leader', 'annotations': {'optime': '1234', 'leader': 'p-0', 'ttl': '30s'}})
    items.append(k8s_client.V1ConfigMap(metadata=k8s_client.V1ObjectMeta(**metadata)))
    metadata.update({'name': 'test-failover', 'annotations': {'leader': 'p-0'}})
    items.append(k8s_client.V1ConfigMap(metadata=k8s_client.V1ObjectMeta(**metadata)))
    metadata.update({'name': 'test-sync', 'annotations': {'leader': 'p-0'}})
    items.append(k8s_client.V1ConfigMap(metadata=k8s_client.V1ObjectMeta(**metadata)))
    metadata = k8s_client.V1ObjectMeta(resource_version='1')
    return k8s_client.V1ConfigMapList(metadata=metadata, items=items, kind='ConfigMapList')


def mock_list_namespaced_pod(self, *args, **kwargs):
    metadata = k8s_client.V1ObjectMeta(resource_version='1', name='p-0', annotations={'status': '{}'})
    items = [k8s_client.V1Pod(metadata=metadata)]
    return k8s_client.V1PodList(items=items, kind='PodList')


def mock_config_map(*args, **kwargs):
    mock = Mock()
    mock.metadata.resource_version = '2'
    return mock


@patch.object(k8s_client.CoreV1Api, 'patch_namespaced_config_map', mock_config_map)
@patch.object(k8s_client.CoreV1Api, 'create_namespaced_config_map', mock_config_map)
@patch('kubernetes.client.api_client.ThreadPool', Mock(), create=True)
@patch.object(Thread, 'start', Mock())
class TestKubernetes(unittest.TestCase):

    @patch('kubernetes.config.load_kube_config', Mock())
    @patch.object(k8s_client.CoreV1Api, 'list_namespaced_config_map', mock_list_namespaced_config_map)
    @patch.object(k8s_client.CoreV1Api, 'list_namespaced_pod', mock_list_namespaced_pod)
    @patch('kubernetes.client.api_client.ThreadPool', Mock(), create=True)
    @patch.object(Thread, 'start', Mock())
    def setUp(self):
        self.k = Kubernetes({'ttl': 30, 'scope': 'test', 'name': 'p-0',
                             'loop_wait': 10, 'retry_timeout': 10, 'labels': {'f': 'b'}})
        self.assertRaises(AttributeError, self.k._pods._build_cache)
        self.k._pods._is_ready = True
        self.assertRaises(AttributeError, self.k._kinds._build_cache)
        self.k._kinds._is_ready = True
        self.k.get_cluster()

    @patch('time.time', Mock(side_effect=[1, 10.9, 100]))
    def test__wait_caches(self):
        self.k._pods._is_ready = False
        with self.k._condition:
            self.assertRaises(RetryFailedError, self.k._wait_caches)

    def test_get_cluster(self):
        with patch.object(k8s_client.CoreV1Api, 'list_namespaced_config_map', mock_list_namespaced_config_map), \
                patch.object(k8s_client.CoreV1Api, 'list_namespaced_pod', mock_list_namespaced_pod), \
                patch('time.time', Mock(return_value=time.time() + 31)):
            self.k.get_cluster()

        with patch.object(Kubernetes, '_wait_caches', Mock(side_effect=Exception)):
            self.assertRaises(KubernetesError, self.k.get_cluster)

    @patch('kubernetes.config.load_kube_config', Mock())
    @patch.object(k8s_client.CoreV1Api, 'create_namespaced_endpoints', Mock())
    def test_update_leader(self):
        k = Kubernetes({'ttl': 30, 'scope': 'test', 'name': 'p-0', 'loop_wait': 10, 'retry_timeout': 10,
                        'labels': {'f': 'b'}, 'use_endpoints': True, 'pod_ip': '10.0.0.0'})
        self.assertIsNotNone(k.update_leader('123'))

    @patch('kubernetes.config.load_kube_config', Mock())
    @patch.object(k8s_client.CoreV1Api, 'create_namespaced_endpoints', Mock())
    def test_update_leader_with_restricted_access(self):
        k = Kubernetes({'ttl': 30, 'scope': 'test', 'name': 'p-0', 'loop_wait': 10, 'retry_timeout': 10,
                        'labels': {'f': 'b'}, 'use_endpoints': True, 'pod_ip': '10.0.0.0'})
        self.assertIsNotNone(k.update_leader('123', True))

    def test_take_leader(self):
        self.k.take_leader()
        self.k._leader_observed_record['leader'] = 'test'
        self.k.patch_or_create = Mock(return_value=False)
        self.k.take_leader()

    def test_manual_failover(self):
        with patch.object(k8s_client.CoreV1Api, 'patch_namespaced_config_map', Mock(side_effect=RetryFailedError(''))):
            self.k.manual_failover('foo', 'bar')

    def test_set_config_value(self):
        self.k.set_config_value('{}')

    @patch.object(k8s_client.CoreV1Api, 'patch_namespaced_pod', Mock(return_value=True))
    def test_touch_member(self):
        self.k.touch_member({'role': 'replica'})
        self.k._name = 'p-1'
        self.k.touch_member({'state': 'running', 'role': 'replica'})
        self.k.touch_member({'state': 'stopped', 'role': 'master'})

    def test_initialize(self):
        self.k.initialize()

    def test_delete_leader(self):
        self.k.delete_leader()

    def test_cancel_initialization(self):
        self.k.cancel_initialization()

    @patch.object(k8s_client.CoreV1Api, 'delete_collection_namespaced_config_map',
                  Mock(side_effect=k8s_client.rest.ApiException(403, '')))
    def test_delete_cluster(self):
        self.k.delete_cluster()

    @patch('kubernetes.config.load_kube_config', Mock())
    @patch.object(k8s_client.CoreV1Api, 'create_namespaced_endpoints',
                  Mock(side_effect=[k8s_client.rest.ApiException(502, ''), k8s_client.rest.ApiException(500, '')]))
    def test_delete_sync_state(self):
        k = Kubernetes({'ttl': 30, 'scope': 'test', 'name': 'p-0', 'loop_wait': 10, 'retry_timeout': 10,
                        'labels': {'f': 'b'}, 'use_endpoints': True, 'pod_ip': '10.0.0.0'})
        self.assertFalse(k.delete_sync_state())

    def test_watch(self):
        self.k.set_ttl(10)
        self.k.watch(None, 0)
        self.k.watch(None, 0)

    def test_set_history_value(self):
        self.k.set_history_value('{}')

    @patch('kubernetes.config.load_kube_config', Mock())
    @patch('patroni.dcs.kubernetes.ObjectCache', Mock())
    @patch.object(k8s_client.CoreV1Api, 'patch_namespaced_pod', Mock(return_value=True))
    @patch.object(k8s_client.CoreV1Api, 'create_namespaced_endpoints', Mock())
    @patch.object(k8s_client.CoreV1Api, 'create_namespaced_service',
                  Mock(side_effect=[True, False, k8s_client.rest.ApiException(500, '')]))
    def test__create_config_service(self):
        k = Kubernetes({'ttl': 30, 'scope': 'test', 'name': 'p-0', 'loop_wait': 10, 'retry_timeout': 10,
                        'labels': {'f': 'b'}, 'use_endpoints': True, 'pod_ip': '10.0.0.0'})
        self.assertIsNotNone(k.patch_or_create_config({'foo': 'bar'}))
        self.assertIsNotNone(k.patch_or_create_config({'foo': 'bar'}))
        k.touch_member({'state': 'running', 'role': 'replica'})


class TestCacheBuilder(unittest.TestCase):

    @patch('kubernetes.config.load_kube_config', Mock())
    @patch('kubernetes.client.api_client.ThreadPool', Mock(), create=True)
    @patch.object(Thread, 'start', Mock())
    def setUp(self):
        self.k = Kubernetes({'ttl': 30, 'scope': 'test', 'name': 'p-0',
                             'loop_wait': 10, 'retry_timeout': 10, 'labels': {'f': 'b'}})

    @patch.object(k8s_client.CoreV1Api, 'list_namespaced_config_map', mock_list_namespaced_config_map)
    @patch('patroni.dcs.kubernetes.ObjectCache._watch')
    def test__build_cache(self, mock_response):
        mock_response.return_value.read_chunked.return_value = [json.dumps(
            {'type': 'MODIFIED', 'object': {'metadata': {
                'name': self.k.config_path, 'resourceVersion': '2', 'annotations': {self.k._CONFIG: 'foo'}}}}
        ).encode('utf-8'), ('\n' + json.dumps(
            {'type': 'DELETED', 'object': {'metadata': {
                'name': self.k.config_path, 'resourceVersion': '3'}}}
        ) + '\n' + json.dumps(
            {'type': 'MDIFIED', 'object': {'metadata': {'name': self.k.config_path}}}
        ) + '\n' + json.dumps({'object': {'code': 410}}) + '\n').encode('utf-8')]
        self.k._kinds._build_cache()

    @patch('patroni.dcs.kubernetes.logger.error', Mock(side_effect=SleepException))
    @patch('patroni.dcs.kubernetes.ObjectCache._build_cache', Mock(side_effect=Exception))
    def test_run(self):
        self.assertRaises(SleepException, self.k._pods.run)
