import json
import time
import unittest

from mock import Mock, patch
from patroni.dcs.kubernetes import Kubernetes, KubernetesError, k8s_client, RetryFailedError
from threading import Thread
from . import SleepException


def mock_list_namespaced_config_map(*args, **kwargs):
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


def mock_list_namespaced_endpoints(*args, **kwargs):
    target_ref = k8s_client.V1ObjectReference(kind='Pod', resource_version='10', name='p-0',
                                              namespace='default', uid='964dfeae-e79b-4476-8a5a-1920b5c2a69d')
    address0 = k8s_client.V1EndpointAddress(ip='10.0.0.0', target_ref=target_ref)
    address1 = k8s_client.V1EndpointAddress(ip='10.0.0.1')
    port = k8s_client.V1EndpointPort(port=5432, name='postgresql', protocol='TCP')
    subset = k8s_client.V1EndpointSubset(addresses=[address1, address0], ports=[port])
    metadata = k8s_client.V1ObjectMeta(resource_version='1', labels={'f': 'b'}, name='test',
                                       annotations={'optime': '1234', 'leader': 'p-0', 'ttl': '30s'})
    endpoint = k8s_client.V1Endpoints(subsets=[subset], metadata=metadata)
    metadata = k8s_client.V1ObjectMeta(resource_version='1')
    return k8s_client.V1EndpointsList(metadata=metadata, items=[endpoint], kind='V1EndpointsList')


def mock_list_namespaced_pod(*args, **kwargs):
    metadata = k8s_client.V1ObjectMeta(resource_version='1', name='p-0', annotations={'status': '{}'},
                                       uid='964dfeae-e79b-4476-8a5a-1920b5c2a69d')
    status = k8s_client.V1PodStatus(pod_ip='10.0.0.0')
    spec = k8s_client.V1PodSpec(hostname='p-0', node_name='kind-control-plane', containers=[])
    items = [k8s_client.V1Pod(metadata=metadata, status=status, spec=spec)]
    return k8s_client.V1PodList(items=items, kind='PodList')


def mock_namespaced_kind(*args, **kwargs):
    mock = Mock()
    mock.metadata.resource_version = '2'
    return mock


class BaseTestKubernetes(unittest.TestCase):

    @patch('socket.TCP_KEEPIDLE', 4, create=True)
    @patch('socket.TCP_KEEPINTVL', 5, create=True)
    @patch('socket.TCP_KEEPCNT', 6, create=True)
    @patch('kubernetes.config.load_kube_config', Mock())
    @patch('kubernetes.client.api_client.ThreadPool', Mock(), create=True)
    @patch.object(Thread, 'start', Mock())
    @patch.object(k8s_client.CoreV1Api, 'list_namespaced_pod', mock_list_namespaced_pod)
    @patch.object(k8s_client.CoreV1Api, 'list_namespaced_config_map', mock_list_namespaced_config_map)
    def setUp(self, config=None):
        config = config or {}
        config.update(ttl=30, scope='test', name='p-0', loop_wait=10, retry_timeout=10, labels={'f': 'b'})
        self.k = Kubernetes(config)
        self.assertRaises(AttributeError, self.k._pods._build_cache)
        self.k._pods._is_ready = True
        self.assertRaises(AttributeError, self.k._kinds._build_cache)
        self.k._kinds._is_ready = True
        self.k.get_cluster()


@patch.object(k8s_client.CoreV1Api, 'patch_namespaced_config_map', mock_namespaced_kind)
class TestKubernetesConfigMaps(BaseTestKubernetes):

    @patch('time.time', Mock(side_effect=[1, 10.9, 100]))
    def test__wait_caches(self):
        self.k._pods._is_ready = False
        with self.k._condition:
            self.assertRaises(RetryFailedError, self.k._wait_caches)

    @patch('time.time', Mock(return_value=time.time() + 100))
    def test_get_cluster(self):
        self.k.get_cluster()

        with patch.object(Kubernetes, '_wait_caches', Mock(side_effect=Exception)):
            self.assertRaises(KubernetesError, self.k.get_cluster)

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

    @patch.object(k8s_client.CoreV1Api, 'patch_namespaced_pod')
    def test_touch_member(self, mock_patch_namespaced_pod):
        mock_patch_namespaced_pod.return_value.metadata.resource_version = '10'
        self.k.touch_member({'role': 'replica'})
        self.k._name = 'p-1'
        self.k.touch_member({'state': 'running', 'role': 'replica'})
        self.k.touch_member({'state': 'stopped', 'role': 'master'})

    def test_initialize(self):
        self.k.initialize()

    def test_delete_leader(self):
        self.k.delete_leader(1)

    def test_cancel_initialization(self):
        self.k.cancel_initialization()

    @patch.object(k8s_client.CoreV1Api, 'delete_collection_namespaced_config_map',
                  Mock(side_effect=k8s_client.rest.ApiException(403, '')))
    def test_delete_cluster(self):
        self.k.delete_cluster()

    def test_watch(self):
        self.k.set_ttl(10)
        self.k.watch(None, 0)
        self.k.watch(None, 0)

    def test_set_history_value(self):
        self.k.set_history_value('{}')


class TestKubernetesEndpoints(BaseTestKubernetes):

    @patch.object(k8s_client.CoreV1Api, 'list_namespaced_endpoints', mock_list_namespaced_endpoints)
    def setUp(self, config=None):
        super(TestKubernetesEndpoints, self).setUp({'use_endpoints': True, 'pod_ip': '10.0.0.0'})

    @patch.object(k8s_client.CoreV1Api, 'patch_namespaced_endpoints')
    def test_update_leader(self, mock_patch_namespaced_endpoints):
        self.assertIsNotNone(self.k.update_leader('123'))
        args = mock_patch_namespaced_endpoints.call_args[0]
        self.assertEqual(args[2].subsets[0].addresses[0].target_ref.resource_version, '10')
        self.k._kinds._object_cache['test'].subsets[:] = []
        self.assertIsNotNone(self.k.update_leader('123'))
        self.k._kinds._object_cache['test'].metadata.annotations['leader'] = 'p-1'
        self.assertFalse(self.k.update_leader('123'))

    @patch.object(k8s_client.CoreV1Api, 'patch_namespaced_endpoints', mock_namespaced_kind)
    def test_update_leader_with_restricted_access(self):
        self.assertIsNotNone(self.k.update_leader('123', True))

    @patch.object(k8s_client.CoreV1Api, 'patch_namespaced_endpoints')
    def test__update_leader_with_retry(self, mock_patch):
        mock_patch.side_effect = k8s_client.rest.ApiException(502, '')
        self.assertFalse(self.k.update_leader('123'))
        mock_patch.side_effect = RetryFailedError('')
        self.assertFalse(self.k.update_leader('123'))
        mock_patch.side_effect = k8s_client.rest.ApiException(409, '')
        with patch('time.time', Mock(side_effect=[0, 100, 200])):
            self.assertFalse(self.k.update_leader('123'))
        with patch('time.sleep', Mock()):
            self.assertFalse(self.k.update_leader('123'))
            mock_patch.side_effect = [k8s_client.rest.ApiException(409, ''), mock_namespaced_kind()]
            self.k._kinds._object_cache['test'].metadata.resource_version = '2'
            self.assertIsNotNone(self.k._update_leader_with_retry({}, '1', []))

    @patch.object(k8s_client.CoreV1Api, 'create_namespaced_endpoints',
                  Mock(side_effect=[k8s_client.rest.ApiException(500, ''), k8s_client.rest.ApiException(502, '')]))
    def test_delete_sync_state(self):
        self.assertFalse(self.k.delete_sync_state())

    @patch.object(k8s_client.CoreV1Api, 'patch_namespaced_pod', mock_namespaced_kind)
    @patch.object(k8s_client.CoreV1Api, 'create_namespaced_endpoints', mock_namespaced_kind)
    @patch.object(k8s_client.CoreV1Api, 'create_namespaced_service',
                  Mock(side_effect=[True, False, k8s_client.rest.ApiException(500, '')]))
    def test__create_config_service(self):
        self.assertIsNotNone(self.k.patch_or_create_config({'foo': 'bar'}))
        self.assertIsNotNone(self.k.patch_or_create_config({'foo': 'bar'}))
        self.k.touch_member({'state': 'running', 'role': 'replica'})


class TestCacheBuilder(BaseTestKubernetes):

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

    @patch('time.sleep', Mock())
    def test__list(self):
        self.k._pods._func = Mock(side_effect=Exception)
        self.assertRaises(Exception, self.k._pods._list)
