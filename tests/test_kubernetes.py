import base64
import datetime
import json
import mock
import socket
import time
import unittest

from mock import Mock, PropertyMock, mock_open, patch
from patroni.dcs.kubernetes import Cluster, k8s_client, k8s_config, K8sConfig, K8sConnectionFailed,\
        K8sException, K8sObject, Kubernetes, KubernetesError, KubernetesRetriableException,\
        Retry, RetryFailedError, SERVICE_HOST_ENV_NAME, SERVICE_PORT_ENV_NAME
from threading import Thread
from . import MockResponse, SleepException


def mock_list_namespaced_config_map(*args, **kwargs):
    metadata = {'resource_version': '1', 'labels': {'f': 'b'}, 'name': 'test-config',
                'annotations': {'initialize': '123', 'config': '{}'}}
    items = [k8s_client.V1ConfigMap(metadata=k8s_client.V1ObjectMeta(**metadata))]
    metadata.update({'name': 'test-leader',
                     'annotations': {'optime': '1234x', 'leader': 'p-0', 'ttl': '30s', 'slots': '{', 'failsafe': '{'}})
    items.append(k8s_client.V1ConfigMap(metadata=k8s_client.V1ObjectMeta(**metadata)))
    metadata.update({'name': 'test-failover', 'annotations': {'leader': 'p-0'}})
    items.append(k8s_client.V1ConfigMap(metadata=k8s_client.V1ObjectMeta(**metadata)))
    metadata.update({'name': 'test-sync', 'annotations': {'leader': 'p-0'}})
    items.append(k8s_client.V1ConfigMap(metadata=k8s_client.V1ObjectMeta(**metadata)))
    metadata.update({'name': 'test-0-leader', 'labels': {Kubernetes._CITUS_LABEL: '0'},
                     'annotations': {'optime': '1234x', 'leader': 'p-0', 'ttl': '30s', 'slots': '{', 'failsafe': '{'}})
    items.append(k8s_client.V1ConfigMap(metadata=k8s_client.V1ObjectMeta(**metadata)))
    metadata.update({'name': 'test-0-config', 'labels': {Kubernetes._CITUS_LABEL: '0'},
                     'annotations': {'initialize': '123', 'config': '{}'}})
    items.append(k8s_client.V1ConfigMap(metadata=k8s_client.V1ObjectMeta(**metadata)))
    metadata.update({'name': 'test-1-leader', 'labels': {Kubernetes._CITUS_LABEL: '1'},
                     'annotations': {'leader': 'p-3', 'ttl': '30s'}})
    items.append(k8s_client.V1ConfigMap(metadata=k8s_client.V1ObjectMeta(**metadata)))
    metadata = k8s_client.V1ObjectMeta(resource_version='1')
    return k8s_client.V1ConfigMapList(metadata=metadata, items=items, kind='ConfigMapList')


def mock_read_namespaced_endpoints(*args, **kwargs):
    target_ref = k8s_client.V1ObjectReference(kind='Pod', resource_version='10', name='p-0',
                                              namespace='default', uid='964dfeae-e79b-4476-8a5a-1920b5c2a69d')
    address0 = k8s_client.V1EndpointAddress(ip='10.0.0.0', target_ref=target_ref)
    address1 = k8s_client.V1EndpointAddress(ip='10.0.0.1')
    port = k8s_client.V1EndpointPort(port=5432, name='postgresql', protocol='TCP')
    subset = k8s_client.V1EndpointSubset(addresses=[address1, address0], ports=[port])
    metadata = k8s_client.V1ObjectMeta(resource_version='1', labels={'f': 'b'}, name='test',
                                       annotations={'optime': '1234', 'leader': 'p-0', 'ttl': '30s'})
    return k8s_client.V1Endpoints(subsets=[subset], metadata=metadata)


def mock_list_namespaced_endpoints(*args, **kwargs):
    return k8s_client.V1EndpointsList(metadata=k8s_client.V1ObjectMeta(resource_version='1'),
                                      items=[mock_read_namespaced_endpoints()], kind='V1EndpointsList')


def mock_list_namespaced_pod(*args, **kwargs):
    metadata = k8s_client.V1ObjectMeta(resource_version='1', labels={'f': 'b', Kubernetes._CITUS_LABEL: '1'},
                                       name='p-0', annotations={'status': '{}'},
                                       uid='964dfeae-e79b-4476-8a5a-1920b5c2a69d')
    status = k8s_client.V1PodStatus(pod_ip='10.0.0.0')
    spec = k8s_client.V1PodSpec(hostname='p-0', node_name='kind-control-plane', containers=[])
    items = [k8s_client.V1Pod(metadata=metadata, status=status, spec=spec)]
    return k8s_client.V1PodList(items=items, kind='PodList')


def mock_namespaced_kind(*args, **kwargs):
    mock = Mock()
    mock.metadata.resource_version = '2'
    return mock


def mock_load_k8s_config(self, *args, **kwargs):
    self._server = ''


class TestK8sConfig(unittest.TestCase):

    def test_load_incluster_config(self):
        for env in ({}, {SERVICE_HOST_ENV_NAME: '', SERVICE_PORT_ENV_NAME: ''}):
            with patch('os.environ', env):
                self.assertRaises(k8s_config.ConfigException, k8s_config.load_incluster_config)

        with patch('os.environ', {SERVICE_HOST_ENV_NAME: 'a', SERVICE_PORT_ENV_NAME: '1'}),\
                patch('os.path.isfile', Mock(side_effect=[False, True, True, False, True, True, True, True])),\
                patch('builtins.open', Mock(side_effect=[
                    mock_open()(), mock_open(read_data='a')(), mock_open(read_data='a')(),
                    mock_open()(), mock_open(read_data='a')(), mock_open(read_data='a')()])):
            for _ in range(0, 4):
                self.assertRaises(k8s_config.ConfigException, k8s_config.load_incluster_config)
            k8s_config.load_incluster_config()
            self.assertEqual(k8s_config.server, 'https://a:1')
            self.assertEqual(k8s_config.headers.get('authorization'), 'Bearer a')

    def test_refresh_token(self):
        with patch('os.environ', {SERVICE_HOST_ENV_NAME: 'a', SERVICE_PORT_ENV_NAME: '1'}),\
                patch('os.path.isfile', Mock(side_effect=[True, True, False, True, True, True])),\
                patch('builtins.open', Mock(side_effect=[
                    mock_open(read_data='cert')(), mock_open(read_data='a')(),
                    mock_open()(), mock_open(read_data='b')(), mock_open(read_data='c')()])):
            k8s_config.load_incluster_config(token_refresh_interval=datetime.timedelta(milliseconds=100))
            self.assertEqual(k8s_config.headers.get('authorization'), 'Bearer a')
            time.sleep(0.1)
            # token file doesn't exist
            self.assertEqual(k8s_config.headers.get('authorization'), 'Bearer a')
            # token file is empty
            self.assertEqual(k8s_config.headers.get('authorization'), 'Bearer a')
            # token refreshed
            self.assertEqual(k8s_config.headers.get('authorization'), 'Bearer b')
            time.sleep(0.1)
            # token refreshed
            self.assertEqual(k8s_config.headers.get('authorization'), 'Bearer c')
            # no need to refresh token
            self.assertEqual(k8s_config.headers.get('authorization'), 'Bearer c')

    def test_load_kube_config(self):
        config = {
            "current-context": "local",
            "contexts": [{"name": "local", "context": {"user": "local", "cluster": "local"}}],
            "clusters": [{"name": "local", "cluster": {"server": "https://a:1/", "certificate-authority": "a"}}],
            "users": [{"name": "local", "user": {"username": "a", "password": "b", "client-certificate": "c"}}]
        }
        with patch('builtins.open', mock_open(read_data=json.dumps(config))):
            k8s_config.load_kube_config()
            self.assertEqual(k8s_config.server, 'https://a:1')
            self.assertEqual(k8s_config.pool_config, {'ca_certs': 'a', 'cert_file': 'c', 'cert_reqs': 'CERT_REQUIRED',
                                                      'maxsize': 10, 'num_pools': 10})

        config["users"][0]["user"]["token"] = "token"
        with patch('builtins.open', mock_open(read_data=json.dumps(config))):
            k8s_config.load_kube_config()
            self.assertEqual(k8s_config.headers.get('authorization'), 'Bearer token')

        config["users"][0]["user"]["client-key-data"] = base64.b64encode(b'foobar').decode('utf-8')
        config["clusters"][0]["cluster"]["certificate-authority-data"] = base64.b64encode(b'foobar').decode('utf-8')
        with patch('builtins.open', mock_open(read_data=json.dumps(config))),\
                patch('os.write', Mock()), patch('os.close', Mock()),\
                patch('os.remove') as mock_remove,\
                patch('atexit.register') as mock_atexit,\
                patch('tempfile.mkstemp') as mock_mkstemp:
            mock_mkstemp.side_effect = [(3, '1.tmp'), (4, '2.tmp')]
            k8s_config.load_kube_config()
            mock_atexit.assert_called_once()
            mock_remove.side_effect = OSError
            mock_atexit.call_args[0][0]()  # call _cleanup_temp_files
            mock_remove.assert_has_calls([mock.call('1.tmp'), mock.call('2.tmp')])


@patch('urllib3.PoolManager.request')
class TestApiClient(unittest.TestCase):

    @patch.object(K8sConfig, '_server', '', create=True)
    @patch('urllib3.PoolManager.request', Mock())
    def setUp(self):
        self.a = k8s_client.ApiClient(True)
        self.mock_get_ep = MockResponse()
        self.mock_get_ep.content = '{"subsets":[{"ports":[{"name":"https","protocol":"TCP","port":443}],' +\
                                   '"addresses":[{"ip":"127.0.0.1"},{"ip":"127.0.0.2"}]}]}'

    def test__do_http_request(self, mock_request):
        mock_request.side_effect = [self.mock_get_ep] + [socket.timeout]
        self.assertRaises(K8sException, self.a.call_api, 'GET', 'f')

    @patch('time.sleep', Mock())
    def test_request(self, mock_request):
        retry = Retry(deadline=10, max_delay=1, max_tries=1, retry_exceptions=KubernetesRetriableException)
        mock_request.side_effect = [self.mock_get_ep] + 3 * [socket.timeout] + [k8s_client.rest.ApiException(500, '')]
        self.assertRaises(k8s_client.rest.ApiException, retry, self.a.call_api, 'GET', 'f', _retry=retry)
        mock_request.side_effect = [self.mock_get_ep, socket.timeout, Mock(), self.mock_get_ep]
        self.assertRaises(k8s_client.rest.ApiException, retry, self.a.call_api, 'GET', 'f', _retry=retry)
        retry.deadline = 0.0001
        mock_request.side_effect = [socket.timeout, socket.timeout, self.mock_get_ep]
        self.assertRaises(K8sConnectionFailed, retry, self.a.call_api, 'GET', 'f', _retry=retry)

    def test__refresh_api_servers_cache(self, mock_request):
        mock_request.side_effect = k8s_client.rest.ApiException(403, '')
        self.a.refresh_api_servers_cache()


class TestCoreV1Api(unittest.TestCase):

    @patch('urllib3.PoolManager.request', Mock())
    @patch.object(K8sConfig, '_server', '', create=True)
    def setUp(self):
        self.a = k8s_client.CoreV1Api()
        self.a._api_client.pool_manager.request = Mock(return_value=MockResponse())

    def test_create_namespaced_service(self):
        self.assertEqual(str(self.a.create_namespaced_service('default', {}, _request_timeout=2)), '{}')

    def test_list_namespaced_endpoints(self):
        self.a._api_client.pool_manager.request.return_value.content = '{"items": [1,2,3]}'
        self.assertIsInstance(self.a.list_namespaced_endpoints('default'), K8sObject)

    def test_patch_namespaced_config_map(self):
        self.assertEqual(str(self.a.patch_namespaced_config_map('foo', 'default', {}, _request_timeout=(1, 2))), '{}')

    def test_list_namespaced_pod(self):
        self.a._api_client.pool_manager.request.return_value.status_code = 409
        self.a._api_client.pool_manager.request.return_value.content = 'foo'
        try:
            self.a.list_namespaced_pod('default', label_selector='foo=bar')
            self.assertFail()
        except k8s_client.rest.ApiException as e:
            self.assertTrue('Reason: ' in str(e))

    def test_delete_namespaced_pod(self):
        self.assertEqual(str(self.a.delete_namespaced_pod('foo', 'default', _request_timeout=(1, 2), body={})), '{}')


class BaseTestKubernetes(unittest.TestCase):

    @patch('urllib3.PoolManager.request', Mock())
    @patch('socket.TCP_KEEPIDLE', 4, create=True)
    @patch('socket.TCP_KEEPINTVL', 5, create=True)
    @patch('socket.TCP_KEEPCNT', 6, create=True)
    @patch.object(Thread, 'start', Mock())
    @patch.object(K8sConfig, 'load_kube_config', mock_load_k8s_config)
    @patch.object(K8sConfig, 'load_incluster_config', Mock(side_effect=k8s_config.ConfigException))
    @patch.object(k8s_client.CoreV1Api, 'list_namespaced_pod', mock_list_namespaced_pod, create=True)
    @patch.object(k8s_client.CoreV1Api, 'list_namespaced_config_map', mock_list_namespaced_config_map, create=True)
    def setUp(self, config=None):
        config = config or {}
        config.update(ttl=30, scope='test', name='p-0', loop_wait=10, group=0,
                      retry_timeout=10, labels={'f': 'b'}, bypass_api_service=True)
        self.k = Kubernetes(config)
        self.k._citus_group = None
        self.assertRaises(AttributeError, self.k._pods._build_cache)
        self.k._pods._is_ready = True
        self.assertRaises(TypeError, self.k._kinds._build_cache)
        self.k._kinds._is_ready = True
        self.k.get_cluster()


@patch.object(k8s_client.CoreV1Api, 'patch_namespaced_config_map', mock_namespaced_kind, create=True)
class TestKubernetesConfigMaps(BaseTestKubernetes):

    @patch('time.time', Mock(side_effect=[1, 10.9, 100]))
    def test__wait_caches(self):
        self.k._pods._is_ready = False
        with self.k._condition:
            self.assertRaises(RetryFailedError, self.k._wait_caches, time.time() + 10)

    @patch('time.time', Mock(return_value=time.time() + 100))
    def test_get_cluster(self):
        self.k.get_cluster()

        with patch.object(Kubernetes, '_wait_caches', Mock(side_effect=Exception)):
            self.assertRaises(KubernetesError, self.k.get_cluster)

    def test__get_citus_cluster(self):
        self.k._citus_group = '0'
        cluster = self.k.get_cluster()
        self.assertIsInstance(cluster, Cluster)
        self.assertIsInstance(cluster.workers[1], Cluster)

    @patch('patroni.dcs.kubernetes.logger.error')
    def test_get_citus_coordinator(self, mock_logger):
        self.assertIsInstance(self.k.get_citus_coordinator(), Cluster)
        with patch.object(Kubernetes, '_cluster_loader', Mock(side_effect=Exception)):
            self.assertIsNone(self.k.get_citus_coordinator())
            mock_logger.assert_called()
            self.assertTrue(mock_logger.call_args[0][0].startswith('Failed to load Citus coordinator'))

    def test_attempt_to_acquire_leader(self):
        with patch.object(k8s_client.CoreV1Api, 'patch_namespaced_config_map', create=True) as mock_patch:
            mock_patch.side_effect = K8sException
            self.assertRaises(KubernetesError, self.k.attempt_to_acquire_leader)
            mock_patch.side_effect = k8s_client.rest.ApiException(409, '')
            self.assertFalse(self.k.attempt_to_acquire_leader())

    def test_take_leader(self):
        self.k.take_leader()
        self.k._leader_observed_record['leader'] = 'test'
        self.k.patch_or_create = Mock(return_value=False)
        self.k.take_leader()

    def test_manual_failover(self):
        with patch.object(k8s_client.CoreV1Api, 'patch_namespaced_config_map',
                          Mock(side_effect=RetryFailedError('')), create=True):
            self.k.manual_failover('foo', 'bar')

    def test_set_config_value(self):
        with patch.object(k8s_client.CoreV1Api, 'patch_namespaced_config_map',
                          Mock(side_effect=k8s_client.rest.ApiException(409, '')), create=True):
            self.k.set_config_value('{}', 1)

    @patch.object(k8s_client.CoreV1Api, 'patch_namespaced_pod', create=True)
    def test_touch_member(self, mock_patch_namespaced_pod):
        mock_patch_namespaced_pod.return_value.metadata.resource_version = '10'
        self.k.touch_member({'role': 'replica'})
        self.k._name = 'p-1'
        self.k.touch_member({'state': 'running', 'role': 'replica'})
        self.k.touch_member({'state': 'stopped', 'role': 'primary'})

    def test_initialize(self):
        self.k.initialize()

    def test_delete_leader(self):
        self.k.delete_leader(1)

    def test_cancel_initialization(self):
        self.k.cancel_initialization()

    @patch.object(k8s_client.CoreV1Api, 'delete_collection_namespaced_config_map',
                  Mock(side_effect=k8s_client.rest.ApiException(403, '')), create=True)
    def test_delete_cluster(self):
        self.k.delete_cluster()

    def test_watch(self):
        self.k.set_ttl(10)
        self.k.watch(None, 0)
        self.k.watch(None, 0)

    def test_set_history_value(self):
        self.k.set_history_value('{}')

    @patch('patroni.dcs.kubernetes.logger.warning')
    def test_reload_config(self, mock_warning):
        self.k.reload_config({'loop_wait': 10, 'ttl': 30, 'retry_timeout': 10, 'retriable_http_codes': '401, 403 '})
        self.assertEqual(self.k._api._retriable_http_codes, self.k._api._DEFAULT_RETRIABLE_HTTP_CODES | set([401, 403]))
        self.k.reload_config({'loop_wait': 10, 'ttl': 30, 'retry_timeout': 10, 'retriable_http_codes': 402})
        self.assertEqual(self.k._api._retriable_http_codes, self.k._api._DEFAULT_RETRIABLE_HTTP_CODES | set([402]))
        self.k.reload_config({'loop_wait': 10, 'ttl': 30, 'retry_timeout': 10, 'retriable_http_codes': [405, 406]})
        self.assertEqual(self.k._api._retriable_http_codes, self.k._api._DEFAULT_RETRIABLE_HTTP_CODES | set([405, 406]))
        self.k.reload_config({'loop_wait': 10, 'ttl': 30, 'retry_timeout': 10, 'retriable_http_codes': True})
        mock_warning.assert_called_once()


class TestKubernetesEndpoints(BaseTestKubernetes):

    @patch.object(k8s_client.CoreV1Api, 'list_namespaced_endpoints', mock_list_namespaced_endpoints, create=True)
    def setUp(self, config=None):
        super(TestKubernetesEndpoints, self).setUp({'use_endpoints': True, 'pod_ip': '10.0.0.0'})

    @patch.object(k8s_client.CoreV1Api, 'patch_namespaced_endpoints', create=True)
    def test_update_leader(self, mock_patch_namespaced_endpoints):
        self.assertIsNotNone(self.k.update_leader('123', failsafe={'foo': 'bar'}))
        args = mock_patch_namespaced_endpoints.call_args[0]
        self.assertEqual(args[2].subsets[0].addresses[0].target_ref.resource_version, '10')
        self.k._kinds._object_cache['test'].subsets[:] = []
        self.assertIsNotNone(self.k.update_leader('123'))
        self.k._kinds._object_cache['test'].metadata.annotations['leader'] = 'p-1'
        self.assertFalse(self.k.update_leader('123'))

    @patch.object(k8s_client.CoreV1Api, 'read_namespaced_endpoints', create=True)
    @patch.object(k8s_client.CoreV1Api, 'patch_namespaced_endpoints', create=True)
    def test__update_leader_with_retry(self, mock_patch, mock_read):
        mock_read.return_value = mock_read_namespaced_endpoints()
        mock_patch.side_effect = k8s_client.rest.ApiException(502, '')
        self.assertFalse(self.k.update_leader('123'))
        mock_patch.side_effect = RetryFailedError('')
        self.assertRaises(KubernetesError, self.k.update_leader, '123')
        mock_patch.side_effect = k8s_client.rest.ApiException(409, '')
        with patch('time.time', Mock(side_effect=[0, 100, 200, 0, 0, 0, 0, 100, 200])):
            self.assertFalse(self.k.update_leader('123'))
            self.assertFalse(self.k.update_leader('123'))
        self.assertFalse(self.k.update_leader('123'))
        mock_patch.side_effect = [k8s_client.rest.ApiException(409, ''), mock_namespaced_kind()]
        mock_read.return_value.metadata.resource_version = '2'
        self.assertIsNotNone(self.k._update_leader_with_retry({}, '1', []))
        mock_patch.side_effect = k8s_client.rest.ApiException(409, '')
        mock_read.side_effect = RetryFailedError('')
        self.assertRaises(KubernetesError, self.k.update_leader, '123')
        mock_read.side_effect = Exception
        self.assertFalse(self.k.update_leader('123'))

    @patch.object(k8s_client.CoreV1Api, 'create_namespaced_endpoints',
                  Mock(side_effect=[k8s_client.rest.ApiException(500, ''),
                                    k8s_client.rest.ApiException(502, '')]), create=True)
    def test_delete_sync_state(self):
        self.assertFalse(self.k.delete_sync_state())

    @patch.object(k8s_client.CoreV1Api, 'patch_namespaced_pod', mock_namespaced_kind, create=True)
    @patch.object(k8s_client.CoreV1Api, 'create_namespaced_endpoints', mock_namespaced_kind, create=True)
    @patch.object(k8s_client.CoreV1Api, 'create_namespaced_service',
                  Mock(side_effect=[True,
                                    False,
                                    k8s_client.rest.ApiException(409, ''),
                                    k8s_client.rest.ApiException(403, ''),
                                    k8s_client.rest.ApiException(500, ''),
                                    Exception("Unexpected")
                                    ]), create=True)
    @patch('patroni.dcs.kubernetes.logger.exception')
    def test__create_config_service(self, mock_logger_exception):
        self.assertIsNotNone(self.k.patch_or_create_config({'foo': 'bar'}))
        self.assertIsNotNone(self.k.patch_or_create_config({'foo': 'bar'}))

        self.k.patch_or_create_config({'foo': 'bar'})
        mock_logger_exception.assert_not_called()

        self.k.patch_or_create_config({'foo': 'bar'})
        mock_logger_exception.assert_not_called()

        self.k.patch_or_create_config({'foo': 'bar'})
        mock_logger_exception.assert_called_once()
        self.assertEqual(('create_config_service failed',), mock_logger_exception.call_args[0])
        mock_logger_exception.reset_mock()

        self.k.touch_member({'state': 'running', 'role': 'replica'})
        mock_logger_exception.assert_called_once()
        self.assertEqual(('create_config_service failed',), mock_logger_exception.call_args[0])


class TestCacheBuilder(BaseTestKubernetes):

    @patch.object(k8s_client.CoreV1Api, 'list_namespaced_config_map', mock_list_namespaced_config_map, create=True)
    @patch('patroni.dcs.kubernetes.ObjectCache._watch')
    def test__build_cache(self, mock_response):
        self.k._citus_group = '0'
        mock_response.return_value.read_chunked.return_value = [json.dumps(
            {'type': 'MODIFIED', 'object': {'metadata': {
                'name': self.k.config_path, 'resourceVersion': '2', 'annotations': {self.k._CONFIG: 'foo'}}}}
        ).encode('utf-8'), ('\n' + json.dumps(
            {'type': 'DELETED', 'object': {'metadata': {
                'name': self.k.config_path, 'resourceVersion': '3'}}}
        ) + '\n' + json.dumps(
            {'type': 'MDIFIED', 'object': {'metadata': {'name': self.k.config_path}}}
        ) + '\n').encode('utf-8'), b'{"object":{', b'"code":410}}\n']
        self.k._kinds._build_cache()

    @patch('patroni.dcs.kubernetes.logger.error', Mock(side_effect=SleepException))
    @patch('patroni.dcs.kubernetes.ObjectCache._build_cache', Mock(side_effect=Exception))
    def test_run(self):
        self.assertRaises(SleepException, self.k._pods.run)

    @patch('time.sleep', Mock())
    def test__list(self):
        self.k._pods._func = Mock(side_effect=Exception)
        self.assertRaises(Exception, self.k._pods._list)

    @patch('patroni.dcs.kubernetes.ObjectCache._watch', Mock(return_value=None))
    def test__do_watch(self):
        self.assertRaises(AttributeError, self.k._kinds._do_watch, '1')

    @patch.object(k8s_client.CoreV1Api, 'list_namespaced_config_map', mock_list_namespaced_config_map, create=True)
    @patch('patroni.dcs.kubernetes.ObjectCache._watch')
    def test_kill_stream(self, mock_watch):
        self.k._kinds.kill_stream()
        mock_watch.return_value.read_chunked.return_value = []
        mock_watch.return_value.connection.sock.close.side_effect = Exception
        self.k._kinds._do_watch('1')
        self.k._kinds.kill_stream()
        type(mock_watch.return_value).connection = PropertyMock(side_effect=Exception)
        self.k._kinds.kill_stream()
