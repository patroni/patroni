import unittest

from mock import Mock, patch
from patroni.dcs.kubernetes import Kubernetes, KubernetesError, k8s_client, k8s_watch


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
    return k8s_client.V1ConfigMapList(items=items)


def mock_list_namespaced_pod(self, *args, **kwargs):
    metadata = k8s_client.V1ObjectMeta(resource_version='1', name='p-0', annotations={'status': '{}'})
    items = [k8s_client.V1Pod(metadata=metadata)]
    return k8s_client.V1PodList(items=items)


@patch.object(k8s_client.CoreV1Api, 'patch_namespaced_config_map', Mock())
@patch.object(k8s_client.CoreV1Api, 'create_namespaced_config_map', Mock())
class TestKubernetes(unittest.TestCase):

    @patch('kubernetes.config.load_kube_config', Mock())
    @patch.object(k8s_client.CoreV1Api, 'list_namespaced_config_map', mock_list_namespaced_config_map)
    @patch.object(k8s_client.CoreV1Api, 'list_namespaced_pod', mock_list_namespaced_pod)
    def setUp(self):
        self.k = Kubernetes({'ttl': 30, 'scope': 'test', 'name': 'p-0', 'retry_timeout': 10, 'labels': {'f': 'b'}})
        with patch('time.time', Mock(return_value=1)):
            self.k.get_cluster()

    @patch.object(k8s_client.CoreV1Api, 'list_namespaced_config_map', mock_list_namespaced_config_map)
    @patch.object(k8s_client.CoreV1Api, 'list_namespaced_pod', mock_list_namespaced_pod)
    def test_get_cluster(self):
        self.k.get_cluster()
        with patch.object(k8s_client.CoreV1Api, 'list_namespaced_pod', Mock(side_effect=Exception)):
            self.assertRaises(KubernetesError, self.k.get_cluster)

    def test_update_leader(self):
        self.k.update_leader('123')

    def test_take_leader(self):
        self.k.take_leader()
        self.k._leader_observed_record['leader'] = 'test'
        self.k.patch_or_create = Mock(return_value=False)
        self.k.take_leader()

    def test_manual_failover(self):
        self.k.manual_failover('foo', 'bar')

    def test_set_config_value(self):
        self.k.set_config_value('{}')

    @patch.object(k8s_client.CoreV1Api, 'patch_namespaced_pod', Mock(side_effect=k8s_client.rest.ApiException(502, '')))
    def test_touch_member(self):
        self.k.touch_member('{}')

    def test_initialize(self):
        self.k.initialize()

    def test_delete_leader(self):
        self.k.delete_leader()

    def test_cancel_initialization(self):
        self.k.cancel_initialization()

    @patch.object(k8s_client.CoreV1Api, 'delete_collection_namespaced_config_map',
                  Mock(side_effect=k8s_client.rest.ApiException(500, '')))
    def test_delete_cluster(self):
        self.k.delete_cluster()

    def test_delete_sync_state(self):
        self.k.delete_sync_state()

    def test_watch(self):
        self.k.set_ttl(10)
        self.k.watch(None, 0)
        self.k.watch(None, 0)
        with patch.object(k8s_watch.Watch, 'stream',
                          Mock(side_effect=[Exception, [], KeyboardInterrupt,
                                            [{'object': {'metadata': {'resourceVersion': '2'}}}]])):
            self.assertFalse(self.k.watch('1', 2))
            self.assertRaises(KeyboardInterrupt, self.k.watch, '1', 2)
            self.assertTrue(self.k.watch('1', 2))
