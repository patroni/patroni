import json
import unittest

from unittest.mock import Mock

import urllib3

from patroni.dcs import Cluster, Leader, Member
from patroni.dcs.nomad import Nomad, NomadClient, NomadConflict, NomadError, NomadNotFound
from patroni.postgresql.mpp import get_mpp


def variable(path, value='', index=1, lock_id=None):
    ret = {'Path': path, 'ModifyIndex': index, 'Items': {'value': value}}
    if lock_id:
        ret['Lock'] = {'ID': lock_id, 'TTL': '30s', 'LockDelay': '10s'}
    return ret


class TestNomadClient(unittest.TestCase):

    def setUp(self):
        self.client = NomadClient(token='secret', namespace='testing', region='global')
        self.client.http.request = Mock()

    @staticmethod
    def response(status=200, data=b'{}', headers=None):
        return urllib3.response.HTTPResponse(status=status, body=data, headers=headers or {}, preload_content=True)

    def test_request(self):
        self.client.http.request.return_value = self.response(
            data=b'{"ModifyIndex":2}', headers={'X-Nomad-Index': '3'})
        ret = self.client.put_variable('service/a b/config', '{}', cas=1)

        self.assertEqual(ret['ModifyIndex'], 2)
        args, kwargs = self.client.http.request.call_args
        self.assertEqual(args[0], 'PUT')
        self.assertIn('/v1/var/service/a%20b/config?', args[1])
        self.assertIn('cas=1', args[1])
        self.assertIn('namespace=testing', args[1])
        self.assertIn('region=global', args[1])
        self.assertEqual(kwargs['headers']['X-Nomad-Token'], 'secret')
        self.assertEqual(json.loads(kwargs['body']), {'Items': {'value': '{}'}})
        self.assertEqual(kwargs['timeout'].total, self.client._read_timeout)

    def test_statuses(self):
        self.client.http.request.return_value = self.response(404, b'not found')
        self.assertRaises(NomadNotFound, self.client.get_variable, 'missing')
        self.client.http.request.return_value = self.response(409, b'conflict')
        self.assertRaises(NomadConflict, self.client.put_variable, 'key', 'value', 1)
        self.client.http.request.return_value = self.response(500, b'broken')
        self.assertRaises(NomadError, self.client.get_variable, 'key')
        self.client.http.request.return_value = self.response(200, b'{')
        self.assertRaises(NomadError, self.client.get_variable, 'key')

    def test_lock_requests(self):
        self.client.http.request.return_value = self.response(data=b'{"Lock":{"ID":"123"}}')
        self.assertEqual(self.client.acquire_lock('leader', 'node1', 30, 10)['Lock']['ID'], '123')
        request = self.client.http.request.call_args
        self.assertIn('lock-acquire=', request.args[1])
        self.assertEqual(json.loads(request.kwargs['body'])['Lock'], {'TTL': '30s', 'LockDelay': '10s'})

        self.client.renew_lock('leader', '123')
        self.assertIn('lock-renew=', self.client.http.request.call_args.args[1])
        self.client.release_lock('leader', '123')
        body = json.loads(self.client.http.request.call_args.kwargs['body'])
        self.assertIn('lock-release=', self.client.http.request.call_args.args[1])
        self.assertNotIn('Items', body)

        self.client.acquire_lock('leader', 'node1', 30, 10, '123')
        self.assertEqual(json.loads(self.client.http.request.call_args.kwargs['body'])['Lock']['ID'], '123')

    def test_list_pagination_and_delete(self):
        self.client.http.request.side_effect = [
            self.response(data=b'[{"Path":"service/a"}]', headers={'X-Nomad-NextToken': 'next'}),
            self.response(data=b'[{"Path":"service/b"}]'),
            self.response(status=204, data=b'')]
        self.assertEqual([v['Path'] for v in self.client.list_variables('service/')], ['service/a', 'service/b'])
        self.assertIn('next_token=next', self.client.http.request.call_args_list[1].args[1])
        self.assertTrue(self.client.delete_variable('service/a', 1))


class TestNomad(unittest.TestCase):

    def setUp(self):
        self.c = Nomad({'scope': 'test', 'name': 'postgresql1', 'ttl': 30, 'retry_timeout': 10,
                        'loop_wait': 10, 'host': 'localhost:4646'}, get_mpp({}))
        self.c._client = Mock()

    def load_fixture(self, unlocked=False):
        prefix = 'service/test/'
        values = {
            prefix + 'initialize': variable(prefix + 'initialize', 'sysid', 1),
            prefix + 'config': variable(prefix + 'config', '{"ttl":30}', 2),
            prefix + 'history': variable(prefix + 'history', '[[1,2,"x"]]', 3),
            prefix + 'status': variable(prefix + 'status', '{"optime":42,"slots":{"a":1}}', 4),
            prefix + 'members/postgresql1': variable(prefix + 'members/postgresql1',
                                                     '{"conn_url":"postgres://localhost/postgres"}', 5,
                                                     None if unlocked else 'member-lock'),
            prefix + 'members/stale': variable(prefix + 'members/stale', '{}', 6),
            prefix + 'leader': variable(prefix + 'leader', 'postgresql1', 7,
                                        None if unlocked else 'leader-lock'),
            prefix + 'failover': variable(prefix + 'failover', '{"leader":"postgresql0"}', 8),
            prefix + 'sync': variable(prefix + 'sync', '{"leader":"postgresql1","sync_standby":"postgresql0"}', 9),
            prefix + 'failsafe': variable(prefix + 'failsafe', '{"postgresql1":"http://localhost:8008"}', 10)}
        self.c._client.list_variables.return_value = [{'Path': path} for path in values]
        self.c._client.get_variable.side_effect = lambda path, deadline=None: values[path]

    def test_get_cluster(self):
        self.load_fixture()
        cluster = self.c.get_cluster()
        self.assertIsInstance(cluster, Cluster)
        self.assertEqual(cluster.initialize, 'sysid')
        self.assertEqual(cluster.leader.name, 'postgresql1')
        self.assertEqual(cluster.leader.session, 'leader-lock')
        self.assertEqual([m.name for m in cluster.members], ['postgresql1'])
        self.assertEqual(cluster.status.last_lsn, 42)
        self.assertEqual(cluster.sync.leader, 'postgresql1')
        self.assertEqual(cluster.failsafe, {'postgresql1': 'http://localhost:8008'})

    def test_unlocked_records_are_stale(self):
        self.load_fixture(unlocked=True)
        cluster = self.c.get_cluster()
        self.assertIsNone(cluster.leader)
        self.assertEqual(cluster.members, [])

    def test_empty_and_disappearing_variables(self):
        self.c._client.list_variables.return_value = []
        self.assertIsNone(self.c.get_cluster().leader)
        self.c._client.list_variables.return_value = [{'Path': 'service/test/config'}]
        self.c._client.get_variable.side_effect = NomadNotFound('gone')
        self.assertIsNone(self.c.get_cluster().config)
        self.c._client.list_variables.side_effect = NomadError('down')
        self.assertRaises(NomadError, self.c.get_cluster)

    def test_touch_member(self):
        self.c._client.acquire_lock.return_value = variable(self.c.member_path, '{}', 1, 'member-lock')
        data = {'conn_url': 'postgres://localhost/postgres'}
        self.assertTrue(self.c.touch_member(data))
        self.assertEqual(self.c._member_lock, 'member-lock')
        self.assertTrue(self.c.touch_member(data))
        self.c._client.renew_lock.assert_called_with(self.c.member_path, 'member-lock')

        changed = {'conn_url': 'postgres://localhost/postgres', 'role': 'replica'}
        self.assertTrue(self.c.touch_member(changed))
        self.c._client.acquire_lock.assert_called_with(self.c.member_path,
                                                       json.dumps(changed, separators=(',', ':')),
                                                       30, 10, 'member-lock')

        self.c._client.renew_lock.side_effect = NomadConflict('lost')
        self.assertFalse(self.c.touch_member(changed))
        self.assertIsNone(self.c._member_lock)

    def test_recovers_member_lock_after_restart(self):
        data = {'conn_url': 'postgres://localhost/postgres'}
        self.c._cluster = Cluster(None, None, None, Mock(),
                                  [Member(1, self.c._name, 'existing-lock', data)], None, Mock(), None, None)
        self.c._cluster_valid_till = float('inf')
        self.assertTrue(self.c.touch_member(data))
        self.c._client.acquire_lock.assert_not_called()
        self.c._client.renew_lock.assert_called_once_with(self.c.member_path, 'existing-lock')

    def test_leader_lifecycle(self):
        self.c._client.acquire_lock.return_value = variable(self.c.leader_path, self.c._name, 1, 'leader-lock')
        self.assertTrue(self.c.attempt_to_acquire_leader())
        self.assertEqual(self.c._leader_lock, 'leader-lock')
        leader = Leader(1, 'leader-lock', Member(-1, self.c._name, None, {}))
        self.assertTrue(self.c._update_leader(leader))
        self.c._client.renew_lock.assert_called_with(self.c.leader_path, 'leader-lock')

        self.c._client.release_lock.return_value = {'ModifyIndex': 2}
        self.assertTrue(self.c._delete_leader(leader))
        self.c._client.delete_variable.assert_called_with(self.c.leader_path, 2)
        self.assertIsNone(self.c._leader_lock)

    def test_recovers_leader_lock_after_restart(self):
        leader = Leader(1, 'existing-lock', Member(-1, self.c._name, None, {}))
        self.assertTrue(self.c._update_leader(leader))
        self.c._client.renew_lock.assert_called_once_with(self.c.leader_path, 'existing-lock')

    def test_leader_conflicts_and_ownership(self):
        self.c._client.acquire_lock.side_effect = NomadConflict('held')
        self.assertFalse(self.c.attempt_to_acquire_leader())
        self.c._client.acquire_lock.side_effect = NomadError('down')
        self.assertRaises(NomadError, self.c.attempt_to_acquire_leader)

        leader = Leader(1, 'other-lock', Member(-1, self.c._name, None, {}))
        self.c._leader_lock = 'leader-lock'
        self.assertFalse(self.c._delete_leader(leader))
        self.c._client.release_lock.assert_not_called()

    def test_persistent_values(self):
        self.c._client.put_variable.return_value = {'ModifyIndex': 12}
        self.assertTrue(self.c.set_config_value('{}', 1))
        self.assertTrue(self.c.set_failover_value('{}'))
        self.assertTrue(self.c.initialize(True, 'sysid'))
        self.assertEqual(self.c.set_sync_state_value('{}', 2), 12)
        self.assertTrue(self.c.set_history_value('[]'))
        self.assertTrue(self.c._write_status('{}'))
        self.assertTrue(self.c._write_failsafe('{}'))
        self.assertTrue(self.c._write_leader_optime('1'))

        self.assertTrue(self.c.cancel_initialization())
        self.assertTrue(self.c.delete_sync_state(2))
        self.c._client.list_variables.return_value = [{'Path': 'service/test/config', 'ModifyIndex': 12}]
        self.c._client.get_variable.return_value = variable('service/test/config', '{}', 12)
        self.assertTrue(self.c.delete_cluster())

    def test_mpp_cluster(self):
        self.c._mpp = get_mpp({'citus': {'group': 0, 'database': 'postgres'}})
        value = variable('service/test/0/initialize', 'sysid', 1)
        self.c._client.list_variables.return_value = [{'Path': value['Path']}]
        self.c._client.get_variable.return_value = value
        cluster = self.c.get_cluster()
        self.assertEqual(cluster.initialize, 'sysid')

    def test_ttl_validation(self):
        self.assertRaises(ValueError, self.c.set_ttl, 9)
        self.assertRaises(ValueError, self.c.set_ttl, 86401)
        self.assertTrue(self.c.set_ttl(31))
        self.assertEqual(self.c.ttl, 31)

    def test_reload_config(self):
        self.c.reload_config({'loop_wait': 5, 'ttl': 30, 'retry_timeout': 6,
                              'nomad': {'url': 'https://nomad.example:4647', 'token': 'new', 'lock_delay': 11}})
        self.assertEqual(self.c._client.base_uri, 'https://nomad.example:4647')
        self.assertEqual(self.c._client.token, 'new')
        self.assertEqual(self.c._lock_delay, 11)


if __name__ == '__main__':
    unittest.main()
