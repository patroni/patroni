import etcd
import os
import requests
import sys
import unittest

from click.testing import CliRunner
from mock import patch, Mock
from patroni.ctl import ctl, members, store_config, load_config, output_members, request_patroni, get_dcs, parse_dcs, \
    wait_for_leader, get_all_members, get_any_member, get_cursor, query_member, configure, PatroniCtlException
from patroni.dcs.etcd import Client
from psycopg2 import OperationalError
from test_etcd import etcd_read, requests_get, socket_getaddrinfo, MockResponse
from test_ha import get_cluster_initialized_without_leader, get_cluster_initialized_with_leader, \
    get_cluster_initialized_with_only_leader, get_cluster_not_initialized_without_leader
from test_postgresql import MockConnect, psycopg2_connect

CONFIG_FILE_PATH = './test-ctl.yaml'


def test_rw_config():
    runner = CliRunner()
    with runner.isolated_filesystem():
        store_config({'etcd': {'host': 'localhost:2379'}}, CONFIG_FILE_PATH + '/dummy')
        sys.argv = ['patronictl.py', '']
        load_config(CONFIG_FILE_PATH + '/dummy', None)
        load_config(CONFIG_FILE_PATH + '/dummy', '0.0.0.0')
        os.remove(CONFIG_FILE_PATH + '/dummy')
        os.rmdir(CONFIG_FILE_PATH)


@patch('patroni.ctl.load_config',
       Mock(return_value={'postgresql': {'data_dir': '.', 'parameters': {}, 'retry_timeout': 5},
                          'restapi': {'auth': 'u:p', 'listen': ''}, 'etcd': {'host': 'localhost:2379'}}))
class TestCtl(unittest.TestCase):

    @patch('socket.getaddrinfo', socket_getaddrinfo)
    def setUp(self):
        with patch.object(Client, 'machines') as mock_machines:
            mock_machines.__get__ = Mock(return_value=['http://remotehost:2379'])
            self.runner = CliRunner()
            self.e = get_dcs({'etcd': {'ttl': 30, 'host': 'ok:2379', 'retry_timeout': 10}}, 'foo')

    @patch('psycopg2.connect', psycopg2_connect)
    def test_get_cursor(self):
        self.assertIsNone(get_cursor(get_cluster_initialized_without_leader(), {}, role='master'))

        self.assertIsNotNone(get_cursor(get_cluster_initialized_with_leader(), {}, role='master'))

        # MockCursor returns pg_is_in_recovery as false
        self.assertIsNone(get_cursor(get_cluster_initialized_with_leader(), {}, role='replica'))

        self.assertIsNotNone(get_cursor(get_cluster_initialized_with_leader(), {'database': 'foo'}, role='any'))

    def test_parse_dcs(self):
        assert parse_dcs(None) is None
        assert parse_dcs('localhost') == {'etcd': {'host': 'localhost:2379'}}
        assert parse_dcs('') == {'etcd': {'host': 'localhost:2379'}}
        assert parse_dcs('localhost:8500') == {'consul': {'host': 'localhost:8500'}}
        assert parse_dcs('zookeeper://localhost') == {'zookeeper': {'hosts': ['localhost:2181']}}
        assert parse_dcs('exhibitor://dummy') == {'exhibitor': {'hosts': ['dummy'], 'port': 8181}}
        assert parse_dcs('consul://localhost') == {'consul': {'host': 'localhost:8500'}}
        self.assertRaises(PatroniCtlException, parse_dcs, 'invalid://test')

    def test_output_members(self):
        cluster = get_cluster_initialized_with_leader()
        self.assertIsNone(output_members(cluster, name='abc', fmt='pretty'))
        self.assertIsNone(output_members(cluster, name='abc', fmt='json'))
        self.assertIsNone(output_members(cluster, name='abc', fmt='tsv'))

    @patch('patroni.ctl.get_dcs')
    @patch('patroni.ctl.request_patroni', Mock(return_value=MockResponse()))
    def test_failover(self, mock_get_dcs):
        mock_get_dcs.return_value = self.e
        mock_get_dcs.return_value.get_cluster = get_cluster_initialized_with_leader
        result = self.runner.invoke(ctl, ['failover', 'dummy'], input='leader\nother\n\ny')
        assert 'leader' in result.output

        result = self.runner.invoke(ctl, ['failover', 'dummy'], input='leader\nother\n2300-01-01T12:23:00\ny')
        assert result.exit_code == 0

        with patch('patroni.dcs.Cluster.is_paused', Mock(return_value=True)):
            result = self.runner.invoke(ctl, ['failover', 'dummy', '--force', '--scheduled', '2015-01-01T12:00:00'])
            assert result.exit_code == 1

        # Aborting failover,as we anser NO to the confirmation
        result = self.runner.invoke(ctl, ['failover', 'dummy'], input='leader\nother\n\nN')
        assert result.exit_code == 1

        # Target and source are equal
        result = self.runner.invoke(ctl, ['failover', 'dummy'], input='leader\nleader\n\ny')
        assert result.exit_code == 1

        # Reality is not part of this cluster
        result = self.runner.invoke(ctl, ['failover', 'dummy'], input='leader\nReality\n\ny')
        assert result.exit_code == 1

        result = self.runner.invoke(ctl, ['failover', 'dummy', '--force'])
        assert 'Member' in result.output

        result = self.runner.invoke(ctl, ['failover', 'dummy', '--force', '--scheduled', '2015-01-01T12:00:00+01:00'])
        assert result.exit_code == 0

        # Invalid timestamp
        result = self.runner.invoke(ctl, ['failover', 'dummy', '--force', '--scheduled', 'invalid'])
        assert result.exit_code != 0

        # Invalid timestamp
        result = self.runner.invoke(ctl, ['failover', 'dummy', '--force', '--scheduled', '2115-02-30T12:00:00+01:00'])
        assert result.exit_code != 0

        # Specifying wrong leader
        result = self.runner.invoke(ctl, ['failover', 'dummy'], input='dummy')
        assert result.exit_code == 1

        with patch('patroni.ctl.request_patroni', Mock(side_effect=Exception)):
            # Non-responding patroni
            result = self.runner.invoke(ctl, ['failover', 'dummy'], input='leader\nother\n2300-01-01T12:23:00\ny')
            assert 'falling back to DCS' in result.output

        with patch('patroni.ctl.request_patroni') as mocked:
            mocked.return_value.status_code = 500
            result = self.runner.invoke(ctl, ['failover', 'dummy'], input='leader\nother\n\ny')
            assert 'Failover failed' in result.output

        # No members available
        mock_get_dcs.return_value.get_cluster = get_cluster_initialized_with_only_leader
        result = self.runner.invoke(ctl, ['failover', 'dummy'], input='leader\nother\n\ny')
        assert result.exit_code == 1

        # No master available
        mock_get_dcs.return_value.get_cluster = get_cluster_initialized_without_leader
        result = self.runner.invoke(ctl, ['failover', 'dummy'], input='leader\nother\n\ny')
        assert result.exit_code == 1

    def test_get_dcs(self):
        self.assertRaises(PatroniCtlException, get_dcs, {'dummy': {}}, 'dummy')

    @patch('psycopg2.connect', psycopg2_connect)
    @patch('patroni.ctl.query_member', Mock(return_value=([['mock column']], None)))
    @patch('patroni.ctl.get_dcs')
    @patch.object(etcd.Client, 'read', etcd_read)
    def test_query(self, mock_get_dcs):
        mock_get_dcs.return_value = self.e
        # Mutually exclusive
        result = self.runner.invoke(ctl, ['query', 'alpha', '--member', 'abc', '--role', 'master'])
        assert result.exit_code == 1

        with self.runner.isolated_filesystem():
            with open('dummy', 'w') as dummy_file:
                dummy_file.write('SELECT 1')

            # Mutually exclusive
            result = self.runner.invoke(ctl, ['query', 'alpha', '--file', 'dummy', '--command', 'dummy'])
            assert result.exit_code == 1

            result = self.runner.invoke(ctl, ['query', 'alpha', '--file', 'dummy'])
            assert result.exit_code == 0

            os.remove('dummy')

        result = self.runner.invoke(ctl, ['query', 'alpha', '--command', 'SELECT 1'])
        assert 'mock column' in result.output

        # --command or --file is mandatory
        result = self.runner.invoke(ctl, ['query', 'alpha'])
        assert result.exit_code == 1

        result = self.runner.invoke(ctl, ['query', 'alpha', '--command', 'SELECT 1', '--username', 'root',
                                          '--password', '--dbname', 'postgres'], input='ab\nab')
        assert 'mock column' in result.output

    def test_query_member(self):
        with patch('patroni.ctl.get_cursor', Mock(return_value=MockConnect().cursor())):
            rows = query_member(None, None, None, 'master', 'SELECT pg_is_in_recovery()', {})
            self.assertTrue('False' in str(rows))

            rows = query_member(None, None, None, 'replica', 'SELECT pg_is_in_recovery()', {})
            self.assertEquals(rows, (None, None))

            with patch('test_postgresql.MockCursor.execute', Mock(side_effect=OperationalError('bla'))):
                rows = query_member(None, None, None, 'replica', 'SELECT pg_is_in_recovery()', {})

        with patch('patroni.ctl.get_cursor', Mock(return_value=None)):
            rows = query_member(None, None, None, None, 'SELECT pg_is_in_recovery()', {})
            self.assertTrue('No connection to' in str(rows))

            rows = query_member(None, None, None, 'replica', 'SELECT pg_is_in_recovery()', {})
            self.assertTrue('No connection to' in str(rows))

        with patch('patroni.ctl.get_cursor', Mock(side_effect=OperationalError('bla'))):
            rows = query_member(None, None, None, 'replica', 'SELECT pg_is_in_recovery()', {})

    @patch('patroni.ctl.get_dcs')
    def test_dsn(self, mock_get_dcs):
        mock_get_dcs.return_value.get_cluster = get_cluster_initialized_with_leader
        result = self.runner.invoke(ctl, ['dsn', 'alpha'])
        assert 'host=127.0.0.1 port=5435' in result.output

        # Mutually exclusive options
        result = self.runner.invoke(ctl, ['dsn', 'alpha', '--role', 'master', '--member', 'dummy'])
        assert result.exit_code == 1

        # Non-existing member
        result = self.runner.invoke(ctl, ['dsn', 'alpha', '--member', 'dummy'])
        assert result.exit_code == 1

    @patch('requests.post', requests_get)
    @patch('patroni.ctl.get_dcs')
    def test_restart_reinit(self, mock_get_dcs):
        mock_get_dcs.return_value.get_cluster = get_cluster_initialized_with_leader
        result = self.runner.invoke(ctl, ['restart', 'alpha'], input='y\n\nnow')
        assert 'Failed: restart for' in result.output
        assert result.exit_code == 0

        result = self.runner.invoke(ctl, ['reinit', 'alpha'], input='y')
        assert result.exit_code == 1

        # successful reinit
        result = self.runner.invoke(ctl, ['reinit', 'alpha', 'other'], input='y')
        assert result.exit_code == 0

        # Aborted restart
        result = self.runner.invoke(ctl, ['restart', 'alpha'], input='N')
        assert result.exit_code == 1

        result = self.runner.invoke(ctl, ['restart', 'alpha', '--pending', '--force'])
        assert result.exit_code == 0

        # Not a member
        result = self.runner.invoke(ctl, ['restart', 'alpha', 'dummy', '--any'], input='y')
        assert result.exit_code == 1

        # Wrong pg version
        result = self.runner.invoke(ctl, ['restart', 'alpha', '--any', '--pg-version', '9.1'], input='y')
        assert 'Error: PostgreSQL version' in result.output
        assert result.exit_code == 1

        result = self.runner.invoke(ctl, ['restart', 'alpha', '--pending', '--force', '--timeout', '10min'])
        assert result.exit_code == 0

        with patch('requests.delete', Mock(return_value=MockResponse(500))):
            # normal restart, the schedule is actually parsed, but not validated in patronictl
            result = self.runner.invoke(ctl, ['restart', 'alpha', 'other', '--force',
                                              '--scheduled', '2300-10-01T14:30'])
            assert 'Failed: flush scheduled restart' in result.output

        with patch('patroni.dcs.Cluster.is_paused', Mock(return_value=True)):
            result = self.runner.invoke(ctl,
                                        ['restart', 'alpha', 'other', '--force', '--scheduled', '2300-10-01T14:30'])
            assert result.exit_code == 1

        with patch('requests.post', Mock(return_value=MockResponse())):
            # normal restart, the schedule is actually parsed, but not validated in patronictl
            result = self.runner.invoke(ctl, ['restart', 'alpha', '--pg-version', '42.0.0',
                                        '--scheduled', '2300-10-01T14:30'], input='y')
            assert result.exit_code == 0

        with patch('requests.post', Mock(return_value=MockResponse(204))):
            # get restart with the non-200 return code
            # normal restart, the schedule is actually parsed, but not validated in patronictl
            result = self.runner.invoke(ctl, ['restart', 'alpha', '--pg-version', '42.0.0',
                                        '--scheduled', '2300-10-01T14:30'], input='y')
            assert result.exit_code == 0

        # force restart with restart already present
        with patch('patroni.ctl.request_patroni', Mock(return_value=MockResponse(204))):
            result = self.runner.invoke(ctl, ['restart', 'alpha', 'other', '--force',
                                              '--scheduled', '2300-10-01T14:30'])
            assert result.exit_code == 0

        with patch('requests.post', Mock(return_value=MockResponse(202))):
            # get restart with the non-200 return code
            # normal restart, the schedule is actually parsed, but not validated in patronictl
            result = self.runner.invoke(
                ctl, ['restart', 'alpha', '--pg-version', '99.0.0', '--scheduled', '2300-10-01T14:30'], input='y'
            )
            assert 'Success: restart scheduled' in result.output
            assert result.exit_code == 0

        with patch('requests.post', Mock(return_value=MockResponse(409))):
            # get restart with the non-200 return code
            # normal restart, the schedule is actually parsed, but not validated in patronictl
            result = self.runner.invoke(
                ctl, ['restart', 'alpha', '--pg-version', '99.0.0', '--scheduled', '2300-10-01T14:30'], input='y'
            )
            assert 'Failed: another restart is already' in result.output
            assert result.exit_code == 0

    @patch('patroni.ctl.get_dcs')
    def test_remove(self, mock_get_dcs):
        mock_get_dcs.return_value.get_cluster = get_cluster_initialized_with_leader
        result = self.runner.invoke(ctl, ['remove', 'alpha'], input='alpha\nslave')
        assert 'Please confirm' in result.output
        assert 'You are about to remove all' in result.output
        # Not typing an exact confirmation
        assert result.exit_code == 1

        # master specified does not match master of cluster
        result = self.runner.invoke(ctl, ['remove', 'alpha'], input='alpha\nYes I am aware\nslave')
        assert result.exit_code == 1

        # cluster specified on cmdline does not match verification prompt
        result = self.runner.invoke(ctl, ['remove', 'alpha'], input='beta\nleader')
        assert result.exit_code == 1

        result = self.runner.invoke(ctl, ['remove', 'alpha'], input='alpha\nYes I am aware\nleader')
        assert result.exit_code == 0

    @patch('patroni.dcs.AbstractDCS.watch', Mock(return_value=None))
    @patch('patroni.dcs.AbstractDCS.get_cluster', Mock(return_value=get_cluster_initialized_with_leader()))
    def test_wait_for_leader(self):
        self.assertRaises(PatroniCtlException, wait_for_leader, self.e, 0)

        cluster = wait_for_leader(self.e, timeout=2)
        assert cluster.leader.member.name == 'leader'

    @patch('requests.post', Mock(side_effect=requests.exceptions.ConnectionError('foo')))
    def test_request_patroni(self):
        member = get_cluster_initialized_with_leader().leader.member
        self.assertRaises(requests.exceptions.ConnectionError, request_patroni, member, 'post', 'dummy', {})

    def test_ctl(self):
        self.runner.invoke(ctl, ['list'])

        result = self.runner.invoke(ctl, ['--help'])
        assert 'Usage:' in result.output

    def test_get_any_member(self):
        self.assertIsNone(get_any_member(get_cluster_initialized_without_leader(), role='master'))

        m = get_any_member(get_cluster_initialized_with_leader(), role='master')
        self.assertEquals(m.name, 'leader')

    def test_get_all_members(self):
        self.assertEquals(list(get_all_members(get_cluster_initialized_without_leader(), role='master')), [])

        r = list(get_all_members(get_cluster_initialized_with_leader(), role='master'))
        self.assertEquals(len(r), 1)
        self.assertEquals(r[0].name, 'leader')

        r = list(get_all_members(get_cluster_initialized_with_leader(), role='replica'))
        self.assertEquals(len(r), 1)
        self.assertEquals(r[0].name, 'other')

        self.assertEquals(len(list(get_all_members(get_cluster_initialized_without_leader(), role='replica'))), 2)

    @patch('patroni.ctl.get_dcs')
    def test_members(self, mock_get_dcs):
        mock_get_dcs.return_value.get_cluster = get_cluster_initialized_with_leader
        result = self.runner.invoke(members, ['alpha'])
        assert '127.0.0.1' in result.output
        assert result.exit_code == 0

    def test_configure(self):
        result = self.runner.invoke(configure, ['--dcs', 'abc', '-c', 'dummy', '-n', 'bla'])
        assert result.exit_code == 0

    @patch('patroni.ctl.get_dcs')
    def test_scaffold(self, mock_get_dcs):
        mock_get_dcs.return_value = self.e
        mock_get_dcs.return_value.get_cluster = get_cluster_not_initialized_without_leader
        mock_get_dcs.return_value.initialize = Mock(return_value=True)
        mock_get_dcs.return_value.touch_member = Mock(return_value=True)
        mock_get_dcs.return_value.attempt_to_acquire_leader = Mock(return_value=True)

        with patch.object(self.e, 'initialize', return_value=False):
            result = self.runner.invoke(ctl, ['scaffold', 'alpha'])
            assert result.exception

        with patch.object(mock_get_dcs.return_value, 'touch_member', Mock(return_value=False)):
            result = self.runner.invoke(ctl, ['scaffold', 'alpha'])
            assert result.exception

        result = self.runner.invoke(ctl, ['scaffold', 'alpha'])
        assert result.exit_code == 0

        mock_get_dcs.return_value.get_cluster = get_cluster_initialized_with_leader
        result = self.runner.invoke(ctl, ['scaffold', 'alpha'])
        assert result.exception

    @patch('patroni.ctl.get_dcs')
    def test_list_extended(self, mock_get_dcs):
        mock_get_dcs.return_value = self.e
        mock_get_dcs.return_value.get_cluster = get_cluster_initialized_with_leader

        result = self.runner.invoke(ctl, ['list', 'dummy', '--extended'])
        assert '2100' in result.output
        assert 'Scheduled restart' in result.output

    @patch('patroni.ctl.get_dcs')
    @patch('requests.delete', Mock(return_value=MockResponse()))
    def test_flush(self, mock_get_dcs):
        mock_get_dcs.return_value = self.e
        mock_get_dcs.return_value.get_cluster = get_cluster_initialized_with_leader

        result = self.runner.invoke(ctl, ['flush', 'dummy', 'restart', '-r', 'master'], input='y')
        assert 'No scheduled restart' in result.output

        result = self.runner.invoke(ctl, ['flush', 'dummy', 'restart', '--force'])
        assert 'Success: flush scheduled restart' in result.output
        with patch.object(requests, 'delete', return_value=MockResponse(404)):
            result = self.runner.invoke(ctl, ['flush', 'dummy', 'restart', '--force'])
            assert 'Failed: flush scheduled restart' in result.output

    @patch('patroni.ctl.get_dcs')
    def test_pause_cluster(self, mock_get_dcs):
        mock_get_dcs.return_value = self.e
        mock_get_dcs.return_value.get_cluster = get_cluster_initialized_with_leader

        with patch('requests.patch', Mock(return_value=MockResponse(200))):
            result = self.runner.invoke(ctl, ['pause', 'dummy'])
            assert 'Success' in result.output

        with patch('requests.patch', Mock(return_value=MockResponse(500))):
            result = self.runner.invoke(ctl, ['pause', 'dummy'])
            assert 'Failed' in result.output

        with patch('requests.patch', Mock(return_value=MockResponse(200))),\
                patch('patroni.dcs.Cluster.is_paused', Mock(return_value=True)):
            result = self.runner.invoke(ctl, ['pause', 'dummy'])
            assert 'Cluster is already paused' in result.output

    @patch('patroni.ctl.get_dcs')
    def test_resume_cluster(self, mock_get_dcs):
        mock_get_dcs.return_value = self.e
        mock_get_dcs.return_value.get_cluster = get_cluster_initialized_with_leader

        with patch('patroni.dcs.Cluster.is_paused', Mock(return_value=True)):
            with patch('requests.patch', Mock(return_value=MockResponse(200))):
                result = self.runner.invoke(ctl, ['resume', 'dummy'])
                assert 'Success' in result.output

            with patch('requests.patch', Mock(return_value=MockResponse(500))):
                result = self.runner.invoke(ctl, ['resume', 'dummy'])
                assert 'Failed' in result.output

            with patch('requests.patch', Mock(return_value=MockResponse(200))),\
                    patch('patroni.dcs.Cluster.is_paused', Mock(return_value=False)):
                result = self.runner.invoke(ctl, ['resume', 'dummy'])
                assert 'Cluster is not paused' in result.output
