import etcd
import os
import pytest
import requests.exceptions
import unittest

from click.testing import CliRunner
from mock import patch, Mock, MagicMock
from patroni.ctl import ctl, members, store_config, load_config, output_members, post_patroni, get_dcs, \
    wait_for_leader, get_all_members, get_any_member, get_cursor, query_member, configure
from patroni.etcd import Etcd, Client
from patroni.exceptions import PatroniCtlException
from psycopg2 import OperationalError
from test_etcd import etcd_read, etcd_write, requests_get, socket_getaddrinfo, MockResponse
from test_zookeeper import MockKazooClient
from test_ha import get_cluster_initialized_without_leader, get_cluster_initialized_with_leader, \
    get_cluster_initialized_with_only_leader
from test_postgresql import MockConnect, psycopg2_connect

CONFIG_FILE_PATH = './test-ctl.yaml'


def test_rw_config():
    runner = CliRunner()
    config = {'a': 'b'}
    with runner.isolated_filesystem():
        store_config(config, CONFIG_FILE_PATH + '/dummy')
        os.remove(CONFIG_FILE_PATH + '/dummy')
        os.rmdir(CONFIG_FILE_PATH)

        with pytest.raises(Exception):
            result = load_config(CONFIG_FILE_PATH, None)
            assert 'Could not load configuration file' in result.output

        os.mkdir(CONFIG_FILE_PATH)
        with pytest.raises(Exception):
            store_config(config, CONFIG_FILE_PATH)

        os.rmdir(CONFIG_FILE_PATH)

    store_config(config, CONFIG_FILE_PATH)
    load_config(CONFIG_FILE_PATH, None)
    load_config(CONFIG_FILE_PATH, '0.0.0.0')


@patch('patroni.ctl.load_config', Mock(return_value={'dcs': {'scheme': 'etcd', 'hostname': 'localhost', 'port': 4001}}))
@patch.object(etcd.Client, 'write', etcd_write)
@patch.object(etcd.Client, 'read', etcd_read)
@patch.object(etcd.Client, 'delete', Mock(side_effect=etcd.EtcdException))
class TestCtl(unittest.TestCase):

    @patch('socket.getaddrinfo', socket_getaddrinfo)
    def setUp(self):
        self.runner = CliRunner()
        with patch.object(Client, 'machines') as mock_machines:
            mock_machines.__get__ = Mock(return_value=['http://remotehost:2379'])
            self.e = Etcd('foo', {'ttl': 30, 'host': 'ok:2379', 'scope': 'test'})

    @patch('psycopg2.connect', psycopg2_connect)
    def test_get_cursor(self):
        self.assertIsNone(get_cursor(get_cluster_initialized_without_leader(), role='master'))

        self.assertIsNotNone(get_cursor(get_cluster_initialized_with_leader(), role='master'))

        # MockCursor returns pg_is_in_recovery as false
        self.assertIsNone(get_cursor(get_cluster_initialized_with_leader(), role='replica'))

        self.assertIsNotNone(get_cursor(get_cluster_initialized_with_leader(), role='any'))

    def test_output_members(self):
        cluster = get_cluster_initialized_with_leader()
        self.assertIsNone(output_members(cluster, name='abc', fmt='pretty'))
        self.assertIsNone(output_members(cluster, name='abc', fmt='json'))
        self.assertIsNone(output_members(cluster, name='abc', fmt='tsv'))

    @patch('patroni.etcd.Etcd.get_cluster', Mock(return_value=get_cluster_initialized_with_leader()))
    @patch('patroni.etcd.Etcd.get_etcd_client', Mock(return_value=None))
    @patch('patroni.etcd.Etcd.set_failover_value', Mock(return_value=None))
    @patch('patroni.ctl.wait_for_leader', Mock(return_value=get_cluster_initialized_with_leader()))
    @patch('requests.get', requests_get)
    @patch('requests.post', requests_get)
    @patch('patroni.ctl.post_patroni', Mock(return_value=MockResponse()))
    def test_failover(self):
        with patch('patroni.etcd.Etcd.get_cluster', Mock(return_value=get_cluster_initialized_with_leader())):
            result = self.runner.invoke(ctl, ['failover', 'dummy', '--dcs', '8.8.8.8'], input='''leader
other

y''')
            assert 'leader' in result.output

            result = self.runner.invoke(ctl, ['failover', 'dummy', '--dcs', '8.8.8.8'], input='''leader
other
2100-01-01T12:23:00
y''')
            assert result.exit_code == 0

            result = self.runner.invoke(ctl, ['failover', 'dummy', '--dcs', '8.8.8.8'], input='''leader
other
2030-01-01T12:23:00
y''')
            assert result.exit_code == 0

            # Aborting failover,as we anser NO to the confirmation
            result = self.runner.invoke(ctl, ['failover', 'dummy', '--dcs', '8.8.8.8'], input='''leader
other

N''')
            assert result.exit_code == 1

            # Target and source are equal
            result = self.runner.invoke(ctl, ['failover', 'dummy', '--dcs', '8.8.8.8'], input='''leader
leader

y''')
            assert result.exit_code == 1

            # Reality is not part of this cluster
            result = self.runner.invoke(ctl, ['failover', 'dummy', '--dcs', '8.8.8.8'], input='''leader
Reality

y''')
            assert result.exit_code == 1

            result = self.runner.invoke(ctl, ['failover', 'dummy', '--force'])
            assert 'Member' in result.output

            result = self.runner.invoke(ctl, ['failover', 'dummy', '--force',
                                              '--scheduled', '2015-01-01T12:00:00+01:00'])
            assert result.exit_code == 0

            # Invalid timestamp
            result = self.runner.invoke(ctl, ['failover', 'dummy', '--force', '--scheduled', 'invalid'])
            assert result.exit_code != 0

            # Invalid timestamp
            result = self.runner.invoke(ctl, ['failover', 'dummy', '--force',
                                              '--scheduled', '2115-02-30T12:00:00+01:00'])
            assert result.exit_code != 0

            # Specifying wrong leader
            result = self.runner.invoke(ctl, ['failover', 'dummy', '--dcs', '8.8.8.8'], input='dummy')
            assert result.exit_code == 1

        with patch('patroni.etcd.Etcd.get_cluster', Mock(return_value=get_cluster_initialized_with_only_leader())):
            # No members available
            result = self.runner.invoke(ctl, ['failover', 'dummy', '--dcs', '8.8.8.8'], input='''leader
other

y''')
            assert result.exit_code == 1

        with patch('patroni.etcd.Etcd.get_cluster', Mock(return_value=get_cluster_initialized_without_leader())):
            # No master available
            result = self.runner.invoke(ctl, ['failover', 'dummy', '--dcs', '8.8.8.8'], input='''leader
other

y''')
            assert result.exit_code == 1

        with patch('patroni.ctl.post_patroni', Mock(side_effect=Exception())):
            # Non-responding patroni
            result = self.runner.invoke(ctl, ['failover', 'dummy', '--dcs', '8.8.8.8'], input='''leader
other

y''')
            assert 'falling back to DCS' in result.output

        mocked = Mock()
        mocked.return_value.status_code = 500
        with patch('patroni.ctl.post_patroni', Mock(return_value=mocked)):
            result = self.runner.invoke(ctl, ['failover', 'dummy', '--dcs', '8.8.8.8'], input='''leader
other

y''')
            assert 'Failover failed' in result.output

    @patch('patroni.zookeeper.KazooClient', MockKazooClient)
    @patch('requests.get', requests_get)
    def test_get_dcs(self):
        self.assertIsNotNone(get_dcs({'dcs': {'scheme': 'zookeeper', 'hostname': 'foo', 'port': 2181}}, 'dummy'))
        self.assertIsNotNone(get_dcs({'dcs': {'scheme': 'exhibitor', 'hostname': 'exhibitor', 'port': 8181}}, 'dummy'))
        self.assertRaises(PatroniCtlException, get_dcs, {'scheme': 'dummy'}, 'dummy')

    @patch('psycopg2.connect', psycopg2_connect)
    @patch('patroni.ctl.query_member', Mock(return_value=([['mock column']], None)))
    def test_query(self):
        with patch('patroni.ctl.get_dcs', Mock(return_value=self.e)):
            # Mutually exclusive
            result = self.runner.invoke(ctl, [
                'query',
                'alpha',
                '--member',
                'abc',
                '--role',
                'master',
            ])
            assert result.exit_code == 1

            with self.runner.isolated_filesystem():
                with open('dummy', 'w') as dummy_file:
                    dummy_file.write('SELECT 1')

                # Mutually exclusive
                result = self.runner.invoke(ctl, [
                    'query',
                    'alpha',
                    '--file',
                    'dummy',
                    '--command',
                    'dummy',
                ])
                assert result.exit_code == 1

                result = self.runner.invoke(ctl, ['query', 'alpha', '--file', 'dummy'])

                os.remove('dummy')

            result = self.runner.invoke(ctl, ['query', 'alpha', '--command', 'SELECT 1'])
            assert 'mock column' in result.output

            # --command or --file is mandatory
            result = self.runner.invoke(ctl, ['query', 'alpha'])
            assert result.exit_code == 1

            result = self.runner.invoke(ctl, ['query', 'alpha', '--command', 'SELECT 1', '--username', 'root',
                                              '--password', '--dbname', 'postgres'], input='ab\nab')
            assert 'mock column' in result.output

    @patch('patroni.ctl.get_cursor', Mock(return_value=MockConnect().cursor()))
    def test_query_member(self):
        rows = query_member(None, None, None, 'master', 'SELECT pg_is_in_recovery()')
        self.assertTrue('False' in str(rows))

        rows = query_member(None, None, None, 'replica', 'SELECT pg_is_in_recovery()')
        self.assertEquals(rows, (None, None))

        with patch('patroni.ctl.get_cursor', Mock(return_value=None)):
            rows = query_member(None, None, None, None, 'SELECT pg_is_in_recovery()')
            self.assertTrue('No connection to' in str(rows))

            rows = query_member(None, None, None, 'replica', 'SELECT pg_is_in_recovery()')
            self.assertTrue('No connection to' in str(rows))

        with patch('patroni.ctl.get_cursor', Mock(side_effect=OperationalError('bla'))):
            rows = query_member(None, None, None, 'replica', 'SELECT pg_is_in_recovery()')

        with patch('test_postgresql.MockCursor.execute', Mock(side_effect=OperationalError('bla'))):
            rows = query_member(None, None, None, 'replica', 'SELECT pg_is_in_recovery()')

    @patch('patroni.dcs.AbstractDCS.get_cluster', Mock(return_value=get_cluster_initialized_with_leader()))
    def test_dsn(self):
        with patch('patroni.ctl.get_dcs', Mock(return_value=self.e)):
            result = self.runner.invoke(ctl, ['dsn', 'alpha', '--dcs', '8.8.8.8'])
            assert 'host=127.0.0.1 port=5435' in result.output

            # Mutually exclusive options
            result = self.runner.invoke(ctl, [
                'dsn',
                'alpha',
                '--role',
                'master',
                '--member',
                'dummy',
            ])
            assert result.exit_code == 1

            # Non-existing member
            result = self.runner.invoke(ctl, ['dsn', 'alpha', '--member', 'dummy'])
            assert result.exit_code == 1

    @patch('patroni.etcd.Etcd.get_cluster', Mock(return_value=get_cluster_initialized_with_leader()))
    @patch('patroni.etcd.Etcd.get_etcd_client', Mock(return_value=None))
    @patch('requests.get', requests_get)
    @patch('requests.post', requests_get)
    def test_restart_reinit(self):
        result = self.runner.invoke(ctl, ['restart', 'alpha', '--dcs', '8.8.8.8'], input='y')
        assert result.exit_code == 0

        result = self.runner.invoke(ctl, ['reinit', 'alpha', '--dcs', '8.8.8.8'], input='y')
        assert result.exit_code == 1

        # Aborted restart
        result = self.runner.invoke(ctl, ['restart', 'alpha', '--dcs', '8.8.8.8'], input='N')
        assert result.exit_code == 1

        # Not a member
        result = self.runner.invoke(ctl, [
            'restart',
            'alpha',
            '--dcs',
            '8.8.8.8',
            'dummy',
            '--any',
        ], input='y')
        assert result.exit_code == 1

        with patch('requests.post', Mock(return_value=MockResponse())):
            result = self.runner.invoke(ctl, ['restart', 'alpha', '--dcs', '8.8.8.8'], input='y')

    @patch('patroni.etcd.Etcd.get_cluster', Mock(return_value=get_cluster_initialized_with_leader()))
    def test_remove(self):
        with patch('patroni.ctl.get_dcs', Mock(return_value=self.e)):
            result = self.runner.invoke(ctl, ['remove', 'alpha', '--dcs', '8.8.8.8'], input='alpha\nslave')
            assert 'Please confirm' in result.output
            assert 'You are about to remove all' in result.output
            # Not typing an exact confirmation
            assert result.exit_code == 1

            # master specified does not match master of cluster
            result = self.runner.invoke(ctl, ['remove', 'alpha', '--dcs', '8.8.8.8'], input='''alpha
Yes I am aware
slave''')
            assert result.exit_code == 1

            # cluster specified on cmdline does not match verification prompt
            result = self.runner.invoke(ctl, ['remove', 'alpha', '--dcs', '8.8.8.8'], input='beta\nleader')
            assert result.exit_code == 1

            result = self.runner.invoke(ctl, ['remove', 'alpha', '--dcs', '8.8.8.8'], input='''alpha
Yes I am aware
leader''')
            assert result.exit_code == 0

    @patch('patroni.etcd.Etcd.watch', Mock(return_value=None))
    @patch('patroni.etcd.Etcd.get_cluster', Mock(return_value=get_cluster_initialized_with_leader()))
    def test_wait_for_leader(self):
        dcs = self.e
        self.assertRaises(PatroniCtlException, wait_for_leader, dcs, 0)

        cluster = wait_for_leader(dcs=dcs, timeout=2)
        assert cluster.leader.member.name == 'leader'

    def test_post_patroni(self):
        with patch('requests.post', MagicMock(side_effect=requests.exceptions.ConnectionError('foo'))):
            member = get_cluster_initialized_with_leader().leader.member
            self.assertRaises(requests.exceptions.ConnectionError, post_patroni, member, 'dummy', {})

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

    @patch('patroni.etcd.Etcd.get_cluster', Mock(return_value=get_cluster_initialized_with_leader()))
    @patch('patroni.etcd.Etcd.get_etcd_client', Mock(return_value=None))
    @patch('requests.get', requests_get)
    @patch('requests.post', requests_get)
    def test_members(self):
        result = self.runner.invoke(members, ['alpha'])
        assert result.exit_code == 0

    def test_configure(self):
        result = self.runner.invoke(configure, [
            '--dcs',
            'abc',
            '-c',
            'dummy',
            '-n',
            'bla',
        ])

        assert result.exit_code == 0
