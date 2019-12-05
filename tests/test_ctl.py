import etcd
import os
import unittest

from click.testing import CliRunner
from datetime import datetime, timedelta
from mock import patch, Mock
from patroni.ctl import ctl, store_config, load_config, output_members, get_dcs, parse_dcs, \
    get_all_members, get_any_member, get_cursor, query_member, configure, PatroniCtlException, apply_config_changes, \
    format_config_for_editing, show_diff, invoke_editor, format_pg_version, find_executable
from patroni.dcs.etcd import Client, Failover
from patroni.utils import tzutc
from psycopg2 import OperationalError
from urllib3 import PoolManager

from . import MockConnect, MockCursor, MockResponse, psycopg2_connect
from .test_etcd import etcd_read, socket_getaddrinfo
from .test_ha import get_cluster_initialized_without_leader, get_cluster_initialized_with_leader, \
    get_cluster_initialized_with_only_leader, get_cluster_not_initialized_without_leader, get_cluster, Member

CONFIG_FILE_PATH = './test-ctl.yaml'


def test_rw_config():
    runner = CliRunner()
    with runner.isolated_filesystem():
        load_config(CONFIG_FILE_PATH + '/dummy', None)
        store_config({'etcd': {'host': 'localhost:2379'}}, CONFIG_FILE_PATH + '/dummy')
        load_config(CONFIG_FILE_PATH + '/dummy', '0.0.0.0')
        os.remove(CONFIG_FILE_PATH + '/dummy')
        os.rmdir(CONFIG_FILE_PATH)


@patch('patroni.ctl.load_config',
       Mock(return_value={'scope': 'alpha', 'postgresql': {'data_dir': '.', 'parameters': {}, 'retry_timeout': 5},
                          'restapi': {'listen': '::', 'certfile': 'a'}, 'etcd': {'host': 'localhost:2379'}}))
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
        scheduled_at = datetime.now(tzutc) + timedelta(seconds=600)
        cluster = get_cluster_initialized_with_leader(Failover(1, 'foo', 'bar', scheduled_at))
        self.assertIsNone(output_members(cluster, name='abc', fmt='pretty'))
        self.assertIsNone(output_members(cluster, name='abc', fmt='json'))
        self.assertIsNone(output_members(cluster, name='abc', fmt='yaml'))
        self.assertIsNone(output_members(cluster, name='abc', fmt='tsv'))

    @patch('patroni.ctl.get_dcs')
    @patch.object(PoolManager, 'request', Mock(return_value=MockResponse()))
    def test_switchover(self, mock_get_dcs):
        mock_get_dcs.return_value = self.e
        mock_get_dcs.return_value.get_cluster = get_cluster_initialized_with_leader
        mock_get_dcs.return_value.set_failover_value = Mock()
        result = self.runner.invoke(ctl, ['switchover', 'dummy'], input='leader\nother\n\ny')
        assert 'leader' in result.output

        result = self.runner.invoke(ctl, ['switchover', 'dummy'], input='leader\nother\n2300-01-01T12:23:00\ny')
        assert result.exit_code == 0

        with patch('patroni.dcs.Cluster.is_paused', Mock(return_value=True)):
            result = self.runner.invoke(ctl, ['switchover', 'dummy', '--force', '--scheduled', '2015-01-01T12:00:00'])
            assert result.exit_code == 1

        # Aborting switchover, as we answer NO to the confirmation
        result = self.runner.invoke(ctl, ['switchover', 'dummy'], input='leader\nother\n\nN')
        assert result.exit_code == 1

        # Aborting scheduled switchover, as we answer NO to the confirmation
        result = self.runner.invoke(ctl, ['switchover', 'dummy', '--scheduled', '2015-01-01T12:00:00+01:00'],
                                    input='leader\nother\n\nN')
        assert result.exit_code == 1

        # Target and source are equal
        result = self.runner.invoke(ctl, ['switchover', 'dummy'], input='leader\nleader\n\ny')
        assert result.exit_code == 1

        # Reality is not part of this cluster
        result = self.runner.invoke(ctl, ['switchover', 'dummy'], input='leader\nReality\n\ny')
        assert result.exit_code == 1

        result = self.runner.invoke(ctl, ['switchover', 'dummy', '--force'])
        assert 'Member' in result.output

        result = self.runner.invoke(ctl, ['switchover', 'dummy', '--force', '--scheduled', '2015-01-01T12:00:00+01:00'])
        assert result.exit_code == 0

        # Invalid timestamp
        result = self.runner.invoke(ctl, ['switchover', 'dummy', '--force', '--scheduled', 'invalid'])
        assert result.exit_code != 0

        # Invalid timestamp
        result = self.runner.invoke(ctl, ['switchover', 'dummy', '--force', '--scheduled', '2115-02-30T12:00:00+01:00'])
        assert result.exit_code != 0

        # Specifying wrong leader
        result = self.runner.invoke(ctl, ['switchover', 'dummy'], input='dummy')
        assert result.exit_code == 1

        with patch.object(PoolManager, 'request', Mock(side_effect=Exception)):
            # Non-responding patroni
            result = self.runner.invoke(ctl, ['switchover', 'dummy'], input='leader\nother\n2300-01-01T12:23:00\ny')
            assert 'falling back to DCS' in result.output

        with patch.object(PoolManager, 'request') as mocked:
            mocked.return_value.status = 500
            result = self.runner.invoke(ctl, ['switchover', 'dummy'], input='leader\nother\n\ny')
            assert 'Switchover failed' in result.output

            mocked.return_value.status = 501
            mocked.return_value.data = b'Server does not support this operation'
            result = self.runner.invoke(ctl, ['switchover', 'dummy'], input='leader\nother\n\ny')
            assert 'Switchover failed' in result.output

        # No members available
        mock_get_dcs.return_value.get_cluster = get_cluster_initialized_with_only_leader
        result = self.runner.invoke(ctl, ['switchover', 'dummy'], input='leader\nother\n\ny')
        assert result.exit_code == 1

        # No master available
        mock_get_dcs.return_value.get_cluster = get_cluster_initialized_without_leader
        result = self.runner.invoke(ctl, ['switchover', 'dummy'], input='leader\nother\n\ny')
        assert result.exit_code == 1

    @patch('patroni.ctl.get_dcs')
    @patch.object(PoolManager, 'request', Mock(return_value=MockResponse()))
    def test_failover(self, mock_get_dcs):
        mock_get_dcs.return_value = self.e
        mock_get_dcs.return_value.get_cluster = get_cluster_initialized_with_leader
        mock_get_dcs.return_value.set_failover_value = Mock()
        result = self.runner.invoke(ctl, ['failover', 'dummy'], input='\n')
        assert 'Failover could be performed only to a specific candidate' in result.output

    @patch('patroni.dcs.dcs_modules', Mock(return_value=['patroni.dcs.dummy', 'patroni.dcs.etcd']))
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
            rows = query_member(None, None, None, 'master', 'SELECT pg_catalog.pg_is_in_recovery()', {})
            self.assertTrue('False' in str(rows))

            rows = query_member(None, None, None, 'replica', 'SELECT pg_catalog.pg_is_in_recovery()', {})
            self.assertEqual(rows, (None, None))

            with patch.object(MockCursor, 'execute', Mock(side_effect=OperationalError('bla'))):
                rows = query_member(None, None, None, 'replica', 'SELECT pg_catalog.pg_is_in_recovery()', {})

        with patch('patroni.ctl.get_cursor', Mock(return_value=None)):
            rows = query_member(None, None, None, None, 'SELECT pg_catalog.pg_is_in_recovery()', {})
            self.assertTrue('No connection to' in str(rows))

            rows = query_member(None, None, None, 'replica', 'SELECT pg_catalog.pg_is_in_recovery()', {})
            self.assertTrue('No connection to' in str(rows))

        with patch('patroni.ctl.get_cursor', Mock(side_effect=OperationalError('bla'))):
            rows = query_member(None, None, None, 'replica', 'SELECT pg_catalog.pg_is_in_recovery()', {})

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

    @patch.object(PoolManager, 'request')
    @patch('patroni.ctl.get_dcs')
    def test_reload(self, mock_get_dcs, mock_post):
        mock_get_dcs.return_value.get_cluster = get_cluster_initialized_with_leader

        result = self.runner.invoke(ctl, ['reload', 'alpha'], input='y')
        assert 'Failed: reload for member' in result.output

        mock_post.return_value.status = 200
        result = self.runner.invoke(ctl, ['reload', 'alpha'], input='y')
        assert 'No changes to apply on member' in result.output

        mock_post.return_value.status = 202
        result = self.runner.invoke(ctl, ['reload', 'alpha'], input='y')
        assert 'Reload request received for member' in result.output

    @patch.object(PoolManager, 'request')
    @patch('patroni.ctl.get_dcs')
    def test_restart_reinit(self, mock_get_dcs, mock_post):
        mock_get_dcs.return_value.get_cluster = get_cluster_initialized_with_leader
        mock_post.return_value.status = 503
        result = self.runner.invoke(ctl, ['restart', 'alpha'], input='now\ny\n')
        assert 'Failed: restart for' in result.output
        assert result.exit_code == 0

        result = self.runner.invoke(ctl, ['reinit', 'alpha'], input='y')
        assert result.exit_code == 1

        # successful reinit
        result = self.runner.invoke(ctl, ['reinit', 'alpha', 'other'], input='y\ny')
        assert result.exit_code == 0

        # Aborted restart
        result = self.runner.invoke(ctl, ['restart', 'alpha'], input='now\nN')
        assert result.exit_code == 1

        result = self.runner.invoke(ctl, ['restart', 'alpha', '--pending', '--force'])
        assert result.exit_code == 0

        # Aborted scheduled restart
        result = self.runner.invoke(ctl, ['restart', 'alpha', '--scheduled', '2019-10-01T14:30'], input='N')
        assert result.exit_code == 1

        # Not a member
        result = self.runner.invoke(ctl, ['restart', 'alpha', 'dummy', '--any'], input='now\ny')
        assert result.exit_code == 1

        # Wrong pg version
        result = self.runner.invoke(ctl, ['restart', 'alpha', '--any', '--pg-version', '9.1'], input='now\ny')
        assert 'Error: Invalid PostgreSQL version format' in result.output
        assert result.exit_code == 1

        result = self.runner.invoke(ctl, ['restart', 'alpha', '--pending', '--force', '--timeout', '10min'])
        assert result.exit_code == 0

        # normal restart, the schedule is actually parsed, but not validated in patronictl
        result = self.runner.invoke(ctl, ['restart', 'alpha', 'other', '--force', '--scheduled', '2300-10-01T14:30'])
        assert 'Failed: flush scheduled restart' in result.output

        with patch('patroni.dcs.Cluster.is_paused', Mock(return_value=True)):
            result = self.runner.invoke(ctl,
                                        ['restart', 'alpha', 'other', '--force', '--scheduled', '2300-10-01T14:30'])
            assert result.exit_code == 1

        # force restart with restart already present
        result = self.runner.invoke(ctl, ['restart', 'alpha', 'other', '--force', '--scheduled', '2300-10-01T14:30'])
        assert result.exit_code == 0

        ctl_args = ['restart', 'alpha', '--pg-version', '99.0', '--scheduled', '2300-10-01T14:30']
        # normal restart, the schedule is actually parsed, but not validated in patronictl
        mock_post.return_value.status = 200
        result = self.runner.invoke(ctl, ctl_args, input='y')
        assert result.exit_code == 0

        # get restart with the non-200 return code
        # normal restart, the schedule is actually parsed, but not validated in patronictl
        mock_post.return_value.status = 204
        result = self.runner.invoke(ctl, ctl_args, input='y')
        assert result.exit_code == 0

        # get restart with the non-200 return code
        # normal restart, the schedule is actually parsed, but not validated in patronictl
        mock_post.return_value.status = 202
        result = self.runner.invoke(ctl, ctl_args, input='y')
        assert 'Success: restart scheduled' in result.output
        assert result.exit_code == 0

        # get restart with the non-200 return code
        # normal restart, the schedule is actually parsed, but not validated in patronictl
        mock_post.return_value.status = 409
        result = self.runner.invoke(ctl, ctl_args, input='y')
        assert 'Failed: another restart is already' in result.output
        assert result.exit_code == 0

    @patch('patroni.ctl.get_dcs')
    def test_remove(self, mock_get_dcs):
        mock_get_dcs.return_value.get_cluster = get_cluster_initialized_with_leader
        result = self.runner.invoke(ctl, ['-k', 'remove', 'alpha'], input='alpha\nslave')
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

    def test_ctl(self):
        self.runner.invoke(ctl, ['list'])

        result = self.runner.invoke(ctl, ['--help'])
        assert 'Usage:' in result.output

    def test_get_any_member(self):
        self.assertIsNone(get_any_member(get_cluster_initialized_without_leader(), role='master'))

        m = get_any_member(get_cluster_initialized_with_leader(), role='master')
        self.assertEqual(m.name, 'leader')

    def test_get_all_members(self):
        self.assertEqual(list(get_all_members(get_cluster_initialized_without_leader(), role='master')), [])

        r = list(get_all_members(get_cluster_initialized_with_leader(), role='master'))
        self.assertEqual(len(r), 1)
        self.assertEqual(r[0].name, 'leader')

        r = list(get_all_members(get_cluster_initialized_with_leader(), role='replica'))
        self.assertEqual(len(r), 1)
        self.assertEqual(r[0].name, 'other')

        self.assertEqual(len(list(get_all_members(get_cluster_initialized_without_leader(), role='replica'))), 2)

    @patch('patroni.ctl.get_dcs')
    def test_members(self, mock_get_dcs):
        mock_get_dcs.return_value.get_cluster = get_cluster_initialized_with_leader
        result = self.runner.invoke(ctl, ['list'])
        assert '127.0.0.1' in result.output
        assert result.exit_code == 0
        with patch('patroni.ctl.load_config', Mock(return_value={})):
            self.runner.invoke(ctl, ['list'])

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
        mock_get_dcs.return_value.delete_cluster = Mock()

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
        cluster = get_cluster_initialized_with_leader(sync=('leader', 'other'))
        mock_get_dcs.return_value.get_cluster = Mock(return_value=cluster)

        result = self.runner.invoke(ctl, ['list', 'dummy', '--extended', '--timestamp'])
        assert '2100' in result.output
        assert 'Scheduled restart' in result.output

    @patch('patroni.ctl.get_dcs')
    @patch.object(PoolManager, 'request', Mock(return_value=MockResponse()))
    def test_flush(self, mock_get_dcs):
        mock_get_dcs.return_value = self.e
        mock_get_dcs.return_value.get_cluster = get_cluster_initialized_with_leader

        result = self.runner.invoke(ctl, ['flush', 'dummy', 'restart', '-r', 'master'], input='y')
        assert 'No scheduled restart' in result.output

        result = self.runner.invoke(ctl, ['flush', 'dummy', 'restart', '--force'])
        assert 'Success: flush scheduled restart' in result.output
        with patch.object(PoolManager, 'request', return_value=MockResponse(404)):
            result = self.runner.invoke(ctl, ['flush', 'dummy', 'restart', '--force'])
            assert 'Failed: flush scheduled restart' in result.output

    @patch.object(PoolManager, 'request')
    @patch('patroni.ctl.get_dcs')
    @patch('patroni.ctl.polling_loop', Mock(return_value=[1]))
    def test_pause_cluster(self, mock_get_dcs, mock_post):
        mock_get_dcs.return_value = self.e
        mock_get_dcs.return_value.get_cluster = get_cluster_initialized_with_leader

        mock_post.return_value.status = 500
        result = self.runner.invoke(ctl, ['pause', 'dummy'])
        assert 'Failed' in result.output

        mock_post.return_value.status = 200
        with patch('patroni.dcs.Cluster.is_paused', Mock(return_value=True)):
            result = self.runner.invoke(ctl, ['pause', 'dummy'])
            assert 'Cluster is already paused' in result.output

        result = self.runner.invoke(ctl, ['pause', 'dummy', '--wait'])
        assert "'pause' request sent" in result.output
        mock_get_dcs.return_value.get_cluster = Mock(side_effect=[get_cluster_initialized_with_leader(),
                                                                  get_cluster(None, None, [], None, None)])
        self.runner.invoke(ctl, ['pause', 'dummy', '--wait'])
        member = Member(1, 'other', 28, {})
        mock_get_dcs.return_value.get_cluster = Mock(side_effect=[get_cluster_initialized_with_leader(),
                                                                  get_cluster(None, None, [member], None, None)])
        self.runner.invoke(ctl, ['pause', 'dummy', '--wait'])

    @patch.object(PoolManager, 'request')
    @patch('patroni.ctl.get_dcs')
    def test_resume_cluster(self, mock_get_dcs, mock_post):
        mock_get_dcs.return_value = self.e
        mock_get_dcs.return_value.get_cluster = get_cluster_initialized_with_leader

        mock_post.return_value.status = 200
        with patch('patroni.dcs.Cluster.is_paused', Mock(return_value=False)):
            result = self.runner.invoke(ctl, ['resume', 'dummy'])
            assert 'Cluster is not paused' in result.output

        with patch('patroni.dcs.Cluster.is_paused', Mock(return_value=True)):
            result = self.runner.invoke(ctl, ['resume', 'dummy'])
            assert 'Success' in result.output

            mock_post.return_value.status = 500
            result = self.runner.invoke(ctl, ['resume', 'dummy'])
            assert 'Failed' in result.output

            mock_post.side_effect = Exception
            result = self.runner.invoke(ctl, ['resume', 'dummy'])
            assert 'Can not find accessible cluster member' in result.output

    def test_apply_config_changes(self):
        config = {"postgresql": {"parameters": {"work_mem": "4MB"}, "use_pg_rewind": True}, "ttl": 30}

        before_editing = format_config_for_editing(config)

        # Spaces are allowed and stripped, numbers and booleans are interpreted
        after_editing, changed_config = apply_config_changes(before_editing, config,
                                                             ["postgresql.parameters.work_mem = 5MB",
                                                              "ttl=15", "postgresql.use_pg_rewind=off", 'a.b=c'])
        self.assertEqual(changed_config, {"a": {"b": "c"}, "postgresql": {"parameters": {"work_mem": "5MB"},
                                                                          "use_pg_rewind": False}, "ttl": 15})

        # postgresql.parameters namespace is flattened
        after_editing, changed_config = apply_config_changes(before_editing, config,
                                                             ["postgresql.parameters.work_mem.sub = x"])
        self.assertEqual(changed_config, {"postgresql": {"parameters": {"work_mem": "4MB", "work_mem.sub": "x"},
                                                         "use_pg_rewind": True}, "ttl": 30})

        # Setting to null deletes
        after_editing, changed_config = apply_config_changes(before_editing, config,
                                                             ["postgresql.parameters.work_mem=null"])
        self.assertEqual(changed_config, {"postgresql": {"use_pg_rewind": True}, "ttl": 30})
        after_editing, changed_config = apply_config_changes(before_editing, config,
                                                             ["postgresql.use_pg_rewind=null",
                                                              "postgresql.parameters.work_mem=null"])
        self.assertEqual(changed_config, {"ttl": 30})

        self.assertRaises(PatroniCtlException, apply_config_changes, before_editing, config, ['a'])

    @patch('sys.stdout.isatty', return_value=False)
    @patch('cdiff.markup_to_pager')
    def test_show_diff(self, mock_markup_to_pager, mock_isatty):
        show_diff("foo:\n  bar: 1\n", "foo:\n  bar: 2\n")
        mock_markup_to_pager.assert_not_called()

        mock_isatty.return_value = True
        show_diff("foo:\n  bar: 1\n", "foo:\n  bar: 2\n")
        mock_markup_to_pager.assert_called_once()

        # Test that unicode handling doesn't fail with an exception
        show_diff(b"foo:\n  bar: \xc3\xb6\xc3\xb6\n".decode('utf-8'),
                  b"foo:\n  bar: \xc3\xbc\xc3\xbc\n".decode('utf-8'))

    @patch('subprocess.call', return_value=1)
    def test_invoke_editor(self, mock_subprocess_call):
        os.environ.pop('EDITOR', None)
        for e in ('', '/bin/vi'):
            with patch('patroni.ctl.find_executable', Mock(return_value=e)):
                self.assertRaises(PatroniCtlException, invoke_editor, 'foo: bar\n', 'test')

    @patch('patroni.ctl.get_dcs')
    def test_show_config(self, mock_get_dcs):
        mock_get_dcs.return_value = self.e
        mock_get_dcs.return_value.get_cluster = get_cluster_initialized_with_leader
        self.runner.invoke(ctl, ['show-config', 'dummy'])

    @patch('patroni.ctl.get_dcs')
    def test_edit_config(self, mock_get_dcs):
        mock_get_dcs.return_value = self.e
        mock_get_dcs.return_value.get_cluster = get_cluster_initialized_with_leader
        mock_get_dcs.return_value.set_config_value = Mock(return_value=False)
        os.environ['EDITOR'] = 'true'
        self.runner.invoke(ctl, ['edit-config', 'dummy'])
        self.runner.invoke(ctl, ['edit-config', 'dummy', '-s', 'foo=bar'])
        self.runner.invoke(ctl, ['edit-config', 'dummy', '--replace', 'postgres0.yml'])
        self.runner.invoke(ctl, ['edit-config', 'dummy', '--apply', '-'], input='foo: bar')
        self.runner.invoke(ctl, ['edit-config', 'dummy', '--force', '--apply', '-'], input='foo: bar')
        mock_get_dcs.return_value.set_config_value.return_value = True
        self.runner.invoke(ctl, ['edit-config', 'dummy', '--force', '--apply', '-'], input='foo: bar')

    @patch('patroni.ctl.get_dcs')
    def test_version(self, mock_get_dcs):
        mock_get_dcs.return_value = self.e
        mock_get_dcs.return_value.get_cluster = get_cluster_initialized_with_leader
        with patch.object(PoolManager, 'request') as mocked:
            result = self.runner.invoke(ctl, ['version'])
            assert 'patronictl version' in result.output
            mocked.return_value.data = b'{"patroni":{"version":"1.2.3"},"server_version": 100001}'
            result = self.runner.invoke(ctl, ['version', 'dummy'])
            assert '1.2.3' in result.output
        with patch.object(PoolManager, 'request', Mock(side_effect=Exception)):
            result = self.runner.invoke(ctl, ['version', 'dummy'])
            assert 'failed to get version' in result.output

    @patch('patroni.ctl.get_dcs')
    def test_history(self, mock_get_dcs):
        mock_get_dcs.return_value.get_cluster = Mock()
        mock_get_dcs.return_value.get_cluster.return_value.history.lines = [[1, 67176, 'no recovery target specified']]
        result = self.runner.invoke(ctl, ['history'])
        assert 'Reason' in result.output

    def test_format_pg_version(self):
        self.assertEqual(format_pg_version(100001), '10.1')
        self.assertEqual(format_pg_version(90605), '9.6.5')

    @patch('sys.platform', 'win32')
    def test_find_executable(self):
        with patch('os.path.isfile', Mock(return_value=True)):
            self.assertEqual(find_executable('vim'), 'vim.exe')
        with patch('os.path.isfile', Mock(return_value=False)):
            self.assertIsNone(find_executable('vim'))
        with patch('os.path.isfile', Mock(side_effect=[False, True])):
            self.assertEqual(find_executable('vim', '/'), '/vim.exe')

    @patch('patroni.ctl.get_dcs')
    def test_get_members(self, mock_get_dcs):
        mock_get_dcs.return_value = self.e
        mock_get_dcs.return_value.get_cluster = get_cluster_not_initialized_without_leader
        result = self.runner.invoke(ctl, ['reinit', 'dummy'])
        assert "cluster doesn\'t have any members" in result.output
