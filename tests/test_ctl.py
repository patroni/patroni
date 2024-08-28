import os
import unittest

from datetime import datetime, timedelta
from unittest import mock
from unittest.mock import Mock, patch, PropertyMock

import click
import etcd

from click.testing import CliRunner
from prettytable import ALL, PrettyTable
from urllib3 import PoolManager

from patroni import global_config
from patroni.ctl import apply_config_changes, CONFIG_FILE_PATH, ctl, format_config_for_editing, \
    format_pg_version, get_all_members, get_any_member, get_cursor, get_dcs, invoke_editor, load_config, \
    output_members, parse_dcs, PatroniCtlException, PatronictlPrettyTable, query_member, show_diff
from patroni.dcs import Cluster, Failover
from patroni.postgresql.config import get_param_diff
from patroni.postgresql.mpp import get_mpp
from patroni.psycopg import OperationalError
from patroni.utils import tzutc

from . import MockConnect, MockCursor, MockResponse, psycopg_connect
from .test_etcd import etcd_read, socket_getaddrinfo
from .test_ha import get_cluster, get_cluster_initialized_with_leader, get_cluster_initialized_with_only_leader, \
    get_cluster_initialized_without_leader, get_cluster_not_initialized_without_leader, Member


def get_default_config(*args):
    return {
        'scope': 'alpha',
        'restapi': {'listen': '::', 'certfile': 'a'},
        'ctl': {'certfile': 'a'},
        'etcd': {'host': 'localhost:2379', 'retry_timeout': 10, 'ttl': 30},
        'citus': {'database': 'citus', 'group': 0},
        'postgresql': {'data_dir': '.', 'pgpass': './pgpass', 'parameters': {}, 'retry_timeout': 5}
    }


@patch.object(PoolManager, 'request', Mock(return_value=MockResponse()))
@patch('patroni.ctl.load_config', get_default_config)
@patch('patroni.dcs.AbstractDCS.get_cluster', Mock(return_value=get_cluster_initialized_with_leader()))
class TestCtl(unittest.TestCase):
    TEST_ROLES = ('primary', 'leader')

    @patch('socket.getaddrinfo', socket_getaddrinfo)
    def setUp(self):
        self.runner = CliRunner()

    @patch('patroni.ctl.logging.debug')
    def test_load_config(self, mock_logger_debug):
        runner = CliRunner()
        with runner.isolated_filesystem():
            self.assertRaises(PatroniCtlException, load_config, './non-existing-config-file', None)

        with patch('os.path.exists', Mock(return_value=True)), \
                patch('patroni.config.Config._load_config_path', Mock(return_value={})):
            load_config(CONFIG_FILE_PATH, None)
            mock_logger_debug.assert_called_once()
            self.assertEqual(('Ignoring configuration file "%s". It does not exists or is not readable.',
                              CONFIG_FILE_PATH),
                             mock_logger_debug.call_args[0])
            mock_logger_debug.reset_mock()

            with patch('os.access', Mock(return_value=True)):
                load_config(CONFIG_FILE_PATH, '')
                mock_logger_debug.assert_called_once()
                self.assertEqual(('Loading configuration from file %s', CONFIG_FILE_PATH),
                                 mock_logger_debug.call_args[0])
                mock_logger_debug.reset_mock()

    @patch('patroni.psycopg.connect', psycopg_connect)
    def test_get_cursor(self):
        with click.Context(click.Command('query')) as ctx:
            ctx.obj = {'__config': {}, '__mpp': get_mpp({})}
            for role in self.TEST_ROLES:
                self.assertIsNone(get_cursor(get_cluster_initialized_without_leader(), None, {}, role=role))
                self.assertIsNotNone(get_cursor(get_cluster_initialized_with_leader(), None, {}, role=role))

            # MockCursor returns pg_is_in_recovery as false
            self.assertIsNone(get_cursor(get_cluster_initialized_with_leader(), None, {}, role='replica'))

            self.assertIsNotNone(get_cursor(get_cluster_initialized_with_leader(), None, {'dbname': 'foo'}, role='any'))

            # Mutually exclusive options
            with self.assertRaises(PatroniCtlException) as e:
                get_cursor(get_cluster_initialized_with_leader(), None, {'dbname': 'foo'}, member_name='other',
                           role='replica')

            self.assertEqual(str(e.exception), '--role and --member are mutually exclusive options')

            # Invalid member provided
            self.assertIsNone(get_cursor(get_cluster_initialized_with_leader(), None, {'dbname': 'foo'},
                                         member_name='invalid'))

            # Valid member provided
            self.assertIsNotNone(get_cursor(get_cluster_initialized_with_leader(), None, {'dbname': 'foo'},
                                            member_name='other'))

    def test_parse_dcs(self):
        assert parse_dcs(None) is None
        assert parse_dcs('localhost') == {'etcd': {'host': 'localhost:2379'}}
        assert parse_dcs('') == {'etcd': {'host': 'localhost:2379'}}
        assert parse_dcs('localhost:8500') == {'consul': {'host': 'localhost:8500'}}
        assert parse_dcs('zookeeper://localhost') == {'zookeeper': {'hosts': ['localhost:2181']}}
        assert parse_dcs('exhibitor://dummy') == {'exhibitor': {'hosts': ['dummy'], 'port': 8181}}
        assert parse_dcs('consul://localhost') == {'consul': {'host': 'localhost:8500'}}
        assert parse_dcs('etcd3://random.com:2399') == {'etcd3': {'host': 'random.com:2399'}}
        self.assertRaises(PatroniCtlException, parse_dcs, 'invalid://test')

    def test_output_members(self):
        with click.Context(click.Command('list')) as ctx:
            ctx.obj = {'__config': {}, '__mpp': get_mpp({})}
            scheduled_at = datetime.now(tzutc) + timedelta(seconds=600)
            cluster = get_cluster_initialized_with_leader(Failover(1, 'foo', 'bar', scheduled_at))
            del cluster.members[1].data['conn_url']
            for fmt in ('pretty', 'json', 'yaml', 'topology'):
                self.assertIsNone(output_members(cluster, name='abc', fmt=fmt))

            with patch('click.echo') as mock_echo:
                self.assertIsNone(output_members(cluster, name='abc', fmt='tsv'))
                self.assertEqual(mock_echo.call_args[0][0], 'abc\tother\t\tReplica\trunning\t\tunknown')

    @patch('patroni.dcs.AbstractDCS.set_failover_value', Mock())
    def test_switchover(self):
        # Confirm
        result = self.runner.invoke(ctl, ['switchover', 'dummy', '--group', '0'], input='leader\nother\n\ny')
        self.assertEqual(result.exit_code, 0)

        # Abort
        result = self.runner.invoke(ctl, ['switchover', 'dummy', '--group', '0'], input='leader\nother\n\nN')
        self.assertEqual(result.exit_code, 1)

        # Without a candidate with --force option
        result = self.runner.invoke(ctl, ['switchover', 'dummy', '--group', '0', '--force'])
        self.assertEqual(result.exit_code, 0)

        # Scheduled (confirm)
        result = self.runner.invoke(ctl, ['switchover', 'dummy', '--group', '0'],
                                    input='leader\nother\n2300-01-01T12:23:00\ny')
        self.assertEqual(result.exit_code, 0)

        # Scheduled (abort)
        result = self.runner.invoke(ctl, ['switchover', 'dummy', '--group', '0',
                                    '--scheduled', '2015-01-01T12:00:00+01:00'], input='leader\nother\n\nN')
        self.assertEqual(result.exit_code, 1)

        # Scheduled with --force option
        result = self.runner.invoke(ctl, ['switchover', 'dummy', '--group', '0',
                                    '--force', '--scheduled', '2015-01-01T12:00:00+01:00'])
        self.assertEqual(result.exit_code, 0)

        # Scheduled in pause mode
        with patch.object(global_config.__class__, 'is_paused', PropertyMock(return_value=True)):
            result = self.runner.invoke(ctl, ['switchover', 'dummy', '--group', '0',
                                              '--force', '--scheduled', '2015-01-01T12:00:00'])
            self.assertEqual(result.exit_code, 1)
            self.assertIn("Can't schedule switchover in the paused state", result.output)

        # Target and source are equal
        result = self.runner.invoke(ctl, ['switchover', 'dummy', '--group', '0'], input='leader\nleader\n\ny')
        self.assertEqual(result.exit_code, 1)
        self.assertIn("Candidate ['other']", result.output)
        self.assertIn('Member leader is already the leader of cluster dummy', result.output)

        # Candidate is not a member of the cluster
        result = self.runner.invoke(ctl, ['switchover', 'dummy', '--group', '0'], input='leader\nReality\n\ny')
        self.assertEqual(result.exit_code, 1)
        self.assertIn('Member Reality does not exist in cluster dummy or is tagged as nofailover', result.output)

        # Invalid timestamp
        result = self.runner.invoke(ctl, ['switchover', 'dummy', '--group', '0', '--force', '--scheduled', 'invalid'])
        self.assertEqual(result.exit_code, 1)
        self.assertIn('Unable to parse scheduled timestamp', result.output)

        # Invalid timestamp
        result = self.runner.invoke(ctl, ['switchover', 'dummy', '--group', '0',
                                          '--force', '--scheduled', '2115-02-30T12:00:00+01:00'])
        self.assertEqual(result.exit_code, 1)
        self.assertIn('Unable to parse scheduled timestamp', result.output)

        # Specifying wrong leader
        result = self.runner.invoke(ctl, ['switchover', 'dummy', '--group', '0'], input='dummy')
        self.assertEqual(result.exit_code, 1)
        self.assertIn('Member dummy is not the leader of cluster dummy', result.output)

        # Errors while sending Patroni REST API request
        with patch('patroni.ctl.request_patroni', Mock(side_effect=Exception)):
            result = self.runner.invoke(ctl, ['switchover', 'dummy', '--group', '0'],
                                        input='leader\nother\n2300-01-01T12:23:00\ny')
            self.assertIn('falling back to DCS', result.output)

        with patch('patroni.ctl.request_patroni') as mock_api_request:
            mock_api_request.return_value.status = 500
            result = self.runner.invoke(ctl, ['switchover', 'dummy', '--group', '0'], input='leader\nother\n\ny')
            self.assertIn('Switchover failed', result.output)

            mock_api_request.return_value.status = 501
            mock_api_request.return_value.data = b'Server does not support this operation'
            result = self.runner.invoke(ctl, ['switchover', 'dummy', '--group', '0'], input='leader\nother\n\ny')
            self.assertIn('Switchover failed', result.output)

        # No members available
        with patch('patroni.dcs.AbstractDCS.get_cluster',
                   Mock(return_value=get_cluster_initialized_with_only_leader())):
            result = self.runner.invoke(ctl, ['switchover', 'dummy', '--group', '0'], input='leader\nother\n\ny')
            self.assertEqual(result.exit_code, 1)
            self.assertIn('No candidates found to switchover to', result.output)

        # No leader available
        with patch('patroni.dcs.AbstractDCS.get_cluster', Mock(return_value=get_cluster_initialized_without_leader())):
            result = self.runner.invoke(ctl, ['switchover', 'dummy', '--group', '0'], input='leader\nother\n\ny')
            self.assertEqual(result.exit_code, 1)
            self.assertIn('This cluster has no leader', result.output)

        # Citus cluster, no group number specified
        result = self.runner.invoke(ctl, ['switchover', 'dummy', '--force'], input='\n')
        self.assertEqual(result.exit_code, 1)
        self.assertIn('For Citus clusters the --group must me specified', result.output)

    @patch('patroni.dcs.AbstractDCS.set_failover_value', Mock())
    def test_failover(self):
        # No candidate specified
        result = self.runner.invoke(ctl, ['failover', 'dummy'], input='0\n')
        self.assertIn('Failover could be performed only to a specific candidate', result.output)

        # Candidate is the same as the leader
        result = self.runner.invoke(ctl, ['failover', 'dummy', '--group', '0'], input='leader\n')
        self.assertIn("Candidate ['other']", result.output)
        self.assertIn('Member leader is already the leader of cluster dummy', result.output)

        cluster = get_cluster_initialized_with_leader(sync=('leader', 'other'))
        cluster.members.append(Member(0, 'async', 28, {'api_url': 'http://127.0.0.1:8012/patroni'}))
        cluster.config.data['synchronous_mode'] = True
        with patch('patroni.dcs.AbstractDCS.get_cluster', Mock(return_value=cluster)):
            # Failover to an async member in sync mode (confirm)
            result = self.runner.invoke(ctl,
                                        ['failover', 'dummy', '--group', '0', '--candidate', 'async'], input='y\ny')
            self.assertIn('Are you sure you want to failover to the asynchronous node async', result.output)
            self.assertEqual(result.exit_code, 0)

            # Failover to an async member in sync mode (abort)
            result = self.runner.invoke(ctl, ['failover', 'dummy', '--group', '0', '--candidate', 'async'], input='N')
            self.assertEqual(result.exit_code, 1)
            self.assertIn('Aborting failover', result.output)

    @patch('patroni.dynamic_loader.iter_modules', Mock(return_value=['patroni.dcs.dummy', 'patroni.dcs.etcd']))
    def test_get_dcs(self):
        with click.Context(click.Command('list')) as ctx:
            ctx.obj = {'__config': {'dummy': {}}, '__mpp': get_mpp({})}
            self.assertRaises(PatroniCtlException, get_dcs, 'dummy', 0)

    @patch('patroni.psycopg.connect', psycopg_connect)
    @patch('patroni.ctl.query_member', Mock(return_value=([['mock column']], None)))
    @patch.object(etcd.Client, 'read', etcd_read)
    def test_query(self):
        # Mutually exclusive
        for role in self.TEST_ROLES:
            result = self.runner.invoke(ctl, ['query', 'alpha', '--member', 'abc', '--role', role])
            assert result.exit_code == 1

        with self.runner.isolated_filesystem():
            with open('dummy', 'w') as dummy_file:
                dummy_file.write('SELECT 1')

            # Mutually exclusive
            result = self.runner.invoke(ctl, ['query', 'alpha', '--file', 'dummy', '--command', 'dummy'])
            assert result.exit_code == 1

            result = self.runner.invoke(ctl, ['query', 'alpha', '--member', 'abc', '--file', 'dummy'])
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
            for role in self.TEST_ROLES:
                rows = query_member(None, None, None, None, role, 'SELECT pg_catalog.pg_is_in_recovery()', {})
                self.assertTrue('False' in str(rows))

            with patch.object(MockCursor, 'execute', Mock(side_effect=OperationalError('bla'))):
                rows = query_member(None, None, None, None, 'replica', 'SELECT pg_catalog.pg_is_in_recovery()', {})

        with patch('patroni.ctl.get_cursor', Mock(return_value=None)):
            # No role nor member given -- generic message
            rows = query_member(None, None, None, None, None, 'SELECT pg_catalog.pg_is_in_recovery()', {})
            self.assertTrue('No connection is available' in str(rows))

            # Member given -- message pointing to member
            rows = query_member(None, None, None, 'foo', None, 'SELECT pg_catalog.pg_is_in_recovery()', {})
            self.assertTrue('No connection to member foo' in str(rows))

            # Role given -- message pointing to role
            rows = query_member(None, None, None, None, 'replica', 'SELECT pg_catalog.pg_is_in_recovery()', {})
            self.assertTrue('No connection to role replica' in str(rows))

        with patch('patroni.ctl.get_cursor', Mock(side_effect=OperationalError('bla'))):
            rows = query_member(None, None, None, None, 'replica', 'SELECT pg_catalog.pg_is_in_recovery()', {})

    def test_dsn(self):
        result = self.runner.invoke(ctl, ['dsn', 'alpha'])
        assert 'host=127.0.0.1 port=5435' in result.output

        # Mutually exclusive options
        for role in self.TEST_ROLES:
            result = self.runner.invoke(ctl, ['dsn', 'alpha', '--role', role, '--member', 'dummy'])
            assert result.exit_code == 1

        # Non-existing member
        result = self.runner.invoke(ctl, ['dsn', 'alpha', '--member', 'dummy'])
        assert result.exit_code == 1

    @patch('patroni.ctl.request_patroni')
    def test_reload(self, mock_post):
        result = self.runner.invoke(ctl, ['reload', 'alpha'], input='y')
        assert 'Failed: reload for member' in result.output

        mock_post.return_value.status = 200
        result = self.runner.invoke(ctl, ['reload', 'alpha'], input='y')
        assert 'No changes to apply on member' in result.output

        mock_post.return_value.status = 202
        result = self.runner.invoke(ctl, ['reload', 'alpha'], input='y')
        assert 'Reload request received for member' in result.output

    @patch('patroni.ctl.request_patroni')
    def test_restart_reinit(self, mock_post):
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

        with patch.object(global_config.__class__, 'is_paused', PropertyMock(return_value=True)):
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

    def test_remove(self):
        result = self.runner.invoke(ctl, ['remove', 'dummy'], input='\n')
        assert 'For Citus clusters the --group must me specified' in result.output
        result = self.runner.invoke(ctl, ['remove', 'alpha', '--group', '0'], input='alpha\nstandby')
        assert 'Please confirm' in result.output
        assert 'You are about to remove all' in result.output
        # Not typing an exact confirmation
        assert result.exit_code == 1

        # leader specified does not match leader of cluster
        result = self.runner.invoke(ctl, ['remove', 'alpha', '--group', '0'], input='alpha\nYes I am aware\nstandby')
        assert result.exit_code == 1

        # cluster specified on cmdline does not match verification prompt
        result = self.runner.invoke(ctl, ['remove', 'alpha', '--group', '0'], input='beta\nleader')
        assert result.exit_code == 1

        result = self.runner.invoke(ctl, ['remove', 'alpha', '--group', '0'], input='alpha\nYes I am aware\nleader')
        assert result.exit_code == 0

    def test_ctl(self):
        result = self.runner.invoke(ctl, ['--help'])
        assert 'Usage:' in result.output

    def test_get_any_member(self):
        with click.Context(click.Command('list')) as ctx:
            ctx.obj = {'__config': {}, '__mpp': get_mpp({})}
            for role in self.TEST_ROLES:
                self.assertIsNone(get_any_member(get_cluster_initialized_without_leader(), None, role=role))

                m = get_any_member(get_cluster_initialized_with_leader(), None, role=role)
                self.assertEqual(m.name, 'leader')

    def test_get_all_members(self):
        with click.Context(click.Command('list')) as ctx:
            ctx.obj = {'__config': {}, '__mpp': get_mpp({})}
            for role in self.TEST_ROLES:
                self.assertEqual(list(get_all_members(get_cluster_initialized_without_leader(), None, role=role)), [])

                r = list(get_all_members(get_cluster_initialized_with_leader(), None, role=role))
                self.assertEqual(len(r), 1)
                self.assertEqual(r[0].name, 'leader')

            r = list(get_all_members(get_cluster_initialized_with_leader(), None, role='replica'))
            self.assertEqual(len(r), 1)
            self.assertEqual(r[0].name, 'other')

            self.assertEqual(len(list(get_all_members(get_cluster_initialized_without_leader(),
                                                      None, role='replica'))), 2)

    def test_members(self):
        result = self.runner.invoke(ctl, ['list'])
        assert '127.0.0.1' in result.output
        assert result.exit_code == 0
        assert 'Citus cluster: alpha -' in result.output

        result = self.runner.invoke(ctl, ['list', '--group', '0'])
        assert 'Citus cluster: alpha (group: 0, 12345678901) -' in result.output

        config = get_default_config()
        del config['citus']
        with patch('patroni.ctl.load_config', Mock(return_value=config)):
            result = self.runner.invoke(ctl, ['list'])
            assert 'Cluster: alpha (12345678901) -' in result.output

        with patch('patroni.ctl.load_config', Mock(return_value={})):
            self.runner.invoke(ctl, ['list'])

        cluster = get_cluster_initialized_with_leader()
        cluster.members[1].data['pending_restart'] = True
        cluster.members[1].data['pending_restart_reason'] = {'param': get_param_diff('', 'very l' + 'o' * 34 + 'ng')}
        with patch('patroni.dcs.AbstractDCS.get_cluster', Mock(return_value=cluster)):
            for cmd in ('list', 'topology'):
                result = self.runner.invoke(ctl, [cmd, 'dummy'])
                self.assertIn('param: [hidden - too long]', result.output)

            result = self.runner.invoke(ctl, ['list', 'dummy', '-f', 'tsv'])
            self.assertIn('param: ->very l' + 'o' * 34 + 'ng', result.output)

            cluster.members[1].data['pending_restart_reason'] = {'param': get_param_diff('', 'new')}
            result = self.runner.invoke(ctl, ['list', 'dummy'])
            self.assertIn('param: ->new', result.output)

    def test_list_extended(self):
        result = self.runner.invoke(ctl, ['list', 'dummy', '--extended', '--timestamp'])
        assert '2100' in result.output
        assert 'Scheduled restart' in result.output

    def test_list_standby_cluster(self):
        cluster = get_cluster_initialized_without_leader(leader=True, sync=('leader', 'other'))
        cluster.config.data.update(synchronous_mode=True, standby_cluster={'port': 5433})
        with patch('patroni.dcs.AbstractDCS.get_cluster', Mock(return_value=cluster)):
            result = self.runner.invoke(ctl, ['list'])
            self.assertEqual(result.exit_code, 0)
            self.assertNotIn('Sync Standby', result.output)

    def test_topology(self):
        cluster = get_cluster_initialized_with_leader()
        cluster.members.append(Member(0, 'cascade', 28,
                                      {'conn_url': 'postgres://replicator:rep-pass@127.0.0.1:5437/postgres',
                                       'api_url': 'http://127.0.0.1:8012/patroni', 'state': 'running',
                                       'tags': {'replicatefrom': 'other'}}))
        cluster.members.append(Member(0, 'wrong_cascade', 28,
                                      {'conn_url': 'postgres://replicator:rep-pass@127.0.0.1:5438/postgres',
                                       'api_url': 'http://127.0.0.1:8013/patroni', 'state': 'running',
                                       'tags': {'replicatefrom': 'nonexistinghost'}}))
        with patch('patroni.dcs.AbstractDCS.get_cluster', Mock(return_value=cluster)):
            result = self.runner.invoke(ctl, ['topology', 'dummy'])
            assert '+\n|     0 | leader          | 127.0.0.1:5435 | Leader  |' in result.output
            assert '|\n|     0 | + other         | 127.0.0.1:5436 | Replica |' in result.output
            assert '|\n|     0 |   + cascade     | 127.0.0.1:5437 | Replica |' in result.output
            assert '|\n|     0 | + wrong_cascade | 127.0.0.1:5438 | Replica |' in result.output

        with patch('patroni.dcs.AbstractDCS.get_cluster', Mock(return_value=get_cluster_initialized_without_leader())):
            result = self.runner.invoke(ctl, ['topology', 'dummy'])
            assert '+\n|     0 | + leader | 127.0.0.1:5435 | Replica |' in result.output
            assert '|\n|     0 | + other  | 127.0.0.1:5436 | Replica |' in result.output

    @patch('patroni.dcs.AbstractDCS.get_cluster', Mock(return_value=get_cluster_initialized_with_leader()))
    def test_flush_restart(self):
        for role in self.TEST_ROLES:
            result = self.runner.invoke(ctl, ['flush', 'dummy', 'restart', '-r', role], input='y')
            assert 'No scheduled restart' in result.output

        result = self.runner.invoke(ctl, ['flush', 'dummy', 'restart', '--force'])
        assert 'Success: flush scheduled restart' in result.output
        with patch('patroni.ctl.request_patroni', Mock(return_value=MockResponse(404))):
            result = self.runner.invoke(ctl, ['flush', 'dummy', 'restart', '--force'])
            assert 'Failed: flush scheduled restart' in result.output

    def test_flush_switchover(self):
        with patch('patroni.dcs.AbstractDCS.get_cluster', Mock(return_value=get_cluster_initialized_with_leader())):
            result = self.runner.invoke(ctl, ['flush', 'dummy', 'switchover'])
            assert 'No pending scheduled switchover' in result.output

        scheduled_at = datetime.now(tzutc) + timedelta(seconds=600)
        with patch('patroni.dcs.AbstractDCS.get_cluster',
                   Mock(return_value=get_cluster_initialized_with_leader(Failover(1, 'a', 'b', scheduled_at)))):
            result = self.runner.invoke(ctl, ['-k', 'flush', 'dummy', 'switchover'])
            assert result.output.startswith('Success: ')

            with patch('patroni.ctl.request_patroni', side_effect=[MockResponse(409), Exception]), \
                    patch('patroni.dcs.AbstractDCS.manual_failover', Mock()):
                result = self.runner.invoke(ctl, ['flush', 'dummy', 'switchover'])
                assert 'Could not find any accessible member of cluster' in result.output

    @patch('patroni.ctl.polling_loop', Mock(return_value=[1]))
    def test_pause_cluster(self):
        with patch('patroni.ctl.request_patroni', Mock(return_value=MockResponse(500))):
            result = self.runner.invoke(ctl, ['pause', 'dummy'])
            assert 'Failed' in result.output

        with patch.object(global_config.__class__, 'is_paused', PropertyMock(return_value=True)):
            result = self.runner.invoke(ctl, ['pause', 'dummy'])
            assert 'Cluster is already paused' in result.output

        result = self.runner.invoke(ctl, ['pause', 'dummy', '--wait'])
        assert "'pause' request sent" in result.output

        with patch('patroni.dcs.AbstractDCS.get_cluster',
                   Mock(side_effect=[get_cluster_initialized_with_leader(), get_cluster(None, None, [], None, None)])):
            self.runner.invoke(ctl, ['pause', 'dummy', '--wait'])
        with patch('patroni.dcs.AbstractDCS.get_cluster',
                   Mock(side_effect=[get_cluster_initialized_with_leader(),
                                     get_cluster(None, None, [Member(1, 'other', 28, {})], None, None)])):
            self.runner.invoke(ctl, ['pause', 'dummy', '--wait'])

    @patch('patroni.ctl.request_patroni')
    @patch('patroni.dcs.AbstractDCS.get_cluster', Mock(return_value=get_cluster_initialized_with_leader()))
    def test_resume_cluster(self, mock_post):
        mock_post.return_value.status = 200
        with patch.object(global_config.__class__, 'is_paused', PropertyMock(return_value=False)):
            result = self.runner.invoke(ctl, ['resume', 'dummy'])
            assert 'Cluster is not paused' in result.output

        with patch.object(global_config.__class__, 'is_paused', PropertyMock(return_value=True)):
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
    @patch('patroni.ctl.markup_to_pager')
    @patch('os.environ.get', return_value=None)
    @patch('shutil.which', return_value=None)
    def test_show_diff(self, mock_which, mock_env_get, mock_markup_to_pager, mock_isatty):
        # no TTY
        show_diff("foo:\n  bar: 1\n", "foo:\n  bar: 2\n")
        mock_markup_to_pager.assert_not_called()

        # TTY but no PAGER nor executable
        mock_isatty.return_value = True
        with self.assertRaises(PatroniCtlException) as e:
            show_diff("foo:\n  bar: 1\n", "foo:\n  bar: 2\n")
        self.assertEqual(
            str(e.exception),
            'No pager could be found. Either set PAGER environment variable with '
            'your pager or install either "less" or "more" in the host.'
        )
        mock_env_get.assert_called_once_with('PAGER')
        mock_which.assert_has_calls([
            mock.call('less'),
            mock.call('more'),
        ])
        mock_markup_to_pager.assert_not_called()

        # TTY with PAGER set but invalid
        mock_env_get.reset_mock()
        mock_env_get.return_value = 'random'
        mock_which.reset_mock()
        with self.assertRaises(PatroniCtlException) as e:
            show_diff("foo:\n  bar: 1\n", "foo:\n  bar: 2\n")
        self.assertEqual(
            str(e.exception),
            'No pager could be found. Either set PAGER environment variable with '
            'your pager or install either "less" or "more" in the host.'
        )
        mock_env_get.assert_called_once_with('PAGER')
        mock_which.assert_has_calls([
            mock.call('random'),
            mock.call('less'),
            mock.call('more'),
        ])
        mock_markup_to_pager.assert_not_called()

        # TTY with valid executable
        mock_which.side_effect = [None, '/usr/bin/less', None]
        show_diff("foo:\n  bar: 1\n", "foo:\n  bar: 2\n")
        mock_markup_to_pager.assert_called_once()

        # Test that unicode handling doesn't fail with an exception
        mock_which.side_effect = [None, '/usr/bin/less', None]
        show_diff(b"foo:\n  bar: \xc3\xb6\xc3\xb6\n".decode('utf-8'),
                  b"foo:\n  bar: \xc3\xbc\xc3\xbc\n".decode('utf-8'))

    @patch('subprocess.call', return_value=1)
    def test_invoke_editor(self, mock_subprocess_call):
        os.environ.pop('EDITOR', None)
        for e in ('', '/bin/vi'):
            with patch('shutil.which', Mock(return_value=e)):
                self.assertRaises(PatroniCtlException, invoke_editor, 'foo: bar\n', 'test')

    def test_show_config(self):
        self.runner.invoke(ctl, ['show-config', 'dummy'])

    @patch('subprocess.call', Mock(return_value=0))
    def test_edit_config(self):
        os.environ['EDITOR'] = 'true'
        self.runner.invoke(ctl, ['edit-config', 'dummy'])
        self.runner.invoke(ctl, ['edit-config', 'dummy', '-s', 'foo=bar'])
        self.runner.invoke(ctl, ['edit-config', 'dummy', '--replace', 'postgres0.yml'])
        self.runner.invoke(ctl, ['edit-config', 'dummy', '--apply', '-'], input='foo: bar')
        self.runner.invoke(ctl, ['edit-config', 'dummy', '--force', '--apply', '-'], input='foo: bar')
        with patch('patroni.dcs.etcd.Etcd.set_config_value', Mock(return_value=True)):
            self.runner.invoke(ctl, ['edit-config', 'dummy', '--force', '--apply', '-'], input='foo: bar')
        with patch('patroni.dcs.AbstractDCS.get_cluster', Mock(return_value=Cluster.empty())):
            result = self.runner.invoke(ctl, ['edit-config', 'dummy'])
            assert result.exit_code == 1
            assert 'The config key does not exist in the cluster dummy' in result.output

    @patch('patroni.ctl.request_patroni')
    def test_version(self, mock_request):
        result = self.runner.invoke(ctl, ['version'])
        assert 'patronictl version' in result.output
        mock_request.return_value.data = b'{"patroni":{"version":"1.2.3"},"server_version": 100001}'
        result = self.runner.invoke(ctl, ['version', 'dummy'])
        assert '1.2.3' in result.output
        mock_request.side_effect = Exception
        result = self.runner.invoke(ctl, ['version', 'dummy'])
        assert 'failed to get version' in result.output

    def test_history(self):
        with patch('patroni.dcs.AbstractDCS.get_cluster') as mock_get_cluster:
            mock_get_cluster.return_value.history.lines = [[1, 67176, 'no recovery target specified']]
            result = self.runner.invoke(ctl, ['history'])
            assert 'Reason' in result.output

    def test_format_pg_version(self):
        self.assertEqual(format_pg_version(100001), '10.1')
        self.assertEqual(format_pg_version(90605), '9.6.5')

    def test_get_members(self):
        with patch('patroni.dcs.AbstractDCS.get_cluster',
                   Mock(return_value=get_cluster_not_initialized_without_leader())):
            result = self.runner.invoke(ctl, ['reinit', 'dummy'])
            assert "cluster doesn\'t have any members" in result.output

    @patch('time.sleep', Mock())
    def test_reinit_wait(self):
        with patch.object(PoolManager, 'request') as mocked:
            mocked.side_effect = [Mock(data=s, status=200) for s in
                                  [b"reinitialize", b'{"state":"creating replica"}', b'{"state":"running"}']]
            result = self.runner.invoke(ctl, ['reinit', 'alpha', 'other', '--wait'], input='y\ny')
        self.assertIn("Waiting for reinitialize to complete on: other", result.output)
        self.assertIn("Reinitialize is completed on: other", result.output)


class TestPatronictlPrettyTable(unittest.TestCase):

    def setUp(self):
        self.pt = PatronictlPrettyTable(' header', ['foo', 'bar'], hrules=ALL)

    def test__get_hline(self):
        expected = '+-----+-----+'
        self.pt._hrule = expected
        self.assertEqual(self.pt._hrule, '+ header----+')
        self.assertFalse(self.pt._is_first_hline())
        self.assertEqual(self.pt._hrule, expected)

    @patch.object(PrettyTable, '_stringify_hrule', Mock(return_value='+-----+-----+'))
    def test__stringify_hrule(self):
        self.assertEqual(self.pt._stringify_hrule((), 'top_'), '+ header----+')
        self.assertFalse(self.pt._is_first_hline())

    def test_output(self):
        self.assertEqual(str(self.pt), '+ header----+\n| foo | bar |\n+-----+-----+')
