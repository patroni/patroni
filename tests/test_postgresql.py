import mock  # for the mock.call method, importing it without a namespace breaks python3
import os
import psycopg2
import shutil
import unittest

from six.moves import builtins
from mock import Mock, MagicMock, PropertyMock, patch, mock_open
from patroni.dcs import Cluster, Leader, Member
from patroni.exceptions import PostgresException, PostgresConnectionException
from patroni.postgresql import Postgresql
from patroni.utils import RetryFailedError
from test_ha import false
import subprocess


def is_file_raise_on_backup(*args, **kwargs):
    if args[0].endswith('.backup'):
        raise Exception("foo")


class MockCursor:

    def __init__(self, connection):
        self.connection = connection
        self.closed = False
        self.results = []

    def execute(self, sql, *params):
        if sql.startswith('blabla') or sql == 'CHECKPOINT':
            raise psycopg2.OperationalError()
        elif sql.startswith('RetryFailedError'):
            raise RetryFailedError('retry')
        elif sql.startswith('SELECT slot_name'):
            self.results = [('blabla',), ('foobar',)]
        elif sql.startswith('SELECT pg_xlog_location_diff'):
            self.results = [(0,)]
        elif sql == 'SELECT pg_is_in_recovery()':
            self.results = [(False, )]
        elif sql.startswith('SELECT to_char(pg_postmaster_start_time'):
            self.results = [('', True, '', '', '', False)]
        else:
            self.results = [(
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )]

    def fetchone(self):
        return self.results[0]

    def fetchall(self):
        return self.results

    def close(self):
        pass

    def __iter__(self):
        for i in self.results:
            yield i

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class MockConnect(Mock):

    autocommit = False
    closed = 0

    def cursor(self):
        return MockCursor(self)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


def pg_controldata_string(*args, **kwargs):
    return b"""
pg_control version number:            942
Catalog version number:               201509161
Database system identifier:           6200971513092291716
Database cluster state:               shut down in recovery
pg_control last modified:             Fri Oct  2 10:57:06 2015
Latest checkpoint location:           0/30000C8
Prior checkpoint location:            0/2000060
Latest checkpoint's REDO location:    0/3000090
Latest checkpoint's REDO WAL file:    000000020000000000000003
Latest checkpoint's TimeLineID:       2
Latest checkpoint's PrevTimeLineID:   2
Latest checkpoint's full_page_writes: on
Latest checkpoint's NextXID:          0/943
Latest checkpoint's NextOID:          24576
Latest checkpoint's NextMultiXactId:  1
Latest checkpoint's NextMultiOffset:  0
Latest checkpoint's oldestXID:        931
Latest checkpoint's oldestXID's DB:   1
Latest checkpoint's oldestActiveXID:  943
Latest checkpoint's oldestMultiXid:   1
Latest checkpoint's oldestMulti's DB: 1
Latest checkpoint's oldestCommitTs:   0
Latest checkpoint's newestCommitTs:   0
Time of latest checkpoint:            Fri Oct  2 10:56:54 2015
Fake LSN counter for unlogged rels:   0/1
Minimum recovery ending location:     0/30241F8
Min recovery ending loc's timeline:   2
Backup start location:                0/0
Backup end location:                  0/0
End-of-backup record required:        no
wal_level setting:                    hot_standby
Current wal_log_hints setting:                on
Current max_connections setting:              100
Current max_worker_processes setting:         8
Current max_prepared_xacts setting:           0
Current max_locks_per_xact setting:           64
Current track_commit_timestamp setting:       off
Maximum data alignment:               8
Database block size:                  8192
Blocks per segment of large relation: 131072
WAL block size:                       8192
Bytes per WAL segment:                16777216
Maximum length of identifiers:        64
Maximum columns in an index:          32
Maximum size of a TOAST chunk:        1996
Size of a large-object chunk:         2048
Date/time type storage:               64-bit integers
Float4 argument passing:              by value
Float8 argument passing:              by value
Data page checksum version:           0
"""


def postmaster_opts_string(*args, **kwargs):
    return '/usr/local/pgsql/bin/postgres "-D" "data/postgresql0" "--listen_addresses=127.0.0.1" \
"--port=5432" "--hot_standby=on" "--wal_keep_segments=8" "--wal_level=hot_standby" \
"--archive_command=mkdir -p ../wal_archive && cp %p ../wal_archive/%f" "--wal_log_hints=on" \
"--max_wal_senders=5" "--archive_timeout=1800s" "--archive_mode=on" "--max_replication_slots=5"\n'


def psycopg2_connect(*args, **kwargs):
    return MockConnect()


@patch('subprocess.call', Mock(return_value=0))
@patch('psycopg2.connect', psycopg2_connect)
@patch('shutil.copy', Mock())
class TestPostgresql(unittest.TestCase):

    @patch('subprocess.call', Mock(return_value=0))
    @patch('psycopg2.connect', psycopg2_connect)
    def setUp(self):
        self.p = Postgresql({'name': 'test0', 'scope': 'batman', 'data_dir': 'data/test0',
                             'listen': '127.0.0.1, *:5432', 'connect_address': '127.0.0.2:5432',
                             'pg_hba': ['hostssl all all 0.0.0.0/0 md5', 'host all all 0.0.0.0/0 md5'],
                             'superuser': {'password': 'test'},
                             'admin': {'username': 'admin', 'password': 'admin'},
                             'pg_rewind': {'username': 'admin', 'password': 'admin'},
                             'replication': {'username': 'replicator',
                                             'password': 'rep-pass',
                                             'network': '127.0.0.1/32'},
                             'parameters': {'foo': 'bar'}, 'recovery_conf': {'foo': 'bar'},
                             'callbacks': {'on_start': 'true', 'on_stop': 'true',
                                           'on_restart': 'true', 'on_role_change': 'true',
                                           'on_reload': 'true'
                                           },
                             'restore': 'true'})
        if not os.path.exists(self.p.data_dir):
            os.makedirs(self.p.data_dir)
        self.leadermem = Member(0, 'leader', 28, {'conn_url': 'postgres://replicator:rep-pass@127.0.0.1:5435/postgres'})
        self.leader = Leader(-1, 28, self.leadermem)
        self.other = Member(0, 'test1', 28, {'conn_url': 'postgres://replicator:rep-pass@127.0.0.1:5433/postgres'})
        self.me = Member(0, 'test0', 28, {'conn_url': 'postgres://replicator:rep-pass@127.0.0.1:5434/postgres'})

    def tearDown(self):
        shutil.rmtree('data')

    def test_data_directory_empty(self):
        self.assertTrue(self.p.data_directory_empty())

    def test_get_initdb_options(self):
        self.p.initdb_options = [{'encoding': 'UTF8'}, 'data-checksums']
        self.assertEquals(self.p.get_initdb_options(), ['--encoding=UTF8', '--data-checksums'])
        self.p.initdb_options = [{'pgdata': 'bar'}]
        self.assertRaises(Exception, self.p.get_initdb_options)
        self.p.initdb_options = [{'foo': 'bar', 1: 2}]
        self.assertRaises(Exception, self.p.get_initdb_options)
        self.p.initdb_options = [1]
        self.assertRaises(Exception, self.p.get_initdb_options)

    def test_initialize(self):
        self.assertTrue(self.p.initialize())
        self.assertTrue(os.path.exists(os.path.join(self.p.data_dir, 'pg_hba.conf')))

    def test_start(self):
        self.assertTrue(self.p.start())
        self.p.is_running = false
        open(os.path.join(self.p.data_dir, 'postmaster.pid'), 'w').close()
        self.assertTrue(self.p.start())

    def test_stop(self):
        self.assertTrue(self.p.stop())
        with patch('subprocess.call', Mock(return_value=1)):
            self.assertTrue(self.p.stop())
            self.p.is_running = Mock(return_value=True)
            self.assertFalse(self.p.stop())

    def test_restart(self):
        self.p.start = false
        self.p.is_running = false
        self.assertFalse(self.p.restart())
        self.assertEquals(self.p.state, 'restart failed (restarting)')

    @patch.object(builtins, 'open', MagicMock())
    def test_write_pgpass(self):
        self.p.write_pgpass({'host': 'localhost', 'port': '5432', 'user': 'foo', 'password': 'bar'})

    @patch('patroni.postgresql.Postgresql.write_pgpass', MagicMock(return_value=dict()))
    def test_sync_from_leader(self):
        self.assertTrue(self.p.sync_from_leader(self.leader))

    @patch('subprocess.call', side_effect=Exception("Test"))
    @patch('patroni.postgresql.Postgresql.write_pgpass', MagicMock(return_value=dict()))
    def test_pg_rewind(self, mock_call):
        self.assertTrue(self.p.rewind(self.leader))
        subprocess.call = mock_call
        self.assertFalse(self.p.rewind(self.leader))

    @patch('patroni.postgresql.Postgresql.rewind', return_value=False)
    @patch('patroni.postgresql.Postgresql.remove_data_directory', MagicMock(return_value=True))
    @patch('patroni.postgresql.Postgresql.single_user_mode', MagicMock(return_value=1))
    @patch('patroni.postgresql.Postgresql.write_pgpass', MagicMock(return_value=dict()))
    def test_follow_the_leader(self, mock_pg_rewind):
        self.p.demote()
        self.p.follow_the_leader(None)
        self.p.demote()
        self.p.follow_the_leader(self.leader)
        self.p.follow_the_leader(Leader(-1, 28, self.other))
        self.p.rewind = mock_pg_rewind
        self.p.follow_the_leader(self.leader)
        self.p.require_rewind()
        with mock.patch('os.path.islink', MagicMock(return_value=True)):
            with mock.patch('patroni.postgresql.Postgresql.can_rewind', new_callable=PropertyMock(return_value=True)):
                with mock.patch('os.unlink', MagicMock(return_value=True)):
                    self.p.follow_the_leader(self.leader, recovery=True)
        self.p.require_rewind()
        with mock.patch('patroni.postgresql.Postgresql.can_rewind', new_callable=PropertyMock(return_value=True)):
            self.p.rewind.return_value = True
            self.p.follow_the_leader(self.leader, recovery=True)
            self.p.rewind.return_value = False
            self.p.follow_the_leader(self.leader, recovery=True)

    def test_can_rewind(self):
        tmp = self.p.pg_rewind
        self.p.pg_rewind = None
        self.assertFalse(self.p.can_rewind)
        self.p.pg_rewind = tmp
        with mock.patch('subprocess.call', MagicMock(return_value=1)):
            self.assertFalse(self.p.can_rewind)
        with mock.patch('subprocess.call', side_effect=OSError("foo")):
            self.assertFalse(self.p.can_rewind)
        tmp = self.p.controldata()
        self.p.controldata = lambda: {'wal_log_hints setting': 'on'}
        self.assertTrue(self.p.can_rewind)
        self.p.controldata = tmp

    @patch('time.sleep', Mock())
    def test_create_replica(self):
        self.p.delete_trigger_file = Mock(side_effect=OSError())
        with patch('subprocess.call', Mock(side_effect=[1, 0])):
            self.assertEquals(self.p.create_replica(self.leader, ''), 0)
        with patch('subprocess.call', Mock(side_effect=[Exception(), 0])):
            self.assertEquals(self.p.create_replica(self.leader, ''), 0)

        self.p.config['create_replica_method'] = ['wale', 'basebackup']
        self.p.config['wale'] = {'command': 'foo'}
        with patch('subprocess.call', Mock(return_value=0)):
            self.assertEquals(self.p.create_replica(self.leader, ''), 0)
            del self.p.config['wale']
            self.assertEquals(self.p.create_replica(self.leader, ''), 0)

        with patch('subprocess.call', Mock(side_effect=Exception("foo"))):
            self.assertEquals(self.p.create_replica(self.leader, ''), 1)

    def test_create_connection_users(self):
        cfg = self.p.config
        cfg['superuser']['username'] = 'test'
        p = Postgresql(cfg)
        p.create_connection_users()

    def test_sync_replication_slots(self):
        self.p.start()
        cluster = Cluster(True, self.leader, 0, [self.me, self.other, self.leadermem], None)
        self.p.sync_replication_slots(cluster)
        self.p.query = Mock(side_effect=psycopg2.OperationalError)
        self.p.schedule_load_slots = True
        self.p.sync_replication_slots(cluster)

    @patch.object(MockConnect, 'closed', 2)
    def test__query(self):
        self.assertRaises(PostgresConnectionException, self.p._query, 'blabla')
        self.p._state = 'restarting'
        self.assertRaises(RetryFailedError, self.p._query, 'blabla')

    def test_query(self):
        self.p.query('select 1')
        self.assertRaises(PostgresConnectionException, self.p.query, 'RetryFailedError')
        self.assertRaises(psycopg2.OperationalError, self.p.query, 'blabla')

    def test_is_leader(self):
        self.assertTrue(self.p.is_leader())

    def test_reload(self):
        self.assertTrue(self.p.reload())

    def test_is_healthy(self):
        self.assertTrue(self.p.is_healthy())
        self.p.is_running = false
        self.assertFalse(self.p.is_healthy())

    def test_promote(self):
        self.p._role = 'replica'
        self.assertTrue(self.p.promote())
        self.assertTrue(self.p.promote())

    def test_last_operation(self):
        self.assertEquals(self.p.last_operation(), '0')

    @patch('subprocess.Popen', Mock(side_effect=OSError()))
    def test_call_nowait(self):
        self.assertFalse(self.p.call_nowait('on_start'))

    def test_non_existing_callback(self):
        self.assertFalse(self.p.call_nowait('foobar'))

    def test_is_leader_exception(self):
        self.p.start()
        self.p.query = Mock(side_effect=psycopg2.OperationalError("not supported"))
        self.assertTrue(self.p.stop())

    def test_check_replication_lag(self):
        self.assertTrue(self.p.check_replication_lag(0))

    @patch('os.rename', Mock())
    @patch('os.path.isdir', Mock(return_value=True))
    def test_move_data_directory(self):
        self.p.is_running = false
        self.p.move_data_directory()
        with patch('os.rename', Mock(side_effect=OSError())):
            self.p.move_data_directory()

    @patch('patroni.postgresql.Postgresql.write_pgpass', MagicMock(return_value=dict()))
    def test_bootstrap(self):
        with patch('subprocess.call', Mock(return_value=1)):
            self.assertRaises(PostgresException, self.p.bootstrap)
        self.p.bootstrap()
        self.p.bootstrap(self.leader)

    def test_remove_data_directory(self):
        self.p.data_dir = 'data_dir'
        self.p.remove_data_directory()
        os.mkdir(self.p.data_dir)
        self.p.remove_data_directory()
        open(self.p.data_dir, 'w').close()
        self.p.remove_data_directory()
        os.symlink('unexisting', self.p.data_dir)
        with patch('os.unlink', Mock(side_effect=Exception)):
            self.p.remove_data_directory()
        self.p.remove_data_directory()

    @patch('subprocess.check_output', MagicMock(return_value=0, side_effect=pg_controldata_string))
    @patch('subprocess.check_output', side_effect=subprocess.CalledProcessError)
    @patch('subprocess.check_output', side_effect=Exception('Failed'))
    def test_controldata(self, check_output_call_error, check_output_generic_exception):
        data = self.p.controldata()
        self.assertEquals(len(data), 50)
        self.assertEquals(data['Database cluster state'], 'shut down in recovery')
        self.assertEquals(data['wal_log_hints setting'], 'on')
        self.assertEquals(int(data['Database block size']), 8192)

        subprocess.check_output = check_output_call_error
        data = self.p.controldata()
        self.assertEquals(data, dict())

        subprocess.check_output = check_output_generic_exception
        self.assertRaises(Exception, self.p.controldata())

    def test_read_postmaster_opts(self):
        m = mock_open(read_data=postmaster_opts_string())
        with patch.object(builtins, 'open', m):
            data = self.p.read_postmaster_opts()
            self.assertEquals(data['wal_level'], 'hot_standby')
            self.assertEquals(int(data['max_replication_slots']), 5)
            self.assertEqual(data.get('D'), None)

            m.side_effect = IOError("foo")
            data = self.p.read_postmaster_opts()
            self.assertEqual(data, dict())

            m.side_effect = Exception("foo")
            self.assertRaises(Exception, self.p.read_postmaster_opts())

    @patch('subprocess.Popen')
    @patch.object(builtins, 'open', MagicMock(return_value=42))
    def test_single_user_mode(self, subprocess_popen_mock):
        subprocess_popen_mock.return_value.wait.return_value = 0
        self.assertEquals(self.p.single_user_mode(options=dict(archive_mode='on', archive_command='false')), 0)
        subprocess_popen_mock.assert_called_once_with(['postgres', '--single', '-D', self.p.data_dir,
                                                      '-c', 'archive_command=false', '-c', 'archive_mode=on',
                                                       'postgres'], stdin=subprocess.PIPE,
                                                      stdout=42,
                                                      stderr=subprocess.STDOUT)
        subprocess_popen_mock.reset_mock()
        self.assertEquals(self.p.single_user_mode(command="CHECKPOINT"), 0)
        subprocess_popen_mock.assert_called_once_with(['postgres', '--single', '-D', self.p.data_dir,
                                                      'postgres'], stdin=subprocess.PIPE,
                                                      stdout=42,
                                                      stderr=subprocess.STDOUT)
        subprocess_popen_mock.return_value = None
        self.assertEquals(self.p.single_user_mode(), 1)

    def fake_listdir(path):
        if path.endswith(os.path.join('pg_xlog', 'archive_status')):
            return ["a", "b", "c"]
        return []

    @patch('os.listdir', MagicMock(side_effect=fake_listdir))
    @patch('os.path.isdir', MagicMock(return_value=True))
    @patch('os.unlink', return_value=True)
    @patch('os.remove', return_value=True)
    @patch('os.path.islink', return_value=False)
    @patch('os.path.isfile', return_value=True)
    def test_cleanup_archive_status(self, mock_file, mock_link, mock_remove, mock_unlink):
        ap = os.path.join(self.p.data_dir, 'pg_xlog', 'archive_status/')
        self.p.cleanup_archive_status()
        mock_remove.assert_has_calls([mock.call(ap+'a'), mock.call(ap+'b'), mock.call(ap+'c')])
        mock_unlink.assert_not_called()

        mock_remove.reset_mock()

        mock_file.return_value = False
        mock_link.return_value = True
        self.p.cleanup_archive_status()
        mock_unlink.assert_has_calls([mock.call(ap+'a'), mock.call(ap+'b'), mock.call(ap+'c')])
        mock_remove.assert_not_called()

        mock_unlink.reset_mock()
        mock_remove.reset_mock()

        mock_file.side_effect = Exception("foo")
        mock_link.side_effect = Exception("foo")
        self.p.cleanup_archive_status()
        mock_unlink.assert_not_called()
        mock_remove.assert_not_called()

    @patch('subprocess.check_output', MagicMock(return_value=0, side_effect=pg_controldata_string))
    def test_sysid(self):
        self.assertEqual(self.p.sysid, "6200971513092291716")

    @patch('os.path.isfile', MagicMock(return_value=True))
    @patch('shutil.copy', side_effect=Exception)
    def test_save_configuration_files(self, mock_copy):
        shutil.copy = mock_copy
        self.p.save_configuration_files()

    @patch('os.path.isfile', MagicMock(side_effect=is_file_raise_on_backup))
    @patch('shutil.copy', side_effect=Exception)
    def test_restore_configuration_files(self, mock_copy):
        shutil.copy = mock_copy
        self.p.restore_configuration_files()
