import mock  # for the mock.call method, importing it without a namespace breaks python3
import os
import psycopg2
import shutil
import subprocess
import unittest

from mock import Mock, MagicMock, PropertyMock, patch
from patroni.dcs import Cluster, Leader, Member
from patroni.exceptions import PostgresException, PostgresConnectionException
from patroni.postgresql import Postgresql
from patroni.utils import RetryFailedError
from six.moves import builtins
from test_ha import false


class MockCursor(object):

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
            self.results = [('', True, '', '', '', '', False)]
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

    def __iter__(self):
        for i in self.results:
            yield i

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class MockConnect(object):

    server_version = '99999'
    autocommit = False
    closed = 0

    def cursor(self):
        return MockCursor(self)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    @staticmethod
    def close():
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


def psycopg2_connect(*args, **kwargs):
    return MockConnect()


def fake_listdir(path):
    return ["a", "b", "c"] if path.endswith('pg_xlog/archive_status') else []


@patch('subprocess.call', Mock(return_value=0))
@patch('psycopg2.connect', psycopg2_connect)
class TestPostgresql(unittest.TestCase):

    @patch('subprocess.call', Mock(return_value=0))
    @patch('psycopg2.connect', psycopg2_connect)
    @patch('os.rename', Mock())
    def setUp(self):
        self.data_dir = 'data/test0'
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
        self.p = Postgresql({'name': 'test0', 'scope': 'batman', 'data_dir': self.data_dir,
                             'listen': '127.0.0.1, *:5432', 'connect_address': '127.0.0.2:5432',
                             'pg_hba': ['host replication replicator 127.0.0.1/32 md5',
                                        'hostssl all all 0.0.0.0/0 md5',
                                        'host all all 0.0.0.0/0 md5'],
                             'superuser': {'username': 'test', 'password': 'test'},
                             'admin': {'username': 'admin', 'password': 'admin'},
                             'pg_rewind': {'username': 'admin', 'password': 'admin'},
                             'replication': {'username': 'replicator',
                                             'password': 'rep-pass'},
                             'parameters': {'foo': 'bar'}, 'recovery_conf': {'foo': 'bar'},
                             'callbacks': {'on_start': 'true', 'on_stop': 'true',
                                           'on_restart': 'true', 'on_role_change': 'true',
                                           'on_reload': 'true'
                                           },
                             'restore': 'true'})
        self.leadermem = Member(0, 'leader', 28, {'conn_url': 'postgres://replicator:rep-pass@127.0.0.1:5435/postgres'})
        self.leader = Leader(-1, 28, self.leadermem)
        self.other = Member(0, 'test1', 28, {'conn_url': 'postgres://replicator:rep-pass@127.0.0.1:5433/postgres',
                            'tags': {'replicatefrom': 'leader'}})
        self.me = Member(0, 'test0', 28, {'conn_url': 'postgres://replicator:rep-pass@127.0.0.1:5434/postgres'})

    def tearDown(self):
        shutil.rmtree('data')

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

        with open(os.path.join(self.data_dir, 'pg_hba.conf')) as f:
            lines = f.readlines()
            assert 'host replication replicator 127.0.0.1/32 md5\n' in lines
            assert 'host all all 0.0.0.0/0 md5\n' in lines

    @patch('os.path.exists', Mock(return_value=True))
    @patch('os.unlink', Mock())
    def test_delete_trigger_file(self):
        self.p.delete_trigger_file()

    def test_start(self):
        self.assertTrue(self.p.start())
        self.p.is_running = false
        open(os.path.join(self.data_dir, 'postmaster.pid'), 'w').close()
        pg_conf = os.path.join(self.data_dir, 'postgresql.conf')
        open(pg_conf, 'w').close()
        self.assertTrue(self.p.start())
        with open(pg_conf) as f:
            lines = f.readlines()
            self.assertTrue("foo = 'bar'\n" in lines)

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

    @patch('subprocess.call', side_effect=OSError)
    @patch('patroni.postgresql.Postgresql.write_pgpass', MagicMock(return_value=dict()))
    def test_pg_rewind(self, mock_call):
        self.assertTrue(self.p.rewind(self.leader))
        subprocess.call = mock_call
        self.assertFalse(self.p.rewind(self.leader))

    @patch('patroni.postgresql.Postgresql.rewind', return_value=False)
    @patch('patroni.postgresql.Postgresql.remove_data_directory', MagicMock(return_value=True))
    @patch('patroni.postgresql.Postgresql.single_user_mode', MagicMock(return_value=1))
    @patch('patroni.postgresql.Postgresql.write_pgpass', MagicMock(return_value=dict()))
    @patch('subprocess.check_output', Mock(return_value=0, side_effect=pg_controldata_string))
    def test_follow(self, mock_pg_rewind):
        self.p.follow(None)
        self.p.follow(self.leader)
        self.p.follow(Leader(-1, 28, self.other))
        self.p.rewind = mock_pg_rewind
        self.p.follow(self.leader)
        with mock.patch('os.path.islink', MagicMock(return_value=True)):
            with mock.patch('patroni.postgresql.Postgresql.can_rewind', new_callable=PropertyMock(return_value=True)):
                with mock.patch('os.unlink', MagicMock(return_value=True)):
                    self.p.follow(self.leader, recovery=True)
        with mock.patch('patroni.postgresql.Postgresql.can_rewind', new_callable=PropertyMock(return_value=True)):
            self.p.rewind.return_value = True
            self.p.follow(self.leader, recovery=True)
            self.p.rewind.return_value = False
            self.p.follow(self.leader, recovery=True)
        with mock.patch('patroni.postgresql.Postgresql.check_recovery_conf', MagicMock(return_value=True)):
            self.assertTrue(self.p.follow(None))

    @patch('subprocess.check_output', Mock(return_value=0, side_effect=pg_controldata_string))
    def test_can_rewind(self):
        tmp = self.p.pg_rewind
        self.p.pg_rewind = None
        self.assertFalse(self.p.can_rewind)
        self.p.pg_rewind = tmp
        with mock.patch('subprocess.call', MagicMock(return_value=1)):
            self.assertFalse(self.p.can_rewind)
        with mock.patch('subprocess.call', side_effect=OSError):
            self.assertFalse(self.p.can_rewind)
        tmp = self.p.controldata
        self.p.controldata = lambda: {'wal_log_hints setting': 'on'}
        self.assertTrue(self.p.can_rewind)
        self.p.controldata = tmp

    @patch('time.sleep', Mock())
    def test_create_replica(self):
        self.p.delete_trigger_file = Mock(side_effect=OSError)
        with patch('subprocess.call', Mock(side_effect=[1, 0])):
            self.assertEquals(self.p.create_replica(self.leader), 0)
        with patch('subprocess.call', Mock(side_effect=[Exception(), 0])):
            self.assertEquals(self.p.create_replica(self.leader), 0)

        self.p.config['create_replica_method'] = ['wale', 'basebackup']
        self.p.config['wale'] = {'command': 'foo'}
        with patch('subprocess.call', Mock(return_value=0)):
            self.assertEquals(self.p.create_replica(self.leader), 0)
            del self.p.config['wale']
            self.assertEquals(self.p.create_replica(self.leader), 0)

        with patch('subprocess.call', Mock(side_effect=Exception("foo"))):
            self.assertEquals(self.p.create_replica(self.leader), 1)

    def test_sync_replication_slots(self):
        self.p.start()
        cluster = Cluster(True, self.leader, 0, [self.me, self.other, self.leadermem], None)
        self.p.sync_replication_slots(cluster)
        self.p.query = Mock(side_effect=psycopg2.OperationalError)
        self.p.schedule_load_slots = True
        self.p.sync_replication_slots(cluster)
        self.p.schedule_load_slots = False
        with mock.patch('patroni.postgresql.Postgresql.role', new_callable=PropertyMock(return_value='replica')):
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

    @patch('subprocess.Popen', Mock(side_effect=OSError))
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
        with patch('os.rename', Mock(side_effect=OSError)):
            self.p.move_data_directory()

    def test_bootstrap(self):
        with patch('subprocess.call', Mock(return_value=1)):
            self.assertRaises(PostgresException, self.p.bootstrap)
        self.p.bootstrap()

    @patch('patroni.postgresql.Postgresql.create_replica', Mock(return_value=0))
    def test_clone(self):
        self.p.clone(self.leader)

    @patch('os.listdir', Mock(return_value=['recovery.conf']))
    @patch('os.path.exists', Mock(return_value=True))
    def test_get_postgres_role_from_data_directory(self):
        self.assertEquals(self.p.get_postgres_role_from_data_directory(), 'replica')

    def test_remove_data_directory(self):
        self.p.remove_data_directory()
        open(self.data_dir, 'w').close()
        self.p.remove_data_directory()
        os.symlink('unexisting', self.data_dir)
        with patch('os.unlink', Mock(side_effect=OSError)):
            self.p.remove_data_directory()
        self.p.remove_data_directory()

    def test_controldata(self):
        with patch('subprocess.check_output', Mock(return_value=0, side_effect=pg_controldata_string)):
            data = self.p.controldata()
            self.assertEquals(len(data), 50)
            self.assertEquals(data['Database cluster state'], 'shut down in recovery')
            self.assertEquals(data['wal_log_hints setting'], 'on')
            self.assertEquals(int(data['Database block size']), 8192)

        with patch('subprocess.check_output', Mock(side_effect=subprocess.CalledProcessError(1, ''))):
            self.assertEquals(self.p.controldata(), {})

    @patch('subprocess.Popen')
    @patch.object(builtins, 'open', MagicMock(return_value=42))
    def test_single_user_mode(self, subprocess_popen_mock):
        subprocess_popen_mock.return_value.wait.return_value = 0
        self.assertEquals(self.p.single_user_mode(options=dict(archive_mode='on', archive_command='false')), 0)
        subprocess_popen_mock.assert_called_once_with(['postgres', '--single', '-D', self.data_dir,
                                                      '-c', 'archive_command=false', '-c', 'archive_mode=on',
                                                       'postgres'], stdin=subprocess.PIPE,
                                                      stdout=42,
                                                      stderr=subprocess.STDOUT)
        subprocess_popen_mock.reset_mock()
        self.assertEquals(self.p.single_user_mode(command="CHECKPOINT"), 0)
        subprocess_popen_mock.assert_called_once_with(['postgres', '--single', '-D', self.data_dir,
                                                      'postgres'], stdin=subprocess.PIPE,
                                                      stdout=42,
                                                      stderr=subprocess.STDOUT)
        subprocess_popen_mock.return_value = None
        self.assertEquals(self.p.single_user_mode(), 1)

    @patch('os.listdir', MagicMock(side_effect=fake_listdir))
    @patch('os.unlink', return_value=True)
    @patch('os.remove', return_value=True)
    @patch('os.path.islink', return_value=False)
    @patch('os.path.isfile', return_value=True)
    def test_cleanup_archive_status(self, mock_file, mock_link, mock_remove, mock_unlink):
        ap = os.path.join(self.data_dir, 'pg_xlog', 'archive_status/')
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

        mock_file.side_effect = OSError
        mock_link.side_effect = OSError
        self.p.cleanup_archive_status()
        mock_unlink.assert_not_called()
        mock_remove.assert_not_called()

    @patch('subprocess.check_output', MagicMock(return_value=0, side_effect=pg_controldata_string))
    def test_sysid(self):
        self.assertEqual(self.p.sysid, "6200971513092291716")

    @patch('os.path.isfile', Mock(return_value=True))
    @patch('shutil.copy', Mock(side_effect=IOError))
    def test_save_configuration_files(self):
        self.p.save_configuration_files()

    @patch('os.path.isfile', Mock(side_effect=[False, True]))
    @patch('shutil.copy', Mock(side_effect=IOError))
    def test_restore_configuration_files(self):
        self.p.restore_configuration_files()

    def test_can_create_replica_without_replication_connection(self):
        self.p.config['create_replica_method'] = []
        self.assertFalse(self.p.can_create_replica_without_replication_connection())
        self.p.config['create_replica_method'] = ['wale', 'basebackup']
        self.p.config['wale'] = {'command': 'foo', 'no_master': 1}
        self.assertTrue(self.p.can_create_replica_without_replication_connection())

    def test_replica_method_can_work_without_replication_connection(self):
        self.assertFalse(self.p.replica_method_can_work_without_replication_connection('basebackup'))
        self.assertFalse(self.p.replica_method_can_work_without_replication_connection('foobar'))
        self.p.config['foo'] = {'command': 'bar', 'no_master': 1}
        self.assertTrue(self.p.replica_method_can_work_without_replication_connection('foo'))
        self.p.config['foo'] = {'command': 'bar'}
        self.assertFalse(self.p.replica_method_can_work_without_replication_connection('foo'))
