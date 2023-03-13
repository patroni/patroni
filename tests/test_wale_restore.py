import subprocess
import unittest

import patroni.psycopg as psycopg

from mock import Mock, PropertyMock, patch, mock_open
from patroni.scripts import wale_restore
from patroni.scripts.wale_restore import WALERestore, main as _main, get_major_version
from threading import current_thread

from . import MockConnect, psycopg_connect

wale_output_header = (
    b'name\tlast_modified\t'
    b'expanded_size_bytes\t'
    b'wal_segment_backup_start\twal_segment_offset_backup_start\t'
    b'wal_segment_backup_stop\twal_segment_offset_backup_stop\n'
)

wale_output_values = (
    b'base_00000001000000000000007F_00000040\t2015-05-18T10:13:25.000Z\t'
    b'167772160\t'
    b'00000001000000000000007F\t00000040\t'
    b'00000001000000000000007F\t00000240\n'
)

wale_output = wale_output_header + wale_output_values

wale_restore.RETRY_SLEEP_INTERVAL = 0.001  # Speed up retries
WALE_TEST_RETRIES = 2


@patch('os.access', Mock(return_value=True))
@patch('os.makedirs', Mock(return_value=True))
@patch('os.path.exists', Mock(return_value=True))
@patch('os.path.isdir', Mock(return_value=True))
@patch('patroni.psycopg.connect', psycopg_connect)
@patch('subprocess.check_output', Mock(return_value=wale_output))
class TestWALERestore(unittest.TestCase):

    def setUp(self):
        self.wale_restore = WALERestore('batman', '/data', 'host=batman port=5432 user=batman',
                                        '/etc', 100, 100, 1, 0, WALE_TEST_RETRIES)

    def test_should_use_s3_to_create_replica(self):
        self.__thread_ident = current_thread().ident
        sleeps = [0]

        def mock_sleep(*args):
            if current_thread().ident == self.__thread_ident:
                sleeps[0] += 1

        self.assertTrue(self.wale_restore.should_use_s3_to_create_replica())
        with patch.object(MockConnect, 'server_version', PropertyMock(return_value=100000)):
            self.assertTrue(self.wale_restore.should_use_s3_to_create_replica())

        with patch('subprocess.check_output', Mock(return_value=wale_output.replace(b'167772160', b'1'))):
            self.assertFalse(self.wale_restore.should_use_s3_to_create_replica())

        with patch('patroni.psycopg.connect', Mock(side_effect=psycopg.Error("foo"))):
            save_no_leader = self.wale_restore.no_leader
            save_leader_connection = self.wale_restore.leader_connection

            self.assertFalse(self.wale_restore.should_use_s3_to_create_replica())

            with patch('time.sleep', mock_sleep):
                self.wale_restore.no_leader = 1
                self.assertTrue(self.wale_restore.should_use_s3_to_create_replica())
                # verify retries
                self.assertEqual(sleeps[0], WALE_TEST_RETRIES)

            self.wale_restore.leader_connection = ''
            self.assertTrue(self.wale_restore.should_use_s3_to_create_replica())

            self.wale_restore.no_leader = save_no_leader
            self.wale_restore.leader_connection = save_leader_connection

        with patch('subprocess.check_output', Mock(side_effect=subprocess.CalledProcessError(1, "cmd", "foo"))):
            self.assertFalse(self.wale_restore.should_use_s3_to_create_replica())
        with patch('subprocess.check_output', Mock(return_value=wale_output_header)):
            self.assertFalse(self.wale_restore.should_use_s3_to_create_replica())
        with patch('subprocess.check_output', Mock(return_value=wale_output + wale_output_values)):
            self.assertFalse(self.wale_restore.should_use_s3_to_create_replica())
        with patch('subprocess.check_output',
                   Mock(return_value=wale_output.replace(b'expanded_size_bytes', b'expanded_size_foo'))):
            self.assertFalse(self.wale_restore.should_use_s3_to_create_replica())

    def test_create_replica_with_s3(self):
        with patch('subprocess.call', Mock(return_value=0)):
            self.assertEqual(self.wale_restore.create_replica_with_s3(), 0)
            with patch.object(self.wale_restore, 'fix_subdirectory_path_if_broken', Mock(return_value=False)):
                self.assertEqual(self.wale_restore.create_replica_with_s3(), 2)

        with patch('subprocess.call', Mock(side_effect=Exception("foo"))):
            self.assertEqual(self.wale_restore.create_replica_with_s3(), 1)

    def test_run(self):
        self.wale_restore.init_error = True
        self.assertEqual(self.wale_restore.run(), 2)  # this would do 2 retries 1 sec each
        self.wale_restore.init_error = False
        with patch.object(self.wale_restore, 'should_use_s3_to_create_replica', Mock(return_value=True)):
            with patch.object(self.wale_restore, 'create_replica_with_s3', Mock(return_value=0)):
                self.assertEqual(self.wale_restore.run(), 0)
        with patch.object(self.wale_restore, 'should_use_s3_to_create_replica', Mock(return_value=False)):
            self.assertEqual(self.wale_restore.run(), 2)
        with patch.object(self.wale_restore, 'should_use_s3_to_create_replica', Mock(return_value=None)):
            self.assertEqual(self.wale_restore.run(), 1)
        with patch.object(self.wale_restore, 'should_use_s3_to_create_replica', Mock(side_effect=Exception)):
            self.assertEqual(self.wale_restore.run(), 2)

    @patch('sys.exit', Mock())
    def test_main(self):
        self.__thread_ident = current_thread().ident
        sleeps = [0]

        def mock_sleep(*args):
            if current_thread().ident == self.__thread_ident:
                sleeps[0] += 1

        with patch.object(WALERestore, 'run', Mock(return_value=0)):
            self.assertEqual(_main(), 0)

        with patch.object(WALERestore, 'run', Mock(return_value=1)), \
                patch('time.sleep', mock_sleep):
            self.assertEqual(_main(), 1)
            self.assertTrue(sleeps[0], WALE_TEST_RETRIES)

    @patch('os.path.isfile', Mock(return_value=True))
    def test_get_major_version(self):
        with patch('builtins.open', mock_open(read_data='9.4')):
            self.assertEqual(get_major_version("data"), 9.4)
        with patch('builtins.open', side_effect=OSError):
            self.assertEqual(get_major_version("data"), 0.0)

    @patch('os.path.islink', Mock(return_value=True))
    @patch('os.readlink', Mock(return_value="foo"))
    @patch('os.remove', Mock())
    @patch('os.mkdir', Mock())
    def test_fix_subdirectory_path_if_broken(self):
        with patch('os.path.exists', Mock(return_value=False)):  # overriding the class-wide mock
            self.assertTrue(self.wale_restore.fix_subdirectory_path_if_broken("data1"))
            for fn in ('os.remove', 'os.mkdir'):
                with patch(fn, side_effect=OSError):
                    self.assertFalse(self.wale_restore.fix_subdirectory_path_if_broken("data3"))
