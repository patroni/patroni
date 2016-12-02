import psycopg2
import subprocess
import unittest

from mock import Mock, MagicMock, patch, PropertyMock, mock_open
from patroni.scripts.wale_restore import WALERestore, main as _main, get_major_version
from six.moves import builtins


wale_output = b'name last_modified expanded_size_bytes wal_segment_backup_start ' +\
              b'wal_segment_offset_backup_start wal_segment_backup_stop wal_segment_offset_backup_stop\n' +\
              b'base_00000001000000000000007F_00000040 2015-05-18T10:13:25.000Z 167772160 ' +\
              b'00000001000000000000007F 00000040 00000001000000000000007F 00000240\n'


@patch('os.access', Mock(return_value=True))
@patch('os.makedirs', Mock(return_value=True))
@patch('os.path.exists', Mock(return_value=True))
@patch('os.path.isdir', Mock(return_value=True))
@patch('psycopg2.extensions.cursor', Mock(autospec=True))
@patch('psycopg2.extensions.connection', Mock(autospec=True))
@patch('psycopg2.connect', MagicMock(autospec=True))
@patch('subprocess.check_output', Mock(return_value=wale_output))
class TestWALERestore(unittest.TestCase):

    def setUp(self):
        self.wale_restore = WALERestore("batman", "/data", "host=batman port=5432 user=batman", "/etc", 100, 100, 1, 0)

    def test_should_use_s3_to_create_replica(self):
        with patch('psycopg2.connect', Mock(side_effect=psycopg2.Error("foo"))):
            self.assertFalse(self.wale_restore.should_use_s3_to_create_replica())
        with patch('subprocess.check_output', Mock(side_effect=subprocess.CalledProcessError(1, "cmd", "foo"))):
            self.assertFalse(self.wale_restore.should_use_s3_to_create_replica())
        with patch('subprocess.check_output', Mock(return_value=wale_output.split(b'\n')[0])):
            self.assertFalse(self.wale_restore.should_use_s3_to_create_replica())
        with patch('subprocess.check_output',
                   Mock(return_value=wale_output.replace(b' wal_segment_offset_backup_stop', b''))):
            self.assertFalse(self.wale_restore.should_use_s3_to_create_replica())
        with patch('subprocess.check_output',
                   Mock(return_value=wale_output.replace(b'expanded_size_bytes', b'expanded_size_foo'))):
            self.assertFalse(self.wale_restore.should_use_s3_to_create_replica())

        self.wale_restore.should_use_s3_to_create_replica()
        self.wale_restore.no_master = 1
        self.assertTrue(self.wale_restore.should_use_s3_to_create_replica())

    def test_create_replica_with_s3(self):
        with patch('subprocess.call', Mock(return_value=0)):
            self.assertEqual(self.wale_restore.create_replica_with_s3(), 0)
            with patch.object(self.wale_restore, 'fix_subdirectory_path_if_broken', Mock(return_value=False)):
                self.assertEqual(self.wale_restore.create_replica_with_s3(), 2)

        with patch('subprocess.call', Mock(side_effect=Exception("foo"))):
            self.assertEqual(self.wale_restore.create_replica_with_s3(), 1)

    def test_run(self):
        with patch.object(self.wale_restore, 'init_error', PropertyMock(return_value=True)):
            self.assertEqual(self.wale_restore.run(), 2)
        with patch.object(self.wale_restore, 'should_use_s3_to_create_replica', Mock(return_value=True)):
            with patch.object(self.wale_restore, 'create_replica_with_s3', Mock(return_value=0)):
                self.assertEqual(self.wale_restore.run(), 0)

    @patch('sys.exit', Mock())
    @patch.object(WALERestore, 'run', Mock(return_value=0))
    def test_main(self):
        self.assertEqual(_main(), None)

    @patch('os.path.isfile', Mock(return_value=True))
    def test_get_major_version(self):
        with patch.object(builtins, 'open', mock_open(read_data='9.4')):
            self.assertEqual(get_major_version("data"), 9.4)
        with patch.object(builtins, 'open', side_effect=OSError):
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
