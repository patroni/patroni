import unittest
from mock import MagicMock, patch, PropertyMock
import os
import psycopg2
import subprocess
from patroni.scripts.wale_restore import WALERestore


def fake_cursor_fetchone(*args, **kwargs):
    return ('16777216',)


def fake_call_fail_for_wal_e(*args, **kwargs):
    if len(args) > 0 and 'backup-fetch' in args[0]:
        return 1
    return 0


def fake_call_fail_for_base_backup(*args, **kwargs):
    if len(args) > 0 and 'backup-fetch' in args[0]:
        return 0
    return 1


def fake_backup_data(self, *args, **kwargs):
    """ return the fake result of WAL-E backup-list"""
    return """name    last_modified   expanded_size_bytes wal_segment_backup_start    wal_segment_offset_backup_start wal_segment_backup_stop wal_segment_offset_backup_stop
base_00000001000000000000007F_00000040  2015-05-18T10:13:25.000Z 167772160   00000001000000000000007F    00000040 00000001000000000000007F    00000240
"""

def fake_backup_data_2(self, *args, **kwargs):
    """ return the fake result of WAL-E backup-list"""
    return """name    last_modified   expanded_size_bytes wal_segment_backup_start    wal_segment_offset_backup_start wal_segment_backup_stop wal_segment_offset_backup_stop """

def fake_backup_data_3(self, *args, **kwargs):
    """ return the fake result of WAL-E backup-list"""
    return """name    last_modified   expanded_size_bytes wal_segment_backup_start    wal_segment_offset_backup_start wal_segment_backup_stop
base_00000001000000000000007F_00000040  2015-05-18T10:13:25.000Z 167772160   00000001000000000000007F    00000040 00000001000000000000007F    00000240
"""

def fake_backup_data_4(self, *args, **kwargs):
    """ return the fake result of WAL-E backup-list"""
    return """name    last_modified   expanded_size_foo wal_segment_backup_start    wal_segment_offset_backup_start wal_segment_backup_stop wal_segment_offset_backup_stop
base_00000001000000000000007F_00000040  2015-05-18T10:13:25.000Z 167772160   00000001000000000000007F    00000040 00000001000000000000007F    00000240
"""


@patch('os.access', MagicMock(return_value=True))
@patch('os.makedirs', MagicMock(return_value=True))
@patch('os.path.exists', MagicMock(return_value=True))
@patch('os.path.isdir', MagicMock(return_value=True))
@patch('psycopg2.extensions.cursor.fetchone', MagicMock(side_effect=fake_cursor_fetchone))
@patch('psycopg2.extensions.cursor', MagicMock(autospec=True))
@patch('psycopg2.extensions.connection', MagicMock(autospec=True))
@patch('psycopg2.connect', MagicMock(autospec=True))
@patch('subprocess.check_output', MagicMock(side_effect=fake_backup_data))
class TestWALERestore(unittest.TestCase):

    def setUp(self):
        self.wale_restore = WALERestore("batman", "/data",
                                        "host=batman port=5432 user=batman", "/etc", 100, 100, 1)

    def tearDown(self):
        pass

    def test_should_use_s3_to_create_replica(self):
        with patch('psycopg2.connect', MagicMock(side_effect=psycopg2.Error("foo"))):
            self.assertFalse(self.wale_restore.should_use_s3_to_create_replica())
        with patch('subprocess.check_output', MagicMock(side_effect=subprocess.CalledProcessError(1, "cmd", "foo"))):
            self.assertFalse(self.wale_restore.should_use_s3_to_create_replica())
        with patch('subprocess.check_output', MagicMock(side_effect=fake_backup_data_2)):
            self.assertFalse(self.wale_restore.should_use_s3_to_create_replica())
        with patch('subprocess.check_output', MagicMock(side_effect=fake_backup_data_3)):
            self.assertFalse(self.wale_restore.should_use_s3_to_create_replica())
        with patch('subprocess.check_output', MagicMock(side_effect=fake_backup_data_4)):
            self.assertFalse(self.wale_restore.should_use_s3_to_create_replica())

        self.wale_restore.should_use_s3_to_create_replica()

    def test_create_replica_with_s3(self):
        with patch('subprocess.call', MagicMock(return_value=0)):
            self.assertEqual(self.wale_restore.create_replica_with_s3(), 0)
        with patch('subprocess.call', MagicMock(side_effect=Exception("foo"))):
            self.assertEqual(self.wale_restore.create_replica_with_s3(), 1)

    def test_run(self):
        with patch.object(self.wale_restore, 'init_error', PropertyMock(return_value=True)):
            self.assertEqual(self.wale_restore.run(), 2)
        with patch.object(self.wale_restore, 'should_use_s3_to_create_replica', MagicMock(return_value=True)):
            with patch.object(self.wale_restore, 'create_replica_with_s3', MagicMock(return_value=0)):
                self.assertEqual(self.wale_restore.run(), 0)
