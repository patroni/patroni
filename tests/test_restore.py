import unittest
from mock import MagicMock, patch
import os
from patroni.scripts.restore import Restore, WALERestore


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


class TestRestore(unittest.TestCase):

    def setUp(self):
        self.restore = Restore("batman", "master", "/data", "host=batman port=5432 user=batman")
        pass

    def tearDown(self):
        pass

    def test_parse_connstring(self):
        self.assertDictEqual(self.restore.master_connection, {'host': 'batman', 'port': '5432', 'user': 'batman'})

    @patch('subprocess.call', MagicMock(return_value=0))
    def test_run(self):
        ret = self.restore.run()
        self.assertEqual(ret, 0)

    @patch('subprocess.call', MagicMock(return_value=1))
    def test_run_fail(self):
        ret = self.restore.run()
        self.assertEqual(ret, 1)


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
        env = {}
        env['WAL_S3_BUCKET'] = 'batman'
        env['WALE_BACKUP_THRESHOLD_PERCENTAGE'] = 100
        env['WALE_BACKUP_THRESHOLD_MEGABYTES'] = 100
        self.wale_restore = WALERestore("batman", "master", "/data", "host=batman port=5432 user=batman", env=env)

    def tearDown(self):
        pass

    def test_setup(self):
        self.wale_restore.setup()
        self.assertFalse(self.wale_restore.init_error)

    # have to redefine the class-level os.access mock inside the function
    # since the class-level mock will be applied after the function level one.
    @patch('os.access', return_value=False)
    def test_setup_fail(self, mock_no_access):
        os.access = mock_no_access
        self.wale_restore.setup()
        self.assertTrue(self.wale_restore.init_error)

    # The 3 tests above only differ with the mock function instead of a subprocess call
    # in the first one, subprocess call should return success only for wal-e command,
    # checking the primary use-case of restoring from WAL-E backup.
    # In the second one, we test fallbacks by failing at WAL-E, but succeeding at
    # pg_basebackup.
    # Finally, the last use case is when all subprocess.call fails. resulting in a
    # failure to restore from replica
    @patch('subprocess.call',
           MagicMock(side_effect=lambda *args, **kwargs: 0 if 'wal-e' in args[0] else 1))
    def test_run(self):
        self.wale_restore.setup()
        ret = self.wale_restore.run()
        self.assertEqual(ret, 0)

    @patch('subprocess.call',
           MagicMock(side_effect=lambda *args, **kwargs: 0 if 'pg_basebackup' in args[0] else 1))
    def test_run_fallback(self):
        self.wale_restore.setup()
        ret = self.wale_restore.run()
        self.assertEqual(ret, 0)

    @patch('subprocess.call', MagicMock(return_value=1))
    def test_run_all_fail(self):
        self.wale_restore.setup()
        ret = self.wale_restore.run()
        self.assertEqual(ret, 1)
