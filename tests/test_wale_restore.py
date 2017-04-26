import psycopg2
import subprocess
import unittest
import pytest
import time

from mock import Mock, MagicMock, patch, mock_open
from patroni.scripts import wale_restore
from patroni.scripts.wale_restore import WALERestore, main as _main, \
    get_major_version, ExitCode
from six.moves import builtins


wale_output_header = (
    b'name\tlast_modified\t'
    b'expanded_size_bytes\t'
    b'wal_segment_backup_start\twal_segment_offset_backup_start\t'
    b'wal_segment_backup_stop\twal_segment_offset_backup_stop\n'
)

wale_output_values =  (
    b'base_00000001000000000000007F_00000040\t2015-05-18T10:13:25.000Z\t'
    b'167772160\t'
    b'00000001000000000000007F\t00000040\t'
    b'00000001000000000000007F\t00000240\n'
)

wale_output = wale_output_header + wale_output_values

wale_restore.RETRY_SLEEP_INTERVAL = 0.1  # Speed up retries
WALE_TEST_RETRIES = 2


def make_wale_restore():
    return WALERestore(
        scope="batman",
        datadir="/data",
        connstring="host=batman port=5432 user=batman",
        env_dir="/etc",
        threshold_mb=100,
        threshold_pct=100,
        use_iam=1,
        no_master=0,
        retries=WALE_TEST_RETRIES,
    )


@pytest.fixture(params=[
        # Nn space
        wale_output,
        # Space
        wale_output.replace(
            b'\t2015-05-18T10:13:25.000Z',
            b'\t2015-05-18 10:13:25.000Z'),
    ])
def fx_wale_spaces(request):
    return request.param


@pytest.fixture()
def fx_wale_restore(request):
    patches = [
        patch('psycopg2.extensions.cursor', Mock(autospec=True)),
        patch('psycopg2.extensions.connection', Mock(autospec=True)),
        patch('psycopg2.connect', MagicMock(autospec=True)),
    ]
    for patch_ in patches:
        patch_.start()

    def _finalize():
        for patch_ in patches:
            patch_.stop()

    request.addfinalizer(_finalize)

    return make_wale_restore()


@pytest.mark.parametrize('exit_code_int,exit_code', [
    (0, ExitCode.SUCCESS),
    (1, ExitCode.RETRY_LATER),
    (2, ExitCode.FAIL),
])
def test_exit_code_enum_members_are_int_compatible(exit_code_int, exit_code):
    assert exit_code_int == exit_code


@pytest.mark.parametrize('mock,exit_code', [
    (Mock(return_value=True), ExitCode.SUCCESS),
    (Mock(return_value=False), ExitCode.FAIL),
    (Mock(return_value=None), ExitCode.RETRY_LATER),  # Handled exception
    (Mock(side_effect=Exception('Unhandled exception')), ExitCode.FAIL)
])
def test_run_exit_codes_by_should_use_s3(mock, exit_code, fx_wale_restore):
    """
    Verify that WALERestore.run() returns the correct values based on the 
    results of WALERestore.should_use_s3t_to_create_replica().
    """
    with patch.object(fx_wale_restore, 'should_use_s3_to_create_replica',
                      mock),\
            patch.object(fx_wale_restore, 'create_replica_with_s3',
                         Mock(return_value=ExitCode.SUCCESS)):
        assert fx_wale_restore.run() == exit_code


def test_should_use_s3_too_many_rows(fx_wale_restore):
    with patch('subprocess.check_output',
               Mock(return_value=wale_output_header +
                       wale_output_values +
                       wale_output_values)):
        assert not fx_wale_restore.should_use_s3_to_create_replica()


def test_should_use_s3_handles_space_in_date(fx_wale_restore, fx_wale_spaces):
    with patch('subprocess.check_output',
               Mock(return_value=fx_wale_spaces)):

        assert fx_wale_restore.should_use_s3_to_create_replica()


def test_should_use_s3_missing_unused_field(fx_wale_restore):
    with patch('subprocess.check_output',
               Mock(return_value=wale_output.replace(b'\twal_segment_offset_backup_stop', b''))):
        assert fx_wale_restore.should_use_s3_to_create_replica()


def test_should_use_s3_missing_used_field(fx_wale_restore):
    with patch('subprocess.check_output',
               Mock(return_value=wale_output.replace(b'expanded_size_bytes', b'expanded_size_foo'))):
        assert fx_wale_restore.should_use_s3_to_create_replica() is None


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
        self.wale_restore = make_wale_restore()

    def test_should_use_s3_to_create_replica(self):
        self.assertTrue(self.wale_restore.should_use_s3_to_create_replica())

        with patch('psycopg2.connect', Mock(side_effect=psycopg2.Error("foo"))):
            save_no_master = self.wale_restore.no_master
            save_master_connection = self.wale_restore.master_connection

            self.assertFalse(self.wale_restore.should_use_s3_to_create_replica())

            with patch('time.sleep', Mock(return_value=None)) as mock_sleep:
                self.wale_restore.no_master = 1
                assert self.wale_restore.should_use_s3_to_create_replica()
                # verify retries
                mock_sleep.assert_has_calls(
                    [((wale_restore.RETRY_SLEEP_INTERVAL,),)] * WALE_TEST_RETRIES
                )

            self.wale_restore.master_connection = ''
            self.assertTrue(self.wale_restore.should_use_s3_to_create_replica())

            self.wale_restore.no_master = save_no_master
            self.wale_restore.master_connection = save_master_connection

        with patch('subprocess.check_output', Mock(side_effect=subprocess.CalledProcessError(1, "cmd", "foo"))):
            self.assertFalse(self.wale_restore.should_use_s3_to_create_replica())
        with patch('subprocess.check_output', Mock(return_value=wale_output.split(b'\n')[0])):
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
            with patch.object(self.wale_restore, 'should_use_s3_to_create_replica', Mock(return_value=None)):
                self.assertEqual(self.wale_restore.run(), 1)
            with patch.object(self.wale_restore, 'should_use_s3_to_create_replica', Mock(side_effect=Exception)):
                self.assertEqual(self.wale_restore.run(), 2)

    @patch('sys.exit', Mock())
    def test_main(self):
        with patch.object(WALERestore, 'run', Mock(return_value=0)):
            self.assertEqual(_main(), 0)

        with patch.object(WALERestore, 'run', Mock(return_value=1)), \
             patch('time.sleep', Mock(return_value=None)) as mock_sleep:
            self.assertEqual(_main(), 1)
            assert mock_sleep.call_count == WALE_TEST_RETRIES

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
