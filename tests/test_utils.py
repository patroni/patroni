import sys
import unittest

from unittest.mock import Mock, patch

from patroni.exceptions import PatroniException
from patroni.utils import apply_keepalive_limit, enable_keepalive, get_major_version, get_postgres_version, \
    polling_loop, process_user_options, Retry, RetryFailedError, unquote, validate_directory


class TestUtils(unittest.TestCase):

    def test_polling_loop(self):
        self.assertEqual(list(polling_loop(0.001, interval=0.001)), [0])

    @patch('os.path.exists', Mock(return_value=True))
    @patch('os.path.isdir', Mock(return_value=True))
    @patch('tempfile.mkstemp', Mock(return_value=("", "")))
    @patch('os.remove', Mock(side_effect=Exception))
    def test_validate_directory_writable(self):
        self.assertRaises(Exception, validate_directory, "/tmp")

    @patch('os.path.exists', Mock(return_value=True))
    @patch('os.path.isdir', Mock(return_value=True))
    @patch('tempfile.mkstemp', Mock(side_effect=OSError))
    def test_validate_directory_not_writable(self):
        self.assertRaises(PatroniException, validate_directory, "/tmp")

    @patch('os.path.exists', Mock(return_value=False))
    @patch('os.makedirs', Mock(side_effect=OSError))
    def test_validate_directory_couldnt_create(self):
        self.assertRaises(PatroniException, validate_directory, "/tmp")

    @patch('os.path.exists', Mock(return_value=True))
    @patch('os.path.isdir', Mock(return_value=False))
    def test_validate_directory_is_not_a_directory(self):
        self.assertRaises(PatroniException, validate_directory, "/tmp")

    def test_enable_keepalive(self):
        with patch('socket.SIO_KEEPALIVE_VALS', 1, create=True):
            self.assertIsNone(enable_keepalive(Mock(), 10, 5))
        with patch('socket.SIO_KEEPALIVE_VALS', None, create=True):
            for platform in ('linux2', 'darwin', 'other'):
                with patch('sys.platform', platform):
                    self.assertIsNone(enable_keepalive(Mock(), 10, 5))

    def test_apply_keepalive_limit(self):
        for platform in ('linux2', 'darwin'):
            with patch('sys.platform', platform):
                self.assertLess(apply_keepalive_limit('TCP_KEEPIDLE', sys.maxsize), sys.maxsize)

    def test_unquote(self):
        self.assertEqual(unquote('value'), 'value')
        self.assertEqual(unquote('value with spaces'), "value with spaces")
        self.assertEqual(unquote(
            '"double quoted value"'),
            'double quoted value')
        self.assertEqual(unquote(
            '\'single quoted value\''),
            'single quoted value')
        self.assertEqual(unquote(
            'value "with" double quotes'),
            'value "with" double quotes')
        self.assertEqual(unquote(
            '"value starting with" double quotes'),
            '"value starting with" double quotes')
        self.assertEqual(unquote(
            '\'value starting with\' single quotes'),
            '\'value starting with\' single quotes')
        self.assertEqual(unquote(
            'value with a \' single quote'),
            'value with a \' single quote')
        self.assertEqual(unquote(
            '\'value with a \'"\'"\' single quote\''),
            'value with a \' single quote')

    def test_get_postgres_version(self):
        with patch('subprocess.check_output', Mock(return_value=b'postgres (PostgreSQL) 9.6.24\n')):
            self.assertEqual(get_postgres_version(), '9.6.24')
        with patch('subprocess.check_output',
                   Mock(return_value=b'postgres (PostgreSQL) 10.23 (Ubuntu 10.23-4.pgdg22.04+1)\n')):
            self.assertEqual(get_postgres_version(), '10.23')
        with patch('subprocess.check_output',
                   Mock(return_value=b'postgres (PostgreSQL) 17beta3 (Ubuntu 17~beta3-1.pgdg22.04+1)\n')):
            self.assertEqual(get_postgres_version(), '17.0')
        with patch('subprocess.check_output',
                   Mock(return_value=b'postgres (PostgreSQL) 9.6beta3\n')):
            self.assertEqual(get_postgres_version(), '9.6.0')
        with patch('subprocess.check_output', Mock(return_value=b'postgres (PostgreSQL) 9.6rc2\n')):
            self.assertEqual(get_postgres_version(), '9.6.0')
        # because why not
        with patch('subprocess.check_output', Mock(return_value=b'postgres (PostgreSQL) 10\n')):
            self.assertEqual(get_postgres_version(), '10.0')
        with patch('subprocess.check_output', Mock(return_value=b'postgres (PostgreSQL) 10wow, something new\n')):
            self.assertEqual(get_postgres_version(), '10.0')
        with patch('subprocess.check_output', Mock(side_effect=OSError)):
            self.assertRaises(PatroniException, get_postgres_version, 'postgres')

    def test_get_major_version(self):
        with patch('subprocess.check_output', Mock(return_value=b'postgres (PostgreSQL) 9.6.24\n')):
            self.assertEqual(get_major_version(), '9.6')
        with patch('subprocess.check_output',
                   Mock(return_value=b'postgres (PostgreSQL) 10.23 (Ubuntu 10.23-4.pgdg22.04+1)\n')):
            self.assertEqual(get_major_version(), '10')
        with patch('subprocess.check_output',
                   Mock(return_value=b'postgres (PostgreSQL) 17beta3 (Ubuntu 17~beta3-1.pgdg22.04+1)\n')):
            self.assertEqual(get_major_version(), '17')
        with patch('subprocess.check_output',
                   Mock(return_value=b'postgres (PostgreSQL) 9.6beta3\n')):
            self.assertEqual(get_major_version(), '9.6')
        with patch('subprocess.check_output', Mock(return_value=b'postgres (PostgreSQL) 9.6rc2\n')):
            self.assertEqual(get_major_version(), '9.6')
        with patch('subprocess.check_output', Mock(return_value=b'postgres (PostgreSQL) 10\n')):
            self.assertEqual(get_major_version(), '10')
        with patch('subprocess.check_output', Mock(side_effect=OSError)):
            self.assertRaises(PatroniException, get_major_version, 'postgres')

    def test_process_user_options(self):
        def error_handler(msg):
            raise Exception(msg)

        self.assertEqual(process_user_options('initdb', ['string'], (), error_handler), ['--string'])
        self.assertEqual(
            process_user_options(
                'initdb',
                [{'key': 'value'}],
                (), error_handler
            ),
            ['--key=value'])
        if sys.platform != 'win32':
            self.assertEqual(
                process_user_options(
                    'initdb',
                    [{'key': 'value with spaces'}],
                    (), error_handler
                ),
                ["--key=value with spaces"])
            self.assertEqual(
                process_user_options(
                    'initdb',
                    [{'key': "'value with spaces'"}],
                    (), error_handler
                ),
                ["--key=value with spaces"])
            self.assertEqual(
                process_user_options(
                    'initdb',
                    {'key': 'value with spaces'},
                    (), error_handler
                ),
                ["--key=value with spaces"])
            self.assertEqual(
                process_user_options(
                    'initdb',
                    {'key': "'value with spaces'"},
                    (), error_handler
                ),
                ["--key=value with spaces"])
            # not allowed options in list of dicts/strs are filtered out
            self.assertEqual(
                process_user_options(
                    'pg_basebackup',
                    [{'checkpoint': 'fast'}, {'dbname': 'dbname=postgres'}, 'gzip', {'label': 'standby'}, 'verbose'],
                    ('dbname', 'verbose'),
                    print
                ),
                ['--checkpoint=fast', '--gzip', '--label=standby'],
            )


@patch('time.sleep', Mock())
class TestRetrySleeper(unittest.TestCase):

    @staticmethod
    def _fail(times=1):
        scope = dict(times=0)

        def inner():
            if scope['times'] >= times:
                pass
            else:
                scope['times'] += 1
                raise PatroniException('Failed!')
        return inner

    def test_reset(self):
        retry = Retry(delay=0, max_tries=2)
        retry(self._fail())
        self.assertEqual(retry._attempts, 1)
        retry.reset()
        self.assertEqual(retry._attempts, 0)

    def test_too_many_tries(self):
        retry = Retry(delay=0)
        self.assertRaises(RetryFailedError, retry, self._fail(times=999))
        self.assertEqual(retry._attempts, 1)

    def test_maximum_delay(self):
        retry = Retry(delay=10, max_tries=100)
        retry(self._fail(times=10))
        self.assertTrue(retry._cur_delay < 4000, retry._cur_delay)
        # gevent's sleep function is picky about the type
        self.assertEqual(type(retry._cur_delay), float)

    def test_deadline(self):
        retry = Retry(deadline=0.0001)
        self.assertRaises(RetryFailedError, retry, self._fail(times=100))

    def test_copy(self):
        def _sleep(t):
            pass

        retry = Retry(sleep_func=_sleep)
        rcopy = retry.copy()
        self.assertTrue(rcopy.sleep_func is _sleep)
