import logging
from mock import MagicMock, Mock, call, patch
import ssl
import unittest
from urllib.error import HTTPError, URLError

from patroni.scripts.barman_recover import BarmanRecover, RetriesExceeded, main, set_up_logging


API_URL = "http://localhost:7480"
BARMAN_SERVER = "my_server"
BACKUP_ID = "backup_id"
SSH_COMMAND = "ssh postgres@localhost"
DATA_DIRECTORY = "/path/to/pgdata"
LOOP_WAIT = 10
RETRY_WAIT = 2
MAX_RETRIES = 5


class TestBarmanRecover(unittest.TestCase):

    def setUp(self):
        with patch.object(BarmanRecover, "_ensure_api_ok") as _:
            self.br = BarmanRecover(API_URL, BARMAN_SERVER, BACKUP_ID, SSH_COMMAND, DATA_DIRECTORY, LOOP_WAIT,
                                    RETRY_WAIT, MAX_RETRIES)

    def test__build_full_url(self):
        self.assertEqual(self.br._build_full_url("/some/path"), f"{API_URL}/some/path")

    @patch.object(ssl, "create_default_context")
    def test__create_ssl_context(self, mock_create_ctx):
        # without certs
        self.assertIsNone(self.br._create_ssl_context())
        mock_create_ctx.assert_not_called()

        # with certs
        self.br.cert_file = "/path/to/certificate.crt"
        self.br.key_file = "/path/to/certificate.key"
        ret = self.br._create_ssl_context()
        self.assertIsNotNone(ret)
        mock_create_ctx.assert_called_once_with(ssl.Purpose.SERVER_AUTH)
        ret.load_cert_chain.assert_called_once_with(self.br.cert_file, self.br.key_file)

    @patch("json.loads")
    def test__deserialize_response(self, mock_json_loads):
        mock_response = MagicMock()
        self.assertIsNotNone(self.br._deserialize_response(mock_response))
        mock_response.read.assert_called_once()
        mock_response.read.return_value.decode.assert_called_once_with("utf-8")
        mock_json_loads.assert_called_once_with(mock_response.read.return_value.decode.return_value)

    @patch("json.dumps")
    def test__serialize_request(self, mock_json_dumps):
        body = "some_body"
        ret = self.br._serialize_request(body)
        self.assertIsNotNone(ret)
        mock_json_dumps.assert_called_once_with(body)
        mock_json_dumps.return_value.encode.assert_called_once_with("utf-8")

    @patch.object(BarmanRecover, "_deserialize_response", Mock(return_value="test"))
    @patch("logging.critical")
    @patch.object(BarmanRecover, "_create_ssl_context")
    @patch("patroni.scripts.barman_recover.urlopen")
    def test__get_request(self, mock_urlopen, mock_create_ctx, mock_logging):
        # with no error
        mock_create_ctx.return_value = None
        self.assertEqual(self.br._get_request("/some/path"), "test")
        mock_urlopen.assert_called_once_with(f"{API_URL}/some/path", context=None)
        mock_create_ctx.assert_called_once()

        # with HTTPError
        http_error = HTTPError("url", 403, "Some error.", [], None)
        mock_urlopen.side_effect = http_error

        with self.assertRaises(SystemExit) as exc:
            self.assertIsNone(self.br._get_request("/some/path"))

        mock_logging.assert_called_once_with("An error occurred while performing an HTTP GET request: "
                                             f"{repr(http_error)}")
        self.assertEqual(exc.exception.code, 4)

        # with URLError
        mock_logging.reset_mock()
        url_error = URLError("Some error.")
        mock_urlopen.side_effect = url_error

        with self.assertRaises(SystemExit) as exc:
            self.assertIsNone(self.br._get_request("/some/path"))

        mock_logging.assert_called_once_with("An error occurred while performing an HTTP GET request: "
                                             f"{repr(url_error)}")
        self.assertEqual(exc.exception.code, 4)

        # with Exception
        mock_logging.reset_mock()
        mock_urlopen.side_effect = Exception("Some error.")

        with patch("sys.exit") as mock_sys:
            with self.assertRaises(Exception):
                self.assertIsNone(self.br._get_request("/some/path"))

            mock_logging.assert_not_called()
            mock_sys.assert_not_called()

    @patch.object(BarmanRecover, "_deserialize_response", Mock(return_value="test"))
    @patch("logging.critical")
    @patch.object(BarmanRecover, "_create_ssl_context")
    @patch("patroni.scripts.barman_recover.urlopen")
    @patch("patroni.scripts.barman_recover.Request")
    @patch.object(BarmanRecover, "_serialize_request")
    def test__post_request(self, mock_serialize, mock_request, mock_urlopen, mock_create_ctx, mock_logging):
        # with no error
        mock_create_ctx.return_value = None
        self.assertEqual(self.br._post_request("/some/path", "some body"), "test")
        mock_serialize.assert_called_once_with("some body")
        mock_request.assert_called_once_with(f"{API_URL}/some/path")
        mock_urlopen.assert_called_once_with(mock_request.return_value, mock_serialize.return_value,
                                             context=mock_create_ctx.return_value)

        # with HTTPError
        http_error = HTTPError("url", 403, "Some error.", [], None)
        mock_urlopen.side_effect = http_error

        with self.assertRaises(SystemExit) as exc:
            self.assertIsNone(self.br._post_request("/some/path", "some body"))

        mock_logging.assert_called_once_with("An error occurred while performing an HTTP POST request: "
                                             f"{repr(http_error)}")
        self.assertEqual(exc.exception.code, 4)

        # with URLError
        mock_logging.reset_mock()
        url_error = URLError("Some error.")
        mock_urlopen.side_effect = url_error

        with self.assertRaises(SystemExit) as exc:
            self.assertIsNone(self.br._post_request("/some/path", "some body"))

        mock_logging.assert_called_once_with("An error occurred while performing an HTTP POST request: "
                                             f"{repr(url_error)}")
        self.assertEqual(exc.exception.code, 4)

        # with Exception
        mock_logging.reset_mock()
        mock_urlopen.side_effect = Exception("Some error.")

        with patch("sys.exit") as mock_sys:
            with self.assertRaises(Exception):
                self.br._post_request("/some/path", "some body")

            mock_logging.assert_not_called()
            mock_sys.assert_not_called()

    @patch("logging.critical")
    @patch.object(BarmanRecover, "_get_request")
    def test__ensure_api_ok(self, mock_get_request, mock_logging):
        # API ok
        mock_get_request.return_value = "OK"

        with patch("sys.exit") as mock_sys:
            self.assertIsNone(self.br._ensure_api_ok())
            mock_logging.assert_not_called()
            mock_sys.assert_not_called()

        # API not ok
        mock_get_request.return_value = "random"

        with self.assertRaises(SystemExit) as exc:
            self.assertIsNone(self.br._ensure_api_ok())

        mock_logging.assert_called_once_with("pg-backup-api is not working: random")
        self.assertEqual(exc.exception.code, 2)

    @patch("logging.warning")
    @patch("time.sleep")
    @patch.object(BarmanRecover, "_post_request")
    def test__create_recovery_operation(self, mock_post_request, mock_sleep, mock_logging):
        # well formed response
        mock_post_request.return_value = {"operation_id": "some_id"}
        self.assertEqual(self.br._create_recovery_operation(), "some_id")
        mock_sleep.assert_not_called()
        mock_logging.assert_not_called()
        mock_post_request.assert_called_once_with(
            f"servers/{BARMAN_SERVER}/operations",
            {
                "type": "recovery",
                "backup_id": BACKUP_ID,
                "remote_ssh_command": SSH_COMMAND,
                "destination_directory": DATA_DIRECTORY,
            }
        )

        # malformed response
        mock_post_request.return_value = {"operation_idd": "some_id"}

        with self.assertRaises(RetriesExceeded) as exc:
            self.br._create_recovery_operation()

        self.assertEqual(str(exc.exception),
                         "Maximum number of retries exceeded for method BarmanRecover._create_recovery_operation.")

        self.assertEqual(mock_sleep.call_count, self.br.max_retries)
        mock_sleep.assert_has_calls([call(self.br.retry_wait)] * self.br.max_retries)

        self.assertEqual(mock_logging.call_count, self.br.max_retries)
        mock_logging.assert_has_calls([call(f"Attempt {i+1} of {self.br.max_retries} on method BarmanRecover."
                                            "_create_recovery_operation failed with KeyError('operation_id').")
                                       for i in range(self.br.max_retries)])

    @patch("logging.warning")
    @patch("time.sleep")
    @patch.object(BarmanRecover, "_get_request")
    def test__get_recovery_operation_status(self, mock_get_request, mock_sleep, mock_logging):
        # well formed response
        mock_get_request.return_value = {"status": "some status"}
        self.assertEqual(self.br._get_recovery_operation_status("some_id"), "some status")
        mock_get_request.assert_called_once_with(f"servers/{BARMAN_SERVER}/operations/some_id")
        mock_sleep.assert_not_called()
        mock_logging.assert_not_called()

        # malformed response
        mock_get_request.return_value = {"statuss": "some status"}

        with self.assertRaises(RetriesExceeded) as exc:
            self.br._get_recovery_operation_status("some_id")

        self.assertEqual(str(exc.exception),
                         "Maximum number of retries exceeded for method BarmanRecover._get_recovery_operation_status.")

        self.assertEqual(mock_sleep.call_count, self.br.max_retries)
        mock_sleep.assert_has_calls([call(self.br.retry_wait)] * self.br.max_retries)

        self.assertEqual(mock_logging.call_count, self.br.max_retries)
        mock_logging.assert_has_calls([call(f"Attempt {i+1} of {self.br.max_retries} on method BarmanRecover."
                                            "_get_recovery_operation_status failed with KeyError('status').")
                                       for i in range(self.br.max_retries)])

    @patch.object(BarmanRecover, "_get_recovery_operation_status")
    @patch("time.sleep")
    @patch("logging.info")
    @patch("logging.critical")
    @patch.object(BarmanRecover, "_create_recovery_operation")
    def test_restore_backup(self, mock_create_op, mock_log_critical, mock_log_info, mock_sleep, mock_get_status):
        # successful fast restore
        mock_create_op.return_value = "some_id"
        mock_get_status.return_value = "DONE"

        self.assertTrue(self.br.restore_backup())

        mock_create_op.assert_called_once()
        mock_get_status.assert_called_once_with("some_id")
        mock_log_info.assert_called_once_with("Created the recovery operation with ID some_id")
        mock_log_critical.assert_not_called()
        mock_sleep.assert_not_called()

        # successful slow restore
        mock_create_op.reset_mock()
        mock_get_status.reset_mock()
        mock_log_info.reset_mock()
        mock_get_status.side_effect = ["IN_PROGRESS"] * 20 + ["DONE"]

        self.assertTrue(self.br.restore_backup())

        mock_create_op.assert_called_once()

        self.assertEqual(mock_get_status.call_count, 21)
        mock_get_status.assert_has_calls([call("some_id")] * 21)

        self.assertEqual(mock_log_info.call_count, 21)
        mock_log_info.assert_has_calls([call("Created the recovery operation with ID some_id")]
                                       + [call("Recovery operation some_id is still in progress")] * 20)

        mock_log_critical.assert_not_called()

        self.assertEqual(mock_sleep.call_count, 20)
        mock_sleep.assert_has_calls([call(LOOP_WAIT)] * 20)

        # failed fast restore
        mock_create_op.reset_mock()
        mock_get_status.reset_mock()
        mock_log_info.reset_mock()
        mock_sleep.reset_mock()
        mock_get_status.side_effect = None
        mock_get_status.return_value = "FAILED"

        self.assertFalse(self.br.restore_backup())

        mock_create_op.assert_called_once()
        mock_get_status.assert_called_once_with("some_id")
        mock_log_info.assert_called_once_with("Created the recovery operation with ID some_id")
        mock_log_critical.assert_not_called()
        mock_sleep.assert_not_called()

        # failed slow restore
        mock_create_op.reset_mock()
        mock_get_status.reset_mock()
        mock_log_info.reset_mock()
        mock_sleep.reset_mock()
        mock_get_status.side_effect = ["IN_PROGRESS"] * 20 + ["FAILED"]

        self.assertFalse(self.br.restore_backup())

        mock_create_op.assert_called_once()

        self.assertEqual(mock_get_status.call_count, 21)
        mock_get_status.assert_has_calls([call("some_id")] * 21)

        self.assertEqual(mock_log_info.call_count, 21)
        mock_log_info.assert_has_calls([call("Created the recovery operation with ID some_id")]
                                       + [call("Recovery operation some_id is still in progress")] * 20)

        mock_log_critical.assert_not_called()

        self.assertEqual(mock_sleep.call_count, 20)
        mock_sleep.assert_has_calls([call(LOOP_WAIT)] * 20)

        # create retries exceeded
        mock_log_info.reset_mock()
        mock_sleep.reset_mock()
        mock_create_op.side_effect = RetriesExceeded
        mock_get_status.side_effect = None

        with self.assertRaises(SystemExit) as exc:
            self.assertIsNone(self.br.restore_backup())

        self.assertEqual(exc.exception.code, 5)
        mock_log_info.assert_not_called()
        mock_log_critical.assert_called_once_with("Maximum number of retries exceeded, exiting.")
        mock_sleep.assert_not_called()

        # get status retries exceeded
        mock_create_op.reset_mock()
        mock_create_op.side_effect = None
        mock_log_critical.reset_mock()
        mock_log_info.reset_mock()
        mock_get_status.side_effect = RetriesExceeded

        with self.assertRaises(SystemExit) as exc:
            self.assertIsNone(self.br.restore_backup())

        self.assertEqual(exc.exception.code, 5)
        mock_log_info.assert_called_once_with("Created the recovery operation with ID some_id")
        mock_log_critical.assert_called_once_with("Maximum number of retries exceeded, exiting.")
        mock_sleep.assert_not_called()


class TestMain(unittest.TestCase):

    @patch("logging.basicConfig")
    def test_set_up_logging(self, mock_log_config):
        log_file = "/path/to/some/file.log"
        set_up_logging(log_file)
        mock_log_config.assert_called_once_with(filename=log_file, level=logging.INFO,
                                                format="%(asctime)s %(levelname)s: %(message)s")

    @patch("logging.critical")
    @patch("logging.info")
    @patch("patroni.scripts.barman_recover.set_up_logging")
    @patch("patroni.scripts.barman_recover.BarmanRecover")
    @patch("patroni.scripts.barman_recover.ArgumentParser")
    def test_main(self, mock_arg_parse, mock_br, mock_set_up_log, mock_log_info, mock_log_critical):
        # successful restore
        args = MagicMock()
        mock_arg_parse.return_value.parse_known_args.return_value = (args, None)
        mock_br.return_value.restore_backup.return_value = True

        with self.assertRaises(SystemExit) as exc:
            main()

        mock_arg_parse.assert_called_once()
        mock_set_up_log.assert_called_once_with(args.log_file)
        mock_br.assert_called_once_with(args.api_url, args.barman_server, args.backup_id, args.ssh_command,
                                        args.data_directory, args.loop_wait, args.retry_wait, args.max_retries,
                                        args.cert_file, args.key_file)
        mock_log_info.assert_called_once_with("Recovery operation finished successfully.")
        mock_log_critical.assert_not_called()
        self.assertEqual(exc.exception.code, 0)

        # failed restore
        mock_arg_parse.reset_mock()
        mock_set_up_log.reset_mock()
        mock_br.reset_mock()
        mock_log_info.reset_mock()
        mock_br.return_value.restore_backup.return_value = False

        with self.assertRaises(SystemExit) as exc:
            main()

        mock_arg_parse.assert_called_once()
        mock_set_up_log.assert_called_once_with(args.log_file)
        mock_br.assert_called_once_with(args.api_url, args.barman_server, args.backup_id, args.ssh_command,
                                        args.data_directory, args.loop_wait, args.retry_wait, args.max_retries,
                                        args.cert_file, args.key_file)
        mock_log_info.assert_not_called()
        mock_log_critical.assert_called_once_with("Recovery operation failed.")
        self.assertEqual(exc.exception.code, 1)
