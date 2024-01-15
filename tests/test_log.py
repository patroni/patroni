import logging
import os
import sys
import unittest
import yaml
import json
from io import StringIO

from mock import Mock, patch
from patroni.config import Config
from patroni.log import PatroniLogger
from queue import Queue, Full

_LOG = logging.getLogger(__name__)


class TestPatroniLogger(unittest.TestCase):

    def setUp(self):
        self._handlers = logging.getLogger().handlers[:]

    def tearDown(self):
        logging.getLogger().handlers[:] = self._handlers

    @patch('logging.FileHandler._open', Mock())
    def test_patroni_logger(self):
        config = {
            'log': {
                'traceback_level': 'DEBUG',
                'max_queue_size': 5,
                'dir': 'foo',
                'file_size': 4096,
                'file_num': 5,
                'loggers': {
                    'foo.bar': 'INFO'
                }
            },
            'restapi': {}, 'postgresql': {'data_dir': 'foo'}
        }
        sys.argv = ['patroni.py']
        os.environ[Config.PATRONI_CONFIG_VARIABLE] = yaml.dump(config, default_flow_style=False)
        logger = PatroniLogger()
        patroni_config = Config(None)
        logger.reload_config(patroni_config['log'])
        _LOG.exception('test')
        logger.start()

        with patch.object(logging.Handler, 'format', Mock(side_effect=Exception)), \
                patch('_pytest.logging.LogCaptureHandler.emit', Mock()):
            logging.error('test')

        self.assertEqual(logger.log_handler.maxBytes, config['log']['file_size'])
        self.assertEqual(logger.log_handler.backupCount, config['log']['file_num'])

        config['log']['level'] = 'DEBUG'
        config['log'].pop('dir')
        with patch('logging.Handler.close', Mock(side_effect=Exception)):
            logger.reload_config(config['log'])
            with patch.object(logging.Logger, 'makeRecord',
                              Mock(side_effect=[logging.LogRecord('', logging.INFO, '', 0, '', (), None), Exception])):
                logging.exception('test')
            logging.error('test')
            with patch.object(Queue, 'put_nowait', Mock(side_effect=Full)):
                self.assertRaises(SystemExit, logger.shutdown)
            self.assertRaises(Exception, logger.shutdown)
        self.assertLessEqual(logger.queue_size, 2)  # "Failed to close the old log handler" could be still in the queue
        self.assertEqual(logger.records_lost, 0)

    def test_interceptor(self):
        logger = PatroniLogger()
        logger.reload_config({'level': 'INFO'})
        logger.start()
        _LOG.info('Lock owner: ')
        _LOG.info('blabla')
        logger.shutdown()
        self.assertEqual(logger.records_lost, 0)

    def test_json_list_format(self):
        config = {
            'type': 'json',
            'format': [
                {'asctime': '@timestamp'},
                {'levelname': 'level'},
                'message'
            ],
            'static_fields': {
                'app': 'patroni'
            }
        }

        test_message = 'test json logging in case of list format'

        with patch('sys.stderr', StringIO()) as stderr_output:
            logger = PatroniLogger()
            logger.reload_config(config)

            _LOG.info(test_message)
            target_log = json.loads(stderr_output.getvalue())

        self.assertIn('@timestamp', target_log)
        self.assertEqual(target_log['message'], test_message)
        self.assertEqual(target_log['level'], 'INFO')
        self.assertEqual(target_log['app'], 'patroni')
        self.assertEqual(len(target_log), len(config['format']) + len(config['static_fields']))

    def test_json_str_format(self):
        config = {
            'type': 'json',
            'format': '%(asctime)s %(levelname)s %(message)s',
            'static_fields': {
                'app': 'patroni'
            }
        }

        test_message = 'test json logging in case of string format'

        with patch('sys.stderr', StringIO()) as stderr_output:
            logger = PatroniLogger()
            logger.reload_config(config)

            _LOG.info(test_message)
            target_log = json.loads(stderr_output.getvalue())

        self.assertIn('asctime', target_log)
        self.assertEqual(target_log['message'], test_message)
        self.assertEqual(target_log['levelname'], 'INFO')
        self.assertEqual(target_log['app'], 'patroni')

    def test_plain_format(self):
        config = {
            'type': 'plain',
            'format': '[%(asctime)s] %(levelname)s %(message)s',
        }

        test_message = 'test plain logging'

        with patch('sys.stderr', StringIO()) as stderr_output:
            logger = PatroniLogger()
            logger.reload_config(config)

            _LOG.info(test_message)
            target_log = stderr_output.getvalue()

        self.assertRegex(target_log, fr'^\[.*\] INFO {test_message}$')

    def test_dateformat(self):
        config = {
            'format': '[%(asctime)s] %(message)s',
            'dateformat': '%Y-%m-%dT%H:%M:%S'
        }

        test_message = 'test date format'

        with patch('sys.stderr', StringIO()) as stderr_output:
            logger = PatroniLogger()
            logger.reload_config(config)

            _LOG.info(test_message)
            target_log = stderr_output.getvalue()

        self.assertRegex(target_log, r'\[\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\]')

    def test_invalid_dateformat(self):
        config = {
            'format': '[%(asctime)s] %(message)s',
            'dateformat': 5
        }

        with self.assertLogs() as captured_log:
            logger = PatroniLogger()
            logger.reload_config(config)

            captured_log_level = captured_log.records[0].levelname
            captured_log_message = captured_log.records[0].message

            self.assertEqual(captured_log_level, 'WARNING')
            self.assertRegex(
                captured_log_message,
                fr'Expected log dateformat to be a string, but got "{type(config["dateformat"])}"'
            )

    def test_invalid_plain_format(self):
        config = {
            'type': 'plain',
            'format': ['message']
        }

        with self.assertLogs() as captured_log:
            logger = PatroniLogger()
            logger.reload_config(config)

            captured_log_level = captured_log.records[0].levelname
            captured_log_message = captured_log.records[0].message

            self.assertEqual(captured_log_level, 'WARNING')
            self.assertRegex(
                captured_log_message,
                r'Expected log format to be a string when log type is plain, but got ".*"'
            )

    def test_invalid_json_format(self):
        config = {
            'type': 'json',
            'format': {
                'asctime': 'timestamp',
                'message': 'message'
            }
        }

        with self.assertLogs() as captured_log:
            logger = PatroniLogger()
            logger.reload_config(config)

            captured_log_level = captured_log.records[0].levelname
            captured_log_message = captured_log.records[0].message

            self.assertEqual(captured_log_level, 'WARNING')
            self.assertRegex(
                captured_log_message,
                r'Expected log format to be a string or a list, but got ".*"'
            )

        with self.assertLogs() as captured_log:
            config['format'] = ['message', ['levelname']]
            logger.reload_config(config)

            captured_log_level = captured_log.records[0].levelname
            captured_log_message = captured_log.records[0].message

            self.assertEqual(captured_log_level, 'WARNING')
            self.assertRegex(
                captured_log_message,
                r'Expected each item of log format to be a string or dictionary, but got ".*"'
            )

        with self.assertLogs() as captured_log:
            config['format'] = [
                'message',
                {'asctime': ['timestamp']}
            ]
            logger.reload_config(config)

            captured_log_level = captured_log.records[0].levelname
            captured_log_message = captured_log.records[0].message

            self.assertEqual(captured_log_level, 'WARNING')
            self.assertRegex(
                captured_log_message,
                r'Expected renamed log field to be a string, but got ".*"'
            )

    @patch('pythonjsonlogger.jsonlogger.JsonFormatter', side_effect=ImportError)
    def test_fail_to_import_python_json_logger(self, _):
        config = {
            'type': 'json'
        }

        with self.assertLogs() as captured_log:
            logger = PatroniLogger()
            logger.reload_config(config)

            captured_log_level = captured_log.records[0].levelname
            captured_log_message = captured_log.records[0].message

            self.assertEqual(captured_log_level, 'ERROR')
            self.assertRegex(
                captured_log_message,
                r'Failed to import "python-json-logger" library. Falling back to the plain logger'
            )
