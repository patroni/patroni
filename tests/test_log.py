import logging
import os
import sys
import unittest
import yaml
import json

from mock import Mock, patch
from patroni.config import Config
from patroni.log import PatroniLogger
from queue import Queue, Full
from typing import Dict, Any

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
            'log': {
                'type': 'json',
                'format': [
                    {
                        'asctime': '@timestamp',
                        'levelname': 'level'
                    },
                    'message'
                ],
                'dir': 'log',
                'static_fields': {
                    'app': 'patroni'
                }
            }
        }

        log_path = f"{config['log']['dir']}/patroni.log"
        test_message = 'test json logging in case of list format'

        try:
            os.remove(log_path)
        except OSError:
            pass

        sys.argv = ['patroni.py']
        os.environ[Config.PATRONI_CONFIG_VARIABLE] = yaml.dump(config, default_flow_style=False)
        logger = PatroniLogger()
        patroni_config = Config(None)
        logger.reload_config(patroni_config['log'])

        logger.start()
        _LOG.info(test_message)
        logger.shutdown()

        target_log: Dict[str, Any] = {}
        with open(log_path) as file:
            for line in file:
                if test_message in line:
                    target_log = json.loads(line)
                    break

        self.assertIn('@timestamp', target_log)
        self.assertEqual(target_log['message'], test_message)
        self.assertEqual(target_log['level'], 'INFO')
        self.assertEqual(target_log['app'], 'patroni')

    def test_json_str_format(self):
        config = {
            'log': {
                'type': 'json',
                'format': '%(asctime)s %(levelname)s %(message)s',
                'dir': 'log',
                'static_fields': {
                    'app': 'patroni'
                }
            }
        }

        log_path = f"{config['log']['dir']}/patroni.log"
        test_message = 'test json logging in case of string format'

        try:
            os.remove(log_path)
        except OSError:
            pass

        sys.argv = ['patroni.py']
        os.environ[Config.PATRONI_CONFIG_VARIABLE] = yaml.dump(config, default_flow_style=False)
        logger = PatroniLogger()
        patroni_config = Config(None)
        logger.reload_config(patroni_config['log'])

        logger.start()
        _LOG.info(test_message)
        logger.shutdown()

        target_log: Dict[str, Any] = {}
        with open(log_path) as file:
            for line in file:
                if test_message in line:
                    target_log = json.loads(line)
                    break

        self.assertIn('asctime', target_log)
        self.assertEqual(target_log['message'], test_message)
        self.assertEqual(target_log['levelname'], 'INFO')
        self.assertEqual(target_log['app'], 'patroni')

    def test_plain_format(self):
        config = {
            'log': {
                'type': 'plain',
                'format': '[%(asctime)s] %(levelname)s %(message)s',
                'dir': 'log'
            }
        }

        log_path = f"{config['log']['dir']}/patroni.log"
        test_message = 'test plain logging'

        try:
            os.remove(log_path)
        except OSError:
            pass

        sys.argv = ['patroni.py']
        os.environ[Config.PATRONI_CONFIG_VARIABLE] = yaml.dump(config, default_flow_style=False)
        logger = PatroniLogger()
        patroni_config = Config(None)
        logger.reload_config(patroni_config['log'])

        logger.start()
        _LOG.info(test_message)
        logger.shutdown()

        target_log = ''
        with open(log_path) as file:
            for line in file:
                if test_message in line:
                    target_log = line
                    break

        self.assertRegexpMatches(target_log, fr'\[.*\] INFO {test_message}')
