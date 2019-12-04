import logging
import os
import sys
import unittest
import yaml

from mock import Mock, patch
from patroni.config import Config
from patroni.log import PatroniLogger
from six.moves.queue import Queue, Full

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

        with patch.object(logging.Handler, 'format', Mock(side_effect=Exception)):
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
