import os
import sys
import unittest
import yaml

from mock import Mock, patch
from patroni.config import Config
from patroni.log import PatroniLogger


class TestPatroniLogger(unittest.TestCase):

    @patch('logging.FileHandler._open', Mock())
    def test_patroni_logger(self):
        config = {
            'log': {
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
        patroni_config = Config()
        logger.reload_config(patroni_config['log'])

        self.assertEqual(logger.handler.maxBytes, config['log']['file_size'])
        self.assertEqual(logger.handler.backupCount, config['log']['file_num'])

        config['log'].pop('dir')
        logger.reload_config(config['log'])
