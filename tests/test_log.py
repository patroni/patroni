import os
import sys
import unittest
import yaml

from mock import Mock, patch
from patroni.config import Config
from patroni.log import PatroniLogger


class TestPatroniLogger(unittest.TestCase):

    @patch('logging.FileHandler._open', Mock())
    def setUp(self):
        self.config = {
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
        os.environ[Config.PATRONI_CONFIG_VARIABLE] = yaml.dump(self.config, default_flow_style=False)
        self.logger = PatroniLogger()
        config = Config()
        self.logger.reload_config(config['log'])

    def test_rotating_handler(self):
        self.assertEqual(self.logger.handler.maxBytes, self.config['log']['file_size'])
        self.assertEqual(self.logger.handler.backupCount, self.config['log']['file_num'])

    def test_reload_config(self):
        self.config['log'].pop('dir')
        self.logger.reload_config(self.config['log'])
