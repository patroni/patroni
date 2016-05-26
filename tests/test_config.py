import unittest

from mock import MagicMock, Mock, patch
from patroni.config import Config
from six.moves import builtins


class TestConfig(unittest.TestCase):

    @patch('os.path.isfile', Mock(return_value=True))
    @patch('json.load', Mock(side_effect=Exception))
    @patch.object(builtins, 'open', MagicMock())
    def setUp(self):
        self.config = Config(config_env='postgresql: {data_dir: foo}')

    @patch.object(Config, '_build_effective_configuration', Mock(side_effect=Exception))
    def test_set_dynamic_configuration(self):
        self.assertIsNone(self.config.set_dynamic_configuration({'foo': 'bar'}))

    def test_reload_local_configuration(self):
        config = Config(config_file='postgres0.yml')
        with patch.object(Config, '_load_config_file', Mock(return_value={})):
            with patch.object(Config, '_build_effective_configuration', Mock(side_effect=Exception)):
                self.assertRaises(Exception, config.reload_local_configuration, True)
            self.assertTrue(config.reload_local_configuration(True))
            self.assertTrue(config.reload_local_configuration())

    @patch('tempfile.mkstemp', Mock(return_value=[3000, 'blabla']))
    @patch('os.path.exists', Mock(return_value=True))
    @patch('os.remove', Mock(side_effect=IOError))
    @patch('os.close', Mock(side_effect=IOError))
    @patch('os.rename', Mock(return_value=None))
    @patch('json.dump', Mock())
    def test_save_cache(self):
        self.config.set_dynamic_configuration({'ttl': 30, 'postgresql': {'foo': 'bar'}})
        with patch('os.fdopen', Mock(side_effect=IOError)):
            self.config.save_cache()
        with patch('os.fdopen', MagicMock()):
            self.config.save_cache()
