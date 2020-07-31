import logging
import os
import unittest

from mock import Mock, patch
from pysyncobj import SyncObj
from patroni.config import Config
from patroni.raft_controller import RaftController, main as _main

from . import SleepException
from .test_raft import remove_files


class TestPatroniRaftController(unittest.TestCase):

    SELF_ADDR = '127.0.0.1:5360'

    def remove_files(self):
        remove_files(self.SELF_ADDR + '.')

    @patch('pysyncobj.tcp_server.TcpServer.bind', Mock())
    def setUp(self):
        self._handlers = logging.getLogger().handlers[:]
        self.remove_files()
        os.environ['PATRONI_RAFT_SELF_ADDR'] = self.SELF_ADDR
        config = Config('postgres0.yml', validator=None)
        self.rc = RaftController(config)

    def tearDown(self):
        logging.getLogger().handlers[:] = self._handlers
        self.remove_files()

    def test_reload_config(self):
        self.rc.reload_config()

    @patch('logging.Logger.error', Mock(side_effect=SleepException))
    @patch.object(SyncObj, 'doTick', Mock(side_effect=Exception))
    def test_run(self):
        self.assertRaises(SleepException, self.rc.run)
        self.rc.shutdown()

    @patch('sys.argv', ['patroni'])
    def test_patroni_raft_controller_main(self):
        self.assertRaises(SystemExit, _main)
