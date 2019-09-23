import logging
import os
import unittest
import sys

from mock import Mock, patch
from pysyncobj import SyncObj
from patroni.raft_controller import RaftController, main as _main

from . import SleepException


class TestPatroniRaftController(unittest.TestCase):

    SELF_ADDR = '127.0.0.1:5360'

    def remove_files(self):
        for f in ('journal', 'dump'):
            f = self.SELF_ADDR + '.' + f
            if os.path.exists(f):
                os.unlink(f)

    @patch('pysyncobj.tcp_server.TcpServer.bind', Mock())
    def setUp(self):
        self._handlers = logging.getLogger().handlers[:]
        self.remove_files()
        sys.argv = ['patroni.py', 'postgres0.yml']
        os.environ['PATRONI_RAFT_SELF_ADDR'] = self.SELF_ADDR
        self.rc = RaftController()

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

    def test_patroni_raft_controller_main(self):
        self.assertRaises(TypeError, _main)
