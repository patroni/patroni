from threading import Event, Lock, Thread
import subprocess
import logging
import time
from six.moves import queue

logger = logging.getLogger(__name__)

class CallbackExecutor(Thread):
    '''
    Execution thread that queues commands and executes them in order.
    Guarantees that commands are executed in the order they are received via
    FIFO queue.
    '''

    def __init__(self, executor=subprocess.Popen):
        super(CallbackExecutor, self).__init__()
        self.daemon = True
        self._cmds_queue = queue.Queue()
        self._executor = executor
        self.start()

    def call(self, cmd):
      # Queue up the command
      self._cmds_queue.put(cmd)

    def run(self):
        while True:
            logger.debug("Callback sleeping until new command is queued")
            cmd = self._cmds_queue.get()
            cmd_str = ' '.join(cmd)
            logger.debug("Callback executor waking to call %s", cmd_str)
            try:
                logger.debug("Spawning process for %s", cmd_str)
                p = self._executor(cmd, close_fds=True)
                logger.debug("Waiting for %s to complete", cmd_str)
                p.wait()
                logger.debug("%s has completed", cmd_str)
            except Exception:
                logger.exception('Failed to execute %s', cmd_str)
            # Mark the command as done in the queue. This is probably unnecessary
            # however if .join() is used later it will be necessary so best
            # to just do it anyways
            self._cmds_queue.task_done()
