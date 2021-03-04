import shlex
from threading import Thread, Lock, Event
from psutil import TimeoutExpired
from patroni.postgresql.cancellable import CancellableSubprocess
import logging

logger = logging.getLogger(__name__)


def synchronized(func):
    def wrapped(self, *args, **kwargs):
        with self._lock:
            return func(self, *args, **kwargs)
    return wrapped


class LivenessThread(Thread):

    def __init__(self, config, connect_args):
        super(LivenessThread, self).__init__(name="liveness")
        self.setDaemon(True)
        self._stopevent = Event()
        self._lock = Lock()
        self.config = config
        self.connect_args, self.env = connect_args
        self.cmd = self.config and shlex.split(self.config['probe']) + [self.connect_args] or []
        self.__lv_failures = 0
        self.cancellable = CancellableSubprocess()

    @property
    def lv_failures(self):
        return self.__lv_failures

    def terminate(self):
        if not self.is_alive():
            logger.info("Liveness probe thread not active, terminate skipped")
            return
        with self._lock:
            if not self._stopevent.is_set():
                self._stopevent.set()
            if self.cancellable._process is not None and self.cancellable._process.is_running():
                self.cancellable._kill_process()
        logger.info("Liveness probe terminated")

    def run(self):
        """Liveness plugin probe"""
        while not self._stopevent.isSet():
            if self.cancellable._process and self.cancellable._process.is_running():
                self.cancellable._kill_process()
            try:
                with self.cancellable._lock:
                    self.cancellable._start_process(self.cmd, env=self.env)
                ret = self.cancellable._process.wait(timeout=self.config['timeout'])
                logger.info("Liveness Probe Completed with status %s", ret)
                if ret == 0:
                    self.__lv_failures = 0
                else:
                    if self.config['max_failures'] > 0:
                        self.__lv_failures += 1
                    logger.error("Liveness probe failed. failures %s out of %s", self.__lv_failures, self.config[
                        'max_failures'])
            except TimeoutExpired:
                self.cancellable._kill_process()
                if self.config['max_failures'] > 0:
                    self.__lv_failures += 1
                logger.error("Liveness probe timedout. failures %s out of %s", self.__lv_failures, self.config[
                    'max_failures'])
            except Exception as e:
                logger.error("Exception during liveness probe")
                logger.exception(e)
            self._stopevent.wait(self.config['interval'])


class Liveness(object):

    def __init__(self, config):
        self.config = config
        self.livenesscheck = None
        self._lock = Lock()

    @synchronized
    def reload_config(self, config):
        self.config = config
        if self.livenesscheck:
            self.livenesscheck.config = self.config
        # next loop iteration will restart if liveness checks are set in config
        if self.livenesscheck and self.livenesscheck.is_alive():
            self._disable()

    def _disable(self):
        if self.livenesscheck and self.livenesscheck.is_alive():
            logger.info("liveness check alive, stopping")
            self.livenesscheck.terminate()

    def _activate(self, connect_args):
        if not self.config:
            logger.info("Liveness check activate skipped, No liveness checks in config")
            return
        self._disable()
        if self.livenesscheck and self.livenesscheck.is_alive():
            logger.info("Liveness check from prev run still active after terminate, skipping activate")
            return
        self.livenesscheck = LivenessThread(self.config, connect_args)
        self.livenesscheck.start()

    @synchronized
    def disable(self):
        self._disable()

    @synchronized
    def activate(self, connect_args):
        self._activate(connect_args)

    @property
    def is_running(self):
        return self.livenesscheck and self.livenesscheck.is_alive() or False

    @property
    def is_healthy(self):
        if self.livenesscheck and self.livenesscheck.is_alive() and self.config['max_failures'] > 0:
            return self.livenesscheck.lv_failures < self.config['max_failures']
        return True
