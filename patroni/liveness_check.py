import shlex
from threading import Thread, Lock, Condition
from psutil import TimeoutExpired
from patroni.postgresql.cancellable import CancellableSubprocess
import logging
import time

logger = logging.getLogger(__name__)


def synchronized(func):
    def wrapped(self, *args, **kwargs):
        with self._lock:
            return func(self, *args, **kwargs)
    return wrapped


class LivenessThread(Thread):

    def __init__(self, config):
        super(LivenessThread, self).__init__(name="liveness")
        self.setDaemon(True)
        self.state = Condition()
        self.active = False
        self.config = config['postgresql'].get('liveness', {})
        self.params = self.config and shlex.split(self.config['probe']) + [config['postgresql'].get(
            'connect_address', ':').split(':')[0], config['postgresql'].get('connect_address', ':').split(':')[1],
                                                                           config.get('database', 'postgres')] or []
        self.__lv_failures = 0
        self.cancellable = CancellableSubprocess()

    @property
    def lv_failures(self):
        return self.__lv_failures

    def resume(self):
        with self.state:
            self.active = True
            self.state.notify()
        logger.info("Liveness check resumed")

    def pause(self):
        with self.state:
            self.active = False
            self.state.notify()
            if self.cancellable._process is not None and self.cancellable._process.is_running():
                self.cancellable._kill_process()
            self.__lv_failures = 0
        logger.info("Liveness check paused")

    def run(self):
        """Liveness plugin probe"""
        self.resume()
        while True:
            with self.state:
                if not self.active:
                    self.state.wait()
            if self.cancellable._process is not None and self.cancellable._process.is_running():
                self.cancellable._kill_process()
            try:
                _ = self.cancellable._start_process(self.params)
                ret = self.cancellable._process.wait(timeout=self.config['timeout'])
                if ret and ret != 0:
                    if self.config['max_failures'] > 0:
                        self.__lv_failures += 1
                else:
                    self.__lv_failures = 0
            except TimeoutExpired:
                self.cancellable._kill_process()
                if self.config['max_failures'] > 0:
                    self.__lv_failures += 1
                logger.error("Liveness probe failed. failures count %s out of %s", self.__lv_failures, self.config[
                    'max_failures'])
            except Exception as e:
                logger.error("Exception during liveness probe")
                logger.exception(e)
                self.cancellable._kill_process()
                self.__lv_failures = 0
            time.sleep(self.config['interval'])


class Liveness(object):

    def __init__(self, config):
        self.config = config['postgresql'].get('liveness', {})
        self.livenesscheck = LivenessThread(config)
        self._lock = Lock()

    @synchronized
    def reload_config(self, config):
        self.config = config['postgresql'].get('liveness', {})
        self.livenesscheck.config = self.config
        self.livenesscheck.params = self.config and shlex.split(self.config['probe']) + [config['postgresql'].get(
            'connect_address', ':').split(':')[0], config['postgresql'].get('connect_address', ':').split(':')[1],
                                                                           config.get('database', 'postgres')] or []
        if not self.config and self.livenesscheck.active:
            self._disable()
        elif self.config:
            self._activate()

    def _disable(self):
        if self.livenesscheck.active:
            self.livenesscheck.pause()

    def _activate(self):
        if not self.config:
            logger.info("Liveness check activate skipped, No liveness checks in config")
            return
        if self.livenesscheck.isAlive():
            logger.info("liveness check alive, pause and resume")
            self.livenesscheck.pause()
            self.livenesscheck.resume()
        elif not self.livenesscheck.isAlive() and not self.livenesscheck.active:
            logger.info("Liveness check not alive, activate")
            self.livenesscheck.start()
        else:
            logger.info("Liveness check unable to determine current state, activate skipped")

    @synchronized
    def disable(self):
        self._disable()

    @synchronized
    def activate(self):
        self._activate()

    @property
    def is_running(self):
        return self.livenesscheck.active

    @property
    def is_healthy(self):
        return self.livenesscheck.lv_failures <= self.config['max_failures'] \
                if self.livenesscheck.active and self.config['max_failures'] > 0 else True
