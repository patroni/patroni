import logging
import shlex

logger = logging.getLogger(__name__)


class Prepromote(object):

    def __init__(self, ha):
        self._postgresql = ha.state_handler

    def pre_promote(self, config, task):
        try:
            task.complete(self.call_pre_promote(config))
        except Exception:
            logger.exception('pre_promote')
            task.complete(False)
        return task.result

    def call_pre_promote(self, cmd):
        """
        Runs a fencing script after the leader lock is acquired but before the replica is promoted.
        If the script exits with a non-zero code, promotion does not happen and the leader key is removed from DCS.
        """
        if cmd:
            try:
                ret = self._postgresql.cancellable.call(shlex.split(cmd))
            except OSError as err:
                logger.error('pre_promote script %s failed: %s', cmd, err)
                return False

            if ret != 0:
                logger.error('pre_promote script %s returned non-zero code %d', cmd, ret)
                return False

        return True
