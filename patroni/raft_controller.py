import logging

from .daemon import AbstractPatroniDaemon, abstract_main
from .dcs.raft import KVStoreTTL

logger = logging.getLogger(__name__)


class RaftController(AbstractPatroniDaemon):

    def __init__(self, config):
        super(RaftController, self).__init__(config)

        config = self.config.get('raft')
        assert 'self_addr' in config
        self._raft = KVStoreTTL(None, None, None, **config)

    def _run_cycle(self):
        try:
            self._raft.doTick(self._raft.conf.autoTickPeriod)
        except Exception:
            logger.exception('doTick')

    def _shutdown(self):
        self._raft.destroy()


def main():
    abstract_main(RaftController)
