import logging

from .config import Config
from .daemon import AbstractPatroniDaemon, abstract_main
from .dcs.raft import KVStoreTTL

logger = logging.getLogger(__name__)


class RaftController(AbstractPatroniDaemon):

    def __init__(self, config: Config) -> None:
        super(RaftController, self).__init__(config)

        kvstore_config = self.config.get('raft')
        assert 'self_addr' in kvstore_config
        self._raft = KVStoreTTL(None, None, None, **kvstore_config)

    def _run_cycle(self) -> None:
        try:
            self._raft.doTick(self._raft.conf.autoTickPeriod)
        except Exception:
            logger.exception('doTick')

    def _shutdown(self) -> None:
        self._raft.destroy()


def main() -> None:
    abstract_main(RaftController)
