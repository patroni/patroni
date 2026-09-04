import logging

from .config import Config
from .daemon import abstract_main, AbstractPatroniDaemon, get_base_arg_parser
from .dcs.raft import KVStoreTTL
from .log import PatroniLogger

logger = logging.getLogger(__name__)


class RaftController(AbstractPatroniDaemon):

    def __init__(self, config: Config, patroni_logger: PatroniLogger) -> None:
        super(RaftController, self).__init__(config, patroni_logger)

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
    parser = get_base_arg_parser()
    args = parser.parse_args()

    abstract_main(RaftController, args.configfile)
