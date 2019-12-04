import logging
import os

from patroni.daemon import AbstractPatroniDaemon, abstract_main
from patroni.dcs.raft import KVStoreTTL
from pysyncobj import SyncObjConf

logger = logging.getLogger(__name__)


class RaftController(AbstractPatroniDaemon):

    def __init__(self, config):
        super(RaftController, self).__init__(config)

        raft_config = self.config.get('raft')
        self_addr = raft_config['self_addr']
        template = os.path.join(raft_config.get('data_dir', ''), self_addr)
        self._syncobj_config = SyncObjConf(autoTick=False, appendEntriesUseBatch=False, dynamicMembershipChange=True,
                                           journalFile=template + '.journal', fullDumpFile=template + '.dump')
        self._raft = KVStoreTTL(self_addr, raft_config.get('partner_addrs', []), self._syncobj_config)

    def _run_cycle(self):
        try:
            self._raft.doTick(self._syncobj_config.autoTickPeriod)
        except Exception:
            logger.exception('doTick')

    def _shutdown(self):
        self._raft.destroy()


def main():
    abstract_main(RaftController)
