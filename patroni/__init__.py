import logging
import os
import signal
import sys
import time
import yaml

from patroni.api import RestApiServer
from patroni.exceptions import PatroniException
from patroni.ha import Ha
from patroni.postgresql import Postgresql
from patroni.utils import reap_children, set_ignore_sigterm, setup_signal_handlers
from patroni.version import __version__

logger = logging.getLogger(__name__)


class Patroni(object):
    PATRONI_CONFIG_VARIABLE = 'PATRONI_CONFIGURATION'

    def __init__(self, config_file=None, config_env=None):
        self._config_file = config_file
        config = yaml.load(config_env) if config_env else self._load_config()

        self.nap_time = config['loop_wait']
        self.tags = self.get_tags(config)
        self.postgresql = Postgresql(config['postgresql'])
        self.dcs = self.get_dcs(self.postgresql.name, config)
        self.version = __version__
        self.api = RestApiServer(self, config['restapi'])
        self.ha = Ha(self)
        self.next_run = time.time()

        self._reload_config_scheduled = False

    @staticmethod
    def get_tags(config):
        return {tag: value for tag, value in config.get('tags', {}).items()
                if tag not in ('clonefrom', 'nofailover', 'noloadbalance') or value}

    def _load_config(self):
        with open(self._config_file) as f:
            return yaml.load(f)

    def reload_config(self):
        try:
            config = self._load_config()
            self.tags = self.get_tags(config)
            self.nap_time = config['loop_wait']
            self.dcs.set_ttl(config.get('ttl') or 30)
            self.api.reload_config(config['restapi'])
            self.postgresql.reload_config(config['postgresql'])
        except Exception:
            logger.exception('Failed to reload config_file=%s', self._config_file)
        self._reload_config_scheduled = False

    def sighup_handler(self, *args):
        self._reload_config_scheduled = True

    @property
    def noloadbalance(self):
        return self.tags.get('noloadbalance', False)

    @property
    def nofailover(self):
        return self.tags.get('nofailover', False)

    @property
    def replicatefrom(self):
        return self.tags.get('replicatefrom')

    @staticmethod
    def get_dcs(name, config):
        if 'etcd' in config:
            from patroni.etcd import Etcd
            return Etcd(name, config['etcd'])
        if 'zookeeper' in config:
            from patroni.zookeeper import ZooKeeper
            return ZooKeeper(name, config['zookeeper'])
        if 'consul' in config:
            from patroni.consul import Consul
            return Consul(name, config['consul'])
        raise PatroniException('Can not find suitable configuration of distributed configuration store')

    def schedule_next_run(self):
        self.next_run += self.nap_time
        current_time = time.time()
        nap_time = self.next_run - current_time
        if nap_time <= 0:
            self.next_run = current_time
        elif self.dcs.watch(nap_time):
            self.next_run = time.time()

    def run(self):
        self.api.start()
        signal.signal(signal.SIGHUP, self.sighup_handler)
        self.next_run = time.time()

        while True:
            if self._reload_config_scheduled:
                self.reload_config()
            logger.info(self.ha.run_cycle())
            reap_children()
            self.schedule_next_run()


def main():
    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.DEBUG)
    logging.getLogger('requests').setLevel(logging.WARNING)
    setup_signal_handlers()

    # Patroni reads the configuration from the command-line argument if it exists, and from the environment otherwise.
    config_env = False
    config_file = len(sys.argv) >= 2 and os.path.isfile(sys.argv[1]) and sys.argv[1]
    if not config_file:
        config_env = os.environ.get(Patroni.PATRONI_CONFIG_VARIABLE)
        if config_env is None:
            print('Usage: {0} config.yml'.format(sys.argv[0]))
            print('\tPatroni may also read the configuration from the {} environment variable'.
                  format(Patroni.PATRONI_CONFIG_VARIABLE))
            return

    patroni = Patroni(config_file, config_env)
    try:
        patroni.run()
    except KeyboardInterrupt:
        set_ignore_sigterm()
    finally:
        patroni.api.shutdown()
        patroni.postgresql.stop(checkpoint=False)
        patroni.dcs.delete_leader()
