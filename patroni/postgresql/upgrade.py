import logging
from typing import Optional

import psycopg2

from ..dcs import Cluster, Member
from ..utils import polling_loop
from . import Postgresql

logger = logging.getLogger(__name__)


class InplaceUpgrade(object):

    def __init__(self, postgresql: Postgresql, desired_version: str, replica_count: int, ha: 'Ha'):
        self.current_pg = postgresql

        self.cluster_version = self.current_pg.get_cluster_version()
        # TODO: add plugin API for discovering desired version from environment
        self.desired_version = desired_version

        self.upgrade_required = float(self.cluster_version) < float(self.desired_version)

        target_config = self.current_pg.config.copy()
        # TODO: handle versioned data_dir
        target_config['data_dir'] = self.current_pg.data_dir + '_new'
        # FIXME: handle case where new data_dir already exists and has wrong major version in it
        target_config['version'] = desired_version

        self.target_pg = Postgresql(target_config, ha.dcs.mpp)

        self.replica_count = replica_count

        self.paused = False
        self.new_data_created = False
        self.upgrade_complete = False
        self.rsyncd_configs_created = False
        self.rsyncd_started = False

        self.ha = ha

        self.replica_connections = {}

        self._old_sysid = None  #FIXME: read the value immediately?

    def do_upgrade(self):
        if not self.upgrade_required:
            logger.info('Current version=%s, desired version=%s. Upgrade is not required',
                        self.cluster_version, self.desired_version)
            return True

        if not (self.current_pg.is_running() and self.current_pg.is_primary()):
            return logger.error('PostgreSQL is not running or in recovery')

        # Should we get the cluster object from the HA loop itself?
        cluster: Cluster = self.ha.dcs.get_cluster()

        if not self.sanity_checks(cluster):
            return False

        self._old_sysid = self.postgresql.sysid  # remember old sysid

        logger.info('Cluster %s is ready to be upgraded', self.postgresql.scope)
        if not self.postgresql.prepare_new_pgdata(self.desired_version):
            return logger.error('initdb failed')

        try:
            self.postgresql.drop_possibly_incompatible_extensions()
        except Exception:
            return logger.error('Failed to drop possibly incompatible extensions')

        if not self.postgresql.pg_upgrade(check=True):
            return logger.error('pg_upgrade --check failed, more details in the %s_upgrade', self.postgresql.data_dir)

        try:
            self.postgresql.drop_possibly_incompatible_objects()
        except Exception:
            return logger.error('Failed to drop possibly incompatible objects')

        logging.info('Enabling maintenance mode')
        if not self.toggle_pause(True):
            return False

        logger.info('Doing a clean shutdown of the cluster before pg_upgrade')
        downtime_start = time.time()
        if not self.postgresql.stop(block_callbacks=True):
            return logger.error('Failed to stop the cluster before pg_upgrade')

        if self.replica_connections:
            from patroni.postgresql.misc import parse_lsn

            controldata = self.postgresql.controldata()

            checkpoint_lsn = controldata.get('Latest checkpoint location')
            if controldata.get('Database cluster state') != 'shut down' or not checkpoint_lsn:
                return logger.error("Cluster wasn't shut down cleanly")

            checkpoint_lsn = parse_lsn(checkpoint_lsn)
            logger.info('Latest checkpoint location: %s', checkpoint_lsn)

            logger.info('Starting rsyncd')
            self.start_rsyncd()

            if not self.wait_for_replicas(checkpoint_lsn):
                return False

            if not (self.rsyncd.pid and self.rsyncd.poll() is None):
                return logger.error('Failed to start rsyncd')

        if self.replica_connections:
            logger.info('Executing CHECKPOINT on replicas %s', ','.join(self.replica_connections.keys()))
            pool = ThreadPool(len(self.replica_connections))
            # Do CHECKPOINT on replicas in parallel with pg_upgrade.
            # It will reduce the time for shutdown and so downtime.
            results = pool.map_async(self.checkpoint, self.replica_connections.items())
            pool.close()

        if not self.postgresql.pg_upgrade():
            return logger.error('Failed to upgrade cluster from %s to %s', self.cluster_version, self.desired_version)

        self.postgresql.switch_pgdata()
        self.upgrade_complete = True

        logger.info('Updating configuration files')
        envdir = update_configs(self.desired_version)

        ret = True
        if self.replica_connections:
            # Check status of replicas CHECKPOINT and remove connections that are failed.
            pool.join()
            if results.ready():
                for name, status in results.get():
                    if not status:
                        ret = False
                        self.replica_connections.pop(name)

        member = cluster.get_member(self.postgresql.name)
        if self.replica_connections:
            primary_ip = member.conn_kwargs().get('host')
            rsync_start = time.time()
            try:
                if not self.rsync_replicas(primary_ip):
                    ret = False
            except Exception as e:
                logger.error('rsync failed: %r', e)
                ret = False
            logger.info('Rsync took %s seconds', time.time() - rsync_start)

            self.stop_rsyncd()
            time.sleep(2)  # Give replicas a bit of time to switch PGDATA

        self.remove_initialize_key()
        kill_patroni()
        self.remove_initialize_key()

        time.sleep(1)
        for _ in polling_loop(10):
            if self.check_patroni_api(member):
                break
        else:
            logger.error('Patroni REST API on primary is not accessible after 10 seconds')

        logger.info('Starting the primary postgres up')
        for _ in polling_loop(10):
            try:
                result = self.request(member, 'post', 'restart', {})
                logger.info('   %s %s', result.status, result.data.decode('utf-8'))
                if result.status < 300:
                    break
            except Exception as e:
                logger.error('POST /restart failed: %r', e)
        else:
            logger.error('Failed to start primary after upgrade')

        logger.info('Upgrade downtime: %s', time.time() - downtime_start)

        # The last attempt to fix initialize key race condition
        cluster = self.dcs.get_cluster()
        if cluster.initialize == self._old_sysid:
            self.dcs.cancel_initialization()

        try:
            self.postgresql.update_extensions()
        except Exception as e:
            logger.error('Failed to update extensions: %r', e)

        # start analyze early
        analyze_thread = Thread(target=self.analyze)
        analyze_thread.start()

        if self.replica_connections:
            self.wait_replicas_restart(cluster)

        self.resume_cluster()

        analyze_thread.join()

        self.reanalyze()

        logger.info('Total upgrade time (with analyze): %s', time.time() - downtime_start)
        self.postgresql.bootstrap.call_post_bootstrap(self.config['bootstrap'])
        self.postgresql.cleanup_old_pgdata()

        if envdir:
            self.start_backup(envdir)

        return ret

    def check_patroni_api(self, member: Member) -> bool:
        # This will run API check with timeout 2 retries 0. Maybe too fragile?
        status = self.ha.fetch_node_status(member)

        if not status.reachable:
            logger.error('Member %s is not reachable', member.name)
            return False

        #TODO: this would be a good place to check for lag

        return True

    def ensure_replicas_state(self, cluster: Cluster) -> bool:
        """
        This method checks the status of all replicas and also tries to open connections
        to all of them and puts into the `self.replica_connections` dict for a future usage.
        """
        self.replica_connections = {}
        streaming = {a: l for a, l in self.current_pg.query(
            ("SELECT client_addr, pg_catalog.pg_{0}_{1}_diff(pg_catalog.pg_current_{0}_{1}(),"
             " COALESCE(replay_{1}, '0/0'))::bigint FROM pg_catalog.pg_stat_replication")
            .format(self.current_pg.wal_name, self.current_pg.lsn_name))}

        def ensure_replica_state(member):
            ip = member.conn_kwargs().get('host')
            lag = streaming.get(ip)
            if lag is None:
                return logger.error('Member %s is not streaming from the primary', member.name)
            if lag > 16*1024*1024:
                return logger.error('Replication lag %s on member %s is too high', lag, member.name)

            if not self.check_patroni_api(member):
                return logger.error('Patroni on %s is not healthy', member.name)

            conn_kwargs = member.conn_kwargs(self.current_pg.config.superuser)
            conn_kwargs['options'] = '-c statement_timeout=0 -c search_path='
            conn_kwargs.pop('connect_timeout', None)

            conn = psycopg2.connect(**conn_kwargs)
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute('SELECT pg_catalog.pg_is_in_recovery()')
            if not cur.fetchone()[0]:
                return logger.error('Member %s is not running as replica!', member.name)
            self.replica_connections[member.name] = (ip, cur)
            return True

        return all(ensure_replica_state(member) for member in cluster.members if member.name != self.current_pg.name)

    def sanity_checks(self, cluster: Cluster) -> Optional[bool]:

        if not cluster.initialize:
            return logger.error('Upgrade can not be triggered because the cluster is not initialized')

        if len(cluster.members) != self.replica_count:
            return logger.error('Upgrade can not be triggered because the number of replicas does not match (%s != %s)',
                                len(cluster.members), self.replica_count)
        if self.ha.is_paused():
            return logger.error('Upgrade can not be triggered because Patroni is in maintenance mode')

        lock_owner = cluster.leader and cluster.leader.name
        if lock_owner != self.current_pg.name:
            return logger.error('Upgrade can not be triggered because the current node does not own the leader lock')

        return self.ensure_replicas_state(cluster)