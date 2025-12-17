import datetime
import logging
import sys
import time
from collections import defaultdict
from enum import Enum
from multiprocessing.pool import ThreadPool
from threading import Condition, Thread
from typing import Optional, Dict, Any, Iterable, cast, List

import psycopg2

from .upgrade_plugins import ReplicaUpgradePlugin, UpgradePreparePlugin, UpgradePluginManager
from ..dcs import Cluster, Member, Upgrade
from ..exceptions import PatroniException
from ..utils import polling_loop
from . import Postgresql
from .misc import postgres_major_version_to_int, format_major_version, PostgresqlRole

logger = logging.getLogger(__name__)

class UpgradeFailure(PatroniException):
    pass


class UpgradeState(str, Enum):
    TRANSIENT = ''
    PRECHECK = 'precheck'
    PREPARE = 'prepare'
    UPGRADE = 'upgrade'
    UPGRADE_REPLICAS = 'upgrade_replicas'
    POST_UPGRADE = 'post_upgrade'
    ROLLBACK_PREPARE = 'rollback_prepare'
    SUCCESS = 'success'
    FAILED = 'failed'
    CHECKED = 'checked'

    def is_active(self):
        return self in [
            UpgradeState.UPGRADE,
            UpgradeState.UPGRADE_REPLICAS,
            UpgradeState.POST_UPGRADE,
            UpgradeState.ROLLBACK_PREPARE,
        ]

class ReplicaSyncStatus(str, Enum):
    NOT_RUNNING = 'not_running'
    RUNNING = 'running'
    SUCCESS = 'success'
    FAILED = 'failed'

class InplaceUpgrade(object):

    def __init__(self, postgresql: Postgresql, ha: 'Ha', dcs_state: Upgrade):
        self.dcs_state = dcs_state
        self.config = dcs_state.config

        self.current_pg = postgresql

        self.cluster_version = self.current_pg.get_cluster_version()
        # TODO: add plugin API for discovering desired version from environment
        # TODO: add API to validate desired version
        self.desired_version = format_major_version(dcs_state.target_version)

        self.upgrade_required = float(self.cluster_version) < float(self.desired_version)

        # TODO: handle versioned data_dir
        temp_datadir = self.current_pg.data_dir + '_new'
        # FIXME: handle case where new data_dir already exists and has wrong major version in it

        self.target_pg = postgresql.new_version(dcs_state.target_version, temp_datadir)

        self.ha = ha


        self.replica_upgrade = self._get_replica_upgrade_plugin(self.config)
        self.plugins = UpgradePluginManager(self.config)

        self.sync_cv = Condition()
        self.sync_result: Optional[bool] = None
        self.sync_running: bool = False

        self.replica_connections = {}
        self.members: Optional[Dict[str, Member]] = None

    @classmethod
    def start_new(cls, postgresql: Postgresql, ha: 'Ha', config: Dict, desired_version: str):
        return cls(postgresql, ha, dcs_state=Upgrade(
            initiator=postgresql.name,
            state=UpgradeState.TRANSIENT,
            source_sysid=postgresql.sysid,
            source_version=postgresql.major_version,
            target_sysid=None,
            target_version=postgres_major_version_to_int(desired_version),
            shutdown_lsn=None,
            config=config,
            progress=[]
        ))

    @classmethod
    def check_config(cls, config: Dict) -> Iterable[str]:
        if not isinstance(config, dict):
            yield f"Upgrade config is not a dict"
            return

    def _get_replica_upgrade_plugin(self, config: dict) -> ReplicaUpgradePlugin:
        method_name = config.get('replica_upgrade_method', 'reinit')
        if not isinstance(method_name, str):
            raise ValueError('replica_sync_method must be a string')

        args = config.get(method_name, {})

        return ReplicaUpgradePlugin.get_plugin(method_name, args)

    def update_dcs(self, event, **kwargs):
        logger.info("Updating DCS data %s: %s", ', '.join(f"{k}={v}" for k,v in kwargs.items()), event)
        self.dcs_state = self.dcs_state._replace(**kwargs)
        self.dcs_state.progress.append((datetime.datetime.now(datetime.timezone.utc).isoformat(), event))
        self.ha.dcs.write_status({'upgrade': self.dcs_state._asdict()})

    def do_upgrade(self, check_only=False):
        if not self.upgrade_required:
            logger.info('Current version=%s, desired version=%s. Upgrade is not required',
                        self.cluster_version, self.desired_version)
            return True

        if not (self.current_pg.is_running() and self.current_pg.is_primary()):
            return logger.error('PostgreSQL is not running or in recovery')

        # Set state in DCS
        self.update_dcs('Upgrade started', state=UpgradeState.PRECHECK)

        # Should we get the cluster object from the HA loop itself?
        cluster: Cluster = self.ha.dcs.get_cluster()

        if not self.sanity_checks(cluster):
            return False

        self.update_dcs('Upgrade pre checks complete', state=UpgradeState.PREPARE)

        logger.info('Cluster %s is ready to be upgraded', self.current_pg.scope)
        if not self.target_pg.prepare_new_pgdata(self.current_pg):
            return logger.error('initdb failed')

        self.update_dcs('New data directory initialized')

        # TODO: implement this too for stuff that is needed to make pg_upgrade check pass
        # Should it happen before preparing new pgdata
        #self.plugins.prepare_precheck(self.current_pg)

        if not self.target_pg.pg_upgrade(self.current_pg, check=True):
            return logger.error('pg_upgrade --check failed, more details in the %s_upgrade', self.current_pg.data_dir)

        self.update_dcs('pg_upgrade --check completed')

        if check_only:
            self.update_dcs('Check successfully executed', state=UpgradeState.CHECKED)
            return

        self.plugins.prepare(self.current_pg)

        self.update_dcs('Cluster prepared for upgrade', state=UpgradeState.UPGRADE)

        logger.info('Doing a clean shutdown of the cluster before pg_upgrade')
        downtime_start = time.time()
        if not self.current_pg.stop(block_callbacks=True):
            return logger.error('Failed to stop the cluster before pg_upgrade')

        self.update_dcs('Primary stopped')

        shutdown_lsn = self._wait_for_shutdown_replication()

        self.update_dcs('Shutdown checkpoint has been replicated', shutdown_lsn=shutdown_lsn)

        if self.replica_connections:
            failed_replicas = self._init_replica_upgrade()

        if not self.target_pg.pg_upgrade(self.current_pg):
            return logger.error('Failed to upgrade cluster from %s to %s', self.cluster_version, self.desired_version)

        self.target_pg.switch_pgdata(self.current_pg)
        self.upgrade_complete = True

        self.update_dcs('pg_upgrade completed')

        # TODO: need a hook here for backup adjustments, or something that needs to run before postgres starts?

        self.update_dcs('Starting replica upgrades', state=UpgradeState.UPGRADE_REPLICAS)

        ret = True
        if self.replica_connections:
            for failed in failed_replicas():
                ret = False
                self.replica_connections.pop(failed)
                self.members.pop(failed)
                self.update_dcs(f'Replica {failed} upgrade failed')

        leader = cluster.get_member(self.current_pg.name)

        upgraded_replicas = self.replica_upgrade.before_primary_start(self.ha, leader, self.members)

        #TODO: should we abort upgrade if replicas failed to upgrade?

        self.ha.dcs.initialize(create_new=False, sysid=self.target_pg.sysid)
        self.update_dcs('Replicas upgraded', state=UpgradeState.POST_UPGRADE)

        logger.info('Starting the primary postgres up')
        for _ in polling_loop(1000000, 10):  # TODO: should be endless loop?
            start_result = self.target_pg.start()
            if start_result is None:
                while not self.target_pg.check_startup_state_changed():
                    time.sleep(10)
                start_result = self.target_pg.is_running()

            if start_result is False:
                logger.info("Failed to start primary postgres")
                continue

            self.update_dcs('Primary started')
            break
        else:
            logger.info("Failed to start primary postgres after upgrade")
            sys.exit(1)

        logger.info('Upgrade downtime: %s', time.time() - downtime_start)

        self.plugins.post_upgrade(self.target_pg)

        analyze_thread = None
        if self.target_pg.major_version < 180000:
            # start analyze early
            analyze_thread = AnalyzeJob(self.target_pg)
            analyze_thread.start()

        if self.members:
            #TODO: wait for replicas to start
            pass

        self.ha.update_postgres(self.target_pg)
        self.update_dcs('Upgrade complete', state=UpgradeState.SUCCESS)

        if analyze_thread:
            analyze_thread.join()
            analyze_thread.reanalyze()

        logger.info('Total upgrade time (with analyze): %s', time.time() - downtime_start)
        self.target_pg.bootstrap.call_post_bootstrap(self.ha.patroni.config['bootstrap'])
        self.current_pg.remove_data_directory()

        # TODO: add hook point for fire and forget actions after upgrade, e.g. backups

        return ret

    def _init_replica_upgrade(self) -> Iterable[str]:
        logger.info('Executing CHECKPOINT on replicas %s', ','.join(self.replica_connections.keys()))
        pool = ThreadPool(len(self.replica_connections))
        # Do CHECKPOINT on replicas in parallel with pg_upgrade.
        # It will reduce the time for shutdown and so downtime.
        results = pool.map_async(self.checkpoint_member, self.replica_connections.items())
        pool.close()

        self.replica_upgrade.before_pg_upgrade(self.current_pg, self.members)

        def failed_replicas():
            # Check status of replicas CHECKPOINT and remove connections that are failed.
            pool.join()
            if results.ready():
                for name, status in results.get():
                    if not status:
                        yield name

        return failed_replicas

    def _wait_for_shutdown_replication(self) -> int:
        from patroni.postgresql.misc import parse_lsn

        controldata = self.current_pg.controldata()

        checkpoint_lsn_str = controldata.get('Latest checkpoint location')
        if controldata.get('Database cluster state') != 'shut down' or not checkpoint_lsn_str:
            raise UpgradeFailure("Cluster wasn't shut down cleanly")

        checkpoint_lsn = parse_lsn(checkpoint_lsn_str)
        logger.info('Latest checkpoint location: %s', checkpoint_lsn)

        self.replica_upgrade.after_primary_stop(self.current_pg, self.members)

        if not self.wait_for_replicas(checkpoint_lsn):
            raise UpgradeFailure("Not all replicas caught up")

        return checkpoint_lsn

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

        self.members = {}

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
            self.members[member.name] = member
            return True

        return all(ensure_replica_state(member) for member in cluster.members if member.name != self.current_pg.name)

    def sanity_checks(self, cluster: Cluster) -> Optional[bool]:
        if not cluster.initialize:
            return logger.error('Upgrade can not be triggered because the cluster is not initialized')

        if self.ha.is_paused():
            return logger.error('Upgrade can not be triggered because Patroni is in maintenance mode')

        lock_owner = cluster.leader and cluster.leader.name
        if lock_owner != self.current_pg.name:
            return logger.error('Upgrade can not be triggered because the current node does not own the leader lock')

        return self.ensure_replicas_state(cluster)

    def wait_for_replicas(self, checkpoint_lsn) -> bool:
        if not self.replica_connections:
            return True

        logger.info('Waiting for replica nodes to catch up with primary')

        query = ("SELECT pg_catalog.pg_{0}_{1}_diff(pg_catalog.pg_last_{0}_replay_{1}(),"
                 " '0/0')::bigint").format(self.current_pg.wal_name, self.current_pg.lsn_name)

        status = {}

        for _ in polling_loop(60):
            synced = True
            for name, (_, cur) in self.replica_connections.items():
                prev = status.get(name)
                if prev and prev >= checkpoint_lsn:
                    continue

                cur.execute(query)
                lsn = cur.fetchone()[0]
                status[name] = lsn

                if lsn < checkpoint_lsn:
                    synced = False

            if synced:
                logger.info('All replicas are ready')
                return True

        for name in self.replica_connections.keys():
            lsn = status.get(name)
            if not lsn or lsn < checkpoint_lsn:
                logger.error('Node %s did not catch up. Lag=%s', name, checkpoint_lsn - lsn)
        return False

    def checkpoint_member(self, member):
        name, (_, cur) = member
        try:
            cur.execute('CHECKPOINT')
            return name, True
        except Exception as e:
            logger.error('CHECKPOINT on % failed: %r', name, e)
            return name, False

    def do_run_rsync_from_primary(self, primary_ip: str):
        with self.sync_cv:
            self.sync_result = None

        if self.current_pg.get_cluster_version() == self.desired_version:
            return

        if not self.current_pg.stop(block_callbacks=True):
            logger.error('Failed to stop the cluster before rsync')
            return

        #FIXME: figure out proper abstraction for this
        from .upgrade_plugins.replicarsync import ReplicaRsync

        rsync = cast(ReplicaRsync, self.replica_upgrade)

        success = rsync.perform_rsync(self.current_pg, self.target_pg, primary_ip)

        with self.sync_cv:
            self.sync_result = success
            self.sync_cv.notify_all()

        if success:
            self.ha.update_postgres(self.target_pg)
            node_to_follow = self.ha._get_node_to_follow(self.ha.cluster)
            self.target_pg.follow(node_to_follow, PostgresqlRole.REPLICA)

    def get_rsync_completion_status(self, timeout: int) -> ReplicaSyncStatus:
        with self.sync_cv:
            if self.sync_result is None:
                self.sync_cv.wait(timeout)
            if self.sync_result is None:
                return ReplicaSyncStatus.RUNNING
            return ReplicaSyncStatus.SUCCESS if self.sync_result else ReplicaSyncStatus.FAILED


class AnalyzeJob(Thread):
    def __init__(self, postgresql: Postgresql):
        self.postgresql = postgresql
        self._statistics = None
        super(AnalyzeJob, self).__init__()

    def run(self):
        self.analyze()

    def analyze(self):
        try:
            self.reset_custom_statistics_target()
        except Exception as e:
            logger.error('Failed to reset custom statistics targets: %r', e)
        self.postgresql.analyze(in_stages=True)
        try:
            self.restore_custom_statistics_target()
        except Exception as e:
            logger.error('Failed to restore custom statistics targets: %r', e)

    def reset_custom_statistics_target(self):
        from patroni.postgresql.connection import get_connection_cursor

        logger.info('Resetting non-default statistics target before analyze')
        self._statistics = defaultdict(lambda: defaultdict(dict))

        conn_kwargs = self.postgresql.local_conn_kwargs

        for d in self.postgresql.query('SELECT datname FROM pg_catalog.pg_database WHERE datallowconn'):
            conn_kwargs['dbname'] = d[0]
            with get_connection_cursor(**conn_kwargs) as cur:
                cur.execute('SELECT attrelid::regclass, quote_ident(attname), attstattarget '
                            'FROM pg_catalog.pg_attribute WHERE attnum > 0 AND NOT attisdropped AND attstattarget > 0')
                for table, column, target in cur.fetchall():
                    query = 'ALTER TABLE {0} ALTER COLUMN {1} SET STATISTICS -1'.format(table, column)
                    logger.info("Executing '%s' in the database=%s. Old value=%s", query, d[0], target)
                    cur.execute(query)
                    self._statistics[d[0]][table][column] = target

    def reanalyze(self):
        from patroni.postgresql.connection import get_connection_cursor

        if not self._statistics:
            return

        conn_kwargs = self.postgresql.local_conn_kwargs

        for db, val in self._statistics.items():
            conn_kwargs['dbname'] = db
            with get_connection_cursor(**conn_kwargs) as cur:
                for table in val.keys():
                    query = 'ANALYZE {0}'.format(table)
                    logger.info("Executing '%s' in the database=%s", query, db)
                    try:
                        cur.execute(query)
                    except Exception:
                        logger.error("Failed to execute '%s'", query)

    def restore_custom_statistics_target(self):
        from patroni.postgresql.connection import get_connection_cursor

        if not self._statistics:
            return

        conn_kwargs = self.postgresql.local_conn_kwargs

        logger.info('Restoring default statistics targets after upgrade')
        for db, val in self._statistics.items():
            conn_kwargs['dbname'] = db
            with get_connection_cursor(**conn_kwargs) as cur:
                for table, val in val.items():
                    for column, target in val.items():
                        query = 'ALTER TABLE {0} ALTER COLUMN {1} SET STATISTICS {2}'.format(table, column, target)
                        logger.info("Executing '%s' in the database=%s", query, db)
                        try:
                            cur.execute(query)
                        except Exception:
                            logger.error("Failed to execute '%s'", query)