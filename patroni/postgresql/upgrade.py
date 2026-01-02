import datetime
import logging
import os
import sys
import time
from collections import defaultdict
from enum import Enum
from multiprocessing.pool import ThreadPool
from threading import Condition, Thread
from typing import Optional, Dict, Any, Iterable, cast, List


from .upgrade_plugins import ReplicaUpgradePlugin, UpgradePreparePlugin, UpgradePluginManager
from .. import psycopg
from ..dcs import Cluster, Member, Upgrade
from ..exceptions import PatroniException, DCSError, PostgresConnectionException
from ..utils import polling_loop
from . import Postgresql, DataDirStatus
from .misc import postgres_major_version_to_int, format_major_version, PostgresqlRole

logger = logging.getLogger(__name__)

class UpgradeFailure(PatroniException):
    pass

class UpgradeConfigError(UpgradeFailure):
    pass

class UpgradeBug(PatroniException):
    pass

def safety_assert(condition: bool):
    """Validate a condition that should be true if the code is correct.

    Raises UpgradeBug exception if condition is not true. Used when violation of an invariant
    could cause data loss.
    """
    if not condition:
        raise UpgradeBug('Upgrade safety assertion failed')


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

# Config validation helpers
def expect_int(config: Dict[str, Any], name: str, required=False, default: Optional[int]=None) -> Optional[int]:
    if name not in config:
        if required:
            raise UpgradeConfigError(f'upgrade.{name} is required')
        return default
    value = config[name]
    if not isinstance(value, int):
        raise UpgradeConfigError(f'upgrade.{name} must be an integer')
    return value

def expect_dict(config: Dict[str, Any], name: str, required: bool=False, default: Optional[dict]=None) -> Optional[dict]:
    if name not in config:
        if required:
            raise UpgradeConfigError(f'upgrade.{name} is required')
        return default
    value = config[name]
    if not isinstance(value, dict):
        raise UpgradeConfigError(f'upgrade.{name} must be a mapping')
    return value

def expect_str(config: Dict[str, Any], name: str, required: bool=False, default: Optional[str]=None) -> Optional[str]:
    if name not in config:
        if required:
            raise UpgradeConfigError(f'upgrade.{name} is required')
        return default
    value = config[name]
    if not isinstance(value, str):
        raise UpgradeConfigError(f'upgrade.{name} must be a string')
    return value

class InplaceUpgrade(object):

    def __init__(self, postgresql: Postgresql, ha: 'Ha', dcs_state: Upgrade):
        self.dcs_state = dcs_state
        self.config = dcs_state.config

        self.initiator = dcs_state.initiator
        self.source_version = dcs_state.source_version
        self.shutdown_lsn = dcs_state.shutdown_lsn
        self.downtime_start = dcs_state.downtime_start

        self.current_pg: Postgresql = postgresql
        self.pre_upgrade_datadir: str = postgresql.data_dir


        self.cluster_version = self.current_pg.get_cluster_version()
        # TODO: add plugin API for discovering desired version from environment
        # TODO: add API to validate desired version
        self.desired_version = dcs_state.target_version

        temp_datadir = self.current_pg.data_dir + '_new' if not postgresql.has_versioned_data_dir else None

        # FIXME: handle case where new data_dir already exists and has wrong major version in it

        self.target_pg: Postgresql = postgresql.new_version(dcs_state.target_version, temp_datadir)

        self.ha = ha


        self.replica_upgrade = self._get_replica_upgrade_plugin(self.config)
        self.plugins = UpgradePluginManager(self.config)

        self.sync_cv = Condition()
        self.sync_result: Optional[bool] = None
        self.sync_running: bool = False

        self.replica_connections = {}
        self.members: Optional[Dict[str, Member]] = None

        self.maximum_lag: int = expect_int(self.config, 'maximum_lag', default=16*1024*1024)
        self.parameters: Optional[Dict[str, str]] = expect_dict(self.config, 'parameters')
        self.upgrade_dir: Optional[str] = expect_str(self.config, 'upgrade_dir')
        self.start_timeout: int = expect_int(self.config, 'start_timeout', default=300)

        self.all_replicas_successful = True

        self.recovery_attempts = 0

    @classmethod
    def start_new(cls, postgresql: Postgresql, ha: 'Ha', config: Dict, desired_version: str):
        return cls(postgresql, ha, dcs_state=Upgrade(
            initiator=postgresql.name,
            state=UpgradeState.TRANSIENT,
            source_sysid=postgresql.sysid,
            source_version=format_major_version(postgresql.major_version),
            target_sysid=None,
            target_version=desired_version,
            shutdown_lsn=None,
            downtime_start=None,
            config=config,
            progress=[]
        ))

    @classmethod
    def check_config(cls, config: Dict) -> Iterable[str]:
        if not isinstance(config, dict):
            yield f"Upgrade config is not a dict"
            return

    def _get_replica_upgrade_plugin(self, config: dict) -> ReplicaUpgradePlugin:
        method_name = config.get('replica_upgrade_method', 'rsync')
        if not isinstance(method_name, str):
            raise ValueError('replica_sync_method must be a string')

        args = config.get(method_name, {})

        return ReplicaUpgradePlugin.get_plugin(method_name, args)

    def update_dcs(self, event: Optional[str] = None, **kwargs):
        logger.info("Updating DCS data %s: %s", ', '.join(f"{k}={v}" for k,v in kwargs.items()), event)
        prev_state = self.dcs_state.state
        self.dcs_state = self.dcs_state._replace(**kwargs)
        if event:
            self.dcs_state.progress.append((datetime.datetime.now(datetime.timezone.utc).isoformat(),
                                            prev_state.value,
                                            event))
        self.ha.dcs.write_status({'upgrade': self.dcs_state._asdict()})

    def do_upgrade(self, check_only=False):
        try:
            if not self._do_upgrade(check_only):
                self.recover_failed_upgrade()
        except UpgradeFailure as e:
            logger.error("Upgrade failure: %s", e)
            self.handle_failed_upgrade()
        except PostgresConnectionException as e:
            logger.error("Aborting upgrade due to PostgreSQL connection failure: %s", e)
        except DCSError as e:
            logger.error("DCS communication error while upgrading, aborting upgrade.")
        except Exception as e:
            logger.exception("Possible bug: unknown error while upgrading, aborting upgrade.")

    def _do_upgrade(self, check_only: bool):
        if not float(self.cluster_version) < float(self.desired_version):
            self.update_dcs('Current version={}, desired version={}. Upgrade is not required'.format(
                            self.cluster_version, self.desired_version),
                            state=UpgradeState.SUCCESS)
            return True

        if not (self.current_pg.is_running() and self.current_pg.is_primary()):
            return self.failure('PostgreSQL is not running or in recovery')

        # Set state in DCS
        self.update_dcs('Upgrade started', state=UpgradeState.PRECHECK)

        # Should we get the cluster object from the HA loop itself?
        cluster: Cluster = self.ha.dcs.get_cluster()

        if not self.sanity_checks(cluster):
            return False

        logger.info(f"Starting upgrade with {len(self.members)} replicas")

        self.update_dcs('Upgrade pre checks complete', state=UpgradeState.PREPARE)

        logger.info('Cluster %s is ready to be upgraded', self.current_pg.scope)
        if self.target_pg.data_dir == self.pre_upgrade_datadir:
            logger.error('Upgrade directory mismatch %s == %s, aborting', self.target_pg.data_dir, self.pre_upgrade_datadir)
            safety_assert(False)
        if not self.target_pg.prepare_new_pgdata(self.current_pg):
            return self.failure('New version initdb failed')

        self.update_dcs('New data directory initialized')

        # TODO: implement this hook for stuff that is needed to make pg_upgrade check pass
        # Should it happen before preparing new pgdata
        #self.plugins.prepare_precheck(self.current_pg)

        if not self.target_pg.pg_upgrade(self.current_pg, check=True,
                                         upgrade_dir=self.upgrade_dir,
                                         parameters=self.parameters):
            return self.failure('pg_upgrade --check failed, check upgrade directory for details',
                                self.current_pg.data_dir)

        self.update_dcs('pg_upgrade --check completed')

        if check_only:
            self.update_dcs('Check successfully executed', state=UpgradeState.CHECKED)
            return True

        self.plugins.prepare(self.current_pg)

        self.downtime_start = time.time()
        self.update_dcs('Cluster prepared for upgrade', state=UpgradeState.UPGRADE,
                        downtime_start=self.downtime_start)

        logger.info('Doing a clean shutdown of the cluster before pg_upgrade')
        if not self.current_pg.stop(block_callbacks=True):
            return logger.error('Failed to stop the cluster before pg_upgrade')

        self.update_dcs('Primary stopped')

        # Start syncing daemon if necessary
        self.replica_upgrade.after_primary_stop(self.current_pg, self.target_pg, self.members)

        self.shutdown_lsn = self._wait_for_shutdown_replication()

        self.update_dcs('Shutdown checkpoint has been replicated', shutdown_lsn=self.shutdown_lsn)

        if self.replica_connections:
            failed_replicas = self._init_replica_upgrade()
            self.replica_upgrade.before_pg_upgrade(self.current_pg, self.target_pg, self.members)

        # Mark data directory as potentially upgraded
        self.target_pg.update_state(self.current_pg.major_version, DataDirStatus.UPGRADING)

        if not self.target_pg.pg_upgrade(self.current_pg):
            return self.failure('Failed to upgrade cluster from %s to %s', self.cluster_version, self.desired_version)

        if not self.current_pg.has_versioned_data_dir:
            self.target_pg.update_state(self.target_pg.major_version, DataDirStatus.SWAPPING)
            self.target_pg.switch_pgdata(self.current_pg)

        # Consider new data directory to be the primary version
        self.target_pg.update_state(self.target_pg.major_version, DataDirStatus.READY)

        self.update_dcs('pg_upgrade completed')

        # TODO: need a hook here for backup adjustments, or something that needs to run before postgres starts?

        self.update_dcs('Starting replica upgrades', state=UpgradeState.UPGRADE_REPLICAS)

        self.all_replicas_successful = True
        if self.replica_connections:
            self.remove_failed_replicas(failed_replicas())

        leader = cluster.get_member(self.current_pg.name)

        upgraded_replicas = self.replica_upgrade.before_primary_start(self.ha, leader, self.members)
        self.remove_failed_replicas(set(self.members) - set(upgraded_replicas))

        #TODO: should we roll back upgrade if replicas failed to upgrade?

        self.switch_over_to_target_pg()

        self.update_dcs('Replicas upgraded', state=UpgradeState.POST_UPGRADE)

        return self.do_post_upgrade()

    def do_post_upgrade(self) -> bool:
        logger.info('Starting the primary PostgreSQL')
        if not self.target_pg.is_running():  # Might be running if we are recovering from a failed attempt
            if not self.try_start_target_pg_as_primary():
                return False

        logger.info('Upgrade downtime: %s', time.time() - self.downtime_start)

        self.plugins.post_upgrade(self.target_pg)

        # start analyze early
        analyze_thread = AnalyzeJob(self.target_pg)
        analyze_thread.start()

        self.update_dcs('Upgrade complete', state=UpgradeState.SUCCESS)

        if self.wait_for_replica_start():
            self.all_replicas_successful = False

        analyze_thread.join()
        analyze_thread.reanalyze()

        logger.info('Total upgrade time (with analyze): %s', time.time() - self.downtime_start)
        self.target_pg.bootstrap.call_post_bootstrap(self.ha.patroni.config['bootstrap'])
        self.current_pg.remove_data_directory()

        # TODO: add hook point for fire and forget actions after upgrade, e.g. backups

        if not self.all_replicas_successful:
            logger.warning('Not all replicas were upgraded successfully')

        return True

    def try_start_target_pg_as_primary(self) -> bool:
        self.target_pg.config.remove_recovery_conf()

        for _ in polling_loop(self.start_timeout, 1):
            start_result = self.target_pg.start()
            if start_result is None:
                while not self.target_pg.check_startup_state_changed():
                    time.sleep(1)
                start_result = self.target_pg.is_running()

            if start_result is False:
                logger.warning("Failed to start primary postgres after upgrade")
                # Retry starting the primary
                continue

            self.update_dcs('Primary started')
            return True
        else:
            logger.error("Failing to start primary postgres after upgrade")
            if self.ha.is_failover_possible():
                # If any replica is online and healthy, then they must have restarted successfully on the new state.
                # Let them try finishing the upgrade.
                self.ha.release_leader_key_voluntarily(self.shutdown_lsn)
            # Keep upgrade in current state, HA loop will retry
            return False

    def switch_over_to_target_pg(self):
        if not self.ha.dcs.initialize(create_new=False, sysid=self.target_pg.sysid):
            raise UpgradeFailure('Updating initialize key failed')
        # We are now committed to starting on the new folder
        self.ha.update_postgres(self.target_pg)

    def remove_failed_replicas(self, failed_replicas: Iterable[str]):
        for failed in failed_replicas:
            self.all_replicas_successful = False
            self.replica_connections.pop(failed, None)
            self.members.pop(failed, None)
            self.update_dcs(f'Replica {failed} upgrade failed')

    def wait_for_replica_start(self) -> Iterable[str]:
        if not self.members:
            return []
        started = set()
        failed = set()

        # TODO: check if this is the correct time and place for this
        self.target_pg.slots_handler.sync_replication_slots(self.ha.cluster, self.ha.patroni)

        for _ in polling_loop(self.start_timeout, 1):
            for member_name, member in self.members.items():
                if member_name not in started and member_name not in failed:
                    status = self.ha.fetch_node_status(member)
                    if status.reachable:
                        if 'server_version' in status.data:
                            if status.data['server_version'] >= postgres_major_version_to_int(self.desired_version):
                                started.add(member_name)
                            else:
                                logger.warning("Unexpected version %s received from %s",
                                               status.data['server_version'], member_name)
                                failed.add(member_name)
            if started | failed >= self.members.keys():
                break
        else:
            timed_out = set(self.members.keys()) - started
            logger.warning('Timed out waiting for replicas %s to start up', ', '.join(timed_out))
            failed.update(timed_out)
        return list(failed)

    def should_recover(self) -> bool:
        # The node that started the upgrade is always eligible to recover it.
        if self.initiator == self.current_pg.name:
            return True

        # If there is no shutdown LSN recorded the upgrade can't have started and any
        if not self.shutdown_lsn:
            return True

        # If either data dir has latest checkpoint then recover
        # TODO: recover data dir state based on state file first.
        for pg in [self.current_pg, self.target_pg]:
            checkpoint_lsn = pg.is_running() and pg.last_operation()
            if not checkpoint_lsn:
                data = pg.controldata()
                if data:
                    checkpoint_lsn, _ = self.current_pg.latest_checkpoint_locations(data)
            if checkpoint_lsn == self.shutdown_lsn:
                return True

        return False

    def too_many_failures(self) -> bool:
        return self.recovery_attempts > 3

    def recover_failed_upgrade(self):
        self.recovery_attempts += 1
        logger.info("Recovering failed upgrade, attempt %s", self.recovery_attempts)
        if self.dcs_state.state in (UpgradeState.TRANSIENT, UpgradeState.PRECHECK):
            self.update_dcs(state=UpgradeState.FAILED)
            return

        if self.dcs_state.state == UpgradeState.UPGRADE:
            # In upgrade state, leader data dir state needs to be resolved. Other instances will have old data.
            if self.initiator == self.current_pg.name:
                try:
                    if self.handle_failed_upgrade():
                        self.update_dcs("Rolling forward upgrade", state=UpgradeState.UPGRADE_REPLICAS)
                except UpgradeFailure:
                    safety_assert(not self.current_pg.is_running())
                    logger.error("Failed to recover failed upgrade, releasing leader key")
                    self.ha.release_leader_key_voluntarily()
                    raise
            else:
                safety_assert(self.current_pg.get_cluster_version() == self.source_version)
            self.update_dcs("Rolling back upgrade", state=UpgradeState.ROLLBACK_PREPARE)

        if self.dcs_state.state == UpgradeState.UPGRADE_REPLICAS:
            if self.initiator == self.current_pg.name:
                # Data dir on the primary is ugpraded
                if self.ha.state_handler != self.target_pg:
                    self.resume_replica_upgrade()
                    self.switch_over_to_target_pg()
                self.update_dcs('Replicas upgraded', state=UpgradeState.POST_UPGRADE)
            else:
                # I might have old or new data dir, read state from disk
                state = self.current_pg.read_state_file()

                if state['status'] in [DataDirStatus.SYNC_SWAP, DataDirStatus.SYNC_SWAP_BACK]:
                    # Rsync not executed yet
                    safety_assert(not self.current_pg.has_versioned_data_dir)
                    #TODO
                    raise UpgradeFailure('Recovering from failed rsync not implemented')
                if state['status'] == DataDirStatus.SYNCING:
                    self.replica_upgrade.rollback_failed(self.current_pg, self.target_pg)
                    self.update_dcs("Rolling back", state=UpgradeState.ROLLBACK_PREPARE)
                if state['status'] == DataDirStatus.READY:
                    if state['version'] == self.desired_version:
                        self.update_dcs(f"Replica {self.current_pg.name} has new version")
                        if not self.current_pg.has_versioned_data_dir:
                            # Update data dir paths to correspond to state on disk
                            self.target_pg.switch_pgdata(self.current_pg, move_source=False, move_target=False)
                        if self.target_pg.is_running():
                            safety_assert(not self.target_pg.is_primary())
                            self.target_pg.stop()

                        self.switch_over_to_target_pg()
                        self.resume_replica_upgrade()
                        self.update_dcs("Rolling forward", state=UpgradeState.POST_UPGRADE)



        if self.dcs_state.state == UpgradeState.PREPARE:
            self.update_dcs("Rolling back upgrade", state=UpgradeState.ROLLBACK_PREPARE)

        if self.dcs_state.state == UpgradeState.ROLLBACK_PREPARE:
            self.handle_rollback()

        if self.dcs_state.state == UpgradeState.POST_UPGRADE:
            if not self.current_pg.has_versioned_data_dir:
                if self.current_pg.get_cluster_version() == self.desired_version:
                    self.target_pg.switch_pgdata(self.current_pg, move_source=False, move_target=False)

            self.do_post_upgrade()


    def resume_replica_upgrade(self):
        """Resumes replica upgrade process.

        This happens when primary failed during syncing replicas. For replicas that were already syncing
        it is still safe to restart the process as primary has not started yet.
        """
        safety_assert(not self.current_pg.is_running())
        cluster: Cluster = self.ha.dcs.get_cluster()
        self.members = {member.name: member for member in cluster.members if member.name != self.current_pg.name}

        #TODO: check that everyone still has shutdown_lsn

        self.replica_upgrade.after_primary_stop(self.current_pg, self.target_pg, self.members)
        self.replica_upgrade.before_pg_upgrade(self.current_pg, self.target_pg, self.members)
        leader = cluster.get_member(self.current_pg.name)
        upgraded_replicas = self.replica_upgrade.before_primary_start(self.ha, leader, self.members)
        self.remove_failed_replicas(set(self.members) - set(upgraded_replicas))

    def handle_failed_upgrade(self) -> bool:
        """Try to recover from failure in the middle of an upgrade.

        May only be executed on the same node that tried to run pg_upgrade.

        :returns: True if upgrade was successful, False otherwise.
        :raises: UpgradeFailure if unable to recover"""

        state = self.target_pg.read_state_file()
        if state['status'] == DataDirStatus.SWAPPING:
            # Swapping state only happens with non-versioned data dirs
            safety_assert(not self.current_pg.has_versioned_data_dir)

            if self.current_pg.data_dir.endswith('_old'):
                # Swap was completed
                self.target_pg.update_state(self.target_pg.major_version, DataDirStatus.READY)
                return True
            elif os.path.exists(self.current_pg.data_dir):
                # Determine if data dir is the old one or new one
                main_version = self.current_pg.get_cluster_version()
                if main_version == state['version']:
                    # Active data dir is still old version, retry swap
                    self.target_pg.switch_pgdata(self.current_pg)
                    self.target_pg.update_state(self.target_pg.major_version, DataDirStatus.READY)
                    return True
                elif main_version == self.desired_version:
                    # Swap has completed successfully, only update data dirs
                    self.target_pg.switch_pgdata(self.current_pg, move_source=False, move_target=False)
                    self.target_pg.update_state(self.target_pg.major_version, UpgradeState.READY)
                    return True
                else:
                    raise UpgradeFailure("Unexpected PostgreSQL version={} found in {}, expecting {} or {}".format(
                                         main_version, self.current_pg.data_dir, self.desired_version, state['version']))
            else:
                if os.path.exists(self.target_pg.data_dir):
                    # No active data dir, but upgraded data dir present
                    safety_assert(self.target_pg.get_cluster_version() == self.desired_version)
                    self.target_pg.switch_pgdata(self.current_pg, move_target=False)
                    self.target_pg.update_state(self.target_pg.major_version, DataDirStatus.READY)
                    return True
                else:
                    # Should not happen
                    #TODO: check old data dir and try to reuse it
                    raise UpgradeFailure("Cannot find upgraded data directory in {0} or {1}"
                                         .format(self.current_pg.data_dir, self.target_pg.data_dir))
        elif state['status'] == DataDirStatus.UPGRADING:
            if os.path.exists(self.current_pg.pg_control):
                # pg_upgrade failed before main ugprade process
                self.current_pg.update_state(self.current_pg.major_version, DataDirStatus.READY)
                return False
            elif os.path.exists(self.current_pg.pg_control + '.old'):
                # pg_ugprade failed during upgrade, but as we haven't started new version yet, it's safe to revert.
                os.rename(self.current_pg.pg_control + '.old', self.current_pg.pg_control)
                self.current_pg.update_state(self.current_pg.major_version, DataDirStatus.READY)
                return False
            else:
                raise UpgradeFailure("Old data directory {} contains no pg_control or pg_control.old"
                                     .format(self.current_pg.data_dir))
        elif state['status'] == DataDirStatus.READY:
            if state['version'] == self.desired_version:
                return True
            elif state['version'] == self.source_version:
                return False
            else:
                raise UpgradeFailure("Unexpected PostgreSQL version={} in state file, expecting {} or {}".format(
                    state['version'], self.source_version, self.desired_version
                ))
        else:
            self.ha.release_leader_key_voluntarily()
            raise UpgradeFailure("Invalid data dir status {}, failing over.".format(state['status']))


    def handle_rollback(self):
        if os.path.exists(self.target_pg.data_dir):
            # Do not delete data dir if there is something running in it
            safety_assert(not self.target_pg.is_running())
            safety_assert(self.ha.state_handler != self.target_pg)
            # Do not delete data dir if it's the one we started the upgrade with
            safety_assert(self.target_pg.data_dir != self.pre_upgrade_datadir)
            logger.info("Removing new version data directory: %s", self.target_pg.data_dir)
            self.target_pg.remove_data_directory()
        self.update_dcs("Rollback completed", state=UpgradeState.FAILED)

    def failure(self, reason: str, *args) -> bool:
        """Log information about to failure and return False"""
        logger.error(reason, args)
        self.update_dcs(reason % args)
        return False

    def _init_replica_upgrade(self) -> Iterable[str]:
        logger.info('Executing CHECKPOINT on replicas %s', ','.join(self.replica_connections.keys()))
        pool = ThreadPool(len(self.replica_connections))
        # Do CHECKPOINT on replicas in parallel with pg_upgrade.
        # It will reduce the time for shutdown and so downtime.
        results = pool.map_async(self.checkpoint_member, self.replica_connections.items())
        pool.close()

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

        if not self.wait_for_replicas(checkpoint_lsn):
            raise UpgradeFailure("Not all replicas caught up")

        return checkpoint_lsn

    def check_patroni_api(self, member: Member) -> bool:
        # This will run API check with timeout 2 retries 0. Maybe too fragile?
        status = self.ha.fetch_node_status(member)

        if not status.reachable:
            logger.error('Member %s is not reachable', member.name)
            return False

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
                return self.failure('Member %s is not streaming from the primary', member.name)
            if lag > self.maximum_lag:
                return self.failure('Replication lag %s on member %s is too high', lag, member.name)

            if not self.check_patroni_api(member):
                return self.failure('Patroni on %s is not healthy', member.name)

            conn_kwargs = member.conn_kwargs(self.current_pg.config.superuser)
            conn_kwargs['options'] = '-c statement_timeout=0 -c search_path='
            conn_kwargs.pop('connect_timeout', None)

            try:
                conn = psycopg.connect(**conn_kwargs)
                conn.autocommit = True
                cur = conn.cursor()
                cur.execute('SELECT pg_catalog.pg_is_in_recovery()')
                if not cur.fetchone()[0]:
                    return self.failure('Member %s is not running as replica!', member.name)
            except psycopg.Error as exc:
                return self.failure("Connecting to member %s failed: %s", member.name, exc)
            self.replica_connections[member.name] = (ip, cur)
            self.members[member.name] = member
            return True

        return all(ensure_replica_state(member) for member in cluster.members if member.name != self.current_pg.name)

    def sanity_checks(self, cluster: Cluster) -> Optional[bool]:
        if self.ha.is_standby_cluster():
            return self.failure('Standby clusters cannot be upgraded')

        if not cluster.initialize:
            return self.failure('Upgrade can not be triggered because the cluster is not initialized')

        if self.ha.is_paused():
            return self.failure('Upgrade can not be triggered because Patroni is in maintenance mode')

        lock_owner = cluster.leader and cluster.leader.name
        if lock_owner != self.current_pg.name:
            return self.failure('Upgrade can not be triggered because the current node does not own the leader lock')

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
                self.update_dcs('All replicas are ready')
                return True

        for name in self.replica_connections.keys():
            lsn = status.get(name)
            if not lsn or lsn < checkpoint_lsn:
                self.failure('Node %s did not catch up. Lag=%s', name, checkpoint_lsn - lsn)
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

        #TODO: figure out proper abstraction for this
        from .upgrade_plugins.replicarsync import ReplicaRsync

        rsync = cast(ReplicaRsync, self.replica_upgrade)

        success = rsync.perform_rsync(self.current_pg, self.target_pg, primary_ip)

        with self.sync_cv:
            self.sync_result = success
            self.sync_cv.notify_all()

        if success:
            self.ha.update_postgres(self.target_pg)
            node_to_follow = self.ha._get_node_to_follow(self.ha.cluster)
            # TODO: run in background to save a bit of time.
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
        except psycopg.Error as e:
            logger.error('Failed to reset custom statistics targets: %r', e)

        self.postgresql.analyze(in_stages=True, missing_stats_only=True)
        try:
            self.restore_custom_statistics_target()
        except psycopg.Error as e:
            logger.error('Failed to restore custom statistics targets: %r', e)

    def reset_custom_statistics_target(self):
        from patroni.postgresql.connection import get_connection_cursor

        logger.info('Resetting non-default statistics target before analyze')
        self._statistics = defaultdict(lambda: defaultdict(dict))

        conn_kwargs = self.postgresql.local_conn_kwargs

        # TODO: store statistics in DCS state so it can be rolled back by others if this node fails during upgrade
        for d in self.postgresql.query('SELECT datname FROM pg_catalog.pg_database WHERE datallowconn'):
            conn_kwargs['dbname'] = d[0]
            with get_connection_cursor(**conn_kwargs) as cur:
                query = ('SELECT attrelid::regclass, quote_ident(attname), attstattarget '
                         'FROM pg_catalog.pg_attribute a WHERE attnum > 0 AND NOT attisdropped AND attstattarget > 0')
                if self.postgresql.major_version >= 180000:
                    query += (' AND NOT EXISTS (SELECT NULL FROM pg_catalog.pg_statistic s\n'
							  ' WHERE s.starelid OPERATOR(pg_catalog.=) a.attrelid\n'
							  ' AND s.staattnum OPERATOR(pg_catalog.=) a.attnum\n'
							  ' AND s.stainherit OPERATOR(pg_catalog.=) '
                              "(SELECT c.relkind IN ('p', 'i') FROM pg_catalog.pg_class c WHERE c.oid = a.attrelid)\n"
                              ')')

                cur.execute(query)
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
                    except psycopg.Error:
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
                        except psycopg.Error:
                            logger.error("Failed to execute '%s'", query)