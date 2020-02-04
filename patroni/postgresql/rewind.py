import logging
import os
import subprocess

from patroni.dcs import Leader
from patroni.postgresql.connection import get_connection_cursor
from patroni.postgresql.misc import parse_history, parse_lsn

logger = logging.getLogger(__name__)

REWIND_STATUS = type('Enum', (), {'INITIAL': 0, 'CHECKPOINT': 1, 'CHECK': 2, 'NEED': 3,
                                  'NOT_NEED': 4, 'SUCCESS': 5, 'FAILED': 6})


class Rewind(object):

    def __init__(self, postgresql):
        self._postgresql = postgresql
        self.reset_state()

    @staticmethod
    def configuration_allows_rewind(data):
        return data.get('wal_log_hints setting', 'off') == 'on' or data.get('Data page checksum version', '0') != '0'

    @property
    def can_rewind(self):
        """ check if pg_rewind executable is there and that pg_controldata indicates
            we have either wal_log_hints or checksums turned on
        """
        # low-hanging fruit: check if pg_rewind configuration is there
        if not self._postgresql.config.get('use_pg_rewind'):
            return False

        cmd = [self._postgresql.pgcommand('pg_rewind'), '--help']
        try:
            ret = subprocess.call(cmd, stdout=open(os.devnull, 'w'), stderr=subprocess.STDOUT)
            if ret != 0:  # pg_rewind is not there, close up the shop and go home
                return False
        except OSError:
            return False
        return self.configuration_allows_rewind(self._postgresql.controldata())

    @property
    def can_rewind_or_reinitialize_allowed(self):
        return self._postgresql.config.get('remove_data_directory_on_diverged_timelines') or self.can_rewind

    def trigger_check_diverged_lsn(self):
        if self.can_rewind_or_reinitialize_allowed and self._state != REWIND_STATUS.NEED:
            self._state = REWIND_STATUS.CHECK

    def check_leader_is_not_in_recovery(self, **kwargs):
        if not kwargs.get('database'):
            kwargs['database'] = self._postgresql.database
        try:
            with get_connection_cursor(connect_timeout=3, options='-c statement_timeout=2000', **kwargs) as cur:
                cur.execute('SELECT pg_catalog.pg_is_in_recovery()')
                if not cur.fetchone()[0]:
                    return True
                logger.info('Leader is still in_recovery and therefore can\'t be used for rewind')
        except Exception:
            return logger.exception('Exception when working with leader')

    def _get_local_timeline_lsn_from_controldata(self):
        timeline = lsn = None
        data = self._postgresql.controldata()
        try:
            if data.get('Database cluster state') == 'shut down in recovery':
                lsn = data.get('Minimum recovery ending location')
                timeline = int(data.get("Min recovery ending loc's timeline"))
                if lsn == '0/0' or timeline == 0:  # it was a master when it crashed
                    data['Database cluster state'] = 'shut down'
            if data.get('Database cluster state') == 'shut down':
                lsn = data.get('Latest checkpoint location')
                timeline = int(data.get("Latest checkpoint's TimeLineID"))
        except (TypeError, ValueError):
            logger.exception('Failed to get local timeline and lsn from pg_controldata output')
        return timeline, lsn

    def _get_local_timeline_lsn(self):
        if self._postgresql.is_running():  # if postgres is running - get timeline and lsn from replication connection
            timeline, lsn = self._postgresql.get_local_timeline_lsn_from_replication_connection()
        else:  # otherwise analyze pg_controldata output
            timeline, lsn = self._get_local_timeline_lsn_from_controldata()
        logger.info('Local timeline=%s lsn=%s', timeline, lsn)
        return timeline, lsn

    def _check_timeline_and_lsn(self, leader):
        local_timeline, local_lsn = self._get_local_timeline_lsn()
        if local_timeline is None or local_lsn is None:
            return

        if isinstance(leader, Leader):
            if leader.member.data.get('role') != 'master':
                return
        # standby cluster
        elif not self.check_leader_is_not_in_recovery(**leader.conn_kwargs(self._postgresql.config.replication)):
            return

        history = need_rewind = None
        try:
            with self._postgresql.get_replication_connection_cursor(**leader.conn_kwargs()) as cur:
                cur.execute('IDENTIFY_SYSTEM')
                master_timeline = cur.fetchone()[1]
                logger.info('master_timeline=%s', master_timeline)
                if local_timeline > master_timeline:  # Not always supported by pg_rewind
                    need_rewind = True
                elif master_timeline > 1:
                    cur.execute('TIMELINE_HISTORY %s', (master_timeline,))
                    history = bytes(cur.fetchone()[1]).decode('utf-8')
                    logger.info('master: history=%s', history)
                else:  # local_timeline == master_timeline == 1
                    need_rewind = False
        except Exception:
            return logger.exception('Exception when working with master via replication connection')

        if history is not None:
            for parent_timeline, switchpoint, _ in parse_history(history):
                if parent_timeline == local_timeline:
                    try:
                        need_rewind = parse_lsn(local_lsn) >= switchpoint
                    except (IndexError, ValueError):
                        logger.exception('Exception when parsing lsn')
                    break
                elif parent_timeline > local_timeline:
                    break

        self._state = need_rewind and REWIND_STATUS.NEED or REWIND_STATUS.NOT_NEED

    def rewind_or_reinitialize_needed_and_possible(self, leader):
        if leader and leader.name != self._postgresql.name and leader.conn_url and self._state == REWIND_STATUS.CHECK:
            self._check_timeline_and_lsn(leader)
        return leader and leader.conn_url and self._state == REWIND_STATUS.NEED

    def check_for_checkpoint_after_promote(self):
        if self._state == REWIND_STATUS.INITIAL and self._postgresql.is_leader() and \
                self._postgresql.get_master_timeline() == self._postgresql.pg_control_timeline():
            self._state = REWIND_STATUS.CHECKPOINT

    def checkpoint_after_promote(self):
        return self._state == REWIND_STATUS.CHECKPOINT

    def pg_rewind(self, r):
        # prepare pg_rewind connection
        env = self._postgresql.config.write_pgpass(r)
        env['PGOPTIONS'] = '-c statement_timeout=0'
        dsn = self._postgresql.config.format_dsn(r, True)
        logger.info('running pg_rewind from %s', dsn)
        try:
            return self._postgresql.cancellable.call([self._postgresql.pgcommand('pg_rewind'), '-D',
                                                      self._postgresql.data_dir, '--source-server', dsn], env=env) == 0
        except OSError:
            return False

    def execute(self, leader):
        if self._postgresql.is_running() and not self._postgresql.stop(checkpoint=False):
            return logger.warning('Can not run pg_rewind because postgres is still running')

        # prepare pg_rewind connection
        r = leader.conn_kwargs(self._postgresql.config.rewind_credentials)

        # 1. make sure that we are really trying to rewind from the master
        # 2. make sure that pg_control contains the new timeline by:
        #   running a checkpoint or
        #   waiting until Patroni on the master will expose checkpoint_after_promote=True
        checkpoint_status = leader.checkpoint_after_promote if isinstance(leader, Leader) else None
        if checkpoint_status is None:  # master still runs the old Patroni
            leader_status = self._postgresql.checkpoint(leader.conn_kwargs(self._postgresql.config.superuser))
            if leader_status:
                return logger.warning('Can not use %s for rewind: %s', leader.name, leader_status)
        elif not checkpoint_status:
            return logger.info('Waiting for checkpoint on %s before rewind', leader.name)
        elif not self.check_leader_is_not_in_recovery(**r):
            return

        if self.pg_rewind(r):
            self._state = REWIND_STATUS.SUCCESS
        elif not self.check_leader_is_not_in_recovery(**r):
            logger.warning('Failed to rewind because master %s become unreachable', leader.name)
        else:
            logger.error('Failed to rewind from healty master: %s', leader.name)

            for name in ('remove_data_directory_on_rewind_failure', 'remove_data_directory_on_diverged_timelines'):
                if self._postgresql.config.get(name):
                    logger.warning('%s is set. removing...', name)
                    self._postgresql.remove_data_directory()
                    self._state = REWIND_STATUS.INITIAL
                    break
            else:
                self._state = REWIND_STATUS.FAILED
        return False

    def reset_state(self):
        self._state = REWIND_STATUS.INITIAL

    @property
    def is_needed(self):
        return self._state in (REWIND_STATUS.CHECK, REWIND_STATUS.NEED)

    @property
    def executed(self):
        return self._state > REWIND_STATUS.NOT_NEED

    @property
    def failed(self):
        return self._state == REWIND_STATUS.FAILED
