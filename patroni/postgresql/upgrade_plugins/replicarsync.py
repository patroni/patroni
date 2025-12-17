import json
import logging
import os
import shutil
import subprocess
import typing
from json import JSONDecodeError
from pathlib import Path
from typing import Optional, List, Dict

from . import ReplicaUpgradePlugin
from .. import Postgresql
from ..upgrade import UpgradeFailure, ReplicaSyncStatus
from ...dcs import Member
from ...utils import polling_loop, remove_directory_if_exists

if typing.TYPE_CHECKING: #  pragma: nocover
    from ...ha import Ha

logger = logging.getLogger(__name__)

def common_prefix(path1: str, path2: str) -> Path:
    p1 = Path(path1).absolute()
    p2 = Path(path2).absolute()
    return Path(*(
        a for a, b in zip(p1.parts, p2.parts)
        if a == b
    ))

class ReplicaRsync(ReplicaUpgradePlugin):
    def __init__(self,
                 conf_dir: str,
                 feedback_dir: Optional[str]=None,
                 port: int=5432,
                 timeout: int=300,):
        self.rsyncd = None
        self.rsyncd_configs_created = False
        self.rsyncd_conf_dir = conf_dir
        self.rsyncd_feedback_dir = feedback_dir if feedback_dir is not None else os.path.join(self.rsyncd_conf_dir, 'feedback')
        self.rsyncd_conf = os.path.join(self.rsyncd_conf_dir, 'rsyncd.conf')
        self.rsync_port = port
        self.rsync_timeout = timeout

        self.rsyncd_started = False

        self.primary_ip = None

    def _create_rsyncd_configs(self, current_pg: Postgresql, target_pg: Postgresql, replicas: typing.Iterable[Member]):
        self.rsyncd_configs_created = True

        if not os.path.exists(self.rsyncd_feedback_dir):
            os.makedirs(self.rsyncd_feedback_dir)

        secrets_file = os.path.join(self.rsyncd_conf_dir, 'rsyncd.secrets')

        auth_users = ','.join(replica.name for replica in replicas)
        replica_ips = ','.join(replica.conn_kwargs().get('host') for replica in replicas)

        data_dir_parent = self.get_data_dir_parent(current_pg, target_pg)

        with open(self.rsyncd_conf, 'w') as f:
            f.write("""port = {0}
use chroot = false

[pgroot]
path = {1}
read only = true
timeout = 300
post-xfer exec = echo $RSYNC_EXIT_STATUS > {2}/$RSYNC_USER_NAME
auth users = {3}
secrets file = {4}
hosts allow = {5}
hosts deny = *
""".format(self.rsync_port, data_dir_parent,
           self.rsyncd_feedback_dir, auth_users, secrets_file, replica_ips))

        with open(secrets_file, 'w') as f:
            for replica in replicas:
                f.write('{0}:{1}\n'.format(replica.name, current_pg.config.replication['password']))
        os.chmod(secrets_file, 0o600)

    @staticmethod
    def get_data_dir_parent(current_pg: Postgresql, target_pg: Postgresql) -> Path:
        if current_pg.has_versioned_data_dir:
            data_dir_parent = common_prefix(current_pg.data_dir, target_pg.data_dir)
        else:
            data_dir_parent = os.path.dirname(os.path.abspath(current_pg.data_dir))
        return data_dir_parent

    def after_primary_stop(self, current_pg: Postgresql, target_pg: Postgresql, replicas: Dict[str, Member]):
        self._create_rsyncd_configs(current_pg, target_pg, replicas.values())
        self.rsyncd = subprocess.Popen(['rsync', '--daemon', '--no-detach', '--config=' + self.rsyncd_conf])
        self.rsyncd_started = True

    def before_pg_upgrade(self, current_pg: Postgresql, target_pg: Postgresql, replicas: Dict[str, Member]):
        if not (self.rsyncd.pid and self.rsyncd.poll() is None):
            raise UpgradeFailure('Failed to start rsyncd')

    def before_primary_start(self, ha: 'Ha', leader: Member, replicas: Dict[str, Member]) -> List[str]:
        logger.info('Notifying replicas %s to start rsync', ','.join(replicas))

        primary_ip = leader.conn_kwargs().get('host')

        status = {name: 'not started' for name in replicas}

        # TODO: execute in parallel
        for member in replicas.values():
            body = {'primary_ip': primary_ip}
            try:
                response = ha.patroni.request(member, 'POST', 'rsync', body, timeout=10, retries=2)
                if response.status != 200:
                    logger.error("Error starting rsync on %s: %s", member.name, response.data)
                status[member.name] = 'started'
            except Exception as e:
                logger.exception(f"Failed to start rsync on {member.name}", member.name, e)

        logger.info('Waiting for replicas %s rsync to complete',
                    ', '.join(k for k, v in status.items() if v == 'started'))
        for member in replicas.values():
            if status[member.name] == 'started':
                for i in polling_loop(self.rsync_timeout, 10):
                    response = ha.patroni.request(member, 'GET', 'rsync', timeout=10, retries=2)
                    if response.status != 200:
                        continue
                    try:
                        response_data = json.loads(response.data)
                    except JSONDecodeError:
                        logger.error('Invalid JSON response from member %s: %s', member.name,  response.data)
                        status[member.name] = 'failed'
                        break

                    try:
                        rsync_result = ReplicaSyncStatus(response_data.get('status'))
                    except ValueError:
                        logger.error("Invalid status response from member %s: %s", member.name, response_data.get('status'))
                        status[member.name] = 'failed'
                        break

                    if rsync_result == ReplicaSyncStatus.FAILED:
                        logger.error("Upgrading replica %s has failed", member.name)
                        status[member.name] = 'failed'
                        break
                    elif rsync_result == ReplicaSyncStatus.SUCCESS:
                        logger.info("Upgrade of replica %s is finished", member.name)
                        status[member.name] = 'success'
                        break
                    elif rsync_result == ReplicaSyncStatus.NOT_RUNNING:
                        logger.info("Upgrade of replica %s did not start", member.name)
                        status[member.name] = 'failed'
                        break
                    elif rsync_result == ReplicaSyncStatus.RUNNING:
                        if i > 0:
                            logger.info("Upgrade of replica %s is still running", member.name)
                        continue
                else:
                    logger.error("Upgrade of replica %s timed out", member.name)
                    status[member.name] = 'timeout'

        self._stop_rsyncd()

        return [name for name, result in status.items() if result == 'success']

    def _stop_rsyncd(self):
        if self.rsyncd_started:
            logger.info('Stopping rsyncd')
            try:
                self.rsyncd.kill()
                self.rsyncd_started = False
            except Exception as e:
                logger.error('Failed to kill rsyncd: %r', e)
                return

        if self.rsyncd_configs_created and os.path.exists(self.rsyncd_conf_dir):
            try:
                shutil.rmtree(self.rsyncd_conf_dir)
                self.rsyncd_configs_created = False
            except Exception as e:
                logger.error('Failed to remove %s: %r', self.rsyncd_conf_dir, e)

    def perform_rsync(self, current_pg: Postgresql, target_pg: Postgresql, primary_ip: str) -> bool:
        # TODO: record state outside of data dir for crash recovery

        if Path(target_pg.data_dir).resolve().parent != Path(current_pg.data_dir).resolve().parent:
            raise UpgradeFailure(f"Data dir parent directories are not same: {current_pg.data_dir}, {target_pg.data_dir}")

        if not current_pg.has_versioned_data_dir:
            target_pg.switch_pgdata(current_pg)

        data_dir_parent = self.get_data_dir_parent(current_pg, target_pg)
        current_subdir = Path(current_pg.data_dir).absolute().relative_to(data_dir_parent)
        target_subdir = Path(target_pg.data_dir).absolute().relative_to(data_dir_parent)

        env = os.environ.copy()
        env['RSYNC_PASSWORD'] = current_pg.config.replication['password']

        exclude_subdirs = ['pg_xlog', 'pg_wal', 'log']
        excludes = [
            f'--exclude={'/' / data_dir / sub_dir / '**'}'
            for data_dir in [current_subdir, target_subdir]
            for sub_dir in exclude_subdirs
        ]
        rsync_prefix = f'rsync://{current_pg.name}@{primary_ip}:{self.rsync_port}/pgroot'

        cmd = ['rsync', '-v',
                        '--archive', '--delete', '--hard-links', '--size-only', '--omit-dir-times',
                        '--no-inc-recursive',] + \
                        excludes + \
                       [f'{rsync_prefix}/{current_subdir}',
                        f'{rsync_prefix}/{target_subdir}',
                        str(data_dir_parent)]
        logger.info('Starting rsync: '+' '.join(cmd))

        if subprocess.call(cmd, env=env) != 0:
            logger.error('Failed to rsync from %s', primary_ip)
            if current_pg.has_versioned_data_dir:
                current_pg.switch_pgdata(target_pg, suffix="_new")
            # XXX: rollback configs?
            return False

        target_pg.update_state(version=target_pg.major_version)

        #TODO: self.plugins.post_upgrade()

        #TODO: check that we got the right sysid
        #TODO: check that primary is now up?
        #TODO: check that standby comes up on the new data dir before removing old one

        remove_directory_if_exists(current_pg.data_dir)
        return True
