import json
import logging
import select
import socket
import time

from kazoo.client import KazooClient, KazooState, KazooRetry
from kazoo.exceptions import ConnectionClosedError, NoNodeError, NodeExistsError, SessionExpiredError
from kazoo.handlers.threading import AsyncResult, SequentialThreadingHandler
from kazoo.protocol.states import KeeperState, WatchedEvent, ZnodeStat
from kazoo.retry import RetryFailedError
from kazoo.security import ACL, make_acl
from typing import Any, Callable, Dict, List, Optional, Union, Tuple, TYPE_CHECKING

from . import AbstractDCS, ClusterConfig, Cluster, Failover, Leader, Member, SyncState, TimelineHistory, citus_group_re
from ..exceptions import DCSError
from ..utils import deep_compare
if TYPE_CHECKING:  # pragma: no cover
    from ..config import Config

logger = logging.getLogger(__name__)


class ZooKeeperError(DCSError):
    pass


class PatroniSequentialThreadingHandler(SequentialThreadingHandler):

    def __init__(self, connect_timeout: Union[int, float]) -> None:
        super(PatroniSequentialThreadingHandler, self).__init__()
        self.set_connect_timeout(connect_timeout)

    def set_connect_timeout(self, connect_timeout: Union[int, float]) -> None:
        self._connect_timeout = max(1.0, connect_timeout / 2.0)  # try to connect to zookeeper node during loop_wait/2

    def create_connection(self, *args: Any, **kwargs: Any) -> socket.socket:
        """This method is trying to establish connection with one of the zookeeper nodes.
           Somehow strategy "fail earlier and retry more often" works way better comparing to
           the original strategy "try to connect with specified timeout".
           Since we want to try connect to zookeeper more often (with the smaller connect_timeout),
           he have to override `create_connection` method in the `SequentialThreadingHandler`
           class (which is used by `kazoo.Client`).

        :param args: always contains `tuple(host, port)` as the first element and could contain
                     `connect_timeout` (negotiated session timeout) as the second element."""

        args_list: List[Any] = list(args)
        if len(args_list) == 0:  # kazoo 2.6.0 slightly changed the way how it calls create_connection method
            kwargs['timeout'] = max(self._connect_timeout, kwargs.get('timeout', self._connect_timeout * 10) / 10.0)
        elif len(args_list) == 1:
            args_list.append(self._connect_timeout)
        else:
            args_list[1] = max(self._connect_timeout, args_list[1] / 10.0)
        return super(PatroniSequentialThreadingHandler, self).create_connection(*args_list, **kwargs)

    def select(self, *args: Any, **kwargs: Any) -> Any:
        """
        Python 3.XY may raise following exceptions if select/poll are called with an invalid socket:
        - `ValueError`: because fd == -1
        - `TypeError`: Invalid file descriptor: -1 (starting from kazoo 2.9)
        Python 2.7 may raise the `IOError` instead of `socket.error` (starting from kazoo 2.9)

        When it is appropriate we map these exceptions to `socket.error`.
        """

        try:
            return super(PatroniSequentialThreadingHandler, self).select(*args, **kwargs)
        except (TypeError, ValueError) as e:
            raise select.error(9, str(e))


class PatroniKazooClient(KazooClient):

    def _call(self, request: Tuple[Any], async_object: AsyncResult) -> Optional[bool]:
        # Before kazoo==2.7.0 it wasn't possible to send requests to zookeeper if
        # the connection is in the SUSPENDED state and Patroni was strongly relying on it.
        # The https://github.com/python-zk/kazoo/pull/588 changed it, and now such requests are queued.
        # We override the `_call()` method in order to keep the old behavior.

        if self._state == KeeperState.CONNECTING:
            async_object.set_exception(SessionExpiredError())
            return False
        return super(PatroniKazooClient, self)._call(request, async_object)


class ZooKeeper(AbstractDCS):

    def __init__(self, config: Dict[str, Any]) -> None:
        super(ZooKeeper, self).__init__(config)

        hosts = config.get('hosts', [])
        if isinstance(hosts, list):
            hosts = ','.join(hosts)

        mapping = {'use_ssl': 'use_ssl', 'verify': 'verify_certs', 'cacert': 'ca',
                   'cert': 'certfile', 'key': 'keyfile', 'key_password': 'keyfile_password'}
        kwargs = {v: config[k] for k, v in mapping.items() if k in config}

        if 'set_acls' in config:
            default_acl: List[ACL] = []
            for principal, permissions in config['set_acls'].items():
                normalizedPermissions = [p.upper() for p in permissions]
                default_acl.append(make_acl(scheme='x509',
                                            credential=principal,
                                            read='READ' in normalizedPermissions,
                                            write='WRITE' in normalizedPermissions,
                                            create='CREATE' in normalizedPermissions,
                                            delete='DELETE' in normalizedPermissions,
                                            admin='ADMIN' in normalizedPermissions,
                                            all='ALL' in normalizedPermissions))
            kwargs['default_acl'] = default_acl

        self._client = PatroniKazooClient(hosts, handler=PatroniSequentialThreadingHandler(config['retry_timeout']),
                                          timeout=config['ttl'], connection_retry=KazooRetry(max_delay=1, max_tries=-1,
                                          sleep_func=time.sleep), command_retry=KazooRetry(max_delay=1, max_tries=-1,
                                          deadline=config['retry_timeout'], sleep_func=time.sleep), **kwargs)
        self._client.add_listener(self.session_listener)

        self._fetch_cluster: bool = True
        self._fetch_status: bool = True
        self.__last_member_data: Optional[Dict[str, Any]] = None

        self._orig_kazoo_connect = self._client._connection._connect
        self._client._connection._connect = self._kazoo_connect

        self._client.start()

    def _kazoo_connect(self, *args: Any) -> Tuple[Union[int, float], Union[int, float]]:
        """Kazoo is using Ping's to determine health of connection to zookeeper. If there is no
        response on Ping after Ping interval (1/2 from read_timeout) it will consider current
        connection dead and try to connect to another node. Without this "magic" it was taking
        up to 2/3 from session timeout (ttl) to figure out that connection was dead and we had
        only small time for reconnect and retry.

        This method is needed to return different value of read_timeout, which is not calculated
        from negotiated session timeout but from value of `loop_wait`. And it is 2 sec smaller
        than loop_wait, because we can spend up to 2 seconds when calling `touch_member()` and
        `write_leader_optime()` methods, which also may hang..."""

        ret = self._orig_kazoo_connect(*args)
        return max(self.loop_wait - 2, 2) * 1000, ret[1]

    def session_listener(self, state: str) -> None:
        if state in [KazooState.SUSPENDED, KazooState.LOST]:
            self.cluster_watcher(None)

    def status_watcher(self, event: Optional[WatchedEvent]) -> None:
        self._fetch_status = True
        self.event.set()

    def cluster_watcher(self, event: Optional[WatchedEvent]) -> None:
        self._fetch_cluster = True
        if not event or event.state != KazooState.CONNECTED or event.path.startswith(self.client_path('')):
            self.status_watcher(event)

    def reload_config(self, config: Union['Config', Dict[str, Any]]) -> None:
        self.set_retry_timeout(config['retry_timeout'])

        loop_wait = config['loop_wait']

        loop_wait_changed = self._loop_wait != loop_wait
        self._loop_wait = loop_wait
        if isinstance(self._client.handler, PatroniSequentialThreadingHandler):
            self._client.handler.set_connect_timeout(loop_wait)

        # We need to reestablish connection to zookeeper if we want to change
        # read_timeout (and Ping interval respectively), because read_timeout
        # is calculated in `_kazoo_connect` method. If we are changing ttl at
        # the same time, set_ttl method will reestablish connection and return
        # `!True`, otherwise we will close existing connection and let kazoo
        # open the new one.
        if not self.set_ttl(config['ttl']) and loop_wait_changed:
            self._client._connection._socket.close()

    def set_ttl(self, ttl: int) -> Optional[bool]:
        """It is not possible to change ttl (session_timeout) in zookeeper without
        destroying old session and creating the new one. This method returns `!True`
        if session_timeout has been changed (`restart()` has been called)."""
        ttl = int(ttl * 1000)
        if self._client._session_timeout != ttl:
            self._client._session_timeout = ttl
            self._client.restart()
            return True

    @property
    def ttl(self) -> int:
        return int(self._client._session_timeout / 1000.0)

    def set_retry_timeout(self, retry_timeout: int) -> None:
        retry = self._client.retry if isinstance(self._client.retry, KazooRetry) else self._client._retry
        retry.deadline = retry_timeout

    def get_node(
            self, key: str, watch: Optional[Callable[[WatchedEvent], None]] = None
    ) -> Optional[Tuple[str, ZnodeStat]]:
        try:
            ret = self._client.get(key, watch)
            return (ret[0].decode('utf-8'), ret[1])
        except NoNodeError:
            return None

    def get_status(self, path: str, leader: Optional[Leader]) -> Tuple[int, Optional[Dict[str, int]]]:
        watch = self.status_watcher if not leader or leader.name != self._name else None

        status = self.get_node(path + self._STATUS, watch)
        if status:
            try:
                status = json.loads(status[0])
                last_lsn = status.get(self._OPTIME)
                slots = status.get('slots')
            except Exception:
                slots = last_lsn = None
        else:
            last_lsn = self.get_node(path + self._LEADER_OPTIME, watch)
            last_lsn = last_lsn and last_lsn[0]
            slots = None

        try:
            last_lsn = int(last_lsn or '')
        except Exception:
            last_lsn = 0

        self._fetch_status = False
        return last_lsn, slots

    @staticmethod
    def member(name: str, value: str, znode: ZnodeStat) -> Member:
        return Member.from_node(znode.version, name, znode.ephemeralOwner, value)

    def get_children(self, key: str, watch: Optional[Callable[[WatchedEvent], None]] = None) -> List[str]:
        try:
            return self._client.get_children(key, watch)
        except NoNodeError:
            return []

    def load_members(self, path: str) -> List[Member]:
        members: List[Member] = []
        for member in self.get_children(path + self._MEMBERS, self.cluster_watcher):
            data = self.get_node(path + self._MEMBERS + member)
            if data is not None:
                members.append(self.member(member, *data))
        return members

    def _cluster_loader(self, path: str) -> Cluster:
        self._fetch_cluster = False
        self.event.clear()
        nodes = set(self.get_children(path, self.cluster_watcher))
        if not nodes:
            self._fetch_cluster = True

        # get initialize flag
        initialize = (self.get_node(path + self._INITIALIZE) or [None])[0] if self._INITIALIZE in nodes else None

        # get global dynamic configuration
        config = self.get_node(path + self._CONFIG, watch=self.cluster_watcher) if self._CONFIG in nodes else None
        config = config and ClusterConfig.from_node(config[1].version, config[0], config[1].mzxid)

        # get timeline history
        history = self.get_node(path + self._HISTORY, watch=self.cluster_watcher) if self._HISTORY in nodes else None
        history = history and TimelineHistory.from_node(history[1].mzxid, history[0])

        # get synchronization state
        sync = self.get_node(path + self._SYNC, watch=self.cluster_watcher) if self._SYNC in nodes else None
        sync = SyncState.from_node(sync and sync[1].version, sync and sync[0])

        # get list of members
        members = self.load_members(path) if self._MEMBERS[:-1] in nodes else []

        # get leader
        leader = self.get_node(path + self._LEADER) if self._LEADER in nodes else None
        if leader:
            member = Member(-1, leader[0], None, {})
            member = ([m for m in members if m.name == leader[0]] or [member])[0]
            leader = Leader(leader[1].version, leader[1].ephemeralOwner, member)
            self._fetch_cluster = member.version == -1

        # get last known leader lsn and slots
        last_lsn, slots = self.get_status(path, leader)

        # failover key
        failover = self.get_node(path + self._FAILOVER, watch=self.cluster_watcher) if self._FAILOVER in nodes else None
        failover = failover and Failover.from_node(failover[1].version, failover[0])

        # get failsafe topology
        failsafe = self.get_node(path + self._FAILSAFE, watch=self.cluster_watcher) if self._FAILSAFE in nodes else None
        try:
            failsafe = json.loads(failsafe[0]) if failsafe else None
        except Exception:
            failsafe = None

        return Cluster(initialize, config, leader, last_lsn, members, failover, sync, history, slots, failsafe)

    def _citus_cluster_loader(self, path: str) -> Dict[int, Cluster]:
        fetch_cluster = False
        ret: Dict[int, Cluster] = {}
        for node in self.get_children(path, self.cluster_watcher):
            if citus_group_re.match(node):
                ret[int(node)] = self._cluster_loader(path + node + '/')
                fetch_cluster = fetch_cluster or self._fetch_cluster
        self._fetch_cluster = fetch_cluster
        return ret

    def _load_cluster(
            self, path: str, loader: Callable[[str], Union[Cluster, Dict[int, Cluster]]]
    ) -> Union[Cluster, Dict[int, Cluster]]:
        cluster = self.cluster if path == self._base_path + '/' else None
        if self._fetch_cluster or cluster is None:
            try:
                cluster = self._client.retry(loader, path)
            except Exception:
                logger.exception('get_cluster')
                self.cluster_watcher(None)
                raise ZooKeeperError('ZooKeeper in not responding properly')
        # The /status ZNode was updated or doesn't exist
        elif self._fetch_status and not self._fetch_cluster or not cluster.last_lsn \
                or cluster.has_permanent_logical_slots(self._name, False) and not cluster.slots:
            # If current node is the leader just clear the event without fetching anything (we are updating the /status)
            if cluster.leader and cluster.leader.name == self._name:
                self.event.clear()
            else:
                try:
                    last_lsn, slots = self.get_status(self.client_path(''), cluster.leader)
                    self.event.clear()
                    new_cluster: List[Any] = list(cluster)
                    new_cluster[3] = last_lsn
                    new_cluster[8] = slots
                    cluster = Cluster(*new_cluster)
                except Exception:
                    pass
        return cluster

    def _bypass_caches(self) -> None:
        self._fetch_cluster = True

    def _create(self, path: str, value: bytes, retry: bool = False, ephemeral: bool = False) -> bool:
        try:
            if retry:
                self._client.retry(self._client.create, path, value, makepath=True, ephemeral=ephemeral)
            else:
                self._client.create_async(path, value, makepath=True, ephemeral=ephemeral).get(timeout=1)
            return True
        except Exception:
            logger.exception('Failed to create %s', path)
        return False

    def attempt_to_acquire_leader(self) -> bool:
        try:
            self._client.retry(self._client.create, self.leader_path, self._name.encode('utf-8'),
                               makepath=True, ephemeral=True)
            return True
        except (ConnectionClosedError, RetryFailedError) as e:
            raise ZooKeeperError(e)
        except Exception as e:
            if not isinstance(e, NodeExistsError):
                logger.error('Failed to create %s: %r', self.leader_path, e)
        logger.info('Could not take out TTL lock')
        return False

    def _set_or_create(self, key: str, value: str, version: Optional[int] = None,
                       retry: bool = False, do_not_create_empty: bool = False) -> Union[int, bool]:
        value_bytes = value.encode('utf-8')
        try:
            if retry:
                ret = self._client.retry(self._client.set, key, value_bytes, version=version or -1)
            else:
                ret = self._client.set_async(key, value_bytes, version=version or -1).get(timeout=1)
            return ret.version
        except NoNodeError:
            if do_not_create_empty and not value_bytes:
                return True
            elif version is None:
                if self._create(key, value_bytes, retry):
                    return 0
            else:
                return False
        except Exception:
            logger.exception('Failed to update %s', key)
        return False

    def set_failover_value(self, value: str, version: Optional[int] = None) -> bool:
        return self._set_or_create(self.failover_path, value, version) is not False

    def set_config_value(self, value: str, version: Optional[int] = None) -> bool:
        return self._set_or_create(self.config_path, value, version, retry=True) is not False

    def initialize(self, create_new: bool = True, sysid: str = "") -> bool:
        sysid_bytes = sysid.encode('utf-8')
        return self._create(self.initialize_path, sysid_bytes, retry=True) if create_new \
            else self._client.retry(self._client.set, self.initialize_path, sysid_bytes)

    def touch_member(self, data: Dict[str, Any]) -> bool:
        cluster = self.cluster
        member = cluster and cluster.get_member(self._name, fallback_to_leader=False)
        member_data = self.__last_member_data or member and member.data
        #  We want to notify leader if some important fields in the member key changed by removing ZNode
        if member and (self._client.client_id is not None and member.session != self._client.client_id[0]
                       or not (member_data and deep_compare(member_data.get('tags', {}), data.get('tags', {}))
                               and (member_data.get('state') == data.get('state')
                                    or 'running' not in (member_data.get('state'), data.get('state')))
                               and member_data.get('version') == data.get('version')
                               and member_data.get('checkpoint_after_promote')
                               == data.get('checkpoint_after_promote'))):
            try:
                self._client.delete_async(self.member_path).get(timeout=1)
            except NoNodeError:
                pass
            except Exception:
                return False
            member = None

        encoded_data = json.dumps(data, separators=(',', ':')).encode('utf-8')
        if member and member_data:
            if deep_compare(data, member_data):
                return True
        else:
            try:
                self._client.create_async(self.member_path, encoded_data, makepath=True, ephemeral=True).get(timeout=1)
                self.__last_member_data = data
                return True
            except Exception as e:
                if not isinstance(e, NodeExistsError):
                    logger.exception('touch_member')
                    return False
        try:
            self._client.set_async(self.member_path, encoded_data).get(timeout=1)
            self.__last_member_data = data
            return True
        except Exception:
            logger.exception('touch_member')

        return False

    def take_leader(self) -> bool:
        return self.attempt_to_acquire_leader()

    def _write_leader_optime(self, last_lsn: str) -> bool:
        return self._set_or_create(self.leader_optime_path, last_lsn) is not False

    def _write_status(self, value: str) -> bool:
        return self._set_or_create(self.status_path, value) is not False

    def _write_failsafe(self, value: str) -> bool:
        return self._set_or_create(self.failsafe_path, value) is not False

    def _update_leader(self) -> bool:
        cluster = self.cluster
        session = cluster and isinstance(cluster.leader, Leader) and cluster.leader.session
        if self._client.client_id and self._client.client_id[0] != session:
            logger.warning('Recreating the leader ZNode due to ownership mismatch')
            try:
                self._client.retry(self._client.delete, self.leader_path)
            except NoNodeError:
                pass
            except (ConnectionClosedError, RetryFailedError) as e:
                raise ZooKeeperError(e)
            except Exception as e:
                logger.error('Failed to remove %s: %r', self.leader_path, e)
                return False

            try:
                self._client.retry(self._client.create, self.leader_path,
                                   self._name.encode('utf-8'), makepath=True, ephemeral=True)
            except (ConnectionClosedError, RetryFailedError) as e:
                raise ZooKeeperError(e)
            except Exception as e:
                logger.error('Failed to create %s: %r', self.leader_path, e)
                return False
        return True

    def _delete_leader(self) -> bool:
        self._client.restart()
        return True

    def _cancel_initialization(self) -> None:
        node = self.get_node(self.initialize_path)
        if node:
            self._client.delete(self.initialize_path, version=node[1].version)

    def cancel_initialization(self) -> bool:
        try:
            self._client.retry(self._cancel_initialization)
            return True
        except Exception:
            logger.exception("Unable to delete initialize key")
        return False

    def delete_cluster(self) -> bool:
        try:
            return self._client.retry(self._client.delete, self.client_path(''), recursive=True)
        except NoNodeError:
            return True

    def set_history_value(self, value: str) -> bool:
        return self._set_or_create(self.history_path, value) is not False

    def set_sync_state_value(self, value: str, version: Optional[int] = None) -> Union[int, bool]:
        return self._set_or_create(self.sync_path, value, version, retry=True, do_not_create_empty=True)

    def delete_sync_state(self, version: Optional[int] = None) -> bool:
        return self.set_sync_state_value("{}", version) is not False

    def watch(self, leader_version: Optional[int], timeout: float) -> bool:
        ret = super(ZooKeeper, self).watch(leader_version, timeout + 0.5)
        if ret and not self._fetch_status:
            self._fetch_cluster = True
        return ret or self._fetch_cluster
