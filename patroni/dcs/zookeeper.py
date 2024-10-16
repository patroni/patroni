import json
import logging
import select
import socket
import time

from typing import Any, Callable, cast, Dict, List, Optional, Tuple, TYPE_CHECKING, Union

from kazoo.client import KazooClient, KazooRetry, KazooState
from kazoo.exceptions import ConnectionClosedError, NodeExistsError, NoNodeError, SessionExpiredError
from kazoo.handlers.threading import AsyncResult, SequentialThreadingHandler
from kazoo.protocol.states import KeeperState, WatchedEvent, ZnodeStat
from kazoo.retry import RetryFailedError
from kazoo.security import ACL, make_acl

from ..exceptions import DCSError
from ..postgresql.mpp import AbstractMPP
from ..utils import deep_compare
from . import AbstractDCS, Cluster, ClusterConfig, Failover, Leader, Member, Status, SyncState, TimelineHistory

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

    def __init__(self, config: Dict[str, Any], mpp: AbstractMPP) -> None:
        super(ZooKeeper, self).__init__(config, mpp)

        hosts: Union[str, List[str]] = config.get('hosts', [])
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
                                          deadline=config['retry_timeout'], sleep_func=time.sleep),
                                          auth_data=list(config.get('auth_data', {}).items()), **kwargs)

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

    def _watcher(self, event: WatchedEvent) -> None:
        if event.state != KazooState.CONNECTED or event.path.startswith(self.client_path('')):
            self.event.set()

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
        old_kazoo = isinstance(self._client.retry, KazooRetry)  # pyright: ignore [reportUnnecessaryIsInstance]
        retry = cast(KazooRetry, self._client.retry) if old_kazoo else self._client._retry
        retry.deadline = retry_timeout

    def get_node(
            self, key: str, watch: Optional[Callable[[WatchedEvent], None]] = None
    ) -> Optional[Tuple[str, ZnodeStat]]:
        try:
            ret = self._client.get(key, watch)
            return (ret[0].decode('utf-8'), ret[1])
        except NoNodeError:
            return None

    def get_status(self, path: str, leader: Optional[Leader]) -> Status:
        status = self.get_node(path + self._STATUS)
        if not status:
            status = self.get_node(path + self._LEADER_OPTIME)
        return Status.from_node(status and status[0])

    @staticmethod
    def member(name: str, value: str, znode: ZnodeStat) -> Member:
        return Member.from_node(znode.version, name, znode.ephemeralOwner, value)

    def get_children(self, key: str) -> List[str]:
        try:
            return self._client.get_children(key)
        except NoNodeError:
            return []

    def load_members(self, path: str) -> List[Member]:
        members: List[Member] = []
        for member in self.get_children(path + self._MEMBERS):
            data = self.get_node(path + self._MEMBERS + member)
            if data is not None:
                members.append(self.member(member, *data))
        return members

    def _postgresql_cluster_loader(self, path: str) -> Cluster:
        """Load and build the :class:`Cluster` object from DCS, which represents a single PostgreSQL cluster.

        :param path: the path in DCS where to load :class:`Cluster` from.

        :returns: :class:`Cluster` instance.
        """
        nodes = set(self.get_children(path))

        # get initialize flag
        initialize = (self.get_node(path + self._INITIALIZE) or [None])[0] if self._INITIALIZE in nodes else None

        # get global dynamic configuration
        config = self.get_node(path + self._CONFIG, watch=self._watcher) if self._CONFIG in nodes else None
        config = config and ClusterConfig.from_node(config[1].version, config[0], config[1].mzxid)

        # get timeline history
        history = self.get_node(path + self._HISTORY) if self._HISTORY in nodes else None
        history = history and TimelineHistory.from_node(history[1].mzxid, history[0])

        # get synchronization state
        sync = self.get_node(path + self._SYNC) if self._SYNC in nodes else None
        sync = SyncState.from_node(sync and sync[1].version, sync and sync[0])

        # get list of members
        members = self.load_members(path) if self._MEMBERS[:-1] in nodes else []

        # get leader
        leader = self.get_node(path + self._LEADER, watch=self._watcher) if self._LEADER in nodes else None
        if leader:
            member = Member(-1, leader[0], None, {})
            member = ([m for m in members if m.name == leader[0]] or [member])[0]
            leader = Leader(leader[1].version, leader[1].ephemeralOwner, member)

        # get last known leader lsn and slots
        status = self.get_status(path, leader)

        # failover key
        failover = self.get_node(path + self._FAILOVER) if self._FAILOVER in nodes else None
        failover = failover and Failover.from_node(failover[1].version, failover[0])

        # get failsafe topology
        failsafe = self.get_node(path + self._FAILSAFE) if self._FAILSAFE in nodes else None
        try:
            failsafe = json.loads(failsafe[0]) if failsafe else None
        except Exception:
            failsafe = None

        return Cluster(initialize, config, leader, status, members, failover, sync, history, failsafe)

    def _mpp_cluster_loader(self, path: str) -> Dict[int, Cluster]:
        """Load and build all PostgreSQL clusters from a single MPP cluster.

        :param path: the path in DCS where to load Cluster(s) from.

        :returns: all MPP groups as :class:`dict`, with group IDs as keys and :class:`Cluster` objects as values.
        """
        ret: Dict[int, Cluster] = {}
        for node in self.get_children(path):
            if self._mpp.group_re.match(node):
                ret[int(node)] = self._postgresql_cluster_loader(path + node + '/')
        return ret

    def _load_cluster(
            self, path: str, loader: Callable[[str], Union[Cluster, Dict[int, Cluster]]]
    ) -> Union[Cluster, Dict[int, Cluster]]:
        try:
            return self._client.retry(loader, path)
        except Exception:
            logger.exception('get_cluster')
            raise ZooKeeperError('ZooKeeper in not responding properly')

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
        if member and member_data:
            # We want delete the member ZNode if our session doesn't match with session id on our member key
            if self._client.client_id is not None and member.session != self._client.client_id[0]:
                logger.warning('Recreating the member ZNode due to ownership mismatch')
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

    def _update_leader(self, leader: Leader) -> bool:
        if self._client.client_id and self._client.client_id[0] != leader.session:
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

    def _delete_leader(self, leader: Leader) -> bool:
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
        if leader_version:
            timeout += 0.5

        try:
            return super(ZooKeeper, self).watch(leader_version, timeout)
        finally:
            self.event.clear()
