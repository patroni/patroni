import json
import logging
import select
import time

from kazoo.client import KazooClient, KazooState, KazooRetry
from kazoo.exceptions import NoNodeError, NodeExistsError, SessionExpiredError
from kazoo.handlers.threading import SequentialThreadingHandler
from kazoo.protocol.states import KeeperState

from . import AbstractDCS, ClusterConfig, Cluster, Failover, Leader, Member, SyncState, TimelineHistory
from ..exceptions import DCSError
from ..utils import deep_compare

logger = logging.getLogger(__name__)


class ZooKeeperError(DCSError):
    pass


class PatroniSequentialThreadingHandler(SequentialThreadingHandler):

    def __init__(self, connect_timeout):
        super(PatroniSequentialThreadingHandler, self).__init__()
        self.set_connect_timeout(connect_timeout)

    def set_connect_timeout(self, connect_timeout):
        self._connect_timeout = max(1.0, connect_timeout/2.0)  # try to connect to zookeeper node during loop_wait/2

    def create_connection(self, *args, **kwargs):
        """This method is trying to establish connection with one of the zookeeper nodes.
           Somehow strategy "fail earlier and retry more often" works way better comparing to
           the original strategy "try to connect with specified timeout".
           Since we want to try connect to zookeeper more often (with the smaller connect_timeout),
           he have to override `create_connection` method in the `SequentialThreadingHandler`
           class (which is used by `kazoo.Client`).

        :param args: always contains `tuple(host, port)` as the first element and could contain
                     `connect_timeout` (negotiated session timeout) as the second element."""

        args = list(args)
        if len(args) == 0:  # kazoo 2.6.0 slightly changed the way how it calls create_connection method
            kwargs['timeout'] = max(self._connect_timeout, kwargs.get('timeout', self._connect_timeout*10)/10.0)
        elif len(args) == 1:
            args.append(self._connect_timeout)
        else:
            args[1] = max(self._connect_timeout, args[1]/10.0)
        return super(PatroniSequentialThreadingHandler, self).create_connection(*args, **kwargs)

    def select(self, *args, **kwargs):
        """Python3 raises `ValueError` if socket is closed, because fd == -1"""
        try:
            return super(PatroniSequentialThreadingHandler, self).select(*args, **kwargs)
        except ValueError as e:
            raise select.error(9, str(e))


class PatroniKazooClient(KazooClient):

    def _call(self, request, async_object):
        # Before kazoo==2.7.0 it wasn't possible to send requests to zookeeper if
        # the connection is in the SUSPENDED state and Patroni was strongly relying on it.
        # The https://github.com/python-zk/kazoo/pull/588 changed it, and now such requests are queued.
        # We override the `_call()` method in order to keep the old behavior.

        if self._state == KeeperState.CONNECTING:
            async_object.set_exception(SessionExpiredError())
            return False
        return super(PatroniKazooClient, self)._call(request, async_object)


class ZooKeeper(AbstractDCS):

    def __init__(self, config):
        super(ZooKeeper, self).__init__(config)

        hosts = config.get('hosts', [])
        if isinstance(hosts, list):
            hosts = ','.join(hosts)

        mapping = {'use_ssl': 'use_ssl', 'verify': 'verify_certs', 'cacert': 'ca',
                   'cert': 'certfile', 'key': 'keyfile', 'key_password': 'keyfile_password'}
        kwargs = {v: config[k] for k, v in mapping.items() if k in config}

        self._client = PatroniKazooClient(hosts, handler=PatroniSequentialThreadingHandler(config['retry_timeout']),
                                          timeout=config['ttl'], connection_retry=KazooRetry(max_delay=1, max_tries=-1,
                                          sleep_func=time.sleep), command_retry=KazooRetry(max_delay=1, max_tries=-1,
                                          deadline=config['retry_timeout'], sleep_func=time.sleep), **kwargs)
        self._client.add_listener(self.session_listener)

        self._fetch_cluster = True
        self._fetch_status = True

        self._orig_kazoo_connect = self._client._connection._connect
        self._client._connection._connect = self._kazoo_connect

        self._client.start()

    def _kazoo_connect(self, *args):
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
        return max(self.loop_wait - 2, 2)*1000, ret[1]

    def session_listener(self, state):
        if state in [KazooState.SUSPENDED, KazooState.LOST]:
            self.cluster_watcher(None)

    def status_watcher(self, event):
        self._fetch_status = True
        self.event.set()

    def cluster_watcher(self, event):
        self._fetch_cluster = True
        self.status_watcher(event)

    def reload_config(self, config):
        self.set_retry_timeout(config['retry_timeout'])

        loop_wait = config['loop_wait']

        loop_wait_changed = self._loop_wait != loop_wait
        self._loop_wait = loop_wait
        self._client.handler.set_connect_timeout(loop_wait)

        # We need to reestablish connection to zookeeper if we want to change
        # read_timeout (and Ping interval respectively), because read_timeout
        # is calculated in `_kazoo_connect` method. If we are changing ttl at
        # the same time, set_ttl method will reestablish connection and return
        # `!True`, otherwise we will close existing connection and let kazoo
        # open the new one.
        if not self.set_ttl(config['ttl']) and loop_wait_changed:
            self._client._connection._socket.close()

    def set_ttl(self, ttl):
        """It is not possible to change ttl (session_timeout) in zookeeper without
        destroying old session and creating the new one. This method returns `!True`
        if session_timeout has been changed (`restart()` has been called)."""
        ttl = int(ttl * 1000)
        if self._client._session_timeout != ttl:
            self._client._session_timeout = ttl
            self._client.restart()
            return True

    @property
    def ttl(self):
        return self._client._session_timeout / 1000.0

    def set_retry_timeout(self, retry_timeout):
        retry = self._client.retry if isinstance(self._client.retry, KazooRetry) else self._client._retry
        retry.deadline = retry_timeout

    def get_node(self, key, watch=None):
        try:
            ret = self._client.get(key, watch)
            return (ret[0].decode('utf-8'), ret[1])
        except NoNodeError:
            return None

    def get_status(self, leader):
        watch = self.status_watcher if not leader or leader.name != self._name else None

        status = self.get_node(self.status_path, watch)
        if status:
            try:
                status = json.loads(status[0])
                last_lsn = status.get(self._OPTIME)
                slots = status.get('slots')
            except Exception:
                slots = last_lsn = None
        else:
            last_lsn = self.get_node(self.leader_optime_path, watch)
            last_lsn = last_lsn and last_lsn[0]
            slots = None

        try:
            last_lsn = int(last_lsn)
        except Exception:
            last_lsn = 0

        self._fetch_status = False
        return last_lsn, slots

    @staticmethod
    def member(name, value, znode):
        return Member.from_node(znode.version, name, znode.ephemeralOwner, value)

    def get_children(self, key, watch=None):
        try:
            return self._client.get_children(key, watch)
        except NoNodeError:
            return []

    def load_members(self):
        members = []
        for member in self.get_children(self.members_path, self.cluster_watcher):
            data = self.get_node(self.members_path + member)
            if data is not None:
                members.append(self.member(member, *data))
        return members

    def _inner_load_cluster(self):
        self._fetch_cluster = False
        self.event.clear()
        nodes = set(self.get_children(self.client_path(''), self.cluster_watcher))
        if not nodes:
            self._fetch_cluster = True

        # get initialize flag
        initialize = (self.get_node(self.initialize_path) or [None])[0] if self._INITIALIZE in nodes else None

        # get global dynamic configuration
        config = self.get_node(self.config_path, watch=self.cluster_watcher) if self._CONFIG in nodes else None
        config = config and ClusterConfig.from_node(config[1].version, config[0], config[1].mzxid)

        # get timeline history
        history = self.get_node(self.history_path, watch=self.cluster_watcher) if self._HISTORY in nodes else None
        history = history and TimelineHistory.from_node(history[1].mzxid, history[0])

        # get synchronization state
        sync = self.get_node(self.sync_path, watch=self.cluster_watcher) if self._SYNC in nodes else None
        sync = SyncState.from_node(sync and sync[1].version, sync and sync[0])

        # get list of members
        members = self.load_members() if self._MEMBERS[:-1] in nodes else []

        # get leader
        leader = self.get_node(self.leader_path) if self._LEADER in nodes else None
        if leader:
            client_id = self._client.client_id
            if not self._ctl and leader[0] == self._name and client_id is not None \
                    and client_id[0] != leader[1].ephemeralOwner:
                logger.info('I am leader but not owner of the session. Removing leader node')
                self._client.delete(self.leader_path)
                leader = None

            if leader:
                member = Member(-1, leader[0], None, {})
                member = ([m for m in members if m.name == leader[0]] or [member])[0]
                leader = Leader(leader[1].version, leader[1].ephemeralOwner, member)
                self._fetch_cluster = member.index == -1

        # get last known leader lsn and slots
        last_lsn, slots = self.get_status(leader)

        # failover key
        failover = self.get_node(self.failover_path, watch=self.cluster_watcher) if self._FAILOVER in nodes else None
        failover = failover and Failover.from_node(failover[1].version, failover[0])

        return Cluster(initialize, config, leader, last_lsn, members, failover, sync, history, slots)

    def _load_cluster(self):
        cluster = self.cluster
        if self._fetch_cluster or cluster is None:
            try:
                cluster = self._client.retry(self._inner_load_cluster)
            except Exception:
                logger.exception('get_cluster')
                self.cluster_watcher(None)
                raise ZooKeeperError('ZooKeeper in not responding properly')
        # The /status ZNode was updated or doesn't exist and we are not leader
        elif (self._fetch_status and not self._fetch_cluster or not cluster.last_lsn
              or cluster.has_permanent_logical_slots(self._name, False) and not cluster.slots) and\
                not (cluster.leader and cluster.leader.name == self._name):
            try:
                last_lsn, slots = self.get_status(cluster.leader)
                self.event.clear()
                cluster = Cluster(cluster.initialize, cluster.config, cluster.leader, last_lsn,
                                  cluster.members, cluster.failover, cluster.sync, cluster.history, slots)
            except Exception:
                pass
        return cluster

    def _bypass_caches(self):
        self._fetch_cluster = True

    def _create(self, path, value, retry=False, ephemeral=False):
        try:
            if retry:
                self._client.retry(self._client.create, path, value, makepath=True, ephemeral=ephemeral)
            else:
                self._client.create_async(path, value, makepath=True, ephemeral=ephemeral).get(timeout=1)
            return True
        except Exception:
            logger.exception('Failed to create %s', path)
        return False

    def attempt_to_acquire_leader(self, permanent=False):
        ret = self._create(self.leader_path, self._name.encode('utf-8'), retry=True, ephemeral=not permanent)
        if not ret:
            logger.info('Could not take out TTL lock')
        return ret

    def _set_or_create(self, key, value, index=None, retry=False, do_not_create_empty=False):
        value = value.encode('utf-8')
        try:
            if retry:
                self._client.retry(self._client.set, key, value, version=index or -1)
            else:
                self._client.set_async(key, value, version=index or -1).get(timeout=1)
            return True
        except NoNodeError:
            if do_not_create_empty and not value:
                return True
            elif index is None:
                return self._create(key, value, retry)
            else:
                return False
        except Exception:
            logger.exception('Failed to update %s', key)
        return False

    def set_failover_value(self, value, index=None):
        return self._set_or_create(self.failover_path, value, index)

    def set_config_value(self, value, index=None):
        return self._set_or_create(self.config_path, value, index, retry=True)

    def initialize(self, create_new=True, sysid=""):
        sysid = sysid.encode('utf-8')
        return self._create(self.initialize_path, sysid, retry=True) if create_new \
            else self._client.retry(self._client.set, self.initialize_path, sysid)

    def touch_member(self, data, permanent=False):
        cluster = self.cluster
        member = cluster and cluster.get_member(self._name, fallback_to_leader=False)
        encoded_data = json.dumps(data, separators=(',', ':')).encode('utf-8')
        if member and (self._client.client_id is not None and member.session != self._client.client_id[0] or
                       not (deep_compare(member.data.get('tags', {}), data.get('tags', {})) and
                            member.data.get('version') == data.get('version') and
                            member.data.get('checkpoint_after_promote') == data.get('checkpoint_after_promote'))):
            try:
                self._client.delete_async(self.member_path).get(timeout=1)
            except NoNodeError:
                pass
            except Exception:
                return False
            member = None

        if member:
            if deep_compare(data, member.data):
                return True
        else:
            try:
                self._client.create_async(self.member_path, encoded_data, makepath=True,
                                          ephemeral=not permanent).get(timeout=1)
                return True
            except Exception as e:
                if not isinstance(e, NodeExistsError):
                    logger.exception('touch_member')
                    return False
        try:
            self._client.set_async(self.member_path, encoded_data).get(timeout=1)
            return True
        except Exception:
            logger.exception('touch_member')

        return False

    def take_leader(self):
        return self.attempt_to_acquire_leader()

    def _write_leader_optime(self, last_lsn):
        return self._set_or_create(self.leader_optime_path, last_lsn)

    def _write_status(self, value):
        return self._set_or_create(self.status_path, value)

    def _update_leader(self):
        return True

    def _delete_leader(self):
        self._client.restart()
        return True

    def _cancel_initialization(self):
        node = self.get_node(self.initialize_path)
        if node:
            self._client.delete(self.initialize_path, version=node[1].version)

    def cancel_initialization(self):
        try:
            self._client.retry(self._cancel_initialization)
        except Exception:
            logger.exception("Unable to delete initialize key")

    def delete_cluster(self):
        try:
            return self._client.retry(self._client.delete, self.client_path(''), recursive=True)
        except NoNodeError:
            return True

    def set_history_value(self, value):
        return self._set_or_create(self.history_path, value)

    def set_sync_state_value(self, value, index=None):
        return self._set_or_create(self.sync_path, value, index, retry=True, do_not_create_empty=True)

    def delete_sync_state(self, index=None):
        return self.set_sync_state_value("{}", index)

    def watch(self, leader_index, timeout):
        ret = super(ZooKeeper, self).watch(leader_index, timeout + 0.5)
        if ret and not self._fetch_status:
            self._fetch_cluster = True
        return ret or self._fetch_cluster
