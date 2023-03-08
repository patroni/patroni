import json
import logging
import os
import threading
import time

from collections import defaultdict
from pysyncobj import SyncObj, SyncObjConf, replicated, FAIL_REASON
from pysyncobj.dns_resolver import globalDnsResolver
from pysyncobj.node import TCPNode
from pysyncobj.transport import TCPTransport, CONNECTION_STATE
from pysyncobj.utility import TcpUtility

from . import AbstractDCS, ClusterConfig, Cluster, Failover, Leader, Member, SyncState, TimelineHistory, citus_group_re
from ..exceptions import DCSError
from ..utils import validate_directory

logger = logging.getLogger(__name__)


class RaftError(DCSError):
    pass


class _TCPTransport(TCPTransport):

    def __init__(self, syncObj, selfNode, otherNodes):
        super(_TCPTransport, self).__init__(syncObj, selfNode, otherNodes)
        self.setOnUtilityMessageCallback('members', syncObj.getMembers)

    def _connectIfNecessarySingle(self, node):
        try:
            return super(_TCPTransport, self)._connectIfNecessarySingle(node)
        except Exception as e:
            logger.debug('Connection to %s failed: %r', node, e)
            return False


def resolve_host(self):
    return globalDnsResolver().resolve(self.host)


setattr(TCPNode, 'ip', property(resolve_host))


class SyncObjUtility(object):

    def __init__(self, otherNodes, conf, retry_timeout=10):
        self._nodes = otherNodes
        self._utility = TcpUtility(conf.password, retry_timeout/max(1, len(otherNodes)))

    def executeCommand(self, command):
        try:
            return self._utility.executeCommand(self.__node, command)
        except Exception:
            return None

    def getMembers(self):
        for self.__node in self._nodes:
            response = self.executeCommand(['members'])
            if response:
                return [member['addr'] for member in response]


class DynMemberSyncObj(SyncObj):

    def __init__(self, selfAddress, partnerAddrs, conf, retry_timeout=10):
        self.__early_apply_local_log = selfAddress is not None
        self.applied_local_log = False

        utility = SyncObjUtility(partnerAddrs, conf, retry_timeout)
        members = utility.getMembers()
        add_self = members and selfAddress not in members

        partnerAddrs = [member for member in (members or partnerAddrs) if member != selfAddress]

        super(DynMemberSyncObj, self).__init__(selfAddress, partnerAddrs, conf, transportClass=_TCPTransport)

        if add_self:
            thread = threading.Thread(target=utility.executeCommand, args=(['add', selfAddress],))
            thread.daemon = True
            thread.start()

    def getMembers(self, args, callback):
        callback([{'addr': node.id, 'leader': node == self._getLeader(), 'status': CONNECTION_STATE.CONNECTED
                   if self.isNodeConnected(node) else CONNECTION_STATE.DISCONNECTED} for node in self.otherNodes] +
                 [{'addr': self.selfNode.id, 'leader': self._isLeader(), 'status': CONNECTION_STATE.CONNECTED}], None)

    def _onTick(self, timeToWait=0.0):
        super(DynMemberSyncObj, self)._onTick(timeToWait)

        # The SyncObj calls onReady callback only when cluster got the leader and is ready for writes.
        # In some cases for us it is safe to "signal" the Raft object when the local log is fully applied.
        # We are using the `applied_local_log` property for that, but not calling the callback function.
        if self.__early_apply_local_log and not self.applied_local_log and self.raftLastApplied == self.raftCommitIndex:
            self.applied_local_log = True


class KVStoreTTL(DynMemberSyncObj):

    def __init__(self, on_ready, on_set, on_delete, **config):
        self.__thread = None
        self.__on_set = on_set
        self.__on_delete = on_delete
        self.__limb = {}
        self.set_retry_timeout(int(config.get('retry_timeout') or 10))

        self_addr = config.get('self_addr')
        partner_addrs = set(config.get('partner_addrs', []))
        if config.get('patronictl'):
            if self_addr:
                partner_addrs.add(self_addr)
            self_addr = None

        # Create raft data_dir if necessary
        raft_data_dir = config.get('data_dir', '')
        if raft_data_dir != '':
            validate_directory(raft_data_dir)

        file_template = (self_addr or '')
        file_template = file_template.replace(':', '_') if os.name == 'nt' else file_template
        file_template = os.path.join(raft_data_dir, file_template)
        conf = SyncObjConf(password=config.get('password'), autoTick=False, appendEntriesUseBatch=False,
                           bindAddress=config.get('bind_addr'), dnsFailCacheTime=(config.get('loop_wait') or 10),
                           dnsCacheTime=(config.get('ttl') or 30), commandsWaitLeader=config.get('commandsWaitLeader'),
                           fullDumpFile=(file_template + '.dump' if self_addr else None),
                           journalFile=(file_template + '.journal' if self_addr else None),
                           onReady=on_ready, dynamicMembershipChange=True)

        super(KVStoreTTL, self).__init__(self_addr, partner_addrs, conf, self.__retry_timeout)
        self.__data = {}

    @staticmethod
    def __check_requirements(old_value, **kwargs):
        return ('prevExist' not in kwargs or bool(kwargs['prevExist']) == bool(old_value)) and \
            ('prevValue' not in kwargs or old_value and old_value['value'] == kwargs['prevValue']) and \
            (not kwargs.get('prevIndex') or old_value and old_value['index'] == kwargs['prevIndex'])

    def set_retry_timeout(self, retry_timeout):
        self.__retry_timeout = retry_timeout

    def retry(self, func, *args, **kwargs):
        event = threading.Event()
        ret = {'result': None, 'error': -1}

        def callback(result, error):
            ret.update(result=result, error=error)
            event.set()

        kwargs['callback'] = callback
        timeout = kwargs.pop('timeout', None) or self.__retry_timeout
        deadline = timeout and time.time() + timeout

        while True:
            event.clear()
            func(*args, **kwargs)
            event.wait(timeout)
            if ret['error'] == FAIL_REASON.SUCCESS:
                return ret['result']
            elif ret['error'] == FAIL_REASON.REQUEST_DENIED:
                break
            elif deadline:
                timeout = deadline - time.time()
                if timeout <= 0:
                    raise RaftError('timeout')
            time.sleep(1)
        return False

    @replicated
    def _set(self, key, value, **kwargs):
        old_value = self.__data.get(key, {})
        if not self.__check_requirements(old_value, **kwargs):
            return False

        if old_value and old_value['created'] != value['created']:
            value['created'] = value['updated']
        value['index'] = self.raftLastApplied + 1

        self.__data[key] = value
        if self.__on_set:
            self.__on_set(key, value)
        return True

    def set(self, key, value, ttl=None, handle_raft_error=True, **kwargs):
        old_value = self.__data.get(key, {})
        if not self.__check_requirements(old_value, **kwargs):
            return False

        value = {'value': value, 'updated': time.time()}
        value['created'] = old_value.get('created', value['updated'])
        if ttl:
            value['expire'] = value['updated'] + ttl
        try:
            return self.retry(self._set, key, value, **kwargs)
        except RaftError:
            if not handle_raft_error:
                raise
            return False

    def __pop(self, key):
        self.__data.pop(key)
        if self.__on_delete:
            self.__on_delete(key)

    @replicated
    def _delete(self, key, recursive=False, **kwargs):
        if recursive:
            for k in list(self.__data.keys()):
                if k.startswith(key):
                    self.__pop(k)
        elif not self.__check_requirements(self.__data.get(key, {}), **kwargs):
            return False
        else:
            self.__pop(key)
        return True

    def delete(self, key, recursive=False, **kwargs):
        if not recursive and not self.__check_requirements(self.__data.get(key, {}), **kwargs):
            return False
        try:
            return self.retry(self._delete, key, recursive=recursive, **kwargs)
        except RaftError:
            return False

    @staticmethod
    def __values_match(old, new):
        return all(old.get(n) == new.get(n) for n in ('created', 'updated', 'expire', 'value'))

    @replicated
    def _expire(self, key, value, callback=None):
        current = self.__data.get(key)
        if current and self.__values_match(current, value):
            self.__pop(key)

    def __expire_keys(self):
        for key, value in self.__data.items():
            if value and 'expire' in value and value['expire'] <= time.time() and \
                    not (key in self.__limb and self.__values_match(self.__limb[key], value)):
                self.__limb[key] = value

                def callback(*args):
                    if key in self.__limb and self.__values_match(self.__limb[key], value):
                        self.__limb.pop(key)
                self._expire(key, value, callback=callback)

    def get(self, key, recursive=False):
        if not recursive:
            return self.__data.get(key)
        return {k: v for k, v in self.__data.items() if k.startswith(key)}

    def _onTick(self, timeToWait=0.0):
        super(KVStoreTTL, self)._onTick(timeToWait)

        if self._isLeader():
            self.__expire_keys()
        else:
            self.__limb.clear()

    def _autoTickThread(self):
        self.__destroying = False
        while not self.__destroying:
            self.doTick(self.conf.autoTickPeriod)

    def startAutoTick(self):
        self.__thread = threading.Thread(target=self._autoTickThread)
        self.__thread.daemon = True
        self.__thread.start()

    def destroy(self):
        if self.__thread:
            self.__destroying = True
            self.__thread.join()
        super(KVStoreTTL, self).destroy()


class Raft(AbstractDCS):

    def __init__(self, config):
        super(Raft, self).__init__(config)
        self._ttl = int(config.get('ttl') or 30)

        ready_event = threading.Event()
        self._sync_obj = KVStoreTTL(ready_event.set, self._on_set, self._on_delete, commandsWaitLeader=False, **config)
        self._sync_obj.startAutoTick()

        while True:
            ready_event.wait(5)
            if ready_event.is_set() or self._sync_obj.applied_local_log:
                break
            else:
                logger.info('waiting on raft')

    def _on_set(self, key, value):
        leader = (self._sync_obj.get(self.leader_path) or {}).get('value')
        if key == value['created'] == value['updated'] and \
                (key.startswith(self.members_path) or key == self.leader_path and leader != self._name) or \
                key in (self.leader_optime_path, self.status_path) and leader != self._name or \
                key in (self.config_path, self.sync_path):
            self.event.set()

    def _on_delete(self, key):
        if key == self.leader_path:
            self.event.set()

    def set_ttl(self, ttl):
        self._ttl = ttl

    @property
    def ttl(self):
        return self._ttl

    def set_retry_timeout(self, retry_timeout):
        self._sync_obj.set_retry_timeout(retry_timeout)

    def reload_config(self, config):
        super(Raft, self).reload_config(config)
        globalDnsResolver().setTimeouts(self.ttl, self.loop_wait)

    @staticmethod
    def member(key, value):
        return Member.from_node(value['index'], os.path.basename(key), None, value['value'])

    def _cluster_from_nodes(self, nodes):
        # get initialize flag
        initialize = nodes.get(self._INITIALIZE)
        initialize = initialize and initialize['value']

        # get global dynamic configuration
        config = nodes.get(self._CONFIG)
        config = config and ClusterConfig.from_node(config['index'], config['value'])

        # get timeline history
        history = nodes.get(self._HISTORY)
        history = history and TimelineHistory.from_node(history['index'], history['value'])

        # get last know leader lsn and slots
        status = nodes.get(self._STATUS)
        if status:
            try:
                status = json.loads(status['value'])
                last_lsn = status.get(self._OPTIME)
                slots = status.get('slots')
            except Exception:
                slots = last_lsn = None
        else:
            last_lsn = nodes.get(self._LEADER_OPTIME)
            last_lsn = last_lsn and last_lsn['value']
            slots = None

        try:
            last_lsn = int(last_lsn)
        except Exception:
            last_lsn = 0

        # get list of members
        members = [self.member(k, n) for k, n in nodes.items() if k.startswith(self._MEMBERS) and k.count('/') == 1]

        # get leader
        leader = nodes.get(self._LEADER)
        if leader:
            member = Member(-1, leader['value'], None, {})
            member = ([m for m in members if m.name == leader['value']] or [member])[0]
            leader = Leader(leader['index'], None, member)

        # failover key
        failover = nodes.get(self._FAILOVER)
        if failover:
            failover = Failover.from_node(failover['index'], failover['value'])

        # get synchronization state
        sync = nodes.get(self._SYNC)
        sync = SyncState.from_node(sync and sync['index'], sync and sync['value'])

        # get failsafe topology
        failsafe = nodes.get(self._FAILSAFE)
        try:
            failsafe = json.loads(failsafe['value']) if failsafe else None
        except Exception:
            failsafe = None

        return Cluster(initialize, config, leader, last_lsn, members, failover, sync, history, slots, failsafe)

    def _cluster_loader(self, path):
        response = self._sync_obj.get(path, recursive=True)
        if not response:
            return Cluster(None, None, None, None, [], None, None, None, None, None)
        nodes = {key[len(path):]: value for key, value in response.items()}
        return self._cluster_from_nodes(nodes)

    def _citus_cluster_loader(self, path):
        clusters = defaultdict(dict)
        response = self._sync_obj.get(path, recursive=True)
        for key, value in response.items():
            key = key[len(path):].split('/', 1)
            if len(key) == 2 and citus_group_re.match(key[0]):
                clusters[int(key[0])][key[1]] = value
        return {group: self._cluster_from_nodes(nodes) for group, nodes in clusters.items()}

    def _load_cluster(self, path, loader):
        return loader(path)

    def _write_leader_optime(self, last_lsn):
        return self._sync_obj.set(self.leader_optime_path, last_lsn, timeout=1)

    def _write_status(self, value):
        return self._sync_obj.set(self.status_path, value, timeout=1)

    def _write_failsafe(self, value):
        return self._sync_obj.set(self.failsafe_path, value, timeout=1)

    def _update_leader(self):
        ret = self._sync_obj.set(self.leader_path, self._name, ttl=self._ttl,
                                 handle_raft_error=False, prevValue=self._name)
        if not ret and self._sync_obj.get(self.leader_path) is None:
            ret = self.attempt_to_acquire_leader()
        return ret

    def attempt_to_acquire_leader(self):
        return self._sync_obj.set(self.leader_path, self._name, ttl=self._ttl, handle_raft_error=False, prevExist=False)

    def set_failover_value(self, value, index=None):
        return self._sync_obj.set(self.failover_path, value, prevIndex=index)

    def set_config_value(self, value, index=None):
        return self._sync_obj.set(self.config_path, value, prevIndex=index)

    def touch_member(self, data):
        data = json.dumps(data, separators=(',', ':'))
        return self._sync_obj.set(self.member_path, data, self._ttl, timeout=2)

    def take_leader(self):
        return self._sync_obj.set(self.leader_path, self._name, ttl=self._ttl)

    def initialize(self, create_new=True, sysid=''):
        return self._sync_obj.set(self.initialize_path, sysid, prevExist=(not create_new))

    def _delete_leader(self):
        return self._sync_obj.delete(self.leader_path, prevValue=self._name, timeout=1)

    def cancel_initialization(self):
        return self._sync_obj.delete(self.initialize_path)

    def delete_cluster(self):
        return self._sync_obj.delete(self.client_path(''), recursive=True)

    def set_history_value(self, value):
        return self._sync_obj.set(self.history_path, value)

    def set_sync_state_value(self, value, index=None):
        return self._sync_obj.set(self.sync_path, value, prevIndex=index)

    def delete_sync_state(self, index=None):
        return self._sync_obj.delete(self.sync_path, prevIndex=index)

    def watch(self, leader_index, timeout):
        try:
            return super(Raft, self).watch(leader_index, timeout)
        finally:
            self.event.clear()
