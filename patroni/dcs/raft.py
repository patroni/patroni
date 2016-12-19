import logging
import os
import threading
import time

from patroni.dcs import AbstractDCS, ClusterConfig, Cluster, Failover, Leader, Member, SyncState
from pysyncobj import SyncObj, SyncObjConf, replicated, FAIL_REASON

logger = logging.getLogger(__name__)


class DynMemberSyncObj(SyncObj):

    def __init__(self, selfAddress, partnerAddrs, conf):
        conf.autoTick = False
        super(DynMemberSyncObj, self).__init__(None, partnerAddrs, conf)
        for self.__node in self._SyncObj__nodes:
            response = self.__utility_message(['members'])
            if response:
                partnerAddrs = [member['addr'] for member in response if member['addr'] != selfAddress]
                if len(partnerAddrs) == len(response):
                    if not conf.dynamicMembershipChange:
                        selfAddress = None
                    elif selfAddress:
                        response = self.__utility_message(['add', selfAddress])  # TODO check response
                break
        conf.autoTick = True
        super(DynMemberSyncObj, self).__init__(selfAddress, partnerAddrs, conf)

    def __utility_message(self, message):
        # __selfNodeAddr is send as a first message, we will abuse that fact
        self._SyncObj__selfNodeAddr = message
        self.__result = None
        self.__node.connectIfRequired()
        self._poller.poll(0.5)
        while self.__node.isConnected():
            self._poller.poll(0.5)
        return self.__result

    def _onMessageReceived(self, nodeAddr, message):
        if self._SyncObj__initialised:  # __initialised is set only when _autoTick thread is running
            super(DynMemberSyncObj, self)._onMessageReceived(nodeAddr, message)
        else:  # otherwise we are doing our nasty job, i.e. receiving response on utility_message
            self.__result = message
            self.__node._Node__conn.disconnect()
            self.__node._Node__lastConnectAttemptTime = 0

    def __get_members(self):
        ret = [{'addr': n.getAddress(), 'status': n.getStatus(),
                'leader': n.getAddress() == self._getLeader()} for n in self._SyncObj__nodes]
        ret.append({'addr': self._getSelfNodeAddr(), 'status': 2, 'leader': self._isLeader()})
        return ret

    # original __onUtilityMessage return data in strange format...
    def _SyncObj__onUtilityMessage(self, conn, message):
        if message[0] == 'members':
            conn.send(self.__get_members())
            return True
        return super(DynMemberSyncObj, self)._SyncObj__onUtilityMessage(conn, message)

    def _SyncObj__doChangeCluster(self, request, reverse=False):
        ret = super(DynMemberSyncObj, self)._SyncObj__doChangeCluster(request, reverse)
        if ret:
            self.forceLogCompaction()
        return ret


class KVStoreTTL(DynMemberSyncObj):

    def __init__(self, selfAddress, partnerAddrs, conf, on_set=None, on_delete=None):
        self.__on_set = on_set
        self.__on_delete = on_delete
        super(KVStoreTTL, self).__init__(selfAddress, partnerAddrs, conf)
        self.__retry_timeout = None
        self.__data = {}
        self.__limb = {}

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

        while ret['error'] not in (FAIL_REASON.SUCCESS, FAIL_REASON.REQUEST_DENIED):
            event.clear()
            func(*args, **kwargs)
            if deadline:
                timeout = deadline - time.time()
                if timeout <= 0:
                    return False
            event.wait(timeout)
            if ret['error'] == FAIL_REASON.QUEUE_FULL:
                time.sleep(1)
        return ret['error'] == FAIL_REASON.SUCCESS and ret['result']

    @replicated
    def _set(self, key, value, **kwargs):
        old_value = self.__data.get(key, {})
        if not self.__check_requirements(old_value, **kwargs):
            return False

        if old_value and old_value['created'] != value['created']:
            value['created'] = value['updated']
        value['index'] = self._SyncObj__raftLastApplied + 1

        self.__data[key] = value
        if self.__on_set:
            self.__on_set(key, value)
        return True

    def set(self, key, value, ttl=None, **kwargs):
        old_value = self.__data.get(key, {})
        if not self.__check_requirements(old_value, **kwargs):
            return False

        value = {'value': value, 'updated': time.time()}
        value['created'] = old_value.get('created', value['updated'])
        if ttl:
            value['expire'] = value['updated'] + ttl
        return self.retry(self._set, key, value, **kwargs)

    def __pop(self, key):
        self.__data.pop(key)
        if self.__on_delete:
            self.__on_delete(key)

    @replicated
    def _delete(self, key, recursive=False, **kwargs):
        if recursive:
            for k in list(self.__data.keys()):
                if k.startswith(key):
                    self.__pop(key)
        elif not self.__check_requirements(self.__data.get(key, {}), **kwargs):
            return False
        else:
            self.__pop(key)
        return True

    def delete(self, key, recursive=False, **kwargs):
        if not recursive and not self.__check_requirements(self.__data.get(key, {}), **kwargs):
            return False
        return self.retry(self._delete, key, recursive=recursive, **kwargs)

    @staticmethod
    def __values_match(old, new):
        return all(old.get(n) == new.get(n) for n in ('created', 'updated', 'expire', 'value'))

    @replicated
    def _expire(self, key, value):
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


class Raft(AbstractDCS):

    def __init__(self, config):
        super(Raft, self).__init__(config)
        self._ttl = int(config.get('ttl') or 30)
        template = os.path.join(config.get('data_dir', ''), config['self_addr'])
        ready_event = threading.Event()
        conf = SyncObjConf(appendEntriesUseBatch=False, dynamicMembershipChange=True, onReady=ready_event.set,
                           journalFile=template + '.journal', fullDumpFile=template + '.dump', logCompactionMinTime=30)
        self._sync_obj = KVStoreTTL(config['self_addr'], config['partner_addrs'], conf, self._on_set, self._on_delete)
        while True:
            ready_event.wait(5)
            if ready_event.isSet():
                break
            else:
                logger.info('waiting on raft')
        self.set_retry_timeout(int(config.get('retry_timeout') or 10))

    def _on_set(self, key, value):
        if value['created'] == value['updated'] and key.startswith(self.members_path) \
                or key in (self.config_path, self.sync_path):
            self.event.set()

    def _on_delete(self, key):
        if key == self.leader_path:
            self.event.set()

    def set_ttl(self, ttl):
        self._ttl = ttl

    def set_retry_timeout(self, retry_timeout):
        self._sync_obj.set_retry_timeout(retry_timeout)

    @staticmethod
    def member(key, value):
        return Member.from_node(value['index'], os.path.basename(key), None, value['value'])

    def _load_cluster(self):
        prefix = self.client_path('')
        response = self._sync_obj.get(prefix, recursive=True)
        if not response:
            self._cluster = Cluster(None, None, None, None, [], None, None)
            return
        nodes = {os.path.relpath(key, prefix): value for key, value in response.items()}

        # get initialize flag
        initialize = nodes.get(self._INITIALIZE)
        initialize = initialize and initialize['value']

        # get global dynamic configuration
        config = nodes.get(self._CONFIG)
        config = config and ClusterConfig.from_node(config['index'], config['value'])

        # get last leader operation
        last_leader_operation = nodes.get(self._LEADER_OPTIME)
        last_leader_operation = 0 if last_leader_operation is None else int(last_leader_operation['value'])

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

        self._cluster = Cluster(initialize, config, leader, last_leader_operation, members, failover, sync)

    def _write_leader_optime(self, last_operation):
        return self._sync_obj.set(self.leader_optime_path, last_operation)

    def update_leader(self):
        return self._sync_obj.set(self.leader_path, self._name, ttl=self._ttl, prevValue=self._name)

    def attempt_to_acquire_leader(self, permanent=False):
        return self._sync_obj.set(self.leader_path, self._name, prevExist=False,
                                  ttl=None if permanent else self._ttl)

    def set_failover_value(self, value, index=None):
        return self._sync_obj.set(self.failover_path, value, prevIndex=index)

    def set_config_value(self, value, index=None):
        return self._sync_obj.set(self.config_path, value, prevIndex=index)

    def touch_member(self, data, ttl=None, permanent=False):
        return self._sync_obj.set(self.member_path, data, None if permanent else ttl or self._ttl)

    def take_leader(self):
        return self._sync_obj.set(self.leader_path, self._name, ttl=self._ttl)

    def initialize(self, create_new=True, sysid=''):
        return self._sync_obj.set(self.initialize_path, sysid, prevExist=(not create_new))

    def delete_leader(self):
        return self._sync_obj.delete(self.leader_path, prevValue=self._name)

    def cancel_initialization(self):
        return self._sync_obj.delete(self.initialize_path)

    def delete_cluster(self):
        return self._sync_obj.delete(self.client_path(''), recursive=True)

    def set_sync_state_value(self, value, index=None):
        return self._sync_obj.set(self.sync_path, value, prevIndex=index)

    def delete_sync_state(self, index=None):
        return self._sync_obj.delete(self.sync_path, prevIndex=index)

    def watch(self, leader_index, timeout):
        try:
            return super(Raft, self).watch(leader_index, timeout)
        finally:
            self.event.clear()
