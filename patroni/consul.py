from __future__ import absolute_import
import logging
import os
import time
import six

from consul import ConsulException, NotFound, base, std
from patroni.dcs import AbstractDCS, Cluster, Failover, Leader, Member
from patroni.exceptions import DCSError
from patroni.utils import sleep
from requests.exceptions import RequestException

logger = logging.getLogger(__name__)


class ConsulError(DCSError):
    pass


class HTTPClient(std.HTTPClient):

    def __init__(self, *args, **kwargs):
        super(HTTPClient, self).__init__(*args, **kwargs)
        self._patch_default_timeout()

    def _patch_default_timeout(self):
        func = self.session.request.__func__ if six.PY3 else self.session.request.im_func
        defaults = list(func.__defaults__ if six.PY3 else func.func_defaults)
        code = func.__code__ if six.PY3 else func.func_code
        defaults[code.co_varnames[code.co_argcount - len(defaults):code.co_argcount].index('timeout')] = 5
        if six.PY3:
            func.__defaults__ = tuple(defaults)
        else:
            func.func_defaults = tuple(defaults)

    def get(self, callback, path, params=None, timeout=None):
        uri = self.uri(path, params)
        if timeout is None and isinstance(params, dict) and 'index' in params:
            timeout = (float(params['wait'][:-1]) if 'wait' in params else 300) + 1
        return callback(self.response(self.session.get(uri, verify=self.verify, timeout=timeout)))


class Client(base.Consul):

    def connect(self, host, port, scheme, verify=True):
        return HTTPClient(host, port, scheme, verify)


def catch_consul_errors(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (ConsulException, RequestException):
            return False
    return wrapper


class Consul(AbstractDCS):

    def __init__(self, name, config):
        super(Consul, self).__init__(name, config)
        self.ttl = config.get('ttl') or 30
        host, port = config.get('host', '127.0.0.1:8500').split(':')
        self.client = Client(host=host, port=port)
        self._scope = config['scope']
        self._session = None
        self._new_cluster = False
        self._cluster_index = None
        self.create_or_restore_session()

    def create_or_restore_session(self):
        index, member = self.client.kv.get(self.member_path)
        self._session = (member or {}).get('Session', None)
        if self.referesh_session(retry=True):
            self.client.kv.delete(self.member_path)

    def create_session(self, retry=False):
        name = self._scope + '-' + self._name
        while not self._session:
            try:
                self._session = self.client.session.create(name=name, lock_delay=0, behavior='delete', ttl=self.ttl)
            except (ConsulException, RequestException):
                if not retry:
                    return
                logger.info('waiting on consul')
                sleep(5)

    def referesh_session(self, retry=False):
        """:returns: `!True` if it had to create new session"""
        if self._session:
            try:
                return not self.client.session.renew(self._session) is None
            except NotFound:
                self._session = None
        if not self._session:
            self.create_session(retry)
        if not self._session:
            raise ConsulError('Failed to renew/create session')
        return True

    def client_path(self, path):
        return super(Consul, self).client_path(path)[1:]

    @staticmethod
    def member(node):
        return Member.from_node(node['ModifyIndex'], os.path.basename(node['Key']), node['Session'], node['Value'])

    def _load_cluster(self, timeout=None):
        if not timeout and self._new_cluster:
            self._new_cluster = False
            return self._cluster

        try:
            path = self.client_path('/')
            index = self._cluster_index if timeout else None
            wait = str(timeout) + 's' if timeout else None
            self._cluster_index, results = self.client.kv.get(path, recurse=True, index=index, wait=wait)

            if results is None:
                raise NotFound

            nodes = {os.path.relpath(node['Key'], path): node for node in results}

            # get initialize flag
            initialize = nodes.get(self._INITIALIZE)
            initialize = initialize and initialize['Value']

            # get last leader operation
            last_leader_operation = nodes.get(self._LEADER_OPTIME, None)
            last_leader_operation = 0 if last_leader_operation is None else int(last_leader_operation['Value'])

            # get list of members
            members = [self.member(n) for k, n in nodes.items() if k.startswith(self._MEMBERS) and k.count('/') == 1]

            # get leader
            leader = nodes.get(self._LEADER, None)
            if leader and leader['Value'] == self._name and self._session != leader.get('Session', 'x'):
                logger.info('I am leader but not owner of the session. Removing leader node')
                self.client.kv.delete(self.leader_path, cas=leader['ModifyIndex'])
                leader = None

            if leader:
                member = Member(-1, leader['Value'], None, {})
                member = ([m for m in members if m.name == leader['Value']] or [member])[0]
                leader = Leader(leader['ModifyIndex'], leader['Session'], member)

            # failover key
            failover = nodes.get(self._FAILOVER, None)
            if failover:
                failover = Failover.from_node(failover['ModifyIndex'], failover['Value'])

            self._cluster = Cluster(initialize, leader, last_leader_operation, members, failover)
        except NotFound:
            self._cluster = Cluster(False, None, None, [], None)
        except:
            if timeout:
                raise
            logger.exception('get_cluster')
            raise ConsulError('Consul is not responding properly')
        self._new_cluster = bool(timeout)
        return self._cluster

    def touch_member(self, connection_string, ttl=None):
        create_member = self.referesh_session()
        cluster = self.cluster
        member_exists = cluster and any(m.name == self._name for m in cluster.members)
        if create_member and member_exists:
            self.client.kv.delete(self.member_path)
        if create_member or not member_exists:
            try:
                self.client.kv.put(self.member_path, connection_string, acquire=self._session)
            except:
                logger.exception('touch_member')
        return True

    @catch_consul_errors
    def attempt_to_acquire_leader(self):
        ret = self.client.kv.put(self.leader_path, self._name, acquire=self._session)
        ret or logger.info('Could not take out TTL lock')
        return ret

    def take_leader(self):
        return self.attempt_to_acquire_leader()

    @catch_consul_errors
    def set_failover_value(self, value, index=None):
        return self.client.kv.put(self.failover_path, value, cas=index or 0)

    @catch_consul_errors
    def write_leader_optime(self, last_operation):
        return self.client.kv.put(self.leader_optime_path, last_operation)

    def update_leader(self):
        return True

    @catch_consul_errors
    def initialize(self, create_new=True, sysid=''):
        kwargs = {'cas': 0} if create_new else {}
        return self.client.kv.put(self.initialize_path, sysid, **kwargs)

    @catch_consul_errors
    def cancel_initialization(self):
        return self.client.kv.delete(self.initialize_path)

    @catch_consul_errors
    def delete_leader(self):
        cluster = self.cluster
        if cluster and isinstance(cluster.leader, Leader) and cluster.leader.name == self._name:
            return self.client.kv.delete(self.leader_path, cas=cluster.leader.index)

    def watch(self, timeout):
        cluster = self.cluster
        if cluster and cluster.leader and cluster.leader.name != self._name:
            end_time = time.time() + timeout
            while timeout >= 1:
                try:
                    cluster = self._load_cluster(timeout)
                    if not cluster or not cluster.leader:
                        return False
                except (ConsulException, RequestException):
                    logging.exception('watch')

                timeout = end_time - time.time()

        try:
            return super(Consul, self).watch(timeout)
        finally:
            self.event.clear()
