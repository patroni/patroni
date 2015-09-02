from __future__ import absolute_import
import logging
import os
import time
import six

from consul import ConsulException, NotFound, base, std
from helpers.dcs import AbstractDCS, Cluster, DCSError, Leader, Member, parse_connection_string
from helpers.utils import sleep
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
        self.ttl = config.get('ttl', None) or 30
        host, port = config.get('host', '127.0.0.1:8500').split(':')
        self.client = Client(host=host, port=port)
        self._session = None
        self.cluster = None
        self.new_cluster = False
        self.cluster_index = None
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
        conn_url, api_url = parse_connection_string(node['Value'])
        return Member(node['ModifyIndex'], os.path.basename(node['Key']), conn_url, api_url, None, None)

    def get_cluster(self, timeout=None):
        if not timeout and self.new_cluster:
            self.new_cluster = False
            return self.cluster

        try:
            path = self.client_path('/')
            index = self.cluster_index if timeout else None
            wait = str(timeout) + 's' if timeout else None
            self.cluster_index, results = self.client.kv.get(path, recurse=True, index=index, wait=wait)

            if results is None:
                raise NotFound

            nodes = {os.path.relpath(node['Key'], path): node for node in results}
            # get initialize flag
            initialize = bool(nodes.get('initialize', False))

            # get last leader operation
            last_leader_operation = nodes.get('optime/leader', None)
            last_leader_operation = 0 if last_leader_operation is None else int(last_leader_operation['Value'])

            # get list of members
            members = [self.member(n) for k, n in nodes.items() if k.startswith('members/') and len(k.split('/')) == 2]

            # get leader
            leader = nodes.get('leader', None)
            if leader and leader['Value'] == self._name and self._session != leader.get('Session', 'x'):
                logger.info('I am leader but not owner of the session. Removing leader node')
                self.client.kv.delete(self.leader_path, cas=leader['ModifyIndex'])
                leader = None

            if leader:
                member = Member(-1, leader['Value'], None, None, None, None)
                member = ([m for m in members if m.name == leader['Value']] or [member])[0]
                leader = Leader(leader['ModifyIndex'], None, None, member)

            self.cluster = Cluster(initialize, leader, last_leader_operation, members)
        except NotFound:
            self.cluster = Cluster(False, None, None, [])
        except:
            if timeout:
                raise
            logger.exception('get_cluster')
            raise ConsulError('Consul is not responding properly')
        self.new_cluster = bool(timeout)
        return self.cluster

    def touch_member(self, connection_string, ttl=None):
        create_member = self.referesh_session()
        member_exists = self.cluster and any(m.name == self._name for m in self.cluster.members)
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
        ret = self.client.kv.put(self.client_path('/leader'), self._name, acquire=self._session)
        ret or logger.info('Could not take out TTL lock')
        return ret

    def take_leader(self):
        return self.attempt_to_acquire_leader()

    @catch_consul_errors
    def write_leader_optime(self, state_handler):
        return self.client.kv.put(self.leader_optime_path, state_handler.last_operation())

    def update_leader(self, state_handler):
        return self.write_leader_optime(state_handler) or True

    @catch_consul_errors
    def race(self, path):
        return self.client.kv.put(self.client_path(path), self._name, cas=0)

    @catch_consul_errors
    def delete_leader(self):
        if self.cluster and isinstance(self.cluster.leader, Leader) and self.cluster.leader.member.name == self._name:
            return self.client.kv.delete(self.leader_path, cas=self.cluster.leader.index)

    def watch(self, timeout):
        if self.cluster and self.cluster.leader and self.cluster.leader.name != self._name:
            end_time = time.time() + timeout
            while timeout >= 1:
                try:
                    cluster = self.get_cluster(timeout)
                    if not cluster or not cluster.leader:
                        return
                except (ConsulException, RequestException):
                    logging.exception('watch')
                    pass

                timeout = end_time - time.time()

        timeout > 0 and super(Consul, self).watch(timeout)
