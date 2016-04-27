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
        # Set a default timeout for the `request.session.request` method, that is used
        # internally  by the methods request.session.get, request.session.post and
        # others. We monkey-patch here to avoid reimplementing each individual method from
        # `std.HTTPClient`. By default, the timeout is not set. It means that a new
        # session may hang almost indefinitely waiting for the server to respond,
        # which is not what we want in Patroni.

        request_func = getattr(self.session.request, '__func__' if six.PY3 else 'im_func')
        defaults_attr_name = '__defaults__' if six.PY3 else 'func_defaults'
        defaults = list(getattr(request_func, defaults_attr_name))
        code = request_func.__code__ if six.PY3 else request_func.func_code
        defaults[code.co_varnames[code.co_argcount - len(defaults):code.co_argcount].index('timeout')] = 5
        setattr(request_func, defaults_attr_name, tuple(defaults))  # monkeypatching

    def get(self, callback, path, params=None):
        # The get function is overridden to handle a special case of it being called
        # with an index and wait parameters. That form indicates that a user needs to
        # wait for the given key to change its value, with a wait timeout supplied. We
        # don't want our monkey-patched timeout to be less than the value of the wait
        # parameter, therefore, we set it to either the value of wait or a default of 5 minutes.

        if isinstance(params, dict) and 'index' in params:
            timeout = (float(params['wait'][:-1]) if 'wait' in params else 300) + 1
        else:
            timeout = None
        return callback(self.response(self.session.get(self.uri(path, params), verify=self.verify, timeout=timeout)))


class ConsulClient(base.Consul):

    @staticmethod
    def connect(host, port, scheme, verify=True):
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
        self.ttl = int((config.get('ttl') or 30)/2)  # My experiments have shown that session expires after 2*ttl time
        host, port = config.get('host', '127.0.0.1:8500').split(':')
        self._client = ConsulClient(host=host, port=port)
        self._scope = config['scope']
        self._session = None
        self._my_member_data = None
        self.create_or_restore_session()

    def create_or_restore_session(self):
        while not self._session:
            try:
                _, member = self._client.kv.get(self.member_path)
                self._session = (member or {}).get('Session')
                if self.refresh_session():
                    self._client.kv.delete(self.member_path)
            except (ConsulException, RequestException):
                logger.info('waiting on consul')
                sleep(5)

    def refresh_session(self):
        """:returns: `!True` if it had to create new session"""
        if self._session:
            try:
                return self._client.session.renew(self._session) is None
            except NotFound:
                self._session = None
        if not self._session:
            name = self._scope + '-' + self._name
            try:
                self._session = self._client.session.create(name=name, lock_delay=0, behavior='delete', ttl=self.ttl)
            except (ConsulException, RequestException):
                logger.exception('session.create')
        if not self._session:
            raise ConsulError('Failed to renew/create session')
        return True

    def client_path(self, path):
        return super(Consul, self).client_path(path)[1:]

    @staticmethod
    def member(node):
        return Member.from_node(node['ModifyIndex'], os.path.basename(node['Key']), node.get('Session'), node['Value'])

    def _load_cluster(self):
        try:
            path = self.client_path('/')
            _, results = self._client.kv.get(path, recurse=True)

            if results is None:
                raise NotFound

            nodes = {}
            for node in results:
                node['Value'] = (node['Value'] or b'').decode('utf-8')
                nodes[os.path.relpath(node['Key'], path)] = node

            # get initialize flag
            initialize = nodes.get(self._INITIALIZE)
            initialize = initialize and initialize['Value']

            # get last leader operation
            last_leader_operation = nodes.get(self._LEADER_OPTIME)
            last_leader_operation = 0 if last_leader_operation is None else int(last_leader_operation['Value'])

            # get list of members
            members = [self.member(n) for k, n in nodes.items() if k.startswith(self._MEMBERS) and k.count('/') == 1]

            # get leader
            leader = nodes.get(self._LEADER)
            if leader and leader['Value'] == self._name and self._session != leader.get('Session', 'x'):
                logger.info('I am leader but not owner of the session. Removing leader node')
                self._client.kv.delete(self.leader_path, cas=leader['ModifyIndex'])
                leader = None

            if leader:
                member = Member(-1, leader['Value'], None, {})
                member = ([m for m in members if m.name == leader['Value']] or [member])[0]
                leader = Leader(leader['ModifyIndex'], leader.get('Session'), member)

            # failover key
            failover = nodes.get(self._FAILOVER)
            if failover:
                failover = Failover.from_node(failover['ModifyIndex'], failover['Value'])

            self._cluster = Cluster(initialize, leader, last_leader_operation, members, failover)
        except NotFound:
            self._cluster = Cluster(False, None, None, [], None)
        except:
            logger.exception('get_cluster')
            raise ConsulError('Consul is not responding properly')

    def touch_member(self, data, **kwargs):
        create_member = self.refresh_session()
        cluster = self.cluster
        member = cluster and ([m for m in cluster.members if m.name == self._name] or [None])[0]
        if create_member and member:
            try:
                self._client.kv.delete(self.member_path)
            except Exception:
                return False

        if not create_member and member and data == self._my_member_data:
            return True

        try:
            self._client.kv.put(self.member_path, data, acquire=self._session)
            self._my_member_data = data
            return True
        except Exception:
            logger.exception('touch_member')
        return False

    @catch_consul_errors
    def attempt_to_acquire_leader(self):
        ret = self._client.kv.put(self.leader_path, self._name, acquire=self._session)
        if not ret:
            logger.info('Could not take out TTL lock')
        return ret

    def take_leader(self):
        return self.attempt_to_acquire_leader()

    @catch_consul_errors
    def set_failover_value(self, value, index=None):
        return self._client.kv.put(self.failover_path, value, cas=index)

    @catch_consul_errors
    def write_leader_optime(self, last_operation):
        return self._client.kv.put(self.leader_optime_path, last_operation)

    @staticmethod
    def update_leader():
        return True

    @catch_consul_errors
    def initialize(self, create_new=True, sysid=''):
        kwargs = {'cas': 0} if create_new else {}
        return self._client.kv.put(self.initialize_path, sysid, **kwargs)

    @catch_consul_errors
    def cancel_initialization(self):
        return self._client.kv.delete(self.initialize_path)

    @catch_consul_errors
    def delete_cluster(self):
        return self._client.kv.delete(self.client_path(''), recurse=True)

    @catch_consul_errors
    def delete_leader(self):
        cluster = self.cluster
        if cluster and isinstance(cluster.leader, Leader) and cluster.leader.name == self._name:
            return self._client.kv.delete(self.leader_path, cas=cluster.leader.index)

    def watch(self, timeout):
        cluster = self.cluster
        if cluster and cluster.leader and cluster.leader.name != self._name and cluster.leader.index:
            end_time = time.time() + timeout
            while timeout >= 1:
                try:
                    idx, _ = self._client.kv.get(self.leader_path, index=cluster.leader.index, wait=str(timeout) + 's')
                    return str(idx) != str(cluster.leader.index)
                except (ConsulException, RequestException):
                    logging.exception('watch')

                timeout = end_time - time.time()

        try:
            return super(Consul, self).watch(timeout)
        finally:
            self.event.clear()
