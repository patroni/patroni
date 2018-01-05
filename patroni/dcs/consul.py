from __future__ import absolute_import
import json
import logging
import os
import socket
import ssl
import time
import urllib3

from consul import ConsulException, NotFound, base
from patroni.dcs import AbstractDCS, ClusterConfig, Cluster, Failover, Leader, Member, SyncState, TimelineHistory
from patroni.exceptions import DCSError
from patroni.utils import deep_compare, parse_bool, Retry, RetryFailedError, split_host_port
from urllib3.exceptions import HTTPError
from six.moves.urllib.parse import urlencode, urlparse
from six.moves.http_client import HTTPException

logger = logging.getLogger(__name__)


class ConsulError(DCSError):
    pass


class ConsulInternalError(ConsulException):
    """An internal Consul server error occurred"""


class InvalidSessionTTL(ConsulInternalError):
    """Session TTL is too small or too big"""


class HTTPClient(object):

    def __init__(self, host='127.0.0.1', port=8500, token=None, scheme='http', verify=True, cert=None, ca_cert=None):
        self.token = token
        self._read_timeout = 10
        self.base_uri = '{0}://{1}:{2}'.format(scheme, host, port)
        kwargs = {}
        if cert:
            if isinstance(cert, tuple):
                # Key and cert are separate
                kwargs['cert_file'] = cert[0]
                kwargs['key_file'] = cert[1]
            else:
                # combined certificate
                kwargs['cert_file'] = cert
        if ca_cert:
            kwargs['ca_certs'] = ca_cert
        if verify or ca_cert:
            kwargs['cert_reqs'] = ssl.CERT_REQUIRED
        self.http = urllib3.PoolManager(num_pools=10, **kwargs)
        self._ttl = None

    def set_read_timeout(self, timeout):
        self._read_timeout = timeout/3.0

    @property
    def ttl(self):
        return self._ttl

    def set_ttl(self, ttl):
        ret = self._ttl != ttl
        self._ttl = ttl
        return ret

    @staticmethod
    def response(response):
        data = response.data.decode('utf-8')
        if response.status == 500:
            msg = '{0} {1}'.format(response.status, data)
            if data.startswith('Invalid Session TTL'):
                raise InvalidSessionTTL(msg)
            else:
                raise ConsulInternalError(msg)
        return base.Response(response.status, response.headers, data)

    def uri(self, path, params=None):
        return '{0}{1}{2}'.format(self.base_uri, path, params and '?' + urlencode(params) or '')

    def __getattr__(self, method):
        if method not in ('get', 'post', 'put', 'delete'):
            raise AttributeError("HTTPClient instance has no attribute '{0}'".format(method))

        def wrapper(callback, path, params=None, data=''):
            # python-consul doesn't allow to specify ttl smaller then 10 seconds
            # because session_ttl_min defaults to 10s, so we have to do this ugly dirty hack...
            if method == 'put' and path == '/v1/session/create':
                ttl = '"ttl": "{0}s"'.format(self._ttl)
                if not data or data == '{}':
                    data = '{' + ttl + '}'
                else:
                    data = data[:-1] + ', ' + ttl + '}'
            kwargs = {'retries': 0, 'preload_content': False, 'body': data}
            if method == 'get' and isinstance(params, dict) and 'index' in params:
                kwargs['timeout'] = (float(params['wait'][:-1]) if 'wait' in params else 300) + 1
            else:
                kwargs['timeout'] = self._read_timeout
            token = params.pop('token', self.token) if isinstance(params, dict) else self.token
            if token:
                kwargs['headers'] = {'X-Consul-Token': token}
            return callback(self.response(self.http.request(method.upper(), self.uri(path, params), **kwargs)))
        return wrapper


class ConsulClient(base.Consul):

    def __init__(self, *args, **kwargs):
        self._cert = kwargs.pop('cert', None)
        self._ca_cert = kwargs.pop('ca_cert', None)
        self._token = kwargs.get('token')
        super(ConsulClient, self).__init__(*args, **kwargs)

    def connect(self, *args, **kwargs):
        kwargs.update(dict(zip(['host', 'port', 'scheme', 'verify'], args)))
        if self._cert:
            kwargs['cert'] = self._cert
        if self._ca_cert:
            kwargs['ca_cert'] = self._ca_cert
        if self._token:
            kwargs['token'] = self._token
        return HTTPClient(**kwargs)


def catch_consul_errors(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (RetryFailedError, ConsulException, HTTPException, HTTPError, socket.error, socket.timeout):
            return False
    return wrapper


class Consul(AbstractDCS):

    def __init__(self, config):
        super(Consul, self).__init__(config)
        self._scope = config['scope']
        self._session = None
        self.__do_not_watch = False
        self._retry = Retry(deadline=config['retry_timeout'], max_delay=1, max_tries=-1,
                            retry_exceptions=(ConsulInternalError, HTTPException,
                                              HTTPError, socket.error, socket.timeout))

        self._my_member_data = {}
        kwargs = {}
        if 'url' in config:
            r = urlparse(config['url'])
            config.update({'scheme': r.scheme, 'host': r.hostname, 'port': r.port or 8500})
        elif 'host' in config:
            host, port = split_host_port(config.get('host', '127.0.0.1:8500'), 8500)
            config['host'] = host
            if 'port' not in config:
                config['port'] = int(port)

        if config.get('cacert'):
            config['ca_cert'] = config.pop('cacert')

        if config.get('key') and config.get('cert'):
            config['cert'] = (config['cert'], config['key'])

        config_keys = ('host', 'port', 'token', 'scheme', 'cert', 'ca_cert', 'dc')
        kwargs = {p: config.get(p) for p in config_keys if config.get(p)}

        verify = config.get('verify')
        if not isinstance(verify, bool):
            verify = parse_bool(verify)
        if isinstance(verify, bool):
            kwargs['verify'] = verify

        self._client = ConsulClient(**kwargs)
        self.set_retry_timeout(config['retry_timeout'])
        self.set_ttl(config.get('ttl') or 30)
        self._last_session_refresh = 0
        self.__session_checks = config.get('checks')
        if not self._ctl:
            self.create_session()

    def retry(self, *args, **kwargs):
        return self._retry.copy()(*args, **kwargs)

    def create_session(self):
        while not self._session:
            try:
                self.refresh_session()
            except ConsulError:
                logger.info('waiting on consul')
                time.sleep(5)

    def set_ttl(self, ttl):
        if self._client.http.set_ttl(ttl/2.0):  # Consul multiplies the TTL by 2x
            self._session = None
            self.__do_not_watch = True

    def set_retry_timeout(self, retry_timeout):
        self._retry.deadline = retry_timeout
        self._client.http.set_read_timeout(retry_timeout)

    def adjust_ttl(self):
        try:
            settings = self._client.agent.self()
            min_ttl = (settings['Config']['SessionTTLMin'] or 10000000000)/1000000000.0
            logger.warning('Changing Session TTL from %s to %s', self._client.http.ttl, min_ttl)
            self._client.http.set_ttl(min_ttl)
        except Exception:
            logger.exception('adjust_ttl')

    def _do_refresh_session(self):
        """:returns: `!True` if it had to create new session"""
        if self._session and self._last_session_refresh + self._loop_wait > time.time():
            return False

        if self._session:
            try:
                self._client.session.renew(self._session)
            except NotFound:
                self._session = None
        ret = not self._session
        if ret:
            try:
                self._session = self._client.session.create(name=self._scope + '-' + self._name,
                                                            checks=self.__session_checks,
                                                            lock_delay=0.001, behavior='delete')
            except InvalidSessionTTL:
                logger.exception('session.create')
                self.adjust_ttl()
                raise

        self._last_session_refresh = time.time()
        return ret

    def refresh_session(self):
        try:
            return self.retry(self._do_refresh_session)
        except (ConsulException, RetryFailedError):
            logger.exception('refresh_session')
        raise ConsulError('Failed to renew/create session')

    def client_path(self, path):
        return super(Consul, self).client_path(path)[1:]

    @staticmethod
    def member(node):
        return Member.from_node(node['ModifyIndex'], os.path.basename(node['Key']), node.get('Session'), node['Value'])

    def _load_cluster(self):
        try:
            path = self.client_path('/')
            _, results = self.retry(self._client.kv.get, path, recurse=True)

            if results is None:
                raise NotFound

            nodes = {}
            for node in results:
                node['Value'] = (node['Value'] or b'').decode('utf-8')
                nodes[os.path.relpath(node['Key'], path)] = node

            # get initialize flag
            initialize = nodes.get(self._INITIALIZE)
            initialize = initialize and initialize['Value']

            # get global dynamic configuration
            config = nodes.get(self._CONFIG)
            config = config and ClusterConfig.from_node(config['ModifyIndex'], config['Value'])

            # get timeline history
            history = nodes.get(self._HISTORY)
            history = history and TimelineHistory.from_node(history['ModifyIndex'], history['Value'])

            # get last leader operation
            last_leader_operation = nodes.get(self._LEADER_OPTIME)
            last_leader_operation = 0 if last_leader_operation is None else int(last_leader_operation['Value'])

            # get list of members
            members = [self.member(n) for k, n in nodes.items() if k.startswith(self._MEMBERS) and k.count('/') == 1]

            # get leader
            leader = nodes.get(self._LEADER)
            if not self._ctl and leader and leader['Value'] == self._name \
                    and self._session != leader.get('Session', 'x'):
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

            # get synchronization state
            sync = nodes.get(self._SYNC)
            sync = SyncState.from_node(sync and sync['ModifyIndex'], sync and sync['Value'])

            self._cluster = Cluster(initialize, config, leader, last_leader_operation, members, failover, sync, history)
        except NotFound:
            self._cluster = Cluster(None, None, None, None, [], None, None, None)
        except Exception:
            logger.exception('get_cluster')
            raise ConsulError('Consul is not responding properly')

    def touch_member(self, data, ttl=None, permanent=False):
        cluster = self.cluster
        member = cluster and cluster.get_member(self._name, fallback_to_leader=False)
        create_member = not permanent and self.refresh_session()

        if member and (create_member or member.session != self._session):
            try:
                self._client.kv.delete(self.member_path)
                create_member = True
            except Exception:
                return False

        if not create_member and member and deep_compare(data, self._my_member_data):
            return True

        try:
            args = {} if permanent else {'acquire': self._session}
            self._client.kv.put(self.member_path, json.dumps(data, separators=(',', ':')), **args)
            self._my_member_data = data
            return True
        except Exception:
            logger.exception('touch_member')
        return False

    @catch_consul_errors
    def _do_attempt_to_acquire_leader(self, kwargs):
        return self.retry(self._client.kv.put, self.leader_path, self._name, **kwargs)

    def attempt_to_acquire_leader(self, permanent=False):
        if not self._session and not permanent:
            self.refresh_session()

        ret = self._do_attempt_to_acquire_leader({} if permanent else {'acquire': self._session})
        if not ret:
            logger.info('Could not take out TTL lock')
        return ret

    def take_leader(self):
        return self.attempt_to_acquire_leader()

    @catch_consul_errors
    def set_failover_value(self, value, index=None):
        return self._client.kv.put(self.failover_path, value, cas=index)

    @catch_consul_errors
    def set_config_value(self, value, index=None):
        return self._client.kv.put(self.config_path, value, cas=index)

    @catch_consul_errors
    def _write_leader_optime(self, last_operation):
        return self._client.kv.put(self.leader_optime_path, last_operation)

    @catch_consul_errors
    def _update_leader(self):
        if self._session:
            self.retry(self._client.session.renew, self._session)
            self._last_session_refresh = time.time()
        return bool(self._session)

    @catch_consul_errors
    def initialize(self, create_new=True, sysid=''):
        kwargs = {'cas': 0} if create_new else {}
        return self.retry(self._client.kv.put, self.initialize_path, sysid, **kwargs)

    @catch_consul_errors
    def cancel_initialization(self):
        return self.retry(self._client.kv.delete, self.initialize_path)

    @catch_consul_errors
    def delete_cluster(self):
        return self.retry(self._client.kv.delete, self.client_path(''), recurse=True)

    @catch_consul_errors
    def set_history_value(self, value):
        return self._client.kv.put(self.history_path, value)

    @catch_consul_errors
    def delete_leader(self):
        cluster = self.cluster
        if cluster and isinstance(cluster.leader, Leader) and cluster.leader.name == self._name:
            return self._client.kv.delete(self.leader_path, cas=cluster.leader.index)

    @catch_consul_errors
    def set_sync_state_value(self, value, index=None):
        return self.retry(self._client.kv.put, self.sync_path, value, cas=index)

    @catch_consul_errors
    def delete_sync_state(self, index=None):
        return self.retry(self._client.kv.delete, self.sync_path, cas=index)

    def watch(self, leader_index, timeout):
        if self.__do_not_watch:
            self.__do_not_watch = False
            return True

        if leader_index:
            end_time = time.time() + timeout
            while timeout >= 1:
                try:
                    idx, _ = self._client.kv.get(self.leader_path, index=leader_index, wait=str(timeout) + 's')
                    return str(idx) != str(leader_index)
                except (ConsulException, HTTPException, HTTPError, socket.error, socket.timeout):
                    logging.exception('watch')

                timeout = end_time - time.time()

        try:
            return super(Consul, self).watch(None, timeout)
        finally:
            self.event.clear()
