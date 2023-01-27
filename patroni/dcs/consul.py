from __future__ import absolute_import
import json
import logging
import os
import re
import socket
import ssl
import time
import urllib3

from collections import defaultdict, namedtuple
from consul import ConsulException, NotFound, base
from urllib3.exceptions import HTTPError
from six.moves.urllib.parse import urlencode, urlparse, quote
from six.moves.http_client import HTTPException

from . import AbstractDCS, Cluster, ClusterConfig, Failover, Leader, Member, SyncState,\
        TimelineHistory, ReturnFalseException, catch_return_false_exception, citus_group_re
from ..exceptions import DCSError
from ..utils import deep_compare, parse_bool, Retry, RetryFailedError, split_host_port, uri, USER_AGENT

logger = logging.getLogger(__name__)


class ConsulError(DCSError):
    pass


class ConsulInternalError(ConsulException):
    """An internal Consul server error occurred"""


class InvalidSessionTTL(ConsulException):
    """Session TTL is too small or too big"""


class InvalidSession(ConsulException):
    """invalid session"""


Response = namedtuple('Response', 'code,headers,body,content')


class HTTPClient(object):

    def __init__(self, host='127.0.0.1', port=8500, token=None, scheme='http', verify=True, cert=None, ca_cert=None):
        self.token = token
        self._read_timeout = 10
        self.base_uri = uri(scheme, (host, port))
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
        kwargs['cert_reqs'] = ssl.CERT_REQUIRED if verify or ca_cert else ssl.CERT_NONE
        self.http = urllib3.PoolManager(num_pools=10, maxsize=10, **kwargs)
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
        content = response.data
        body = content.decode('utf-8')
        if response.status == 500:
            msg = '{0} {1}'.format(response.status, body)
            if body.startswith('Invalid Session TTL'):
                raise InvalidSessionTTL(msg)
            elif body.startswith('invalid session'):
                raise InvalidSession(msg)
            else:
                raise ConsulInternalError(msg)
        return Response(response.status, response.headers, body, content)

    def uri(self, path, params=None):
        return '{0}{1}{2}'.format(self.base_uri, path, params and '?' + urlencode(params) or '')

    def __getattr__(self, method):
        if method not in ('get', 'post', 'put', 'delete'):
            raise AttributeError("HTTPClient instance has no attribute '{0}'".format(method))

        def wrapper(callback, path, params=None, data='', headers=None):
            # python-consul doesn't allow to specify ttl smaller then 10 seconds
            # because session_ttl_min defaults to 10s, so we have to do this ugly dirty hack...
            if method == 'put' and path == '/v1/session/create':
                ttl = '"ttl": "{0}s"'.format(self._ttl)
                if not data or data == '{}':
                    data = '{' + ttl + '}'
                else:
                    data = data[:-1] + ', ' + ttl + '}'
            if isinstance(params, list):  # starting from v1.1.0 python-consul switched from `dict` to `list` for params
                params = {k: v for k, v in params}
            kwargs = {'retries': 0, 'preload_content': False, 'body': data}
            if method == 'get' and isinstance(params, dict) and 'index' in params:
                timeout = float(params['wait'][:-1]) if 'wait' in params else 300
                # According to the documentation a small random amount of additional wait time is added to the
                # supplied maximum wait time to spread out the wake up time of any concurrent requests. This adds
                # up to wait / 16 additional time to the maximum duration. Since our goal is actually getting a
                # response rather read timeout we will add to the timeout a slightly bigger value.
                kwargs['timeout'] = timeout + max(timeout/15.0, 1)
            else:
                kwargs['timeout'] = self._read_timeout
            kwargs['headers'] = (headers or {}).copy()
            kwargs['headers'].update(urllib3.make_headers(user_agent=USER_AGENT))
            token = params.pop('token', self.token) if isinstance(params, dict) else self.token
            if token:
                kwargs['headers']['X-Consul-Token'] = token
            return callback(self.response(self.http.request(method.upper(), self.uri(path, params), **kwargs)))
        return wrapper


class ConsulClient(base.Consul):

    def __init__(self, *args, **kwargs):
        self._cert = kwargs.pop('cert', None)
        self._ca_cert = kwargs.pop('ca_cert', None)
        self.token = kwargs.get('token')
        super(ConsulClient, self).__init__(*args, **kwargs)

    def http_connect(self, *args, **kwargs):
        kwargs.update(dict(zip(['host', 'port', 'scheme', 'verify'], args)))
        if self._cert:
            kwargs['cert'] = self._cert
        if self._ca_cert:
            kwargs['ca_cert'] = self._ca_cert
        if self.token:
            kwargs['token'] = self.token
        return HTTPClient(**kwargs)

    def connect(self, *args, **kwargs):
        return self.http_connect(*args, **kwargs)

    def reload_config(self, config):
        self.http.token = self.token = config.get('token')
        self.consistency = config.get('consistency', 'default')
        self.dc = config.get('dc')


def catch_consul_errors(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (RetryFailedError, ConsulException, HTTPException, HTTPError, socket.error, socket.timeout):
            return False
    return wrapper


def force_if_last_failed(func):
    def wrapper(*args, **kwargs):
        if wrapper.last_result is False:
            kwargs['force'] = True
        wrapper.last_result = func(*args, **kwargs)
        return wrapper.last_result

    wrapper.last_result = None
    return wrapper


def service_name_from_scope_name(scope_name):
    """Translate scope name to service name which can be used in dns.

    230 = 253 - len('replica.') - len('.service.consul')
    """

    def replace_char(match):
        c = match.group(0)
        return '-' if c in '. _' else "u{:04d}".format(ord(c))

    service_name = re.sub(r'[^a-z0-9\-]', replace_char, scope_name.lower())
    return service_name[0:230]


class Consul(AbstractDCS):

    def __init__(self, config):
        super(Consul, self).__init__(config)
        self._base_path = self._base_path[1:]
        self._scope = config['scope']
        self._session = None
        self.__do_not_watch = False
        self._retry = Retry(deadline=config['retry_timeout'], max_delay=1, max_tries=-1,
                            retry_exceptions=(ConsulInternalError, HTTPException,
                                              HTTPError, socket.error, socket.timeout))

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

        config_keys = ('host', 'port', 'token', 'scheme', 'cert', 'ca_cert', 'dc', 'consistency')
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
        self.__session_checks = config.get('checks', [])
        self._register_service = config.get('register_service', False)
        self._previous_loop_register_service = self._register_service
        self._service_tags = sorted(config.get('service_tags', []))
        self._previous_loop_service_tags = self._service_tags
        if self._register_service:
            self._set_service_name()
        self._service_check_interval = config.get('service_check_interval', '5s')
        self._service_check_tls_server_name = config.get('service_check_tls_server_name', None)
        if not self._ctl:
            self.create_session()
        self._previous_loop_token = self._client.token

    def retry(self, *args, **kwargs):
        return self._retry.copy()(*args, **kwargs)

    def create_session(self):
        while not self._session:
            try:
                self.refresh_session()
            except ConsulError:
                logger.info('waiting on consul')
                time.sleep(5)

    def reload_config(self, config):
        super(Consul, self).reload_config(config)

        consul_config = config.get('consul', {})
        self._client.reload_config(consul_config)
        self._previous_loop_service_tags = self._service_tags
        self._service_tags = sorted(consul_config.get('service_tags', []))

        should_register_service = consul_config.get('register_service', False)
        if should_register_service and not self._register_service:
            self._set_service_name()

        self._previous_loop_register_service = self._register_service
        self._register_service = should_register_service

    def set_ttl(self, ttl):
        if self._client.http.set_ttl(ttl/2.0):  # Consul multiplies the TTL by 2x
            self._session = None
            self.__do_not_watch = True

    @property
    def ttl(self):
        return self._client.http.ttl * 2  # we multiply the value by 2 because it was divided in the `set_ttl()` method

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

    def _do_refresh_session(self, force=False):
        """:returns: `!True` if it had to create new session"""
        if not force and self._session and self._last_session_refresh + self._loop_wait > time.time():
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

    @staticmethod
    def member(node):
        return Member.from_node(node['ModifyIndex'], os.path.basename(node['Key']), node.get('Session'), node['Value'])

    def _cluster_from_nodes(self, nodes):
        # get initialize flag
        initialize = nodes.get(self._INITIALIZE)
        initialize = initialize and initialize['Value']

        # get global dynamic configuration
        config = nodes.get(self._CONFIG)
        config = config and ClusterConfig.from_node(config['ModifyIndex'], config['Value'])

        # get timeline history
        history = nodes.get(self._HISTORY)
        history = history and TimelineHistory.from_node(history['ModifyIndex'], history['Value'])

        # get last known leader lsn and slots
        status = nodes.get(self._STATUS)
        if status:
            try:
                status = json.loads(status['Value'])
                last_lsn = status.get(self._OPTIME)
                slots = status.get('slots')
            except Exception:
                slots = last_lsn = None
        else:
            last_lsn = nodes.get(self._LEADER_OPTIME)
            last_lsn = last_lsn and last_lsn['Value']
            slots = None

        try:
            last_lsn = int(last_lsn)
        except Exception:
            last_lsn = 0

        # get list of members
        members = [self.member(n) for k, n in nodes.items() if k.startswith(self._MEMBERS) and k.count('/') == 1]

        # get leader
        leader = nodes.get(self._LEADER)

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

        # get failsafe topology
        failsafe = nodes.get(self._FAILSAFE)
        try:
            failsafe = json.loads(failsafe['Value']) if failsafe else None
        except Exception:
            failsafe = None

        return Cluster(initialize, config, leader, last_lsn, members, failover, sync, history, slots, failsafe)

    def _cluster_loader(self, path):
        _, results = self.retry(self._client.kv.get, path, recurse=True)
        if results is None:
            raise NotFound
        nodes = {}
        for node in results:
            node['Value'] = (node['Value'] or b'').decode('utf-8')
            nodes[node['Key'][len(path):]] = node

        return self._cluster_from_nodes(nodes)

    def _citus_cluster_loader(self, path):
        _, results = self.retry(self._client.kv.get, path, recurse=True)
        clusters = defaultdict(dict)
        for node in results or []:
            key = node['Key'][len(path):].split('/', 1)
            if len(key) == 2 and citus_group_re.match(key[0]):
                node['Value'] = (node['Value'] or b'').decode('utf-8')
                clusters[int(key[0])][key[1]] = node
        return {group: self._cluster_from_nodes(nodes) for group, nodes in clusters.items()}

    def _load_cluster(self, path, loader):
        try:
            return loader(path)
        except NotFound:
            return Cluster(None, None, None, None, [], None, None, None, None, None)
        except Exception:
            logger.exception('get_cluster')
            raise ConsulError('Consul is not responding properly')

    @catch_consul_errors
    def touch_member(self, data):
        cluster = self.cluster
        member = cluster and cluster.get_member(self._name, fallback_to_leader=False)

        try:
            create_member = self.refresh_session()
        except DCSError:
            return False

        if member and (create_member or member.session != self._session):
            self._client.kv.delete(self.member_path)
            create_member = True

        if self._register_service or self._previous_loop_register_service:
            try:
                self.update_service(not create_member and member and member.data or {}, data)
            except Exception:
                logger.exception('update_service')

        if not create_member and member and deep_compare(data, member.data):
            return True

        try:
            self._client.kv.put(self.member_path, json.dumps(data, separators=(',', ':')), acquire=self._session)
            return True
        except InvalidSession:
            self._session = None
            logger.error('Our session disappeared from Consul, can not "touch_member"')
        except Exception:
            logger.exception('touch_member')
        return False

    def _set_service_name(self):
        self._service_name = service_name_from_scope_name(self._scope)
        if self._scope != self._service_name:
            logger.warning('Using %s as consul service name instead of scope name %s', self._service_name, self._scope)

    @catch_consul_errors
    def register_service(self, service_name, **kwargs):
        logger.info('Register service %s, params %s', service_name, kwargs)
        return self._client.agent.service.register(service_name, **kwargs)

    @catch_consul_errors
    def deregister_service(self, service_id):
        logger.info('Deregister service %s', service_id)
        # service_id can contain special characters, but is used as part of uri in deregister request
        service_id = quote(service_id)
        return self._client.agent.service.deregister(service_id)

    def _update_service(self, data):
        service_name = self._service_name
        role = data['role'].replace('_', '-')
        state = data['state']
        api_parts = urlparse(data['api_url'])
        api_parts = api_parts._replace(path='/{0}'.format(role))
        conn_parts = urlparse(data['conn_url'])
        check = base.Check.http(api_parts.geturl(), self._service_check_interval,
                                deregister='{0}s'.format(self._client.http.ttl * 10))
        if self._service_check_tls_server_name is not None:
            check['TLSServerName'] = self._service_check_tls_server_name
        tags = self._service_tags[:]
        tags.append(role)
        if role == 'master':
            tags.append('primary')
        elif role == 'primary':
            tags.append('master')
        self._previous_loop_service_tags = self._service_tags
        self._previous_loop_token = self._client.token

        params = {
            'service_id': '{0}/{1}'.format(self._scope, self._name),
            'address': conn_parts.hostname,
            'port': conn_parts.port,
            'check': check,
            'tags': tags,
            'enable_tag_override': True,
        }

        if state == 'stopped' or (not self._register_service and self._previous_loop_register_service):
            self._previous_loop_register_service = self._register_service
            return self.deregister_service(params['service_id'])

        self._previous_loop_register_service = self._register_service
        if role in ['master', 'primary', 'replica', 'standby-leader']:
            if state != 'running':
                return
            return self.register_service(service_name, **params)

        logger.warning('Could not register service: unknown role type %s', role)

    @force_if_last_failed
    def update_service(self, old_data, new_data, force=False):
        update = False

        for key in ['role', 'api_url', 'conn_url', 'state']:
            if key not in new_data:
                logger.warning('Could not register service: not enough params in member data')
                return
            if old_data.get(key) != new_data[key]:
                update = True

        if (
            force or update or self._register_service != self._previous_loop_register_service
            or self._service_tags != self._previous_loop_service_tags
            or self._client.token != self._previous_loop_token
        ):
            return self._update_service(new_data)

    def _do_attempt_to_acquire_leader(self, retry):
        try:
            return retry(self._client.kv.put, self.leader_path, self._name, acquire=self._session)
        except InvalidSession:
            logger.error('Our session disappeared from Consul. Will try to get a new one and retry attempt')
            self._session = None
            retry.deadline = retry.stoptime - time.time()

            retry(self._do_refresh_session)

            retry.deadline = retry.stoptime - time.time()
            if retry.deadline < 1:
                raise ConsulError('_do_attempt_to_acquire_leader timeout')

            return retry(self._client.kv.put, self.leader_path, self._name, acquire=self._session)

    @catch_return_false_exception
    def attempt_to_acquire_leader(self):
        retry = self._retry.copy()
        self._run_and_handle_exceptions(self._do_refresh_session, retry=retry)

        retry.deadline = retry.stoptime - time.time()
        if retry.deadline < 1:
            raise ConsulError('attempt_to_acquire_leader timeout')

        ret = self._run_and_handle_exceptions(self._do_attempt_to_acquire_leader, retry, retry=None)
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
    def _write_leader_optime(self, last_lsn):
        return self._client.kv.put(self.leader_optime_path, last_lsn)

    @catch_consul_errors
    def _write_status(self, value):
        return self._client.kv.put(self.status_path, value)

    @catch_consul_errors
    def _write_failsafe(self, value):
        return self._client.kv.put(self.failsafe_path, value)

    @staticmethod
    def _run_and_handle_exceptions(method, *args, **kwargs):
        retry = kwargs.pop('retry', None)
        try:
            return retry(method, *args, **kwargs) if retry else method(*args, **kwargs)
        except (RetryFailedError, InvalidSession, HTTPException, HTTPError, socket.error, socket.timeout) as e:
            raise ConsulError(e)
        except ConsulException:
            raise ReturnFalseException

    @catch_return_false_exception
    def _update_leader(self):
        retry = self._retry.copy()

        self._run_and_handle_exceptions(self._do_refresh_session, True, retry=retry)

        if self._session:
            cluster = self.cluster
            leader_session = cluster and isinstance(cluster.leader, Leader) and cluster.leader.session
            if leader_session != self._session:
                retry.deadline = retry.stoptime - time.time()
                if retry.deadline < 1:
                    raise ConsulError('update_leader timeout')
                logger.warning('Recreating the leader key due to session mismatch')
                if cluster.leader:
                    self._run_and_handle_exceptions(self._client.kv.delete, self.leader_path, cas=cluster.leader.index)

                retry.deadline = retry.stoptime - time.time()
                if retry.deadline < 0.5:
                    raise ConsulError('update_leader timeout')
                self._run_and_handle_exceptions(self._client.kv.put, self.leader_path,
                                                self._name, acquire=self._session)

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
    def _delete_leader(self):
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
        self._last_session_refresh = 0
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
                    logger.exception('watch')

                timeout = end_time - time.time()

        try:
            return super(Consul, self).watch(None, timeout)
        finally:
            self.event.clear()
