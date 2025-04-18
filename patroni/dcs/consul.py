from __future__ import absolute_import

import json
import logging
import os
import re
import socket
import ssl
import time

from collections import defaultdict
from http.client import HTTPException
from typing import Any, Callable, Dict, List, Mapping, NamedTuple, Optional, Tuple, TYPE_CHECKING, Union
from urllib.parse import quote, urlencode, urlparse

import urllib3

from consul import base, Check, ConsulException, NotFound
from urllib3.exceptions import HTTPError

from ..exceptions import DCSError
from ..postgresql.misc import PostgresqlRole, PostgresqlState
from ..postgresql.mpp import AbstractMPP
from ..utils import deep_compare, parse_bool, Retry, RetryFailedError, split_host_port, uri, USER_AGENT
from . import AbstractDCS, catch_return_false_exception, Cluster, ClusterConfig, \
    Failover, Leader, Member, ReturnFalseException, Status, SyncState, TimelineHistory

if TYPE_CHECKING:  # pragma: no cover
    from ..config import Config

logger = logging.getLogger(__name__)


class ConsulError(DCSError):
    pass


class ConsulInternalError(ConsulException):
    """An internal Consul server error occurred"""


class InvalidSessionTTL(ConsulException):
    """Session TTL is too small or too big"""


class InvalidSession(ConsulException):
    """invalid session"""


class Response(NamedTuple):
    code: int
    headers: Union[Mapping[str, str], Mapping[bytes, bytes], None]
    body: str
    content: bytes


class HTTPClient(object):

    def __init__(self, host: str = '127.0.0.1', port: int = 8500, token: Optional[str] = None, scheme: str = 'http',
                 verify: bool = True, cert: Optional[str] = None, ca_cert: Optional[str] = None) -> None:
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
        self.http = urllib3.PoolManager(num_pools=10, maxsize=10, headers={}, **kwargs)
        self._ttl = 30

    def set_read_timeout(self, timeout: float) -> None:
        self._read_timeout = timeout / 3.0

    @property
    def ttl(self) -> int:
        return self._ttl

    def set_ttl(self, ttl: int) -> bool:
        ret = self._ttl != ttl
        self._ttl = ttl
        return ret

    @staticmethod
    def response(response: urllib3.response.HTTPResponse) -> Response:
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

    def uri(self, path: str,
            params: Union[None, Dict[str, Any], List[Tuple[str, Any]], Tuple[Tuple[str, Any], ...]] = None) -> str:
        return '{0}{1}{2}'.format(self.base_uri, path, params and '?' + urlencode(params) or '')

    def __getattr__(self, method: str) -> Callable[[Callable[[Response], Union[bool, Any, Tuple[str, Any]]],
                                                    str, Union[None, Dict[str, Any], List[Tuple[str, Any]]],
                                                    str, Optional[Dict[str, str]]], Union[bool, Any, Tuple[str, Any]]]:
        if method not in ('get', 'post', 'put', 'delete'):
            raise AttributeError("HTTPClient instance has no attribute '{0}'".format(method))

        def wrapper(callback: Callable[[Response], Union[bool, Any, Tuple[str, Any]]], path: str,
                    params: Union[None, Dict[str, Any], List[Tuple[str, Any]]] = None, data: str = '',
                    headers: Optional[Dict[str, str]] = None) -> Union[bool, Any, Tuple[str, Any]]:
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
            kwargs: Dict[str, Any] = {'retries': 0, 'preload_content': False, 'body': data}
            if method == 'get' and isinstance(params, dict) and 'index' in params:
                timeout = float(params['wait'][:-1]) if 'wait' in params else 300
                # According to the documentation a small random amount of additional wait time is added to the
                # supplied maximum wait time to spread out the wake up time of any concurrent requests. This adds
                # up to wait / 16 additional time to the maximum duration. Since our goal is actually getting a
                # response rather read timeout we will add to the timeout a slightly bigger value.
                kwargs['timeout'] = timeout + max(timeout / 15.0, 1)
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

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """
        Consul client with Patroni customisations.

        .. note::

            Parameters, *token*, *cert* and *ca_cert* are not passed to the parent class :class:`consul.base.Consul`.

        Original class documentation,

            *token* is an optional ``ACL token``. If supplied it will be used by
            default for all requests made with this client session. It's still
            possible to override this token by passing a token explicitly for a
            request.

            *consistency* sets the consistency mode to use by default for all reads
            that support the consistency option. It's still possible to override
            this by passing explicitly for a given request. *consistency* can be
            either 'default', 'consistent' or 'stale'.

            *dc* is the datacenter that this agent will communicate with.
            By default, the datacenter of the host is used.

            *verify* is whether to verify the SSL certificate for HTTPS requests

            *cert* client side certificates for HTTPS requests

        :param args: positional arguments to pass to :class:`consul.base.Consul`
        :param kwargs: keyword arguments, with *cert*, *ca_cert* and *token* removed, passed to
                       :class:`consul.base.Consul`
        """
        self._cert = kwargs.pop('cert', None)
        self._ca_cert = kwargs.pop('ca_cert', None)
        self.token = kwargs.get('token')
        super(ConsulClient, self).__init__(*args, **kwargs)

    def http_connect(self, *args: Any, **kwargs: Any) -> HTTPClient:
        kwargs.update(dict(zip(['host', 'port', 'scheme', 'verify'], args)))
        if self._cert:
            kwargs['cert'] = self._cert
        if self._ca_cert:
            kwargs['ca_cert'] = self._ca_cert
        if self.token:
            kwargs['token'] = self.token
        return HTTPClient(**kwargs)

    def connect(self, *args: Any, **kwargs: Any) -> HTTPClient:
        return self.http_connect(*args, **kwargs)  # pragma: no cover

    def reload_config(self, config: Dict[str, Any]) -> None:
        self.http.token = self.token = config.get('token')
        self.consistency = config.get('consistency', 'default')
        self.dc = config.get('dc')


def catch_consul_errors(func: Callable[..., Any]) -> Callable[..., Any]:
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except (RetryFailedError, ConsulException, HTTPException, HTTPError, socket.error, socket.timeout):
            return False
    return wrapper


def force_if_last_failed(func: Callable[..., Any]) -> Callable[..., Any]:
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        if getattr(wrapper, 'last_result', None) is False:
            kwargs['force'] = True
        last_result = func(*args, **kwargs)
        setattr(wrapper, 'last_result', last_result)
        return last_result

    setattr(wrapper, 'last_result', None)
    return wrapper


def service_name_from_scope_name(scope_name: str) -> str:
    """Translate scope name to service name which can be used in dns.

    230 = 253 - len('replica.') - len('.service.consul')
    """

    def replace_char(match: Any) -> str:
        c = match.group(0)
        return '-' if c in '. _' else "u{:04d}".format(ord(c))

    service_name = re.sub(r'[^a-z0-9\-]', replace_char, scope_name.lower())
    return service_name[0:230]


class Consul(AbstractDCS):

    def __init__(self, config: Dict[str, Any], mpp: AbstractMPP) -> None:
        super(Consul, self).__init__(config, mpp)
        self._base_path = self._base_path[1:]
        self._scope = config['scope']
        self._session = None
        self.__do_not_watch = False
        self._retry = Retry(deadline=config['retry_timeout'], max_delay=1, max_tries=-1,
                            retry_exceptions=(ConsulInternalError, HTTPException,
                                              HTTPError, socket.error, socket.timeout))

        if 'url' in config:
            url: str = config['url']
            r = urlparse(url)
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
        kwargs: Dict[str, Any] = {p: config.get(p) for p in config_keys if config.get(p)}

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

    def retry(self, method: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        return self._retry.copy()(method, *args, **kwargs)

    def create_session(self) -> None:
        while not self._session:
            try:
                self.refresh_session()
            except ConsulError:
                logger.info('waiting on consul')
                time.sleep(5)

    def reload_config(self, config: Union['Config', Dict[str, Any]]) -> None:
        super(Consul, self).reload_config(config)

        consul_config = config.get('consul', {})
        self._client.reload_config(consul_config)
        self._previous_loop_service_tags = self._service_tags
        self._service_tags: List[str] = consul_config.get('service_tags', [])
        self._service_tags.sort()

        should_register_service = consul_config.get('register_service', False)
        if should_register_service and not self._register_service:
            self._set_service_name()

        self._previous_loop_register_service = self._register_service
        self._register_service = should_register_service

    def set_ttl(self, ttl: int) -> Optional[bool]:
        if self._client.http.set_ttl(ttl / 2.0):  # Consul multiplies the TTL by 2x
            self._session = None
            self.__do_not_watch = True
        return None

    @property
    def ttl(self) -> int:
        return self._client.http.ttl * 2  # we multiply the value by 2 because it was divided in the `set_ttl()` method

    def set_retry_timeout(self, retry_timeout: int) -> None:
        self._retry.deadline = retry_timeout
        self._client.http.set_read_timeout(retry_timeout)

    def adjust_ttl(self) -> None:
        try:
            settings = self._client.agent.self()
            min_ttl = (settings['Config']['SessionTTLMin'] or 10000000000) / 1000000000.0
            logger.warning('Changing Session TTL from %s to %s', self._client.http.ttl, min_ttl)
            self._client.http.set_ttl(min_ttl)
        except Exception:
            logger.exception('adjust_ttl')

    def _do_refresh_session(self, force: bool = False) -> bool:
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

    def refresh_session(self) -> bool:
        try:
            return self.retry(self._do_refresh_session)
        except (ConsulException, RetryFailedError):
            logger.exception('refresh_session')
        raise ConsulError('Failed to renew/create session')

    @staticmethod
    def member(node: Dict[str, str]) -> Member:
        return Member.from_node(node['ModifyIndex'], os.path.basename(node['Key']), node.get('Session'), node['Value'])

    def _cluster_from_nodes(self, nodes: Dict[str, Any]) -> Cluster:
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
        status = nodes.get(self._STATUS) or nodes.get(self._LEADER_OPTIME)
        status = Status.from_node(status and status['Value'])

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

        return Cluster(initialize, config, leader, status, members, failover, sync, history, failsafe)

    @property
    def _consistency(self) -> str:
        return 'consistent' if self._ctl else self._client.consistency

    def _postgresql_cluster_loader(self, path: str) -> Cluster:
        """Load and build the :class:`Cluster` object from DCS, which represents a single PostgreSQL cluster.

        :param path: the path in DCS where to load :class:`Cluster` from.

        :returns: :class:`Cluster` instance.
        """
        _, results = self.retry(self._client.kv.get, path, recurse=True, consistency=self._consistency)
        if results is None:
            return Cluster.empty()
        nodes: Dict[str, Dict[str, Any]] = {}
        for node in results:
            node['Value'] = (node['Value'] or b'').decode('utf-8')
            nodes[node['Key'][len(path):]] = node

        return self._cluster_from_nodes(nodes)

    def _mpp_cluster_loader(self, path: str) -> Dict[int, Cluster]:
        """Load and build all PostgreSQL clusters from a single MPP cluster.

        :param path: the path in DCS where to load Cluster(s) from.

        :returns: all MPP groups as :class:`dict`, with group IDs as keys and :class:`Cluster` objects as values.
        """
        results: Optional[List[Dict[str, Any]]]
        _, results = self.retry(self._client.kv.get, path, recurse=True, consistency=self._consistency)
        clusters: Dict[int, Dict[str, Dict[str, Any]]] = defaultdict(dict)
        for node in results or []:
            key = node['Key'][len(path):].split('/', 1)
            if len(key) == 2 and self._mpp.group_re.match(key[0]):
                node['Value'] = (node['Value'] or b'').decode('utf-8')
                clusters[int(key[0])][key[1]] = node
        return {group: self._cluster_from_nodes(nodes) for group, nodes in clusters.items()}

    def _load_cluster(
            self, path: str, loader: Callable[[str], Union[Cluster, Dict[int, Cluster]]]
    ) -> Union[Cluster, Dict[int, Cluster]]:
        try:
            return loader(path)
        except Exception:
            logger.exception('get_cluster')
            raise ConsulError('Consul is not responding properly')

    @catch_consul_errors
    def touch_member(self, data: Dict[str, Any]) -> bool:
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

    def _set_service_name(self) -> None:
        self._service_name = service_name_from_scope_name(self._scope)
        if self._scope != self._service_name:
            logger.warning('Using %s as consul service name instead of scope name %s', self._service_name, self._scope)

    @catch_consul_errors
    def register_service(self, service_name: str, **kwargs: Any) -> bool:
        logger.info('Register service %s, params %s', service_name, kwargs)
        return self._client.agent.service.register(service_name, **kwargs)

    @catch_consul_errors
    def deregister_service(self, service_id: str) -> bool:
        logger.info('Deregister service %s', service_id)
        # service_id can contain special characters, but is used as part of uri in deregister request
        service_id = quote(service_id)
        return self._client.agent.service.deregister(service_id)

    def _update_service(self, data: Dict[str, Any]) -> Optional[bool]:
        service_name = self._service_name
        role = data['role'].replace('_', '-')
        state = data['state']
        api_url: str = data['api_url']
        api_parts = urlparse(api_url)
        api_parts = api_parts._replace(path='/{0}'.format(role))
        conn_url: str = data['conn_url']
        conn_parts = urlparse(conn_url)
        check = Check.http(api_parts.geturl(), self._service_check_interval,
                           deregister='{0}s'.format(self._client.http.ttl * 10))
        if self._service_check_tls_server_name is not None:
            check['TLSServerName'] = self._service_check_tls_server_name
        tags = self._service_tags[:]
        tags.append(role)
        if data['role'] == PostgresqlRole.PRIMARY:
            tags.append(PostgresqlRole.MASTER)
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

        if state == PostgresqlState.STOPPED or (not self._register_service and self._previous_loop_register_service):
            self._previous_loop_register_service = self._register_service
            return self.deregister_service(params['service_id'])

        self._previous_loop_register_service = self._register_service
        if data['role'] in [PostgresqlRole.PRIMARY, PostgresqlRole.REPLICA, PostgresqlRole.STANDBY_LEADER]:
            if state != PostgresqlState.RUNNING:
                return
            return self.register_service(service_name, **params)

        logger.warning('Could not register service: unknown role type %s', role)

    @force_if_last_failed
    def update_service(self, old_data: Dict[str, Any], new_data: Dict[str, Any], force: bool = False) -> Optional[bool]:
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

    def _do_attempt_to_acquire_leader(self, retry: Retry) -> bool:
        try:
            return retry(self._client.kv.put, self.leader_path, self._name, acquire=self._session)
        except InvalidSession:
            self._session = None

            if not retry.ensure_deadline(0):
                logger.error('Our session disappeared from Consul. Deadline exceeded, giving up')
                return False

            logger.error('Our session disappeared from Consul. Will try to get a new one and retry attempt')

            retry(self._do_refresh_session)

            retry.ensure_deadline(1, ConsulError('_do_attempt_to_acquire_leader timeout'))
            return retry(self._client.kv.put, self.leader_path, self._name, acquire=self._session)

    @catch_return_false_exception
    def attempt_to_acquire_leader(self) -> bool:
        retry = self._retry.copy()
        self._run_and_handle_exceptions(self._do_refresh_session, retry=retry)

        retry.ensure_deadline(1, ConsulError('attempt_to_acquire_leader timeout'))

        ret = self._run_and_handle_exceptions(self._do_attempt_to_acquire_leader, retry, retry=None)
        if not ret:
            logger.info('Could not take out TTL lock')

        return ret

    def take_leader(self) -> bool:
        return self.attempt_to_acquire_leader()

    @catch_consul_errors
    def set_failover_value(self, value: str, version: Optional[int] = None) -> bool:
        return self._client.kv.put(self.failover_path, value, cas=version)

    @catch_consul_errors
    def set_config_value(self, value: str, version: Optional[int] = None) -> bool:
        return self._client.kv.put(self.config_path, value, cas=version)

    @catch_consul_errors
    def _write_leader_optime(self, last_lsn: str) -> bool:
        return self._client.kv.put(self.leader_optime_path, last_lsn)

    @catch_consul_errors
    def _write_status(self, value: str) -> bool:
        return self._client.kv.put(self.status_path, value)

    @catch_consul_errors
    def _write_failsafe(self, value: str) -> bool:
        return self._client.kv.put(self.failsafe_path, value)

    @staticmethod
    def _run_and_handle_exceptions(method: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        retry = kwargs.pop('retry', None)
        try:
            return retry(method, *args, **kwargs) if retry else method(*args, **kwargs)
        except (RetryFailedError, InvalidSession, HTTPException, HTTPError, socket.error, socket.timeout) as e:
            raise ConsulError(e)
        except ConsulException:
            raise ReturnFalseException

    @catch_return_false_exception
    def _update_leader(self, leader: Leader) -> bool:
        retry = self._retry.copy()

        self._run_and_handle_exceptions(self._do_refresh_session, True, retry=retry)

        if self._session and leader.session != self._session:
            retry.ensure_deadline(1, ConsulError('update_leader timeout'))

            logger.warning('Recreating the leader key due to session mismatch')
            self._run_and_handle_exceptions(self._client.kv.delete, self.leader_path, cas=leader.version)

            retry.ensure_deadline(0.5, ConsulError('update_leader timeout'))

            self._run_and_handle_exceptions(self._client.kv.put, self.leader_path, self._name, acquire=self._session)

        return bool(self._session)

    @catch_consul_errors
    def initialize(self, create_new: bool = True, sysid: str = '') -> bool:
        kwargs = {'cas': 0} if create_new else {}
        return self.retry(self._client.kv.put, self.initialize_path, sysid, **kwargs)

    @catch_consul_errors
    def cancel_initialization(self) -> bool:
        return self.retry(self._client.kv.delete, self.initialize_path)

    @catch_consul_errors
    def delete_cluster(self) -> bool:
        return self.retry(self._client.kv.delete, self.client_path(''), recurse=True)

    @catch_consul_errors
    def set_history_value(self, value: str) -> bool:
        return self._client.kv.put(self.history_path, value)

    @catch_consul_errors
    def _delete_leader(self, leader: Leader) -> bool:
        return self._client.kv.delete(self.leader_path, cas=int(leader.version))

    @catch_consul_errors
    def set_sync_state_value(self, value: str, version: Optional[int] = None) -> Union[int, bool]:
        retry = self._retry.copy()
        ret = retry(self._client.kv.put, self.sync_path, value, cas=version)
        if ret:  # We have no other choice, only read after write :(
            if not retry.ensure_deadline(0.5):
                return False
            _, ret = self.retry(self._client.kv.get, self.sync_path, consistency='consistent')
            if ret and (ret.get('Value') or b'').decode('utf-8') == value:
                return ret['ModifyIndex']
        return False

    @catch_consul_errors
    def delete_sync_state(self, version: Optional[int] = None) -> bool:
        return self.retry(self._client.kv.delete, self.sync_path, cas=version)

    def watch(self, leader_version: Optional[int], timeout: float) -> bool:
        self._last_session_refresh = 0
        if self.__do_not_watch:
            self.__do_not_watch = False
            return True

        if leader_version:
            end_time = time.time() + timeout
            while timeout >= 1:
                try:
                    idx, _ = self._client.kv.get(self.leader_path, index=leader_version, wait=str(timeout) + 's')
                    return str(idx) != str(leader_version)
                except (ConsulException, HTTPException, HTTPError, socket.error, socket.timeout):
                    logger.exception('watch')

                timeout = end_time - time.time()

        try:
            return super(Consul, self).watch(None, timeout)
        finally:
            self.event.clear()
