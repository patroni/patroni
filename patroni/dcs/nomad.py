import json
import logging
import os
import socket
import ssl

from collections import defaultdict
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple, Union
from urllib.parse import quote, urlencode, urlparse

import urllib3

from urllib3.exceptions import HTTPError

from ..exceptions import DCSError
from ..postgresql.mpp import AbstractMPP
from ..utils import parse_bool, Retry, RetryFailedError, split_host_port, uri, USER_AGENT
from . import AbstractDCS, Cluster, ClusterConfig, Failover, Leader, Member, Status, SyncState, TimelineHistory

logger = logging.getLogger(__name__)


class NomadError(DCSError):
    pass


class NomadConflict(Exception):
    pass


class NomadNotFound(Exception):
    pass


class NomadClient(object):

    def __init__(self, host: str = '127.0.0.1', port: int = 4646, token: Optional[str] = None,
                 scheme: str = 'http', verify: bool = True, cert: Optional[str] = None,
                 key: Optional[str] = None, cacert: Optional[str] = None, namespace: Optional[str] = None,
                 region: Optional[str] = None) -> None:
        self.base_uri = uri(scheme, (host, port))
        self.token = token
        self.namespace = namespace
        self.region = region
        self._read_timeout = 10.0
        kwargs: Dict[str, Any] = {'cert_reqs': ssl.CERT_REQUIRED if verify or cacert else ssl.CERT_NONE}
        if cert:
            kwargs['cert_file'] = cert
        if key:
            kwargs['key_file'] = key
        if cacert:
            kwargs['ca_certs'] = cacert
        self.http = urllib3.PoolManager(num_pools=10, maxsize=10, **kwargs)

    def set_read_timeout(self, timeout: float) -> None:
        self._read_timeout = timeout / 3.0

    def _request(self, method: str, endpoint: str, params: Optional[Mapping[str, Any]] = None,
                 body: Optional[Dict[str, Any]] = None) -> Tuple[Any, Mapping[str, str]]:
        query = dict(params or {})
        if self.namespace:
            query['namespace'] = self.namespace
        if self.region:
            query['region'] = self.region
        url = self.base_uri + endpoint + (query and '?' + urlencode(query) or '')
        headers = urllib3.make_headers(user_agent=USER_AGENT)
        if self.token:
            headers['X-Nomad-Token'] = self.token
        if body is not None:
            headers['Content-Type'] = 'application/json'
        try:
            response = self.http.request(method, url, body=body is not None and json.dumps(body) or None,
                                         headers=headers, timeout=self._read_timeout, retries=0)
        except (HTTPError, socket.error, socket.timeout) as e:
            raise NomadError(str(e))
        content = response.data or b''
        message = content.decode('utf-8', errors='replace')
        if response.status == 404:
            raise NomadNotFound(message)
        if response.status == 409:
            raise NomadConflict(message)
        if response.status < 200 or response.status >= 300:
            raise NomadError('{0}: {1}'.format(response.status, message))
        if not content:
            return True, response.headers
        try:
            return json.loads(message), response.headers
        except (TypeError, ValueError) as e:
            raise NomadError('Invalid response from Nomad: {0}'.format(e))

    @staticmethod
    def _path(path: str) -> str:
        return quote(path.lstrip('/'), safe='/~')

    def get_variable(self, path: str) -> Dict[str, Any]:
        return self._request('GET', '/v1/var/' + self._path(path))[0]

    def list_variables(self, prefix: str) -> List[Dict[str, Any]]:
        ret: List[Dict[str, Any]] = []
        params: Dict[str, Any] = {'prefix': prefix}
        while True:
            values, headers = self._request('GET', '/v1/vars', params)
            ret.extend(values)
            next_token = headers.get('X-Nomad-NextToken')
            if not next_token:
                return ret
            params['next_token'] = next_token

    def put_variable(self, path: str, value: str, cas: Optional[int] = None,
                     lock_id: Optional[str] = None) -> Dict[str, Any]:
        params = {'cas': cas} if cas is not None else None
        body: Dict[str, Any] = {'Items': {'value': value}}
        if lock_id:
            body['Lock'] = {'ID': lock_id}
        return self._request('PUT', '/v1/var/' + self._path(path), params, body)[0]

    def delete_variable(self, path: str, cas: Optional[int] = None) -> bool:
        params = {'cas': cas} if cas is not None else None
        return bool(self._request('DELETE', '/v1/var/' + self._path(path), params)[0])

    def acquire_lock(self, path: str, value: str, ttl: int, lock_delay: int) -> Dict[str, Any]:
        body = {'Items': {'value': value},
                'Lock': {'TTL': '{0}s'.format(ttl), 'LockDelay': '{0}s'.format(lock_delay)}}
        return self._request('PUT', '/v1/var/' + self._path(path), {'lock-acquire': ''}, body)[0]

    def renew_lock(self, path: str, lock_id: str) -> Dict[str, Any]:
        body = {'Lock': {'ID': lock_id}}
        return self._request('PUT', '/v1/var/' + self._path(path), {'lock-renew': ''}, body)[0]

    def release_lock(self, path: str, lock_id: str) -> Dict[str, Any]:
        body = {'Lock': {'ID': lock_id}}
        return self._request('PUT', '/v1/var/' + self._path(path), {'lock-release': ''}, body)[0]


def catch_nomad_errors(func: Callable[..., Any]) -> Callable[..., Any]:
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except (NomadError, NomadConflict, NomadNotFound, RetryFailedError):
            return False
    return wrapper


class Nomad(AbstractDCS):

    def __init__(self, config: Dict[str, Any], mpp: AbstractMPP) -> None:
        super(Nomad, self).__init__(config, mpp)
        self._base_path = self._base_path[1:]
        self._retry = Retry(deadline=config['retry_timeout'], max_delay=1, max_tries=-1,
                            retry_exceptions=NomadError)
        self._ttl = 30
        self._lock_delay = int(config.get('lock_delay', 10))
        if self._lock_delay < 10 or self._lock_delay > 86400:
            raise ValueError('Nomad lock delay must be between 10 and 86400 seconds')
        self._member_lock: Optional[str] = None
        self._leader_lock: Optional[str] = None
        self._member_value: Optional[str] = None

        host, port, scheme = '127.0.0.1', 4646, config.get('scheme', 'http')
        if config.get('url'):
            parsed = urlparse(config['url'])
            scheme, host, port = parsed.scheme, parsed.hostname, parsed.port or 4646
        elif config.get('host'):
            host, parsed_port = split_host_port(config['host'], 4646)
            port = int(config.get('port', parsed_port))
        elif config.get('port'):
            port = int(config['port'])

        verify = config.get('verify', True)
        if not isinstance(verify, bool):
            verify = parse_bool(verify)
        self._client = NomadClient(host=host, port=port, scheme=scheme, token=config.get('token'),
                                   verify=verify is not False, cert=config.get('cert'), key=config.get('key'),
                                   cacert=config.get('cacert'), namespace=config.get('nomad_namespace'),
                                   region=config.get('region'))
        self.set_retry_timeout(config['retry_timeout'])
        self.set_ttl(config.get('ttl') or 30)

    def set_ttl(self, ttl: int) -> Optional[bool]:
        if ttl < 10 or ttl > 86400:
            raise ValueError('Nomad lock TTL must be between 10 and 86400 seconds')
        changed = self._ttl != ttl
        self._ttl = ttl
        if changed:
            self.event.set()
        return changed

    @property
    def ttl(self) -> int:
        return self._ttl

    def set_retry_timeout(self, retry_timeout: int) -> None:
        self._retry.deadline = retry_timeout
        self._client.set_read_timeout(retry_timeout)

    @staticmethod
    def _value(node: Optional[Dict[str, Any]]) -> Optional[str]:
        return node and node.get('Items', {}).get('value')

    @staticmethod
    def _lock_id(node: Optional[Dict[str, Any]]) -> Optional[str]:
        return node and node.get('Lock', {}).get('ID')

    @classmethod
    def member(cls, node: Dict[str, Any]) -> Member:
        return Member.from_node(node['ModifyIndex'], os.path.basename(node['Path']),
                                cls._lock_id(node), cls._value(node))

    def _cluster_from_nodes(self, nodes: Dict[str, Dict[str, Any]]) -> Cluster:
        initialize = self._value(nodes.get(self._INITIALIZE))
        config_node = nodes.get(self._CONFIG)
        config = config_node and ClusterConfig.from_node(config_node['ModifyIndex'], self._value(config_node))
        history_node = nodes.get(self._HISTORY)
        history = history_node and TimelineHistory.from_node(history_node['ModifyIndex'], self._value(history_node))
        status = Status.from_node(self._value(nodes.get(self._STATUS) or nodes.get(self._LEADER_OPTIME)))
        members = [self.member(node) for key, node in nodes.items()
                   if key.startswith(self._MEMBERS) and key.count('/') == 1 and self._lock_id(node)]

        leader_node = nodes.get(self._LEADER)
        leader = None
        if leader_node and self._lock_id(leader_node):
            leader_name = self._value(leader_node) or ''
            member = next((item for item in members if item.name == leader_name),
                          Member(-1, leader_name, None, {}))
            leader = Leader(leader_node['ModifyIndex'], self._lock_id(leader_node), member)

        failover_node = nodes.get(self._FAILOVER)
        failover = failover_node and Failover.from_node(failover_node['ModifyIndex'], self._value(failover_node))
        sync_node = nodes.get(self._SYNC)
        sync = SyncState.from_node(sync_node and sync_node['ModifyIndex'], self._value(sync_node))
        failsafe_node = nodes.get(self._FAILSAFE)
        try:
            failsafe = json.loads(self._value(failsafe_node)) if failsafe_node else None
        except (TypeError, ValueError):
            failsafe = None
        return Cluster(initialize, config, leader, status, members, failover, sync, history, failsafe)

    def _load_nodes(self, path: str) -> Dict[str, Dict[str, Any]]:
        nodes: Dict[str, Dict[str, Any]] = {}
        for metadata in self._client.list_variables(path):
            variable_path = metadata['Path']
            try:
                node = self._client.get_variable(variable_path)
            except NomadNotFound:
                continue
            nodes[variable_path[len(path):]] = node
        return nodes

    def _postgresql_cluster_loader(self, path: str) -> Cluster:
        return self._cluster_from_nodes(self._retry.copy()(self._load_nodes, path))

    def _mpp_cluster_loader(self, path: str) -> Dict[int, Cluster]:
        clusters: Dict[int, Dict[str, Dict[str, Any]]] = defaultdict(dict)
        for key, node in self._retry.copy()(self._load_nodes, path).items():
            parts = key.split('/', 1)
            if len(parts) == 2 and self._mpp.group_re.match(parts[0]):
                clusters[int(parts[0])][parts[1]] = node
        return {group: self._cluster_from_nodes(nodes) for group, nodes in clusters.items()}

    def _load_cluster(self, path: str, loader: Callable[[str], Union[Cluster, Dict[int, Cluster]]]
                      ) -> Union[Cluster, Dict[int, Cluster]]:
        try:
            return loader(path)
        except (NomadError, RetryFailedError) as e:
            logger.exception('get_cluster')
            raise NomadError('Nomad is not responding properly: {0}'.format(e))

    @catch_nomad_errors
    def touch_member(self, data: Dict[str, Any]) -> bool:
        value = json.dumps(data, separators=(',', ':'))
        if not self._member_lock:
            member = self.cluster and self.cluster.get_member(self._name, fallback_to_leader=False)
            if member and member.session:
                self._member_lock = member.session
                self._member_value = json.dumps(member.data, separators=(',', ':'))
        if not self._member_lock:
            result = self._client.acquire_lock(self.member_path, value, self._ttl, self._lock_delay)
            self._member_lock = self._lock_id(result)
            self._member_value = value
            return bool(self._member_lock)
        try:
            if value != self._member_value:
                self._client.put_variable(self.member_path, value, lock_id=self._member_lock)
                self._member_value = value
            self._client.renew_lock(self.member_path, self._member_lock)
            return True
        except (NomadConflict, NomadNotFound):
            self._member_lock = self._member_value = None
            return False

    def attempt_to_acquire_leader(self) -> bool:
        try:
            result = self._retry.copy()(self._client.acquire_lock, self.leader_path, self._name,
                                        self._ttl, self._lock_delay)
            self._leader_lock = self._lock_id(result)
            return bool(self._leader_lock)
        except NomadConflict:
            logger.info('Could not take out TTL lock')
            return False
        except RetryFailedError as e:
            raise NomadError(e)

    def take_leader(self) -> bool:
        return self.attempt_to_acquire_leader()

    def _update_leader(self, leader: Leader) -> bool:
        if not self._leader_lock and leader.name == self._name:
            self._leader_lock = leader.session
        if not self._leader_lock or leader.session != self._leader_lock or leader.name != self._name:
            return False
        try:
            self._retry.copy()(self._client.renew_lock, self.leader_path, self._leader_lock)
            return True
        except (NomadConflict, NomadNotFound):
            self._leader_lock = None
            return False
        except RetryFailedError as e:
            raise NomadError(e)

    def _set_value(self, path: str, value: str, version: Optional[int] = None) -> Dict[str, Any]:
        return self._client.put_variable(path, value, version)

    @catch_nomad_errors
    def set_failover_value(self, value: str, version: Optional[int] = None) -> bool:
        return bool(self._set_value(self.failover_path, value, version))

    @catch_nomad_errors
    def set_config_value(self, value: str, version: Optional[int] = None) -> bool:
        return bool(self._set_value(self.config_path, value, version))

    @catch_nomad_errors
    def _write_leader_optime(self, last_lsn: str) -> bool:
        return bool(self._set_value(self.leader_optime_path, last_lsn))

    @catch_nomad_errors
    def _write_status(self, value: str) -> bool:
        return bool(self._set_value(self.status_path, value))

    @catch_nomad_errors
    def _write_failsafe(self, value: str) -> bool:
        return bool(self._set_value(self.failsafe_path, value))

    @catch_nomad_errors
    def initialize(self, create_new: bool = True, sysid: str = '') -> bool:
        return bool(self._retry.copy()(self._client.put_variable, self.initialize_path, sysid,
                                       0 if create_new else None))

    @catch_nomad_errors
    def cancel_initialization(self) -> bool:
        return self._retry.copy()(self._client.delete_variable, self.initialize_path)

    @catch_nomad_errors
    def delete_cluster(self) -> bool:
        retry = self._retry.copy()

        def delete_all() -> bool:
            for metadata in self._client.list_variables(self.client_path('')):
                try:
                    node = self._client.get_variable(metadata['Path'])
                except NomadNotFound:
                    continue
                lock_id = self._lock_id(node)
                if lock_id:
                    node = self._client.release_lock(node['Path'], lock_id)
                self._client.delete_variable(metadata['Path'], node.get('ModifyIndex'))
            return True

        return retry(delete_all)

    @catch_nomad_errors
    def set_history_value(self, value: str) -> bool:
        return bool(self._set_value(self.history_path, value))

    @catch_nomad_errors
    def _delete_leader(self, leader: Leader) -> bool:
        if not self._leader_lock or leader.session != self._leader_lock or leader.name != self._name:
            return False
        result = self._client.release_lock(self.leader_path, self._leader_lock)
        self._leader_lock = None
        return self._client.delete_variable(self.leader_path, result['ModifyIndex'])

    @catch_nomad_errors
    def set_sync_state_value(self, value: str, version: Optional[int] = None) -> Union[int, bool]:
        result = self._set_value(self.sync_path, value, version)
        return result['ModifyIndex']

    @catch_nomad_errors
    def delete_sync_state(self, version: Optional[int] = None) -> bool:
        return self._client.delete_variable(self.sync_path, version)

    def watch(self, leader_version: Optional[Any], timeout: float) -> bool:
        try:
            return super(Nomad, self).watch(leader_version, timeout)
        finally:
            self.event.clear()
