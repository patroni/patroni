from __future__ import absolute_import

import abc
import json
import logging
import os
import random
import socket
import time

from collections import defaultdict
from copy import deepcopy
from http.client import HTTPException
from queue import Queue
from threading import Thread
from typing import Any, Callable, Collection, Dict, List, Optional, Tuple, Type, TYPE_CHECKING, Union
from urllib.parse import urlparse

import etcd
import urllib3.util.connection

from dns import resolver
from dns.exception import DNSException
from urllib3 import Timeout
from urllib3.exceptions import HTTPError, ProtocolError, ReadTimeoutError

from ..exceptions import DCSError
from ..postgresql.mpp import AbstractMPP
from ..request import get as requests_get
from ..utils import Retry, RetryFailedError, split_host_port, uri, USER_AGENT
from . import AbstractDCS, catch_return_false_exception, Cluster, ClusterConfig, \
    Failover, Leader, Member, ReturnFalseException, Status, SyncState, TimelineHistory

if TYPE_CHECKING:  # pragma: no cover
    from ..config import Config

logger = logging.getLogger(__name__)


class EtcdRaftInternal(etcd.EtcdException):
    """Raft Internal Error"""


class StaleEtcdNode(Exception):
    """Node is stale (raft term is older than previous known)."""


class EtcdError(DCSError):
    pass


_AddrInfo = Tuple[socket.AddressFamily, socket.SocketKind, int, str,
                  Union[Tuple[str, int], Tuple[str, int, int, int], Tuple[int, bytes]]]


class DnsCachingResolver(Thread):

    def __init__(self, cache_time: float = 600.0, cache_fail_time: float = 30.0) -> None:
        super(DnsCachingResolver, self).__init__()
        self._cache: Dict[Tuple[str, int], Tuple[float, List[_AddrInfo]]] = {}
        self._cache_time = cache_time
        self._cache_fail_time = cache_fail_time
        self._resolve_queue: Queue[Tuple[Tuple[str, int], int]] = Queue()
        self.daemon = True
        self.start()

    def run(self) -> None:
        while True:
            (host, port), attempt = self._resolve_queue.get()
            response = self._do_resolve(host, port)
            if response:
                self._cache[(host, port)] = (time.time(), response)
            else:
                if attempt < 10:
                    self.resolve_async(host, port, attempt + 1)
                    time.sleep(1)

    def resolve(self, host: str, port: int) -> List[_AddrInfo]:
        current_time = time.time()
        cached_time, response = self._cache.get((host, port), (0, []))
        time_passed = current_time - cached_time
        if time_passed > self._cache_time or (not response and time_passed > self._cache_fail_time):
            new_response = self._do_resolve(host, port)
            if new_response:
                self._cache[(host, port)] = (current_time, new_response)
                response = new_response
        return response

    def resolve_async(self, host: str, port: int, attempt: int = 0) -> None:
        self._resolve_queue.put(((host, port), attempt))

    def remove(self, host: str, port: int) -> None:
        self._cache.pop((host, port), None)

    @staticmethod
    def _do_resolve(host: str, port: int) -> List[_AddrInfo]:
        try:
            return socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM, socket.IPPROTO_TCP)
        except Exception as e:
            logger.warning('failed to resolve host %s: %s', host, e)
            return []


class StaleEtcdNodeGuard(object):

    def __init__(self) -> None:
        self._reset_cluster_raft_term()

    def _reset_cluster_raft_term(self) -> None:
        self._cluster_id = None
        self._raft_term = 0

    def _check_cluster_raft_term(self, cluster_id: Optional[str], value: Union[None, str, int]) -> None:
        """Check that observed Raft Term in Etcd cluster is increasing.

        :param cluster_id: last observed Etcd Cluster ID
        :param raft_term: last observed Raft Term

        :raises:
            :exc::`StaleEtcdNode` if last observed *raft_term* is smaller than previously known *raft_term*.
        """
        if not (cluster_id and value):
            return

        # We need to reset the memorized value when we notice that Cluster ID changed.
        if self._cluster_id and self._cluster_id != cluster_id:
            logger.warning('Etcd Cluster ID changed from %s to %s', self._cluster_id, cluster_id)
            self._raft_term = 0
        self._cluster_id = cluster_id

        try:
            raft_term = int(value)
        except Exception:
            return

        if raft_term < self._raft_term:
            logger.warning('Connected to Etcd node with term %d. Old known term %d. Switching to another node.',
                           raft_term, self._raft_term)
            raise StaleEtcdNode
        self._raft_term = raft_term


class AbstractEtcdClientWithFailover(abc.ABC, etcd.Client, StaleEtcdNodeGuard):

    ERROR_CLS: Type[Exception]

    def __init__(self, config: Dict[str, Any], dns_resolver: DnsCachingResolver, cache_ttl: int = 300) -> None:
        StaleEtcdNodeGuard.__init__(self)
        self._dns_resolver = dns_resolver
        self.set_machines_cache_ttl(cache_ttl)
        self._machines_cache_updated = 0
        kwargs = {p: config.get(p) for p in ('host', 'port', 'protocol', 'use_proxies', 'version_prefix',
                                             'username', 'password', 'cert', 'ca_cert') if config.get(p)}
        super(AbstractEtcdClientWithFailover, self).__init__(read_timeout=config['retry_timeout'], **kwargs)
        # For some reason python3-etcd on debian and ubuntu are not based on the latest version
        # Workaround for the case when https://github.com/jplana/python-etcd/pull/196 is not applied
        self.http.connection_pool_kw.pop('ssl_version', None)
        self._config = config
        self._load_machines_cache()
        self._allow_reconnect = True
        # allow passing retry argument to api_execute in params
        self._comparison_conditions.add('retry')
        self._read_options.add('retry')
        self._del_conditions.add('retry')

    def _calculate_timeouts(self, etcd_nodes: int, timeout: Optional[float] = None) -> Tuple[int, float, int]:
        """Calculate a request timeout and number of retries per single etcd node.
        In case if the timeout per node is too small (less than one second) we will reduce the number of nodes.
        For the cluster with only one node we will try to do 2 retries.
        For clusters with 2 nodes we will try to do 1 retry for every node.
        No retries for clusters with 3 or more nodes. We better rely on switching to a different node."""

        per_node_timeout = timeout = float(timeout or self.read_timeout)

        max_retries = 4 - min(etcd_nodes, 3)
        per_node_retries = 1
        min_timeout = 1.0

        while etcd_nodes > 0:
            per_node_timeout = float(timeout) / etcd_nodes
            if per_node_timeout >= min_timeout:
                # for small clusters we will try to do more than on try on every node
                while per_node_retries < max_retries and per_node_timeout / (per_node_retries + 1) >= min_timeout:
                    per_node_retries += 1
                per_node_timeout /= per_node_retries
                break
            # if the timeout per one node is to small try to reduce number of nodes
            etcd_nodes -= 1
            max_retries = 1

        return etcd_nodes, per_node_timeout, per_node_retries - 1

    def reload_config(self, config: Dict[str, Any]) -> None:
        self.username = config.get('username')
        self.password = config.get('password')

    def _get_headers(self) -> Dict[str, str]:
        basic_auth = ':'.join((self.username, self.password)) if self.username and self.password else None
        return urllib3.make_headers(basic_auth=basic_auth, user_agent=USER_AGENT)

    def _prepare_common_parameters(self, etcd_nodes: int, timeout: Optional[float] = None) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {'headers': self._get_headers(),
                                  'redirect': self.allow_redirect, 'preload_content': False}

        if timeout is not None:
            kwargs.update(retries=0, timeout=timeout)
        else:
            _, per_node_timeout, per_node_retries = self._calculate_timeouts(etcd_nodes)
            connect_timeout = max(1.0, per_node_timeout / 2.0)
            kwargs.update(timeout=Timeout(connect=connect_timeout, total=per_node_timeout), retries=per_node_retries)
        return kwargs

    def set_machines_cache_ttl(self, cache_ttl: int) -> None:
        self._machines_cache_ttl = cache_ttl

    @abc.abstractmethod
    def _prepare_get_members(self, etcd_nodes: int) -> Dict[str, Any]:
        """returns: request parameters"""

    @abc.abstractmethod
    def _get_members(self, base_uri: str, **kwargs: Any) -> List[str]:
        """returns: list of clientURLs"""

    @property
    def machines_cache(self) -> List[str]:
        base_uri, cache = self._base_uri, self._machines_cache
        return ([base_uri] if base_uri in cache else []) + [machine for machine in cache if machine != base_uri]

    def _get_machines_list(self, machines_cache: List[str]) -> List[str]:
        """Gets list of members from Etcd cluster using API

        :param machines_cache: initial list of Etcd members
        :returns: list of clientURLs retrieved from Etcd cluster
        :raises EtcdConnectionFailed: if failed"""
        kwargs = self._prepare_get_members(len(machines_cache))

        for base_uri in machines_cache:
            try:
                machines = list(set(self._get_members(base_uri, **kwargs)))
                logger.debug("Retrieved list of machines: %s", machines)
                if machines:
                    random.shuffle(machines)
                    if not self._use_proxies:
                        self._update_dns_cache(self._dns_resolver.resolve_async, machines)
                    return machines
            except Exception as e:
                self.http.clear()
                logger.error("Failed to get list of machines from %s%s: %r", base_uri, self.version_prefix, e)

        raise etcd.EtcdConnectionFailed('No more machines in the cluster')

    @property
    def machines(self) -> List[str]:
        """Original `machines` method(property) of `etcd.Client` class raise exception
        when it failed to get list of etcd cluster members. This method is being called
        only when request failed on one of the etcd members during `api_execute` call.
        For us it's more important to execute original request rather then get new topology
        of etcd cluster. So we will catch this exception and return empty list of machines.
        Later, during next `api_execute` call we will forcefully update machines_cache.

        Also this method implements the same timeout-retry logic as `api_execute`, because
        the original method was retrying 2 times with the `read_timeout` on each node.

        After the next refactoring the whole logic was moved to the _get_machines_list() method."""

        return self._get_machines_list(self.machines_cache)

    def set_read_timeout(self, timeout: float) -> None:
        self._read_timeout = timeout

    def _do_http_request(self, retry: Optional[Retry], machines_cache: List[str],
                         request_executor: Callable[..., urllib3.response.HTTPResponse],
                         method: str, path: str, fields: Optional[Dict[str, Any]] = None,
                         **kwargs: Any) -> Any:
        is_watch_request = isinstance(fields, dict) and fields.get('wait') == 'true'
        if fields is not None:
            kwargs['fields'] = fields
        some_request_failed = False
        for i, base_uri in enumerate(machines_cache):
            if i > 0:
                logger.info("Retrying on %s", base_uri)
            try:
                response = request_executor(method, base_uri + path, **kwargs)
                response.data.decode('utf-8')
                if some_request_failed:
                    self.set_base_uri(base_uri)
                    self._refresh_machines_cache()
                return self._handle_server_response(response)
            except (HTTPError, HTTPException, socket.error, socket.timeout, StaleEtcdNode) as e:
                self.http.clear()
                if not retry:
                    if len(machines_cache) == 1:
                        self.set_base_uri(self._base_uri)  # trigger Etcd3 watcher restart
                    # switch to the next etcd node because we don't know exactly what happened,
                    # whether the key didn't received an update or there is a network problem.
                    elif i + 1 < len(machines_cache):
                        self.set_base_uri(machines_cache[i + 1])
                if is_watch_request and isinstance(e, (ReadTimeoutError, ProtocolError)):
                    logger.debug("Watch timed out.")
                    raise etcd.EtcdWatchTimedOut("Watch timed out: {0}".format(e), cause=e)
                logger.error("Request to server %s failed: %r", base_uri, e)
                logger.info("Reconnection allowed, looking for another server.")
                if not retry:
                    raise etcd.EtcdException('{0} {1} request failed'.format(method, path))
                some_request_failed = True

        raise etcd.EtcdConnectionFailed('No more machines in the cluster')

    @abc.abstractmethod
    def _prepare_request(self, kwargs: Dict[str, Any], params: Optional[Dict[str, Any]] = None,
                         method: Optional[str] = None) -> Callable[..., urllib3.response.HTTPResponse]:
        """returns: request_executor"""

    def api_execute(self, path: str, method: str, params: Optional[Dict[str, Any]] = None,
                    timeout: Optional[float] = None) -> Any:
        retry = params.pop('retry', None) if isinstance(params, dict) else None

        # Update machines_cache if previous attempt of update has failed
        if self._update_machines_cache:
            try:
                self._load_machines_cache()
            except etcd.EtcdException as e:
                # If etcd cluster isn't accessible _load_machines_cache() -> _refresh_machines_cache() may raise
                # etcd.EtcdException. We need to convert it to etcd.EtcdConnectionFailed for failsafe_mode to work.
                raise etcd.EtcdConnectionFailed('No more machines in the cluster') from e
        elif not self._use_proxies and time.time() - self._machines_cache_updated > self._machines_cache_ttl:
            self._refresh_machines_cache()

        machines_cache = self.machines_cache
        etcd_nodes = len(machines_cache)

        kwargs = self._prepare_common_parameters(etcd_nodes, timeout)
        request_executor = self._prepare_request(kwargs, params, method)

        while True:
            try:
                return self._do_http_request(retry, machines_cache, request_executor, method, path, **kwargs)
            except etcd.EtcdWatchTimedOut:
                raise
            except etcd.EtcdConnectionFailed as ex:
                try:
                    if self._load_machines_cache():
                        machines_cache = self.machines_cache
                        etcd_nodes = len(machines_cache)
                except Exception as e:
                    logger.debug('Failed to update list of etcd nodes: %r', e)
                if TYPE_CHECKING:  # pragma: no cover
                    assert isinstance(retry, Retry)  # etcd.EtcdConnectionFailed is raised only if retry is not None!
                sleeptime = retry.sleeptime
                remaining_time = retry.stoptime - sleeptime - time.time()
                nodes, timeout, retries = self._calculate_timeouts(etcd_nodes, remaining_time)
                if nodes == 0:
                    self._update_machines_cache = True
                    self.set_base_uri(self._base_uri)  # trigger Etcd3 watcher restart
                    raise ex
                retry.sleep_func(sleeptime)
                retry.update_delay()
                # We still have some time left. Partially reduce `machines_cache` and retry request
                kwargs.update(timeout=Timeout(connect=max(1.0, timeout / 2.0), total=timeout), retries=retries)
                machines_cache = machines_cache[:nodes]

    @staticmethod
    def get_srv_record(host: str) -> List[Tuple[str, int]]:
        try:
            return [(r.target.to_text(True), r.port) for r in resolver.query(host, 'SRV')]
        except DNSException:
            return []

    def _get_machines_cache_from_srv(self, srv: str, srv_suffix: Optional[str] = None) -> List[str]:
        """Fetch list of etcd-cluster member by resolving _etcd-server._tcp. SRV record.
        This record should contain list of host and peer ports which could be used to run
        'GET http://{host}:{port}/members' request (peer protocol)"""

        ret: List[str] = []
        for r in ['-client-ssl', '-client', '-ssl', '', '-server-ssl', '-server']:
            r = '{0}-{1}'.format(r, srv_suffix) if srv_suffix else r
            protocol = 'https' if '-ssl' in r else 'http'
            endpoint = '/members' if '-server' in r else ''
            for host, port in self.get_srv_record('_etcd{0}._tcp.{1}'.format(r, srv)):
                url = uri(protocol, (host, port), endpoint)
                if endpoint:
                    try:
                        response = requests_get(url, timeout=self.read_timeout, verify=False)
                        if response.status < 400:
                            for member in json.loads(response.data.decode('utf-8')):
                                ret.extend(member['clientURLs'])
                            break
                    except Exception:
                        logger.exception('GET %s', url)
                else:
                    ret.append(url)
            if ret:
                self._protocol = protocol
                break
        else:
            logger.warning('Can not resolve SRV for %s', srv)
        return list(set(ret))

    def _get_machines_cache_from_dns(self, host: str, port: int) -> List[str]:
        """One host might be resolved into multiple ip addresses. We will make list out of it"""
        if self.protocol == 'http':
            # Filter out unexpected results when python is compiled with --disable-ipv6 and running on IPv6 system.
            ret = [uri(self.protocol, (res[4][0], res[4][1])) for res in self._dns_resolver.resolve(host, port)
                   if isinstance(res[4][0], str) and isinstance(res[4][1], int)]
            if ret:
                return list(set(ret))
        return [uri(self.protocol, (host, port))]

    def _get_machines_cache_from_config(self) -> List[str]:
        if 'proxy' in self._config:
            return [uri(self.protocol, (self._config['host'], self._config['port']))]

        machines_cache = []
        if 'srv' in self._config:
            machines_cache = self._get_machines_cache_from_srv(self._config['srv'], self._config.get('srv_suffix'))

        if not machines_cache and 'hosts' in self._config:
            machines_cache = list(self._config['hosts'])

        if not machines_cache and 'host' in self._config:
            machines_cache = self._get_machines_cache_from_dns(self._config['host'], self._config['port'])
        return machines_cache

    @staticmethod
    def _update_dns_cache(func: Callable[[str, int], None], machines: List[str]) -> None:
        for url in machines:
            r = urlparse(url)
            if r.hostname:
                port = r.port or (443 if r.scheme == 'https' else 80)
                func(r.hostname, port)

    def _load_machines_cache(self) -> bool:
        """This method should fill up `_machines_cache` from scratch.
        It could happen only in two cases:
        1. During class initialization
        2. When all etcd members failed"""

        self._update_machines_cache = True

        if 'srv' not in self._config and 'host' not in self._config and 'hosts' not in self._config:
            raise Exception('Neither srv, hosts, host nor url are defined in etcd section of config')

        machines_cache = self._get_machines_cache_from_config()
        # Can not bootstrap list of etcd-cluster members, giving up
        if not machines_cache:
            raise etcd.EtcdException

        # enforce resolving dns name,they might get new ips
        self._update_dns_cache(self._dns_resolver.remove, machines_cache)

        # after filling up the initial list of machines_cache we should ask etcd-cluster about actual list
        ret = self._refresh_machines_cache(machines_cache)

        self._update_machines_cache = False
        return ret

    def _refresh_machines_cache(self, machines_cache: Optional[List[str]] = None) -> bool:
        """Get etcd cluster topology using Etcd API and put it to self._machines_cache

        :param machines_cache: the list of nodes we want to run through executing API request
                               in addition to values stored in the self._machines_cache
        :returns: `True` if self._machines_cache was updated with new values
        :raises EtcdException: if failed to get topology and `machines_cache` was specified.

        The self._machines_cache will not be updated if nodes from the list are
        not accessible or if they are not returning correct results."""

        if self._use_proxies:
            value = self._get_machines_cache_from_config()
        else:
            try:
                # we want to go through the list obtained from the config file + last known health topology
                value = self._get_machines_list(list(set((machines_cache or []) + self.machines_cache)))
            except etcd.EtcdConnectionFailed:
                value = []

        if value:
            ret = set(self._machines_cache) != set(value)
            self._machines_cache = value
        elif machines_cache:  # we are just starting or all nodes were not available at some point
            raise etcd.EtcdException("Could not get the list of servers, "
                                     "maybe you provided the wrong "
                                     "host(s) to connect to?")
        else:
            return False

        if self._base_uri not in self._machines_cache:
            self.set_base_uri(self._machines_cache[0])
        self._machines_cache_updated = time.time()
        return ret

    def set_base_uri(self, value: str) -> None:
        if self._base_uri != value:
            logger.info('Selected new etcd server %s', value)
            self._base_uri = value


class EtcdClient(AbstractEtcdClientWithFailover):

    ERROR_CLS = EtcdError

    def __init__(self, config: Dict[str, Any], dns_resolver: DnsCachingResolver, cache_ttl: int = 300) -> None:
        super(EtcdClient, self).__init__({**config, 'version_prefix': None}, dns_resolver, cache_ttl)

    def __del__(self) -> None:
        try:
            self.http.clear()
        except (ReferenceError, TypeError, AttributeError):
            pass

    def _prepare_get_members(self, etcd_nodes: int) -> Dict[str, Any]:
        return self._prepare_common_parameters(etcd_nodes)

    def _handle_server_response(self, response: urllib3.response.HTTPResponse) -> Any:
        self._check_cluster_raft_term(response.headers.get('x-etcd-cluster-id'), response.headers.get('x-raft-term'))
        return super(EtcdClient, self)._handle_server_response(response)

    def _get_members(self, base_uri: str, **kwargs: Any) -> List[str]:
        response = self.http.request(self._MGET, base_uri + self.version_prefix + '/machines', **kwargs)
        data = self._handle_server_response(response).data.decode('utf-8')
        return [m.strip() for m in data.split(',') if m.strip()]

    def _prepare_request(self, kwargs: Dict[str, Any], params: Optional[Dict[str, Any]] = None,
                         method: Optional[str] = None) -> Callable[..., urllib3.response.HTTPResponse]:
        kwargs['fields'] = params
        if method in (self._MPOST, self._MPUT):
            kwargs['encode_multipart'] = False
        return self.http.request


class AbstractEtcd(AbstractDCS):

    def __init__(self, config: Dict[str, Any], mpp: AbstractMPP, client_cls: Type[AbstractEtcdClientWithFailover],
                 retry_errors_cls: Union[Type[Exception], Tuple[Type[Exception], ...]]) -> None:
        super(AbstractEtcd, self).__init__(config, mpp)
        self._retry = Retry(deadline=config['retry_timeout'], max_delay=1, max_tries=-1,
                            retry_exceptions=retry_errors_cls)
        self._ttl = int(config.get('ttl') or 30)
        self._abstract_client = self.get_etcd_client(config, client_cls)
        self.__do_not_watch = False
        self._has_failed = False

    @property
    @abc.abstractmethod
    def _client(self) -> AbstractEtcdClientWithFailover:
        """return correct type of etcd client"""

    def reload_config(self, config: Union['Config', Dict[str, Any]]) -> None:
        super(AbstractEtcd, self).reload_config(config)
        self._client.reload_config(config.get(self.__class__.__name__.lower(), {}))

    def retry(self, method: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        retry = self._retry.copy()
        kwargs['retry'] = retry
        return retry(method, *args, **kwargs)

    def _handle_exception(self, e: Exception, name: str = '', do_sleep: bool = False,
                          raise_ex: Optional[Exception] = None) -> None:
        if not self._has_failed:
            logger.exception(name)
        else:
            logger.error(e)
            if do_sleep:
                time.sleep(1)
        self._has_failed = True
        if isinstance(raise_ex, Exception):
            raise raise_ex

    def handle_etcd_exceptions(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        try:
            retval = func(self, *args, **kwargs)
            self._has_failed = False
            return retval
        except (RetryFailedError, etcd.EtcdException) as e:
            self._handle_exception(e)
            return False
        except Exception as e:
            self._handle_exception(e, raise_ex=self._client.ERROR_CLS('unexpected error'))

    def _run_and_handle_exceptions(self, method: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        retry = kwargs.pop('retry', self.retry)
        try:
            return retry(method, *args, **kwargs) if retry else method(*args, **kwargs)
        except (RetryFailedError, etcd.EtcdConnectionFailed) as e:
            raise self._client.ERROR_CLS(e)
        except etcd.EtcdException as e:
            self._handle_exception(e)
            raise ReturnFalseException
        except Exception as e:
            self._handle_exception(e, raise_ex=self._client.ERROR_CLS('unexpected error'))

    def set_socket_options(self, sock: socket.socket,
                           socket_options: Optional[Collection[Tuple[int, int, int]]]) -> None:
        if socket_options:
            for opt in socket_options:
                sock.setsockopt(*opt)

    def get_etcd_client(self, config: Dict[str, Any],
                        client_cls: Type[AbstractEtcdClientWithFailover]) -> AbstractEtcdClientWithFailover:
        config = deepcopy(config)
        if 'proxy' in config:
            config['use_proxies'] = True
            config['url'] = config['proxy']

        if 'url' in config and isinstance(config['url'], str):
            r = urlparse(config['url'])
            config.update({'protocol': r.scheme, 'host': r.hostname, 'port': r.port or 2379,
                           'username': r.username, 'password': r.password})
        elif 'hosts' in config:
            hosts = config.pop('hosts')
            default_port = config.pop('port', 2379)
            protocol = config.get('protocol', 'http')

            if isinstance(hosts, str):
                hosts = hosts.split(',')

            config_hosts: List[str] = []
            for value in hosts:
                if isinstance(value, str):
                    config_hosts.append(uri(protocol, split_host_port(value.strip(), default_port)))
            config['hosts'] = config_hosts
        elif 'host' in config:
            host, port = split_host_port(config['host'], 2379)
            config['host'] = host
            if 'port' not in config:
                config['port'] = int(port)

        if config.get('cacert'):
            config['ca_cert'] = config.pop('cacert')

        if config.get('key') and config.get('cert'):
            config['cert'] = (config['cert'], config['key'])

        for p in ('discovery_srv', 'srv_domain'):
            if p in config:
                config['srv'] = config.pop(p)

        dns_resolver = DnsCachingResolver()

        def create_connection_patched(
                address: Tuple[str, int], timeout: Any = object(),
                source_address: Optional[Any] = None, socket_options: Optional[Collection[Tuple[int, int, int]]] = None
        ) -> socket.socket:
            host, port = address
            if host.startswith('['):
                host = host.strip('[]')
            err = None
            for af, socktype, proto, _, sa in dns_resolver.resolve(host, port):
                sock = None
                try:
                    sock = socket.socket(af, socktype, proto)
                    self.set_socket_options(sock, socket_options)
                    if timeout is None or isinstance(timeout, (float, int)):
                        sock.settimeout(timeout)
                    if source_address:
                        sock.bind(source_address)
                    sock.connect(sa)
                    return sock

                except socket.error as e:
                    err = e
                    if sock is not None:
                        sock.close()
                        sock = None

            if err is not None:
                raise err

            raise socket.error("getaddrinfo returns an empty list")

        urllib3.util.connection.create_connection = create_connection_patched

        client = None
        while not client:
            try:
                client = client_cls(config, dns_resolver)
                if 'use_proxies' in config and not client.machines:
                    raise etcd.EtcdException
            except etcd.EtcdException:
                logger.info('waiting on etcd')
                time.sleep(5)
        return client

    def set_ttl(self, ttl: int) -> Optional[bool]:
        ttl = int(ttl)
        ret = self._ttl != ttl
        self._ttl = ttl
        self._client.set_machines_cache_ttl(ttl * 10)
        return ret

    @property
    def ttl(self) -> int:
        return self._ttl

    def set_retry_timeout(self, retry_timeout: int) -> None:
        self._retry.deadline = retry_timeout
        self._client.set_read_timeout(retry_timeout)


def catch_etcd_errors(func: Callable[..., Any]) -> Any:
    def wrapper(self: AbstractEtcd, *args: Any, **kwargs: Any) -> Any:
        return self.handle_etcd_exceptions(func, *args, **kwargs)
    return wrapper


class Etcd(AbstractEtcd):

    def __init__(self, config: Dict[str, Any], mpp: AbstractMPP) -> None:
        super(Etcd, self).__init__(config, mpp, EtcdClient, (etcd.EtcdLeaderElectionInProgress, EtcdRaftInternal))
        self.__do_not_watch = False

    @property
    def _client(self) -> EtcdClient:
        if TYPE_CHECKING:  # pragma: no cover
            assert isinstance(self._abstract_client, EtcdClient)
        return self._abstract_client

    def set_ttl(self, ttl: int) -> Optional[bool]:
        self.__do_not_watch = super(Etcd, self).set_ttl(ttl)
        return None

    @staticmethod
    def member(node: etcd.EtcdResult) -> Member:
        return Member.from_node(node.modifiedIndex, os.path.basename(node.key), node.ttl, node.value)

    def _cluster_from_nodes(self, etcd_index: int, nodes: Dict[str, etcd.EtcdResult]) -> Cluster:
        # get initialize flag
        initialize = nodes.get(self._INITIALIZE)
        initialize = initialize and initialize.value

        # get global dynamic configuration
        config = nodes.get(self._CONFIG)
        config = config and ClusterConfig.from_node(config.modifiedIndex, config.value)

        # get timeline history
        history = nodes.get(self._HISTORY)
        history = history and TimelineHistory.from_node(history.modifiedIndex, history.value)

        # get last know leader lsn and slots
        status = nodes.get(self._STATUS) or nodes.get(self._LEADER_OPTIME)
        status = Status.from_node(status and status.value)

        # get list of members
        members = [self.member(n) for k, n in nodes.items() if k.startswith(self._MEMBERS) and k.count('/') == 1]

        # get leader
        leader = nodes.get(self._LEADER)
        if leader:
            member = Member(-1, leader.value, None, {})
            member = ([m for m in members if m.name == leader.value] or [member])[0]
            version = etcd_index if etcd_index > leader.modifiedIndex else leader.modifiedIndex + 1
            leader = Leader(version, leader.ttl, member)

        # failover key
        failover = nodes.get(self._FAILOVER)
        if failover:
            failover = Failover.from_node(failover.modifiedIndex, failover.value)

        # get synchronization state
        sync = nodes.get(self._SYNC)
        sync = SyncState.from_node(sync and sync.modifiedIndex, sync and sync.value)

        # get failsafe topology
        failsafe = nodes.get(self._FAILSAFE)
        try:
            failsafe = json.loads(failsafe.value) if failsafe else None
        except Exception:
            failsafe = None

        return Cluster(initialize, config, leader, status, members, failover, sync, history, failsafe)

    def _postgresql_cluster_loader(self, path: str) -> Cluster:
        """Load and build the :class:`Cluster` object from DCS, which represents a single PostgreSQL cluster.

        :param path: the path in DCS where to load :class:`Cluster` from.

        :returns: :class:`Cluster` instance.
        """
        try:
            result = self.retry(self._client.read, path, recursive=True, quorum=self._ctl)
        except etcd.EtcdKeyNotFound:
            return Cluster.empty()
        nodes = {node.key[len(result.key):].lstrip('/'): node for node in result.leaves}
        return self._cluster_from_nodes(result.etcd_index, nodes)

    def _mpp_cluster_loader(self, path: str) -> Dict[int, Cluster]:
        """Load and build all PostgreSQL clusters from a single MPP cluster.

        :param path: the path in DCS where to load Cluster(s) from.

        :returns: all MPP groups as :class:`dict`, with group IDs as keys and :class:`Cluster` objects as values.
        """
        try:
            result = self.retry(self._client.read, path, recursive=True, quorum=self._ctl)
        except etcd.EtcdKeyNotFound:
            return {}

        clusters: Dict[int, Dict[str, etcd.EtcdResult]] = defaultdict(dict)
        for node in result.leaves:
            key = node.key[len(result.key):].lstrip('/').split('/', 1)
            if len(key) == 2 and self._mpp.group_re.match(key[0]):
                clusters[int(key[0])][key[1]] = node
        return {group: self._cluster_from_nodes(result.etcd_index, nodes) for group, nodes in clusters.items()}

    def _load_cluster(
            self, path: str, loader: Callable[[str], Union[Cluster, Dict[int, Cluster]]]
    ) -> Union[Cluster, Dict[int, Cluster]]:
        cluster = None
        try:
            cluster = loader(path)
        except Exception as e:
            self._handle_exception(e, 'get_cluster', raise_ex=EtcdError('Etcd is not responding properly'))
        self._has_failed = False
        if TYPE_CHECKING:  # pragma: no cover
            assert cluster is not None
        return cluster

    @catch_etcd_errors
    def touch_member(self, data: Dict[str, Any]) -> bool:
        value = json.dumps(data, separators=(',', ':'))
        return bool(self._client.set(self.member_path, value, self._ttl))

    @catch_etcd_errors
    def take_leader(self) -> bool:
        return self.retry(self._client.write, self.leader_path, self._name, ttl=self._ttl)

    def _do_attempt_to_acquire_leader(self) -> bool:
        try:
            return bool(self.retry(self._client.write, self.leader_path, self._name, ttl=self._ttl, prevExist=False))
        except etcd.EtcdAlreadyExist:
            logger.info('Could not take out TTL lock')
            return False

    @catch_return_false_exception
    def attempt_to_acquire_leader(self) -> bool:
        return self._run_and_handle_exceptions(self._do_attempt_to_acquire_leader, retry=None)

    @catch_etcd_errors
    def set_failover_value(self, value: str, version: Optional[int] = None) -> bool:
        return bool(self._client.write(self.failover_path, value, prevIndex=version or 0))

    @catch_etcd_errors
    def set_config_value(self, value: str, version: Optional[int] = None) -> bool:
        return bool(self._client.write(self.config_path, value, prevIndex=version or 0))

    @catch_etcd_errors
    def _write_leader_optime(self, last_lsn: str) -> bool:
        return bool(self._client.set(self.leader_optime_path, last_lsn))

    @catch_etcd_errors
    def _write_status(self, value: str) -> bool:
        return bool(self._client.set(self.status_path, value))

    def _do_update_leader(self) -> bool:
        try:
            return self.retry(self._client.write, self.leader_path, self._name,
                              prevValue=self._name, ttl=self._ttl) is not None
        except etcd.EtcdKeyNotFound:
            return self._do_attempt_to_acquire_leader()

    @catch_etcd_errors
    def _write_failsafe(self, value: str) -> bool:
        return bool(self._client.set(self.failsafe_path, value))

    @catch_return_false_exception
    def _update_leader(self, leader: Leader) -> bool:
        return bool(self._run_and_handle_exceptions(self._do_update_leader, retry=None))

    @catch_etcd_errors
    def initialize(self, create_new: bool = True, sysid: str = "") -> bool:
        return bool(self.retry(self._client.write, self.initialize_path, sysid, prevExist=(not create_new)))

    @catch_etcd_errors
    def _delete_leader(self, leader: Leader) -> bool:
        return bool(self._client.delete(self.leader_path, prevValue=self._name))

    @catch_etcd_errors
    def cancel_initialization(self) -> bool:
        return bool(self.retry(self._client.delete, self.initialize_path))

    @catch_etcd_errors
    def delete_cluster(self) -> bool:
        return bool(self.retry(self._client.delete, self.client_path(''), recursive=True))

    @catch_etcd_errors
    def set_history_value(self, value: str) -> bool:
        return bool(self._client.write(self.history_path, value))

    @catch_etcd_errors
    def set_sync_state_value(self, value: str, version: Optional[int] = None) -> Union[int, bool]:
        return self.retry(self._client.write, self.sync_path, value, prevIndex=version or 0).modifiedIndex

    @catch_etcd_errors
    def delete_sync_state(self, version: Optional[int] = None) -> bool:
        return bool(self.retry(self._client.delete, self.sync_path, prevIndex=version or 0))

    def watch(self, leader_version: Optional[int], timeout: float) -> bool:
        if self.__do_not_watch:
            self.__do_not_watch = False
            return True

        if leader_version:
            end_time = time.time() + timeout

            while timeout >= 1:  # when timeout is too small urllib3 doesn't have enough time to connect
                try:
                    result = self._client.watch(self.leader_path, index=leader_version, timeout=timeout + 0.5)
                    self._has_failed = False
                    if result.action == 'compareAndSwap':
                        time.sleep(0.01)
                    # Synchronous work of all cluster members with etcd is less expensive
                    # than reestablishing http connection every time from every replica.
                    return True
                except etcd.EtcdWatchTimedOut:
                    self._has_failed = False
                    return False
                except (etcd.EtcdEventIndexCleared, etcd.EtcdWatcherCleared):  # Watch failed
                    self._has_failed = False
                    return True  # leave the loop, because watch with the same parameters will fail anyway
                except etcd.EtcdException as e:
                    self._handle_exception(e, 'watch', True)

                timeout = end_time - time.time()

        try:
            return super(Etcd, self).watch(None, timeout)
        finally:
            self.event.clear()


etcd.EtcdError.error_exceptions[300] = EtcdRaftInternal
