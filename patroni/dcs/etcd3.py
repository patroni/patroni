from __future__ import absolute_import
import base64
import etcd
import json
import logging
import os
import six
import socket
import sys
import time
import urllib3

from threading import Condition, Lock, Thread

from . import ClusterConfig, Cluster, Failover, Leader, Member, SyncState, TimelineHistory
from .etcd import AbstractEtcdClientWithFailover, AbstractEtcd, catch_etcd_errors
from ..exceptions import DCSError, PatroniException
from ..utils import deep_compare, enable_keepalive, iter_response_objects, RetryFailedError, USER_AGENT

logger = logging.getLogger(__name__)


class Etcd3Error(DCSError):
    pass


class UnsupportedEtcdVersion(PatroniException):
    pass


# google.golang.org/grpc/codes
GRPCCode = type('Enum', (), {'OK': 0, 'Canceled': 1, 'Unknown': 2, 'InvalidArgument': 3, 'DeadlineExceeded': 4,
                             'NotFound': 5, 'AlreadyExists': 6, 'PermissionDenied': 7, 'ResourceExhausted': 8,
                             'FailedPrecondition': 9, 'Aborted': 10, 'OutOfRange': 11, 'Unimplemented': 12,
                             'Internal': 13, 'Unavailable': 14, 'DataLoss': 15, 'Unauthenticated': 16})
GRPCcodeToText = {v: k for k, v in GRPCCode.__dict__.items() if not k.startswith('__') and isinstance(v, int)}


class Etcd3Exception(etcd.EtcdException):
    pass


class Etcd3ClientError(Etcd3Exception):

    def __init__(self, code=None, error=None, status=None):
        if not hasattr(self, 'error'):
            self.error = error and error.strip()
        self.codeText = GRPCcodeToText.get(code)
        self.status = status

    def __repr__(self):
        return "<{0} error: '{1}', code: {2}>".format(self.__class__.__name__, self.error, self.code)

    __str__ = __repr__

    def as_dict(self):
        return {'error': self.error, 'code': self.code, 'codeText': self.codeText, 'status': self.status}

    @classmethod
    def get_subclasses(cls):
        for subclass in cls.__subclasses__():
            for subsubclass in subclass.get_subclasses():
                yield subsubclass
            yield subclass


class Unknown(Etcd3ClientError):
    code = GRPCCode.Unknown


class InvalidArgument(Etcd3ClientError):
    code = GRPCCode.InvalidArgument


class DeadlineExceeded(Etcd3ClientError):
    code = GRPCCode.DeadlineExceeded
    error = "context deadline exceeded"


class NotFound(Etcd3ClientError):
    code = GRPCCode.NotFound


class FailedPrecondition(Etcd3ClientError):
    code = GRPCCode.FailedPrecondition


class Unavailable(Etcd3ClientError):
    code = GRPCCode.Unavailable


# https://github.com/etcd-io/etcd/blob/master/etcdserver/api/v3rpc/rpctypes/error.go
class LeaseNotFound(NotFound):
    error = "etcdserver: requested lease not found"


class UserEmpty(InvalidArgument):
    error = "etcdserver: user name is empty"


class PermissionDenied(Etcd3ClientError):
    code = GRPCCode.PermissionDenied
    error = "etcdserver: permission denied"


class AuthNotEnabled(FailedPrecondition):
    error = "etcdserver: authentication is not enabled"


class InvalidAuthToken(Etcd3ClientError):
    code = GRPCCode.Unauthenticated
    error = "etcdserver: invalid auth token"


errStringToClientError = {s.error: s for s in Etcd3ClientError.get_subclasses() if hasattr(s, 'error')}
errCodeToClientError = {s.code: s for s in Etcd3ClientError.__subclasses__()}


def _raise_for_data(data, status_code=None):
    try:
        error = data.get('error') or data.get('Error')
        if isinstance(error, dict):  # streaming response
            status_code = error.get('http_code')
            code = error['grpc_code']
            error = error['message']
        else:
            code = data.get('code') or data.get('Code')
    except Exception:
        error = str(data)
        code = GRPCCode.Unknown
    err = errStringToClientError.get(error) or errCodeToClientError.get(code) or Unknown
    raise err(code, error, status_code)


def to_bytes(v):
    return v if isinstance(v, bytes) else v.encode('utf-8')


def prefix_range_end(v):
    v = bytearray(to_bytes(v))
    for i in range(len(v) - 1, -1, -1):
        if v[i] < 0xff:
            v[i] += 1
            break
    return bytes(v)


def base64_encode(v):
    return base64.b64encode(to_bytes(v)).decode('utf-8')


def base64_decode(v):
    return base64.b64decode(v).decode('utf-8')


def build_range_request(key, range_end=None):
    fields = {'key': base64_encode(key)}
    if range_end:
        fields['range_end'] = base64_encode(range_end)
    return fields


class Etcd3Client(AbstractEtcdClientWithFailover):

    ERROR_CLS = Etcd3Error

    def __init__(self, config, dns_resolver, cache_ttl=300):
        self._token = None
        self._cluster_version = None
        self.version_prefix = '/v3beta'
        super(Etcd3Client, self).__init__(config, dns_resolver, cache_ttl)

        if six.PY2:  # pragma: no cover
            # Old grpc-gateway sometimes sends double 'transfer-encoding: chunked' headers,
            # what breaks the old (python2.7) httplib.HTTPConnection (it closes the socket).
            def dedup_addheader(httpm, key, value):
                prev = httpm.dict.get(key)
                if prev is None:
                    httpm.dict[key] = value
                elif key != 'transfer-encoding' or prev != value:
                    combined = ", ".join((prev, value))
                    httpm.dict[key] = combined

            import httplib
            httplib.HTTPMessage.addheader = dedup_addheader

        try:
            self.authenticate()
        except Exception as e:
            logger.fatal('Etcd3 authentication failed: %r', e)
            sys.exit(1)

    def _get_headers(self):
        headers = urllib3.make_headers(user_agent=USER_AGENT)
        if self._token and self._cluster_version >= (3, 3, 0):
            headers['authorization'] = self._token
        return headers

    def _prepare_request(self, kwargs, params=None, method=None):
        if params is not None:
            kwargs['body'] = json.dumps(params)
            kwargs['headers']['Content-Type'] = 'application/json'
        return self.http.urlopen

    @staticmethod
    def _handle_server_response(response):
        data = response.data
        try:
            data = data.decode('utf-8')
            data = json.loads(data)
        except (TypeError, ValueError, UnicodeError) as e:
            if response.status < 400:
                raise etcd.EtcdException('Server response was not valid JSON: %r' % e)
        if response.status < 400:
            return data
        _raise_for_data(data, response.status)

    def _ensure_version_prefix(self, base_uri, **kwargs):
        if self.version_prefix != '/v3':
            response = self.http.urlopen(self._MGET, base_uri + '/version', **kwargs)
            response = self._handle_server_response(response)

            server_version_str = response['etcdserver']
            server_version = tuple(int(x) for x in server_version_str.split('.'))
            cluster_version_str = response['etcdcluster']
            self._cluster_version = tuple(int(x) for x in cluster_version_str.split('.'))

            if self._cluster_version < (3, 0) or server_version < (3, 0, 4):
                raise UnsupportedEtcdVersion('Detected Etcd version {0} is lower than 3.0.4'.format(server_version_str))

            if self._cluster_version < (3, 3):
                if self.version_prefix != '/v3alpha':
                    if self._cluster_version < (3, 1):
                        logger.warning('Detected Etcd version %s is lower than 3.1.0, watches are not supported',
                                       cluster_version_str)
                    if self.username and self.password:
                        logger.warning('Detected Etcd version %s is lower than 3.3.0, authentication is not supported',
                                       cluster_version_str)
                    self.version_prefix = '/v3alpha'
            elif self._cluster_version < (3, 4):
                self.version_prefix = '/v3beta'
            else:
                self.version_prefix = '/v3'

    def _prepare_get_members(self, etcd_nodes):
        kwargs = self._prepare_common_parameters(etcd_nodes)
        self._prepare_request(kwargs, {})
        return kwargs

    def _get_members(self, base_uri, **kwargs):
        self._ensure_version_prefix(base_uri, **kwargs)
        resp = self.http.urlopen(self._MPOST, base_uri + self.version_prefix + '/cluster/member/list', **kwargs)
        members = self._handle_server_response(resp)['members']
        return set(url for member in members for url in member.get('clientURLs', []))

    def call_rpc(self, method, fields, retry=None):
        fields['retry'] = retry
        return self.api_execute(self.version_prefix + method, self._MPOST, fields)

    def authenticate(self):
        if self._cluster_version >= (3, 3) and self.username and self.password:
            logger.info('Trying to authenticate on Etcd...')
            old_token, self._token = self._token, None
            try:
                response = self.call_rpc('/auth/authenticate', {'name': self.username, 'password': self.password})
            except AuthNotEnabled:
                logger.info('Etcd authentication is not enabled')
                self._token = None
            except Exception:
                self._token = old_token
                raise
            else:
                self._token = response.get('token')
            return old_token != self._token

    def _handle_auth_errors(func):
        def wrapper(self, *args, **kwargs):
            def retry(ex):
                if self.username and self.password:
                    self.authenticate()
                    return func(self, *args, **kwargs)
                else:
                    logger.fatal('Username or password not set, authentication is not possible')
                    raise ex

            try:
                return func(self, *args, **kwargs)
            except (UserEmpty, PermissionDenied) as e:  # no token provided
                # PermissionDenied is raised on 3.0 and 3.1
                if self._cluster_version < (3, 3) and (not isinstance(e, PermissionDenied)
                                                       or self._cluster_version < (3, 2)):
                    raise UnsupportedEtcdVersion('Authentication is required by Etcd cluster but not '
                                                 'supported on version lower than 3.3.0. Cluster version: '
                                                 '{0}'.format('.'.join(map(str, self._cluster_version))))
                return retry(e)
            except InvalidAuthToken as e:
                logger.error('Invalid auth token: %s', self._token)
                return retry(e)

        return wrapper

    @_handle_auth_errors
    def range(self, key, range_end=None, retry=None):
        params = build_range_request(key, range_end)
        params['serializable'] = True  # For better performance. We can tolerate stale reads.
        return self.call_rpc('/kv/range', params, retry)

    def prefix(self, key, retry=None):
        return self.range(key, prefix_range_end(key), retry)

    def lease_grant(self, ttl, retry=None):
        return self.call_rpc('/lease/grant', {'TTL': ttl}, retry)['ID']

    def lease_keepalive(self, ID, retry=None):
        return self.call_rpc('/lease/keepalive', {'ID': ID}, retry).get('result', {}).get('TTL')

    def txn(self, compare, success, retry=None):
        return self.call_rpc('/kv/txn', {'compare': [compare], 'success': [success]}, retry).get('succeeded')

    @_handle_auth_errors
    def put(self, key, value, lease=None, create_revision=None, mod_revision=None, retry=None):
        fields = {'key': base64_encode(key), 'value': base64_encode(value)}
        if lease:
            fields['lease'] = lease
        if create_revision is not None:
            compare = {'target': 'CREATE', 'create_revision': create_revision}
        elif mod_revision is not None:
            compare = {'target': 'MOD', 'mod_revision': mod_revision}
        else:
            return self.call_rpc('/kv/put', fields, retry)
        compare['key'] = fields['key']
        return self.txn(compare, {'request_put': fields}, retry)

    @_handle_auth_errors
    def deleterange(self, key, range_end=None, mod_revision=None, retry=None):
        fields = build_range_request(key, range_end)
        if mod_revision is None:
            return self.call_rpc('/kv/deleterange', fields, retry)
        compare = {'target': 'MOD', 'mod_revision': mod_revision, 'key': fields['key']}
        return self.txn(compare, {'request_delete_range': fields}, retry)

    def deleteprefix(self, key, retry=None):
        return self.deleterange(key, prefix_range_end(key), retry=retry)

    def watchrange(self, key, range_end=None, start_revision=None, filters=None):
        """returns: response object"""
        params = build_range_request(key, range_end)
        if start_revision is not None:
            params['start_revision'] = start_revision
        params['filters'] = filters or []
        kwargs = self._prepare_common_parameters(1, self.read_timeout)
        request_executor = self._prepare_request(kwargs, {'create_request': params})
        kwargs.update(timeout=urllib3.Timeout(connect=kwargs['timeout']), retries=0)
        return request_executor(self._MPOST, self._base_uri + self.version_prefix + '/watch', **kwargs)

    def watchprefix(self, key, start_revision=None, filters=None):
        return self.watchrange(key, prefix_range_end(key), start_revision, filters)


class KVCache(Thread):

    def __init__(self, dcs, client):
        Thread.__init__(self)
        self.daemon = True
        self._dcs = dcs
        self._client = client
        self.condition = Condition()
        self._config_key = base64_encode(dcs.config_path)
        self._leader_key = base64_encode(dcs.leader_path)
        self._optime_key = base64_encode(dcs.leader_optime_path)
        self._name = base64_encode(dcs._name)
        self._is_ready = False
        self._response = None
        self._response_lock = Lock()
        self._object_cache = {}
        self._object_cache_lock = Lock()
        self.start()

    def set(self, value, overwrite=False):
        with self._object_cache_lock:
            name = value['key']
            old_value = self._object_cache.get(name)
            ret = not old_value or int(old_value['mod_revision']) < int(value['mod_revision'])
            if ret or overwrite and old_value['mod_revision'] == value['mod_revision']:
                self._object_cache[name] = value
        return ret, old_value

    def delete(self, name, mod_revision):
        with self._object_cache_lock:
            old_value = self._object_cache.get(name)
            ret = old_value and int(old_value['mod_revision']) < int(mod_revision)
            if ret:
                del self._object_cache[name]
        return not old_value or ret, old_value

    def copy(self):
        with self._object_cache_lock:
            return [v.copy() for v in self._object_cache.values()]

    def get(self, name):
        with self._object_cache_lock:
            return self._object_cache.get(name)

    def _process_event(self, event):
        kv = event['kv']
        key = kv['key']
        if event.get('type') == 'DELETE':
            success, old_value = self.delete(key, kv['mod_revision'])
        else:
            success, old_value = self.set(kv, True)

        if success:
            old_value = old_value and old_value.get('value')
            new_value = kv.get('value')

            value_changed = old_value != new_value and \
                (key == self._leader_key or key == self._optime_key and new_value is not None or
                 key == self._config_key and old_value is not None and new_value is not None)

            if value_changed:
                logger.debug('%s changed from %s to %s', key, old_value, new_value)

            # We also want to wake up HA loop on replicas if leader optime was updated
            if value_changed and (key != self._optime_key or self.get(self._leader_key) != self._name):
                self._dcs.event.set()

    def _process_message(self, message):
        logger.debug('Received message: %s', message)
        if 'error' in message:
            _raise_for_data(message)
        for event in message.get('result', {}).get('events', []):
            self._process_event(event)

    @staticmethod
    def _finish_response(response):
        try:
            response.close()
        finally:
            response.release_conn()

    def _do_watch(self, revision):
        with self._response_lock:
            self._response = None
        response = self._client.watchprefix(self._dcs.cluster_prefix, revision)
        with self._response_lock:
            if self._response is None:
                self._response = response

        if not self._response:
            return self._finish_response(response)

        for message in iter_response_objects(response):
            self._process_message(message)

    def _build_cache(self):
        result = self._dcs.retry(self._client.prefix, self._dcs.cluster_prefix)
        with self._object_cache_lock:
            self._object_cache = {node['key']: node for node in result.get('kvs', [])}
        with self.condition:
            self._is_ready = True
            self.condition.notify()

        try:
            self._do_watch(result['header']['revision'])
        except Exception as e:
            logger.error('watchprefix failed: %r', e)
        finally:
            with self.condition:
                self._is_ready = False
            with self._response_lock:
                response, self._response = self._response, None
            if response:
                self._finish_response(response)

    def run(self):
        while True:
            try:
                self._build_cache()
            except Exception as e:
                logger.error('KVCache.run %r', e)
                time.sleep(1)

    def kill_stream(self):
        sock = None
        with self._response_lock:
            if self._response:
                try:
                    sock = self._response.connection.sock
                except Exception:
                    sock = None
            else:
                self._response = False
        if sock:
            try:
                sock.shutdown(socket.SHUT_RDWR)
                sock.close()
            except Exception as e:
                logger.debug('Error on socket.shutdown: %r', e)

    def is_ready(self):
        """Must be called only when holding the lock on `condition`"""
        return self._is_ready


class PatroniEtcd3Client(Etcd3Client):

    def __init__(self, *args, **kwargs):
        self._kv_cache = None
        super(PatroniEtcd3Client, self).__init__(*args, **kwargs)

    def configure(self, etcd3):
        self._etcd3 = etcd3

    def start_watcher(self):
        if self._cluster_version >= (3, 1):
            self._kv_cache = KVCache(self._etcd3, self)

    def _restart_watcher(self):
        if self._kv_cache:
            self._kv_cache.kill_stream()

    def set_base_uri(self, value):
        super(PatroniEtcd3Client, self).set_base_uri(value)
        self._restart_watcher()

    def authenticate(self):
        ret = super(PatroniEtcd3Client, self).authenticate()
        if ret:
            self._restart_watcher()
        return ret

    def _wait_cache(self, timeout):
        stop_time = time.time() + timeout
        while not self._kv_cache.is_ready():
            timeout = stop_time - time.time()
            if timeout <= 0:
                raise RetryFailedError('Exceeded retry deadline')
            self._kv_cache.condition.wait(timeout)

    def get_cluster(self):
        if self._kv_cache:
            with self._kv_cache.condition:
                self._wait_cache(self._etcd3._retry.deadline)
                return self._kv_cache.copy()
        else:
            return self._etcd3.retry(self.prefix, self._etcd3.cluster_prefix).get('kvs', [])

    def call_rpc(self, method, fields, retry=None):
        ret = super(PatroniEtcd3Client, self).call_rpc(method, fields, retry)

        if self._kv_cache:
            value = delete = None
            if method == '/kv/txn' and ret.get('succeeded'):
                on_success = fields['success'][0]
                value = on_success.get('request_put')
                delete = on_success.get('request_delete_range')
            elif method == '/kv/put' and ret:
                value = fields
            elif method == '/kv/deleterange' and ret:
                delete = fields

            if value:
                value['mod_revision'] = ret['header']['revision']
                self._kv_cache.set(value)
            elif delete and 'range_end' not in delete:
                self._kv_cache.delete(delete['key'], ret['header']['revision'])

        return ret


class Etcd3(AbstractEtcd):

    def __init__(self, config):
        super(Etcd3, self).__init__(config, PatroniEtcd3Client, (DeadlineExceeded, Unavailable, FailedPrecondition))
        self.__do_not_watch = False
        self._lease = None
        self._last_lease_refresh = 0

        self._client.configure(self)
        if not self._ctl:
            self._client.start_watcher()
            self.create_lease()

    def set_socket_options(self, sock, socket_options):
        enable_keepalive(sock, self.ttl, int(self.loop_wait + self._retry.deadline))

    def set_ttl(self, ttl):
        self.__do_not_watch = super(Etcd3, self).set_ttl(ttl)
        if self.__do_not_watch:
            self._lease = None

    def _do_refresh_lease(self, retry=None):
        if self._lease and self._last_lease_refresh + self._loop_wait > time.time():
            return False

        if self._lease and not self._client.lease_keepalive(self._lease, retry):
            self._lease = None

        ret = not self._lease
        if ret:
            self._lease = self._client.lease_grant(self._ttl, retry)

        self._last_lease_refresh = time.time()
        return ret

    def refresh_lease(self):
        try:
            return self.retry(self._do_refresh_lease)
        except (Etcd3ClientError, RetryFailedError):
            logger.exception('refresh_lease')
        raise Etcd3Error('Failed ro keepalive/grant lease')

    def create_lease(self):
        while not self._lease:
            try:
                self.refresh_lease()
            except Etcd3Error:
                logger.info('waiting on etcd')
                time.sleep(5)

    @property
    def cluster_prefix(self):
        return self.client_path('')

    @staticmethod
    def member(node):
        return Member.from_node(node['mod_revision'], os.path.basename(node['key']), node['lease'], node['value'])

    def _load_cluster(self):
        cluster = None
        try:
            path_len = len(self.cluster_prefix)

            nodes = {}
            for node in self._client.get_cluster():
                node['key'] = base64_decode(node['key'])
                node['value'] = base64_decode(node.get('value', ''))
                node['lease'] = node.get('lease')
                nodes[node['key'][path_len:].lstrip('/')] = node

            # get initialize flag
            initialize = nodes.get(self._INITIALIZE)
            initialize = initialize and initialize['value']

            # get global dynamic configuration
            config = nodes.get(self._CONFIG)
            config = config and ClusterConfig.from_node(config['mod_revision'], config['value'])

            # get timeline history
            history = nodes.get(self._HISTORY)
            history = history and TimelineHistory.from_node(history['mod_revision'], history['value'])

            # get last leader operation
            last_leader_operation = nodes.get(self._LEADER_OPTIME)
            last_leader_operation = 0 if last_leader_operation is None else int(last_leader_operation['value'])

            # get list of members
            members = [self.member(n) for k, n in nodes.items() if k.startswith(self._MEMBERS) and k.count('/') == 1]

            # get leader
            leader = nodes.get(self._LEADER)
            if not self._ctl and leader and leader['value'] == self._name and self._lease != leader.get('lease'):
                logger.warning('I am the leader but not owner of the lease')

            if leader:
                member = Member(-1, leader['value'], None, {})
                member = ([m for m in members if m.name == leader['value']] or [member])[0]
                leader = Leader(leader['mod_revision'], leader['lease'], member)

            # failover key
            failover = nodes.get(self._FAILOVER)
            if failover:
                failover = Failover.from_node(failover['mod_revision'], failover['value'])

            # get synchronization state
            sync = nodes.get(self._SYNC)
            sync = SyncState.from_node(sync and sync['mod_revision'], sync and sync['value'])

            cluster = Cluster(initialize, config, leader, last_leader_operation, members, failover, sync, history)
        except UnsupportedEtcdVersion:
            raise
        except Exception as e:
            self._handle_exception(e, 'get_cluster', raise_ex=Etcd3Error('Etcd is not responding properly'))
        self._has_failed = False
        return cluster

    @catch_etcd_errors
    def touch_member(self, data, permanent=False):
        if not permanent:
            try:
                self.refresh_lease()
            except Etcd3Error:
                return False

        cluster = self.cluster
        member = cluster and cluster.get_member(self._name, fallback_to_leader=False)

        if member and member.session == self._lease and deep_compare(data, member.data):
            return True

        data = json.dumps(data, separators=(',', ':'))
        try:
            return self._client.put(self.member_path, data, None if permanent else self._lease)
        except LeaseNotFound:
            self._lease = None
            logger.error('Our lease disappeared from Etcd, can not "touch_member"')

    @catch_etcd_errors
    def take_leader(self):
        return self.retry(self._client.put, self.leader_path, self._name, self._lease)

    @catch_etcd_errors
    def _do_attempt_to_acquire_leader(self, permanent):
        try:
            return self.retry(self._client.put, self.leader_path, self._name, None if permanent else self._lease, 0)
        except LeaseNotFound:
            self._lease = None
            logger.error('Our lease disappeared from Etcd. Will try to get a new one and retry attempt')
            self.refresh_lease()
            return self.retry(self._client.put, self.leader_path, self._name, None if permanent else self._lease, 0)

    def attempt_to_acquire_leader(self, permanent=False):
        if not self._lease and not permanent:
            self.refresh_lease()

        ret = self._do_attempt_to_acquire_leader(permanent)
        if not ret:
            logger.info('Could not take out TTL lock')
        return ret

    @catch_etcd_errors
    def set_failover_value(self, value, index=None):
        return self._client.put(self.failover_path, value, mod_revision=index)

    @catch_etcd_errors
    def set_config_value(self, value, index=None):
        return self._client.put(self.config_path, value, mod_revision=index)

    @catch_etcd_errors
    def _write_leader_optime(self, last_operation):
        return self._client.put(self.leader_optime_path, last_operation)

    @catch_etcd_errors
    def _update_leader(self):
        if not self._lease:
            self.refresh_lease()
        elif self.retry(self._client.lease_keepalive, self._lease):
            self._last_lease_refresh = time.time()

        if self._lease:
            cluster = self.cluster
            leader_lease = cluster and isinstance(cluster.leader, Leader) and cluster.leader.session
            if leader_lease != self._lease:
                self.take_leader()
        return bool(self._lease)

    @catch_etcd_errors
    def initialize(self, create_new=True, sysid=""):
        return self.retry(self._client.put, self.initialize_path, sysid, None, 0 if create_new else None)

    @catch_etcd_errors
    def _delete_leader(self):
        cluster = self.cluster
        if cluster and isinstance(cluster.leader, Leader) and cluster.leader.name == self._name:
            return self._client.deleterange(self.leader_path, mod_revision=cluster.leader.index)

    @catch_etcd_errors
    def cancel_initialization(self):
        return self.retry(self._client.deleterange, self.initialize_path)

    @catch_etcd_errors
    def delete_cluster(self):
        return self.retry(self._client.deleteprefix, self.cluster_prefix)

    @catch_etcd_errors
    def set_history_value(self, value):
        return self._client.put(self.history_path, value)

    @catch_etcd_errors
    def set_sync_state_value(self, value, index=None):
        return self.retry(self._client.put, self.sync_path, value, mod_revision=index)

    @catch_etcd_errors
    def delete_sync_state(self, index=None):
        return self.retry(self._client.deleterange, self.sync_path, mod_revision=index)

    def watch(self, leader_index, timeout):
        if self.__do_not_watch:
            self.__do_not_watch = False
            return True

        try:
            return super(Etcd3, self).watch(None, timeout)
        finally:
            self.event.clear()
