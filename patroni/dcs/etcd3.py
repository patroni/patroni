from __future__ import absolute_import
import base64
import etcd
import json
import logging
import os
import six
import socket
import time
import urllib3

from json.decoder import WHITESPACE
from patroni.dcs import ClusterConfig, Cluster, Failover, Leader, Member, SyncState, TimelineHistory
from patroni.dcs.etcd import AbstractEtcdClientWithFailover, AbstractEtcd, catch_etcd_errors
from patroni.exceptions import DCSError, PatroniException
from patroni.utils import deep_compare, Retry, RetryFailedError
from threading import Lock, Thread

logger = logging.getLogger(__name__)


class EtcdError(DCSError):
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


def Error(name, parent=None, error=None):
    class ClientError(parent if isinstance(parent, six.class_types) else Etcd3ClientError):
        pass

    ClientError.__name__ = name
    if not isinstance(parent, six.class_types):
        ClientError.code = parent if isinstance(parent, int) else GRPCCode.__dict__[name]
    if error is not None:
        ClientError.error = error

    return ClientError


Unknown = Error('Unknown')
InvalidArgument = Error('InvalidArgument')
DeadlineExceeded = Error('DeadlineExceeded', error='context deadline exceeded')
NotFound = Error('NotFound')
ResourceExhausted = Error('ResourceExhausted')
FailedPrecondition = Error('FailedPrecondition')
OutOfRange = Error('OutOfRange')
Unavailable = Error('Unavailable')

# https://github.com/etcd-io/etcd/blob/master/etcdserver/api/v3rpc/rpctypes/error.go
EmptyKey = Error('EmptyKey', InvalidArgument, 'etcdserver: key is not provided')
KeyNotFound = Error('KeyNotFound', InvalidArgument, 'etcdserver: key not found')
ValueProvided = Error('ValueProvided', InvalidArgument, 'etcdserver: value is provided')
LeaseProvided = Error('LeaseProvided', InvalidArgument, 'etcdserver: lease is provided')
TooManyOps = Error('TooManyOps', InvalidArgument, 'etcdserver: too many operations in txn request')
DuplicateKey = Error('DuplicateKey', InvalidArgument, 'etcdserver: duplicate key given in txn request')
Compacted = Error('Compacted', OutOfRange, 'etcdserver: mvcc: required revision has been compacted')
FutureRev = Error('FutureRev', OutOfRange, 'etcdserver: mvcc: required revision is a future revision')
NoSpace = Error('NoSpace', ResourceExhausted, 'etcdserver: mvcc: database space exceeded')
LeaseNotFound = Error('LeaseNotFound', NotFound, 'etcdserver: requested lease not found')
LeaseExist = Error('LeaseExist', FailedPrecondition, 'etcdserver: lease already exists')
LeaseTTLTooLarge = Error('LeaseTTLTooLarge', OutOfRange, 'etcdserver: too large lease TTL')
MemberExist = Error('MemberExist', FailedPrecondition, 'etcdserver: member ID already exist')
PeerURLExist = Error('PeerURLExist', FailedPrecondition, 'etcdserver: Peer URLs already exists')
MemberNotEnoughStarted = Error('MemberNotEnoughStarted', FailedPrecondition,
                               'etcdserver: re-configuration failed due to not enough started members')
MemberBadURLs = Error('MemberBadURLs', InvalidArgument, 'etcdserver: given member URLs are invalid')
MemberNotFound = Error('MemberNotFound', NotFound, 'etcdserver: member not found')
MemberNotLearner = Error('MemberNotLearner', FailedPrecondition, 'etcdserver: can only promote a learner member')
MemberLearnerNotReady = Error('MemberLearnerNotReady', FailedPrecondition,
                              'etcdserver: can only promote a learner member which is in sync with leader')
TooManyLearners = Error('TooManyLearners', FailedPrecondition, 'etcdserver: too many learner members in cluster')
RequestTooLarge = Error('RequestTooLarge', InvalidArgument, 'etcdserver: request is too large')
TooManyRequests = Error('TooManyRequests', ResourceExhausted, 'etcdserver: too many requests')
RootUserNotExist = Error('RootUserNotExist', FailedPrecondition, 'etcdserver: root user does not exist')
RootRoleNotExist = Error('RootRoleNotExist', FailedPrecondition, 'etcdserver: root user does not have root role')
UserAlreadyExist = Error('UserAlreadyExist', FailedPrecondition, 'etcdserver: user name already exists')
UserEmpty = Error('UserEmpty', InvalidArgument, 'etcdserver: user name is empty')
UserNotFound = Error('UserNotFound', FailedPrecondition, 'etcdserver: user name not found')
RoleAlreadyExist = Error('RoleAlreadyExist', FailedPrecondition, 'etcdserver: role name already exists')
RoleNotFound = Error('RoleNotFound', FailedPrecondition, 'etcdserver: role name not found')
RoleEmpty = Error('RoleEmpty', InvalidArgument, 'etcdserver: role name is empty')
AuthFailed = Error('AuthFailed', InvalidArgument, 'etcdserver: authentication failed, invalid user ID or password')
PermissionDenied = Error('PermissionDenied', GRPCCode.PermissionDenied, 'etcdserver: permission denied')
RoleNotGranted = Error('RoleNotGranted', FailedPrecondition, 'etcdserver: role is not granted to the user')
PermissionNotGranted = Error('PermissionNotGranted', FailedPrecondition,
                             'etcdserver: permission is not granted to the role')
AuthNotEnabled = Error('AuthNotEnabled', FailedPrecondition, 'etcdserver: authentication is not enabled')
InvalidAuthToken = Error('InvalidAuthToken', GRPCCode.Unauthenticated, 'etcdserver: invalid auth token')
InvalidAuthMgmt = Error('InvalidAuthMgmt', InvalidArgument, 'etcdserver: invalid auth management')
NoLeader = Error('NoLeader', Unavailable, 'etcdserver: no leader')
NotLeader = Error('NotLeader', FailedPrecondition, 'etcdserver: not leader')
LeaderChanged = Error('LeaderChanged', Unavailable, 'etcdserver: leader changed')
NotCapable = Error('NotCapable', Unavailable, 'etcdserver: not capable')
Stopped = Error('Stopped', Unavailable, 'etcdserver: server stopped')
Timeout = Error('Timeout', Unavailable, 'etcdserver: request timed out')
TimeoutDueToLeaderFail = Error('TimeoutDueToLeaderFail', Unavailable,
                               'etcdserver: request timed out, possibly due to previous leader failure')
TimeoutDueToConnectionLost = Error('TimeoutDueToConnectionLost', Unavailable,
                                   'etcdserver: request timed out, possibly due to connection lost')
Unhealthy = Error('Unhealthy', Unavailable, 'etcdserver: unhealthy cluster')
Corrupt = Error('Corrupt', GRPCCode.DataLoss, 'etcdserver: corrupt cluster')
BadLeaderTransferee = Error('BadLeaderTransferee', FailedPrecondition, 'etcdserver: bad leader transferee')

errStringToClientError = {s.error: s for s in Etcd3ClientError.get_subclasses() if hasattr(s, 'error')}
errCodeToClientError = {s.code: s for s in Etcd3ClientError.__subclasses__()}


def _raise_for_status(response):
    if response.status < 400:
        return
    data = response.data.decode('utf-8')
    try:
        data = json.loads(data)
        error = data.get('error') or data.get('Error')
        if isinstance(error, dict):  # streaming response
            code = error['grpc_code']
            error = error['message']
        else:
            code = data.get('code') or data.get('Code')
    except Exception:
        error = data
        code = GRPCCode.Unknown
    err = errStringToClientError.get(error) or errCodeToClientError.get(code) or Unknown
    raise err(code, error, response.status)


def to_bytes(v):
    return v if isinstance(v, bytes) else v.encode('utf-8')


def increment_last_byte(v):
    v = bytearray(to_bytes(v))
    v[-1] += 1
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


class Watcher(Thread):

    def __init__(self, client, event, config_path, leader_path):
        """
        :param client: reference to Etcd3Client
        :param event: reference to Etcd3.event
        :param config_path: the config key name
        :param leader_path: the leader key name
        """
        super(Watcher, self).__init__()
        self.daemon = True
        self._client = client
        self._event = event
        self._config_path = config_path
        self._config_path_base64 = base64_encode(config_path)
        self._config_value = None
        self._leader_path = leader_path
        self._leader_path_base64 = base64_encode(leader_path)
        self._decoder = json.JSONDecoder()
        self._response = None
        self._response_lock = Lock()
        self.start()

    def _process_event(self, event):
        key = event['kv']['key']
        value = event['kv'].get('value', '')
        if key == self._leader_path_base64:
            if event.get('type') == 'DELETE':
                logger.debug('Leader key was deleted')
                self._event.set()
            elif self._leader_value and value != self._leader_value:
                logger.debug('Leader changed from %s to %s', base64_decode(self._leader_value), base64_decode(value))
                self._event.set()
            self._leader_value = value
        elif key == self._config_path_base64:
            if value != self._config_value:
                logger.debug('Config changed to %s', base64_decode(value))
                self._event.set()
            self._config_value = value

    def _process_message(self, message):
        for event in message.get('events', []):
            self._process_event(event)

    def _process_chunk(self, chunk):
        length = len(chunk)
        idx = WHITESPACE.match(chunk, 0).end()
        while idx < length:
            try:
                message, idx = self._decoder.raw_decode(chunk, idx)
            except ValueError:  # malformed or incomplete JSON, unlikely to happen
                break
            else:
                self._process_message(message.get('result', message))
                idx = WHITESPACE.match(chunk, idx).end()
        return chunk[idx:]

    @staticmethod
    def _finish_response(response):
        try:
            response.close()
        finally:
            response.release_conn()

    def _do_watch(self):
        with self._response_lock:
            self._response = None
        response = self._client.watch(self._config_path, increment_last_byte(self._leader_path))
        with self._response_lock:
            if self._response is None:
                self._response = response

        if not self._response:
            return self._finish_response(response)

        left = ''
        for chunk in response.stream():
            chunk = left + chunk.decode('utf-8')
            left = self._process_chunk(chunk)

    def run(self):
        while True:
            try:
                self._leader_value = None
                self._do_watch()
            except Exception as e:
                logger.error('Watcher.run %r', e)
            finally:
                with self._response_lock:
                    response, self._response = self._response, None
                if response:
                    self._finish_response(response)
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


class Etcd3Client(AbstractEtcdClientWithFailover):

    def __init__(self, config, dns_resolver, cache_ttl=300):
        self._token = None
        self._cluster_version = None
        self._watcher = None
        self.version_prefix = '/v3beta'
        super(Etcd3Client, self).__init__(config, dns_resolver, cache_ttl)

        if six.PY2:
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
            import sys
            sys.exit(1)

    def _get_headers(self):
        if self._token and self._cluster_version >= (3, 3, 0):
            return {'authorization': self._token}
        return {}

    def _prepare_request(self, params=None):
        kwargs = self._build_request_parameters()
        if params is None:
            kwargs['body'] = ''
        else:
            kwargs['body'] = json.dumps(params)
            kwargs['headers']['Content-Type'] = 'application/json'
        return self.http.urlopen, kwargs

    def _next_server(self, cause=None):
        ret = super(Etcd3Client, self)._next_server(cause)
        self._restart_watcher()
        return ret

    @staticmethod
    def _handle_server_response(response):
        _raise_for_status(response)
        try:
            return json.loads(response.data.decode('utf-8'))
        except (TypeError, ValueError, UnicodeError) as e:
            raise etcd.EtcdException('Server response was not valid JSON: %r' % e)

    def _ensure_version_prefix(self):
        if self.version_prefix != '/v3':
            request_executor, kwargs = self._prepare_request()
            response = request_executor(self._MGET, self._base_uri + '/version', **kwargs)
            response = self._handle_server_response(response)

            server_version_str = response['etcdserver']
            server_version = tuple(int(x) for x in server_version_str.split('.'))
            cluster_version_str = response['etcdcluster']
            try:
                self._cluster_version = tuple(int(x) for x in cluster_version_str.split('.'))
            except ValueError:
                raise Etcd3Exception

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

    def _refresh_machines_cache(self):
        self._ensure_version_prefix()
        super(Etcd3Client, self)._refresh_machines_cache()

    def _get_members(self):
        request_executor, kwargs = self._prepare_request({})
        resp = request_executor(self._MPOST, self._base_uri + self.version_prefix + '/cluster/member/list', **kwargs)
        members = self._handle_server_response(resp)['members']
        return set(url for member in members for url in member['clientURLs'])

    def call_rpc(self, method, fields=None):
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
                if old_token:
                    self._restart_watcher()
            except Exception:
                self._token = old_token
                raise
            else:
                self._token = response.get('token')
                self._restart_watcher()

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
    def range(self, key, range_end=None):
        params = build_range_request(key, range_end)
        params['serializable'] = True  # For better performance. We can tolerate stale reads.
        return self.call_rpc('/kv/range', params)

    def prefix(self, key):
        return self.range(key, increment_last_byte(key))

    def lease_grant(self, ttl):
        return self.call_rpc('/lease/grant', {'TTL': ttl})['ID']

    def lease_keepalive(self, ID):
        return self.call_rpc('/lease/keepalive', {'ID': ID}).get('result', {}).get('TTL')

    @_handle_auth_errors
    def put(self, key, value, lease=None, create_revision=None, mod_revision=None):
        fields = {'key': base64_encode(key), 'value': base64_encode(value)}
        if lease:
            fields['lease'] = lease
        if create_revision is not None:
            compare = {'target': 'CREATE', 'create_revision': create_revision}
        elif mod_revision is not None:
            compare = {'target': 'MOD', 'mod_revision': mod_revision}
        else:
            return self.call_rpc('/kv/put', fields)
        compare['key'] = fields['key']
        return self.call_rpc('/kv/txn', {'compare': [compare], 'success': [{'request_put': fields}]}).get('succeeded')

    @_handle_auth_errors
    def deleterange(self, key, range_end=None, mod_revision=None):
        fields = build_range_request(key, range_end)
        if mod_revision is None:
            return self.call_rpc('/kv/deleterange', fields)
        compare = {'target': 'MOD', 'mod_revision': mod_revision, 'key': fields['key']}
        ret = self.call_rpc('/kv/txn', {'compare': [compare], 'success': [{'request_delete_range': fields}]})
        return ret.get('succeeded')

    def deleteprefix(self, key):
        return self.deleterange(key, increment_last_byte(key))

    def watch(self, key, range_end=None, filters=None):
        """returns: response object"""
        params = build_range_request(key, range_end)
        params['filters'] = filters or []
        request_executor, kwargs = self._prepare_request({'create_request': params})
        kwargs.update(timeout=urllib3.Timeout(connect=kwargs['timeout']), retries=0)
        return request_executor(self._MPOST, self._base_uri + self.version_prefix + '/watch', **kwargs)

    def start_watcher(self, etcd3):
        if self._cluster_version >= (3, 1):
            self._watcher = Watcher(self, etcd3.event, etcd3.config_path, etcd3.leader_path)

    def _restart_watcher(self):
        if self._watcher:
            self._watcher.kill_stream()


class Etcd3(AbstractEtcd):

    def __init__(self, config):
        super(Etcd3, self).__init__(config, Etcd3Client, Etcd3ClientError)
        self._retry = Retry(deadline=config['retry_timeout'], max_delay=1, max_tries=-1,
                            retry_exceptions=(DeadlineExceeded, Unavailable))
        self.__do_not_watch = False
        self._lease = None
        self._last_lease_refresh = 0
        if not self._ctl:
            self._client.start_watcher(self)
            self.create_lease()

    def set_ttl(self, ttl):
        self.__do_not_watch = super(Etcd3, self).set_ttl(ttl)
        if self.__do_not_watch:
            self._lease = None

    def _do_refresh_lease(self):
        if self._lease and self._last_lease_refresh + self._loop_wait > time.time():
            return False

        if self._lease and not self._client.lease_keepalive(self._lease):
            self._lease = None

        ret = not self._lease
        if ret:
            self._lease = self._client.lease_grant(self._ttl)

        self._last_lease_refresh = time.time()
        return ret

    def refresh_lease(self):
        try:
            return self.retry(self._do_refresh_lease)
        except (Etcd3ClientError, RetryFailedError):
            logger.exception('refresh_lease')
        raise EtcdError('Failed ro keepalive/grant lease')

    def create_lease(self):
        while not self._lease:
            try:
                self.refresh_lease()
            except EtcdError:
                logger.info('waiting on etcd')
                time.sleep(5)

    @staticmethod
    def member(node):
        return Member.from_node(node['mod_revision'], os.path.basename(node['key']), node['lease'], node['value'])

    def _load_cluster(self):
        cluster = None
        try:
            path = self.client_path('')
            result = self.retry(self._client.prefix, path)
            nodes = {}
            for node in result.get('kvs', []):
                node['key'] = base64_decode(node['key'])
                node['value'] = base64_decode(node.get('value', ''))
                node['lease'] = node.get('lease')
                nodes[node['key'][len(path):].lstrip('/')] = node

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
            self._handle_exception(e, 'get_cluster', raise_ex=EtcdError('Etcd is not responding properly'))
        self._has_failed = False
        return cluster

    @catch_etcd_errors
    def touch_member(self, data, permanent=False):
        if not permanent:
            self.refresh_lease()

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
    def delete_leader(self):
        cluster = self.cluster
        if cluster and isinstance(cluster.leader, Leader) and cluster.leader.name == self._name:
            return self._client.deleterange(self.leader_path, mod_revision=cluster.leader.index)

    @catch_etcd_errors
    def cancel_initialization(self):
        return self.retry(self._client.deleterange, self.initialize_path)

    @catch_etcd_errors
    def delete_cluster(self):
        return self.retry(self._client.deleteprefix, self.client_path(''))

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
