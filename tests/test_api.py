import datetime
import json
import psycopg2
import unittest

from mock import Mock, patch
from patroni.api import RestApiHandler, RestApiServer
from patroni.dcs import ClusterConfig, Member
from patroni.utils import tzutc
from six import BytesIO as IO
from six.moves import BaseHTTPServer
from test_postgresql import psycopg2_connect, MockCursor


future_restart_time = datetime.datetime.now(tzutc) + datetime.timedelta(days=5)
postmaster_start_time = datetime.datetime.now(tzutc)


class MockPostgresql(object):

    name = 'test'
    state = 'running'
    role = 'master'
    server_version = '999999'
    sysid = 'dummysysid'
    scope = 'dummy'
    pending_restart = True

    @staticmethod
    def connection():
        return psycopg2_connect()

    @staticmethod
    def postmaster_start_time():
        return str(postmaster_start_time)


class MockHa(object):

    state_handler = MockPostgresql()

    @staticmethod
    def reinitialize():
        return 'reinitialize'

    @staticmethod
    def restart():
        return (True, '')

    @staticmethod
    def restart_scheduled():
        return False

    @staticmethod
    def delete_future_restart():
        return True

    @staticmethod
    def fetch_nodes_statuses(members):
        return [[None, True, None, None, {}]]

    @staticmethod
    def schedule_future_restart(data):
        return True

    @staticmethod
    def is_lagging(xlog):
        return False

    @staticmethod
    def get_effective_tags():
        return {'nosync': True}

    @staticmethod
    def wakeup():
        pass


class MockPatroni(object):

    ha = MockHa()
    config = Mock()
    postgresql = ha.state_handler
    dcs = Mock()
    tags = {}
    version = '0.00'
    noloadbalance = Mock(return_value=False)
    scheduled_restart = {'schedule': future_restart_time,
                         'postmaster_start_time': postgresql.postmaster_start_time()}

    @staticmethod
    def sighup_handler():
        pass


class MockRequest(object):

    def __init__(self, request):
        self.request = request.encode('utf-8')

    def makefile(self, *args, **kwargs):
        return IO(self.request)

    def sendall(self, *args, **kwargs):
        pass


class MockRestApiServer(RestApiServer):

    def __init__(self, Handler, request):
        self.socket = 0
        self.serve_forever = Mock()
        BaseHTTPServer.HTTPServer.__init__ = Mock()
        MockRestApiServer._BaseServer__is_shut_down = Mock()
        MockRestApiServer._BaseServer__shutdown_request = True
        config = {'listen': '127.0.0.1:8008', 'auth': 'test:test'}
        super(MockRestApiServer, self).__init__(MockPatroni(), config)
        config['certfile'] = 'dumb'
        self.reload_config(config)
        Handler(MockRequest(request), ('0.0.0.0', 8080), self)


@patch('ssl.wrap_socket', Mock(return_value=0))
class TestRestApiHandler(unittest.TestCase):

    _authorization = '\nAuthorization: Basic dGVzdDp0ZXN0'

    def test_do_GET(self):
        MockRestApiServer(RestApiHandler, 'GET /replica')
        with patch.object(RestApiHandler, 'get_postgresql_status', Mock(return_value={})):
            MockRestApiServer(RestApiHandler, 'GET /replica')
        with patch.object(RestApiHandler, 'get_postgresql_status', Mock(return_value={'role': 'master'})):
            MockRestApiServer(RestApiHandler, 'GET /replica')
        MockRestApiServer(RestApiHandler, 'GET /master')
        MockPatroni.dcs.cluster.leader.name = MockPostgresql.name
        MockRestApiServer(RestApiHandler, 'GET /replica')
        MockPatroni.dcs.cluster = None
        with patch.object(RestApiHandler, 'get_postgresql_status', Mock(return_value={'role': 'master'})):
            MockRestApiServer(RestApiHandler, 'GET /master')
        with patch.object(MockHa, 'restart_scheduled', Mock(return_value=True)):
            MockRestApiServer(RestApiHandler, 'GET /master')
        self.assertIsNotNone(MockRestApiServer(RestApiHandler, 'GET /master'))

    def test_do_OPTIONS(self):
        self.assertIsNotNone(MockRestApiServer(RestApiHandler, 'OPTIONS / HTTP/1.0'))

    def test_do_GET_patroni(self):
        self.assertIsNotNone(MockRestApiServer(RestApiHandler, 'GET /patroni'))

    def test_basicauth(self):
        self.assertIsNotNone(MockRestApiServer(RestApiHandler, 'POST /restart HTTP/1.0'))
        MockRestApiServer(RestApiHandler, 'POST /restart HTTP/1.0\nAuthorization:')

    @patch.object(MockPatroni, 'dcs')
    def test_do_GET_config(self, mock_dcs):
        mock_dcs.cluster.config.data = {}
        self.assertIsNotNone(MockRestApiServer(RestApiHandler, 'GET /config'))
        mock_dcs.cluster.config = None
        self.assertIsNotNone(MockRestApiServer(RestApiHandler, 'GET /config'))

    @patch.object(MockPatroni, 'dcs')
    def test_do_PATCH_config(self, mock_dcs):
        config = {'postgresql': {'use_slots': False, 'use_pg_rewind': True, 'parameters': {'wal_level': 'logical'}}}
        mock_dcs.get_cluster.return_value.config = ClusterConfig.from_node(1, json.dumps(config))
        request = 'PATCH /config HTTP/1.0' + self._authorization
        self.assertIsNotNone(MockRestApiServer(RestApiHandler, request))
        request += '\nContent-Length: '
        self.assertIsNotNone(MockRestApiServer(RestApiHandler, request + '34\n\n{"postgresql":{"use_slots":false}}'))
        config['ttl'] = 5
        config['postgresql'].update({'use_slots': {'foo': True}, "parameters": None})
        config = json.dumps(config)
        request += str(len(config)) + '\n\n' + config
        MockRestApiServer(RestApiHandler, request)
        mock_dcs.set_config_value.return_value = False
        MockRestApiServer(RestApiHandler, request)

    @patch.object(MockPatroni, 'dcs')
    def test_do_PUT_config(self, mock_dcs):
        mock_dcs.get_cluster.return_value.config = ClusterConfig.from_node(1, '{}')
        request = 'PUT /config HTTP/1.0' + self._authorization + '\nContent-Length: '
        self.assertIsNotNone(MockRestApiServer(RestApiHandler, request + '2\n\n{}'))
        config = '{"foo": "bar"}'
        request += str(len(config)) + '\n\n' + config
        MockRestApiServer(RestApiHandler, request)
        mock_dcs.set_config_value.return_value = False
        MockRestApiServer(RestApiHandler, request)
        mock_dcs.get_cluster.return_value.config = ClusterConfig.from_node(1, config)
        MockRestApiServer(RestApiHandler, request)

    @patch.object(MockPatroni, 'sighup_handler', Mock(side_effect=Exception))
    def test_do_POST_reload(self):
        with patch.object(MockPatroni, 'config') as mock_config:
            mock_config.reload_local_configuration.return_value = False
            MockRestApiServer(RestApiHandler, 'POST /reload HTTP/1.0' + self._authorization)
        self.assertIsNotNone(MockRestApiServer(RestApiHandler, 'POST /reload HTTP/1.0' + self._authorization))

    @patch.object(MockPatroni, 'dcs')
    def test_do_POST_restart(self, mock_dcs):
        mock_dcs.get_cluster.return_value.is_paused.return_value = False
        request = 'POST /restart HTTP/1.0' + self._authorization
        self.assertIsNotNone(MockRestApiServer(RestApiHandler, request))

        with patch.object(MockHa, 'restart', Mock(side_effect=Exception)):
            MockRestApiServer(RestApiHandler, request)

        post = request + '\nContent-Length: '

        def make_request(request=None, **kwargs):
            request = json.dumps(kwargs) if request is None else request
            return '{0}{1}\n\n{2}'.format(post, len(request), request)

        # empty request
        request = make_request('')
        MockRestApiServer(RestApiHandler, request)
        # invalid request
        request = make_request('foobar=baz')
        MockRestApiServer(RestApiHandler, request)
        # wrong role
        request = make_request(schedule=future_restart_time.isoformat(), role='unknown', postgres_version='9.5.3')
        MockRestApiServer(RestApiHandler, request)
        # wrong version
        request = make_request(schedule=future_restart_time.isoformat(), role='master', postgres_version='9.5.3.1')
        MockRestApiServer(RestApiHandler, request)
        # unknown filter
        request = make_request(schedule=future_restart_time.isoformat(), batman='lives')
        MockRestApiServer(RestApiHandler, request)
        # incorrect schedule
        request = make_request(schedule='2016-08-42 12:45TZ+1', role='master')
        MockRestApiServer(RestApiHandler, request)
        # everything fine, but the schedule is missing
        request = make_request(role='master', postgres_version='9.5.2')
        MockRestApiServer(RestApiHandler, request)
        for retval in (True, False):
            with patch.object(MockHa, 'schedule_future_restart', Mock(return_value=retval)):
                request = make_request(schedule=future_restart_time.isoformat())
                MockRestApiServer(RestApiHandler, request)
            with patch.object(MockHa, 'restart', Mock(return_value=(retval, "foo"))):
                request = make_request(role='master', postgres_version='9.5.2')
                MockRestApiServer(RestApiHandler, request)

        mock_dcs.get_cluster.return_value.is_paused.return_value = True
        MockRestApiServer(RestApiHandler, make_request(schedule='2016-08-42 12:45TZ+1', role='master'))
        # Valid timeout
        MockRestApiServer(RestApiHandler, make_request(timeout='60s'))
        # Invalid timeout
        MockRestApiServer(RestApiHandler, make_request(timeout='42towels'))

    def test_do_DELETE_restart(self):
        for retval in (True, False):
            with patch.object(MockHa, 'delete_future_restart', Mock(return_value=retval)):
                request = 'DELETE /restart HTTP/1.0' + self._authorization
                self.assertIsNotNone(MockRestApiServer(RestApiHandler, request))

    @patch.object(MockPatroni, 'dcs')
    def test_do_POST_reinitialize(self, mock_dcs):
        cluster = mock_dcs.get_cluster.return_value
        cluster.is_paused.return_value = False
        request = 'POST /reinitialize HTTP/1.0' + self._authorization
        MockRestApiServer(RestApiHandler, request)
        with patch.object(MockHa, 'reinitialize', Mock(return_value=None)):
            MockRestApiServer(RestApiHandler, request)

    @patch('time.sleep', Mock())
    def test_RestApiServer_query(self):
        with patch.object(MockCursor, 'execute', Mock(side_effect=psycopg2.OperationalError)):
            self.assertIsNotNone(MockRestApiServer(RestApiHandler, 'GET /patroni'))
        with patch.object(MockPostgresql, 'connection', Mock(side_effect=psycopg2.OperationalError)):
            self.assertIsNotNone(MockRestApiServer(RestApiHandler, 'GET /patroni'))

    @patch('time.sleep', Mock())
    @patch.object(MockPatroni, 'dcs')
    def test_do_POST_failover(self, dcs):
        dcs.loop_wait = 10
        cluster = dcs.get_cluster.return_value

        post = 'POST /failover HTTP/1.0' + self._authorization + '\nContent-Length: '

        MockRestApiServer(RestApiHandler, post + '7\n\n{"1":2}')

        request = post + '0\n\n'
        MockRestApiServer(RestApiHandler, request)

        cluster.leader.name = 'postgresql1'
        MockRestApiServer(RestApiHandler, request)

        MockRestApiServer(RestApiHandler, post + '25\n\n{"leader": "postgresql1"}')

        cluster.leader.name = 'postgresql2'
        request = post + '53\n\n{"leader": "postgresql1", "candidate": "postgresql2"}'
        MockRestApiServer(RestApiHandler, request)

        cluster.leader.name = 'postgresql1'
        MockRestApiServer(RestApiHandler, request)

        cluster.members = [Member(0, 'postgresql0', 30, {'api_url': 'http'}),
                           Member(0, 'postgresql2', 30, {'api_url': 'http'})]
        MockRestApiServer(RestApiHandler, request)

        cluster.failover = None
        MockRestApiServer(RestApiHandler, request)

        dcs.get_cluster.side_effect = [cluster]
        MockRestApiServer(RestApiHandler, request)

        cluster2 = cluster.copy()
        cluster2.leader.name = 'postgresql0'
        dcs.get_cluster.side_effect = [cluster, cluster2]
        MockRestApiServer(RestApiHandler, request)

        cluster2.leader.name = 'postgresql2'
        dcs.get_cluster.side_effect = [cluster, cluster2]
        MockRestApiServer(RestApiHandler, request)

        dcs.get_cluster.side_effect = None
        dcs.manual_failover.return_value = False
        MockRestApiServer(RestApiHandler, request)
        dcs.manual_failover.return_value = True

        with patch.object(MockHa, 'fetch_nodes_statuses', Mock(return_value=[])):
            MockRestApiServer(RestApiHandler, request)

        # Valid future date
        request = post + '103\n\n{"leader": "postgresql1", "member": "postgresql2",' +\
                         ' "scheduled_at": "6016-02-15T18:13:30.568224+01:00"}'
        MockRestApiServer(RestApiHandler, request)
        with patch.object(MockPatroni, 'dcs') as d:
            d.manual_failover.return_value = False
            MockRestApiServer(RestApiHandler, request)

        # Exception: No timezone specified
        request = post + '97\n\n{"leader": "postgresql1", "member": "postgresql2",' +\
                         ' "scheduled_at": "6016-02-15T18:13:30.568224"}'
        MockRestApiServer(RestApiHandler, request)

        # Exception: Scheduled in the past
        request = post + '103\n\n{"leader": "postgresql1", "member": "postgresql2", "scheduled_at": "'
        MockRestApiServer(RestApiHandler, request + '1016-02-15T18:13:30.568224+01:00"}')

        # Invalid date
        self.assertIsNotNone(MockRestApiServer(RestApiHandler, request + '2010-02-29T18:13:30.568224+01:00"}'))
