#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import pytest
import unittest
import psycopg2
import requests
import patroni.exceptions
import etcd
from mock import patch, Mock

from click.testing import CliRunner
from patroni.ctl import ctl, members, store_config, load_config, output_members, post_patroni, get_dcs, \
    wait_for_leader, get_all_members, get_any_member, get_cursor, query_member, configure
from patroni.ha import Ha
from patroni.etcd import Etcd, Client
from test_ha import get_cluster_initialized_without_leader, get_cluster_initialized_with_leader, \
    get_cluster_initialized_with_only_leader, MockPostgresql, MockPatroni, run_async, \
    get_cluster_not_initialized_without_leader
from test_etcd import etcd_read, etcd_write, requests_get, socket_getaddrinfo, MockResponse
from test_postgresql import MockConnect, psycopg2_connect

CONFIG_FILE_PATH = './test-ctl.yaml'

def test_rw_config():
    runner = CliRunner()
    config = {'a':'b'}
    with runner.isolated_filesystem():
        store_config(config, CONFIG_FILE_PATH + '/dummy')
        os.remove(CONFIG_FILE_PATH + '/dummy')
        os.rmdir(CONFIG_FILE_PATH)

        with pytest.raises(Exception):
            result = load_config(CONFIG_FILE_PATH, None)
            assert 'Could not load configuration file' in result.output

        os.mkdir(CONFIG_FILE_PATH)
        with pytest.raises(Exception):
            store_config(config, CONFIG_FILE_PATH)

        os.rmdir(CONFIG_FILE_PATH)

    store_config(config, CONFIG_FILE_PATH)
    load_config(CONFIG_FILE_PATH, None)
    load_config(CONFIG_FILE_PATH, '0.0.0.0')

@patch('patroni.ctl.load_config', Mock(return_value={'dcs': {'scheme': 'etcd', 'hostname': 'localhost', 'port': 4001}}))
class TestCtl(unittest.TestCase):

    @patch('socket.getaddrinfo', socket_getaddrinfo)
    @patch.object(Client, 'machines')
    def setUp(self, mock_machines):
        mock_machines.__get__ = Mock(return_value=['http://remotehost:2379'])
        self.p = MockPostgresql()
        self.e = Etcd('foo', {'ttl': 30, 'host': 'ok:2379', 'scope': 'test'})
        self.e.client.read = etcd_read
        self.e.client.write = etcd_write
        self.e.client.delete = Mock(side_effect=etcd.EtcdException())
        self.ha = Ha(MockPatroni(self.p, self.e))
        self.ha._async_executor.run_async = run_async
        self.ha.old_cluster = self.e.get_cluster()
        self.ha.cluster = get_cluster_not_initialized_without_leader()
        self.ha.load_cluster_from_dcs = Mock()

    @patch('psycopg2.connect', psycopg2_connect)
    def test_get_cursor(self):
        c = get_cursor(get_cluster_initialized_without_leader(), role='master')
        assert c is None

        c = get_cursor(get_cluster_initialized_with_leader(), role='master')
        assert c is not None

        c = get_cursor(get_cluster_initialized_with_leader(), role='replica')
        # # MockCursor returns pg_is_in_recovery as false
        assert c is None

        c = get_cursor(get_cluster_initialized_with_leader(), role='any')
        assert c is not None

    def test_output_members(self):
        cluster = get_cluster_initialized_with_leader()
        output_members(cluster, name='abc', format='pretty')
        output_members(cluster, name='abc', format='json')
        output_members(cluster, name='abc', format='tsv')


    @patch('patroni.etcd.Etcd.get_cluster', Mock(return_value=get_cluster_initialized_with_leader()))
    @patch('patroni.etcd.Etcd.get_etcd_client', Mock(return_value=None))
    @patch('patroni.etcd.Etcd.set_failover_value', Mock(return_value=None))
    @patch('patroni.ctl.wait_for_leader', Mock(return_value=get_cluster_initialized_with_leader()))
    @patch('requests.get', requests_get)
    @patch('requests.post', requests_get)
    @patch('patroni.ctl.post_patroni', Mock(return_value=MockResponse()))
    def test_failover(self):
        runner = CliRunner()

        with patch('patroni.etcd.Etcd.get_cluster', Mock(return_value=get_cluster_initialized_with_leader())):
            result = runner.invoke(ctl, ['failover', 'dummy', '--dcs', '8.8.8.8'], input='''leader
other

y''')
            assert 'leader' in result.output

            result = runner.invoke(ctl, ['failover', 'dummy', '--dcs', '8.8.8.8'], input='''leader
other
2100-01-01T12:23:00
y''')
            assert result.exit_code == 0

            result = runner.invoke(ctl, ['failover', 'dummy', '--dcs', '8.8.8.8'], input='''leader
other
2030-01-01T12:23:00
y''')
            assert result.exit_code == 0

            ## Aborting failover,as we anser NO to the confirmation
            result = runner.invoke(ctl, ['failover', 'dummy', '--dcs', '8.8.8.8'], input='''leader
other
2030-01-01T12:23:00
y''')
            assert result.exit_code == 0

            ## Aborting failover,as we anser NO to the confirmation
            result = runner.invoke(ctl, ['failover', 'dummy', '--dcs', '8.8.8.8'], input='''leader
other
N''')
            assert result.exit_code == 1

            ## Target and source are equal
            result = runner.invoke(ctl, ['failover', 'dummy', '--dcs', '8.8.8.8'], input='''leader
leader

y''')
            assert result.exit_code == 1

            ## Reality is not part of this cluster
            result = runner.invoke(ctl, ['failover', 'dummy', '--dcs', '8.8.8.8'], input='''leader
Reality

y''')
            assert result.exit_code == 1

            result = runner.invoke(ctl, ['failover', 'dummy', '--force'])
            assert 'Member' in result.output

            result = runner.invoke(ctl, ['failover', 'dummy', '--force', '--scheduled', '2015-01-01T12:00:00+01:00'])
            assert result.exit_code == 0

            ## Invalid timestamp
            result = runner.invoke(ctl, ['failover', 'dummy', '--force', '--scheduled', 'invalid'])
            assert result.exit_code != 0

            ## Invalid timestamp
            result = runner.invoke(ctl, ['failover', 'dummy', '--force', '--scheduled', '2115-02-30T12:00:00+01:00'])
            assert result.exit_code != 0

            ## Specifying wrong leader
            result = runner.invoke(ctl, ['failover', 'dummy', '--dcs', '8.8.8.8'], input='dummy')
            assert result.exit_code == 1

        with patch('patroni.etcd.Etcd.get_cluster', Mock(return_value=get_cluster_initialized_with_only_leader())):
            ## No members available
            result = runner.invoke(ctl, ['failover', 'dummy', '--dcs', '8.8.8.8'], input='''leader
other
y''')
            assert result.exit_code == 1

        with patch('patroni.etcd.Etcd.get_cluster', Mock(return_value=get_cluster_initialized_without_leader())):
            ## No master available
            result = runner.invoke(ctl, ['failover', 'dummy', '--dcs', '8.8.8.8'], input='''leader
other

y''')
            assert result.exit_code == 1

        with patch('patroni.ctl.post_patroni', Mock(side_effect=Exception())):
            ## Non-responding patroni
            result = runner.invoke(ctl, ['failover', 'dummy', '--dcs', '8.8.8.8'], input='''leader
other

y''')
            assert 'falling back to DCS' in result.output

        mocked = Mock()
        mocked.return_value.status_code = 500
        with patch('patroni.ctl.post_patroni', Mock(return_value=mocked)):
            result = runner.invoke(ctl, ['failover', 'dummy', '--dcs', '8.8.8.8'], input='''leader
other

y''')
            assert 'Failover failed' in result.output


    def test_(self):
        self.assertRaises(patroni.exceptions.PatroniCtlException, get_dcs, {'scheme': 'dummy'}, 'dummy')

    @patch('psycopg2.connect', psycopg2_connect)
    @patch('patroni.ctl.query_member', Mock(return_value=([['mock column']], None)))
    def test_query(self):
        runner = CliRunner()

        with patch('patroni.ctl.get_dcs', Mock(return_value=self.e)):
            ## Mutually exclusive
            result = runner.invoke(ctl, [
                'query',
                'alpha',
                '--member',
                'abc',
                '--role',
                'master',
            ])
            assert result.exit_code == 1

            with runner.isolated_filesystem():
                dummy_file = open('dummy', 'w')
                dummy_file.write('SELECT 1')
                dummy_file.close()

                ## Mutually exclusive
                result = runner.invoke(ctl, [
                    'query',
                    'alpha',
                    '--file',
                    'dummy',
                    '--command',
                    'dummy',
                ])
                assert result.exit_code == 1

                result = runner.invoke(ctl, ['query', 'alpha', '--file', 'dummy'])

                os.remove('dummy')

            result = runner.invoke(ctl, ['query', 'alpha', '--command', 'SELECT 1'])
            assert 'mock column' in result.output

            ## --command or --file is mandatory
            result = runner.invoke(ctl, ['query', 'alpha'])
            assert result.exit_code == 1

            result = runner.invoke(ctl, ['query', 'alpha', '--command', 'SELECT 1',
                                         '--username', 'root', '--password', '--dbname', 'postgres'], input='ab\nab')
            assert 'mock column' in result.output

    @patch('patroni.ctl.get_cursor', Mock(return_value=MockConnect().cursor()))
    def test_query_member(self):
        rows = query_member(None, None, None, 'master', 'SELECT pg_is_in_recovery()')
        assert 'False' in str(rows)

        rows = query_member(None, None, None, 'replica', 'SELECT pg_is_in_recovery()')
        assert rows == (None, None)

        with patch('patroni.ctl.get_cursor', Mock(return_value=None)):
            rows = query_member(None, None, None, None, 'SELECT pg_is_in_recovery()')
            assert 'No connection to' in str(rows)

            rows = query_member(None, None, None, 'replica', 'SELECT pg_is_in_recovery()')
            assert 'No connection to' in str(rows)

        with patch('patroni.ctl.get_cursor', Mock(side_effect=psycopg2.OperationalError('bla'))):
            rows = query_member(None, None, None, 'replica', 'SELECT pg_is_in_recovery()')

        with patch('test_postgresql.MockCursor.execute', Mock(side_effect=psycopg2.OperationalError('bla'))):
            rows = query_member(None, None, None, 'replica', 'SELECT pg_is_in_recovery()')

    @patch('patroni.dcs.AbstractDCS.get_cluster', Mock(return_value=get_cluster_initialized_with_leader()))
    def test_dsn(self):
        runner = CliRunner()

        with patch('patroni.ctl.get_dcs', Mock(return_value=self.e)):
            result = runner.invoke(ctl, ['dsn', 'alpha', '--dcs', '8.8.8.8'])
            assert 'host=127.0.0.1 port=5435' in result.output

            ## Mutually exclusive options
            result = runner.invoke(ctl, [
                'dsn',
                'alpha',
                '--role',
                'master',
                '--member',
                'dummy',
            ])
            assert result.exit_code == 1

            ## Non-existing member
            result = runner.invoke(ctl, ['dsn', 'alpha', '--member', 'dummy'])
            assert result.exit_code == 1

    @patch('patroni.etcd.Etcd.get_cluster', Mock(return_value=get_cluster_initialized_with_leader()))
    @patch('patroni.etcd.Etcd.get_etcd_client', Mock(return_value=None))
    @patch('requests.get', requests_get)
    @patch('requests.post', requests_get)
    def test_restart_reinit(self):
        runner = CliRunner()

        result = runner.invoke(ctl, ['restart', 'alpha', '--dcs', '8.8.8.8'], input='y')
        assert result.exit_code == 0

        result = runner.invoke(ctl, ['reinit', 'alpha', '--dcs', '8.8.8.8'], input='y')
        assert result.exit_code == 1

        # Aborted restart
        result = runner.invoke(ctl, ['restart', 'alpha', '--dcs', '8.8.8.8'], input='N')
        assert result.exit_code == 1

        ## Not a member
        result = runner.invoke(ctl, [
            'restart',
            'alpha',
            '--dcs',
            '8.8.8.8',
            'dummy',
            '--any',
        ], input='y')
        assert result.exit_code == 1

        with patch('requests.post', Mock(return_value=MockResponse())):
            result = runner.invoke(ctl, ['restart', 'alpha', '--dcs', '8.8.8.8'], input='y')

    @patch('patroni.etcd.Etcd.get_cluster', Mock(return_value=get_cluster_initialized_with_leader()))
    @patch('patroni.etcd.Etcd.get_etcd_client', Mock(return_value=None))
    def test_remove(self):
        runner = CliRunner()

        result = runner.invoke(ctl, ['remove', 'alpha', '--dcs', '8.8.8.8'], input='alpha\nslave')
        assert 'Please confirm' in result.output
        assert 'You are about to remove all' in result.output
        ## Not typing an exact confirmation
        assert result.exit_code == 1

        ## master specified does not match master of cluster
        result = runner.invoke(ctl, ['remove', 'alpha', '--dcs', '8.8.8.8'], input='''alpha
Yes I am aware
slave''')
        assert result.exit_code == 1

        ## cluster specified on cmdline does not match verification prompt
        result = runner.invoke(ctl, ['remove', 'alpha', '--dcs', '8.8.8.8'], input='beta\nleader')
        assert result.exit_code == 1

        with patch('patroni.etcd.Etcd.get_cluster', get_cluster_initialized_with_leader):
            result = runner.invoke(ctl, ['remove', 'alpha', '--dcs', '8.8.8.8'],
                                   input='''alpha
Yes I am aware
leader''')
            assert 'object has no attribute' in str(result.exception)

        with patch('patroni.ctl.get_dcs', Mock(return_value=Mock())):
            ## Not implemented DCS
            result = runner.invoke(ctl, ['remove', 'alpha', '--dcs', '8.8.8.8'],
                                   input='''alpha
Yes I am aware
leader''')
            assert result.exit_code == 1

    @patch('patroni.etcd.Etcd.watch', Mock(return_value=None))
    @patch('patroni.etcd.Etcd.get_cluster', Mock(return_value=get_cluster_initialized_with_leader()))
    def test_wait_for_leader(self):
        dcs = self.e
        self.assertRaises(patroni.exceptions.PatroniCtlException, wait_for_leader, dcs, 0)

        cluster = wait_for_leader(dcs=dcs, timeout=2)
        assert cluster.leader.member.name == 'leader'

    def test_post_patroni(self):
        member = get_cluster_initialized_with_leader().leader.member
        self.assertRaises(requests.exceptions.ConnectionError, post_patroni, member, 'dummy', {})

    def test_ctl(self):
        runner = CliRunner()

        runner.invoke(ctl, ['list'])

        result = runner.invoke(ctl, ['--help'])
        assert 'Usage:' in result.output

    def test_get_any_member(self):
        m = get_any_member(get_cluster_initialized_without_leader(), role='master')
        assert m is None

        m = get_any_member(get_cluster_initialized_with_leader(), role='master')
        assert m.name == 'leader'

    def test_get_all_members(self):
        r = list(get_all_members(get_cluster_initialized_without_leader(), role='master'))
        assert len(r) == 0

        r = list(get_all_members(get_cluster_initialized_with_leader(), role='master'))
        assert len(r) == 1
        assert r[0].name == 'leader'

        r = list(get_all_members(get_cluster_initialized_with_leader(), role='replica'))
        assert len(r) == 1
        assert r[0].name == 'other'

        r = list(get_all_members(get_cluster_initialized_without_leader(), role='replica'))
        assert len(r) == 2

    @patch('patroni.etcd.Etcd.get_cluster', Mock(return_value=get_cluster_initialized_with_leader()))
    @patch('patroni.etcd.Etcd.get_etcd_client', Mock(return_value=None))
    @patch('requests.get', requests_get)
    @patch('requests.post', requests_get)
    def test_members(self):
        runner = CliRunner()

        result = runner.invoke(members, ['alpha'])
        assert result.exit_code == 0

    def test_configure(self):
        runner = CliRunner()

        result = runner.invoke(configure, [
            '--dcs',
            'abc',
            '-c',
            'dummy',
            '-n',
            'bla',
        ])

        assert result.exit_code == 0


