import os
import mock
import pytest
import unittest
import requests
import patroni.exceptions
from mock import patch, Mock

from click.testing import CliRunner
from patroni.ctl import ctl, members, store_config, load_config, output_members, post_patroni, get_dcs, wait_for_leader
from patroni.dcs import AbstractDCS, Member
from patroni.etcd import Etcd
from test_ha import get_cluster_initialized_without_leader, get_cluster_initialized_with_leader, get_cluster

CONFIG_FILE_PATH = './test-ctl.yaml'

class TestCtl(unittest.TestCase):

    def test_output_members(self):
        cluster = get_cluster_initialized_with_leader()
        output_members(cluster, name='abc', format='pretty')
        output_members(cluster, name='abc', format='json')


    def test_rw_config(self):
        runner = CliRunner()
        config = 'a:b'
        with runner.isolated_filesystem():
            store_config(config, CONFIG_FILE_PATH)
            os.remove(CONFIG_FILE_PATH)
            os.mkdir(CONFIG_FILE_PATH)
            with pytest.raises(Exception):
                result = load_config(CONFIG_FILE_PATH, None)
                assert 'Could not load configuration file' in result.output

            with pytest.raises(Exception):
                store_config(config, CONFIG_FILE_PATH)

            os.rmdir(CONFIG_FILE_PATH)

        store_config(config, 'abc/CONFIG_FILE_PATH')
        load_config(CONFIG_FILE_PATH, None)

    @patch('patroni.ctl.get_dcs', Mock(return_value=AbstractDCS('dummy',{'namespace':'dummy', 'scope':'dummy'})))
    @patch('patroni.ctl.post_patroni', Mock(return_value=None))
    def test_failover(self):
        runner = CliRunner()
       
        with patch('patroni.dcs.AbstractDCS.get_cluster', Mock(return_value=get_cluster_initialized_without_leader())):
            result = runner.invoke(ctl, ['failover', 'alpha', '--dcs', '8.8.8.8'])
            assert 'This cluster has no master' in str(result.exception)

        with patch('patroni.dcs.AbstractDCS.get_cluster', Mock(return_value=get_cluster_initialized_with_leader())):
            result = runner.invoke(ctl, ['failover', 'alpha', '--dcs', '8.8.8.8'], input='nonsense')
            assert 'is not the leader of cluster' in str(result.exception)
            
            result = runner.invoke(ctl, ['failover', 'alpha', '--dcs', '8.8.8.8', '--master', 'nonsense'])
            assert 'is not the leader of cluster' in str(result.exception)
            
            result = runner.invoke(ctl, ['failover', 'alpha', '--dcs', '8.8.8.8'], input='leader\nother\nn')
            assert 'Aborting failover' in str(result.exception)
            
            with patch('patroni.ctl.wait_for_leader', Mock(return_value = get_cluster_initialized_with_leader())):
                result = runner.invoke(ctl, ['failover', 'alpha', '--dcs', '8.8.8.8'], input='leader\nother\nY')
                assert 'master did not change after' in result.output
                
                result = runner.invoke(ctl, ['failover', 'alpha', '--dcs', '8.8.8.8'], input='leader\nother\nY')
                assert 'Failover failed' in result.output
        
    def test_get_dcs(self):
        self.assertRaises(patroni.exceptions.PatroniCtlException, get_dcs, {'scheme':'dummy'}, 'dummy')


    @patch('patroni.ctl.get_dcs', Mock(return_value=AbstractDCS('dummy',{'namespace':'dummy', 'scope':'dummy'})))
    @patch('patroni.dcs.AbstractDCS.get_cluster', Mock(return_value=get_cluster_initialized_with_leader()))
    @patch('patroni.etcd.Etcd.get_etcd_client', Mock(return_value=None))
    def test_remove(self):
        runner = CliRunner()

        result = runner.invoke(ctl, ['remove', 'alpha', '--dcs', '8.8.8.8'], input='alpha\nslave')
        assert 'Please confirm' in result.output
        assert 'You are about to remove all' in result.output
        assert 'You did not exactly type' in str(result.exception)

        result = runner.invoke(ctl, ['remove', 'alpha', '--dcs', '8.8.8.8'], input='alpha\nYes I am aware\nleader')
        assert 'We have not implemented this for DCS of type' in str(result.exception)

        result = runner.invoke(ctl, ['remove', 'alpha', '--dcs', '8.8.8.8'], input='alpha\nYes I am aware\nslave')
        assert 'You did not specify the current master of the cluster' in str(result.exception)

        result = runner.invoke(ctl, ['remove', 'alpha', '--dcs', '8.8.8.8'], input='beta\nleader')
        assert 'Cluster names specified do not match' in str(result.exception)
    
        with patch('patroni.dcs.AbstractDCS.get_cluster', Mock(return_value=Etcd('dummy', {'namespace':'dummy', 'scope':'dummy'}))):
            result = runner.invoke(ctl, ['remove', 'alpha', '--dcs', '8.8.8.8'], input='alpha\nYes I am aware\nleader')
            assert 'object has no attribute' in str(result.exception)

    @patch('patroni.dcs.AbstractDCS.watch', Mock(return_value=None))
    @patch('patroni.dcs.AbstractDCS.get_cluster', Mock(return_value=get_cluster_initialized_with_leader()))
    def test_wait_for_leader(self):
        dcs = AbstractDCS('dummy',{'namespace':'dummy', 'scope':'dummy'})
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


    def test_members(self):
        runner = CliRunner()

        runner.invoke(members)


