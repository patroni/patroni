import boto.ec2
import sys
import unittest
import urllib3

from mock import Mock, patch
from collections import namedtuple
from patroni.scripts.aws import AWSConnection, main as _main


class MockEc2Connection(object):

    @staticmethod
    def get_all_volumes(*args, **kwargs):
        oid = namedtuple('Volume', 'id')
        return [oid(id='a'), oid(id='b')]

    @staticmethod
    def create_tags(objects, *args, **kwargs):
        if len(objects) == 0:
            raise boto.exception.BotoServerError(503, 'Service Unavailable', 'Request limit exceeded')
        return True


@patch('boto.ec2.connect_to_region', Mock(return_value=MockEc2Connection()))
class TestAWSConnection(unittest.TestCase):

    @patch('patroni.scripts.aws.requests_get', Mock(return_value=urllib3.HTTPResponse(
        status=200, body=b'{"instanceId": "012345", "region": "eu-west-1"}')))
    def setUp(self):
        self.conn = AWSConnection('test')

    def test_on_role_change(self):
        self.assertTrue(self.conn.on_role_change('master'))
        with patch.object(MockEc2Connection, 'get_all_volumes', Mock(return_value=[])):
            self.conn._retry.max_tries = 1
            self.assertFalse(self.conn.on_role_change('master'))

    @patch('patroni.scripts.aws.requests_get', Mock(side_effect=Exception('foo')))
    def test_non_aws(self):
        conn = AWSConnection('test')
        self.assertFalse(conn.on_role_change("master"))

    @patch('patroni.scripts.aws.requests_get', Mock(return_value=urllib3.HTTPResponse(status=200, body=b'foo')))
    def test_aws_bizare_response(self):
        conn = AWSConnection('test')
        self.assertFalse(conn.aws_available())

    @patch('patroni.scripts.aws.requests_get', Mock(return_value=urllib3.HTTPResponse(
        status=200, body=b'{"instanceId": "012345", "region": "eu-west-1"}')))
    @patch('sys.exit', Mock())
    def test_main(self):
        self.assertIsNone(_main())
        sys.argv = ['aws.py', 'on_start', 'replica', 'foo']
        self.assertIsNone(_main())
