import botocore
import sys
import unittest
import urllib3

from mock import Mock, patch
from collections import namedtuple
from patroni.scripts.aws import AWSConnection, main as _main


class MockVolumes(object):

    @staticmethod
    def filter(*args, **kwargs):
        oid = namedtuple('Volume', 'id')
        return [oid(id='a'), oid(id='b')]


class MockEc2Connection(object):

    volumes = MockVolumes()

    @staticmethod
    def create_tags(Resources, **kwargs):
        if len(Resources) == 0:
            raise botocore.exceptions.ClientError({'Error': {'Code': 503, 'Message': 'Request limit exceeded'}},
                                                  'create_tags')
        return True


@patch('boto3.resource', Mock(return_value=MockEc2Connection()))
class TestAWSConnection(unittest.TestCase):

    @patch('patroni.scripts.aws.requests_get', Mock(return_value=urllib3.HTTPResponse(
        status=200, body=b'{"instanceId": "012345", "region": "eu-west-1"}')))
    def setUp(self):
        self.conn = AWSConnection('test')

    def test_on_role_change(self):
        self.assertTrue(self.conn.on_role_change('primary'))
        with patch.object(MockVolumes, 'filter', Mock(return_value=[])):
            self.conn._retry.max_tries = 1
            self.assertFalse(self.conn.on_role_change('primary'))

    @patch('patroni.scripts.aws.requests_get', Mock(side_effect=Exception('foo')))
    def test_non_aws(self):
        conn = AWSConnection('test')
        self.assertFalse(conn.on_role_change("primary"))

    @patch('patroni.scripts.aws.requests_get', Mock(return_value=urllib3.HTTPResponse(status=200, body=b'foo')))
    def test_aws_bizare_response(self):
        conn = AWSConnection('test')
        self.assertFalse(conn.aws_available())

    @patch('patroni.scripts.aws.requests_get', Mock(return_value=urllib3.HTTPResponse(status=503, body=b'Error')))
    @patch('sys.exit', Mock())
    def test_main(self):
        self.assertIsNone(_main())
        sys.argv = ['aws.py', 'on_start', 'replica', 'foo']
        self.assertIsNone(_main())
