import botocore
import botocore.awsrequest
import sys
import unittest

from mock import Mock, PropertyMock, patch
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


class MockIMDSFetcher(object):

    def __init__(self, timeout):
        pass

    @staticmethod
    def _fetch_metadata_token():
        return ''

    @staticmethod
    def _get_request(*args):
        return botocore.awsrequest.AWSResponse(url='', status_code=200, headers={}, raw=None)


@patch('boto3.resource', Mock(return_value=MockEc2Connection()))
@patch('patroni.scripts.aws.IMDSFetcher', MockIMDSFetcher)
class TestAWSConnection(unittest.TestCase):

    @patch.object(botocore.awsrequest.AWSResponse, 'text',
                  PropertyMock(return_value='{"instanceId": "012345", "region": "eu-west-1"}'))
    def test_on_role_change(self):
        conn = AWSConnection('test')
        self.assertTrue(conn.on_role_change('primary'))
        with patch.object(MockVolumes, 'filter', Mock(return_value=[])):
            conn._retry.max_tries = 1
            self.assertFalse(conn.on_role_change('primary'))

    @patch.object(MockIMDSFetcher, '_get_request', Mock(side_effect=Exception('foo')))
    def test_non_aws(self):
        conn = AWSConnection('test')
        self.assertFalse(conn.on_role_change("primary"))

    @patch.object(botocore.awsrequest.AWSResponse, 'text', PropertyMock(return_value='boo'))
    def test_aws_bizare_response(self):
        conn = AWSConnection('test')
        self.assertFalse(conn.aws_available())

    @patch.object(MockIMDSFetcher, '_get_request', Mock(return_value=botocore.awsrequest.AWSResponse(
        url='', status_code=503, headers={}, raw=None)))
    @patch('sys.exit', Mock())
    def test_main(self):
        self.assertIsNone(_main())
        sys.argv = ['aws.py', 'on_start', 'replica', 'foo']
        self.assertIsNone(_main())
