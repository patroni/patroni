import ssl
import unittest

from unittest.mock import Mock, patch

from patroni.request import PatroniRequest


@patch('ssl.SSLContext.load_verify_locations', Mock())
class TestPatroniRequest(unittest.TestCase):

    def test_crlfile(self):
        # without crlfile urllib3 creates the SSL context
        request = PatroniRequest({'ctl': {'cacert': 'ca.crt', 'certfile': 'a', 'keyfile': 'b'}})
        self.assertNotIn('ssl_context', request._pool.connection_pool_kw)

        # ctl.crlfile: server certificates are checked against it, hostname included
        with patch('ssl.SSLContext.load_verify_locations') as mock_load:
            request.reload_config({'ctl': {'cacert': 'ca.crt', 'certfile': 'a', 'keyfile': 'b', 'crlfile': 'crl.pem'}})
            mock_load.assert_called_once_with(cafile='crl.pem')
        ctx = request._pool.connection_pool_kw['ssl_context']
        self.assertIsInstance(ctx, ssl.SSLContext)
        self.assertTrue(ctx.verify_flags & ssl.VERIFY_CRL_CHECK_LEAF)
        self.assertTrue(ctx.check_hostname)

        # falls back to restapi.crlfile, like ctl.cacert to restapi.cafile
        request.reload_config({'restapi': {'cafile': 'ca.crt', 'crlfile': 'crl.pem'}})
        self.assertIn('ssl_context', request._pool.connection_pool_kw)

        # removed again when crlfile is gone
        request.reload_config({'ctl': {'cacert': 'ca.crt'}})
        self.assertNotIn('ssl_context', request._pool.connection_pool_kw)

    def test_crlfile_insecure(self):
        # insecure with a client certificate: the chain is verified, the hostname is not
        request = PatroniRequest({'ctl': {'certfile': 'a', 'crlfile': 'crl.pem'}}, True)
        ctx = request._pool.connection_pool_kw['ssl_context']
        self.assertTrue(ctx.verify_flags & ssl.VERIFY_CRL_CHECK_LEAF)
        self.assertFalse(ctx.check_hostname)

        # insecure without a client certificate: nothing is verified, the crlfile is not used
        request.reload_config({'ctl': {'crlfile': 'crl.pem'}})
        self.assertNotIn('ssl_context', request._pool.connection_pool_kw)
