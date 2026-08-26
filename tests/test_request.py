import base64
import unittest

from patroni.request import PatroniRequest


class TestPatroniRequestAuthentication(unittest.TestCase):

    @staticmethod
    def _authorization(config):
        return PatroniRequest(config)._pool.headers.get('authorization')

    @staticmethod
    def _basic(value):
        return 'Basic {0}'.format(base64.b64encode(value.encode('utf-8')).decode('ascii'))

    def test_outgoing_auth_precedence(self):
        config = {
            'ctl': {'auth': 'ctl:secret'},
            'restapi': {
                'auth': 'flat:secret',
                'authentication': {'username': 'nested', 'password': 'secret', 'mode': 'permissive'},
            },
        }
        self.assertEqual(self._basic('ctl:secret'), self._authorization(config))

        del config['ctl']['auth']
        self.assertEqual(self._basic('flat:secret'), self._authorization(config))

        del config['restapi']['auth']
        self.assertEqual(self._basic('nested:secret'), self._authorization(config))

    def test_structured_auth_is_sent_in_every_mode(self):
        for mode in ('disabled', 'permissive', 'strict'):
            with self.subTest(mode=mode):
                config = {'restapi': {'authentication': {
                    'username': 'patroni', 'password': 'secret', 'mode': mode}}}
                self.assertEqual(self._basic('patroni:secret'), self._authorization(config))

    def test_incomplete_structured_auth_is_not_sent(self):
        for authentication in ({}, {'username': 'patroni'}, {'password': 'secret'},
                               {'username': '', 'password': 'secret'},
                               {'username': 'patroni', 'password': ''}, 'invalid'):
            with self.subTest(authentication=authentication):
                config = {'restapi': {'authentication': authentication}}
                self.assertIsNone(self._authorization(config))


if __name__ == '__main__':
    unittest.main()
