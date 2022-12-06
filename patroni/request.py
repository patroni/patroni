import json
import urllib3
import six

from six.moves.urllib_parse import urlparse, urlunparse

from .utils import USER_AGENT


class PatroniRequest(object):

    def __init__(self, config, insecure=None):
        self._insecure = insecure
        self._pool = urllib3.PoolManager(num_pools=10, maxsize=10)
        self.reload_config(config)

    @staticmethod
    def _get_cfg_value(config, name):
        return config.get('ctl', {}).get(name) or config.get('restapi', {}).get(name)

    def _apply_pool_param(self, param, value):
        if value:
            self._pool.connection_pool_kw[param] = value
        else:
            self._pool.connection_pool_kw.pop(param, None)

    def _apply_ssl_file_param(self, config, name):
        value = self._get_cfg_value(config, name + 'file')
        self._apply_pool_param(name + '_file', value)
        return value

    def reload_config(self, config):
        self._pool.headers = urllib3.make_headers(basic_auth=self._get_cfg_value(config, 'auth'), user_agent=USER_AGENT)

        insecure = self._insecure if isinstance(self._insecure, bool) else config.get('ctl', {}).get('insecure', False)
        if self._apply_ssl_file_param(config, 'cert'):
            #  With client certificate the cert_reqs must be set to CERT_REQUIRED even if insecure option is used
            self._pool.connection_pool_kw['cert_reqs'] = 'CERT_REQUIRED'
            #  The assert_hostname = False helps to silence warnings
            self._pool.connection_pool_kw['assert_hostname'] = False if insecure else None

            self._apply_ssl_file_param(config, 'key')

            password = self._get_cfg_value(config, 'keyfile_password')
            self._apply_pool_param('key_password', password)
        else:
            self._pool.connection_pool_kw['cert_reqs'] = 'CERT_NONE' if insecure else 'CERT_REQUIRED'
            self._pool.connection_pool_kw.pop('key_file', None)

        cacert = config.get('ctl', {}).get('cacert') or config.get('restapi', {}).get('cafile')
        self._apply_pool_param('ca_certs', cacert)

    def request(self, method, url, body=None, **kwargs):
        if body is not None and not isinstance(body, six.string_types):
            body = json.dumps(body)
        return self._pool.request(method.upper(), url, body=body, **kwargs)

    def __call__(self, member, method='GET', endpoint=None, data=None, **kwargs):
        url = member.api_url
        if endpoint:
            scheme, netloc, _, _, _, _ = urlparse(url)
            url = urlunparse((scheme, netloc, endpoint, '', '', ''))
        return self.request(method, url, data, **kwargs)


def get(url, verify=True, **kwargs):
    http = PatroniRequest({}, not verify)
    return http.request('GET', url, **kwargs)
