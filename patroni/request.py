import json
import urllib3

from six.moves.urllib_parse import urlparse, urlunparse


class PatroniRequest(object):

    def __init__(self, config, insecure=False):
        cert_reqs = 'CERT_NONE' if insecure or config.get('ctl', {}).get('insecure', False) else 'CERT_REQUIRED'
        self._pool = urllib3.PoolManager(cert_reqs=cert_reqs)
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
        self._pool.headers = urllib3.make_headers(basic_auth=self._get_cfg_value(config, 'auth'))

        if self._apply_ssl_file_param(config, 'cert'):
            self._apply_ssl_file_param(config, 'key')
        else:
            self._pool.connection_pool_kw.pop('key_file', None)

        cacert = config.get('ctl', {}).get('cacert') or config.get('restapi', {}).get('cafile')
        self._apply_pool_param('ca_certs', cacert)

    def __call__(self, member, method='GET', endpoint=None, data=None, **kwargs):
        url = member.api_url
        if endpoint:
            scheme, netloc, _, _, _, _ = urlparse(url)
            url = urlunparse((scheme, netloc, endpoint, '', '', ''))
        if data is not None:
            kwargs['body'] = json.dumps(data)
        return self._pool.request(method.upper(), url, **kwargs)
