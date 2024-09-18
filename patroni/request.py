"""Facilities for handling communication with Patroni's REST API."""
import json

from typing import Any, Dict, Optional, Union

import urllib3

from .config import Config
from .dcs import Member
from .utils import USER_AGENT


class HTTPSConnectionPool(urllib3.HTTPSConnectionPool):

    def _validate_conn(self, *args: Any, **kwargs: Any) -> None:
        """Override parent method to silence warnings about requests without certificate verification enabled."""


class PatroniPoolManager(urllib3.PoolManager):

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(PatroniPoolManager, self).__init__(*args, **kwargs)
        self.pool_classes_by_scheme = {'http': urllib3.HTTPConnectionPool, 'https': HTTPSConnectionPool}


class PatroniRequest(object):
    """Wrapper for performing requests to Patroni's REST API.

    Prepares the request manager with the configured settings before performing the request.
    """

    def __init__(self, config: Union[Config, Dict[str, Any]], insecure: Optional[bool] = None) -> None:
        """Create a new :class:`PatroniRequest` instance with given *config*.

        :param config: Patroni YAML configuration.
        :param insecure: how to deal with SSL certs verification:

            * If ``True`` it will perform REST API requests without verifying SSL certs; or
            * If ``False`` it will perform REST API requests and verify SSL certs; or
            * If ``None`` it will behave according to the value of ``ctl.insecure`` configuration; or
            * If none of the above applies, then it falls back to ``False``.
        """
        self._insecure = insecure
        self._pool = PatroniPoolManager(num_pools=10, maxsize=10)
        self.reload_config(config)

    @staticmethod
    def _get_ctl_value(config: Union[Config, Dict[str, Any]], name: str, default: Any = None) -> Optional[Any]:
        """Get value of *name* setting from the ``ctl`` section of the *config*.

        :param config: Patroni YAML configuration.
        :param name: name of the setting value to be retrieved.

        :returns: value of ``ctl.*name*`` if present, ``None`` otherwise.
        """
        return config.get('ctl', {}).get(name, default)

    @staticmethod
    def _get_restapi_value(config: Union[Config, Dict[str, Any]], name: str) -> Optional[Any]:
        """Get value of *name* setting from the ``restapi`` section of the *config*.

        :param config: Patroni YAML configuration.
        :param name: name of the setting value to be retrieved.

        :returns: value of ``restapi -> *name*`` if present, ``None`` otherwise.
        """
        return config.get('restapi', {}).get(name)

    def _apply_pool_param(self, param: str, value: Any) -> None:
        """Configure *param* as *value* in the request manager.

        :param param: name of the setting to be changed.
        :param value: new value for *param*. If ``None``, ``0``, ``False``, and similar values, then explicit *param*
            declaration is removed, in which case it takes its default value, if any.
        """
        if value:
            self._pool.connection_pool_kw[param] = value
        else:
            self._pool.connection_pool_kw.pop(param, None)

    def _apply_ssl_file_param(self, config: Union[Config, Dict[str, Any]], name: str) -> Union[str, None]:
        """Apply a given SSL related param to the request manager.

        :param config: Patroni YAML configuration.
        :param name: prefix of the Patroni SSL related setting name. Currently, supports these:

            * ``cert``: gets translated to ``certfile``
            * ``key``: gets translated to ``keyfile``

            Will attempt to fetch the requested key first from ``ctl`` section.

        :returns: value of ``ctl.*name*file`` if present, ``None`` otherwise.
        """
        value = self._get_ctl_value(config, name + 'file')
        self._apply_pool_param(name + '_file', value)
        return value

    def reload_config(self, config: Union[Config, Dict[str, Any]]) -> None:
        """Apply *config* to request manager.

        Configure these HTTP headers for requests:

            * ``authorization``: based on Patroni' CTL or REST API authentication config;
            * ``user-agent``: based on ``patroni.utils.USER_AGENT``.

        Also configure SSL related settings for requests:

            * ``ca_certs`` is configured if ``ctl.cacert`` or ``restapi.cafile`` is available;
            * ``cert``, ``key`` and ``key_password`` are configured if ``ctl.certfile`` is available.

        :param config: Patroni YAML configuration.
        """
        # ``ctl -> auth`` is equivalent to ``ctl -> authentication -> username`` + ``:`` +
        # ``ctl -> authentication -> password``. And the same for ``restapi -> auth``
        basic_auth = self._get_ctl_value(config, 'auth') or self._get_restapi_value(config, 'auth')
        self._pool.headers = urllib3.make_headers(basic_auth=basic_auth, user_agent=USER_AGENT)
        self._pool.connection_pool_kw['cert_reqs'] = 'CERT_REQUIRED'

        insecure = self._insecure if isinstance(self._insecure, bool)\
            else self._get_ctl_value(config, 'insecure', False)

        if self._apply_ssl_file_param(config, 'cert'):
            if insecure:  # The assert_hostname = False helps to silence warnings
                self._pool.connection_pool_kw['assert_hostname'] = False

            self._apply_ssl_file_param(config, 'key')
            password = self._get_ctl_value(config, 'keyfile_password')
            self._apply_pool_param('key_password', password)
        else:
            if insecure:  # Disable server certificate validation if requested
                self._pool.connection_pool_kw['cert_reqs'] = 'CERT_NONE'
            self._pool.connection_pool_kw.pop('assert_hostname', None)
            self._pool.connection_pool_kw.pop('key_file', None)

        cacert = self._get_ctl_value(config, 'cacert') or self._get_restapi_value(config, 'cafile')
        self._apply_pool_param('ca_certs', cacert)

    def request(self, method: str, url: str, body: Optional[Any] = None,
                **kwargs: Any) -> urllib3.response.HTTPResponse:
        """Perform an HTTP request.

        :param method: the HTTP method to be used, e.g. ``GET``.
        :param url: the URL to be requested.
        :param body: anything to be used as the request body.
        :param kwargs: keyword arguments to be passed to :func:`urllib3.PoolManager.request`.

        :returns: the response returned upon request.
        """
        if body is not None and not isinstance(body, str):
            body = json.dumps(body)
        return self._pool.request(method.upper(), url, body=body, **kwargs)

    def __call__(self, member: Member, method: str = 'GET', endpoint: Optional[str] = None,
                 data: Optional[Any] = None, **kwargs: Any) -> urllib3.response.HTTPResponse:
        """Turn :class:`PatroniRequest` into a callable object.

        When called, perform a request through the manager.

        :param member: DCS member so we can fetch from it the configured base URL for the REST API.
        :param method: HTTP method to be used, e.g. ``GET``.
        :param endpoint: URL path of this request, e.g. ``switchover``.
        :param data: anything to be used as the request body.

        :returns: the response returned upon request.
        """
        url = member.get_endpoint_url(endpoint)
        return self.request(method, url, data, **kwargs)


def get(url: str, verify: bool = True, **kwargs: Any) -> urllib3.response.HTTPResponse:
    """Perform an HTTP GET request.

    .. note::
        It uses :class:`PatroniRequest` so all relevant configuration is applied before processing the request.

    :param url: full URL for this GET request.
    :param verify: if it should verify SSL certificates when processing the request.

    :returns: the response returned from the request.
    """
    http = PatroniRequest({}, not verify)
    return http.request('GET', url, **kwargs)
