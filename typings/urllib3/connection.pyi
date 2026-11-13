import ssl
from http.client import HTTPConnection as _HTTPConnection
from typing import Any, Optional


class HTTPConnection(_HTTPConnection): ...


class HTTPSConnection(HTTPConnection):
    sock: Optional[ssl.SSLSocket]


def create_urllib3_context(ssl_version: Optional[int] = None, cert_reqs: Optional[int] = None,
                           options: Optional[int] = None, ciphers: Optional[str] = None,
                           **kwargs: Any) -> ssl.SSLContext: ...
