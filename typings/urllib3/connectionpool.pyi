from typing import Type

from .connection import HTTPConnection, HTTPSConnection


class HTTPConnectionPool:
    ConnectionCls: Type[HTTPConnection]


class HTTPSConnectionPool(HTTPConnectionPool):
    def _validate_conn(self, conn: HTTPSConnection) -> None: ...
