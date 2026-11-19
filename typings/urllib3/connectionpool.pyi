from typing import Type
from .connection import HTTPConnection
class HTTPConnectionPool:
    ConnectionCls: Type[HTTPConnection]
class HTTPSConnectionPool(HTTPConnectionPool): ...
