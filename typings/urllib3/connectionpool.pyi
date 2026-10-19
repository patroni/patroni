from .connection import HTTPSConnection


class HTTPConnectionPool: ...


class HTTPSConnectionPool(HTTPConnectionPool):
    def _validate_conn(self, conn: HTTPSConnection) -> None: ...
