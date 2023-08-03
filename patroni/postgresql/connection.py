import logging

from contextlib import contextmanager
from threading import Lock
from typing import Any, Dict, Iterator, Union, TYPE_CHECKING
if TYPE_CHECKING:  # pragma: no cover
    from psycopg import Connection as Connection3, Cursor
    from psycopg2 import connection, cursor

from .. import psycopg

logger = logging.getLogger(__name__)


class Connection(object):
    server_version: int

    def __init__(self) -> None:
        self._lock = Lock()
        self._connection = None
        self._cursor_holder = None

    def set_conn_kwargs(self, conn_kwargs: Dict[str, Any]) -> None:
        self._conn_kwargs = conn_kwargs

    def get(self) -> Union['connection', 'Connection3[Any]']:
        with self._lock:
            if not self._connection or self._connection.closed != 0:
                self._connection = psycopg.connect(**self._conn_kwargs)
                self.server_version = getattr(self._connection, 'server_version', 0)
        return self._connection

    def cursor(self) -> Union['cursor', 'Cursor[Any]']:
        if not self._cursor_holder or self._cursor_holder.closed or self._cursor_holder.connection.closed != 0:
            logger.info("establishing a new patroni connection to the postgres cluster")
            self._cursor_holder = self.get().cursor()
        return self._cursor_holder

    def close(self) -> None:
        if self._connection and self._connection.closed == 0:
            self._connection.close()
            logger.info("closed patroni connection to the postgresql cluster")
        self._cursor_holder = self._connection = None


@contextmanager
def get_connection_cursor(**kwargs: Any) -> Iterator[Union['cursor', 'Cursor[Any]']]:
    conn = psycopg.connect(**kwargs)
    with conn.cursor() as cur:
        yield cur
    conn.close()
