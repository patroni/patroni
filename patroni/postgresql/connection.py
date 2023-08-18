import logging

from contextlib import contextmanager
from threading import Lock
from typing import Any, Dict, Iterator, List, Union, Tuple, TYPE_CHECKING
if TYPE_CHECKING:  # pragma: no cover
    from psycopg import Connection as Connection3, Cursor
    from psycopg2 import connection, cursor

from .. import psycopg
from ..exceptions import PostgresConnectionException

logger = logging.getLogger(__name__)


class Connection:
    """Helper class to manage connections from Patroni to PostgreSQL.

    :ivar server_version: PostgreSQL version in integer format where we are connected to.
    """

    server_version: int

    def __init__(self) -> None:
        """Create an instance of :class:`Connection` class."""
        self._lock = Lock()  # used to make sure that only one connection to postgres is established
        self._connection = None

    def set_conn_kwargs(self, conn_kwargs: Dict[str, Any]) -> None:
        """Set connection parameters, like user, password, host, port and so on.

        :param conn_kwargs: connection parameters as a dictionary.
        """
        self._conn_kwargs = conn_kwargs

    def get(self) -> Union['connection', 'Connection3[Any]']:
        """Get ``psycopg``/``psycopg2`` connection object.

        .. note::
            Opens a new connection if necessary.

        :returns: ``psycopg`` or ``psycopg2`` connection object.
        """
        with self._lock:
            if not self._connection or self._connection.closed != 0:
                logger.info("establishing a new patroni connection to postgres")
                self._connection = psycopg.connect(**self._conn_kwargs)
                self.server_version = getattr(self._connection, 'server_version', 0)
        return self._connection

    def query(self, sql: str, *params: Any) -> List[Tuple[Any, ...]]:
        """Execute a query with parameters and optionally returns a response.

        :param sql: SQL statement to execute.
        :param params: parameters to pass.

        :returns: a query response as a list of tuples if there is any.
        :raises:
            :exc:`~psycopg.Error` if had issues while executing *sql*.

            :exc:`~patroni.exceptions.PostgresConnectionException`: if had issues while connecting to the database.
        """
        cursor = None
        try:
            with self.get().cursor() as cursor:
                cursor.execute(sql.encode('utf-8'), params or None)
                return cursor.fetchall() if cursor.rowcount and cursor.rowcount > 0 else []
        except psycopg.Error as exc:
            if cursor and cursor.connection.closed == 0:
                # When connected via unix socket, psycopg2 can't recoginze 'connection lost' and leaves
                # `self._connection.closed == 0`, but the generic exception is raised. It doesn't make
                # sense to continue with existing connection and we will close it, to avoid its reuse.
                if type(exc) in (psycopg.DatabaseError, psycopg.OperationalError):
                    self.close()
                else:
                    raise exc
            raise PostgresConnectionException('connection problems') from exc

    def close(self) -> None:
        """Close the psycopg connection to postgres."""
        if self._connection and self._connection.closed == 0:
            self._connection.close()
            logger.info("closed patroni connection to postgres")
        self._connection = None


@contextmanager
def get_connection_cursor(**kwargs: Any) -> Iterator[Union['cursor', 'Cursor[Any]']]:
    conn = psycopg.connect(**kwargs)
    with conn.cursor() as cur:
        yield cur
    conn.close()
