"""Abstraction layer for :mod:`psycopg` module.

This module is able to handle both :mod:`pyscopg2` and :mod:`psycopg`, and it exposes a common interface for both.
:mod:`psycopg2` takes precedence. :mod:`psycopg` will only be used if :mod:`psycopg2` is either absent or older than
``2.5.4``.
"""
from typing import Any, Callable, Dict, Optional, TYPE_CHECKING, Union

if TYPE_CHECKING:  # pragma: no cover
    from psycopg import Connection
    from psycopg2 import connection, cursor

__all__ = ['connect', 'parse_conninfo', 'quote_ident', 'quote_literal',
           'DatabaseError', 'Error', 'OperationalError', 'ProgrammingError']

_legacy = False
try:
    from psycopg2 import __version__

    from . import MIN_PSYCOPG2, parse_version
    if parse_version(__version__) < MIN_PSYCOPG2:
        raise ImportError
    from psycopg2 import connect as _connect, DatabaseError, Error, OperationalError, ProgrammingError
    from psycopg2.extensions import adapt

    try:
        from psycopg2.extensions import parse_dsn, quote_ident as _quote_ident

        def _parse_conninfo(conninfo: str, **kwargs: Any) -> Any:
            """Wraps :func:`parse_dsn` function.

            Exists only to please pyright.
            """
            return parse_dsn(conninfo)
    except ImportError:
        _legacy = True

        def _parse_conninfo(conninfo: str, **kwargs: Any) -> Any:
            """Return ``None`` and rely on fallback."""
            return None

    def quote_literal(value: Any, conn: Optional[Any] = None) -> str:
        """Quote *value* as a SQL literal.

        .. note::
            *value* is quoted through :mod:`psycopg2` adapters.

        :param value: value to be quoted.
        :param conn: if a connection is given then :func:`quote_literal` checks if any special handling based on server
            parameters needs to be applied to *value* before quoting it as a SQL literal.

        :returns: *value* quoted as a SQL literal.
        """
        value = adapt(value)
        if conn:
            value.prepare(conn)
        return value.getquoted().decode('utf-8')
except ImportError:
    import types

    from psycopg import DatabaseError, Error, OperationalError, ProgrammingError, sql
    # isort: off
    from psycopg import connect as __connect  # pyright: ignore [reportUnknownVariableType]
    from psycopg.conninfo import conninfo_to_dict as _parse_conninfo

    def __get_parameter_status(self: 'Connection[Any]', param_name: str) -> Optional[str]:
        """Helper function to be injected into :class:`Connection` object.

        :param param_name: the name of the connection parameter.

        :returns: the value for the *param_name* or ``None``.
        """
        return self.info.parameter_status(param_name)

    def _connect(dsn: Optional[str] = None, **kwargs: Any) -> 'Connection[Any]':
        """Call :func:`psycopg.connect` with *dsn* and ``**kwargs``.

        .. note::
            Will create following methods and attributes in the returning connection to keep compatibility
            with the object that would be returned by :func:`psycopg2.connect`:

                * ``server_version`` attribute.
                * ``get_parameter_status`` method.

        :param dsn: DSN to call :func:`psycopg.connect` with.
        :param kwargs: keyword arguments to call :func:`psycopg.connect` with.

        :returns: a connection to the database.
        """
        ret: 'Connection[Any]' = __connect(dsn or "", **kwargs)
        # compatibility with psycopg2
        setattr(ret, 'server_version', ret.pgconn.server_version)
        setattr(ret, 'get_parameter_status', types.MethodType(__get_parameter_status, ret))
        return ret

    def _quote_ident(value: Any, scope: Any) -> str:
        """Quote *value* as a SQL identifier.

        :param value: value to be quoted.
        :param scope: connection to evaluate the returning string into.

        :returns: *value* quoted as a SQL identifier.
        """
        return sql.Identifier(value).as_string(scope)

    def quote_literal(value: Any, conn: Optional[Any] = None) -> str:
        """Quote *value* as a SQL literal.

        :param value: value to be quoted.
        :param conn: connection to evaluate the returning string into.

        :returns: *value* quoted as a SQL literal.
        """
        return sql.Literal(value).as_string(conn)


def connect(*args: Any, **kwargs: Any) -> Union['connection', 'Connection[Any]']:
    """Get a connection to the database.

    .. note::
        The connection will have ``autocommit`` enabled.

        It also enforces ``search_path=pg_catalog`` for non-replication connections to mitigate security issues as
        Patroni relies on superuser connections.

    :param args: positional arguments to call :func:`~psycopg.connect` function from :mod:`psycopg` module.
    :param kwargs: keyword arguments to call :func:`~psycopg.connect` function from :mod:`psycopg` module.

    :returns: a connection to the database. Can be either a :class:`psycopg.Connection` if using :mod:`psycopg`, or a
        :class:`psycopg2.extensions.connection` if using :mod:`psycopg2`.
    """
    if kwargs and 'replication' not in kwargs and kwargs.get('fallback_application_name') != 'Patroni ctl':
        options = [kwargs['options']] if 'options' in kwargs else []
        options.append('-c search_path=pg_catalog')
        kwargs['options'] = ' '.join(options)
    ret = _connect(*args, **kwargs)
    ret.autocommit = True
    return ret


def quote_ident(value: Any, conn: Optional[Union['cursor', 'connection', 'Connection[Any]']] = None) -> str:
    """Quote *value* as a SQL identifier.

    :param value: value to be quoted.
    :param conn: connection to evaluate the returning string into. Can be either a :class:`psycopg.Connection` if
        using :mod:`psycopg`, or a :class:`psycopg2.extensions.connection` if using :mod:`psycopg2`.

    :returns: *value* quoted as a SQL identifier.
    """
    if _legacy or conn is None:
        return '"{0}"'.format(value.replace('"', '""'))
    return _quote_ident(value, conn)


def parse_conninfo(value: str, fallback: Callable[[str], Optional[Dict[str, str]]]) -> Optional[Dict[str, str]]:
    """Parse connection string.

    :param value: value to parse.
    :param fallback: a function to use if we have only very old ``psycopg2``, which doesn't expose :func:`parse_dsn`.

    :returns: a :class:`dict` object, or ``None`` if failed to parse.
    """
    try:
        ret = _parse_conninfo(value)
    except Exception:
        ret = None
    return ret or fallback(value)
