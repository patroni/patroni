"""Abstraction layer for ``psycopg`` module.

This module is able to handle both ``pyscopg2`` and ``psycopg3``, and it exposes a common interface for both.
``psycopg2`` takes precedence. ``psycopg3`` will only be used if ``psycopg2`` is either absent or older than ``2.5.4``.
"""
from typing import Any, Optional


__all__ = ['connect', 'quote_ident', 'quote_literal', 'DatabaseError', 'Error', 'OperationalError', 'ProgrammingError']

_legacy = False
try:
    from psycopg2 import __version__
    from . import MIN_PSYCOPG2, parse_version
    if parse_version(__version__) < MIN_PSYCOPG2:
        raise ImportError
    from psycopg2 import connect as _connect, Error, DatabaseError, OperationalError, ProgrammingError
    from psycopg2.extensions import adapt

    try:
        from psycopg2.extensions import quote_ident as _quote_ident
    except ImportError:
        _legacy = True

    def quote_literal(value: Any, conn: Optional[Any] = None) -> str:
        """Quote *value* as a SQL literal.

        .. note::
            *value* is quoted through ``psycopg`` adapters.

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
    from psycopg import connect as __connect, sql, Error, DatabaseError, OperationalError, ProgrammingError, Connection

    def _connect(dsn: str = "", **kwargs: Any) -> Any:
        """Call ``psycopg.connect`` with ``dsn`` and ``**kwargs``.

        .. note::
            Will create ``server_version`` attribute in the returning connection, so it keeps compatibility with the
            object that would be returned by ``psycopg2.connect``.

        :param dsn: DSN to call ``psycopg.connect`` with.
        :param kwargs: keyword arguments to call ``psycopg.connect`` with.

        :returns: a connection to the database.
        """
        ret = __connect(dsn, **kwargs)
        setattr(ret, 'server_version', ret.pgconn.server_version)  # compatibility with psycopg2
        return ret

    def _quote_ident(value: Any, conn: Connection) -> str:
        """Quote *value* as a SQL identifier.

        :param value: value to be quoted.
        :param conn: connection to evaluate the returning string into.

        :returns: *value* quoted as a SQL identifier.
        """
        return sql.Identifier(value).as_string(conn)

    def quote_literal(value: Any, conn: Optional[Any] = None) -> str:
        """Quote *value* as a SQL literal.

        :param value: value to be quoted.
        :param conn: connection to evaluate the returning string into.

        :returns: *value* quoted as a SQL literal.
        """
        return sql.Literal(value).as_string(conn)


def connect(*args: Any, **kwargs: Any) -> Any:
    """Get a connection to the database.

    .. note::
        The connection will have ``autocommit`` enabled.

        It also enforces ``search_path=pg_catalog`` for non-replication connections to mitigate security issues as
        Patroni relies on superuser connections.

    :param args: positional arguments to call ``connect`` function from ``psycopg`` module.
    :param kwargs: keyword arguments to call ``connect`` function from ``psycopg`` module.

    :returns: a connection to the database. Can be either a :class:`psycopg.Connection` if using ``psycopg3``, or a
        :class:`psycopg2.extensions.connection` if using ``psycopg2``.
    """
    if kwargs and 'replication' not in kwargs and kwargs.get('fallback_application_name') != 'Patroni ctl':
        options = [kwargs['options']] if 'options' in kwargs else []
        options.append('-c search_path=pg_catalog')
        kwargs['options'] = ' '.join(options)
    ret = _connect(*args, **kwargs)
    ret.autocommit = True
    return ret


def quote_ident(value: Any, conn: Optional[Any] = None) -> str:
    """Quote *value* as a SQL identifier.

    :param value: value to be quoted.
    :param conn: connection to evaluate the returning string into. Can be either a :class:`psycopg.Connection` if
        using ``psycopg3``, or a :class:`psycopg2.extensions.connection` if using ``psycopg2``.

    :returns: *value* quoted as a SQL identifier.
    """
    if _legacy or conn is None:
        return '"{0}"'.format(value.replace('"', '""'))
    return _quote_ident(value, conn)
