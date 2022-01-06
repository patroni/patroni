__all__ = ['connect', 'quote_ident', 'quote_literal', 'DatabaseError', 'Error', 'OperationalError', 'ProgrammingError']

_legacy = False
try:
    from psycopg2 import __version__
    from . import MIN_PSYCOPG2, parse_version
    if parse_version(__version__) < MIN_PSYCOPG2:
        raise ImportError
    from psycopg2 import connect, Error, DatabaseError, OperationalError, ProgrammingError
    from psycopg2.extensions import adapt

    try:
        from psycopg2.extensions import quote_ident as _quote_ident
    except ImportError:
        _legacy = True

    def quote_literal(value, conn=None):
        value = adapt(value)
        if conn:
            value.prepare(conn)
        return value.getquoted().decode('utf-8')
except ImportError:
    from psycopg import connect as _connect, sql, Error, DatabaseError, OperationalError, ProgrammingError

    def connect(*args, **kwargs):
        ret = _connect(*args, **kwargs)
        ret.server_version = ret.pgconn.server_version  # compatibility with psycopg2
        return ret

    def _quote_ident(value, conn):
        return sql.Identifier(value).as_string(conn)

    def quote_literal(value, conn=None):
        return sql.Literal(value).as_string(conn)


def quote_ident(value, conn=None):
    if _legacy or conn is None:
        return '"{0}"'.format(value.replace('"', '""'))
    return _quote_ident(value, conn)
