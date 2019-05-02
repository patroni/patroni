import logging

from patroni.exceptions import PostgresException

logger = logging.getLogger(__name__)


def postgres_version_to_int(pg_version):
    """Convert the server_version to integer

    >>> postgres_version_to_int('9.5.3')
    90503
    >>> postgres_version_to_int('9.3.13')
    90313
    >>> postgres_version_to_int('10.1')
    100001
    >>> postgres_version_to_int('10')  # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    PostgresException: 'Invalid PostgreSQL version format: X.Y or X.Y.Z is accepted: 10'
    >>> postgres_version_to_int('9.6')  # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    PostgresException: 'Invalid PostgreSQL version format: X.Y or X.Y.Z is accepted: 9.6'
    >>> postgres_version_to_int('a.b.c')  # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    PostgresException: 'Invalid PostgreSQL version: a.b.c'
    """

    try:
        components = list(map(int, pg_version.split('.')))
    except ValueError:
        raise PostgresException('Invalid PostgreSQL version: {0}'.format(pg_version))

    if len(components) < 2 or len(components) == 2 and components[0] < 10 or len(components) > 3:
        raise PostgresException('Invalid PostgreSQL version format: X.Y or X.Y.Z is accepted: {0}'.format(pg_version))

    if len(components) == 2:
        # new style verion numbers, i.e. 10.1 becomes 100001
        components.insert(1, 0)

    return int(''.join('{0:02d}'.format(c) for c in components))


def postgres_major_version_to_int(pg_version):
    """
    >>> postgres_major_version_to_int('10')
    100000
    >>> postgres_major_version_to_int('9.6')
    90600
    """
    return postgres_version_to_int(pg_version + '.0')


def parse_lsn(lsn):
    t = lsn.split('/')
    return int(t[0], 16) * 0x100000000 + int(t[1], 16)


def parse_history(data):
    for line in data.split('\n'):
        values = line.strip().split('\t')
        if len(values) == 3:
            try:
                values[0] = int(values[0])
                values[1] = parse_lsn(values[1])
                yield values
            except (IndexError, ValueError):
                logger.exception('Exception when parsing timeline history line "%s"', values)
