from patroni.exceptions import PostgresException


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
