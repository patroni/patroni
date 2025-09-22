import errno
import logging
import os

from enum import Enum
from typing import Iterable, Tuple

from ..exceptions import PostgresException

logger = logging.getLogger(__name__)


class PostgresqlState(str, Enum):
    """Possible values of :attr:`Postgresql.state`.

    Numeric indexes should NEVER change once assigned to maintain
    backward compatibility with existing monitoring systems.
    """

    INITDB = ('initializing new cluster', 0)
    INITDB_FAILED = ('initdb failed', 1)
    CUSTOM_BOOTSTRAP = ('running custom bootstrap script', 2)
    CUSTOM_BOOTSTRAP_FAILED = ('custom bootstrap failed', 3)
    CREATING_REPLICA = ('creating replica', 4)
    RUNNING = ('running', 5)
    STARTING = ('starting', 6)
    BOOTSTRAP_STARTING = ('starting after custom bootstrap', 7)
    START_FAILED = ('start failed', 8)
    RESTARTING = ('restarting', 9)
    RESTART_FAILED = ('restart failed', 10)
    STOPPING = ('stopping', 11)
    STOPPED = ('stopped', 12)
    STOP_FAILED = ('stop failed', 13)
    CRASHED = ('crashed', 14)

    def __new__(cls, value: str, index: int) -> 'PostgresqlState':
        obj = str.__new__(cls, value)
        obj._value_ = value
        # Use setattr to avoid pyright type checking issues
        setattr(obj, 'index', index)
        return obj

    def __repr__(self) -> str:
        """Get an "official" string representation of a :class:`PostgresqlState` member."""
        return self.value

    def __str__(self) -> str:
        """Get a string representation of a :class:`PostgresqlState` member."""
        return self.__repr__()


class PostgresqlRole(str, Enum):
    """Possible values of :attr:`Postgresql.role`."""

    PRIMARY = 'primary'
    MASTER = 'master'
    STANDBY_LEADER = 'standby_leader'
    REPLICA = 'replica'
    DEMOTED = 'demoted'
    UNINITIALIZED = 'uninitialized'
    PROMOTED = 'promoted'

    def __repr__(self) -> str:
        """Get an "official" string representation of a :class:`PostgresqlRole` member."""
        return self.value

    def __str__(self) -> str:
        """Get a string representation of a :class:`PostgresqlRole` member."""
        return self.__repr__()


def postgres_version_to_int(pg_version: str) -> int:
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
        # new style version numbers, i.e. 10.1 becomes 100001
        components.insert(1, 0)

    return int(''.join('{0:02d}'.format(c) for c in components))


def postgres_major_version_to_int(pg_version: str) -> int:
    """
    >>> postgres_major_version_to_int('10')
    100000
    >>> postgres_major_version_to_int('9.6')
    90600
    """
    return postgres_version_to_int(pg_version + '.0')


def get_major_from_minor_version(version: int) -> int:
    """Extract major PostgreSQL version from the provided full version.

    :param version: integer representation of PostgreSQL full version (major + minor).

    :returns: integer representation of the PostgreSQL major version.

    :Example:

        >>> get_major_from_minor_version(100012)
        100000

        >>> get_major_from_minor_version(90313)
        90300
    """
    return version // 100 * 100


def parse_lsn(lsn: str) -> int:
    t = lsn.split('/')
    return int(t[0], 16) * 0x100000000 + int(t[1], 16)


def parse_history(data: str) -> Iterable[Tuple[int, int, str]]:
    for line in data.split('\n'):
        values = line.strip().split('\t')
        if len(values) == 3:
            try:
                yield int(values[0]), parse_lsn(values[1]), values[2]
            except (IndexError, ValueError):
                logger.exception('Exception when parsing timeline history line "%s"', values)


def format_lsn(lsn: int, full: bool = False) -> str:
    template = '{0:X}/{1:08X}' if full else '{0:X}/{1:X}'
    return template.format(lsn >> 32, lsn & 0xFFFFFFFF)


def fsync_dir(path: str) -> None:
    if os.name != 'nt':
        fd = os.open(path, os.O_DIRECTORY)
        try:
            os.fsync(fd)
        except OSError as e:
            # Some filesystems don't like fsyncing directories and raise EINVAL. Ignoring it is usually safe.
            if e.errno != errno.EINVAL:
                raise
        finally:
            os.close(fd)
