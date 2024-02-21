"""Define general variables and functions for :mod:`patroni`.

:var PATRONI_ENV_PREFIX: prefix for Patroni related configuration environment variables.
:var KUBERNETES_ENV_PREFIX: prefix for Kubernetes related configuration environment variables.
:var MIN_PSYCOPG2: minimum version of :mod:`psycopg2` required by Patroni to work.
:var MIN_PSYCOPG3: minimum version of :mod:`psycopg` required by Patroni to work.
"""
from typing import Iterator, Tuple

PATRONI_ENV_PREFIX = 'PATRONI_'
KUBERNETES_ENV_PREFIX = 'KUBERNETES_'
MIN_PSYCOPG2 = (2, 5, 4)
MIN_PSYCOPG3 = (3, 0, 0)


def parse_version(version: str) -> Tuple[int, ...]:
    """Convert *version* from human-readable format to tuple of integers.

    .. note::
        Designed for easy comparison of software versions in Python.

    :param version: human-readable software version, e.g. ``2.5.4.dev1 (dt dec pq3 ext lo64)``.

    :returns: tuple of *version* parts, each part as an integer.

    :Example:

        >>> parse_version('2.5.4.dev1 (dt dec pq3 ext lo64)')
        (2, 5, 4)
    """
    def _parse_version(version: str) -> Iterator[int]:
        """Yield each part of a human-readable version string as an integer.

        :param version: human-readable software version, e.g. ``2.5.4.dev1``.

        :yields: each part of *version* as an integer.

        :Example:

            >>> tuple(_parse_version('2.5.4.dev1'))
            (2, 5, 4)
        """
        for e in version.split('.'):
            try:
                yield int(e)
            except ValueError:
                break
    return tuple(_parse_version(version.split(' ')[0]))
