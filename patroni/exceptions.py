"""Implement high-level Patroni exceptions.

More specific exceptions can be found in other modules, as subclasses of any exception defined in this module.
"""
from typing import Any


class PatroniException(Exception):
    """Parent class for all kind of Patroni exceptions.

    :ivar value: description of the exception.
    """

    def __init__(self, value: Any) -> None:
        """Create a new instance of :class:`PatroniException` with the given description.

        :param value: description of the exception.
        """
        self.value = value


class PatroniFatalException(PatroniException):
    """Catastrophic exception that prevents Patroni from performing its job."""

    pass


class PostgresException(PatroniException):
    """Any exception related with Postgres management."""

    pass


class DCSError(PatroniException):
    """Parent class for all kind of DCS related exceptions."""

    pass


class PostgresConnectionException(PostgresException):
    """Any problem faced while connecting to a Postgres instance."""

    pass


class WatchdogError(PatroniException):
    """Any problem faced while managing a watchdog device."""

    pass


class ConfigParseError(PatroniException):
    """Any issue identified while loading or validating the Patroni configuration."""

    pass


class PatroniAssertionError(PatroniException):
    """Any issue related to type/value validation."""

    pass
