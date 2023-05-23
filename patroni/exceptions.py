from typing import Any


class PatroniException(Exception):

    """Parent class for all kind of exceptions related to selected distributed configuration store"""

    def __init__(self, value: Any) -> None:
        self.value = value


class PatroniFatalException(PatroniException):
    pass


class PostgresException(PatroniException):
    pass


class DCSError(PatroniException):
    pass


class PostgresConnectionException(PostgresException):
    pass


class WatchdogError(PatroniException):
    pass


class ConfigParseError(PatroniException):
    pass
