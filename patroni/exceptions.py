class PatroniException(Exception):

    """Parent class for all kind of exceptions related to selected distributed configuration store"""

    def __init__(self, value):
        self.value = value

    def __str__(self):
        """
        >>> str(DCSError('foo'))
        "'foo'"
        """
        return repr(self.value)


class PostgresException(PatroniException):
    pass


class DCSError(PatroniException):
    pass
