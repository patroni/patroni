class EtcdError(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class CurrentLeaderError(EtcdError):
    pass


class HealthiestMemberError(EtcdError):
    pass
