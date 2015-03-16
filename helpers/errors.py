class CurrentLeaderError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

class HealthiestMemberError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)
