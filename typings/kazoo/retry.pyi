from kazoo.exceptions import KazooException
class RetryFailedError(KazooException):
    ...
class KazooRetry:
    deadline: float
    def __init__(self, max_tries=..., delay=..., backoff=..., max_jitter=..., max_delay=..., ignore_expire=..., sleep_func=..., deadline=..., interrupt=...) -> None:
        ...
