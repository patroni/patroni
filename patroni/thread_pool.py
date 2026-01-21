from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from typing import Any


class PatroniThreadPoolExecutor(ThreadPoolExecutor):

    def __init__(self, max_workers: int, **kwargs: Any) -> None:
        super(PatroniThreadPoolExecutor, self).__init__(max_workers=max_workers, **kwargs)

        # warmup
        barrier = Barrier(max_workers + 1)
        for _ in range(max_workers):
            self.submit(barrier.wait)
        barrier.wait()


__executor: PatroniThreadPoolExecutor


def configure_global_pool(max_workers: int) -> None:
    global __executor
    __executor = PatroniThreadPoolExecutor(max_workers=max_workers, thread_name_prefix='Global Pool')


def get_executor() -> ThreadPoolExecutor:
    global __executor  # noqa: F824
    return __executor
