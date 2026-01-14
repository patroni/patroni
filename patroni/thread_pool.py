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
