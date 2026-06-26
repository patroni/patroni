import sys
import threading
import time
import types

from collections import defaultdict, deque
from typing import Any, Callable, Dict, List, Tuple


def __getattr__(mod: types.ModuleType, name: str) -> Any:
    """This function exists just to make pyright happy.

    Without it pyright complains about access to unknown members of global_config module.
    """
    return getattr(sys.modules[__name__], name)  # pragma: no cover


class MetricsCollector(types.ModuleType):
    """A class for metrics collection. Provides convenient methods to record and retrieve metric statistics."""

    __file__ = __file__  # just to make unittest and pytest happy

    def __init__(self):
        """Initialize :class:`MetricsCollector` object."""
        super().__init__(__name__)

        self._metrics_collector_lock = threading.Lock()
        self.stats_expiry_seconds = 3600

        self._history: Dict[str, deque[Tuple[float, float]]] = defaultdict(deque)

    def refresh_history(self) -> None:
        """Clean up old entries from the history of all metrics."""
        current_time = time.monotonic()
        with self._metrics_collector_lock:
            for name in self._history:
                self._refresh_history(name, current_time)

    def _refresh_history(self, name: str, current_time: float) -> None:
        """Clean up old entries from the history of a metric.

        :param name: The name of the metric.
        :param current_time: The current time to compare against entry timestamps.
        """
        window_start = current_time - self.stats_expiry_seconds
        while self._history[name] and self._history[name][0][0] < window_start:
            self._history[name].popleft()

    def record_duration(self, name: str) -> Any:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                start = time.monotonic()
                try:
                    return func(*args, **kwargs)
                finally:
                    end = time.monotonic()
                    with self._metrics_collector_lock:
                        self._history[name].append((end, end - start))
                        self._refresh_history(name, end)
            return wrapper
        return decorator

    @staticmethod
    def calculate_metrics_percentile(data: List[float], percentile: float) -> float:
        """Calculate percentile of a list of metric values.

        :param data: list of metric values.
        :param percentile: percentile to calculate (0-100).

        :returns: the percentile value, or 0 if data is empty.
        """
        data_len = len(data)
        if data_len == 1:
            return round(data[0], 4)

        sorted_data = sorted(data)
        index = (percentile / 100.0) * (data_len - 1)
        lower_index = int(index)
        upper_index = min(lower_index + 1, data_len - 1)
        fraction = index - lower_index
        result = sorted_data[lower_index] * (1 - fraction) + sorted_data[upper_index] * fraction
        return round(result, 4)

    def get_duration_stats(self, history: deque[Tuple[float, float]]) -> Tuple[float, float]:
        """Get statistics of a metric duration.

        :param history: The deque containing the metric history.

        :returns: a tuple containing the average duration and 99th percentile of the metric.
        """
        average_duration = percentile_99 = 0.0
        with self._metrics_collector_lock:
            if not history:
                return average_duration, percentile_99

            durations = [entry[1] for entry in history]

        average_duration = round(sum(durations) / len(durations), 4)
        percentile_99 = self.calculate_metrics_percentile(durations, 99)

        return average_duration, percentile_99

    def get_loop_duration_stats(self) -> Tuple[float, float]:
        """Get statistics of heartbeat loop iterations.
        Executed in the API.

        :returns: a tuple containing the average duration and 99th percentile of loop iterations.
        """
        return self.get_duration_stats(self._history['loop'])

    def reload_config(self, value: int) -> None:
        """Reload metrics collector configuration.

        :param value: value of the metrics_collector_retention parameter.
        """
        self.stats_expiry_seconds = value


sys.modules[__name__] = MetricsCollector()
