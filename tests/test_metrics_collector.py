import time
import unittest

from unittest.mock import Mock, patch

import patroni.metrics_collector as mc


@mc.record_duration('dummy')
def dummy_function():
    pass


class TestMetricsCollector(unittest.TestCase):

    def setUp(self):
        mc._history.clear()

    def test_record_loop_duration(self):
        timestamps = [i for i in range(0, 6, 1)]
        with patch('time.monotonic', Mock(side_effect=timestamps)):
            for _ in range(len(timestamps) // 2):
                dummy_function()

            self.assertEqual(len(mc._history['dummy']), 3)

    def test__refresh_history(self):
        # default 3600
        start_time = time.monotonic()
        recent_time = start_time + 1800  # +30 mins
        mc._history['dummy'].append((start_time, 0.1))
        mc._history['dummy'].append((recent_time, 0.2))
        with patch('time.monotonic', Mock(side_effect=[start_time + 3700, start_time + 3701])):
            dummy_function()
        self.assertEqual(len(mc._history['dummy']), 2)
        self.assertEqual([entry[0] for entry in mc._history['dummy']], [recent_time, start_time + 3701])
        self.assertEqual(list(mc._history['dummy']), [(recent_time, 0.2), (start_time + 3701, 1)])

        # custom 900
        mc.reload_config(900)
        with patch('time.monotonic', Mock(side_effect=[start_time + 3702, start_time + 3704])):
            dummy_function()
        self.assertEqual(len(mc._history['dummy']), 2)
        self.assertEqual([entry[0] for entry in mc._history['dummy']], [start_time + 3701, start_time + 3704])
        self.assertEqual(list(mc._history['dummy']), [(start_time + 3701, 1), (start_time + 3704, 2)])

    def test_get_loop_duration_stats(self):
        avg, p99 = mc.get_loop_duration_stats()
        self.assertEqual(avg, 0.0)
        self.assertEqual(p99, 0.0)

        start_time = time.monotonic()
        mc._history['loop'].append((start_time - 1800, 0.2))

        avg, p99 = mc.get_loop_duration_stats()
        self.assertEqual(avg, 0.2)
        self.assertEqual(p99, 0.2)

        mc._history['loop'].append((start_time - 90, 0.1))

        avg, p99 = mc.get_loop_duration_stats()
        self.assertEqual(avg, 0.15)
        self.assertEqual(p99, 0.199)
