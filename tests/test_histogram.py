"""
Tests for ServerMetrics latency histogram (v1.4.0).

Covers:
  - _percentile() linear interpolation accuracy
  - p50 / p75 / p90 / p95 / p99 / p999 return correct values
  - latency_histogram() dict shape and content
  - record_latency() feeds both windows
  - EMA window (_latency_window) unchanged -- still maxlen=10
  - Histogram window (_histogram_window) -- maxlen=1000
  - Empty window edge cases return 0.0
  - Single-sample edge case
  - health_report() includes latency_histogram per inner server
  - prometheus_metrics() includes p50, p95, p99 per server
"""

import statistics
import time
import unittest
from collections import deque

from huddle_cluster import HuddleCluster, Server, ServerMetrics, create_cluster


class TestPercentileAccuracy(unittest.TestCase):
    """_percentile() must match numpy.percentile(data, p, method='linear')."""

    def _metrics_with(self, samples: list) -> ServerMetrics:
        m = ServerMetrics()
        for v in samples:
            m.record_latency(v)
        return m

    def test_empty_returns_zero(self):
        m = ServerMetrics()
        self.assertEqual(m._percentile(50), 0.0)
        self.assertEqual(m._percentile(99), 0.0)

    def test_single_sample(self):
        m = self._metrics_with([42.0])
        self.assertAlmostEqual(m._percentile(0),   42.0)
        self.assertAlmostEqual(m._percentile(50),  42.0)
        self.assertAlmostEqual(m._percentile(100), 42.0)

    def test_two_samples_interpolation(self):
        # samples [10, 20]: p50 should be 15.0 (linear interpolation)
        m = self._metrics_with([10.0, 20.0])
        self.assertAlmostEqual(m._percentile(50), 15.0, places=6)

    def test_uniform_distribution(self):
        # [1, 2, 3, 4, 5] -- easy to verify manually
        m = self._metrics_with([1.0, 2.0, 3.0, 4.0, 5.0])
        # p0=1, p25=2, p50=3, p75=4, p100=5
        self.assertAlmostEqual(m._percentile(0),   1.0, places=6)
        self.assertAlmostEqual(m._percentile(25),  2.0, places=6)
        self.assertAlmostEqual(m._percentile(50),  3.0, places=6)
        self.assertAlmostEqual(m._percentile(75),  4.0, places=6)
        self.assertAlmostEqual(m._percentile(100), 5.0, places=6)

    def test_p95_with_twenty_samples(self):
        # 20 samples [1..20]: p95 = 19.25 with linear interpolation
        # idx = 0.95 * 19 = 18.05; lo=18 (val=19), hi=19 (val=20)
        # result = 19 + 0.05 * (20-19) = 19.05
        m = self._metrics_with(list(range(1, 21)))
        self.assertAlmostEqual(m._percentile(95), 19.05, places=6)

    def test_order_independent(self):
        # Percentile result must not depend on insertion order
        samples = [30.0, 10.0, 50.0, 20.0, 40.0]
        m1 = self._metrics_with(samples)
        m2 = self._metrics_with(list(reversed(samples)))
        self.assertAlmostEqual(m1._percentile(50), m2._percentile(50), places=9)
        self.assertAlmostEqual(m1._percentile(99), m2._percentile(99), places=9)


class TestPercentileMethods(unittest.TestCase):
    """Named percentile helpers delegate correctly to _percentile()."""

    def setUp(self):
        self.m = ServerMetrics()
        for v in range(1, 101):        # 100 samples: 1ms .. 100ms
            self.m.record_latency(float(v))

    def test_p50(self):
        # median of [1..100]: idx=0.50*99=49.5; lo=49(val=50), hi=50(val=51)
        # result = 50 + 0.5*(51-50) = 50.5
        self.assertAlmostEqual(self.m.p50_latency(), 50.5, places=6)

    def test_p75(self):
        # idx=0.75*99=74.25; lo=74(val=75), hi=75(val=76)
        # result = 75 + 0.25*(76-75) = 75.25
        self.assertAlmostEqual(self.m.p75_latency(), 75.25, places=6)

    def test_p90(self):
        # idx=0.90*99=89.1; lo=89(val=90), hi=90(val=91)
        # result = 90 + 0.1*(91-90) = 90.1
        self.assertAlmostEqual(self.m.p90_latency(), 90.1, places=6)

    def test_p95(self):
        # idx=0.95*99=94.05; lo=94(val=95), hi=95(val=96)
        # result = 95 + 0.05*(96-95) = 95.05
        self.assertAlmostEqual(self.m.p95_latency(), 95.05, places=6)

    def test_p99(self):
        # idx=0.99*99=98.01; lo=98(val=99), hi=99(val=100)
        # result = 99 + 0.01*(100-99) = 99.01
        self.assertAlmostEqual(self.m.p99_latency(), 99.01, places=6)

    def test_p999_approximation_few_samples(self):
        # With 100 samples p999 is an approximation.
        # Must be >= p99 and <= max sample.
        self.assertGreaterEqual(self.m.p999_latency(), self.m.p99_latency())
        self.assertLessEqual(self.m.p999_latency(), 100.0)

    def test_p999_exact_with_1000_samples(self):
        m = ServerMetrics()
        for v in range(1, 1001):
            m.record_latency(float(v))
        # idx=0.999*999=998.001; lo=998(val=999), hi=999(val=1000)
        # result = 999 + 0.001*(1000-999) = 999.001
        self.assertAlmostEqual(m.p999_latency(), 999.001, places=3)

    def test_monotonicity(self):
        # p50 <= p75 <= p90 <= p95 <= p99 <= p999
        m = self.m
        self.assertLessEqual(m.p50_latency(), m.p75_latency())
        self.assertLessEqual(m.p75_latency(), m.p90_latency())
        self.assertLessEqual(m.p90_latency(), m.p95_latency())
        self.assertLessEqual(m.p95_latency(), m.p99_latency())
        self.assertLessEqual(m.p99_latency(), m.p999_latency())

    def test_empty_all_zero(self):
        m = ServerMetrics()
        self.assertEqual(m.p50_latency(),  0.0)
        self.assertEqual(m.p75_latency(),  0.0)
        self.assertEqual(m.p90_latency(),  0.0)
        self.assertEqual(m.p95_latency(),  0.0)
        self.assertEqual(m.p99_latency(),  0.0)
        self.assertEqual(m.p999_latency(), 0.0)


class TestLatencyHistogramDict(unittest.TestCase):
    """latency_histogram() returns the expected dict shape."""

    def test_empty(self):
        h = ServerMetrics().latency_histogram()
        expected_keys = {"sample_count", "p50_ms", "p75_ms", "p90_ms",
                         "p95_ms", "p99_ms", "p999_ms"}
        self.assertEqual(set(h.keys()), expected_keys)
        self.assertEqual(h["sample_count"], 0)
        self.assertEqual(h["p50_ms"], 0.0)

    def test_values_rounded_to_3dp(self):
        m = ServerMetrics()
        for v in range(1, 101):
            m.record_latency(float(v))
        h = m.latency_histogram()
        for key in ("p50_ms", "p75_ms", "p90_ms", "p95_ms", "p99_ms", "p999_ms"):
            val = h[key]
            self.assertAlmostEqual(val, round(val, 3), places=9,
                                   msg=f"{key} not rounded to 3dp: {val}")

    def test_sample_count_tracks_window_size(self):
        m = ServerMetrics()
        for i in range(1, 6):
            m.record_latency(float(i))
            self.assertEqual(m.latency_histogram()["sample_count"], i)

    def test_sample_count_capped_at_1000(self):
        m = ServerMetrics()
        for i in range(1500):
            m.record_latency(float(i % 100))
        self.assertEqual(m.latency_histogram()["sample_count"], 1000)


class TestWindowSeparation(unittest.TestCase):
    """EMA window (maxlen=10) and histogram window (maxlen=1000) are independent."""

    def test_ema_window_still_maxlen_10(self):
        m = ServerMetrics()
        for i in range(50):
            m.record_latency(float(i))
        self.assertEqual(len(m._latency_window), 10)

    def test_histogram_window_maxlen_1000(self):
        m = ServerMetrics()
        for i in range(1200):
            m.record_latency(float(i % 100))
        self.assertEqual(len(m._histogram_window), 1000)

    def test_avg_response_ms_driven_by_ema_window(self):
        m = ServerMetrics()
        # Feed 10 samples of 100ms (fills EMA window)
        for _ in range(10):
            m.record_latency(100.0)
        # Now feed 1 sample of 0ms -- EMA window shifts, avg drops
        m.record_latency(0.0)
        # avg_response_ms uses _latency_window (last 10 samples)
        expected_avg = (9 * 100.0 + 0.0) / 10
        self.assertAlmostEqual(m.avg_response_ms, expected_avg, places=6)

    def test_histogram_independent_of_ema(self):
        m = ServerMetrics()
        # Feed 20 samples so EMA window has rolled over but histogram has all 20
        for i in range(20):
            m.record_latency(float(i))
        self.assertEqual(len(m._histogram_window), 20)
        # Histogram p50 should reflect all 20 samples, not just last 10
        expected_p50 = m._percentile(50)
        all_sorted = sorted(float(i) for i in range(20))
        # manual: idx = 0.5*19=9.5; lo=9(val=9), hi=10(val=10); result=9.5
        self.assertAlmostEqual(expected_p50, 9.5, places=6)


class TestHealthReportHistogram(unittest.TestCase):
    """health_report() must include latency_histogram per inner-ring server."""

    def setUp(self):
        self.cluster = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
        ])
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_histogram_present_in_inner_ring(self):
        report = self.cluster.health_report()
        for entry in report["inner_ring"]:
            self.assertIn("latency_histogram", entry,
                          f"latency_histogram missing for server {entry['id']}")

    def test_histogram_keys(self):
        report = self.cluster.health_report()
        expected = {"sample_count", "p50_ms", "p75_ms", "p90_ms",
                    "p95_ms", "p99_ms", "p999_ms"}
        for entry in report["inner_ring"]:
            self.assertEqual(set(entry["latency_histogram"].keys()), expected)

    def test_histogram_not_in_outer_ring(self):
        # Outer ring does not include histogram (keep payload small)
        report = self.cluster.health_report()
        for entry in report["outer_ring"]:
            self.assertNotIn("latency_histogram", entry)

    def test_histogram_updates_after_latency_recording(self):
        servers = self.cluster.inner_servers()
        s = servers[0]
        for i in range(50):
            self.cluster.record_latency(s, float(i + 1))

        report = self.cluster.health_report()
        inner_ids = {e["id"]: e for e in report["inner_ring"]}
        hist = inner_ids[s.id]["latency_histogram"]
        self.assertEqual(hist["sample_count"], 50)
        self.assertGreater(hist["p99_ms"], 0.0)
        self.assertGreater(hist["p95_ms"], 0.0)


class TestPrometheusHistogram(unittest.TestCase):
    """prometheus_metrics() must include p50, p95, p99 per server."""

    def setUp(self):
        self.cluster = create_cluster([("s1", "127.0.0.1", 8001)])
        self.cluster.start()
        s = self.cluster.inner_servers()[0]
        for i in range(1, 101):
            self.cluster.record_latency(s, float(i))

    def tearDown(self):
        self.cluster.stop()

    def test_p50_metric_present(self):
        output = self.cluster.prometheus_metrics()
        self.assertIn("huddle_server_p50_latency_ms", output)

    def test_p95_metric_present(self):
        output = self.cluster.prometheus_metrics()
        self.assertIn("huddle_server_p95_latency_ms", output)

    def test_p99_metric_present(self):
        output = self.cluster.prometheus_metrics()
        self.assertIn("huddle_server_p99_latency_ms", output)

    def test_server_label_in_histogram_metrics(self):
        output = self.cluster.prometheus_metrics()
        for metric in ("p50", "p95", "p99"):
            self.assertIn(f'server="s1"', output,
                          f'server label missing in {metric} line')

    def test_p50_value_nonzero_after_recording(self):
        output = self.cluster.prometheus_metrics()
        for line in output.splitlines():
            if line.startswith('huddle_server_p50_latency_ms{server="s1"}'):
                value = float(line.split("} ")[1])
                self.assertGreater(value, 0.0)
                break
        else:
            self.fail("p50 line for s1 not found in prometheus output")


if __name__ == "__main__":
    unittest.main()