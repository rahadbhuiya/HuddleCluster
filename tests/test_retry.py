"""
Tests for request_with_retry() (v1.4.0).

Covers:
  - Success on first attempt -- no retry used
  - Retry on second server after first fails
  - RetryExhaustedError raised when all attempts fail
  - max_retries=0 means exactly one attempt
  - Non-retryable exception propagates immediately without using a retry slot
  - retry_on filter: only specified exception types trigger retry
  - affinity_key used on first attempt, abandoned on retry
  - Failed server's error_rate is penalised
  - Failed server marked unhealthy when error_rate > 0.5
  - Retry stats: total_retries, successful_retries, exhausted_retries
  - health_report() includes retry_stats
  - prometheus_metrics() includes retry counters
  - _get_server_excluding() skips already-tried servers
  - Thread safety: concurrent request_with_retry calls
  - RetryExhaustedError attributes: last_error, attempts, tried_server_ids
"""

import threading
import time
import unittest
from unittest.mock import MagicMock, call

from huddle_cluster import (
    HuddleCluster,
    RetryExhaustedError,
    Server,
    create_cluster,
)


class TestSuccessFirstAttempt(unittest.TestCase):
    """fn succeeds immediately -- no retry consumed."""

    def setUp(self):
        self.cluster = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
        ])
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_returns_fn_result(self):
        result = self.cluster.request_with_retry(lambda s: "ok")
        self.assertEqual(result, "ok")

    def test_fn_receives_server(self):
        received = []
        self.cluster.request_with_retry(lambda s: received.append(s))
        self.assertEqual(len(received), 1)
        self.assertIsInstance(received[0], Server)

    def test_no_retry_stats_on_success(self):
        self.cluster.request_with_retry(lambda s: 42)
        stats = self.cluster._retry_stats
        self.assertEqual(stats["total_retries"],      0)
        self.assertEqual(stats["successful_retries"], 0)
        self.assertEqual(stats["exhausted_retries"],  0)


class TestRetryOnDifferentServer(unittest.TestCase):
    """First server fails, second succeeds -- exactly one retry used."""

    def setUp(self):
        self.cluster = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
        ])
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_retries_on_different_server(self):
        called_ids = []

        def fn(server):
            called_ids.append(server.id)
            if len(called_ids) == 1:
                raise ConnectionError("s1 down")
            return "ok"

        result = self.cluster.request_with_retry(fn, max_retries=2)
        self.assertEqual(result, "ok")
        self.assertEqual(len(called_ids), 2)
        # Second attempt must use a different server
        self.assertNotEqual(called_ids[0], called_ids[1])

    def test_successful_retry_increments_stat(self):
        attempts = [0]

        def fn(server):
            attempts[0] += 1
            if attempts[0] == 1:
                raise ConnectionError("first fail")
            return "ok"

        self.cluster.request_with_retry(fn, max_retries=2)
        self.assertEqual(self.cluster._retry_stats["total_retries"],      1)
        self.assertEqual(self.cluster._retry_stats["successful_retries"], 1)
        self.assertEqual(self.cluster._retry_stats["exhausted_retries"],  0)


class TestRetryExhausted(unittest.TestCase):
    """All attempts fail -- RetryExhaustedError raised."""

    def setUp(self):
        self.cluster = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
            ("s3", "127.0.0.1", 8003),
        ])
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_raises_retry_exhausted_error(self):
        with self.assertRaises(RetryExhaustedError):
            self.cluster.request_with_retry(
                lambda s: (_ for _ in ()).throw(ConnectionError("down")),
                max_retries=2,
            )

    def test_exhausted_error_attributes(self):
        err = ConnectionError("all down")

        def always_fail(server):
            raise err

        try:
            self.cluster.request_with_retry(always_fail, max_retries=2)
            self.fail("Expected RetryExhaustedError")
        except RetryExhaustedError as e:
            self.assertIsInstance(e.last_error, ConnectionError)
            self.assertEqual(e.attempts, 3)          # 1 + 2 retries
            self.assertEqual(len(e.tried_server_ids), 3)

    def test_exhausted_stat_incremented(self):
        def always_fail(server):
            raise RuntimeError("fail")

        with self.assertRaises(RetryExhaustedError):
            self.cluster.request_with_retry(always_fail, max_retries=1)

        self.assertEqual(self.cluster._retry_stats["exhausted_retries"], 1)

    def test_total_retries_count(self):
        def always_fail(server):
            raise RuntimeError("fail")

        with self.assertRaises(RetryExhaustedError):
            self.cluster.request_with_retry(always_fail, max_retries=2)

        # 2 retries after the first attempt
        self.assertEqual(self.cluster._retry_stats["total_retries"], 2)


class TestMaxRetriesZero(unittest.TestCase):
    """max_retries=0 means exactly one attempt -- no retry."""

    def setUp(self):
        self.cluster = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
        ])
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_one_attempt_only(self):
        call_count = [0]

        def fn(server):
            call_count[0] += 1
            raise ConnectionError("fail")

        with self.assertRaises(RetryExhaustedError) as ctx:
            self.cluster.request_with_retry(
                fn, max_retries=0,
                retry_on=(ConnectionError,),
            )

        self.assertEqual(call_count[0], 1)
        self.assertEqual(ctx.exception.attempts, 1)


class TestNonRetryableException(unittest.TestCase):
    """Exceptions not in retry_on propagate immediately."""

    def setUp(self):
        self.cluster = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
        ])
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_non_retryable_raised_immediately(self):
        call_count = [0]

        def fn(server):
            call_count[0] += 1
            raise ValueError("bad input")  # not in retry_on

        with self.assertRaises(ValueError):
            self.cluster.request_with_retry(
                fn,
                max_retries=2,
                retry_on=(ConnectionError,),
            )

        # Must have called fn exactly once -- no retry
        self.assertEqual(call_count[0], 1)

    def test_non_retryable_does_not_increment_retry_stats(self):
        def fn(server):
            raise ValueError("bad")

        with self.assertRaises(ValueError):
            self.cluster.request_with_retry(
                fn, max_retries=2, retry_on=(ConnectionError,)
            )

        self.assertEqual(self.cluster._retry_stats["total_retries"], 0)
        self.assertEqual(self.cluster._retry_stats["exhausted_retries"], 0)


class TestRetryOnFilter(unittest.TestCase):
    """retry_on tuple controls which exceptions trigger retry."""

    def setUp(self):
        self.cluster = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
        ])
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_retries_only_on_specified_exceptions(self):
        attempts = [0]

        def fn(server):
            attempts[0] += 1
            if attempts[0] == 1:
                raise ConnectionError("retryable")
            return "ok"

        result = self.cluster.request_with_retry(
            fn,
            max_retries=2,
            retry_on=(ConnectionError, TimeoutError),
        )
        self.assertEqual(result, "ok")
        self.assertEqual(attempts[0], 2)

    def test_timeout_triggers_retry(self):
        attempts = [0]

        def fn(server):
            attempts[0] += 1
            if attempts[0] < 3:
                raise TimeoutError("timeout")
            return "ok"

        result = self.cluster.request_with_retry(
            fn,
            max_retries=3,
            retry_on=(ConnectionError, TimeoutError),
        )
        self.assertEqual(result, "ok")


class TestAffinityWithRetry(unittest.TestCase):
    """affinity_key used for first attempt, dropped on retry."""

    def setUp(self):
        self.cluster = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
        ])
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_affinity_on_first_attempt(self):
        # Pre-bind user-X to s1
        self.cluster._affinity_map["user-X"] = "s1"

        first_server = [None]
        attempts = [0]

        def fn(server):
            attempts[0] += 1
            if attempts[0] == 1:
                first_server[0] = server.id
                raise ConnectionError("s1 failed")
            return "ok"

        self.cluster.request_with_retry(
            fn, max_retries=2, affinity_key="user-X"
        )
        self.assertEqual(first_server[0], "s1")

    def test_retry_ignores_affinity_key(self):
        # Bind to s1 -- s1 will always fail
        self.cluster._affinity_map["user-Y"] = "s1"

        servers_tried = []

        def fn(server):
            servers_tried.append(server.id)
            if server.id == "s1":
                raise ConnectionError("s1 down")
            return "ok"

        result = self.cluster.request_with_retry(
            fn, max_retries=2, affinity_key="user-Y"
        )
        self.assertEqual(result, "ok")
        # First attempt must be s1 (affinity), second must be s2
        self.assertEqual(servers_tried[0], "s1")
        self.assertNotEqual(servers_tried[1], "s1")


class TestServerPenalty(unittest.TestCase):
    """Failed server's error_rate is penalised on retry."""

    def setUp(self):
        self.cluster = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
        ])
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_error_rate_incremented_on_failure(self):
        servers_map = {s.id: s for s in self.cluster.all_servers()}
        s1 = servers_map["s1"]
        initial_rate = s1.metrics.error_rate

        # Force s1 to be the first server tried and fail
        self.cluster._inner_ring = __import__("collections").deque(
            [s for s in self.cluster._inner_ring if s.id == "s1"]
            + [s for s in self.cluster._inner_ring if s.id != "s1"]
        )
        attempts = [0]

        def fn(server):
            attempts[0] += 1
            if server.id == "s1" and attempts[0] == 1:
                raise ConnectionError("fail")
            return "ok"

        self.cluster.request_with_retry(fn, max_retries=2)
        self.assertGreater(s1.metrics.error_rate, initial_rate)

    def test_server_marked_unhealthy_at_high_error_rate(self):
        servers_map = {s.id: s for s in self.cluster.all_servers()}
        target = servers_map["s1"]

        # Pre-set high error rate so one more failure tips it over 0.5
        target.metrics.error_rate = 0.46

        # Ensure s1 is first in ring
        import collections
        self.cluster._inner_ring = collections.deque(
            [s for s in self.cluster._inner_ring if s.id == "s1"]
            + [s for s in self.cluster._inner_ring if s.id != "s1"]
        )

        attempts = [0]

        def fn(server):
            attempts[0] += 1
            if server.id == "s1":
                raise ConnectionError("fail")
            return "ok"

        self.cluster.request_with_retry(fn, max_retries=2)
        self.assertFalse(target.metrics.is_healthy)


class TestGetServerExcluding(unittest.TestCase):
    """_get_server_excluding() never returns an excluded server when alternatives exist."""

    def setUp(self):
        self.cluster = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
            ("s3", "127.0.0.1", 8003),
        ])
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_excludes_tried_servers(self):
        excluded = {"s1"}
        server = self.cluster._get_server_excluding(excluded)
        self.assertNotIn(server.id, excluded)

    def test_returns_server_when_all_excluded(self):
        all_ids = {s.id for s in self.cluster.inner_servers()}
        # Even if all excluded, still returns something (fallback)
        server = self.cluster._get_server_excluding(all_ids)
        self.assertIsNotNone(server)


class TestHealthReportRetryStats(unittest.TestCase):
    """health_report() includes retry_stats dict."""

    def setUp(self):
        self.cluster = create_cluster([("s1", "127.0.0.1", 8001)])
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_retry_stats_present(self):
        report = self.cluster.health_report()
        self.assertIn("retry_stats", report)

    def test_retry_stats_keys(self):
        report = self.cluster.health_report()
        expected = {"total_retries", "successful_retries", "exhausted_retries"}
        self.assertEqual(set(report["retry_stats"].keys()), expected)

    def test_retry_stats_update_reflected(self):
        def always_fail(server):
            raise ConnectionError("fail")

        with self.assertRaises(RetryExhaustedError):
            self.cluster.request_with_retry(always_fail, max_retries=1)

        report = self.cluster.health_report()
        self.assertGreater(report["retry_stats"]["total_retries"], 0)
        self.assertGreater(report["retry_stats"]["exhausted_retries"], 0)


class TestPrometheusRetryMetrics(unittest.TestCase):
    """prometheus_metrics() includes retry counters."""

    def setUp(self):
        self.cluster = create_cluster([("s1", "127.0.0.1", 8001)])
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_retries_total_present(self):
        output = self.cluster.prometheus_metrics()
        self.assertIn("huddle_cluster_retries_total", output)

    def test_retries_exhausted_present(self):
        output = self.cluster.prometheus_metrics()
        self.assertIn("huddle_cluster_retries_exhausted_total", output)

    def test_retries_successful_present(self):
        output = self.cluster.prometheus_metrics()
        self.assertIn("huddle_cluster_retries_successful_total", output)


class TestThreadSafety(unittest.TestCase):
    """Concurrent request_with_retry calls must not corrupt retry_stats."""

    def setUp(self):
        self.cluster = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
            ("s3", "127.0.0.1", 8003),
        ])
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_concurrent_retries_no_crash(self):
        errors = []
        attempt_counts = []

        def fn(server):
            if len(attempt_counts) % 3 == 0:
                attempt_counts.append(1)
                raise ConnectionError("occasional fail")
            attempt_counts.append(1)
            return "ok"

        def worker():
            try:
                for _ in range(10):
                    try:
                        self.cluster.request_with_retry(fn, max_retries=1)
                    except RetryExhaustedError:
                        pass
            except Exception as e:
                errors.append(str(e))

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [], f"Thread errors: {errors}")

        # Stats must be internally consistent
        stats = self.cluster._retry_stats
        self.assertGreaterEqual(
            stats["total_retries"],
            stats["successful_retries"] + stats["exhausted_retries"],
        )


if __name__ == "__main__":
    unittest.main()