"""
Unit tests for HuddleCluster Adaptive Request Hedging & Speculative Execution (v4.19.0)
========================================================================================
Verifies:
1. Fast path: Primary finishes within delay, zero backup requests dispatched.
2. Speculative backup win: Primary stalls, coolest backup server responds quickly,
   backup result is returned and hedged_wins is incremented.
3. Hedging budget throttling: Safeguard prevents excessive speculative backups when
   exceeding the configured budget ratio.
4. Non-idempotent exclusion: allow_hedging=False disables hedging for write operations.
5. Error failover: Primary failure before delay immediately falls back / retries.
6. Telemetry exposition: health_report()["hedging"] and Prometheus metrics format.
"""

import time
import unittest

from huddle_cluster import HuddleCluster, Position, Server


class TestRequestHedging(unittest.TestCase):
    def setUp(self):
        self.cluster = HuddleCluster(
            heat_threshold=0.6,
            cool_threshold=0.3,
            min_inner_size=2,
            max_inner_size=3,
            hedging_enabled=True,
            hedging_delay_percentile=95.0,
            hedging_delay_floor_ms=20.0,
            hedging_budget_ratio=0.50,
        )
        self.s1 = Server("s1", "127.0.0.1", 8001)
        self.s2 = Server("s2", "127.0.0.1", 8002)
        self.s3 = Server("s3", "127.0.0.1", 8003)
        self.s1.position = Position.INNER
        self.s2.position = Position.INNER
        self.s3.position = Position.OUTER
        self.s1.temperature = 0.4
        self.s2.temperature = 0.2
        self.s3.temperature = 0.1

        self.cluster._inner_ring.append(self.s1)
        self.cluster._inner_ring.append(self.s2)
        self.cluster._outer_ring.append(self.s3)

    def tearDown(self):
        self.cluster.stop()

    def test_fast_path_no_speculative_backup(self):
        """When primary responds well within hedging delay, 0 backup requests are launched."""
        def fast_fn(server: Server):
            time.sleep(0.002)
            return f"ok_from_{server.id}"

        res = self.cluster.hedged_request(fast_fn, hedging_delay_ms=50.0)
        self.assertEqual(res, "ok_from_s1")

        status = self.cluster.hedging_status()
        self.assertEqual(status["total_requests"], 1)
        self.assertEqual(status["hedged_requests"], 0)
        self.assertEqual(status["hedged_wins"], 0)

    def test_speculative_backup_win(self):
        """When primary stalls beyond hedging delay, fastest coolest server wins."""
        def slow_primary_fast_backup(server: Server):
            if server.id == "s1":
                time.sleep(0.15)  # primary is slow (150ms)
                return f"slow_{server.id}"
            else:
                time.sleep(0.005)  # backup is fast (5ms)
                return f"fast_{server.id}"

        # Delay 20ms, timeout 500ms
        res = self.cluster.hedged_request(
            slow_primary_fast_backup,
            hedging_delay_ms=20.0,
            timeout_ms=500.0,
        )
        self.assertEqual(res, "fast_s2")

        status = self.cluster.hedging_status()
        self.assertEqual(status["total_requests"], 1)
        self.assertEqual(status["hedged_requests"], 1)
        self.assertEqual(status["hedged_wins"], 1)

    def test_budget_ratio_throttling(self):
        """When hedging budget ratio is exceeded, speculative requests are suppressed."""
        # Force low budget ratio
        self.cluster._hedging_budget_ratio = 0.01
        self.cluster._hedging_total_requests = 20
        self.cluster._hedging_hedged_requests = 10  # 50% >> 1%

        call_count = {"count": 0}

        def stall_fn(server: Server):
            call_count["count"] += 1
            time.sleep(0.04)
            return f"val_{server.id}"

        res = self.cluster.hedged_request(
            stall_fn,
            hedging_delay_ms=10.0,
            timeout_ms=300.0,
        )
        self.assertTrue(res.startswith("val_"))
        # Since budget was exceeded and total > 10, no backup request should have launched
        status = self.cluster.hedging_status()
        self.assertGreater(status["hedged_throttled"], 0)
        self.assertEqual(status["hedged_requests"], 10)  # unchanged

    def test_allow_hedging_disabled_for_non_idempotent(self):
        """When allow_hedging=False, request executes directly on primary without hedging."""
        called_servers = []

        def write_fn(server: Server):
            called_servers.append(server.id)
            time.sleep(0.03)
            return "write_done"

        res = self.cluster.hedged_request(
            write_fn,
            hedging_delay_ms=10.0,
            allow_hedging=False,
        )
        self.assertEqual(res, "write_done")
        self.assertEqual(len(called_servers), 1)

        status = self.cluster.hedging_status()
        self.assertEqual(status["hedged_requests"], 0)

    def test_primary_error_failover(self):
        """When primary raises an exception before hedging delay, failover to secondary succeeds."""
        def fail_primary(server: Server):
            if server.id == "s1":
                raise ValueError("primary connection reset")
            return f"recovered_by_{server.id}"

        res = self.cluster.hedged_request(
            fail_primary,
            hedging_delay_ms=50.0,
            timeout_ms=300.0,
        )
        self.assertEqual(res, "recovered_by_s2")

    def test_telemetry_and_prometheus_metrics(self):
        """Telemetry includes hedging metrics in health_report and prometheus_metrics."""
        report = self.cluster.health_report()
        self.assertIn("hedging", report)
        self.assertTrue(report["hedging"]["enabled"])
        self.assertEqual(report["hedging"]["delay_percentile"], 95.0)

        metrics = self.cluster.prometheus_metrics()
        self.assertIn("huddle_hedging_total_requests", metrics)
        self.assertIn("huddle_hedging_hedged_requests_total", metrics)
        self.assertIn("huddle_hedging_wins_total", metrics)
        self.assertIn("huddle_hedging_throttled_total", metrics)


if __name__ == "__main__":
    unittest.main()
