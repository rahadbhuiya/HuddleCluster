"""
Unit tests for HuddleCluster Proactive Synthetic Canary Prober (v4.18.0)
=======================================================================
Verifies active health probing for resting outer-ring nodes, thermal cooling,
autonomous promotion to inner ring, error handling, and Prometheus telemetry.
"""

import http.server
import threading
import time
import unittest

from huddle_cluster import HuddleCluster, Position, Server


class MockHealthServer:
    """Lightweight test HTTP server responding to canary probe requests."""

    def __init__(self, host: str = "127.0.0.1", port: int = 0):
        self.host = host
        self.port = port
        self.response_status = 200
        self.delay_sec = 0.0
        self.requests_received = 0
        self._server = None
        self._thread = None

    def start(self):
        handler_self = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                handler_self.requests_received += 1
                if handler_self.delay_sec > 0:
                    time.sleep(handler_self.delay_sec)
                self.send_response(handler_self.response_status)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"status": "ok"}')

            def log_message(self, *args):
                # Suppress console logging during tests
                pass

        self._server = http.server.HTTPServer((self.host, self.port), Handler)
        self.port = self._server.server_port
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self):
        if self._server:
            self._server.shutdown()
            self._server.server_close()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)


class TestCanaryProber(unittest.TestCase):
    def setUp(self):
        self.mock_server = MockHealthServer()
        self.mock_server.start()

    def tearDown(self):
        self.mock_server.stop()

    def test_canary_prober_disabled_by_default(self):
        cluster = HuddleCluster(canary_probe_path=None)
        status = cluster.canary_probe_status()
        self.assertFalse(status["enabled"])
        self.assertIsNone(status["path"])

        # Manual probe when disabled returns early
        res = cluster.canary_probe_now()
        self.assertFalse(res["enabled"])
        self.assertEqual(res["probed"], 0)

    def test_canary_prober_initialization_params(self):
        cluster = HuddleCluster(
            canary_probe_path="/health",
            canary_probe_interval_sec=5.0,
            canary_probe_timeout_sec=1.5,
            canary_probe_pings_per_cycle=4,
        )
        status = cluster.canary_probe_status()
        self.assertTrue(status["enabled"])
        self.assertEqual(status["path"], "/health")
        self.assertEqual(status["interval_sec"], 5.0)
        self.assertEqual(status["timeout_sec"], 1.5)
        self.assertEqual(status["pings_per_cycle"], 4)
        self.assertEqual(status["probes_total"], 0)

    def test_canary_prober_recovers_outer_node(self):
        cluster = HuddleCluster(
            heat_threshold=0.55,
            cool_threshold=0.35,
            min_inner_size=2,
            max_inner_size=4,
            rotation_cooldown_sec=0.0,
            min_outer_dwell_sec=0.0,
            ema_alpha=0.6,
            canary_probe_path="/health",
            canary_probe_timeout_sec=1.0,
            canary_probe_pings_per_cycle=3,
        )

        s1 = Server(id="s1", host="127.0.0.1", port=self.mock_server.port)
        s2 = Server(id="s2", host="127.0.0.1", port=self.mock_server.port)
        s3 = Server(id="s3", host="127.0.0.1", port=self.mock_server.port)

        # Inner ring healthy servers
        cluster.add_server(s1, force_inner=True)
        cluster.add_server(s2, force_inner=True)
        cluster.record_latency(s1, 10.0)
        cluster.record_latency(s2, 10.0)

        # Outer ring degraded server
        cluster.add_server(s3, force_inner=False)
        self.assertEqual(s3.position, Position.OUTER)

        # Simulate s3 having high initial temperature and high latency
        for _ in range(5):
            s3.metrics.record_latency(350.0)
        s3.temperature = 0.85

        self.assertFalse(s3.is_cooled(cluster.cool_threshold))

        # Run manual canary probe cycle against mock server (responds in ~1ms)
        probe_res = cluster.canary_probe_now()
        self.assertTrue(probe_res["enabled"])
        self.assertEqual(probe_res["probed"], 1)

        # Multiple pings populate window with healthy latency, cooling s3 down
        for _ in range(3):
            cluster.canary_probe_now()

        # Check that s3 has cooled below cool_threshold
        self.assertTrue(s3.is_cooled(cluster.cool_threshold))

        # Perform rotation step: cooled outer node s3 should be promoted to inner ring
        rotated = cluster.rotate()
        self.assertTrue(rotated)
        self.assertEqual(s3.position, Position.INNER)
        self.assertIn(s3, cluster.inner_servers())

    def test_canary_prober_error_handling(self):
        # Point to a dead/unused port
        dead_port = 59999
        cluster = HuddleCluster(
            canary_probe_path="/health",
            canary_probe_timeout_sec=0.3,
            request_timeout_ms=300.0,
        )

        s_bad = Server(id="s-bad", host="127.0.0.1", port=dead_port)
        cluster.add_server(s_bad, force_inner=False)

        res = cluster.canary_probe_now()
        self.assertTrue(res["enabled"])
        self.assertEqual(res["probed"], 1)
        self.assertFalse(res["results"][0]["success"])

        status = cluster.canary_probe_status()
        self.assertGreaterEqual(status["probes_failed"], 1)
        self.assertGreater(s_bad.metrics.error_rate, 0.0)

    def test_canary_prober_background_thread_lifecycle(self):
        cluster = HuddleCluster(
            canary_probe_path="/health",
            canary_probe_interval_sec=0.2,
            canary_probe_timeout_sec=0.5,
        )

        s = Server(id="srv-bg", host="127.0.0.1", port=self.mock_server.port)
        cluster.add_server(s, force_inner=False)

        cluster.start(rotation_interval_sec=0.5)
        self.assertTrue(cluster._canary_probe_thread.is_alive())

        # Wait briefly for background canary prober to fire at least once
        time.sleep(0.6)

        status = cluster.canary_probe_status()
        self.assertGreater(status["probes_total"], 0)

        # Stop cluster and confirm thread terminates
        cluster.stop(timeout=2.0)
        self.assertFalse(cluster._canary_probe_thread.is_alive())

    def test_health_report_and_prometheus_metrics(self):
        cluster = HuddleCluster(
            canary_probe_path="/canary",
            canary_probe_interval_sec=2.0,
            canary_probe_timeout_sec=1.0,
        )
        s = Server(id="s-diag", host="127.0.0.1", port=self.mock_server.port)
        cluster.add_server(s, force_inner=False)

        cluster.canary_probe_now()

        # Check health report
        report = cluster.health_report()
        self.assertIn("canary_prober", report)
        self.assertTrue(report["canary_prober"]["enabled"])
        self.assertEqual(report["canary_prober"]["path"], "/canary")
        self.assertGreater(report["canary_prober"]["probes_total"], 0)

        # Check Prometheus metrics
        prom = cluster.prometheus_metrics()
        self.assertIn("huddle_canary_probes_total", prom)
        self.assertIn("huddle_canary_last_probe_ms", prom)


if __name__ == "__main__":
    unittest.main()
