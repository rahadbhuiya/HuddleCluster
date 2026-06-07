"""
Tests for built-in HTTP health checker (v1.4.0).
"""

import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

from huddle_cluster import create_cluster


class _HealthHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        if self.server.healthy:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')
        else:
            self.send_response(503)
            self.end_headers()
            self.wfile.write(b'{"status":"down"}')


class _ReuseServer(HTTPServer):
    allow_reuse_address = True


def _start_health_server(port: int, healthy: bool = True) -> _ReuseServer:
    server = _ReuseServer(("127.0.0.1", port), _HealthHandler)
    server.healthy = healthy
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    # Wait briefly until the server is accepting connections
    import socket as _sock
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        try:
            s = _sock.create_connection(("127.0.0.1", port), timeout=0.1)
            s.close()
            break
        except OSError:
            time.sleep(0.05)
    return server


def _stop_server(server: _ReuseServer) -> None:
    server.shutdown()
    server.server_close()




class TestHealthCheckerDisabled(unittest.TestCase):

    def setUp(self):
        self.cluster = create_cluster([("s1", "127.0.0.1", 18001)])
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_no_thread_when_path_not_set(self):
        self.assertIsNone(self.cluster._health_check_thread)

    def test_health_check_status_empty_when_disabled(self):
        self.assertEqual(self.cluster.health_check_status(), [])


class TestHealthCheckerEnabled(unittest.TestCase):
    """Uses setUpClass so the health server is created once per class."""

    @classmethod
    def setUpClass(cls):
        cls.hs = _start_health_server(18100, healthy=True)

    @classmethod
    def tearDownClass(cls):
        _stop_server(cls.hs)

    def setUp(self):
        self.hs.healthy = True
        self.cluster = create_cluster(
            [("s1", "127.0.0.1", 18100)],
            health_check_path="/health",
            health_check_interval_sec=60.0,   # manual-only; no background firing
            health_check_timeout_sec=1.0,
            health_check_failures=2,
        )
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()
        # Reset failure counts for next test
        self.cluster._health_fail_counts.clear()

    def test_thread_started(self):
        self.assertIsNotNone(self.cluster._health_check_thread)
        self.assertTrue(self.cluster._health_check_thread.is_alive())

    def test_healthy_server_stays_healthy(self):
        self.hs.healthy = True
        self.cluster._run_health_checks()
        s = self.cluster.all_servers()[0]
        self.assertTrue(s.metrics.is_healthy)

    def test_single_failure_not_enough(self):
        self.hs.healthy = False
        self.cluster._run_health_checks()
        s = self.cluster.all_servers()[0]
        self.assertTrue(s.metrics.is_healthy,
                        "One failure must not mark server unhealthy (threshold=2)")

    def test_threshold_failures_marks_unhealthy(self):
        self.hs.healthy = False
        self.cluster._run_health_checks()
        self.cluster._run_health_checks()
        s = self.cluster.all_servers()[0]
        self.assertFalse(s.metrics.is_healthy,
                         "Server must be unhealthy after 2 consecutive failures")

    def test_recovery_restores_healthy(self):
        self.hs.healthy = False
        self.cluster._run_health_checks()
        self.cluster._run_health_checks()
        s = self.cluster.all_servers()[0]
        self.assertFalse(s.metrics.is_healthy)

        self.hs.healthy = True
        self.cluster._run_health_checks()
        self.assertTrue(s.metrics.is_healthy)

    def test_failure_counter_resets_on_success(self):
        self.hs.healthy = False
        self.cluster._run_health_checks()
        self.assertEqual(self.cluster._health_fail_counts.get("s1", 0), 1)

        self.hs.healthy = True
        self.cluster._run_health_checks()
        self.assertEqual(self.cluster._health_fail_counts.get("s1", 0), 0)

    def test_failure_counter_increments(self):
        self.hs.healthy = False
        self.cluster._run_health_checks()
        self.assertEqual(self.cluster._health_fail_counts.get("s1", 0), 1)
        self.cluster._run_health_checks()
        self.assertEqual(self.cluster._health_fail_counts.get("s1", 0), 2)

    def test_health_check_url_in_status(self):
        status = self.cluster.health_check_status()
        self.assertEqual(len(status), 1)
        self.assertEqual(status[0]["health_check_url"],
                         "http://127.0.0.1:18100/health")

    def test_status_shows_healthy(self):
        self.hs.healthy = True
        self.cluster._run_health_checks()
        self.assertTrue(self.cluster.health_check_status()[0]["is_healthy"])

    def test_status_shows_unhealthy(self):
        self.hs.healthy = False
        self.cluster._run_health_checks()
        self.cluster._run_health_checks()
        self.assertFalse(self.cluster.health_check_status()[0]["is_healthy"])

    def test_status_shows_consecutive_failures(self):
        self.hs.healthy = False
        self.cluster._run_health_checks()
        self.assertEqual(
            self.cluster.health_check_status()[0]["consecutive_failures"], 1
        )


class TestHealthCheckerTimeout(unittest.TestCase):

    def setUp(self):
        self.cluster = create_cluster(
            [("s1", "127.0.0.1", 19999)],
            health_check_path="/health",
            health_check_interval_sec=60.0,
            health_check_timeout_sec=0.2,
            health_check_failures=1,
        )
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_unreachable_counts_as_failure(self):
        self.cluster._run_health_checks()
        self.assertEqual(self.cluster._health_fail_counts.get("s1", 0), 1)

    def test_unreachable_marks_unhealthy_at_threshold(self):
        self.cluster._run_health_checks()
        s = self.cluster.all_servers()[0]
        self.assertFalse(s.metrics.is_healthy)


class TestHealthCheckerAlerts(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.hs = _start_health_server(18200, healthy=True)

    @classmethod
    def tearDownClass(cls):
        _stop_server(cls.hs)

    def setUp(self):
        self.hs.healthy = True
        self.cluster = create_cluster(
            [("s1", "127.0.0.1", 18200)],
            health_check_path="/health",
            health_check_interval_sec=60.0,
            health_check_timeout_sec=1.0,
            health_check_failures=2,
            alert_webhooks=["http://example.com/hook"],
            alert_on={"circuit_breaker", "health_recovered"},
        )
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_circuit_breaker_alert_on_failure(self):
        self.hs.healthy = False
        self.cluster._run_health_checks()
        self.cluster._run_health_checks()
        alerts = [a for a in self.cluster._alert_history
                  if a.event == "circuit_breaker"]
        self.assertTrue(len(alerts) >= 1)
        self.assertEqual(alerts[-1].data.get("reason"), "health_check_failed")

    def test_recovery_alert_on_restore(self):
        self.hs.healthy = False
        self.cluster._run_health_checks()
        self.cluster._run_health_checks()
        self.hs.healthy = True
        self.cluster._run_health_checks()
        alerts = [a for a in self.cluster._alert_history
                  if a.event == "health_recovered"]
        self.assertTrue(len(alerts) >= 1)


class TestHealthReportIntegration(unittest.TestCase):

    def setUp(self):
        self.cluster = create_cluster(
            [("s1", "127.0.0.1", 18300)],
            health_check_path="/health",
            health_check_interval_sec=60.0,
            health_check_timeout_sec=1.0,
            health_check_failures=3,
        )
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_health_checker_section_present(self):
        self.assertIn("health_checker", self.cluster.health_report())

    def test_health_checker_enabled_true(self):
        self.assertTrue(self.cluster.health_report()["health_checker"]["enabled"])

    def test_health_checker_path_correct(self):
        self.assertEqual(
            self.cluster.health_report()["health_checker"]["path"], "/health"
        )

    def test_health_checker_enabled_false_when_not_configured(self):
        cluster = create_cluster([("s1", "127.0.0.1", 18301)])
        cluster.start()
        try:
            self.assertFalse(cluster.health_report()["health_checker"]["enabled"])
        finally:
            cluster.stop()

    def test_health_checker_servers_in_report(self):
        self.assertIn("servers", self.cluster.health_report()["health_checker"])


class TestMultipleServers(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.hs1 = _start_health_server(18400, healthy=True)
        cls.hs2 = _start_health_server(18401, healthy=True)

    @classmethod
    def tearDownClass(cls):
        _stop_server(cls.hs1)
        _stop_server(cls.hs2)

    def setUp(self):
        self.hs1.healthy = True
        self.hs2.healthy = True
        self.cluster = create_cluster(
            [
                ("s1", "127.0.0.1", 18400),
                ("s2", "127.0.0.1", 18401),
            ],
            health_check_path="/health",
            health_check_interval_sec=60.0,
            health_check_timeout_sec=1.0,
            health_check_failures=2,
        )
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_only_failing_server_marked_unhealthy(self):
        self.hs2.healthy = False
        self.cluster._run_health_checks()
        self.cluster._run_health_checks()
        servers = {s.id: s for s in self.cluster.all_servers()}
        self.assertTrue(servers["s1"].metrics.is_healthy)
        self.assertFalse(servers["s2"].metrics.is_healthy)

    def test_failure_counters_independent(self):
        self.hs1.healthy = False
        self.hs2.healthy = True
        self.cluster._run_health_checks()
        self.assertEqual(self.cluster._health_fail_counts.get("s1", 0), 1)
        self.assertEqual(self.cluster._health_fail_counts.get("s2", 0), 0)


class TestThreadSafety(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.hs = _start_health_server(18500, healthy=True)

    @classmethod
    def tearDownClass(cls):
        _stop_server(cls.hs)

    def setUp(self):
        self.cluster = create_cluster(
            [("s1", "127.0.0.1", 18500)],
            health_check_path="/health",
            health_check_interval_sec=60.0,
            health_check_timeout_sec=1.0,
            health_check_failures=3,
        )
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_concurrent_health_checks_no_crash(self):
        errors = []

        def flipper():
            for i in range(10):
                self.hs.healthy = (i % 2 == 0)
                try:
                    self.cluster._run_health_checks()
                except Exception as e:
                    errors.append(str(e))

        threads = [threading.Thread(target=flipper) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [], f"Thread errors: {errors}")


if __name__ == "__main__":
    unittest.main()