"""
Tests for Admin REST API (v1.4.0).

Covers:
  - serve_admin() starts HTTP server, returns port
  - serve_admin() raises RuntimeError if called twice
  - stop_admin() shuts down the server cleanly
  - stop_admin() safe to call when not running
  - GET /admin/health returns health_report JSON
  - GET /admin/servers returns all server info
  - GET /admin/canary returns canary_status
  - GET /admin/alerts returns alert_history
  - POST /admin/evict/<id> evicts inner-ring server
  - POST /admin/evict/<id> returns 404 for unknown server
  - POST /admin/set_healthy/<id>?healthy=false marks unhealthy
  - POST /admin/set_healthy/<id>?healthy=true restores healthy
  - POST /admin/set_healthy/<id> returns 404 for unknown server
  - POST /admin/clear_affinity clears all sticky bindings
  - POST /admin/ramp/<id> starts traffic ramp
  - POST /admin/ramp/<id> returns 400 for invalid params
  - POST /admin/stop_ramp/<id> cancels ramp
  - Unknown GET/POST endpoint returns 404
  - Response Content-Type is application/json
  - Thread safety: concurrent admin requests
"""

import json
import threading
import time
import unittest
import urllib.request
import urllib.error

from huddle_cluster import create_cluster


def _get(port: int, path: str) -> tuple:
    """Returns (status_code, parsed_json_body)."""
    url = f"http://127.0.0.1:{port}{path}"
    try:
        with urllib.request.urlopen(url, timeout=3.0) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def _post(port: int, path: str) -> tuple:
    url = f"http://127.0.0.1:{port}{path}"
    req = urllib.request.Request(url, data=b"", method="POST")
    try:
        with urllib.request.urlopen(req, timeout=3.0) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


class TestServeAdmin(unittest.TestCase):

    def setUp(self):
        self.cluster = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
        ])
        self.cluster.start()
        self.port = self.cluster.serve_admin(port=0)  # OS picks free port

    def tearDown(self):
        self.cluster.stop_admin()
        self.cluster.stop()

    def test_returns_port(self):
        self.assertIsInstance(self.port, int)
        self.assertGreater(self.port, 0)

    def test_raises_if_called_twice(self):
        with self.assertRaises(RuntimeError):
            self.cluster.serve_admin(port=0)

    def test_stop_admin_safe_if_not_running(self):
        cluster = create_cluster([("s1", "127.0.0.1", 8001)])
        cluster.start()
        cluster.stop_admin()  # must not raise
        cluster.stop()

    def test_stop_admin_shuts_down(self):
        self.cluster.stop_admin()
        # After stop, _admin_server should be None
        self.assertIsNone(self.cluster._admin_server)


class TestGetEndpoints(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cluster = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
        ])
        cls.cluster.start()
        cls.port = cls.cluster.serve_admin(port=0)

    @classmethod
    def tearDownClass(cls):
        cls.cluster.stop_admin()
        cls.cluster.stop()

    def test_health_endpoint_status(self):
        status, _ = _get(self.port, "/admin/health")
        self.assertEqual(status, 200)

    def test_health_endpoint_has_inner_ring(self):
        _, body = _get(self.port, "/admin/health")
        self.assertIn("inner_ring", body)

    def test_servers_endpoint_status(self):
        status, _ = _get(self.port, "/admin/servers")
        self.assertEqual(status, 200)

    def test_servers_endpoint_returns_list(self):
        _, body = _get(self.port, "/admin/servers")
        self.assertIsInstance(body, list)
        self.assertGreater(len(body), 0)

    def test_servers_endpoint_fields(self):
        _, body = _get(self.port, "/admin/servers")
        entry = body[0]
        for key in ("id", "host", "port", "position", "temperature",
                    "weight", "is_healthy", "avg_latency_ms",
                    "error_rate", "rotation_count"):
            self.assertIn(key, entry, f"Missing field: {key}")

    def test_canary_endpoint(self):
        status, body = _get(self.port, "/admin/canary")
        self.assertEqual(status, 200)
        self.assertIsInstance(body, list)

    def test_alerts_endpoint(self):
        status, body = _get(self.port, "/admin/alerts")
        self.assertEqual(status, 200)
        self.assertIsInstance(body, list)

    def test_unknown_get_returns_404(self):
        status, _ = _get(self.port, "/admin/unknown_endpoint")
        self.assertEqual(status, 404)

    def test_content_type_json(self):
        url = f"http://127.0.0.1:{self.port}/admin/health"
        with urllib.request.urlopen(url, timeout=3.0) as resp:
            ct = resp.headers.get("Content-Type", "")
        self.assertIn("application/json", ct)


class TestPostEvict(unittest.TestCase):

    def setUp(self):
        self.cluster = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
        ])
        self.cluster.start()
        self.port = self.cluster.serve_admin(port=0)

    def tearDown(self):
        self.cluster.stop_admin()
        self.cluster.stop()

    def test_evict_known_server(self):
        status, body = _post(self.port, "/admin/evict/s1")
        self.assertEqual(status, 200)
        self.assertEqual(body["evicted"], "s1")

    def test_evict_moves_server_to_outer(self):
        _post(self.port, "/admin/evict/s1")
        outer_ids = {s.id for s in self.cluster.outer_servers()}
        self.assertIn("s1", outer_ids)

    def test_evict_unknown_returns_404(self):
        status, _ = _post(self.port, "/admin/evict/ghost_server")
        self.assertEqual(status, 404)

    def test_evict_outer_server_returns_404(self):
        # Evict s1 first, then try to evict again
        _post(self.port, "/admin/evict/s1")
        status, _ = _post(self.port, "/admin/evict/s1")
        self.assertEqual(status, 404)


class TestPostSetHealthy(unittest.TestCase):

    def setUp(self):
        self.cluster = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
        ])
        self.cluster.start()
        self.port = self.cluster.serve_admin(port=0)

    def tearDown(self):
        self.cluster.stop_admin()
        self.cluster.stop()

    def test_set_unhealthy(self):
        status, body = _post(self.port, "/admin/set_healthy/s1?healthy=false")
        self.assertEqual(status, 200)
        self.assertFalse(body["is_healthy"])

    def test_set_unhealthy_updates_server(self):
        _post(self.port, "/admin/set_healthy/s1?healthy=false")
        s = {sv.id: sv for sv in self.cluster.all_servers()}["s1"]
        self.assertFalse(s.metrics.is_healthy)

    def test_set_healthy_restores(self):
        _post(self.port, "/admin/set_healthy/s1?healthy=false")
        _post(self.port, "/admin/set_healthy/s1?healthy=true")
        s = {sv.id: sv for sv in self.cluster.all_servers()}["s1"]
        self.assertTrue(s.metrics.is_healthy)

    def test_default_healthy_is_true(self):
        status, body = _post(self.port, "/admin/set_healthy/s1")
        self.assertEqual(status, 200)
        self.assertTrue(body["is_healthy"])

    def test_unknown_server_returns_404(self):
        status, _ = _post(self.port, "/admin/set_healthy/ghost?healthy=false")
        self.assertEqual(status, 404)


class TestPostClearAffinity(unittest.TestCase):

    def setUp(self):
        self.cluster = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
        ])
        self.cluster.start()
        self.port = self.cluster.serve_admin(port=0)
        # Create some affinity bindings
        for i in range(5):
            self.cluster.get_server(affinity_key=f"user-{i}")

    def tearDown(self):
        self.cluster.stop_admin()
        self.cluster.stop()

    def test_clear_affinity_returns_count(self):
        status, body = _post(self.port, "/admin/clear_affinity")
        self.assertEqual(status, 200)
        self.assertIn("removed_bindings", body)
        self.assertGreaterEqual(body["removed_bindings"], 1)

    def test_clear_affinity_empties_map(self):
        _post(self.port, "/admin/clear_affinity")
        self.assertEqual(self.cluster.affinity_map_size(), 0)


class TestPostRamp(unittest.TestCase):

    def setUp(self):
        self.cluster = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
        ])
        self.cluster.start()
        self.port = self.cluster.serve_admin(port=0)

    def tearDown(self):
        self.cluster.stop_admin()
        self.cluster.stop()

    def test_start_ramp(self):
        status, body = _post(
            self.port,
            "/admin/ramp/s1?initial=0.1&target=1.0&ramp_sec=60"
        )
        self.assertEqual(status, 200)
        self.assertEqual(body["server_id"], "s1")
        self.assertAlmostEqual(body["start_weight"], 0.1, places=5)

    def test_ramp_registered(self):
        _post(self.port, "/admin/ramp/s1?initial=0.1&target=1.0&ramp_sec=60")
        self.assertIn("s1", self.cluster._ramps)

    def test_ramp_unknown_server_returns_400(self):
        status, _ = _post(
            self.port, "/admin/ramp/ghost?initial=0.1&target=1.0&ramp_sec=60"
        )
        self.assertEqual(status, 400)

    def test_stop_ramp(self):
        _post(self.port, "/admin/ramp/s1?initial=0.1&target=1.0&ramp_sec=60")
        status, body = _post(self.port, "/admin/stop_ramp/s1")
        self.assertEqual(status, 200)
        self.assertTrue(body["stopped"])
        self.assertNotIn("s1", self.cluster._ramps)

    def test_stop_ramp_nonexistent(self):
        status, body = _post(self.port, "/admin/stop_ramp/ghost")
        self.assertEqual(status, 200)
        self.assertFalse(body["stopped"])


class TestUnknownEndpoint(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cluster = create_cluster([("s1", "127.0.0.1", 8001)])
        cls.cluster.start()
        cls.port = cls.cluster.serve_admin(port=0)

    @classmethod
    def tearDownClass(cls):
        cls.cluster.stop_admin()
        cls.cluster.stop()

    def test_unknown_post_returns_404(self):
        status, _ = _post(self.port, "/admin/does_not_exist")
        self.assertEqual(status, 404)

    def test_root_path_returns_404(self):
        status, _ = _get(self.port, "/")
        self.assertEqual(status, 404)


class TestThreadSafety(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cluster = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
        ])
        cls.cluster.start()
        cls.port = cls.cluster.serve_admin(port=0)

    @classmethod
    def tearDownClass(cls):
        cls.cluster.stop_admin()
        cls.cluster.stop()

    def test_concurrent_admin_requests(self):
        errors = []

        def requester():
            try:
                for _ in range(10):
                    status, _ = _get(self.port, "/admin/health")
                    if status != 200:
                        errors.append(f"Expected 200, got {status}")
                    status, _ = _get(self.port, "/admin/servers")
                    if status != 200:
                        errors.append(f"Servers: expected 200, got {status}")
            except Exception as e:
                errors.append(str(e))

        threads = [threading.Thread(target=requester) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [], f"Errors: {errors}")


if __name__ == "__main__":
    unittest.main()