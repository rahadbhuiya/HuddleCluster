"""
Tests for Real-time Web Dashboard (v1.4.0).
"""

import json
import threading
import time
import unittest
import urllib.request

from huddle_cluster import create_cluster


def _get(port, path, timeout=3.0):
    url = f"http://127.0.0.1:{port}{path}"
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return resp.status, resp.read(), resp.headers


class TestDashboardLifecycle(unittest.TestCase):

    def setUp(self):
        self.cluster = create_cluster([("s1", "127.0.0.1", 8001)])
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop_dashboard()
        self.cluster.stop()

    def test_returns_port(self):
        port = self.cluster.serve_dashboard(port=0)
        self.assertIsInstance(port, int)
        self.assertGreater(port, 0)

    def test_raises_if_called_twice(self):
        self.cluster.serve_dashboard(port=0)
        with self.assertRaises(RuntimeError):
            self.cluster.serve_dashboard(port=0)

    def test_stop_safe_when_not_running(self):
        self.cluster.stop_dashboard()  # must not raise

    def test_stop_shuts_down(self):
        self.cluster.serve_dashboard(port=0)
        self.cluster.stop_dashboard()
        self.assertIsNone(getattr(self.cluster, "_dashboard_server", None))


class TestDashboardEndpoints(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cluster = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
        ])
        cls.cluster.start()
        cls.port = cls.cluster.serve_dashboard(port=0)

    @classmethod
    def tearDownClass(cls):
        cls.cluster.stop_dashboard()
        cls.cluster.stop()

    def test_root_returns_html(self):
        status, body, headers = _get(self.port, "/")
        self.assertEqual(status, 200)
        self.assertIn(b"HuddleCluster", body)

    def test_root_content_type_html(self):
        _, _, headers = _get(self.port, "/")
        self.assertIn("text/html", headers.get("Content-Type", ""))

    def test_dashboard_path_returns_html(self):
        status, body, _ = _get(self.port, "/dashboard")
        self.assertEqual(status, 200)
        self.assertIn(b"HuddleCluster", body)

    def test_snapshot_returns_json(self):
        status, body, _ = _get(self.port, "/dashboard/snapshot")
        self.assertEqual(status, 200)
        data = json.loads(body)
        self.assertIn("inner_ring", data)

    def test_snapshot_content_type(self):
        _, _, headers = _get(self.port, "/dashboard/snapshot")
        self.assertIn("application/json", headers.get("Content-Type", ""))

    def test_snapshot_has_all_sections(self):
        _, body, _ = _get(self.port, "/dashboard/snapshot")
        data = json.loads(body)
        for key in ("inner_ring", "outer_ring", "status",
                    "fairness_score", "retry_stats", "canary_ramps"):
            self.assertIn(key, data, f"Missing key: {key}")

    def test_unknown_path_returns_404(self):
        try:
            _get(self.port, "/unknown")
            self.fail("Expected 404")
        except urllib.error.HTTPError as exc:
            self.assertEqual(exc.code, 404)

    def test_html_contains_script_tag(self):
        _, body, _ = _get(self.port, "/")
        self.assertIn(b"<script>", body)

    def test_html_has_sse_eventsource(self):
        _, body, _ = _get(self.port, "/")
        self.assertIn(b"EventSource", body)

    def test_snapshot_updates_after_latency(self):
        s = self.cluster.inner_servers()[0]
        for i in range(5):
            self.cluster.record_latency(s, float(i * 10 + 10))

        _, body, _ = _get(self.port, "/dashboard/snapshot")
        data = json.loads(body)
        inner_ids = {e["id"]: e for e in data["inner_ring"]}
        self.assertIn(s.id, inner_ids)
        self.assertGreater(inner_ids[s.id]["avg_latency_ms"], 0)


class TestSSEStream(unittest.TestCase):
    """SSE /dashboard/stream emits 'data: {...}' lines."""

    @classmethod
    def setUpClass(cls):
        cls.cluster = create_cluster([("s1", "127.0.0.1", 8001)])
        cls.cluster.start()
        cls.port = cls.cluster.serve_dashboard(port=0)

    @classmethod
    def tearDownClass(cls):
        cls.cluster.stop_dashboard()
        cls.cluster.stop()

    def test_stream_content_type(self):
        url = f"http://127.0.0.1:{self.port}/dashboard/stream"
        req = urllib.request.Request(url)
        # Read just the headers by opening with a very short timeout
        import socket as _sock
        s = _sock.create_connection(("127.0.0.1", self.port), timeout=2.0)
        s.sendall(b"GET /dashboard/stream HTTP/1.0\r\nHost: 127.0.0.1\r\n\r\n")
        response = b""
        try:
            s.settimeout(3.0)
            while b"\r\n\r\n" not in response:
                chunk = s.recv(1024)
                if not chunk:
                    break
                response += chunk
        finally:
            s.close()
        self.assertIn(b"text/event-stream", response)

    def test_stream_emits_data_line(self):
        import socket as _sock
        s = _sock.create_connection(("127.0.0.1", self.port), timeout=2.0)
        s.sendall(b"GET /dashboard/stream HTTP/1.0\r\nHost: 127.0.0.1\r\n\r\n")
        received = b""
        s.settimeout(5.0)
        try:
            while b"data:" not in received:
                chunk = s.recv(4096)
                if not chunk:
                    break
                received += chunk
        finally:
            s.close()
        self.assertIn(b"data:", received)

    def test_stream_data_is_valid_json(self):
        import socket as _sock
        s = _sock.create_connection(("127.0.0.1", self.port), timeout=2.0)
        s.sendall(b"GET /dashboard/stream HTTP/1.0\r\nHost: 127.0.0.1\r\n\r\n")
        received = b""
        s.settimeout(5.0)
        try:
            while b"data:" not in received or received.count(b"\n\n") < 1:
                chunk = s.recv(4096)
                if not chunk:
                    break
                received += chunk
        finally:
            s.close()

        # Extract the first data: ... line after headers
        body_start = received.find(b"\r\n\r\n")
        body = received[body_start + 4:]
        for line in body.decode(errors="ignore").splitlines():
            if line.startswith("data:"):
                payload = line[5:].strip()
                parsed = json.loads(payload)
                self.assertIn("inner_ring", parsed)
                return
        self.fail("No valid data: line found in SSE stream")


class TestThreadSafety(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cluster = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
        ])
        cls.cluster.start()
        cls.port = cls.cluster.serve_dashboard(port=0)

    @classmethod
    def tearDownClass(cls):
        cls.cluster.stop_dashboard()
        cls.cluster.stop()

    def test_concurrent_snapshot_requests(self):
        errors = []

        def requester():
            try:
                for _ in range(5):
                    status, body, _ = _get(self.port, "/dashboard/snapshot")
                    if status != 200:
                        errors.append(f"Status {status}")
                    json.loads(body)
            except Exception as e:
                errors.append(str(e))

        threads = [threading.Thread(target=requester) for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [], f"Errors: {errors}")


if __name__ == "__main__":
    unittest.main()