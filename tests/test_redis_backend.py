"""
Tests for RedisBackend (v1.4.0).

Uses fakeredis for in-memory Redis simulation -- no real Redis server needed.
Falls back to unittest.mock if fakeredis is not available.
"""

import json
import threading
import time
import unittest

from huddle_cluster import create_cluster
from huddle_cluster_pkg.backends_redis import RedisBackend


# Use fakeredis if available, otherwise mock

try:
    import fakeredis
    _HAS_FAKEREDIS = True
except ImportError:
    _HAS_FAKEREDIS = False


def _make_backend() -> RedisBackend:
    """Return a RedisBackend using an in-memory Redis fake."""
    backend = RedisBackend(key="test:huddle:state")
    if _HAS_FAKEREDIS:
        backend._client = fakeredis.FakeRedis(decode_responses=True)
    else:
        # Build a minimal dict-backed mock that handles get/set/setex/delete/exists/ttl
        from unittest.mock import MagicMock
        store = {}

        mock = MagicMock()
        mock.ping.return_value = True
        mock.get.side_effect  = lambda k: store.get(k)
        mock.set.side_effect  = lambda k, v: store.update({k: v}) or True
        mock.setex.side_effect = lambda k, ttl, v: store.update({k: v}) or True
        mock.delete.side_effect = lambda k: bool(store.pop(k, None))
        mock.exists.side_effect = lambda k: int(k in store)
        mock.ttl.return_value  = -1
        backend._client = mock
    return backend


class TestPing(unittest.TestCase):

    def test_ping_returns_true(self):
        backend = _make_backend()
        self.assertTrue(backend.ping())


class TestSaveLoad(unittest.TestCase):

    def setUp(self):
        self.cluster = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
        ])
        self.cluster.start()
        self.backend = _make_backend()

    def tearDown(self):
        self.cluster.stop()

    def test_save_returns_true(self):
        result = self.backend.save(self.cluster)
        self.assertTrue(result)

    def test_key_exists_after_save(self):
        self.backend.save(self.cluster)
        self.assertTrue(self.backend.exists())

    def test_load_returns_0_when_no_key(self):
        result = self.backend.load(self.cluster)
        self.assertEqual(result, 0)

    def test_load_returns_count(self):
        self.backend.save(self.cluster)
        restored = self.backend.load(self.cluster)
        self.assertEqual(restored, 2)

    def test_save_stores_valid_json(self):
        self.backend.save(self.cluster)
        raw = self.backend._client.get("test:huddle:state")
        data = json.loads(raw)
        self.assertIn("servers", data)
        self.assertIn("version", data)
        self.assertIn("saved_at", data)

    def test_load_restores_rotation_count(self):
        servers = {s.id: s for s in self.cluster.all_servers()}
        servers["s1"].rotation_count = 42
        self.backend.save(self.cluster)

        # Reset
        servers["s1"].rotation_count = 0
        self.backend.load(self.cluster)

        sv = {s.id: s for s in self.cluster.all_servers()}
        self.assertEqual(sv["s1"].rotation_count, 42)

    def test_load_restores_avg_response_ms(self):
        servers = {s.id: s for s in self.cluster.all_servers()}
        servers["s1"].metrics.avg_response_ms = 77.7
        self.backend.save(self.cluster)
        servers["s1"].metrics.avg_response_ms = 0.0
        self.backend.load(self.cluster)
        sv = {s.id: s for s in self.cluster.all_servers()}
        self.assertAlmostEqual(sv["s1"].metrics.avg_response_ms, 77.7, places=5)

    def test_load_restores_histogram(self):
        s = self.cluster.inner_servers()[0]
        for i in range(1, 21):
            self.cluster.record_latency(s, float(i))
        self.backend.save(self.cluster)

        s.metrics._histogram_window.clear()
        self.backend.load(self.cluster)
        self.assertEqual(len(s.metrics._histogram_window), 20)

    def test_round_trip_preserves_error_rate(self):
        servers = {s.id: s for s in self.cluster.all_servers()}
        servers["s2"].metrics.error_rate = 0.15
        self.backend.save(self.cluster)
        servers["s2"].metrics.error_rate = 0.0
        self.backend.load(self.cluster)
        sv = {s.id: s for s in self.cluster.all_servers()}
        self.assertAlmostEqual(sv["s2"].metrics.error_rate, 0.15, places=5)

    def test_load_skips_unknown_servers(self):
        self.backend.save(self.cluster)

        # New cluster with only s1 -- s2 entry should be skipped
        c2 = create_cluster([("s1", "127.0.0.1", 8001)])
        c2.start()
        try:
            restored = self.backend.load(c2)
            self.assertEqual(restored, 1)
        finally:
            c2.stop()

    def test_delete(self):
        self.backend.save(self.cluster)
        self.assertTrue(self.backend.exists())
        self.backend.delete()
        self.assertFalse(self.backend.exists())

    def test_delete_nonexistent_returns_false(self):
        result = self.backend.delete()
        self.assertFalse(result)

    def test_multiple_saves_overwrite(self):
        servers = {s.id: s for s in self.cluster.all_servers()}
        servers["s1"].rotation_count = 1
        self.backend.save(self.cluster)
        servers["s1"].rotation_count = 99
        self.backend.save(self.cluster)

        servers["s1"].rotation_count = 0
        self.backend.load(self.cluster)
        sv = {s.id: s for s in self.cluster.all_servers()}
        self.assertEqual(sv["s1"].rotation_count, 99)


class TestTTL(unittest.TestCase):
    """TTL-based expiry stored correctly (fakeredis only)."""

    @unittest.skipUnless(_HAS_FAKEREDIS, "requires fakeredis")
    def test_key_expires_with_ttl(self):
        import fakeredis
        cluster = create_cluster([("s1", "127.0.0.1", 8001)])
        cluster.start()
        try:
            backend = RedisBackend(key="test:ttl", ttl_sec=60)
            backend._client = fakeredis.FakeRedis(decode_responses=True)
            backend.save(cluster)
            self.assertTrue(backend.exists())
            ttl = backend._client.ttl("test:ttl")
            # ttl >= 0 means an expiry is set (0 = expiring now, positive = seconds remaining)
            # ttl == -1 means no expiry set (which would be a bug)
            self.assertGreaterEqual(ttl, 0,
                f"Expected TTL >= 0 (expiry set), got {ttl}")
        finally:
            cluster.stop()


class TestAutoSync(unittest.TestCase):

    def setUp(self):
        self.cluster = create_cluster([("s1", "127.0.0.1", 8001)])
        self.cluster.start()
        self.backend = _make_backend()

    def tearDown(self):
        self.backend.stop_auto_sync()
        self.cluster.stop()

    def test_raises_if_started_twice(self):
        self.backend.start_auto_sync(self.cluster, interval_sec=60.0)
        with self.assertRaises(RuntimeError):
            self.backend.start_auto_sync(self.cluster, interval_sec=60.0)

    def test_raises_for_invalid_direction(self):
        with self.assertRaises(ValueError):
            self.backend.start_auto_sync(self.cluster, direction="push")

    def test_stop_safe_when_not_running(self):
        self.backend.stop_auto_sync()  # must not raise

    def test_stop_twice(self):
        self.backend.start_auto_sync(self.cluster, interval_sec=60.0)
        self.backend.stop_auto_sync()
        self.backend.stop_auto_sync()  # must not raise

    def test_auto_save_eventually_writes_key(self):
        self.backend.start_auto_sync(
            self.cluster, interval_sec=0.05, direction="save"
        )
        time.sleep(0.3)
        self.backend.stop_auto_sync()
        self.assertTrue(self.backend.exists())


class TestImportError(unittest.TestCase):

    def test_raises_without_redis_package(self):
        import sys
        with unittest.mock.patch.dict(sys.modules, {"redis": None}):
            import importlib
            import huddle_cluster_pkg.backends_redis as mod
            importlib.reload(mod)
            with self.assertRaises(ImportError):
                mod.RedisBackend()
            importlib.reload(mod)


class TestInfo(unittest.TestCase):

    def test_info_keys(self):
        backend = _make_backend()
        info = backend.info()
        for key in ("url", "key", "db", "ttl_sec", "running", "exists"):
            self.assertIn(key, info)

    def test_info_not_running_initially(self):
        backend = _make_backend()
        self.assertFalse(backend.info()["running"])


class TestConcurrentSave(unittest.TestCase):

    def test_concurrent_saves_no_crash(self):
        cluster = create_cluster([("s1", "127.0.0.1", 8001)])
        cluster.start()
        backend = _make_backend()
        errors = []

        def saver():
            try:
                for _ in range(10):
                    backend.save(cluster)
                    time.sleep(0.005)
            except Exception as e:
                errors.append(str(e))

        threads = [threading.Thread(target=saver) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        cluster.stop()
        self.assertEqual(errors, [], f"Errors: {errors}")

        # Final key must be valid JSON
        raw = backend._client.get("test:huddle:state")
        self.assertIsNotNone(raw)
        json.loads(raw)


import unittest.mock  # ensure available for TestImportError

if __name__ == "__main__":
    unittest.main()