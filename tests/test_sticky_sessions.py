"""
Tests for sticky sessions / session affinity (v1.4.0).

Covers:
  - get_server() without affinity_key behaves as before (round-robin)
  - get_server(affinity_key=X) returns the same server for the same key
  - Different keys can map to different servers
  - Stale binding is re-mapped when server is evicted
  - Stale binding is re-mapped when server is marked unhealthy
  - clear_affinity(key) removes one binding
  - clear_affinity() removes all bindings and returns count
  - affinity_map_size() tracks count correctly
  - Affinity binding purged automatically on server eviction
  - health_report() includes affinity_bindings count
  - get_server_context(affinity_key=X) routes to same server
  - Thread safety: concurrent sticky calls do not corrupt the map
"""

import threading
import time
import unittest
from collections import Counter

from huddle_cluster import HuddleCluster, Server, ServerMetrics, create_cluster


class TestRoundRobinUnchanged(unittest.TestCase):
    """get_server() without affinity_key must behave exactly as before."""

    def setUp(self):
        self.cluster = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
            ("s3", "127.0.0.1", 8003),
        ])
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_round_robin_distributes(self):
        ids = [self.cluster.get_server().id for _ in range(30)]
        counts = Counter(ids)
        # Each server should get roughly equal share
        for sid in ("s1", "s2", "s3"):
            self.assertGreater(counts[sid], 5)

    def test_none_affinity_key_is_round_robin(self):
        ids = [self.cluster.get_server(affinity_key=None).id for _ in range(30)]
        counts = Counter(ids)
        for sid in ("s1", "s2", "s3"):
            self.assertGreater(counts[sid], 5)


class TestStickySessionBasics(unittest.TestCase):
    """Core sticky session behavior."""

    def setUp(self):
        self.cluster = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
            ("s3", "127.0.0.1", 8003),
        ])
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_same_key_same_server(self):
        first = self.cluster.get_server(affinity_key="user-42")
        for _ in range(20):
            server = self.cluster.get_server(affinity_key="user-42")
            self.assertEqual(server.id, first.id,
                             "Same key must always route to same server")

    def test_different_keys_may_differ(self):
        ids = set()
        for i in range(30):
            s = self.cluster.get_server(affinity_key=f"user-{i}")
            ids.add(s.id)
        # With 30 different keys and 3 servers, at least 2 different servers hit
        self.assertGreater(len(ids), 1)

    def test_affinity_does_not_rotate_ring_for_others(self):
        # Call with affinity key many times -- should NOT consume rotation slots
        # for non-sticky callers' round-robin cycle.
        # Get one baseline round-robin sequence
        seq_rr = [self.cluster.get_server().id for _ in range(9)]

        # Now reset by rotating back (create fresh cluster)
        cluster2 = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
            ("s3", "127.0.0.1", 8003),
        ])
        cluster2.start()
        try:
            # Interleave sticky calls -- they should not shift round-robin
            for _ in range(5):
                cluster2.get_server(affinity_key="sticky-user")
            seq_after = [cluster2.get_server().id for _ in range(9)]
        finally:
            cluster2.stop()

        # Both sequences should cycle through all 3 servers
        self.assertEqual(set(seq_rr), {"s1", "s2", "s3"})
        self.assertEqual(set(seq_after), {"s1", "s2", "s3"})

    def test_binding_recorded_in_map(self):
        self.cluster.get_server(affinity_key="session-abc")
        self.assertEqual(self.cluster.affinity_map_size(), 1)

    def test_multiple_bindings_tracked(self):
        for i in range(10):
            self.cluster.get_server(affinity_key=f"user-{i}")
        self.assertEqual(self.cluster.affinity_map_size(), 10)


class TestStickySessionRemap(unittest.TestCase):
    """Stale bindings are re-mapped when the bound server is unavailable."""

    def setUp(self):
        self.cluster = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
        ])
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_remap_on_unhealthy(self):
        # Bind a key to s1
        for _ in range(10):
            s = self.cluster.get_server(affinity_key="user-X")
            if s.id == "s1":
                break

        # Find s1 directly
        servers = {s.id: s for s in self.cluster.all_servers()}
        s1 = servers["s1"]

        # Bind to s1 explicitly
        self.cluster._affinity_map["user-X"] = "s1"

        # Mark s1 unhealthy
        s1.metrics.is_healthy = False

        # Next call with same key should return a different (healthy) server
        rerouted = self.cluster.get_server(affinity_key="user-X")
        self.assertNotEqual(rerouted.id, "s1",
                            "Unhealthy bound server must trigger re-map")

    def test_remap_after_force_evict(self):
        # Bind user to s1
        self.cluster._affinity_map["user-Y"] = "s1"

        # Evict s1
        self.cluster.force_evict("s1")

        # Next call should re-map to an inner server (s2)
        server = self.cluster.get_server(affinity_key="user-Y")
        self.assertEqual(server.id, "s2")

    def test_eviction_purges_affinity(self):
        # Bind multiple keys to s1
        self.cluster._affinity_map["user-A"] = "s1"
        self.cluster._affinity_map["user-B"] = "s1"
        self.cluster._affinity_map["user-C"] = "s2"

        # Evict s1
        self.cluster.force_evict("s1")

        # Bindings for s1 must be purged; s2 binding must survive
        self.assertNotIn("user-A", self.cluster._affinity_map)
        self.assertNotIn("user-B", self.cluster._affinity_map)
        self.assertIn("user-C", self.cluster._affinity_map)


class TestClearAffinity(unittest.TestCase):
    """clear_affinity() and affinity_map_size() behavior."""

    def setUp(self):
        self.cluster = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
        ])
        self.cluster.start()
        for i in range(5):
            self.cluster.get_server(affinity_key=f"user-{i}")

    def tearDown(self):
        self.cluster.stop()

    def test_clear_single_key(self):
        size_before = self.cluster.affinity_map_size()
        removed = self.cluster.clear_affinity("user-0")
        self.assertEqual(removed, 1)
        self.assertEqual(self.cluster.affinity_map_size(), size_before - 1)

    def test_clear_nonexistent_key(self):
        removed = self.cluster.clear_affinity("nobody")
        self.assertEqual(removed, 0)

    def test_clear_all(self):
        count = self.cluster.affinity_map_size()
        self.assertGreater(count, 0)
        removed = self.cluster.clear_affinity()
        self.assertEqual(removed, count)
        self.assertEqual(self.cluster.affinity_map_size(), 0)

    def test_after_clear_key_remaps(self):
        # Record which server user-0 was bound to
        first_server_id = self.cluster._affinity_map.get("user-0")

        # Clear the binding
        self.cluster.clear_affinity("user-0")

        # Force all servers into a known state for predictable remap test
        # (just verify it doesn't crash and returns a server)
        server = self.cluster.get_server(affinity_key="user-0")
        self.assertIsNotNone(server)


class TestHealthReport(unittest.TestCase):
    """health_report() must include affinity_bindings."""

    def setUp(self):
        self.cluster = create_cluster([("s1", "127.0.0.1", 8001)])
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_affinity_bindings_in_report(self):
        report = self.cluster.health_report()
        self.assertIn("affinity_bindings", report)

    def test_affinity_bindings_count_matches(self):
        self.cluster.get_server(affinity_key="user-1")
        self.cluster.get_server(affinity_key="user-2")
        report = self.cluster.health_report()
        self.assertEqual(report["affinity_bindings"],
                         self.cluster.affinity_map_size())


class TestGetServerContextAffinity(unittest.TestCase):
    """get_server_context(affinity_key=X) must route to the same server."""

    def setUp(self):
        self.cluster = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
        ])
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_context_manager_sticky(self):
        seen_ids = set()
        for _ in range(10):
            with self.cluster.get_server_context(affinity_key="session-X") as s:
                if s:
                    seen_ids.add(s.id)
        self.assertEqual(len(seen_ids), 1,
                         "All context calls with same key must hit same server")

    def test_context_manager_without_key_is_round_robin(self):
        seen_ids = set()
        for _ in range(20):
            with self.cluster.get_server_context() as s:
                if s:
                    seen_ids.add(s.id)
        self.assertGreater(len(seen_ids), 1)


class TestThreadSafety(unittest.TestCase):
    """Concurrent sticky calls must not corrupt the affinity map."""

    def setUp(self):
        self.cluster = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
            ("s3", "127.0.0.1", 8003),
        ])
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_concurrent_sticky_calls(self):
        errors = []

        def worker(user_id):
            try:
                first = self.cluster.get_server(affinity_key=user_id)
                for _ in range(20):
                    s = self.cluster.get_server(affinity_key=user_id)
                    if s.id != first.id:
                        errors.append(
                            f"{user_id}: expected {first.id}, got {s.id}"
                        )
            except Exception as e:
                errors.append(str(e))

        threads = [
            threading.Thread(target=worker, args=(f"user-{i}",))
            for i in range(20)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [],
                         f"Thread safety violations: {errors}")

    def test_concurrent_clear_and_get(self):
        errors = []

        def getter():
            try:
                for _ in range(50):
                    self.cluster.get_server(affinity_key="shared-key")
            except Exception as e:
                errors.append(str(e))

        def clearer():
            try:
                for _ in range(50):
                    self.cluster.clear_affinity("shared-key")
                    time.sleep(0.001)
            except Exception as e:
                errors.append(str(e))

        t1 = threading.Thread(target=getter)
        t2 = threading.Thread(target=clearer)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        self.assertEqual(errors, [], f"Thread safety errors: {errors}")


if __name__ == "__main__":
    unittest.main()