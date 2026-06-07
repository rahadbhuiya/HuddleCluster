"""
Tests for WebSocket / long-connection draining (v1.4.0).

Covers:
  - Position.DRAINING exists in enum
  - ws_connection() increments and decrements active_connections
  - ws_open() increments active_connections
  - ws_close() decrements active_connections and clamps at 0
  - ws_close() double-close does not go negative
  - Eviction with no open connections -> immediate outer ring (no drain)
  - Eviction with open connections and drain disabled -> immediate outer ring
  - Eviction with open connections and drain enabled -> DRAINING state
  - Draining server not returned by get_server()
  - Draining server included in all_servers()
  - Draining server included in draining_servers()
  - After connections close -> server moves to outer ring
  - After drain timeout -> server moves to outer ring even with open connections
  - _check_draining_servers() handles empty draining dict
  - health_report() includes draining_ring section
  - Draining server position is DRAINING
  - force_evict with open connections and drain enabled -> draining
  - Thread safety: concurrent ws_open/ws_close
"""

import collections
import time
import threading
import unittest

from huddle_cluster import HuddleCluster, Position, Server, create_cluster


class TestPositionEnum(unittest.TestCase):

    def test_draining_position_exists(self):
        self.assertIn("DRAINING", [p.name for p in Position])

    def test_draining_value(self):
        self.assertEqual(Position.DRAINING.value, "draining")


class TestWsConnectionContextManager(unittest.TestCase):

    def setUp(self):
        self.cluster = create_cluster([("s1", "127.0.0.1", 8001)])
        self.cluster.start()
        self.server = self.cluster.inner_servers()[0]

    def tearDown(self):
        self.cluster.stop()

    def test_increments_on_enter(self):
        before = self.server.metrics.active_connections
        with self.cluster.ws_connection(self.server):
            self.assertEqual(self.server.metrics.active_connections, before + 1)

    def test_decrements_on_exit(self):
        before = self.server.metrics.active_connections
        with self.cluster.ws_connection(self.server):
            pass
        self.assertEqual(self.server.metrics.active_connections, before)

    def test_decrements_on_exception(self):
        before = self.server.metrics.active_connections
        try:
            with self.cluster.ws_connection(self.server):
                raise RuntimeError("ws error")
        except RuntimeError:
            pass
        self.assertEqual(self.server.metrics.active_connections, before)

    def test_nested_connections(self):
        self.cluster.ws_open(self.server)
        self.cluster.ws_open(self.server)
        self.assertEqual(self.server.metrics.active_connections, 2)
        self.cluster.ws_close(self.server)
        self.assertEqual(self.server.metrics.active_connections, 1)
        self.cluster.ws_close(self.server)
        self.assertEqual(self.server.metrics.active_connections, 0)

    def test_yields_server(self):
        with self.cluster.ws_connection(self.server) as s:
            self.assertIs(s, self.server)


class TestWsOpenClose(unittest.TestCase):

    def setUp(self):
        self.cluster = create_cluster([("s1", "127.0.0.1", 8001)])
        self.cluster.start()
        self.server = self.cluster.inner_servers()[0]

    def tearDown(self):
        self.cluster.stop()

    def test_ws_open_increments(self):
        before = self.server.metrics.active_connections
        self.cluster.ws_open(self.server)
        self.assertEqual(self.server.metrics.active_connections, before + 1)

    def test_ws_close_decrements(self):
        self.cluster.ws_open(self.server)
        before = self.server.metrics.active_connections
        self.cluster.ws_close(self.server)
        self.assertEqual(self.server.metrics.active_connections, before - 1)

    def test_ws_close_clamps_at_zero(self):
        self.server.metrics.active_connections = 0
        self.cluster.ws_close(self.server)  # double-close -- must not go negative
        self.assertEqual(self.server.metrics.active_connections, 0)

    def test_ws_close_does_not_go_negative(self):
        for _ in range(5):
            self.cluster.ws_close(self.server)
        self.assertGreaterEqual(self.server.metrics.active_connections, 0)


class TestImmediateEvictionWithoutDrain(unittest.TestCase):
    """When drain is disabled or no connections, eviction is immediate."""

    def setUp(self):
        # drain disabled (default)
        self.cluster = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
        ])
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_no_drain_when_disabled(self):
        s = self.cluster.inner_servers()[0]
        s.metrics.active_connections = 5  # open connections
        self.cluster.force_evict(s.id)
        # With drain disabled, server goes directly to outer ring
        self.assertEqual(s.position, Position.OUTER)
        self.assertNotIn(s.id, self.cluster._draining_servers)

    def test_immediate_eviction_no_connections(self):
        cluster = create_cluster(
            [("s1", "127.0.0.1", 8001), ("s2", "127.0.0.1", 8002)],
            ws_drain_timeout_sec=30.0,
        )
        cluster.start()
        try:
            s = cluster.inner_servers()[0]
            s.metrics.active_connections = 0  # no connections
            cluster.force_evict(s.id)
            self.assertEqual(s.position, Position.OUTER)
            self.assertNotIn(s.id, cluster._draining_servers)
        finally:
            cluster.stop()


class TestDraining(unittest.TestCase):
    """Core draining behavior when ws_drain_timeout_sec > 0."""

    def setUp(self):
        self.cluster = create_cluster(
            [("s1", "127.0.0.1", 8001), ("s2", "127.0.0.1", 8002)],
            ws_drain_timeout_sec=30.0,
        )
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def _get_server(self, sid):
        return {s.id: s for s in self.cluster.all_servers()}[sid]

    def test_eviction_with_connections_starts_drain(self):
        s = self.cluster.inner_servers()[0]
        self.cluster.ws_open(s)
        self.cluster.force_evict(s.id)
        self.assertEqual(s.position, Position.DRAINING)
        self.assertIn(s.id, self.cluster._draining_servers)

    def test_draining_server_not_in_inner_ring(self):
        s = self.cluster.inner_servers()[0]
        self.cluster.ws_open(s)
        self.cluster.force_evict(s.id)
        inner_ids = {sv.id for sv in self.cluster.inner_servers()}
        self.assertNotIn(s.id, inner_ids)

    def test_draining_server_not_in_outer_ring(self):
        s = self.cluster.inner_servers()[0]
        self.cluster.ws_open(s)
        self.cluster.force_evict(s.id)
        outer_ids = {sv.id for sv in self.cluster.outer_servers()}
        self.assertNotIn(s.id, outer_ids)

    def test_draining_server_in_all_servers(self):
        s = self.cluster.inner_servers()[0]
        self.cluster.ws_open(s)
        self.cluster.force_evict(s.id)
        all_ids = {sv.id for sv in self.cluster.all_servers()}
        self.assertIn(s.id, all_ids)

    def test_draining_servers_method(self):
        s = self.cluster.inner_servers()[0]
        self.cluster.ws_open(s)
        self.cluster.force_evict(s.id)
        draining_ids = {sv.id for sv in self.cluster.draining_servers()}
        self.assertIn(s.id, draining_ids)

    def test_get_server_skips_draining(self):
        s = self.cluster.inner_servers()[0]
        self.cluster.ws_open(s)
        self.cluster.force_evict(s.id)
        # Remaining inner server should be returned, never the draining one
        for _ in range(20):
            picked = self.cluster.get_server()
            if picked:
                self.assertNotEqual(picked.id, s.id)

    def test_moves_to_outer_when_connections_close(self):
        s = self.cluster.inner_servers()[0]
        self.cluster.ws_open(s)
        self.cluster.force_evict(s.id)
        self.assertEqual(s.position, Position.DRAINING)

        # Close connection
        self.cluster.ws_close(s)
        self.assertEqual(s.metrics.active_connections, 0)

        # Trigger drain check
        with self.cluster._lock:
            self.cluster._check_draining_servers()

        self.assertEqual(s.position, Position.OUTER)
        self.assertNotIn(s.id, self.cluster._draining_servers)

    def test_moves_to_outer_on_timeout(self):
        cluster = create_cluster(
            [("s1", "127.0.0.1", 8001), ("s2", "127.0.0.1", 8002)],
            ws_drain_timeout_sec=0.05,  # 50ms timeout
        )
        cluster.start()
        try:
            s = cluster.inner_servers()[0]
            cluster.ws_open(s)
            cluster.force_evict(s.id)
            self.assertEqual(s.position, Position.DRAINING)

            # Wait for timeout to elapse
            time.sleep(0.15)

            with cluster._lock:
                cluster._check_draining_servers()

            self.assertEqual(s.position, Position.OUTER)
            self.assertNotIn(s.id, cluster._draining_servers)
        finally:
            cluster.stop()


class TestDrainCheckEmpty(unittest.TestCase):
    """_check_draining_servers() must be a no-op when no servers are draining."""

    def setUp(self):
        self.cluster = create_cluster(
            [("s1", "127.0.0.1", 8001)],
            ws_drain_timeout_sec=30.0,
        )
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_no_crash_when_empty(self):
        self.assertEqual(self.cluster._draining_servers, {})
        with self.cluster._lock:
            self.cluster._check_draining_servers()  # must not raise


class TestHealthReport(unittest.TestCase):

    def setUp(self):
        self.cluster = create_cluster(
            [("s1", "127.0.0.1", 8001), ("s2", "127.0.0.1", 8002)],
            ws_drain_timeout_sec=30.0,
        )
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_draining_ring_key_present(self):
        report = self.cluster.health_report()
        self.assertIn("draining_ring", report)

    def test_draining_ring_empty_initially(self):
        report = self.cluster.health_report()
        self.assertEqual(report["draining_ring"], [])

    def test_draining_ring_shows_draining_server(self):
        s = self.cluster.inner_servers()[0]
        self.cluster.ws_open(s)
        self.cluster.force_evict(s.id)

        report = self.cluster.health_report()
        draining_ids = [e["id"] for e in report["draining_ring"]]
        self.assertIn(s.id, draining_ids)

    def test_draining_ring_entry_keys(self):
        s = self.cluster.inner_servers()[0]
        self.cluster.ws_open(s)
        self.cluster.force_evict(s.id)

        report = self.cluster.health_report()
        entry = report["draining_ring"][0]
        for key in ("id", "active_connections", "drain_elapsed_sec",
                    "drain_timeout_sec", "reason"):
            self.assertIn(key, entry)

    def test_drain_timeout_in_report(self):
        s = self.cluster.inner_servers()[0]
        self.cluster.ws_open(s)
        self.cluster.force_evict(s.id)
        report = self.cluster.health_report()
        entry = report["draining_ring"][0]
        self.assertEqual(entry["drain_timeout_sec"], 30.0)


class TestThreadSafety(unittest.TestCase):

    def setUp(self):
        self.cluster = create_cluster(
            [("s1", "127.0.0.1", 8001), ("s2", "127.0.0.1", 8002)],
            ws_drain_timeout_sec=5.0,
        )
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_concurrent_open_close(self):
        servers = self.cluster.all_servers()
        s = servers[0]
        errors = []

        def worker():
            try:
                for _ in range(50):
                    with self.cluster.ws_connection(s):
                        time.sleep(0.001)
            except Exception as e:
                errors.append(str(e))

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [], f"Thread errors: {errors}")
        # After all threads finish, connections must be back to 0
        self.assertEqual(s.metrics.active_connections, 0)


if __name__ == "__main__":
    unittest.main()