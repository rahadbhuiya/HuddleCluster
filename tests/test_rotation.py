"""
test_rotation.py — Unit tests for HuddleCluster rotation logic
===============================================================
Tests cover:
  - Basic eviction (inner → outer) when temperature exceeds heat_threshold
  - Basic pull (outer → inner) when temperature drops below cool_threshold
  - Thundering-herd cap: at most 1/3 of inner evicted per cycle
  - Flapping prevention: min_outer_dwell_sec honoured
  - Rotation cooldown: a server isn't evicted twice too quickly
  - Health-fail eviction: unhealthy server leaves inner immediately
  - Emergency server fallback when inner ring is empty
  - Manual force_evict
  - NEW v1.1.0: record_latency() raises temperature
  - NEW v1.1.0: get_server_context() records latency automatically
  - NEW v1.1.0: ema_alpha tuning
"""

import time
import unittest

from huddle_cluster import (
    EvictionReason,
    HuddleCluster,
    Position,
    Server,
    ServerMetrics,
    create_cluster,
)


#  Helpers 


def _make_server(sid: str, cpu: float = 0.0, healthy: bool = True) -> Server:
    s = Server(id=sid, host="127.0.0.1", port=8000)
    s.metrics = ServerMetrics(cpu_usage=cpu, is_healthy=healthy)
    s.update_temperature()
    return s


def _hot_server(sid: str) -> Server:
    s = Server(id=sid, host="127.0.0.1", port=8000)
    s.metrics = ServerMetrics(
        cpu_usage=1.0,
        memory_usage=1.0,
        active_connections=1000,
        avg_response_ms=5000,
        error_rate=1.0,
    )
    for _ in range(100):
        s.update_temperature()
    return s


def _cool_server(sid: str) -> Server:
    s = Server(id=sid, host="127.0.0.1", port=8000)
    s.metrics = ServerMetrics(cpu_usage=0.05, memory_usage=0.05)
    for _ in range(20):
        s.update_temperature()
    return s


def _cluster(min_inner=2, max_inner=5, min_outer_dwell=0.0, rotation_cooldown=0.0) -> HuddleCluster:
    return HuddleCluster(
        min_inner_size=min_inner,
        max_inner_size=max_inner,
        min_outer_dwell_sec=min_outer_dwell,
        rotation_cooldown_sec=rotation_cooldown,
        circuit_breaker_threshold=1.0,   # disabled in unit tests (error_rate tests handled separately)
    )


#  Original Tests 


class TestEvictionToOuter(unittest.TestCase):
    def test_hot_server_evicted(self):
        c = _cluster(min_inner=1, max_inner=3, rotation_cooldown=0.0)
        hot = _hot_server("hot1")
        cold = _cool_server("cold1")
        cold2 = _cool_server("cold2")
        c.add_server(cold,  force_inner=True)
        c.add_server(cold2, force_inner=True)
        c.add_server(hot,   force_inner=True)
        hot.last_rotated = time.monotonic() - 999
        c.rotate()
        self.assertEqual(hot.position, Position.OUTER)
        self.assertIn(hot, c.outer_servers())

    def test_cool_servers_stay_inner(self):
        c = _cluster(min_inner=1, max_inner=3, rotation_cooldown=0.0)
        s1 = _cool_server("c1")
        s2 = _cool_server("c2")
        c.add_server(s1, force_inner=True)
        c.add_server(s2, force_inner=True)
        c.rotate()
        self.assertEqual(s1.position, Position.INNER)
        self.assertEqual(s2.position, Position.INNER)

    def test_eviction_increments_rotation_count(self):
        c = _cluster(min_inner=1, max_inner=3, rotation_cooldown=0.0)
        hot    = _hot_server("h1")
        filler = _cool_server("f1")
        c.add_server(filler, force_inner=True)
        c.add_server(hot,    force_inner=True)
        hot.last_rotated = time.monotonic() - 999
        before = hot.rotation_count
        c.rotate()
        self.assertGreater(hot.rotation_count, before)


class TestPullFromOuter(unittest.TestCase):
    def test_cool_outer_pulled_to_inner(self):
        c = _cluster(min_inner=1, max_inner=3, min_outer_dwell=0.0, rotation_cooldown=0.0)
        inner1     = _cool_server("i1")
        outer_cool = _cool_server("o1")
        c.add_server(inner1,     force_inner=True)
        c.add_server(outer_cool, force_inner=False)
        outer_cool.last_rotated = time.monotonic() - 999
        c.rotate()
        self.assertEqual(outer_cool.position, Position.INNER)

    def test_hot_outer_not_pulled(self):
        c = _cluster(min_inner=1, max_inner=3, min_outer_dwell=0.0, rotation_cooldown=0.0)
        inner1    = _cool_server("i1")
        outer_hot = _hot_server("oh1")
        c.add_server(inner1,    force_inner=True)
        c.add_server(outer_hot, force_inner=False)
        outer_hot.last_rotated = time.monotonic() - 999
        c.rotate()
        self.assertEqual(outer_hot.position, Position.OUTER)


class TestThunderingHerdCap(unittest.TestCase):
    def test_eviction_capped_at_third(self):
        c = _cluster(min_inner=2, max_inner=8, rotation_cooldown=0.0)
        servers = []
        for i in range(6):
            s = _hot_server(f"h{i}")
            c.add_server(s, force_inner=True)
            servers.append(s)
        c.rotate()
        evicted = [s for s in servers if s.position == Position.OUTER]
        self.assertLessEqual(len(evicted), 2)

    def test_min_inner_preserved(self):
        c = _cluster(min_inner=2, max_inner=4, rotation_cooldown=0.0)
        for i in range(4):
            s = _hot_server(f"h{i}")
            c.add_server(s, force_inner=True)
        for _ in range(10):
            c.rotate()
        self.assertGreaterEqual(len(c.inner_servers()), 2)


class TestFlappingPrevention(unittest.TestCase):
    def test_server_blocked_by_dwell(self):
        c = _cluster(min_inner=1, max_inner=2, min_outer_dwell=9999.0, rotation_cooldown=0.0)
        inner = _cool_server("stay")
        outer = _cool_server("wait")
        c.add_server(inner, force_inner=True)
        c.add_server(outer, force_inner=False)
        c.rotate()
        self.assertEqual(outer.position, Position.OUTER)

    def test_server_enters_after_dwell(self):
        c = _cluster(min_inner=1, max_inner=2, min_outer_dwell=0.0, rotation_cooldown=0.0)
        inner = _cool_server("stay")
        outer = _cool_server("ready")
        c.add_server(inner, force_inner=True)
        c.add_server(outer, force_inner=False)
        outer.last_rotated = time.monotonic() - 999
        c.rotate()
        self.assertEqual(outer.position, Position.INNER)


class TestRotationCooldown(unittest.TestCase):
    def test_cooldown_prevents_immediate_re_eviction(self):
        c = _cluster(min_inner=1, max_inner=3, rotation_cooldown=9999.0, min_outer_dwell=0.0)
        hot     = _hot_server("hc1")
        filler  = _cool_server("f1")
        filler2 = _cool_server("f2")
        c.add_server(filler,  force_inner=True)
        c.add_server(filler2, force_inner=True)
        c.add_server(hot,     force_inner=True)
        hot.last_rotated = time.monotonic()
        c.rotate()
        self.assertEqual(hot.position, Position.INNER)


class TestHealthFailEviction(unittest.TestCase):
    def test_unhealthy_evicted(self):
        c = _cluster(min_inner=1, max_inner=3, rotation_cooldown=0.0)
        sick     = _make_server("sick1", cpu=0.1, healthy=False)
        healthy1 = _cool_server("h1")
        healthy2 = _cool_server("h2")
        c.add_server(healthy1, force_inner=True)
        c.add_server(healthy2, force_inner=True)
        c.add_server(sick,     force_inner=True)
        c.rotate()
        self.assertEqual(sick.position, Position.OUTER)


class TestEmergencyFallback(unittest.TestCase):
    def test_emergency_server_returned(self):
        c = _cluster(min_inner=1, max_inner=3)
        s = _cool_server("only")
        c.add_server(s, force_inner=False)
        c._inner_ring.clear()
        result = c.get_server()
        self.assertIsNotNone(result)

    def test_empty_cluster_returns_none(self):
        c = _cluster()
        self.assertIsNone(c.get_server())


class TestForceEvict(unittest.TestCase):
    def test_force_evict_moves_to_outer(self):
        c = _cluster(min_inner=1, max_inner=3)
        s1 = _cool_server("s1")
        s2 = _cool_server("s2")
        c.add_server(s1, force_inner=True)
        c.add_server(s2, force_inner=True)
        result = c.force_evict("s1")
        self.assertTrue(result)
        self.assertEqual(s1.position, Position.OUTER)

    def test_force_evict_unknown_server(self):
        c = _cluster()
        self.assertFalse(c.force_evict("ghost"))


class TestRoundRobin(unittest.TestCase):
    def test_round_robin_cycles(self):
        c = _cluster(min_inner=1, max_inner=5)
        servers = [_cool_server(f"rr{i}") for i in range(3)]
        for s in servers:
            c.add_server(s, force_inner=True)
        seen = set()
        for _ in range(9):
            s = c.get_server()
            seen.add(s.id)
        self.assertEqual(seen, {"rr0", "rr1", "rr2"})


class TestCreateCluster(unittest.TestCase):
    def test_factory_populates_inner(self):
        addrs = [(f"s{i}", "127.0.0.1", 8000 + i) for i in range(6)]
        c = create_cluster(addrs)
        self.assertGreaterEqual(len(c.inner_servers()), 1)
        self.assertEqual(len(c.all_servers()), 6)


#  NEW v1.1.0 Tests 


class TestRecordLatency(unittest.TestCase):
    """record_latency() must update avg_response_ms and influence temperature."""

    def test_latency_updates_avg(self):
        c = _cluster(min_inner=1, max_inner=3)
        s = _cool_server("lat1")
        c.add_server(s, force_inner=True)
        c.record_latency(s, 200.0)
        self.assertAlmostEqual(s.metrics.avg_response_ms, 200.0, places=1)

    def test_high_latency_raises_temperature(self):
        c = _cluster(min_inner=1, max_inner=3)
        s = _cool_server("lat2")
        c.add_server(s, force_inner=True)
        temp_before = s.temperature
        for _ in range(60):
            c.record_latency(s, 4900.0)
        self.assertGreater(s.temperature, temp_before,
                           "High latency should increase server temperature")

    def test_normal_latency_keeps_temp_low(self):
        c = _cluster(min_inner=1, max_inner=3)
        s = _cool_server("lat3")
        c.add_server(s, force_inner=True)
        for _ in range(30):
            c.record_latency(s, 20.0)
        self.assertLess(s.temperature, 0.3,
                        "Low latency should keep temperature below cool_threshold")

    def test_slow_and_overloaded_server_self_evicts(self):
        """
        Core benchmark fix: a server with high CPU + high latency self-evicts
        without needing an external metrics_updater.
        Uses ema_alpha=0.9 for fast convergence in test environment.
        """
        c = HuddleCluster(
            min_inner_size=1,
            max_inner_size=3,
            rotation_cooldown_sec=0.0,
            min_outer_dwell_sec=0.0,
            ema_alpha=0.9,
        )
        slow   = _cool_server("slow")
        filler = _cool_server("fill")
        extra  = _cool_server("xtra")
        c.add_server(filler, force_inner=True)
        c.add_server(extra,  force_inner=True)
        c.add_server(slow,   force_inner=True)

        # Simulate fully overloaded server
        slow.metrics.cpu_usage    = 1.0
        slow.metrics.memory_usage = 1.0
        slow.metrics.error_rate   = 1.0
        slow.metrics.active_connections = 1000
        for _ in range(30):
            c.record_latency(slow, 5000.0)

        slow.last_rotated = time.monotonic() - 999
        c.rotate()

        self.assertEqual(slow.position, Position.OUTER,
                         "Overloaded server should self-evict via feedback")

    def test_p95_latency_computed(self):
        c = _cluster(min_inner=1, max_inner=3)
        s = _cool_server("p95")
        c.add_server(s, force_inner=True)
        for i in range(50):
            c.record_latency(s, float(i * 10))
        p95 = s.metrics.p95_latency()
        self.assertGreater(p95, 0.0)
        self.assertLessEqual(p95, 490.0)

    def test_health_report_includes_latency(self):
        """health_report() must include avg_latency_ms and p95_latency_ms."""
        c = _cluster(min_inner=1, max_inner=3)
        s = _cool_server("rep1")
        c.add_server(s, force_inner=True)
        for _ in range(10):
            c.record_latency(s, 50.0)
        report = c.health_report()
        inner_entry = report["inner_ring"][0]
        self.assertIn("avg_latency_ms", inner_entry)
        self.assertIn("p95_latency_ms", inner_entry)


class TestGetServerContext(unittest.TestCase):
    """get_server_context() must auto-record latency and handle errors."""

    def test_context_records_latency(self):
        c = _cluster(min_inner=1, max_inner=3)
        s = _cool_server("ctx1")
        c.add_server(s, force_inner=True)
        with c.get_server_context() as server:
            self.assertIsNotNone(server)
            time.sleep(0.02)
        self.assertGreater(server.metrics.avg_response_ms, 0.0)

    def test_context_increments_error_rate_on_exception(self):
        c = _cluster(min_inner=1, max_inner=3)
        s = _cool_server("ctx2")
        c.add_server(s, force_inner=True)
        for _ in range(10):
            try:
                with c.get_server_context() as server:
                    raise RuntimeError("upstream timeout")
            except RuntimeError:
                pass
        self.assertGreater(server.metrics.error_rate, 0.0)

    def test_context_marks_unhealthy_after_many_errors(self):
        c = _cluster(min_inner=1, max_inner=3)
        s = _cool_server("ctx3")
        c.add_server(s, force_inner=True)
        for _ in range(30):
            try:
                with c.get_server_context() as server:
                    raise RuntimeError("fail")
            except RuntimeError:
                pass
        self.assertGreater(server.metrics.error_rate, 0.3)


class TestEmaAlphaTuning(unittest.TestCase):
    """ema_alpha kwarg should control reactivity per-cluster."""

    def test_high_alpha_reacts_faster(self):
        c_fast = HuddleCluster(
            ema_alpha=0.9, min_inner_size=1, max_inner_size=3,
            rotation_cooldown_sec=0.0, min_outer_dwell_sec=0.0,
        )
        c_slow = HuddleCluster(
            ema_alpha=0.1, min_inner_size=1, max_inner_size=3,
            rotation_cooldown_sec=0.0, min_outer_dwell_sec=0.0,
        )
        s_fast = _cool_server("fa")
        s_slow = _cool_server("sl")
        c_fast.add_server(s_fast, force_inner=True)
        c_slow.add_server(s_slow, force_inner=True)
        for _ in range(5):
            c_fast.record_latency(s_fast, 4000.0)
            c_slow.record_latency(s_slow, 4000.0)
        self.assertGreater(s_fast.temperature, s_slow.temperature,
                           "Higher ema_alpha should produce faster temperature rise")

    def test_invalid_alpha_raises(self):
        with self.assertRaises(ValueError):
            HuddleCluster(ema_alpha=0.0)
        with self.assertRaises(ValueError):
            HuddleCluster(ema_alpha=1.5)


if __name__ == "__main__":
    unittest.main(verbosity=2)
