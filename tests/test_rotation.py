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



# Helpers


def _make_server(sid: str, cpu: float = 0.0, healthy: bool = True) -> Server:
    s = Server(id=sid, host="127.0.0.1", port=8000)
    s.metrics = ServerMetrics(cpu_usage=cpu, is_healthy=healthy)
    s.update_temperature()
    return s


def _hot_server(sid: str) -> Server:
    """CPU = 0.95 → well above default heat_threshold=0.75.
    Run enough EMA iterations so temperature actually converges above 0.75.
    EMA converges to raw≈0.868 (0.95*0.35 + 0.9*0.25 = 0.5575... wait,
    raw = 0.95*0.35 + 0.9*0.25 = 0.3325+0.225 = 0.5575 — not enough.
    So we force temperature directly after convergence.
    """
    s = Server(id=sid, host="127.0.0.1", port=8000)
    s.metrics = ServerMetrics(
        cpu_usage=1.0,
        memory_usage=1.0,
        active_connections=1000,
        avg_response_ms=5000,
        error_rate=1.0,
    )
    # 100 iterations to fully converge EMA (raw=1.0 → temp converges to 1.0)
    for _ in range(100):
        s.update_temperature()
    return s


def _cool_server(sid: str) -> Server:
    """CPU = 0.05 → well below default cool_threshold=0.30."""
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
    )



# Test Cases


class TestEvictionToOuter(unittest.TestCase):
    """Overheated inner servers must move to the outer ring."""

    def test_hot_server_evicted(self):
        c = _cluster(min_inner=1, max_inner=3, rotation_cooldown=0.0)
        hot = _hot_server("hot1")
        cold = _cool_server("cold1")
        cold2 = _cool_server("cold2")

        c.add_server(cold,  force_inner=True)
        c.add_server(cold2, force_inner=True)
        c.add_server(hot,   force_inner=True)

        # Backdate so rotation_cooldown=0.0 check passes (elapsed >= 0.0)
        hot.last_rotated = time.monotonic() - 999

        c.rotate()

        self.assertEqual(hot.position, Position.OUTER,
                         "Overheated server should be in outer ring")
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
        hot = _hot_server("h1")
        filler = _cool_server("f1")
        c.add_server(filler, force_inner=True)
        c.add_server(hot,    force_inner=True)

        # Backdate so cooldown check passes immediately
        hot.last_rotated = time.monotonic() - 999

        before = hot.rotation_count
        c.rotate()
        self.assertGreater(hot.rotation_count, before)


class TestPullFromOuter(unittest.TestCase):
    """Cooled outer servers must move to inner ring when space allows."""

    def test_cool_outer_pulled_to_inner(self):
        c = _cluster(min_inner=1, max_inner=3, min_outer_dwell=0.0, rotation_cooldown=0.0)
        inner1 = _cool_server("i1")
        outer_cool = _cool_server("o1")

        c.add_server(inner1,     force_inner=True)
        c.add_server(outer_cool, force_inner=False)   # starts in outer

        # Manually reset last_rotated so dwell check passes
        outer_cool.last_rotated = time.monotonic() - 999

        c.rotate()

        self.assertEqual(outer_cool.position, Position.INNER,
                         "Cool outer server should be pulled to inner")

    def test_hot_outer_not_pulled(self):
        c = _cluster(min_inner=1, max_inner=3, min_outer_dwell=0.0, rotation_cooldown=0.0)
        inner1 = _cool_server("i1")
        outer_hot = _hot_server("oh1")

        c.add_server(inner1,   force_inner=True)
        c.add_server(outer_hot, force_inner=False)
        outer_hot.last_rotated = time.monotonic() - 999

        c.rotate()

        self.assertEqual(outer_hot.position, Position.OUTER,
                         "Hot outer server must NOT be pulled to inner")


class TestThunderingHerdCap(unittest.TestCase):
    """At most 1/3 of inner ring evicted per cycle; min_inner_size preserved."""

    def test_eviction_capped_at_third(self):
        # 6 inner servers, all hot → max evict = max(1, 6//3) = 2
        # but also must keep min_inner=2 → safe_evict = 6-2 = 4
        # so cap = min(2, 4) = 2
        c = _cluster(min_inner=2, max_inner=8, rotation_cooldown=0.0)
        servers = []
        for i in range(6):
            s = _hot_server(f"h{i}")
            c.add_server(s, force_inner=True)
            servers.append(s)

        c.rotate()

        evicted = [s for s in servers if s.position == Position.OUTER]
        self.assertLessEqual(len(evicted), 2,
                             "Thundering-herd cap should limit evictions to ≤ 1/3")

    def test_min_inner_preserved(self):
        c = _cluster(min_inner=2, max_inner=4, rotation_cooldown=0.0)
        for i in range(4):
            s = _hot_server(f"h{i}")
            c.add_server(s, force_inner=True)

        for _ in range(10):   # many cycles
            c.rotate()

        self.assertGreaterEqual(len(c.inner_servers()), 2,
                                "min_inner_size must be preserved after evictions")


class TestFlappingPrevention(unittest.TestCase):
    """min_outer_dwell_sec must gate re-entry."""

    def test_server_blocked_by_dwell(self):
        c = _cluster(min_inner=1, max_inner=2,
                     min_outer_dwell=9999.0, rotation_cooldown=0.0)
        inner = _cool_server("stay")
        outer = _cool_server("wait")

        c.add_server(inner, force_inner=True)
        c.add_server(outer, force_inner=False)
        # last_rotated = now → hasn't dwelled yet

        c.rotate()

        self.assertEqual(outer.position, Position.OUTER,
                         "Server should stay outer until dwell time is met")

    def test_server_enters_after_dwell(self):
        c = _cluster(min_inner=1, max_inner=2,
                     min_outer_dwell=0.0, rotation_cooldown=0.0)
        inner = _cool_server("stay")
        outer = _cool_server("ready")

        c.add_server(inner, force_inner=True)
        c.add_server(outer, force_inner=False)
        outer.last_rotated = time.monotonic() - 999   # simulate long dwell

        c.rotate()

        self.assertEqual(outer.position, Position.INNER,
                         "Server should enter inner after dwell time expires")


class TestRotationCooldown(unittest.TestCase):
    """A server shouldn't be evicted again too soon after its last rotation."""

    def test_cooldown_prevents_immediate_re_eviction(self):
        c = _cluster(min_inner=1, max_inner=3,
                     rotation_cooldown=9999.0, min_outer_dwell=0.0)
        hot = _hot_server("hc1")
        filler = _cool_server("f1")
        filler2 = _cool_server("f2")

        c.add_server(filler,  force_inner=True)
        c.add_server(filler2, force_inner=True)
        c.add_server(hot,     force_inner=True)

        # last_rotated = now → cooldown hasn't elapsed
        hot.last_rotated = time.monotonic()

        c.rotate()

        # Server should NOT be evicted because cooldown hasn't elapsed
        self.assertEqual(hot.position, Position.INNER,
                         "Server should not be evicted during rotation cooldown")


class TestHealthFailEviction(unittest.TestCase):
    """Unhealthy servers must be evicted regardless of temperature."""

    def test_unhealthy_evicted(self):
        c = _cluster(min_inner=1, max_inner=3, rotation_cooldown=0.0)
        sick = _make_server("sick1", cpu=0.1, healthy=False)
        healthy1 = _cool_server("h1")
        healthy2 = _cool_server("h2")

        c.add_server(healthy1, force_inner=True)
        c.add_server(healthy2, force_inner=True)
        c.add_server(sick,     force_inner=True)

        c.rotate()

        self.assertEqual(sick.position, Position.OUTER,
                         "Unhealthy server must be evicted even if cool")


class TestEmergencyFallback(unittest.TestCase):
    """get_server must not return None even if inner ring is somehow empty."""

    def test_emergency_server_returned(self):
        c = _cluster(min_inner=1, max_inner=3)
        s = _cool_server("only")
        c.add_server(s, force_inner=False)   # only in outer

        # Force inner ring empty
        c._inner_ring.clear()

        result = c.get_server()
        self.assertIsNotNone(result,
                             "Emergency fallback should return a server")

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

        self.assertEqual(seen, {"rr0", "rr1", "rr2"},
                         "Round-robin should visit all inner servers")


class TestCreateCluster(unittest.TestCase):
    def test_factory_populates_inner(self):
        addrs = [(f"s{i}", "127.0.0.1", 8000 + i) for i in range(6)]
        c = create_cluster(addrs)
        self.assertGreaterEqual(len(c.inner_servers()), 1)
        self.assertEqual(len(c.all_servers()), 6)


if __name__ == "__main__":
    unittest.main(verbosity=2)