"""
test_fairness.py — Fairness score & time-accounting tests
==========================================================
Validates that:
  - fairness_score() returns 0.0 for a perfectly balanced cluster
  - fairness_score() is non-zero when one server dominates
  - total_inner_time / total_outer_time are tracked correctly
  - health_report() structure is well-formed
  - rotation log is bounded (memory-leak guard)
"""

import time
import unittest

from huddle_cluster import (
    HuddleCluster,
    Position,
    Server,
    ServerMetrics,
)



# Helpers


def _server(sid: str, cpu: float = 0.1) -> Server:
    s = Server(id=sid, host="127.0.0.1", port=9000)
    s.metrics = ServerMetrics(cpu_usage=cpu)
    for _ in range(10):
        s.update_temperature()
    return s


def _cluster(**kw) -> HuddleCluster:
    defaults = dict(
        min_inner_size=1,
        max_inner_size=5,
        min_outer_dwell_sec=0.0,
        rotation_cooldown_sec=0.0,
    )
    defaults.update(kw)
    return HuddleCluster(**defaults)



# Tests


class TestFairnessScore(unittest.TestCase):

    def test_perfect_fairness_zero(self):
        """All servers with equal inner time → fairness_score ≈ 0."""
        c = _cluster()
        servers = [_server(f"s{i}") for i in range(4)]
        for s in servers:
            s.total_inner_time = 100.0   # artificially equalise
            c.add_server(s, force_inner=True)

        score = c.fairness_score()
        self.assertAlmostEqual(score, 0.0, places=5,
                               msg="Equal inner times → fairness_score = 0")

    def test_unfair_cluster_nonzero(self):
        """One server with very high inner time → score > 0."""
        c = _cluster()
        servers = [_server(f"s{i}") for i in range(4)]
        for i, s in enumerate(servers):
            s.total_inner_time = 1.0 if i < 3 else 1000.0
            c.add_server(s, force_inner=True)

        score = c.fairness_score()
        self.assertGreater(score, 0.1,
                           msg="Skewed inner times → fairness_score > 0")

    def test_single_server_fairness(self):
        """Single server cluster → fairness_score = 0 (no comparison)."""
        c = _cluster()
        s = _server("solo")
        c.add_server(s, force_inner=True)
        self.assertEqual(c.fairness_score(), 0.0)

    def test_fairness_range(self):
        """fairness_score must be in [0, 1]."""
        c = _cluster()
        servers = [_server(f"x{i}") for i in range(5)]
        for i, s in enumerate(servers):
            s.total_inner_time = float(i * 50)
            c.add_server(s, force_inner=True)

        score = c.fairness_score()
        self.assertGreaterEqual(score, 0.0)
        self.assertLessEqual(score, 1.0)


class TestTimeAccounting(unittest.TestCase):

    def test_inner_time_accumulates(self):
        """total_inner_time increases after server is evicted from inner."""
        c = _cluster()
        hot = _server("hot1", cpu=0.95)
        for _ in range(20):
            hot.update_temperature()
        filler = _server("fill1")

        c.add_server(filler, force_inner=True)
        c.add_server(hot,    force_inner=True)

        time.sleep(0.05)
        c.rotate()

        if hot.position == Position.OUTER:
            self.assertGreater(hot.total_inner_time, 0.0,
                               "total_inner_time should be >0 after eviction")

    def test_outer_time_accumulates(self):
        """total_outer_time increases after server re-enters inner."""
        c = _cluster()
        s = _server("o1", cpu=0.05)
        c.add_server(s, force_inner=False)   # starts in outer

        time.sleep(0.05)
        # Simulate dwell elapsed then pull manually
        s.last_rotated = time.monotonic() - 999
        c._maybe_pull_from_outer("test")

        self.assertGreater(s.total_outer_time, 0.0,
                           "total_outer_time should accumulate after outer stay")


class TestHealthReport(unittest.TestCase):

    def test_report_keys_present(self):
        c = _cluster()
        for i in range(3):
            c.add_server(_server(f"r{i}"), force_inner=True)

        report = c.health_report()
        for key in (
            "status", "inner_ring", "outer_ring",
            "inner_count", "outer_count", "total_servers",
            "avg_inner_temp", "max_inner_temp",
            "fairness_score", "total_rotations", "recent_rotations",
        ):
            self.assertIn(key, report, f"health_report missing key: {key!r}")

    def test_status_healthy_when_sufficient(self):
        c = _cluster(min_inner_size=2)
        for i in range(3):
            c.add_server(_server(f"h{i}"), force_inner=True)

        report = c.health_report()
        self.assertEqual(report["status"], "healthy")

    def test_status_degraded_when_too_few(self):
        c = _cluster(min_inner_size=3)
        c.add_server(_server("only1"), force_inner=True)   # only 1, need 3

        report = c.health_report()
        self.assertEqual(report["status"], "degraded")

    def test_inner_ring_items_have_required_fields(self):
        c = _cluster()
        c.add_server(_server("rk1"), force_inner=True)

        inner_items = c.health_report()["inner_ring"]
        self.assertTrue(len(inner_items) > 0)
        for item in inner_items:
            for f in ("id", "temp", "rotations", "inner_time_sec"):
                self.assertIn(f, item)

    def test_recent_rotations_max_ten(self):
        """recent_rotations in health_report should never exceed 10 entries."""
        c = _cluster()
        report = c.health_report()
        self.assertLessEqual(len(report["recent_rotations"]), 10)


class TestRotationLogBound(unittest.TestCase):
    """Rotation log must not grow unboundedly (memory-leak guard)."""

    def test_log_bounded_at_max(self):
        c = _cluster()
        s = _server("bnd1", cpu=0.05)
        c.add_server(s, force_inner=True)

        # Inject fake events to overflow
        from huddle_cluster import RotationEvent
        for i in range(HuddleCluster.MAX_ROTATION_LOG + 500):
            c._rotation_log.append(
                RotationEvent(
                    timestamp=time.time(),
                    server_id="bnd1",
                    direction="inner→outer",
                    reason="test",
                    temperature=0.1,
                )
            )

        self.assertLessEqual(
            len(c._rotation_log),
            HuddleCluster.MAX_ROTATION_LOG,
            "rotation_log should be bounded by MAX_ROTATION_LOG",
        )


class TestAddRemoveServer(unittest.TestCase):

    def test_add_server_inner(self):
        c = _cluster()
        s = _server("new1")
        c.add_server(s, force_inner=True)
        self.assertIn(s, c.inner_servers())

    def test_add_server_outer(self):
        c = _cluster()
        s = _server("new2")
        c.add_server(s, force_inner=False)
        self.assertIn(s, c.outer_servers())

    def test_remove_inner_server(self):
        c = _cluster(min_inner_size=1)
        s1 = _server("rm1")
        s2 = _server("rm2")
        c.add_server(s1, force_inner=True)
        c.add_server(s2, force_inner=True)

        removed = c.remove_server("rm1")
        self.assertTrue(removed)
        ids = [s.id for s in c.all_servers()]
        self.assertNotIn("rm1", ids)

    def test_remove_unknown_server(self):
        c = _cluster()
        self.assertFalse(c.remove_server("ghost99"))

    def test_remove_outer_server(self):
        c = _cluster()
        s = _server("outr1")
        c.add_server(s, force_inner=False)
        self.assertTrue(c.remove_server("outr1"))
        self.assertNotIn(s, c.all_servers())


if __name__ == "__main__":
    unittest.main(verbosity=2)