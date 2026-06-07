"""
Tests for canary / rolling deploy traffic ramps (v1.4.0).

Covers:
  - TrafficRamp.current_weight() linear interpolation
  - TrafficRamp.progress() returns correct fraction
  - TrafficRamp.is_complete() fires at ramp_sec
  - TrafficRamp.to_dict() returns expected keys
  - start_traffic_ramp() raises ValueError for unknown server
  - start_traffic_ramp() raises ValueError for bad weights
  - start_traffic_ramp() returns TrafficRamp with correct fields
  - stop_traffic_ramp() removes ramp and sets target weight
  - stop_traffic_ramp() returns False for unknown server
  - canary_status() returns list of ramp dicts
  - canary_status() empty when no ramps active
  - _weighted_select() returns server proportional to weight
  - Canary server receives ~correct fraction of traffic
  - Full-weight server receives more traffic than canary
  - Round-robin resumes after ramp completes
  - _update_ramps() removes completed ramps and sets server weight
  - health_report() includes canary_ramps
  - Multiple concurrent ramps tracked independently
  - Thread safety: concurrent start/stop ramp calls
"""

import collections
import statistics
import threading
import time
import unittest

from huddle_cluster import TrafficRamp, create_cluster


class TestTrafficRampMath(unittest.TestCase):

    def _make(self, start=0.1, target=1.0, ramp_sec=60.0, offset_sec=0.0):
        r = TrafficRamp(
            server_id="s1",
            start_weight=start,
            target_weight=target,
            ramp_sec=ramp_sec,
            start_time=time.monotonic() - offset_sec,
        )
        return r

    def test_current_weight_at_start(self):
        r = self._make(start=0.1, target=1.0, offset_sec=0.0)
        self.assertAlmostEqual(r.current_weight(), 0.1, delta=0.02)

    def test_current_weight_at_end(self):
        r = self._make(start=0.1, target=1.0, ramp_sec=10.0, offset_sec=10.0)
        self.assertAlmostEqual(r.current_weight(), 1.0, places=6)

    def test_current_weight_midpoint(self):
        r = self._make(start=0.0, target=1.0, ramp_sec=100.0, offset_sec=50.0)
        self.assertAlmostEqual(r.current_weight(), 0.5, delta=0.05)

    def test_current_weight_clamped_at_target(self):
        r = self._make(start=0.1, target=1.0, ramp_sec=1.0, offset_sec=999.0)
        self.assertAlmostEqual(r.current_weight(), 1.0, places=6)

    def test_progress_at_start(self):
        r = self._make(offset_sec=0.0)
        self.assertAlmostEqual(r.progress(), 0.0, delta=0.05)

    def test_progress_at_end(self):
        r = self._make(ramp_sec=10.0, offset_sec=10.0)
        self.assertAlmostEqual(r.progress(), 1.0, places=6)

    def test_progress_clamped_at_1(self):
        r = self._make(ramp_sec=1.0, offset_sec=9999.0)
        self.assertEqual(r.progress(), 1.0)

    def test_is_complete_false_at_start(self):
        r = self._make(ramp_sec=60.0, offset_sec=0.0)
        self.assertFalse(r.is_complete())

    def test_is_complete_true_after_ramp(self):
        r = self._make(ramp_sec=1.0, offset_sec=2.0)
        self.assertTrue(r.is_complete())

    def test_to_dict_keys(self):
        r = self._make()
        d = r.to_dict()
        for key in ("server_id", "start_weight", "target_weight", "ramp_sec",
                    "current_weight", "progress_pct", "complete"):
            self.assertIn(key, d)

    def test_to_dict_values(self):
        r = self._make(start=0.2, target=0.8, ramp_sec=30.0, offset_sec=15.0)
        d = r.to_dict()
        self.assertEqual(d["server_id"],    "s1")
        self.assertEqual(d["start_weight"], 0.2)
        self.assertEqual(d["target_weight"], 0.8)
        self.assertAlmostEqual(d["current_weight"], 0.5, delta=0.05)

    def test_zero_ramp_sec_immediately_complete(self):
        r = self._make(ramp_sec=0.0)
        self.assertTrue(r.is_complete())
        self.assertAlmostEqual(r.current_weight(), 1.0, places=6)


class TestStartTrafficRamp(unittest.TestCase):

    def setUp(self):
        self.cluster = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
        ])
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_returns_traffic_ramp(self):
        ramp = self.cluster.start_traffic_ramp("s1")
        self.assertIsInstance(ramp, TrafficRamp)

    def test_ramp_stored_in_dict(self):
        self.cluster.start_traffic_ramp("s1", initial_weight=0.1)
        self.assertIn("s1", self.cluster._ramps)

    def test_ramp_fields_correct(self):
        ramp = self.cluster.start_traffic_ramp(
            "s1", initial_weight=0.05, target_weight=1.0, ramp_sec=120.0
        )
        self.assertEqual(ramp.server_id,    "s1")
        self.assertEqual(ramp.start_weight, 0.05)
        self.assertEqual(ramp.target_weight, 1.0)
        self.assertEqual(ramp.ramp_sec,     120.0)

    def test_raises_for_unknown_server(self):
        with self.assertRaises(ValueError):
            self.cluster.start_traffic_ramp("does_not_exist")

    def test_raises_for_zero_initial_weight(self):
        with self.assertRaises(ValueError):
            self.cluster.start_traffic_ramp("s1", initial_weight=0.0)

    def test_raises_for_negative_initial_weight(self):
        with self.assertRaises(ValueError):
            self.cluster.start_traffic_ramp("s1", initial_weight=-0.1)

    def test_raises_for_zero_target_weight(self):
        with self.assertRaises(ValueError):
            self.cluster.start_traffic_ramp("s1", target_weight=0.0)

    def test_raises_for_negative_ramp_sec(self):
        with self.assertRaises(ValueError):
            self.cluster.start_traffic_ramp("s1", ramp_sec=-1.0)

    def test_replaces_existing_ramp(self):
        self.cluster.start_traffic_ramp("s1", initial_weight=0.1)
        self.cluster.start_traffic_ramp("s1", initial_weight=0.2)
        self.assertEqual(self.cluster._ramps["s1"].start_weight, 0.2)


class TestStopTrafficRamp(unittest.TestCase):

    def setUp(self):
        self.cluster = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
        ])
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_returns_true_when_ramp_exists(self):
        self.cluster.start_traffic_ramp("s1")
        result = self.cluster.stop_traffic_ramp("s1")
        self.assertTrue(result)

    def test_returns_false_when_no_ramp(self):
        result = self.cluster.stop_traffic_ramp("s1")
        self.assertFalse(result)

    def test_removes_ramp_from_dict(self):
        self.cluster.start_traffic_ramp("s1")
        self.cluster.stop_traffic_ramp("s1")
        self.assertNotIn("s1", self.cluster._ramps)

    def test_sets_server_weight_to_target(self):
        self.cluster.start_traffic_ramp("s1", target_weight=2.5)
        self.cluster.stop_traffic_ramp("s1")
        s = {sv.id: sv for sv in self.cluster.all_servers()}["s1"]
        self.assertAlmostEqual(s.weight, 2.5, places=6)


class TestCanaryStatus(unittest.TestCase):

    def setUp(self):
        self.cluster = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
        ])
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_empty_when_no_ramps(self):
        self.assertEqual(self.cluster.canary_status(), [])

    def test_returns_list(self):
        self.cluster.start_traffic_ramp("s1")
        self.assertIsInstance(self.cluster.canary_status(), list)

    def test_contains_ramp_dict(self):
        self.cluster.start_traffic_ramp("s1", initial_weight=0.1)
        status = self.cluster.canary_status()
        self.assertEqual(len(status), 1)
        self.assertEqual(status[0]["server_id"], "s1")

    def test_multiple_ramps(self):
        self.cluster.start_traffic_ramp("s1", initial_weight=0.1)
        self.cluster.start_traffic_ramp("s2", initial_weight=0.2)
        ids = {s["server_id"] for s in self.cluster.canary_status()}
        self.assertEqual(ids, {"s1", "s2"})

    def test_empty_after_stop(self):
        self.cluster.start_traffic_ramp("s1")
        self.cluster.stop_traffic_ramp("s1")
        self.assertEqual(self.cluster.canary_status(), [])


class TestWeightedRouting(unittest.TestCase):
    """Canary server gets proportionally less traffic."""

    def setUp(self):
        self.cluster = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
        ])
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def _route_n(self, n=2000):
        counts = collections.Counter()
        for _ in range(n):
            s = self.cluster.get_server()
            if s:
                counts[s.id] += 1
        return counts

    def test_canary_gets_less_traffic(self):
        # s1 = canary at 10% weight, s2 = normal at 100% weight
        # Expected: s1 gets ~9% (10/(10+100)) of requests
        self.cluster.start_traffic_ramp(
            "s1", initial_weight=0.1, target_weight=1.0, ramp_sec=3600.0
        )
        counts = self._route_n(2000)
        total = sum(counts.values())
        s1_fraction = counts["s1"] / total
        # s1 should get roughly 10/(10+100) ≈ 9% ±5%
        self.assertLess(s1_fraction, 0.25,
                        f"Canary got too much traffic: {s1_fraction:.2%}")
        self.assertGreater(s1_fraction, 0.01,
                           f"Canary got no traffic at all: {s1_fraction:.2%}")

    def test_full_weight_server_gets_more(self):
        self.cluster.start_traffic_ramp(
            "s1", initial_weight=0.1, target_weight=1.0, ramp_sec=3600.0
        )
        counts = self._route_n(2000)
        # s2 (weight=1.0) should get much more than s1 (weight=0.1)
        self.assertGreater(counts["s2"], counts["s1"])

    def test_equal_weights_roughly_equal(self):
        # No ramp -- both servers same weight, expect ~50/50
        counts = self._route_n(2000)
        total = sum(counts.values())
        for sid in ("s1", "s2"):
            fraction = counts[sid] / total
            self.assertGreater(fraction, 0.3,
                               f"{sid} got too little: {fraction:.2%}")
            self.assertLess(fraction, 0.7,
                            f"{sid} got too much: {fraction:.2%}")

    def test_all_servers_receive_traffic_during_ramp(self):
        self.cluster.start_traffic_ramp(
            "s1", initial_weight=0.05, target_weight=1.0, ramp_sec=3600.0
        )
        counts = self._route_n(3000)
        # Both servers must receive at least some traffic
        self.assertGreater(counts["s1"], 0)
        self.assertGreater(counts["s2"], 0)


class TestUpdateRamps(unittest.TestCase):
    """_update_ramps() cleans up completed ramps and sets server weight."""

    def setUp(self):
        self.cluster = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
        ])
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_completed_ramp_removed(self):
        # Create a ramp that is already complete (offset past ramp_sec)
        ramp = TrafficRamp(
            server_id="s1",
            start_weight=0.1,
            target_weight=1.0,
            ramp_sec=1.0,
            start_time=time.monotonic() - 999.0,  # way past completion
        )
        self.cluster._ramps["s1"] = ramp
        with self.cluster._lock:
            self.cluster._update_ramps()
        self.assertNotIn("s1", self.cluster._ramps)

    def test_server_weight_set_to_target_on_completion(self):
        ramp = TrafficRamp(
            server_id="s1",
            start_weight=0.1,
            target_weight=2.5,
            ramp_sec=1.0,
            start_time=time.monotonic() - 999.0,
        )
        self.cluster._ramps["s1"] = ramp
        with self.cluster._lock:
            self.cluster._update_ramps()
        s = {sv.id: sv for sv in self.cluster.all_servers()}["s1"]
        self.assertAlmostEqual(s.weight, 2.5, places=6)

    def test_incomplete_ramp_not_removed(self):
        self.cluster.start_traffic_ramp("s1", ramp_sec=3600.0)
        with self.cluster._lock:
            self.cluster._update_ramps()
        self.assertIn("s1", self.cluster._ramps)

    def test_no_crash_when_no_ramps(self):
        self.assertEqual(self.cluster._ramps, {})
        with self.cluster._lock:
            self.cluster._update_ramps()  # must not raise

    def test_round_robin_after_ramp_complete(self):
        # With ramp complete, _ramps should be empty -> round-robin resumes
        ramp = TrafficRamp(
            server_id="s1",
            start_weight=0.1,
            target_weight=1.0,
            ramp_sec=0.001,
            start_time=time.monotonic() - 1.0,
        )
        self.cluster._ramps["s1"] = ramp
        with self.cluster._lock:
            self.cluster._update_ramps()
        self.assertFalse(self.cluster._ramps)


class TestHealthReport(unittest.TestCase):

    def setUp(self):
        self.cluster = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
        ])
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_canary_ramps_key_present(self):
        report = self.cluster.health_report()
        self.assertIn("canary_ramps", report)

    def test_canary_ramps_empty_initially(self):
        report = self.cluster.health_report()
        self.assertEqual(report["canary_ramps"], [])

    def test_canary_ramps_shows_active_ramp(self):
        self.cluster.start_traffic_ramp("s1", initial_weight=0.1)
        report = self.cluster.health_report()
        ids = [r["server_id"] for r in report["canary_ramps"]]
        self.assertIn("s1", ids)


class TestMultipleRamps(unittest.TestCase):

    def setUp(self):
        self.cluster = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
            ("s3", "127.0.0.1", 8003),
        ])
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_two_concurrent_ramps(self):
        self.cluster.start_traffic_ramp("s1", initial_weight=0.1, ramp_sec=3600.0)
        self.cluster.start_traffic_ramp("s2", initial_weight=0.5, ramp_sec=3600.0)
        self.assertEqual(len(self.cluster._ramps), 2)

    def test_different_ramp_speeds(self):
        self.cluster.start_traffic_ramp("s1", initial_weight=0.1, ramp_sec=3600.0)
        self.cluster.start_traffic_ramp("s2", initial_weight=0.5, ramp_sec=1800.0)
        status = {s["server_id"]: s for s in self.cluster.canary_status()}
        self.assertAlmostEqual(status["s1"]["current_weight"], 0.1, delta=0.05)
        self.assertAlmostEqual(status["s2"]["current_weight"], 0.5, delta=0.05)


class TestThreadSafety(unittest.TestCase):

    def setUp(self):
        self.cluster = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
        ])
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_concurrent_start_stop_ramp(self):
        errors = []

        def starter():
            try:
                for _ in range(20):
                    self.cluster.start_traffic_ramp(
                        "s1", initial_weight=0.1, ramp_sec=3600.0
                    )
                    time.sleep(0.005)
            except Exception as e:
                errors.append(str(e))

        def stopper():
            try:
                for _ in range(20):
                    self.cluster.stop_traffic_ramp("s1")
                    time.sleep(0.005)
            except Exception as e:
                errors.append(str(e))

        def router():
            try:
                for _ in range(100):
                    self.cluster.get_server()
            except Exception as e:
                errors.append(str(e))

        threads = (
            [threading.Thread(target=starter) for _ in range(3)]
            + [threading.Thread(target=stopper) for _ in range(3)]
            + [threading.Thread(target=router)  for _ in range(4)]
        )
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [], f"Thread errors: {errors}")


if __name__ == "__main__":
    unittest.main()