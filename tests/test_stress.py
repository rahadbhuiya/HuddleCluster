"""
test_stress.py — Load & chaos tests for HuddleCluster
======================================================
Tests cover:
  - High-concurrency request routing (thread safety)
  - Rapid fluctuating metrics (no deadlock / crash)
  - Mass server registration and removal
  - Background rotation daemon under load
  - Chaos: random health toggles + metric spikes
  - Long-run fairness convergence
"""

import random
import threading
import time
import unittest

from huddle_cluster import (
    HuddleCluster,
    Server,
    ServerMetrics,
    create_cluster,
)



# Helpers


def _make_server(sid: str) -> Server:
    s = Server(id=sid, host="10.0.0.1", port=8080)
    s.metrics = ServerMetrics(cpu_usage=0.1, memory_usage=0.1)
    for _ in range(5):
        s.update_temperature()
    return s


def _cluster(n_inner=5, **kw) -> HuddleCluster:
    kw.setdefault("min_inner_size", 2)
    kw.setdefault("max_inner_size", n_inner)
    kw.setdefault("min_outer_dwell_sec", 0.0)
    kw.setdefault("rotation_cooldown_sec", 0.0)
    return HuddleCluster(**kw)



# Stress Tests


class TestConcurrentGetServer(unittest.TestCase):
    """Many threads calling get_server() simultaneously — no crash, no None."""

    NUM_THREADS  = 50
    REQUESTS_PER = 200

    def test_concurrent_routing(self):
        c = _cluster(n_inner=5)
        for i in range(8):
            c.add_server(_make_server(f"srv{i}"), force_inner=(i < 5))

        errors = []

        def worker():
            for _ in range(self.REQUESTS_PER):
                s = c.get_server()
                if s is None:
                    errors.append("get_server returned None")

        threads = [threading.Thread(target=worker)
                   for _ in range(self.NUM_THREADS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=15)

        self.assertEqual(errors, [],
                         f"Concurrent routing errors: {errors}")

    def test_no_deadlock_under_load(self):
        """Concurrent routing + concurrent rotation must not deadlock."""
        c = _cluster(n_inner=4)
        for i in range(6):
            c.add_server(_make_server(f"dl{i}"), force_inner=(i < 4))

        stop = threading.Event()

        def router():
            while not stop.is_set():
                c.get_server()

        def rotator():
            while not stop.is_set():
                # Randomly spike metrics then rotate
                for s in c.all_servers():
                    s.metrics.cpu_usage = random.uniform(0.0, 1.0)
                    s.update_temperature()
                c.rotate()
                time.sleep(0.005)

        threads = (
            [threading.Thread(target=router)  for _ in range(10)] +
            [threading.Thread(target=rotator) for _ in range(3)]
        )
        for t in threads:
            t.start()

        time.sleep(2.0)
        stop.set()

        for t in threads:
            t.join(timeout=5)
            self.assertFalse(t.is_alive(), "Thread still alive — possible deadlock")


class TestRapidMetricFluctuations(unittest.TestCase):
    """Metrics that spike up and down every cycle — cluster must stay stable."""

    def test_no_crash_on_metric_chaos(self):
        c = _cluster(n_inner=4)
        servers = []
        for i in range(8):
            s = _make_server(f"mc{i}")
            c.add_server(s, force_inner=(i < 4))
            servers.append(s)

        for cycle in range(100):
            for s in servers:
                # Randomly flip metrics
                s.metrics.cpu_usage    = random.random()
                s.metrics.memory_usage = random.random()
                s.metrics.error_rate   = random.uniform(0, 0.5)
                s.metrics.is_healthy   = random.random() > 0.1
                s.update_temperature()
            c.rotate()

        # Cluster should still have ≥ min_inner_size servers in inner
        self.assertGreaterEqual(len(c.inner_servers()), c.min_inner_size)

    def test_inner_never_exceeds_max(self):
        c = _cluster(n_inner=3)
        for i in range(10):
            c.add_server(_make_server(f"ex{i}"), force_inner=(i < 3))

        for _ in range(50):
            for s in c.all_servers():
                s.metrics.cpu_usage = random.uniform(0.0, 0.2)
                s.update_temperature()
            c.rotate()

        self.assertLessEqual(len(c.inner_servers()), c.max_inner_size)


class TestMassRegistrationAndRemoval(unittest.TestCase):
    """Add and remove many servers rapidly."""

    def test_add_many_servers(self):
        c = _cluster(n_inner=10)
        for i in range(100):
            c.add_server(_make_server(f"bulk{i}"))

        self.assertEqual(len(c.all_servers()), 100)

    def test_remove_all_but_one(self):
        c = _cluster(n_inner=5, min_inner_size=1)
        servers = [_make_server(f"r{i}") for i in range(10)]
        for s in servers:
            c.add_server(s, force_inner=True)

        # Remove all except the first
        for s in servers[1:]:
            c.remove_server(s.id)

        self.assertEqual(len(c.all_servers()), 1)

    def test_concurrent_add_remove(self):
        """Simultaneous adds and removes must not corrupt state."""
        c = _cluster(n_inner=5)
        for i in range(20):
            c.add_server(_make_server(f"base{i}"), force_inner=(i < 5))

        errors = []

        def adder():
            for i in range(50):
                try:
                    s = _make_server(f"add_{threading.get_ident()}_{i}")
                    c.add_server(s)
                except Exception as e:
                    errors.append(str(e))

        def remover():
            for _ in range(30):
                all_s = c.all_servers()
                if all_s:
                    target = random.choice(all_s)
                    try:
                        c.remove_server(target.id)
                    except Exception as e:
                        errors.append(str(e))
                time.sleep(0.001)

        threads = (
            [threading.Thread(target=adder)   for _ in range(5)] +
            [threading.Thread(target=remover) for _ in range(5)]
        )
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        self.assertEqual(errors, [], f"Concurrent add/remove errors: {errors}")


class TestBackgroundDaemon(unittest.TestCase):
    """The rotation daemon should run, rotate, and stop cleanly."""

    def test_daemon_starts_and_stops(self):
        c = _cluster(n_inner=3)
        for i in range(5):
            c.add_server(_make_server(f"bg{i}"), force_inner=(i < 3))

        call_count = {"n": 0}

        def on_rotation(event):
            call_count["n"] += 1

        c._on_rotation = on_rotation

        def updater(s):
            s.metrics.cpu_usage = random.uniform(0.0, 1.0)

        c._metrics_updater = updater

        c.start(rotation_interval_sec=0.05)
        time.sleep(0.5)
        c.stop(timeout=2.0)

        self.assertFalse(c._running, "Cluster should be stopped after stop()")

    def test_double_start_safe(self):
        c = _cluster()
        c.add_server(_make_server("ds1"), force_inner=True)
        c.start(rotation_interval_sec=0.1)
        c.start(rotation_interval_sec=0.1)   # should not raise
        c.stop()


class TestChaosMonkey(unittest.TestCase):
    """
    Randomised chaos: random health toggles, metric spikes, evictions,
    removals, additions over 3 seconds. Cluster must remain usable.
    """

    DURATION_SEC = 3.0

    def test_survive_chaos(self):
        c = _cluster(n_inner=4, min_inner_size=2)
        servers = [_make_server(f"chaos{i}") for i in range(10)]
        for i, s in enumerate(servers):
            c.add_server(s, force_inner=(i < 4))

        deadline = time.monotonic() + self.DURATION_SEC
        errors = []

        def chaos_worker():
            while time.monotonic() < deadline:
                try:
                    action = random.choice([
                        "spike", "cool", "health_flip",
                        "get", "rotate", "force_evict",
                    ])

                    all_s = c.all_servers()
                    if not all_s:
                        continue
                    target = random.choice(all_s)

                    if action == "spike":
                        target.metrics.cpu_usage    = random.uniform(0.8, 1.0)
                        target.metrics.memory_usage = random.uniform(0.8, 1.0)
                        target.update_temperature()

                    elif action == "cool":
                        target.metrics.cpu_usage    = random.uniform(0.0, 0.1)
                        target.metrics.memory_usage = random.uniform(0.0, 0.1)
                        target.update_temperature()

                    elif action == "health_flip":
                        target.metrics.is_healthy = random.random() > 0.2

                    elif action == "get":
                        s = c.get_server()
                        if s is None:
                            errors.append("get_server returned None during chaos")

                    elif action == "rotate":
                        c.rotate()

                    elif action == "force_evict":
                        c.force_evict(target.id)

                except Exception as e:
                    errors.append(f"{action}: {e}")

                time.sleep(random.uniform(0.0, 0.01))

        threads = [threading.Thread(target=chaos_worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=self.DURATION_SEC + 5)

        # After chaos, cluster should still be in a sane state
        report = c.health_report()
        self.assertIn(report["status"], ("healthy", "degraded"))
        self.assertEqual(
            errors, [],
            f"Chaos errors encountered:\n" + "\n".join(errors[:10]),
        )


class TestLongRunFairness(unittest.TestCase):
    """
    Over many rotation cycles with varying loads, fairness_score
    should stay below a reasonable threshold.
    """

    def test_fairness_convergence(self):
        c = _cluster(n_inner=4, min_inner_size=2)
        servers = [_make_server(f"fair{i}") for i in range(8)]
        for i, s in enumerate(servers):
            c.add_server(s, force_inner=(i < 4))

        phase = [0]

        for cycle in range(300):
            # Alternate: first half hot, second half cool
            for i, s in enumerate(servers):
                if i < 4:
                    load = 0.9 if (cycle // 50) % 2 == 0 else 0.1
                else:
                    load = 0.1 if (cycle // 50) % 2 == 0 else 0.9
                s.metrics.cpu_usage = load
                s.metrics.memory_usage = load * 0.8
                s.update_temperature()

            c.rotate()

        score = c.fairness_score()
        # After many rotations, fairness should be reasonable
        # (not perfectly zero because load isn't identical, but < 0.6)
        self.assertLess(score, 0.6,
                        f"Fairness score {score:.3f} too high after long run")


if __name__ == "__main__":
    unittest.main(verbosity=2)