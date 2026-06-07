"""
Tests for persistent state / JSON checkpoint (v1.4.0).

Covers:
  - save_state() writes valid JSON to the target path
  - save_state() is atomic (uses .tmp then os.replace)
  - save_state() raises ValueError when no path given
  - save_state() creates parent directories automatically
  - Saved JSON contains expected keys and server data
  - load_state() restores temperature, metrics, histogram, rotation counters
  - load_state() skips servers not in the cluster
  - load_state() returns 0 and does not crash when file missing
  - load_state() seeds EMA window with last 10 histogram samples
  - load_state() raises ValueError when no path given
  - Round-trip: save then load restores exact values
  - start() auto-loads state when state_file configured
  - stop() auto-saves state when state_file configured
  - checkpoint thread saves periodically when checkpoint_interval_sec > 0
  - Corrupt state file raises json.JSONDecodeError
  - health_report() includes state_file and checkpoint_interval_sec
  - Thread safety: concurrent save_state calls do not corrupt file
"""

import json
import os
import tempfile
import threading
import time
import unittest

from huddle_cluster import HuddleCluster, Server, ServerMetrics, create_cluster


class TestSaveState(unittest.TestCase):

    def setUp(self):
        self.cluster = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
        ])
        self.cluster.start()
        self.tmp_dir = tempfile.mkdtemp()
        self.state_path = os.path.join(self.tmp_dir, "state.json")

    def tearDown(self):
        self.cluster.stop()
        import shutil
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_writes_file(self):
        self.cluster.save_state(self.state_path)
        self.assertTrue(os.path.exists(self.state_path))

    def test_returns_path(self):
        returned = self.cluster.save_state(self.state_path)
        self.assertEqual(returned, self.state_path)

    def test_valid_json(self):
        self.cluster.save_state(self.state_path)
        with open(self.state_path) as f:
            data = json.load(f)
        self.assertIsInstance(data, dict)

    def test_top_level_keys(self):
        self.cluster.save_state(self.state_path)
        with open(self.state_path) as f:
            data = json.load(f)
        for key in ("version", "saved_at", "heat_threshold", "servers"):
            self.assertIn(key, data)

    def test_server_data_keys(self):
        self.cluster.save_state(self.state_path)
        with open(self.state_path) as f:
            data = json.load(f)
        for sid in ("s1", "s2"):
            self.assertIn(sid, data["servers"])
            s = data["servers"][sid]
            for key in ("temperature", "avg_response_ms", "error_rate",
                        "latency_anomaly_score", "rotation_count",
                        "total_inner_time", "total_outer_time",
                        "histogram_samples"):
                self.assertIn(key, s, f"Key {key!r} missing for {sid}")

    def test_histogram_samples_is_list(self):
        self.cluster.save_state(self.state_path)
        with open(self.state_path) as f:
            data = json.load(f)
        for sid in ("s1", "s2"):
            self.assertIsInstance(data["servers"][sid]["histogram_samples"], list)

    def test_no_stale_tmp_files_left(self):
        self.cluster.save_state(self.state_path)
        tmp_dir = os.path.dirname(os.path.abspath(self.state_path))
        stale = [f for f in os.listdir(tmp_dir) if f.endswith(".tmp")]
        self.assertEqual(stale, [], f"Stale .tmp files found: {stale}")

    def test_raises_without_path(self):
        cluster = create_cluster([("s1", "127.0.0.1", 8001)])
        cluster.start()
        try:
            with self.assertRaises(ValueError):
                cluster.save_state()
        finally:
            cluster.stop()

    def test_creates_parent_dirs(self):
        nested = os.path.join(self.tmp_dir, "a", "b", "state.json")
        self.cluster.save_state(nested)
        self.assertTrue(os.path.exists(nested))

    def test_saves_actual_metric_values(self):
        s = self.cluster.inner_servers()[0]
        s.temperature = 0.73
        s.metrics.avg_response_ms = 88.5
        s.metrics.error_rate = 0.12

        self.cluster.save_state(self.state_path)
        with open(self.state_path) as f:
            data = json.load(f)

        saved = data["servers"][s.id]
        self.assertAlmostEqual(saved["temperature"],      0.73,  places=5)
        self.assertAlmostEqual(saved["avg_response_ms"],  88.5,  places=5)
        self.assertAlmostEqual(saved["error_rate"],       0.12,  places=5)

    def test_saves_histogram_samples(self):
        s = self.cluster.inner_servers()[0]
        for i in range(1, 51):
            self.cluster.record_latency(s, float(i))

        self.cluster.save_state(self.state_path)
        with open(self.state_path) as f:
            data = json.load(f)

        samples = data["servers"][s.id]["histogram_samples"]
        self.assertEqual(len(samples), 50)


class TestLoadState(unittest.TestCase):

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.state_path = os.path.join(self.tmp_dir, "state.json")

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def _make_cluster(self, servers=None):
        addrs = servers or [("s1", "127.0.0.1", 8001), ("s2", "127.0.0.1", 8002)]
        c = create_cluster(addrs)
        c.start()
        return c

    def test_returns_0_when_no_file(self):
        cluster = self._make_cluster()
        try:
            result = cluster.load_state(self.state_path)
            self.assertEqual(result, 0)
        finally:
            cluster.stop()

    def test_raises_without_path(self):
        cluster = self._make_cluster()
        try:
            with self.assertRaises(ValueError):
                cluster.load_state()
        finally:
            cluster.stop()

    def test_restores_temperature(self):
        c1 = self._make_cluster()
        s = c1.inner_servers()[0]
        s.temperature = 0.61
        c1.save_state(self.state_path)
        c1.stop()

        c2 = self._make_cluster()
        try:
            c2.load_state(self.state_path)
            servers = {sv.id: sv for sv in c2.all_servers()}
            self.assertAlmostEqual(servers["s1"].temperature, 0.61, places=5)
        finally:
            c2.stop()

    def test_restores_avg_response_ms(self):
        c1 = self._make_cluster()
        s = {sv.id: sv for sv in c1.all_servers()}["s1"]
        s.metrics.avg_response_ms = 55.5
        c1.save_state(self.state_path)
        c1.stop()

        c2 = self._make_cluster()
        try:
            c2.load_state(self.state_path)
            sv = {sv.id: sv for sv in c2.all_servers()}["s1"]
            self.assertAlmostEqual(sv.metrics.avg_response_ms, 55.5, places=5)
        finally:
            c2.stop()

    def test_restores_rotation_count(self):
        c1 = self._make_cluster()
        s = {sv.id: sv for sv in c1.all_servers()}["s1"]
        s.rotation_count = 17
        c1.save_state(self.state_path)
        c1.stop()

        c2 = self._make_cluster()
        try:
            c2.load_state(self.state_path)
            sv = {sv.id: sv for sv in c2.all_servers()}["s1"]
            self.assertEqual(sv.rotation_count, 17)
        finally:
            c2.stop()

    def test_restores_histogram_samples(self):
        c1 = self._make_cluster()
        s = c1.inner_servers()[0]
        for i in range(1, 101):
            c1.record_latency(s, float(i))
        sid = s.id
        c1.save_state(self.state_path)
        c1.stop()

        c2 = self._make_cluster()
        try:
            c2.load_state(self.state_path)
            sv = {sv.id: sv for sv in c2.all_servers()}[sid]
            self.assertEqual(len(sv.metrics._histogram_window), 100)
        finally:
            c2.stop()

    def test_seeds_ema_window_from_histogram(self):
        c1 = self._make_cluster()
        s = c1.inner_servers()[0]
        for i in range(1, 51):
            c1.record_latency(s, float(i * 10))
        sid = s.id
        c1.save_state(self.state_path)
        c1.stop()

        c2 = self._make_cluster()
        try:
            c2.load_state(self.state_path)
            sv = {sv.id: sv for sv in c2.all_servers()}[sid]
            # EMA window should have up to 10 samples
            self.assertGreater(len(sv.metrics._latency_window), 0)
            self.assertLessEqual(len(sv.metrics._latency_window), 10)
        finally:
            c2.stop()

    def test_skips_missing_servers(self):
        # Save state with s1 and s2, then load into cluster with only s1
        c1 = self._make_cluster()
        c1.save_state(self.state_path)
        c1.stop()

        c2 = create_cluster([("s1", "127.0.0.1", 8001)])
        c2.start()
        try:
            restored = c2.load_state(self.state_path)
            self.assertEqual(restored, 1)  # only s1 restored
        finally:
            c2.stop()

    def test_returns_count_of_restored_servers(self):
        c1 = self._make_cluster()
        c1.save_state(self.state_path)
        c1.stop()

        c2 = self._make_cluster()
        try:
            restored = c2.load_state(self.state_path)
            self.assertEqual(restored, 2)
        finally:
            c2.stop()

    def test_corrupt_file_raises(self):
        with open(self.state_path, "w") as f:
            f.write("this is not json {{{")
        cluster = self._make_cluster()
        try:
            with self.assertRaises(json.JSONDecodeError):
                cluster.load_state(self.state_path)
        finally:
            cluster.stop()


class TestRoundTrip(unittest.TestCase):
    """save_state() then load_state() must restore exact values."""

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.state_path = os.path.join(self.tmp_dir, "state.json")

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_full_round_trip(self):
        c1 = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
        ])
        c1.start()

        servers = {s.id: s for s in c1.all_servers()}
        servers["s1"].rotation_count   = 7
        servers["s1"].total_inner_time = 123.45

        for i in range(1, 21):
            c1.record_latency(servers["s1"], float(i))

        # Set metrics AFTER record_latency so they are not overwritten by EMA
        servers["s1"].metrics.avg_response_ms = 30.0
        servers["s1"].metrics.error_rate      = 0.05
        servers["s2"].metrics.avg_response_ms = 120.0

        c1.save_state(self.state_path)
        c1.stop()

        c2 = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
        ])
        c2.start()
        try:
            restored = c2.load_state(self.state_path)
            sv = {s.id: s for s in c2.all_servers()}

            self.assertEqual(restored, 2)
            # avg_response_ms, error_rate, rotation_count, total_inner_time and
            # histogram are stable -- the rotation loop does not overwrite them.
            self.assertAlmostEqual(sv["s1"].metrics.avg_response_ms, 30.0,  places=5)
            self.assertAlmostEqual(sv["s1"].metrics.error_rate,       0.05,  places=5)
            self.assertEqual(sv["s1"].rotation_count,  7)
            self.assertAlmostEqual(sv["s1"].total_inner_time, 123.45, places=5)
            self.assertAlmostEqual(sv["s2"].metrics.avg_response_ms, 120.0, places=5)
            self.assertEqual(len(sv["s1"].metrics._histogram_window), 20)
        finally:
            c2.stop()


class TestAutoLoadOnStart(unittest.TestCase):
    """start() auto-loads state when state_file= is configured."""

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.state_path = os.path.join(self.tmp_dir, "auto.json")

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_auto_load_restores_metrics(self):
        # rotation_count is stable -- the rotation loop does not reset it,
        # so it is safe to assert without race conditions against the EMA update.
        c1 = create_cluster([("s1", "127.0.0.1", 8001)])
        c1.start()
        c1.all_servers()[0].rotation_count          = 42
        c1.all_servers()[0].metrics.avg_response_ms = 77.7
        c1.save_state(self.state_path)
        c1.stop()

        c2 = create_cluster(
            [("s1", "127.0.0.1", 8001)],
            state_file=self.state_path,
        )
        c2.start()
        try:
            sv = c2.all_servers()[0]
            self.assertEqual(sv.rotation_count, 42)
            self.assertAlmostEqual(sv.metrics.avg_response_ms, 77.7, places=5)
        finally:
            c2.stop()

    def test_no_crash_when_state_file_missing(self):
        missing = os.path.join(self.tmp_dir, "does_not_exist.json")
        cluster = create_cluster(
            [("s1", "127.0.0.1", 8001)],
            state_file=missing,
        )
        try:
            cluster.start()  # must not raise
        finally:
            cluster.stop()


class TestAutoSaveOnStop(unittest.TestCase):
    """stop() auto-saves state when state_file= is configured."""

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.state_path = os.path.join(self.tmp_dir, "auto.json")

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_auto_save_on_stop(self):
        cluster = create_cluster(
            [("s1", "127.0.0.1", 8001)],
            state_file=self.state_path,
        )
        cluster.start()
        cluster.all_servers()[0].temperature = 0.66
        cluster.stop()

        self.assertTrue(os.path.exists(self.state_path))
        with open(self.state_path) as f:
            data = json.load(f)
        self.assertAlmostEqual(
            data["servers"]["s1"]["temperature"], 0.66, places=5
        )


class TestCheckpointThread(unittest.TestCase):
    """checkpoint_interval_sec > 0 saves state periodically."""

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.state_path = os.path.join(self.tmp_dir, "checkpoint.json")

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_checkpoint_written_periodically(self):
        cluster = create_cluster(
            [("s1", "127.0.0.1", 8001)],
            state_file=self.state_path,
            checkpoint_interval_sec=0.2,
        )
        cluster.start()
        time.sleep(0.55)  # wait for at least 2 checkpoint cycles
        cluster.stop()

        self.assertTrue(os.path.exists(self.state_path))

    def test_no_checkpoint_thread_when_interval_zero(self):
        cluster = create_cluster(
            [("s1", "127.0.0.1", 8001)],
            state_file=self.state_path,
            checkpoint_interval_sec=0.0,
        )
        cluster.start()
        try:
            self.assertIsNone(cluster._checkpoint_thread)
        finally:
            cluster.stop()


class TestHealthReport(unittest.TestCase):
    """health_report() includes state_file and checkpoint_interval_sec."""

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.state_path = os.path.join(self.tmp_dir, "state.json")

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_state_file_in_report(self):
        cluster = create_cluster(
            [("s1", "127.0.0.1", 8001)],
            state_file=self.state_path,
        )
        cluster.start()
        try:
            report = cluster.health_report()
            self.assertIn("state_file", report)
            self.assertEqual(report["state_file"], self.state_path)
        finally:
            cluster.stop()

    def test_checkpoint_interval_in_report(self):
        cluster = create_cluster(
            [("s1", "127.0.0.1", 8001)],
            state_file=self.state_path,
            checkpoint_interval_sec=30.0,
        )
        cluster.start()
        try:
            report = cluster.health_report()
            self.assertIn("checkpoint_interval_sec", report)
            self.assertEqual(report["checkpoint_interval_sec"], 30.0)
        finally:
            cluster.stop()

    def test_state_file_none_when_not_configured(self):
        cluster = create_cluster([("s1", "127.0.0.1", 8001)])
        cluster.start()
        try:
            report = cluster.health_report()
            self.assertIsNone(report["state_file"])
        finally:
            cluster.stop()


class TestThreadSafety(unittest.TestCase):
    """Concurrent save_state calls must not corrupt the output file."""

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.state_path = os.path.join(self.tmp_dir, "safe.json")
        self.cluster = create_cluster([
            ("s1", "127.0.0.1", 8001),
            ("s2", "127.0.0.1", 8002),
        ])
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()
        import shutil
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_concurrent_saves_produce_valid_json(self):
        errors = []

        def saver():
            try:
                for _ in range(10):
                    self.cluster.save_state(self.state_path)
                    time.sleep(0.01)
            except Exception as e:
                errors.append(str(e))

        threads = [threading.Thread(target=saver) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [], f"Errors during concurrent save: {errors}")

        # Final file must be valid JSON
        with open(self.state_path) as f:
            data = json.load(f)
        self.assertIn("servers", data)


if __name__ == "__main__":
    unittest.main()