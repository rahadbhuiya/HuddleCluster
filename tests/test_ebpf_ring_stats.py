"""
Unit tests for EBPFDataPlane ring buffer stats and XDP counters (v4.24.0)
=========================================================================
Covers:
  1. RingStats dataclass — inner and outer ring packet/byte/temperature summary.
  2. XDPCounters dataclass — drop rate calculation.
  3. get_ring_stats()       — inner / outer split stats.
  4. get_xdp_counters()     — pass / tx / drop / redirect counters.
  5. record_bytes()         — per-server byte accumulation by server ID.
  6. record_xdp_action()    — bulk XDP counter update.
  7. record_latency_us()    — latency histogram bucketing.
  8. get_latency_histogram()— bucket labels and counts.
  9. get_server_bytes()     — per-server cumulative bytes.
 10. reset_counters()       — full telemetry reset without losing ring state.
 11. sync_outer_ring()      — outer ring mirroring.
 12. Backward compat        — telemetry_status() still works after v4.24.0 changes.
"""


import unittest

from huddle_cluster import Server
from huddle_cluster_pkg.ebpf_controller import EBPFDataPlane, RingStats, XDPCounters


def _make_server(sid: str, host: str, temp: float = 0.3, weight: float = 1.0) -> Server:
    s = Server(sid, host, 8080)
    s.temperature = temp
    s.weight = weight
    return s


class TestRingStatsDataclass(unittest.TestCase):
    """RingStats dataclass basic sanity."""

    def test_defaults(self):
        rs = RingStats(ring="inner")
        self.assertEqual(rs.ring, "inner")
        self.assertEqual(rs.server_count, 0)
        self.assertEqual(rs.packets_routed, 0)
        self.assertEqual(rs.bytes_routed, 0)
        self.assertEqual(rs.avg_temperature, 0.0)
        self.assertEqual(rs.server_ids, [])


    def test_custom_values(self):
        rs = RingStats(
            ring="outer",
            server_count=3,
            packets_routed=500,
            bytes_routed=256_000,
            avg_temperature=0.72,
            server_ids=["a", "b", "c"],
        )
        self.assertEqual(rs.server_count, 3)
        self.assertEqual(rs.bytes_routed, 256_000)
        self.assertIn("b", rs.server_ids)


class TestXDPCountersDataclass(unittest.TestCase):
    """XDPCounters dataclass basic sanity."""


    def test_defaults(self):
        xc = XDPCounters()
        self.assertEqual(xc.xdp_pass, 0)
        self.assertEqual(xc.drop_rate_pct, 0.0)

    def test_custom_values(self):
        xc = XDPCounters(xdp_pass=80, xdp_tx=10, xdp_drop=10, drop_rate_pct=10.0)
        self.assertEqual(xc.drop_rate_pct, 10.0)


class TestGetRingStats(unittest.TestCase):
    """get_ring_stats() returns correct inner/outer breakdown."""

    def setUp(self):
        self.dp = EBPFDataPlane(interface="eth0", enabled=False)
        self.s1 = _make_server("s1", "10.0.0.1", temp=0.2)
        self.s2 = _make_server("s2", "10.0.0.2", temp=0.4)
        self.s3 = _make_server("s3", "10.0.0.3", temp=0.8)

    def test_empty_rings(self):
        stats = self.dp.get_ring_stats()
        self.assertEqual(stats["inner"].server_count, 0)
        self.assertEqual(stats["outer"].server_count, 0)

    def test_inner_ring_server_count(self):
        self.dp.sync_inner_ring([self.s1, self.s2])
        stats = self.dp.get_ring_stats()
        self.assertEqual(stats["inner"].server_count, 2)
        self.assertIn("s1", stats["inner"].server_ids)
        self.assertIn("s2", stats["inner"].server_ids)

    def test_outer_ring_server_count(self):
        self.dp.sync_outer_ring([self.s3])
        stats = self.dp.get_ring_stats()
        self.assertEqual(stats["outer"].server_count, 1)
        self.assertIn("s3", stats["outer"].server_ids)

    def test_inner_avg_temperature(self):
        self.dp.sync_inner_ring([self.s1, self.s2])
        stats = self.dp.get_ring_stats()
        # s1.temp=0.2, s2.temp=0.4 → avg=0.3
        self.assertAlmostEqual(stats["inner"].avg_temperature, 0.3, places=3)

    def test_inner_packets_after_record(self):
        self.dp.sync_inner_ring([self.s1, self.s2])
        self.dp.record_forwarded_packet(server_index=0, byte_count=1500)
        self.dp.record_forwarded_packet(server_index=1, byte_count=800)
        stats = self.dp.get_ring_stats()
        self.assertEqual(stats["inner"].packets_routed, 2)
        self.assertEqual(stats["inner"].bytes_routed, 2300)

    def test_returns_ringstat_instances(self):
        stats = self.dp.get_ring_stats()
        self.assertIsInstance(stats["inner"], RingStats)
        self.assertIsInstance(stats["outer"], RingStats)


class TestGetXDPCounters(unittest.TestCase):
    """get_xdp_counters() returns correct counts and drop rate."""

    def setUp(self):
        self.dp = EBPFDataPlane(enabled=False)

    def test_initial_all_zero(self):
        xc = self.dp.get_xdp_counters()
        self.assertEqual(xc.xdp_pass, 0)
        self.assertEqual(xc.xdp_tx, 0)
        self.assertEqual(xc.xdp_drop, 0)
        self.assertEqual(xc.xdp_redirect, 0)
        self.assertEqual(xc.drop_rate_pct, 0.0)

    def test_record_xdp_action_accumulates(self):
        self.dp.record_xdp_action(xdp_pass=90, xdp_tx=5, xdp_drop=5)
        xc = self.dp.get_xdp_counters()
        self.assertEqual(xc.xdp_pass, 90)
        self.assertEqual(xc.xdp_drop, 5)

    def test_drop_rate_calculation(self):
        # 10 drop out of 100 total → 10%
        self.dp.record_xdp_action(xdp_pass=80, xdp_tx=10, xdp_drop=10)
        xc = self.dp.get_xdp_counters()
        self.assertAlmostEqual(xc.drop_rate_pct, 10.0, places=2)

    def test_zero_drop_rate_when_no_drops(self):
        self.dp.record_xdp_action(xdp_pass=100)
        xc = self.dp.get_xdp_counters()
        self.assertEqual(xc.drop_rate_pct, 0.0)

    def test_returns_xdpcounters_instance(self):
        xc = self.dp.get_xdp_counters()
        self.assertIsInstance(xc, XDPCounters)

    def test_record_forwarded_packet_increments_xdp_tx(self):
        self.dp.sync_inner_ring([_make_server("s1", "10.0.0.1")])
        self.dp.record_forwarded_packet(server_index=0, byte_count=100)
        xc = self.dp.get_xdp_counters()
        self.assertEqual(xc.xdp_tx, 1)


class TestRecordBytes(unittest.TestCase):
    """record_bytes() accumulates per-server byte counts."""

    def setUp(self):
        self.dp = EBPFDataPlane(enabled=False)
        self.s1 = _make_server("s1", "10.0.0.1")
        self.dp.sync_inner_ring([self.s1])


    def test_record_bytes_by_server_id(self):
        self.dp.record_bytes("s1", 4096)
        self.dp.record_bytes("s1", 1024)
        srv_bytes = self.dp.get_server_bytes()
        self.assertEqual(srv_bytes["s1"], 5120)

    def test_record_bytes_unknown_server(self):
        self.dp.record_bytes("ghost", 512)
        srv_bytes = self.dp.get_server_bytes()
        self.assertEqual(srv_bytes["ghost"], 512)


    def test_record_bytes_increments_total(self):
        self.dp.record_bytes("s1", 2000)
        status = self.dp.telemetry_status()
        self.assertEqual(status["total_bytes_forwarded"], 2000)



class TestLatencyHistogram(unittest.TestCase):
    """record_latency_us() and get_latency_histogram() correctness."""

    def setUp(self):
        self.dp = EBPFDataPlane(enabled=False)

    def test_bucket_keys_present(self):
        hist = self.dp.get_latency_histogram()
        self.assertIn("le_50us", hist)
        self.assertIn("le_100us", hist)
        self.assertIn("le_inf", hist)

    def test_sample_falls_in_correct_bucket(self):
        self.dp.record_latency_us(40.0)   # → le_50us
        self.dp.record_latency_us(80.0)   # → le_100us
        self.dp.record_latency_us(99999)  # → le_inf
        hist = self.dp.get_latency_histogram()
        self.assertEqual(hist["le_50us"], 1)
        self.assertEqual(hist["le_100us"], 1)
        self.assertEqual(hist["le_inf"], 1)

    def test_boundary_value_goes_into_bucket(self):
        self.dp.record_latency_us(50.0)  # exactly on boundary → le_50us
        hist = self.dp.get_latency_histogram()
        self.assertEqual(hist["le_50us"], 1)

    def test_multiple_samples_accumulate(self):
        for _ in range(5):
            self.dp.record_latency_us(30.0)  # all → le_50us
        hist = self.dp.get_latency_histogram()
        self.assertEqual(hist["le_50us"], 5)



class TestResetCounters(unittest.TestCase):
    """reset_counters() zeros all telemetry but preserves ring state."""

    def setUp(self):
        self.dp = EBPFDataPlane(enabled=False)
        self.s1 = _make_server("s1", "10.0.0.1")
        self.s2 = _make_server("s2", "10.0.0.2")
        self.dp.sync_inner_ring([self.s1, self.s2])

    def test_reset_clears_packets(self):
        self.dp.record_forwarded_packet(server_index=0, byte_count=500)
        self.dp.reset_counters()
        status = self.dp.telemetry_status()
        self.assertEqual(status["total_packets_forwarded"], 0)
        self.assertEqual(status["total_bytes_forwarded"], 0)

    def test_reset_clears_xdp_counters(self):
        self.dp.record_xdp_action(xdp_pass=50, xdp_drop=10)
        self.dp.reset_counters()
        xc = self.dp.get_xdp_counters()
        self.assertEqual(xc.xdp_pass, 0)
        self.assertEqual(xc.xdp_drop, 0)


    def test_reset_clears_histogram(self):
        self.dp.record_latency_us(30.0)
        self.dp.reset_counters()
        hist = self.dp.get_latency_histogram()
        self.assertTrue(all(v == 0 for v in hist.values()))

    def test_reset_preserves_ring_membership(self):
        self.dp.reset_counters()
        stats = self.dp.get_ring_stats()
        # Inner ring still has 2 servers after reset
        self.assertEqual(stats["inner"].server_count, 2)

    def test_reset_clears_server_bytes(self):
        self.dp.record_bytes("s1", 9999)
        self.dp.reset_counters()
        srv_bytes = self.dp.get_server_bytes()
        self.assertEqual(srv_bytes.get("s1", 0), 0)



class TestBackwardCompatibility(unittest.TestCase):
    """telemetry_status() and existing tests still work unchanged."""

    def test_telemetry_status_keys(self):
        dp = EBPFDataPlane(enabled=False)
        status = dp.telemetry_status()
        self.assertIn("ebpf_kernel_mode", status)
        self.assertIn("supported_os", status)
        self.assertIn("interface", status)
        self.assertIn("active_server_count", status)
        self.assertIn("total_packets_forwarded", status)
        self.assertIn("active_table", status)


    def test_ip_to_int_still_works(self):
        dp = EBPFDataPlane(enabled=False)
        val = dp.ip_to_int("192.168.1.10")
        self.assertIsInstance(val, int)
        self.assertGreater(val, 0)
        self.assertEqual(dp.ip_to_int("bad_ip"), 0)


if __name__ == "__main__":
    unittest.main()
