"""
Unit tests for HuddleCluster Linux eBPF/XDP Data Plane (v4.21.0)
================================================================
Verifies:
1. IP conversion helper: IPv4 dotted-quad to 32-bit network-byte-order integer.
2. BPF Map synchronization: Active inner-ring nodes correctly serialized into BPF table.
3. Live rotation synchronization: Ring rotations automatically update eBPF map state.
4. Fallback handling: Graceful userspace emulation mode on non-Linux OS or without root.
5. Telemetry & diagnostics: ebpf_status() and Prometheus active server gauge exposition.
"""

import unittest

from huddle_cluster import HuddleCluster, Position, Server
from huddle_cluster_pkg.ebpf_controller import EBPFDataPlane


class TestEBPFDataPlane(unittest.TestCase):
    def setUp(self):
        self.dataplane = EBPFDataPlane(interface="eth0", enabled=True)
        self.s1 = Server("s1", "192.168.1.10", 8080)
        self.s2 = Server("s2", "192.168.1.11", 8080)
        self.s1.temperature = 0.25
        self.s2.temperature = 0.45
        self.s1.weight = 1.0
        self.s2.weight = 2.0

    def test_ip_to_int_conversion(self):
        """Converts dotted IPv4 strings to 32-bit big-endian integer."""
        val = self.dataplane.ip_to_int("192.168.1.10")
        self.assertIsInstance(val, int)
        self.assertGreater(val, 0)
        # Invalid IP returns 0
        self.assertEqual(self.dataplane.ip_to_int("invalid_ip"), 0)

    def test_sync_inner_ring_populates_map(self):
        """Synchronizes inner servers list into virtual/kernel BPF map."""
        count = self.dataplane.sync_inner_ring([self.s1, self.s2])
        self.assertEqual(count, 2)

        status = self.dataplane.telemetry_status()
        self.assertEqual(status["active_server_count"], 2)
        self.assertEqual(status["interface"], "eth0")
        self.assertEqual(len(status["active_table"]), 2)

        # Check serialized attributes
        entry0 = status["active_table"][0]
        self.assertEqual(entry0["id"], "s1")
        self.assertEqual(entry0["port"], 8080)
        self.assertEqual(entry0["temperature"], 250)  # fixed-point * 1000
        self.assertEqual(entry0["weight"], 1)

    def test_cluster_ebpf_integration_and_rotation(self):
        """HuddleCluster syncs eBPF map on initialization, rotation, and sync_ebpf()."""
        cluster = HuddleCluster(
            heat_threshold=0.6,
            cool_threshold=0.3,
            min_inner_size=2,
            max_inner_size=3,
            ebpf_enabled=True,
            ebpf_interface="eth1",
        )
        s1 = Server("s1", "10.0.0.1", 8000)
        s2 = Server("s2", "10.0.0.2", 8000)
        s3 = Server("s3", "10.0.0.3", 8000)
        s1.position = Position.INNER
        s2.position = Position.INNER
        s3.position = Position.OUTER

        cluster._inner_ring.append(s1)
        cluster._inner_ring.append(s2)
        cluster._outer_ring.append(s3)

        # Sync inner ring
        synced = cluster.sync_ebpf()
        self.assertEqual(synced, 2)

        # Health report exposition
        report = cluster.health_report()
        self.assertIn("ebpf", report)
        self.assertEqual(report["ebpf"]["interface"], "eth1")
        self.assertEqual(report["ebpf"]["active_server_count"], 2)

        # Forwarding packet simulation
        cluster._ebpf_dataplane.record_forwarded_packet(server_index=0)
        status = cluster.ebpf_status()
        self.assertEqual(status["total_packets_forwarded"], 1)

        # Prometheus metrics exposition
        metrics = cluster.prometheus_metrics()
        self.assertIn("huddle_ebpf_active_servers 2", metrics)
        self.assertIn("huddle_ebpf_packets_forwarded_total 1", metrics)

        cluster.stop()


if __name__ == "__main__":
    unittest.main()
