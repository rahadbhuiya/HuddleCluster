"""
Unit tests for Real-Time Hardware Sensor Integration (CPU / GPU NVML).
v4.23.0
"""

import time
import unittest
from huddle_cluster import HuddleCluster, Server
from huddle_cluster_pkg.hardware_sensors import (
    HardwareSensorReader,
    HardwareTelemetryManager,
    HardwareTelemetry,
)


class TestHardwareSensors(unittest.TestCase):
    def setUp(self):
        self.reader = HardwareSensorReader(
            cpu_throttle_temp_c=85.0,
            gpu_throttle_temp_c=82.0,
            temp_floor_c=35.0,
        )

    def test_hardware_stress_calculation(self):
        # Baseline cool temps
        cool_stress = self.reader.calculate_hardware_stress(
            cpu_temp_c=40.0,
            cpu_util_pct=10.0,
        )
        self.assertLess(cool_stress, 0.25)

        # High CPU stress near throttle limit (85C)
        hot_cpu_stress = self.reader.calculate_hardware_stress(
            cpu_temp_c=85.0,
            cpu_util_pct=95.0,
        )
        self.assertGreaterEqual(hot_cpu_stress, 0.8)

        # High GPU temperature and VRAM saturation
        hot_gpu_stress = self.reader.calculate_hardware_stress(
            cpu_temp_c=45.0,
            cpu_util_pct=20.0,
            gpu_temp_c=82.0,
            vram_usage_ratio=0.95,
        )
        self.assertGreaterEqual(hot_gpu_stress, 0.8)

    def test_telemetry_manager_ingestion_and_fusion(self):
        manager = HardwareTelemetryManager(
            enabled=True,
            sensor_weight=0.5,
            cpu_throttle_c=85.0,
            gpu_throttle_c=82.0,
        )

        manager.record_telemetry(
            server_id="s1",
            cpu_temp_c=85.0,
            cpu_util_pct=90.0,
            gpu_temp_c=82.0,
            vram_used_mb=23000,
            vram_total_mb=24000,
        )

        status = manager.telemetry_status()
        self.assertEqual(status["total_telemetry_updates"], 1)
        self.assertIn("s1", status["servers"])
        self.assertGreaterEqual(status["servers"]["s1"]["thermal_stress"], 0.8)

        # Test temperature fusion:
        # Latency temp = 0.20, Hardware stress >= 0.80, weight = 0.50
        # Fused = 0.5 * 0.20 + 0.5 * stress >= 0.50
        fused = manager.fuse_temperature("s1", latency_temp=0.20)
        self.assertGreaterEqual(fused, 0.50)

    def test_hardware_overheat_triggers_eviction(self):
        cluster = HuddleCluster(
            heat_threshold=0.60,
            cool_threshold=0.30,
            min_inner_size=1,
            max_inner_size=3,
            rotation_cooldown_sec=0.0,
            min_outer_dwell_sec=0.0,
            hardware_sensors_enabled=True,
            hardware_sensor_weight=0.60,
        )

        s1 = Server("s1", "127.0.0.1", 8001, weight=1.0)
        s2 = Server("s2", "127.0.0.1", 8002, weight=1.0)
        cluster.add_server(s1, force_inner=True)
        cluster.add_server(s2, force_inner=True)

        s1.last_rotated = time.monotonic() - 10.0
        s1.temperature = 0.15  # Low latency temp

        # Record extreme silicon stress on s1 (GPU boiling at 90C + 98% VRAM)
        cluster.record_hardware_telemetry(
            server_id="s1",
            cpu_temp_c=85.0,
            gpu_temp_c=90.0,
            vram_usage_ratio=0.98,
        )

        # s1's temperature should now be fused above 0.60 heat threshold
        self.assertGreaterEqual(s1.temperature, 0.60)

        # Rotation should evict s1 purely due to hardware stress
        rotated = cluster.rotate()
        self.assertTrue(rotated)
        self.assertIn(s1, cluster.outer_servers())

    def test_prometheus_and_health_exposition(self):
        cluster = HuddleCluster(
            hardware_sensors_enabled=True,
        )
        cluster.record_hardware_telemetry(
            server_id="s1",
            cpu_temp_c=68.5,
            gpu_temp_c=74.2,
            vram_usage_ratio=0.65,
        )

        report = cluster.health_report()
        self.assertIn("hardware_sensors", report)
        self.assertTrue(report["hardware_sensors"]["enabled"])
        self.assertIn("s1", report["hardware_sensors"]["servers"])

        prom = cluster.prometheus_metrics()
        self.assertIn("huddle_hardware_telemetry_updates_total", prom)
        self.assertIn('huddle_server_cpu_temp_celsius{server="s1"} 68.5', prom)
        self.assertIn('huddle_server_gpu_temp_celsius{server="s1"} 74.2', prom)


if __name__ == "__main__":
    unittest.main()
