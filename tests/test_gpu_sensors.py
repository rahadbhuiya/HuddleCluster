"""
Unit tests for GPUSensorReader and multi-GPU support (v4.25.0)
==============================================================
All GPU tests use unittest.mock to patch pynvml — no physical GPU required.

Covers:
  1. _GPUDeviceStats dataclass defaults.
  2. GPUSensorReader.gpu_count() via NVML mock.
  3. GPUSensorReader.read_all_gpus() — single GPU, multi-GPU.
  4. GPUSensorReader.get_gpu_summary() — aggregated fields, empty case.
  5. GPUSensorReader.gpu_eviction_score() — temp + VRAM + power weighting.
  6. GPUSensorReader.should_evict_for_gpu() — threshold guards.
  7. HardwareTelemetry new fields: gpu_count, gpu_eviction_score.
  8. HardwareTelemetryManager.record_telemetry() with gpu_count / gpu_eviction_score.
  9. HardwareTelemetryManager.get_overheated_servers() filtering.
 10. HardwareTelemetryManager.telemetry_status() contains new fields.
 11. Backward compat — existing HardwareSensorReader.calculate_hardware_stress().
 12. No-GPU fallback — empty list / zero score when no GPU available.
"""

import unittest
from unittest.mock import MagicMock, patch
from dataclasses import fields as dc_fields

from huddle_cluster_pkg.hardware_sensors import (
    _GPUDeviceStats,
    GPUSensorReader,
    HardwareSensorReader,
    HardwareTelemetry,
    HardwareTelemetryManager,
)



# Helpers


def _mock_nvml_device(temp=60.0, util=50.0, used_mb=4096.0, total_mb=8192.0, pwr_mw=None):
    """Return a mock pynvml device handle with realistic attribute structure."""
    handle = MagicMock()
    util_rates = MagicMock()
    util_rates.gpu = util
    mem_info = MagicMock()
    mem_info.used  = int(used_mb * 1024 * 1024)
    mem_info.total = int(total_mb * 1024 * 1024)

    handle.__class__ = object  # avoid isinstance checks
    return handle, temp, util_rates, mem_info, pwr_mw



# 1. _GPUDeviceStats dataclass


class TestGPUDeviceStatsDataclass(unittest.TestCase):

    def test_defaults(self):
        d = _GPUDeviceStats(index=0)
        self.assertEqual(d.index, 0)
        self.assertEqual(d.name, "Unknown")
        self.assertEqual(d.temp_c, 0.0)
        self.assertEqual(d.util_pct, 0.0)
        self.assertEqual(d.vram_ratio, 0.0)
        self.assertIsNone(d.power_draw_w)

    def test_custom_values(self):
        d = _GPUDeviceStats(index=1, name="RTX 4090", temp_c=72.0, vram_ratio=0.85)
        self.assertEqual(d.name, "RTX 4090")
        self.assertAlmostEqual(d.vram_ratio, 0.85)



# 2–3. GPUSensorReader — gpu_count and read_all_gpus


class TestGPUSensorReaderNVML(unittest.TestCase):
    """Tests that mock pynvml to avoid needing a real GPU."""

    def _make_reader(self):
        reader = GPUSensorReader.__new__(GPUSensorReader)
        reader.gpu_throttle_c   = 82.0
        reader.vram_evict_ratio = 0.90
        reader.power_evict_w    = None
        reader._nvml_ok         = True
        return reader

    @patch("huddle_cluster_pkg.hardware_sensors.GPUSensorReader._read_nvml_all")
    def test_read_all_gpus_returns_list(self, mock_read):
        mock_read.return_value = [
            _GPUDeviceStats(index=0, name="RTX 3080", temp_c=70.0, util_pct=80.0,
                            vram_used_mb=8000, vram_total_mb=10240, vram_ratio=0.78),
        ]
        reader = self._make_reader()
        devices = reader.read_all_gpus()
        self.assertEqual(len(devices), 1)
        self.assertEqual(devices[0].name, "RTX 3080")
        self.assertAlmostEqual(devices[0].temp_c, 70.0)

    @patch("huddle_cluster_pkg.hardware_sensors.GPUSensorReader._read_nvml_all")
    def test_read_all_gpus_multi_gpu(self, mock_read):
        mock_read.return_value = [
            _GPUDeviceStats(index=0, temp_c=65.0, vram_ratio=0.5),
            _GPUDeviceStats(index=1, temp_c=75.0, vram_ratio=0.8),
        ]
        reader = self._make_reader()
        devices = reader.read_all_gpus()
        self.assertEqual(len(devices), 2)
        self.assertEqual(devices[1].index, 1)

    def test_gpu_count_from_nvml(self):
        """gpu_count() returns device count from mocked NVML."""
        import sys, types
        fake_nvml = types.ModuleType("pynvml")
        fake_nvml.nvmlDeviceGetCount = MagicMock(return_value=3)
        fake_nvml.nvmlInit = MagicMock()
        reader = self._make_reader()
        with patch.dict(sys.modules, {"pynvml": fake_nvml}):
            count = reader.gpu_count()
        self.assertEqual(count, 3)



# 4. get_gpu_summary


class TestGetGPUSummary(unittest.TestCase):

    def _reader_with_devices(self, devices):
        reader = GPUSensorReader.__new__(GPUSensorReader)
        reader.gpu_throttle_c   = 82.0
        reader.vram_evict_ratio = 0.90
        reader.power_evict_w    = None
        reader._nvml_ok         = False
        reader.read_all_gpus    = MagicMock(return_value=devices)
        return reader

    def test_empty_when_no_gpu(self):
        reader = self._reader_with_devices([])
        summary = reader.get_gpu_summary()
        self.assertEqual(summary["gpu_count"], 0)
        self.assertIsNone(summary["max_temp_c"])
        self.assertEqual(summary["devices"], [])

    def test_single_gpu_summary(self):
        devices = [_GPUDeviceStats(index=0, temp_c=70.0, util_pct=60.0,
                                   vram_used_mb=4096, vram_total_mb=8192,
                                   vram_ratio=0.5, power_draw_w=180.0)]
        reader = self._reader_with_devices(devices)
        s = reader.get_gpu_summary()
        self.assertEqual(s["gpu_count"], 1)
        self.assertEqual(s["max_temp_c"], 70.0)
        self.assertAlmostEqual(s["max_vram_ratio"], 0.5)
        self.assertEqual(s["total_power_w"], 180.0)

    def test_multi_gpu_max_temp(self):
        devices = [
            _GPUDeviceStats(index=0, temp_c=65.0, vram_ratio=0.4),
            _GPUDeviceStats(index=1, temp_c=79.0, vram_ratio=0.9),
        ]
        reader = self._reader_with_devices(devices)
        s = reader.get_gpu_summary()
        self.assertEqual(s["gpu_count"], 2)
        self.assertEqual(s["max_temp_c"], 79.0)
        self.assertAlmostEqual(s["max_vram_ratio"], 0.9)

    def test_avg_temp_calculation(self):
        devices = [
            _GPUDeviceStats(index=0, temp_c=60.0, vram_ratio=0.3),
            _GPUDeviceStats(index=1, temp_c=80.0, vram_ratio=0.7),
        ]
        reader = self._reader_with_devices(devices)
        s = reader.get_gpu_summary()
        self.assertAlmostEqual(s["avg_temp_c"], 70.0, places=1)

    def test_total_vram_sum(self):
        devices = [
            _GPUDeviceStats(index=0, vram_used_mb=4096, vram_total_mb=8192, vram_ratio=0.5),
            _GPUDeviceStats(index=1, vram_used_mb=6144, vram_total_mb=8192, vram_ratio=0.75),
        ]
        reader = self._reader_with_devices(devices)
        s = reader.get_gpu_summary()
        self.assertAlmostEqual(s["total_vram_mb"], 16384.0, places=0)
        self.assertAlmostEqual(s["used_vram_mb"],  10240.0, places=0)



# 5. gpu_eviction_score


class TestGPUEvictionScore(unittest.TestCase):

    def _reader_with_devices(self, devices):
        reader = GPUSensorReader.__new__(GPUSensorReader)
        reader.gpu_throttle_c   = 82.0
        reader.vram_evict_ratio = 0.90
        reader.power_evict_w    = None
        reader._nvml_ok         = False
        reader.read_all_gpus    = MagicMock(return_value=devices)
        return reader


    def test_zero_when_no_gpu(self):
        reader = self._reader_with_devices([])
        self.assertEqual(reader.gpu_eviction_score(), 0.0)

    def test_cool_gpu_low_score(self):
        devices = [_GPUDeviceStats(index=0, temp_c=40.0, vram_ratio=0.1)]
        reader = self._reader_with_devices(devices)
        score = reader.gpu_eviction_score()
        self.assertLess(score, 0.5)

    def test_hot_gpu_high_score(self):
        # At throttle temp (82°C) + high VRAM → score near 1.0
        devices = [_GPUDeviceStats(index=0, temp_c=82.0, vram_ratio=0.95)]
        reader = self._reader_with_devices(devices)
        score = reader.gpu_eviction_score()
        self.assertGreater(score, 0.8)

    def test_worst_gpu_dominates(self):
        devices = [
            _GPUDeviceStats(index=0, temp_c=40.0, vram_ratio=0.1),
            _GPUDeviceStats(index=1, temp_c=82.0, vram_ratio=0.95),
        ]
        reader = self._reader_with_devices(devices)
        score = reader.gpu_eviction_score()
        self.assertGreater(score, 0.8)


    def test_score_bounded_0_to_1(self):
        devices = [_GPUDeviceStats(index=0, temp_c=200.0, vram_ratio=2.0)]
        reader = self._reader_with_devices(devices)
        score = reader.gpu_eviction_score()
        self.assertLessEqual(score, 1.0)
        self.assertGreaterEqual(score, 0.0)



# 6. should_evict_for_gpu


class TestShouldEvictForGPU(unittest.TestCase):

    def _reader_with_devices(self, devices):
        reader = GPUSensorReader.__new__(GPUSensorReader)
        reader.gpu_throttle_c   = 82.0
        reader.vram_evict_ratio = 0.90
        reader.power_evict_w    = None
        reader._nvml_ok         = False
        reader.read_all_gpus    = MagicMock(return_value=devices)
        return reader

    def test_no_evict_when_cool(self):
        devices = [_GPUDeviceStats(index=0, temp_c=60.0, vram_ratio=0.5)]
        reader = self._reader_with_devices(devices)
        self.assertFalse(reader.should_evict_for_gpu())


    def test_evict_when_temp_exceeds_threshold(self):
        devices = [_GPUDeviceStats(index=0, temp_c=83.0, vram_ratio=0.4)]
        reader = self._reader_with_devices(devices)
        self.assertTrue(reader.should_evict_for_gpu())

    def test_evict_when_vram_saturated(self):
        devices = [_GPUDeviceStats(index=0, temp_c=60.0, vram_ratio=0.95)]
        reader = self._reader_with_devices(devices)
        self.assertTrue(reader.should_evict_for_gpu())

    def test_no_evict_when_no_gpu(self):
        reader = self._reader_with_devices([])
        self.assertFalse(reader.should_evict_for_gpu())



# 7. HardwareTelemetry new fields



class TestHardwareTelemetryNewFields(unittest.TestCase):

    def test_new_fields_have_defaults(self):
        t = HardwareTelemetry(server_id="s1", timestamp=0.0)
        self.assertEqual(t.gpu_count, 0)
        self.assertEqual(t.gpu_eviction_score, 0.0)

    def test_new_fields_settable(self):
        t = HardwareTelemetry(server_id="s1", timestamp=0.0,
                              gpu_count=2, gpu_eviction_score=0.85)
        self.assertEqual(t.gpu_count, 2)
        self.assertAlmostEqual(t.gpu_eviction_score, 0.85)




# 8–10. HardwareTelemetryManager

class TestHardwareTelemetryManagerV425(unittest.TestCase):

    def setUp(self):
        self.mgr = HardwareTelemetryManager(enabled=True, sensor_weight=0.3)


    def test_record_telemetry_stores_gpu_count(self):
        t = self.mgr.record_telemetry(
            "gpu-server-1", cpu_temp_c=50.0, gpu_count=4, gpu_eviction_score=0.72
        )
        self.assertEqual(t.gpu_count, 4)
        self.assertAlmostEqual(t.gpu_eviction_score, 0.72)

    def test_telemetry_status_contains_gpu_fields(self):
        self.mgr.record_telemetry(
            "s1", cpu_temp_c=55.0, gpu_temp_c=70.0,
            gpu_count=1, gpu_eviction_score=0.6,
        )
        status = self.mgr.telemetry_status()
        s1 = status["servers"]["s1"]
        self.assertIn("gpu_count", s1)
        self.assertIn("gpu_eviction_score", s1)
        self.assertEqual(s1["gpu_count"], 1)

    def test_get_overheated_servers_empty_when_all_cool(self):
        self.mgr.record_telemetry("s1", cpu_temp_c=45.0, gpu_eviction_score=0.3)
        self.mgr.record_telemetry("s2", cpu_temp_c=50.0, gpu_eviction_score=0.5)
        hot = self.mgr.get_overheated_servers(gpu_score_threshold=0.75)
        self.assertEqual(hot, [])

    def test_get_overheated_servers_returns_hot_servers(self):
        self.mgr.record_telemetry("s1", cpu_temp_c=45.0, gpu_eviction_score=0.3)
        self.mgr.record_telemetry("s2", cpu_temp_c=70.0, gpu_eviction_score=0.88)
        self.mgr.record_telemetry("s3", cpu_temp_c=60.0, gpu_eviction_score=0.76)
        hot = self.mgr.get_overheated_servers(gpu_score_threshold=0.75)
        self.assertIn("s2", hot)
        self.assertIn("s3", hot)
        self.assertNotIn("s1", hot)

    def test_get_overheated_servers_custom_threshold(self):
        self.mgr.record_telemetry("s1", cpu_temp_c=50.0, gpu_eviction_score=0.60)
        hot_strict = self.mgr.get_overheated_servers(gpu_score_threshold=0.80)
        hot_loose  = self.mgr.get_overheated_servers(gpu_score_threshold=0.55)
        self.assertEqual(hot_strict, [])
        self.assertIn("s1", hot_loose)



# 11. Backward compatibility — HardwareSensorReader


class TestHardwareSensorReaderBackwardCompat(unittest.TestCase):

    def setUp(self):
        self.reader = HardwareSensorReader()

    def test_calculate_hardware_stress_cpu_only(self):
        score = self.reader.calculate_hardware_stress(cpu_temp_c=60.0, cpu_util_pct=50.0)
        self.assertGreater(score, 0.0)
        self.assertLessEqual(score, 1.0)

    def test_calculate_hardware_stress_with_gpu(self):
        score = self.reader.calculate_hardware_stress(
            cpu_temp_c=50.0, gpu_temp_c=80.0, vram_usage_ratio=0.9
        )
        self.assertGreater(score, 0.5)

    def test_read_local_cpu_temperature_returns_float(self):
        temp = self.reader.read_local_cpu_temperature()
        self.assertIsInstance(temp, float)
        self.assertGreater(temp, 0.0)




# 12. No-GPU fallback


class TestNoGPUFallback(unittest.TestCase):

    def test_gpu_eviction_score_zero_no_gpu(self):
        reader = GPUSensorReader.__new__(GPUSensorReader)
        reader.gpu_throttle_c   = 82.0
        reader.vram_evict_ratio = 0.90
        reader.power_evict_w    = None
        reader._nvml_ok         = False
        reader.read_all_gpus    = MagicMock(return_value=[])
        self.assertEqual(reader.gpu_eviction_score(), 0.0)

    def test_should_evict_false_no_gpu(self):
        reader = GPUSensorReader.__new__(GPUSensorReader)
        reader.gpu_throttle_c   = 82.0
        reader.vram_evict_ratio = 0.90
        reader.power_evict_w    = None
        reader._nvml_ok         = False
        reader.read_all_gpus    = MagicMock(return_value=[])
        self.assertFalse(reader.should_evict_for_gpu())

    def test_get_gpu_summary_empty_no_gpu(self):
        reader = GPUSensorReader.__new__(GPUSensorReader)
        reader.gpu_throttle_c   = 82.0
        reader.vram_evict_ratio = 0.90
        reader.power_evict_w    = None
        reader._nvml_ok         = False
        reader.read_all_gpus    = MagicMock(return_value=[])
        summary = reader.get_gpu_summary()
        self.assertEqual(summary["gpu_count"], 0)
        self.assertEqual(summary["devices"], [])


if __name__ == "__main__":
    unittest.main()
