"""
HuddleCluster - Real-Time Hardware Sensor Integration (CPU / GPU NVML)
v4.23.0

Reads physical silicon telemetry (CPU thermal zones, NVIDIA GPU NVML temperature,
VRAM saturation, and compute load) and fuses hardware thermal metrics directly into
the emperor penguin multi-ring eviction algorithm.
"""

from __future__ import annotations

import logging
import os
import platform
import subprocess
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

log = logging.getLogger("HuddleCluster.HardwareSensors")


@dataclass
class HardwareTelemetry:
    server_id:          str
    timestamp:          float
    cpu_temp_c:         float = 45.0  # Celsius
    cpu_util_pct:       float = 0.0   # 0.0 - 100.0%
    gpu_temp_c:         Optional[float] = None  # Celsius
    gpu_util_pct:       Optional[float] = None  # 0.0 - 100.0%
    vram_used_mb:       Optional[float] = None
    vram_total_mb:      Optional[float] = None
    vram_usage_ratio:   float = 0.0   # 0.0 - 1.0
    power_draw_w:       Optional[float] = None
    thermal_stress:     float = 0.0   # Normalized 0.0 - 1.0 hardware score


class HardwareSensorReader:
    """
    Collects real-time hardware telemetry from Linux sysfs, NVIDIA NVML, or OS APIs.
    """

    def __init__(
        self,
        cpu_throttle_temp_c: float = 85.0,
        gpu_throttle_temp_c: float = 82.0,
        temp_floor_c: float = 35.0,
    ) -> None:
        self.cpu_throttle_c: float = cpu_throttle_temp_c
        self.gpu_throttle_c: float = gpu_throttle_temp_c
        self.temp_floor_c:   float = temp_floor_c
        self.is_linux:       bool  = platform.system() == "Linux"
        self._nvml_available: bool = False
        self._init_nvml()

    def _init_nvml(self) -> None:
        """Attempt to initialize pynvml if installed."""
        try:
            import pynvml  # type: ignore
            pynvml.nvmlInit()
            self._nvml_available = True
            log.info("NVIDIA Management Library (NVML) initialized successfully.")
        except Exception:
            self._nvml_available = False

    def read_local_cpu_temperature(self) -> float:
        """Read host CPU temperature from Linux sysfs thermal zones or fallback."""
        if not self.is_linux:
            return 45.0  # Safe default on non-Linux OS

        # Check /sys/class/thermal/thermal_zone*/temp
        thermal_base = "/sys/class/thermal"
        if os.path.exists(thermal_base):
            try:
                for zone in os.listdir(thermal_base):
                    if zone.startswith("thermal_zone"):
                        path = os.path.join(thermal_base, zone, "temp")
                        if os.path.exists(path):
                            with open(path, "r") as f:
                                raw = f.read().strip()
                                if raw.isdigit():
                                    val = float(raw)
                                    return val / 1000.0 if val > 1000 else val
            except Exception as exc:
                log.debug(f"Failed reading thermal sysfs: {exc}")

        return 45.0

    def read_local_gpu_telemetry(self) -> Dict[str, Any]:
        """Read GPU temperature, utilization, and VRAM via NVML or nvidia-smi."""
        telemetry: Dict[str, Any] = {
            "gpu_temp_c": None,
            "gpu_util_pct": None,
            "vram_used_mb": None,
            "vram_total_mb": None,
            "vram_usage_ratio": 0.0,
            "power_draw_w": None,
        }

        if self._nvml_available:
            try:
                import pynvml
                handle = pynvml.nvmlDeviceGetHandleByIndex(0)
                temp = pynvml.nvmlDeviceGetTemperature(handle, pynvml.NVML_TEMPERATURE_GPU)
                util = pynvml.nvmlDeviceGetUtilizationRates(handle)
                mem = pynvml.nvmlDeviceGetMemoryInfo(handle)

                telemetry["gpu_temp_c"] = float(temp)
                telemetry["gpu_util_pct"] = float(util.gpu)
                telemetry["vram_used_mb"] = float(mem.used) / (1024 * 1024)
                telemetry["vram_total_mb"] = float(mem.total) / (1024 * 1024)
                if mem.total > 0:
                    telemetry["vram_usage_ratio"] = float(mem.used) / float(mem.total)
                return telemetry
            except Exception as exc:
                log.debug(f"NVML read failed: {exc}")

        # Fallback to nvidia-smi query if command exists
        try:
            res = subprocess.run(
                [
                    "nvidia-smi",
                    "--query-gpu=temperature.gpu,utilization.gpu,memory.used,memory.total,power.draw",
                    "--format=csv,noheader,nounits",
                ],
                capture_output=True,
                text=True,
                timeout=2.0,
            )
            if res.returncode == 0 and res.stdout.strip():
                parts = [p.strip() for p in res.stdout.strip().split(",")]
                if len(parts) >= 4:
                    telemetry["gpu_temp_c"] = float(parts[0])
                    telemetry["gpu_util_pct"] = float(parts[1])
                    telemetry["vram_used_mb"] = float(parts[2])
                    telemetry["vram_total_mb"] = float(parts[3])
                    if telemetry["vram_total_mb"] > 0:
                        telemetry["vram_usage_ratio"] = telemetry["vram_used_mb"] / telemetry["vram_total_mb"]
                if len(parts) >= 5:
                    telemetry["power_draw_w"] = float(parts[4])
        except Exception:
            pass

        return telemetry

    def calculate_hardware_stress(
        self,
        cpu_temp_c: float,
        cpu_util_pct: float = 0.0,
        gpu_temp_c: Optional[float] = None,
        vram_usage_ratio: float = 0.0,
    ) -> float:
        """
        Normalize hardware parameters into a single composite stress score in [0.0, 1.0].
        """
        # 1. CPU thermal stress
        cpu_span = max(1.0, self.cpu_throttle_c - self.temp_floor_c)
        cpu_norm = max(0.0, min(1.0, (cpu_temp_c - self.temp_floor_c) / cpu_span))

        # 2. CPU load factor
        load_norm = max(0.0, min(1.0, cpu_util_pct / 100.0))

        # Composite CPU score
        cpu_score = 0.7 * cpu_norm + 0.3 * load_norm

        # 3. GPU thermal & VRAM stress (if GPU active)
        if gpu_temp_c is not None:
            gpu_span = max(1.0, self.gpu_throttle_c - self.temp_floor_c)
            gpu_norm = max(0.0, min(1.0, (gpu_temp_c - self.temp_floor_c) / gpu_span))
            vram_norm = max(0.0, min(1.0, vram_usage_ratio))
            gpu_score = 0.6 * gpu_norm + 0.4 * vram_norm
            # Take highest stress between CPU and GPU
            return round(max(cpu_score, gpu_score), 4)

        return round(cpu_score, 4)


class HardwareTelemetryManager:
    """
    Maintains per-server hardware telemetry table and computes fused thermal adjustments.
    """

    def __init__(
        self,
        enabled: bool = True,
        sensor_weight: float = 0.35,
        cpu_throttle_c: float = 85.0,
        gpu_throttle_c: float = 82.0,
    ) -> None:
        self.enabled:        bool                   = enabled
        self.sensor_weight:  float                  = max(0.0, min(1.0, sensor_weight))
        self.reader:         HardwareSensorReader   = HardwareSensorReader(
            cpu_throttle_temp_c=cpu_throttle_c,
            gpu_throttle_temp_c=gpu_throttle_c,
        )
        self._lock:          threading.RLock        = threading.RLock()
        self._telemetry_map: Dict[str, HardwareTelemetry] = {}
        self._total_updates: int                    = 0

    def record_telemetry(
        self,
        server_id: str,
        cpu_temp_c: float,
        cpu_util_pct: float = 0.0,
        gpu_temp_c: Optional[float] = None,
        gpu_util_pct: Optional[float] = None,
        vram_used_mb: Optional[float] = None,
        vram_total_mb: Optional[float] = None,
        vram_usage_ratio: Optional[float] = None,
        power_draw_w: Optional[float] = None,
    ) -> HardwareTelemetry:
        """Ingest fresh hardware readings for a given backend server."""
        if not self.enabled:
            stress = 0.0
        else:
            calc_vram_ratio = vram_usage_ratio
            if calc_vram_ratio is None and vram_used_mb is not None and vram_total_mb and vram_total_mb > 0:
                calc_vram_ratio = vram_used_mb / vram_total_mb
            stress = self.reader.calculate_hardware_stress(
                cpu_temp_c=cpu_temp_c,
                cpu_util_pct=cpu_util_pct,
                gpu_temp_c=gpu_temp_c,
                vram_usage_ratio=calc_vram_ratio or 0.0,
            )

        data = HardwareTelemetry(
            server_id=server_id,
            timestamp=time.time(),
            cpu_temp_c=cpu_temp_c,
            cpu_util_pct=cpu_util_pct,
            gpu_temp_c=gpu_temp_c,
            gpu_util_pct=gpu_util_pct,
            vram_used_mb=vram_used_mb,
            vram_total_mb=vram_total_mb,
            vram_usage_ratio=calc_vram_ratio or 0.0,
            power_draw_w=power_draw_w,
            thermal_stress=stress,
        )

        with self._lock:
            self._telemetry_map[server_id] = data
            self._total_updates += 1

        return data

    def fuse_temperature(self, server_id: str, latency_temp: float) -> float:
        """
        Blends hardware thermal stress into the latency-based temperature:
        T_effective = (1 - weight) * T_latency + weight * T_hardware
        """
        if not self.enabled:
            return latency_temp

        with self._lock:
            telem = self._telemetry_map.get(server_id)
            if not telem:
                return latency_temp

            w = self.sensor_weight
            fused = (1.0 - w) * latency_temp + w * telem.thermal_stress
            return round(max(0.0, min(1.0, fused)), 4)

    def get_telemetry(self, server_id: str) -> Optional[HardwareTelemetry]:
        """Return latest hardware telemetry for a server."""
        with self._lock:
            return self._telemetry_map.get(server_id)

    def telemetry_status(self) -> Dict[str, Any]:
        """Diagnostic report of all monitored hardware endpoints."""
        with self._lock:
            servers = {}
            for s_id, t in self._telemetry_map.items():
                servers[s_id] = {
                    "cpu_temp_c":       t.cpu_temp_c,
                    "cpu_util_pct":     t.cpu_util_pct,
                    "gpu_temp_c":       t.gpu_temp_c,
                    "gpu_util_pct":     t.gpu_util_pct,
                    "vram_usage_ratio": round(t.vram_usage_ratio, 4),
                    "thermal_stress":   t.thermal_stress,
                    "last_updated_sec": round(time.time() - t.timestamp, 2),
                }

            return {
                "enabled":                  self.enabled,
                "sensor_weight":            self.sensor_weight,
                "total_telemetry_updates":  self._total_updates,
                "monitored_servers_count":  len(self._telemetry_map),
                "servers":                  servers,
            }
