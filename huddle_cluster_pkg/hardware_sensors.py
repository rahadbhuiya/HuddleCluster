"""
HuddleCluster - Real-Time Hardware Sensor Integration (CPU / GPU NVML)
v4.25.0

Reads physical silicon telemetry (CPU thermal zones, NVIDIA GPU NVML temperature,
VRAM saturation, and compute load) and fuses hardware thermal metrics directly into
the emperor penguin multi-ring eviction algorithm.

New in v4.25.0:
  - GPUSensorReader: dedicated multi-GPU reader class with per-device stats.
  - Multi-GPU support: reads all available GPUs (not just index 0).
  - gpu_count(): returns number of detected NVIDIA GPUs.
  - get_gpu_summary(): aggregated stats across all GPUs (max temp, total VRAM, etc.).
  - gpu_eviction_score(): single normalized [0,1] score fusing temp + VRAM + power.
  - should_evict_for_gpu(): boolean guard — True when any GPU exceeds threshold.
  - HardwareTelemetry gains gpu_count and gpu_eviction_score fields.
  - HardwareTelemetryManager gains get_overheated_servers() helper.
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



# Telemetry dataclass


@dataclass
class HardwareTelemetry:
    server_id:          str
    timestamp:          float
    cpu_temp_c:         float = 45.0   # Celsius
    cpu_util_pct:       float = 0.0    # 0.0 – 100.0 %
    gpu_temp_c:         Optional[float] = None   # Hottest GPU (Celsius)
    gpu_util_pct:       Optional[float] = None   # Busiest GPU utilization
    vram_used_mb:       Optional[float] = None
    vram_total_mb:      Optional[float] = None
    vram_usage_ratio:   float = 0.0    # 0.0 – 1.0
    power_draw_w:       Optional[float] = None
    thermal_stress:     float = 0.0    # Normalized 0.0 – 1.0 composite score
    # New in v4.25.0
    gpu_count:          int   = 0      # Number of NVIDIA GPUs detected
    gpu_eviction_score: float = 0.0    # GPU-only eviction pressure score [0,1]



# GPUSensorReader  (new in v4.25.0)


@dataclass
class _GPUDeviceStats:
    """Raw per-device stats from NVML or nvidia-smi."""
    index:          int
    name:           str   = "Unknown"
    temp_c:         float = 0.0
    util_pct:       float = 0.0
    vram_used_mb:   float = 0.0
    vram_total_mb:  float = 0.0
    vram_ratio:     float = 0.0
    power_draw_w:   Optional[float] = None


class GPUSensorReader:
    """
    Dedicated multi-GPU reader that queries every detected NVIDIA GPU.

    Uses pynvml when available, falls back to nvidia-smi CSV output.
    All methods are safe to call on non-GPU or non-Linux hosts — they
    return empty lists / zero values instead of raising.

    New in v4.25.0.
    """

    def __init__(
        self,
        gpu_throttle_temp_c: float = 82.0,
        vram_evict_ratio: float = 0.90,
        power_evict_w: Optional[float] = None,
    ) -> None:
        """
        Args:
            gpu_throttle_temp_c: Temperature (°C) above which a GPU is
                considered overheated.  Default 82 °C.
            vram_evict_ratio:    VRAM utilization fraction above which a GPU
                is considered memory-saturated.  Default 0.90 (90 %).
            power_evict_w:       Optional watt ceiling.  When set, exceeding
                this triggers the eviction flag.  Default None (disabled).
        """
        self.gpu_throttle_c   = gpu_throttle_temp_c
        self.vram_evict_ratio = vram_evict_ratio
        self.power_evict_w    = power_evict_w
        self._nvml_ok: bool   = False
        self._init_nvml()

    
    # Initialisation
    

    def _init_nvml(self) -> None:
        try:
            import pynvml  # type: ignore
            pynvml.nvmlInit()
            self._nvml_ok = True
            count = pynvml.nvmlDeviceGetCount()
            log.info("NVML initialized — %d GPU(s) detected.", count)
        except Exception:
            self._nvml_ok = False

    
    # Public API
    

    def gpu_count(self) -> int:
        """Return number of NVIDIA GPUs visible to NVML or nvidia-smi."""
        if self._nvml_ok:
            try:
                import pynvml
                return pynvml.nvmlDeviceGetCount()
            except Exception:
                pass
        return self._smi_gpu_count()

    def read_all_gpus(self) -> List[_GPUDeviceStats]:
        """
        Read per-device stats for every detected GPU.

        Returns:
            List of :class:`_GPUDeviceStats`, one per GPU, ordered by index.
            Empty list when no GPU is available.
        """
        if self._nvml_ok:
            devices = self._read_nvml_all()
            if devices:
                return devices
        return self._read_smi_all()

    def get_gpu_summary(self) -> Dict[str, Any]:
        """
        Aggregate per-device readings into a cluster-friendly summary dict.

        Returns::

            {
                "gpu_count":        2,
                "max_temp_c":       79.0,
                "avg_temp_c":       71.5,
                "max_util_pct":     94.0,
                "total_vram_mb":    16384.0,
                "used_vram_mb":     13107.2,
                "max_vram_ratio":   0.90,
                "total_power_w":    210.5,
                "devices":          [ {...}, ... ],
            }
        """
        devices = self.read_all_gpus()
        if not devices:
            return {
                "gpu_count": 0,
                "max_temp_c": None,
                "avg_temp_c": None,
                "max_util_pct": None,
                "total_vram_mb": 0.0,
                "used_vram_mb": 0.0,
                "max_vram_ratio": 0.0,
                "total_power_w": None,
                "devices": [],
            }

        temps     = [d.temp_c for d in devices]
        utils     = [d.util_pct for d in devices]
        powers    = [d.power_draw_w for d in devices if d.power_draw_w is not None]
        vram_used = sum(d.vram_used_mb for d in devices)
        vram_tot  = sum(d.vram_total_mb for d in devices)
        max_vram  = max(d.vram_ratio for d in devices)

        return {
            "gpu_count":     len(devices),
            "max_temp_c":    max(temps),
            "avg_temp_c":    round(sum(temps) / len(temps), 2),
            "max_util_pct":  max(utils),
            "total_vram_mb": round(vram_tot, 2),
            "used_vram_mb":  round(vram_used, 2),
            "max_vram_ratio": round(max_vram, 4),
            "total_power_w": round(sum(powers), 2) if powers else None,
            "devices": [
                {
                    "index":        d.index,
                    "name":         d.name,
                    "temp_c":       d.temp_c,
                    "util_pct":     d.util_pct,
                    "vram_used_mb": round(d.vram_used_mb, 2),
                    "vram_total_mb": round(d.vram_total_mb, 2),
                    "vram_ratio":   round(d.vram_ratio, 4),
                    "power_draw_w": d.power_draw_w,
                }
                for d in devices
            ],
        }

    def gpu_eviction_score(self) -> float:
        """
        Return a single normalized [0.0, 1.0] eviction-pressure score that
        fuses the worst-case temperature, VRAM saturation, and power draw
        across all GPUs.

        Score components (applied to the hottest / most-loaded device):
          - 50 % GPU temperature normalized against throttle threshold
          - 35 % VRAM usage ratio
          - 15 % power draw (if data available, otherwise weight shifted to temp)

        Returns:
            0.0  — no GPU or all GPUs completely idle / cool.
            1.0  — GPU at or beyond throttle threshold with VRAM fully saturated.
        """
        devices = self.read_all_gpus()
        if not devices:
            return 0.0

        scores: List[float] = []
        for d in devices:
            temp_norm  = max(0.0, min(1.0, d.temp_c / max(1.0, self.gpu_throttle_c)))
            vram_norm  = max(0.0, min(1.0, d.vram_ratio))

            if d.power_draw_w is not None and self.power_evict_w:
                pwr_norm = max(0.0, min(1.0, d.power_draw_w / self.power_evict_w))
                score    = 0.50 * temp_norm + 0.35 * vram_norm + 0.15 * pwr_norm
            else:
                score    = 0.60 * temp_norm + 0.40 * vram_norm

            scores.append(score)

        return round(max(scores), 4)

    def should_evict_for_gpu(self) -> bool:
        """
        Return True when any GPU exceeds the temperature OR VRAM threshold,
        indicating the host should be rotated to the outer cooling ring.

        Returns:
            True  if eviction is recommended due to GPU thermal / VRAM pressure.
            False if all GPUs are within safe operating limits.
        """
        devices = self.read_all_gpus()
        for d in devices:
            if d.temp_c >= self.gpu_throttle_c:
                return True
            if d.vram_ratio >= self.vram_evict_ratio:
                return True
            if self.power_evict_w and d.power_draw_w and d.power_draw_w >= self.power_evict_w:
                return True
        return False

    
    # NVML backend
    

    def _read_nvml_all(self) -> List[_GPUDeviceStats]:
        try:
            import pynvml
            count   = pynvml.nvmlDeviceGetCount()
            devices = []
            for idx in range(count):
                handle = pynvml.nvmlDeviceGetHandleByIndex(idx)
                name   = pynvml.nvmlDeviceGetName(handle)
                if isinstance(name, bytes):
                    name = name.decode()
                temp   = float(pynvml.nvmlDeviceGetTemperature(
                    handle, pynvml.NVML_TEMPERATURE_GPU
                ))
                util   = pynvml.nvmlDeviceGetUtilizationRates(handle)
                mem    = pynvml.nvmlDeviceGetMemoryInfo(handle)
                used   = float(mem.used) / (1024 * 1024)
                total  = float(mem.total) / (1024 * 1024)
                ratio  = (used / total) if total > 0 else 0.0

                try:
                    pwr = float(pynvml.nvmlDeviceGetPowerUsage(handle)) / 1000.0
                except Exception:
                    pwr = None

                devices.append(_GPUDeviceStats(
                    index=idx,
                    name=name,
                    temp_c=temp,
                    util_pct=float(util.gpu),
                    vram_used_mb=used,
                    vram_total_mb=total,
                    vram_ratio=round(ratio, 4),
                    power_draw_w=pwr,
                ))
            return devices
        except Exception as exc:
            log.debug("NVML multi-GPU read failed: %s", exc)
            return []

    
    # nvidia-smi fallback backend
    

    def _smi_gpu_count(self) -> int:
        try:
            res = subprocess.run(
                ["nvidia-smi", "--list-gpus"],
                capture_output=True, text=True, timeout=2.0,
            )
            if res.returncode == 0:
                return sum(1 for line in res.stdout.splitlines() if line.strip())
        except Exception:
            pass
        return 0

    def _read_smi_all(self) -> List[_GPUDeviceStats]:
        try:
            res = subprocess.run(
                [
                    "nvidia-smi",
                    "--query-gpu=index,name,temperature.gpu,utilization.gpu,"
                    "memory.used,memory.total,power.draw",
                    "--format=csv,noheader,nounits",
                ],
                capture_output=True, text=True, timeout=3.0,
            )
            if res.returncode != 0 or not res.stdout.strip():
                return []

            devices: List[_GPUDeviceStats] = []
            for line in res.stdout.strip().splitlines():
                parts = [p.strip() for p in line.split(",")]
                if len(parts) < 6:
                    continue
                idx        = int(parts[0])
                name       = parts[1]
                temp       = float(parts[2])
                util       = float(parts[3])
                used       = float(parts[4])
                total      = float(parts[5])
                ratio      = (used / total) if total > 0 else 0.0
                pwr: Optional[float] = None
                if len(parts) >= 7:
                    try:
                        pwr = float(parts[6])
                    except ValueError:
                        pass
                devices.append(_GPUDeviceStats(
                    index=idx, name=name,
                    temp_c=temp, util_pct=util,
                    vram_used_mb=used, vram_total_mb=total,
                    vram_ratio=round(ratio, 4),
                    power_draw_w=pwr,
                ))
            return devices
        except Exception as exc:
            log.debug("nvidia-smi read failed: %s", exc)
            return []



# HardwareSensorReader  (CPU + single-GPU — unchanged API, updated internals)


class HardwareSensorReader:
    """
    Collects real-time hardware telemetry from Linux sysfs, NVIDIA NVML, or OS APIs.

    For multi-GPU workloads prefer :class:`GPUSensorReader` directly.
    This class internally delegates GPU reads to GPUSensorReader for consistency.
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
        self._gpu_reader = GPUSensorReader(gpu_throttle_temp_c=gpu_throttle_temp_c)

    @property
    def _nvml_available(self) -> bool:
        return self._gpu_reader._nvml_ok

    def read_local_cpu_temperature(self) -> float:
        """Read host CPU temperature from Linux sysfs thermal zones or fallback."""
        if not self.is_linux:
            return 45.0

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
                log.debug("Failed reading thermal sysfs: %s", exc)

        return 45.0

    def read_local_gpu_telemetry(self) -> Dict[str, Any]:
        """Read GPU temperature, utilization, and VRAM (hottest GPU only)."""
        summary = self._gpu_reader.get_gpu_summary()
        return {
            "gpu_temp_c":       summary["max_temp_c"],
            "gpu_util_pct":     summary["max_util_pct"],
            "vram_used_mb":     summary["used_vram_mb"] if summary["gpu_count"] else None,
            "vram_total_mb":    summary["total_vram_mb"] if summary["gpu_count"] else None,
            "vram_usage_ratio": summary["max_vram_ratio"],
            "power_draw_w":     summary["total_power_w"],
        }

    def calculate_hardware_stress(
        self,
        cpu_temp_c: float,
        cpu_util_pct: float = 0.0,
        gpu_temp_c: Optional[float] = None,
        vram_usage_ratio: float = 0.0,
    ) -> float:
        """Normalize hardware parameters into a composite stress score [0.0, 1.0]."""
        cpu_span  = max(1.0, self.cpu_throttle_c - self.temp_floor_c)
        cpu_norm  = max(0.0, min(1.0, (cpu_temp_c - self.temp_floor_c) / cpu_span))
        load_norm = max(0.0, min(1.0, cpu_util_pct / 100.0))
        cpu_score = 0.7 * cpu_norm + 0.3 * load_norm

        if gpu_temp_c is not None:
            gpu_span  = max(1.0, self.gpu_throttle_c - self.temp_floor_c)
            gpu_norm  = max(0.0, min(1.0, (gpu_temp_c - self.temp_floor_c) / gpu_span))
            vram_norm = max(0.0, min(1.0, vram_usage_ratio))
            gpu_score = 0.6 * gpu_norm + 0.4 * vram_norm
            return round(max(cpu_score, gpu_score), 4)

        return round(cpu_score, 4)



# HardwareTelemetryManager


class HardwareTelemetryManager:
    """
    Maintains per-server hardware telemetry table and computes fused thermal adjustments.

    New in v4.25.0:
        - get_overheated_servers(): returns list of server IDs whose GPU eviction
          score exceeds a given threshold.
    """

    def __init__(
        self,
        enabled: bool = True,
        sensor_weight: float = 0.35,
        cpu_throttle_c: float = 85.0,
        gpu_throttle_c: float = 82.0,
    ) -> None:
        self.enabled:        bool                 = enabled
        self.sensor_weight:  float                = max(0.0, min(1.0, sensor_weight))
        self.reader: HardwareSensorReader         = HardwareSensorReader(
            cpu_throttle_temp_c=cpu_throttle_c,
            gpu_throttle_temp_c=gpu_throttle_c,
        )
        self._lock:          threading.RLock      = threading.RLock()
        self._telemetry_map: Dict[str, HardwareTelemetry] = {}
        self._total_updates: int                  = 0

    
    # Ingestion
    

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
        gpu_count: int = 0,
        gpu_eviction_score: float = 0.0,
    ) -> HardwareTelemetry:
        """Ingest fresh hardware readings for a given backend server."""
        if not self.enabled:
            stress = 0.0
        else:
            calc_vram = vram_usage_ratio
            if calc_vram is None and vram_used_mb is not None and vram_total_mb and vram_total_mb > 0:
                calc_vram = vram_used_mb / vram_total_mb
            stress = self.reader.calculate_hardware_stress(
                cpu_temp_c=cpu_temp_c,
                cpu_util_pct=cpu_util_pct,
                gpu_temp_c=gpu_temp_c,
                vram_usage_ratio=calc_vram or 0.0,
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
            vram_usage_ratio=calc_vram or 0.0,
            power_draw_w=power_draw_w,
            thermal_stress=stress,
            gpu_count=gpu_count,
            gpu_eviction_score=gpu_eviction_score,
        )

        with self._lock:
            self._telemetry_map[server_id] = data
            self._total_updates += 1

        return data

    
    # Fusion & query
    

    def fuse_temperature(self, server_id: str, latency_temp: float) -> float:
        """
        Blend hardware thermal stress into the latency-based temperature::

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

    def get_overheated_servers(self, gpu_score_threshold: float = 0.75) -> List[str]:
        """
        Return a list of server IDs whose GPU eviction score exceeds the threshold.

        Use this to proactively rotate GPU-heavy servers to the outer ring before
        latency spikes appear.

        Args:
            gpu_score_threshold: Eviction score above which a server is considered
                overheated.  Default 0.75.

        Returns:
            List of server_id strings (may be empty).

        Example::

            hot = manager.get_overheated_servers(threshold=0.80)
            for sid in hot:
                cluster.force_evict(sid)
        """
        with self._lock:
            return [
                sid
                for sid, t in self._telemetry_map.items()
                if t.gpu_eviction_score >= gpu_score_threshold
            ]

    
    # Diagnostics
    

    def telemetry_status(self) -> Dict[str, Any]:
        """Diagnostic report of all monitored hardware endpoints."""
        with self._lock:
            servers = {}
            for s_id, t in self._telemetry_map.items():
                servers[s_id] = {
                    "cpu_temp_c":           t.cpu_temp_c,
                    "cpu_util_pct":         t.cpu_util_pct,
                    "gpu_temp_c":           t.gpu_temp_c,
                    "gpu_util_pct":         t.gpu_util_pct,
                    "vram_usage_ratio":     round(t.vram_usage_ratio, 4),
                    "thermal_stress":       t.thermal_stress,
                    "gpu_count":            t.gpu_count,
                    "gpu_eviction_score":   t.gpu_eviction_score,
                    "last_updated_sec":     round(time.time() - t.timestamp, 2),
                }

            return {
                "enabled":                  self.enabled,
                "sensor_weight":            self.sensor_weight,
                "total_telemetry_updates":  self._total_updates,
                "monitored_servers_count":  len(self._telemetry_map),
                "servers":                  servers,
            }
