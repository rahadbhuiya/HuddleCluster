"""
HuddleCluster — Penguin-inspired Self-Organizing Server Load Balancer
======================================================================
Inspired by how emperor penguins rotate in a huddle to share warmth,
this DSA ensures fair, self-regulating load distribution across servers.

Inner Ring  → Active servers handling requests (warm)
Outer Ring  → Resting servers recovering from load (cold)

Rotation Rules:
  - Overheated inner server  → self-evicts to outer ring
  - Coolest outer server     → automatically moves to inner ring
  - Guarantees fairness over time (every server gets rest)
  - No central coordinator needed — threshold-driven, self-organizing

Author : Rahad Bhuiya (inspired by Penguin Biology)
Version: 1.4.1
License: MIT

Changelog v1.3.0
-----------------
  NEW  Absolute latency floor (absolute_latency_floor_ms) -- evict servers
       exceeding this absolute latency regardless of relative anomaly score.
       Guards against majority degradation where median rises above acceptable.
  NEW  Cold start protection (cold_start_sec) -- new servers warm up in outer
       ring for configurable period before inner-ring promotion.
  NEW  Weighted server capacity (weight on Server) -- servers with higher
       weight tolerate more load before eviction. weight=2.0 needs 2x heat.
  NEW  Adaptive thresholds (adaptive_thresholds=True) -- heat/cool thresholds
       auto-adjust based on cluster P95 latency history.
  NEW  Prometheus exporter -- prometheus_metrics() returns /metrics text.
  NEW  Gossip protocol (GossipAgent) -- UDP multicast temperature sharing
       for distributed multi-node deployments.

Changelog v1.2.0
-----------------
  FIX  Relative latency anomaly scoring — temperature now uses
       (server_avg / cluster_avg) ratio instead of (ms / 5000).
       A 5x slower server now scores anomaly=1.0 regardless of baseline.
       Previously 60ms/5000=0.0018 — never triggered eviction.
  FIX  Fairness score now measures inner-ring servers only.
       Outer ring intentionally rests — comparing them made score always bad.
  NEW  ServerMetrics.update_latency_anomaly(cluster_avg_ms)
  NEW  ServerMetrics.latency_anomaly_score field
  IMPROVED  record_latency() now computes cluster-wide avg and updates
            all anomaly scores for accurate relative comparison.

Changelog v1.1.0
-----------------
  NEW  record_latency()     — real-time latency feedback loop so the cluster
                               detects slow servers automatically without an
                               external metrics_updater. Fixes the core gap
                               identified in benchmark testing.
  NEW  get_server_context() — context manager that auto-records latency after
                               each request; zero-boilerplate integration.
  NEW  ServerMetrics.window_avg_ms — rolling 50-sample window for stable EMA.
  FIX  Temperature formula now blends latency from record_latency() so a
       server that becomes slow is evicted within 1–2 rotation cycles.
  FIX  Benchmark showed HuddleCluster performed worse on slow-server scenario
       because temperature never updated without explicit metrics_updater.
       record_latency() closes this gap.
  IMPROVED  health_report() now includes per-server p95 latency.
  IMPROVED  EMA_ALPHA tunable per-instance via constructor kwarg.
"""

from __future__ import annotations

import heapq
import json
import logging
import math
import socket
import statistics
import struct
import threading
import time
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Generator, List, Optional

#  Logging Setup 

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] HuddleCluster │ %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("huddle")


#  Enums & Data Classes 


class Position(Enum):
    INNER = "inner"   # Active — taking requests
    OUTER = "outer"   # Resting — cooling down


class EvictionReason(Enum):
    OVERHEATED       = "overheated"
    MANUAL           = "manual"
    HEALTH_FAIL      = "health_fail"
    ABSOLUTE_LATENCY = "absolute_latency"   # v1.3.0: exceeded floor threshold


@dataclass
class ServerMetrics:
    """
    Live metrics snapshot for a server.

    Two ways to populate:
      1. External updater — set fields directly via your metrics_updater callback.
      2. Auto-feedback    — call cluster.record_latency(server, ms) after each
                            request; avg_response_ms is updated automatically.

    NEW v1.1.0: window_avg_ms computed from a rolling 10-sample window so
    latency spikes don't immediately inflate temperature (same spirit as EMA).
    """
    cpu_usage:             float = 0.0   # 0.0–1.0
    memory_usage:          float = 0.0   # 0.0–1.0
    active_connections:    int   = 0
    avg_response_ms:       float = 0.0   # set externally or via record_latency()
    error_rate:            float = 0.0   # 0.0–1.0 (errors / total_requests)
    is_healthy:            bool  = True  # False = immediately evict
    latency_anomaly_score: float = 0.0   # v1.2.0: relative slowness vs cluster avg

    # Rolling window for latency — internal use; not part of public API
    _latency_window: deque = field(
        default_factory=lambda: deque(maxlen=10), repr=False
    )

    def record_latency(self, ms: float) -> None:
        """Push one latency sample into the rolling window and refresh avg_response_ms."""
        self._latency_window.append(ms)
        if self._latency_window:
            self.avg_response_ms = statistics.mean(self._latency_window)

    def update_latency_anomaly(self, cluster_avg_ms: float) -> None:
        """
        v1.2.0 — Relative latency anomaly score.

        Computes how much slower THIS server is vs the cluster average.
        Uses ratio-based scoring instead of absolute ms so slow servers
        are detected regardless of baseline latency magnitude.

        anomaly = clamp((self_avg / cluster_avg - 1.0) / 4.0, 0, 1)

        Examples (cluster_avg = 12ms):
          self=12ms  → ratio=1.0x → anomaly=0.00  (normal)
          self=24ms  → ratio=2.0x → anomaly=0.50  (2x slower — warm)
          self=36ms  → ratio=3.0x → anomaly=1.00  (3x slower → max heat)
          self=60ms  → ratio=5.0x → anomaly=1.00  (5x slower → clamped)
        """
        if cluster_avg_ms <= 0 or self.avg_response_ms <= 0:
            self.latency_anomaly_score = 0.0
            return
        ratio = self.avg_response_ms / cluster_avg_ms
        # Scale: 1x=0.0, 2x=0.5, 3x+=1.0 (clamped)
        # More sensitive than /4: a server 3x slower triggers eviction quickly.
        self.latency_anomaly_score = max(0.0, min(1.0, (ratio - 1.0) / 2.0))

    def p95_latency(self) -> float:
        """95th-percentile latency from the rolling window, or 0.0 if no data."""
        w = list(self._latency_window)
        if not w:
            return 0.0
        w.sort()
        idx = max(0, int(len(w) * 0.95) - 1)
        return w[idx]


@dataclass
class RotationEvent:
    timestamp:   float
    server_id:   str
    direction:   str          # "inner->outer" or "outer->inner"
    reason:      str
    temperature: float


#  Server 


class Server:
    """
    Represents one server node inside the HuddleCluster.

    Temperature is a composite score (0.0–1.0):
      higher = more loaded = hotter = should rest in outer ring.

    FIX (Oscillation): Temperature uses Exponential Moving Average (EMA)
    so a single spike doesn't immediately trigger eviction.

    NEW v1.1.0: avg_response_ms now contributes via record_latency() feedback
    so slow servers self-evict without needing an external metrics_updater.
    """

    # Weight coefficients for temperature calculation
    # v1.2.0: latency_anomaly_score is the primary eviction signal (0.70).
    # Formula: raw = W_CPU*cpu + W_MEM*mem + W_CONN*conn + W_RESP*anomaly + W_ERR*err
    # With anomaly=1.0 (server 3x+ slower than cluster): raw=0.70 -> EMA -> eviction.
    # CPU/mem weights kept small — often unavailable in cloud/container envs.
    _W_CPU  = 0.10
    _W_MEM  = 0.05
    _W_CONN = 0.10
    _W_RESP = 0.70   # latency_anomaly_score — relative slowness vs cluster avg
    _W_ERR  = 0.05

    # EMA smoothing factor — higher = more reactive, lower = more stable
    # Can be overridden per-instance via HuddleCluster(ema_alpha=...)
    _EMA_ALPHA = 0.25

    def __init__(self, id: str, host: str, port: int, weight: float = 1.0):
        """
        weight (v1.3.0): capacity multiplier. A server with weight=2.0 needs
        to reach 2x the base heat_threshold before eviction. Use for larger
        instances that can handle proportionally more load.
        """
        if weight <= 0:
            raise ValueError("weight must be > 0")
        self.id     = id
        self.host   = host
        self.port   = port
        self.weight = weight   # v1.3.0: capacity multiplier

        self.metrics:     ServerMetrics = ServerMetrics()
        self.position:    Position      = Position.OUTER
        self.temperature: float         = 0.0

        self.last_rotated:     float = time.monotonic()
        self.total_inner_time: float = 0.0
        self.total_outer_time: float = 0.0
        self.rotation_count:   int   = 0

        self._consecutive_evictions: int   = 0
        self._cold_until:            float = 0.0   # v1.3.0: cold-start timestamp
        self._lock = threading.Lock()

    def is_cold_start(self) -> bool:
        """v1.3.0: True if server is still in cold-start protection period."""
        return time.monotonic() < self._cold_until

    def effective_heat_threshold(self, base_threshold: float) -> float:
        """
        v1.3.0: Weighted eviction threshold.
        weight=2.0 -> server needs temp >= base_threshold*2.0 to be evicted.
        """
        return min(1.0, base_threshold * self.weight)

    #  Temperature 

    def update_temperature(self) -> float:
        """
        Recalculate temperature using EMA.

        FIX (Oscillation/Flapping):
          Raw score is smoothed through EMA so transient spikes
          don't cause immediate eviction → re-entry loops.

        NEW v1.1.0: avg_response_ms is now populated by record_latency()
          so this formula reacts to real observed latency automatically.
        """
        m = self.metrics
        # Use relative anomaly score if available (set by record_latency),
        # otherwise fall back to absolute ms score for external metrics_updater compat.
        latency_score = (
            m.latency_anomaly_score
            if m.latency_anomaly_score > 0
            else min(m.avg_response_ms / 2_000.0, 1.0)
        )
        raw = (
            m.cpu_usage                                * self._W_CPU  +
            m.memory_usage                             * self._W_MEM  +
            min(m.active_connections / 1_000.0, 1.0)  * self._W_CONN +
            latency_score                              * self._W_RESP +
            m.error_rate                               * self._W_ERR
        )
        raw = max(0.0, min(1.0, raw))  # clamp to [0, 1]

        with self._lock:
            self.temperature = (
                self._EMA_ALPHA * raw
                + (1.0 - self._EMA_ALPHA) * self.temperature
            )
        return self.temperature

    def is_overheated(self, threshold: float) -> bool:
        return self.temperature >= self.effective_heat_threshold(threshold)

    def is_cooled(self, cooldown_threshold: float) -> bool:
        return self.temperature <= cooldown_threshold

    #  Heap ordering (min-heap by temperature in outer ring) 

    def __lt__(self, other: "Server") -> bool:
        return self.temperature < other.temperature

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Server) and self.id == other.id

    def __hash__(self) -> int:
        return hash(self.id)

    def __repr__(self) -> str:
        return (
            f"Server(id={self.id!r}, "
            f"pos={self.position.value}, "
            f"temp={self.temperature:.3f})"
        )


#  HuddleCluster 


class HuddleCluster:
    """
    Penguin-inspired self-organizing server cluster.

    ┌──────────────────────────────────────────────────────────┐
    │                     HuddleCluster                        │
    │                                                          │
    │   ┌─ Inner Ring ──────────────────────────────────────┐  │
    │   │  [S1] ──► [S2] ──► [S3]  (circular, round-robin)  │  │
    │   │           ↑                    │                  │  │
    │   │      cooled, enters       overheated, exits       │  │
    │   └───────────────────────────────│───────────────────┘  │
    │                                   ▼                      │
    │   ┌─ Outer Ring (min-heap by temp) ───────────────────┐  │
    │   │  [S4 temp=0.1] [S5 temp=0.2]  (resting/cooling)   │  │
    │   └───────────────────────────────────────────────────┘  │
    └──────────────────────────────────────────────────────────┘

    Key Fixes Applied
    -----------------
    1. Thundering Herd   → max evictions per cycle capped at 1/3 of inner ring
    2. Oscillation       → EMA smoothing on temperature (in Server class)
    3. Flapping          → hysteresis gap + minimum outer dwell time
    4. Lock Contention   → RLock with fine-grained critical sections
    5. Metrics Staleness → EMA already handles this
    6. Memory Leak       → rotation log bounded + circular buffer
    7. Empty Inner Ring  → emergency fallback server selection
    8. Back-off          → exponential back-off for repeat evictions
    9. Blind Temperature → NEW: record_latency() feedback loop so cluster
                           detects slow servers without external metrics_updater
    """

    # Default tuning constants
    DEFAULT_HEAT_THRESHOLD    = 0.55   # Above this → evict to outer
    DEFAULT_COOL_THRESHOLD    = 0.30   # Below this → pull to inner
    # Gap between thresholds is intentional — prevents flapping
    # Do NOT set cool_threshold close to heat_threshold

    DEFAULT_MIN_INNER         = 2
    DEFAULT_MAX_INNER         = 5
    DEFAULT_ROTATION_COOLDOWN = 5.0    # Seconds between rotations for one server
    DEFAULT_MIN_OUTER_DWELL   = 10.0   # Minimum seconds in outer before re-entry
    DEFAULT_ROTATION_INTERVAL = 1.0    # Background thread interval (seconds)
    DEFAULT_EMA_ALPHA         = 0.60   # v1.2.0: raised for faster convergence
    MAX_ROTATION_LOG          = 1_000  # Circular buffer size

    def __init__(
        self,
        heat_threshold:            float    = DEFAULT_HEAT_THRESHOLD,
        cool_threshold:            float    = DEFAULT_COOL_THRESHOLD,
        min_inner_size:            int      = DEFAULT_MIN_INNER,
        max_inner_size:            int      = DEFAULT_MAX_INNER,
        rotation_cooldown_sec:     float    = DEFAULT_ROTATION_COOLDOWN,
        min_outer_dwell_sec:       float    = DEFAULT_MIN_OUTER_DWELL,
        ema_alpha:                 float    = DEFAULT_EMA_ALPHA,
        # v1.3.0 new parameters
        absolute_latency_floor_ms: Optional[float] = None,
        cold_start_sec:            float    = 0.0,
        adaptive_thresholds:       bool     = False,
        gossip_agent:              Optional["GossipAgent"] = None,
        metrics_updater:           Optional[Callable[[Server], None]] = None,
        on_rotation:               Optional[Callable[[RotationEvent], None]] = None,
    ):
        #  Validation 
        if cool_threshold >= heat_threshold:
            raise ValueError(
                f"cool_threshold ({cool_threshold}) must be "
                f"strictly less than heat_threshold ({heat_threshold}). "
                f"Gap prevents flapping."
            )
        if min_inner_size < 1:
            raise ValueError("min_inner_size must be >= 1")
        if max_inner_size < min_inner_size:
            raise ValueError("max_inner_size must be >= min_inner_size")
        if not (0.0 < ema_alpha <= 1.0):
            raise ValueError("ema_alpha must be in (0, 1]")

        self.heat_threshold        = heat_threshold
        self.cool_threshold        = cool_threshold
        self.min_inner_size        = min_inner_size
        self.max_inner_size        = max_inner_size
        self.rotation_cooldown_sec = rotation_cooldown_sec
        self.min_outer_dwell_sec   = min_outer_dwell_sec
        self.ema_alpha             = ema_alpha
        self.absolute_latency_floor_ms = absolute_latency_floor_ms
        self.cold_start_sec            = cold_start_sec
        self._gossip_agent             = gossip_agent
        self._metrics_updater          = metrics_updater
        self._on_rotation              = on_rotation

        # Adaptive thresholds controller (v1.3.0)
        self._adaptive: Optional[AdaptiveThresholdController] = (
            AdaptiveThresholdController(
                base_heat=heat_threshold,
                base_cool=cool_threshold,
            ) if adaptive_thresholds else None
        )

        # P95 tracking window for adaptive thresholds + Prometheus
        self._p95_window: deque = deque(maxlen=100)

        #  Data Structures 
        self._inner_ring: deque[Server] = deque()
        # Outer ring is a min-heap: coolest server always at index 0
        self._outer_ring: list[Server]  = []

        # FIX (Memory Leak): bounded circular buffer
        self._rotation_log: deque[RotationEvent] = deque(
            maxlen=self.MAX_ROTATION_LOG
        )

        #  Concurrency 
        # FIX (Lock Contention): use RLock (reentrant) so internal
        # helpers can acquire without deadlock
        self._lock = threading.RLock()

        #  Background Thread 
        self._running = False
        self._rotation_thread: Optional[threading.Thread] = None

        log.info(
            "HuddleCluster initialized -- "
            f"heat={heat_threshold}, cool={cool_threshold}, "
            f"inner=[{min_inner_size}..{max_inner_size}], "
            f"ema_alpha={ema_alpha}, "
            f"floor={absolute_latency_floor_ms}ms, "
            f"cold_start={cold_start_sec}s, "
            f"adaptive={adaptive_thresholds}"
        )

    #  Server Registration 

    def add_server(self, server: Server, force_inner: bool = False) -> None:
        """
        Register a new server into the cluster.
        If force_inner=True and inner ring has space, place it there directly.
        Otherwise it starts in the outer ring.
        """
        server._EMA_ALPHA = self.ema_alpha
        with self._lock:
            # v1.3.0: cold start protection -- always start in outer
            if self.cold_start_sec > 0:
                server._cold_until = time.monotonic() + self.cold_start_sec
                server.position    = Position.OUTER
                heapq.heappush(self._outer_ring, server)
                log.info(
                    f"Added {server.id!r} -> outer ring "
                    f"(cold start, eligible in {self.cold_start_sec:.0f}s)"
                )
                return
            if force_inner and len(self._inner_ring) < self.max_inner_size:
                server.position = Position.INNER
                self._inner_ring.append(server)
                log.info(f"Added {server.id!r} -> inner ring")
            else:
                server.position = Position.OUTER
                heapq.heappush(self._outer_ring, server)
                log.info(f"Added {server.id!r} -> outer ring")

    def remove_server(self, server_id: str) -> bool:
        """
        Gracefully remove a server from the cluster.
        If removed from inner, attempt to pull from outer to maintain size.
        """
        with self._lock:
            for s in list(self._inner_ring):
                if s.id == server_id:
                    self._inner_ring.remove(s)
                    log.info(f"Removed {server_id!r} from inner ring")
                    self._maybe_pull_from_outer("maintain_min")
                    return True

            for i, s in enumerate(self._outer_ring):
                if s.id == server_id:
                    self._outer_ring.pop(i)
                    heapq.heapify(self._outer_ring)
                    log.info(f"Removed {server_id!r} from outer ring")
                    return True

        log.warning(f"remove_server: {server_id!r} not found")
        return False

    def force_evict(self, server_id: str) -> bool:
        """Manually evict a server from inner ring to outer ring."""
        with self._lock:
            for s in list(self._inner_ring):
                if s.id == server_id:
                    self._move_to_outer(s, EvictionReason.MANUAL)
                    return True
        return False

    #  Request Routing 

    def get_server(self) -> Optional[Server]:
        """
        Get next server via circular round-robin from inner ring.

        FIX (Empty Inner Ring / Thundering Herd):
          If inner is empty (shouldn't happen normally), fallback to
          emergency selection from all available servers.
        """
        with self._lock:
            if not self._inner_ring:
                return self._emergency_server()
            server = self._inner_ring[0]
            self._inner_ring.rotate(-1)
            return server

    def record_latency(self, server: Server, latency_ms: float) -> None:
        """
        v1.1.0 — Latency Feedback Loop.
        v1.2.0 — Relative anomaly scoring (cluster-aware).

        Call this after every request with the observed round-trip time.
        The cluster:
          1. Updates the server's rolling latency window
          2. Computes cluster-wide average latency across all inner servers
          3. Updates each server's latency_anomaly_score (relative, not absolute)
          4. Refreshes the server's EMA temperature

        Relative scoring means a server running at 60ms when cluster avg is 12ms
        scores anomaly=1.0 (5x slower → max heat) and self-evicts quickly,
        whereas absolute scoring (60/5000=0.0018) would never trigger eviction.
        """
        server.metrics.record_latency(latency_ms)

        # Compute cluster baseline using MEDIAN (not mean).
        # Median is robust to outliers — a single slow server won't pull
        # the baseline up and mask its own anomaly.
        with self._lock:
            inner = list(self._inner_ring)
        avgs = [s.metrics.avg_response_ms for s in inner if s.metrics.avg_response_ms > 0]
        cluster_baseline = statistics.median(avgs) if avgs else latency_ms

        server.metrics.update_latency_anomaly(cluster_baseline)
        server.update_temperature()

        # Feed adaptive threshold controller (v1.3.0)
        with self._lock:
            inner_snap = list(self._inner_ring)
        p95_vals = [s.metrics.p95_latency() for s in inner_snap if s.metrics.p95_latency() > 0]
        if p95_vals:
            cluster_p95 = statistics.mean(p95_vals)
            self._p95_window.append(cluster_p95)
            if self._adaptive:
                self._adaptive.record_p95(cluster_p95)

    @contextmanager
    def get_server_context(self) -> Generator[Optional[Server], None, None]:
        """
        NEW v1.1.0 — Zero-boilerplate request routing with auto latency recording.

        Context manager that picks a server, times the block, and
        automatically calls record_latency() on exit.

        Example:
            with cluster.get_server_context() as server:
                if server:
                    response = requests.get(f"http://{server.host}:{server.port}/api")

        If the block raises an exception, the server's error_rate is incremented
        and is_healthy is set to False if error_rate exceeds 0.5.
        """
        server = self.get_server()
        t0 = time.perf_counter()
        try:
            yield server
            if server is not None:
                elapsed_ms = (time.perf_counter() - t0) * 1000
                self.record_latency(server, elapsed_ms)
        except Exception:
            if server is not None:
                elapsed_ms = (time.perf_counter() - t0) * 1000
                self.record_latency(server, elapsed_ms)
                # Count error toward error_rate
                win = server.metrics._latency_window
                total = len(win)
                # Increment error_rate by blending: new = old*0.9 + 0.1
                server.metrics.error_rate = min(
                    1.0, server.metrics.error_rate * 0.9 + 0.1
                )
                if server.metrics.error_rate > 0.5:
                    server.metrics.is_healthy = False
                server.update_temperature()
            raise

    def _emergency_server(self) -> Optional[Server]:
        """
        FIX (Thundering Herd edge case):
        When inner ring is completely empty, pick least-hot server
        from anywhere in the cluster. Log a warning.
        """
        all_servers = list(self._inner_ring) + list(self._outer_ring)
        if not all_servers:
            log.error("Emergency server requested but cluster is empty!")
            return None

        best = min(all_servers, key=lambda s: s.temperature)
        log.warning(
            f"  Inner ring empty! Emergency routing to {best.id!r} "
            f"(temp={best.temperature:.3f}). Check your thresholds."
        )
        return best

    #  Core Rotation Logic 

    def rotate(self) -> bool:
        """
        One full rotation cycle — the penguin huddle step.

        Step 1: Evict overheated inner servers → outer ring
        Step 2: Pull cooled outer servers → inner ring
        Returns True if any rotation happened.

        FIX (Thundering Herd):
          Max evictions per cycle = max(1, inner_size // 3)

        FIX (Flapping):
          - rotation_cooldown_sec: minimum time between evictions per server
          - min_outer_dwell_sec: minimum time in outer before re-entry
          - hysteresis gap: heat_threshold >> cool_threshold
        """
        # v1.3.0: update adaptive thresholds before rotation
        if self._adaptive:
            self.heat_threshold, self.cool_threshold = self._adaptive.maybe_adapt()

        with self._lock:
            rotated = False
            now     = time.monotonic()

            # Step 1: Evict overheated OR floor-breaching inner servers
            candidates = []
            for s in list(self._inner_ring):
                cooldown_ok  = (now - s.last_rotated) >= self.rotation_cooldown_sec
                overheated   = s.is_overheated(self.heat_threshold)
                unhealthy    = not s.metrics.is_healthy

                # v1.3.0: absolute latency floor -- evict regardless of anomaly
                floor_breach = (
                    self.absolute_latency_floor_ms is not None
                    and s.metrics.avg_response_ms > 0
                    and s.metrics.avg_response_ms > self.absolute_latency_floor_ms
                )

                if floor_breach and (unhealthy or cooldown_ok):
                    candidates.append((s, EvictionReason.ABSOLUTE_LATENCY))
                elif overheated and (unhealthy or cooldown_ok):
                    candidates.append((s, EvictionReason.OVERHEATED))

            max_evict  = max(1, len(self._inner_ring) // 3)
            safe_evict = len(self._inner_ring) - self.min_inner_size
            to_evict   = candidates[: min(max_evict, max(0, safe_evict))]

            for server, reason in to_evict:
                self._move_to_outer(server, reason)
                rotated = True

            # Step 2: Pull cooled outer servers into inner
            while (
                self._outer_ring
                and len(self._inner_ring) < self.max_inner_size
            ):
                coolest    = self._outer_ring[0]
                dwell_time = time.monotonic() - coolest.last_rotated

                # v1.3.0: cold-start protection gate
                if coolest.is_cold_start():
                    break

                if dwell_time < self.min_outer_dwell_sec:
                    break

                if coolest.is_cooled(self.cool_threshold):
                    heapq.heappop(self._outer_ring)
                    self._move_to_inner(coolest)
                    rotated = True
                else:
                    break

            # Step 3: Health check evictions
            for server in list(self._inner_ring):
                if not server.metrics.is_healthy:
                    if len(self._inner_ring) > self.min_inner_size:
                        self._move_to_outer(server, EvictionReason.HEALTH_FAIL)
                        rotated = True

            return rotated

    #  Internal Move Helpers 

    def _move_to_outer(self, server: Server, reason: EvictionReason) -> None:
        now     = time.monotonic()
        elapsed = now - server.last_rotated
        server.total_inner_time       += elapsed
        server.position                = Position.OUTER
        server.last_rotated            = now
        server.rotation_count         += 1
        server._consecutive_evictions += 1

        # v1.3.0: floor-breach evictions forcibly raise temperature so the
        # server does not immediately pass the cool_threshold and re-enter.
        if reason == EvictionReason.ABSOLUTE_LATENCY:
            server.temperature = max(server.temperature, 0.8)

        self._inner_ring.remove(server)
        heapq.heappush(self._outer_ring, server)

        event = RotationEvent(
            timestamp=time.time(),
            server_id=server.id,
            direction="inner->outer",
            reason=reason.value,
            temperature=server.temperature,
        )
        self._rotation_log.append(event)
        if self._on_rotation:
            self._on_rotation(event)

        log.info(
            f" {server.id!r} inner→outer  "
            f"reason={reason.value}  temp={server.temperature:.3f}  "
            f"evictions={server._consecutive_evictions}"
        )

    def _move_to_inner(self, server: Server) -> None:
        now     = time.monotonic()
        elapsed = now - server.last_rotated
        server.total_outer_time             += elapsed
        server.position                      = Position.INNER
        server.last_rotated                  = now
        server.rotation_count               += 1
        server._consecutive_evictions        = 0

        self._inner_ring.append(server)

        event = RotationEvent(
            timestamp=time.time(),
            server_id=server.id,
            direction="outer->inner",
            reason="cooled",
            temperature=server.temperature,
        )
        self._rotation_log.append(event)
        if self._on_rotation:
            self._on_rotation(event)

        log.info(
            f" {server.id!r} outer→inner  "
            f"reason=cooled  temp={server.temperature:.3f}"
        )

    def _maybe_pull_from_outer(self, reason: str = "") -> None:
        """Pull from outer to maintain min_inner_size."""
        while (
            self._outer_ring
            and len(self._inner_ring) < self.min_inner_size
        ):
            s = heapq.heappop(self._outer_ring)
            self._move_to_inner(s)
            log.info(f"Pulled {s.id!r} to maintain min_inner ({reason})")

    #  Background Rotation Daemon 

    def start(self, rotation_interval_sec: float = DEFAULT_ROTATION_INTERVAL) -> None:
        """Start the background rotation daemon thread."""
        if self._running:
            log.warning("HuddleCluster already running")
            return
        self._running = True
        self._rotation_thread = threading.Thread(
            target=self._rotation_loop,
            args=(rotation_interval_sec,),
            name="huddle-rotation",
            daemon=True,
        )
        self._rotation_thread.start()
        if self._gossip_agent:
            self._gossip_agent.start(self)
        log.info(f"HuddleCluster started (interval={rotation_interval_sec}s)")

    def stop(self, timeout: float = 5.0) -> None:
        """Gracefully stop the rotation daemon."""
        self._running = False
        if self._rotation_thread and self._rotation_thread.is_alive():
            self._rotation_thread.join(timeout=timeout)
        if self._gossip_agent:
            self._gossip_agent.stop()
        log.info("HuddleCluster stopped")

    def _rotation_loop(self, interval: float) -> None:
        while self._running:
            try:
                # 1. Update metrics for all servers (if external updater provided)
                if self._metrics_updater:
                    for s in self.all_servers():
                        self._metrics_updater(s)
                        s.update_temperature()

                # 2. Always update temperatures (picks up record_latency() changes)
                else:
                    for s in self.all_servers():
                        s.update_temperature()

                # 3. Rotate
                self.rotate()

            except Exception as exc:
                log.exception(f"Rotation loop error: {exc}")

            time.sleep(interval)

    #  Analytics, Fairness & Observability 

    def fairness_score(self) -> float:
        """
        Gini-inspired fairness score — measures how evenly inner-ring
        servers share active duty time among themselves.

          0.0 = perfectly fair (all inner servers get equal time)
          1.0 = completely unfair

        v1.2.0: Only inner-ring servers are compared. Outer-ring servers
        are intentionally resting — including them in fairness math would
        always produce a misleadingly bad score.

        Alert if this exceeds 0.3 in production.
        """
        servers = self.inner_servers()   # v1.2.0: inner only
        if len(servers) < 2:
            return 0.0
        times = [s.total_inner_time for s in servers]
        total = sum(times)
        if total == 0:
            return 0.0
        mean_t = statistics.mean(times)
        if mean_t == 0:
            return 0.0
        deviation = sum(abs(t - mean_t) for t in times)
        return deviation / (2 * len(times) * mean_t)

    def health_report(self) -> dict:
        """
        Snapshot of cluster state — expose via /huddle/health endpoint.

        NEW v1.1.0: inner_ring entries now include p95_latency_ms.
        """
        with self._lock:
            inner = list(self._inner_ring)
            outer = list(self._outer_ring)

        inner_temps = [s.temperature for s in inner]
        return {
            "status":        "degraded" if len(inner) < self.min_inner_size else "healthy",
            "inner_ring": [
                {
                    "id":             s.id,
                    "weight":         s.weight,
                    "temp":           round(s.temperature, 4),
                    "rotations":      s.rotation_count,
                    "inner_time_sec": round(s.total_inner_time, 2),
                    "avg_latency_ms": round(s.metrics.avg_response_ms, 2),
                    "p95_latency_ms": round(s.metrics.p95_latency(), 2),
                    "anomaly_score":  round(s.metrics.latency_anomaly_score, 4),
                }
                for s in inner
            ],
            "outer_ring": [
                {
                    "id":             s.id,
                    "weight":         s.weight,
                    "temp":           round(s.temperature, 4),
                    "outer_time_sec": round(s.total_outer_time, 2),
                    "avg_latency_ms": round(s.metrics.avg_response_ms, 2),
                    "cold_start":     s.is_cold_start(),
                }
                for s in outer
            ],
            "inner_count":      len(inner),
            "outer_count":      len(outer),
            "total_servers":    len(inner) + len(outer),
            "avg_inner_temp":   round(statistics.mean(inner_temps), 4) if inner_temps else 0,
            "max_inner_temp":   round(max(inner_temps), 4) if inner_temps else 0,
            "fairness_score":   round(self.fairness_score(), 4),
            "total_rotations":  sum(s.rotation_count for s in inner + outer),
            "recent_rotations": [
                {
                    "server_id":   e.server_id,
                    "direction":   e.direction,
                    "reason":      e.reason,
                    "temperature": e.temperature,
                }
                for e in list(self._rotation_log)[-10:]
            ],
        }


    def prometheus_metrics(self) -> str:
        """
        v1.3.0 -- Prometheus text exposition format for /metrics endpoint.

        Example FastAPI integration:
            @app.get("/metrics", response_class=PlainTextResponse)
            def metrics():
                return cluster.prometheus_metrics()
        """
        lines = [
            "# HELP huddle_server_temperature EMA temperature score (0=cool, 1=hot)",
            "# TYPE huddle_server_temperature gauge",
        ]
        for s in self.all_servers():
            lines.append(
                f'huddle_server_temperature{{server="{s.id}",ring="{s.position.value}"}} '
                f"{s.temperature:.4f}"
            )
        lines += [
            "",
            "# HELP huddle_server_avg_latency_ms Rolling average response latency",
            "# TYPE huddle_server_avg_latency_ms gauge",
        ]
        for s in self.all_servers():
            lines.append(
                f'huddle_server_avg_latency_ms{{server="{s.id}"}} {s.metrics.avg_response_ms:.2f}'
            )
        lines += [
            "",
            "# HELP huddle_server_anomaly_score Relative latency anomaly (0=normal,1=max)",
            "# TYPE huddle_server_anomaly_score gauge",
        ]
        for s in self.all_servers():
            lines.append(
                f'huddle_server_anomaly_score{{server="{s.id}"}} {s.metrics.latency_anomaly_score:.4f}'
            )
        lines += [
            "",
            "# HELP huddle_server_rotations_total Total ring rotation count",
            "# TYPE huddle_server_rotations_total counter",
        ]
        for s in self.all_servers():
            lines.append(
                f'huddle_server_rotations_total{{server="{s.id}"}} {s.rotation_count}'
            )
        lines += [
            "",
            "# HELP huddle_cluster_inner_count Number of active inner-ring servers",
            "# TYPE huddle_cluster_inner_count gauge",
            f"huddle_cluster_inner_count {len(self._inner_ring)}",
            "",
            "# HELP huddle_cluster_fairness_gini Inner-ring Gini fairness (0=fair)",
            "# TYPE huddle_cluster_fairness_gini gauge",
            f"huddle_cluster_fairness_gini {self.fairness_score():.4f}",
            "",
            "# HELP huddle_cluster_heat_threshold Current eviction threshold",
            "# TYPE huddle_cluster_heat_threshold gauge",
            f"huddle_cluster_heat_threshold {self.heat_threshold:.3f}",
            "",
        ]
        if self._p95_window:
            p95 = statistics.median(self._p95_window)
            lines += [
                "# HELP huddle_cluster_p95_latency_ms Cluster-wide P95 latency estimate",
                "# TYPE huddle_cluster_p95_latency_ms gauge",
                f"huddle_cluster_p95_latency_ms {p95:.2f}",
                "",
            ]
        if self._gossip_agent:
            peers = list(self._gossip_agent.peer_states().keys())
            lines += [
                "# HELP huddle_gossip_peer_count Known gossip peers",
                "# TYPE huddle_gossip_peer_count gauge",
                f"huddle_gossip_peer_count {len(peers)}",
                "",
            ]
        return "\n".join(lines)

    def all_servers(self) -> list[Server]:
        """Return all servers (inner + outer)."""
        with self._lock:
            return list(self._inner_ring) + list(self._outer_ring)

    def inner_servers(self) -> list[Server]:
        with self._lock:
            return list(self._inner_ring)

    def outer_servers(self) -> list[Server]:
        with self._lock:
            return list(self._outer_ring)

    def __repr__(self) -> str:
        return (
            f"HuddleCluster("
            f"inner={len(self._inner_ring)}, "
            f"outer={len(self._outer_ring)}, "
            f"running={self._running})"
        )




#  Adaptive Threshold Controller 


class AdaptiveThresholdController:
    """
    v1.3.0 -- Auto-adjusts heat/cool thresholds based on cluster P95 history.

    When cluster P95 is stable and low, thresholds tighten (more sensitive).
    When P95 is high or rising (sustained load), thresholds loosen to avoid
    over-eviction. Uses a rolling window of cluster P95 samples.
    """

    def __init__(
        self,
        base_heat:           float = 0.55,
        base_cool:           float = 0.30,
        max_delta:           float = 0.10,
        window_size:         int   = 20,
        adaptation_interval: float = 5.0,
    ):
        self.base_heat           = base_heat
        self.base_cool           = base_cool
        self.max_delta           = max_delta
        self.window_size         = window_size
        self.adaptation_interval = adaptation_interval

        self._p95_window:  deque = deque(maxlen=window_size)
        self._current_heat = base_heat
        self._current_cool   = base_cool
        self._last_adapted   = time.monotonic()
        self._lock           = threading.Lock()

    def record_p95(self, p95_ms: float) -> None:
        with self._lock:
            self._p95_window.append(p95_ms)

    def maybe_adapt(self) -> tuple:
        now = time.monotonic()
        with self._lock:
            if (now - self._last_adapted) < self.adaptation_interval:
                return self._current_heat, self._current_cool

            # Need at least window_size samples to establish baseline
            if len(self._p95_window) < self.window_size:
                return self._current_heat, self._current_cool

            # Establish baseline from oldest half of window
            window   = list(self._p95_window)
            half     = max(1, len(window) // 2)
            baseline = statistics.median(window[:half])
            recent   = statistics.median(window[-max(3, half//2):])
            ratio    = recent / max(baseline, 1.0)

            if ratio > 1.5:
                adjustment = self.max_delta           # sustained stress -- loosen
            elif ratio < 0.7:
                adjustment = -self.max_delta          # very healthy -- tighten
            else:
                t          = (ratio - 0.7) / 0.8
                adjustment = (t * 2 - 1) * self.max_delta

            new_heat = max(0.35, min(0.85, self.base_heat + adjustment))
            new_cool = max(0.10, min(new_heat - 0.15, self.base_cool + adjustment * 0.5))

            if abs(new_heat - self._current_heat) > 0.01:
                log.info(
                    f"Adaptive thresholds: heat {self._current_heat:.2f}->"
                    f"{new_heat:.2f}, cool {self._current_cool:.2f}->{new_cool:.2f} "
                    f"(P95 ratio={ratio:.2f}, baseline={baseline:.1f}ms, recent={recent:.1f}ms)"
                )
            self._current_heat = new_heat
            self._current_cool = new_cool
            self._last_adapted = now

        return self._current_heat, self._current_cool

    @property
    def heat_threshold(self) -> float:
        return self._current_heat

    @property
    def cool_threshold(self) -> float:
        return self._current_cool


#  Gossip Agent 


class GossipAgent:
    """
    v1.3.0 -- Lightweight UDP multicast gossip for distributed temperature sharing.

    Each HuddleCluster instance broadcasts its inner-ring server temperatures
    to peer clusters. Peers use received data as advisory signals only.

    Protocol: UDP multicast, JSON payload, best-effort delivery.

    Usage:
        agent  = GossipAgent(node_id="node-1")
        cluster = create_cluster([...])
        cluster.start(gossip_agent=agent)  # or pass gossip_agent= to HuddleCluster()
        peers  = agent.peer_states()       # {node_id: [{id, temp, avg_ms, pos}]}
    """

    MULTICAST_GROUP   = "224.0.0.251"
    DEFAULT_PORT      = 9999
    MAX_MESSAGE_BYTES = 4096

    def __init__(
        self,
        node_id:            str,
        gossip_port:        int   = DEFAULT_PORT,
        broadcast_interval: float = 2.0,
        ttl:                int   = 1,
    ):
        self.node_id            = node_id
        self.gossip_port        = gossip_port
        self.broadcast_interval = broadcast_interval
        self.ttl                = ttl

        self._cluster:     Optional["HuddleCluster"] = None
        self._peer_states: dict                       = {}
        self._running      = False
        self._lock         = threading.Lock()
        self._recv_thread: Optional[threading.Thread] = None
        self._send_thread: Optional[threading.Thread] = None

    def start(self, cluster: "HuddleCluster") -> None:
        self._cluster = cluster
        self._running = True
        self._recv_thread = threading.Thread(
            target=self._recv_loop, daemon=True, name="huddle-gossip-recv"
        )
        self._send_thread = threading.Thread(
            target=self._send_loop, daemon=True, name="huddle-gossip-send"
        )
        self._recv_thread.start()
        self._send_thread.start()
        log.info(f"GossipAgent started: node={self.node_id!r}, port={self.gossip_port}")

    def stop(self) -> None:
        self._running = False
        log.info("GossipAgent stopped")

    def peer_states(self) -> dict:
        with self._lock:
            return dict(self._peer_states)

    def _build_message(self) -> bytes:
        if self._cluster is None:
            return b""
        servers = [
            {"id": s.id, "temp": round(s.temperature, 4),
             "avg_ms": round(s.metrics.avg_response_ms, 2), "pos": s.position.value}
            for s in self._cluster.inner_servers()
        ]
        return json.dumps({"node_id": self.node_id, "servers": servers}).encode()

    def _send_loop(self) -> None:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, self.ttl)
            while self._running:
                try:
                    data = self._build_message()
                    if data:
                        sock.sendto(data, (self.MULTICAST_GROUP, self.gossip_port))
                except Exception as e:
                    log.debug(f"Gossip send error: {e}")
                time.sleep(self.broadcast_interval)
        finally:
            sock.close()

    def _recv_loop(self) -> None:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.settimeout(1.0)
            try:
                sock.bind(("", self.gossip_port))
                mreq = struct.pack("4sL", socket.inet_aton(self.MULTICAST_GROUP), socket.INADDR_ANY)
                sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            except OSError as e:
                log.warning(f"GossipAgent cannot join multicast (non-fatal): {e}")
                return
            while self._running:
                try:
                    data, _ = sock.recvfrom(self.MAX_MESSAGE_BYTES)
                    msg     = json.loads(data.decode())
                    nid     = msg.get("node_id", "")
                    if nid and nid != self.node_id:
                        with self._lock:
                            self._peer_states[nid] = msg.get("servers", [])
                except socket.timeout:
                    pass
                except Exception as e:
                    log.debug(f"Gossip recv error: {e}")
        finally:
            sock.close()

#  Quick-start helper 


def create_cluster(
    server_addresses: list[tuple[str, str, int]],
    **kwargs,
) -> HuddleCluster:
    """
    Convenience factory.

    server_addresses: list of (id, host, port) tuples
    kwargs: passed to HuddleCluster constructor

    Example:
        cluster = create_cluster([
            ("s1", "10.0.0.1", 8080),
            ("s2", "10.0.0.2", 8080),
            ("s3", "10.0.0.3", 8080),
        ])
        cluster.start()
    """
    cluster = HuddleCluster(**kwargs)
    for idx, addr in enumerate(server_addresses):
        sid, host, port = addr[0], addr[1], addr[2]
        weight          = float(addr[3]) if len(addr) > 3 else 1.0
        s               = Server(id=sid, host=host, port=port, weight=weight)
        cluster.add_server(s, force_inner=(idx < cluster.max_inner_size))
    return cluster


#  Demo 

if __name__ == "__main__":
    import random
    from time import sleep

    print("HuddleCluster v1.3.0 demo")

    cluster = create_cluster([
        ("s1", "127.0.0.1", 8001),
        ("s2", "127.0.0.1", 8002),
        ("s3", "127.0.0.1", 8003),
        ("s4", "127.0.0.1", 8004),
    ])
    cluster.start()

    try:
        for i in range(30):
            with cluster.get_server_context() as server:
                if server:
                    # Simulate: s2 becomes slow after iteration 10
                    if server.id == "s2" and i > 10:
                        sleep(0.08)   # 80 ms — hot
                    else:
                        sleep(0.015)  # 15 ms — normal
                    print(f"[{i:02d}] → {server.id}  temp={server.temperature:.3f}  "
                          f"avg_ms={server.metrics.avg_response_ms:.1f}")

    except KeyboardInterrupt:
        pass
    finally:
        cluster.stop()