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
Version: 4.13.0
License: MIT

Changelog v1.4.0
-----------------
  NEW  Persistent state -- state_file and checkpoint_interval_sec parameters.
       Cluster temperature state is saved to JSON and restored on restart,
       preventing cold-start degradation after rolling restarts.
  NEW  Webhook alerting -- alert_webhooks, alert_on, alert_headers,
       alert_timeout_sec. POST JSON payloads to any HTTP endpoint on
       eviction, promotion, or health-state change events.
  NEW  Built-in HTTP health checker -- health_check_path,
       health_check_interval_sec, health_check_timeout_sec,
       health_check_failures. Probes upstream servers directly; failed
       servers are evicted without needing an external health check loop.
  NEW  WebSocket connection draining -- ws_drain_timeout_sec,
       ws_connection(), ws_open(), ws_close(). Gracefully waits for
       active WebSocket connections to finish before evicting a server.
  NEW  huddle_cluster_pkg -- optional extension package:
       backends_redis.py   Redis shared-state backend (multi-node restarts)
       grpc_cluster.py     Thermal-aware gRPC channel routing
       discovery_k8s.py    Kubernetes pod auto-discovery via Watch API
  FIX  ConnectionAbortedError (WinError 10053) now caught in the
       dashboard SSE stream and admin HTTP handler so Windows clients
       that disconnect no longer print tracebacks to the console.

Changelog v1.3.3
-----------------
  NEW  Server.tags -- arbitrary key-value metadata on server objects.
       Tags appear in health_report(), prometheus_metrics() labels,
       and Server.__repr__(). Pass via Server(tags={...}) or as 5th
       tuple element in create_cluster().
  NEW  on_eviction callback -- dedicated callback fired on every eviction
       (separate from on_rotation which fires on all rotations).
  NEW  Throughput metrics -- requests_total counter and requests_per_second
       gauge in health_report() and prometheus_metrics().
  NEW  batch_record_latency() -- feed multiple (server, ms) pairs at once.
  NEW  request_timeout_ms -- configurable dead-server timeout threshold.
  NEW  Graceful shutdown -- drain_timeout_sec in stop().
  NEW  Circuit breaker -- circuit_breaker_threshold parameter.

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
import http.server
import json
import logging
import os
import queue as _queue
import random
import socket
import statistics
import struct
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Generator, Optional

#  Version 
__version__ = "4.14.0"
__author__  = "Rahad Bhuiya"
__license__ = "MIT"

#  Logging Setup 

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] HuddleCluster │ %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("huddle")


#  Enums & Data Classes 


class Position(Enum):
    INNER    = "inner"     # Active -- taking requests
    OUTER    = "outer"     # Resting -- cooling down
    DRAINING = "draining"  # Finishing open connections, not taking new requests


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
      1. External updater  -- set fields directly via your metrics_updater callback.
      2. Auto-feedback     -- call cluster.record_latency(server, ms) after each
                             request; avg_response_ms is updated automatically.

    Latency tracking uses two separate windows:
      _latency_window    -- 10-sample rolling window for EMA temperature scoring.
                            Small so the cluster reacts quickly to degradation.
      _histogram_window  -- 1000-sample rolling window for accurate percentiles
                            (P50/P75/P90/P95/P99/P999). Large enough for P999 to
                            be meaningful once the server has served enough traffic.
    """
    cpu_usage:             float = 0.0   # 0.0-1.0
    memory_usage:          float = 0.0   # 0.0-1.0
    active_connections:    int   = 0
    avg_response_ms:       float = 0.0   # set externally or via record_latency()
    error_rate:            float = 0.0   # 0.0-1.0 (errors / total_requests)
    is_healthy:            bool  = True  # False = immediately evict
    latency_anomaly_score: float = 0.0   # v1.2.0: relative slowness vs cluster avg

    # 10-sample window for EMA temperature scoring -- internal use
    _latency_window: deque = field(
        default_factory=lambda: deque(maxlen=10), repr=False
    )

    # 1000-sample window for accurate percentile calculations -- internal use
    _histogram_window: deque = field(
        default_factory=lambda: deque(maxlen=1000), repr=False
    )

    def record_latency(self, ms: float) -> None:
        """
        Push one latency sample into both the EMA window and the histogram window.

        Called by HuddleCluster.record_latency() after every request.
        You generally do not call this directly.

        Args:
            ms: Observed round-trip time in milliseconds.
        """
        self._latency_window.append(ms)
        self._histogram_window.append(ms)
        if self._latency_window:
            self.avg_response_ms = statistics.mean(self._latency_window)

    def update_latency_anomaly(self, cluster_avg_ms: float) -> None:
        """
        v1.2.0 -- Relative latency anomaly score.

        Computes how much slower THIS server is vs the cluster average.
        Uses ratio-based scoring so slow servers are detected regardless
        of the absolute baseline latency.

        Formula: clamp((self_avg / cluster_avg - 1.0) / 2.0, 0, 1)

        Examples (cluster_avg = 12 ms):
          self = 12 ms  ->  ratio 1.0x  ->  anomaly 0.00  (normal)
          self = 24 ms  ->  ratio 2.0x  ->  anomaly 0.50  (warm)
          self = 36 ms  ->  ratio 3.0x  ->  anomaly 1.00  (hot, clamped)
          self = 60 ms  ->  ratio 5.0x  ->  anomaly 1.00  (clamped)
        """
        if cluster_avg_ms <= 0 or self.avg_response_ms <= 0:
            self.latency_anomaly_score = 0.0
            return
        ratio = self.avg_response_ms / cluster_avg_ms
        self.latency_anomaly_score = max(0.0, min(1.0, (ratio - 1.0) / 2.0))

    
    # Percentile methods -- all use the 1000-sample histogram window.
    

    def _percentile(self, p: float) -> float:
        """
        Compute the p-th percentile (0-100) using linear interpolation.

        Matches the behaviour of numpy.percentile(data, p, method='linear').
        Returns 0.0 when the histogram window has no samples yet.

        Args:
            p: Percentile to compute, in the range [0, 100].
        """
        w = sorted(self._histogram_window)
        n = len(w)
        if n == 0:
            return 0.0
        if n == 1:
            return w[0]
        idx = (p / 100.0) * (n - 1)
        lo = int(idx)
        hi = min(lo + 1, n - 1)
        frac = idx - lo
        return w[lo] + frac * (w[hi] - w[lo])

    def p50_latency(self) -> float:
        """Median latency from the 1000-sample histogram window."""
        return self._percentile(50)

    def p75_latency(self) -> float:
        """75th-percentile latency from the 1000-sample histogram window."""
        return self._percentile(75)

    def p90_latency(self) -> float:
        """90th-percentile latency from the 1000-sample histogram window."""
        return self._percentile(90)

    def p95_latency(self) -> float:
        """
        95th-percentile latency.

        Uses the 1000-sample histogram window when samples exist,
        otherwise returns 0.0.
        """
        return self._percentile(95)

    def p99_latency(self) -> float:
        """
        99th-percentile latency from the 1000-sample histogram window.

        Accurate once the server has received at least 100 requests.
        """
        return self._percentile(99)

    def p999_latency(self) -> float:
        """
        99.9th-percentile latency from the 1000-sample histogram window.

        Meaningful once the server has received at least 1000 requests
        (i.e. the histogram window is full). With fewer samples the result
        is a conservative approximation biased toward the maximum observed
        latency.
        """
        return self._percentile(99.9)

    def latency_histogram(self) -> dict:
        """
        Full latency percentile snapshot as a dict.

        Returns:
            Dict with keys: sample_count, p50_ms, p75_ms, p90_ms,
            p95_ms, p99_ms, p999_ms. All latency values in milliseconds,
            rounded to 3 decimal places.

        Example::

            {
                "sample_count": 487,
                "p50_ms": 12.451,
                "p75_ms": 18.302,
                "p90_ms": 24.100,
                "p95_ms": 29.870,
                "p99_ms": 48.210,
                "p999_ms": 97.650,
            }
        """
        return {
            "sample_count": len(self._histogram_window),
            "p50_ms":        round(self.p50_latency(),  3),
            "p75_ms":        round(self.p75_latency(),  3),
            "p90_ms":        round(self.p90_latency(),  3),
            "p95_ms":        round(self.p95_latency(),  3),
            "p99_ms":        round(self.p99_latency(),  3),
            "p999_ms":       round(self.p999_latency(), 3),
        }


@dataclass
class RotationEvent:
    timestamp:   float
    server_id:   str
    direction:   str          # "inner->outer" or "outer->inner"
    reason:      str
    temperature: float


@dataclass
class AlertEvent:
    """
    A structured alert fired when a notable cluster event occurs.

    Delivered asynchronously to each configured webhook URL as a JSON
    HTTP POST.  A bounded history of recent alerts is also kept in memory
    and exposed via alert_history() and health_report().

    Attributes:
        event:      Short event name.  One of:
                    "eviction", "promotion", "degraded",
                    "circuit_breaker", "retry_exhausted".
        level:      Severity string -- "INFO", "WARNING", or "CRITICAL".
        timestamp:  Unix timestamp (time.time()) when the event occurred.
        server_id:  ID of the affected server, or None for cluster events.
        data:       Event-specific dict with additional context.
    """
    event:     str
    level:     str
    timestamp: float
    server_id: Optional[str]
    data:      dict

    def to_dict(self) -> dict:
        """Serialize to a JSON-safe dict for HTTP delivery."""
        return {
            "event":     self.event,
            "level":     self.level,
            "timestamp": self.timestamp,
            "server_id": self.server_id,
            "data":      self.data,
        }


#  Exceptions 


class RetryExhaustedError(Exception):
    """
    Raised by request_with_retry() when every attempt fails.

    Attributes:
        last_error:        Exception from the final attempt.
        attempts:          Total number of attempts made (1 + retries).
        tried_server_ids:  IDs of all servers that were tried, in order.

    Example::

        try:
            result = cluster.request_with_retry(fn, max_retries=2)
        except RetryExhaustedError as e:
            print(f"Failed after {e.attempts} attempts: {e.last_error}")
            print(f"Tried servers: {e.tried_server_ids}")
    """

    def __init__(
        self,
        last_error: Optional[Exception],
        attempts: int,
        tried_server_ids: list,
    ) -> None:
        self.last_error       = last_error
        self.attempts         = attempts
        self.tried_server_ids = tried_server_ids
        super().__init__(
            f"All {attempts} attempt(s) failed. "
            f"Tried servers: {tried_server_ids}. "
            f"Last error: {last_error!r}"
        )


@dataclass
class TrafficRamp:
    """
    Tracks a gradual traffic-weight transition for one server.

    Used by start_traffic_ramp() to implement canary and rolling deploys.
    The routing weight interpolates linearly from start_weight to
    target_weight over ramp_sec seconds.

    Attributes:
        server_id:    ID of the server being ramped.
        start_weight: Initial routing weight (fraction of normal traffic).
        target_weight: Final routing weight once ramp completes.
        ramp_sec:     Duration of the ramp in seconds.
        start_time:   Monotonic timestamp when the ramp began.
    """
    server_id:    str
    start_weight: float
    target_weight: float
    ramp_sec:     float
    start_time:   float = field(default_factory=time.monotonic)

    def current_weight(self) -> float:
        """
        Linearly interpolated weight at the current moment.

        Returns start_weight at t=0, target_weight at t=ramp_sec,
        and target_weight for all t > ramp_sec.
        """
        elapsed  = time.monotonic() - self.start_time
        fraction = min(1.0, elapsed / self.ramp_sec) if self.ramp_sec > 0 else 1.0
        return self.start_weight + fraction * (self.target_weight - self.start_weight)

    def progress(self) -> float:
        """Ramp progress as a fraction in [0.0, 1.0]."""
        if self.ramp_sec <= 0:
            return 1.0
        return min(1.0, (time.monotonic() - self.start_time) / self.ramp_sec)

    def is_complete(self) -> bool:
        """True when the ramp has reached target_weight."""
        return self.progress() >= 1.0

    def to_dict(self) -> dict:
        return {
            "server_id":    self.server_id,
            "start_weight": self.start_weight,
            "target_weight": self.target_weight,
            "ramp_sec":     self.ramp_sec,
            "current_weight": round(self.current_weight(), 4),
            "progress_pct": round(self.progress() * 100, 1),
            "complete":     self.is_complete(),
        }


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

    def __init__(
        self,
        id:     str,
        host:   str,
        port:   int,
        weight: float = 1.0,
        tags:   Optional[dict] = None,
    ):
        """
        weight (v1.3.0): capacity multiplier. A server with weight=2.0 needs
        to reach 2x the base heat_threshold before eviction. Use for larger
        instances that can handle proportionally more load.

        tags (v1.3.3): arbitrary key-value metadata attached to this server.
        Tags appear in health_report() and prometheus_metrics() labels.
        Example: tags={"region": "us-east", "tier": "primary", "az": "1a"}
        """
        if weight <= 0:
            raise ValueError("weight must be > 0")
        self.id     = id
        self.host   = host
        self.port   = port
        self.weight = weight
        self.tags   = tags or {}   # v1.3.3: arbitrary metadata

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
        """
        True if the server is still in its cold-start protection period.

        During cold start, the server stays in the outer ring regardless of
        its temperature, preventing traffic spikes on fresh instances that
        have not yet warmed JIT compilers or application caches.

        Returns False once cold_start_sec seconds have elapsed since add_server().
        """
        return time.monotonic() < self._cold_until

    def effective_heat_threshold(self, base_threshold: float) -> float:
        """
        Compute the eviction threshold adjusted for this server's weight.

        A server with weight=2.0 can handle proportionally more load before
        eviction: effective_threshold = min(1.0, base_threshold * weight).

        Examples:
            weight=1.0, base=0.55 -> threshold=0.55 (default)
            weight=2.0, base=0.55 -> threshold=1.10 -> clamped to 1.0 (never evicted by temp)
            weight=0.5, base=0.55 -> threshold=0.275 (evicts sooner)

        Args:
            base_threshold: The cluster-level heat_threshold setting.

        Returns:
            Effective eviction temperature for this server, in [0, 1].
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
        # v1.3.0 parameters
        absolute_latency_floor_ms: Optional[float] = None,
        cold_start_sec:            float    = 0.0,
        adaptive_thresholds:       bool     = False,
        gossip_agent:              Optional["GossipAgent"] = None,
        request_timeout_ms:        float    = 500.0,
        circuit_breaker_threshold: float    = 0.5,
        metrics_updater:           Optional[Callable[[Server], None]] = None,
        on_rotation:               Optional[Callable[[RotationEvent], None]] = None,
        on_eviction:               Optional[Callable[["Server", "EvictionReason"], None]] = None,
        # v1.4.0 parameters
        state_file:                Optional[str]   = None,
        checkpoint_interval_sec:   float           = 0.0,
        # v1.4.0 alerting parameters
        alert_webhooks:            Optional[list]  = None,
        alert_on:                  Optional[set]   = None,
        alert_headers:             Optional[dict]  = None,
        alert_timeout_sec:         float           = 5.0,
        # v1.4.0 connection draining
        ws_drain_timeout_sec:      float           = 0.0,
        # v1.4.0 built-in HTTP health checker
        health_check_path:         Optional[str]   = None,
        health_check_interval_sec: float           = 10.0,
        health_check_timeout_sec:  float           = 3.0,
        health_check_failures:     int             = 2,
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
        self.request_timeout_ms        = request_timeout_ms
        self.circuit_breaker_threshold = circuit_breaker_threshold
        self._metrics_updater          = metrics_updater
        self._on_rotation              = on_rotation
        self._on_eviction              = on_eviction   # v1.3.3

        # Adaptive thresholds controller (v1.3.0)
        self._adaptive: Optional[AdaptiveThresholdController] = (
            AdaptiveThresholdController(
                base_heat=heat_threshold,
                base_cool=cool_threshold,
            ) if adaptive_thresholds else None
        )

        # P95 tracking window for adaptive thresholds + Prometheus
        self._p95_window: deque = deque(maxlen=100)

        # v1.3.3: throughput tracking
        self._request_count: int   = 0
        self._window_start:  float = time.monotonic()
        self._rps_window:    deque = deque(maxlen=60)  # 60 seconds of RPS samples

        # v1.4.0: sticky sessions -- maps affinity_key -> server_id
        # Entries are evicted automatically when the mapped server leaves the inner ring.
        self._affinity_map: dict = {}

        # v1.4.0: retry statistics -- monotonically increasing counters
        self._retry_stats: dict = {
            "total_retries":       0,
            "successful_retries":  0,
            "exhausted_retries":   0,
        }

        # v1.4.0: persistent state
        self._state_file:           Optional[str]             = state_file
        self._checkpoint_interval:  float                     = checkpoint_interval_sec
        self._checkpoint_thread:    Optional[threading.Thread] = None
        # Per-instance lock so concurrent save_state() calls are serialised.
        # On Windows, os.replace() raises WinError 5 if two threads try to
        # rename different .tmp files onto the same target simultaneously.
        self._state_write_lock: threading.Lock = threading.Lock()

        # v1.4.0: alerting / webhooks
        # Default: alert on eviction, degraded cluster, circuit breaker trips.
        # Promotion ("INFO") is excluded by default -- too noisy for most setups.
        _default_alert_on = {"eviction", "degraded", "circuit_breaker", "retry_exhausted"}
        self._alert_webhooks: list  = list(alert_webhooks or [])
        self._alert_on:       set   = set(alert_on) if alert_on is not None else _default_alert_on
        self._alert_headers:  dict  = dict(alert_headers or {})
        self._alert_timeout:  float = alert_timeout_sec
        # Bounded queue so a slow/down webhook never blocks routing threads
        self._alert_queue:    _queue.Queue = _queue.Queue(maxsize=1000)
        self._alert_thread:   Optional[threading.Thread] = None
        # Bounded history of the last 100 fired alerts (regardless of delivery)
        self._alert_history:  deque = deque(maxlen=100)

        # v1.4.0: WebSocket / long-connection draining
        # Maps server_id -> (server, drain_start_monotonic, eviction_reason)
        # Draining servers are removed from the inner ring but not yet in the
        # outer ring -- they are finishing their open connections.
        self._ws_drain_timeout:  float = ws_drain_timeout_sec
        self._draining_servers:  dict  = {}

        # v1.4.0: canary / rolling deploy traffic ramps
        # Maps server_id -> TrafficRamp for servers currently being ramped up.
        # When any ramp is active, get_server() switches from plain round-robin
        # to smooth weighted random selection so canaries receive less traffic.
        self._ramps: dict = {}

        # v1.4.0: built-in HTTP health checker
        # Periodically GETs health_check_path on each server.
        # After health_check_failures consecutive non-2xx or timeout responses,
        # the server's is_healthy is set to False so the circuit breaker evicts it.
        # On recovery (2xx response after failure), is_healthy is restored to True.
        self._health_check_path:     Optional[str]             = health_check_path
        self._health_check_interval: float                     = health_check_interval_sec
        self._health_check_timeout:  float                     = health_check_timeout_sec
        self._health_check_failures: int                       = max(1, health_check_failures)
        self._health_check_thread:   Optional[threading.Thread] = None
        # Per-server consecutive failure counters  {server_id: int}
        self._health_fail_counts:    dict                      = {}

        # v1.4.0: Admin REST API
        self._admin_port:   Optional[int]                      = None
        self._admin_server: Optional[http.server.HTTPServer]   = None
        self._admin_thread: Optional[threading.Thread]         = None
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
        Register a server in the cluster.

        If cold_start_sec > 0, the server always starts in the outer ring
        regardless of force_inner, with a cold_start timer preventing
        early promotion.

        Args:
            server:      Server instance to register (see Server class).
            force_inner: If True and inner ring has space, place in inner ring
                         directly. Ignored when cold_start_sec > 0.

        Example:
            cluster.add_server(
                Server(id="s4", host="10.0.0.4", port=8080,
                       weight=2.0, tags={"region": "eu-west"}),
                force_inner=True,
            )
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

        If removed from the inner ring, attempts to pull from the outer ring
        to maintain min_inner_size. Logs a warning if server_id is not found.

        Args:
            server_id: The id string of the server to remove.

        Returns:
            True if the server was found and removed, False otherwise.
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
        """
        Manually evict a server from the inner ring to the outer ring.

        Useful for operator-triggered maintenance or draining a specific server
        before a deployment. The on_eviction callback fires with reason=MANUAL.

        Args:
            server_id: The id string of the server to evict.

        Returns:
            True if the server was found in the inner ring and evicted.
        """
        with self._lock:
            for s in list(self._inner_ring):
                if s.id == server_id:
                    self._move_to_outer(s, EvictionReason.MANUAL)
                    return True
        return False

    def _emergency_server(self) -> Optional[Server]:
        """
        Fallback when the inner ring is unexpectedly empty.

        Returns the coolest server from the outer ring so requests are never
        dropped outright. If the outer ring is also empty (truly no servers),
        returns None.

        Must be called with self._lock held.
        """
        if self._outer_ring:
            return self._outer_ring[0]  # min-heap: index 0 is the coolest
        # Last resort: check draining servers
        for s, _, _ in self._draining_servers.values():
            return s
        return None

    #  Request Routing 

    def get_server(self, affinity_key: Optional[str] = None) -> Optional[Server]:
        """
        Get next server from the inner ring.

        Without affinity_key: round-robin (or weighted selection when traffic
        ramps are active -- see start_traffic_ramp()).
        With affinity_key: sticky session routing (see class docstring).

        Args:
            affinity_key: Any string that identifies a client or session.
                          Pass None for standard stateless routing.

        Returns:
            A Server instance, or None if the cluster has no servers at all.
        """
        with self._lock:
            if not self._inner_ring:
                return self._emergency_server()

            if affinity_key is None:
                # Use weighted selection when ramps are active; otherwise
                # fast O(1) round-robin to avoid overhead on every request.
                if self._ramps:
                    return self._weighted_select()
                server = self._inner_ring[0]
                self._inner_ring.rotate(-1)
                return server

            # Sticky session: look up existing binding
            inner_ids = {s.id: s for s in self._inner_ring}
            bound_id  = self._affinity_map.get(affinity_key)

            if bound_id is not None:
                bound_server = inner_ids.get(bound_id)
                if bound_server is not None and bound_server.metrics.is_healthy:
                    return bound_server

            server = self._inner_ring[0]
            self._inner_ring.rotate(-1)
            self._affinity_map[affinity_key] = server.id
            return server

    def _weighted_select(self) -> Optional[Server]:
        """
        Weighted random server selection used when traffic ramps are active.

        Each inner server's effective weight is taken from its active TrafficRamp
        (if any) or its static server.weight.  Smooth weighted random selection
        (single random draw proportional to cumulative weight) is used so that
        low-weight canaries receive proportionally less traffic without starving.

        Must be called with self._lock held.
        """
        servers = list(self._inner_ring)
        if not servers:
            return None

        weights = []
        for s in servers:
            ramp = self._ramps.get(s.id)
            w    = ramp.current_weight() if ramp else s.weight
            weights.append(max(1e-6, w))   # guard against zero weight

        total = sum(weights)
        r     = random.uniform(0.0, total)
        cumulative = 0.0
        for server, weight in zip(servers, weights):
            cumulative += weight
            if r <= cumulative:
                return server
        return servers[-1]   # floating-point safety fallback

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

    def batch_record_latency(self, measurements: list) -> None:
        """
        v1.3.3 -- Feed multiple latency samples in one call.

        measurements: list of (server, latency_ms) tuples.

        Example:
            cluster.batch_record_latency([
                (s1, 15.2),
                (s2, 18.7),
                (s3, 220.0),   # slow server
            ])
        """
        for server, latency_ms in measurements:
            server.metrics.record_latency(latency_ms)

        # Compute cluster baseline once for all servers (efficient)
        with self._lock:
            inner = list(self._inner_ring)
        avgs = [s.metrics.avg_response_ms for s in inner if s.metrics.avg_response_ms > 0]
        cluster_baseline = statistics.median(avgs) if avgs else 1.0

        for server, _ in measurements:
            server.metrics.update_latency_anomaly(cluster_baseline)
            server.update_temperature()

        # Feed P95 window
        p95_vals = [s.metrics.p95_latency() for s in inner if s.metrics.p95_latency() > 0]
        if p95_vals:
            cluster_p95 = statistics.mean(p95_vals)
            self._p95_window.append(cluster_p95)
            if self._adaptive:
                self._adaptive.record_p95(cluster_p95)

    @contextmanager
    def get_server_context(
        self, affinity_key: Optional[str] = None
    ) -> Generator[Optional[Server], None, None]:
        """
        Zero-boilerplate request routing with auto latency recording.

        Context manager that picks a server, times the block, and
        automatically calls record_latency() on exit.

        Args:
            affinity_key: Optional sticky-session key. When provided, the same
                server is returned for the same key (see get_server()).

        Example::

            # Stateless
            with cluster.get_server_context() as server:
                response = requests.get(f"http://{server.host}:{server.port}/api")

            # Sticky
            with cluster.get_server_context(affinity_key=user_id) as server:
                response = requests.get(f"http://{server.host}:{server.port}/api")

        If the block raises an exception, the server's error_rate is incremented
        and is_healthy is set to False if error_rate exceeds 0.5.
        """
        server = self.get_server(affinity_key=affinity_key)
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
                server.metrics.error_rate = min(
                    1.0, server.metrics.error_rate * 0.9 + 0.1
                )
                if server.metrics.error_rate > 0.5:
                    server.metrics.is_healthy = False
                    self._fire_alert(
                        event="circuit_breaker",
                        level="WARNING",
                        server_id=server.id,
                        data={
                            "error_rate":  round(server.metrics.error_rate, 4),
                            "temperature": round(server.temperature, 4),
                        },
                    )
                server.update_temperature()
            raise

    def clear_affinity(self, key: Optional[str] = None) -> int:
        """
        Remove sticky-session bindings from the affinity map.

        Args:
            key: If provided, remove only this key's binding.
                 If None, clear the entire affinity map.

        Returns:
            Number of bindings removed.

        Example::

            # Clear one session on logout
            cluster.clear_affinity(user_id)

            # Clear all bindings (e.g. after a deployment)
            cluster.clear_affinity()
        """
        with self._lock:
            if key is None:
                count = len(self._affinity_map)
                self._affinity_map.clear()
                return count
            if key in self._affinity_map:
                del self._affinity_map[key]
                return 1
            return 0

    def affinity_map_size(self) -> int:
        """Return the number of active sticky-session bindings."""
        with self._lock:
            return len(self._affinity_map)

    
    # WebSocket / long-connection draining
    

    @contextmanager
    def ws_connection(self, server: Server) -> "Generator[Server, None, None]":
        """
        Context manager for tracking long-lived connections (WebSocket, SSE,
        HTTP streaming, gRPC, etc.).

        Increments the server's active_connections count on entry and
        decrements it on exit even if an exception is raised.

        While active_connections > 0 and ws_drain_timeout_sec > 0, the
        cluster will not immediately evict the server to the outer ring.
        Instead it is marked as DRAINING -- no new requests are routed to
        it, but existing connections are allowed to finish.

        Args:
            server: The server returned by get_server() for this connection.

        Example::

            server = cluster.get_server()
            with cluster.ws_connection(server) as s:
                await websocket.handle(s.host, s.port)
        """
        server.metrics.active_connections += 1
        try:
            yield server
        finally:
            server.metrics.active_connections = max(
                0, server.metrics.active_connections - 1
            )

    def ws_open(self, server: Server) -> None:
        """
        Manually register one long-lived connection on server.

        Use ws_connection() for automatic cleanup.  Use this only when
        the connection lifetime does not map cleanly to a Python scope
        (e.g. async tasks, callbacks).

        Args:
            server: The server this connection belongs to.
        """
        server.metrics.active_connections += 1

    def ws_close(self, server: Server) -> None:
        """
        Manually release one long-lived connection on server.

        Must be called exactly once per ws_open() call, even on error.
        active_connections is clamped to 0 to guard against double-close.

        Args:
            server: The server whose connection just closed.
        """
        server.metrics.active_connections = max(
            0, server.metrics.active_connections - 1
        )

    def draining_servers(self) -> list:
        """
        Return servers currently in the DRAINING state.

        Draining servers are no longer receiving new requests but are
        still finishing open long-lived connections before moving to the
        outer ring.

        Returns:
            List of Server objects with position == Position.DRAINING.
        """
        with self._lock:
            return [s for s, _, _ in self._draining_servers.values()]

    
    # Canary / rolling deploy
    

    def start_traffic_ramp(
        self,
        server_id:     str,
        initial_weight: float = 0.05,
        target_weight:  float = 1.0,
        ramp_sec:       float = 60.0,
    ) -> TrafficRamp:
        """
        Gradually increase traffic to a server (canary / rolling deploy).

        Starts a linear ramp from initial_weight to target_weight over
        ramp_sec seconds.  While a ramp is active, get_server() uses
        weighted random selection instead of plain round-robin so that
        the canary server receives only its proportional share of traffic.

        When the ramp completes, the server's static weight is updated to
        target_weight and the ramp entry is removed automatically by the
        rotation loop.

        Args:
            server_id:      ID of the inner-ring server to ramp.
            initial_weight: Starting routing weight.  0.05 means the
                            server receives ~5% of requests relative to
                            weight-1.0 peers.
            target_weight:  Final weight when ramp completes.  Typically
                            1.0 (equal share with all other servers).
            ramp_sec:       Duration of the ramp in seconds.

        Returns:
            The TrafficRamp tracking object.

        Raises:
            ValueError: server_id not found in the cluster.
            ValueError: initial_weight or target_weight out of range (0, inf).

        Example::

            # Deploy new server with 5% canary traffic, ramp to 100% in 10 min
            cluster.add_server(("v2", "10.0.0.4", 8080))
            cluster.start_traffic_ramp("v2",
                initial_weight=0.05,
                target_weight=1.0,
                ramp_sec=600.0)
        """
        if initial_weight <= 0:
            raise ValueError(f"initial_weight must be > 0, got {initial_weight}")
        if target_weight <= 0:
            raise ValueError(f"target_weight must be > 0, got {target_weight}")
        if ramp_sec < 0:
            raise ValueError(f"ramp_sec must be >= 0, got {ramp_sec}")

        with self._lock:
            all_ids = {s.id for s in self.all_servers()}
            if server_id not in all_ids:
                raise ValueError(
                    f"Server {server_id!r} not found in cluster. "
                    f"Known servers: {sorted(all_ids)}"
                )
            ramp = TrafficRamp(
                server_id=server_id,
                start_weight=initial_weight,
                target_weight=target_weight,
                ramp_sec=ramp_sec,
            )
            self._ramps[server_id] = ramp
            log.info(
                f"Traffic ramp started for {server_id!r}: "
                f"{initial_weight}->{target_weight} over {ramp_sec}s"
            )
            return ramp

    def stop_traffic_ramp(self, server_id: str) -> bool:
        """
        Cancel an active traffic ramp and immediately set the server to
        its target weight.

        Args:
            server_id: ID of the server whose ramp to cancel.

        Returns:
            True if a ramp was found and cancelled, False otherwise.
        """
        with self._lock:
            ramp = self._ramps.pop(server_id, None)
            if ramp is None:
                return False
            # Apply target weight immediately
            for s in self.all_servers():
                if s.id == server_id:
                    s.weight = ramp.target_weight
                    break
            log.info(
                f"Traffic ramp cancelled for {server_id!r}; "
                f"weight set to {ramp.target_weight}"
            )
            return True

    def canary_status(self) -> list:
        """
        Return current status of all active traffic ramps.

        Returns:
            List of dicts with keys: server_id, start_weight, target_weight,
            ramp_sec, current_weight, progress_pct, complete.

        Example::

            for status in cluster.canary_status():
                print(f"{status['server_id']}: "
                      f"{status['progress_pct']:.1f}% "
                      f"(weight={status['current_weight']:.3f})")
        """
        with self._lock:
            return [ramp.to_dict() for ramp in self._ramps.values()]

    def _update_ramps(self) -> None:
        """
        Called each rotation cycle to clean up completed ramps.

        When a ramp completes, the server's static weight is set to
        target_weight and the ramp entry is removed from _ramps so that
        get_server() reverts to fast round-robin.
        """
        if not self._ramps:
            return

        completed = [sid for sid, r in self._ramps.items() if r.is_complete()]
        server_map = {s.id: s for s in self.all_servers()}

        for sid in completed:
            ramp = self._ramps.pop(sid)
            s    = server_map.get(sid)
            if s:
                s.weight = ramp.target_weight
            log.info(
                f"Traffic ramp complete for {sid!r}; "
                f"weight set to {ramp.target_weight}"
            )

    def _get_server_excluding(self, excluded_ids: set) -> Optional[Server]:
        """
        Pick the next inner-ring server whose ID is not in excluded_ids.

        Rotates the ring so that load is distributed across retries.
        If every inner server has already been tried, falls back to the
        round-robin choice (better than returning None and giving up).

        Args:
            excluded_ids: Set of server IDs to skip if possible.

        Returns:
            A Server instance, or None if the cluster is completely empty.
        """
        with self._lock:
            if not self._inner_ring:
                return self._emergency_server()

            n = len(self._inner_ring)
            for _ in range(n):
                server = self._inner_ring[0]
                self._inner_ring.rotate(-1)
                if server.id not in excluded_ids:
                    return server

            # Every server already tried -- return round-robin choice anyway
            server = self._inner_ring[0]
            self._inner_ring.rotate(-1)
            return server

    def request_with_retry(
        self,
        fn: "Callable[[Server], Any]",
        max_retries: int = 2,
        retry_on: tuple = (Exception,),
        affinity_key: Optional[str] = None,
    ) -> "Any":
        """
        Execute fn(server) with automatic retry on a different server each time.

        On failure the failed server's error_rate is penalised and a fresh
        server is chosen for the next attempt.  Retries always use a server
        that has not been tried in this call (if one is available), so a
        single bad server cannot stall repeated retries.

        Args:
            fn:            Callable ``fn(server) -> result``.  Must be safe to
                           call more than once (i.e. idempotent for your use
                           case -- GET requests usually are, POST may not be).
            max_retries:   Extra attempts after the first failure.  Total
                           attempts = max_retries + 1.  Default 2.
            retry_on:      Tuple of exception types that trigger a retry.
                           Any exception NOT in this tuple is re-raised
                           immediately without consuming a retry slot.
                           Default: (Exception,) -- retry on anything.
            affinity_key:  Optional sticky-session key used for the FIRST
                           attempt only.  On retry a fresh server is chosen
                           regardless of the key, because the bound server
                           just failed.

        Returns:
            Whatever fn(server) returns on the first successful attempt.

        Raises:
            RetryExhaustedError: All attempts failed.  Inspect
                ``.last_error``, ``.attempts``, ``.tried_server_ids``.
            Any exception not in retry_on: re-raised immediately.

        Example::

            import requests as req

            def fetch(server):
                r = req.get(
                    f"http://{server.host}:{server.port}/api/data",
                    timeout=2,
                )
                r.raise_for_status()
                return r.json()

            try:
                data = cluster.request_with_retry(
                    fetch,
                    max_retries=2,
                    retry_on=(req.exceptions.ConnectionError,
                              req.exceptions.Timeout),
                )
            except RetryExhaustedError as e:
                logger.error("All servers failed: %s", e.last_error)
        """
        if not isinstance(retry_on, tuple):
            retry_on = tuple(retry_on)

        tried_ids:  list           = []
        last_exc:   Optional[Exception] = None

        for attempt in range(max_retries + 1):
            # First attempt: respect affinity_key if given.
            # Subsequent attempts: exclude already-tried servers.
            if attempt == 0:
                if affinity_key is not None:
                    server = self.get_server(affinity_key=affinity_key)
                else:
                    server = self.get_server()
            else:
                server = self._get_server_excluding(set(tried_ids))

            if server is None:
                break

            tried_ids.append(server.id)
            t0 = time.perf_counter()

            try:
                result   = fn(server)
                elapsed  = (time.perf_counter() - t0) * 1000
                self.record_latency(server, elapsed)

                if attempt > 0:
                    with self._lock:
                        self._retry_stats["successful_retries"] += 1

                return result

            except Exception as exc:
                elapsed = (time.perf_counter() - t0) * 1000
                self.record_latency(server, elapsed)

                # Non-retryable exception -- propagate immediately
                if not isinstance(exc, retry_on):
                    raise

                # Penalise the failed server
                server.metrics.error_rate = min(
                    1.0, server.metrics.error_rate * 0.9 + 0.1
                )
                if server.metrics.error_rate > 0.5:
                    server.metrics.is_healthy = False
                    self._fire_alert(
                        event="circuit_breaker",
                        level="WARNING",
                        server_id=server.id,
                        data={
                            "error_rate":  round(server.metrics.error_rate, 4),
                            "temperature": round(server.temperature, 4),
                            "context":     "request_with_retry",
                        },
                    )
                server.update_temperature()

                last_exc = exc
                with self._lock:
                    if attempt < max_retries:
                        self._retry_stats["total_retries"] += 1

        # All attempts exhausted
        with self._lock:
            self._retry_stats["exhausted_retries"] += 1

        self._fire_alert(
            event="retry_exhausted",
            level="WARNING",
            server_id=None,
            data={
                "attempts":      len(tried_ids),
                "tried_servers": tried_ids,
                "last_error":    repr(last_exc),
            },
        )

        raise RetryExhaustedError(
            last_error=last_exc,
            attempts=len(tried_ids),
            tried_server_ids=tried_ids,
        )
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
        Run one full rotation cycle (the penguin huddle step).

        Normally called automatically by the background daemon every
        rotation_interval_sec. Can be called manually for testing or
        custom scheduling.

        Steps:
            1. Evict overheated or floor-breaching inner servers to outer ring
               (capped at max(1, |I|/3) per cycle -- thundering herd prevention).
            2. Promote coolest eligible outer server to inner ring
               (gated by min_outer_dwell_sec -- flapping prevention).
            3. Evict unhealthy or circuit-breaker-tripped inner servers.

        Returns:
            True if any server changed rings during this cycle.
        

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

            # Step 3: Health evictions + circuit breaker (v1.3.3)
            # Note: health/circuit evictions also respect min_inner_size guard
            # but are NOT subject to the thundering herd cap (intentional --
            # unhealthy servers should be removed promptly).
            for server in list(self._inner_ring):
                if len(self._inner_ring) <= self.min_inner_size:
                    break
                unhealthy    = not server.metrics.is_healthy
                circuit_open = (
                    self.circuit_breaker_threshold < 1.0
                    and server.metrics.error_rate >= self.circuit_breaker_threshold
                )
                if unhealthy or circuit_open:
                    self._move_to_outer(server, EvictionReason.HEALTH_FAIL)
                    rotated = True

            # Degraded cluster alert: inner ring below minimum size
            if len(self._inner_ring) < self.min_inner_size:
                self._fire_alert(
                    event="degraded",
                    level="CRITICAL",
                    server_id=None,
                    data={
                        "inner_count":    len(self._inner_ring),
                        "min_inner_size": self.min_inner_size,
                        "outer_count":    len(self._outer_ring),
                    },
                )

            return rotated

    #  Internal Move Helpers 

    def _move_to_outer(self, server: Server, reason: EvictionReason) -> None:
        now     = time.monotonic()
        elapsed = now - server.last_rotated
        server.total_inner_time       += elapsed
        server.last_rotated            = now
        server.rotation_count         += 1

        # Purge stale affinity bindings for this server so future requests
        # with the same key are re-mapped to a healthy inner server.
        stale_keys = [k for k, v in self._affinity_map.items() if v == server.id]
        for k in stale_keys:
            del self._affinity_map[k]
        server._consecutive_evictions += 1

        if reason == EvictionReason.ABSOLUTE_LATENCY:
            server.temperature = max(server.temperature, 0.8)

        # Remove from inner ring -- server stops receiving new requests now
        self._inner_ring.remove(server)

        # If draining is enabled and the server has open long-lived connections,
        # park it in the draining dict instead of the outer ring.
        # The rotation loop calls _check_draining_servers() each cycle and
        # completes the eviction once all connections close or the timeout elapses.
        if (self._ws_drain_timeout > 0
                and server.metrics.active_connections > 0
                and server.id not in self._draining_servers):
            server.position = Position.DRAINING
            self._draining_servers[server.id] = (server, now, reason)
            log.info(
                f" {server.id!r} inner->draining  "
                f"connections={server.metrics.active_connections}  "
                f"timeout={self._ws_drain_timeout}s  "
                f"reason={reason.value}"
            )
            return  # Eviction alert fires later when drain completes

        # No draining needed -- move directly to outer ring
        self._complete_eviction(server, reason)

    def _complete_eviction(self, server: Server, reason: EvictionReason) -> None:
        """
        Place server in the outer ring and fire the eviction event.

        Called directly from _move_to_outer() when draining is not needed,
        and from _check_draining_servers() when drain completes.
        """
        server.position = Position.OUTER
        server.last_rotated = time.monotonic()
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

        if self._on_eviction:
            try:
                self._on_eviction(server, reason)
            except Exception as exc:
                log.warning(f"on_eviction callback error: {exc}")

        log.info(
            f" {server.id!r} inner->outer  "
            f"reason={reason.value}  temp={server.temperature:.3f}  "
            f"evictions={server._consecutive_evictions}"
        )

        self._fire_alert(
            event="eviction",
            level="CRITICAL" if server.temperature >= 0.9 else "WARNING",
            server_id=server.id,
            data={
                "reason":                reason.value,
                "temperature":           round(server.temperature, 4),
                "inner_count":           len(self._inner_ring),
                "outer_count":           len(self._outer_ring),
                "consecutive_evictions": server._consecutive_evictions,
            },
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
            f" {server.id!r} outer->inner  "
            f"reason=cooled  temp={server.temperature:.3f}"
        )

        self._fire_alert(
            event="promotion",
            level="INFO",
            server_id=server.id,
            data={
                "temperature": round(server.temperature, 4),
                "inner_count": len(self._inner_ring),
            },
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
        """
        Start the background rotation daemon thread.

        If state_file was set in the constructor and the file exists,
        server temperatures and metrics are restored automatically before
        the first rotation cycle runs.

        If checkpoint_interval_sec > 0, a background checkpoint thread
        saves state to state_file every checkpoint_interval_sec seconds.
        """
        if self._running:
            log.warning("HuddleCluster already running")
            return
        self._running = True

        # Auto-load state before the first rotation cycle
        if self._state_file:
            try:
                self.load_state()
            except Exception as exc:
                log.warning(f"Could not load state from {self._state_file!r}: {exc}")

        self._rotation_thread = threading.Thread(
            target=self._rotation_loop,
            args=(rotation_interval_sec,),
            name="huddle-rotation",
            daemon=True,
        )
        self._rotation_thread.start()

        # Auto-checkpoint thread
        if self._checkpoint_interval > 0 and self._state_file:
            self._checkpoint_thread = threading.Thread(
                target=self._checkpoint_loop,
                name="huddle-checkpoint",
                daemon=True,
            )
            self._checkpoint_thread.start()

        if self._gossip_agent:
            self._gossip_agent.start(self)

        # Alert delivery thread
        if self._alert_webhooks:
            self._alert_thread = threading.Thread(
                target=self._alert_delivery_loop,
                name="huddle-alerts",
                daemon=True,
            )
            self._alert_thread.start()

        # Built-in HTTP health checker thread
        if self._health_check_path:
            self._health_check_thread = threading.Thread(
                target=self._health_check_loop,
                name="huddle-health",
                daemon=True,
            )
            self._health_check_thread.start()

        log.info(f"HuddleCluster started (interval={rotation_interval_sec}s)")

    def stop(self, timeout: float = 5.0, drain_timeout_sec: float = 0.0) -> None:
        """
        Gracefully stop the rotation daemon.

        drain_timeout_sec: if > 0, wait for in-flight requests to complete
            before stopping. Stops waiting when all active_connections reach 0
            or the timeout elapses.

        If state_file was set, the final cluster state is saved to disk
        before the thread is joined so the next startup restores temperatures.
        """
        if drain_timeout_sec > 0:
            log.info(f"Draining connections (timeout={drain_timeout_sec}s)...")
            deadline = time.monotonic() + drain_timeout_sec
            while time.monotonic() < deadline:
                with self._lock:
                    total_active = sum(
                        s.metrics.active_connections for s in self._inner_ring
                    )
                if total_active == 0:
                    log.info("All connections drained.")
                    break
                time.sleep(0.05)
            else:
                log.warning("Drain timeout elapsed; stopping with active connections.")

        self._running = False

        # Save state before threads stop so we capture the latest temperatures
        if self._state_file:
            try:
                self.save_state()
            except Exception as exc:
                log.warning(f"Could not save state to {self._state_file!r}: {exc}")

        if self._rotation_thread and self._rotation_thread.is_alive():
            self._rotation_thread.join(timeout=timeout)
        if self._checkpoint_thread and self._checkpoint_thread.is_alive():
            self._checkpoint_thread.join(timeout=timeout)
        if self._alert_thread and self._alert_thread.is_alive():
            # Sentinel None tells the delivery loop to drain remaining
            # items and exit cleanly
            self._alert_queue.put(None)
            self._alert_thread.join(timeout=timeout)
        if self._health_check_thread and self._health_check_thread.is_alive():
            self._health_check_thread.join(timeout=timeout)
        if self._gossip_agent:
            self._gossip_agent.stop()
        log.info("HuddleCluster stopped")

    def _rotation_loop(self, interval: float) -> None:
        while self._running:
            try:
                if self._metrics_updater:
                    for s in self.all_servers():
                        self._metrics_updater(s)
                        s.update_temperature()
                else:
                    for s in self.all_servers():
                        s.update_temperature()

                self._check_draining_servers()
                self._update_ramps()
                self.rotate()

            except Exception as exc:
                log.exception(f"Rotation loop error: {exc}")

            time.sleep(interval)

    def _check_draining_servers(self) -> None:
        """
        Complete eviction for servers whose drain period has finished.

        Called every rotation cycle.  A draining server moves to the outer
        ring when either:
          - active_connections reaches 0 (all long-lived connections closed), or
          - ws_drain_timeout_sec elapses (hard deadline).

        Must be called under the cluster lock or from the rotation thread
        (which holds no external lock but is the sole writer to _draining_servers).
        """
        if not self._draining_servers:
            return

        now       = time.monotonic()
        completed = []

        for sid, (server, drain_start, reason) in list(self._draining_servers.items()):
            connections = server.metrics.active_connections
            elapsed     = now - drain_start

            if connections <= 0:
                completed.append((sid, server, reason, elapsed, connections, "drained"))
            elif elapsed >= self._ws_drain_timeout:
                completed.append((sid, server, reason, elapsed, connections, "timeout"))

        for sid, server, reason, elapsed, conns, why in completed:
            del self._draining_servers[sid]

            if why == "timeout":
                log.warning(
                    f" {server.id!r} drain->outer (timeout after {elapsed:.1f}s, "
                    f"connections={conns} still open)"
                )
            else:
                log.info(
                    f" {server.id!r} drain->outer (all connections closed, "
                    f"elapsed={elapsed:.1f}s)"
                )

            self._complete_eviction(server, reason)

    def _checkpoint_loop(self) -> None:
        """
        Background thread: save state to disk every checkpoint_interval_sec.
        Exits cleanly when _running becomes False.
        """
        while self._running:
            time.sleep(self._checkpoint_interval)
            if not self._running:
                break
            try:
                self.save_state()
            except Exception as exc:
                log.warning(f"Auto-checkpoint failed: {exc}")

    
    # Built-in HTTP health checker
    

    def _health_check_loop(self) -> None:
        """
        Background thread: ping each server's health endpoint periodically.

        Sleeps for health_check_interval_sec BEFORE the first check so that
        a freshly started cluster is not hammered immediately, and tests can
        manipulate server state before the first automatic probe.
        Exits cleanly when _running becomes False.
        """
        while self._running:
            # Sleep first (before checking) so the first probe does not race
            # with startup code and does not fire before the caller has had a
            # chance to set up their own state.
            deadline = time.monotonic() + self._health_check_interval
            while self._running and time.monotonic() < deadline:
                time.sleep(0.25)
            if not self._running:
                break
            try:
                self._run_health_checks()
            except Exception as exc:
                log.warning(f"Health check loop error: {exc}")

    def _run_health_checks(self) -> None:
        """Ping the health endpoint on every registered server once."""
        path = self._health_check_path
        if not path:
            return

        servers = self.all_servers()
        for server in servers:
            url = f"http://{server.host}:{server.port}{path}"
            healthy = False
            try:
                req = urllib.request.Request(url, method="GET")
                with urllib.request.urlopen(req, timeout=self._health_check_timeout) as resp:
                    healthy = 200 <= resp.status < 300
            except Exception:
                healthy = False

            with self._lock:
                sid = server.id
                if healthy:
                    if not server.metrics.is_healthy:
                        # Recovery: restore and reset failure counter
                        server.metrics.is_healthy = True
                        self._health_fail_counts[sid] = 0
                        log.info(f"Health check: {sid!r} recovered ({url})")
                        self._fire_alert(
                            "health_recovered", "INFO",
                            server_id=sid,
                            data={"url": url},
                        )
                    else:
                        self._health_fail_counts[sid] = 0
                else:
                    count = self._health_fail_counts.get(sid, 0) + 1
                    self._health_fail_counts[sid] = count
                    log.warning(
                        f"Health check: {sid!r} failed ({url}) "
                        f"[{count}/{self._health_check_failures}]"
                    )
                    if count >= self._health_check_failures:
                        if server.metrics.is_healthy:
                            server.metrics.is_healthy = False
                            log.warning(
                                f"Health check: {sid!r} marked unhealthy "
                                f"after {count} consecutive failures"
                            )
                            self._fire_alert(
                                "circuit_breaker", "WARNING",
                                server_id=sid,
                                data={
                                    "reason":          "health_check_failed",
                                    "consecutive_failures": count,
                                    "url":             url,
                                },
                            )

    def health_check_status(self) -> list:
        """
        Return the current health-check state for every registered server.

        Returns:
            List of dicts with keys: server_id, is_healthy, consecutive_failures,
            health_check_url. Empty list when health_check_path is not configured.

        Example::

            for s in cluster.health_check_status():
                print(s["server_id"], "healthy" if s["is_healthy"] else "UNHEALTHY")
        """
        if not self._health_check_path:
            return []
        result = []
        for server in self.all_servers():
            result.append({
                "server_id":           server.id,
                "is_healthy":          server.metrics.is_healthy,
                "consecutive_failures": self._health_fail_counts.get(server.id, 0),
                "health_check_url":    (
                    f"http://{server.host}:{server.port}{self._health_check_path}"
                ),
            })
        return result

    
    # Admin REST API
    

    def serve_admin(self, port: int = 9000, host: str = "127.0.0.1") -> int:
        """
        Start a lightweight HTTP admin API on the given port.

        The admin server runs in a background daemon thread and does not
        block the caller. Call stop_admin() to shut it down, or it exits
        automatically when the main process ends.

        Endpoints
        ---------
        GET  /admin/health          Full health_report() as JSON
        GET  /admin/servers         All servers with metrics
        GET  /admin/canary          Active traffic ramp status
        GET  /admin/alerts          Recent alert history (last 20)
        POST /admin/evict/<id>      Force-evict server to outer ring
        POST /admin/set_healthy/<id>?healthy=true|false
                                    Manually set server is_healthy flag
        POST /admin/clear_affinity  Clear all sticky-session bindings
        POST /admin/ramp/<id>?initial=0.1&target=1.0&ramp_sec=60
                                    Start a traffic ramp on a server
        POST /admin/stop_ramp/<id>  Cancel an active traffic ramp

        Args:
            port: TCP port to listen on (default 9000).
            host: Interface to bind to (default 127.0.0.1 = loopback only).

        Returns:
            The port the admin server is bound to.

        Example::

            cluster.start()
            cluster.serve_admin(port=9000)
            # curl http://127.0.0.1:9000/admin/health
        """
        if self._admin_server is not None:
            raise RuntimeError("Admin server is already running.")

        cluster = self  # closure reference

        class _AdminHandler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass  # silence default access log

            def _send_json(self, data: object, status: int = 200) -> None:
                body = json.dumps(data, indent=2).encode()
                try:
                    self.send_response(status)
                    self.send_header("Content-Type",   "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError,
                        ConnectionAbortedError, OSError):
                    pass

            def _send_error(self, msg: str, status: int = 400) -> None:
                self._send_json({"error": msg}, status)

            def do_GET(self):
                p = self.path.rstrip("/")
                if p == "/admin/health":
                    self._send_json(cluster.health_report())
                elif p == "/admin/servers":
                    self._send_json([
                        {
                            "id":             s.id,
                            "host":           s.host,
                            "port":           s.port,
                            "position":       s.position.value,
                            "temperature":    round(s.temperature, 4),
                            "weight":         s.weight,
                            "is_healthy":     s.metrics.is_healthy,
                            "avg_latency_ms": round(s.metrics.avg_response_ms, 2),
                            "error_rate":     round(s.metrics.error_rate, 4),
                            "rotation_count": s.rotation_count,
                        }
                        for s in cluster.all_servers()
                    ])
                elif p == "/admin/canary":
                    self._send_json(cluster.canary_status())
                elif p == "/admin/alerts":
                    self._send_json(cluster.alert_history(limit=20))
                else:
                    self._send_error("Unknown endpoint", 404)

            def do_POST(self):
                parsed = urllib.parse.urlparse(self.path)
                path   = parsed.path.rstrip("/")
                params = dict(urllib.parse.parse_qsl(parsed.query))

                if path.startswith("/admin/evict/"):
                    sid = path[len("/admin/evict/"):]
                    ok  = cluster.force_evict(sid)
                    if ok:
                        self._send_json({"evicted": sid})
                    else:
                        self._send_error(f"Server {sid!r} not in inner ring", 404)

                elif path.startswith("/admin/set_healthy/"):
                    sid     = path[len("/admin/set_healthy/"):]
                    healthy = params.get("healthy", "true").lower() != "false"
                    servers = {s.id: s for s in cluster.all_servers()}
                    if sid not in servers:
                        self._send_error(f"Unknown server {sid!r}", 404)
                        return
                    servers[sid].metrics.is_healthy = healthy
                    self._send_json({"server_id": sid, "is_healthy": healthy})

                elif path == "/admin/clear_affinity":
                    removed = cluster.clear_affinity()
                    self._send_json({"removed_bindings": removed})

                elif path.startswith("/admin/ramp/"):
                    sid = path[len("/admin/ramp/"):]
                    try:
                        ramp = cluster.start_traffic_ramp(
                            sid,
                            initial_weight=float(params.get("initial", 0.05)),
                            target_weight= float(params.get("target",  1.0)),
                            ramp_sec=      float(params.get("ramp_sec", 60.0)),
                        )
                        self._send_json(ramp.to_dict())
                    except ValueError as exc:
                        self._send_error(str(exc), 400)

                elif path.startswith("/admin/stop_ramp/"):
                    sid = path[len("/admin/stop_ramp/"):]
                    ok  = cluster.stop_traffic_ramp(sid)
                    self._send_json({"stopped": ok, "server_id": sid})

                else:
                    self._send_error("Unknown endpoint", 404)

        class _ReuseAdminServer(http.server.HTTPServer):
            allow_reuse_address = True

        srv = _ReuseAdminServer((host, port), _AdminHandler)
        self._admin_port   = srv.server_address[1]
        self._admin_server = srv

        self._admin_thread = threading.Thread(
            target=srv.serve_forever,
            name="huddle-admin",
            daemon=True,
        )
        self._admin_thread.start()
        log.info(f"Admin API listening on http://{host}:{self._admin_port}/admin/")
        return self._admin_port

    def stop_admin(self) -> None:
        """
        Shut down the admin HTTP server started by serve_admin().

        Safe to call even if the admin server was never started.
        """
        if self._admin_server is not None:
            self._admin_server.shutdown()
            self._admin_server.server_close()
            self._admin_server = None
            self._admin_port   = None
        if self._admin_thread and self._admin_thread.is_alive():
            self._admin_thread.join(timeout=3.0)
            self._admin_thread = None
        log.info("Admin API stopped")

    
    # Real-time Web Dashboard
    

    def serve_dashboard(self, port: int = 8888, host: str = "127.0.0.1") -> int:
        """
        Start a real-time web dashboard on the given port.

        Opens a browser-ready HTML page that auto-refreshes cluster state
        every 2 seconds using Server-Sent Events (SSE). No external
        dependencies -- pure stdlib HTTP server + inline HTML/JS/CSS.

        Args:
            port: TCP port to listen on (default 8888). Pass 0 for OS-assigned.
            host: Interface to bind (default 127.0.0.1 = loopback only).

        Returns:
            The port the dashboard server is bound to.

        Example::

            cluster.start()
            port = cluster.serve_dashboard(port=8888)
            # Open http://127.0.0.1:8888 in your browser
        """
        if hasattr(self, "_dashboard_server") and self._dashboard_server is not None:
            raise RuntimeError("Dashboard server is already running.")

        cluster = self
        _DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>HuddleCluster Dashboard</title>
<style>
  :root{--bg:#0f1117;--card:#1a1d27;--border:#2a2d3a;--text:#e2e8f0;
        --muted:#64748b;--green:#22c55e;--red:#ef4444;--amber:#f59e0b;
        --blue:#3b82f6;--purple:#a855f7}
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:var(--bg);color:var(--text);font-family:
       system-ui,-apple-system,sans-serif;font-size:14px;padding:1.5rem}
  h1{font-size:1.25rem;font-weight:600;margin-bottom:1.25rem;
     display:flex;align-items:center;gap:.75rem}
  .dot{width:8px;height:8px;border-radius:50%;background:var(--green);
       animation:pulse 2s infinite}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
  .grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));
        gap:1rem;margin-bottom:1.25rem}
  .card{background:var(--card);border:1px solid var(--border);
        border-radius:.75rem;padding:1rem}
  .card h2{font-size:.75rem;font-weight:500;color:var(--muted);
           text-transform:uppercase;letter-spacing:.05em;margin-bottom:.75rem}
  .stat{font-size:1.75rem;font-weight:600;line-height:1}
  .stat-sub{font-size:.75rem;color:var(--muted);margin-top:.25rem}
  .ring{display:flex;flex-direction:column;gap:.5rem;margin-bottom:.75rem}
  .ring-label{font-size:.75rem;font-weight:500;color:var(--muted);
              margin-bottom:.25rem;text-transform:uppercase;letter-spacing:.05em}
  .server{background:var(--bg);border:1px solid var(--border);border-radius:.5rem;
          padding:.6rem .75rem;display:flex;align-items:center;gap:.5rem}
  .server-id{font-weight:500;flex:1;font-size:.8rem}
  .badge{font-size:.7rem;padding:.1rem .45rem;border-radius:.25rem;font-weight:500}
  .badge-inner{background:#14532d;color:#86efac}
  .badge-outer{background:#422006;color:#fed7aa}
  .badge-drain{background:#312e81;color:#a5b4fc}
  .temp-bar{height:4px;border-radius:2px;background:var(--border);width:80px;
            overflow:hidden;flex-shrink:0}
  .temp-fill{height:100%;border-radius:2px;transition:width .3s}
  .ms{font-size:.75rem;color:var(--muted);flex-shrink:0}
  .health{width:8px;height:8px;border-radius:50%;flex-shrink:0}
  .h-ok{background:var(--green)}
  .h-bad{background:var(--red)}
  table{width:100%;border-collapse:collapse;font-size:.8rem}
  th{text-align:left;padding:.4rem .6rem;color:var(--muted);font-weight:500;
     border-bottom:1px solid var(--border)}
  td{padding:.4rem .6rem;border-bottom:1px solid #1e2130}
  tr:last-child td{border-bottom:none}
  .ts{color:var(--muted);font-size:.7rem}
  #err{color:var(--red);font-size:.8rem;padding:.5rem 0;display:none}
</style>
</head>
<body>
<h1><span class="dot"></span>HuddleCluster &mdash; Live Dashboard</h1>
<div id="err">Connection lost. Reconnecting...</div>
<div class="grid" id="stats"></div>
<div class="grid">
  <div class="card" style="grid-column:1/-1">
    <h2>Servers</h2>
    <div id="servers"></div>
  </div>
</div>
<div class="grid">
  <div class="card" style="grid-column:1/-1">
    <h2>Recent Rotations</h2>
    <table id="rotations">
      <thead><tr><th>Server</th><th>Direction</th><th>Reason</th>
      <th>Temp</th><th>Time</th></tr></thead>
      <tbody></tbody>
    </table>
  </div>
</div>
<script>
function tempColor(t){
  if(t<0.3)return'#22c55e';
  if(t<0.6)return'#f59e0b';
  return'#ef4444';
}
function render(d){
  // Stats row
  document.getElementById('stats').innerHTML=`
    <div class="card"><h2>Status</h2>
      <div class="stat" style="color:${d.status==='healthy'?'#22c55e':'#ef4444'}">${d.status}</div>
      <div class="stat-sub">inner ${d.inner_count} / outer ${d.outer_count}</div>
    </div>
    <div class="card"><h2>Avg Temp (inner)</h2>
      <div class="stat">${(d.avg_inner_temp*100).toFixed(1)}%</div>
      <div class="stat-sub">max ${(d.max_inner_temp*100).toFixed(1)}%</div>
    </div>
    <div class="card"><h2>Fairness (Gini)</h2>
      <div class="stat">${d.fairness_score.toFixed(3)}</div>
      <div class="stat-sub">0=perfect, 1=one server</div>
    </div>
    <div class="card"><h2>Req/s</h2>
      <div class="stat">${d.requests_per_sec.toFixed(1)}</div>
      <div class="stat-sub">rotations ${d.total_rotations}</div>
    </div>
    <div class="card"><h2>Retries</h2>
      <div class="stat">${d.retry_stats.total_retries}</div>
      <div class="stat-sub">exhausted ${d.retry_stats.exhausted_retries}</div>
    </div>
    <div class="card"><h2>Canary Ramps</h2>
      <div class="stat">${d.canary_ramps.length}</div>
      <div class="stat-sub">${d.canary_ramps.map(r=>r.server_id+' '+r.progress_pct+'%').join(', ')||'none'}</div>
    </div>`;

  // Servers
  const inner=d.inner_ring.map(s=>`
    <div class="server">
      <span class="health ${s.is_healthy!==false?'h-ok':'h-bad'}"></span>
      <span class="server-id">${s.id}</span>
      <span class="badge badge-inner">inner</span>
      <span class="ms">${s.avg_latency_ms}ms</span>
      <div class="temp-bar"><div class="temp-fill"
        style="width:${(s.temp*100).toFixed(0)}%;background:${tempColor(s.temp)}"></div></div>
    </div>`).join('');
  const drain=(d.draining_ring||[]).map(s=>`
    <div class="server">
      <span class="health h-bad"></span>
      <span class="server-id">${s.id}</span>
      <span class="badge badge-drain">draining ${s.active_connections} conn</span>
    </div>`).join('');
  const outer=d.outer_ring.map(s=>`
    <div class="server">
      <span class="health ${s.cold_start?'h-bad':'h-ok'}"></span>
      <span class="server-id">${s.id}</span>
      <span class="badge badge-outer">outer</span>
      <span class="ms">${s.avg_latency_ms}ms</span>
    </div>`).join('');
  document.getElementById('servers').innerHTML=
    `<div class="ring-label">Inner</div><div class="ring">${inner||'<span style="color:var(--red)">empty</span>'}</div>`+
    (drain?`<div class="ring-label">Draining</div><div class="ring">${drain}</div>`:'')+
    `<div class="ring-label">Outer</div><div class="ring">${outer||'<em style="color:var(--muted)">none</em>'}</div>`;

  // Rotations
  const rows=(d.recent_rotations||[]).slice(-10).reverse().map(r=>`
    <tr><td>${r.server_id}</td><td>${r.direction}</td>
    <td>${r.reason}</td><td>${(r.temperature*100).toFixed(1)}%</td>
    <td class="ts">recent</td></tr>`).join('');
  document.querySelector('#rotations tbody').innerHTML=rows||
    '<tr><td colspan="5" style="color:var(--muted)">No rotations yet</td></tr>';
}

const src=new EventSource('/dashboard/stream');
src.onmessage=e=>{
  document.getElementById('err').style.display='none';
  try{render(JSON.parse(e.data))}catch(ex){console.error(ex)}
};
src.onerror=()=>{document.getElementById('err').style.display='block'};
</script>
</body>
</html>"""

        class _DashHandler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                if self.path in ("/", "/dashboard"):
                    body = _DASHBOARD_HTML.encode()
                    self.send_response(200)
                    self.send_header("Content-Type",   "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)

                elif self.path == "/dashboard/stream":
                    # Server-Sent Events endpoint
                    self.send_response(200)
                    self.send_header("Content-Type",  "text/event-stream")
                    self.send_header("Cache-Control", "no-cache")
                    self.send_header("Connection",    "keep-alive")
                    self.send_header("X-Accel-Buffering", "no")
                    self.end_headers()
                    try:
                        while True:
                            report = cluster.health_report()
                            payload = json.dumps(report)
                            msg = f"data: {payload}\n\n"
                            self.wfile.write(msg.encode())
                            self.wfile.flush()
                            time.sleep(2.0)
                    except (BrokenPipeError, ConnectionResetError,
                            ConnectionAbortedError, OSError):
                        pass

                elif self.path == "/dashboard/snapshot":
                    # One-shot JSON snapshot for testing / curl
                    body = json.dumps(cluster.health_report(), indent=2).encode()
                    self.send_response(200)
                    self.send_header("Content-Type",   "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)

                else:
                    self.send_response(404)
                    self.end_headers()

        class _ReuseServer(http.server.HTTPServer):
            allow_reuse_address = True

        srv = _ReuseServer((host, port), _DashHandler)
        actual_port = srv.server_address[1]

        self._dashboard_server = srv
        self._dashboard_port   = actual_port
        self._dashboard_thread = threading.Thread(
            target=srv.serve_forever,
            name="huddle-dashboard",
            daemon=True,
        )
        self._dashboard_thread.start()
        log.info(
            f"Dashboard listening on http://{host}:{actual_port}/ "
            f"(SSE stream: /dashboard/stream)"
        )
        return actual_port

    def stop_dashboard(self) -> None:
        """
        Shut down the dashboard HTTP server started by serve_dashboard().

        Safe to call even if the dashboard was never started.
        """
        srv = getattr(self, "_dashboard_server", None)
        if srv is not None:
            srv.shutdown()
            srv.server_close()
            self._dashboard_server = None
            self._dashboard_port   = None
        t = getattr(self, "_dashboard_thread", None)
        if t and t.is_alive():
            t.join(timeout=3.0)
            self._dashboard_thread = None
        log.info("Dashboard stopped")

    def _fire_alert(
        self,
        event:     str,
        level:     str,
        server_id: Optional[str] = None,
        data:      Optional[dict] = None,
    ) -> None:
        """
        Enqueue an alert for async delivery to configured webhook URLs.

        Returns immediately -- HTTP delivery happens in the background
        alert thread so routing latency is never affected.

        If no webhooks are configured, or the event is not in alert_on,
        this is a no-op.

        If the alert queue is full (1000 pending alerts), the alert is
        dropped and a warning is logged.  This prevents a slow/down
        webhook from consuming unbounded memory.

        Args:
            event:     Short event name (e.g. "eviction", "degraded").
            level:     Severity -- "INFO", "WARNING", or "CRITICAL".
            server_id: ID of affected server, or None for cluster events.
            data:      Additional context dict included in the payload.
        """
        if not self._alert_webhooks or event not in self._alert_on:
            return

        alert = AlertEvent(
            event=event,
            level=level,
            timestamp=time.time(),
            server_id=server_id,
            data=data or {},
        )
        self._alert_history.append(alert)

        try:
            self._alert_queue.put_nowait(alert)
        except _queue.Full:
            log.warning(
                "Alert queue full (%d pending); dropping %r alert.",
                self._alert_queue.qsize(), event,
            )

    def _alert_delivery_loop(self) -> None:
        """
        Background thread: drain the alert queue and POST to each webhook.

        Runs until a None sentinel is placed in the queue (by stop()).
        HTTP errors are logged as warnings and never crash the thread.
        Each alert is delivered to ALL configured webhook URLs.
        """
        while True:
            try:
                alert = self._alert_queue.get(timeout=1.0)
            except _queue.Empty:
                if not self._running:
                    break
                continue

            if alert is None:
                # Drain remaining items before exiting
                while not self._alert_queue.empty():
                    item = self._alert_queue.get_nowait()
                    if item is not None:
                        self._deliver_alert(item)
                break

            self._deliver_alert(alert)
            self._alert_queue.task_done()

    def _deliver_alert(self, alert: AlertEvent) -> None:
        """POST one alert to every configured webhook URL."""
        payload = json.dumps(alert.to_dict()).encode("utf-8")
        headers = {"Content-Type": "application/json", **self._alert_headers}

        for url in self._alert_webhooks:
            try:
                req = urllib.request.Request(
                    url, data=payload, headers=headers, method="POST"
                )
                with urllib.request.urlopen(req, timeout=self._alert_timeout):
                    pass
                log.debug("Alert %r delivered to %r", alert.event, url)
            except urllib.error.HTTPError as exc:
                log.warning(
                    "Alert delivery HTTP error to %r: %s %s",
                    url, exc.code, exc.reason,
                )
            except Exception as exc:
                log.warning("Alert delivery to %r failed: %s", url, exc)

    def alert_history(self, limit: int = 20) -> list:
        """
        Return the most recent alerts as a list of dicts.

        Includes alerts regardless of whether HTTP delivery succeeded.
        Useful for debugging and surfacing recent events via the admin API
        or health endpoint.

        Args:
            limit: Maximum number of alerts to return (newest last).
                   Clamped to the history buffer size (100).

        Returns:
            List of alert dicts with keys: event, level, timestamp,
            server_id, data.

        Example::

            for alert in cluster.alert_history(limit=5):
                print(alert["event"], alert["level"], alert["data"])
        """
        return [a.to_dict() for a in list(self._alert_history)[-limit:]]

    
    # Persistent state
    

    def save_state(self, path: Optional[str] = None) -> str:
        """
        Serialize cluster state to a JSON file.

        Saves per-server temperature, metrics, latency histogram samples,
        and rotation counters.  The write is atomic: data goes to a
        ``<path>.tmp`` file first, then renamed over the target so the
        file is never left in a half-written state.

        Args:
            path: Destination file path.  If None, uses the state_file
                  configured in the constructor.

        Returns:
            The path that was written to.

        Raises:
            ValueError: No path provided and no state_file configured.
            OSError:    File system error during write or rename.

        Example::

            cluster.save_state("/var/lib/huddle/cluster.json")
        """
        target = path or self._state_file
        if not target:
            raise ValueError(
                "No state file path given. Pass a path or set "
                "state_file= in the constructor."
            )

        with self._lock:
            servers_data: dict = {}
            for s in self.all_servers():
                servers_data[s.id] = {
                    "temperature":           s.temperature,
                    "avg_response_ms":       s.metrics.avg_response_ms,
                    "error_rate":            s.metrics.error_rate,
                    "latency_anomaly_score": s.metrics.latency_anomaly_score,
                    "rotation_count":        s.rotation_count,
                    "total_inner_time":      s.total_inner_time,
                    "total_outer_time":      s.total_outer_time,
                    # Full 1000-sample histogram window
                    "histogram_samples":     list(s.metrics._histogram_window),
                }
            state = {
                "version":        __version__,
                "saved_at":       time.time(),
                "heat_threshold": self.heat_threshold,
                "servers":        servers_data,
            }

        tmp_path = None
        with self._state_write_lock:
            try:
                target_dir = os.path.dirname(os.path.abspath(target))
                os.makedirs(target_dir, exist_ok=True)
                import tempfile as _tempfile
                with _tempfile.NamedTemporaryFile(
                    mode="w",
                    suffix=".tmp",
                    dir=target_dir,
                    delete=False,
                    encoding="utf-8",
                ) as fh:
                    tmp_path = fh.name
                    json.dump(state, fh, indent=2)
                os.replace(tmp_path, target)
                tmp_path = None
            except Exception:
                if tmp_path and os.path.exists(tmp_path):
                    try:
                        os.remove(tmp_path)
                    except OSError:
                        pass
                raise

        log.info(f"State saved to {target!r} ({len(servers_data)} servers)")
        return target

    def load_state(self, path: Optional[str] = None) -> int:
        """
        Restore cluster state from a JSON file written by save_state().

        Only servers that are currently registered in the cluster are
        restored; extra entries in the file are silently skipped.  This
        means you can safely add or remove servers between restarts.

        After loading, each server's EMA latency window is seeded with
        the last 10 histogram samples so the first rotation cycle has
        realistic latency data immediately.

        Args:
            path: Source file path.  If None, uses the state_file
                  configured in the constructor.

        Returns:
            Number of servers whose state was restored.

        Raises:
            ValueError: No path provided and no state_file configured.
            OSError / json.JSONDecodeError: File cannot be read or parsed.

        Example::

            restored = cluster.load_state("/var/lib/huddle/cluster.json")
            print(f"Restored {restored} servers")
        """
        target = path or self._state_file
        if not target:
            raise ValueError(
                "No state file path given. Pass a path or set "
                "state_file= in the constructor."
            )

        if not os.path.exists(target):
            log.info(f"No state file at {target!r}; starting fresh.")
            return 0

        with open(target, "r", encoding="utf-8") as fh:
            state = json.load(fh)

        servers_data: dict = state.get("servers", {})
        restored = 0

        with self._lock:
            current: dict = {s.id: s for s in self.all_servers()}

            for sid, data in servers_data.items():
                s = current.get(sid)
                if s is None:
                    log.debug(f"load_state: server {sid!r} not in cluster; skipped.")
                    continue

                s.temperature                   = float(data.get("temperature", 0.0))
                s.metrics.avg_response_ms       = float(data.get("avg_response_ms", 0.0))
                s.metrics.error_rate            = float(data.get("error_rate", 0.0))
                s.metrics.latency_anomaly_score = float(data.get("latency_anomaly_score", 0.0))
                s.rotation_count                = int(data.get("rotation_count", 0))
                s.total_inner_time              = float(data.get("total_inner_time", 0.0))
                s.total_outer_time              = float(data.get("total_outer_time", 0.0))

                # Restore histogram window
                samples = [float(v) for v in data.get("histogram_samples", [])]
                s.metrics._histogram_window.clear()
                s.metrics._histogram_window.extend(samples)

                # Seed fast EMA window with last 10 histogram samples
                # so the first rotation cycle has realistic latency data
                s.metrics._latency_window.clear()
                s.metrics._latency_window.extend(samples[-10:])

                restored += 1
                log.debug(
                    f"load_state: restored {sid!r} "
                    f"(temp={s.temperature:.3f}, "
                    f"avg_ms={s.metrics.avg_response_ms:.1f})"
                )

        log.info(
            f"State loaded from {target!r} "
            f"({restored}/{len(servers_data)} servers restored)"
        )
        return restored

    #  Analytics, Fairness & Observability 

    def fairness_score(self) -> float:
        """
        Gini coefficient measuring inner-ring server dwell-time fairness.

        Computes the Gini coefficient over total_inner_time of all currently
        active inner-ring servers. Outer-ring servers are excluded -- they are
        intentionally resting, not unfairly skipped.

        Interpretation:
            0.00 -- perfectly fair (all inner servers share duty equally)
            0.30 -- alert threshold for production monitoring
            1.00 -- completely unfair (one server bears all load)

        Returns:
            Gini coefficient in [0, 1]. Returns 0.0 for clusters with
            fewer than 2 inner-ring servers.
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
        Snapshot of cluster state -- expose via /huddle/health endpoint.

        Each inner-ring server now includes a full latency_histogram dict
        with p50/p75/p90/p95/p99/p999 in milliseconds.
        """
        with self._lock:
            inner = list(self._inner_ring)
            outer = list(self._outer_ring)

        inner_temps = [s.temperature for s in inner]
        return {
            "status":        "degraded" if len(inner) < self.min_inner_size else "healthy",
            "inner_ring": [
                {
                    "id":               s.id,
                    "weight":           s.weight,
                    "tags":             s.tags,
                    "temp":             round(s.temperature, 4),
                    "rotations":        s.rotation_count,
                    "inner_time_sec":   round(s.total_inner_time, 2),
                    "avg_latency_ms":   round(s.metrics.avg_response_ms, 2),
                    "p95_latency_ms":   round(s.metrics.p95_latency(), 2),
                    "anomaly_score":    round(s.metrics.latency_anomaly_score, 4),
                    "latency_histogram": s.metrics.latency_histogram(),
                }
                for s in inner
            ],
            "outer_ring": [
                {
                    "id":             s.id,
                    "weight":         s.weight,
                    "tags":           s.tags,
                    "temp":           round(s.temperature, 4),
                    "outer_time_sec": round(s.total_outer_time, 2),
                    "avg_latency_ms": round(s.metrics.avg_response_ms, 2),
                    "cold_start":     s.is_cold_start(),
                }
                for s in outer
            ],
            "draining_ring": [
                {
                    "id":                  s.id,
                    "active_connections":  s.metrics.active_connections,
                    "drain_elapsed_sec":   round(time.monotonic() - start, 2),
                    "drain_timeout_sec":   self._ws_drain_timeout,
                    "reason":              reason.value,
                }
                for s, start, reason in self._draining_servers.values()
            ],
            "inner_count":      len(inner),
            "outer_count":      len(outer),
            "total_servers":    len(inner) + len(outer),
            "avg_inner_temp":   round(statistics.mean(inner_temps), 4) if inner_temps else 0,
            "max_inner_temp":   round(max(inner_temps), 4) if inner_temps else 0,
            "fairness_score":   round(self.fairness_score(), 4),
            "total_rotations":  sum(s.rotation_count for s in inner + outer),
            "requests_per_sec": round(statistics.mean(self._rps_window), 2) if self._rps_window else 0.0,
            "requests_total":   sum(self._rps_window) if self._rps_window else 0,
            "affinity_bindings": len(self._affinity_map),
            "retry_stats":        dict(self._retry_stats),
            "state_file":         self._state_file,
            "checkpoint_interval_sec": self._checkpoint_interval,
            "canary_ramps":       self.canary_status(),
            "health_checker": {
                "enabled":        self._health_check_path is not None,
                "path":           self._health_check_path,
                "interval_sec":   self._health_check_interval,
                "timeout_sec":    self._health_check_timeout,
                "failure_threshold": self._health_check_failures,
                "servers":        self.health_check_status(),
            },
            "alerts": {
                "webhooks_configured": len(self._alert_webhooks),
                "events_monitored":    sorted(self._alert_on),
                "history_count":       len(self._alert_history),
                "recent":              self.alert_history(limit=5),
            },
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

        v1.4.0 -- Added per-server p50, p99 latency histograms.

        Example FastAPI integration::

            @app.get("/metrics", response_class=PlainTextResponse)
            def metrics():
                return cluster.prometheus_metrics()
        """
        lines = [
            "# HELP huddle_server_temperature EMA temperature score (0=cool, 1=hot)",
            "# TYPE huddle_server_temperature gauge",
        ]
        for s in self.all_servers():
            tag_labels = "".join(f',{k}="{v}"' for k, v in s.tags.items())
            lines.append(
                f'huddle_server_temperature{{server="{s.id}",ring="{s.position.value}"{tag_labels}}} '
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
            "# HELP huddle_server_p50_latency_ms Per-server P50 latency (1000-sample window)",
            "# TYPE huddle_server_p50_latency_ms gauge",
        ]
        for s in self.all_servers():
            lines.append(
                f'huddle_server_p50_latency_ms{{server="{s.id}"}} {s.metrics.p50_latency():.2f}'
            )
        lines += [
            "",
            "# HELP huddle_server_p95_latency_ms Per-server P95 latency (1000-sample window)",
            "# TYPE huddle_server_p95_latency_ms gauge",
        ]
        for s in self.all_servers():
            lines.append(
                f'huddle_server_p95_latency_ms{{server="{s.id}"}} {s.metrics.p95_latency():.2f}'
            )
        lines += [
            "",
            "# HELP huddle_server_p99_latency_ms Per-server P99 latency (1000-sample window)",
            "# TYPE huddle_server_p99_latency_ms gauge",
        ]
        for s in self.all_servers():
            lines.append(
                f'huddle_server_p99_latency_ms{{server="{s.id}"}} {s.metrics.p99_latency():.2f}'
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

        rps = statistics.mean(self._rps_window) if self._rps_window else 0.0
        lines += [
            "# HELP huddle_cluster_requests_per_second Routing throughput",
            "# TYPE huddle_cluster_requests_per_second gauge",
            f"huddle_cluster_requests_per_second {rps:.2f}",
            "",
            "# HELP huddle_cluster_retries_total Extra attempts after first failure",
            "# TYPE huddle_cluster_retries_total counter",
            f"huddle_cluster_retries_total {self._retry_stats['total_retries']}",
            "",
            "# HELP huddle_cluster_retries_successful_total Retries that eventually succeeded",
            "# TYPE huddle_cluster_retries_successful_total counter",
            f"huddle_cluster_retries_successful_total {self._retry_stats['successful_retries']}",
            "",
            "# HELP huddle_cluster_retries_exhausted_total Requests where all retries failed",
            "# TYPE huddle_cluster_retries_exhausted_total counter",
            f"huddle_cluster_retries_exhausted_total {self._retry_stats['exhausted_retries']}",
            "",
        ]
        return "\n".join(lines)

    def all_servers(self) -> list[Server]:
        """Return all registered servers (inner + draining + outer ring). Thread-safe."""
        with self._lock:
            draining = [s for s, _, _ in self._draining_servers.values()]
            return list(self._inner_ring) + draining + list(self._outer_ring)

    def inner_servers(self) -> list[Server]:
        """Return active inner-ring servers in current round-robin order. Thread-safe."""
        with self._lock:
            return list(self._inner_ring)

    def outer_servers(self) -> list[Server]:
        """Return resting outer-ring servers sorted by temperature (coolest first). Thread-safe."""
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
    for idx, addr in enumerate(server_addresses):  # noqa: E501
        sid    = addr[0]
        host   = addr[1]
        port   = addr[2]
        weight = float(addr[3]) if len(addr) > 3 else 1.0
        tags   = addr[4] if len(addr) > 4 else {}
        s      = Server(id=sid, host=host, port=port, weight=weight, tags=tags)
        cluster.add_server(s, force_inner=(idx < cluster.max_inner_size))
    return cluster


#  Demo 

if __name__ == "__main__":
    from time import sleep

    print("HuddleCluster v1.4.0 demo")

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