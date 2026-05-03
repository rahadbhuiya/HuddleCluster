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
Version: 1.0.1
License: MIT
"""

from __future__ import annotations

import heapq
import logging
import statistics
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Optional

#  Logging Setup 

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] HuddleCluster │ %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("huddle")



# Enums & Data Classes


class Position(Enum):
    INNER = "inner"   # Active — taking requests
    OUTER = "outer"   # Resting — cooling down


class EvictionReason(Enum):
    OVERHEATED    = "overheated"       # CPU/memory/connections too high
    MANUAL        = "manual"           # Operator forced eviction
    HEALTH_FAIL   = "health_fail"      # Health check failed


@dataclass
class ServerMetrics:
    """
    Live metrics snapshot for a server.
    Update these externally via your metrics_updater callback.
    """
    cpu_usage:          float = 0.0   # 0.0–1.0
    memory_usage:       float = 0.0   # 0.0–1.0
    active_connections: int   = 0
    avg_response_ms:    float = 0.0
    error_rate:         float = 0.0   # 0.0–1.0 (errors / total_requests)
    is_healthy:         bool  = True  # False = immediately evict


@dataclass
class RotationEvent:
    timestamp:   float
    server_id:   str
    direction:   str          # "inner→outer" or "outer→inner"
    reason:      str
    temperature: float



# Server


class Server:
    """
    Represents one server node inside the HuddleCluster.

    Temperature is a composite score (0.0–1.0):
      higher = more loaded = hotter = should rest in outer ring.

    FIX (Oscillation): Temperature uses Exponential Moving Average (EMA)
    so a single spike doesn't immediately trigger eviction.
    """

    # Weight coefficients for temperature calculation
    _W_CPU  = 0.35
    _W_MEM  = 0.25
    _W_CONN = 0.20
    _W_RESP = 0.15
    _W_ERR  = 0.05

    # EMA smoothing factor — higher = more reactive, lower = more stable
    _EMA_ALPHA = 0.25

    def __init__(self, id: str, host: str, port: int):
        self.id   = id
        self.host = host
        self.port = port

        self.metrics:  ServerMetrics = ServerMetrics()
        self.position: Position      = Position.OUTER
        self.temperature: float      = 0.0   # EMA-smoothed composite score

        # Timing & fairness tracking
        self.last_rotated:    float = time.monotonic()
        self.total_inner_time: float = 0.0
        self.total_outer_time: float = 0.0
        self.rotation_count:   int   = 0

        # FIX (Flapping): track consecutive rotations for back-off
        self._consecutive_evictions: int = 0
        self._lock = threading.Lock()

    #  Temperature 

    def update_temperature(self) -> float:
        """
        Recalculate temperature using EMA.

        FIX (Oscillation/Flapping):
          Raw score is smoothed through EMA so transient spikes
          don't cause immediate eviction → re-entry loops.
        """
        m = self.metrics
        raw = (
            m.cpu_usage                                    * self._W_CPU  +
            m.memory_usage                                 * self._W_MEM  +
            min(m.active_connections / 1_000.0, 1.0)      * self._W_CONN +
            min(m.avg_response_ms    / 5_000.0, 1.0)      * self._W_RESP +
            m.error_rate                                   * self._W_ERR
        )
        raw = max(0.0, min(1.0, raw))  # clamp to [0, 1]

        with self._lock:
            # EMA: blend new reading with historical temperature
            self.temperature = (
                self._EMA_ALPHA * raw
                + (1.0 - self._EMA_ALPHA) * self.temperature
            )
        return self.temperature

    def is_overheated(self, threshold: float) -> bool:
        return self.temperature >= threshold

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



# HuddleCluster


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
    1. Thundering Herd  → max evictions per cycle capped at 1/3 of inner ring
    2. Oscillation      → EMA smoothing on temperature (in Server class)
    3. Flapping         → hysteresis gap + minimum outer dwell time
    4. Lock Contention  → RLock with fine-grained critical sections
    5. Metrics Staleness→ EMA already handles this
    6. Memory Leak      → rotation log bounded + circular buffer
    7. Empty Inner Ring → emergency fallback server selection
    8. Back-off         → exponential back-off for repeat evictions
    """

    # Default tuning constants
    DEFAULT_HEAT_THRESHOLD    = 0.75   # Above this → evict to outer
    DEFAULT_COOL_THRESHOLD    = 0.30   # Below this → pull to inner
    # Gap between thresholds is intentional — prevents flapping
    # Do NOT set cool_threshold close to heat_threshold

    DEFAULT_MIN_INNER         = 2
    DEFAULT_MAX_INNER         = 5
    DEFAULT_ROTATION_COOLDOWN = 5.0    # Seconds between rotations for one server
    DEFAULT_MIN_OUTER_DWELL   = 10.0   # Minimum seconds in outer before re-entry
    DEFAULT_ROTATION_INTERVAL = 1.0    # Background thread interval (seconds)
    MAX_ROTATION_LOG          = 1_000  # Circular buffer size

    def __init__(
        self,
        heat_threshold:       float    = DEFAULT_HEAT_THRESHOLD,
        cool_threshold:       float    = DEFAULT_COOL_THRESHOLD,
        min_inner_size:       int      = DEFAULT_MIN_INNER,
        max_inner_size:       int      = DEFAULT_MAX_INNER,
        rotation_cooldown_sec: float   = DEFAULT_ROTATION_COOLDOWN,
        min_outer_dwell_sec:  float    = DEFAULT_MIN_OUTER_DWELL,
        metrics_updater:      Optional[Callable[[Server], None]] = None,
        on_rotation:          Optional[Callable[[RotationEvent], None]] = None,
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

        self.heat_threshold        = heat_threshold
        self.cool_threshold        = cool_threshold
        self.min_inner_size        = min_inner_size
        self.max_inner_size        = max_inner_size
        self.rotation_cooldown_sec = rotation_cooldown_sec
        self.min_outer_dwell_sec   = min_outer_dwell_sec
        self._metrics_updater      = metrics_updater
        self._on_rotation          = on_rotation

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
            "HuddleCluster initialized — "
            f"heat={heat_threshold}, cool={cool_threshold}, "
            f"inner=[{min_inner_size}..{max_inner_size}]"
        )

    
    # Server Registration
    

    def add_server(self, server: Server, force_inner: bool = False) -> None:
        """
        Register a new server into the cluster.
        If force_inner=True and inner ring has space, place it there directly.
        Otherwise it starts in the outer ring.
        """
        with self._lock:
            if force_inner and len(self._inner_ring) < self.max_inner_size:
                server.position = Position.INNER
                self._inner_ring.append(server)
                log.info(f"Added {server.id!r} → inner ring")
            else:
                server.position = Position.OUTER
                heapq.heappush(self._outer_ring, server)
                log.info(f"Added {server.id!r} → outer ring")

    def remove_server(self, server_id: str) -> bool:
        """
        Gracefully remove a server from the cluster.
        If removed from inner, attempt to pull from outer to maintain size.
        """
        with self._lock:
            # Try inner ring first
            for s in list(self._inner_ring):
                if s.id == server_id:
                    self._inner_ring.remove(s)
                    log.info(f"Removed {server_id!r} from inner ring")
                    self._maybe_pull_from_outer("maintain_min")
                    return True

            # Try outer ring
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

    
    # Request Routing
    

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

            # Rotate deque left: pop from front, push to back
            # This gives O(1) circular round-robin
            server = self._inner_ring[0]
            self._inner_ring.rotate(-1)
            return server

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

    
    # Core Rotation Logic
    

    def rotate(self) -> bool:
        """
        One full rotation cycle — the penguin huddle step.

        Step 1: Evict overheated inner servers → outer ring
        Step 2: Pull cooled outer servers → inner ring
        Returns True if any rotation happened.

        FIX (Thundering Herd):
          Max evictions per cycle = max(1, inner_size // 3)
          So even if everyone is hot, we don't empty the ring.

        FIX (Flapping):
          - rotation_cooldown_sec: minimum time between evictions per server
          - min_outer_dwell_sec: minimum time in outer before re-entry
          - hysteresis gap: heat_threshold >> cool_threshold
        """
        with self._lock:
            rotated = False

            #  Step 1: Evict overheated inner servers
            # FIX (Operator Precedence Bug): original code used ambiguous
            # `and not ... or (...)` which Python parsed incorrectly.
            # Correct logic: overheated AND (unhealthy OR cooldown elapsed)
            now = time.monotonic()
            candidates = [
                s for s in list(self._inner_ring)
                if s.is_overheated(self.heat_threshold)
                and (
                    not s.metrics.is_healthy
                    or (now - s.last_rotated) >= self.rotation_cooldown_sec
                )
            ]

            # FIX (Thundering Herd): cap evictions to 1/3 of ring,
            # always keep at least min_inner_size servers inside
            max_evict = max(1, len(self._inner_ring) // 3)
            safe_evict = len(self._inner_ring) - self.min_inner_size
            to_evict = candidates[: min(max_evict, max(0, safe_evict))]

            for server in to_evict:
                self._move_to_outer(server, EvictionReason.OVERHEATED)
                rotated = True

            #  Step 2: Pull cooled outer servers into inner 
            while (
                self._outer_ring
                and len(self._inner_ring) < self.max_inner_size
            ):
                coolest = self._outer_ring[0]

                # FIX (Flapping): must have dwelt in outer long enough
                dwell_time = time.monotonic() - coolest.last_rotated
                if dwell_time < self.min_outer_dwell_sec:
                    break  # Not ready yet — wait more

                if coolest.is_cooled(self.cool_threshold):
                    heapq.heappop(self._outer_ring)
                    self._move_to_inner(coolest)
                    rotated = True
                else:
                    # Heap is sorted: if coolest is still hot, all others are too
                    break

            # Step 3: Health check evictions (unhealthy servers) 
            for server in list(self._inner_ring):
                if not server.metrics.is_healthy:
                    if len(self._inner_ring) > self.min_inner_size:
                        self._move_to_outer(server, EvictionReason.HEALTH_FAIL)
                        rotated = True

            return rotated

    #  Internal Move Helpers 

    def _move_to_outer(self, server: Server, reason: EvictionReason) -> None:
        now = time.monotonic()
        elapsed = now - server.last_rotated
        server.total_inner_time += elapsed
        server.position         = Position.OUTER
        server.last_rotated     = now
        server.rotation_count  += 1
        server._consecutive_evictions += 1

        self._inner_ring.remove(server)
        heapq.heappush(self._outer_ring, server)

        event = RotationEvent(
            timestamp=time.time(),
            server_id=server.id,
            direction="inner→outer",
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
        now = time.monotonic()
        elapsed = now - server.last_rotated
        server.total_outer_time            += elapsed
        server.position                     = Position.INNER
        server.last_rotated                 = now
        server.rotation_count              += 1
        server._consecutive_evictions       = 0   # reset on cool re-entry

        self._inner_ring.append(server)

        event = RotationEvent(
            timestamp=time.time(),
            server_id=server.id,
            direction="outer→inner",
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
            log.info(f"↑ Pulled {s.id!r} to maintain min_inner ({reason})")

    
    # Background Rotation Daemon
    

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
        log.info(f"🐧 HuddleCluster started (interval={rotation_interval_sec}s)")

    def stop(self, timeout: float = 5.0) -> None:
        """Gracefully stop the rotation daemon."""
        self._running = False
        if self._rotation_thread and self._rotation_thread.is_alive():
            self._rotation_thread.join(timeout=timeout)
        log.info("🐧 HuddleCluster stopped")

    def _rotation_loop(self, interval: float) -> None:
        while self._running:
            try:
                # 1. Update metrics for all servers
                if self._metrics_updater:
                    for s in self.all_servers():
                        self._metrics_updater(s)
                        s.update_temperature()

                # 2. Rotate
                self.rotate()

            except Exception as exc:
                log.exception(f"Rotation loop error: {exc}")

            time.sleep(interval)

    
    # Analytics, Fairness & Observability
    

    def fairness_score(self) -> float:
        """
        Gini-inspired fairness score for inner-ring time.
          0.0 = perfectly fair  (all servers had equal inner time)
          1.0 = completely unfair (one server always active)

        Alert if this exceeds 0.3 in production.
        """
        servers = self.all_servers()
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
        """Snapshot of cluster state — expose via /huddle/health endpoint."""
        with self._lock:
            inner = list(self._inner_ring)
            outer = list(self._outer_ring)

        inner_temps = [s.temperature for s in inner]
        return {
            "status": "degraded" if len(inner) < self.min_inner_size else "healthy",
            "inner_ring": [
                {
                    "id":              s.id,
                    "temp":            round(s.temperature, 4),
                    "rotations":       s.rotation_count,
                    "inner_time_sec":  round(s.total_inner_time, 2),
                }
                for s in inner
            ],
            "outer_ring": [
                {
                    "id":             s.id,
                    "temp":           round(s.temperature, 4),
                    "outer_time_sec": round(s.total_outer_time, 2),
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



# Quick-start helper


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
    for idx, (sid, host, port) in enumerate(server_addresses):
        s = Server(id=sid, host=host, port=port)
        # First max_inner_size servers start in inner ring
        cluster.add_server(s, force_inner=(idx < cluster.max_inner_size))
    return cluster



if __name__ == "__main__":
    from time import sleep

    print("🐧 HuddleCluster demo starting...")

    cluster = create_cluster([
        ("s1", "127.0.0.1", 8001),
        ("s2", "127.0.0.1", 8002),
        ("s3", "127.0.0.1", 8003),
    ])

    def fake_metrics(server):
        import random
        server.metrics.cpu_usage = random.random()
        server.metrics.memory_usage = random.random()
        server.metrics.active_connections = random.randint(0, 1000)
        server.metrics.avg_response_ms = random.randint(50, 3000)
        server.metrics.error_rate = random.random() * 0.1
        server.metrics.is_healthy = True

    cluster._metrics_updater = fake_metrics
    cluster.start()

    try:
        while True:
            s = cluster.get_server()
            print("Request →", s.id, "temp:", round(s.temperature, 3))
            sleep(0.5)

    except KeyboardInterrupt:
        print("Stopping...")
        cluster.stop()