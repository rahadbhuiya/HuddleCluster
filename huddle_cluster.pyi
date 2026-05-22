"""
HuddleCluster type stubs — PEP 561
===================================
Provides full type information for IDE autocomplete (PyCharm, VS Code,
Pylance) and static analysis tools (mypy, pyright, pytype).

Install: pip install huddle-cluster  (py.typed marker auto-enables stubs)
"""

from __future__ import annotations

from contextlib import contextmanager
from enum import Enum
from typing import Callable, Generator, Iterator, Optional

#  Enums 

class Position(Enum):
    INNER: str
    OUTER: str

class EvictionReason(Enum):
    OVERHEATED:       str
    MANUAL:           str
    HEALTH_FAIL:      str
    ABSOLUTE_LATENCY: str

#  Data Classes 

class RotationEvent:
    timestamp:   float
    server_id:   str
    direction:   str
    reason:      str
    temperature: float

    def __init__(
        self,
        timestamp:   float,
        server_id:   str,
        direction:   str,
        reason:      str,
        temperature: float,
    ) -> None: ...

class ServerMetrics:
    cpu_usage:             float
    memory_usage:          float
    active_connections:    int
    avg_response_ms:       float
    error_rate:            float
    is_healthy:            bool
    latency_anomaly_score: float

    def record_latency(self, ms: float) -> None:
        """Push one latency sample into the rolling window and refresh avg_response_ms."""
        ...

    def update_latency_anomaly(self, cluster_median_ms: float) -> None:
        """
        Compute relative latency anomaly vs cluster median.

        anomaly = clamp((avg_ms / median_ms - 1) / 2, 0, 1)

        Examples (cluster_median = 12 ms):
            12 ms  -> 0.00  (normal)
            24 ms  -> 0.50  (2x slower)
            36 ms+ -> 1.00  (3x or more slower, clamped)
        """
        ...

    def p95_latency(self) -> float:
        """95th-percentile latency from the rolling window. Returns 0.0 if no data."""
        ...

#  Server 

class Server:
    """
    One server node in a HuddleCluster.

    Attributes:
        id:          Unique identifier string.
        host:        Hostname or IP address.
        port:        Port number.
        weight:      Capacity multiplier (default 1.0). A server with weight=2.0
                     requires 2x the base heat_threshold before eviction.
        tags:        Arbitrary key-value metadata (e.g. region, tier, az).
                     Appears in health_report() and prometheus_metrics() labels.
        metrics:     Live ServerMetrics snapshot.
        position:    Current ring position (INNER or OUTER).
        temperature: EMA-smoothed composite load score in [0, 1].

    Example:
        s = Server(
            id="api-1",
            host="10.0.0.1",
            port=8080,
            weight=2.0,
            tags={"region": "us-east", "tier": "primary"},
        )
    """

    id:                   str
    host:                 str
    port:                 int
    weight:               float
    tags:                 dict[str, str]
    metrics:              ServerMetrics
    position:             Position
    temperature:          float
    last_rotated:         float
    total_inner_time:     float
    total_outer_time:     float
    rotation_count:       int

    def __init__(
        self,
        id:     str,
        host:   str,
        port:   int,
        weight: float = 1.0,
        tags:   Optional[dict[str, str]] = None,
    ) -> None: ...

    def is_cold_start(self) -> bool:
        """True if the server is still in its cold-start protection period."""
        ...

    def effective_heat_threshold(self, base_threshold: float) -> float:
        """
        Eviction threshold adjusted for this server's weight.

        Returns min(1.0, base_threshold * self.weight).
        A server with weight=2.0 needs temperature >= base_threshold * 2.0.
        """
        ...

    def update_temperature(self) -> float:
        """Recompute EMA temperature from current metrics. Returns new temperature."""
        ...

    def is_overheated(self, threshold: float) -> bool:
        """True if temperature >= effective_heat_threshold(threshold)."""
        ...

    def is_cooled(self, cooldown_threshold: float) -> bool:
        """True if temperature <= cooldown_threshold."""
        ...

    def __lt__(self, other: Server) -> bool: ...
    def __eq__(self, other: object) -> bool: ...
    def __hash__(self) -> int: ...
    def __repr__(self) -> str: ...

#  Adaptive Threshold Controller 

class AdaptiveThresholdController:
    """
    Auto-adjusts heat/cool thresholds based on cluster P95 latency history.

    When recent P95 exceeds the window baseline by >1.5x, thresholds loosen
    (avoid over-eviction under sustained load). When P95 drops below 0.7x
    baseline, thresholds tighten (faster anomaly detection).

    Used internally by HuddleCluster when adaptive_thresholds=True.

    Example:
        ctrl = AdaptiveThresholdController(base_heat=0.55, base_cool=0.30)
        ctrl.record_p95(45.2)
        heat, cool = ctrl.maybe_adapt()
    """

    base_heat:           float
    base_cool:           float
    max_delta:           float
    window_size:         int
    adaptation_interval: float

    def __init__(
        self,
        base_heat:           float = 0.55,
        base_cool:           float = 0.30,
        max_delta:           float = 0.10,
        window_size:         int   = 20,
        adaptation_interval: float = 5.0,
    ) -> None: ...

    def record_p95(self, p95_ms: float) -> None:
        """Push a cluster P95 latency sample into the rolling window."""
        ...

    def maybe_adapt(self) -> tuple[float, float]:
        """
        Recompute thresholds if adaptation_interval has elapsed.

        Returns:
            (heat_threshold, cool_threshold) — current effective values.
        """
        ...

    @property
    def heat_threshold(self) -> float:
        """Current effective heat eviction threshold."""
        ...

    @property
    def cool_threshold(self) -> float:
        """Current effective cool promotion threshold."""
        ...

#  Gossip Agent 

class GossipAgent:
    """
    Lightweight UDP multicast gossip for distributed temperature sharing.

    Broadcasts inner-ring server temperatures to peer HuddleCluster instances.
    Peers receive data as advisory signals only — they do not modify local
    server objects. Best-effort delivery; the cluster remains fully functional
    without gossip.

    Example:
        agent   = GossipAgent(node_id="node-1", gossip_port=9999)
        cluster = create_cluster([...], gossip_agent=agent)
        cluster.start()

        # Inspect peer temperatures
        peers = agent.peer_states()
        # {"node-2": [{"id": "s0", "temp": 0.12, "avg_ms": 15.3}]}
    """

    node_id:            str
    gossip_port:        int
    broadcast_interval: float
    ttl:                int

    MULTICAST_GROUP:   str
    DEFAULT_PORT:      int
    MAX_MESSAGE_BYTES: int

    def __init__(
        self,
        node_id:            str,
        gossip_port:        int   = 9999,
        broadcast_interval: float = 2.0,
        ttl:                int   = 1,
    ) -> None: ...

    def start(self, cluster: HuddleCluster) -> None:
        """Start broadcast and receive daemon threads."""
        ...

    def stop(self) -> None:
        """Stop gossip threads."""
        ...

    def peer_states(self) -> dict[str, list[dict]]:
        """
        Return latest known server states from peer nodes.

        Returns:
            {node_id: [{"id": str, "temp": float, "avg_ms": float, "pos": str}]}
        """
        ...

#  HuddleCluster 

class HuddleCluster:
    """
    Penguin-inspired self-organizing server load balancer.

    Organises servers into an active inner ring (deque, round-robin) and a
    resting outer ring (min-heap by temperature). Servers rotate between rings
    based on a composite EMA temperature score dominated by relative latency
    anomaly.

    Quick start:
        cluster = create_cluster([
            ("s1", "10.0.0.1", 8080),
            ("s2", "10.0.0.2", 8080),
            ("s3", "10.0.0.3", 8080),
        ])
        cluster.start()

        with cluster.get_server_context() as server:
            resp = requests.get(f"http://{server.host}:{server.port}/api")

        cluster.stop()

    Args:
        heat_threshold:            Evict inner server above this temperature (default 0.55).
        cool_threshold:            Promote outer server below this temperature (default 0.30).
        min_inner_size:            Minimum active servers in inner ring (default 2).
        max_inner_size:            Maximum active servers in inner ring (default 5).
        rotation_cooldown_sec:     Minimum seconds between evictions per server (default 5.0).
        min_outer_dwell_sec:       Minimum rest time before re-entry (default 10.0).
        ema_alpha:                 EMA smoothing factor in (0, 1] (default 0.60).
        absolute_latency_floor_ms: Evict any server exceeding this absolute latency (default None).
        cold_start_sec:            New servers warm up in outer ring for this long (default 0.0).
        adaptive_thresholds:       Auto-adjust thresholds from cluster P95 history (default False).
        gossip_agent:              GossipAgent for distributed deployments (default None).
        request_timeout_ms:        Dead-server timeout threshold in ms (default 500.0).
        circuit_breaker_threshold: Evict server when error_rate >= this value (default 0.5).
        metrics_updater:           fn(Server) called each rotation cycle to refresh metrics.
        on_rotation:               fn(RotationEvent) called on every ring rotation.
        on_eviction:               fn(Server, EvictionReason) called on every eviction.
    """

    heat_threshold:            float
    cool_threshold:            float
    min_inner_size:            int
    max_inner_size:            int
    rotation_cooldown_sec:     float
    min_outer_dwell_sec:       float
    ema_alpha:                 float
    absolute_latency_floor_ms: Optional[float]
    cold_start_sec:            float
    request_timeout_ms:        float
    circuit_breaker_threshold: float

    def __init__(
        self,
        heat_threshold:            float    = 0.55,
        cool_threshold:            float    = 0.30,
        min_inner_size:            int      = 2,
        max_inner_size:            int      = 5,
        rotation_cooldown_sec:     float    = 5.0,
        min_outer_dwell_sec:       float    = 10.0,
        ema_alpha:                 float    = 0.60,
        absolute_latency_floor_ms: Optional[float] = None,
        cold_start_sec:            float    = 0.0,
        adaptive_thresholds:       bool     = False,
        gossip_agent:              Optional[GossipAgent] = None,
        request_timeout_ms:        float    = 500.0,
        circuit_breaker_threshold: float    = 0.5,
        metrics_updater:           Optional[Callable[[Server], None]] = None,
        on_rotation:               Optional[Callable[[RotationEvent], None]] = None,
        on_eviction:               Optional[Callable[[Server, EvictionReason], None]] = None,
    ) -> None: ...

    #  Server Registration 

    def add_server(self, server: Server, force_inner: bool = False) -> None:
        """
        Register a server in the cluster.

        If cold_start_sec > 0, the server always starts in the outer ring
        regardless of force_inner.

        Args:
            server:      Server instance to register.
            force_inner: If True and inner ring has space, place in inner ring directly.
        """
        ...

    def remove_server(self, server_id: str) -> bool:
        """
        Gracefully remove a server. Returns True if found and removed.

        If removed from inner ring, attempts to pull from outer to maintain
        min_inner_size.
        """
        ...

    def force_evict(self, server_id: str) -> bool:
        """
        Manually evict a server from inner to outer ring.

        Returns True if the server was found in the inner ring.
        """
        ...

    #  Request Routing 

    def get_server(self) -> Optional[Server]:
        """
        Get the next server via circular round-robin from the inner ring.

        Falls back to the globally coolest server if the inner ring is empty.
        Returns None only if the cluster has no servers at all.
        """
        ...

    def record_latency(self, server: Server, latency_ms: float) -> None:
        """
        Record an observed round-trip latency for a server.

        Updates the server's rolling latency window, recomputes the
        cluster-wide median baseline, updates the anomaly score, and
        refreshes the EMA temperature. Also feeds the adaptive threshold
        controller and Prometheus P95 window.

        Call this after every request for accurate anomaly detection.

        Example:
            server = cluster.get_server()
            t0 = time.perf_counter()
            response = requests.get(f"http://{server.host}:{server.port}/api")
            cluster.record_latency(server, (time.perf_counter() - t0) * 1000)
        """
        ...

    def batch_record_latency(self, measurements: list[tuple[Server, float]]) -> None:
        """
        Feed multiple latency samples in one call.

        More efficient than calling record_latency() in a loop because the
        cluster median is computed only once for all measurements.

        Args:
            measurements: List of (server, latency_ms) tuples.

        Example:
            cluster.batch_record_latency([
                (s1, 15.2),
                (s2, 18.7),
                (s3, 220.0),
            ])
        """
        ...

    @contextmanager
    def get_server_context(self) -> Generator[Optional[Server], None, None]:
        """
        Context manager: route a request with automatic latency recording.

        Picks a server, times the block, and calls record_latency() on exit.
        On exception, also increments the server's error_rate and marks it
        unhealthy if error_rate exceeds 0.5.

        Example:
            with cluster.get_server_context() as server:
                if server:
                    resp = requests.get(f"http://{server.host}:{server.port}/api")
        """
        ...

    #  Lifecycle 

    def start(self, rotation_interval_sec: float = 1.0) -> None:
        """
        Start the background rotation daemon thread.

        Args:
            rotation_interval_sec: How often to run the rotation cycle (default 1.0s).
                                   Use 0.3s for faster anomaly detection in tests.
        """
        ...

    def stop(self, timeout: float = 5.0, drain_timeout_sec: float = 0.0) -> None:
        """
        Stop the rotation daemon.

        Args:
            timeout:          Max seconds to wait for daemon thread to exit.
            drain_timeout_sec: If > 0, wait for active_connections to reach 0
                              before stopping (graceful drain).
        """
        ...

    def rotate(self) -> bool:
        """
        Run one full rotation cycle manually.

        Normally called automatically by the background daemon. Useful for
        testing or for custom rotation scheduling.

        Returns True if any server changed rings.
        """
        ...

    #  Analytics & Observability 

    def health_report(self) -> dict:
        """
        Return a JSON-serializable snapshot of cluster state.

        Suitable for direct exposure as a /huddle/health HTTP endpoint.

        Returns dict with keys:
            status:           "healthy" or "degraded"
            heat_threshold:   current effective eviction threshold
            cool_threshold:   current effective promotion threshold
            inner_ring:       list of per-server dicts (id, weight, tags, temp,
                              rotations, inner_time_sec, avg_latency_ms,
                              p95_latency_ms, anomaly_score)
            outer_ring:       list of per-server dicts (id, weight, tags, temp,
                              outer_time_sec, avg_latency_ms, cold_start)
            inner_count:      number of active inner-ring servers
            outer_count:      number of resting outer-ring servers
            total_servers:    inner_count + outer_count
            avg_inner_temp:   mean temperature across inner ring
            max_inner_temp:   max temperature across inner ring
            fairness_score:   Gini coefficient of inner-ring dwell times (0=fair)
            total_rotations:  cumulative rotation count across all servers
            requests_per_sec: recent routing throughput (rolling 60s window)
            requests_total:   approximate total requests in window
            recent_rotations: last 10 rotation events
            gossip_peers:     list of known peer node IDs (if gossip enabled)
        """
        ...

    def prometheus_metrics(self) -> str:
        """
        Return cluster metrics in Prometheus text exposition format.

        Expose via a /metrics HTTP endpoint:

            from fastapi import FastAPI
            from fastapi.responses import PlainTextResponse

            app = FastAPI()

            @app.get("/metrics", response_class=PlainTextResponse)
            def metrics():
                return cluster.prometheus_metrics()

        Metrics exposed:
            huddle_server_temperature{server, ring, ...tags}
            huddle_server_avg_latency_ms{server}
            huddle_server_anomaly_score{server}
            huddle_server_rotations_total{server}
            huddle_cluster_inner_count
            huddle_cluster_fairness_gini
            huddle_cluster_heat_threshold
            huddle_cluster_p95_latency_ms
            huddle_cluster_requests_per_second
            huddle_gossip_peer_count  (if gossip enabled)
        """
        ...

    def fairness_score(self) -> float:
        """
        Gini coefficient of inner-ring server dwell times.

        Measures how evenly active servers share duty time.
        0.00 = perfectly fair, 1.00 = completely unfair.
        Alert if this exceeds 0.30 in production.
        """
        ...

    def all_servers(self) -> list[Server]:
        """Return all servers (inner + outer)."""
        ...

    def inner_servers(self) -> list[Server]:
        """Return active inner-ring servers."""
        ...

    def outer_servers(self) -> list[Server]:
        """Return resting outer-ring servers."""
        ...

    def __repr__(self) -> str: ...

#  Factory 

def create_cluster(
    server_addresses: list[
        tuple[str, str, int]                              # (id, host, port)
        | tuple[str, str, int, float]                     # (id, host, port, weight)
        | tuple[str, str, int, float, dict[str, str]]     # (id, host, port, weight, tags)
    ],
    **kwargs,
) -> HuddleCluster:
    """
    Convenience factory to create and populate a HuddleCluster.

    Each address tuple can be:
        (id, host, port)                      -- weight=1.0, tags={}
        (id, host, port, weight)              -- custom weight
        (id, host, port, weight, tags)        -- custom weight and tags

    All kwargs are forwarded to HuddleCluster.__init__().

    Example:
        cluster = create_cluster(
            [
                ("s1", "10.0.0.1", 8080),
                ("s2", "10.0.0.2", 8080, 2.0),
                ("s3", "10.0.0.3", 8080, 1.0, {"region": "us-east"}),
            ],
            adaptive_thresholds=True,
            cold_start_sec=30.0,
            on_eviction=lambda s, r: print(f"{s.id} evicted: {r.value}"),
        )
        cluster.start()
    """
    ...

#  Version 

__version__: str
__author__:  str
__license__: str
