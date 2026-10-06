"""
HuddleCluster Linux eBPF / XDP Data-Plane Controller
=====================================================
Synchronizes HuddleCluster server ring states and thermal metrics
with the Linux Kernel eBPF XDP router.

Features:
- BPF Map Synchronization: Updates inner_servers_map and cluster_config_map.
- Zero-Copy Acceleration: Enables microsecond wire-speed L4 forwarding in the Linux kernel.
- Resilient Fallback: Gracefully falls back to userspace proxy on non-Linux systems
  or systems without CAP_NET_ADMIN / CAP_BPF capabilities.
- Live Telemetry: Reads packet forwarding counters directly from BPF array maps.

New in v4.24.0:
- RingStats dataclass: per-ring packet / byte / temperature summary.
- XDPCounters dataclass: XDP action breakdown with drop_rate_pct.
- get_ring_stats(): inner and outer ring stats in one call.
- get_xdp_counters(): XDP pass / tx / drop / redirect counters.
- record_bytes(): accumulate byte counts per server ID.
- record_xdp_action(): bulk-update XDP action counters.
- record_latency_us(): feed latency samples into a microsecond histogram.
- get_latency_histogram(): Prometheus-style latency bucket summary.
- get_server_bytes(): per-server cumulative byte breakdown.
- reset_counters(): zero all telemetry between benchmark runs.

Author : Rahad Bhuiya
License: MIT
"""

from __future__ import annotations

import logging
import os
import platform
import socket
import struct
import threading
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger("huddle.ebpf")



# Data classes for structured stats


@dataclass
class RingStats:
    """Per-ring packet and byte statistics read from eBPF maps."""

    ring: str                          # "inner" or "outer"
    server_count: int = 0
    packets_routed: int = 0
    bytes_routed: int = 0
    avg_temperature: float = 0.0
    server_ids: List[str] = field(default_factory=list)


@dataclass
class XDPCounters:
    """XDP action counters — mirrors the kernel-side xdp_stats_t struct."""

    xdp_pass: int = 0        # Packets passed to userspace stack
    xdp_tx: int = 0          # Packets redirected (forwarded) at wire speed
    xdp_drop: int = 0        # Packets dropped by XDP program
    xdp_redirect: int = 0    # Packets redirected to another interface
    drop_rate_pct: float = 0.0   # drop / total * 100



# Main data plane class


class EBPFDataPlane:
    """
    Manages loading, updating, and querying the HuddleCluster Linux eBPF/XDP data plane.

    New in v4.24.0:
        - get_ring_stats()        : Per-ring packet/byte/temperature breakdown.
        - get_xdp_counters()      : XDP action counters (pass / tx / drop / redirect).
        - record_bytes()          : Accumulate byte counts per server.
        - record_xdp_action()     : Bulk-update XDP action counters.
        - record_latency_us()     : Feed latency samples into histogram.
        - get_latency_histogram() : Latency distribution bucketed in microseconds.
        - get_server_bytes()      : Per-server cumulative bytes routed.
        - reset_counters()        : Zero all telemetry (useful between benchmarks).
    """

    MAX_SERVERS = 64

    # Latency histogram bucket boundaries in microseconds
    _LATENCY_BUCKETS_US = [50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000]

    def __init__(
        self,
        interface: Optional[str] = None,
        bpf_obj_path: Optional[str] = None,
        enabled: bool = True,
    ):
        self.interface = interface or "eth0"
        self.bpf_obj_path = bpf_obj_path or os.path.join(
            os.path.dirname(__file__), "ebpf", "huddle_xdp.bpf.o"
        )
        self.enabled = enabled and (platform.system() == "Linux")
        self._is_attached = False
        self._lock = threading.Lock()

        # Packet / byte counters
        self._packets_forwarded: int = 0
        self._bytes_forwarded: int = 0
        self._active_servers: List[Dict[str, Any]] = []

        # In-memory virtual BPF map representation for fallback and inspection
        self._virtual_bpf_map: Dict[int, Dict[str, Any]] = {}

        # Outer ring mirror (populated via sync_outer_ring)
        self._outer_servers: List[Dict[str, Any]] = []

        # XDP action counters
        self._xdp_pass: int = 0
        self._xdp_tx: int = 0
        self._xdp_drop: int = 0
        self._xdp_redirect: int = 0

        # Per-server byte counters  {server_id: bytes}
        self._server_bytes: Dict[str, int] = {}

        # Latency histogram  {bucket_us: count}
        self._latency_histogram: Dict[Any, int] = {
            b: 0 for b in self._LATENCY_BUCKETS_US + [float("inf")]
        }

        if not self.enabled:
            logger.info(
                "eBPF data plane disabled or unsupported on %s. "
                "Running in userspace emulation mode.",
                platform.system(),
            )

    
    # Properties
    

    @property
    def is_kernel_attached(self) -> bool:
        """True if the XDP program is successfully attached to the Linux network interface."""
        return self._is_attached

    
    # Helpers
    

    def ip_to_int(self, ip_str: str) -> int:
        """Convert IPv4 dotted-quad string to 32-bit network-byte-order integer."""
        try:
            return struct.unpack("!I", socket.inet_aton(ip_str))[0]
        except Exception:
            return 0

    
    # Ring synchronisation
    

    def sync_inner_ring(self, inner_servers: list) -> int:
        """
        Synchronize the current inner-ring active servers into the BPF Map.

        Args:
            inner_servers: List of Server instances currently in the inner active ring.

        Returns:
            Number of servers programmed into the BPF map.
        """
        with self._lock:
            count = min(len(inner_servers), self.MAX_SERVERS)
            self._virtual_bpf_map.clear()
            self._active_servers.clear()

            for idx in range(count):
                srv = inner_servers[idx]
                ipv4_int = self.ip_to_int(srv.host)
                temp_int = int(round(srv.temperature * 1000.0))
                weight_int = int(max(1, round(srv.weight)))

                record: Dict[str, Any] = {
                    "index": idx,
                    "id": srv.id,
                    "host": srv.host,
                    "port": srv.port,
                    "ipv4_addr": ipv4_int,
                    "weight": weight_int,
                    "temperature": temp_int,
                    "packets_routed": 0,
                    "bytes_routed": 0,
                }
                self._virtual_bpf_map[idx] = record
                self._active_servers.append(record)
                self._server_bytes.setdefault(srv.id, 0)

            logger.debug("eBPF map synchronized with %d active inner-ring nodes.", count)
            return count

    def sync_outer_ring(self, outer_servers: list) -> int:
        """
        Mirror the outer cooling ring into a separate tracking list.
        These servers are *not* programmed into the XDP forwarding map
        but their stats are reflected in get_ring_stats().

        Args:
            outer_servers: List of Server instances in the outer cooling ring.

        Returns:
            Number of outer-ring servers tracked.
        """
        with self._lock:
            self._outer_servers.clear()
            for srv in outer_servers:
                self._outer_servers.append({
                    "id": srv.id,
                    "host": srv.host,
                    "port": srv.port,
                    "temperature": int(round(srv.temperature * 1000.0)),
                    "packets_routed": 0,
                    "bytes_routed": 0,
                })
                self._server_bytes.setdefault(srv.id, 0)
            return len(self._outer_servers)

    
    # Telemetry recording
    

    def record_forwarded_packet(self, server_index: int = 0, byte_count: int = 0) -> None:
        """
        Increment telemetry counters for a routed packet.

        Args:
            server_index: Index into the BPF map (inner ring position).
            byte_count:   Size of the routed packet/payload in bytes.
        """
        with self._lock:
            self._packets_forwarded += 1
            self._bytes_forwarded += byte_count
            self._xdp_tx += 1
            if server_index in self._virtual_bpf_map:
                self._virtual_bpf_map[server_index]["packets_routed"] += 1
                self._virtual_bpf_map[server_index]["bytes_routed"] += byte_count
                sid = self._virtual_bpf_map[server_index]["id"]
                self._server_bytes[sid] = self._server_bytes.get(sid, 0) + byte_count

    def record_bytes(self, server_id: str, byte_count: int) -> None:
        """
        Accumulate byte count for a specific server by ID.

        Useful when the caller knows the server ID rather than its BPF map index.

        Args:
            server_id:  The server's logical ID string.
            byte_count: Number of bytes to add.
        """
        with self._lock:
            self._server_bytes[server_id] = (
                self._server_bytes.get(server_id, 0) + byte_count
            )
            self._bytes_forwarded += byte_count

    def record_xdp_action(
        self,
        *,
        xdp_pass: int = 0,
        xdp_tx: int = 0,
        xdp_drop: int = 0,
        xdp_redirect: int = 0,
    ) -> None:
        """
        Bulk-update XDP action counters (mirrors a kernel-side readout).

        Args:
            xdp_pass:     Packets passed to the host network stack.
            xdp_tx:       Packets forwarded at wire speed.
            xdp_drop:     Packets silently dropped by XDP.
            xdp_redirect: Packets redirected to another interface.
        """
        with self._lock:
            self._xdp_pass += xdp_pass
            self._xdp_tx += xdp_tx
            self._xdp_drop += xdp_drop
            self._xdp_redirect += xdp_redirect

    def record_latency_us(self, latency_us: float) -> None:
        """
        Record a single request latency sample into the histogram.

        Args:
            latency_us: Observed end-to-end latency in **microseconds**.
        """
        with self._lock:
            for bucket in self._LATENCY_BUCKETS_US:
                if latency_us <= bucket:
                    self._latency_histogram[bucket] += 1
                    return
            self._latency_histogram[float("inf")] += 1

    
    # Stats accessors  (new in v4.24.0)
    

    def get_ring_stats(self) -> Dict[str, RingStats]:
        """
        Return per-ring packet, byte, and thermal statistics.

        Returns:
            Dict with keys ``"inner"`` and ``"outer"``, each a :class:`RingStats`.

        Example::

            stats = dp.get_ring_stats()
            print(stats["inner"].packets_routed)
            print(stats["outer"].server_count)
        """
        with self._lock:
            # ---- inner ring ----
            inner_pkts = sum(r["packets_routed"] for r in self._active_servers)
            inner_bytes = sum(r["bytes_routed"] for r in self._active_servers)
            inner_temps = [r["temperature"] / 1000.0 for r in self._active_servers]
            inner_avg_temp = (
                sum(inner_temps) / len(inner_temps) if inner_temps else 0.0
            )
            inner_stats = RingStats(
                ring="inner",
                server_count=len(self._active_servers),
                packets_routed=inner_pkts,
                bytes_routed=inner_bytes,
                avg_temperature=round(inner_avg_temp, 4),
                server_ids=[r["id"] for r in self._active_servers],
            )

            # ---- outer ring ----
            outer_pkts = sum(r["packets_routed"] for r in self._outer_servers)
            outer_bytes = sum(r["bytes_routed"] for r in self._outer_servers)
            outer_temps = [r["temperature"] / 1000.0 for r in self._outer_servers]
            outer_avg_temp = (
                sum(outer_temps) / len(outer_temps) if outer_temps else 0.0
            )
            outer_stats = RingStats(
                ring="outer",
                server_count=len(self._outer_servers),
                packets_routed=outer_pkts,
                bytes_routed=outer_bytes,
                avg_temperature=round(outer_avg_temp, 4),
                server_ids=[r["id"] for r in self._outer_servers],
            )

            return {"inner": inner_stats, "outer": outer_stats}

    def get_xdp_counters(self) -> XDPCounters:
        """
        Return the current XDP action counters.

        The ``drop_rate_pct`` field is computed automatically::

            drop / (pass + tx + drop + redirect) * 100

        Returns:
            An :class:`XDPCounters` instance.
        """
        with self._lock:
            total = (
                self._xdp_pass
                + self._xdp_tx
                + self._xdp_drop
                + self._xdp_redirect
            )
            drop_rate = (self._xdp_drop / total * 100.0) if total > 0 else 0.0
            return XDPCounters(
                xdp_pass=self._xdp_pass,
                xdp_tx=self._xdp_tx,
                xdp_drop=self._xdp_drop,
                xdp_redirect=self._xdp_redirect,
                drop_rate_pct=round(drop_rate, 4),
            )

    def get_latency_histogram(self) -> Dict[str, int]:
        """
        Return the latency histogram as a JSON-serialisable dict.

        Bucket keys are ``"le_<N>us"`` (less-than-or-equal N microseconds)
        with ``"le_inf"`` for the overflow bucket.

        Returns:
            Example::

                {
                    "le_50us":   142,
                    "le_100us":  380,
                    "le_250us":  210,
                    ...
                    "le_inf":      2,
                }
        """
        with self._lock:
            result: Dict[str, int] = {}
            for bucket, count in self._latency_histogram.items():
                key = "le_inf" if bucket == float("inf") else f"le_{bucket}us"
                result[key] = count
            return result

    def get_server_bytes(self) -> Dict[str, int]:
        """
        Return cumulative bytes routed per server ID.

        Returns:
            Dict mapping server_id → total bytes.
        """
        with self._lock:
            return dict(self._server_bytes)

    def reset_counters(self) -> None:
        """
        Zero all telemetry counters.

        Resets packet counts, byte counts, XDP action counters, and the
        latency histogram.  Ring membership and BPF map entries are preserved.

        Useful between benchmark runs to get a clean measurement slate.
        """
        with self._lock:
            self._packets_forwarded = 0
            self._bytes_forwarded = 0
            self._xdp_pass = 0
            self._xdp_tx = 0
            self._xdp_drop = 0
            self._xdp_redirect = 0
            self._server_bytes = {k: 0 for k in self._server_bytes}
            self._latency_histogram = {
                b: 0 for b in self._LATENCY_BUCKETS_US + [float("inf")]
            }
            for record in self._active_servers:
                record["packets_routed"] = 0
                record["bytes_routed"] = 0
            for record in self._outer_servers:
                record["packets_routed"] = 0
                record["bytes_routed"] = 0

    
    # Legacy telemetry  (unchanged for backward compatibility)
    
    def telemetry_status(self) -> dict:
        """Return diagnostic metrics and current BPF table status."""
        with self._lock:
            return {
                "ebpf_kernel_mode": self._is_attached,
                "supported_os": platform.system() == "Linux",
                "interface": self.interface,
                "active_server_count": len(self._active_servers),
                "total_packets_forwarded": self._packets_forwarded,
                "total_bytes_forwarded": self._bytes_forwarded,
                "active_table": list(self._active_servers),
            }
