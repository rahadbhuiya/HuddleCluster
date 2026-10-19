"""
HuddleCluster — WAN Latency & Network Jitter Simulator
======================================================
v4.26.0

Provides high-fidelity wide-area network (WAN) simulation for cross-region
consensus and cluster orchestration. Simulates realistic inter-datacenter
transmissions with configurable latency profiles, statistical jitter
(Gaussian or uniform), stochastic packet drop, bandwidth choking, and
data corruption.

Used to test and harden Raft leader election (ClusterHA), multi-region
gossip (MultiRegionManager), and dynamic failover under degraded network
conditions (NLnet Milestone 2).

Author : Rahad Bhuiya
Version: 4.26.0
License: MIT
"""

from __future__ import annotations

import dataclasses
import json
import logging
import math
import random
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar

logger = logging.getLogger("HuddleCluster.WanSimulator")

T = TypeVar("T")



# Exceptions


class WanSimulationError(Exception):
    """Base error for WAN simulation failures."""
    pass


class WanPacketLossException(WanSimulationError, IOError):
    """Raised when simulated packet drop occurs."""
    pass


class WanConnectionTimeoutException(WanSimulationError, TimeoutError):
    """Raised when simulated transmission exceeds deadline."""
    pass


class WanDataCorruptedException(WanSimulationError, ValueError):
    """Raised when simulated data corruption occurs."""
    pass



# Profiles & Presets


@dataclass
class LatencyProfile:
    """
    Specification for a network link between regions.

    Attributes:
        mean_latency_ms:   Average one-way latency in milliseconds.
        jitter_ms:         Standard deviation (or half-range) of jitter in ms.
        packet_loss_rate:  Fraction of packets dropped (0.0 = 0%, 1.0 = 100%).
        bandwidth_mbps:    Optional bandwidth cap in Megabits per second.
        corruption_rate:   Fraction of packets corrupted (0.0 to 1.0).
        distribution:      Jitter distribution ('gaussian' or 'uniform').
    """
    mean_latency_ms:  float = 50.0
    jitter_ms:        float = 10.0
    packet_loss_rate: float = 0.0
    bandwidth_mbps:   Optional[float] = None
    corruption_rate:  float = 0.0
    distribution:     str = "gaussian"

    def __post_init__(self) -> None:
        if self.mean_latency_ms < 0:
            raise ValueError("mean_latency_ms cannot be negative")
        if self.jitter_ms < 0:
            raise ValueError("jitter_ms cannot be negative")
        if not (0.0 <= self.packet_loss_rate <= 1.0):
            raise ValueError("packet_loss_rate must be between 0.0 and 1.0")
        if not (0.0 <= self.corruption_rate <= 1.0):
            raise ValueError("corruption_rate must be between 0.0 and 1.0")
        if self.bandwidth_mbps is not None and self.bandwidth_mbps <= 0:
            raise ValueError("bandwidth_mbps must be positive")
        if self.distribution not in ("gaussian", "uniform"):
            raise ValueError("distribution must be 'gaussian' or 'uniform'")


# Built-in industry presets
PRESET_LOCAL_LAN = LatencyProfile(
    mean_latency_ms=1.5,
    jitter_ms=0.5,
    packet_loss_rate=0.0,
    distribution="uniform",
)

PRESET_INTRA_REGION = LatencyProfile(
    mean_latency_ms=8.0,
    jitter_ms=2.0,
    packet_loss_rate=0.001,
    distribution="gaussian",
)

PRESET_TRANSATLANTIC = LatencyProfile(
    # e.g., US-East to EU-West
    mean_latency_ms=85.0,
    jitter_ms=12.0,
    packet_loss_rate=0.005,
    distribution="gaussian",
)

PRESET_TRANSPACIFIC = LatencyProfile(
    # e.g., US-West to AP-Southeast (Tokyo/Singapore)
    mean_latency_ms=155.0,
    jitter_ms=22.0,
    packet_loss_rate=0.012,
    distribution="gaussian",
)

PRESET_CROSS_CONTINENT = LatencyProfile(
    # e.g., EU-Central to AP-South (Frankfurt to Mumbai/Sydney)
    mean_latency_ms=220.0,
    jitter_ms=35.0,
    packet_loss_rate=0.020,
    distribution="gaussian",
)

PRESET_DEGRADED_WAN = LatencyProfile(
    mean_latency_ms=320.0,
    jitter_ms=80.0,
    packet_loss_rate=0.080,
    distribution="gaussian",
)

PRESET_HIGH_LOSS = LatencyProfile(
    mean_latency_ms=110.0,
    jitter_ms=25.0,
    packet_loss_rate=0.250,
    distribution="gaussian",
)



# Transmission Telemetry


@dataclass
class TransmissionReport:
    """Telemetry report for a single transmission test or simulated call."""
    from_region:   str
    to_region:     str
    success:       bool
    latency_ms:    float
    delay_sec:     float
    dropped:       bool = False
    corrupted:     bool = False
    bytes_sent:    int = 0
    timestamp:     float = field(default_factory=time.time)



# Simulator Engine


class WanLatencySimulator:
    """
    Multi-region network degradation and latency simulator.

    Maintains a matrix of region-pair latency profiles. Allows injecting
    artificial delays, jitter, and stochastic loss into arbitrary Python
    callables, HTTP client requests, or ClusterHA Raft peer communications.
    """

    def __init__(
        self,
        default_profile: Optional[LatencyProfile] = None,
        same_region_profile: Optional[LatencyProfile] = None,
        random_seed: Optional[int] = None,
    ) -> None:
        self._default_profile = default_profile or LatencyProfile(mean_latency_ms=45.0, jitter_ms=8.0)
        self._same_region_profile = same_region_profile or PRESET_LOCAL_LAN
        self._links: Dict[Tuple[str, str], LatencyProfile] = {}
        self._lock = threading.RLock()
        self._rng = random.Random(random_seed)

        # Telemetry metrics
        self._total_transmissions: int = 0
        self._total_dropped: int = 0
        self._total_corrupted: int = 0
        self._total_bytes: int = 0
        self._total_latency_ms: float = 0.0
        self._link_stats: Dict[Tuple[str, str], Dict[str, Any]] = {}

    
    # Topology Configuration
    

    def set_link(
        self,
        from_region: str,
        to_region: str,
        profile: LatencyProfile,
        symmetric: bool = True,
    ) -> None:
        """
        Configure network profile for traffic between two regions.

        Args:
            from_region: Origin region name (e.g. 'us-east').
            to_region:   Destination region name (e.g. 'eu-west').
            profile:     LatencyProfile specifying mean delay, jitter, loss.
            symmetric:   If True, also configures the reverse link.
        """
        src = from_region.strip().lower()
        dst = to_region.strip().lower()
        with self._lock:
            self._links[(src, dst)] = profile
            if symmetric and src != dst:
                self._links[(dst, src)] = profile
        logger.debug(
            "Configured WAN link [%s -> %s]: mean=%.1fms jitter=%.1fms loss=%.1f%%",
            src, dst, profile.mean_latency_ms, profile.jitter_ms, profile.packet_loss_rate * 100.0,
        )


    def get_link(self, from_region: str, to_region: str) -> LatencyProfile:
        """Get the configured profile for a region link."""
        src = from_region.strip().lower()
        dst = to_region.strip().lower()
        with self._lock:
            if src == dst:
                return self._same_region_profile
            return self._links.get((src, dst), self._default_profile)

    def list_links(self) -> Dict[str, Dict[str, Any]]:
        """Return dict of all explicitly configured links."""
        with self._lock:
            res = {}
            for (src, dst), prof in self._links.items():
                res[f"{src}->{dst}"] = dataclasses.asdict(prof)
            return res


    
    # Simulation Math & Execution
    

    def calculate_delay_ms(self, from_region: str, to_region: str, payload_bytes: int = 0) -> float:
        """
        Calculate one-way latency sample in milliseconds including jitter and
        optional bandwidth-induced transmission delay.
        """
        prof = self.get_link(from_region, to_region)

        # 1. Base latency + jitter
        if prof.distribution == "gaussian":
            jitter = self._rng.gauss(0.0, prof.jitter_ms)
        else:  # uniform
            jitter = self._rng.uniform(-prof.jitter_ms, prof.jitter_ms)

        latency = prof.mean_latency_ms + jitter

        # 2. Bandwidth serialisation delay: bytes / (mbps * 125,000 bytes/sec) * 1000 ms/sec
        if prof.bandwidth_mbps and payload_bytes > 0:
            bytes_per_ms = (prof.bandwidth_mbps * 1_000_000) / 8.0 / 1000.0
            serialization_delay_ms = payload_bytes / max(1.0, bytes_per_ms)
            latency += serialization_delay_ms

        # Physical floor: at least 0.1ms
        return max(0.1, round(latency, 3))

    def evaluate_loss(self, from_region: str, to_region: str) -> bool:
        """Return True if stochastic packet loss occurs on this link."""
        prof = self.get_link(from_region, to_region)
        if prof.packet_loss_rate <= 0.0:
            return False
        return self._rng.random() < prof.packet_loss_rate

    def evaluate_corruption(self, from_region: str, to_region: str) -> bool:
        """Return True if stochastic data corruption occurs."""
        prof = self.get_link(from_region, to_region)
        if prof.corruption_rate <= 0.0:
            return False
        return self._rng.random() < prof.corruption_rate


    def simulate_transmission(
        self,
        from_region: str,
        to_region: str,
        payload_bytes: int = 0,
    ) -> TransmissionReport:
        """
        Simulate a packet transmission and return the outcome report without sleeping.
        """
        src = from_region.strip().lower()
        dst = to_region.strip().lower()

        dropped = self.evaluate_loss(src, dst)
        corrupted = False if dropped else self.evaluate_corruption(src, dst)
        latency_ms = self.calculate_delay_ms(src, dst, payload_bytes)
        delay_sec = latency_ms / 1000.0

        success = not (dropped or corrupted)

        # Update telemetry
        with self._lock:
            self._total_transmissions += 1
            self._total_bytes += payload_bytes
            if dropped:
                self._total_dropped += 1
            elif corrupted:
                self._total_corrupted += 1
            else:
                self._total_latency_ms += latency_ms

            key = (src, dst)
            stat = self._link_stats.setdefault(key, {
                "sent": 0, "dropped": 0, "corrupted": 0, "total_ms": 0.0,
            })
            stat["sent"] += 1
            if dropped:
                stat["dropped"] += 1
            elif corrupted:
                stat["corrupted"] += 1
            else:
                stat["total_ms"] += latency_ms

        return TransmissionReport(
            from_region=src,
            to_region=dst,
            success=success,
            latency_ms=latency_ms,
            delay_sec=delay_sec,
            dropped=dropped,
            corrupted=corrupted,
            bytes_sent=payload_bytes,
        )
    

    def transmit(
        self,
        from_region: str,
        to_region: str,
        fn: Callable[[], T],
        payload_bytes: int = 0,
        apply_delay: bool = True,
        delay_multiplier: float = 1.0,
    ) -> T:
        """
        Execute callable ``fn`` subject to WAN simulation parameters.

        Args:
            from_region:      Origin region.
            to_region:        Destination region.
            fn:               Function to execute on successful transmission.
            payload_bytes:    Size in bytes for bandwidth delay calculation.
            apply_delay:      If True, invokes time.sleep() for the simulated latency.
            delay_multiplier: Multiplier on sleep duration (e.g. 0.01 for fast tests).

        Raises:
            WanPacketLossException:    If the packet is dropped.
            WanDataCorruptedException: If the payload is corrupted.
        """
        report = self.simulate_transmission(from_region, to_region, payload_bytes)

        if apply_delay and report.delay_sec > 0:
            sleep_time = report.delay_sec * delay_multiplier
            if sleep_time > 0:
                time.sleep(sleep_time)

        if report.dropped:
            raise WanPacketLossException(
                f"Simulated WAN packet drop between '{from_region}' and '{to_region}'"
            )

        if report.corrupted:
            raise WanDataCorruptedException(
                f"Simulated payload corruption between '{from_region}' and '{to_region}'"
            )

        return fn()

    
    # Raft / ClusterHA Integration
    


    def attach_to_ha(
        self,
        ha_instance: Any,
        node_region_map: Dict[str, str],
        delay_multiplier: float = 0.01,
    ) -> Callable[[], None]:
        """
        Monkey-patches a ClusterHA instance's `_rpc_post` method so all peer
        Raft RPCs (vote requests, append entries heartbeats, snapshots)
        experience simulated cross-region WAN latency and packet loss.

        Args:
            ha_instance:      ClusterHA object to instrument.
            node_region_map:  Mapping from node_id or peer URL to region name.
            delay_multiplier: Scaling factor for sleep time (default 0.01 for fast tests).

        Returns:
            A restore() zero-arg function that removes the hook and restores original RPC.
        """
        original_rpc_post = ha_instance._rpc_post
        local_id = getattr(ha_instance, "_id", "unknown")
        local_region = node_region_map.get(local_id, "default")

        def simulated_rpc_post(url: str, payload: Dict) -> Dict:
            # Determine peer region from URL or payload
            peer_region = "default"
            for k, reg in node_region_map.items():
                if k in url:
                    peer_region = reg
                    break

            payload_bytes = len(json.dumps(payload).encode("utf-8"))

            return self.transmit(
                from_region=local_region,
                to_region=peer_region,
                fn=lambda: original_rpc_post(url, payload),
                payload_bytes=payload_bytes,
                apply_delay=True,
                delay_multiplier=delay_multiplier,
            )

        ha_instance._rpc_post = simulated_rpc_post

        def restore() -> None:
            ha_instance._rpc_post = original_rpc_post

        return restore



    
    # Telemetry & Diagnostics
    

    def get_metrics(self) -> Dict[str, Any]:
        """Return cumulative transmission telemetry."""
        with self._lock:
            delivered = self._total_transmissions - self._total_dropped - self._total_corrupted
            avg_ms = (self._total_latency_ms / delivered) if delivered > 0 else 0.0
            drop_rate_pct = (
                (self._total_dropped / self._total_transmissions * 100.0)
                if self._total_transmissions > 0 else 0.0
            )

            links_summary = {}
            for (src, dst), stat in self._link_stats.items():
                deliv = stat["sent"] - stat["dropped"] - stat["corrupted"]
                links_summary[f"{src}->{dst}"] = {
                    "sent": stat["sent"],
                    "dropped": stat["dropped"],
                    "corrupted": stat["corrupted"],
                    "avg_latency_ms": round((stat["total_ms"] / deliv), 2) if deliv > 0 else 0.0,
                    "drop_rate_pct": round((stat["dropped"] / stat["sent"] * 100.0), 2) if stat["sent"] > 0 else 0.0,
                }

            return {
                "total_transmissions": self._total_transmissions,
                "total_delivered": delivered,
                "total_dropped": self._total_dropped,
                "total_corrupted": self._total_corrupted,
                "total_bytes": self._total_bytes,
                "drop_rate_pct": round(drop_rate_pct, 2),
                "avg_latency_ms": round(avg_ms, 2),
                "links": links_summary,
            }

    def reset_metrics(self) -> None:
        """Reset all transmission counters."""
        with self._lock:
            self._total_transmissions = 0
            self._total_dropped = 0
            self._total_corrupted = 0
            self._total_bytes = 0
            self._total_latency_ms = 0.0
            self._link_stats.clear()
