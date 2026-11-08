"""
HuddleCluster — Adaptive Geo-Routing & Latency-Aware Selection Engine
=====================================================================
v4.30.0

Provides intelligent multi-region request routing that balances:
  1. Geographic proximity (Haversine great-circle distance).
  2. Live measured round-trip latency (EMA smoothed).
  3. Upstream thermal & load penalty (from hardware/GPU sensors).
  4. Regional failure isolation and automatic fallback policies.

Routing Policies:
  - LOCAL_FIRST: Always route to local region unless all nodes fail or overheat.
  - CLOSEST_LATENCY: Route strictly to minimum smoothed RTT region.
  - BALANCED_GLOBAL: Multi-objective weighted score minimizing latency and thermal strain.

Author : Rahad Bhuiya
Version: 4.30.0
License: MIT
"""

from __future__ import annotations

import math
import time
import threading
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger("HuddleCluster.GeoRouting")



# Enums and Value Objects


class GeoPolicy(str, Enum):
    LOCAL_FIRST = "LOCAL_FIRST"
    CLOSEST_LATENCY = "CLOSEST_LATENCY"
    BALANCED_GLOBAL = "BALANCED_GLOBAL"


@dataclass
class GeoCoordinates:
    """Geographic latitude and longitude in decimal degrees."""
    latitude: float
    longitude: float

    def distance_to(self, other: GeoCoordinates) -> float:
        """
        Calculate great-circle distance in kilometers using Haversine formula.
        """
        r = 6371.0  # Earth's radius in km
        phi1 = math.radians(self.latitude)
        phi2 = math.radians(other.latitude)
        delta_phi = math.radians(other.latitude - self.latitude)
        delta_lambda = math.radians(other.longitude - self.longitude)

        a = (
            math.sin(delta_phi / 2.0) ** 2
            + math.cos(phi1) * math.cos(phi2) * (math.sin(delta_lambda / 2.0) ** 2)
        )
        c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
        return r * c


@dataclass
class UpstreamEndpoint:
    """Upstream endpoint metadata with regional and health telemetry."""
    endpoint_id: str
    region: str
    host: str
    port: int
    coordinates: Optional[GeoCoordinates] = None
    is_healthy: bool = True
    active_connections: int = 0
    thermal_score: float = 0.0  # Normalized 0.0 (cool) to 1.0 (overheated)
    smoothed_rtt_ms: float = 20.0
    total_requests: int = 0
    total_errors: int = 0
    last_updated: float = field(default_factory=time.time)


# Predefined region coordinate references for cloud regions
STANDARD_REGION_COORDINATES: Dict[str, GeoCoordinates] = {
    "us-east-1": GeoCoordinates(38.13, -78.45),      # North Virginia
    "us-west-1": GeoCoordinates(37.77, -122.42),     # N. California
    "eu-west-1": GeoCoordinates(53.33, -6.25),       # Dublin, Ireland
    "eu-central-1": GeoCoordinates(50.11, 8.68),     # Frankfurt, Germany
    "ap-southeast-1": GeoCoordinates(1.35, 103.82),  # Singapore
    "ap-northeast-1": GeoCoordinates(35.68, 139.77), # Tokyo, Japan
}



# Adaptive Geo Router Core


class GeoRouter:
    """
    Adaptive Geo-Routing & Latency-Aware Selection Engine.
    """

    def __init__(
        self,
        local_region: str = "us-east-1",
        policy: GeoPolicy = GeoPolicy.BALANCED_GLOBAL,
        ema_alpha: float = 0.25,
        latency_weight: float = 0.45,
        geo_weight: float = 0.25,
        thermal_weight: float = 0.30,
        thermal_threshold: float = 0.85,
    ) -> None:
        self.local_region = local_region
        self.policy = policy
        self.ema_alpha = max(0.01, min(1.0, ema_alpha))
        self.latency_weight = latency_weight
        self.geo_weight = geo_weight
        self.thermal_weight = thermal_weight
        self.thermal_threshold = thermal_threshold

        self._endpoints: Dict[str, UpstreamEndpoint] = {}
        self._region_coords: Dict[str, GeoCoordinates] = dict(STANDARD_REGION_COORDINATES)
        self._lock = threading.RLock()

    def set_region_coordinates(self, region: str, coords: GeoCoordinates) -> None:
        with self._lock:
            self._region_coords[region] = coords

    def get_region_coordinates(self, region: str) -> Optional[GeoCoordinates]:
        with self._lock:
            return self._region_coords.get(region)

    def register_endpoint(
        self,
        endpoint_id: str,
        region: str,
        host: str,
        port: int,
        coordinates: Optional[GeoCoordinates] = None,
        initial_rtt_ms: float = 20.0,
    ) -> UpstreamEndpoint:
        """Register or update an upstream destination."""
        with self._lock:
            coords = coordinates or self._region_coords.get(region)
            endpoint = UpstreamEndpoint(
                endpoint_id=endpoint_id,
                region=region,
                host=host,
                port=port,
                coordinates=coords,
                smoothed_rtt_ms=initial_rtt_ms,
            )
            self._endpoints[endpoint_id] = endpoint
            logger.info("Registered geo endpoint '%s' in region '%s'", endpoint_id, region)
            return endpoint

    def unregister_endpoint(self, endpoint_id: str) -> Optional[UpstreamEndpoint]:
        with self._lock:
            return self._endpoints.pop(endpoint_id, None)

    def record_rtt(self, endpoint_id: str, sample_rtt_ms: float, is_error: bool = False) -> None:
        """Update smoothed RTT using Exponential Moving Average (EMA)."""
        with self._lock:
            ep = self._endpoints.get(endpoint_id)
            if not ep:
                return

            ep.total_requests += 1
            if is_error:
                ep.total_errors += 1
                # Heavily penalize errors in RTT calculation
                sample_rtt_ms = max(sample_rtt_ms, ep.smoothed_rtt_ms * 2.0)

            ep.smoothed_rtt_ms = (
                self.ema_alpha * sample_rtt_ms + (1.0 - self.ema_alpha) * ep.smoothed_rtt_ms
            )
            ep.last_updated = time.time()

    def update_thermal_score(self, endpoint_id: str, thermal_score: float) -> None:
        """Update endpoint thermal strain score [0.0 to 1.0]."""
        with self._lock:
            ep = self._endpoints.get(endpoint_id)
            if ep:
                ep.thermal_score = max(0.0, min(1.0, thermal_score))
                ep.last_updated = time.time()

    def set_endpoint_health(self, endpoint_id: str, is_healthy: bool) -> None:
        with self._lock:
            ep = self._endpoints.get(endpoint_id)
            if ep:
                ep.is_healthy = is_healthy

    def select_best_endpoint(
        self,
        client_coords: Optional[GeoCoordinates] = None,
        preferred_region: Optional[str] = None,
    ) -> Optional[UpstreamEndpoint]:
        """
        Evaluate and select optimal upstream endpoint based on configured policy.
        """
        with self._lock:
            candidates = [ep for ep in self._endpoints.values() if ep.is_healthy]
            if not candidates:
                return None

            # 1. LOCAL_FIRST Policy
            if self.policy == GeoPolicy.LOCAL_FIRST:
                target_reg = preferred_region or self.local_region
                local_candidates = [
                    ep for ep in candidates
                    if ep.region == target_reg and ep.thermal_score < self.thermal_threshold
                ]
                if local_candidates:
                    # Select local candidate with minimum latency
                    return min(local_candidates, key=lambda e: e.smoothed_rtt_ms)

                # Fallback to general pool if local region is saturated or unavailable
                logger.debug("Local region '%s' saturated or empty, falling back", target_reg)

            # 2. CLOSEST_LATENCY Policy
            if self.policy == GeoPolicy.CLOSEST_LATENCY:
                # Filter out critically overheated endpoints if alternatives exist
                acceptable = [ep for ep in candidates if ep.thermal_score < self.thermal_threshold]
                pool = acceptable if acceptable else candidates
                return min(pool, key=lambda e: e.smoothed_rtt_ms)

            # 3. BALANCED_GLOBAL Multi-Objective Scoring
            reference_coords = client_coords or self._region_coords.get(self.local_region)
            scored = []
            for ep in candidates:
                cost = self._calculate_composite_cost(ep, reference_coords)
                scored.append((cost, ep))

            scored.sort(key=lambda item: item[0])
            return scored[0][1]

    def _calculate_composite_cost(
        self,
        ep: UpstreamEndpoint,
        client_coords: Optional[GeoCoordinates],
    ) -> float:
        """
        Compute normalized composite cost: lower is better.
        Cost = w_lat * norm_lat + w_geo * norm_geo + w_therm * thermal_score
        """
        # Latency component: normalize against 250ms baseline
        norm_latency = min(1.0, ep.smoothed_rtt_ms / 250.0)

        # Geographic distance component: normalize against 12,000 km baseline
        norm_geo = 0.0
        if client_coords and ep.coordinates:
            dist_km = client_coords.distance_to(ep.coordinates)
            norm_geo = min(1.0, dist_km / 12000.0)

        # Thermal component: exponential penalty if over threshold
        thermal_cost = ep.thermal_score
        if ep.thermal_score >= self.thermal_threshold:
            thermal_cost *= 2.5

        # Error penalty
        error_rate = ep.total_errors / max(1, ep.total_requests)
        error_penalty = error_rate * 2.0

        return (
            (self.latency_weight * norm_latency)
            + (self.geo_weight * norm_geo)
            + (self.thermal_weight * thermal_cost)
            + error_penalty
        )

    def get_routing_stats(self) -> Dict[str, Any]:
        """Aggregate telemetry and statistics across all regions."""
        with self._lock:
            regional_summary: Dict[str, Dict[str, Any]] = {}
            for ep in self._endpoints.values():
                reg = ep.region
                if reg not in regional_summary:
                    regional_summary[reg] = {
                        "endpoint_count": 0,
                        "healthy_count": 0,
                        "avg_rtt_ms": 0.0,
                        "avg_thermal": 0.0,
                        "total_requests": 0,
                    }
                s = regional_summary[reg]
                s["endpoint_count"] += 1
                if ep.is_healthy:
                    s["healthy_count"] += 1
                s["avg_rtt_ms"] += ep.smoothed_rtt_ms
                s["avg_thermal"] += ep.thermal_score
                s["total_requests"] += ep.total_requests

            for reg, s in regional_summary.items():
                count = max(1, s["endpoint_count"])
                s["avg_rtt_ms"] = round(s["avg_rtt_ms"] / count, 2)
                s["avg_thermal"] = round(s["avg_thermal"] / count, 3)

            return {
                "policy": self.policy.value,
                "local_region": self.local_region,
                "total_endpoints": len(self._endpoints),
                "regions": regional_summary,
            }
