"""
Tests for HuddleCluster Adaptive Geo-Routing & Latency-Aware Selection (v4.30.0)
================================================================================
"""

import math
import pytest
from huddle_cluster_pkg.geo_routing import (
    GeoRouter,
    GeoPolicy,
    GeoCoordinates,
    UpstreamEndpoint,
    STANDARD_REGION_COORDINATES,
)


def test_haversine_distance_calculation():
    # London (51.5074, -0.1278) to Paris (48.8566, 2.3522) ~ 343 km
    london = GeoCoordinates(51.5074, -0.1278)
    paris = GeoCoordinates(48.8566, 2.3522)
    dist = london.distance_to(paris)
    assert 340 < dist < 350

    # Same location -> 0 km
    assert london.distance_to(london) == 0.0

    # New York to Tokyo ~ 10,850 km
    ny = GeoCoordinates(40.7128, -74.0060)
    tokyo = GeoCoordinates(35.6762, 139.6503)
    dist_ny_tokyo = ny.distance_to(tokyo)
    assert 10800 < dist_ny_tokyo < 10900


def test_endpoint_registration_and_lookup():
    router = GeoRouter(local_region="us-east-1")
    ep = router.register_endpoint(
        endpoint_id="ep-1",
        region="us-east-1",
        host="10.0.0.1",
        port=8080,
    )
    assert ep.endpoint_id == "ep-1"
    assert ep.region == "us-east-1"
    assert ep.coordinates is not None
    assert ep.coordinates.latitude == STANDARD_REGION_COORDINATES["us-east-1"].latitude

    # Unregister
    removed = router.unregister_endpoint("ep-1")
    assert removed is ep
    assert router.unregister_endpoint("nonexistent") is None


def test_rtt_ema_smoothing_and_error_penalty():
    router = GeoRouter(ema_alpha=0.5)
    router.register_endpoint("ep-1", "us-east-1", "10.0.0.1", 8080, initial_rtt_ms=20.0)

    # 1. Normal latency recording
    # With alpha=0.5: new_rtt = 0.5 * 40 + 0.5 * 20 = 30.0
    router.record_rtt("ep-1", sample_rtt_ms=40.0)
    ep = router._endpoints["ep-1"]
    assert ep.smoothed_rtt_ms == 30.0
    assert ep.total_requests == 1
    assert ep.total_errors == 0

    # 2. Error recording applies heavy penalty
    router.record_rtt("ep-1", sample_rtt_ms=10.0, is_error=True)
    assert ep.total_requests == 2
    assert ep.total_errors == 1
    # Sample is penalized to max(10, 30*2) = 60; smoothed = 0.5*60 + 0.5*30 = 45.0
    assert ep.smoothed_rtt_ms == 45.0


def test_local_first_policy():
    router = GeoRouter(local_region="us-east-1", policy=GeoPolicy.LOCAL_FIRST)
    router.register_endpoint("us-1", "us-east-1", "10.0.1.1", 8080, initial_rtt_ms=25.0)
    router.register_endpoint("eu-1", "eu-central-1", "10.0.2.1", 8080, initial_rtt_ms=10.0)

    # Even though EU has lower RTT, LOCAL_FIRST picks local region
    best = router.select_best_endpoint()
    assert best is not None
    assert best.endpoint_id == "us-1"

    # If US overheats past thermal threshold (e.g. 0.90), fallback to EU
    router.update_thermal_score("us-1", 0.95)
    best_fallback = router.select_best_endpoint()
    assert best_fallback is not None
    assert best_fallback.endpoint_id == "eu-1"


def test_closest_latency_policy():
    router = GeoRouter(policy=GeoPolicy.CLOSEST_LATENCY)
    router.register_endpoint("fast", "us-west-1", "10.0.1.1", 8080, initial_rtt_ms=5.0)
    router.register_endpoint("slow", "us-east-1", "10.0.1.2", 8080, initial_rtt_ms=50.0)

    best = router.select_best_endpoint()
    assert best.endpoint_id == "fast"

    # If fast becomes unhealthy
    router.set_endpoint_health("fast", False)
    best_healthy = router.select_best_endpoint()
    assert best_healthy.endpoint_id == "slow"


def test_balanced_global_scoring():
    router = GeoRouter(
        local_region="us-east-1",
        policy=GeoPolicy.BALANCED_GLOBAL,
        latency_weight=0.5,
        geo_weight=0.2,
        thermal_weight=0.3,
    )
    # ep1: close geo, moderate rtt, low thermal
    router.register_endpoint("ep1", "us-east-1", "10.0.1.1", 8080, initial_rtt_ms=30.0)
    router.update_thermal_score("ep1", 0.1)

    # ep2: farther geo (eu-west-1), slightly faster rtt, very high thermal
    router.register_endpoint("ep2", "eu-west-1", "10.0.2.1", 8080, initial_rtt_ms=22.0)
    router.update_thermal_score("ep2", 0.90)

    # Balanced scoring should prefer ep1 due to severe thermal penalty on ep2
    best = router.select_best_endpoint()
    assert best.endpoint_id == "ep1"


def test_empty_candidates_returns_none():
    router = GeoRouter()
    assert router.select_best_endpoint() is None

    router.register_endpoint("ep1", "us-east-1", "10.0.1.1", 8080)
    router.set_endpoint_health("ep1", False)
    assert router.select_best_endpoint() is None


def test_routing_stats_aggregation():
    router = GeoRouter()
    router.register_endpoint("ep-us-1", "us-east-1", "10.0.1.1", 8080, initial_rtt_ms=20.0)
    router.register_endpoint("ep-us-2", "us-east-1", "10.0.1.2", 8080, initial_rtt_ms=30.0)
    router.register_endpoint("ep-eu-1", "eu-central-1", "10.0.2.1", 8080, initial_rtt_ms=100.0)

    router.update_thermal_score("ep-us-1", 0.4)
    router.update_thermal_score("ep-us-2", 0.6)

    stats = router.get_routing_stats()
    assert stats["total_endpoints"] == 3
    assert "us-east-1" in stats["regions"]
    assert "eu-central-1" in stats["regions"]

    us_stats = stats["regions"]["us-east-1"]
    assert us_stats["endpoint_count"] == 2
    assert us_stats["avg_rtt_ms"] == 25.0
    assert us_stats["avg_thermal"] == 0.5
