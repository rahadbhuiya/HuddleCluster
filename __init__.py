"""
HuddleCluster - Penguin-inspired Self-Organizing Server Load Balancer

A load-balancing algorithm inspired by Emperor Penguin huddle behaviour.

Quick start:
    from huddle_cluster import create_cluster

    cluster = create_cluster([
        ("s1", "10.0.0.1", 8080),
        ("s2", "10.0.0.2", 8080),
        ("s3", "10.0.0.3", 8080),
    ])
    cluster.start()

    with cluster.get_server_context() as server:
        response = requests.get(f"http://{server.host}:{server.port}/api")

    cluster.stop()

GitHub: https://github.com/rahadbhuiya/HuddleCluster
"""

try:
    from .huddle_cluster import (
        __version__,
        __author__,
        __license__,
        HuddleCluster,
        Server,
        ServerMetrics,
        RotationEvent,
        AlertEvent,
        TrafficRamp,
        Position,
        EvictionReason,
        RetryExhaustedError,
        AdaptiveThresholdController,
        GossipAgent,
        create_cluster,
    )
except ImportError:
    from huddle_cluster import (
        __version__,
        __author__,
        __license__,
        HuddleCluster,
        Server,
        ServerMetrics,
        RotationEvent,
        AlertEvent,
        TrafficRamp,
        Position,
        EvictionReason,
        RetryExhaustedError,
        AdaptiveThresholdController,
        GossipAgent,
        create_cluster,
    )

__all__ = [
    "__version__",
    "HuddleCluster",
    "Server",
    "ServerMetrics",
    "RotationEvent",
    "AlertEvent",
    "TrafficRamp",
    "Position",
    "EvictionReason",
    "RetryExhaustedError",
    "AdaptiveThresholdController",
    "GossipAgent",
    "create_cluster",
]