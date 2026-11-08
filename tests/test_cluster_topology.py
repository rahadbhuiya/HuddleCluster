"""
Tests for HuddleCluster Real-Time Cluster Topology Graph & Visualizer (v4.31.0)
================================================================================
"""

import json
import pytest
from huddle_cluster_pkg.cluster_topology import (
    ClusterTopologyGraph,
    TopologyNode,
    TopologyLink,
)


def test_topology_graph_node_and_link_management():
    graph = ClusterTopologyGraph("test-cluster")

    # Add nodes
    n1 = graph.add_node("node-1", region="us-east-1", role="leader", temperature_c=38.5)
    n2 = graph.add_node("node-2", region="us-east-1", role="worker", temperature_c=42.0)
    n3 = graph.add_node("node-3", region="eu-central-1", role="worker", temperature_c=45.0)

    assert len(graph.list_nodes()) == 3
    assert graph.get_node("node-1").role == "leader"

    # Add links
    l1 = graph.add_link("node-1", "node-2", latency_ms=1.2)
    l2 = graph.add_link("node-1", "node-3", latency_ms=85.0)

    assert len(graph.list_links()) == 2
    assert l1.latency_ms == 1.2

    # Remove node removes its links
    graph.remove_node("node-3")
    assert len(graph.list_nodes()) == 2
    assert len(graph.list_links()) == 1


def test_topology_to_dict_and_json():
    graph = ClusterTopologyGraph("prod-cluster")
    graph.add_node("master-1", region="us-west-1", role="leader")
    graph.add_node("worker-1", region="us-west-1", role="worker")
    graph.add_link("master-1", "worker-1", latency_ms=0.8)

    d = graph.to_dict()
    assert d["cluster_name"] == "prod-cluster"
    assert d["total_nodes"] == 2
    assert d["total_links"] == 1

    json_str = graph.to_json()
    parsed = json.loads(json_str)
    assert parsed["total_nodes"] == 2
    assert parsed["nodes"][0]["node_id"] == "master-1"


def test_topology_to_dot():
    graph = ClusterTopologyGraph("huddle-dot")
    graph.add_node("lead", region="us-east-1", role="leader", status="healthy")
    graph.add_node("foll", region="us-east-1", role="follower", status="healthy")
    graph.add_link("lead", "foll", latency_ms=2.5)

    dot = graph.to_dot()
    assert 'digraph "huddle-dot"' in dot
    assert 'subgraph "cluster_us_east_1"' in dot
    assert '"lead" -> "foll"' in dot
    assert 'label="2.5ms"' in dot


def test_topology_to_ascii():
    graph = ClusterTopologyGraph("ascii-cluster")
    graph.add_node("srv-1", region="us-east-1", role="leader", temperature_c=35.0, status="healthy")
    graph.add_node("srv-2", region="us-east-1", role="worker", temperature_c=55.0, status="dead")
    graph.add_link("srv-1", "srv-2", latency_ms=1.5, packet_loss_pct=2.0)

    ascii_out = graph.to_ascii()
    assert "=== ASCII-CLUSTER TOPOLOGY GRAPH ===" in ascii_out
    assert "Region: [us-east-1]" in ascii_out
    assert "[OK]" in ascii_out
    assert "[DEAD]" in ascii_out
    assert "srv-1 ---> srv-2 (1.5ms loss=2.0%)" in ascii_out
