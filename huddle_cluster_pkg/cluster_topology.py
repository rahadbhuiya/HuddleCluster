"""
HuddleCluster — Real-Time Cluster Topology Graph & Visualizer Engine
=====================================================================
v4.31.0

Provides cluster-wide topology graph generation, real-time status mapping,
and multi-format rendering (JSON, ASCII art, and Graphviz DOT) for CLI
diagnostics and dashboard observability.

Features:
  - ClusterTopologyGraph: Graph model representing nodes, regions, links, and thermal tiers.
  - Multi-format rendering:
      * ASCII tree / grid visualization for terminal CLI.
      * Structured JSON format for web UI graphs.
      * Graphviz DOT notation for topology diagram generation.
  - Dynamic link latency overlay and health-status coloring.

Author : Rahad Bhuiya
Version: 4.31.0
License: MIT
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger("HuddleCluster.Topology")


@dataclass
class TopologyNode:
    """Represents a node in the cluster topology graph."""
    node_id: str
    region: str = "default"
    role: str = "worker"  # 'leader', 'follower', 'candidate', 'worker'
    thermal_tier: str = "core"  # 'core', 'mid', 'outer', 'evicted'
    status: str = "healthy"  # 'healthy', 'degraded', 'dead', 'quarantined'
    temperature_c: float = 40.0
    active_connections: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TopologyLink:
    """Represents a directional or bi-directional link between two nodes."""
    source_id: str
    target_id: str
    latency_ms: float = 1.0
    packet_loss_pct: float = 0.0
    status: str = "healthy"  # 'healthy', 'degraded', 'down'


class ClusterTopologyGraph:
    """
    Graph data structure representing cluster topology, relationships, and health.
    """

    def __init__(self, cluster_name: str = "huddle-cluster") -> None:
        self.cluster_name = cluster_name
        self._nodes: Dict[str, TopologyNode] = {}
        self._links: Dict[str, TopologyLink] = {}

    def add_node(
        self,
        node_id: str,
        region: str = "default",
        role: str = "worker",
        thermal_tier: str = "core",
        status: str = "healthy",
        temperature_c: float = 40.0,
        active_connections: int = 0,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> TopologyNode:
        node = TopologyNode(
            node_id=node_id,
            region=region,
            role=role,
            thermal_tier=thermal_tier,
            status=status,
            temperature_c=temperature_c,
            active_connections=active_connections,
            metadata=metadata or {},
        )
        self._nodes[node_id] = node
        return node

    def remove_node(self, node_id: str) -> Optional[TopologyNode]:
        node = self._nodes.pop(node_id, None)
        if node:
            # Remove any linked edges
            links_to_remove = [
                k for k, link in self._links.items()
                if link.source_id == node_id or link.target_id == node_id
            ]
            for k in links_to_remove:
                self._links.pop(k, None)
        return node

    def add_link(
        self,
        source_id: str,
        target_id: str,
        latency_ms: float = 1.0,
        packet_loss_pct: float = 0.0,
        status: str = "healthy",
    ) -> TopologyLink:
        link_key = f"{source_id}->{target_id}"
        link = TopologyLink(
            source_id=source_id,
            target_id=target_id,
            latency_ms=latency_ms,
            packet_loss_pct=packet_loss_pct,
            status=status,
        )
        self._links[link_key] = link
        return link

    def get_node(self, node_id: str) -> Optional[TopologyNode]:
        return self._nodes.get(node_id)

    def list_nodes(self) -> List[TopologyNode]:
        return list(self._nodes.values())

    def list_links(self) -> List[TopologyLink]:
        return list(self._links.values())


    # Export Formats


    def to_dict(self) -> Dict[str, Any]:
        """Export topology graph as structured dictionary."""
        return {
            "cluster_name": self.cluster_name,
            "total_nodes": len(self._nodes),
            "total_links": len(self._links),
            "nodes": [asdict(n) for n in self._nodes.values()],
            "links": [asdict(l) for l in self._links.values()],
        }

    def to_json(self, indent: int = 2) -> str:
        """Export topology graph as formatted JSON string."""
        return json.dumps(self.to_dict(), indent=indent)

    def to_dot(self) -> str:
        """Export topology graph in Graphviz DOT format."""
        lines = [f'digraph "{self.cluster_name}" {{', '  rankdir="LR";']

        # Group by region
        regions: Dict[str, List[TopologyNode]] = {}
        for node in self._nodes.values():
            regions.setdefault(node.region, []).append(node)

        for region, nodes in regions.items():
            cluster_id = region.replace("-", "_")
            lines.append(f'  subgraph "cluster_{cluster_id}" {{')
            lines.append(f'    label = "{region}";')
            lines.append('    style = "rounded,dashed";')
            lines.append('    color = "#888888";')
            for n in nodes:
                color = "#4CAF50" if n.status == "healthy" else ("#F44336" if n.status == "dead" else "#FF9800")
                shape = "box" if n.role == "leader" else "ellipse"
                label = f"{n.node_id}\\n({n.role}, {n.temperature_c}°C)"
                lines.append(f'    "{n.node_id}" [label="{label}", shape="{shape}", color="{color}"];')
            lines.append("  }")

        for link in self._links.values():
            edge_label = f"{link.latency_ms}ms"
            edge_color = "#4CAF50" if link.status == "healthy" else "#F44336"
            lines.append(
                f'  "{link.source_id}" -> "{link.target_id}" '
                f'[label="{edge_label}", color="{edge_color}"];'
            )

        lines.append("}")
        return "\n".join(lines)

    def to_ascii(self) -> str:
        """Render topology as a clean ASCII diagram for CLI output."""
        lines = [
            f"=== {self.cluster_name.upper()} TOPOLOGY GRAPH ===",
            f"Nodes: {len(self._nodes)} | Links: {len(self._links)}",
            "",
        ]

        regions: Dict[str, List[TopologyNode]] = {}
        for node in self._nodes.values():
            regions.setdefault(node.region, []).append(node)

        for region, nodes in sorted(regions.items()):
            lines.append(f"Region: [{region}]")
            for node in sorted(nodes, key=lambda n: n.node_id):
                status_icon = "[OK]" if node.status == "healthy" else (
                    "[DEAD]" if node.status == "dead" else "[WARN]"
                )
                role_tag = f"<{node.role.upper()}>"
                lines.append(
                    f"  * {node.node_id:15} {role_tag:10} tier={node.thermal_tier:6} "
                    f"temp={node.temperature_c:4.1f}C  {status_icon}"
                )
            lines.append("")

        if self._links:
            lines.append("Inter-Node Links:")
            for link in sorted(self._links.values(), key=lambda l: (l.source_id, l.target_id)):
                loss_str = f" loss={link.packet_loss_pct:.1f}%" if link.packet_loss_pct > 0 else ""
                lines.append(f"  {link.source_id} ---> {link.target_id} ({link.latency_ms:.1f}ms{loss_str})")

        return "\n".join(lines)
