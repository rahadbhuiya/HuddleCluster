"""
HuddleCluster — Multi-Region Support
======================================
Cross-datacenter topology awareness.  Nodes declare their region via join
metadata; the MultiRegionManager tracks which regions are active, provides
health-aware per-region node lookup, and integrates with ClusterScheduler
for region-aware workload placement.

Nodes tag themselves at enrollment::

    huddle-cluster agent start --id web-01 --port 8080 \\
        --meta region=us-east

    # or from Python:
    agent = AgentNode(
        node_id="web-01", master_url="...", port=8080,
        metadata={"region": "us-east"},
    )

REST endpoints (mounted when multi_region=MultiRegionManager(...)):

    GET  /v1/regions              → all regions with alive-node counts
    GET  /v1/regions/<name>       → alive nodes in a specific region
    POST /v1/regions/announce     → node self-announces its region

Region-aware scheduling (via ClusterScheduler.pick):

    node = scheduler.pick(nodes, preferred_region="us-east")
    # Prefers us-east nodes; falls back to global pool if none available.

Author : Rahad Bhuiya
Version: 3.5.0
License: MIT
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)

REGION_METADATA_KEY = "region"


class MultiRegionManager:
    """
    Cross-datacenter topology awareness for HuddleCluster.

    Attach to a MasterNode::

        from huddle_cluster_pkg import MasterNode
        from huddle_cluster_pkg.cluster_multi_region import MultiRegionManager

        mr = MultiRegionManager(
            preferred_region="us-east",
            on_region_up=lambda r, nodes: print(f"{r} up: {len(nodes)} node(s)"),
            on_region_down=lambda r: alert_ops(f"{r} is DOWN"),
        )
        master = MasterNode(port=7070, multi_region=mr)
        master.start()
    """

    def __init__(
        self,
        refresh_interval_sec: float = 5.0,
        preferred_region: Optional[str] = None,
        fallback_to_global: bool = True,
        on_region_up: Optional[Callable[[str, List[Dict]], None]] = None,
        on_region_down: Optional[Callable[[str], None]] = None,
    ) -> None:
        """
        Args:
            refresh_interval_sec: How often to sync the region map.
            preferred_region:     Default region preference for scheduling.
            fallback_to_global:   Fall back to the whole cluster when the
                                  preferred region has no alive nodes.
            on_region_up:         Callback(region, nodes) when first alive
                                  node appears in a region.
            on_region_down:       Callback(region) when last alive node
                                  disappears from a region.
        """
        self.refresh_interval_sec = refresh_interval_sec
        self.preferred_region     = preferred_region
        self.fallback_to_global   = fallback_to_global

        self._on_region_up   = on_region_up
        self._on_region_down = on_region_down

        self._lock     = threading.RLock()
        self._master: Optional[Any] = None
        self._running  = False

        self._registry: Dict[str, Set[str]] = {}  # region → set of node_ids
        self._regions_up: Set[str] = set()

        self._refresh_thread: Optional[threading.Thread] = None

    
    # Lifecycle
    

    def attach(self, master: Any) -> None:
        self._master  = master
        self._running = True
        self._refresh_thread = threading.Thread(
            target=self._refresh_loop,
            name="multi-region-refresh",
            daemon=True,
        )
        self._refresh_thread.start()
        logger.info(
            "MultiRegionManager started (refresh=%.0fs, preferred=%s)",
            self.refresh_interval_sec, self.preferred_region or "none",
        )

    def stop(self) -> None:
        self._running = False

    
    # Public API
    

    def announce(self, node_id: str, region: str) -> None:
        """Register node_id in region (runtime, no restart needed)."""
        region = region.strip().lower()
        with self._lock:
            self._registry.setdefault(region, set()).add(node_id)
        logger.debug("MultiRegion: '%s' → region '%s'", node_id, region)

    def regions(self) -> List[str]:
        """All known region names."""
        with self._lock:
            return sorted(self._registry.keys())

    def alive_nodes_for_region(self, region: str) -> List[Dict[str, Any]]:
        """Alive nodes in the named region, sorted by node_id."""
        region = region.strip().lower()
        with self._lock:
            registered = self._registry.get(region, set())
            if not registered or self._master is None:
                return []
        result = [
            n for n in self._master.nodes()
            if n["node_id"] in registered and n["status"] == "alive"
        ]
        result.sort(key=lambda n: n["node_id"])
        return result

    def preferred_nodes(self, region: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Return alive nodes for the specified (or default preferred) region.
        Falls back to the full cluster pool when ``fallback_to_global`` is
        True and no nodes are available in the region.
        """
        target = (region or self.preferred_region or "").strip().lower()
        if target:
            regional = self.alive_nodes_for_region(target)
            if regional:
                return regional
            if not self.fallback_to_global:
                return []
        if self._master is None:
            return []
        return [n for n in self._master.nodes() if n["status"] == "alive"]

    def summary(self) -> Dict[str, Any]:
        """All regions with alive-node counts — for monitoring."""
        with self._lock:
            all_regions = sorted(self._registry.keys())
        out: Dict[str, Any] = {}
        for region in all_regions:
            nodes = self.alive_nodes_for_region(region)
            out[region] = {
                "alive_count": len(nodes),
                "nodes": [
                    {"node_id": n["node_id"],
                     "address": n["address"],
                     "port":    n["port"]}
                    for n in nodes
                ],
            }
        return {
            "regions":          out,
            "preferred_region": self.preferred_region,
            "fallback_global":  self.fallback_to_global,
            "regions_up":       sorted(self._regions_up),
        }

    
    # Internal refresh loop
    

    def _refresh_loop(self) -> None:
        while self._running:
            time.sleep(self.refresh_interval_sec)
            if not self._running:
                break
            try:
                self._sync_from_metadata()
                self._check_health()
            except Exception:
                logger.exception("MultiRegionManager refresh raised")

    def _sync_from_metadata(self) -> None:
        if self._master is None:
            return
        for node in self._master.nodes():
            raw = (node.get("metadata") or {}).get(REGION_METADATA_KEY)
            if not raw:
                continue
            region = str(raw).strip().lower()
            if region:
                with self._lock:
                    self._registry.setdefault(region, set()).add(
                        node["node_id"]
                    )

    def _check_health(self) -> None:
        with self._lock:
            all_regions = list(self._registry.keys())

        for region in all_regions:
            nodes  = self.alive_nodes_for_region(region)
            is_up  = len(nodes) > 0
            with self._lock:
                was_up = region in self._regions_up

            if is_up and not was_up:
                with self._lock:
                    self._regions_up.add(region)
                logger.info("Region '%s' UP (%d node(s))", region, len(nodes))
                if self._on_region_up:
                    try:
                        self._on_region_up(region, nodes)
                    except Exception:
                        logger.exception("on_region_up callback raised")

            elif not is_up and was_up:
                with self._lock:
                    self._regions_up.discard(region)
                logger.warning("Region '%s' DOWN", region)
                if self._on_region_down:
                    try:
                        self._on_region_down(region)
                    except Exception:
                        logger.exception("on_region_down callback raised")