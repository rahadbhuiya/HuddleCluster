"""
HuddleCluster — Cluster Scheduler
===================================
Penguin-inspired thermal-fitness scheduler that selects the "coolest" node
from the cluster registry for each incoming workload placement request.

The single-instance HuddleCluster uses an inner/outer ring rotation to give
every backend server rest between requests.  The ClusterScheduler applies the
same philosophy at the cluster level: nodes that have been used recently are
considered "warmer" and yielded to less often; nodes that are freshly joined
or have been idle are "cooler" and preferred.

REST endpoints (mounted by MasterNode when a ClusterScheduler is attached):

    GET  /v1/scheduler/next  [?affinity=KEY]  → pick the best node for a workload
    POST /v1/scheduler/report                 → record workload completion

Author : Rahad Bhuiya
Version: 3.0.0
License: MIT
"""

from __future__ import annotations

import logging
import math
import threading
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)



# Fitness scoring


def _node_fitness(node_dict: Dict[str, Any], now: float) -> float:
    """
    Return a fitness score for a single node (higher = better candidate).

    Factors (all normalised to roughly [0, 1]):
    - Freshness: nodes seen recently score higher than ones we haven't heard
      from in a while (within their heartbeat budget).
    - Stability: nodes that have died many times score lower.
    - Load hint: if the node forwarded a ``requests_per_sec`` metric, use it
      as an inverse signal (high RPS = warmer = lower fitness).  Optional.
    - Bias: newly-joined nodes (low heartbeat count) get a mild bonus so they
      warm up gracefully instead of staying idle while a single hot node
      absorbs everything.
    """
    status = node_dict.get("status", "dead")
    if status in ("dead", "leaving"):
        return -1.0                          # ineligible

    last_seen = float(node_dict.get("last_seen_ago_sec", 0))
    death_count = int(node_dict.get("death_count", 0))
    hb_count = int(node_dict.get("heartbeat_count", 0))
    metrics = node_dict.get("metrics") or {}

    # Freshness (0..1): decay by 1 every 60 s of silence
    freshness = max(0.0, 1.0 - last_seen / 60.0)

    # Stability (0..1): each death halves the score, floor at 0.1
    stability = max(0.1, 1.0 / (1.0 + death_count))

    # Quarantine penalty: nodes not yet fully trusted get a 50 % penalty
    quarantine_factor = 0.5 if status == "quarantined" else 1.0

    # Load hint from forwarded metrics (optional)
    rps = metrics.get("requests_per_sec")
    if rps is not None:
        try:
            load_factor = max(0.1, 1.0 / (1.0 + float(rps) / 100.0))
        except (TypeError, ValueError):
            load_factor = 1.0
    else:
        load_factor = 1.0

    # Warm-up bonus for new nodes (low hb_count means underused)
    warmup = 1.0 + max(0.0, 1.0 - hb_count / 50.0) * 0.2

    score = freshness * stability * quarantine_factor * load_factor * warmup
    return round(score, 4)



# ClusterScheduler

class ClusterScheduler:
    """
    Thermal-fitness scheduler for the HuddleCluster multi-node system.

    Attach it to a MasterNode to enable workload placement APIs::

        from huddle_cluster_pkg import MasterNode
        from huddle_cluster_pkg.cluster_scheduler import ClusterScheduler

        scheduler = ClusterScheduler(cooldown_sec=5.0)
        master = MasterNode(port=7070, scheduler=scheduler)
        master.start()

    Then clients call ``GET /v1/scheduler/next`` to receive the address of
    the best available node and (optionally) send a completion report back
    via ``POST /v1/scheduler/report`` so the scheduler can track workload
    history for future scoring.

    Design notes
    ------------
    - The scheduler does NOT route traffic.  It recommends *which node to
      use* and the client connects to that node directly.
    - The "thermal" model: each time a node is selected it becomes "warmer"
      (``_heat[node_id]`` is incremented).  Heat decays exponentially over
      ``cooldown_sec`` so idle nodes cool back down and become eligible
      again without having to finish an explicit workload.
    - Sticky affinity: callers may pass ``?affinity=<key>`` to get the same
      node every time for a given session/user, falling back to the best
      available node if the sticky one is dead/quarantined.
    - Thread-safe: all state protected by a single RLock.
    """

    def __init__(
        self,
        cooldown_sec: float = 10.0,
        prefer_alive: bool = True,
        circuit_breaker: Optional[Any] = None,
    ) -> None:
        """
        Args:
            cooldown_sec: Half-life in seconds for heat decay.  After this
                          many seconds without being selected a node's heat
                          drops to 50 % of its last value.
            prefer_alive: If True (default), alive nodes are always preferred
                          over quarantined ones even if the quarantined node
                          has a higher raw score.
            circuit_breaker: Optional ClusterCircuitBreaker instance.  When
                             provided, nodes whose breaker is open are
                             excluded from the eligible pool before scoring.
        """
        self._cooldown      = cooldown_sec
        self._prefer_alive  = prefer_alive
        self._circuit_breaker = circuit_breaker

        self._lock = threading.RLock()
        # heat[node_id] = (heat_value, last_update_time)
        self._heat: Dict[str, tuple] = {}
        # affinity_map[key] = node_id
        self._affinity_map: Dict[str, str] = {}
        # workload_count[node_id] = total placements
        self._workload_count: Dict[str, int] = {}
        # completion reports: [(node_id, duration_ms, success)]
        self._reports: List[Dict[str, Any]] = []


    # Public API
    

    def pick(
        self,
        nodes: List[Dict[str, Any]],
        affinity_key: Optional[str] = None,
        preferred_region: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Return the single best node dict from ``nodes`` for the next workload.

        ``nodes`` is the list returned by ``MasterNode.nodes()`` — dicts with
        at minimum ``node_id``, ``address``, ``port``, ``status`` fields.
        Each node's ``metadata`` dict may include a ``"region"`` key (set via
        join metadata) for region-aware placement.

        If ``preferred_region`` is given, the eligible pool is narrowed to
        nodes whose metadata region matches (case-insensitive) — but only if
        at least one such node is eligible; otherwise the full pool is used,
        so traffic is never dropped just because a region is unavailable.

        Returns None if no eligible node exists.
        """
        now = time.time()
        eligible = [n for n in nodes if n.get("status") not in ("dead", "leaving")]
        if not eligible:
            return None

        # Exclude nodes whose circuit breaker is open
        if self._circuit_breaker is not None:
            eligible = [
                n for n in eligible
                if not self._circuit_breaker.is_open(n["node_id"])
            ]
            if not eligible:
                return None

        if preferred_region:
            target = preferred_region.strip().lower()
            regional = [
                n for n in eligible
                if str((n.get("metadata") or {}).get("region", ""))
                   .strip().lower() == target
            ]
            if regional:
                eligible = regional

        with self._lock:
            # Sticky affinity: if we've seen this key before and that node
            # is still alive/quarantined, return it.
            if affinity_key is not None:
                bound_id = self._affinity_map.get(affinity_key)
                if bound_id is not None:
                    bound = next((n for n in eligible if n["node_id"] == bound_id), None)
                    if bound is not None:
                        logger.debug(
                            "Scheduler: affinity hit — '%s' → node '%s'",
                            affinity_key, bound_id,
                        )
                        return bound

            chosen = self._select(eligible, now)
            if chosen is None:
                return None

            node_id = chosen["node_id"]
            self._apply_heat(node_id, now)
            self._workload_count[node_id] = self._workload_count.get(node_id, 0) + 1

            if affinity_key is not None:
                self._affinity_map[affinity_key] = node_id

        logger.debug("Scheduler: selected node '%s' (heat applied)", node_id)
        return chosen

    def record_report(
        self,
        node_id: str,
        duration_ms: Optional[float] = None,
        success: bool = True,
    ) -> None:
        """
        Record a workload completion report from a client.  Used for future
        scoring signal (e.g. auto-scaling) and surfaced in scheduler_stats().
        """
        with self._lock:
            self._reports.append({
                "node_id": node_id,
                "duration_ms": duration_ms,
                "success": success,
                "ts": time.time(),
            })
            # keep only the last 1000 reports to bound memory
            if len(self._reports) > 1000:
                self._reports = self._reports[-1000:]

    def scheduler_stats(self) -> Dict[str, Any]:
        """Return placement stats and heat map — useful for monitoring."""
        now = time.time()
        with self._lock:
            heat_snapshot = {
                nid: round(self._current_heat(nid, now), 4)
                for nid in self._heat
            }
            return {
                "cooldown_sec":    self._cooldown,
                "prefer_alive":    self._prefer_alive,
                "circuit_breaker": "enabled" if self._circuit_breaker is not None else "disabled",
                "heat":            heat_snapshot,
                "workload_count":  dict(self._workload_count),
                "affinity_bindings": len(self._affinity_map),
                "report_count":    len(self._reports),
            }

    
    # Internal helpers
    

    def _current_heat(self, node_id: str, now: float) -> float:
        """
        Exponential decay: heat halves every ``cooldown_sec`` seconds.
        Formula: h * 0.5^((now - t) / cooldown_sec)
        """
        if node_id not in self._heat:
            return 0.0
        heat_val, last_t = self._heat[node_id]
        elapsed = now - last_t
        decayed = heat_val * (0.5 ** (elapsed / self._cooldown))
        return decayed

    def _apply_heat(self, node_id: str, now: float) -> None:
        """Increment heat by 1.0 on top of the decayed value."""
        current = self._current_heat(node_id, now)
        self._heat[node_id] = (current + 1.0, now)

    def _select(
        self, eligible: List[Dict[str, Any]], now: float
    ) -> Optional[Dict[str, Any]]:
        """
        Pick the node with the highest combined score:
            combined = fitness(node) / (1 + heat(node))

        Dividing by heat means recently-used nodes need progressively higher
        fitness scores to be chosen again — the same penguin-huddle rotation
        logic the inner ring uses, lifted to the cluster level.

        If ``prefer_alive`` is True, nodes with status=='alive' are separated
        from 'quarantined' ones and the alive pool is tried first.
        """
        def score(node: Dict[str, Any]) -> float:
            fit = _node_fitness(node, now)
            if fit < 0:
                return -1.0
            heat = self._current_heat(node["node_id"], now)
            return fit / (1.0 + heat)

        if self._prefer_alive:
            alive = [n for n in eligible if n.get("status") == "alive"]
            if alive:
                best = max(alive, key=score)
                if score(best) >= 0:
                    return best
            # Fall back to quarantined if nothing alive
            quarantined = [n for n in eligible if n.get("status") == "quarantined"]
            if quarantined:
                best = max(quarantined, key=score)
                return best if score(best) >= 0 else None
            return None
        else:
            best = max(eligible, key=score)
            return best if score(best) >= 0 else None