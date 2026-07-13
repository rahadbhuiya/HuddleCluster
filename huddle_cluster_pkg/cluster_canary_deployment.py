"""
HuddleCluster — Canary Deployment
====================================
Weight-based traffic splitting that lets you gradually shift requests from
stable nodes to a canary (new-version) pool.

Workflow
--------
1. Tag canary nodes via metadata (``canary=true``) or runtime announce.
2. ``POST /v1/canary/start`` with ``{"weight": 5}`` → 5 % of picks go to
   canary nodes.
3. Monitor metrics.  Use ``POST /v1/canary/advance`` to step the weight up
   (5 → 25 → 50 → 100), or ``POST /v1/canary/abort`` to send all traffic
   back to stable nodes immediately.
4. At weight 100 all traffic goes to canary.  Call ``promote`` to make
   canary nodes permanent and clear the deployment.

Selection model
---------------
The canary manager wraps ``ClusterScheduler.pick()``.  On each call it
decides — based on the current weight — whether to draw from the canary
pool or the stable pool using a simple probabilistic split.  The scheduler's
thermal fitness and circuit-breaker/rate-limiter exclusions still apply
within each pool; the canary manager only controls which pool the scheduler
selects from.

REST endpoints (mounted when ``canary=ClusterCanaryDeployment(...)``):

    POST /v1/canary/start      → begin deployment with initial weight
    GET  /v1/canary/status     → current weight, pool sizes, history
    POST /v1/canary/advance    → step weight up to next level
    POST /v1/canary/promote    → graduate canary to stable; end deployment
    POST /v1/canary/abort      → return all traffic to stable immediately

Author : Rahad Bhuiya
Version: 4.2.0
License: MIT
"""

from __future__ import annotations

import logging
import random
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)

# Deployment phases
PHASE_IDLE      = "idle"
PHASE_ACTIVE    = "active"
PHASE_PROMOTED  = "promoted"
PHASE_ABORTED   = "aborted"


class CanaryEvent:
    """Record of a single phase transition."""

    def __init__(self, action: str, weight: float, note: str = "") -> None:
        self.action = action
        self.weight = weight
        self.note   = note
        self.ts     = time.time()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "action": self.action,
            "weight": self.weight,
            "note":   self.note,
            "ts":     round(self.ts, 3),
        }


class ClusterCanaryDeployment:
    """
    Canary deployment manager for HuddleCluster.

    Attach to a MasterNode and wire with ClusterScheduler::

        from huddle_cluster_pkg import MasterNode
        from huddle_cluster_pkg.cluster_scheduler        import ClusterScheduler
        from huddle_cluster_pkg.cluster_canary_deployment import ClusterCanaryDeployment

        canary    = ClusterCanaryDeployment(
            stable_tag="stable",
            canary_tag="canary",
            weight_steps=[5, 25, 50, 100],
            on_promote=lambda: finalize_deploy(),
            on_abort=lambda: rollback(),
        )
        scheduler = ClusterScheduler(canary=canary)
        master    = MasterNode(
            port=7070,
            scheduler=scheduler,
            canary=canary,
        )
        master.start()

    Tag nodes via metadata::

        huddle-cluster agent start --id web-v2-1 --meta canary=true
        huddle-cluster agent start --id web-v1-1 --meta stable=true  # or simply no tag

    Then kick off a deployment::

        curl -X POST http://localhost:7070/v1/canary/start   -d '{"weight": 5}'
        curl -X POST http://localhost:7070/v1/canary/advance
        curl -X POST http://localhost:7070/v1/canary/promote
    """

    def __init__(
        self,
        stable_tag: str = "stable",
        canary_tag: str = "canary",
        weight_steps: Optional[List[float]] = None,
        on_promote: Optional[Callable[[], None]] = None,
        on_abort: Optional[Callable[[], None]] = None,
        on_weight_change: Optional[Callable[[float], None]] = None,
    ) -> None:
        """
        Args:
            stable_tag:       Metadata key whose value marks stable nodes.
                              Nodes whose metadata does NOT contain ``canary_tag``
                              are treated as stable automatically.
            canary_tag:       Metadata key (``canary=true``) marking new nodes.
            weight_steps:     Ordered list of traffic percentages to step
                              through.  Default: [5, 25, 50, 100].
            on_promote:       Callback when deployment is promoted (weight=100
                              and promote() is called).
            on_abort:         Callback when deployment is aborted.
            on_weight_change: Callback(new_weight) on every weight adjustment.
        """
        self.stable_tag   = stable_tag
        self.canary_tag   = canary_tag
        self.weight_steps = list(weight_steps or [5, 25, 50, 100])

        self._on_promote       = on_promote
        self._on_abort         = on_abort
        self._on_weight_change = on_weight_change

        self._lock     = threading.RLock()
        self._phase    = PHASE_IDLE
        self._weight: float = 0.0          # 0–100 % to canary pool
        self._step_idx: int = 0
        self._history: List[CanaryEvent] = []

        # Runtime-announced canary node IDs (in addition to metadata tags)
        self._canary_ids: Set[str] = set()
        self._master: Optional[Any] = None

    
    # Lifecycle
    

    def attach(self, master: Any) -> None:
        self._master = master
        logger.info("ClusterCanaryDeployment attached (steps=%s)", self.weight_steps)

    def stop(self) -> None:
        pass   # no background thread

    
    # Deployment control
    

    def start(self, weight: Optional[float] = None) -> bool:
        """
        Begin a canary deployment at the given weight (or first step).
        Returns False if a deployment is already active.
        """
        with self._lock:
            if self._phase == PHASE_ACTIVE:
                return False
            self._phase    = PHASE_ACTIVE
            self._step_idx = 0
            initial        = weight if weight is not None else self.weight_steps[0]
            self._weight   = float(max(0.0, min(100.0, initial)))
            self._record("start", self._weight, f"initial weight {self._weight:.0f}%")
        logger.info("CanaryDeployment started at %.0f%%", self._weight)
        self._fire_weight_change(self._weight)
        return True

    def advance(self) -> bool:
        """
        Step weight up to the next level.  Returns False if idle/promoted/aborted.
        """
        with self._lock:
            if self._phase != PHASE_ACTIVE:
                return False
            # Find next step above current weight
            next_steps = [s for s in self.weight_steps if s > self._weight]
            if not next_steps:
                return False   # already at maximum
            self._weight   = float(next_steps[0])
            self._step_idx += 1
            self._record("advance", self._weight, f"stepped to {self._weight:.0f}%")
        logger.info("CanaryDeployment advanced to %.0f%%", self._weight)
        self._fire_weight_change(self._weight)
        return True

    def set_weight(self, weight: float) -> bool:
        """Directly set any weight 0–100.  Returns False if not active."""
        with self._lock:
            if self._phase != PHASE_ACTIVE:
                return False
            self._weight = float(max(0.0, min(100.0, weight)))
            self._record("set_weight", self._weight, f"manual set to {self._weight:.0f}%")
        logger.info("CanaryDeployment weight set to %.0f%%", self._weight)
        self._fire_weight_change(self._weight)
        return True

    def promote(self) -> bool:
        """
        Graduate canary nodes to stable and end the deployment.
        Returns False if not active.
        """
        with self._lock:
            if self._phase != PHASE_ACTIVE:
                return False
            self._phase  = PHASE_PROMOTED
            self._weight = 100.0
            self._record("promote", 100.0, "canary promoted to stable")
        logger.info("CanaryDeployment promoted — canary is now stable")
        if self._on_promote:
            try:
                self._on_promote()
            except Exception:
                logger.exception("on_promote callback raised")
        return True

    def abort(self) -> bool:
        """
        Return all traffic to stable nodes immediately.
        Returns False if not active.
        """
        with self._lock:
            if self._phase != PHASE_ACTIVE:
                return False
            self._phase  = PHASE_ABORTED
            self._weight = 0.0
            self._record("abort", 0.0, "deployment aborted — 100% stable")
        logger.warning("CanaryDeployment ABORTED — all traffic to stable")
        if self._on_abort:
            try:
                self._on_abort()
            except Exception:
                logger.exception("on_abort callback raised")
        return True

    def announce_canary(self, node_id: str) -> None:
        """Runtime-tag a node as canary (alternative to metadata)."""
        with self._lock:
            self._canary_ids.add(node_id)
        logger.debug("CanaryDeployment: '%s' announced as canary", node_id)

    def remove_canary(self, node_id: str) -> bool:
        """Remove a node from the runtime canary set."""
        with self._lock:
            if node_id in self._canary_ids:
                self._canary_ids.discard(node_id)
                return True
        return False

    def status(self) -> Dict[str, Any]:
        """Full deployment state for monitoring / REST."""
        with self._lock:
            phase  = self._phase
            weight = self._weight
            history = [e.to_dict() for e in self._history[-20:]]
            canary_ids = sorted(self._canary_ids)

        canary_nodes, stable_nodes = self._pool_sizes()
        return {
            "phase":            phase,
            "weight_pct":       weight,
            "weight_steps":     self.weight_steps,
            "canary_tag":       self.canary_tag,
            "canary_nodes":     canary_nodes,
            "stable_nodes":     stable_nodes,
            "announced_canary": canary_ids,
            "history":          history,
        }

    
    # Scheduler integration
    

    def pick_pool(
        self,
        nodes: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Given a list of eligible nodes, return the pool the scheduler should
        draw from this call: canary pool or stable pool, chosen probabilistically
        based on the current weight.

        Returns the full list unchanged when the deployment is idle/aborted.
        """
        with self._lock:
            phase  = self._phase
            weight = self._weight
            canary_ids = set(self._canary_ids)

        if phase not in (PHASE_ACTIVE, PHASE_PROMOTED):
            return nodes   # no deployment — use full pool

        canary = [
            n for n in nodes
            if self._is_canary(n, canary_ids)
        ]
        stable = [
            n for n in nodes
            if not self._is_canary(n, canary_ids)
        ]

        if not canary:
            return stable or nodes
        if not stable:
            return canary

        # Weighted coin flip: weight% of calls go to canary
        if random.uniform(0, 100) < weight:
            return canary
        return stable

    
    # Internal helpers
    

    def _is_canary(
        self, node: Dict[str, Any], canary_ids: Set[str]
    ) -> bool:
        if node["node_id"] in canary_ids:
            return True
        meta = node.get("metadata") or {}
        val  = str(meta.get(self.canary_tag, "")).strip().lower()
        return val in ("true", "1", "yes")

    def _pool_sizes(self):
        if self._master is None:
            return 0, 0
        with self._lock:
            canary_ids = set(self._canary_ids)
        nodes   = [n for n in self._master.nodes() if n["status"] == "alive"]
        canary  = sum(1 for n in nodes if self._is_canary(n, canary_ids))
        stable  = len(nodes) - canary
        return canary, stable

    def _record(self, action: str, weight: float, note: str = "") -> None:
        self._history.append(CanaryEvent(action, weight, note))
        if len(self._history) > 100:
            self._history = self._history[-100:]

    def _fire_weight_change(self, weight: float) -> None:
        if self._on_weight_change:
            try:
                self._on_weight_change(weight)
            except Exception:
                logger.exception("on_weight_change callback raised")