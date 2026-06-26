"""
HuddleCluster — Rolling Updater
=================================
Orchestrates zero-downtime node upgrades across the cluster, one batch at a
time.  Like the AutoScaler, it does NOT perform the actual update itself —
that is the caller's responsibility.  Provide an ``update_fn`` that knows how
to upgrade a single node (SSH, K8s rolling restart, Ansible, Docker pull,
etc.) and the updater handles the sequencing, health gating, and drain timing.

Algorithm per batch
-------------------
1. Pick the next ``batch_size`` nodes that haven't been updated yet.
2. Check the health gate — if alive_ratio < ``health_gate_ratio`` abort
   or pause (configurable) rather than making a degraded cluster worse.
3. For each node in the batch call ``update_fn(node_dict)``.
4. Wait up to ``drain_timeout_sec`` for each node to send a heartbeat again
   (proving it came back healthy) before moving to the next batch.
5. If a node does not come back within ``drain_timeout_sec``, the rollout
   is marked FAILED and stops.

REST endpoints (mounted by MasterNode when a RollingUpdater is attached):

    POST /v1/rollout/start      → kick off a rollout
    GET  /v1/rollout/status     → progress, phase, per-node outcomes
    POST /v1/rollout/pause      → pause after the current batch finishes
    POST /v1/rollout/resume     → resume a paused rollout
    POST /v1/rollout/abort      → stop immediately (already-updated nodes stay)

Author : Rahad Bhuiya
Version: 3.2.0
License: MIT
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

# Rollout phases
PHASE_IDLE     = "idle"
PHASE_RUNNING  = "running"
PHASE_PAUSED   = "paused"
PHASE_DONE     = "done"
PHASE_FAILED   = "failed"
PHASE_ABORTED  = "aborted"


class NodeOutcome:
    """Result of updating a single node."""

    def __init__(self, node_id: str) -> None:
        self.node_id    = node_id
        self.status     = "pending"   # pending | updated | failed | skipped
        self.started_at: Optional[float] = None
        self.ended_at:   Optional[float] = None
        self.error:      Optional[str]   = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_id":    self.node_id,
            "status":     self.status,
            "started_at": round(self.started_at, 3) if self.started_at else None,
            "ended_at":   round(self.ended_at,   3) if self.ended_at   else None,
            "error":      self.error,
        }


class ClusterRollingUpdater:
    """
    Zero-downtime rolling update orchestrator for HuddleCluster.

    Attach it to a MasterNode::

        from huddle_cluster_pkg import MasterNode
        from huddle_cluster_pkg.cluster_rolling_updater import ClusterRollingUpdater

        def upgrade_node(node: dict):
            # SSH in and run the upgrade, restart the service, etc.
            subprocess.run(["ansible-playbook", "upgrade.yml",
                            f"--limit={node['address']}"])

        updater = ClusterRollingUpdater(
            update_fn=upgrade_node,
            batch_size=1,
            drain_timeout_sec=60,
            health_gate_ratio=0.5,
        )
        master = MasterNode(port=7070, rolling_updater=updater)
        master.start()

    Then trigger a rollout via REST::

        POST /v1/rollout/start          → starts rolling update
        GET  /v1/rollout/status         → check progress
        POST /v1/rollout/pause          → pause after current batch
        POST /v1/rollout/resume         → resume
        POST /v1/rollout/abort          → stop now
    """

    def __init__(
        self,
        update_fn: Callable[[Dict[str, Any]], None],
        batch_size: int = 1,
        drain_timeout_sec: float = 60.0,
        health_gate_ratio: float = 0.5,
        update_order: str = "alive_first",
        on_node_updated: Optional[Callable[[str], None]] = None,
        on_node_failed: Optional[Callable[[str, str], None]] = None,
        on_rollout_complete: Optional[Callable[[], None]] = None,
    ) -> None:
        """
        Args:
            update_fn:            Callable(node_dict) → None.  Called for each
                                  node; expected to block until the update is
                                  done (or raise on failure).
            batch_size:           How many nodes to update in parallel per wave.
            drain_timeout_sec:    How long to wait for a node to return healthy
                                  (send a heartbeat) after update_fn returns.
            health_gate_ratio:    Minimum fraction of alive nodes required before
                                  each batch.  If alive_ratio < this, the rollout
                                  pauses (and logs a warning) rather than
                                  degrading the cluster further.
            update_order:         "alive_first" (default) — update alive nodes
                                  before dead/quarantined; "stable_first" —
                                  prefer nodes with the fewest deaths.
            on_node_updated:      Callback(node_id) after a node successfully
                                  returns healthy post-update.
            on_node_failed:       Callback(node_id, error) when a node fails to
                                  return within drain_timeout_sec.
            on_rollout_complete:  Callback() when every node has been processed.
        """
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        if not (0.0 <= health_gate_ratio < 1.0):
            raise ValueError("health_gate_ratio must be in [0, 1)")
        if update_order not in ("alive_first", "stable_first"):
            raise ValueError("update_order must be 'alive_first' or 'stable_first'")

        self._update_fn          = update_fn
        self.batch_size          = batch_size
        self.drain_timeout_sec   = drain_timeout_sec
        self.health_gate_ratio   = health_gate_ratio
        self.update_order        = update_order

        self._on_node_updated    = on_node_updated
        self._on_node_failed     = on_node_failed
        self._on_rollout_complete = on_rollout_complete

        self._lock               = threading.RLock()
        self._phase              = PHASE_IDLE
        self._outcomes:  Dict[str, NodeOutcome] = {}
        self._started_at: Optional[float] = None
        self._ended_at:   Optional[float] = None
        self._pause_event = threading.Event()
        self._abort_event = threading.Event()
        self._thread:     Optional[threading.Thread] = None
        self._master:     Optional[Any] = None

    
    # Lifecycle (called by MasterNode)
    

    def attach(self, master: Any) -> None:
        """Attach to a MasterNode.  Called automatically by MasterNode."""
        self._master = master

    
    # Public control API
    

    def start_rollout(self) -> bool:
        """
        Kick off a rolling update across all currently-registered alive nodes.
        Returns False if a rollout is already in progress.
        """
        with self._lock:
            if self._phase in (PHASE_RUNNING, PHASE_PAUSED):
                return False
            self._phase      = PHASE_RUNNING
            self._started_at = time.time()
            self._ended_at   = None
            self._outcomes   = {}
            self._pause_event.clear()
            self._abort_event.clear()

        self._thread = threading.Thread(
            target=self._run,
            name="rolling-updater",
            daemon=True,
        )
        self._thread.start()
        logger.info("Rolling update started (batch_size=%d, drain_timeout=%.0fs)",
                    self.batch_size, self.drain_timeout_sec)
        return True

    def pause(self) -> bool:
        """Pause after the current batch finishes.  Returns False if not running."""
        with self._lock:
            if self._phase != PHASE_RUNNING:
                return False
            self._pause_event.set()
        logger.info("Rolling update pause requested")
        return True

    def resume(self) -> bool:
        """Resume a paused rollout.  Returns False if not paused."""
        with self._lock:
            if self._phase != PHASE_PAUSED:
                return False
            self._phase = PHASE_RUNNING
            self._pause_event.clear()
        logger.info("Rolling update resumed")
        return True

    def abort(self) -> bool:
        """Abort the rollout immediately.  Already-updated nodes are not reverted."""
        with self._lock:
            if self._phase not in (PHASE_RUNNING, PHASE_PAUSED):
                return False
            self._abort_event.set()
            self._pause_event.clear()   # unblock if waiting on pause
        logger.info("Rolling update abort requested")
        return True

    def status(self) -> Dict[str, Any]:
        """Return full rollout state for monitoring / REST endpoint."""
        with self._lock:
            outcomes = {nid: o.to_dict() for nid, o in self._outcomes.items()}
            total    = len(outcomes)
            done     = sum(1 for o in self._outcomes.values()
                           if o.status in ("updated", "failed", "skipped"))
            return {
                "phase":              self._phase,
                "batch_size":         self.batch_size,
                "drain_timeout_sec":  self.drain_timeout_sec,
                "health_gate_ratio":  self.health_gate_ratio,
                "update_order":       self.update_order,
                "total_nodes":        total,
                "nodes_done":         done,
                "nodes_remaining":    max(0, total - done),
                "started_at":         round(self._started_at, 3) if self._started_at else None,
                "ended_at":           round(self._ended_at,   3) if self._ended_at   else None,
                "outcomes":           outcomes,
            }

    
    # Internal rollout loop
    

    def _run(self) -> None:
        try:
            self._do_rollout()
        except Exception:
            logger.exception("Rolling updater raised an unexpected error")
            with self._lock:
                self._phase    = PHASE_FAILED
                self._ended_at = time.time()

    def _do_rollout(self) -> None:
        nodes = self._sorted_nodes()
        if not nodes:
            logger.warning("Rolling update: no eligible nodes found, nothing to do")
            with self._lock:
                self._phase    = PHASE_DONE
                self._ended_at = time.time()
            return

        with self._lock:
            for n in nodes:
                self._outcomes[n["node_id"]] = NodeOutcome(n["node_id"])

        batches = [nodes[i:i + self.batch_size]
                   for i in range(0, len(nodes), self.batch_size)]

        for batch in batches:
            #  abort check 
            if self._abort_event.is_set():
                with self._lock:
                    self._phase    = PHASE_ABORTED
                    self._ended_at = time.time()
                logger.info("Rolling update aborted")
                return

            #  pause check 
            if self._pause_event.is_set():
                with self._lock:
                    self._phase = PHASE_PAUSED
                logger.info("Rolling update paused — waiting for resume")
                while self._pause_event.is_set():
                    if self._abort_event.is_set():
                        with self._lock:
                            self._phase    = PHASE_ABORTED
                            self._ended_at = time.time()
                        return
                    time.sleep(0.1)

            #  health gate 
            if not self._health_gate_ok():
                logger.warning(
                    "Rolling update health gate blocked: alive ratio < %.0f%% — "
                    "pausing to avoid further degradation",
                    self.health_gate_ratio * 100,
                )
                with self._lock:
                    self._phase = PHASE_PAUSED
                # Wait until healthy enough to continue or aborted
                while not self._health_gate_ok():
                    if self._abort_event.is_set():
                        with self._lock:
                            self._phase    = PHASE_ABORTED
                            self._ended_at = time.time()
                        return
                    time.sleep(1.0)
                with self._lock:
                    self._phase = PHASE_RUNNING

            #  update batch in parallel threads 
            threads = []
            for node in batch:
                t = threading.Thread(
                    target=self._update_one,
                    args=(node,),
                    daemon=True,
                )
                threads.append(t)
                t.start()
            for t in threads:
                t.join()

            #  check if any node in the batch hard-failed 
            batch_ids = {n["node_id"] for n in batch}
            if any(self._outcomes[nid].status == "failed" for nid in batch_ids):
                with self._lock:
                    self._phase    = PHASE_FAILED
                    self._ended_at = time.time()
                logger.error("Rolling update FAILED — stopping after failed batch")
                return

        # All batches done
        with self._lock:
            self._phase    = PHASE_DONE
            self._ended_at = time.time()
        logger.info("Rolling update complete")
        if self._on_rollout_complete:
            try:
                self._on_rollout_complete()
            except Exception:
                logger.exception("on_rollout_complete callback raised")

    def _update_one(self, node: Dict[str, Any]) -> None:
        node_id = node["node_id"]
        outcome = self._outcomes[node_id]
        outcome.started_at = time.time()

        try:
            logger.info("Rolling update: calling update_fn for '%s'", node_id)
            self._update_fn(node)
        except Exception as exc:
            outcome.status  = "failed"
            outcome.ended_at = time.time()
            outcome.error   = str(exc)
            logger.error("Rolling update: update_fn raised for '%s': %s", node_id, exc)
            if self._on_node_failed:
                try:
                    self._on_node_failed(node_id, str(exc))
                except Exception:
                    logger.exception("on_node_failed callback raised")
            return

        # Wait for the node to come back (send a heartbeat)
        logger.info(
            "Rolling update: waiting up to %.0fs for '%s' to return healthy",
            self.drain_timeout_sec, node_id,
        )
        deadline = time.time() + self.drain_timeout_sec
        came_back = False
        while time.time() < deadline:
            with self._master._lock:
                node_rec = self._master._nodes.get(node_id)
                if node_rec and node_rec.status == "alive":
                    came_back = True
                    break
            time.sleep(0.5)

        outcome.ended_at = time.time()
        if came_back:
            outcome.status = "updated"
            logger.info("Rolling update: '%s' returned healthy", node_id)
            if self._on_node_updated:
                try:
                    self._on_node_updated(node_id)
                except Exception:
                    logger.exception("on_node_updated callback raised")
        else:
            outcome.status = "failed"
            outcome.error  = f"did not return healthy within {self.drain_timeout_sec:.0f}s"
            logger.error("Rolling update: '%s' did not return within timeout", node_id)
            if self._on_node_failed:
                try:
                    self._on_node_failed(node_id, outcome.error)
                except Exception:
                    logger.exception("on_node_failed callback raised")

    def _health_gate_ok(self) -> bool:
        if self._master is None or self.health_gate_ratio == 0.0:
            return True
        with self._master._lock:
            total = len(self._master._nodes)
            if total == 0:
                return True
            alive = sum(1 for n in self._master._nodes.values()
                        if n.status == "alive")
        return (alive / total) >= self.health_gate_ratio

    def _sorted_nodes(self) -> List[Dict[str, Any]]:
        if self._master is None:
            return []
        nodes = self._master.nodes()
        eligible = [n for n in nodes
                    if n.get("status") not in ("dead", "leaving")]
        if self.update_order == "stable_first":
            eligible.sort(key=lambda n: n.get("death_count", 0))
        else:  # alive_first
            status_rank = {"alive": 0, "quarantined": 1}
            eligible.sort(key=lambda n: status_rank.get(n.get("status", ""), 9))
        return eligible