"""
HuddleCluster — Cluster Auto Scaler
=====================================
Monitors cluster load signals and fires scale-up / scale-down recommendations
via callbacks.  The autoscaler itself does NOT provision or deprovision nodes —
that is the caller's responsibility.  It is infrastructure-agnostic: wire
``on_scale_up`` / ``on_scale_down`` to your Kubernetes client, Terraform
runner, cloud SDK, or a simple shell script.

Scale decisions are based on two signals:

1. **Heat pressure** (via ClusterScheduler, if attached): if the average heat
   across alive nodes exceeds ``scale_up_heat_threshold``, the cluster is
   overloaded and a scale-up recommendation fires.

2. **Node count pressure** (standalone, no scheduler required): if the number
   of alive nodes drops below ``min_nodes``, a scale-up fires; if it exceeds
   ``max_nodes``, a scale-down fires.

Cooldown periods prevent thrashing — after any scaling action (up or down) the
autoscaler waits ``scale_up_cooldown_sec`` / ``scale_down_cooldown_sec`` before
allowing another action in the same direction.

REST endpoint (mounted by MasterNode when an autoscaler is attached):

    GET /v1/autoscaler/status   → current recommendation, last action, history

Author : Rahad Bhuiya
Version: 3.1.0
License: MIT
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)



# Scale decision constants


SCALE_UP   = "scale_up"
SCALE_DOWN = "scale_down"
SCALE_NONE = "none"


class ScaleEvent:
    """Record of a single scaling action."""

    def __init__(
        self,
        direction: str,
        reason: str,
        alive_nodes: int,
        recommended_delta: int,
        ts: float,
    ) -> None:
        self.direction       = direction
        self.reason          = reason
        self.alive_nodes     = alive_nodes
        self.recommended_delta = recommended_delta
        self.ts              = ts

    def to_dict(self) -> Dict[str, Any]:
        return {
            "direction":        self.direction,
            "reason":           self.reason,
            "alive_nodes":      self.alive_nodes,
            "recommended_delta": self.recommended_delta,
            "timestamp":        round(self.ts, 3),
        }


class ClusterAutoScaler:
    """
    Load-signal-based auto-scaling advisor for HuddleCluster.

    Attach to a MasterNode::

        from huddle_cluster_pkg import MasterNode
        from huddle_cluster_pkg.cluster_scheduler import ClusterScheduler
        from huddle_cluster_pkg.cluster_autoscaler import ClusterAutoScaler

        def add_node(delta):
            # call your cloud/K8s API here
            print(f"Provisioning {delta} new node(s)")

        def remove_node(delta):
            print(f"Deprovisioning {delta} node(s)")

        autoscaler = ClusterAutoScaler(
            min_nodes=2,
            max_nodes=10,
            scale_up_heat_threshold=0.7,   # avg heat across nodes > 70% capacity
            scale_down_heat_threshold=0.2,  # avg heat < 20% — cluster is idle
            on_scale_up=add_node,
            on_scale_down=remove_node,
        )
        master = MasterNode(port=7070, scheduler=scheduler, autoscaler=autoscaler)
        master.start()

    The autoscaler runs a background loop (``check_interval_sec``) that reads
    the live node list (and scheduler stats when available) and decides whether
    to fire a scale recommendation.
    """

    def __init__(
        self,
        min_nodes: int = 1,
        max_nodes: int = 10,
        scale_up_heat_threshold: float = 0.7,
        scale_down_heat_threshold: float = 0.2,
        scale_up_cooldown_sec: float = 120.0,
        scale_down_cooldown_sec: float = 300.0,
        check_interval_sec: float = 30.0,
        scale_up_step: int = 1,
        scale_down_step: int = 1,
        on_scale_up: Optional[Callable[[int], None]] = None,
        on_scale_down: Optional[Callable[[int], None]] = None,
    ) -> None:
        """
        Args:
            min_nodes:                  Minimum desired alive nodes.  Triggers
                                        scale-up if alive count falls below.
            max_nodes:                  Maximum desired alive nodes.  Triggers
                                        scale-down if alive count exceeds.
            scale_up_heat_threshold:    Average heat ratio (0–1) above which a
                                        scale-up fires.  Requires a
                                        ClusterScheduler to be attached to the
                                        master.
            scale_down_heat_threshold:  Average heat ratio below which a
                                        scale-down fires (when alive > min_nodes).
            scale_up_cooldown_sec:      Seconds to wait after a scale-up before
                                        allowing another.
            scale_down_cooldown_sec:    Seconds to wait after a scale-down before
                                        allowing another.
            check_interval_sec:         How often (in seconds) to evaluate load.
            scale_up_step:              How many nodes to recommend adding per event.
            scale_down_step:            How many nodes to recommend removing per event.
            on_scale_up:                Callback(delta: int) fired when a scale-up
                                        is recommended.
            on_scale_down:              Callback(delta: int) fired when a scale-down
                                        is recommended.
        """
        if min_nodes < 1:
            raise ValueError("min_nodes must be >= 1")
        if max_nodes < min_nodes:
            raise ValueError("max_nodes must be >= min_nodes")
        if not (0.0 < scale_up_heat_threshold <= 1.0):
            raise ValueError("scale_up_heat_threshold must be in (0, 1]")
        if not (0.0 <= scale_down_heat_threshold < scale_up_heat_threshold):
            raise ValueError(
                "scale_down_heat_threshold must be in [0, scale_up_heat_threshold)"
            )

        self.min_nodes                 = min_nodes
        self.max_nodes                 = max_nodes
        self.scale_up_heat_threshold   = scale_up_heat_threshold
        self.scale_down_heat_threshold = scale_down_heat_threshold
        self.scale_up_cooldown_sec     = scale_up_cooldown_sec
        self.scale_down_cooldown_sec   = scale_down_cooldown_sec
        self.check_interval_sec        = check_interval_sec
        self.scale_up_step             = scale_up_step
        self.scale_down_step           = scale_down_step

        self._on_scale_up   = on_scale_up
        self._on_scale_down = on_scale_down

        self._lock                     = threading.RLock()
        self._last_scale_up_ts:  float = 0.0
        self._last_scale_down_ts: float = 0.0
        self._last_decision:      str  = SCALE_NONE
        self._last_reason:        str  = ""
        self._history: List[ScaleEvent] = []

        self._thread:  Optional[threading.Thread] = None
        self._running: bool = False

    
    # Lifecycle
    

    def start(self, master: Any) -> None:
        """Start the background evaluation loop.  Called by MasterNode."""
        self._master  = master
        self._running = True
        self._thread  = threading.Thread(
            target=self._loop,
            name="autoscaler",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "AutoScaler started (min=%d max=%d interval=%.0fs)",
            self.min_nodes, self.max_nodes, self.check_interval_sec,
        )

    def stop(self) -> None:
        """Stop the evaluation loop."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

    
    # Public API
    

    def evaluate(
        self,
        alive_nodes: int,
        avg_heat: Optional[float] = None,
    ) -> str:
        """
        Run one evaluation cycle and return SCALE_UP / SCALE_DOWN / SCALE_NONE.

        Can be called directly (e.g. in tests) without the background loop.

        ``avg_heat`` is the mean heat value across alive nodes from
        ClusterScheduler.scheduler_stats()["heat"].  Pass None if no
        scheduler is attached.
        """
        now = time.time()
        decision = SCALE_NONE
        reason   = ""

        #  scale-up conditions 
        if alive_nodes < self.min_nodes:
            decision = SCALE_UP
            reason   = f"alive_nodes ({alive_nodes}) < min_nodes ({self.min_nodes})"

        elif (avg_heat is not None
              and avg_heat > self.scale_up_heat_threshold
              and alive_nodes < self.max_nodes):
            decision = SCALE_UP
            reason   = (
                f"avg_heat ({avg_heat:.2f}) > threshold ({self.scale_up_heat_threshold})"
            )

        #  scale-down conditions 
        elif alive_nodes > self.max_nodes:
            decision = SCALE_DOWN
            reason   = f"alive_nodes ({alive_nodes}) > max_nodes ({self.max_nodes})"

        elif (avg_heat is not None
              and avg_heat < self.scale_down_heat_threshold
              and alive_nodes > self.min_nodes):
            decision = SCALE_DOWN
            reason   = (
                f"avg_heat ({avg_heat:.2f}) < threshold ({self.scale_down_heat_threshold})"
            )

        #  cooldown guard 
        # Keep the raw (pre-cooldown) decision/reason around separately —
        # these are what we report as "last_decision"/"last_reason", since
        # that's meant to answer "what does the autoscaler currently think
        # should happen", not "did we just fire an action this exact tick".
        # Without this distinction, a condition that's been true for a
        # while (e.g. alive_nodes still below min_nodes) would flicker
        # last_decision back to "none" on every cooldown-suppressed tick,
        # even though the underlying condition never went away — which
        # made the REST status/dashboard misleadingly look "healthy"
        # between cooldown-gated firings.
        raw_decision, raw_reason = decision, reason

        if decision == SCALE_UP:
            if now - self._last_scale_up_ts < self.scale_up_cooldown_sec:
                wait = self.scale_up_cooldown_sec - (now - self._last_scale_up_ts)
                logger.debug("Scale-up suppressed by cooldown (%.0fs remaining)", wait)
                decision = SCALE_NONE
                reason   = ""

        elif decision == SCALE_DOWN:
            if now - self._last_scale_down_ts < self.scale_down_cooldown_sec:
                wait = self.scale_down_cooldown_sec - (now - self._last_scale_down_ts)
                logger.debug("Scale-down suppressed by cooldown (%.0fs remaining)", wait)
                decision = SCALE_NONE
                reason   = ""

        #  fire action 
        if decision == SCALE_UP:
            delta = self.scale_up_step
            self._record(SCALE_UP, reason, alive_nodes, delta, now)
            logger.info("Scale-up recommended: +%d node(s) — %s", delta, reason)
            if self._on_scale_up:
                try:
                    self._on_scale_up(delta)
                except Exception:
                    logger.exception("on_scale_up callback raised")

        elif decision == SCALE_DOWN:
            delta = self.scale_down_step
            self._record(SCALE_DOWN, reason, alive_nodes, -delta, now)
            logger.info("Scale-down recommended: -%d node(s) — %s", delta, reason)
            if self._on_scale_down:
                try:
                    self._on_scale_down(delta)
                except Exception:
                    logger.exception("on_scale_down callback raised")

        with self._lock:
            self._last_decision = raw_decision
            self._last_reason   = raw_reason

        return decision

    def status(self) -> Dict[str, Any]:
        """Return current autoscaler state for monitoring / REST endpoint."""
        with self._lock:
            return {
                "min_nodes":                 self.min_nodes,
                "max_nodes":                 self.max_nodes,
                "scale_up_heat_threshold":   self.scale_up_heat_threshold,
                "scale_down_heat_threshold": self.scale_down_heat_threshold,
                "scale_up_cooldown_sec":     self.scale_up_cooldown_sec,
                "scale_down_cooldown_sec":   self.scale_down_cooldown_sec,
                "check_interval_sec":        self.check_interval_sec,
                "last_decision":             self._last_decision,
                "last_reason":               self._last_reason,
                "last_scale_up_ts":          round(self._last_scale_up_ts, 3),
                "last_scale_down_ts":        round(self._last_scale_down_ts, 3),
                "scale_event_count":         len(self._history),
                "history":                   [e.to_dict() for e in self._history[-10:]],
            }

    
    # Internal helpers
    

    def _record(
        self, direction: str, reason: str,
        alive: int, delta: int, now: float,
    ) -> None:
        event = ScaleEvent(direction, reason, alive, delta, now)
        with self._lock:
            self._history.append(event)
            if len(self._history) > 200:
                self._history = self._history[-200:]
            if direction == SCALE_UP:
                self._last_scale_up_ts   = now
            else:
                self._last_scale_down_ts = now

    def _loop(self) -> None:
        while self._running:
            time.sleep(self.check_interval_sec)
            if not self._running:
                break
            try:
                self._tick()
            except Exception:
                logger.exception("AutoScaler tick raised")

    def _tick(self) -> None:
        """One evaluation cycle — gather signals then call evaluate()."""
        alive_nodes = sum(
            1 for n in self._master._nodes.values()
            if n.status == "alive"
        )

        avg_heat: Optional[float] = None
        scheduler = getattr(self._master, "_scheduler", None)
        if scheduler is not None:
            try:
                stats = scheduler.scheduler_stats()
                heat_vals = list(stats.get("heat", {}).values())
                if heat_vals:
                    avg_heat = sum(heat_vals) / len(heat_vals)
            except Exception:
                logger.exception("AutoScaler could not read scheduler stats")

        self.evaluate(alive_nodes=alive_nodes, avg_heat=avg_heat)