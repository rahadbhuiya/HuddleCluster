"""
HuddleCluster — Cluster Circuit Breaker
=========================================
Tracks per-node error rates forwarded via heartbeat metrics and
automatically trips the breaker for nodes that exceed the error
threshold.  Tripped nodes are excluded from the scheduler's eligible
pool so traffic is rerouted before clients experience failures.

The breaker follows the standard three-state model:

``closed``    — node is healthy; requests flow normally.
``open``      — breaker tripped; node excluded from scheduling.
``half-open`` — probe window after reset_timeout_sec; one "test" pick
               is allowed to verify recovery.

Error rate is read from the ``error_rate`` key that AgentNode forwards
when it is paired with a HuddleCluster instance (v1.x thermal metrics).
Nodes that do not forward ``error_rate`` are always considered healthy
by the breaker (it only acts on evidence).

REST endpoints (mounted when ``circuit_breaker=ClusterCircuitBreaker(...)``):

    GET  /v1/breakers                  → all breaker states
    GET  /v1/breakers/<node_id>        → single breaker state
    POST /v1/breakers/<node_id>/reset  → manually reset a tripped breaker

Integration with ClusterScheduler:

    # Pass a ClusterCircuitBreaker to ClusterScheduler; it will be
    # consulted during pick() and tripped nodes are excluded.
    breaker  = ClusterCircuitBreaker(trip_threshold=0.5)
    scheduler = ClusterScheduler(circuit_breaker=breaker)

Author : Rahad Bhuiya
Version: 4.0.0
License: MIT
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

# Breaker states
CLOSED    = "closed"
OPEN      = "open"
HALF_OPEN = "half_open"


class BreakerState:
    """Per-node breaker record."""

    def __init__(self, node_id: str) -> None:
        self.node_id        = node_id
        self.state          = CLOSED
        self.tripped_at: Optional[float] = None
        self.last_error_rate: float = 0.0
        self.trip_count:    int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_id":         self.node_id,
            "state":           self.state,
            "tripped_at":      round(self.tripped_at, 3) if self.tripped_at else None,
            "last_error_rate": round(self.last_error_rate, 4),
            "trip_count":      self.trip_count,
        }


class ClusterCircuitBreaker:
    """
    Cluster-level circuit breaker.

    Attach to a MasterNode::

        from huddle_cluster_pkg import MasterNode
        from huddle_cluster_pkg.cluster_circuit_breaker import ClusterCircuitBreaker
        from huddle_cluster_pkg.cluster_scheduler       import ClusterScheduler

        breaker  = ClusterCircuitBreaker(
            trip_threshold=0.5,        # error_rate > 50% → trip
            reset_timeout_sec=30.0,    # try again after 30 s
            on_trip=lambda nid, er: alert_ops(nid, er),
            on_reset=lambda nid: print(f"{nid} recovered"),
        )
        scheduler = ClusterScheduler(circuit_breaker=breaker)
        master = MasterNode(
            port=7070,
            scheduler=scheduler,
            circuit_breaker=breaker,
        )
        master.start()

    The breaker consults node heartbeat metrics; nodes that do not forward
    ``error_rate`` are treated as healthy (breaker only acts on evidence).

    REST::

        GET  /v1/breakers                  → all breaker states
        GET  /v1/breakers/<node_id>        → single node
        POST /v1/breakers/<node_id>/reset  → manual reset
    """

    def __init__(
        self,
        trip_threshold: float = 0.5,
        reset_timeout_sec: float = 30.0,
        check_interval_sec: float = 5.0,
        on_trip: Optional[Callable[[str, float], None]] = None,
        on_reset: Optional[Callable[[str], None]] = None,
    ) -> None:
        """
        Args:
            trip_threshold:    Error rate (0–1) above which the breaker
                               trips.  Default 0.5 = 50 % error rate.
            reset_timeout_sec: Seconds after tripping before the breaker
                               enters half-open and allows a probe.
            check_interval_sec: How often to evaluate node metrics.
            on_trip:           Callback(node_id, error_rate) when breaker
                               trips.
            on_reset:          Callback(node_id) when breaker resets to
                               closed.
        """
        if not (0.0 < trip_threshold <= 1.0):
            raise ValueError("trip_threshold must be in (0, 1]")

        self.trip_threshold    = trip_threshold
        self.reset_timeout_sec = reset_timeout_sec
        self.check_interval_sec = check_interval_sec

        self._on_trip  = on_trip
        self._on_reset = on_reset

        self._lock    = threading.RLock()
        self._states: Dict[str, BreakerState] = {}
        self._master: Optional[Any] = None
        self._running = False
        self._thread:  Optional[threading.Thread] = None

    
    # Lifecycle
    

    def attach(self, master: Any) -> None:
        """Called automatically by MasterNode.start()."""
        self._master  = master
        self._running = True
        self._thread  = threading.Thread(
            target=self._loop,
            name="circuit-breaker",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "ClusterCircuitBreaker started (trip=%.0f%%, reset=%.0fs)",
            self.trip_threshold * 100, self.reset_timeout_sec,
        )

    def stop(self) -> None:
        self._running = False

    
    # Public API
    

    def is_open(self, node_id: str) -> bool:
        """True if the breaker is open (or half-open) for this node."""
        with self._lock:
            state = self._states.get(node_id)
            if state is None:
                return False
            return state.state in (OPEN, HALF_OPEN)

    def is_tripped(self, node_id: str) -> bool:
        """True if the breaker is strictly open (not half-open)."""
        with self._lock:
            state = self._states.get(node_id)
            return state is not None and state.state == OPEN

    def reset(self, node_id: str) -> bool:
        """Manually reset a breaker to closed.  Returns False if not found."""
        with self._lock:
            state = self._states.get(node_id)
            if state is None:
                return False
            if state.state == CLOSED:
                return True   # already closed, nothing to do
            state.state     = CLOSED
            state.tripped_at = None
        logger.info("CircuitBreaker: '%s' manually reset to CLOSED", node_id)
        if self._on_reset:
            try:
                self._on_reset(node_id)
            except Exception:
                logger.exception("on_reset callback raised")
        return True

    def state_for(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Return the breaker state dict for a node, or None if unknown."""
        with self._lock:
            s = self._states.get(node_id)
            return s.to_dict() if s else None

    def all_states(self) -> List[Dict[str, Any]]:
        """All known breaker states, sorted by node_id."""
        with self._lock:
            return sorted(
                (s.to_dict() for s in self._states.values()),
                key=lambda d: d["node_id"],
            )

    def summary(self) -> Dict[str, Any]:
        """Summary for REST endpoint and monitoring."""
        with self._lock:
            states  = [s.to_dict() for s in self._states.values()]
            open_ct = sum(1 for s in self._states.values()
                          if s.state in (OPEN, HALF_OPEN))
        return {
            "trip_threshold":    self.trip_threshold,
            "reset_timeout_sec": self.reset_timeout_sec,
            "total_nodes":       len(states),
            "open_breakers":     open_ct,
            "states":            sorted(states, key=lambda d: d["node_id"]),
        }

    
    # Internal evaluation loop
    

    def _loop(self) -> None:
        while self._running:
            time.sleep(self.check_interval_sec)
            if not self._running:
                break
            try:
                self._evaluate()
            except Exception:
                logger.exception("CircuitBreaker evaluation raised")

    def _evaluate(self) -> None:
        if self._master is None:
            return
        nodes = self._master.nodes()
        now   = time.time()

        for node in nodes:
            node_id    = node["node_id"]
            metrics    = node.get("metrics") or {}
            error_rate = metrics.get("error_rate")

            with self._lock:
                state = self._states.setdefault(node_id, BreakerState(node_id))

                # Reset half-open → open if not yet recovered
                # (half-open is set externally via probe; the loop just
                #  keeps track of the timeout transition)
                if state.state == OPEN and state.tripped_at is not None:
                    elapsed = now - state.tripped_at
                    if elapsed >= self.reset_timeout_sec:
                        state.state = HALF_OPEN
                        logger.info(
                            "CircuitBreaker: '%s' → HALF-OPEN (probe window)",
                            node_id,
                        )

                # If no error_rate metric — nothing to act on
                if error_rate is None:
                    continue

                try:
                    er = float(error_rate)
                except (TypeError, ValueError):
                    continue

                state.last_error_rate = er

                # Trip the breaker
                if er > self.trip_threshold and state.state == CLOSED:
                    state.state     = OPEN
                    state.tripped_at = now
                    state.trip_count += 1
                    logger.warning(
                        "CircuitBreaker: '%s' TRIPPED (error_rate=%.0f%%)",
                        node_id, er * 100,
                    )
                    if self._on_trip:
                        try:
                            self._on_trip(node_id, er)
                        except Exception:
                            logger.exception("on_trip callback raised")

                # Auto-reset when error_rate recovers
                elif er <= self.trip_threshold and state.state in (OPEN, HALF_OPEN):
                    state.state     = CLOSED
                    state.tripped_at = None
                    logger.info(
                        "CircuitBreaker: '%s' AUTO-RESET to CLOSED "
                        "(error_rate=%.0f%%)",
                        node_id, er * 100,
                    )
                    if self._on_reset:
                        try:
                            self._on_reset(node_id)
                        except Exception:
                            logger.exception("on_reset callback raised")