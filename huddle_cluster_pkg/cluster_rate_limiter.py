"""
HuddleCluster — Cluster Rate Limiter
=======================================
Per-node token-bucket rate limiter.  Each node in the cluster gets its own
bucket with configurable capacity and refill rate.  When a node's bucket is
empty the scheduler skips it and picks the next best eligible node instead,
so burst traffic is naturally spread across the cluster rather than hammering
a single node.

Token bucket algorithm
----------------------
*  Capacity:    the maximum number of tokens a bucket can hold (= max burst).
*  Refill rate: tokens added per second (continuous, not bursty).
*  Consume:     each ``scheduler.pick()`` call consumes 1 token from the
               chosen node.  If the bucket is empty, that node is excluded
               from the current selection cycle.

Buckets are created lazily on first use and reset automatically when a node
deregisters.

REST endpoints (mounted when ``rate_limiter=ClusterRateLimiter(...)``):

    GET  /v1/ratelimits                    → all node bucket states
    GET  /v1/ratelimits/<node_id>          → single node bucket state
    POST /v1/ratelimits/<node_id>/reset    → refill a node's bucket to capacity

Integration with ClusterScheduler::

    limiter   = ClusterRateLimiter(capacity=100, refill_rate=50)
    scheduler = ClusterScheduler(rate_limiter=limiter)

    # During pick(), rate-limited nodes are excluded from the eligible pool.
    # The scheduler tries successively lower-scoring nodes until it finds one
    # with tokens available, or returns None if all eligible nodes are empty.

Author : Rahad Bhuiya
Version: 4.1.0
License: MIT
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)



# TokenBucket


class TokenBucket:
    """Thread-safe token bucket for a single node."""

    def __init__(self, capacity: float, refill_rate: float) -> None:
        self.capacity     = capacity
        self.refill_rate  = refill_rate
        self._tokens      = capacity          # start full
        self._last_refill = time.monotonic()
        self._lock        = threading.Lock()
        self._consumed    = 0                 # lifetime consume count

    def consume(self, tokens: float = 1.0) -> bool:
        """
        Try to consume ``tokens`` from the bucket.
        Returns True if successful, False if the bucket is empty.
        """
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens   -= tokens
                self._consumed += 1
                return True
            return False

    def fill(self) -> None:
        """Manually refill the bucket to capacity (operator reset)."""
        with self._lock:
            self._tokens      = self.capacity
            self._last_refill = time.monotonic()

    def to_dict(self, node_id: str) -> Dict[str, Any]:
        with self._lock:
            self._refill()
            return {
                "node_id":       node_id,
                "capacity":      self.capacity,
                "refill_rate":   self.refill_rate,
                "tokens":        round(self._tokens, 3),
                "utilisation":   round(1.0 - self._tokens / self.capacity, 4),
                "rate_limited":  self._tokens < 1.0,
                "consumed_total": self._consumed,
            }

    
    # Internal
    

    def _refill(self) -> None:
        """Add tokens based on elapsed time.  Caller must hold self._lock."""
        now     = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(
            self.capacity,
            self._tokens + elapsed * self.refill_rate,
        )
        self._last_refill = now



# ClusterRateLimiter


class ClusterRateLimiter:
    """
    Per-node token-bucket rate limiter for HuddleCluster.

    Attach to a MasterNode and pass to ClusterScheduler::

        from huddle_cluster_pkg import MasterNode
        from huddle_cluster_pkg.cluster_rate_limiter import ClusterRateLimiter
        from huddle_cluster_pkg.cluster_scheduler    import ClusterScheduler

        limiter   = ClusterRateLimiter(
            capacity=100,        # max burst per node
            refill_rate=50.0,    # tokens added per second
            on_rate_limited=lambda nid: print(f"{nid} rate-limited"),
        )
        scheduler = ClusterScheduler(rate_limiter=limiter)
        master    = MasterNode(
            port=7070,
            scheduler=scheduler,
            rate_limiter=limiter,
        )
        master.start()

    The rate limiter creates one bucket per node, lazily, on first use.
    Buckets for nodes that have left the cluster are removed automatically
    on the next REST summary call.

    REST::

        GET  /v1/ratelimits                  → all node bucket states
        GET  /v1/ratelimits/<node_id>        → single node
        POST /v1/ratelimits/<node_id>/reset  → refill to capacity
    """

    def __init__(
        self,
        capacity: float = 100.0,
        refill_rate: float = 50.0,
        on_rate_limited: Optional[Any] = None,
    ) -> None:
        """
        Args:
            capacity:        Maximum tokens per node (burst ceiling).
            refill_rate:     Tokens added per second (sustain throughput).
            on_rate_limited: Optional callback(node_id) fired the first
                             time a node's bucket empties in a given
                             selection cycle.
        """
        if capacity <= 0:
            raise ValueError("capacity must be > 0")
        if refill_rate <= 0:
            raise ValueError("refill_rate must be > 0")

        self.capacity       = capacity
        self.refill_rate    = refill_rate
        self._on_rate_limited = on_rate_limited

        self._lock:    threading.RLock = threading.RLock()
        self._buckets: Dict[str, TokenBucket] = {}
        self._master:  Optional[Any] = None

    
    # Lifecycle
    

    def attach(self, master: Any) -> None:
        """Called automatically by MasterNode.start()."""
        self._master = master
        logger.info(
            "ClusterRateLimiter started (capacity=%.0f, refill=%.0f/s)",
            self.capacity, self.refill_rate,
        )

    def stop(self) -> None:
        pass   # no background threads to stop

    
    # Core API
    

    def consume(self, node_id: str, tokens: float = 1.0) -> bool:
        """
        Try to consume ``tokens`` from ``node_id``'s bucket.
        Returns True if tokens were available, False if rate-limited.
        Creates the bucket lazily if this node hasn't been seen before.
        """
        bucket = self._get_or_create(node_id)
        ok     = bucket.consume(tokens)
        if not ok:
            logger.debug("RateLimiter: '%s' rate-limited", node_id)
            if self._on_rate_limited:
                try:
                    self._on_rate_limited(node_id)
                except Exception:
                    logger.exception("on_rate_limited callback raised")
        return ok

    def is_rate_limited(self, node_id: str) -> bool:
        """True if the node's bucket currently has fewer than 1 token."""
        bucket = self._get_or_create(node_id)
        with bucket._lock:
            bucket._refill()
            return bucket._tokens < 1.0

    def reset(self, node_id: str) -> bool:
        """
        Refill node_id's bucket to capacity (operator reset).
        Returns False if the node has no bucket yet.
        """
        with self._lock:
            bucket = self._buckets.get(node_id)
        if bucket is None:
            return False
        bucket.fill()
        logger.info("RateLimiter: '%s' bucket reset to capacity", node_id)
        return True

    def bucket_for(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Return the bucket state dict for a node, or None if unknown."""
        with self._lock:
            bucket = self._buckets.get(node_id)
        return bucket.to_dict(node_id) if bucket else None

    def all_buckets(self) -> List[Dict[str, Any]]:
        """All known bucket states, sorted by node_id."""
        with self._lock:
            items = list(self._buckets.items())
        return sorted(
            (b.to_dict(nid) for nid, b in items),
            key=lambda d: d["node_id"],
        )

    def summary(self) -> Dict[str, Any]:
        """Summary for REST endpoint and monitoring."""
        buckets      = self.all_buckets()
        rate_limited = sum(1 for b in buckets if b["rate_limited"])
        return {
            "capacity":          self.capacity,
            "refill_rate":       self.refill_rate,
            "total_nodes":       len(buckets),
            "rate_limited_nodes": rate_limited,
            "buckets":           buckets,
        }

    
    # Internal
    

    def _get_or_create(self, node_id: str) -> TokenBucket:
        with self._lock:
            if node_id not in self._buckets:
                self._buckets[node_id] = TokenBucket(
                    self.capacity, self.refill_rate
                )
            return self._buckets[node_id]