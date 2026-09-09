"""
HuddleCluster — Cluster Webhooks & Alerts
===========================================
Asynchronous, signed webhook dispatcher for HuddleCluster MasterNode.
Dispatches cluster lifecycle events (node join/leave/death, quarantine,
health ratio alerts, circuit breaker trips, auto-scaler decisions)
to external HTTP endpoints (Slack, Discord, PagerDuty, or custom receivers).

Features:
- Non-blocking delivery using a background worker thread and queue.
- HMAC-SHA256 request signing via `X-Huddle-Signature` header.
- Custom headers per subscription (e.g. `Authorization: Bearer ...`).
- Event filtering per subscription (or `*` for all events).
- Exponential backoff retry on network failures and HTTP 5xx responses.
- In-memory delivery history ring buffer queryable via REST API.
- Safe lifecycle management (`start()`, `stop()`, `attach(master)`).

Author : Rahad Bhuiya
License: MIT
"""

from __future__ import annotations

import collections
import hashlib
import hmac
import json
import logging
import queue
import threading
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)

# Built-in cluster event types
EVENT_ALL = "*"
EVENT_NODE_JOINED = "node.joined"
EVENT_NODE_LEFT = "node.left"
EVENT_NODE_DEAD = "node.dead"
EVENT_NODE_QUARANTINED = "node.quarantined"
EVENT_NODE_RECOVERED = "node.recovered"
EVENT_CLUSTER_UNHEALTHY = "cluster.unhealthy"
EVENT_CLUSTER_RECOVERED = "cluster.recovered"
EVENT_CIRCUIT_BREAKER_TRIPPED = "circuit_breaker.tripped"
EVENT_AUTOSCALER_SCALED = "autoscaler.scaled"
EVENT_WEBHOOK_TEST = "webhook.test"

VALID_EVENTS = frozenset({
    EVENT_ALL,
    EVENT_NODE_JOINED,
    EVENT_NODE_LEFT,
    EVENT_NODE_DEAD,
    EVENT_NODE_QUARANTINED,
    EVENT_NODE_RECOVERED,
    EVENT_CLUSTER_UNHEALTHY,
    EVENT_CLUSTER_RECOVERED,
    EVENT_CIRCUIT_BREAKER_TRIPPED,
    EVENT_AUTOSCALER_SCALED,
    EVENT_WEBHOOK_TEST,
})


@dataclass
class WebhookSubscription:
    webhook_id: str
    url: str
    events: List[str] = field(default_factory=lambda: [EVENT_ALL])
    secret: Optional[str] = None
    headers: Dict[str, str] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    active: bool = True

    def matches(self, event: str) -> bool:
        if not self.active:
            return False
        return EVENT_ALL in self.events or event in self.events

    def to_dict(self, mask_secret: bool = True) -> Dict[str, Any]:
        d = {
            "webhook_id": self.webhook_id,
            "url": self.url,
            "events": list(self.events),
            "headers": dict(self.headers),
            "created_at": self.created_at,
            "active": self.active,
        }
        if self.secret:
            d["secret"] = "********" if mask_secret else self.secret
        else:
            d["secret"] = None
        return d


@dataclass
class WebhookDelivery:
    delivery_id: str
    webhook_id: str
    url: str
    event: str
    status: str            # "success" | "failed"
    status_code: Optional[int]
    error: Optional[str]
    duration_ms: float
    attempts: int
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class ClusterWebhooks:
    """
    Cluster-wide webhook notification and alerting engine for MasterNode.
    """

    def __init__(
        self,
        webhooks: Optional[List[Dict[str, Any]]] = None,
        default_secret: Optional[str] = None,
        max_retries: int = 3,
        timeout_sec: float = 5.0,
        max_history: int = 100,
        cluster_id: str = "huddle-cluster",
    ) -> None:
        self._default_secret = default_secret
        self._max_retries = max(1, max_retries)
        self._timeout = timeout_sec
        self._max_history = max_history
        self._cluster_id = cluster_id

        self._lock = threading.Lock()
        self._subscriptions: Dict[str, WebhookSubscription] = {}
        self._history: collections.deque[WebhookDelivery] = collections.deque(
            maxlen=max_history
        )

        self._queue: queue.Queue = queue.Queue(maxsize=1000)
        self._running = False
        self._worker_thread: Optional[threading.Thread] = None
        self._master: Optional[Any] = None

        if webhooks:
            for wh in webhooks:
                if isinstance(wh, str):
                    self.register(url=wh)
                elif isinstance(wh, dict):
                    self.register(
                        url=wh["url"],
                        events=wh.get("events"),
                        secret=wh.get("secret", default_secret),
                        headers=wh.get("headers"),
                    )

    def attach(self, master: Any) -> None:
        """Attach to a running MasterNode and intercept cluster lifecycle events."""
        self._master = master
        self.start()

        # Wire callbacks into master
        orig_join = getattr(master, "_on_join", None)
        orig_leave = getattr(master, "_on_leave", None)
        orig_dead = getattr(master, "_on_dead", None)
        orig_quarantined = getattr(master, "_on_quarantined", None)
        orig_unhealthy = getattr(master, "_on_cluster_unhealthy", None)
        orig_recovered = getattr(master, "_on_cluster_recovered", None)

        def hooked_join(node: Any) -> None:
            if orig_join:
                orig_join(node)
            self.dispatch(EVENT_NODE_JOINED, {
                "node_id": getattr(node, "node_id", str(node)),
                "address": getattr(node, "address", ""),
                "port": getattr(node, "port", 0),
                "metadata": getattr(node, "metadata", {}),
            })

        def hooked_leave(node: Any) -> None:
            if orig_leave:
                orig_leave(node)
            self.dispatch(EVENT_NODE_LEFT, {
                "node_id": getattr(node, "node_id", str(node)),
                "address": getattr(node, "address", ""),
                "port": getattr(node, "port", 0),
            })

        def hooked_dead(node: Any) -> None:
            if orig_dead:
                orig_dead(node)
            self.dispatch(EVENT_NODE_DEAD, {
                "node_id": getattr(node, "node_id", str(node)),
                "address": getattr(node, "address", ""),
                "port": getattr(node, "port", 0),
                "death_count": getattr(node, "death_count", 0),
            })

        def hooked_quarantined(node: Any) -> None:
            if orig_quarantined:
                orig_quarantined(node)
            self.dispatch(EVENT_NODE_QUARANTINED, {
                "node_id": getattr(node, "node_id", str(node)),
                "address": getattr(node, "address", ""),
                "port": getattr(node, "port", 0),
                "recent_deaths": getattr(node, "recent_deaths", []),
            })

        def hooked_unhealthy(summary: Dict[str, Any]) -> None:
            if orig_unhealthy:
                orig_unhealthy(summary)
            self.dispatch(EVENT_CLUSTER_UNHEALTHY, summary)

        def hooked_recovered(summary: Dict[str, Any]) -> None:
            if orig_recovered:
                orig_recovered(summary)
            self.dispatch(EVENT_CLUSTER_RECOVERED, summary)

        master._on_join = hooked_join
        master._on_leave = hooked_leave
        master._on_dead = hooked_dead
        master._on_quarantined = hooked_quarantined
        master._on_cluster_unhealthy = hooked_unhealthy
        master._on_cluster_recovered = hooked_recovered

    def start(self) -> None:
        """Start the background delivery worker thread."""
        with self._lock:
            if self._running:
                return
            self._running = True
            self._worker_thread = threading.Thread(
                target=self._worker_loop,
                name="huddle-cluster-webhooks",
                daemon=True,
            )
            self._worker_thread.start()
            logger.info("ClusterWebhooks worker started (%d subscriptions)", len(self._subscriptions))

    def stop(self) -> None:
        """Gracefully stop the delivery worker thread."""
        with self._lock:
            if not self._running:
                return
            self._running = False

        try:
            self._queue.put_nowait(None)
        except queue.Full:
            pass

        if self._worker_thread and self._worker_thread.is_alive():
            self._worker_thread.join(timeout=2.0)
        logger.info("ClusterWebhooks worker stopped")

    def register(
        self,
        url: str,
        events: Optional[List[str]] = None,
        secret: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> WebhookSubscription:
        """Register a new webhook subscription."""
        if not url or not (url.startswith("http://") or url.startswith("https://")):
            raise ValueError(f"Invalid webhook URL: {url!r}. Must start with http:// or https://")

        sub_events = list(events) if events else [EVENT_ALL]
        for ev in sub_events:
            if ev not in VALID_EVENTS:
                raise ValueError(f"Unknown webhook event: {ev!r}. Valid events: {sorted(VALID_EVENTS)}")

        wh_id = f"wh-{uuid.uuid4().hex[:8]}"
        sub = WebhookSubscription(
            webhook_id=wh_id,
            url=url,
            events=sub_events,
            secret=secret or self._default_secret,
            headers=dict(headers or {}),
        )

        with self._lock:
            self._subscriptions[wh_id] = sub
        logger.info("Registered webhook %s -> %s (events=%s)", wh_id, url, sub_events)
        return sub

    def unregister(self, webhook_id: str) -> bool:
        """Unregister a webhook subscription by ID."""
        with self._lock:
            removed = self._subscriptions.pop(webhook_id, None)
        if removed:
            logger.info("Unregistered webhook %s (%s)", webhook_id, removed.url)
            return True
        return False

    def list_subscriptions(self, mask_secret: bool = True) -> List[Dict[str, Any]]:
        """List all active webhook subscriptions."""
        with self._lock:
            return [s.to_dict(mask_secret=mask_secret) for s in self._subscriptions.values()]

    def get_subscription(self, webhook_id: str, mask_secret: bool = True) -> Optional[Dict[str, Any]]:
        """Get a single subscription by ID."""
        with self._lock:
            sub = self._subscriptions.get(webhook_id)
            return sub.to_dict(mask_secret=mask_secret) if sub else None

    def dispatch(self, event: str, data: Dict[str, Any]) -> int:
        """
        Enqueue an event to be sent to all matching subscriptions.
        Returns the count of matched subscriptions enqueued.
        """
        if not self._running:
            return 0

        with self._lock:
            matched = [s for s in self._subscriptions.values() if s.matches(event)]

        if not matched:
            return 0

        payload = {
            "event": event,
            "timestamp": time.time(),
            "cluster_id": self._cluster_id,
            "data": data,
        }

        enqueued = 0
        for sub in matched:
            try:
                self._queue.put_nowait((sub, event, payload))
                enqueued += 1
            except queue.Full:
                logger.warning("Webhook delivery queue full; dropping event %s for %s", event, sub.url)
        return enqueued

    def test_ping(self, target_id: Optional[str] = None) -> Dict[str, Any]:
        """Dispatch a synthetic test ping event."""
        with self._lock:
            if target_id:
                if target_id not in self._subscriptions:
                    return {"ok": False, "error": f"Webhook subscription '{target_id}' not found"}
                targets = [self._subscriptions[target_id]]
            else:
                targets = list(self._subscriptions.values())

        if not targets:
            return {"ok": False, "error": "No webhook subscriptions registered"}

        payload = {
            "event": EVENT_WEBHOOK_TEST,
            "timestamp": time.time(),
            "cluster_id": self._cluster_id,
            "data": {"message": "HuddleCluster synthetic test ping", "targets_count": len(targets)},
        }

        for sub in targets:
            try:
                self._queue.put_nowait((sub, EVENT_WEBHOOK_TEST, payload))
            except queue.Full:
                pass

        return {"ok": True, "dispatched_to": len(targets)}

    def deliveries(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Return the most recent delivery results."""
        with self._lock:
            items = list(self._history)
        items.reverse()  # most recent first
        return [d.to_dict() for d in items[:limit]]

    def _worker_loop(self) -> None:
        """Background thread worker draining delivery queue."""
        while self._running:
            try:
                item = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue

            if item is None:
                break

            sub, event, payload = item
            self._deliver(sub, event, payload)
            self._queue.task_done()

    def _deliver(self, sub: WebhookSubscription, event: str, payload_dict: Dict[str, Any]) -> None:
        """Execute HTTP POST with retry and record outcome."""
        delivery_id = f"del-{uuid.uuid4().hex[:8]}"
        payload_bytes = json.dumps(payload_dict, separators=(",", ":")).encode("utf-8")
        timestamp_str = str(payload_dict.get("timestamp", time.time()))

        headers = {
            "Content-Type": "application/json",
            "User-Agent": "HuddleCluster-WebhookDispatcher/4.16.0",
            "X-Huddle-Event": event,
            "X-Huddle-Delivery": delivery_id,
            "X-Huddle-Timestamp": timestamp_str,
            **sub.headers,
        }

        if sub.secret:
            sig = hmac.new(sub.secret.encode("utf-8"), payload_bytes, hashlib.sha256).hexdigest()
            headers["X-Huddle-Signature"] = f"sha256={sig}"

        start_time = time.time()
        last_status_code: Optional[int] = None
        last_error: Optional[str] = None
        success = False
        attempts = 0

        for attempt in range(1, self._max_retries + 1):
            attempts = attempt
            try:
                req = urllib.request.Request(
                    sub.url,
                    data=payload_bytes,
                    headers=headers,
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                    last_status_code = resp.status
                    if 200 <= resp.status < 300:
                        success = True
                        break
                    else:
                        last_error = f"HTTP status {resp.status}"
            except urllib.error.HTTPError as err:
                last_status_code = err.code
                last_error = f"HTTP {err.code}: {err.reason}"
                # 4xx errors (except 429) should generally not retry
                if 400 <= err.code < 500 and err.code != 429:
                    break
            except Exception as err:
                last_error = str(err)

            if attempt < self._max_retries:
                backoff = 0.2 * (2 ** (attempt - 1))
                time.sleep(backoff)

        duration_ms = (time.time() - start_time) * 1000.0

        delivery = WebhookDelivery(
            delivery_id=delivery_id,
            webhook_id=sub.webhook_id,
            url=sub.url,
            event=event,
            status="success" if success else "failed",
            status_code=last_status_code,
            error=None if success else last_error,
            duration_ms=round(duration_ms, 2),
            attempts=attempts,
            timestamp=time.time(),
        )

        with self._lock:
            self._history.append(delivery)

        if success:
            logger.debug("Webhook %s delivered to %s (event=%s, %d attempts)", sub.webhook_id, sub.url, event, attempts)
        else:
            logger.warning("Webhook %s delivery failed to %s (event=%s): %s", sub.webhook_id, sub.url, event, last_error)
