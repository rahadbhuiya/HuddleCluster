"""
HuddleCluster — Observability
================================
Structured JSON logging and distributed trace IDs for a HuddleCluster
master. This is the fourth and final piece of Level 4 (Observability &
Control Plane), sitting alongside the circuit breaker, rate limiter, and
canary deployment manager.

What it does
------------
1. **Structured JSON logging** — ``configure_logging()`` swaps the target
   logger's handlers for one that emits single-line JSON records
   (``ts``, ``level``, ``logger``, ``service``, ``message``, plus
   ``trace_id`` / ``node_id`` / ``fields`` when present) instead of plain
   text. Safe to call multiple times — it's a no-op after the first call.
2. **Distributed trace IDs** — every inbound HTTP request to the master is
   assigned a trace ID: propagated from an ``X-Trace-Id`` request header if
   the caller already has one (e.g. an upstream load balancer or another
   HuddleCluster node), otherwise minted fresh. The trace ID is echoed back
   on the response (``X-Trace-Id``) and attached to every log line and
   event recorded while handling that request, via a thread-local context
   — so a single ``trace_id`` lets you grep one request's whole story out
   of the logs, across node joins, heartbeats, scheduler picks, and
   control-plane actions (canary/breaker/rate-limit).
3. **In-memory event ring buffer** — ``record_event()`` appends a
   structured event (independent of the logging handlers) that's queryable
   via ``GET /v1/observability/logs`` without needing a log aggregator.

REST endpoints (mounted when ``observability=ClusterObservability(...)``):

    GET /v1/observability/status   → config + counters + recent events
    GET /v1/observability/logs     → queryable event buffer
                                      (?limit=&trace_id=&event=&node_id=)

Every other JSON/text response from the master (regardless of which
endpoint) also gets an ``X-Trace-Id`` response header and a buffered
``http_request`` event when observability is attached.

Author : Rahad Bhuiya
Version: 4.3.0
License: MIT
"""

from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# Trace context (thread-local — one HTTP request is handled per thread
# in http.server's default threading model, so this is safe without
# needing to pass trace_id through every function signature).


class _TraceLocal(threading.local):
    trace_id: Optional[str] = None


_trace_local = _TraceLocal()


def new_trace_id() -> str:
    """Mint a fresh, short, URL-safe trace ID."""
    return uuid.uuid4().hex[:16]


def current_trace_id() -> Optional[str]:
    """The trace ID for whatever request/task is executing on this thread."""
    return getattr(_trace_local, "trace_id", None)


class _TraceFilter(logging.Filter):
    """Stamps every LogRecord with the current thread's trace ID, if any."""

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "trace_id"):
            record.trace_id = current_trace_id()
        return True


# Structured JSON log formatter


class JsonLogFormatter(logging.Formatter):
    """Formats LogRecords as single-line JSON."""

    def __init__(self, service_name: str = "huddle-cluster-master") -> None:
        super().__init__()
        self.service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "ts":      round(record.created, 3),
            "level":   record.levelname.lower(),
            "logger":  record.name,
            "service": self.service_name,
            "message": record.getMessage(),
        }
        trace_id = getattr(record, "trace_id", None)
        if trace_id:
            payload["trace_id"] = trace_id
        node_id = getattr(record, "node_id", None)
        if node_id:
            payload["node_id"] = node_id
        fields = getattr(record, "fields", None)
        if fields:
            payload["fields"] = fields
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


# Buffered event record


@dataclass
class LogEvent:
    """One structured event in the in-memory ring buffer."""

    event: str
    trace_id: Optional[str] = None
    node_id: Optional[str] = None
    level: str = "info"
    fields: Dict[str, Any] = field(default_factory=dict)
    ts: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "event":    self.event,
            "trace_id": self.trace_id,
            "node_id":  self.node_id,
            "level":    self.level,
            "ts":       round(self.ts, 3),
        }
        if self.fields:
            d["fields"] = self.fields
        return d


# ClusterObservability


class ClusterObservability:
    """
    Observability manager for HuddleCluster.

    Attach to a MasterNode::

        from huddle_cluster_pkg import MasterNode
        from huddle_cluster_pkg.cluster_observability import ClusterObservability

        obs    = ClusterObservability(service_name="huddle-cluster-prod")
        master = MasterNode(port=7070, observability=obs)
        master.start()

    Once attached, every request handled by the master's HTTP server gets
    a trace ID (propagated or minted), the process logger switches to JSON
    output, and recent events become queryable::

        curl http://localhost:7070/v1/observability/status
        curl http://localhost:7070/v1/observability/logs?limit=20
        curl -H "X-Trace-Id: my-existing-trace" http://localhost:7070/v1/status

    Other components (scheduler, canary, circuit breaker, ...) can also
    call ``record_event()`` directly to fold their own state changes into
    the same trace-correlated event stream.
    """

    def __init__(
        self,
        service_name: str = "huddle-cluster-master",
        json_logs: bool = True,
        log_level: int = logging.INFO,
        buffer_size: int = 500,
        logger_name: Optional[str] = None,
    ) -> None:
        """
        Args:
            service_name: Included as the ``service`` field on every JSON
                          log line and reported in status().
            json_logs:    If True (default), attaching this instance to a
                          MasterNode reconfigures logging to emit JSON.
                          Set False to keep buffered events/trace IDs
                          without touching the logging config.
            log_level:    Level applied to the reconfigured logger.
            buffer_size:  Max events kept in the in-memory ring buffer.
            logger_name:  Logger to reconfigure. None = root logger (so
                          every module's log lines become JSON). Pass a
                          specific name to scope the change narrowly.
        """
        if buffer_size <= 0:
            raise ValueError("buffer_size must be > 0")

        self.service_name = service_name
        self.json_logs    = json_logs
        self.log_level    = log_level
        self.buffer_size  = buffer_size
        self._logger_name = logger_name

        self._lock  = threading.RLock()
        self._events: List[LogEvent] = []
        self._request_count = 0
        self._master: Optional[Any] = None
        self._logging_configured = False

    
    # Lifecycle
    

    def attach(self, master: Any) -> None:
        self._master = master
        if self.json_logs:
            self.configure_logging(self._logger_name)
        logger.info(
            "ClusterObservability attached (service=%s json_logs=%s)",
            self.service_name, self.json_logs,
        )

    def stop(self) -> None:
        pass   # no background thread — nothing to tear down

    
    # Structured JSON logging
    

    def configure_logging(self, logger_name: Optional[str] = None) -> None:
        """
        Reconfigure a logger to emit single-line JSON. Idempotent: calling
        this more than once (e.g. from multiple ClusterObservability
        instances, or repeated attach() calls) only configures it once.
        """
        target = logging.getLogger(logger_name)
        if getattr(target, "_huddle_json_configured", False):
            return

        handler = logging.StreamHandler()
        handler.setFormatter(JsonLogFormatter(service_name=self.service_name))
        handler.addFilter(_TraceFilter())
        target.handlers = [handler]
        target.setLevel(self.log_level)
        target._huddle_json_configured = True   # type: ignore[attr-defined]

    
    # Trace context
    

    def start_trace(self, incoming_trace_id: Optional[str] = None) -> str:
        """
        Begin a trace on the current thread: reuse ``incoming_trace_id``
        if given and non-empty (propagation from an upstream caller),
        otherwise mint a new one. Returns the trace ID now active on this
        thread — callers should echo it back (e.g. as a response header).
        """
        trace_id = (
            incoming_trace_id.strip()
            if incoming_trace_id and incoming_trace_id.strip()
            else new_trace_id()
        )
        _trace_local.trace_id = trace_id
        return trace_id

    def end_trace(self) -> None:
        """Clear the current thread's trace context."""
        _trace_local.trace_id = None

    
    # Event recording
    

    def record_event(
        self,
        event: str,
        node_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        level: str = "info",
        **fields: Any,
    ) -> LogEvent:
        """
        Record a structured event: buffered for GET /v1/observability/logs
        *and* emitted through the logging pipeline (so it appears in JSON
        log output too, correlated by trace_id).

        Args:
            event:    Short event name, e.g. "http_request", "node_join".
            node_id:  Optional node this event concerns.
            trace_id: Explicit trace ID. Defaults to the current thread's
                      active trace (set via start_trace()).
            level:    debug/info/warning/error/critical.
            **fields: Arbitrary extra structured fields (e.g. status=200,
                      duration_ms=12.4).
        """
        trace_id = trace_id or current_trace_id()
        evt = LogEvent(
            event=event, trace_id=trace_id, node_id=node_id,
            level=level, fields=dict(fields),
        )

        with self._lock:
            self._events.append(evt)
            self._request_count += 1
            if len(self._events) > self.buffer_size:
                self._events = self._events[-self.buffer_size:]

        log_fn = getattr(logger, level, logger.info)
        extra: Dict[str, Any] = {"trace_id": trace_id}
        if node_id:
            extra["node_id"] = node_id
        if fields:
            extra["fields"] = fields
        try:
            log_fn(event, extra=extra)
        except Exception:
            logger.exception("failed to emit log line for event %r", event)
        return evt

    def events(
        self,
        limit: int = 50,
        trace_id: Optional[str] = None,
        event: Optional[str] = None,
        node_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Most recent buffered events, newest last, optionally filtered."""
        with self._lock:
            items = list(self._events)

        if trace_id:
            items = [e for e in items if e.trace_id == trace_id]
        if event:
            items = [e for e in items if e.event == event]
        if node_id:
            items = [e for e in items if e.node_id == node_id]
        if limit is not None and limit >= 0:
            items = items[-limit:] if limit else []
        return [e.to_dict() for e in items]

    def summary(self) -> Dict[str, Any]:
        """Full observability state for monitoring / REST."""
        with self._lock:
            events_recorded = self._request_count
            events_buffered = len(self._events)
            recent = [e.to_dict() for e in self._events[-10:]]
        return {
            "service_name":    self.service_name,
            "json_logs":       self.json_logs,
            "log_level":       logging.getLevelName(self.log_level),
            "buffer_size":     self.buffer_size,
            "events_recorded": events_recorded,
            "events_buffered": events_buffered,
            "recent_events":   recent,
        }