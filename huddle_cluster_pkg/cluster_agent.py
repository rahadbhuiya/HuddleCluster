"""
HuddleCluster — Agent Node
===========================
An AgentNode wraps an optional HuddleCluster instance and handles all
communication with the MasterNode:

  - Enrolls with the master on startup   (POST /v1/nodes/join)
  - Sends periodic heartbeats with metrics  (POST /v1/nodes/{id}/heartbeat)
  - Deregisters gracefully on shutdown   (DELETE /v1/nodes/{id})

Author : Rahad Bhuiya
Version: 2.0.0
License: MIT
"""

from __future__ import annotations

import json
import logging
import socket
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger(__name__)

DEFAULT_HEARTBEAT_INTERVAL: float = 10.0
DEFAULT_JOIN_RETRY: int = 5
DEFAULT_JOIN_BACKOFF: float = 2.0
DEFAULT_REQUEST_TIMEOUT: float = 3.0       # per-HTTP-call socket timeout (join/heartbeat/leave)
_API_V1 = "/v1"


class AgentNode:
    """
    Registers with a MasterNode and sends periodic heartbeats.

    Usage::

        from huddle_cluster_pkg import AgentNode

        agent = AgentNode(
            node_id="web-01",
            master_url="http://192.168.1.10:7070",
            port=8080,
        )
        agent.start()
        # ... use normally ...
        agent.stop()

    Pair with a HuddleCluster to forward live thermal metrics::

        from huddle_cluster import HuddleCluster, create_cluster
        from huddle_cluster_pkg import AgentNode

        cluster = create_cluster(["s1:8001", "s2:8002"])
        cluster.start()

        agent = AgentNode(
            node_id="lb-01",
            master_url="http://master:7070",
            port=8080,
            cluster=cluster,
        )
        agent.start()
    """

    def __init__(
        self,
        node_id: str,
        master_url: str,
        port: int,
        cluster: Any = None,
        address: Optional[str] = None,
        heartbeat_interval_sec: float = DEFAULT_HEARTBEAT_INTERVAL,
        request_timeout_sec: float = DEFAULT_REQUEST_TIMEOUT,
        metadata: Optional[Dict[str, Any]] = None,
        on_master_unreachable: Optional[Callable[[], None]] = None,
        on_recovered: Optional[Callable[[], None]] = None,
    ) -> None:
        if not node_id or not node_id.strip():
            raise ValueError("node_id must be a non-empty string")
        if not master_url:
            raise ValueError("master_url is required")
        if port < 1 or port > 65535:
            raise ValueError(f"port must be in range 1-65535, got {port}")

        self._node_id  = node_id.strip()
        self._master   = master_url.rstrip("/")
        self._port     = port
        self._cluster  = cluster
        self._address  = (address or "").strip() or self._detect_address()
        self._interval = heartbeat_interval_sec
        self._request_timeout = request_timeout_sec
        self._metadata = metadata or {}
        self._on_unreachable = on_master_unreachable
        self._on_recovered   = on_recovered

        self._running               = False
        self._joined                = False
        self._hb_count              = 0
        self._last_hb_ok: Optional[float] = None
        self._consecutive_failures  = 0
        self._hb_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

    
    # Properties
    

    @property
    def node_id(self) -> str:
        return self._node_id

    @property
    def joined(self) -> bool:
        return self._joined

    @property
    def heartbeat_count(self) -> int:
        return self._hb_count

    @property
    def address(self) -> str:
        return self._address

    
    # Public
    

    def start(self, retry: int = DEFAULT_JOIN_RETRY) -> None:
        """
        Start the agent. Returns immediately (does not block).

        The initial join (with retry/backoff) and the heartbeat loop both
        run inside the same background thread, so a slow or unreachable
        master never blocks the calling thread — including on platforms
        where a failed connection takes the full socket timeout to give
        up rather than failing instantly (observed on Windows when
        connecting to a port nothing is listening on).
        """
        if self._running:
            raise RuntimeError("AgentNode is already running")
        self._running = True
        self._stop_event.clear()        # allow start() -> stop() -> start() reuse
        self._hb_thread = threading.Thread(
            target=self._run,
            args=(retry,),
            name=f"agent-{self._node_id}",
            daemon=True,
        )
        self._hb_thread.start()
        logger.info(
            "AgentNode '%s' starting (master=%s, address=%s:%d)",
            self._node_id, self._master, self._address, self._port,
        )

    def _run(self, retry: int) -> None:
        """Background thread entry point: join (with retry), then heartbeat forever."""
        self._join_with_retry(retry)
        self._heartbeat_loop()

    def stop(self) -> None:
        """Send a leave request and stop the heartbeat thread."""
        if not self._running:
            return
        self._running = False
        self._stop_event.set()          # wake the sleeping heartbeat thread
        if self._joined:
            self._send_leave()
        if self._hb_thread:
            self._hb_thread.join(timeout=2.0)
        logger.info("AgentNode '%s' stopped", self._node_id)

    def status(self) -> Dict[str, Any]:
        """Return the current agent state as a dict."""
        return {
            "node_id":              self._node_id,
            "address":              self._address,
            "port":                 self._port,
            "master_url":           self._master,
            "joined":               self._joined,
            "running":              self._running,
            "heartbeat_count":      self._hb_count,
            "last_heartbeat_ok":    self._last_hb_ok,
            "interval_sec":         self._interval,
            "request_timeout_sec":  self._request_timeout,
            "consecutive_failures": self._consecutive_failures,
            "metadata":             self._metadata,
        }

    
    # Internal — join
    
    def _join_with_retry(self, max_retry: int) -> None:
        for attempt in range(1, max_retry + 1):
            if self._send_join():
                self._joined = True
                return
            if attempt < max_retry:
                wait = DEFAULT_JOIN_BACKOFF * attempt
                logger.warning(
                    "Join attempt %d/%d failed — retrying in %.1fs",
                    attempt, max_retry, wait,
                )
                if self._stop_event.wait(timeout=wait):
                    return    # stop() requested during backoff — exit quietly
        logger.error(
            "AgentNode '%s' failed to join master after %d attempts",
            self._node_id, max_retry,
        )

    def _send_join(self) -> bool:
        payload = {
            "node_id":  self._node_id,
            "address":  self._address,
            "port":     self._port,
            "metadata": self._metadata,
        }
        resp = self._post(f"{_API_V1}/nodes/join", payload)
        if resp and resp.get("ok"):
            action = resp.get("action", "joined")
            logger.info(
                "AgentNode '%s' %s master (%s)",
                self._node_id, action, self._master,
            )
            return True
        logger.warning("Join rejected by master: %s", resp)
        return False

    
    # Internal — heartbeat loop
    

    def _heartbeat_loop(self) -> None:
        while self._running:
            # wait() returns True if event was set (stop called), False on timeout
            if self._stop_event.wait(timeout=self._interval):
                break
            if not self._running:
                break
            self._send_heartbeat()

    def _send_heartbeat(self) -> bool:
        """
        Send one heartbeat to the master.

        State updates are done inside the lock; callbacks are fired
        OUTSIDE the lock so that any callback can safely call back into
        the agent without risking a deadlock.
        """
        metrics: Dict[str, Any] = {}
        if self._cluster is not None:
            try:
                report = self._cluster.health_report()
                metrics = {
                    "inner_servers":    report.get("inner_count", 0),
                    "outer_servers":    report.get("outer_count", 0),
                    "fairness_score":   report.get("fairness_score", 0.0),
                    "rotation_count":   report.get("rotation_count", 0),
                    "requests_per_sec": report.get("requests_per_sec", 0.0),
                }
            except Exception:
                logger.debug("Could not collect cluster metrics", exc_info=True)

        url  = f"{_API_V1}/nodes/{self._node_id}/heartbeat"
        resp = self._post(url, {"metrics": metrics})
        ok              = bool(resp and resp.get("ok"))
        master_reachable = resp is not None   # got any HTTP response at all

        # -- update state under lock; record what post-lock actions to take --
        fire_unreachable = False
        fire_recovered   = False
        should_rejoin    = False

        with self._lock:
            if ok:
                prev_failures            = self._consecutive_failures
                self._hb_count          += 1
                self._last_hb_ok         = time.time()
                self._consecutive_failures = 0
                if prev_failures > 0:
                    fire_recovered = True
            else:
                self._consecutive_failures += 1
                logger.warning(
                    "Heartbeat #%d failed for node '%s' (consecutive=%d)",
                    self._hb_count + 1,
                    self._node_id,
                    self._consecutive_failures,
                )
                if self._consecutive_failures == 1:
                    fire_unreachable = True
                # If master IS reachable but returned an error (e.g. it
                # restarted and lost our registration), re-join every 3
                # consecutive failures so the next heartbeat can succeed.
                if master_reachable and self._consecutive_failures % 3 == 0:
                    should_rejoin = True

        # -- fire actions OUTSIDE the lock --

        if should_rejoin:
            if self._send_join():
                self._joined = True

        if fire_recovered:
            logger.info("AgentNode '%s' reconnected to master", self._node_id)
            if self._on_recovered:
                try:
                    self._on_recovered()
                except Exception:
                    logger.exception("on_recovered callback raised")
            # Re-register so master re-adds us if it had dropped us.
            if not should_rejoin:
                self._send_join()

        if fire_unreachable and self._on_unreachable:
            try:
                self._on_unreachable()
            except Exception:
                logger.exception("on_master_unreachable callback raised")

        return ok

    
    # Internal — leave
    

    def _send_leave(self) -> None:
        url = f"{self._master}{_API_V1}/nodes/{self._node_id}"
        req = urllib.request.Request(url, method="DELETE")
        req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=self._request_timeout):
                pass
            logger.info("AgentNode '%s' deregistered from master", self._node_id)
        except Exception as exc:
            logger.warning("Leave request failed (master may be down): %s", exc)

    
    # Internal — HTTP helper
    

    def _post(self, path: str, payload: Dict) -> Optional[Dict]:
        """
        POST JSON to master.  Returns:
          - The parsed JSON body on any HTTP response (200 or 4xx/5xx).
          - None when the master is unreachable (connection refused, timeout, etc.).
        Distinguishing these two lets callers know whether the master is *up*
        even if it rejected the request.
        """
        url  = f"{self._master}{path}"
        data = json.dumps(payload).encode()
        req  = urllib.request.Request(url, data=data, method="POST")
        req.add_header("Content-Type",   "application/json")
        req.add_header("Content-Length", str(len(data)))
        try:
            with urllib.request.urlopen(req, timeout=self._request_timeout) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            # Master IS reachable but returned 4xx/5xx — read the JSON body.
            # HTTPError must be caught BEFORE URLError (it is a subclass of it).
            try:
                return json.loads(exc.read())
            except Exception:
                return {"ok": False, "error": str(exc)}
        except urllib.error.URLError as exc:
            # Connection refused, network timeout, DNS failure — master is DOWN.
            logger.debug("POST %s failed (master unreachable): %s", url, exc)
            return None
        except Exception as exc:
            logger.debug("POST %s unexpected error: %s", url, exc)
            return None

    
    # Internal — address detection
    

    @staticmethod
    def _detect_address() -> str:
        """Best-effort non-loopback local IP address."""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.settimeout(0.1)
            s.connect(("8.8.8.8", 80))
            addr = s.getsockname()[0]
            s.close()
            return addr
        except Exception:
            return "127.0.0.1"

    def __repr__(self) -> str:
        return (
            f"AgentNode(id={self._node_id!r}, "
            f"master={self._master!r}, "
            f"joined={self._joined}, "
            f"hb={self._hb_count})"
        )