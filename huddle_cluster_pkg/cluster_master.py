"""
HuddleCluster — Master Node
============================
The MasterNode is the central coordinator for a multi-node HuddleCluster
deployment.  It does NOT route traffic itself; instead it:

  - Accepts node enrollment  (POST  /v1/nodes/join)
  - Receives periodic heartbeats (POST /v1/nodes/{id}/heartbeat)
  - Tracks nodes that stop heartbeating and marks them dead
  - Accepts graceful departures (DELETE /v1/nodes/{id})
  - Exposes a REST API consumed by the CLI and external tooling

Author : Rahad Bhuiya
Version: 2.1.0
License: MIT
"""

from __future__ import annotations

import http.server
import json
import logging
import re
import threading
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

DEFAULT_MASTER_PORT: int = 7070
DEFAULT_HEARTBEAT_TIMEOUT: float = 30.0   # seconds until node is marked dead
DEFAULT_FLAP_WINDOW: float = 300.0        # seconds — window for counting repeated deaths
DEFAULT_FLAP_THRESHOLD: int = 3           # deaths within window that triggers quarantine
DEFAULT_QUARANTINE_RECOVERY: int = 3      # consecutive heartbeats needed to exit quarantine
_API_V1 = "/v1"



# NodeRecord


@dataclass
class NodeRecord:
    """One registered agent inside the master's node registry."""

    node_id: str
    address: str
    port: int
    metadata: Dict[str, Any] = field(default_factory=dict)
    status: str = "alive"          # alive | dead | quarantined | leaving
    joined_at: float = field(default_factory=time.time)
    last_heartbeat: float = field(default_factory=time.time)
    heartbeat_count: int = 0
    metrics: Dict[str, Any] = field(default_factory=dict)
    death_count: int = 0
    recent_deaths: List[float] = field(default_factory=list)
    consecutive_alive_heartbeats: int = 0

    @property
    def url(self) -> str:
        return f"http://{self.address}:{self.port}"

    @property
    def last_seen_ago(self) -> float:
        return round(time.time() - self.last_heartbeat, 2)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["url"] = self.url
        d["last_seen_ago_sec"] = self.last_seen_ago
        return d



# MasterNode


class MasterNode:
    """
    Central coordinator for a HuddleCluster deployment.

    Usage::

        master = MasterNode(port=7070)
        master.start()       # non-blocking — starts HTTP + monitor threads
        # ...
        master.stop()

    REST API (all responses are JSON):

        GET  /v1/health                       → {"status": "ok"}
        GET  /v1/status                       → cluster summary
        GET  /v1/nodes                        → list of all nodes
        GET  /v1/nodes/<id>                   → single node record
        POST /v1/nodes/join                   → register a new agent
        POST /v1/nodes/<id>/heartbeat         → agent heartbeat
        DELETE /v1/nodes/<id>                 → agent graceful leave

    Auto recovery:
        A node that dies and comes back too often within ``flap_window_sec``
        (>= ``flap_threshold`` deaths) is not trusted immediately — it is
        marked ``quarantined`` until it sends ``quarantine_recovery_heartbeats``
        consecutive heartbeats, then promoted back to ``alive`` with a clean
        slate. Nodes dead longer than ``purge_after_sec`` (if set) are removed
        from the registry entirely.
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = DEFAULT_MASTER_PORT,
        heartbeat_timeout_sec: float = DEFAULT_HEARTBEAT_TIMEOUT,
        flap_window_sec: float = DEFAULT_FLAP_WINDOW,
        flap_threshold: int = DEFAULT_FLAP_THRESHOLD,
        quarantine_recovery_heartbeats: int = DEFAULT_QUARANTINE_RECOVERY,
        purge_after_sec: Optional[float] = None,
        on_node_join: Optional[Callable[[NodeRecord], None]] = None,
        on_node_leave: Optional[Callable[[NodeRecord], None]] = None,
        on_node_dead: Optional[Callable[[NodeRecord], None]] = None,
        on_node_quarantined: Optional[Callable[[NodeRecord], None]] = None,
        on_node_purged: Optional[Callable[[NodeRecord], None]] = None,
    ) -> None:
        self._host = host
        self._port = port
        self._timeout = heartbeat_timeout_sec
        self._flap_window = flap_window_sec
        self._flap_threshold = flap_threshold
        self._quarantine_recovery = quarantine_recovery_heartbeats
        self._purge_after = purge_after_sec
        if (purge_after_sec is not None
                and purge_after_sec <= heartbeat_timeout_sec):
            logger.warning(
                "purge_after_sec (%.1f) should be greater than "
                "heartbeat_timeout_sec (%.1f), otherwise nodes become "
                "purge-eligible the instant they die, leaving no grace "
                "period for quarantine/recovery",
                purge_after_sec, heartbeat_timeout_sec,
            )

        self._nodes: Dict[str, NodeRecord] = {}
        self._lock = threading.RLock()

        self._on_join = on_node_join
        self._on_leave = on_node_leave
        self._on_dead = on_node_dead
        self._on_quarantined = on_node_quarantined
        self._on_purged = on_node_purged

        self._http: Optional[http.server.HTTPServer] = None
        self._http_thread: Optional[threading.Thread] = None
        self._monitor_thread: Optional[threading.Thread] = None
        self._running = False
        self._started_at: Optional[float] = None

    
    # Public API
    

    @property
    def port(self) -> int:
        return self._port

    @property
    def host(self) -> str:
        return self._host

    @property
    def is_running(self) -> bool:
        return self._running

    def start(self) -> None:
        """Start the master node (non-blocking). Raises if already running."""
        if self._running:
            raise RuntimeError("MasterNode is already running")
        self._running = True
        self._started_at = time.time()
        self._start_http()
        self._start_monitor()
        logger.info("MasterNode started on %s:%d (timeout=%.0fs)",
                    self._host, self._port, self._timeout)

    def stop(self) -> None:
        """Gracefully shut down the master."""
        if not self._running:
            return
        self._running = False
        if self._http:
            self._http.shutdown()
        if self._http_thread:
            self._http_thread.join(timeout=1.0)
        if self._monitor_thread:
            self._monitor_thread.join(timeout=1.0)
        logger.info("MasterNode stopped")

    def nodes(self) -> List[Dict[str, Any]]:
        """Snapshot of all registered nodes (any status)."""
        with self._lock:
            return [n.to_dict() for n in self._nodes.values()]

    def alive_nodes(self) -> List[Dict[str, Any]]:
        """Snapshot of nodes whose status is 'alive' (fully trusted)."""
        with self._lock:
            return [n.to_dict() for n in self._nodes.values()
                    if n.status == "alive"]

    def quarantined_nodes(self) -> List[Dict[str, Any]]:
        """Snapshot of nodes currently quarantined (flapping, not yet fully trusted)."""
        with self._lock:
            return [n.to_dict() for n in self._nodes.values()
                    if n.status == "quarantined"]

    def node_count(self) -> int:
        with self._lock:
            return len(self._nodes)

    def status(self) -> Dict[str, Any]:
        """High-level cluster status suitable for the CLI and dashboards."""
        with self._lock:
            total       = len(self._nodes)
            alive       = sum(1 for n in self._nodes.values() if n.status == "alive")
            dead        = sum(1 for n in self._nodes.values() if n.status == "dead")
            quarantined = sum(1 for n in self._nodes.values() if n.status == "quarantined")
        return {
            "master": f"{self._host}:{self._port}",
            "uptime_sec": round(
                time.time() - (self._started_at or time.time()), 1
            ),
            "total_nodes": total,
            "alive_nodes": alive,
            "dead_nodes":  dead,
            "quarantined_nodes": quarantined,
            "heartbeat_timeout_sec": self._timeout,
            "flap_window_sec": self._flap_window,
            "flap_threshold": self._flap_threshold,
            "quarantine_recovery_heartbeats": self._quarantine_recovery,
            "purge_after_sec": self._purge_after,
        }

    
    # Internal — HTTP server
    

    def _start_http(self) -> None:
        master = self                            # captured in closure

        class _Handler(http.server.BaseHTTPRequestHandler):
            # Suppress the default "127.0.0.1 - - [date] GET ..." log line.
            def log_message(self, fmt, *args):   # type: ignore[override]
                logger.debug("HTTP %s", fmt % args)

            #  helpers 

            def _send_json(self, code: int, body: Any) -> None:
                data = json.dumps(body, indent=2).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def _read_json(self) -> Optional[Dict]:
                length = int(self.headers.get("Content-Length", 0))
                if not length:
                    return {}
                try:
                    return json.loads(self.rfile.read(length))
                except (json.JSONDecodeError, ValueError):
                    return None

            #  GET 

            def do_GET(self) -> None:
                path = self.path.split("?")[0]

                if path == f"{_API_V1}/health":
                    self._send_json(200, {"status": "ok"})

                elif path == f"{_API_V1}/status":
                    self._send_json(200, master.status())

                elif path == f"{_API_V1}/nodes":
                    self._send_json(200, {"nodes": master.nodes()})

                elif re.match(rf"{_API_V1}/nodes/([^/]+)$", path):
                    node_id = path.rsplit("/", 1)[-1]
                    with master._lock:
                        node = master._nodes.get(node_id)
                    if node:
                        self._send_json(200, node.to_dict())
                    else:
                        self._send_json(404,
                            {"error": f"node '{node_id}' not found"})
                else:
                    self._send_json(404, {"error": "not found"})

            #  POST 

            def do_POST(self) -> None:
                path = self.path.split("?")[0]
                body = self._read_json()
                if body is None:
                    self._send_json(400, {"error": "invalid JSON"})
                    return

                if path == f"{_API_V1}/nodes/join":
                    result = master._handle_join(body)
                    self._send_json(200 if result.get("ok") else 400, result)

                elif re.match(rf"{_API_V1}/nodes/([^/]+)/heartbeat$", path):
                    parts = path.rsplit("/", 2)
                    node_id = parts[-2]
                    result = master._handle_heartbeat(node_id, body)
                    self._send_json(200 if result.get("ok") else 404, result)

                else:
                    self._send_json(404, {"error": "not found"})

            #  DELETE 

            def do_DELETE(self) -> None:
                path = self.path.split("?")[0]
                m = re.match(rf"{_API_V1}/nodes/([^/]+)$", path)
                if m:
                    node_id = m.group(1)
                    result = master._handle_leave(node_id)
                    self._send_json(200 if result.get("ok") else 404, result)
                else:
                    self._send_json(404, {"error": "not found"})

        self._http = http.server.HTTPServer((self._host, self._port), _Handler)
        self._http.allow_reuse_address = True
        self._http_thread = threading.Thread(
            target=lambda: self._http.serve_forever(poll_interval=0.05),
            name="master-http",
            daemon=True,
        )
        self._http_thread.start()

    
    # Internal — heartbeat monitor
    

    def _start_monitor(self) -> None:
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop,
            name="master-monitor",
            daemon=True,
        )
        self._monitor_thread.start()

    def _monitor_loop(self) -> None:
        check_every = max(0.05, self._timeout / 5)
        while self._running:
            time.sleep(check_every)
            if self._running:
                self._check_heartbeats()

    def _check_heartbeats(self) -> None:
        now = time.time()
        newly_dead: List[NodeRecord] = []
        purged: List[NodeRecord] = []

        with self._lock:
            for node in list(self._nodes.values()):
                timed_out = (now - node.last_heartbeat) > self._timeout

                if node.status in ("alive", "quarantined") and timed_out:
                    node.status = "dead"
                    self._record_death(node, now)
                    newly_dead.append(node)

                elif (node.status == "dead"
                      and self._purge_after is not None
                      and (now - node.last_heartbeat) > self._purge_after):
                    purged.append(node)

            for node in purged:
                self._nodes.pop(node.node_id, None)

        for node in newly_dead:
            logger.warning(
                "Node '%s' heartbeat timed out (%.0fs) — marked dead",
                node.node_id, node.last_seen_ago,
            )
            if self._on_dead:
                try:
                    self._on_dead(node)
                except Exception:
                    logger.exception("on_node_dead callback raised")

        for node in purged:
            logger.info(
                "Node '%s' purged from registry (dead longer than %.0fs)",
                node.node_id, self._purge_after,
            )
            if self._on_purged:
                try:
                    self._on_purged(node)
                except Exception:
                    logger.exception("on_node_purged callback raised")

    def _record_death(self, node: NodeRecord, now: float) -> None:
        """
        Record a death event on the node for flap-window tracking.
        Caller must hold self._lock.
        """
        node.death_count += 1
        node.recent_deaths.append(now)
        cutoff = now - self._flap_window
        node.recent_deaths = [t for t in node.recent_deaths if t >= cutoff]
        node.consecutive_alive_heartbeats = 0

    
    # Internal — request handlers
    

    def _handle_join(self, data: Dict) -> Dict:
        node_id = (data.get("node_id") or "").strip()
        address  = (data.get("address")  or "").strip()
        port     = data.get("port")

        if not node_id:
            return {"ok": False, "error": "node_id is required"}
        if not address:
            return {"ok": False, "error": "address is required"}
        if not isinstance(port, int) or port < 1 or port > 65535:
            return {"ok": False, "error": "valid integer port (1-65535) is required"}

        fire_quarantined = False
        quarantined_node: Optional[NodeRecord] = None
        new_record: Optional[NodeRecord] = None

        with self._lock:
            existing = self._nodes.get(node_id)
            if existing:
                # Allow re-join (e.g. agent restarted): refresh record.
                existing.address        = address
                existing.port           = port
                existing.metadata       = data.get("metadata") or {}
                now = time.time()
                existing.last_heartbeat = now

                if existing.status == "dead":
                    cutoff = now - self._flap_window
                    existing.recent_deaths = [
                        t for t in existing.recent_deaths if t >= cutoff
                    ]
                    if len(existing.recent_deaths) >= self._flap_threshold:
                        existing.status = "quarantined"
                        existing.consecutive_alive_heartbeats = 1
                        fire_quarantined  = True
                        quarantined_node  = existing
                    else:
                        existing.status = "alive"
                elif existing.status == "leaving":
                    existing.status = "alive"
                # already "alive" or "quarantined" — leave as-is on a plain
                # rejoin; quarantine is only cleared via proven heartbeats

                action = "re-joined"
            else:
                new_record = NodeRecord(
                    node_id  = node_id,
                    address  = address,
                    port     = port,
                    metadata = data.get("metadata") or {},
                )
                self._nodes[node_id] = new_record
                action = "joined"

        if fire_quarantined:
            logger.warning(
                "Node '%s' is flapping (rejoined with >= %d deaths in last "
                "%.0fs) — quarantined",
                node_id, self._flap_threshold, self._flap_window,
            )
            if self._on_quarantined:
                try:
                    self._on_quarantined(quarantined_node)
                except Exception:
                    logger.exception("on_node_quarantined callback raised")

        if action == "joined":
            logger.info("Node '%s' joined from %s:%d", node_id, address, port)
            if self._on_join:
                try:
                    self._on_join(new_record)
                except Exception:
                    logger.exception("on_node_join callback raised")
        else:
            logger.info("Node '%s' re-joined from %s:%d", node_id, address, port)

        return {
            "ok": True,
            "action": action,
            "node_id": node_id,
            "heartbeat_timeout_sec": self._timeout,
        }

    def _handle_heartbeat(self, node_id: str, data: Dict) -> Dict:
        fire_quarantined = False
        quarantined_node: Optional[NodeRecord] = None

        with self._lock:
            node = self._nodes.get(node_id)
            if not node:
                return {"ok": False, "error": f"unknown node '{node_id}'"}

            now = time.time()
            node.last_heartbeat  = now
            node.heartbeat_count += 1
            node.metrics         = data.get("metrics") or {}

            if node.status == "dead":
                cutoff = now - self._flap_window
                node.recent_deaths = [t for t in node.recent_deaths if t >= cutoff]
                if len(node.recent_deaths) >= self._flap_threshold:
                    # Recovering, but has died too often recently — don't
                    # trust it immediately; require proven stability first.
                    node.status = "quarantined"
                    node.consecutive_alive_heartbeats = 1
                    fire_quarantined = True
                    quarantined_node = node
                else:
                    node.status = "alive"
                    logger.info("Node '%s' recovered (heartbeat received)", node_id)

            elif node.status == "quarantined":
                node.consecutive_alive_heartbeats += 1
                if node.consecutive_alive_heartbeats >= self._quarantine_recovery:
                    node.status = "alive"
                    node.recent_deaths = []      # clean slate after proven stability
                    logger.info(
                        "Node '%s' promoted out of quarantine after %d "
                        "consecutive heartbeats",
                        node_id, node.consecutive_alive_heartbeats,
                    )

            heartbeat_count = node.heartbeat_count

        if fire_quarantined:
            logger.warning(
                "Node '%s' is flapping (>= %d deaths in last %.0fs) — "
                "quarantined, needs %d consecutive heartbeats to recover full trust",
                node_id, self._flap_threshold, self._flap_window,
                self._quarantine_recovery,
            )
            if self._on_quarantined:
                try:
                    self._on_quarantined(quarantined_node)
                except Exception:
                    logger.exception("on_node_quarantined callback raised")

        return {"ok": True, "node_id": node_id, "heartbeat": heartbeat_count}

    def _handle_leave(self, node_id: str) -> Dict:
        with self._lock:
            node = self._nodes.get(node_id)
            if not node:
                return {"ok": False, "error": f"unknown node '{node_id}'"}
            node.status = "leaving"

        logger.info("Node '%s' is leaving the cluster", node_id)
        if self._on_leave:
            try:
                self._on_leave(node)
            except Exception:
                logger.exception("on_node_leave callback raised")

        with self._lock:
            self._nodes.pop(node_id, None)

        return {"ok": True, "node_id": node_id, "action": "left"}

    
    # Repr

    def __repr__(self) -> str:
        return (
            f"MasterNode(host={self._host!r}, port={self._port}, "
            f"nodes={len(self._nodes)}, running={self._running})"
        )