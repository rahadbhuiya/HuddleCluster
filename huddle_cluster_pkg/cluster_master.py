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
Version: 2.0.0
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
_API_V1 = "/v1"



# NodeRecord


@dataclass
class NodeRecord:
    """One registered agent inside the master's node registry."""

    node_id: str
    address: str
    port: int
    metadata: Dict[str, Any] = field(default_factory=dict)
    status: str = "alive"          # alive | dead | leaving
    joined_at: float = field(default_factory=time.time)
    last_heartbeat: float = field(default_factory=time.time)
    heartbeat_count: int = 0
    metrics: Dict[str, Any] = field(default_factory=dict)

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
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = DEFAULT_MASTER_PORT,
        heartbeat_timeout_sec: float = DEFAULT_HEARTBEAT_TIMEOUT,
        on_node_join: Optional[Callable[[NodeRecord], None]] = None,
        on_node_leave: Optional[Callable[[NodeRecord], None]] = None,
        on_node_dead: Optional[Callable[[NodeRecord], None]] = None,
    ) -> None:
        self._host = host
        self._port = port
        self._timeout = heartbeat_timeout_sec

        self._nodes: Dict[str, NodeRecord] = {}
        self._lock = threading.RLock()

        self._on_join = on_node_join
        self._on_leave = on_node_leave
        self._on_dead = on_node_dead

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
        """Snapshot of nodes whose status is 'alive'."""
        with self._lock:
            return [n.to_dict() for n in self._nodes.values()
                    if n.status == "alive"]

    def node_count(self) -> int:
        with self._lock:
            return len(self._nodes)

    def status(self) -> Dict[str, Any]:
        """High-level cluster status suitable for the CLI and dashboards."""
        with self._lock:
            total = len(self._nodes)
            alive = sum(1 for n in self._nodes.values() if n.status == "alive")
            dead  = sum(1 for n in self._nodes.values() if n.status == "dead")
        return {
            "master": f"{self._host}:{self._port}",
            "uptime_sec": round(
                time.time() - (self._started_at or time.time()), 1
            ),
            "total_nodes": total,
            "alive_nodes": alive,
            "dead_nodes":  dead,
            "heartbeat_timeout_sec": self._timeout,
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
        check_every = max(1.0, self._timeout / 5)
        while self._running:
            time.sleep(check_every)
            if self._running:
                self._check_heartbeats()

    def _check_heartbeats(self) -> None:
        now = time.time()
        newly_dead: List[NodeRecord] = []
        with self._lock:
            for node in self._nodes.values():
                if (node.status == "alive"
                        and (now - node.last_heartbeat) > self._timeout):
                    node.status = "dead"
                    newly_dead.append(node)

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

        with self._lock:
            existing = self._nodes.get(node_id)
            if existing:
                # Allow re-join (e.g. agent restarted): refresh record.
                existing.address        = address
                existing.port           = port
                existing.metadata       = data.get("metadata") or {}
                existing.last_heartbeat = time.time()
                existing.status         = "alive"
                logger.info("Node '%s' re-joined from %s:%d", node_id, address, port)
                return {
                    "ok": True,
                    "action": "re-joined",
                    "node_id": node_id,
                    "heartbeat_timeout_sec": self._timeout,
                }

            record = NodeRecord(
                node_id  = node_id,
                address  = address,
                port     = port,
                metadata = data.get("metadata") or {},
            )
            self._nodes[node_id] = record

        logger.info("Node '%s' joined from %s:%d", node_id, address, port)
        if self._on_join:
            try:
                self._on_join(record)
            except Exception:
                logger.exception("on_node_join callback raised")

        return {
            "ok": True,
            "action": "joined",
            "node_id": node_id,
            "heartbeat_timeout_sec": self._timeout,
        }

    def _handle_heartbeat(self, node_id: str, data: Dict) -> Dict:
        with self._lock:
            node = self._nodes.get(node_id)
            if not node:
                return {"ok": False, "error": f"unknown node '{node_id}'"}
            now = time.time()
            node.last_heartbeat  = now
            node.heartbeat_count += 1
            node.metrics         = data.get("metrics") or {}
            recovered = node.status == "dead"
            node.status = "alive"

        if recovered:
            logger.info("Node '%s' recovered (heartbeat received)", node_id)

        return {"ok": True, "node_id": node_id,
                "heartbeat": node.heartbeat_count}

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