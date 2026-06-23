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
Version: 3.0.0
License: MIT
"""

from __future__ import annotations

import http.server
import json
import logging
import re
import threading
import time
import urllib.parse
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

# Scheduler is imported lazily to avoid a hard dependency — if the user
# never passes scheduler=... it's never imported at all.
_ClusterSchedulerType = None


def _get_scheduler_class():
    global _ClusterSchedulerType
    if _ClusterSchedulerType is None:
        from huddle_cluster_pkg.cluster_scheduler import ClusterScheduler
        _ClusterSchedulerType = ClusterScheduler
    return _ClusterSchedulerType

DEFAULT_MASTER_PORT: int = 7070
DEFAULT_HEARTBEAT_TIMEOUT: float = 30.0   # seconds until node is marked dead
DEFAULT_FLAP_WINDOW: float = 300.0        # seconds — window for counting repeated deaths
DEFAULT_FLAP_THRESHOLD: int = 3           # deaths within window that triggers quarantine
DEFAULT_QUARANTINE_RECOVERY: int = 3      # consecutive heartbeats needed to exit quarantine
_API_V1 = "/v1"
_API_VERSION = "1.0.0"          # REST API contract version, reported in /v1/status
_VALID_NODE_STATUSES = {"alive", "dead", "quarantined", "leaving"}

# RBAC: higher rank can do everything a lower rank can. Unrecognized role
# strings rank 0 (no access) so a typo'd role fails closed, not open.
_ROLE_RANK: Dict[str, int] = {"viewer": 1, "admin": 2}


def _role_satisfies(role: str, required: str) -> bool:
    return _ROLE_RANK.get(role, 0) >= _ROLE_RANK.get(required, 99)



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
        GET  /v1/openapi.json                 → OpenAPI 3.0 spec for this API
        GET  /v1/docs                         → interactive Swagger UI
        GET  /v1/status                       → cluster summary
        GET  /v1/metrics                      → Prometheus text exposition
        GET  /v1/nodes  [?status=][&limit=][&offset=]
                                               → paginated, filterable node list
        GET  /v1/nodes/<id>                   → single node record
        POST /v1/nodes/join                   → register a new agent
        POST /v1/nodes/<id>/heartbeat         → agent heartbeat
        DELETE /v1/nodes/<id>                 → agent graceful leave

    Scheduler (Level 3, optional — enabled by passing scheduler=ClusterScheduler()):
        GET  /v1/scheduler/next  [?affinity=] → pick the best node for a workload
        GET  /v1/scheduler/stats              → heat map and placement counts
        POST /v1/scheduler/report             → record workload completion

    Dashboard:
        GET /dashboard                        → cluster topology web UI
        (HTML page outside the /v1/ namespace; the page loads without auth,
        but its fetch() calls to /v1/status and /v1/nodes still respect
        api_keys if configured)

    Auto recovery:
        A node that dies and comes back too often within ``flap_window_sec``
        (>= ``flap_threshold`` deaths) is not trusted immediately — it is
        marked ``quarantined`` until it sends ``quarantine_recovery_heartbeats``
        consecutive heartbeats, then promoted back to ``alive`` with a clean
        slate. Nodes dead longer than ``purge_after_sec`` (if set) are removed
        from the registry entirely.

    Authentication / RBAC:
        Pass ``api_keys={"<key>": "admin"|"viewer", ...}`` to require a
        ``Authorization: Bearer <key>`` header on every request except
        ``GET /v1/health``. ``viewer`` keys may only use GET endpoints;
        ``admin`` keys may also join/heartbeat/leave. Omitting ``api_keys``
        (the default) leaves the API open, exactly as before this existed.
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
        unhealthy_alive_ratio: Optional[float] = None,
        api_keys: Optional[Dict[str, str]] = None,
        scheduler: Optional[Any] = None,
        on_node_join: Optional[Callable[[NodeRecord], None]] = None,
        on_node_leave: Optional[Callable[[NodeRecord], None]] = None,
        on_node_dead: Optional[Callable[[NodeRecord], None]] = None,
        on_node_quarantined: Optional[Callable[[NodeRecord], None]] = None,
        on_node_purged: Optional[Callable[[NodeRecord], None]] = None,
        on_cluster_unhealthy: Optional[Callable[[Dict[str, Any]], None]] = None,
        on_cluster_recovered: Optional[Callable[[Dict[str, Any]], None]] = None,
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

        self._unhealthy_alive_ratio = unhealthy_alive_ratio
        self._cluster_unhealthy = False

        self._api_keys = api_keys
        if api_keys:
            for key, role in api_keys.items():
                if role not in _ROLE_RANK:
                    logger.warning(
                        "API key ending in '...%s' has unrecognized role "
                        "'%s' (expected 'admin' or 'viewer') — it will be "
                        "treated as having no access",
                        key[-4:] if len(key) >= 4 else key, role,
                    )

        self._scheduler = scheduler

        self._nodes: Dict[str, NodeRecord] = {}
        self._lock = threading.RLock()

        self._on_join = on_node_join
        self._on_leave = on_node_leave
        self._on_dead = on_node_dead
        self._on_quarantined = on_node_quarantined
        self._on_purged = on_node_purged
        self._on_cluster_unhealthy = on_cluster_unhealthy
        self._on_cluster_recovered = on_cluster_recovered

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

    def nodes(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Snapshot of all registered nodes, sorted by node_id for stable
        pagination. If `status` is given, it may be a single status or a
        comma-separated list (e.g. "alive,quarantined") to filter by.
        """
        with self._lock:
            records = list(self._nodes.values())

        if status is not None:
            wanted = {s.strip() for s in status.split(",") if s.strip()}
            records = [n for n in records if n.status in wanted]

        records.sort(key=lambda n: n.node_id)
        return [n.to_dict() for n in records]

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
            "api_version": _API_VERSION,
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
            "cluster_unhealthy": self._cluster_unhealthy,
            "unhealthy_alive_ratio": self._unhealthy_alive_ratio,
            "scheduler": "enabled" if self._scheduler is not None else "disabled",
        }

    def prometheus_metrics(self) -> str:
        """
        Prometheus text exposition format for a cluster-wide /v1/metrics
        endpoint. Aggregates master-level counts plus, for each node, the
        metrics it last forwarded via heartbeat (e.g. from an attached
        HuddleCluster's own health_report()). Nodes that haven't forwarded
        a given field simply don't get a line for it — Prometheus treats
        missing label combinations as absent, not zero.
        """
        with self._lock:
            nodes = list(self._nodes.values())
            total       = len(nodes)
            alive       = sum(1 for n in nodes if n.status == "alive")
            dead        = sum(1 for n in nodes if n.status == "dead")
            quarantined = sum(1 for n in nodes if n.status == "quarantined")
            uptime      = time.time() - (self._started_at or time.time())
            unhealthy   = self._cluster_unhealthy

        lines = [
            "# HELP huddle_master_uptime_seconds Seconds since the master started",
            "# TYPE huddle_master_uptime_seconds gauge",
            f"huddle_master_uptime_seconds {uptime:.1f}",
            "",
            "# HELP huddle_master_total_nodes Total nodes ever registered (any status)",
            "# TYPE huddle_master_total_nodes gauge",
            f"huddle_master_total_nodes {total}",
            "",
            "# HELP huddle_master_alive_nodes Nodes currently fully trusted",
            "# TYPE huddle_master_alive_nodes gauge",
            f"huddle_master_alive_nodes {alive}",
            "",
            "# HELP huddle_master_dead_nodes Nodes that missed their heartbeat deadline",
            "# TYPE huddle_master_dead_nodes gauge",
            f"huddle_master_dead_nodes {dead}",
            "",
            "# HELP huddle_master_quarantined_nodes Nodes flapping, not yet fully trusted",
            "# TYPE huddle_master_quarantined_nodes gauge",
            f"huddle_master_quarantined_nodes {quarantined}",
            "",
            "# HELP huddle_master_unhealthy 1 if alive-node ratio is below threshold, else 0",
            "# TYPE huddle_master_unhealthy gauge",
            f"huddle_master_unhealthy {1 if unhealthy else 0}",
        ]

        #  per-node gauges 

        lines += [
            "",
            "# HELP huddle_node_up Node status as a number: 1=alive, 0.5=quarantined, 0=dead",
            "# TYPE huddle_node_up gauge",
        ]
        status_value = {"alive": 1, "quarantined": 0.5, "dead": 0, "leaving": 0}
        for n in nodes:
            lines.append(
                f'huddle_node_up{{node_id="{n.node_id}"}} '
                f"{status_value.get(n.status, 0)}"
            )

        lines += [
            "",
            "# HELP huddle_node_heartbeat_count Total heartbeats received from this node",
            "# TYPE huddle_node_heartbeat_count counter",
        ]
        for n in nodes:
            lines.append(f'huddle_node_heartbeat_count{{node_id="{n.node_id}"}} {n.heartbeat_count}')

        lines += [
            "",
            "# HELP huddle_node_death_count Total times this node has been marked dead",
            "# TYPE huddle_node_death_count counter",
        ]
        for n in nodes:
            lines.append(f'huddle_node_death_count{{node_id="{n.node_id}"}} {n.death_count}')

        lines += [
            "",
            "# HELP huddle_node_last_seen_seconds Seconds since the last heartbeat",
            "# TYPE huddle_node_last_seen_seconds gauge",
        ]
        for n in nodes:
            lines.append(
                f'huddle_node_last_seen_seconds{{node_id="{n.node_id}"}} {n.last_seen_ago:.1f}'
            )

        # ---- forwarded per-node metrics (optional — only if present) ---

        forwarded = {
            "fairness_score":   ("huddle_node_fairness_score",
                                 "Per-node load-balancing fairness score (0-1)", "gauge"),
            "inner_servers":    ("huddle_node_inner_servers",
                                 "Backend servers currently in the inner (active) ring", "gauge"),
            "outer_servers":    ("huddle_node_outer_servers",
                                 "Backend servers currently in the outer (cooled-down) ring", "gauge"),
            "rotation_count":   ("huddle_node_rotation_count",
                                 "Total inner/outer ring rotations on this node", "counter"),
            "requests_per_sec": ("huddle_node_requests_per_sec",
                                 "Current request rate reported by this node", "gauge"),
        }
        for field, (metric_name, help_text, mtype) in forwarded.items():
            node_values = [
                (n.node_id, n.metrics[field]) for n in nodes
                if isinstance(n.metrics, dict) and field in n.metrics
            ]
            if not node_values:
                continue
            lines += [
                "",
                f"# HELP {metric_name} {help_text}",
                f"# TYPE {metric_name} {mtype}",
            ]
            for node_id, value in node_values:
                lines.append(f'{metric_name}{{node_id="{node_id}"}} {value}')

        return "\n".join(lines) + "\n"

    def dashboard_html(self) -> str:
        """
        Self-contained HTML/CSS/JS dashboard showing cluster-wide node
        topology. Served at GET /dashboard (outside the /v1/ API namespace).
        The page itself needs no auth to load — it's a static shell — but
        its JS fetch() calls to /v1/status and /v1/nodes will get 401/403
        from the browser if the master has api_keys configured, in which
        case the user enters a key in the page itself (stored in their own
        browser's localStorage, never sent anywhere but back to this master).
        """
        return r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>HuddleCluster — Dashboard</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
<style>
  :root {
    --bg: #0E1217;
    --panel: #161B24;
    --panel-border: #232A38;
    --text: #E7EAF0;
    --text-dim: #7C8698;
    --alive: #34D399;
    --quarantined: #FBBF24;
    --dead: #F87171;
    --accent: #5EEAD4;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0;
    background: var(--bg);
    color: var(--text);
    font-family: 'Space Grotesk', system-ui, sans-serif;
    min-height: 100vh;
  }
  .mono { font-family: 'JetBrains Mono', ui-monospace, monospace; }

  header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 20px 28px;
    border-bottom: 1px solid var(--panel-border);
    flex-wrap: wrap;
    gap: 12px;
  }
  .brand { display: flex; align-items: baseline; gap: 10px; }
  .brand h1 { font-size: 20px; font-weight: 700; margin: 0; letter-spacing: -0.01em; }
  .brand span { font-size: 13px; color: var(--text-dim); }

  .live {
    display: flex; align-items: center; gap: 8px;
    font-size: 12px; color: var(--text-dim); text-transform: uppercase;
    letter-spacing: 0.08em;
  }
  .pulse-dot {
    width: 8px; height: 8px; border-radius: 50%;
    background: var(--accent);
    box-shadow: 0 0 0 0 rgba(94,234,212,0.6);
    animation: pulse 2s infinite;
  }
  @keyframes pulse {
    0%   { box-shadow: 0 0 0 0 rgba(94,234,212,0.55); }
    70%  { box-shadow: 0 0 0 8px rgba(94,234,212,0); }
    100% { box-shadow: 0 0 0 0 rgba(94,234,212,0); }
  }
  @media (prefers-reduced-motion: reduce) {
    .pulse-dot { animation: none; }
  }

  .key-box { display: flex; align-items: center; gap: 8px; }
  .key-box input {
    background: var(--panel);
    border: 1px solid var(--panel-border);
    color: var(--text);
    border-radius: 6px;
    padding: 7px 10px;
    font-size: 12px;
    font-family: 'JetBrains Mono', monospace;
    width: 190px;
  }
  .key-box input:focus { outline: 2px solid var(--accent); outline-offset: 1px; }
  .key-box button {
    background: var(--panel);
    border: 1px solid var(--panel-border);
    color: var(--text-dim);
    border-radius: 6px;
    padding: 7px 12px;
    font-size: 12px;
    cursor: pointer;
    font-family: 'Space Grotesk', sans-serif;
  }
  .key-box button:hover { color: var(--text); border-color: var(--accent); }

  main { padding: 24px 28px 40px; max-width: 1180px; margin: 0 auto; }

  .auth-error {
    display: none;
    background: rgba(248,113,113,0.1);
    border: 1px solid rgba(248,113,113,0.35);
    color: var(--dead);
    border-radius: 8px;
    padding: 10px 14px;
    font-size: 13px;
    margin-bottom: 18px;
  }

  .cards { display: flex; gap: 14px; flex-wrap: wrap; margin-bottom: 22px; }
  .card {
    background: var(--panel);
    border: 1px solid var(--panel-border);
    border-left: 3px solid var(--accent);
    border-radius: 8px;
    padding: 14px 18px;
    min-width: 130px;
    flex: 1;
  }
  .card.alive       { border-left-color: var(--alive); }
  .card.quarantined { border-left-color: var(--quarantined); }
  .card.dead        { border-left-color: var(--dead); }
  .card .num { font-family: 'JetBrains Mono', monospace; font-size: 26px; font-weight: 600; }
  .card .lbl { font-size: 12px; color: var(--text-dim); margin-top: 2px; text-transform: uppercase; letter-spacing: 0.06em; }

  .health-pill {
    display: inline-flex; align-items: center; gap: 6px;
    padding: 5px 12px; border-radius: 100px;
    font-size: 12px; font-weight: 600; letter-spacing: 0.04em;
    margin-bottom: 18px;
    background: rgba(52,211,153,0.12); color: var(--alive);
    border: 1px solid rgba(52,211,153,0.3);
  }
  .health-pill.bad {
    background: rgba(248,113,113,0.12); color: var(--dead);
    border: 1px solid rgba(248,113,113,0.3);
  }

  .huddle-strip {
    display: flex; flex-wrap: wrap; gap: 6px;
    padding: 16px;
    background: var(--panel);
    border: 1px solid var(--panel-border);
    border-radius: 8px;
    margin-bottom: 22px;
    min-height: 20px;
  }
  .huddle-dot {
    width: 13px; height: 13px; border-radius: 50%;
    background: var(--alive);
    cursor: default;
  }
  .huddle-dot.quarantined { background: var(--quarantined); }
  .huddle-dot.dead { background: var(--dead); }

  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  thead th {
    text-align: left; font-size: 11px; text-transform: uppercase;
    letter-spacing: 0.06em; color: var(--text-dim);
    padding: 10px 14px; border-bottom: 1px solid var(--panel-border);
  }
  tbody td {
    padding: 12px 14px;
    border-bottom: 1px solid var(--panel-border);
  }
  tbody tr:hover { background: rgba(255,255,255,0.02); }
  .badge {
    display: inline-flex; align-items: center; gap: 6px;
    font-size: 11px; font-weight: 600; text-transform: uppercase;
    letter-spacing: 0.04em;
  }
  .badge::before {
    content: ''; width: 7px; height: 7px; border-radius: 50%;
    background: currentColor;
  }
  .badge.alive { color: var(--alive); }
  .badge.quarantined { color: var(--quarantined); }
  .badge.dead { color: var(--dead); }

  .empty {
    text-align: center; padding: 60px 20px; color: var(--text-dim);
    background: var(--panel); border: 1px solid var(--panel-border);
    border-radius: 8px;
  }
  .empty .cmd {
    display: inline-block; margin-top: 14px;
    background: var(--bg); border: 1px solid var(--panel-border);
    border-radius: 6px; padding: 10px 16px;
    font-family: 'JetBrains Mono', monospace; font-size: 12.5px;
    color: var(--accent);
  }
  footer {
    text-align: center; color: var(--text-dim); font-size: 11px;
    padding: 24px 0 8px;
  }
</style>
</head>
<body>

<header>
  <div class="brand">
    <h1>HuddleCluster</h1>
    <span>cluster dashboard</span>
  </div>
  <div style="display:flex; align-items:center; gap:20px;">
    <div class="live"><span class="pulse-dot"></span><span id="refresh-label">live</span></div>
    <div class="key-box">
      <input id="api-key" type="password" placeholder="API key (if required)" autocomplete="off"/>
      <button id="key-save">Save</button>
    </div>
  </div>
</header>

<main>
  <div class="auth-error" id="auth-error"></div>
  <div class="health-pill" id="health-pill">checking…</div>

  <div class="cards" id="cards"></div>

  <div class="huddle-strip" id="huddle-strip"></div>

  <div id="table-wrap">
    <table id="node-table" style="display:none;">
      <thead>
        <tr>
          <th>Status</th><th>Node ID</th><th>Address</th>
          <th>Heartbeats</th><th>Last seen</th><th>Metrics</th>
        </tr>
      </thead>
      <tbody id="node-rows"></tbody>
    </table>
    <div class="empty" id="empty-state" style="display:none;">
      No nodes registered with this master yet.
      <div class="cmd">huddle-cluster agent start --id node-1 --master http://&lt;this-host&gt;:&lt;port&gt; --port 8080</div>
    </div>
  </div>

  <footer>auto-refreshing every 3s</footer>
</main>

<script>
const KEY_STORAGE = 'huddlecluster_dashboard_api_key';
let apiKey = localStorage.getItem(KEY_STORAGE) || '';
document.getElementById('api-key').value = apiKey;

document.getElementById('key-save').addEventListener('click', () => {
  apiKey = document.getElementById('api-key').value.trim();
  localStorage.setItem(KEY_STORAGE, apiKey);
  refresh();
});

function authHeaders() {
  return apiKey ? { 'Authorization': 'Bearer ' + apiKey } : {};
}

function fmtAgo(sec) {
  if (sec < 60) return sec.toFixed(0) + 's ago';
  if (sec < 3600) return (sec/60).toFixed(0) + 'm ago';
  return (sec/3600).toFixed(1) + 'h ago';
}

function fmtMetrics(m) {
  if (!m || Object.keys(m).length === 0) return '—';
  return Object.entries(m).slice(0, 3)
    .map(([k, v]) => k + ': ' + (typeof v === 'number' ? v.toFixed(2).replace(/\.00$/, '') : v))
    .join('  ·  ');
}

const STATUS_RANK = { dead: 0, quarantined: 1, alive: 2, leaving: 3 };

async function refresh() {
  const errBox = document.getElementById('auth-error');
  try {
    const [statusRes, nodesRes] = await Promise.all([
      fetch('/v1/status', { headers: authHeaders() }),
      fetch('/v1/nodes',  { headers: authHeaders() }),
    ]);

    if (statusRes.status === 401 || statusRes.status === 403 ||
        nodesRes.status  === 401 || nodesRes.status  === 403) {
      errBox.style.display = 'block';
      errBox.textContent = statusRes.status === 401
        ? 'Missing or invalid API key — enter one above and click Save.'
        : 'This key doesn\'t have permission to view cluster data.';
      return;
    }
    errBox.style.display = 'none';

    const status = await statusRes.json();
    const nodesBody = await nodesRes.json();
    const nodes = nodesBody.nodes || [];

    renderHealth(status);
    renderCards(status);
    renderHuddle(nodes);
    renderTable(nodes);
  } catch (e) {
    errBox.style.display = 'block';
    errBox.textContent = 'Cannot reach the master — is it still running?';
  }
}

function renderHealth(status) {
  const pill = document.getElementById('health-pill');
  if (status.cluster_unhealthy) {
    pill.className = 'health-pill bad';
    pill.textContent = 'DEGRADED — below ' + Math.round((status.unhealthy_alive_ratio||0)*100) + '% alive';
  } else {
    pill.className = 'health-pill';
    pill.textContent = 'HEALTHY';
  }
}

function renderCards(status) {
  const cards = [
    ['total',       status.total_nodes,       ''],
    ['alive',       status.alive_nodes,       'alive'],
    ['quarantined', status.quarantined_nodes, 'quarantined'],
    ['dead',        status.dead_nodes,        'dead'],
  ];
  document.getElementById('cards').innerHTML = cards.map(([label, val, cls]) =>
    '<div class="card ' + cls + '"><div class="num">' + (val ?? 0) +
    '</div><div class="lbl">' + label + '</div></div>'
  ).join('');
}

function renderHuddle(nodes) {
  document.getElementById('huddle-strip').innerHTML = nodes.map(n =>
    '<div class="huddle-dot ' + n.status + '" title="' + n.node_id + ' — ' + n.status + '"></div>'
  ).join('');
}

function renderTable(nodes) {
  const table = document.getElementById('node-table');
  const empty = document.getElementById('empty-state');
  if (nodes.length === 0) {
    table.style.display = 'none';
    empty.style.display = 'block';
    return;
  }
  table.style.display = 'table';
  empty.style.display = 'none';

  const sorted = [...nodes].sort((a, b) => {
    const r = (STATUS_RANK[a.status] ?? 9) - (STATUS_RANK[b.status] ?? 9);
    return r !== 0 ? r : a.node_id.localeCompare(b.node_id);
  });

  document.getElementById('node-rows').innerHTML = sorted.map(n => `
    <tr>
      <td><span class="badge ${n.status}">${n.status}</span></td>
      <td class="mono">${n.node_id}</td>
      <td class="mono">${n.address}:${n.port}</td>
      <td class="mono">${n.heartbeat_count}</td>
      <td>${fmtAgo(n.last_seen_ago_sec ?? 0)}</td>
      <td class="mono">${fmtMetrics(n.metrics)}</td>
    </tr>
  `).join('');
}

refresh();
setInterval(refresh, 3000);
</script>
</body>
</html>"""

    def openapi_spec(self) -> Dict[str, Any]:
        """
        OpenAPI 3.0.3 specification describing the full REST API, served at
        GET /v1/openapi.json (no auth required, same as /v1/health — clients
        need the spec before they can know how auth even works).
        """
        node_record_schema = {
            "type": "object",
            "properties": {
                "node_id":       {"type": "string"},
                "address":       {"type": "string"},
                "port":          {"type": "integer"},
                "status":        {"type": "string", "enum": sorted(_VALID_NODE_STATUSES)},
                "metadata":      {"type": "object"},
                "joined_at":     {"type": "number"},
                "last_heartbeat":{"type": "number"},
                "last_seen_ago_sec": {"type": "number"},
                "heartbeat_count": {"type": "integer"},
                "death_count":   {"type": "integer"},
                "recent_deaths": {"type": "array", "items": {"type": "number"}},
                "consecutive_alive_heartbeats": {"type": "integer"},
                "metrics":       {"type": "object"},
            },
        }
        error_schema = {
            "type": "object",
            "properties": {
                "ok":    {"type": "boolean", "enum": [False]},
                "error": {"type": "string"},
            },
        }
        auth_responses = {
            "401": {"description": "Missing or invalid API key",
                     "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Error"}}}},
            "403": {"description": "Valid key, insufficient role",
                     "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Error"}}}},
        }

        return {
            "openapi": "3.0.3",
            "info": {
                "title": "HuddleCluster Master API",
                "version": _API_VERSION,
                "description": (
                    "REST API for the HuddleCluster MasterNode — the central "
                    "coordinator for a multi-node HuddleCluster deployment. "
                    "Does not route traffic itself; tracks node enrollment, "
                    "heartbeats, and cluster-wide health."
                ),
            },
            "servers": [
                {"url": f"http://{self._host}:{self._port}{_API_V1}"}
            ],
            "security": [{"BearerAuth": []}],
            "components": {
                "securitySchemes": {
                    "BearerAuth": {
                        "type": "http",
                        "scheme": "bearer",
                        "bearerFormat": "API key",
                        "description": (
                            "Required on every endpoint except /health and "
                            "/openapi.json, when the master is configured "
                            "with api_keys. Omit entirely if the master has "
                            "no api_keys configured (open API)."
                        ),
                    },
                },
                "schemas": {
                    "NodeRecord": node_record_schema,
                    "Error": error_schema,
                },
            },
            "paths": {
                "/health": {
                    "get": {
                        "operationId": "getHealth",
                        "summary": "Liveness check (never requires auth)",
                        "security": [],
                        "responses": {
                            "200": {"description": "Master is up",
                                     "content": {"application/json": {"schema": {
                                         "type": "object",
                                         "properties": {"status": {"type": "string", "enum": ["ok"]}},
                                     }}}},
                        },
                    },
                },
                "/openapi.json": {
                    "get": {
                        "operationId": "getOpenApiSpec",
                        "summary": "This specification (never requires auth)",
                        "security": [],
                        "responses": {
                            "200": {"description": "OpenAPI 3.0 document"},
                        },
                    },
                },
                "/status": {
                    "get": {
                        "operationId": "getStatus",
                        "summary": "Cluster-wide summary counts and configuration",
                        "responses": {
                            "200": {"description": "Cluster status"},
                            **auth_responses,
                        },
                    },
                },
                "/metrics": {
                    "get": {
                        "operationId": "getMetrics",
                        "summary": "Prometheus text exposition for the whole cluster",
                        "responses": {
                            "200": {"description": "Prometheus metrics",
                                     "content": {"text/plain": {"schema": {"type": "string"}}}},
                            **auth_responses,
                        },
                    },
                },
                "/nodes": {
                    "get": {
                        "operationId": "listNodes",
                        "summary": "List registered nodes, with optional filtering and pagination",
                        "parameters": [
                            {"name": "status", "in": "query", "required": False,
                             "schema": {"type": "string"},
                             "description": "Comma-separated status filter, e.g. 'alive,quarantined'"},
                            {"name": "limit", "in": "query", "required": False,
                             "schema": {"type": "integer", "minimum": 0}},
                            {"name": "offset", "in": "query", "required": False,
                             "schema": {"type": "integer", "minimum": 0}},
                        ],
                        "responses": {
                            "200": {"description": "Paginated node list",
                                     "content": {"application/json": {"schema": {
                                         "type": "object",
                                         "properties": {
                                             "nodes": {"type": "array",
                                                       "items": {"$ref": "#/components/schemas/NodeRecord"}},
                                             "total": {"type": "integer"},
                                             "limit": {"type": ["integer", "null"]},
                                             "offset": {"type": "integer"},
                                         },
                                     }}}},
                            "400": {"description": "Invalid status/limit/offset value",
                                     "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Error"}}}},
                            **auth_responses,
                        },
                    },
                },
                "/nodes/{node_id}": {
                    "get": {
                        "operationId": "getNode",
                        "summary": "Single node record",
                        "parameters": [
                            {"name": "node_id", "in": "path", "required": True, "schema": {"type": "string"}},
                        ],
                        "responses": {
                            "200": {"description": "Node record",
                                     "content": {"application/json": {"schema": {"$ref": "#/components/schemas/NodeRecord"}}}},
                            "404": {"description": "No node with that ID",
                                     "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Error"}}}},
                            **auth_responses,
                        },
                    },
                    "delete": {
                        "operationId": "leaveNode",
                        "summary": "Graceful departure (requires admin role)",
                        "parameters": [
                            {"name": "node_id", "in": "path", "required": True, "schema": {"type": "string"}},
                        ],
                        "responses": {
                            "200": {"description": "Removed"},
                            "404": {"description": "No node with that ID"},
                            **auth_responses,
                        },
                    },
                },
                "/nodes/join": {
                    "post": {
                        "operationId": "joinNode",
                        "summary": "Register a new agent, or refresh an existing one (requires admin role)",
                        "requestBody": {
                            "required": True,
                            "content": {"application/json": {"schema": {
                                "type": "object",
                                "required": ["node_id", "address", "port"],
                                "properties": {
                                    "node_id": {"type": "string"},
                                    "address": {"type": "string"},
                                    "port": {"type": "integer"},
                                    "metadata": {"type": "object"},
                                },
                            }}}},
                        "responses": {
                            "200": {"description": "Joined or re-joined"},
                            "400": {"description": "Missing/invalid fields",
                                     "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Error"}}}},
                            **auth_responses,
                        },
                    },
                },
                "/nodes/{node_id}/heartbeat": {
                    "post": {
                        "operationId": "heartbeat",
                        "summary": "Send a heartbeat with optional forwarded metrics (requires admin role)",
                        "parameters": [
                            {"name": "node_id", "in": "path", "required": True, "schema": {"type": "string"}},
                        ],
                        "requestBody": {
                            "required": False,
                            "content": {"application/json": {"schema": {
                                "type": "object",
                                "properties": {"metrics": {"type": "object"}},
                            }}}},
                        "responses": {
                            "200": {"description": "Heartbeat recorded"},
                            "404": {"description": "Unknown node_id",
                                     "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Error"}}}},
                            **auth_responses,
                        },
                    },
                },
            },
        }

    def swagger_html(self) -> str:
        """
        Interactive API documentation at GET /v1/docs, rendered by the
        public Swagger UI CDN bundle against this master's own
        /v1/openapi.json. Needs no build step, same philosophy as
        dashboard_html(). Because the spec already declares a BearerAuth
        security scheme, Swagger UI renders its own "Authorize" button —
        paste an API key there and every "Try it out" call carries it
        automatically. The page itself never requires auth, same as
        /v1/openapi.json and /dashboard.
        """
        return r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>HuddleCluster — API Docs</title>
<link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css"/>
<style>
  :root { --bg: #0E1217; --panel: #161B24; --panel-border: #232A38; --text: #E7EAF0; --accent: #5EEAD4; }
  * { box-sizing: border-box; }
  body { margin: 0; background: var(--bg); font-family: system-ui, sans-serif; }
  header {
    display: flex; align-items: center; justify-content: space-between;
    padding: 18px 28px; border-bottom: 1px solid var(--panel-border);
    color: var(--text);
  }
  header h1 { font-size: 19px; margin: 0; font-weight: 700; letter-spacing: -0.01em; }
  header a { color: var(--accent); text-decoration: none; font-size: 13px; }
  header a:hover { text-decoration: underline; }
  #swagger-ui { background: #fff; }
  .swagger-ui .topbar { display: none; }
</style>
</head>
<body>
<header>
  <h1>HuddleCluster — API Docs</h1>
  <a href="/dashboard">&larr; back to dashboard</a>
</header>
<div id="swagger-ui"></div>
<script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
<script>
  window.onload = () => {
    SwaggerUIBundle({
      url: '/v1/openapi.json',
      dom_id: '#swagger-ui',
      presets: [SwaggerUIBundle.presets.apis],
      layout: 'BaseLayout',
    });
  };
</script>
</body>
</html>"""

    
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

            def _send_text(self, code: int, body: str, content_type: str) -> None:
                data = body.encode()
                self.send_response(code)
                self.send_header("Content-Type", content_type)
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

            def _check_auth(self, required_role: str) -> bool:
                """
                Returns True if the request is authorized to proceed.
                If not, sends the 401/403 response itself and returns False.
                When master._api_keys is None, auth is disabled entirely
                and this always returns True (open API, same as before
                RBAC existed).
                """
                if master._api_keys is None:
                    return True

                header = self.headers.get("Authorization", "")
                if not header.startswith("Bearer "):
                    self._send_json(401, {
                        "ok": False,
                        "error": "missing Authorization header (expected 'Bearer <api_key>')"
                    })
                    return False

                key  = header[len("Bearer "):].strip()
                role = master._api_keys.get(key)
                if role is None:
                    logger.warning("Rejected request: invalid API key")
                    self._send_json(401, {"ok": False, "error": "invalid API key"})
                    return False

                if not _role_satisfies(role, required_role):
                    logger.warning(
                        "Rejected request: role '%s' lacks '%s' permission",
                        role, required_role,
                    )
                    self._send_json(403, {
                        "ok": False,
                        "error": f"role '{role}' lacks required '{required_role}' permission"
                    })
                    return False

                return True

            #  GET 

            def do_GET(self) -> None:
                path = self.path.split("?")[0]

                if path == "/dashboard":
                    self._send_text(200, master.dashboard_html(),
                                     "text/html; charset=utf-8")

                elif path == f"{_API_V1}/health":
                    self._send_json(200, {"status": "ok"})   # never requires auth

                elif path == f"{_API_V1}/openapi.json":
                    self._send_json(200, master.openapi_spec())   # never requires auth

                elif path == f"{_API_V1}/docs":
                    self._send_text(200, master.swagger_html(),
                                     "text/html; charset=utf-8")   # never requires auth

                elif path == f"{_API_V1}/status":
                    if not self._check_auth("viewer"):
                        return
                    self._send_json(200, master.status())

                elif path == f"{_API_V1}/scheduler/next":
                    if not self._check_auth("viewer"):
                        return
                    if master._scheduler is None:
                        self._send_json(503, {
                            "ok": False,
                            "error": "scheduler is not enabled on this master",
                        })
                        return
                    qs = urllib.parse.parse_qs(
                        urllib.parse.urlsplit(self.path).query
                    )
                    affinity = qs.get("affinity", [None])[0]
                    node = master._scheduler.pick(master.nodes(), affinity_key=affinity)
                    if node is None:
                        self._send_json(503, {
                            "ok": False,
                            "error": "no eligible node available",
                        })
                    else:
                        self._send_json(200, {"ok": True, "node": node})

                elif path == f"{_API_V1}/scheduler/stats":
                    if not self._check_auth("viewer"):
                        return
                    if master._scheduler is None:
                        self._send_json(503, {
                            "ok": False,
                            "error": "scheduler is not enabled on this master",
                        })
                        return
                    self._send_json(200, master._scheduler.scheduler_stats())

                elif path == f"{_API_V1}/metrics":
                    if not self._check_auth("viewer"):
                        return
                    self._send_text(200, master.prometheus_metrics(),
                                     "text/plain; version=0.0.4; charset=utf-8")

                elif path == f"{_API_V1}/nodes":
                    if not self._check_auth("viewer"):
                        return

                    qs = urllib.parse.parse_qs(
                        urllib.parse.urlsplit(self.path).query
                    )
                    status_filter = qs.get("status", [None])[0]
                    if status_filter is not None:
                        requested = {s.strip() for s in status_filter.split(",") if s.strip()}
                        unknown = requested - _VALID_NODE_STATUSES
                        if unknown:
                            self._send_json(400, {
                                "ok": False,
                                "error": f"unknown status value(s): {', '.join(sorted(unknown))} "
                                         f"(valid: {', '.join(sorted(_VALID_NODE_STATUSES))})",
                            })
                            return

                    try:
                        limit  = int(qs["limit"][0])  if "limit"  in qs else None
                        offset = int(qs["offset"][0]) if "offset" in qs else 0
                    except (ValueError, IndexError):
                        self._send_json(400, {"ok": False,
                            "error": "limit and offset must be integers"})
                        return
                    if limit is not None and limit < 0:
                        self._send_json(400, {"ok": False, "error": "limit must be >= 0"})
                        return
                    if offset < 0:
                        self._send_json(400, {"ok": False, "error": "offset must be >= 0"})
                        return

                    all_nodes = master.nodes(status=status_filter)
                    total = len(all_nodes)
                    page = (all_nodes[offset:offset + limit]
                            if limit is not None else all_nodes[offset:])

                    self._send_json(200, {
                        "nodes": page,
                        "total": total,
                        "limit": limit,
                        "offset": offset,
                    })

                elif re.match(rf"{_API_V1}/nodes/([^/]+)$", path):
                    if not self._check_auth("viewer"):
                        return
                    node_id = path.rsplit("/", 1)[-1]
                    with master._lock:
                        node = master._nodes.get(node_id)
                    if node:
                        self._send_json(200, node.to_dict())
                    else:
                        self._send_json(404,
                            {"ok": False, "error": f"node '{node_id}' not found"})
                else:
                    self._send_json(404, {"ok": False, "error": "not found"})

            #  POST 

            def do_POST(self) -> None:
                path = self.path.split("?")[0]

                if path == f"{_API_V1}/nodes/join":
                    if not self._check_auth("admin"):
                        return
                    body = self._read_json()
                    if body is None:
                        self._send_json(400, {"ok": False, "error": "invalid JSON"})
                        return
                    result = master._handle_join(body)
                    self._send_json(200 if result.get("ok") else 400, result)

                elif re.match(rf"{_API_V1}/nodes/([^/]+)/heartbeat$", path):
                    if not self._check_auth("admin"):
                        return
                    body = self._read_json()
                    if body is None:
                        self._send_json(400, {"ok": False, "error": "invalid JSON"})
                        return
                    parts = path.rsplit("/", 2)
                    node_id = parts[-2]
                    result = master._handle_heartbeat(node_id, body)
                    self._send_json(200 if result.get("ok") else 404, result)

                elif path == f"{_API_V1}/scheduler/report":
                    if not self._check_auth("admin"):
                        return
                    if master._scheduler is None:
                        self._send_json(503, {
                            "ok": False,
                            "error": "scheduler is not enabled on this master",
                        })
                        return
                    body = self._read_json()
                    if body is None:
                        self._send_json(400, {"ok": False, "error": "invalid JSON"})
                        return
                    node_id = (body.get("node_id") or "").strip()
                    if not node_id:
                        self._send_json(400, {"ok": False,
                            "error": "node_id is required"})
                        return
                    master._scheduler.record_report(
                        node_id=node_id,
                        duration_ms=body.get("duration_ms"),
                        success=bool(body.get("success", True)),
                    )
                    self._send_json(200, {"ok": True, "recorded": node_id})

                else:
                    self._send_json(404, {"ok": False, "error": "not found"})

            #  DELETE 

            def do_DELETE(self) -> None:
                path = self.path.split("?")[0]
                m = re.match(rf"{_API_V1}/nodes/([^/]+)$", path)
                if m:
                    if not self._check_auth("admin"):
                        return
                    node_id = m.group(1)
                    result = master._handle_leave(node_id)
                    self._send_json(200 if result.get("ok") else 404, result)
                else:
                    self._send_json(404, {"ok": False, "error": "not found"})

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
                self._check_cluster_health()

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

    def _check_cluster_health(self) -> None:
        """
        Fire on_cluster_unhealthy / on_cluster_recovered based on the
        fraction of nodes currently alive, if unhealthy_alive_ratio is set.
        An empty cluster (no nodes registered yet) is never considered
        unhealthy — there's nothing to be unhealthy about.
        """
        if self._unhealthy_alive_ratio is None:
            return

        with self._lock:
            total = len(self._nodes)
            if total == 0:
                return
            alive = sum(1 for n in self._nodes.values() if n.status == "alive")

        ratio = alive / total
        is_unhealthy = ratio < self._unhealthy_alive_ratio

        fire_unhealthy = False
        fire_recovered = False
        if is_unhealthy and not self._cluster_unhealthy:
            self._cluster_unhealthy = True
            fire_unhealthy = True
        elif not is_unhealthy and self._cluster_unhealthy:
            self._cluster_unhealthy = False
            fire_recovered = True

        if not (fire_unhealthy or fire_recovered):
            return

        snapshot = {
            "total_nodes": total,
            "alive_nodes": alive,
            "alive_ratio": round(ratio, 3),
            "threshold": self._unhealthy_alive_ratio,
        }

        if fire_unhealthy:
            logger.warning(
                "Cluster health degraded: %.0f%% alive (%d/%d) — below "
                "threshold %.0f%%",
                ratio * 100, alive, total, self._unhealthy_alive_ratio * 100,
            )
            if self._on_cluster_unhealthy:
                try:
                    self._on_cluster_unhealthy(snapshot)
                except Exception:
                    logger.exception("on_cluster_unhealthy callback raised")

        if fire_recovered:
            logger.info(
                "Cluster health recovered: %.0f%% alive (%d/%d)",
                ratio * 100, alive, total,
            )
            if self._on_cluster_recovered:
                try:
                    self._on_cluster_recovered(snapshot)
                except Exception:
                    logger.exception("on_cluster_recovered callback raised")

    
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