"""
HuddleCluster — CLI
====================
Entry point: ``huddle-cluster`` (registered in pyproject.toml).

Commands
--------

    huddle-cluster master start  [--host HOST] [--port PORT] [--timeout SEC]
                                 [--flap-window SEC] [--flap-threshold N]
                                 [--quarantine-recovery N] [--purge-after SEC]
                                 [--api-key KEY=ROLE ...]

    huddle-cluster agent  start  --id ID --master URL --port PORT
                                 [--address IP] [--interval SEC]
                                 [--retry N] [--meta key=val ...] [--api-key KEY]

    huddle-cluster nodes  list   [--master URL] [--api-key KEY]
                                 [--status alive,quarantined] [--limit N] [--offset N]
    huddle-cluster nodes  status NODE_ID [--master URL] [--api-key KEY]

    huddle-cluster cluster status [--master URL] [--api-key KEY]
    huddle-cluster cluster health [--master URL]
    huddle-cluster cluster metrics [--master URL] [--api-key KEY]
    huddle-cluster cluster openapi [--master URL]

Author : Rahad Bhuiya
Version: 2.6.0
License: MIT
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict

_API_V1 = "/v1"
_DEFAULT_MASTER = "http://localhost:7070"



# HTTP helpers


def _build_get_request(master_url: str, path: str, api_key: Optional[str]) -> urllib.request.Request:
    url = f"{master_url.rstrip('/')}{path}"
    req = urllib.request.Request(url)
    if api_key:
        req.add_header("Authorization", f"Bearer {api_key}")
    return req


def _report_fetch_error(master_url: str, exc: Exception) -> None:
    if isinstance(exc, urllib.error.HTTPError):
        if exc.code == 401:
            print("\n[error] Authentication required — pass --api-key, "
                  "or the key was rejected")
        elif exc.code == 403:
            print("\n[error] This API key doesn't have permission for this "
                  "request (viewer keys can't modify the cluster)")
        else:
            try:
                body = json.loads(exc.read())
                print(f"\n[error] {body.get('error', exc.reason)}")
            except Exception:
                print(f"\n[error] HTTP {exc.code}: {exc.reason}")
    elif isinstance(exc, urllib.error.URLError):
        print(f"\n[error] Cannot reach master at {master_url}")
        print(f"        {exc.reason}")
        print("        Is the master running?  huddle-cluster master start")
    else:
        print(f"\n[error] {exc}", file=sys.stderr)
    sys.exit(1)


def _get(master_url: str, path: str, api_key: Optional[str] = None) -> Dict[str, Any]:
    req = _build_get_request(master_url, path, api_key)
    try:
        with urllib.request.urlopen(req, timeout=5.0) as resp:
            return json.loads(resp.read())
    except Exception as exc:
        _report_fetch_error(master_url, exc)


def _get_text(master_url: str, path: str, api_key: Optional[str] = None) -> str:
    req = _build_get_request(master_url, path, api_key)
    try:
        with urllib.request.urlopen(req, timeout=5.0) as resp:
            return resp.read().decode()
    except Exception as exc:
        _report_fetch_error(master_url, exc)


def _print_json(data: Dict) -> None:
    print(json.dumps(data, indent=2))



# Command handlers


def cmd_master_start(args: argparse.Namespace) -> None:
    """Start a MasterNode (blocking until Ctrl-C)."""
    import logging
    from huddle_cluster_pkg.cluster_master import MasterNode

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s [MASTER] %(message)s",
        datefmt="%H:%M:%S",
    )

    api_keys: Optional[Dict[str, str]] = None
    if args.api_key:
        api_keys = {}
        for item in args.api_key:
            if "=" not in item:
                print(f"[warn] ignoring malformed --api-key entry: {item!r} "
                      f"(expected KEY=ROLE, e.g. --api-key secret123=admin)")
                continue
            k, role = item.split("=", 1)
            api_keys[k.strip()] = role.strip()

    master = MasterNode(
        host=args.host,
        port=args.port,
        heartbeat_timeout_sec=args.timeout,
        flap_window_sec=args.flap_window,
        flap_threshold=args.flap_threshold,
        quarantine_recovery_heartbeats=args.quarantine_recovery,
        purge_after_sec=args.purge_after,
        api_keys=api_keys,
    )

    def on_join(node):
        print(f"  ✓  JOIN        {node.node_id:<20} {node.address}:{node.port}")

    def on_leave(node):
        print(f"  ←  LEAVE       {node.node_id}")

    def on_dead(node):
        ago = f"{node.last_seen_ago:.0f}s ago"
        print(f"  ✗  DEAD        {node.node_id:<20} last seen {ago}")

    def on_quarantined(node):
        print(f"  ⚠  QUARANTINE  {node.node_id:<20} {node.death_count} deaths recorded")

    def on_purged(node):
        print(f"  🗑  PURGED      {node.node_id}")

    master._on_join        = on_join
    master._on_leave       = on_leave
    master._on_dead        = on_dead
    master._on_quarantined = on_quarantined
    master._on_purged      = on_purged
    master.start()

    print(f"\nHuddleCluster Master")
    print(f"  Listening : {args.host}:{args.port}")
    print(f"  HB timeout: {args.timeout}s")
    print(f"  Flap rule : quarantine after {args.flap_threshold} deaths / "
          f"{args.flap_window:.0f}s, recover after {args.quarantine_recovery} heartbeats")
    if args.purge_after:
        print(f"  Purge     : dead nodes removed after {args.purge_after:.0f}s")
    print(f"  Auth      : {'enabled (' + str(len(api_keys)) + ' key(s))' if api_keys else 'disabled (open API)'}")
    print(f"  API prefix: http://{args.host}:{args.port}{_API_V1}/")
    _dash_host = "localhost" if args.host in ("0.0.0.0", "::") else args.host
    print(f"  Dashboard : http://{_dash_host}:{args.port}/dashboard")
    print(f"  API docs  : http://{_dash_host}:{args.port}{_API_V1}/docs")
    print("\n  Press Ctrl-C to stop.\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down master...")
        master.stop()
        print("Master stopped.")


def cmd_agent_start(args: argparse.Namespace) -> None:
    """Start an AgentNode (blocking until Ctrl-C)."""
    import logging
    from huddle_cluster_pkg.cluster_agent import AgentNode

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s [AGENT] %(message)s",
        datefmt="%H:%M:%S",
    )

    meta: Dict[str, str] = {}
    for item in args.meta or []:
        if "=" not in item:
            print(f"[warn] ignoring malformed --meta entry: {item!r} (expected key=value)")
            continue
        k, v = item.split("=", 1)
        meta[k.strip()] = v.strip()

    agent = AgentNode(
        node_id=args.id,
        master_url=args.master,
        port=args.port,
        address=args.address or None,
        heartbeat_interval_sec=args.interval,
        metadata=meta,
        api_key=args.api_key,
    )
    agent.start(retry=args.retry)

    print(f"\nHuddleCluster Agent")
    print(f"  Node ID  : {args.id}")
    print(f"  Address  : {agent.address}:{args.port}")
    print(f"  Master   : {args.master}")
    print(f"  HB every : {args.interval}s")
    print(f"  Joined   : {agent.joined}")
    print("\n  Press Ctrl-C to stop.\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down agent...")
        agent.stop()
        print("Agent stopped.")


def cmd_nodes_list(args: argparse.Namespace) -> None:
    params = {}
    if args.status:
        params["status"] = args.status
    if args.limit is not None:
        params["limit"] = args.limit
    if args.offset:
        params["offset"] = args.offset

    path = f"{_API_V1}/nodes"
    if params:
        path += "?" + urllib.parse.urlencode(params)

    data  = _get(args.master, path, args.api_key)
    nodes = data.get("nodes", [])
    total = data.get("total")

    if not nodes:
        print("No nodes match." if params else "No nodes registered with the master.")
        return

    hdr = f"{'NODE ID':<22} {'ADDRESS':<22} {'STATUS':<12} {'HB':<8} LAST SEEN"
    print(hdr)
    print("─" * len(hdr))
    for n in nodes:
        last = f"{n.get('last_seen_ago_sec', 0):.1f}s ago"
        addr = f"{n['address']}:{n['port']}"
        hb   = str(n.get("heartbeat_count", 0))
        print(f"{n['node_id']:<22} {addr:<22} {n['status']:<12} {hb:<8} {last}")

    if total is not None and (args.limit is not None or args.offset):
        print(f"\nShowing {len(nodes)} of {total} matching node(s)")


def cmd_nodes_status(args: argparse.Namespace) -> None:
    data = _get(args.master, f"{_API_V1}/nodes/{args.node_id}", args.api_key)
    _print_json(data)


def cmd_cluster_status(args: argparse.Namespace) -> None:
    data = _get(args.master, f"{_API_V1}/status", args.api_key)

    print(f"\nHuddleCluster Status")
    print(f"  Master      : {data.get('master')}")
    print(f"  Uptime      : {data.get('uptime_sec')}s")
    print(f"  Total nodes : {data.get('total_nodes', 0)}")
    print(f"  Alive       : {data.get('alive_nodes', 0)}")
    print(f"  Quarantined : {data.get('quarantined_nodes', 0)}")
    print(f"  Dead        : {data.get('dead_nodes', 0)}")
    print(f"  HB timeout  : {data.get('heartbeat_timeout_sec')}s")


def cmd_cluster_health(args: argparse.Namespace) -> None:
    data   = _get(args.master, f"{_API_V1}/health")
    status = data.get("status", "unknown")
    ok     = status == "ok"
    symbol = "✓" if ok else "✗"
    print(f"{symbol}  Master health: {status}")
    if not ok:
        sys.exit(1)


def cmd_cluster_metrics(args: argparse.Namespace) -> None:
    text = _get_text(args.master, f"{_API_V1}/metrics", args.api_key)
    print(text, end="")


def cmd_cluster_openapi(args: argparse.Namespace) -> None:
    data = _get(args.master, f"{_API_V1}/openapi.json")   # never needs auth
    _print_json(data)



# Argument parser


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="huddle-cluster",
        description="HuddleCluster — cluster management CLI  (v2.0.0)",
    )
    groups = parser.add_subparsers(dest="group", metavar="COMMAND")
    groups.required = True

    # master
    master_p = groups.add_parser("master", help="Master node commands")
    master_s = master_p.add_subparsers(dest="action", metavar="ACTION")
    master_s.required = True

    ms = master_s.add_parser("start", help="Start a MasterNode (blocking)")
    ms.add_argument("--host",    default="0.0.0.0",
                    help="Bind host  (default: 0.0.0.0)")
    ms.add_argument("--port",    type=int, default=7070,
                    help="Listen port  (default: 7070)")
    ms.add_argument("--timeout", type=float, default=30.0,
                    help="Heartbeat timeout in seconds  (default: 30)")
    ms.add_argument("--flap-window", type=float, default=300.0,
                    help="Window in seconds for counting repeated deaths  (default: 300)")
    ms.add_argument("--flap-threshold", type=int, default=3,
                    help="Deaths within the window that trigger quarantine  (default: 3)")
    ms.add_argument("--quarantine-recovery", type=int, default=3,
                    help="Consecutive heartbeats needed to exit quarantine  (default: 3)")
    ms.add_argument("--purge-after", type=float, default=None,
                    help="Remove dead nodes from the registry after this many seconds "
                         "(default: never purge)")
    ms.add_argument("--api-key", action="append", metavar="KEY=ROLE",
                    help="Add an API key with a role (admin or viewer); repeatable, "
                         "e.g. --api-key secret123=admin --api-key view456=viewer. "
                         "If never given, the API is open (no auth).")
    ms.set_defaults(func=cmd_master_start)

    # agent
    agent_p = groups.add_parser("agent", help="Agent node commands")
    agent_s = agent_p.add_subparsers(dest="action", metavar="ACTION")
    agent_s.required = True

    ag = agent_s.add_parser("start", help="Start an AgentNode (blocking)")
    ag.add_argument("--id",       required=True,
                    help="Unique node identifier, e.g. web-01")
    ag.add_argument("--master",   required=True,
                    help="Master URL, e.g. http://192.168.1.10:7070")
    ag.add_argument("--port",     type=int, required=True,
                    help="This node's port")
    ag.add_argument("--address",  default=None,
                    help="This node's IP  (auto-detected if omitted)")
    ag.add_argument("--interval", type=float, default=10.0,
                    help="Heartbeat interval in seconds  (default: 10)")
    ag.add_argument("--retry",    type=int, default=5,
                    help="Join retry attempts  (default: 5)")
    ag.add_argument("--meta",     nargs="*", metavar="KEY=VAL",
                    help="Metadata key=value pairs, e.g. --meta region=us-east role=lb")
    ag.add_argument("--api-key",  default=None,
                    help="API key to authenticate with the master, if it requires auth")
    ag.set_defaults(func=cmd_agent_start)

    # nodes
    nodes_p = groups.add_parser("nodes", help="Node management commands")
    nodes_s = nodes_p.add_subparsers(dest="action", metavar="ACTION")
    nodes_s.required = True

    nl = nodes_s.add_parser("list", help="List all registered nodes")
    nl.add_argument("--master", default=_DEFAULT_MASTER,
                    help=f"Master URL  (default: {_DEFAULT_MASTER})")
    nl.add_argument("--api-key", default=None,
                    help="API key, if the master requires auth")
    nl.add_argument("--status", default=None,
                    help="Filter by status, comma-separated "
                         "(e.g. --status alive,quarantined)")
    nl.add_argument("--limit", type=int, default=None,
                    help="Max number of nodes to return")
    nl.add_argument("--offset", type=int, default=0,
                    help="Number of nodes to skip  (default: 0)")
    nl.set_defaults(func=cmd_nodes_list)

    ns = nodes_s.add_parser("status", help="Detailed status for one node")
    ns.add_argument("node_id", help="Node ID to inspect")
    ns.add_argument("--master", default=_DEFAULT_MASTER,
                    help=f"Master URL  (default: {_DEFAULT_MASTER})")
    ns.add_argument("--api-key", default=None,
                    help="API key, if the master requires auth")
    ns.set_defaults(func=cmd_nodes_status)

    # cluster
    cluster_p = groups.add_parser("cluster", help="Cluster-level commands")
    cluster_s = cluster_p.add_subparsers(dest="action", metavar="ACTION")
    cluster_s.required = True

    cs = cluster_s.add_parser("status", help="Show cluster status summary")
    cs.add_argument("--master", default=_DEFAULT_MASTER,
                    help=f"Master URL  (default: {_DEFAULT_MASTER})")
    cs.add_argument("--api-key", default=None,
                    help="API key, if the master requires auth")
    cs.set_defaults(func=cmd_cluster_status)

    ch = cluster_s.add_parser("health", help="Quick health check (exit 1 if not ok)")
    ch.add_argument("--master", default=_DEFAULT_MASTER,
                    help=f"Master URL  (default: {_DEFAULT_MASTER})")
    ch.set_defaults(func=cmd_cluster_health)

    cm = cluster_s.add_parser("metrics", help="Print Prometheus text exposition")
    cm.add_argument("--master", default=_DEFAULT_MASTER,
                    help=f"Master URL  (default: {_DEFAULT_MASTER})")
    cm.add_argument("--api-key", default=None,
                    help="API key, if the master requires auth")
    cm.set_defaults(func=cmd_cluster_metrics)

    co = cluster_s.add_parser("openapi", help="Print the OpenAPI 3.0 spec for the REST API")
    co.add_argument("--master", default=_DEFAULT_MASTER,
                    help=f"Master URL  (default: {_DEFAULT_MASTER})")
    co.set_defaults(func=cmd_cluster_openapi)

    return parser


def main() -> None:
    parser = build_parser()
    args   = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()