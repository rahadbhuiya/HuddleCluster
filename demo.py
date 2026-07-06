"""
HuddleCluster — Full System Demo
==================================
Starts a complete cluster with every Level 2, 3, and 4 feature enabled.
Run with:

    python demo.py

Then open these URLs in your browser:
    http://localhost:7070/dashboard      — real-time topology dashboard
    http://localhost:7070/v1/docs        — interactive Swagger UI
    http://localhost:7070/v1/status      — cluster JSON status
    http://localhost:7070/v1/metrics     — Prometheus metrics
    http://localhost:7070/v1/scheduler/stats
    http://localhost:7070/v1/autoscaler/status
    http://localhost:7070/v1/discovery/services
    http://localhost:7070/v1/regions
    http://localhost:7070/v1/breakers

Press Ctrl-C to stop everything.
"""

import signal
import sys
import time
import threading

#  Cluster imports 
from huddle_cluster_pkg import (
    MasterNode,
    AgentNode,
    ClusterScheduler,
    ClusterAutoScaler,
    ClusterRollingUpdater,
    ServiceDiscovery,
    ClusterHA,
    MultiRegionManager,
    ClusterCircuitBreaker,
)


# Colour helpers (work on Windows with ANSI support)

RESET  = "\033[0m"
BOLD   = "\033[1m"
GREEN  = "\033[32m"
YELLOW = "\033[33m"
CYAN   = "\033[36m"
RED    = "\033[31m"
BLUE   = "\033[34m"

def _c(colour, text):
    return f"{colour}{text}{RESET}"

def banner(text):
    width = 60
    line  = "─" * width
    print(f"\n{_c(BOLD+CYAN, line)}")
    print(f"{_c(BOLD+CYAN, text.center(width))}")
    print(f"{_c(BOLD+CYAN, line)}")

def info(label, value):
    print(f"  {_c(BOLD, label):<30} {_c(GREEN, str(value))}")

def event(msg):
    ts = time.strftime("%H:%M:%S")
    print(f"  {_c(YELLOW, ts)}  {msg}")


# Callbacks (all optional — just for demo output)


def on_node_join(node):
    event(f"NODE JOIN   {_c(GREEN, node.node_id)} ({node.address}:{node.port})")

def on_node_dead(node):
    event(f"NODE DEAD   {_c(RED, node.node_id)}")

def on_node_quarantined(node):
    event(f"QUARANTINE  {_c(YELLOW, node.node_id)}")

def on_cluster_unhealthy(s):
    event(f"{_c(RED, 'CLUSTER UNHEALTHY')}  {s['alive_count']}/{s['total_count']} alive")

def on_cluster_recovered(s):
    event(f"{_c(GREEN, 'CLUSTER RECOVERED')}  {s['alive_count']}/{s['total_count']} alive")

def on_scale_up(delta):
    event(f"{_c(YELLOW, 'AUTOSCALER')}  scale-up recommendation: +{delta} node(s)")

def on_scale_down(delta):
    event(f"{_c(YELLOW, 'AUTOSCALER')}  scale-down recommendation: -{delta} node(s)")

def on_service_up(service, nodes):
    event(f"{_c(GREEN, 'SERVICE UP')}  {service!r}  ({len(nodes)} node(s))")

def on_service_down(service):
    event(f"{_c(RED, 'SERVICE DOWN')} {service!r}")

def on_region_up(region, nodes):
    event(f"{_c(GREEN, 'REGION UP')}   {region!r}  ({len(nodes)} node(s))")

def on_region_down(region):
    event(f"{_c(RED, 'REGION DOWN')} {region!r}")

def on_breaker_trip(node_id, error_rate):
    event(f"{_c(RED, 'BREAKER TRIP')} {node_id}  error_rate={error_rate:.0%}")

def on_breaker_reset(node_id):
    event(f"{_c(GREEN, 'BREAKER RESET')} {node_id}")


# Build all feature components


circuit_breaker = ClusterCircuitBreaker(
    trip_threshold=0.6,
    reset_timeout_sec=20.0,
    check_interval_sec=3.0,
    on_trip=on_breaker_trip,
    on_reset=on_breaker_reset,
)

scheduler = ClusterScheduler(
    cooldown_sec=8.0,
    prefer_alive=True,
    circuit_breaker=circuit_breaker,
)

autoscaler = ClusterAutoScaler(
    min_nodes=1,
    max_nodes=6,
    scale_up_heat_threshold=0.8,
    scale_down_heat_threshold=0.1,
    scale_up_cooldown_sec=30,
    scale_down_cooldown_sec=60,
    check_interval_sec=10.0,
    on_scale_up=on_scale_up,
    on_scale_down=on_scale_down,
)

service_discovery = ServiceDiscovery(
    refresh_interval_sec=3.0,
    on_service_up=on_service_up,
    on_service_down=on_service_down,
)

multi_region = MultiRegionManager(
    refresh_interval_sec=3.0,
    preferred_region="us-east",
    fallback_to_global=True,
    on_region_up=on_region_up,
    on_region_down=on_region_down,
)

ha = ClusterHA(
    node_id="master-primary",
    peers=[],                    # solo mode — instant leader, no peers needed
    election_timeout_sec=2.0,
)

# Rolling updater — just simulates; update_fn is a no-op for the demo
def demo_update_fn(node):
    time.sleep(0.5)   # simulate doing work

rolling_updater = ClusterRollingUpdater(
    update_fn=demo_update_fn,
    batch_size=1,
    drain_timeout_sec=5.0,
    health_gate_ratio=0.4,
    on_node_updated=lambda nid: event(f"{_c(GREEN,'ROLLOUT')} {nid} updated"),
    on_rollout_complete=lambda: event(f"{_c(GREEN,'ROLLOUT')} all nodes updated"),
)


# Start master


master = MasterNode(
    host="127.0.0.1",
    port=7070,
    heartbeat_timeout_sec=15,
    flap_window_sec=60,
    flap_threshold=3,
    quarantine_recovery_heartbeats=3,
    purge_after_sec=120,
    unhealthy_alive_ratio=0.4,
    scheduler=scheduler,
    autoscaler=autoscaler,
    rolling_updater=rolling_updater,
    service_discovery=service_discovery,
    ha=ha,
    multi_region=multi_region,
    circuit_breaker=circuit_breaker,
    on_node_join=on_node_join,
    on_node_dead=on_node_dead,
    on_node_quarantined=on_node_quarantined,
    on_cluster_unhealthy=on_cluster_unhealthy,
    on_cluster_recovered=on_cluster_recovered,
)


# Agent definitions — simulate a real multi-region, multi-service cluster


AGENT_CONFIGS = [
    {
        "node_id":  "web-us-east-1",
        "port":     8081,
        "metadata": {"region": "us-east", "services": "web,api", "tier": "primary"},
    },
    {
        "node_id":  "web-us-east-2",
        "port":     8082,
        "metadata": {"region": "us-east", "services": "web", "tier": "primary"},
    },
    {
        "node_id":  "api-eu-west-1",
        "port":     8083,
        "metadata": {"region": "eu-west", "services": "api", "tier": "primary"},
    },
    {
        "node_id":  "worker-ap-south-1",
        "port":     8084,
        "metadata": {"region": "ap-south", "services": "worker", "tier": "secondary"},
    },
]

agents = [
    AgentNode(
        node_id=cfg["node_id"],
        master_url="http://127.0.0.1:7070",
        port=cfg["port"],
        heartbeat_interval_sec=5,
        metadata=cfg["metadata"],
    )
    for cfg in AGENT_CONFIGS
]


# Background thread: show scheduler picks every 5 s


_stop_event = threading.Event()

def scheduler_demo_loop():
    regions_to_try = ["us-east", "eu-west", "ap-south", None]
    idx = 0
    while not _stop_event.is_set():
        _stop_event.wait(5)
        if _stop_event.is_set():
            break
        nodes = master.nodes()
        if not nodes:
            continue
        region = regions_to_try[idx % len(regions_to_try)]
        idx += 1
        chosen = scheduler.pick(nodes, preferred_region=region)
        if chosen:
            region_tag = f" [region={region}]" if region else " [no region pref]"
            event(
                f"{_c(BLUE,'SCHEDULER')}   picked "
                f"{_c(BOLD, chosen['node_id'])}{region_tag}"
            )


# Startup


def main():
    banner("HuddleCluster — Full System Demo")

    print(f"\n{_c(BOLD, 'Starting master on port 7070...')}")
    master.start()
    time.sleep(0.3)

    print(f"{_c(BOLD, 'Starting agents...')}")
    for agent in agents:
        agent.start()
        time.sleep(0.2)

    time.sleep(1.0)   # let everything settle

    # Print status
    banner("System Ready")

    status = master.status()
    info("Cluster nodes",        f"{status['alive_nodes']} alive / {status['total_nodes']} total")
    info("HA role",              status.get("ha", {}).get("role", "n/a"))
    info("Scheduler",            status.get("scheduler", "disabled"))
    info("Auto Scaler",          status.get("autoscaler", "disabled"))
    info("Rolling Updater",      status.get("rolling_updater", "disabled"))
    info("Service Discovery",    status.get("service_discovery", "disabled"))
    info("Multi-Region",         status.get("multi_region", "disabled"))
    info("Circuit Breaker",      status.get("circuit_breaker", "disabled"))

    banner("Browser URLs")
    urls = [
        ("Dashboard (topology)",    "http://localhost:7070/dashboard"),
        ("Swagger UI (try any API)","http://localhost:7070/v1/docs"),
        ("Cluster status",          "http://localhost:7070/v1/status"),
        ("Prometheus metrics",      "http://localhost:7070/v1/metrics"),
        ("Nodes list",              "http://localhost:7070/v1/nodes"),
        ("Scheduler next pick",     "http://localhost:7070/v1/scheduler/next"),
        ("Scheduler stats",         "http://localhost:7070/v1/scheduler/stats"),
        ("Autoscaler status",       "http://localhost:7070/v1/autoscaler/status"),
        ("Service discovery",       "http://localhost:7070/v1/discovery/services"),
        ("Regions",                 "http://localhost:7070/v1/regions"),
        ("Circuit breakers",        "http://localhost:7070/v1/breakers"),
        ("HA status",               "http://localhost:7070/v1/ha/status"),
        ("OpenAPI spec",            "http://localhost:7070/v1/openapi.json"),
        ("Rollout status",          "http://localhost:7070/v1/rollout/status"),
    ]
    for label, url in urls:
        print(f"  {_c(BOLD, label):<32} {_c(CYAN, url)}")

    banner("Live Events  (Ctrl-C to stop)")

    # Start scheduler demo loop
    t = threading.Thread(target=scheduler_demo_loop, daemon=True)
    t.start()

    # Wait for Ctrl-C
    def _shutdown(sig, frame):
        print(f"\n{_c(YELLOW, 'Shutting down...')}")
        _stop_event.set()
        for agent in agents:
            try:
                agent.stop()
            except Exception:
                pass
        master.stop()
        print(_c(GREEN, "Done."))
        sys.exit(0)

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # Keep main thread alive
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()