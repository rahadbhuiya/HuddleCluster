"""
Service discovery demo — interactive, with the built-in DNS responder
enabled so you can actually `nslookup` a service name and get back
real node IPs.

Nodes and the services they advertise:
  web-1   -> web
  web-2   -> web, api
  api-1   -> api
  cache-1 -> cache

Commands (type at the prompt):
  kill <node_id>     stop heartbeating a node (goes dead after ~3s)
  revive <node_id>   resume heartbeating
  status             print current service -> alive-node mapping
  quit

Usage: python run_service_discovery_demo.py
"""
from huddle_cluster_pkg import MasterNode, ServiceDiscovery
import threading
import time

discovery = ServiceDiscovery(
    refresh_interval_sec=1.0,   # shortened for demo (default 5.0)
    dns_port=8053,              # nonstandard port — no admin/root needed
    on_service_up=lambda name, nodes: print(f"\n[EVENT] service '{name}' UP ({len(nodes)} node(s))\n"),
    on_service_down=lambda name: print(f"\n[EVENT] service '{name}' DOWN (no alive nodes left)\n"),
)
master = MasterNode(port=7070, service_discovery=discovery, heartbeat_timeout_sec=3.0)
master.start()

NODES = {
    "web-1":   ["web"],
    "web-2":   ["web", "api"],
    "api-1":   ["api"],
    "cache-1": ["cache"],
}
for node_id, services in NODES.items():
    master._handle_join({
        "node_id": node_id, "address": "10.0.0.1", "port": 8080,
        "metadata": {"services": services},
    })

killed = set()
lock = threading.Lock()


def _keep_alive():
    while True:
        time.sleep(1)
        with lock:
            targets = [n for n in NODES if n not in killed]
        for node_id in targets:
            try:
                master._handle_heartbeat(node_id, {})
            except Exception:
                pass


threading.Thread(target=_keep_alive, daemon=True).start()
time.sleep(0.3)


def print_status():
    for name in ["web", "api", "cache"]:
        alive = discovery.alive_nodes_for(name)
        ids = [n["node_id"] for n in alive]
        print(f"  {name:<8} -> {ids if ids else '(none alive)'}")


print("\nService discovery demo master running on :7070")
print(f"  Nodes/services: {NODES}")
print(f"  DNS responder on 127.0.0.1:8053 (domain: cluster.local)\n")
print("Commands: kill <node_id>  |  revive <node_id>  |  status  |  quit")
print("Try:  kill web-1   (web-2 still serves 'web' -> no service_down event)")
print("      kill web-2   (now BOTH web providers dead -> 'web' service_down fires)")
print("      kill api-1   then also kill web-2 -> 'api' also goes down\n")
print("Also try in another terminal:")
print("  curl.exe http://localhost:7070/v1/discovery/services")
print("  curl.exe http://localhost:7070/v1/discovery/services/web")
print("  nslookup -port=8053 web.cluster.local 127.0.0.1")
print("  (nslookup's -port flag needs Windows 10+; if it errors, use the")
print("   REST endpoints above instead — same underlying data)\n")

try:
    while True:
        cmd = input("> ").strip().split()
        if not cmd:
            continue
        if cmd[0] == "quit":
            break
        elif cmd[0] == "status":
            print_status()
        elif cmd[0] == "kill" and len(cmd) == 2:
            if cmd[1] not in NODES:
                print(f"  unknown node, valid: {list(NODES)}")
                continue
            with lock:
                killed.add(cmd[1])
            print(f"  {cmd[1]} will stop heartbeating — dead in ~3s")
        elif cmd[0] == "revive" and len(cmd) == 2:
            if cmd[1] not in NODES:
                print(f"  unknown node, valid: {list(NODES)}")
                continue
            with lock:
                killed.discard(cmd[1])
            print(f"  {cmd[1]} heartbeating resumed")
        else:
            print("  unknown command. Use: kill <id> | revive <id> | status | quit")
except (KeyboardInterrupt, EOFError):
    pass

print("\nShutting down...")
master.stop()