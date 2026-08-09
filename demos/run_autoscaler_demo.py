"""
Auto scaler demo — interactive, node-count-based signal (min_nodes/
max_nodes — the standalone signal that doesn't need real heat/thermal
data, unlike the heat-pressure signal which needs a live scheduler
with real traffic).

Commands (type at the prompt):
  kill <node_id>     stop heartbeating a node — it goes dead after
                      heartbeat_timeout_sec (3s here, shortened for demo)
  revive <node_id>   resume heartbeating a dead node
  add <node_id>      join a brand new node
  status              print current alive count + autoscaler status
  quit

Usage: python run_autoscaler_demo.py
"""
from huddle_cluster_pkg import MasterNode, ClusterAutoScaler
import threading
import time

autoscaler = ClusterAutoScaler(
    min_nodes=3,
    max_nodes=5,
    scale_up_cooldown_sec=5.0,     # shortened for demo (default 120s)
    scale_down_cooldown_sec=5.0,   # shortened for demo (default 300s)
    check_interval_sec=2.0,        # shortened for demo (default 30s)
    on_scale_up=lambda delta: print(f"\n[EVENT] SCALE UP recommended: +{delta} node(s)\n"),
    on_scale_down=lambda delta: print(f"\n[EVENT] SCALE DOWN recommended: -{delta} node(s)\n"),
)
master = MasterNode(port=7070, autoscaler=autoscaler, heartbeat_timeout_sec=3.0)
master.start()

NODE_IDS = ["web-1", "web-2", "web-3"]
for node_id in NODE_IDS:
    master._handle_join({"node_id": node_id, "address": "10.0.0.1", "port": 8080, "metadata": {}})

killed = set()
lock = threading.Lock()


def _keep_alive():
    while True:
        time.sleep(1)
        with lock:
            alive_targets = [n for n in NODE_IDS if n not in killed]
        for node_id in alive_targets:
            try:
                master._handle_heartbeat(node_id, {})
            except Exception:
                pass


threading.Thread(target=_keep_alive, daemon=True).start()
time.sleep(0.3)


def print_status():
    n = master.node_count()
    alive = sum(1 for x in master.nodes() if x["status"] == "alive")
    print(f"  total_nodes={n} alive={alive}  (min={autoscaler.min_nodes} max={autoscaler.max_nodes})")
    import json as _json
    print(" ", _json.dumps(autoscaler.status(), indent=2).replace("\n", "\n  "))


print("\nAuto scaler demo master running on :7070")
print(f"  3 nodes: {NODE_IDS}, min_nodes=3, max_nodes=5")
print(f"  heartbeat_timeout=3s (shortened), scale cooldowns=5s (shortened)\n")
print("Commands: kill <node_id>  |  revive <node_id>  |  add <node_id>  |  status  |  quit")
print("Try:  kill web-1          (drops alive count to 2, below min_nodes=3 -> scale up)")
print("      add web-4 / add web-5 / add web-6   (pushes alive above max_nodes=5 -> scale down)")
print("\nAlso in another terminal:  curl.exe http://localhost:7070/v1/autoscaler/status\n")

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
            with lock:
                killed.add(cmd[1])
            print(f"  {cmd[1]} will stop heartbeating — goes dead in ~3s")
        elif cmd[0] == "revive" and len(cmd) == 2:
            with lock:
                killed.discard(cmd[1])
            if cmd[1] not in NODE_IDS:
                NODE_IDS.append(cmd[1])
            print(f"  {cmd[1]} heartbeating resumed")
        elif cmd[0] == "add" and len(cmd) == 2:
            node_id = cmd[1]
            master._handle_join({"node_id": node_id, "address": "10.0.0.1", "port": 8080, "metadata": {}})
            if node_id not in NODE_IDS:
                NODE_IDS.append(node_id)
            print(f"  {node_id} joined")
        else:
            print("  unknown command. Use: kill <id> | revive <id> | add <id> | status | quit")
except (KeyboardInterrupt, EOFError):
    pass

print("\nShutting down...")
master.stop()