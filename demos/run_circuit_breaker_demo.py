"""
Circuit breaker demo — one terminal, interactive.

4 fake nodes, kept alive by a real background heartbeat thread (same
approach as the canary demo). Each node's simulated error_rate is
controlled by typing commands right in this terminal — no need for a
second terminal for the failure-injection part (you'll still want a
second terminal to watch /v1/breakers or GET /v1/scheduler/next).

Commands (type at the prompt):
  fail <node_id> <rate>   e.g. "fail web-2 0.9"  (90% error rate)
  heal <node_id>          reset a node back to 0% errors
  status                  print current breaker states
  quit                    stop the demo

Usage: python run_circuit_breaker_demo.py
"""
from huddle_cluster_pkg import (
    MasterNode, ClusterScheduler, ClusterCircuitBreaker,
)
import threading
import time

breaker = ClusterCircuitBreaker(
    trip_threshold=0.5,      # trip above 50% error rate
    reset_timeout_sec=15.0,  # probe again after 15s (shortened for demo)
    check_interval_sec=1.0,  # evaluate every 1s (shortened for demo)
    on_trip=lambda node_id, er: print(f"\n[EVENT] '{node_id}' TRIPPED (error_rate={er:.0%}) — excluded from scheduling\n"),
    on_reset=lambda node_id: print(f"\n[EVENT] '{node_id}' reset to CLOSED — back in rotation\n"),
)
scheduler = ClusterScheduler(circuit_breaker=breaker)
master = MasterNode(port=7070, scheduler=scheduler, circuit_breaker=breaker)
master.start()

NODE_IDS = ["web-1", "web-2", "web-3", "web-4"]
for node_id in NODE_IDS:
    master._handle_join({"node_id": node_id, "address": "10.0.0.1", "port": 8080, "metadata": {}})

error_rates = {nid: 0.0 for nid in NODE_IDS}
lock = threading.Lock()


def _keep_alive():
    while True:
        time.sleep(2)
        with lock:
            rates = dict(error_rates)
        for node_id, rate in rates.items():
            try:
                master._handle_heartbeat(node_id, {"metrics": {"error_rate": rate}})
            except Exception:
                pass


threading.Thread(target=_keep_alive, daemon=True).start()


def print_status():
    for s in breaker.all_states():
        print(f"  {s['node_id']:<10} {s['state']:<10} "
              f"error_rate={s['last_error_rate']:.0%}  trips={s['trip_count']}")
    if not breaker.all_states():
        print("  (no breaker states yet — wait a couple seconds for the first heartbeat)")


print("\nCircuit breaker demo master running on :7070")
print(f"  4 nodes: {NODE_IDS}, all healthy (0% error rate) to start")
print(f"  trip_threshold=50%, reset_timeout=15s, check_interval=1s\n")
print("Commands: fail <node_id> <rate 0-1>  |  heal <node_id>  |  status  |  quit")
print("Example:  fail web-2 0.9\n")
print("Also try in another terminal while a node is tripped:")
print("  curl.exe http://localhost:7070/v1/breakers")
print("  curl.exe http://localhost:7070/v1/scheduler/next   (run several times — tripped node never picked)")
print()

try:
    while True:
        cmd = input("> ").strip().split()
        if not cmd:
            continue
        if cmd[0] == "quit":
            break
        elif cmd[0] == "status":
            print_status()
        elif cmd[0] == "fail" and len(cmd) == 3:
            node_id, rate = cmd[1], float(cmd[2])
            if node_id not in error_rates:
                print(f"  unknown node '{node_id}', valid: {NODE_IDS}")
                continue
            with lock:
                error_rates[node_id] = rate
            print(f"  {node_id} error_rate set to {rate:.0%} — will apply on next heartbeat (<=2s)")
        elif cmd[0] == "heal" and len(cmd) == 2:
            node_id = cmd[1]
            if node_id not in error_rates:
                print(f"  unknown node '{node_id}', valid: {NODE_IDS}")
                continue
            with lock:
                error_rates[node_id] = 0.0
            print(f"  {node_id} error_rate set to 0% — breaker auto-resets once it observes this (<=2s heartbeat + <=1s eval)")
        else:
            print("  unknown command. Use: fail <node_id> <rate>  |  heal <node_id>  |  status  |  quit")
except (KeyboardInterrupt, EOFError):
    pass

print("\nShutting down...")
master.stop()