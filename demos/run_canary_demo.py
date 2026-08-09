"""
Canary deployment demo — one master, a scheduler wired for canary
routing, and a few announced "stable" vs "canary" nodes (fake, just
node records — no real backend servers needed to see the routing
percentages work).

Usage: python run_canary_demo.py
Then in another terminal, drive it with curl (see printed commands).
"""
from huddle_cluster_pkg import MasterNode, ClusterScheduler, ClusterCanaryDeployment
import threading
import time

canary = ClusterCanaryDeployment(
    weight_steps=[5, 25, 50, 100],
    on_promote=lambda: print("[EVENT] promoted — canary is now the only version"),
    on_abort=lambda: print("[EVENT] aborted — rolled back to 100% stable"),
    on_weight_change=lambda w: print(f"[EVENT] canary weight now {w:.0f}%"),
)
scheduler = ClusterScheduler(canary=canary)
master = MasterNode(port=7070, scheduler=scheduler, canary=canary)
master.start()

# Register some fake nodes directly (skips needing real agent processes)
# — half tagged canary=true, half stable.
FAKE_NODES = (
    [(f"web-v1-{i}", {}) for i in range(1, 5)]
    + [(f"web-v2-{i}", {"canary": "true"}) for i in range(1, 3)]
)
for node_id, meta in FAKE_NODES:
    master._handle_join({
        "node_id": node_id, "address": "10.0.0.1", "port": 8080, "metadata": meta,
    })


def _keep_alive():
    """Real background heartbeat, so these fake nodes don't get marked
    dead after heartbeat_timeout_sec (30s) like a one-time join would.
    A real deployment has actual AgentNode processes doing this."""
    while True:
        time.sleep(5)
        for node_id, _ in FAKE_NODES:
            try:
                master._handle_heartbeat(node_id, {})
            except Exception:
                pass


threading.Thread(target=_keep_alive, daemon=True).start()

print("\nCanary demo master running on :7070")
print("  4 stable nodes (web-v1-1..4), 2 canary nodes (web-v2-1..2)")
print("  (a background thread now sends real heartbeats every 5s so")
print("   these stay 'alive' — no more 30s heartbeat-timeout death)\n")
print("Try these in another terminal:")
print('  curl.exe -X POST http://localhost:7070/v1/canary/start -d "{\\"weight\\": 5}"')
print("  curl.exe http://localhost:7070/v1/canary/status")
print('  curl.exe -X POST http://localhost:7070/v1/canary/advance')
print('  curl.exe -X POST http://localhost:7070/v1/canary/promote')
print('  curl.exe -X POST http://localhost:7070/v1/canary/abort')
print("\nTo see routing in action, run this in a THIRD terminal while canary is active:")
print("  python route_sample.py     (prints how many of 200 picks went to canary vs stable)")
print("\nPress Ctrl-C to stop.\n")

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\nShutting down...")
    master.stop()