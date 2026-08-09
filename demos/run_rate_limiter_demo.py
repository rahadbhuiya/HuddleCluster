"""
Rate limiter demo — 3 nodes, deliberately small token buckets
(capacity=5, refill=1/sec) so you can exhaust them with a quick burst
and watch the scheduler start excluding rate-limited nodes.

Usage: python run_rate_limiter_demo.py
"""
from huddle_cluster_pkg import MasterNode, ClusterScheduler, ClusterRateLimiter
import threading
import time

limiter = ClusterRateLimiter(
    capacity=5,        # small on purpose — easy to burst through
    refill_rate=1.0,   # 1 token/sec — recovery is slow enough to see
    on_rate_limited=lambda node_id: print(f"[EVENT] '{node_id}' rate-limited (bucket empty)"),
)
scheduler = ClusterScheduler(rate_limiter=limiter)
master = MasterNode(port=7070, scheduler=scheduler, rate_limiter=limiter)
master.start()

NODE_IDS = ["web-1", "web-2", "web-3"]
for node_id in NODE_IDS:
    master._handle_join({"node_id": node_id, "address": "10.0.0.1", "port": 8080, "metadata": {}})


def _keep_alive():
    while True:
        time.sleep(2)
        for node_id in NODE_IDS:
            try:
                master._handle_heartbeat(node_id, {})
            except Exception:
                pass


threading.Thread(target=_keep_alive, daemon=True).start()
time.sleep(0.3)

print("\nRate limiter demo master running on :7070")
print(f"  3 nodes: {NODE_IDS}")
print(f"  capacity=5 tokens/node, refill=1/sec (15 tokens total across all 3 nodes)\n")
print("Try in another terminal:")
print("  curl.exe http://localhost:7070/v1/ratelimits")
print("  curl.exe http://localhost:7070/v1/scheduler/next     (each call consumes 1 token)")
print("  curl.exe -X POST http://localhost:7070/v1/ratelimits/web-1/reset")
print("\nOr run burst_sample.py in a third terminal to fire 25 rapid picks")
print("(more than the 15 total tokens available) and watch some get rejected.\n")
print("Press Ctrl-C to stop.\n")

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\nShutting down...")
    master.stop()