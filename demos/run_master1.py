"""
HA demo — master 1 of 3.

Run this + run_master2.py + run_master3.py in three terminals. They
elect a leader among themselves. Kill whichever one is currently
leader (Ctrl-C) and watch the remaining two elect a new one — a
3-node cluster tolerates exactly 1 failure (see docs/CLUSTER.md,
"Honest limitations", for why 2 nodes can't).

Usage: python run_master1.py
"""
from huddle_cluster_pkg import MasterNode, ClusterHA
import time

ha = ClusterHA(
    node_id="master-1",
    peers=["http://127.0.0.1:7071", "http://127.0.0.1:7072"],
    election_timeout_sec=1.0,
    heartbeat_interval_sec=0.3,
    sync_interval_sec=2.0,
    state_file="ha_state_master1.json",   # survives a restart
)

master = MasterNode(
    port=7070,
    ha=ha,
)
master.start()

print("master-1 running on :7070 — Ctrl-C to stop")
print("Check role anytime:  curl http://localhost:7070/v1/ha/status")
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\nShutting down master-1...")
    master.stop()