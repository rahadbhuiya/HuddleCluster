"""
Observability demo — a master with structured JSON logging + trace IDs
+ OTLP export enabled, pointed at fake_otlp_collector.py.

Usage: python run_observability_demo.py
(run fake_otlp_collector.py in another terminal FIRST)
"""
from huddle_cluster_pkg import MasterNode, ClusterObservability
import time

obs = ClusterObservability(
    service_name="huddlecluster-demo",
    otlp_endpoint="http://localhost:4318",
    otlp_flush_interval_sec=2.0,   # short, so you see exports quickly in the demo
)
master = MasterNode(port=7070, observability=obs)
master.start()

print("Observability demo master running on :7070")
print("Watch the fake_otlp_collector.py terminal for exported events.\n")
print("Try these in another terminal to generate events:")
print("  curl.exe http://localhost:7070/v1/status")
print("  curl.exe http://localhost:7070/v1/observability/status")
print("  curl.exe http://localhost:7070/v1/observability/logs")
print("\nEach request you make becomes a buffered event, exported to the")
print("fake collector within otlp_flush_interval_sec (2s here).\n")
print("Press Ctrl-C to stop.\n")

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\nShutting down (final flush happens here)...")
    master.stop()