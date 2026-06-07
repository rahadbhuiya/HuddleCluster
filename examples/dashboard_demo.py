"""
HuddleCluster Dashboard Demo

Run this script from the project root:
    python examples/dashboard_demo.py

Then open http://127.0.0.1:8888 in your browser.
Press Ctrl+C to stop.
"""

import os
import sys

# Ensure the project root is on sys.path so the local huddle_cluster.py
# is imported instead of any installed (older) package version.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import random
import threading
import time

from huddle_cluster import create_cluster


def simulate_traffic(cluster, stop_event):
    """Simulate requests with occasional slow/failing servers."""
    while not stop_event.is_set():
        server = cluster.get_server()
        if server is None:
            time.sleep(0.1)
            continue

        # Simulate latency: most requests fast, some slow
        latency_ms = random.gauss(15, 3)
        if random.random() < 0.05:          # 5% chance of slow request
            latency_ms = random.gauss(200, 20)

        cluster.record_latency(server, max(1.0, latency_ms))
        time.sleep(random.uniform(0.05, 0.15))


def simulate_load_spike(cluster, stop_event):
    """Occasionally spike one server's CPU/memory to trigger eviction."""
    servers_cycled = 0
    while not stop_event.is_set():
        time.sleep(8)
        servers = cluster.inner_servers()
        if not servers or stop_event.is_set():
            continue

        # Pick a random inner server and heat it up
        s = random.choice(servers)
        print(f"  Spiking {s.id} ...")
        s.metrics.cpu_usage    = random.uniform(0.85, 0.98)
        s.metrics.memory_usage = random.uniform(0.80, 0.95)

        # Hold the spike for a few seconds then cool down
        time.sleep(3)
        if not stop_event.is_set():
            s.metrics.cpu_usage    = random.uniform(0.10, 0.25)
            s.metrics.memory_usage = random.uniform(0.10, 0.30)
            servers_cycled += 1
            print(f"  {s.id} cooled down (total spikes: {servers_cycled})")


if __name__ == "__main__":
    print("Starting HuddleCluster dashboard demo...")
    print()

    cluster = create_cluster(
        [
            ("web-1", "127.0.0.1", 8001),
            ("web-2", "127.0.0.1", 8002),
            ("web-3", "127.0.0.1", 8003),
            ("web-4", "127.0.0.1", 8004),
        ],
        min_inner_size=2,
        max_inner_size=4,
        heat_threshold=0.55,
        cool_threshold=0.30,
        adaptive_thresholds=True,
    )
    cluster.start(rotation_interval_sec=2.0)

    # Start dashboard on port 8888
    port = cluster.serve_dashboard(port=8888)
    print(f"Dashboard:    http://127.0.0.1:{port}")

    # Start admin API on port 9000
    admin_port = cluster.serve_admin(port=9000)
    print(f"Admin API:    http://127.0.0.1:{admin_port}/admin/health")
    print()
    print("Useful curl commands:")
    print(f"  curl http://127.0.0.1:{admin_port}/admin/servers")
    print(f"  curl -X POST http://127.0.0.1:{admin_port}/admin/evict/web-1")
    print(f"  curl -X POST 'http://127.0.0.1:{admin_port}/admin/ramp/web-4?initial=0.1&ramp_sec=30'")
    print()
    print("Simulating traffic... Press Ctrl+C to stop.")
    print()

    stop_event = threading.Event()

    # Start background traffic simulation threads
    traffic_thread = threading.Thread(
        target=simulate_traffic, args=(cluster, stop_event), daemon=True
    )
    spike_thread = threading.Thread(
        target=simulate_load_spike, args=(cluster, stop_event), daemon=True
    )
    traffic_thread.start()
    spike_thread.start()

    try:
        while True:
            report = cluster.health_report()
            inner = [s["id"] for s in report["inner_ring"]]
            outer = [s["id"] for s in report["outer_ring"]]
            print(
                f"\r  inner={inner}  outer={outer}  "
                f"rps={report['requests_per_sec']:.1f}  "
                f"fairness={report['fairness_score']:.3f}",
                end="",
                flush=True,
            )
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\nStopping...")
    finally:
        stop_event.set()
        cluster.stop_dashboard()
        cluster.stop_admin()
        cluster.stop()
        print("Done.")