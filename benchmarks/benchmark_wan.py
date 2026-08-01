"""
benchmark_wan.py — WAN-latency simulation benchmark for HuddleCluster
========================================================================
benchmark_http.py validates HuddleCluster against real HTTP servers, but
on loopback / a Docker bridge network — tight, low-jitter latencies
(12-22ms). That's real network I/O, but it isn't WAN. This script runs
the same kind of real-HTTP comparison against upstream servers configured
with region-realistic one-way latency + proportional jitter + a small
rate of simulated packet loss, to see whether HuddleCluster's adaptive
routing still helps once individual servers are meaningfully slower and
less predictable than same-datacenter peers.

Two important caveats, stated up front rather than left implicit:

1. This is APPLICATION-LEVEL latency simulation (each upstream server
   sleeps for a randomised duration before responding), not kernel-level
   network impairment. `tc netem` would be more realistic — it affects
   actual TCP handshake/ACK timing, not just handler latency — but it
   requires root + CAP_NET_ADMIN + the sch_netem kernel module, none of
   which are guaranteed in a sandboxed/CI container. (Concretely: in the
   environment this script was developed and run in, `tc qdisc add ...
   netem` fails with "Specified qdisc kind is unknown" because
   /lib/modules/<kernel>/kernel/net/sched/ doesn't exist in that
   container — no module to load.) If your environment has netem
   available, `--use-netem` attempts it and falls back to app-level
   simulation with a warning if the qdisc add fails.

2. This still runs all "regions" as local processes on one host talking
   over loopback. It validates HuddleCluster's *algorithm* under
   WAN-like latency/jitter/loss characteristics — it does NOT validate
   real cross-region network behavior (actual internet routing,
   asymmetric paths, real congestion, DNS latency, TLS handshake cost
   over a real WAN link, etc.). A genuine multi-region validation needs
   servers actually deployed in different cloud regions/AZs. Treat this
   as "meaningfully closer to WAN than benchmark_http.py", not as proof
   HuddleCluster works well over a real WAN.

Usage:
  python benchmark_wan.py
  python benchmark_wan.py --use-netem     # attempt kernel-level netem, e.g. on lo

Requirements:
  pip install fastapi uvicorn httpx numpy

Output:
  wan_benchmark_results.json
"""

import argparse
import json
import shutil
import statistics
import subprocess
import sys
import threading
import time

import httpx
import numpy as np

sys.path.insert(0, ".")
from huddle_cluster import create_cluster


#  Simulated regions 
# One-way latency roughly modelled on real inter-region RTT/2 figures
# (these are ballpark, not measured — the point is relative spread and
# jitter-as-fraction-of-latency, not precise real-world accuracy).
REGIONS = [
    {"id": "us-east-1",  "port": 18101, "latency_ms": 8,   "jitter_pct": 15},
    {"id": "us-east-2",  "port": 18102, "latency_ms": 10,  "jitter_pct": 15},
    {"id": "us-west-1",  "port": 18103, "latency_ms": 35,  "jitter_pct": 20},
    {"id": "eu-west-1",  "port": 18104, "latency_ms": 40,  "jitter_pct": 20},
    {"id": "ap-south-1", "port": 18105, "latency_ms": 95,  "jitter_pct": 30},
    {"id": "ap-ne-1",    "port": 18106, "latency_ms": 110, "jitter_pct": 30},
]
BASE_URL    = "http://127.0.0.1"
N_REQUESTS  = 300
CONCURRENCY = 20
LOSS_PCT    = 1.0   # simulated packet loss per request, applied to all regions


def _netem_available() -> bool:
    if not shutil.which("tc"):
        return False
    probe = subprocess.run(
        ["tc", "qdisc", "add", "dev", "lo", "root", "netem", "delay", "1ms"],
        capture_output=True,
    )
    if probe.returncode == 0:
        subprocess.run(["tc", "qdisc", "del", "dev", "lo", "root"], capture_output=True)
        return True
    return False


def start_regions(use_netem: bool):
    procs = []
    for r in REGIONS:
        jitter_ms = r["latency_ms"] * r["jitter_pct"] / 100.0
        # If netem is handling latency at the kernel level, don't also
        # add it at the application level — that would double-count it.
        app_latency = 0.5 if use_netem else r["latency_ms"]
        app_jitter  = 0.2 if use_netem else jitter_ms
        cmd = [
            sys.executable, "upstream_server.py",
            "--port",     str(r["port"]),
            "--latency",  str(app_latency),
            "--jitter",   str(app_jitter),
            "--loss-pct", str(LOSS_PCT),
            "--id",       r["id"],
        ]
        procs.append(subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))

    print("  Starting simulated regions", end="", flush=True)
    for r in REGIONS:
        url = f"{BASE_URL}:{r['port']}/health"
        for _ in range(30):
            try:
                if httpx.get(url, timeout=1.0).status_code == 200:
                    break
            except Exception:
                pass
            time.sleep(0.3)
            print(".", end="", flush=True)
    print(" ready!")

    if use_netem:
        for r in REGIONS:
            subprocess.run(
                ["tc", "qdisc", "add", "dev", "lo", "root", "netem",
                 "delay", f"{r['latency_ms']}ms", f"{r['latency_ms']*r['jitter_pct']//100}ms"],
                capture_output=True,
            )
            # Note: tc netem on `lo` applies globally to loopback, not
            # per-port — a real per-region qdisc setup needs per-region
            # network namespaces/veth pairs, which is out of scope for a
            # single-host script. This --use-netem path is provided for
            # environments where you've already set that up; here it
            # only demonstrates the attempt/fallback logic.
            break

    return procs


def stop_regions(procs, use_netem: bool):
    for p in procs:
        p.terminate()
    for p in procs:
        try:
            p.wait(timeout=3)
        except subprocess.TimeoutExpired:
            p.kill()
    if use_netem:
        subprocess.run(["tc", "qdisc", "del", "dev", "lo", "root"], capture_output=True)


class SimpleRoundRobin:
    def __init__(self, servers):
        self.servers = servers
        self._i = 0
        self._lock = threading.Lock()

    def get_server(self):
        with self._lock:
            s = self.servers[self._i % len(self.servers)]
            self._i += 1
            return s

    def record_latency(self, server, ms):
        pass


def make_hc_cluster():
    cluster = create_cluster(
        [(r["id"], "127.0.0.1", r["port"]) for r in REGIONS],
        rotation_cooldown_sec=0.0,
        min_outer_dwell_sec=0.5,
        ema_alpha=0.5,
    )
    cluster.start(rotation_interval_sec=0.3)
    time.sleep(0.5)
    return cluster


def send_request(balancer, latencies, lock, hc_cluster=None):
    server = balancer.get_server()
    port = server.port if hasattr(server, "host") else server["port"]
    url = f"{BASE_URL}:{port}/api/work"
    t0 = time.perf_counter()
    try:
        with httpx.Client(timeout=6.0) as client:
            client.get(url)
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        with lock:
            latencies.append(elapsed_ms)
        if hc_cluster is not None:
            hc_cluster.record_latency(server, elapsed_ms)
    except Exception:
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        with lock:
            latencies.append(min(elapsed_ms, 5000.0))
        if hc_cluster is not None:
            hc_cluster.record_latency(server, 5000.0)


def run_benchmark(balancer, hc_cluster=None, n=N_REQUESTS, concurrency=CONCURRENCY):
    latencies, lock = [], threading.Lock()
    for start in range(0, n, concurrency):
        end = min(start + concurrency, n)
        batch = [
            threading.Thread(target=send_request, args=(balancer, latencies, lock, hc_cluster))
            for _ in range(end - start)
        ]
        for t in batch: t.start()
        for t in batch: t.join()
    return {
        "p50": float(np.percentile(latencies, 50)),
        "p95": float(np.percentile(latencies, 95)),
        "p99": float(np.percentile(latencies, 99)),
        "avg": float(statistics.mean(latencies)),
        "n":   len(latencies),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--use-netem", action="store_true",
                         help="Attempt kernel-level tc netem instead of "
                              "application-level latency simulation.")
    args = parser.parse_args()

    use_netem = False
    if args.use_netem:
        print("Checking for tc netem availability...")
        if _netem_available():
            use_netem = True
            print("  netem available — using kernel-level latency injection.")
        else:
            print("  netem NOT available on this host/container "
                  "(missing sch_netem kernel module, no CAP_NET_ADMIN, or "
                  "`tc` not installed) — falling back to application-level "
                  "simulation.")

    print(f"\n{'='*70}")
    print("HuddleCluster — WAN-Latency Simulation Benchmark")
    print(f"{'='*70}")
    region_desc = ", ".join(f"{r['id']} ({r['latency_ms']}ms+jitter)" for r in REGIONS)
    print(f"Regions: {region_desc}")
    print(f"Simulated packet loss: {LOSS_PCT}% per request")
    print(f"Latency injection mode: {'kernel (tc netem)' if use_netem else 'application-level'}")

    procs = start_regions(use_netem)
    results = {}
    try:
        rr = SimpleRoundRobin(list(REGIONS))
        print("\n  Running Round Robin...")
        results["round_robin"] = run_benchmark(rr)

        hc = make_hc_cluster()
        print("  Running HuddleCluster...")
        results["huddlecluster"] = run_benchmark(hc, hc_cluster=hc)
        hc.stop()

    finally:
        stop_regions(procs, use_netem)

    print(f"\n{'='*70}")
    print(f"{'Balancer':<15} {'P50 (ms)':>10} {'P95 (ms)':>10} {'P99 (ms)':>10} {'Avg (ms)':>10}")
    print("-" * 70)
    for name, r in results.items():
        print(f"{name:<15} {r['p50']:>10.1f} {r['p95']:>10.1f} {r['p99']:>10.1f} {r['avg']:>10.1f}")
    print(f"{'='*70}")

    if results["round_robin"]["p95"] > 0:
        improvement = (
            (results["round_robin"]["p95"] - results["huddlecluster"]["p95"])
            / results["round_robin"]["p95"] * 100
        )
        print(f"\nP95 change vs Round Robin under WAN-like conditions: {improvement:+.1f}%")
        print("(Positive = HuddleCluster's P95 was lower / better.)")

    with open("wan_benchmark_results.json", "w") as f:
        json.dump({
            "regions": REGIONS,
            "loss_pct": LOSS_PCT,
            "injection_mode": "netem" if use_netem else "application",
            "results": results,
        }, f, indent=2)
    print("\nResults saved to wan_benchmark_results.json")
    print(
        "\nReminder: this simulates WAN-like latency/jitter/loss "
        "characteristics on one host — it is not a real multi-region "
        "network validation. See the module docstring for what this "
        "does and doesn't prove."
    )


if __name__ == "__main__":
    main()