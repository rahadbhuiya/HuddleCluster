"""
benchmark_http.py — Real HTTP Benchmark for HuddleCluster
===========================================================
Starts 6 real FastAPI upstream servers, then routes actual
HTTP requests through HuddleCluster, Round Robin, and Least
Connections. Measures real network latency (not simulated).

This provides the "real deployment validation" required for
publication in applied systems venues.

Usage:
  python benchmark_http.py

Requirements:
  pip install fastapi uvicorn httpx

Output:
  http_benchmark_results.png
  http_benchmark_results.json
"""

import asyncio
import json
import os
import signal
import statistics
import subprocess
import sys
import threading
import time

import httpx
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

sys.path.insert(0, ".")
from huddle_cluster import create_cluster, HuddleCluster

#  Server config 
SERVERS = [
    {"id": "s0", "port": 8001, "latency": 12},
    {"id": "s1", "port": 8002, "latency": 14},
    {"id": "s2", "port": 8003, "latency": 16},
    {"id": "s3", "port": 8004, "latency": 18},
    {"id": "s4", "port": 8005, "latency": 20},
    {"id": "s5", "port": 8006, "latency": 22},
]
BASE_URL   = "http://127.0.0.1"
N_REQUESTS = 300
CONCURRENCY = 20



# Server Management


def start_servers():
    """Start all upstream servers as subprocesses."""
    procs = []
    for s in SERVERS:
        cmd = [
            sys.executable, "upstream_server.py",
            "--port",    str(s["port"]),
            "--latency", str(s["latency"]),
            "--id",      s["id"],
        ]
        p = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        procs.append(p)

    # Wait for servers to be ready
    print("  Starting servers", end="", flush=True)
    for s in SERVERS:
        url = f"{BASE_URL}:{s['port']}/health"
        for _ in range(30):
            try:
                r = httpx.get(url, timeout=1.0)
                if r.status_code == 200:
                    break
            except Exception:
                pass
            time.sleep(0.3)
            print(".", end="", flush=True)
    print(" ready!")
    return procs


def stop_servers(procs):
    """Gracefully stop all server processes."""
    for p in procs:
        try:
            p.terminate()
            p.wait(timeout=3)
        except Exception:
            p.kill()
    print("  Servers stopped.")


def inject_slow(server_id):
    """Tell a server to become slow via its admin endpoint."""
    port = next(s["port"] for s in SERVERS if s["id"] == server_id)
    try:
        httpx.post(f"{BASE_URL}:{port}/admin/slow", timeout=2.0)
        print(f"   {server_id} is now SLOW (5x latency)")
    except Exception as e:
        print(f"  Warning: could not slow {server_id}: {e}")


def inject_kill(server_id):
    """Tell a server to simulate a crash."""
    port = next(s["port"] for s in SERVERS if s["id"] == server_id)
    try:
        httpx.post(f"{BASE_URL}:{port}/admin/kill", timeout=2.0)
        print(f"   {server_id} is now DEAD")
    except Exception as e:
        print(f"  Warning: could not kill {server_id}: {e}")


def reset_all_servers():
    """Reset all servers to normal state."""
    for s in SERVERS:
        try:
            httpx.post(f"{BASE_URL}:{s['port']}/admin/normal", timeout=1.0)
            httpx.post(f"{BASE_URL}:{s['port']}/admin/revive", timeout=1.0)
        except Exception:
            pass



# Balancers


class RoundRobin:
    def __init__(self, servers):
        self.servers = servers
        self.index   = 0
        self._lock   = threading.Lock()

    def get_server(self):
        with self._lock:
            s           = self.servers[self.index % len(self.servers)]
            self.index += 1
            return s

    def record_latency(self, server, ms):
        pass  # no-op for RR


class LeastConnections:
    def __init__(self):
        self.conns = {s["id"]: 0 for s in SERVERS}
        self._lock = threading.Lock()

    def get_server(self):
        with self._lock:
            sid = min(self.conns, key=self.conns.get)
            self.conns[sid] += 1
            return next(s for s in SERVERS if s["id"] == sid)

    def record_done(self, server):
        with self._lock:
            self.conns[server["id"]] = max(0, self.conns[server["id"]] - 1)

    def record_latency(self, server, ms):
        self.record_done(server)



# HTTP Request Runner


def send_http_request(balancer, latencies, lock, hc_cluster=None):
    """Send one real HTTP GET to the selected upstream server."""
    server = balancer.get_server()
    # For HuddleCluster server objects, extract host/port
    if hasattr(server, "host"):
        port = server.port
        sid  = server.id
    else:
        port = server["port"]
        sid  = server["id"]

    url = f"{BASE_URL}:{port}/api/work"
    t0  = time.perf_counter()
    try:
        with httpx.Client(timeout=6.0) as client:
            resp = client.get(url)
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        with lock:
            latencies.append(elapsed_ms)
        # Feed latency back to HuddleCluster
        if hc_cluster is not None:
            hc_cluster.record_latency(server, elapsed_ms)
        elif hasattr(balancer, "record_latency"):
            balancer.record_latency(server, elapsed_ms)
    except Exception:
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        with lock:
            latencies.append(min(elapsed_ms, 5000.0))
        if hc_cluster is not None:
            hc_cluster.record_latency(server, 5000.0)


def run_http_benchmark(balancer, n=N_REQUESTS, concurrency=CONCURRENCY,
                       inject_fn=None, hc_cluster=None):
    """Route n real HTTP requests through balancer, concurrency at a time."""
    latencies = []
    lock      = threading.Lock()
    halfway   = n // 2
    injected  = False

    for start in range(0, n, concurrency):
        if not injected and start >= halfway and inject_fn:
            inject_fn()
            injected = True

        end   = min(start + concurrency, n)
        batch = [
            threading.Thread(
                target=send_http_request,
                args=(balancer, latencies, lock),
                kwargs={"hc_cluster": hc_cluster}
            )
            for _ in range(end - start)
        ]
        for t in batch: t.start()
        for t in batch: t.join()

    return {
        "latencies": latencies,
        "p50":       np.percentile(latencies, 50),
        "p95":       np.percentile(latencies, 95),
        "p99":       np.percentile(latencies, 99),
        "avg":       statistics.mean(latencies),
        "n":         len(latencies),
    }



# Scenarios


def make_hc_cluster():
    cluster = create_cluster(
        [(s["id"], "127.0.0.1", s["port"]) for s in SERVERS],
        rotation_cooldown_sec=0.0,
        min_outer_dwell_sec=0.5,
        ema_alpha=0.5,
    )
    cluster.start(rotation_interval_sec=0.3)
    time.sleep(0.5)
    return cluster


def scenario_normal():
    print("\n  SCENARIO 1: Normal Load (real HTTP)")
    reset_all_servers()

    # Round Robin
    rr_r = run_http_benchmark(RoundRobin(SERVERS))

    # Least Connections
    reset_all_servers()
    lc_r = run_http_benchmark(LeastConnections())

    # HuddleCluster
    reset_all_servers()
    cluster = make_hc_cluster()
    hc_r    = run_http_benchmark(cluster, hc_cluster=cluster)
    cluster.stop()

    return rr_r, lc_r, hc_r


def scenario_slow_server():
    print("\n  SCENARIO 2: Slow Server (real HTTP, s2 goes 5x slow at halfway)")
    reset_all_servers()

    rr_r = run_http_benchmark(RoundRobin(SERVERS),
                               inject_fn=lambda: inject_slow("s2"))

    reset_all_servers()
    lc_r = run_http_benchmark(LeastConnections(),
                               inject_fn=lambda: inject_slow("s2"))

    reset_all_servers()
    cluster = make_hc_cluster()
    hc_r    = run_http_benchmark(cluster, hc_cluster=cluster,
                                  inject_fn=lambda: inject_slow("s2"))
    cluster.stop()

    return rr_r, lc_r, hc_r


def scenario_server_failure():
    print("\n  SCENARIO 3: Server Failure (real HTTP, s1 crashes at halfway)")
    reset_all_servers()

    rr_r = run_http_benchmark(RoundRobin(SERVERS),
                               inject_fn=lambda: inject_kill("s1"))

    reset_all_servers()
    lc_r = run_http_benchmark(LeastConnections(),
                               inject_fn=lambda: inject_kill("s1"))

    reset_all_servers()
    cluster = make_hc_cluster()
    hc_r    = run_http_benchmark(cluster, hc_cluster=cluster,
                                  inject_fn=lambda: inject_kill("s1"))
    cluster.stop()

    return rr_r, lc_r, hc_r



# Summary & Charts


def print_summary(results, names):
    print("\n" + "=" * 72)
    print(f"{'':26} {'Round Robin':>12} {'Least Conn':>12} {'Huddle':>12}  {'vs RR':>6}")
    print("=" * 72)
    for name, (rr_r, lc_r, hc_r) in zip(names, results):
        print(f"\n  {name}")
        for key, label in [("p50","P50 (ms)"),("p95","P95 (ms)"),
                            ("p99","P99 (ms)"),("avg","Avg (ms)")]:
            rr_v, lc_v, hc_v = rr_r[key], lc_r[key], hc_r[key]
            diff = (rr_v - hc_v) / rr_v * 100 if rr_v else 0
            mark = "GOOD" if diff > 3 else ("BAD" if diff < -3 else "OK")
            print(f"    {label:<22} {rr_v:>12.1f} {lc_v:>12.1f} "
                  f"{hc_v:>12.1f}  {mark} {diff:+.1f}%")
    print("=" * 72)
    print("  (Real HTTP benchmark — actual network calls, not simulated)")


def plot_results(results, names):
    colors  = {"RR": "#4C72B0", "LC": "#DD8452", "HC": "#55A868"}
    metrics = [("p50","P50 (ms)"), ("p95","P95 (ms)"), ("avg","Avg (ms)")]

    fig, axes = plt.subplots(len(results), len(metrics),
                              figsize=(14, 4.5 * len(results)))
    fig.suptitle(
        "HuddleCluster — Real HTTP Benchmark\n"
        "(6 FastAPI upstream servers, actual network calls)",
        fontsize=13, fontweight="bold", y=0.99
    )

    for row, (name, (rr_r, lc_r, hc_r)) in enumerate(zip(names, results)):
        for col, (key, label) in enumerate(metrics):
            ax   = axes[row][col] if len(results) > 1 else axes[col]
            vals = [rr_r[key], lc_r[key], hc_r[key]]
            bars = ax.bar(["RR", "LC", "HC"], vals,
                          color=list(colors.values()),
                          width=0.5, edgecolor="white", linewidth=0.8)

            # % improvement label on HC bar
            base = vals[0]
            if base > 0:
                pct   = (base - vals[2]) / base * 100
                color = "#2e7d32" if pct > 3 else ("#c62828" if pct < -3 else "#555555")
                ax.text(2, vals[2] + max(vals) * 0.04,
                        f"{pct:+.1f}%", ha="center", fontsize=8.5,
                        color=color, fontweight="bold")

            if col == 0:
                ax.set_ylabel(name, fontsize=9.5, fontweight="bold")
            ax.set_title(label, fontsize=9)
            ax.set_ylim(0, max(vals) * 1.28)
            ax.spines[["top", "right"]].set_visible(False)

    patches = [mpatches.Patch(color=c, label=l) for l, c in
               [("Round Robin","#4C72B0"),("Least Connections","#DD8452"),
                ("HuddleCluster","#55A868")]]
    fig.legend(handles=patches, loc="lower center", ncol=3,
               fontsize=10, bbox_to_anchor=(0.5, 0.01))
    fig.tight_layout(rect=[0, 0.05, 1, 0.97])
    fig.savefig("http_benchmark_results.png", dpi=150, bbox_inches="tight")
    print("\n  Saved: http_benchmark_results.png")
    plt.show()



# Main


if __name__ == "__main__":
    # Install deps if needed
    try:
        import httpx
        from fastapi import FastAPI
        import uvicorn
    except ImportError:
        print("Installing dependencies...")
        subprocess.check_call([
            sys.executable, "-m", "pip", "install",
            "fastapi", "uvicorn", "httpx", "--break-system-packages", "-q"
        ])
        import httpx
        from fastapi import FastAPI
        import uvicorn

    print("=" * 60)
    print("    HuddleCluster Real HTTP Benchmark")
    print("  6 FastAPI servers · 3 scenarios · ~10 minutes")
    print("=" * 60)

    procs = start_servers()

    try:
        r1 = scenario_normal()
        r2 = scenario_slow_server()
        r3 = scenario_server_failure()

        all_results = [r1, r2, r3]
        names = [
            "1. Normal Load",
            "2. Slow Server (5x at halfway)",
            "3. Server Failure (crash at halfway)",
        ]

        # Save JSON
        json_out = {}
        for name, (rr_r, lc_r, hc_r) in zip(names, all_results):
            json_out[name] = {
                "rr": {k: round(v, 3) for k, v in rr_r.items() if k != "latencies"},
                "lc": {k: round(v, 3) for k, v in lc_r.items() if k != "latencies"},
                "hc": {k: round(v, 3) for k, v in hc_r.items() if k != "latencies"},
            }
        with open("http_benchmark_results.json", "w") as f:
            json.dump(json_out, f, indent=2)
        print("  Saved: http_benchmark_results.json")

        print_summary(all_results, names)
        plot_results(all_results, names)

    finally:
        stop_servers(procs)
        print("\n  Done!")
