"""
benchmark_industry.py -- Industry Baseline Benchmark
======================================================
Compares HuddleCluster against NGINX (Round Robin) and
NGINX (Least Connections) -- two production-grade baselines.

Prerequisites:
  docker compose up -d   (from benchmarks/ directory)

Then:
  python benchmark_industry.py

Output:
  industry_benchmark_results.png
  industry_benchmark_results.json
"""

import json
import statistics
import sys
import threading
import time

import httpx
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

sys.path.insert(0, "..")
from huddle_cluster import create_cluster

#  Endpoints 
NGINX_RR_URL  = "http://127.0.0.1:9001"   # NGINX round-robin
NGINX_LC_URL  = "http://127.0.0.1:9002"   # NGINX least-connections
# HuddleCluster connects directly to upstream servers for latency feedback
# Note: upstreams are on 8001-8006 (Docker port-mapped)
HC_SERVERS = [
    {"id": f"s{i}", "port": 8001 + i}
    for i in range(6)
]

# Admin endpoints for slow/kill injection
ADMIN_SERVERS = [
    {"id": f"s{i}", "port": 8001 + i}
    for i in range(6)
]

N_REQUESTS  = 400
CONCURRENCY = 20




#  Helpers 

def wait_ready(url, name, retries=30):
    for _ in range(retries):
        try:
            r = httpx.get(f"{url}/health", timeout=2.0)
            if r.status_code == 200:
                print(f"  {name} ready.")
                return True
        except Exception:
            pass
        time.sleep(1.0)
    print(f"  WARNING: {name} not responding at {url}")
    return False


def reset_servers():
    for s in ADMIN_SERVERS:
        url = f"http://127.0.0.1:{s['port']}"
        try:
            httpx.post(f"{url}/admin/normal", timeout=2.0)
            httpx.post(f"{url}/admin/revive", timeout=2.0)
        except Exception:
            pass


def inject_slow(server_id):
    """Tell a server to slow down via its admin endpoint.
    Tries direct port first; if not accessible, silently skips
    (NGINX benchmark still runs, just without injected slowness)."""
    port = next(s["port"] for s in ADMIN_SERVERS if s["id"] == server_id)
    try:
        httpx.post(f"http://127.0.0.1:{port}/admin/slow", timeout=2.0)
        print(f"  {server_id} is now SLOW (5x latency)")
    except Exception:
        # Upstream not directly accessible -- skip injection
        # (containers communicate on internal Docker network)
        print(f"  Note: {server_id} admin not reachable on host -- running without slow injection")


def inject_kill(server_id):
    port = next(s["port"] for s in ADMIN_SERVERS if s["id"] == server_id)
    try:
        httpx.post(f"http://127.0.0.1:{port}/admin/kill", timeout=2.0)
        print(f"  {server_id} is now DEAD")
    except Exception:
        print(f"  Note: {server_id} admin not reachable on host -- running without failure injection")


#  HTTP request runners 

def run_nginx(base_url, n=N_REQUESTS, concurrency=CONCURRENCY, inject_fn=None):
    latencies = []
    lock      = threading.Lock()
    injected  = [False]

    def req():
        t0 = time.perf_counter()
        try:
            with httpx.Client(timeout=8.0) as client:
                client.get(f"{base_url}/api/work")
            elapsed = (time.perf_counter() - t0) * 1000.0
        except Exception:
            elapsed = min((time.perf_counter() - t0) * 1000.0, 6000.0)
        with lock:
            latencies.append(elapsed)

    for start in range(0, n, concurrency):
        if not injected[0] and start >= n // 2 and inject_fn:
            inject_fn()
            injected[0] = True
        end   = min(start + concurrency, n)
        batch = [threading.Thread(target=req) for _ in range(end - start)]
        for t in batch: t.start()
        for t in batch: t.join()

    return _stats(latencies)


def run_huddle(n=N_REQUESTS, concurrency=CONCURRENCY, inject_fn=None):
    cluster = create_cluster(
        [(s["id"], "127.0.0.1", s["port"]) for s in HC_SERVERS],
        rotation_cooldown_sec=0.0,
        min_outer_dwell_sec=0.5,
        ema_alpha=0.5,
    )
    cluster.start(rotation_interval_sec=0.3)
    time.sleep(0.5)

    latencies = []
    lock      = threading.Lock()
    injected  = [False]

    def req():
        server = cluster.get_server()
        if server is None:
            return
        t0 = time.perf_counter()
        try:
            with httpx.Client(timeout=8.0) as client:
                client.get(f"http://127.0.0.1:{server.port}/api/work")
            elapsed = (time.perf_counter() - t0) * 1000.0
        except Exception:
            elapsed = min((time.perf_counter() - t0) * 1000.0, 6000.0)
        cluster.record_latency(server, elapsed)
        with lock:
            latencies.append(elapsed)

    for start in range(0, n, concurrency):
        if not injected[0] and start >= n // 2 and inject_fn:
            inject_fn()
            injected[0] = True
        end   = min(start + concurrency, n)
        batch = [threading.Thread(target=req) for _ in range(end - start)]
        for t in batch: t.start()
        for t in batch: t.join()

    cluster.stop()
    return _stats(latencies)


def _stats(latencies):
    return {
        "latencies": latencies,
        "p50":       round(np.percentile(latencies, 50), 2),
        "p95":       round(np.percentile(latencies, 95), 2),
        "p99":       round(np.percentile(latencies, 99), 2),
        "avg":       round(statistics.mean(latencies), 2),
        "n":         len(latencies),
    }


#  Scenarios 

def scenario_normal():
    print("\nScenario 1: Normal Load")
    reset_servers()
    time.sleep(0.5)
    rr = run_nginx(NGINX_RR_URL)
    reset_servers(); time.sleep(0.5)
    lc = run_nginx(NGINX_LC_URL)
    reset_servers(); time.sleep(0.5)
    hc = run_huddle()
    return rr, lc, hc


def scenario_slow():
    print("\nScenario 2: Slow Server (s2 5x at halfway)")
    reset_servers(); time.sleep(0.5)
    rr = run_nginx(NGINX_RR_URL,  inject_fn=lambda: inject_slow("s2"))
    reset_servers(); time.sleep(0.5)
    lc = run_nginx(NGINX_LC_URL,  inject_fn=lambda: inject_slow("s2"))
    reset_servers(); time.sleep(0.5)
    hc = run_huddle(inject_fn=lambda: inject_slow("s2"))
    return rr, lc, hc


def scenario_failure():
    print("\nScenario 3: Server Failure (s1 crash at halfway)")
    reset_servers(); time.sleep(0.5)
    rr = run_nginx(NGINX_RR_URL,  inject_fn=lambda: inject_kill("s1"))
    reset_servers(); time.sleep(0.5)
    lc = run_nginx(NGINX_LC_URL,  inject_fn=lambda: inject_kill("s1"))
    reset_servers(); time.sleep(0.5)
    hc = run_huddle(inject_fn=lambda: inject_kill("s1"))
    return rr, lc, hc


#  Output 

def print_summary(results, names):
    print("\n" + "=" * 74)
    print(f"{'':28} {'NGINX RR':>12} {'NGINX LC':>12} {'HuddleCluster':>14}  vs NGINX-RR")
    print("=" * 74)
    for name, (rr, lc, hc) in zip(names, results):
        print(f"\n  {name}")
        for key, label in [("p50","P50 (ms)"),("p95","P95 (ms)"),
                            ("p99","P99 (ms)"),("avg","Avg (ms)")]:
            rv, lv, hv = rr[key], lc[key], hc[key]
            diff = (rv - hv) / rv * 100 if rv else 0
            mark = "better" if diff > 3 else ("worse" if diff < -3 else "similar")
            print(f"    {label:<22} {rv:>12.1f} {lv:>12.1f} {hv:>14.1f}  {mark} ({diff:+.1f}%)")
    print("=" * 74)
    print("  (Containerized benchmark: NGINX upstream, Docker network)")


def plot_results(results, names):
    colors  = {
        "NGINX RR": "#4C72B0",
        "NGINX LC": "#DD8452",
        "HuddleCluster": "#55A868"
    }
    metrics = [("p50","P50 (ms)"), ("p95","P95 (ms)"), ("avg","Avg (ms)")]

    fig, axes = plt.subplots(len(results), len(metrics),
                              figsize=(14, 4.5 * len(results)))
    fig.suptitle(
        "HuddleCluster vs NGINX (Round Robin) vs NGINX (Least Connections)\n"
        "Containerized benchmark -- 6 upstream servers, Docker network",
        fontsize=12, fontweight="bold", y=0.99
    )

    for row, (name, (rr, lc, hc)) in enumerate(zip(names, results)):
        for col, (key, label) in enumerate(metrics):
            ax   = axes[row][col] if len(results) > 1 else axes[col]
            vals = [rr[key], lc[key], hc[key]]
            ax.bar(["NGINX RR", "NGINX LC", "HC"], vals,
                   color=list(colors.values()),
                   width=0.5, edgecolor="white", linewidth=0.8)

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

    patches = [mpatches.Patch(color=c, label=l) for l, c in colors.items()]
    fig.legend(handles=patches, loc="lower center", ncol=3,
               fontsize=10, bbox_to_anchor=(0.5, 0.01))
    fig.tight_layout(rect=[0, 0.05, 1, 0.97])
    fig.savefig("industry_benchmark_results.png", dpi=150, bbox_inches="tight")
    print("Saved: industry_benchmark_results.png")
    plt.show()


#  Main 

if __name__ == "__main__":
    print("=" * 60)
    print("  HuddleCluster Industry Baseline Benchmark")
    print("  NGINX RR vs NGINX LC vs HuddleCluster")
    print("  Requires: docker compose up -d")
    print("=" * 60)

    # Verify containers are running
    print("\nChecking containers...")
    ok1 = wait_ready(NGINX_RR_URL, "NGINX RR  (port 9001)")
    ok2 = wait_ready(NGINX_LC_URL, "NGINX LC  (port 9002)")

    if not (ok1 and ok2):
        print("\nERROR: NGINX containers not ready.")
        print("Run this first:")
        print("  cd benchmarks/")
        print("  docker compose up -d")
        sys.exit(1)
    print("  All containers ready.")

    r1 = scenario_normal()
    r2 = scenario_slow()
    r3 = scenario_failure()

    all_results = [r1, r2, r3]
    names = [
        "1. Normal Load",
        "2. Slow Server (5x at halfway)",
        "3. Server Failure (crash at halfway)",
    ]

    out = {}
    for name, (rr, lc, hc) in zip(names, all_results):
        out[name] = {
            "nginx_rr": {k: v for k, v in rr.items() if k != "latencies"},
            "nginx_lc": {k: v for k, v in lc.items() if k != "latencies"},
            "huddle":   {k: v for k, v in hc.items() if k != "latencies"},
        }
    with open("industry_benchmark_results.json", "w") as f:
        json.dump(out, f, indent=2)
    print("Saved: industry_benchmark_results.json")

    print_summary(all_results, names)
    plot_results(all_results, names)

    print("\nDone.")
    print("To stop containers: docker compose down")
