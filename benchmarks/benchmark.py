"""
HuddleCluster Benchmark Suite v1.1.0
======================================
Compares HuddleCluster vs Round Robin vs Least Connections across 4 scenarios:
  1. Normal Load     — balanced traffic, all servers healthy
  2. Slow Server     — one server degrades mid-benchmark (5x latency)
  3. Traffic Spike   — sudden 3x burst then back to normal
  4. Server Failure  — one server dies completely mid-benchmark

KEY FIX (v1.1.0):
  record_latency() is now called after every HuddleCluster request.
  This closes the feedback loop — cluster detects slow servers automatically.

Run:
  python benchmark.py

Output:
  - Console summary table with % improvement over Round Robin
  - benchmark_results.png  (4-scenario × 4-metric chart)
"""

import random
import statistics
import threading
import time

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np

import sys
sys.path.insert(0, ".")
from huddle_cluster import create_cluster, HuddleCluster, Server, ServerMetrics



# Fake Server — simulates a real upstream with tunable behaviour


class FakeServer:
    def __init__(self, server_id: str, base_latency_ms: float = 15.0):
        self.id                 = server_id
        self.base_latency       = base_latency_ms
        self.active_connections = 0
        self.total_requests     = 0
        self.is_slow            = False   # 5x latency when True
        self.is_dead            = False   # simulates crash
        self._lock              = threading.Lock()

    def handle_request(self) -> float:
        """Simulate handling one request. Returns observed latency in ms."""
        if self.is_dead:
            time.sleep(0.5)           # dead server = long timeout
            return 500.0

        with self._lock:
            self.active_connections += 1
            self.total_requests     += 1

        # Latency increases with concurrent load (realistic)
        load_penalty = self.active_connections * 2.0
        base         = self.base_latency * 5.0 if self.is_slow else self.base_latency
        latency_ms   = max(1.0, base + load_penalty + random.gauss(0, 3))
        time.sleep(latency_ms / 1000.0)

        with self._lock:
            self.active_connections -= 1
        return latency_ms

    def reset(self):
        self.active_connections = 0
        self.total_requests     = 0
        self.is_slow            = False
        self.is_dead            = False



# Balancers — Round Robin & Least Connections


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


class LeastConnections:
    def __init__(self, servers):
        self.servers = servers

    def get_server(self):
        return min(self.servers, key=lambda s: s.active_connections)



# Generic Benchmark Runner


def run_benchmark(
    get_server_fn,
    fake_servers,
    num_requests  = 400,
    concurrency   = 20,
    inject_fn     = None,    # optional: called at halfway mark
) -> dict:
    """
    Send `num_requests` requests concurrently (`concurrency` at a time).
    inject_fn(fake_servers) is called after the first half to simulate
    mid-benchmark failures / slowdowns.
    """
    latencies = []
    lock      = threading.Lock()
    halfway   = num_requests // 2
    injected  = False

    def send_request():
        server     = get_server_fn()
        fake       = next((f for f in fake_servers if f.id == server.id), fake_servers[0])
        lat        = fake.handle_request()
        with lock:
            latencies.append(lat)

    batches_done = 0
    batch_size   = concurrency

    for start in range(0, num_requests, batch_size):
        # Inject failure / slowdown halfway through
        if not injected and start >= halfway and inject_fn:
            inject_fn(fake_servers)
            injected = True

        end   = min(start + batch_size, num_requests)
        batch = [threading.Thread(target=send_request) for _ in range(end - start)]
        for t in batch: t.start()
        for t in batch: t.join()

    return {
        "latencies":    latencies,
        "p50":          np.percentile(latencies, 50),
        "p95":          np.percentile(latencies, 95),
        "p99":          np.percentile(latencies, 99),
        "avg":          statistics.mean(latencies),
        "distribution": [f.total_requests for f in fake_servers],
    }


def run_huddle_benchmark(
    cluster,
    fake_servers,
    num_requests  = 400,
    concurrency   = 20,
    inject_fn     = None,
) -> dict:
    """
    Same as run_benchmark but wires record_latency() into every request.
    This is what makes the feedback loop work in v1.1.0.
    """
    latencies = []
    lock      = threading.Lock()
    halfway   = num_requests // 2
    injected  = False

    def send_request():
        server = cluster.get_server()
        if server is None:
            return
        fake   = next((f for f in fake_servers if f.id == server.id), fake_servers[0])
        lat    = fake.handle_request()

        # ← KEY: feed observed latency back into the cluster
        cluster.record_latency(server, lat)

        with lock:
            latencies.append(lat)

    for start in range(0, num_requests, concurrency):
        if not injected and start >= halfway and inject_fn:
            inject_fn(fake_servers)
            injected = True

        end   = min(start + concurrency, num_requests)
        batch = [threading.Thread(target=send_request) for _ in range(end - start)]
        for t in batch: t.start()
        for t in batch: t.join()

    return {
        "latencies":    latencies,
        "p50":          np.percentile(latencies, 50),
        "p95":          np.percentile(latencies, 95),
        "p99":          np.percentile(latencies, 99),
        "avg":          statistics.mean(latencies),
        "distribution": [f.total_requests for f in fake_servers],
    }


def gini(counts) -> float:
    """Gini coefficient: 0 = perfect fairness, 1 = totally unfair."""
    arr   = sorted(counts)
    n     = len(arr)
    total = sum(arr)
    if total == 0:
        return 0.0
    return sum((2 * i - n - 1) * v for i, v in enumerate(arr, 1)) / (n * total)


def fresh_servers(n=6):
    servers = [FakeServer(f"s{i}", base_latency_ms=12.0 + i * 2) for i in range(n)]
    return servers


def fresh_cluster(servers, **kwargs):
    c = create_cluster(
        [(s.id, "127.0.0.1", 8080 + i) for i, s in enumerate(servers)],
        rotation_cooldown_sec=0.0,
        min_outer_dwell_sec=0.5,
        ema_alpha=0.5,
        **kwargs,
    )
    return c



# SCENARIO 1 — Normal Load


def scenario_normal():
    print("\n SCENARIO 1: Normal Load (baseline)")
    N = 400

    # Round Robin
    fake = fresh_servers()
    rr_r = run_benchmark(RoundRobin(fake).get_server, fake, N)
    rr_r["fairness"] = gini(rr_r["distribution"])

    # Least Connections
    fake = fresh_servers()
    lc_r = run_benchmark(LeastConnections(fake).get_server, fake, N)
    lc_r["fairness"] = gini(lc_r["distribution"])

    # HuddleCluster
    fake    = fresh_servers()
    cluster = fresh_cluster(fake)
    cluster.start(rotation_interval_sec=0.3)
    time.sleep(0.3)
    hc_r = run_huddle_benchmark(cluster, fake, N)
    hc_r["fairness"] = cluster.fairness_score()
    cluster.stop()

    return rr_r, lc_r, hc_r



# SCENARIO 2 — One Server Goes Slow at Halfway

def scenario_slow_server():
    print("\n SCENARIO 2: Slow Server (5x latency at halfway mark)")
    N = 400

    def inject_slow(fake_servers):
        print("   s2 is now 5x SLOW")
        fake_servers[2].is_slow = True

    # Round Robin
    fake = fresh_servers()
    rr_r = run_benchmark(RoundRobin(fake).get_server, fake, N, inject_fn=inject_slow)
    rr_r["fairness"] = gini(rr_r["distribution"])

    # Least Connections
    fake = fresh_servers()
    lc_r = run_benchmark(LeastConnections(fake).get_server, fake, N, inject_fn=inject_slow)
    lc_r["fairness"] = gini(lc_r["distribution"])

    # HuddleCluster — with record_latency feedback, cluster detects slowness
    fake    = fresh_servers()
    cluster = fresh_cluster(fake)
    cluster.start(rotation_interval_sec=0.3)
    time.sleep(0.3)
    hc_r = run_huddle_benchmark(cluster, fake, N, inject_fn=inject_slow)
    cluster.stop()
    hc_r["fairness"] = cluster.fairness_score()

    return rr_r, lc_r, hc_r



# SCENARIO 3 — Traffic Spike (3x burst at halfway)


def scenario_traffic_spike():
    print("\n  SCENARIO 3: Traffic Spike (3x burst)")
    NORMAL_BATCH   = 10
    SPIKE_BATCH    = 40
    NORMAL_ROUNDS  = 10
    SPIKE_ROUNDS   = 5

    def spiked_run(get_server_fn, fake_servers):
        latencies = []
        lock      = threading.Lock()

        def req():
            server = get_server_fn()
            fake   = next((f for f in fake_servers if f.id == server.id), fake_servers[0])
            lat    = fake.handle_request()
            with lock:
                latencies.append(lat)

        def run_batch(size):
            batch = [threading.Thread(target=req) for _ in range(size)]
            for t in batch: t.start()
            for t in batch: t.join()

        for _ in range(NORMAL_ROUNDS):
            run_batch(NORMAL_BATCH)

        print("      SPIKE!")
        for _ in range(SPIKE_ROUNDS):
            run_batch(SPIKE_BATCH)

        for _ in range(NORMAL_ROUNDS):
            run_batch(NORMAL_BATCH)

        return {
            "latencies":    latencies,
            "p50":          np.percentile(latencies, 50),
            "p95":          np.percentile(latencies, 95),
            "p99":          np.percentile(latencies, 99),
            "avg":          statistics.mean(latencies),
            "distribution": [f.total_requests for f in fake_servers],
        }

    def spiked_huddle(cluster, fake_servers):
        latencies = []
        lock      = threading.Lock()

        def req():
            server = cluster.get_server()
            if server is None:
                return
            fake = next((f for f in fake_servers if f.id == server.id), fake_servers[0])
            lat  = fake.handle_request()
            cluster.record_latency(server, lat)   # ← feedback
            with lock:
                latencies.append(lat)

        def run_batch(size):
            batch = [threading.Thread(target=req) for _ in range(size)]
            for t in batch: t.start()
            for t in batch: t.join()

        for _ in range(NORMAL_ROUNDS):
            run_batch(NORMAL_BATCH)

        print("      SPIKE!")
        for _ in range(SPIKE_ROUNDS):
            run_batch(SPIKE_BATCH)

        for _ in range(NORMAL_ROUNDS):
            run_batch(NORMAL_BATCH)

        return {
            "latencies":    latencies,
            "p50":          np.percentile(latencies, 50),
            "p95":          np.percentile(latencies, 95),
            "p99":          np.percentile(latencies, 99),
            "avg":          statistics.mean(latencies),
            "distribution": [f.total_requests for f in fake_servers],
        }

    fake = fresh_servers()
    rr_r = spiked_run(RoundRobin(fake).get_server, fake)
    rr_r["fairness"] = gini(rr_r["distribution"])

    fake = fresh_servers()
    lc_r = spiked_run(LeastConnections(fake).get_server, fake)
    lc_r["fairness"] = gini(lc_r["distribution"])

    fake    = fresh_servers()
    cluster = fresh_cluster(fake)
    cluster.start(rotation_interval_sec=0.3)
    time.sleep(0.3)
    hc_r = spiked_huddle(cluster, fake)
    cluster.stop()
    hc_r["fairness"] = cluster.fairness_score()

    return rr_r, lc_r, hc_r



# SCENARIO 4 — Server Failure (crash at halfway)


def scenario_server_failure():
    print("\n SCENARIO 4: Server Failure (s1 crashes at halfway mark)")
    N = 300

    def inject_failure(fake_servers):
        print("     ⚡ s1 is DEAD")
        fake_servers[1].is_dead = True

    # Round Robin
    fake = fresh_servers()
    rr_r = run_benchmark(RoundRobin(fake).get_server, fake, N, inject_fn=inject_failure)
    rr_r["fairness"] = gini(rr_r["distribution"])

    # Least Connections
    fake = fresh_servers()
    lc_r = run_benchmark(LeastConnections(fake).get_server, fake, N, inject_fn=inject_failure)
    lc_r["fairness"] = gini(lc_r["distribution"])

    # HuddleCluster — high latency from dead server pushes it out
    fake    = fresh_servers()
    cluster = fresh_cluster(fake)
    cluster.start(rotation_interval_sec=0.3)
    time.sleep(0.3)
    hc_r = run_huddle_benchmark(cluster, fake, N, inject_fn=inject_failure)
    cluster.stop()
    hc_r["fairness"] = cluster.fairness_score()

    return rr_r, lc_r, hc_r



# Summary Table


def print_summary(all_results, names):
    metrics = [("p50", "P50 (ms)"), ("p95", "P95 (ms)"),
               ("p99", "P99 (ms)"), ("avg", "Avg (ms)"), ("fairness", "Fairness(Gini)")]

    print("\n" + "=" * 72)
    print(f"{'':26} {'Round Robin':>12} {'Least Conn':>12} {'Huddle':>12}  {'vs RR':>6}")
    print("=" * 72)

    for name, (rr_r, lc_r, hc_r) in zip(names, all_results):
        print(f"\n  {name}")
        for key, label in metrics:
            rr_v = rr_r[key]
            lc_v = lc_r[key]
            hc_v = hc_r[key]
            if rr_v != 0:
                diff = (rr_v - hc_v) / abs(rr_v) * 100
            else:
                diff = 0.0
            marker = "GOOD" if diff > 3 else ("BAD" if diff < -3 else "OK")
            print(f"    {label:<22} {rr_v:>12.2f} {lc_v:>12.2f} {hc_v:>12.2f}  "
                  f"{marker} {diff:+.1f}%")

    print("=" * 72)
    print("  GOOD = HuddleCluster better by >3%   BAD = worse by >3%   OK = similar")



# Charts


def plot_results(all_results, names):
    COLORS = {
        "Round Robin":   "#4C72B0",
        "Least Conn":    "#DD8452",
        "HuddleCluster": "#55A868",
    }
    metrics = [("p50", "P50 Latency (ms)"), ("p95", "P95 Latency (ms)"),
               ("p99", "P99 Latency (ms)"), ("avg", "Avg Latency (ms)")]

    n_scenarios = len(all_results)
    n_metrics   = len(metrics)
    fig, axes   = plt.subplots(n_scenarios, n_metrics, figsize=(22, 4.5 * n_scenarios))
    fig.suptitle(
        "HuddleCluster v1.1.0 — Benchmark Results\n"
        "With record_latency() Feedback Loop",
        fontsize=15, fontweight="bold", y=0.98,
    )

    for row, (name, (rr_r, lc_r, hc_r)) in enumerate(zip(names, all_results)):
        for col, (key, label) in enumerate(metrics):
            ax   = axes[row][col]
            vals = [rr_r[key], lc_r[key], hc_r[key]]
            bars = ax.bar(
                ["RR", "LC", "HC"], vals,
                color=[COLORS["Round Robin"], COLORS["Least Conn"], COLORS["HuddleCluster"]],
                edgecolor="white", linewidth=0.8, width=0.55,
            )

            # % label on Huddle bar
            base = vals[0]
            if base > 0:
                pct   = (base - vals[2]) / base * 100
                color = "#2e7d32" if pct > 0 else "#c62828"
                ax.text(2, vals[2] + max(vals) * 0.03,
                        f"{pct:+.1f}%", ha="center", fontsize=8.5,
                        color=color, fontweight="bold")

            if col == 0:
                ax.set_ylabel(name, fontsize=9.5, fontweight="bold", labelpad=6)
            ax.set_title(label, fontsize=9)
            ax.set_ylim(0, max(vals) * 1.28)
            ax.spines[["top", "right"]].set_visible(False)
            ax.tick_params(axis="x", labelsize=8)

    # Distribution subplot row (extra row)
    fig2, axes2 = plt.subplots(1, n_scenarios, figsize=(22, 4))
    fig2.suptitle("Request Distribution per Server (Fairness)", fontsize=13, fontweight="bold")
    server_labels = [f"S{i}" for i in range(6)]
    x = np.arange(6)

    for col, (name, (rr_r, lc_r, hc_r)) in enumerate(zip(names, all_results)):
        ax = axes2[col]
        ax.bar(x - 0.25, rr_r["distribution"], 0.25,
               label="Round Robin", color=COLORS["Round Robin"])
        ax.bar(x,         lc_r["distribution"], 0.25,
               label="Least Conn",  color=COLORS["Least Conn"])
        ax.bar(x + 0.25, hc_r["distribution"], 0.25,
               label="HuddleCluster", color=COLORS["HuddleCluster"])
        ax.set_title(name, fontsize=9.5, fontweight="bold")
        ax.set_xticks(x)
        ax.set_xticklabels(server_labels, fontsize=8)
        ax.spines[["top", "right"]].set_visible(False)
        if col == 0:
            ax.legend(fontsize=8)

    # Shared legend for latency chart
    patches = [mpatches.Patch(color=c, label=l) for l, c in COLORS.items()]
    fig.legend(handles=patches, loc="lower center", ncol=3,
               fontsize=10, bbox_to_anchor=(0.5, 0.01))

    fig.tight_layout(rect=[0, 0.05, 1, 0.96])
    fig2.tight_layout()

    fig.savefig("benchmark_results.png",     dpi=150, bbox_inches="tight")
    fig2.savefig("benchmark_fairness.png",   dpi=150, bbox_inches="tight")

    print("\n Saved: benchmark_results.png")
    print(" Saved: benchmark_fairness.png")
    plt.show()



# Main


if __name__ == "__main__":
    print("=" * 60)
    print("   HuddleCluster Benchmark Suite v1.1.0")
    print("  4 scenarios — ~8-12 minutes total")
    print("=" * 60)

    r1 = scenario_normal()
    r2 = scenario_slow_server()
    r3 = scenario_traffic_spike()
    r4 = scenario_server_failure()

    all_results = [r1, r2, r3, r4]
    names       = [
        "1. Normal Load",
        "2. Slow Server (5x at halfway)",
        "3. Traffic Spike (3x burst)",
        "4. Server Failure (crash at halfway)",
    ]

    print_summary(all_results, names)
    plot_results(all_results, names)
