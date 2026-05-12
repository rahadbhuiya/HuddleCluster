"""
HuddleCluster Statistical Benchmark
=====================================
Runs each scenario N_TRIALS times, computes mean ± std and 95% confidence
intervals, and performs Welch's t-test against Round Robin baseline.

Outputs:
  statistical_results.json   — raw numbers for paper
  statistical_summary.png    — bar chart with error bars
  convergence_analysis.png   — how fast does HuddleCluster detect slow server?
  overhead_analysis.png      — CPU/memory/rotation cost measurement

Run:
  python benchmark_statistical.py

Takes ~20-30 minutes for N_TRIALS=10.
Set N_TRIALS=5 for a quick run (~10 min).
"""

import json
import sys
import time
import threading
import statistics
import random
import os
import tracemalloc

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from scipy import stats

sys.path.insert(0, ".")
from benchmark import (
    fresh_servers, fresh_cluster, run_benchmark, run_huddle_benchmark,
    RoundRobin, LeastConnections, gini
)
from huddle_cluster import create_cluster

#  Config 
N_TRIALS    = 10     # Set to 5 for quick run, 30 for publication
N_REQUESTS  = 300
CONCURRENCY = 20
ALPHA       = 0.05   # significance level


# 1. Multi-trial runner


def run_trial(scenario_fn):
    """Run one trial of a scenario. Returns (rr, lc, hc) result dicts."""
    return scenario_fn()


def run_trials(scenario_fn, n=N_TRIALS, label=""):
    """Run scenario n times, collect all latency arrays."""
    rr_all, lc_all, hc_all = [], [], []
    for i in range(n):
        print(f"  Trial {i+1}/{n}", end="\r", flush=True)
        rr_r, lc_r, hc_r = scenario_fn()
        rr_all.append(rr_r)
        lc_all.append(lc_r)
        hc_all.append(hc_r)
    print(f"  {label} — {n} trials done     ")
    return rr_all, lc_all, hc_all


def aggregate(results, key):
    """Extract a metric across trials → list of floats."""
    return [r[key] for r in results]


def stats_summary(values):
    """Return dict with mean, std, ci95_lo, ci95_hi."""
    arr  = np.array(values)
    mean = float(np.mean(arr))
    std  = float(np.std(arr, ddof=1))
    n    = len(arr)
    se   = std / np.sqrt(n)
    t_crit = stats.t.ppf(1 - ALPHA / 2, df=n - 1)
    return {
        "mean":    round(mean, 3),
        "std":     round(std,  3),
        "ci95_lo": round(mean - t_crit * se, 3),
        "ci95_hi": round(mean + t_crit * se, 3),
        "n":       n,
    }


def welch_t(a_vals, b_vals):
    """Welch's t-test: returns (t_stat, p_value, significant)."""
    t_stat, p_val = stats.ttest_ind(a_vals, b_vals, equal_var=False)
    return round(float(t_stat), 4), round(float(p_val), 4), bool(p_val < ALPHA)


def scenario_normal_small():
    """Normal load — small N for speed in multi-trial."""
    from benchmark import FakeServer, fresh_cluster, gini
    import time

    def mk():
        return [FakeServer(f"s{i}", 12.0 + i * 2) for i in range(6)]

    fake = mk()
    rr_r = run_benchmark(RoundRobin(fake).get_server, fake, N_REQUESTS)
    rr_r["fairness"] = gini(rr_r["distribution"])

    fake = mk()
    lc_r = run_benchmark(LeastConnections(fake).get_server, fake, N_REQUESTS)
    lc_r["fairness"] = gini(lc_r["distribution"])

    fake    = mk()
    cluster = fresh_cluster(fake)
    cluster.start(0.3)
    time.sleep(0.3)
    hc_r = run_huddle_benchmark(cluster, fake, N_REQUESTS)
    hc_r["fairness"] = cluster.fairness_score()
    cluster.stop()

    return rr_r, lc_r, hc_r


def scenario_slow_small():
    """Slow server — small N."""
    from benchmark import FakeServer, fresh_cluster, gini
    import time

    def mk():
        return [FakeServer(f"s{i}", 12.0) for i in range(6)]

    def inject(fakes):
        fakes[2].is_slow = True

    fake = mk()
    rr_r = run_benchmark(RoundRobin(fake).get_server, fake, N_REQUESTS, inject_fn=inject)
    rr_r["fairness"] = gini(rr_r["distribution"])

    fake = mk()
    lc_r = run_benchmark(LeastConnections(fake).get_server, fake, N_REQUESTS, inject_fn=inject)
    lc_r["fairness"] = gini(lc_r["distribution"])

    fake    = mk()
    cluster = fresh_cluster(fake)
    cluster.start(0.3)
    time.sleep(0.3)
    hc_r = run_huddle_benchmark(cluster, fake, N_REQUESTS, inject_fn=inject)
    hc_r["fairness"] = cluster.fairness_score()
    cluster.stop()

    return rr_r, lc_r, hc_r


def scenario_failure_small():
    """Server failure — small N."""
    from benchmark import FakeServer, fresh_cluster, gini
    import time

    def mk():
        return [FakeServer(f"s{i}", 12.0) for i in range(6)]

    def inject(fakes):
        fakes[1].is_dead = True

    fake = mk()
    rr_r = run_benchmark(RoundRobin(fake).get_server, fake, N_REQUESTS, inject_fn=inject)
    rr_r["fairness"] = gini(rr_r["distribution"])

    fake = mk()
    lc_r = run_benchmark(LeastConnections(fake).get_server, fake, N_REQUESTS, inject_fn=inject)
    lc_r["fairness"] = gini(lc_r["distribution"])

    fake    = mk()
    cluster = fresh_cluster(fake)
    cluster.start(0.3)
    time.sleep(0.3)
    hc_r = run_huddle_benchmark(cluster, fake, N_REQUESTS, inject_fn=inject)
    hc_r["fairness"] = cluster.fairness_score()
    cluster.stop()

    return rr_r, lc_r, hc_r



# 2. Convergence Analysis


def convergence_analysis(n_reps=5):
    """
    Measure: after slow injection, how many requests does it take
    before HuddleCluster evicts the slow server?
    Returns list of (requests_to_detect) across repetitions.
    """
    from benchmark import FakeServer, fresh_cluster
    import time, threading

    detection_counts = []

    for rep in range(n_reps):
        fake    = [FakeServer(f"s{i}", 12.0) for i in range(6)]
        cluster = fresh_cluster(fake)
        cluster.start(0.3)
        time.sleep(0.3)

        servers_map = {s.id: s for s in cluster.all_servers()}
        lock        = threading.Lock()
        total_reqs  = [0]
        detected_at = [None]

        # Phase 1: warm up — 30 normal requests
        def req_normal():
            server = cluster.get_server()
            fk     = next(f for f in fake if f.id == server.id)
            lat    = fk.handle_request()
            cluster.record_latency(server, lat)

        for _ in range(3):
            batch = [threading.Thread(target=req_normal) for _ in range(10)]
            for t in batch: t.start()
            for t in batch: t.join()
            total_reqs[0] += 10

        # Inject slow
        fake[2].is_slow = True
        inject_at = total_reqs[0]

        # Phase 2: keep sending until s2 is evicted
        for _ in range(100):
            batch = [threading.Thread(target=req_normal) for _ in range(5)]
            for t in batch: t.start()
            for t in batch: t.join()
            total_reqs[0] += 5

            s2 = servers_map.get("s2")
            if s2 and s2.position.value == "outer":
                detected_at[0] = total_reqs[0] - inject_at
                break

        cluster.stop()
        if detected_at[0]:
            detection_counts.append(detected_at[0])
            print(f"  Rep {rep+1}: detected after {detected_at[0]} requests post-injection")
        else:
            print(f"  Rep {rep+1}: NOT detected within 500 requests")

    return detection_counts



# 3. Overhead Measurement


def measure_overhead():
    """
    Measure the overhead of HuddleCluster's get_server() + record_latency()
    vs plain Round Robin. Returns microseconds per call.
    """
    from benchmark import FakeServer, fresh_cluster
    import time

    N = 10_000

    # Round Robin overhead
    fake = [FakeServer(f"s{i}", 0.0) for i in range(6)]
    rr   = RoundRobin(fake)
    t0   = time.perf_counter()
    for _ in range(N):
        rr.get_server()
    rr_us = (time.perf_counter() - t0) / N * 1_000_000

    # HuddleCluster overhead (get_server only)
    cluster = fresh_cluster(fake)
    cluster.start(0.3)
    time.sleep(0.3)

    t0 = time.perf_counter()
    for _ in range(N):
        cluster.get_server()
    hc_get_us = (time.perf_counter() - t0) / N * 1_000_000

    # HuddleCluster overhead (get_server + record_latency)
    t0 = time.perf_counter()
    for _ in range(N):
        s = cluster.get_server()
        if s:
            cluster.record_latency(s, 15.0)
    hc_full_us = (time.perf_counter() - t0) / N * 1_000_000

    cluster.stop()

    return {
        "rr_get_us":      round(rr_us, 3),
        "hc_get_us":      round(hc_get_us, 3),
        "hc_full_us":     round(hc_full_us, 3),
        "overhead_ratio": round(hc_full_us / rr_us, 2),
    }


def measure_memory():
    """Measure peak memory of cluster with 100 servers over 1000 rotations."""
    from benchmark import FakeServer, fresh_cluster
    import time, tracemalloc

    tracemalloc.start()
    fake    = [FakeServer(f"s{i}", 12.0) for i in range(20)]
    cluster = fresh_cluster(fake)
    cluster.start(0.3)

    for _ in range(50):
        s = cluster.get_server()
        if s:
            cluster.record_latency(s, random.uniform(10, 30))
        time.sleep(0.01)

    cluster.stop()
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return {"current_kb": round(current / 1024, 1), "peak_kb": round(peak / 1024, 1)}



# 4. Plot: Error Bars


def plot_statistical(results_json):
    data     = results_json["scenarios"]
    scenarios = list(data.keys())
    metrics   = ["p95", "avg"]
    m_labels  = {"p95": "P95 Latency (ms)", "avg": "Avg Latency (ms)"}
    colors    = {"rr": "#4C72B0", "lc": "#DD8452", "hc": "#55A868"}
    labels    = {"rr": "Round Robin", "lc": "Least Connections", "hc": "HuddleCluster"}

    fig, axes = plt.subplots(len(scenarios), len(metrics),
                              figsize=(14, 4.5 * len(scenarios)))
    fig.suptitle(
        f"HuddleCluster Statistical Benchmark ({N_TRIALS} trials, 95% CI)\n"
        "Error bars show 95% confidence interval",
        fontsize=13, fontweight="bold", y=0.99
    )

    for row, scenario in enumerate(scenarios):
        sc_data = data[scenario]
        for col, metric in enumerate(metrics):
            ax = axes[row][col] if len(scenarios) > 1 else axes[col]

            x      = np.arange(3)
            algos  = ["rr", "lc", "hc"]
            means  = [sc_data[a][metric]["mean"] for a in algos]
            ci_los = [sc_data[a][metric]["mean"] - sc_data[a][metric]["ci95_lo"] for a in algos]
            ci_his = [sc_data[a][metric]["ci95_hi"] - sc_data[a][metric]["mean"] for a in algos]

            bars = ax.bar(
                x, means,
                color=[colors[a] for a in algos],
                width=0.55, edgecolor="white", linewidth=0.8,
                yerr=[ci_los, ci_his], capsize=5, error_kw={"linewidth": 1.5, "color": "#333333"}
            )

            # p-value annotation on HC bar
            p_info = sc_data.get("ttest_rr_vs_hc", {}).get(metric, {})
            p_val  = p_info.get("p_value", 1.0)
            sig    = p_info.get("significant", False)
            color  = "#2e7d32" if sig else "#999999"
            label  = f"p={p_val:.3f}{'*' if sig else ''}"
            ax.text(2, means[2] + max(means) * 0.06, label,
                    ha="center", fontsize=8, color=color, fontweight="bold")

            if col == 0:
                ax.set_ylabel(scenario, fontsize=9.5, fontweight="bold")
            ax.set_title(m_labels[metric], fontsize=9)
            ax.set_xticks(x)
            ax.set_xticklabels(["RR", "LC", "HC"], fontsize=9)
            ax.set_ylim(0, max(means) * 1.35)
            ax.spines[["top", "right"]].set_visible(False)

    patches = [mpatches.Patch(color=c, label=labels[a]) for a, c in colors.items()]
    fig.legend(handles=patches, loc="lower center", ncol=3, fontsize=10,
               bbox_to_anchor=(0.5, 0.01))
    fig.tight_layout(rect=[0, 0.05, 1, 0.97])
    fig.savefig("statistical_summary.png", dpi=150, bbox_inches="tight")
    print("  Saved: statistical_summary.png")
    plt.close()


def plot_convergence(detection_counts):
    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    fig.suptitle("HuddleCluster Convergence Analysis", fontsize=13, fontweight="bold")

    # Left: detection counts
    ax = axes[0]
    ax.bar(range(1, len(detection_counts) + 1), detection_counts,
           color="#55A868", edgecolor="white")
    mean_d = np.mean(detection_counts)
    ax.axhline(mean_d, color="#c62828", linestyle="--", linewidth=1.5,
               label=f"Mean = {mean_d:.1f} requests")
    ax.set_xlabel("Trial")
    ax.set_ylabel("Requests after injection until eviction")
    ax.set_title("Slow Server Detection Speed")
    ax.legend(fontsize=9)
    ax.spines[["top", "right"]].set_visible(False)

    # Right: temperature trace for one trial
    ax2 = axes[1]
    from benchmark import FakeServer, fresh_cluster
    import threading as th

    fake    = [FakeServer(f"s{i}", 12.0) for i in range(6)]
    cluster = fresh_cluster(fake)
    cluster.start(0.3)
    time.sleep(0.3)

    temps      = []
    anomalies  = []
    s2_server  = next(s for s in cluster.all_servers() if s.id == "s2")
    lock       = th.Lock()

    def req():
        server = cluster.get_server()
        fk     = next(f for f in fake if f.id == server.id)
        lat    = fk.handle_request()
        cluster.record_latency(server, lat)
        with lock:
            temps.append(s2_server.temperature)
            anomalies.append(s2_server.metrics.latency_anomaly_score)

    # 20 normal requests
    for _ in range(4):
        batch = [th.Thread(target=req) for _ in range(5)]
        for t in batch: t.start()
        for t in batch: t.join()

    inject_point = len(temps)
    fake[2].is_slow = True

    # 80 more
    for _ in range(16):
        batch = [th.Thread(target=req) for _ in range(5)]
        for t in batch: t.start()
        for t in batch: t.join()

    cluster.stop()

    x_vals = list(range(len(temps)))
    ax2.plot(x_vals, temps, color="#4C72B0", linewidth=1.8, label="Temperature T(s2)")
    ax2.plot(x_vals, [a * 0.70 for a in anomalies], color="#DD8452",
             linewidth=1.5, linestyle="--", label="Anomaly × W_RESP")
    ax2.axvline(inject_point, color="#c62828", linestyle=":", linewidth=1.5,
                label="Slow injection")
    ax2.axhline(0.55, color="#555555", linestyle="--", linewidth=1,
                label="Heat threshold (0.55)")
    ax2.set_xlabel("Total requests")
    ax2.set_ylabel("Score (0–1)")
    ax2.set_title("Temperature Evolution During Degradation")
    ax2.legend(fontsize=8)
    ax2.spines[["top", "right"]].set_visible(False)

    fig.tight_layout()
    fig.savefig("convergence_analysis.png", dpi=150, bbox_inches="tight")
    print("  Saved: convergence_analysis.png")
    plt.close()


def plot_overhead(overhead, memory):
    fig, axes = plt.subplots(1, 2, figsize=(11, 4))
    fig.suptitle("HuddleCluster Overhead Analysis", fontsize=13, fontweight="bold")

    # Latency overhead
    ax = axes[0]
    labels = ["RR\nget_server()", "HC\nget_server()", "HC\nget+record"]
    vals   = [overhead["rr_get_us"], overhead["hc_get_us"], overhead["hc_full_us"]]
    colors = ["#4C72B0", "#55A868", "#55A868"]
    bars   = ax.bar(labels, vals, color=colors, edgecolor="white", width=0.5)
    for bar, val in zip(bars, vals):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.05,
                f"{val:.2f} μs", ha="center", fontsize=9, fontweight="bold")
    ax.set_ylabel("Time per call (μs)")
    ax.set_title(f"Routing Overhead per Request\n(overhead ratio HC/RR = {overhead['overhead_ratio']}x)")
    ax.spines[["top", "right"]].set_visible(False)

    # Memory
    ax2 = axes[1]
    mem_labels = ["Current\nmemory", "Peak\nmemory"]
    mem_vals   = [memory["current_kb"], memory["peak_kb"]]
    ax2.bar(mem_labels, mem_vals, color="#7B68EE", edgecolor="white", width=0.4)
    for i, val in enumerate(mem_vals):
        ax2.text(i, val + 1, f"{val} KB", ha="center", fontsize=10, fontweight="bold")
    ax2.set_ylabel("Memory (KB)")
    ax2.set_title("Memory Usage\n(20 servers, 50 request cycles)")
    ax2.spines[["top", "right"]].set_visible(False)

    fig.tight_layout()
    fig.savefig("overhead_analysis.png", dpi=150, bbox_inches="tight")
    print("  Saved: overhead_analysis.png")
    plt.close()



# Main


if __name__ == "__main__":
    print("=" * 60)
    print(f"   HuddleCluster Statistical Benchmark")
    print(f"  {N_TRIALS} trials × 3 scenarios")
    print(f"  This will take ~{N_TRIALS * 3} minutes")
    print("=" * 60)

    all_results = {}

    #  Scenario 1: Normal 
    print("\n  Scenario 1/3: Normal Load")
    rr_t, lc_t, hc_t = run_trials(scenario_normal_small, N_TRIALS, "Normal")

    sc1 = {}
    for algo, trials in [("rr", rr_t), ("lc", lc_t), ("hc", hc_t)]:
        sc1[algo] = {
            m: stats_summary(aggregate(trials, m))
            for m in ["p50", "p95", "p99", "avg", "fairness"]
        }
    sc1["ttest_rr_vs_hc"] = {
        m: dict(zip(["t_stat", "p_value", "significant"],
                    welch_t(aggregate(rr_t, m), aggregate(hc_t, m))))
        for m in ["p50", "p95", "p99", "avg"]
    }
    sc1["ttest_rr_vs_lc"] = {
        m: dict(zip(["t_stat", "p_value", "significant"],
                    welch_t(aggregate(rr_t, m), aggregate(lc_t, m))))
        for m in ["p50", "p95", "p99", "avg"]
    }
    all_results["Normal Load"] = sc1

    #  Scenario 2: Slow Server 
    print("\n  Scenario 2/3: Slow Server")
    rr_t, lc_t, hc_t = run_trials(scenario_slow_small, N_TRIALS, "Slow Server")

    sc2 = {}
    for algo, trials in [("rr", rr_t), ("lc", lc_t), ("hc", hc_t)]:
        sc2[algo] = {
            m: stats_summary(aggregate(trials, m))
            for m in ["p50", "p95", "p99", "avg", "fairness"]
        }
    sc2["ttest_rr_vs_hc"] = {
        m: dict(zip(["t_stat", "p_value", "significant"],
                    welch_t(aggregate(rr_t, m), aggregate(hc_t, m))))
        for m in ["p50", "p95", "p99", "avg"]
    }
    all_results["Slow Server (5x)"] = sc2

    #  Scenario 3: Server Failure 
    print("\n  Scenario 3/3: Server Failure")
    rr_t, lc_t, hc_t = run_trials(scenario_failure_small, N_TRIALS, "Failure")

    sc3 = {}
    for algo, trials in [("rr", rr_t), ("lc", lc_t), ("hc", hc_t)]:
        sc3[algo] = {
            m: stats_summary(aggregate(trials, m))
            for m in ["p50", "p95", "p99", "avg", "fairness"]
        }
    sc3["ttest_rr_vs_hc"] = {
        m: dict(zip(["t_stat", "p_value", "significant"],
                    welch_t(aggregate(rr_t, m), aggregate(hc_t, m))))
        for m in ["p50", "p95", "p99", "avg"]
    }
    all_results["Server Failure"] = sc3

    #  Save JSON 
    output = {"n_trials": N_TRIALS, "n_requests": N_REQUESTS,
              "alpha": ALPHA, "scenarios": all_results}
    with open("statistical_results.json", "w") as f:
        json.dump(output, f, indent=2)
    print("\n Saved: statistical_results.json")

    #  Print Summary 
    print("\n" + "=" * 72)
    print(f"{'':28} {'RR mean±std':>14} {'LC mean±std':>14} {'HC mean±std':>14}  p-val")
    print("=" * 72)
    for sc_name, sc_data in all_results.items():
        print(f"\n  {sc_name}")
        for m in ["p50", "p95", "avg"]:
            rr_s = sc_data["rr"][m]
            lc_s = sc_data["lc"][m]
            hc_s = sc_data["hc"][m]
            p    = sc_data["ttest_rr_vs_hc"][m]["p_value"]
            sig  = "*" if sc_data["ttest_rr_vs_hc"][m]["significant"] else " "
            print(f"    {m:8} "
                  f"{rr_s['mean']:6.1f}±{rr_s['std']:5.1f}   "
                  f"{lc_s['mean']:6.1f}±{lc_s['std']:5.1f}   "
                  f"{hc_s['mean']:6.1f}±{hc_s['std']:5.1f}   "
                  f"p={p:.3f}{sig}")
    print("=" * 72)
    print("  * = statistically significant (p < 0.05, Welch's t-test)")

    #  Charts 
    print("\n  Generating charts...")
    plot_statistical(output)

    #  Convergence 
    print("\n   Convergence analysis (5 reps)...")
    det = convergence_analysis(n_reps=5)
    if det:
        print(f"  Mean detection: {np.mean(det):.1f} requests "
              f"(range {min(det)}–{max(det)})")
    plot_convergence(det)

    #  Overhead 
    print("\n   Measuring overhead...")
    overhead = measure_overhead()
    memory   = measure_memory()
    print(f"  RR get_server():          {overhead['rr_get_us']:.3f} μs")
    print(f"  HC get_server():          {overhead['hc_get_us']:.3f} μs")
    print(f"  HC get + record_latency(): {overhead['hc_full_us']:.3f} μs")
    print(f"  Overhead ratio:           {overhead['overhead_ratio']}x vs RR")
    print(f"  Peak memory (20 servers): {memory['peak_kb']} KB")
    plot_overhead(overhead, memory)

    print("\n  Done!")
    print("   Files: statistical_results.json, statistical_summary.png,")
    print("          convergence_analysis.png, overhead_analysis.png")
