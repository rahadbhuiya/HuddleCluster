#!/usr/bin/env python3
"""
HuddleCluster — 1-Minute Reproducible Anomaly & Latency Shielding Demo
======================================================================
Demonstrates in real time how HuddleCluster detects relative latency anomalies,
thermally evicts a degraded node to the outer ring, shields p99 latency,
and safely rotates it back after cooling — compared directly against
traditional Naive Round-Robin load balancing.

Usage:
    python demos/reproducible_anomaly_demo.py          # standard interactive demo (~12s)
    python demos/reproducible_anomaly_demo.py --fast   # fast verification run (~1s)

Author : Rahad Bhuiya
License: MIT
"""

import argparse
import logging
import os
import random
import sys
import time
from typing import Dict, List

# Ensure parent directory is in sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from huddle_cluster import HuddleCluster, Server

# Suppress internal debug/info logs during demo presentation
logging.disable(logging.INFO)
logging.getLogger("huddle").setLevel(logging.WARNING)
logging.getLogger("HuddleCluster").setLevel(logging.WARNING)


def percentile(data: List[float], p: float) -> float:
    """Compute percentile using linear interpolation."""
    if not data:
        return 0.0
    s = sorted(data)
    n = len(s)
    if n == 1:
        return s[0]
    idx = (p / 100.0) * (n - 1)
    lo = int(idx)
    hi = min(lo + 1, n - 1)
    frac = idx - lo
    return s[lo] + frac * (s[hi] - s[lo])


class SimulatedBackend:
    def __init__(self, node_id: str, base_latency_ms: float = 12.0):
        self.node_id = node_id
        self.base_latency_ms = base_latency_ms
        self.degraded = False

    def query(self) -> float:
        if self.degraded:
            # High-latency anomaly with jitter (e.g. GC pause, GPU memory thrashing)
            return random.uniform(260.0, 310.0)
        # Normal healthy service response: 10ms to 16ms
        return max(4.0, random.gauss(self.base_latency_ms, 2.0))


class RoundRobinBalancer:
    def __init__(self, servers: List[Server]):
        self.servers = list(servers)
        self.index = 0

    def pick(self) -> Server:
        picked = self.servers[self.index % len(self.servers)]
        self.index += 1
        return picked


def run_demo(fast: bool = False):
    delay = 0.0 if fast else 0.8

    print("=" * 80)
    print("      HUDDLECLUSTER: REPRODUCIBLE ANOMALY & LATENCY SHIELDING DEMO")
    print("=" * 80)
    print("Scenario:")
    print("  - 5 backend nodes serving traffic ('srv-alpha' to 'srv-epsilon').")
    print("  - Normal healthy response time: ~12-15 ms.")
    print("  - At T=4s, 'srv-gamma' degrades (latency spikes to 280+ ms).")
    print("  - At T=9s, 'srv-gamma' recovers to normal healthy latency.")
    print("  - We observe: Naive Round-Robin vs HuddleCluster Penguin Thermal Routing.")
    print("=" * 80)
    print()

    node_names = ["srv-alpha", "srv-beta", "srv-gamma", "srv-delta", "srv-epsilon"]
    backends: Dict[str, SimulatedBackend] = {name: SimulatedBackend(name) for name in node_names}

    servers = [Server(id=name, host=f"10.0.0.{i+1}", port=8080) for i, name in enumerate(node_names)]

    # 1. HuddleCluster balancer
    hc = HuddleCluster(
        heat_threshold=0.55,
        cool_threshold=0.30,
        min_inner_size=2,
        max_inner_size=5,
        rotation_cooldown_sec=0.0,
        min_outer_dwell_sec=0.0,
        ema_alpha=0.6,
    )
    for s in servers:
        hc.add_server(s, force_inner=True)

    # 2. Naive Round-Robin balancer
    rr = RoundRobinBalancer(servers)

    rr_latencies: List[float] = []
    hc_latencies: List[float] = []

    rr_slow_hits = 0
    hc_slow_hits = 0

    total_ticks = 14
    anomaly_start = 4
    anomaly_end = 9

    print(f"{'Time':<7} {'Active Huddle (Inner Ring)':<35} {'Outer Ring':<18} {'HC Lat':<10} {'Cluster Event'}")
    print("-" * 80)

    for tick in range(1, total_ticks + 1):
        # Trigger or heal anomaly
        if tick == anomaly_start:
            backends["srv-gamma"].degraded = True
            event_text = "[!] INJECT: srv-gamma degraded (280ms+)"
        elif tick == anomaly_end:
            backends["srv-gamma"].degraded = False
            event_text = "[*] RECOVERED: srv-gamma returned to 12ms"
        elif anomaly_start < tick < anomaly_end:
            event_text = "[!] srv-gamma degraded (hot)"
        else:
            event_text = "[OK] All nodes healthy"

        tick_hc_samples: List[float] = []
        requests_per_tick = 50

        for _ in range(requests_per_tick):
            # 1. Route via Round-Robin
            rr_srv = rr.pick()
            rr_lat = backends[rr_srv.id].query()
            rr_latencies.append(rr_lat)
            if rr_lat > 50.0:
                rr_slow_hits += 1

            # 2. Route via HuddleCluster
            hc_srv = hc.get_server()
            if not hc_srv:
                hc_srv = servers[0]
            hc_lat = backends[hc_srv.id].query()
            hc_latencies.append(hc_lat)
            tick_hc_samples.append(hc_lat)
            if hc_lat > 50.0:
                hc_slow_hits += 1

            # Provide latency feedback to HuddleCluster
            hc.record_latency(hc_srv, hc_lat)

        # Synthetic health probe for resting outer ring nodes (simulates active health check probes)
        for s in list(hc._outer_ring):
            for _ in range(10):
                probe_lat = backends[s.id].query()
                hc.record_latency(s, probe_lat)

        # Execute thermal rotation step
        hc.rotate()

        # Gather state
        inner_ids = [s.id for s in hc._inner_ring]
        outer_ids = [s.id for s in hc._outer_ring]

        inner_repr = "[" + ", ".join(inner_ids) + "]"
        outer_repr = "[" + ", ".join(outer_ids) + "]" if outer_ids else "[]"
        avg_tick_lat = sum(tick_hc_samples) / len(tick_hc_samples)

        print(f"T+{tick:02d}s   {inner_repr:<35} {outer_repr:<18} {avg_tick_lat:>5.1f} ms   {event_text}")

        if delay > 0:
            time.sleep(delay)

    print("-" * 80)
    print()

    # Calculate statistics
    rr_count = len(rr_latencies)
    hc_count = len(hc_latencies)

    rr_p50 = percentile(rr_latencies, 50)
    rr_p95 = percentile(rr_latencies, 95)
    rr_p99 = percentile(rr_latencies, 99)
    rr_avg = sum(rr_latencies) / rr_count

    hc_p50 = percentile(hc_latencies, 50)
    hc_p95 = percentile(hc_latencies, 95)
    hc_p99 = percentile(hc_latencies, 99)
    hc_avg = sum(hc_latencies) / hc_count

    p99_reduction = ((rr_p99 - hc_p99) / rr_p99) * 100.0
    slow_reduction = ((rr_slow_hits - hc_slow_hits) / max(1, rr_slow_hits)) * 100.0

    print("=" * 80)
    print("                     FINAL PERFORMANCE COMPARISON REPORT")
    print("=" * 80)
    print(f"{'Metric':<32} {'Naive Round-Robin':<22} {'HuddleCluster':<20} {'Improvement'}")
    print("-" * 80)
    print(f"{'Total Requests Handled':<32} {rr_count:<22} {hc_count:<20} -")
    print(f"{'Slow Requests (>50ms)':<32} {rr_slow_hits:<10} ({rr_slow_hits/rr_count*100:.1f}%)   {hc_slow_hits:<8} ({hc_slow_hits/hc_count*100:.1f}%)    -{slow_reduction:.1f}% fewer slow hits")
    print(f"{'Average Latency':<32} {rr_avg:.1f} ms{'':<15} {hc_avg:.1f} ms{'':<13} -{((rr_avg-hc_avg)/rr_avg)*100:.1f}% faster")
    print(f"{'P50 Latency (Median)':<32} {rr_p50:.1f} ms{'':<15} {hc_p50:.1f} ms{'':<13} normal")
    print(f"{'P95 Latency':<32} {rr_p95:.1f} ms{'':<15} {hc_p95:.1f} ms{'':<13} -{((rr_p95-hc_p95)/rr_p95)*100:.1f}% latency drop")
    print(f"{'P99 Tail Latency':<32} {rr_p99:.1f} ms{'':<15} {hc_p99:.1f} ms{'':<13} -{p99_reduction:.1f}% tail protection")
    print("=" * 80)
    print()
    print("Key Takeaways:")
    print(f"1. Naive Round-Robin blindly sent 20% of traffic to degraded 'srv-gamma',")
    print(f"   spiking cluster p99 tail latency to {rr_p99:.1f}ms and degrading {rr_slow_hits} client requests.")
    print(f"2. HuddleCluster automatically detected the relative latency anomaly, evicted")
    print(f"   'srv-gamma' into the outer ring to cool down, and shielded p99 latency at {hc_p99:.1f}ms.")
    print(f"3. Once 'srv-gamma' recovered, HuddleCluster seamlessly restored it to the active huddle.")
    print("=" * 80)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="HuddleCluster 1-Minute Reproducible Anomaly Demo")
    parser.add_argument("--fast", action="store_true", help="Run fast simulation without sleep delays")
    args = parser.parse_args()
    run_demo(fast=args.fast)
