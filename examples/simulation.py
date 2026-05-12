"""
simulation.py — Terminal visual simulation of HuddleCluster penguin rotation
=============================================================================
Watch servers migrate between inner and outer rings in real time.

Usage
-----
    python simulation.py                  # 8 servers, default settings
    python simulation.py --servers 12     # custom server count
    python simulation.py --cycles 30      # run for 30 rotation cycles then exit
    python simulation.py --interval 0.5   # rotate every 0.5 seconds

Dependencies
------------
    pip install rich          # for coloured terminal output
    # Falls back to plain-text output if rich is not installed
"""

from __future__ import annotations

import argparse
import math
import random
import sys
import time
from dataclasses import dataclass
from typing import List

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from huddle_cluster import HuddleCluster, Position, Server, ServerMetrics

# Rich optional import

try:
    from rich.console import Console
    from rich.live   import Live
    from rich.table  import Table
    from rich        import box
    _RICH = True
except ImportError:
    _RICH = False



# Simulation helpers


_LOAD_PROFILES = [
    "idle",       # cpu ≈ 0.05
    "normal",     # cpu ≈ 0.35
    "busy",       # cpu ≈ 0.65
    "critical",   # cpu ≈ 0.90
]


@dataclass
class SimServer:
    server: Server
    profile: str
    phase_offset: float   # seconds offset for sinusoidal load curve


def _make_sim_servers(n: int) -> List[SimServer]:
    sims = []
    for i in range(n):
        s = Server(id=f"srv-{i:02d}", host=f"10.0.0.{i+1}", port=8080)
        s.metrics = ServerMetrics()
        sims.append(SimServer(
            server=s,
            profile=random.choice(_LOAD_PROFILES),
            phase_offset=random.uniform(0, math.pi * 2),
        ))
    return sims


def _update_metrics(sim: SimServer, t: float) -> None:
    """Generate realistic-looking sinusoidal load per server."""
    base_loads = {
        "idle":     0.05,
        "normal":   0.35,
        "busy":     0.65,
        "critical": 0.90,
    }
    base = base_loads[sim.profile]
    # Sinusoidal oscillation ± 0.15 + small noise
    cpu = base + 0.15 * math.sin(t + sim.phase_offset) + random.gauss(0, 0.02)
    cpu = max(0.0, min(1.0, cpu))

    sim.server.metrics.cpu_usage    = cpu
    sim.server.metrics.memory_usage = max(0.0, min(1.0, cpu * 0.8 + random.gauss(0, 0.03)))
    sim.server.metrics.active_connections = int(cpu * 800)
    sim.server.metrics.avg_response_ms    = 50 + cpu * 500 + random.uniform(-10, 10)
    sim.server.metrics.error_rate         = max(0.0, (cpu - 0.7) * 0.2) if cpu > 0.7 else 0.0
    sim.server.metrics.is_healthy         = random.random() > 0.02   # 98 % healthy

    # Occasionally shift load profile to simulate traffic patterns
    if random.random() < 0.01:
        sim.profile = random.choice(_LOAD_PROFILES)

    sim.server.update_temperature()



# Rich renderer


def _temp_bar(temp: float, width: int = 20) -> str:
    """ASCII progress bar for temperature."""
    filled = int(temp * width)
    bar = "█" * filled + "░" * (width - filled)
    return bar


def _build_rich_table(cluster: HuddleCluster, sims: List[SimServer], cycle: int) -> Table:
    report = cluster.health_report()

    table = Table(
        title=f"🐧  HuddleCluster Simulation  — Cycle {cycle}  "
              f"[dim]fairness={report['fairness_score']:.3f}  "
              f"status={report['status']}[/dim]",
        box=box.ROUNDED,
        show_lines=True,
        style="bold",
    )
    table.add_column("Server",    style="cyan bold",  min_width=10)
    table.add_column("Position",  style="white",      min_width=8)
    table.add_column("Temp",      min_width=24)
    table.add_column("CPU",       min_width=6)
    table.add_column("Profile",   min_width=10)
    table.add_column("Rotations", min_width=9, justify="right")
    table.add_column("Inner (s)", min_width=9, justify="right")

    sim_by_id = {sim.server.id: sim for sim in sims}

    # Inner ring first
    for entry in report["inner_ring"]:
        sid = entry["id"]
        sim = sim_by_id.get(sid)
        temp = entry["temp"]
        bar = _temp_bar(temp)
        table.add_row(
            sid,
            "[green]● INNER[/green]",
            f"[green]{bar}[/green] {temp:.3f}",
            f"{sim.server.metrics.cpu_usage*100:.0f}%" if sim else "—",
            sim.profile if sim else "—",
            str(entry["rotations"]),
            f"{entry['inner_time_sec']:.1f}",
        )

    # Outer ring
    for entry in report["outer_ring"]:
        sid = entry["id"]
        sim = sim_by_id.get(sid)
        temp = entry["temp"]
        bar = _temp_bar(temp)
        colour = "red" if temp > 0.6 else "yellow"
        table.add_row(
            sid,
            f"[{colour}]○ OUTER[/{colour}]",
            f"[{colour}]{bar}[/{colour}] {temp:.3f}",
            f"{sim.server.metrics.cpu_usage*100:.0f}%" if sim else "—",
            sim.profile if sim else "—",
            str(sim.server.rotation_count if sim else "—"),
            "—",
        )

    return table



# Plain-text renderer (no rich)


def _plain_render(cluster: HuddleCluster, cycle: int) -> None:
    report = cluster.health_report()
    print(f"\n{'='*60}")
    print(f"  Cycle {cycle:4d} | status={report['status']} | "
          f"fairness={report['fairness_score']:.3f}")
    print(f"{'='*60}")
    print(f"  {'ID':<12} {'Ring':<8} {'Temp':>6}  {'CPU':>6}")
    print(f"  {'-'*40}")
    for e in report["inner_ring"]:
        print(f"  {e['id']:<12} {'INNER':<8} {e['temp']:>6.3f}  —")
    for e in report["outer_ring"]:
        print(f"  {e['id']:<12} {'OUTER':<8} {e['temp']:>6.3f}  —")
    sys.stdout.flush()



# Main simulation loop


def run_simulation(
    n_servers:   int   = 8,
    max_cycles:  int   = 0,        # 0 = run forever
    interval:    float = 1.0,
) -> None:
    sims = _make_sim_servers(n_servers)

    cluster = HuddleCluster(
        heat_threshold=0.75,
        cool_threshold=0.30,
        min_inner_size=2,
        max_inner_size=max(2, n_servers // 2),
        rotation_cooldown_sec=2.0,
        min_outer_dwell_sec=3.0,
    )

    for i, sim in enumerate(sims):
        cluster.add_server(
            sim.server,
            force_inner=(i < cluster.max_inner_size),
        )

    cycle = 0
    t0 = time.monotonic()

    if _RICH:
        console = Console()
        with Live(console=console, refresh_per_second=4) as live:
            while True:
                t = time.monotonic() - t0
                for sim in sims:
                    _update_metrics(sim, t)
                cluster.rotate()
                cycle += 1
                live.update(_build_rich_table(cluster, sims, cycle))
                if max_cycles and cycle >= max_cycles:
                    break
                time.sleep(interval)
    else:
        while True:
            t = time.monotonic() - t0
            for sim in sims:
                _update_metrics(sim, t)
            cluster.rotate()
            cycle += 1
            _plain_render(cluster, cycle)
            if max_cycles and cycle >= max_cycles:
                break
            time.sleep(interval)

    print("\n🐧 Simulation complete.")
    print(f"   Total cycles    : {cycle}")
    print(f"   Total rotations : {sum(s.rotation_count for s in cluster.all_servers())}")
    print(f"   Fairness score  : {cluster.fairness_score():.4f}")



# CLI


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="HuddleCluster terminal simulation",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--servers",  type=int,   default=8,   help="Number of servers")
    parser.add_argument("--cycles",   type=int,   default=0,   help="Max cycles (0 = infinite)")
    parser.add_argument("--interval", type=float, default=1.0, help="Seconds between rotations")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    try:
        run_simulation(
            n_servers=args.servers,
            max_cycles=args.cycles,
            interval=args.interval,
        )
    except KeyboardInterrupt:
        print("\n🐧 Simulation interrupted.")