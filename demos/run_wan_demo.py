"""
HuddleCluster — Multi-Region WAN Latency & Degraded Network Simulation Demo
=============================================================================
Demonstrates:
  1. Setting up inter-continental WAN topologies (US-East, EU-Central, AP-South).
  2. Injecting simulated transatlantic / transpacific latency and stochastic packet loss.
  3. Testing Raft RPC communication and evaluating degraded network metrics.
"""

import os
import sys
import time

# Ensure repository root is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from huddle_cluster_pkg.wan_simulator import (
    LatencyProfile,
    PRESET_CROSS_CONTINENT,
    PRESET_DEGRADED_WAN,
    PRESET_LOCAL_LAN,
    PRESET_TRANSATLANTIC,
    PRESET_TRANSPACIFIC,
    WanLatencySimulator,
    WanPacketLossException,
)



def main():
    print("=" * 70)
    print(" HuddleCluster v4.26.0 — Multi-Region WAN Simulator Demo")
    print("=" * 70)

    # 1. Initialize simulator
    sim = WanLatencySimulator(random_seed=42)

    # 2. Configure multi-region topology
    print("\n[+] Configuring Global Inter-Region Topology:")
    sim.set_link("us-east", "eu-central", PRESET_TRANSATLANTIC)
    sim.set_link("us-east", "ap-south", PRESET_CROSS_CONTINENT)
    sim.set_link("eu-central", "ap-south", PRESET_TRANSPACIFIC)
    sim.set_link("ap-south", "ap-south", PRESET_LOCAL_LAN)

    links = sim.list_links()
    for link, conf in links.items():
        print(f"    - Link {link:<22} : {conf['mean_latency_ms']:>6.1f}ms +/- {conf['jitter_ms']:<4.1f}ms | loss={conf['packet_loss_rate']*100:.1f}%")

    # 3. Simulate burst of cross-region RPC packets
    print("\n[+] Simulating 100 cross-region Raft state replication RPC packets...")
    sent = 0
    dropped = 0
    for i in range(100):
        src, dst = ("us-east", "eu-central") if i % 2 == 0 else ("eu-central", "ap-south")
        try:
            report = sim.simulate_transmission(src, dst, payload_bytes=1024)
            if report.dropped:
                dropped += 1
            sent += 1
        except Exception as e:
            dropped += 1

    print(f"    Total simulated packets: {sent}")
    print(f"    Simulated drops:         {dropped}")


    # 4. Display aggregated telemetry report
    metrics = sim.get_metrics()
    print("\n[+] Simulation Telemetry Metrics:")
    print(f"    - Delivered packets:     {metrics['total_delivered']}/{metrics['total_transmissions']}")
    print(f"    - Overall Drop Rate:     {metrics['drop_rate_pct']:.2f}%")
    print(f"    - Average Latency:       {metrics['avg_latency_ms']:.2f}ms")

    print("\n[+] Per-Link Statistics:")
    for link_name, stat in metrics["links"].items():
        print(f"    * {link_name:<20}: avg={stat['avg_latency_ms']:>6.1f}ms | sent={stat['sent']} dropped={stat['dropped']} ({stat['drop_rate_pct']}%)")

    print("\n[OK] WAN Latency Simulation Demo finished successfully.")



if __name__ == "__main__":
    main()
