<p align="center">
  <img src="https://raw.githubusercontent.com/rahadbhuiya/HuddleCluster/main/assets/logo.svg" width="380" alt="HuddleCluster"/>
</p>

<p align="center">
  <a href="https://pypi.org/project/huddle-cluster/"><img src="https://img.shields.io/pypi/v/huddle-cluster?color=0e7a0e&label=PyPI" alt="PyPI version"/></a>
  <a href="https://pypi.org/project/huddle-cluster/"><img src="https://img.shields.io/pypi/pyversions/huddle-cluster?color=0e7a0e" alt="Python versions"/></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue" alt="MIT License"/></a>
  <a href="https://doi.org/10.5281/zenodo.20348019"><img src="https://img.shields.io/badge/DOI-10.5281%2Fzenodo.20348019-blue" alt="DOI"/></a>
  <a href="https://github.com/rahadbhuiya/HuddleCluster/actions"><img src="https://img.shields.io/github/actions/workflow/status/rahadbhuiya/HuddleCluster/ci.yml?label=tests" alt="CI"/></a>
</p>

<p align="center">
  <strong>Penguin-inspired self-organizing load balancer with adaptive thermal eviction.</strong>
</p>

---

Emperor Penguins survive Antarctic blizzards without any central coordinator — each bird follows one rule: if you're cold, push inward; if you're warm, drift outward. The huddle self-organizes.

HuddleCluster applies this directly to server scheduling. Servers that run hot rotate to an outer ring to cool down. Cooled servers rotate back in. No manual tuning. No fixed thresholds. The cluster finds its own equilibrium.

---

## Install

```bash
pip install huddle-cluster
```

Optional extras: `fastapi` · `redis` · `grpc` · `kubernetes`

---

## Single-instance

```python
from huddle_cluster import create_cluster
import requests

cluster = create_cluster([
    ("web-1", "10.0.0.1", 8080),
    ("web-2", "10.0.0.2", 8080),
    ("web-3", "10.0.0.3", 8080),
])
cluster.start()

with cluster.get_server_context() as server:
    response = requests.get(f"http://{server.host}:{server.port}/api")
```

What the cluster reports at any point:

```python
print(cluster.health_report())
```

```json
{
  "inner_servers": ["web-1", "web-3"],
  "outer_servers": ["web-2"],
  "fairness_score": 0.94,
  "rotation_count": 12,
  "requests_per_sec": 847.3,
  "cluster_health": "healthy"
}
```

---

## Multi-node cluster

Coordinate a fleet of hosts — each node runs its own HuddleCluster; the master tracks enrollment, heartbeats, and health.

```bash
# Start the coordinator
huddle-cluster master start --port 7070

# Enroll nodes on each host
huddle-cluster agent start --id web-01 --master http://master:7070 --port 8080

# Inspect from anywhere
huddle-cluster nodes list
```

```
NODE ID                ADDRESS                STATUS       HB       LAST SEEN
─────────────────────────────────────────────────────────────────────────────
web-01                 10.0.0.1:8080          alive        142      0.8s ago
web-02                 10.0.0.2:8080          alive        139      1.1s ago
web-03                 10.0.0.3:8080          dead         41       34.2s ago
```

Ask the scheduler which node to send the next workload to:

```bash
curl http://master:7070/v1/scheduler/next
```

```json
{ "ok": true, "node": { "node_id": "web-01", "address": "10.0.0.1", "port": 8080 } }
```

Live topology and Prometheus metrics are built in:

```
http://master:7070/dashboard      → real-time cluster topology
http://master:7070/v1/metrics     → Prometheus scrape endpoint
http://master:7070/v1/docs        → interactive API explorer (Swagger UI)
```

---

## How it works

| Concept | What it means |
|---|---|
| **Inner ring** | Active servers handling traffic right now |
| **Outer ring** | Servers cooling down after a hot streak |
| **Thermal score** | EMA of relative latency anomaly, CPU, memory, error rate |
| **Rotation** | Overheated servers evict outward; cooled servers return inward |
| **Relative anomaly** | Compared to the cluster median — adapts to any baseline automatically |

No server is permanently marked bad. Every server gets rest and returns.

---

## Performance

Under server failure, P95 latency stays under **86 ms** where NGINX round-robin reaches **5,027 ms** — a 58× reduction. Full methodology and results in the research paper below.

---

## Documentation

| | |
|---|---|
| **Single-instance guide** | [`USAGE.md`](USAGE.md) |
| **Cluster system** | [`docs/CLUSTER.md`](docs/CLUSTER.md) — MasterNode, Scheduler, RBAC, dashboard, API |
| **API explorer** | `http://your-master:7070/v1/docs` (live, once the master is running) |
| **Research paper** | [`docs/HuddleCluster.pdf`](docs/HuddleCluster.pdf) · [arXiv preprint](docs/HuddleCluster_arxiv.pdf) |

---

## Roadmap

- Thermal eviction, relative anomaly scoring, adaptive thresholds — v1.x
- Redis backend, gRPC routing, Kubernetes discovery, Prometheus, webhooks — v1.4
- Cluster system: MasterNode, AgentNode, CLI — v2.0
- Auto recovery, RBAC, metrics, dashboard, OpenAPI + Swagger UI — v2.x
- Cluster Scheduler — thermal-fitness workload placement — v3.0
- Cluster Auto Scaler — load-signal scale recommendations — v3.1
- Rolling Updater — zero-downtime batch upgrades with health gate — v3.2
- Service Discovery — health-aware registry, metadata-driven, DNS responder — v3.3
- HA Master — simplified Raft leader election, state replication, write redirect — v3.4
- Multi-Region — cross-datacenter topology, region-aware scheduling — v3.5
- Cluster Circuit Breaker — error-rate-based automatic trip/reset, scheduler exclusion — v4.0
- Rate Limiter — per-node token bucket, burst protection, scheduler exclusion — v4.1
- Canary Deployment — weight-based traffic splitting, start/advance/promote/abort — v4.2
- Observability — structured JSON logging, distributed trace IDs — v4.3, Level 4 complete
- TLS/HTTPS + mTLS, threaded HTTP server — v4.4, Level 5 (Production Hardening) in progress
- State persistence — HA term/voted_for + node registry survive restarts — v4.5
- mTLS node identity — client cert CN recorded on join — v4.6
- HA failover staleness fix + documented Raft limitations — v4.7
- OTLP log export (Jaeger/Tempo/OTel Collector compatible) — v4.8
- Docker + Kubernetes deployment manifests, SIGTERM graceful shutdown fix — v4.9
- WAN-latency simulation benchmark (partial — see docs for scope) — v4.10, Level 5 complete (7/7 addressed; 2 items — Raft hardening and WAN validation — improved but honestly still partial, see docs/CLUSTER.md)

---

## Commercial Support & Consulting

Need specialized architecture, custom integrations, or production deployment assistance?

We offer direct engineering support and consulting:
- **Custom Adapters & Integrations:** Tailoring HuddleCluster for your custom stack (AI/LLM inference clusters, high-frequency trading, IoT).
- **Production Deployment & Tuning:** Multi-region HA setup, Kubernetes migration, and stress testing.
- **Priority Enterprise Support & SLAs:** Dedicated hotline, rapid issue resolution, and feature requests.

**Contact:** [rahadbhuiya2021@gmail.com](mailto:rahadbhuiya2021@gmail.com)  
**Support this open-source project:** [GitHub Sponsors](https://github.com/sponsors/rahadbhuiya) | [Buy Me a Coffee](https://buymeacoffee.com/rahadbhuiya)

---

## Citation

```
Bhuiya, R. (2025). HuddleCluster: A Penguin-Inspired Self-Organizing Load Balancer
with Adaptive Thermal Eviction. https://github.com/rahadbhuiya/HuddleCluster
```

```
Bhuiya, Rahad (2026). HuddleCluster. figshare. Journal contribution.
https://doi.org/10.6084/m9.figshare.32397180
```

```
Bhuiya, Rahad (2026). HuddleCluster. Zenodo. https://doi.org/10.5281/zenodo.20348019
```

---

**Author:** Rahad Bhuiya &nbsp;·&nbsp; **License:** MIT