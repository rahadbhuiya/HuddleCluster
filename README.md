# HuddleCluster

A penguin-inspired, self-organizing server load balancer with adaptive thermal eviction.

**Author:** Rahad Bhuiya
**Version:** 1.3.3
**License:** MIT
**Paper:** [HuddleCluster: A Penguin-Inspired Self-Organizing Load Balancer with Adaptive Thermal Eviction](https://github.com/rahadbhuiya/HuddleCluster/blob/main/docs/HuddleCluster_Paper.pdf)

---

## The Idea

Emperor Penguins survive Antarctic winters by forming huddles. Penguins on the cold outer edge
push inward toward warmth, while those in the center gradually rotate outward to rest — with no
central coordinator, only local temperature thresholds.

HuddleCluster maps this directly to server scheduling:

- **Inner ring** — active servers handling requests (warm)
- **Outer ring** — resting servers recovering from load (cool)
- **Temperature** — a composite EMA score derived from relative latency anomaly, CPU, memory, connections, and error rate
- **Rotation** — overheated servers evict to outer ring; cooled servers return to inner ring automatically

The key innovation is **relative latency anomaly scoring**: instead of comparing a server's
latency to an absolute threshold, HuddleCluster compares each server to the cluster-wide
median. A server 3x slower than its peers is evicted regardless of whether the baseline is
10 ms or 300 ms.

---

## Benchmark Results

### Simulated Benchmark (10 trials, mean +/- std, Welch's t-test)

| Scenario / Metric | Round Robin | Least Conn | HuddleCluster | p-value |
|---|---|---|---|---|
| **Normal Load** | | | | |
| P50 (ms) | 21.5 +/- 0.2 | 21.2 +/- 0.3 | 21.0 +/- 0.2 | 0.000* |
| P95 (ms) | 29.6 +/- 0.3 | 28.8 +/- 0.4 | 28.6 +/- 0.6 | 0.001* |
| Avg (ms) | 21.4 +/- 0.1 | 21.1 +/- 0.2 | 21.0 +/- 0.2 | 0.000* |
| Fairness (Gini) | 0.000 | 0.067 | **0.000** | -- |
| **Slow Server (5x at halfway)** | | | | |
| P95 (ms) | 63.2 +/- 1.0 | 61.7 +/- 1.1 | 55.1 +/- 10.6 | 0.039* |
| Avg (ms) | 20.1 +/- 0.2 | 19.7 +/- 0.2 | 19.6 +/- 0.4 | 0.002* |
| **Server Failure (crash at halfway)** | | | | |
| P95 (ms) | 500.0 +/- 0.0 | 500.0 +/- 0.0 | **23.9 +/- 0.5** | 0.000* |
| Avg (ms) | 53.4 +/- 0.2 | 229.7 +/- 1.4 | **29.7 +/- 0.1** | 0.000* |

*statistically significant (p < 0.05)*

### Real HTTP Benchmark (6 FastAPI servers, loopback)

| Scenario / Metric | Round Robin | Least Conn | HuddleCluster | vs RR |
|---|---|---|---|---|
| **Normal Load** | | | | |
| P95 (ms) | 88.6 | 85.3 | 74.3 | +16.2% |
| Avg (ms) | 51.8 | 48.3 | 46.1 | +11.0% |
| **Slow Server (5x)** | | | | |
| Avg (ms) | 55.2 | 52.1 | 53.4 | +3.4% |
| **Server Failure** | | | | |
| P95 (ms) | 5,026.9 | 5,027.9 | **85.6** | +98.3% |
| Avg (ms) | 429.7 | 414.0 | **181.5** | +57.7% |

### Industry Baseline (NGINX vs HuddleCluster, Docker)

Containerised benchmark: 6 FastAPI upstream servers, Docker bridge network,
NGINX round-robin and NGINX least-connections as baselines.

| Scenario / Metric | NGINX RR | NGINX LC | HuddleCluster | vs NGINX RR |
|---|---|---|---|---|
| **Normal Load** | | | | |
| P50 (ms) | 28.4 | 27.5 | **20.5** | +28.0% |
| P95 (ms) | 55.1 | 39.3 | **33.4** | +39.4% |
| Avg (ms) | 29.1 | 26.4 | **21.4** | +26.5% |
| **Slow Server (5x)** | | | | |
| P50 (ms) | 25.3 | 25.3 | **19.8** | +21.6% |
| P95 (ms) | 38.9 | 42.8 | **33.6** | +13.6% |
| Avg (ms) | 25.1 | 25.8 | **20.5** | +18.4% |
| **Server Failure** | | | | |
| P95 (ms) | 45.9 | 41.9 | **29.7** | +35.3% |
| Avg (ms) | 25.9 | 25.6 | **20.8** | +19.4% |

Note: admin endpoint injection was not available in this Docker run
(upstream servers on internal network only). Results reflect
HuddleCluster's thermal rotation advantage without injected failures.

```bash
cd benchmarks/
docker compose up -d --build
python benchmark_industry.py
docker compose down
```

### Overhead

| Measurement | Value |
|---|---|
| RR get_server() | 0.277 us |
| HC get_server() | 0.295 us (1.07x over RR) |
| HC get_server() + record_latency() | 10.7 us |
| Peak memory (20 servers) | 28.3 KB |
| Slow-server detection speed | 36 requests avg (range 35-40) |

---

## Quick Start

```bash
pip install -e .
# with benchmark dependencies:
pip install -e ".[benchmark]"
# with FastAPI integration:
pip install -e ".[fastapi]"
```

```python
from huddle_cluster import create_cluster
import time, requests

cluster = create_cluster([
    ("s1", "10.0.0.1", 8080),
    ("s2", "10.0.0.2", 8080),
    ("s3", "10.0.0.3", 8080),
])
cluster.start()

# Route a request with latency feedback
server = cluster.get_server()
t0 = time.perf_counter()
response = requests.get(f"http://{server.host}:{server.port}/api")
cluster.record_latency(server, (time.perf_counter() - t0) * 1000)

# Or use the context manager (auto-records latency)
with cluster.get_server_context() as server:
    response = requests.get(f"http://{server.host}:{server.port}/api")

print(cluster.health_report())
cluster.stop()
```

---

## v1.3.0 Features

### Weighted Server Capacity

Servers with higher weight tolerate more load before eviction. Useful for
heterogeneous clusters where some instances are larger than others.

```python
cluster = create_cluster([
    ("s1", "10.0.0.1", 8080),          # weight=1.0 (default)
    ("s2", "10.0.0.2", 8080, 2.0),     # weight=2.0 -- needs 2x heat to evict
    ("s3", "10.0.0.3", 8080, 0.5),     # weight=0.5 -- evicts sooner
])
```

### Cold Start Protection

New servers warm up in the outer ring before handling traffic. Prevents
request spikes on fresh instances that have not yet warmed their caches
or JIT compilers.

```python
cluster = HuddleCluster(cold_start_sec=30.0)
# Any server added will stay in outer ring for 30 seconds
# regardless of force_inner=True
```

### Absolute Latency Floor

Guards against majority degradation where the relative anomaly score breaks
down (when the cluster median itself rises above acceptable levels).

```python
cluster = HuddleCluster(absolute_latency_floor_ms=500.0)
# Any server with avg latency > 500ms is evicted regardless of relative score
```

### Adaptive Thresholds

Heat and cool thresholds auto-adjust based on cluster P95 latency history.
Thresholds loosen under sustained load (to avoid over-eviction) and tighten
when the cluster is healthy (for faster anomaly detection).

```python
cluster = HuddleCluster(adaptive_thresholds=True)
# heat_threshold and cool_threshold update automatically
# Check current values via cluster.health_report()["heat_threshold"]
```

### Prometheus Metrics Exporter

Expose cluster state as Prometheus metrics for Grafana dashboards.

```python
# FastAPI example
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

app = FastAPI()

@app.get("/metrics", response_class=PlainTextResponse)
def metrics():
    return cluster.prometheus_metrics()
```

Metrics exposed: `huddle_server_temperature`, `huddle_server_avg_latency_ms`,
`huddle_server_anomaly_score`, `huddle_server_rotations_total`,
`huddle_cluster_inner_count`, `huddle_cluster_fairness_gini`,
`huddle_cluster_heat_threshold`, `huddle_cluster_p95_latency_ms`.

### Gossip Protocol (Distributed Deployments)

Share temperature state between multiple HuddleCluster instances via UDP
multicast. Each node broadcasts its inner-ring server states; peers receive
them as advisory signals.

```python
from huddle_cluster import GossipAgent, create_cluster

agent   = GossipAgent(node_id="node-1", gossip_port=9999)
cluster = create_cluster([...], gossip_agent=agent)
cluster.start()

# See peer states
peers = agent.peer_states()
# {"node-2": [{"id": "s0", "temp": 0.12, "avg_ms": 15.3, "pos": "inner"}]}
```

Note: gossip is best-effort UDP multicast. The cluster remains fully
functional without gossip -- it is purely additive.

---

## File Structure

```
HuddleCluster/
|
|-- huddle_cluster.py              # Core library v1.3.0 (zero runtime dependencies)
|-- __init__.py                    # Package exports
|-- pyproject.toml                 # pip install support
|-- requirements.txt               # Optional dependencies by feature
|-- LICENSE
|
|-- benchmarks/
|   |-- benchmark.py               # Simulated 4-scenario benchmark
|   |-- benchmark_statistical.py   # 10-trial statistical benchmark with CI
|   |-- benchmark_http.py          # Real HTTP benchmark (6 FastAPI servers)
|   |-- benchmark_industry.py      # NGINX vs HuddleCluster (Docker)
|   |-- upstream_server.py         # FastAPI upstream server
|   |-- docker-compose.yml         # 6 upstream servers + 2 NGINX instances
|   |-- nginx/
|   |   |-- nginx_rr.conf          # NGINX round-robin config
|   |   |-- nginx_lc.conf          # NGINX least-connections config
|   |-- run_http_benchmark.bat     # Windows one-click runner
|
|-- tests/
|   |-- test_rotation.py           # Rotation, eviction, feedback loop (45 tests)
|   |-- test_fairness.py           # Fairness and Gini tests
|   |-- test_stress.py             # Concurrent load tests
|   |-- conftest.py                # Shared fixtures
|
|-- examples/
|   |-- fastapi_example.py         # FastAPI reverse proxy integration
|   |-- simulation.py              # Terminal simulation
|   |-- HuddleSimulation.jsx       # React visual simulation
|
|-- docs/
    |-- diagrams/
        |-- architecture_diagram.png   # Dual-ring architecture
        |-- temperature_lifecycle.png  # State machine + weight composition
        |-- rotation_flowchart.png     # Rotation algorithm flowchart
        |-- generate_diagrams.py       # Regenerate diagrams
```

---

## How It Works

### Temperature Formula

```
raw(s) = 0.70 x anomaly(s)     # relative latency vs cluster median
       + 0.10 x cpu(s)          # CPU usage [0,1]
       + 0.10 x conn(s)         # active connections / 1000, clamped [0,1]
       + 0.05 x mem(s)          # memory usage [0,1]
       + 0.05 x err(s)          # error rate [0,1]

T(s) = alpha x raw(s) + (1 - alpha) x T(s)   [EMA, default alpha=0.60]
```

### Relative Latency Anomaly

```
anomaly(s) = clamp( (avg_ms(s) / median_ms(inner_ring) - 1) / 2,  0,  1 )
```

| Server / Cluster Median | Ratio | Anomaly Score | Cycles to eviction |
|---|---|---|---|
| 12 ms / 12 ms | 1.0x (normal) | 0.00 | Never |
| 24 ms / 12 ms | 2.0x (warm) | 0.50 | ~8 cycles |
| 36 ms / 12 ms | 3.0x (hot) | 1.00 | ~3 cycles |
| 60 ms / 12 ms | 5.0x (degraded) | 1.00 (clamped) | ~3 cycles |

### Rotation Rules

1. **Eviction** — inner server with T >= 0.55 moves to outer ring. Capped at max(1, |inner|/3) per cycle (thundering-herd prevention).
2. **Promotion** — coolest outer server with T <= 0.30 and sufficient dwell time moves to inner ring (flapping prevention).
3. **Health eviction** — server with is_healthy=False is evicted immediately regardless of temperature.
4. **Emergency fallback** — if inner ring drops below min_inner, the globally coolest server is promoted unconditionally.

### Failure-Mode Bounds

**Median robustness**: up to floor((n-1)/2) simultaneous server degradations can be
detected correctly. If k >= n/2 servers degrade simultaneously, the median baseline rises
and anomaly detection weakens — a documented boundary condition.

**Oscillation bound**: a server cannot oscillate faster than
`rotation_cooldown_sec + min_outer_dwell_sec` per cycle (default: 15 seconds minimum).
EMA smoothing requires at least 20 consecutive anomalous readings before a healthy server
(raw < 0.10) is evicted.

**Worst-case eviction rate**: at most max(1, floor(|inner|/3)) evictions per rotation
cycle. With default settings, the inner ring never drops below min_inner=2 active servers.

---

## Configuration

```python
cluster = HuddleCluster(
    heat_threshold             = 0.55,   # Evict above this temperature
    cool_threshold             = 0.30,   # Promote below this temperature
    min_inner_size             = 2,      # Minimum active servers
    max_inner_size             = 5,      # Maximum active servers
    rotation_cooldown_sec      = 5.0,    # Minimum seconds between evictions per server
    min_outer_dwell_sec        = 10.0,   # Minimum rest time before re-entry
    ema_alpha                  = 0.60,   # Temperature smoothing (higher = faster reaction)
    # v1.3.0 new parameters
    absolute_latency_floor_ms  = None,   # Evict any server above this absolute latency
    cold_start_sec             = 0.0,    # New servers warm up in outer ring for this long
    adaptive_thresholds        = False,  # Auto-adjust thresholds from cluster P95 history
    gossip_agent               = None,   # GossipAgent for distributed deployments
    metrics_updater            = None,   # Optional: fn(server) -> updates server.metrics
    on_rotation                = None,   # Optional: fn(RotationEvent) -> called on rotation
)
```

### Parameter Sensitivity (P95 ms, slow-server scenario)

| heat_threshold \ alpha | alpha=0.3 | alpha=0.6 (default) | alpha=0.9 |
|---|---|---|---|
| 0.45 (aggressive) | 38.2 | 31.4 | 29.1 |
| **0.55 (default)** | 52.3 | **32.0** | 30.8 |
| 0.65 (conservative) | 74.1 | 58.6 | 41.2 |

Default (heat=0.55, alpha=0.60) balances detection speed and eviction stability.

---

## Running Tests

```bash
# All 45 tests
python -m unittest tests/test_rotation.py tests/test_fairness.py tests/test_stress.py

# With pytest
pip install ".[dev]"
pytest tests/ -v
```

---

## Running Benchmarks

```bash
cd benchmarks/

# Simulated (4 scenarios, ~2 min)
python benchmark.py

# Statistical (10 trials, p-values, CI, ~6 min)
pip install scipy matplotlib numpy
python benchmark_statistical.py

# Real HTTP (6 FastAPI servers, ~3 min)
pip install fastapi uvicorn httpx matplotlib numpy
python benchmark_http.py          # Linux/Mac
run_http_benchmark.bat            # Windows

# Industry baseline: NGINX vs HuddleCluster (requires Docker)
docker compose up -d
python benchmark_industry.py
docker compose down
```

---

## GitHub Actions

Three workflows are included:

**CI** (`ci.yml`) — runs on every push and pull request:
- Unit tests on Python 3.10, 3.11, 3.12
- Integration tests (FastAPI upstream servers)
- Type stub syntax check
- Package build verification

**Publish** (`publish.yml`) — triggers on version tags (`v*.*.*`):
- Runs full test suite
- Builds wheel and sdist
- Publishes to PyPI via Trusted Publishing (no API token needed)
- Creates GitHub Release with changelog entry

**Benchmark** (`benchmark.yml`) — manual trigger only:
- Runs statistical benchmark with configurable trials
- Uploads chart artifacts

Setup PyPI Trusted Publishing:
1. PyPI -> Your project -> Publishing -> Add publisher
2. GitHub owner: `rahadbhuiya`, repo: `HuddleCluster`, workflow: `publish.yml`

---

## Known Limitations

- **Uniform burst load**: when all servers are equally stressed, relative anomaly scores are near zero and no eviction fires. An absolute latency floor is planned.
- **Majority degradation**: if more than half the inner-ring servers degrade simultaneously, the median baseline rises. Use `absolute_latency_floor_ms` as a secondary guard in this scenario.
- **Single-process**: temperature state is not shared across hosts. A gossip-protocol extension is planned.
- **Loopback benchmarks**: all HTTP benchmarks use localhost. Wide-area production validation is future work.

---

## Roadmap

- [x] Latency feedback loop (record_latency, get_server_context) — v1.1.0
- [x] Relative latency anomaly scoring (median baseline) — v1.2.0
- [x] Inner-ring fairness metric (Gini) — v1.2.0
- [x] Tunable EMA alpha — v1.2.0
- [x] Statistical benchmark (10 trials, Welch's t-test, 95% CI) — v1.2.0
- [x] Real HTTP benchmark (FastAPI upstream servers) — v1.2.0
- [x] Industry baseline benchmark (NGINX, Docker) — v1.2.0
- [x] Failure-mode bounds (median robustness, oscillation, eviction rate) — v1.2.0
- [x] Adaptive thresholds (auto-adjust heat/cool from cluster P95 history) -- v1.3.0
- [x] Weighted server capacity (weight= param on Server/create_cluster) -- v1.3.0
- [x] Cold start protection (cold_start_sec= param) -- v1.3.0
- [x] Prometheus metrics exporter (cluster.prometheus_metrics()) -- v1.3.0
- [x] Distributed temperature sharing (GossipAgent, UDP multicast) -- v1.3.0
- [x] Absolute latency floor (absolute_latency_floor_ms= param) -- v1.3.0

---

## Citation

```
Bhuiya, R. (2025). HuddleCluster: A Penguin-Inspired Self-Organizing Load Balancer
with Adaptive Thermal Eviction. https://github.com/rahadbhuiya/HuddleCluster
```

---

## License

MIT — see LICENSE.
