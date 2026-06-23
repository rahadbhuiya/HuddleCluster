# HuddleCluster — Usage Guide

## Installation

```bash
pip install huddle-cluster
```

With optional extras:

```bash
pip install "huddle-cluster[fastapi]"       # FastAPI integration
pip install "huddle-cluster[redis]"         # Redis shared-state backend
pip install "huddle-cluster[grpc]"          # gRPC channel routing
pip install "huddle-cluster[kubernetes]"    # Kubernetes auto-discovery
pip install "huddle-cluster[simulation]"    # Terminal simulation (rich)
```

---

## Basic Usage

```python
from huddle_cluster import create_cluster

cluster = create_cluster([
    ("web-1", "10.0.0.1", 8080),
    ("web-2", "10.0.0.2", 8080),
    ("web-3", "10.0.0.3", 8080),
    ("web-4", "10.0.0.4", 8080),
])
cluster.start()

# Get a server and route a request
server = cluster.get_server()
print(f"Routing to {server.host}:{server.port}")

# Record how long the request took (feeds the temperature algorithm)
cluster.record_latency(server, latency_ms=42.0)

cluster.stop()
```

---

## Context Manager (recommended)

Automatically records latency and handles exceptions:

```python
import requests

with cluster.get_server_context() as server:
    response = requests.get(f"http://{server.host}:{server.port}/api/data")
# latency is recorded automatically on exit
```

---

## Retry Helper

Automatically retries on failure, skipping unhealthy servers:

```python
from huddle_cluster import create_cluster

cluster = create_cluster([...])
cluster.start()

def call_api(server):
    import requests
    return requests.get(f"http://{server.host}:{server.port}/api", timeout=2)

result = cluster.request_with_retry(call_api, max_attempts=3)
```

---

## FastAPI Reverse Proxy

```bash
pip install "huddle-cluster[fastapi]"
```

```python
import httpx
from fastapi import FastAPI, Request
from fastapi.responses import Response
from huddle_cluster import create_cluster

app = FastAPI()

cluster = create_cluster([
    ("s1", "127.0.0.1", 8001),
    ("s2", "127.0.0.1", 8002),
    ("s3", "127.0.0.1", 8003),
])
cluster.start()

@app.get("/{path:path}")
async def proxy(request: Request, path: str):
    server = cluster.get_server()
    if server is None:
        return Response("No servers available", status_code=503)

    url = f"http://{server.host}:{server.port}/{path}"
    async with httpx.AsyncClient() as client:
        import time
        t0 = time.perf_counter()
        resp = await client.get(url, params=dict(request.query_params))
        cluster.record_latency(server, (time.perf_counter() - t0) * 1000)

    return Response(resp.content, status_code=resp.status_code,
                    headers=dict(resp.headers))
```

---

## Live Dashboard

```bash
python -c "
from huddle_cluster import create_cluster
cluster = create_cluster([('s1','127.0.0.1',8001),('s2','127.0.0.1',8002)])
cluster.start()
cluster.start_dashboard(host='127.0.0.1', port=8888)
input('Press Enter to stop...')
cluster.stop()
"
```

Then open `http://127.0.0.1:8888` in your browser.

Or run the bundled demo (after cloning the repo):

```bash
python examples/dashboard_demo.py
```

---

## Prometheus Metrics

```python
cluster = create_cluster([...])
cluster.start()

# Returns a plain-text Prometheus metrics string
print(cluster.prometheus_metrics())
```

Example output:

```
huddle_inner_size{cluster="default"} 2
huddle_outer_size{cluster="default"} 2
huddle_temperature{server="web-1"} 0.12
huddle_temperature{server="web-2"} 0.31
huddle_requests_total{server="web-1"} 1042
huddle_evictions_total{cluster="default"} 3
```

Expose via FastAPI:

```python
from fastapi.responses import PlainTextResponse

@app.get("/metrics")
def metrics():
    return PlainTextResponse(cluster.prometheus_metrics())
```

---

## Health Report

```python
report = cluster.health_report()
print(report)
# {
#   "inner": [{"id": "web-1", "temperature": 0.12, "requests": 1042}, ...],
#   "outer": [{"id": "web-3", "temperature": 0.03, "requests": 487}, ...],
#   "fairness_gini": 0.08,
#   "rps": 142.3,
#   "version": "1.4.0"
# }
```

---

## Persistent State

Saves and restores cluster temperature state across restarts:

```python
cluster = create_cluster(
    [...],
    state_file="huddle_state.json",
    checkpoint_interval_sec=30.0,
)
cluster.start()
# State is saved on stop() and restored on the next start()
cluster.stop()
```

---

## Webhook Alerting

```python
cluster = create_cluster(
    [...],
    alert_webhooks=["https://hooks.slack.com/services/YOUR/WEBHOOK/URL"],
    alert_on={"eviction", "promotion", "health_fail"},
)
```

Payload sent on each event:

```json
{
  "event": "eviction",
  "server_id": "web-3",
  "reason": "overheated",
  "temperature": 0.81,
  "timestamp": 1717776000.0
}
```

---

## Built-in HTTP Health Checker

Automatically evicts servers that fail health checks:

```python
cluster = create_cluster(
    [...],
    health_check_path="/health",
    health_check_interval_sec=10.0,
    health_check_timeout_sec=3.0,
    health_check_failures=2,
)
cluster.start()
```

---

## Redis Backend (multi-node)

Share temperature state across multiple hosts:

```bash
pip install "huddle-cluster[redis]"
```

```python
from huddle_cluster import create_cluster
from huddle_cluster_pkg.backends_redis import RedisBackend

cluster = create_cluster([...])
cluster.start()

backend = RedisBackend(url="redis://localhost:6379", key="huddle:prod")
backend.start_auto_sync(cluster, interval_sec=30.0)

# ... serve traffic ...

backend.stop_auto_sync()
cluster.stop()
```

---

## gRPC Channel Routing

```bash
pip install "huddle-cluster[grpc]"
```

```python
from huddle_cluster_pkg.grpc_cluster import create_grpc_cluster

cluster = create_grpc_cluster([
    ("s1", "10.0.0.1", 50051),
    ("s2", "10.0.0.2", 50051),
    ("s3", "10.0.0.3", 50051),
])
cluster.start()

with cluster.get_channel() as channel:
    stub = MyService.Stub(channel)
    response = stub.MyMethod(request)

cluster.stop()
```

---

## Kubernetes Auto-Discovery

```bash
pip install "huddle-cluster[kubernetes]"
```

```python
from huddle_cluster import create_cluster
from huddle_cluster_pkg.discovery_k8s import K8sDiscovery

cluster = create_cluster([], min_inner_size=1)
cluster.start()

discovery = K8sDiscovery(
    namespace="production",
    label_selector="app=api-server",
    port=8080,
)
discovery.start(cluster)

# Pods are added/removed automatically as they come and go

discovery.stop()
cluster.stop()
```

---

## Weighted Server Capacity

Servers with higher weight tolerate more load before eviction — useful when
instances have different hardware specs.

```python
from huddle_cluster import create_cluster

cluster = create_cluster([
    ("web-1", "10.0.0.1", 8080),         # weight=1.0 (default)
    ("web-2", "10.0.0.2", 8080, 2.0),    # weight=2.0 — needs 2× heat to evict
    ("web-3", "10.0.0.3", 8080, 0.5),    # weight=0.5 — evicts at half the threshold
])
```

---

## Cold Start Protection

New servers warm up in the outer ring for a fixed period before becoming
eligible for inner-ring traffic. Prevents request spikes on instances that
haven't warmed their caches or JIT compilers yet.

```python
from huddle_cluster import HuddleCluster

cluster = HuddleCluster(cold_start_sec=30.0)
cluster.start()
# Any server added via add_server() stays in the outer ring for 30 seconds
# regardless of load, then graduates to the inner ring normally.
```

---

## Absolute Latency Floor

Guards against majority degradation — when the cluster median itself rises
above an acceptable level, relative anomaly scoring alone isn't enough.

```python
cluster = HuddleCluster(absolute_latency_floor_ms=500.0)
# Any server whose rolling average latency exceeds 500 ms is evicted
# regardless of how it compares to the rest of the cluster.
```

---

## Adaptive Thresholds

Heat and cool thresholds auto-adjust from the cluster's P95 latency history.
Thresholds loosen under sustained load to avoid over-eviction, and tighten
when the cluster is healthy for faster anomaly detection.

```python
cluster = HuddleCluster(adaptive_thresholds=True)
```

---

## Server Tags

Attach arbitrary metadata to servers. Tags appear in `health_report()`,
Prometheus metric labels, and log output.

```python
from huddle_cluster import HuddleCluster, Server

cluster = HuddleCluster()
cluster.add_server(Server(
    id="web-1", host="10.0.0.1", port=8080,
    tags={"region": "us-east", "tier": "primary", "az": "1a"},
))
cluster.start()
```

---

## Eviction Callback

```python
from huddle_cluster import HuddleCluster, EvictionReason

def on_evicted(server, reason: EvictionReason):
    print(f"{server.id} evicted — reason: {reason.value}")
    # reason values: overheated | unhealthy | absolute_latency | manual

cluster = HuddleCluster(on_eviction=on_evicted)
```

---

## Circuit Breaker

Servers with an error rate above the threshold are evicted immediately,
independent of latency scoring.

```python
cluster = HuddleCluster(circuit_breaker_threshold=0.5)
# Any server with error_rate > 0.5 (50%) is evicted on the next rotation tick.
```

---

## Gossip Protocol

Share temperature state across hosts over UDP multicast — no Redis or central
broker required. Each `GossipAgent` broadcasts its cluster's current temperatures
and merges what it receives from peers.

```python
from huddle_cluster import create_cluster, GossipAgent

cluster = create_cluster([("s1", "10.0.0.1", 8080)])
cluster.start()

agent = GossipAgent(
    node_id="host-1",
    gossip_port=9999,
    broadcast_interval=2.0,
)
agent.start(cluster)

# On each peer host:
# agent = GossipAgent(node_id="host-2", gossip_port=9999)
# agent.start(cluster)

# All agents on the same multicast group merge each other's state automatically.
agent.stop()
cluster.stop()
```

---

## WebSocket Connection Draining

When a server is evicted, the cluster waits for active WebSocket connections
to finish before removing it from the ring.

```python
cluster = HuddleCluster(ws_drain_timeout_sec=10.0)
cluster.start()

# Track a WebSocket connection — the server won't be evicted until
# the context manager exits (or the drain timeout is reached).
with cluster.ws_connection(server) as server:
    await websocket.send(data)
    await websocket.recv()
```

---

## Graceful Shutdown

```python
# Basic stop
cluster.stop()

# Wait up to 10 s for in-flight requests to finish before stopping.
# When state_file is configured, current temperatures are saved to disk
# so the next start() restores them.
cluster.stop(drain_timeout_sec=10.0)
```

---

## Performance Overhead

The thermal scheduling layer adds less than 1 μs per request:

| Operation | Time |
|---|---|
| `get_server()` — round-robin baseline | 0.277 μs |
| `get_server()` — HuddleCluster | 0.295 μs |
| `get_server()` + `record_latency()` | 10.7 μs |
| Peak memory (20 servers) | 28.3 KB |
| Slow-server detection speed | ~36 requests avg |

---

## Known Limitations

- **Uniform burst load** — when all servers are equally stressed, relative anomaly scores are near zero and no eviction fires. Use `absolute_latency_floor_ms` as a secondary guard.
- **Majority degradation** — if more than half the inner-ring servers degrade simultaneously, the cluster median itself rises. Use `absolute_latency_floor_ms` in this case.
- **Single-process by default** — temperature state is not shared across hosts without the Redis backend (`RedisBackend`) or the Gossip protocol (`GossipAgent`).
- **Loopback benchmarks** — all HTTP benchmarks use localhost. Wide-area production validation is future work.

---

## All Public Imports

```python
from huddle_cluster import (
    create_cluster,              # factory function (recommended entry point)
    HuddleCluster,               # main cluster class
    Server,                      # server dataclass
    ServerMetrics,               # metrics dataclass (cpu, memory, connections, error_rate)
    RotationEvent,               # emitted on every rotation cycle
    AlertEvent,                  # emitted on eviction/promotion/health_fail
    TrafficRamp,                 # canary / traffic ramp descriptor
    Position,                    # enum: INNER | OUTER
    EvictionReason,              # enum: OVERHEATED | UNHEALTHY | ABSOLUTE_LATENCY | MANUAL
    RetryExhaustedError,         # raised when request_with_retry runs out of attempts
    AdaptiveThresholdController, # auto-tunes heat/cool thresholds from P95 history
    GossipAgent,                 # UDP multicast for multi-node temperature sharing
)
```