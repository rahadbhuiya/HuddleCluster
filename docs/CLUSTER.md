# HuddleCluster — Cluster System

Multi-node cluster management built on top of the single-instance HuddleCluster.
A `MasterNode` coordinates enrolled `AgentNode`s across any number of hosts.

---

## Quick start

```bash
# Terminal 1 — start the master
huddle-cluster master start --port 7070

# Terminal 2 — enroll an agent
huddle-cluster agent start --id web-01 --master http://localhost:7070 --port 8080

# Terminal 3 — inspect the cluster
huddle-cluster nodes list
huddle-cluster cluster status
```

Or from Python:

```python
from huddle_cluster_pkg import MasterNode, AgentNode, ClusterScheduler

master = MasterNode(
    port=7070,
    heartbeat_timeout_sec=30,
    api_keys={"admin-key": "admin", "readonly-key": "viewer"},
    scheduler=ClusterScheduler(cooldown_sec=10),
    unhealthy_alive_ratio=0.5,          # alert if < 50% alive
    on_cluster_unhealthy=lambda s: alert_ops(s),
)
master.start()
```

---

## REST API

| Method | Path | Auth | Description |
|---|---|---|---|
| `GET` | `/v1/health` | none | Liveness check |
| `GET` | `/v1/openapi.json` | none | OpenAPI 3.0 spec |
| `GET` | `/v1/docs` | none | Interactive Swagger UI |
| `GET` | `/v1/status` | viewer | Cluster summary |
| `GET` | `/v1/metrics` | viewer | Prometheus text exposition |
| `GET` | `/v1/nodes` `?status=&limit=&offset=` | viewer | Filterable node list |
| `GET` | `/v1/nodes/{id}` | viewer | Single node record |
| `POST` | `/v1/nodes/join` | admin | Agent enrollment |
| `POST` | `/v1/nodes/{id}/heartbeat` | admin | Heartbeat + metrics |
| `DELETE` | `/v1/nodes/{id}` | admin | Graceful departure |
| `GET` | `/v1/scheduler/next` `?affinity=` | viewer | Pick the best node |
| `GET` | `/v1/scheduler/stats` | viewer | Heat map + placement counts |
| `POST` | `/v1/scheduler/report` | admin | Record workload completion |
| `GET` | `/dashboard` | none | Web topology dashboard |

---

## Authentication

```python
# No auth (open, default — backward compatible)
master = MasterNode(port=7070)

# With RBAC
master = MasterNode(
    port=7070,
    api_keys={
        "admin-secret": "admin",     # join/heartbeat/leave + all reads
        "dashboard-key": "viewer",   # read-only
    },
)
```

Every request needs `Authorization: Bearer <key>` except `/v1/health`, `/v1/openapi.json`, `/v1/docs`, and `/dashboard`.

Agents and CLI commands take `--api-key`:

```bash
huddle-cluster agent start --id node-1 --master http://host:7070 --port 8080 --api-key admin-secret
huddle-cluster nodes list --api-key dashboard-key
```

---

## Auto Recovery

Nodes that crash and rejoin too many times within a window are **quarantined** instead of immediately trusted:

```python
master = MasterNode(
    port=7070,
    flap_window_sec=300,                # window to count deaths in
    flap_threshold=3,                   # deaths within window → quarantine
    quarantine_recovery_heartbeats=3,   # consecutive heartbeats to exit quarantine
    purge_after_sec=3600,               # remove dead nodes after 1h (opt-in)
    on_node_quarantined=lambda n: print(f"{n.node_id} is flapping"),
    on_node_purged=lambda n: print(f"{n.node_id} purged"),
)
```

A quarantined node is excluded from `alive_nodes()` and the scheduler's
eligible pool, but still visible in `GET /v1/nodes` with `status: "quarantined"`.

---

## Monitoring & Metrics

```bash
# Prometheus scrape target — one endpoint for the whole cluster
curl http://localhost:7070/v1/metrics
```

```
huddle_master_total_nodes 3
huddle_master_alive_nodes 2
huddle_master_quarantined_nodes 1
huddle_node_up{node_id="web-01"} 1
huddle_node_fairness_score{node_id="web-01"} 0.94
```

Per-node forwarded metrics (e.g. `fairness_score`, `inner_servers`) only appear
when an agent actually reports them — absent metrics are absent, not zero.

Cluster health alerting:

```python
master = MasterNode(
    port=7070,
    unhealthy_alive_ratio=0.5,
    on_cluster_unhealthy=lambda s: page_oncall(s),
    on_cluster_recovered=lambda s: resolve_incident(s),
)
```

---

## Scheduler

Thermal-fitness workload placement across nodes — same penguin rotation model
as the single-instance inner/outer ring, lifted to the cluster level:

```bash
# Ask the master which node to use
curl http://localhost:7070/v1/scheduler/next
# {"ok": true, "node": {"node_id": "web-02", "address": "10.0.0.2", "port": 8080, ...}}

# Sticky session
curl "http://localhost:7070/v1/scheduler/next?affinity=user-12345"

# Report completion for future scoring
curl -X POST http://localhost:7070/v1/scheduler/report \
  -d '{"node_id": "web-02", "duration_ms": 245, "success": true}'
```

Scoring factors: freshness · stability · quarantine penalty · forwarded RPS · warm-up bonus.
Heat decays exponentially so idle nodes cool back down automatically.

```python
from huddle_cluster_pkg import MasterNode, ClusterScheduler

master = MasterNode(
    port=7070,
    scheduler=ClusterScheduler(
        cooldown_sec=10.0,   # heat half-life in seconds
        prefer_alive=True,   # alive nodes always before quarantined
    ),
)
```

---

## Filtering & Pagination

```bash
huddle-cluster nodes list --status dead
huddle-cluster nodes list --status alive,quarantined --limit 20 --offset 0
curl "http://localhost:7070/v1/nodes?status=alive&limit=10&offset=0"
```

Response includes `total`, `limit`, `offset` alongside `nodes` — plain
`GET /v1/nodes` with no params behaves exactly as before.

---

## Web Dashboard & API Docs

```
http://localhost:7070/dashboard     # real-time cluster topology
http://localhost:7070/v1/docs       # interactive Swagger UI (try any endpoint)
http://localhost:7070/v1/openapi.json  # machine-readable spec
```

The dashboard auto-refreshes every 3 seconds. If `api_keys` is configured,
enter a key in the field at the top of the page — it's stored only in your
own browser's localStorage.

---

## CLI reference

```bash
huddle-cluster master start  [--host] [--port] [--timeout] [--flap-window]
                             [--flap-threshold] [--quarantine-recovery]
                             [--purge-after] [--api-key KEY=ROLE ...]

huddle-cluster agent  start  --id ID --master URL --port PORT
                             [--address IP] [--interval SEC]
                             [--retry N] [--meta key=val ...] [--api-key KEY]

huddle-cluster nodes  list   [--master URL] [--api-key KEY]
                             [--status alive,quarantined] [--limit N] [--offset N]
huddle-cluster nodes  status NODE_ID [--master URL] [--api-key KEY]

huddle-cluster cluster status  [--master URL] [--api-key KEY]
huddle-cluster cluster health  [--master URL]
huddle-cluster cluster metrics [--master URL] [--api-key KEY]
huddle-cluster cluster openapi [--master URL]
```

---

## Node status lifecycle

```
          join
  ──────────────────► alive
                        │
         missed timeout │
                        ▼
                       dead ──── heartbeat ──► alive (if flap_threshold not exceeded)
                        │
                        │                    ──► quarantined (if flapping)
                        │                            │
                   purge_after_sec                   │ quarantine_recovery_heartbeats
                        │                            ▼
                        ▼                          alive (clean slate)
                    [removed]
```

---

## Behaviour Highlights

- **Dead detection** — a node is marked `dead` if no heartbeat arrives within `heartbeat_timeout_sec`. It auto-recovers to `alive` when heartbeats resume (or to `quarantined` if it has been flapping).
- **Auto-rejoin** — if the master restarts and loses its registry, each agent re-registers itself within 3 × `heartbeat_interval` automatically.
- **Fast shutdown** — `master.stop()` and `agent.stop()` both complete in under 100 ms.

---

## Running Tests

```bash
# All tests
python -m pytest tests/ -v

# Cluster-specific tests only
python -m pytest tests/test_cluster_master.py tests/test_cluster_agent.py tests/test_cluster_scheduler.py -v

# With coverage
python -m pytest tests/ --cov=huddle_cluster_pkg --cov-report=term-missing
```

---

## Running Benchmarks

```bash
# HTTP benchmark (requires Docker)
docker compose -f benchmarks/docker-compose.yml up -d
python benchmarks/benchmark_http.py

# Simulated benchmark (no Docker needed)
python benchmarks/benchmark_sim.py

# NGINX baseline comparison
python benchmarks/benchmark_nginx.py
```

---

## GitHub Actions / CI

The repository ships two workflows under `.github/workflows/`:

| Workflow | Trigger | What it does |
|---|---|---|
| `ci.yml` | Push / PR to `main` | Runs the full test suite on Python 3.9–3.12 |
| `publish.yml` | Tag push `v*` | Builds and publishes to PyPI via Trusted Publisher |

To publish a new release:

```bash
git tag -a v3.0.0 -m "Release v3.0.0"
git push origin v3.0.0
# publish.yml picks up the tag and pushes to PyPI automatically
```

---

## Roadmap

**Level 2 — Production Ready (complete)**
Auto recovery · Prometheus metrics · RBAC/auth · Web dashboard · REST API expansion

**Level 3 — Kubernetes/Swarm-grade (in progress)**
- [x] Scheduler — thermal-fitness workload placement — v3.0.0
- [ ] Auto scaling
- [ ] Rolling updates
- [ ] Service discovery
- [ ] High-availability master
- [ ] Multi-region support