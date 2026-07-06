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
| `GET` | `/v1/autoscaler/status` | viewer | Scale decision, history, cooldowns |
| `POST` | `/v1/rollout/start` | admin | Start rolling update |
| `GET` | `/v1/rollout/status` | viewer | Progress, phase, per-node outcomes |
| `POST` | `/v1/rollout/pause` | admin | Pause after current batch |
| `POST` | `/v1/rollout/resume` | admin | Resume paused rollout |
| `POST` | `/v1/rollout/abort` | admin | Stop immediately |
| `GET` | `/v1/discovery/services` | viewer | All services + alive node counts |
| `GET` | `/v1/discovery/services/{name}` | viewer | Alive nodes for one service |
| `POST` | `/v1/discovery/announce` | admin | Node self-announces a service |
| `DELETE` | `/v1/discovery/services/{name}/{node_id}` | admin | Deregister a node |
| `GET` | `/v1/ha/status` | none | HA role, term, leader URL, peers |
| `POST` | `/v1/ha/vote` | none | Raft RequestVote RPC (peer-to-peer) |
| `POST` | `/v1/ha/sync` | none | Raft AppendEntries / state snapshot RPC |
| `GET` | `/v1/regions` | viewer | All regions with alive-node counts |
| `GET` | `/v1/regions/{name}` | viewer | Alive nodes in a specific region |
| `POST` | `/v1/regions/announce` | admin | Node self-announces its region |
| `GET` | `/v1/breakers` | viewer | All circuit breaker states |
| `GET` | `/v1/breakers/{node_id}` | viewer | Single node breaker state |
| `POST` | `/v1/breakers/{node_id}/reset` | admin | Manually reset a breaker |
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

## Auto Scaler

Monitors alive node count and scheduler heat, then fires callbacks when the cluster should grow or shrink. Wire the callbacks to your provisioning system — Kubernetes, Terraform, a cloud SDK, or a shell script. The autoscaler never provisions nodes itself.

```python
from huddle_cluster_pkg import MasterNode, ClusterScheduler, ClusterAutoScaler

def add_nodes(delta):
    print(f"Provisioning {delta} node(s)")   # call your infra API here

def remove_nodes(delta):
    print(f"Deprovisioning {delta} node(s)")

autoscaler = ClusterAutoScaler(
    min_nodes=2,
    max_nodes=10,
    scale_up_heat_threshold=0.7,    # avg heat > 70% → scale up
    scale_down_heat_threshold=0.2,  # avg heat < 20% → scale down
    scale_up_cooldown_sec=120,      # wait 2 min between scale-ups
    scale_down_cooldown_sec=300,    # wait 5 min between scale-downs
    scale_up_step=1,
    scale_down_step=1,
    on_scale_up=add_nodes,
    on_scale_down=remove_nodes,
)
master = MasterNode(
    port=7070,
    scheduler=ClusterScheduler(),   # heat signals need a scheduler
    autoscaler=autoscaler,
)
master.start()
```

Scale-up fires when: alive nodes < `min_nodes`, or avg heat > `scale_up_heat_threshold`.
Scale-down fires when: alive nodes > `max_nodes`, or avg heat < `scale_down_heat_threshold` (and alive > `min_nodes`).

Cooldowns prevent thrashing. The autoscaler works without a scheduler (node-count signals only), or alongside one for heat-based decisions too.

```bash
curl http://localhost:7070/v1/autoscaler/status
```

```json
{
  "min_nodes": 2, "max_nodes": 10,
  "last_decision": "scale_up",
  "last_reason": "avg_heat (0.82) > threshold (0.70)",
  "scale_event_count": 3,
  "history": [...]
}
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

## Rolling Updater

Updates all cluster nodes one batch at a time with zero downtime. Provide an `update_fn` — the actual upgrade command — and the updater handles sequencing, health gating, and drain timing.

```python
from huddle_cluster_pkg import MasterNode, ClusterRollingUpdater
import subprocess

def upgrade(node):
    # SSH in, pull new image, restart — whatever your infra needs
    subprocess.run(
        ["ansible-playbook", "upgrade.yml", f"--limit={node['address']}"],
        check=True,
    )

updater = ClusterRollingUpdater(
    update_fn=upgrade,
    batch_size=1,           # one node at a time
    drain_timeout_sec=60,   # wait up to 60s for node to come back healthy
    health_gate_ratio=0.5,  # pause if < 50% of nodes are alive
    update_order="alive_first",
    on_node_updated=lambda nid: print(f"{nid} upgraded"),
    on_node_failed=lambda nid, err: alert_ops(nid, err),
    on_rollout_complete=lambda: close_incident(),
)
master = MasterNode(port=7070, rolling_updater=updater)
master.start()
```

Trigger and control via REST:

```bash
# Kick off
curl -X POST http://localhost:7070/v1/rollout/start

# Check progress
curl http://localhost:7070/v1/rollout/status
```

```json
{
  "phase": "running",
  "total_nodes": 4, "nodes_done": 2, "nodes_remaining": 2,
  "outcomes": {
    "web-01": { "status": "updated", "started_at": 1720000010.1 },
    "web-02": { "status": "updated", "started_at": 1720000071.3 },
    "web-03": { "status": "pending" },
    "web-04": { "status": "pending" }
  }
}
```

```bash
curl -X POST http://localhost:7070/v1/rollout/pause
curl -X POST http://localhost:7070/v1/rollout/resume
curl -X POST http://localhost:7070/v1/rollout/abort   # stops now, no revert
```

Health gate: if the alive ratio falls below `health_gate_ratio` before a batch, the rollout pauses automatically and resumes once the cluster recovers — preventing a cascading failure during an upgrade.

---

## Service Discovery

Nodes advertise the services they provide via join metadata. The registry automatically excludes dead and quarantined nodes from results.

```bash
# Nodes announce via metadata at join time
huddle-cluster agent start --id api-01 --port 8080 \
    --meta services=api,internal-rpc

# Or announce at runtime via REST
curl -X POST http://localhost:7070/v1/discovery/announce \
  -d '{"node_id": "api-01", "service": "api"}'
```

```python
from huddle_cluster_pkg import MasterNode, ServiceDiscovery

sd = ServiceDiscovery(
    refresh_interval_sec=5.0,
    dns_port=8053,              # optional built-in DNS responder
    dns_domain="cluster.local",
    on_service_up=lambda svc, nodes: print(f"{svc} up: {len(nodes)} node(s)"),
    on_service_down=lambda svc: alert_ops(f"{svc} has no alive nodes"),
)
master = MasterNode(port=7070, service_discovery=sd)
master.start()
```

Look up alive nodes for a service:

```bash
curl http://localhost:7070/v1/discovery/services/api
```

```json
{
  "service": "api",
  "alive_count": 2,
  "nodes": [
    {"node_id": "api-01", "address": "10.0.0.1", "port": 8080},
    {"node_id": "api-02", "address": "10.0.0.2", "port": 8080}
  ]
}
```

List all services with their alive counts:

```bash
curl http://localhost:7070/v1/discovery/services
```

Optional DNS: if `dns_port` is set, the registry answers A-record queries for `<service>.<dns_domain>` using alive node addresses (pure stdlib, no external deps):

```bash
dig @localhost -p 8053 api.cluster.local A
# Returns A records for 10.0.0.1 and 10.0.0.2
```

---

## High-Availability Master

Run multiple masters with Raft-based leader election — no single point of failure.

```python
from huddle_cluster_pkg import MasterNode, ClusterHA

# Master 1
ha1 = ClusterHA(
    node_id="master-1",
    peers=["http://master-2:7071", "http://master-3:7072"],
    election_timeout_sec=2.0,
    heartbeat_interval_sec=0.5,
    sync_interval_sec=1.0,
)
master1 = MasterNode(port=7070, ha=ha1)
master1.start()
```

Each master runs independently. One is elected leader via Raft; the others become followers.

```bash
curl http://master-1:7070/v1/ha/status
```

```json
{
  "node_id": "master-1",
  "role": "leader",
  "term": 3,
  "leader_id": "master-1",
  "leader_url": "http://master-1:7070",
  "peers": ["http://master-2:7071", "http://master-3:7072"],
  "peer_count": 2
}
```

**Leader writes, followers redirect.** Agents and clients that send writes (join, heartbeat, leave) to a follower get `HTTP 307` with `X-Leader-URL` and `{"leader_url": "..."}` in the body, so they can retry against the leader.

**State replication.** The leader pushes a full registry snapshot to all followers every `sync_interval_sec`. Followers serve read requests (`GET /v1/nodes`, `GET /v1/status`, etc.) from their local cache.

**Failover.** When the leader stops, followers detect the missing heartbeat after `election_timeout_sec` and hold a new election. A 3-node cluster tolerates 1 failure; a 5-node cluster tolerates 2. A 2-node cluster cannot tolerate any failure — this is correct Raft behaviour, not a limitation of HuddleCluster.

```python
# Useful defaults
ClusterHA(
    node_id="m1",
    peers=["http://m2:7071"],
    election_timeout_sec=2.0,    # randomised [2, 4]s to avoid split votes
    heartbeat_interval_sec=0.5,  # leader heartbeat cadence
    sync_interval_sec=1.0,       # how often to push state snapshots
    request_timeout_sec=1.0,     # RPC call timeout
)
```

---

## Multi-Region

Nodes declare their region via join metadata; the manager tracks which regions are alive and lets the scheduler prefer nearby nodes without ever dropping traffic if a region goes dark.

```bash
huddle-cluster agent start --id web-01 --port 8080 --meta region=us-east
```

```python
from huddle_cluster_pkg import MasterNode, ClusterScheduler, MultiRegionManager

mr = MultiRegionManager(
    preferred_region="us-east",
    fallback_to_global=True,   # use the whole cluster if us-east is empty
    on_region_up=lambda r, nodes: print(f"{r} up: {len(nodes)} node(s)"),
    on_region_down=lambda r: alert_ops(f"{r} has no alive nodes"),
)
master = MasterNode(
    port=7070,
    scheduler=ClusterScheduler(),
    multi_region=mr,
)
master.start()
```

Region-aware placement, narrowing the scheduler's pool when possible:

```python
node = scheduler.pick(master.nodes(), preferred_region="us-east")
# Returns a us-east node if any are alive; otherwise falls back to the
# full cluster automatically — a regional outage degrades gracefully
# rather than dropping requests.
```

Look up regions via REST:

```bash
curl http://localhost:7070/v1/regions
curl http://localhost:7070/v1/regions/us-east
```

```json
{
  "region": "us-east",
  "alive_count": 3,
  "nodes": [
    {"node_id": "web-01", "address": "10.0.1.1", "port": 8080},
    {"node_id": "web-02", "address": "10.0.1.2", "port": 8080},
    {"node_id": "web-03", "address": "10.0.1.3", "port": 8080}
  ]
}
```

Runtime announcement (no restart needed):

```bash
curl -X POST http://localhost:7070/v1/regions/announce \
  -d '{"node_id": "web-04", "region": "eu-west"}'
```

---

## Cluster Circuit Breaker

Tracks `error_rate` forwarded by agents via heartbeat metrics and automatically trips when a node exceeds the threshold. Tripped nodes are excluded from the scheduler's eligible pool before traffic reaches them.

```python
from huddle_cluster_pkg import MasterNode, ClusterScheduler, ClusterCircuitBreaker

breaker   = ClusterCircuitBreaker(
    trip_threshold=0.5,        # error_rate > 50% → trip
    reset_timeout_sec=30.0,    # half-open probe after 30 s
    on_trip=lambda nid, er: alert_ops(nid, er),
    on_reset=lambda nid: resolve_incident(nid),
)
scheduler = ClusterScheduler(circuit_breaker=breaker)
master    = MasterNode(port=7070, scheduler=scheduler, circuit_breaker=breaker)
master.start()
```

Agents forward `error_rate` via their paired `HuddleCluster` instance metrics. Nodes that do not forward this metric are always treated as healthy — the breaker only acts on evidence.

```bash
# All breaker states
curl http://localhost:7070/v1/breakers

# Single node
curl http://localhost:7070/v1/breakers/web-01

# Manual reset after operator investigation
curl -X POST http://localhost:7070/v1/breakers/web-01/reset
```

```json
{
  "trip_threshold": 0.5,
  "open_breakers": 1,
  "states": [
    {"node_id": "web-01", "state": "open",
     "last_error_rate": 0.82, "trip_count": 2},
    {"node_id": "web-02", "state": "closed",
     "last_error_rate": 0.12, "trip_count": 0}
  ]
}
```

States: `closed` (healthy) → `open` (tripped, excluded from scheduling) → `half_open` (probe window after timeout) → `closed` (auto-reset on recovery).

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
- [x] Auto scaling — ClusterAutoScaler with heat + node-count signals — v3.1.0
- [x] Rolling updates — ClusterRollingUpdater with health gate — v3.2.0
- [x] Service discovery — health-aware registry, metadata-driven, DNS responder — v3.3.0
- [x] High-availability master — simplified Raft leader election + state replication — v3.4.0
- [x] Multi-region support — cross-datacenter topology, region-aware scheduling — v3.5.0