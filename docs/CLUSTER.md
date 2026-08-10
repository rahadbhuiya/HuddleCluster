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
| `GET` | `/v1/ratelimits` | viewer | All node token-bucket states |
| `GET` | `/v1/ratelimits/{node_id}` | viewer | Single node bucket state |
| `POST` | `/v1/ratelimits/{node_id}/reset` | admin | Refill a bucket to capacity |
| `POST` | `/v1/canary/start` | admin | Begin canary deployment |
| `GET` | `/v1/canary/status` | viewer | Phase, weight, pool sizes, history |
| `POST` | `/v1/canary/advance` | admin | Step weight to next level |
| `POST` | `/v1/canary/promote` | admin | Graduate canary to stable |
| `POST` | `/v1/canary/abort` | admin | Return all traffic to stable |
| `POST` | `/v1/canary/announce` | admin | Runtime-tag a node as canary |
| `GET` | `/dashboard` | none | Web topology dashboard |

---

## Persistence

Neither the node registry nor Raft term/voted_for survive a process
restart by default — a crashed master comes back with an empty registry
(nodes self-heal by re-joining on their next heartbeat) and, if HA is
enabled, an HA node could in principle revote in a term it already voted
in. Both are opt-in to persist, as of v4.5.0:

```python
master = MasterNode(
    port=7070,
    state_file="/var/lib/huddle/master_registry.json",
    state_save_interval_sec=5.0,     # default; snapshot cadence while running
)

ha = ClusterHA(
    node_id="master-1",
    peers=["http://master-2:7071"],
    state_file="/var/lib/huddle/ha_state.json",   # term/voted_for only
)
```

Notes:
- Both writes are atomic (`tmp` file + `fsync` + `os.replace`) — a crash
  mid-write never leaves a half-written file.
- `MasterNode.state_file` snapshots the full node registry (status,
  metadata, heartbeat counters) every `state_save_interval_sec` while
  running, plus once more on a graceful `stop()`. On restart, restored
  nodes keep their last-known status — the normal heartbeat-timeout
  logic then re-evaluates liveness within one timeout window.
- `ClusterHA.state_file` only persists `term`/`voted_for` (a few bytes),
  written synchronously on every vote grant and term change — this one
  is a correctness property, not just convenience, so there's no
  interval/batching.
- Circuit breaker trip state, rate limiter buckets, and canary rollout
  progress are **not** persisted (still in-memory only) — these are
  short-lived/self-correcting by design (a breaker retrips within
  seconds if the underlying error rate is still high), so the
  restart-loses-it tradeoff is deliberate for now.

---

## TLS / HTTPS

By default the master serves plain HTTP — fine for a private network, but
`api_keys` travel in plaintext unless you put TLS in front of it. As of
v4.4.0 the master can terminate TLS itself, so a reverse proxy is no
longer required just to get encryption:

**No cert handy for local testing?** `python gen_cert.py` (repo root)
generates a throwaway self-signed `server.crt`/`server.key` for
`localhost`/`127.0.0.1` — pure Python (`cryptography` package), no
`openssl` CLI required. Fine for trying TLS locally; don't use it for
anything real (see [Agent-side TLS configuration](#agent-side-tls-configuration)
for how to point an agent at it without disabling verification).

```python
# HTTPS — server certificate only
master = MasterNode(
    port=7070,
    tls_certfile="/etc/huddle/server.crt",
    tls_keyfile="/etc/huddle/server.key",
)

# mTLS — also verify the client's certificate (strong node identity,
# stronger than an API key alone since it can't be replayed off a
# compromised log line)
master = MasterNode(
    port=7070,
    tls_certfile="/etc/huddle/server.crt",
    tls_keyfile="/etc/huddle/server.key",
    tls_ca_certs="/etc/huddle/ca.crt",             # CA that signs client certs
    tls_require_client_cert=True,                   # reject if no valid client cert
)
```

Or via the CLI:

```bash
huddle-cluster master start --port 7070 \
    --tls-cert /etc/huddle/server.crt --tls-key /etc/huddle/server.key \
    --tls-ca /etc/huddle/ca.crt --tls-require-client-cert
```

Notes:
- `tls_certfile`/`tls_keyfile` must be given together — one without the
  other raises `ValueError` at construction time.
- `tls_require_client_cert=True` requires `tls_ca_certs` for the same
  reason — there'd be nothing to verify the client cert against otherwise.
- `tls_ca_certs` without `tls_require_client_cert` verifies a client cert
  *if the client presents one*, but doesn't require one (`CERT_OPTIONAL`).
- Once TLS is enabled, plain HTTP requests to that port are rejected at
  the TLS handshake — there's no dual HTTP+HTTPS listener on one port.
- Certificate generation/rotation/renewal is out of scope here — bring
  your own certs (e.g. from your internal CA, `cert-manager`, or Let's
  Encrypt for a public master).

### Agent-side TLS configuration

If the master uses HTTPS, `AgentNode` needs to know whether/how to trust
its certificate — this is a separate setting from the master's own
`tls_*` params:

```python
from huddle_cluster_pkg import AgentNode

# Recommended: point the agent at the master's cert (or the CA that
# signed it). Verifies the master's identity properly.
agent = AgentNode(
    node_id="web-1", master_url="https://master:7070", port=8080,
    tls_ca_certs="/etc/huddle/server.crt",
)

# Dev/testing only: skip verification entirely. Do NOT use this over an
# untrusted network — it defeats TLS's protection against
# man-in-the-middle attacks.
agent = AgentNode(
    node_id="web-1", master_url="https://master:7070", port=8080,
    tls_verify=False,
)

# mTLS: present a client certificate if the master requires one
# (tls_require_client_cert=True on the master side).
agent = AgentNode(
    node_id="web-1", master_url="https://master:7070", port=8080,
    tls_ca_certs="/etc/huddle/server.crt",
    tls_client_cert="/etc/huddle/agent.crt",
    tls_client_key="/etc/huddle/agent.key",
)
```

Or via the CLI: `--tls-ca`, `--tls-no-verify`, `--tls-client-cert`,
`--tls-client-key` on `huddle-cluster agent start`.

**If you skip this against a self-signed master cert:** the agent's
`urlopen()` uses Python's default certificate verification (system
trust store), which a self-signed cert fails. Versions before v4.11.0
swallowed that failure into a bare `None` return with only a
debug-level log line — join retries would fail silently with no
actionable error. As of v4.11.0, TLS certificate verification failures
are logged at `warning` level with a message pointing at `tls_ca_certs`/
`tls_verify`, so this is diagnosable without turning on debug logging.

### Node identity via mTLS

When a node joins over a connection with a verified client certificate
(`tls_ca_certs` configured), the certificate's Common Name is recorded
on that node's record as `tls_client_cn`:

```bash
curl https://master:7070/v1/nodes/agent-1 -H "Authorization: Bearer ..."
```
```json
{ "node_id": "agent-1", "tls_client_cn": "agent-1.internal.example.com", ... }
```

This is a stronger identity signal than an API key alone: an API key is
a bearer secret (whoever holds it can join as anyone), whereas a client
certificate is bound to a private key that never leaves the agent and
can't be replayed from a log line or a leaked config file. It's *not*
currently used to gate authorization (that's still `api_keys` /
`_check_auth`) — think of it as an audit trail and a building block, not
a replacement for RBAC. `tls_client_cn` is `None` for plain-HTTP
connections or when no client cert was presented.

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
                             [--tls-cert] [--tls-key] [--tls-ca] [--tls-require-client-cert]
                             [--state-file] [--state-save-interval]
                             [--features PATH_OR_JSON]

huddle-cluster agent  start  --id ID --master URL --port PORT
                             [--address IP] [--interval SEC]
                             [--retry N] [--meta key=val ...] [--api-key KEY]
                             [--tls-ca] [--tls-no-verify] [--tls-client-cert] [--tls-client-key]

huddle-cluster nodes  list   [--master URL] [--api-key KEY]
                             [--status alive,quarantined] [--limit N] [--offset N]
huddle-cluster nodes  status NODE_ID [--master URL] [--api-key KEY]

huddle-cluster cluster status  [--master URL] [--api-key KEY]
huddle-cluster cluster health  [--master URL]
huddle-cluster cluster metrics [--master URL] [--api-key KEY]
huddle-cluster cluster openapi [--master URL]
```

### CLI feature config (`--features`)

As of v4.13.0, `--features` on `master start` enables the "advanced"
features (circuit breaker, rate limiter, canary, autoscaler, service
discovery, observability, HA) straight from the CLI, without writing
any Python. It takes either a path to a `.json` file or an inline JSON
string; each top-level key names a feature, and its value is passed as
keyword arguments to that feature's constructor (same arguments as the
Python API sections above).

```bash
huddle-cluster master start --port 7070 --features features.json
```

`features.json`:

```json
{
  "circuit_breaker": { "trip_threshold": 0.5, "reset_timeout_sec": 60 },
  "rate_limiter":    { "capacity": 100, "refill_rate": 50 },
  "autoscaler":      { "min_nodes": 3, "max_nodes": 10 },
  "observability":   { "service_name": "prod-master", "otlp_endpoint": "http://otel-collector:4318" },
  "canary":          { "weight_steps": [5, 25, 50, 100] },
  "service_discovery": { "dns_port": 8053 },
  "ha": {
    "node_id": "master-1",
    "peers": ["http://master-2:7071", "http://master-3:7072"],
    "state_file": "/var/lib/huddle/ha_state.json"
  }
}
```

Or inline, for a quick one-off:

```bash
huddle-cluster master start --port 7070 \
  --features '{"autoscaler": {"min_nodes": 3, "max_nodes": 10}}'
```

Notes:
- If `circuit_breaker`, `rate_limiter`, or `canary` is present, a
  `ClusterScheduler` is automatically built and wired with them — that's
  what makes `GET /v1/scheduler/next` actually apply their exclusion/
  weighting logic, not just expose their status endpoints.
- Callback hooks (`on_trip`, `on_scale_up`, `on_weight_change`, etc.)
  aren't configurable from JSON (they're Python functions) — CLI-started
  features rely on the same internal `logger.info()`/`logger.warning()`
  calls those classes already make on every state change. Use the
  Python API directly (`MasterNode(..., circuit_breaker=ClusterCircuitBreaker(on_trip=...))`)
  if you need custom callback behavior.
- Bad config (unknown feature name, invalid constructor argument, wrong
  type) fails fast with a specific error message at startup, not a raw
  traceback.

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

**State replication.** The leader pushes a full registry snapshot to all followers every `sync_interval_sec`, *and* once immediately upon winning an election (as of v4.7.0) rather than waiting for the first periodic tick — this closes most of the staleness window right after a failover, when a second failure would otherwise be most costly. Followers serve read requests (`GET /v1/nodes`, `GET /v1/status`, etc.) from their local cache.

**Failover.** When the leader stops, followers detect the missing heartbeat after `election_timeout_sec` and hold a new election. A 3-node cluster tolerates 1 failure; a 5-node cluster tolerates 2. A 2-node cluster cannot tolerate any failure — this is correct Raft behaviour, not a limitation of HuddleCluster.

### Honest limitations

This is a **simplified** Raft, and it's worth being precise about what
that means rather than calling it "hardened" and moving on:

- **Full-snapshot replication, not a log.** Real Raft replicates a log
  of individual entries with a commit index and matches log positions
  between leader and followers before committing. This implementation
  replicates the *entire current registry* on each sync tick (or
  immediately on election, since v4.7.0). Simpler, but it means there's
  no notion of "this specific write is durably committed to a majority"
  — a write on the leader is visible to followers only at the next
  snapshot push, not synchronously as part of the write itself.
- **No cluster membership changes.** `peers` is fixed at construction.
  Adding/removing a master requires restarting every node with an
  updated peer list — there's no joint-consensus membership-change
  protocol.
- **Term/vote are persisted (v4.5.0+), the registry snapshot is not
  (per-node, via `state_file` on `MasterNode` itself).** These are two
  separate persistence mechanisms — see [Persistence](#persistence)
  above.
- **Not independently audited or chaos-tested** against network
  partitions, clock skew, or Byzantine peers. Treat it as "meaningfully
  better than nothing" for tolerating a clean node failure, not as a
  drop-in replacement for etcd/Consul in an adversarial or
  regulatory-compliance context.

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

## Rate Limiter

Per-node token bucket that prevents burst traffic from overwhelming individual nodes. When a node's bucket empties, the scheduler skips it and picks the next eligible node, naturally spreading load.

```python
from huddle_cluster_pkg import MasterNode, ClusterScheduler, ClusterRateLimiter

limiter   = ClusterRateLimiter(
    capacity=100,      # max burst per node
    refill_rate=50.0,  # tokens added per second (sustained throughput)
    on_rate_limited=lambda nid: print(f"{nid} rate-limited"),
)
scheduler = ClusterScheduler(rate_limiter=limiter)
master    = MasterNode(port=7070, scheduler=scheduler, rate_limiter=limiter)
master.start()
```

Each `scheduler.pick()` call consumes 1 token from the chosen node's bucket. Buckets refill continuously; a node with `refill_rate=50` can sustain 50 requests/second indefinitely while handling bursts up to `capacity=100`.

```bash
# All node bucket states
curl http://localhost:7070/v1/ratelimits

# Single node
curl http://localhost:7070/v1/ratelimits/web-01

# Operator refill (e.g. after a quiet period or manual investigation)
curl -X POST http://localhost:7070/v1/ratelimits/web-01/reset
```

```json
{
  "capacity": 100, "refill_rate": 50,
  "rate_limited_nodes": 1,
  "buckets": [
    {"node_id": "web-01", "tokens": 0.0, "rate_limited": true,
     "utilisation": 1.0, "consumed_total": 4820},
    {"node_id": "web-02", "tokens": 87.3, "rate_limited": false,
     "utilisation": 0.127, "consumed_total": 3201}
  ]
}
```

---

## Canary Deployment

Gradually shift traffic from stable nodes to canary (new-version) nodes, with full control at every step.

```python
from huddle_cluster_pkg import MasterNode, ClusterScheduler, ClusterCanaryDeployment

canary    = ClusterCanaryDeployment(
    weight_steps=[5, 25, 50, 100],
    on_promote=lambda: finalize_deploy(),
    on_abort=lambda: rollback(),
    on_weight_change=lambda w: print(f"Traffic at {w:.0f}% canary"),
)
scheduler = ClusterScheduler(canary=canary)
master    = MasterNode(port=7070, scheduler=scheduler, canary=canary)
master.start()
```

Tag nodes as canary via metadata at join time:

```bash
huddle-cluster agent start --id web-v2-1 --port 8081 --meta canary=true
huddle-cluster agent start --id web-v1-1 --port 8080  # stable by default
```

Or at runtime:

```bash
curl -X POST http://localhost:7070/v1/canary/announce \
  -d '{"node_id": "web-v2-2"}'
```

Control the deployment:

```bash
# Start at 5% canary traffic
curl -X POST http://localhost:7070/v1/canary/start -d '{"weight": 5}'

# Step up to 25%
curl -X POST http://localhost:7070/v1/canary/advance

# Check status
curl http://localhost:7070/v1/canary/status

# Looks good — graduate
curl -X POST http://localhost:7070/v1/canary/promote

# Something wrong — back to stable immediately
curl -X POST http://localhost:7070/v1/canary/abort
```

```json
{
  "phase": "active",
  "weight_pct": 25.0,
  "canary_nodes": 2,
  "stable_nodes": 6,
  "history": [
    {"action": "start",   "weight": 5.0},
    {"action": "advance", "weight": 25.0}
  ]
}
```

Each `scheduler.pick()` routes probabilistically — `weight%` of calls go to the canary pool, the remainder to the stable pool — using the scheduler's existing thermal fitness scoring within each pool.

---

## Observability

```python
from huddle_cluster_pkg import MasterNode, ClusterObservability

obs = ClusterObservability(
    service_name="huddle-cluster-prod",
    otlp_endpoint="http://otel-collector:4318",   # optional — see below
)
master = MasterNode(port=7070, observability=obs)
master.start()
```

**Structured JSON logging.** Once attached, the process logger emits
single-line JSON instead of plain text (`ts`, `level`, `service`,
`message`, plus `trace_id`/`node_id`/`fields` when present). Set
`json_logs=False` to skip touching the logging config.

**Distributed trace IDs.** Every request gets a trace ID — propagated
from an `X-Trace-Id` request header if present, minted otherwise —
echoed back on the response and attached to every log line and
buffered event from that request.

**Local query API** (always available, no external system needed):

```bash
curl https://master:7070/v1/observability/status
curl https://master:7070/v1/observability/logs?limit=20&trace_id=abc123
```

**OTLP export (optional).** As of v4.8.0, set `otlp_endpoint` to also
push buffered events to any OTLP/HTTP-compatible collector (Jaeger,
Tempo, an OpenTelemetry Collector, Grafana Cloud, etc.) — JSON encoding,
no `opentelemetry-*`/protobuf/grpc dependency required:

```python
obs = ClusterObservability(
    otlp_endpoint="http://otel-collector:4318",
    otlp_headers={"Authorization": "Bearer <token>"},   # optional
    otlp_flush_interval_sec=5.0,                          # default
)
```

Notes:
- Export runs on a background thread, POSTing to
  `{otlp_endpoint}/v1/logs` as OTLP logs JSON on each flush interval,
  plus once more on `stop()` to flush anything pending.
- Best-effort: a failed export is logged and retried next interval —
  it never raises into request-handling code, and never drops events
  (they stay in the local buffer, subject to the normal `buffer_size`
  eviction, until a send succeeds).
- `summary()` / `GET /v1/observability/status` report `otlp.exported_count`,
  `otlp.error_count`, and `otlp.last_error` for monitoring the exporter
  itself.
- Local trace IDs are 8 bytes (16 hex chars); OTLP's `traceId` field
  expects 16 bytes (32 hex chars), so they're left-padded with zeros on
  export — collectors that validate length accept them, but don't
  expect them to collide-resist the same way a full 16-byte ID would in
  a very high-volume, multi-service trace (HuddleCluster's trace IDs
  are scoped to a single master's request lifecycle, not a full
  distributed span tree).

---

## Behaviour Highlights

- **Dead detection** — a node is marked `dead` if no heartbeat arrives within `heartbeat_timeout_sec`. It auto-recovers to `alive` when heartbeats resume (or to `quarantined` if it has been flapping).
- **Auto-rejoin** — if the master restarts and loses its registry, each agent re-registers itself within 3 × `heartbeat_interval` automatically.
- **Fast shutdown** — `master.stop()` and `agent.stop()` both complete in under 100 ms.

---

## Deployment

As of v4.9.0, `deploy/` has a Dockerfile, a docker-compose demo, and
basic Kubernetes manifests — a starting point, not a hardened deployment
you should point at production without reading it first.

**Docker:**

```bash
docker build -f deploy/docker/Dockerfile -t huddlecluster:latest .
docker run -p 7070:7070 huddlecluster:latest master start --port 7070
docker run huddlecluster:latest agent start --id web-1 --master http://host.docker.internal:7070 --port 8080
```

**docker-compose (1 master + 3 agents, local demo):**

```bash
cd deploy/docker && docker compose up --build
curl http://localhost:7070/v1/nodes
```

**Kubernetes:**

```bash
kubectl apply -f deploy/k8s/
```

`deploy/k8s/master.yaml` — a single-replica Deployment (Recreate
strategy, so two masters never write the same PVC concurrently),
PersistentVolumeClaim for `--state-file`, and a Secret for the API key.
`deploy/k8s/agent-daemonset.yaml` — one agent per node via DaemonSet,
using `spec.nodeName` as the agent ID.

**Graceful shutdown matters here:** `docker stop` and Kubernetes pod
termination send `SIGTERM`, not `SIGINT` (Ctrl-C) — the CLI now
(v4.9.0) translates `SIGTERM` into the same graceful-stop path, so
`--state-file` gets a final flush before the process exits.
`terminationGracePeriodSeconds` in the Deployment/DaemonSet gives that
shutdown time to complete rather than being hard-killed.

**What this deployment setup does *not* give you** — same honesty as
elsewhere in this doc:
- No Helm chart (plain manifests only) — no templating for
  multi-environment values, no chart versioning
- No image published to a registry — `image: huddlecluster:latest`
  assumes you build and push it yourself
- No HA wiring in the manifests — `master.yaml` deploys one replica;
  see [High-Availability Master](#high-availability-master) to run 3
  and wire `ClusterHA(peers=...)` across them yourself
- No network policies, pod security context, or ingress/TLS-at-the-edge
  configuration — bring your own per your cluster's standards
- Not load-tested at Kubernetes scale (hundreds of agent Pods against
  one master) — the [WAN / high-latency benchmark](#running-benchmarks)
  below is Docker-bridge/local only, not a K8s-scale validation

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

# WAN-latency simulation (region-realistic latency/jitter/loss, no Docker needed)
python benchmarks/benchmark_wan.py
```

### WAN / high-latency benchmark

`benchmark_http.py` validates against real HTTP servers, but on loopback
— tight, low-jitter latencies (12-22ms), not WAN. `benchmark_wan.py`
runs the same kind of comparison against upstream servers configured
with region-realistic one-way latency (8ms same-region up to 110ms
cross-continent), jitter proportional to latency, and a small rate of
simulated packet loss, to check whether adaptive routing still helps
once individual servers are meaningfully slower and less predictable.

**Be precise about what this does and doesn't validate** — full
reasoning is in the script's docstring, but briefly:
- It's **application-level** latency simulation (each upstream server
  sleeps a randomised duration before responding), not kernel-level
  `tc netem`. netem would be more realistic but needs root +
  CAP_NET_ADMIN + the `sch_netem` kernel module — not guaranteed in a
  sandboxed/CI container (confirmed unavailable in the container this
  was developed in). `--use-netem` attempts it and falls back with a
  warning if unavailable.
- It still runs everything as local processes on one host over
  loopback. It validates the **algorithm** under WAN-like
  latency/jitter/loss characteristics, not real cross-region network
  behavior (actual routing, real congestion, DNS, TLS handshake cost
  over an actual WAN link). A genuine multi-region validation needs
  servers actually deployed in different cloud regions.

Sample result from one run (`--n 300`, 1% simulated loss per request,
6 simulated regions from 8ms to 110ms base latency):

| Balancer | P50 | P95 | P99 | Avg |
|---|---|---|---|---|
| Round Robin | 348.6ms | 637.0ms | 682.6ms | 369.6ms |
| HuddleCluster | 327.0ms | 465.0ms | 524.8ms | 326.9ms |

P95 was ~27% lower for HuddleCluster in that run. Numbers vary between
runs due to randomised jitter/loss — that's real variance, not a typo;
re-run it yourself rather than treating the table above as a
guaranteed result.

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

**Level 3 — Kubernetes/Swarm-grade (complete)**
- [x] Scheduler — thermal-fitness workload placement — v3.0.0
- [x] Auto scaling — ClusterAutoScaler with heat + node-count signals — v3.1.0
- [x] Rolling updates — ClusterRollingUpdater with health gate — v3.2.0
- [x] Service discovery — health-aware registry, metadata-driven, DNS responder — v3.3.0
- [x] High-availability master — simplified Raft leader election + state replication — v3.4.0
- [x] Multi-region support — cross-datacenter topology, region-aware scheduling — v3.5.0

**Level 4 — Observability & Control Plane (complete)**
- [x] Circuit breaker — error-rate-based automatic trip/reset, scheduler exclusion — v4.0.0
- [x] Rate limiter — per-node token bucket, burst protection, scheduler exclusion — v4.1.0
- [x] Canary deployment — weight-based traffic splitting, start/advance/promote/abort — v4.2.0
- [x] Observability — structured JSON logging, distributed trace IDs — v4.3.0