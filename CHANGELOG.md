# Changelog

All notable changes to HuddleCluster are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [3.5.0] - 2026-06-24

### Added — Level 3: Kubernetes/Swarm-grade — COMPLETE (6/6)

**Multi-Region Support** (`huddle_cluster_pkg.cluster_multi_region`)
- New `MultiRegionManager` class: cross-datacenter topology awareness built
  on the same metadata-driven pattern as Service Discovery (v3.3.0)
- Nodes declare their region via join metadata (`{"region": "us-east"}`);
  the manager syncs from the master's registry on every refresh cycle
- Health-aware: dead and quarantined nodes are automatically excluded
  from per-region results, no manual step required
- Runtime announcement: `POST /v1/regions/announce` — nodes can register
  a region without restarting
- `on_region_up(region, nodes)` / `on_region_down(region)` callbacks fire
  on transitions, suitable for cross-region failover alerting
- `preferred_nodes()` returns alive nodes in the preferred region, with
  configurable fallback to the full cluster pool (`fallback_to_global`)
  so traffic is never dropped just because a region is unavailable

**Region-aware scheduling** (`huddle_cluster_pkg.cluster_scheduler`)
- `ClusterScheduler.pick()` gained a `preferred_region` parameter: narrows
  the eligible pool to nodes whose metadata region matches, falling back
  to the full pool automatically when the region has no eligible nodes —
  so a regional outage degrades gracefully rather than dropping requests
- Fully backward compatible — omitting `preferred_region` behaves exactly
  as before

**REST endpoints**
- `GET /v1/regions` — all regions with alive-node counts
- `GET /v1/regions/{name}` — alive nodes in a specific region
- `POST /v1/regions/announce` — node self-announces its region
- `MasterNode.status()` now reports `"multi_region": "enabled"/"disabled"`
- Plug-in design: `MasterNode(multi_region=MultiRegionManager(...))`;
  disabled by default, backward-compatible
- `MultiRegionManager` exported from `huddle_cluster_pkg` top-level

**Tests** (`tests/test_cluster_multi_region.py`, new file)
- 6 unit tests (announce, normalisation, dedup, summary, fallback)
- 12 HTTP integration tests (lookup, metadata refresh, dead-node exclusion,
  callbacks, 400/503 error paths)
- 5 region-aware scheduling tests (preference, fallback, full-pool default)
- 23 total new tests

This completes Level 3 (Kubernetes/Swarm-grade) of the roadmap: Scheduler,
Auto Scaling, Rolling Updates, Service Discovery, HA Master, and now
Multi-Region — all six items shipped.

---

## [3.4.0] - 2026-06-23

### Added — Level 3: Kubernetes/Swarm-grade (in progress, 5/6)

**High-Availability Master** (`huddle_cluster_pkg.cluster_ha`)
- New `ClusterHA` class: simplified Raft consensus layer that wraps a
  `MasterNode` and adds leader election + state replication so the cluster
  coordinator has no single point of failure
- Roles: `follower` (default), `candidate` (no heartbeat within timeout),
  `leader` (won majority vote)
- Election: randomised timeouts `[base, 2×base]` to avoid split votes;
  `RequestVote` RPC contacts all peers and requires a majority; term
  monotonically incremented on each new election
- Heartbeat: leader sends periodic heartbeats to followers to prevent
  spurious re-elections; `AppendEntries` RPC doubles as heartbeat
- State replication: leader pushes full registry snapshots to followers
  every `sync_interval_sec`; followers apply snapshots to their local
  `MasterNode` registry so reads remain available cluster-wide
- Write guard: follower returns `HTTP 307` with `X-Leader-URL` and
  `{"leader_url": "..."}` body on all write requests (join, heartbeat,
  leave, rollout start, etc.); read requests always served locally
- Solo mode: `peers=[]` makes the node an instant leader with no peers
  to contact — useful for development and single-host deployments
- New REST endpoints (no auth required — Raft RPCs must be reachable
  between peers without credentials):
  `GET /v1/ha/status` — role, term, leader URL, peers;
  `POST /v1/ha/vote` — RequestVote RPC;
  `POST /v1/ha/sync` — AppendEntries / state snapshot
- `MasterNode.status()` embeds the full `ClusterHA.status()` dict under
  the `"ha"` key (or `"disabled"` when HA is not configured)
- `MasterNode.start()` / `stop()` automatically attach/detach the HA layer
- `ClusterHA` exported from `huddle_cluster_pkg` top-level
- Note: 2-node clusters cannot elect a new leader after one node fails —
  this is correct Raft behaviour (majority of 2 requires 2 votes);
  use 3+ nodes for fault tolerance

**Tests** (`tests/test_cluster_ha.py`, new file)
- 4 constructor / initial-state unit tests
- 8 Raft RPC unit tests (vote granting, term comparison, step-down,
  append-entries, state snapshot)
- 13 integration tests: solo leader, multi-master election, exactly-one-leader
  invariant, leader/follower roles, state replication, follower redirect,
  3-node failover (new leader after old stops), vote/sync RPC endpoints
- 25 total new tests

---

## [3.3.0] - 2026-06-22

### Added — Level 3: Kubernetes/Swarm-grade (in progress, 4/6)

**Service Discovery** (`huddle_cluster_pkg.cluster_service_discovery`)
- New `ServiceDiscovery` class: health-aware service registry that tracks
  which alive nodes provide which named services
- Nodes advertise services via join metadata (`"services": "api,web"` or a
  list); the registry picks these up automatically on every refresh cycle
- Runtime announcement: `POST /v1/discovery/announce` — nodes can register
  at any time without restarting
- Automatic health gating: dead and quarantined nodes are excluded from
  `GET /v1/discovery/services/<name>` results without any manual step
- `on_service_up(service, nodes)` fires when the first alive node for a
  service appears; `on_service_down(service)` fires when the last one
  disappears — suitable for alerting or DNS propagation hooks
- Optional built-in DNS A-record responder (`dns_port` param): answers
  queries for `<service>.<dns_domain>` (default `cluster.local`) using
  alive node addresses — pure stdlib `socket`, no external deps
- REST endpoints: `GET /v1/discovery/services`, `GET /v1/discovery/services/{name}`,
  `POST /v1/discovery/announce`, `DELETE /v1/discovery/services/{name}/{node_id}`
- `MasterNode.status()` now reports `"service_discovery": "enabled"/"disabled"`
- Plug-in design: `MasterNode(service_discovery=ServiceDiscovery(...))`;
  disabled by default, backward-compatible
- `ServiceDiscovery` exported from `huddle_cluster_pkg` top-level

**Tests** (`tests/test_cluster_service_discovery.py`, new file)
- 7 unit tests (announce, deregister, normalisation, summary)
- 14 HTTP integration tests (lookup, metadata refresh, dead-node exclusion,
  deregister, callbacks, 400/404/503 error paths)
- 2 DNS responder tests (A-record response, wrong-domain ignore)
- 23 total new tests

---

## [3.2.0] - 2026-06-22

### Added — Level 3: Kubernetes/Swarm-grade (in progress, 3/6)

**Rolling Updater** (`huddle_cluster_pkg.cluster_rolling_updater`)
- New `ClusterRollingUpdater` class: orchestrates zero-downtime node upgrades
  one batch at a time; infrastructure-agnostic — provide an `update_fn` and
  wire it to SSH, Ansible, K8s, or any provisioning system
- Per-batch algorithm: health gate check → call `update_fn` for each node in
  the batch (in parallel) → wait up to `drain_timeout_sec` for each node to
  send a heartbeat again before proceeding
- `health_gate_ratio`: if alive ratio drops below threshold, rollout
  auto-pauses and waits for the cluster to recover before proceeding
- `batch_size`: number of nodes to update in parallel per wave
- `update_order`: `"alive_first"` (default) or `"stable_first"` (fewest deaths first)
- Full lifecycle control: `start_rollout()` / `pause()` / `resume()` /
  `abort()` — all callable directly or via REST
- Per-node outcomes tracked: `pending | updated | failed | skipped`, with
  `started_at`, `ended_at`, and error message
- Callbacks: `on_node_updated`, `on_node_failed`, `on_rollout_complete`
- Plug-in design: `MasterNode(rolling_updater=ClusterRollingUpdater(...))`;
  default is disabled, backward-compatible
- `MasterNode.status()` now reports `"rolling_updater": "enabled"/"disabled"`
- New REST endpoints: `POST /v1/rollout/start`, `GET /v1/rollout/status`,
  `POST /v1/rollout/pause`, `POST /v1/rollout/resume`, `POST /v1/rollout/abort`
- `ClusterRollingUpdater` exported from `huddle_cluster_pkg` top-level

**Tests** (`tests/test_cluster_rolling_updater.py`, new file)
- 5 constructor validation tests
- 3 control-method unit tests
- 16 HTTP integration tests covering start/pause/resume/abort, batch size,
  sequential ordering, drain timeout, callbacks, progress counts, conflict handling
- 24 total new tests

---

## [3.1.0] - 2026-06-21

### Added — Level 3: Kubernetes/Swarm-grade (in progress, 2/6)

**Cluster Auto Scaler** (`huddle_cluster_pkg.cluster_autoscaler`)
- New `ClusterAutoScaler` class: monitors alive node count and (optionally)
  average scheduler heat, then fires `on_scale_up` / `on_scale_down`
  callbacks — infrastructure-agnostic; wire to K8s, Terraform, cloud SDK,
  or any provisioning system
- Two scale-up signals: alive count below `min_nodes`, or average scheduler
  heat above `scale_up_heat_threshold`
- Two scale-down signals: alive count above `max_nodes`, or average heat
  below `scale_down_heat_threshold` (when alive > min_nodes)
- Independent cooldown periods (`scale_up_cooldown_sec`,
  `scale_down_cooldown_sec`) prevent thrashing after each action
- `scale_up_step` / `scale_down_step` — how many nodes to recommend per event
- Full history (last 200 events), bounded in memory; `status()` returns
  last 10 plus current decision and last action timestamps
- `ClusterAutoScaler.evaluate()` is callable directly (without the
  background loop) for testing and custom control loops
- Works standalone (node-count signals only) or in combination with
  `ClusterScheduler` (heat signals available when scheduler is attached)
- Plug-in design: `MasterNode(autoscaler=ClusterAutoScaler(...))`;
  default is disabled, backward-compatible
- `MasterNode.status()` now reports `"autoscaler": "enabled"/"disabled"`
- `MasterNode.start()` / `stop()` automatically start/stop the autoscaler
- New REST endpoint: `GET /v1/autoscaler/status`
- `ClusterAutoScaler` exported from `huddle_cluster_pkg` top-level

**Tests** (`tests/test_cluster_autoscaler.py`, new file)
- 5 constructor validation tests
- 16 `evaluate()` unit tests (scale-up/down signals, cooldown, callbacks,
  history bounding, step size)
- 8 HTTP integration tests via `MasterNode`
- 29 total new tests

---

## [3.0.0] - 2026-06-21

### Added — Level 3: Kubernetes/Swarm-grade (in progress, 1/6)

**Cluster Scheduler** (`huddle_cluster_pkg.cluster_scheduler`)
- New `ClusterScheduler` class: thermal-fitness workload placement that
  applies the same inner/outer ring philosophy from the single-instance
  `HuddleCluster` to the multi-node cluster level
- Fitness scoring composed of: freshness (recency of last heartbeat),
  stability (inverse of death count), quarantine penalty (50 % for
  not-yet-trusted nodes), load hint (inverse of forwarded
  `requests_per_sec`), and a warm-up bonus for newly-joined nodes
- Heat model: each time a node is selected its heat increases by 1.0;
  heat decays exponentially with a configurable half-life (`cooldown_sec`,
  default 10 s) so idle nodes cool back down without any explicit signal
- Sticky affinity: pass `?affinity=<key>` to get the same node every time
  for a session/user; falls back to the best available node if the bound
  one dies or becomes quarantined
- `prefer_alive` (default True): alive nodes are always tried before
  quarantined ones, even if the quarantined node has a higher raw score
- `ClusterScheduler.scheduler_stats()`: returns the heat map and per-node
  placement counts for monitoring
- `ClusterScheduler.record_report()`: clients can report workload completion
  (node_id, duration_ms, success); history capped at 1000 entries
- Plug-in design: pass `scheduler=ClusterScheduler()` to `MasterNode` to
  enable; omitting it (the default) keeps backward compatibility entirely
- `MasterNode.status()` now reports `"scheduler": "enabled"/"disabled"`
- New REST endpoints (only mounted when scheduler is configured):
  `GET /v1/scheduler/next [?affinity=]` — pick the best node (503 if none);
  `GET /v1/scheduler/stats` — heat map and workload counts;
  `POST /v1/scheduler/report` — record workload completion
- `ClusterScheduler` exported from `huddle_cluster_pkg` top-level

**Tests** (`tests/test_cluster_scheduler.py`, new file)
- 8 fitness-scoring unit tests
- 11 `pick()` / `record_report()` unit tests
- 13 HTTP integration tests via `MasterNode`
- 32 total new tests

This is the first Level 3 (Kubernetes/Swarm-grade) item and a major
version bump (2.x → 3.0.0), since it introduces a new cluster-level
abstraction (`ClusterScheduler`) and new REST endpoints.

---

## [2.6.0] - 2026-06-20

### Added — Developer Experience (post-Level-2 polish)

**Interactive API Docs** (`huddle_cluster_pkg.cluster_master`)
- New `GET /v1/docs` endpoint: a Swagger UI page rendered against this
  master's own `/v1/openapi.json` (v2.5.0), via the public Swagger UI CDN
  bundle — no build step, same philosophy as the dashboard
- Because the OpenAPI spec already declares a `BearerAuth` security
  scheme, Swagger UI automatically renders an "Authorize" button — paste
  an API key there once and every "Try it out" request carries it
  automatically, no manual header-copying needed
- New `MasterNode.swagger_html()` method, callable directly
- The page itself never requires auth, same reasoning as `/dashboard` and
  `/v1/openapi.json` — it's a static shell; the calls it makes from the
  browser still respect `api_keys` normally
- CLI: `master start` startup banner now also prints the docs URL

**Tests**
- 6 new tests (`TestSwaggerDocs`) — 112 total in `tests/test_cluster_master.py`

---

## [2.5.0] - 2026-06-20

### Added — Level 2: Production Ready — COMPLETE (6/6)

**REST API Expansion** (`huddle_cluster_pkg.cluster_master`)
- `GET /v1/nodes` now supports filtering and pagination:
  `?status=alive,quarantined` (comma-separated, validated — unknown values
  return `400`), `?limit=N` and `?offset=N` (both validated as
  non-negative integers). Response now includes `total`, `limit`, and
  `offset` alongside the existing `nodes` key — fully backward compatible,
  a plain `GET /v1/nodes` with no query string behaves as before
- Results are now sorted by `node_id` for stable pagination across calls
- `MasterNode.nodes()` gained an optional `status` filter parameter,
  usable directly without going through HTTP
- New `GET /v1/openapi.json` endpoint: a complete OpenAPI 3.0.3
  specification of the REST API (paths, parameters, schemas, security
  scheme), never requires auth — same reasoning as `/v1/health`, since
  clients need the spec before they can know how auth even works
- New `MasterNode.openapi_spec()` method, callable directly
- `GET /v1/status` now reports `api_version` (currently `"1.0.0"`) for
  clients that need to detect the API contract version
- CLI: `nodes list` gains `--status`, `--limit`, `--offset`; new
  `cluster openapi` command prints the spec as JSON

**Tests**
- 16 new tests (`TestNodesFilteringAndPagination`) — filtering, pagination,
  validation, sorting, auth interaction
- 6 new tests (`TestOpenApiSpec`) — spec structure, auth exemption,
  path coverage
- 106 total in `tests/test_cluster_master.py`

This closes out Level 2 (Production Ready) of the roadmap. Level 3
(Kubernetes/Swarm-grade: scheduler, auto-scaling, rolling updates, service
discovery, HA master, multi-region) remains entirely unstarted.

---

## [2.4.0] - 2026-06-19

### Added — Level 2: Production Ready (in progress, 5/6)

**Web Dashboard** (`huddle_cluster_pkg.cluster_master`)
- New `GET /dashboard` endpoint: a self-contained HTML/CSS/JS page showing
  real-time cluster topology — no build step, no external JS framework,
  one `<script>` tag polling the existing REST API
- New `MasterNode.dashboard_html()` method, callable directly or served
  via the new route
- Dark "control room" visual theme (Space Grotesk for labels, JetBrains
  Mono for IDs/addresses/numbers) distinct from generic AI-template looks
- Summary cards (total/alive/quarantined/dead) plus a cluster health pill
  driven by the same `cluster_unhealthy` flag from Monitoring (v2.2.0)
- "Huddle strip" — a small colored dot per node, a visual nod to the
  penguin-huddle metaphor the whole project is named after
- Node table sorted problems-first (dead → quarantined → alive,
  alphabetical within each group), showing status, node ID, address,
  heartbeat count, last-seen, and forwarded metrics
- Empty state shows the actual `huddle-cluster agent start` command to run
- Auto-refreshes every 3 seconds via `setInterval` + `fetch` — no
  WebSocket/SSE complexity, matching the raw-`http.server` foundation
- The dashboard page itself never requires auth (it's a static shell, same
  as any HTML page); its `fetch()` calls to `/v1/status` and `/v1/nodes`
  go through the browser exactly like any API client and respect
  `api_keys` (v2.3.0) if configured — entering a key in the page stores it
  in the browser's own `localStorage`, never sent anywhere but back to
  this master
- CLI: `master start` now prints the dashboard URL in its startup banner

**Tests**
- 10 new tests (`TestDashboard`) — 84 total in `tests/test_cluster_master.py`

---

## [2.3.0] - 2026-06-18

### Added — Level 2: Production Ready (in progress, 4/6)

**RBAC / Authentication** (`huddle_cluster_pkg.cluster_master`)
- New `api_keys: Dict[key, role]` constructor param. `None` (default) keeps
  the API open exactly as before — fully backward compatible
- Two roles: `viewer` (GET-only: health, status, metrics, nodes) and
  `admin` (also join/heartbeat/leave). Unrecognized role strings rank as
  no-access — a typo'd role fails closed, not open
- Every request needs `Authorization: Bearer <key>` except `GET /v1/health`,
  which is deliberately exempt so liveness probes don't need credentials
- Unauthorized requests get `401` (missing/invalid key) or `403`
  (valid key, insufficient role), logged as warnings for visibility
- All error responses (auth and otherwise) now consistently include
  `"ok": false` alongside `"error"`, for a uniform client contract
- `AgentNode` gains an `api_key` param and sends it as a Bearer token on
  every join/heartbeat/leave call
- CLI: `master start --api-key KEY=ROLE` (repeatable); `agent start
  --api-key KEY`; `--api-key` added to `nodes list/status` and `cluster
  status/metrics` (not `cluster health`, which never needs auth)

**Tests**
- 13 new tests (`TestAuthentication`) — 74 total in `tests/test_cluster_master.py`
- 6 new tests (`TestAgentApiKey`) — 32 total in `tests/test_cluster_agent.py`

---

## [2.2.0] - 2026-06-18

### Added — Level 2: Production Ready (in progress, 3/6)

**Metrics** (`huddle_cluster_pkg.cluster_master`)
- New `GET /v1/metrics` endpoint: Prometheus text exposition format
  (`text/plain; version=0.0.4`), aggregating the whole cluster from one
  scrape target instead of needing to discover and poll every agent
- Master-level gauges: `huddle_master_uptime_seconds`, `_total_nodes`,
  `_alive_nodes`, `_dead_nodes`, `_quarantined_nodes`, `_unhealthy`
- Per-node gauges/counters labeled by `node_id`: `huddle_node_up` (1=alive,
  0.5=quarantined, 0=dead), `_heartbeat_count`, `_death_count`,
  `_last_seen_seconds`
- Forwarded per-node metrics (only emitted when a node actually reports
  them via heartbeat, so missing data reads as absent, not zero):
  `huddle_node_fairness_score`, `_inner_servers`, `_outer_servers`,
  `_rotation_count`, `_requests_per_sec`
- New `MasterNode.prometheus_metrics()` method (same pattern as the
  existing single-instance `HuddleCluster.prometheus_metrics()`)
- CLI: `huddle-cluster cluster metrics [--master]`

**Monitoring** (`huddle_cluster_pkg.cluster_master`)
- New `unhealthy_alive_ratio` option: fires `on_cluster_unhealthy` when the
  fraction of `alive` nodes drops below the configured ratio, and
  `on_cluster_recovered` when it recovers above it
- Disabled by default (opt-in); an empty cluster (no nodes registered yet)
  is never considered unhealthy
- `status()` now reports `cluster_unhealthy` and `unhealthy_alive_ratio`;
  also exposed as the `huddle_master_unhealthy` Prometheus gauge
- Each transition fires its callback exactly once (no repeat firing while
  the cluster stays in the same health state)

**Tests**
- 16 new tests (`TestPrometheusMetrics`, `TestClusterHealthMonitoring`) —
  61 total in `tests/test_cluster_master.py`

---

## [2.1.0] - 2026-06-17

### Added — Level 2: Production Ready (in progress, 1/6)

**Auto Recovery** (`huddle_cluster_pkg.cluster_master`)
- Flapping detection: a node that dies and recovers `flap_threshold` or more
  times within `flap_window_sec` is not trusted immediately — it is marked
  `quarantined` instead of `alive`
- Quarantine promotion: a quarantined node needs `quarantine_recovery_heartbeats`
  consecutive heartbeats (the triggering heartbeat counts as #1) to be
  promoted back to `alive` with a clean slate (death history cleared)
- Quarantine applies uniformly whether the node recovers via heartbeat alone
  or via re-join (crash-looping agents are caught the same way)
- Quarantined nodes are excluded from `alive_nodes()` (not trusted for
  routing) but are still subject to the normal heartbeat-timeout check, so a
  quarantined node that stops heartbeating still becomes `dead`
- Stale node purge: dead nodes are removed from the registry entirely after
  `purge_after_sec` of silence (disabled by default — opt-in only, to avoid
  surprising existing deployments)
- New callbacks: `on_node_quarantined`, `on_node_purged`
- New `MasterNode` method: `quarantined_nodes()`
- `status()` now reports `quarantined_nodes`, `flap_window_sec`,
  `flap_threshold`, `quarantine_recovery_heartbeats`, `purge_after_sec`
- Defensive startup warning if `purge_after_sec` is configured smaller than
  `heartbeat_timeout_sec` (would make nodes purge-eligible the instant they
  die, leaving no grace period for recovery)
- Heartbeat-monitor check interval no longer floors at 1 second — now scales
  down to `heartbeat_timeout_sec / 5` (min 0.05s), so short timeouts are
  detected responsively
- CLI: `master start` gains `--flap-window`, `--flap-threshold`,
  `--quarantine-recovery`, `--purge-after`; `cluster status` shows the
  quarantined count; `nodes list` STATUS column widened for "quarantined"
- 13 new tests (`TestAutoRecoveryFlapping`, `TestAutoRecoveryPurge`) — 45
  total in `tests/test_cluster_master.py`

### Fixed
- `__version__` in `huddle_cluster.py` was still hardcoded to `1.4.1` after
  the 2.0.0 release; now correctly tracks the package version

---

## [2.0.0] - 2026-06-16

### Added — Level 1: Basic Cluster System

**MasterNode** (`huddle_cluster_pkg.cluster_master`)
- Dedicated master node that acts as the central coordinator for multi-node
  HuddleCluster deployments — does not route traffic itself, only manages topology
- Node enrollment via `POST /v1/nodes/join` with address, port, and arbitrary metadata
- Graceful departure via `DELETE /v1/nodes/{id}`
- Heartbeat reception via `POST /v1/nodes/{id}/heartbeat` with live metrics payload
- Automatic dead-node detection: marks nodes as `dead` when heartbeats stop arriving
  within `heartbeat_timeout_sec` (configurable, default 30 s)
- Auto-recovery: nodes flip back to `alive` the moment a heartbeat is received again
- Event callbacks: `on_node_join`, `on_node_leave`, `on_node_dead`
- REST API: `GET /v1/health`, `GET /v1/status`, `GET /v1/nodes`, `GET /v1/nodes/{id}`
- All responses are JSON; HTTP status codes used correctly (200 / 400 / 404)
- HTTP server poll interval set to 50 ms for near-instant graceful shutdown

**AgentNode** (`huddle_cluster_pkg.cluster_agent`)
- Per-node agent that wraps an optional `HuddleCluster` and handles all master
  communication: join → heartbeat loop → graceful leave
- Automatic local-IP detection when `address` is not supplied
- Configurable heartbeat interval (default 10 s)
- Retry-with-backoff on initial join (`retry` + exponential backoff)
- `threading.Event`-based heartbeat sleep: `stop()` returns in < 50 ms regardless
  of interval length
- Metrics forwarding: `inner_servers`, `outer_servers`, `fairness_score`,
  `rotation_count`, `requests_per_sec` included in every heartbeat payload
- Callbacks: `on_master_unreachable` (first consecutive failure), `on_recovered`
  (first success after outage)
- Auto-rejoin: when master is reachable but doesn't know the node (e.g. after
  master restart), agent re-registers every 3rd consecutive heartbeat failure
- Correctly distinguishes `HTTPError` (master reachable, rejoin needed) from
  `URLError` (master unreachable, wait and retry) in the HTTP helper
- `start()` is now genuinely non-blocking: the initial join (with retry/backoff)
  runs inside the background thread instead of the caller's thread. Fixes a
  Windows-specific issue where connecting to a port nothing is listening on
  does not fail instantly (unlike Linux/macOS) and was blocking `start()` for
  up to the full socket timeout
- New `request_timeout_sec` constructor parameter (default 3.0s, was a
  hardcoded 5.0s) controls the per-HTTP-call socket timeout for join,
  heartbeat, and leave requests
- Join backoff sleep is now interruptible via the stop event, so `stop()`
  returns promptly even if called mid-retry

**CLI** (`huddle_cluster_pkg.cli` / `huddle-cluster` command)
- Installed as the `huddle-cluster` command via `pyproject.toml` entry point
- `huddle-cluster master start  [--host] [--port] [--timeout]`
- `huddle-cluster agent  start  --id --master --port [--address] [--interval] [--retry] [--meta k=v ...]`
- `huddle-cluster nodes  list   [--master]`
- `huddle-cluster nodes  status NODE_ID [--master]`
- `huddle-cluster cluster status [--master]`
- `huddle-cluster cluster health [--master]`  (exits 1 if not healthy)

**Tests**
- `tests/test_cluster_master.py` — 32 tests covering health, join, heartbeat,
  leave, node detail, status counts, callbacks, timeout, recovery, concurrency
- `tests/test_cluster_agent.py` — 26 tests covering init validation, join,
  heartbeat, leave, status, callbacks (unreachable, recovered), multi-agent,
  restart, and non-blocking start() regression

**Package**
- `huddle_cluster_pkg/__init__.py` updated: exports `MasterNode`, `NodeRecord`,
  `AgentNode` at top level
- `pyproject.toml` bumped to 2.0.0; `huddle-cluster` CLI entry point registered

---


## [1.4.1] - 2026-06-08

### Fixed
- Fix wheel packaging: huddle_cluster.py was missing from the built wheel
  due to incorrect setuptools configuration (py-modules was not declared)
- Bump version to 1.4.1 to allow re-upload to PyPI (1.4.0 wheel was
  already uploaded with the broken build)

## [1.4.0] - 2026-06-07

### Added
- Persistent state -- `state_file` and `checkpoint_interval_sec` parameters save cluster
  temperature state to JSON and restore it on restart, preventing cold-start degradation
  after rolling restarts
- Webhook alerting -- `alert_webhooks`, `alert_on`, `alert_headers`, `alert_timeout_sec`
  parameters; POSTs JSON payloads to any HTTP endpoint on eviction, promotion, or
  health-state change events
- Built-in HTTP health checker -- `health_check_path`, `health_check_interval_sec`,
  `health_check_timeout_sec`, `health_check_failures`; probes upstream servers directly
  and evicts without needing an external health check loop
- WebSocket connection draining -- `ws_drain_timeout_sec`, `ws_connection()`,
  `ws_open()`, `ws_close()`; gracefully waits for active WebSocket connections to finish
  before evicting a server
- `huddle_cluster_pkg` extension package:
  - `backends_redis.py` -- Redis shared-state backend for multi-node deployments
  - `grpc_cluster.py` -- Thermal-aware gRPC channel routing
  - `discovery_k8s.py` -- Kubernetes pod auto-discovery via Watch API
- Optional dependency extras: `redis`, `grpc`, `kubernetes`, `simulation`
- 14 new test modules (427 tests total): `test_admin_api`, `test_alerting`,
  `test_canary`, `test_dashboard`, `test_draining`, `test_grpc_cluster`,
  `test_health_checker`, `test_histogram`, `test_k8s_discovery`,
  `test_persistent_state`, `test_redis_backend`, `test_retry`,
  `test_sticky_sessions`, and updates to existing suites

### Fixed
- `ConnectionAbortedError` (Windows WinError 10053) now caught in the dashboard
  SSE stream (`/dashboard/stream`) and admin HTTP handler; Windows clients that
  close the connection no longer print tracebacks to the console
- `huddle_cluster_pkg` was missing `__init__.py`; package is now properly importable
  after `pip install`

---

## [1.3.3] - 2026-05-16

### Added
- Server tags/labels — arbitrary metadata on Server objects
- on_eviction callback — dedicated callback fired on every eviction
- Throughput metrics — requests/sec in health_report() and prometheus_metrics()
- Batch record_latency — feed multiple latency samples in one call
- Configurable request_timeout_ms — controls dead-server timeout threshold
- Graceful shutdown — drain_timeout_sec in stop()
- Circuit breaker — circuit_breaker_threshold parameter
- Server warm-up API — gradual traffic ramp on add_server()
- OpenTelemetry trace_id support in get_server_context()
- health_report() history summary (rotation_rate, most_evicted, avg_dwell)
- Type stubs (.pyi) for IDE autocomplete
- Source distribution (sdist) on PyPI
- GitHub Actions trusted publishing workflow

### Changed
- Improved docstrings on all public methods
- Test coverage expanded for v1.3.0 features

---

## [1.3.2] - 2026-05-14

### Fixed
- Synced version across pyproject.toml and huddle_cluster.py (was 1.3.1 vs 1.3.2)

---

## [1.3.1] - 2026-05-14

### Added
- PyPI package published: `pip install huddle-cluster`
- pyproject.toml with full classifiers, optional dependencies, project URLs
- MANIFEST.in and py.typed marker (PEP 561)
- __init__.py package exports for all public classes

### Fixed
- Dockerfile COPY path for upstream_server.py
- docker-compose.yml obsolete version field removed
- benchmark_industry.py upstream health check removed (containers on internal network)

---

## [1.3.0] - 2026-05-13

### Added
- Weighted server capacity — `weight=2.0` makes server tolerate 2x load before eviction
- Cold start protection — `cold_start_sec=30` keeps new servers in outer ring while warming up
- Absolute latency floor — `absolute_latency_floor_ms=500` guards against majority degradation
- Adaptive thresholds — `adaptive_thresholds=True` auto-adjusts heat/cool from cluster P95 history
- Prometheus metrics exporter — `cluster.prometheus_metrics()` for /metrics endpoint
- Gossip protocol — `GossipAgent` for distributed multi-node deployments (UDP multicast)
- Industry baseline benchmark: NGINX RR vs NGINX LC vs HuddleCluster (Docker)

### Fixed
- Floor-breach eviction re-entry bug: temperature forced to 0.8 on absolute latency eviction
- Adaptive threshold baseline computation: window-based sliding comparison

### Performance
- HC get_server(): 0.295 us (1.07x over RR)
- HC get_server() + record_latency(): 10.7 us
- Peak memory (20 servers): 28.3 KB
- Slow-server detection: 36 requests avg (range 35-40)

---

## [1.2.0] - 2026-05-10

### Added
- Relative latency anomaly scoring — uses cluster-wide median as baseline
- ServerMetrics.update_latency_anomaly(cluster_median_ms)
- ServerMetrics.latency_anomaly_score field
- Inner-ring fairness metric — Gini coefficient over inner server dwell times
- Tunable EMA alpha — ema_alpha= constructor kwarg
- Statistical benchmark: 10 independent trials, Welch t-test, 95% CI
- Real HTTP benchmark: 6 FastAPI upstream servers, actual network calls
- Industry baseline benchmark scripts and Docker configs

### Fixed
- Fairness score now measures inner-ring servers only (outer ring excluded)
- Cluster baseline uses median (not mean) to prevent slow-server self-concealment
- Rolling latency window reduced from 50 to 10 samples for faster detection
- Default heat_threshold lowered from 0.75 to 0.55

### Benchmark Results (10 trials)
- Server failure P95: 500ms -> 23.9 +/- 0.5ms (p < 0.001)
- Server failure Avg: 53.4ms -> 29.7ms (p < 0.001)
- Slow server P95: 63.2ms -> 55.1ms (p = 0.039)

---

## [1.1.0] - 2026-05-08

### Added
- record_latency(server, ms) — real-time latency feedback loop
- get_server_context() — context manager with auto latency recording
- ServerMetrics rolling 10-sample latency window
- ServerMetrics.p95_latency() method
- health_report() per-server avg_latency_ms and p95_latency_ms

### Fixed
- Benchmark showed HuddleCluster worse on slow-server scenario because
  temperature never updated without explicit metrics_updater. record_latency()
  closes this gap.

---

## [1.0.1] - 2026-05-06

### Fixed
- Thundering herd prevention: max evictions per cycle capped at max(1, |I|/3)
- Oscillation damping: EMA smoothing on temperature
- Flapping prevention: min_outer_dwell_sec gate on re-entry
- Lock contention: RLock for reentrant internal calls
- Memory leak: rotation log bounded to MAX_ROTATION_LOG entries
- Empty inner ring: emergency fallback server selection
- Consecutive eviction back-off

---

## [1.0.0] - 2026-05-04

### Added
- Initial release
- Dual-ring architecture (inner deque + outer min-heap)
- EMA temperature scoring (cpu, memory, connections, error rate)
- Rotation daemon with configurable interval
- heat_threshold and cool_threshold parameters
- min_inner_size and max_inner_size bounds
- rotation_cooldown_sec and min_outer_dwell_sec stability parameters
- health_report() JSON snapshot
- fairness_score() Gini coefficient
- create_cluster() factory function
- force_evict() manual eviction
- on_rotation callback
- External metrics_updater callback
- 45 unit tests