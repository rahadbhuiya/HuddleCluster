# Changelog

All notable changes to HuddleCluster are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---


## [4.14.0] - 2026-08-06

### Added — Helm chart (`deploy/helm/huddlecluster/`)

**Chart contents**
- `Chart.yaml`, `values.yaml` (fully commented), `templates/` (master
  Deployment/StatefulSet, agent DaemonSet, Services, Secrets, PVC,
  ConfigMap, ServiceAccount, NOTES.txt)
- Master runs as a Deployment by default, or a StatefulSet when
  `master.ha.enabled=true` — each replica computes its own
  `node_id`/`peers` at container startup from `$HOSTNAME` and the
  headless Service's per-Pod DNS names (standard Kubernetes pattern
  for this kind of peer discovery)
- `values.yaml`'s `master.features` block maps directly onto the
  v4.13.0 CLI `--features` config — circuit breaker, rate limiter,
  canary, autoscaler, service discovery, observability all
  configurable from Helm values
- TLS (existing Secret or your own), state persistence (PVC / per-Pod
  volumeClaimTemplates in HA mode), and API keys (Secret-mounted, read
  by a wrapper entrypoint script — never baked into Pod args, so they
  don't show up in `kubectl describe pod`/`kubectl get pod -o yaml`)
- Agent DaemonSet points at the master Service, mirrors the master's
  TLS trust configuration

**Important caveat — read before real use:** `helm` wasn't available
in the sandbox this chart was developed in (network-restricted;
binary downloads from `get.helm.sh` and GitHub releases were blocked),
so it was **not** validated with `helm lint`/`helm template` against a
real Helm installation. Verification that *was* done: brace-balance
checking every template, manually rendering the default-values path
and confirming valid YAML, extracting the embedded shell wrapper
scripts and syntax-checking with `sh -n`, extracting the embedded
Python snippets and syntax-checking with `ast.parse`. One real bug was
caught and fixed this way (the agent DaemonSet wrapper used
Kubernetes' `$(VAR)` env-substitution syntax inside a `sh -c` string,
where it's interpreted as shell command substitution instead — fixed
to plain `$VAR`). Despite that scrutiny, **run `helm lint
deploy/helm/huddlecluster` yourself before deploying this anywhere
real** — see `deploy/helm/huddlecluster/README.md`.

**Docs**
- `docs/CLUSTER.md` Deployment section: Helm install example, and the
  "what this doesn't give you" list updated to reflect the chart now
  existing (with the verification caveat above still noted)

---



## [4.13.0] - 2026-08-06

### Added — CLI wiring for advanced features (`--features`)

**The gap:** every advanced feature added since Level 4 (circuit
breaker, rate limiter, canary, autoscaler, service discovery,
observability, HA) was Python-API-only — `huddle-cluster master start`
had no way to enable any of them. Using them required writing a custom
Python script to construct the feature classes and pass them into
`MasterNode(...)` by hand, which meant the CLI tool alone wasn't
sufficient to run a production-configured master.

**`huddle-cluster master start` gained `--features PATH_OR_JSON`:**
- Takes a path to a `.json` file or an inline JSON string
- Each top-level key names a feature (`circuit_breaker`, `rate_limiter`,
  `canary`, `autoscaler`, `service_discovery`, `observability`, `ha`);
  its value is passed as keyword arguments to that feature's constructor
- When `circuit_breaker`/`rate_limiter`/`canary` is present, a
  `ClusterScheduler` is automatically built and wired with them, so
  `GET /v1/scheduler/next` actually applies their logic rather than
  just exposing status endpoints
- Startup banner gained a `Features:` line listing what's enabled
- Bad config (unknown feature name, invalid constructor kwarg, wrong
  value type, missing required arg like HA's `node_id`) fails fast at
  startup with a specific, actionable message — `SystemExit`, not a
  raw traceback
- Callback hooks (`on_trip`, `on_scale_up`, etc.) aren't configurable
  from JSON since they're Python functions — CLI-started features rely
  on those classes' existing internal `logger.info()`/`logger.warning()`
  calls on every state change; use the Python API directly for custom
  callback behavior
- Fully backward compatible — omitting `--features` behaves exactly as
  before (verified with a dedicated regression test)

**Docs**
- New "CLI feature config (`--features`)" subsection under CLI
  reference in `docs/CLUSTER.md`, with a full example JSON file

**Tests** (`tests/test_cli_features.py`, new file, 16 tests)
- Unit tests for config loading/validation (inline JSON, file path,
  invalid JSON, non-object top-level, unknown keys)
- Unit tests for feature instantiation (correct classes built with
  correct kwargs, bad kwargs rejected with a clear message, HA's
  required `node_id` enforced)
- Integration tests actually spawning `huddle-cluster master start
  --features ...` as a subprocess and querying its real REST API:
  multiple features enabled together and correctly reflected in
  `GET /v1/status` (including the auto-wired scheduler), a bad-config
  run exits non-zero with a clear stderr message, and a regression
  test confirming `master start` without `--features` behaves exactly
  as it did before this feature existed

---

## [4.12.0] - 2026-08-05

### Fixed — ClusterAutoScaler.status() misreported last_decision/last_reason during cooldown

**Reported symptom:** a user running the interactive autoscaler demo
killed a node to trigger scale-up (alive_nodes below min_nodes). The
`on_scale_up` callback fired repeatedly and `history`/`scale_event_count`
in `status()` correctly showed multiple scale-up events — but the
top-level `last_decision`/`last_reason` fields showed `"none"`/`""`
the entire time, even while the scale-up condition was clearly still
active. This made `GET /v1/autoscaler/status` (and any dashboard/
monitoring built on it) misleadingly look healthy between cooldown-
gated firings.

**Root cause:** `evaluate()` computed the real decision (e.g.
`SCALE_UP`) from the alive-node/heat conditions, then a cooldown guard
would locally reset that same `decision`/`reason` variable to
`SCALE_NONE`/`""` on ticks where the cooldown hadn't expired yet (to
correctly suppress firing a *new* action). The bug: `last_decision`/
`last_reason` were assigned from those same, now-overwritten
variables — so any cooldown-suppressed tick blew away the correct
"scale_up condition is still active" state, even though nothing about
the underlying condition had changed.

**Fix:** capture the raw (pre-cooldown) decision/reason in separate
variables before the cooldown guard runs, and report those via
`last_decision`/`last_reason`. The cooldown-gated variables are still
used for whether to actually fire `_record()`/`on_scale_up`/
`on_scale_down` this tick — unchanged — and `evaluate()`'s return
value is unchanged (still `SCALE_NONE` on a cooldown-suppressed tick,
since no new action was taken). Only the *reporting* of "what does the
autoscaler currently think should happen" was wrong, and only that
changed.

**Tests** (`tests/test_cluster_autoscaler.py`, +3 tests, 32 total in file)
- Regression test reproducing the exact reported scenario: evaluate
  twice with the same alive_nodes below min_nodes and a long cooldown;
  `last_decision` must stay `SCALE_UP` with the reason preserved on
  the second (cooldown-suppressed) call, even though the call's return
  value is correctly `SCALE_NONE`
- Companion test confirming `last_decision` genuinely reports `SCALE_NONE`
  once the condition actually clears (not just cooldown-suppressed)
- Same regression for the scale-down direction
- Full `test_cluster_autoscaler.py` re-run clean, no regressions (32 tests)

---

## [4.11.0] - 2026-08-01

### Fixed — AgentNode couldn't join an HTTPS master with a self-signed certificate

**Reported symptom:** a real user pointed a freshly-generated agent at a
master started with `--tls-cert`/`--tls-key` (self-signed dev cert) and
got `Join rejected by master: None` in an infinite retry loop, with no
indication of why.

**Root cause:** `AgentNode` had no TLS trust configuration at all —
`urllib.request.urlopen()` was called with Python's default SSL
context, which verifies against the system trust store and correctly
rejects a self-signed certificate. That failure (`URLError` wrapping an
`SSLCertVerificationError`) was caught by the same branch as "master
unreachable" and logged at `debug` level — invisible unless debug
logging was explicitly enabled — then surfaced as a bare `None`,
indistinguishable from a network-down condition.

**Fix:**
- `AgentNode` gained `tls_verify` (default `True`), `tls_ca_certs`,
  `tls_client_cert`, `tls_client_key` constructor params. Builds an
  `ssl.SSLContext` once and threads it through all three `urlopen()`
  call sites (join, heartbeat, leave) via a shared `_urlopen()` helper
- `tls_ca_certs` (recommended): verify the master's cert against a
  specific CA/cert file instead of the system trust store — the
  correct fix for a self-signed or internal-CA master cert
- `tls_verify=False` (dev/testing only, documented as such): skip
  verification entirely
- `tls_client_cert`/`tls_client_key`: present a client certificate for
  mTLS, if the master requires one (pairs with the master's
  `tls_require_client_cert` from v4.4.0)
- TLS certificate verification failures are now logged at `warning`
  level with an actionable message (which param to set), instead of a
  swallowed `debug`-level line — this alone would have made the
  original symptom immediately diagnosable
- CLI: `huddle-cluster agent start` gained `--tls-ca`,
  `--tls-no-verify`, `--tls-client-cert`, `--tls-client-key`; startup
  banner prints the resolved TLS trust mode
- Fully backward compatible — omitting all `tls_*` params behaves
  exactly as before (plain HTTP unaffected; HTTPS against a
  publicly-trusted cert unaffected, since the default case doesn't
  build a custom SSLContext at all)

**Docs**
- New "Agent-side TLS configuration" subsection in `docs/CLUSTER.md`
  (under TLS/HTTPS, before Node identity via mTLS), explaining exactly
  the failure mode above and the three ways to resolve it

**Tests** (`tests/test_agent_tls.py`, new file, 8 tests, skipped if
`openssl` CLI unavailable)
- Reproduces the exact reported bug (join fails against a self-signed
  cert with default settings) as a named regression test, plus verifies
  each fix path (`tls_ca_certs`, `tls_verify=False`, mTLS with client
  cert) actually succeeds, plus a plain-HTTP regression guard
- Full `test_cluster_agent.py` (32) + `test_master_tls.py` (10) +
  new file (8) re-run clean, no regressions (50 tests)

---

## [4.10.0] - 2026-07-29

### Added — Level 5: Production Hardening (7/8) — WAN-latency simulation benchmark

**Benchmarks**
- `benchmarks/benchmark_wan.py` (new): real-HTTP benchmark (same style
  as `benchmark_http.py`) against 6 simulated regions with
  region-realistic one-way latency (8ms same-region up to 110ms
  cross-continent), jitter proportional to latency, and 1% simulated
  packet loss per request — compares HuddleCluster vs Round Robin
  under WAN-like conditions instead of loopback's tight 12-22ms range
- `benchmarks/upstream_server.py`: added optional `--jitter` and
  `--loss-pct` CLI params (backward compatible — both default to the
  prior fixed behavior, so `benchmark_http.py`/`benchmark_industry.py`
  etc. are unaffected)
- `--use-netem` flag attempts kernel-level `tc netem` instead of
  application-level sleep-based simulation, with a runtime
  availability probe and clean fallback — confirmed in this repo's
  dev container that netem is *not* available (`tc qdisc add ...
  netem` fails: "Specified qdisc kind is unknown" — no
  `sch_netem` kernel module) — so the default path is application-level

**Honesty note, stated in both the script's docstring and
`docs/CLUSTER.md`:** this is a real, run benchmark (see actual sample
results in `docs/CLUSTER.md`'s new "WAN / high-latency benchmark"
subsection), but it simulates WAN latency/jitter/loss characteristics
on one host over loopback — it validates HuddleCluster's *algorithm*
under those characteristics, not real cross-region network behavior.
Full multi-region validation needs servers actually deployed across
cloud regions, which is out of scope for this repo/session. This is
explicitly the item, of the 8 production-readiness gaps identified,
that could only be *partially* closed rather than fixed outright — see
the docstring and docs section for the complete reasoning rather than
overclaiming "WAN validated" here.

**Docs**
- `docs/CLUSTER.md`: new "WAN / high-latency benchmark" subsection
  under Running Benchmarks, including a real sample result table

---

## [4.9.0] - 2026-07-29

### Added — Level 5: Production Hardening (6/N) — Docker + Kubernetes deployment

**Deployment** (`deploy/`, new directory)
- `deploy/docker/Dockerfile` — multi-stage (build wheel → slim runtime),
  runs as a non-root user, one image serves both `master` and `agent`
  roles via CLI args. Verified end-to-end in this repo: wheel builds,
  installs cleanly into a fresh venv, `huddle-cluster master start`
  serves real traffic
- `deploy/docker/docker-compose.yml` — local demo: 1 master + 3 agents
  with a healthcheck-gated startup order
- `deploy/k8s/master.yaml` — single-replica Deployment (`Recreate`
  strategy so two masters never write the same PVC concurrently),
  PersistentVolumeClaim for `--state-file`, Secret for the API key,
  readiness/liveness probes against `/v1/health`, resource
  requests/limits
- `deploy/k8s/agent-daemonset.yaml` — one agent per node via
  DaemonSet, node identity from `spec.nodeName`

**CLI fixes found and fixed while building/testing the above**
- `huddle-cluster master start` was missing `--state-file` /
  `--state-save-interval` entirely — the v4.5.0 persistence feature
  was only reachable from the Python API, not the CLI. Added both,
  plus a `Persist:` line in the startup banner
- **Real bug:** the CLI only handled `KeyboardInterrupt` (Ctrl-C /
  SIGINT) for graceful shutdown. `docker stop` and Kubernetes pod
  termination send `SIGTERM`, which was previously an unhandled hard
  kill — meaning `--state-file`'s final snapshot-on-stop (v4.5.0)
  would never actually run in a container. New
  `_handle_termination_signals()` translates SIGTERM into the same
  graceful-stop path as Ctrl-C, for both `master start` and
  `agent start`. Verified with a real `kill -TERM` against a running
  process — snapshot file is written correctly

**Docs**
- New "Deployment" section in `docs/CLUSTER.md` (before Running
  Tests), including an explicit "what this does *not* give you" list
  (no Helm chart, no published image, no HA wiring in the manifests,
  no network policies/ingress, not K8s-scale load-tested)

**Notes**
- No new automated tests for the Dockerfile/K8s manifests themselves
  (no `docker`/`kubectl` available in this sandbox to run them against
  a real daemon) — verified instead by building the wheel, installing
  it into a clean venv, and exercising the actual CLI commands the
  container runs, plus a manual `kill -TERM` for the SIGTERM fix

---

## [4.8.0] - 2026-07-29

### Added — Level 5: Production Hardening (5/N) — OTLP log export

**ClusterObservability**
- New `otlp_endpoint`/`otlp_headers`/`otlp_flush_interval_sec`/
  `otlp_timeout_sec` constructor params. When `otlp_endpoint` is set,
  buffered events are exported to `{endpoint}/v1/logs` as OTLP logs
  (JSON encoding) on a background daemon thread — no
  `opentelemetry-*`/protobuf/grpc dependency needed, just `urllib`
- Best-effort and non-blocking: export failures are logged and retried
  next interval; never raised into request-handling code, never drop
  events (stay in the local ring buffer, subject to normal
  `buffer_size` eviction, until a send succeeds)
- `stop()` now performs a final flush so events recorded just before
  shutdown aren't silently lost
- `summary()` (and `GET /v1/observability/status`) report
  `otlp.exported_count`, `otlp.error_count`, `otlp.last_error` when
  OTLP export is enabled
- Fully backward compatible — omitting `otlp_endpoint` behaves exactly
  as v4.3.0 (local buffer + JSON logs only, no network calls)

**Docs**
- New "Observability" section in `docs/CLUSTER.md` (this had been
  undocumented in CLUSTER.md since v4.3.0 — added the base
  trace-ID/JSON-logging behavior alongside the new OTLP piece)

**Tests** (`tests/test_cluster_observability_otlp.py`, new file, 10 tests)
- Uses a real tiny in-process HTTP server as a fake OTLP collector
  (not a urllib mock) — verifies the actual request shape, headers,
  resource/service.name, no-duplicate-export-after-success, retry
  after a transient collector failure, and stop()-triggers-final-flush
- Full `test_cluster_observability.py` (36) + new file (10) re-run
  clean together, no regressions (46 tests)

---

## [4.7.0] - 2026-07-29

### Added — Level 5: Production Hardening (4/N) — HA failover staleness fix + honest limitations doc

**ClusterHA**
- `_become_leader()` now pushes a full state snapshot to all followers
  immediately upon winning an election, instead of waiting for the next
  `_sync_loop` tick (previously up to `sync_interval_sec` — default
  1s, but configurable much higher). This closes most of the window
  where a second failure right after a failover could hand leadership
  to a follower still holding stale state
- This is **not** a claim of full Raft log-based consistency — see the
  new "Honest limitations" subsection below

**Docs**
- `docs/CLUSTER.md`: new "Honest limitations" subsection under
  High-Availability Master, explicitly documenting what this simplified
  Raft does *not* provide (log-based replication/commit index, cluster
  membership changes, independent chaos-testing) rather than
  overclaiming "hardened"

**Tests**
- `tests/test_cluster_ha_sync_on_election.py` (new): verifies a
  follower receives the leader's pre-election state well within 0.5s
  of a leader emerging, even with `sync_interval_sec=20.0` — would fail
  without the immediate post-election push
- Full `test_cluster_ha.py` + `test_cluster_ha_persistence.py` +
  new file re-run clean (37 tests)

---

## [4.6.0] - 2026-07-29

### Added — Level 5: Production Hardening (3/N) — mTLS node identity

**MasterNode**
- `NodeRecord` gained a `tls_client_cn` field: when a node joins over a
  connection with a verified client certificate (`tls_ca_certs`
  configured on the master), the certificate's Common Name is captured
  and recorded on that node's record — a non-replayable identity signal
  that complements (not replaces) `api_keys`/RBAC for authorization
- New `_tls_client_cn()` handler helper reads the peer certificate off
  the live TLS connection via `ssl.SSLSocket.getpeercert()`; returns
  `None` over plain HTTP or when no client cert was presented
- `_handle_join()` gained a `tls_client_cn` parameter; threaded through
  on both the new-node and re-join paths
- Fully backward compatible — `tls_client_cn` defaults to `None` and
  existing persisted registry snapshots (pre-v4.6.0) load fine since
  it's an optional dataclass field

**Docs**
- New "Node identity via mTLS" subsection under TLS/HTTPS in
  `docs/CLUSTER.md`, with the explicit scope note that this is an audit
  trail today, not (yet) an authorization mechanism

**Tests** (`tests/test_master_tls.py`, +2 tests, 10 total in file)
- mTLS join records the client cert CN on the resulting `NodeRecord`
- Plain-HTTP join leaves `tls_client_cn` as `None`
- Full `test_cluster_master.py` + `test_master_tls.py` +
  `test_master_registry_persistence.py` re-run clean (131 tests)

---

## [4.5.0] - 2026-07-29

### Added — Level 5: Production Hardening (2/N) — state persistence

**ClusterHA** (`huddle_cluster_pkg.cluster_ha`)
- New `state_file` constructor param: persists Raft `term`/`voted_for`
  to disk (atomic write: tmp file + `fsync` + `os.replace`), written
  synchronously on every vote grant, election start, and step-down
- Restores `term`/`voted_for` on construction if `state_file` exists
- Fixes a real (if narrow) Raft safety gap: without this, a restarted
  HA node forgot it had already voted in a term and could vote again,
  a double-vote that Raft's safety proof depends on not happening
- Corrupt/unreadable state files fall back to term 0 with a warning
  rather than crashing

**MasterNode** (`huddle_cluster_pkg.cluster_master`)
- New `state_file` / `state_save_interval_sec` (default 5.0) constructor
  params: periodically snapshots the full node registry to disk (atomic
  write, same pattern), plus once more on graceful `stop()`
- Restores the registry on `start()` if `state_file` exists; restored
  nodes keep their last-known status, and the existing heartbeat-timeout
  logic naturally re-evaluates liveness within one timeout window
- Explicitly *not* persisted (documented, deliberate): circuit breaker
  trip state, rate limiter buckets, canary rollout progress — these are
  short-lived/self-correcting, so restart-loses-it is an acceptable
  tradeoff for now

**Docs**
- New "Persistence" section in `docs/CLUSTER.md` (before TLS/HTTPS)

**Tests**
- `tests/test_cluster_ha_persistence.py` (new, 11 tests): atomic write,
  restart restores term/voted_for, corrupt-file fallback, and the core
  safety test — restart does not allow a double-vote in the same term
- `tests/test_master_registry_persistence.py` (new, 9 tests): snapshot
  on stop, periodic snapshot while running, restart restores nodes
  (including status/metadata), corrupt/missing file handling, atomic
  write, parent-directory creation
- Full existing suite (`test_cluster_master.py`, `test_cluster_ha.py`)
  re-run clean, no regressions (157 tests across all four files)

---

## [4.4.0] - 2026-07-29

### Added — Level 5: Production Hardening (1/N) — TLS/HTTPS + threaded server

**MasterNode**
- New constructor params: `tls_certfile`, `tls_keyfile`, `tls_ca_certs`,
  `tls_require_client_cert`. When `tls_certfile`/`tls_keyfile` are given,
  the master's HTTP listener terminates TLS itself — no reverse proxy
  required just for encryption. `tls_ca_certs` + `tls_require_client_cert`
  enables mTLS: the master verifies (and can require) a client
  certificate on every connection, which is a stronger, non-replayable
  node identity than an API key alone
- `tls_certfile` and `tls_keyfile` must be given together
  (`ValueError` otherwise); `tls_require_client_cert=True` requires
  `tls_ca_certs` (`ValueError` otherwise)
- Swapped `http.server.HTTPServer` for `http.server.ThreadingHTTPServer`
  — the master previously handled one HTTP request at a time; large
  fleets doing frequent heartbeats could bottleneck the control plane.
  All existing state mutations were already guarded by `self._lock`
  (`threading.RLock`), so this is a safe, non-breaking change
- Fully backward compatible — omitting the `tls_*` params behaves
  exactly as before (plain HTTP)

**CLI**
- `huddle-cluster master start` gained `--tls-cert`, `--tls-key`,
  `--tls-ca`, `--tls-require-client-cert`; startup banner now prints
  `https://` URLs and TLS/mTLS status when enabled

**Docs**
- New "TLS / HTTPS" section in `docs/CLUSTER.md` (right after
  Authentication) covering server-cert-only HTTPS and mTLS, with
  Python and CLI examples

**Tests** (`tests/test_master_tls.py`, new file, skipped if `openssl`
CLI unavailable)
- 8 tests: plain HTTPS, cert verification against a CA, plain HTTP
  rejected on a TLS-only port, certfile/keyfile pairing validation,
  require-client-cert-without-ca validation, mTLS rejects a connection
  with no/wrong client cert, mTLS accepts a valid client cert, and a
  regression check that TLS-less masters are unaffected
- Full existing suite (869 tests as of v4.3.0) re-run clean after the
  `ThreadingHTTPServer` swap, no regressions

---

## [4.3.0] - 2026-07-28

### Added — Level 4: Observability & Control Plane — COMPLETE (4/4)

**Observability** (`huddle_cluster_pkg.cluster_observability`)
- New `ClusterObservability` class: structured JSON logging + distributed
  trace IDs for a HuddleCluster master
- `configure_logging()` swaps a logger's handlers for one that emits
  single-line JSON records (`ts`, `level`, `logger`, `service`, `message`,
  plus `trace_id` / `node_id` / `fields` when present); idempotent, and
  wired to the root logger automatically on `attach()` unless
  `json_logs=False`
- Every inbound HTTP request to the master is assigned a trace ID —
  propagated from an `X-Trace-Id` request header when the caller already
  has one, otherwise minted fresh via `new_trace_id()`. The ID is echoed
  back on the response (`X-Trace-Id`) and attached to every log line and
  buffered event recorded while handling that request via a thread-local
  context (`start_trace()` / `current_trace_id()` / `end_trace()`)
- `record_event(event, node_id=None, trace_id=None, level="info", **fields)`
  — in-memory ring buffer (default 500 events) queryable independent of
  any external log aggregator; also emitted through the logging pipeline
  so it shows up in JSON log output too
- `events(limit=, trace_id=, event=, node_id=)` for filtered buffer reads;
  `summary()` for config + counters + recent events
- `MasterNode` gained an `observability=` parameter; when set, every
  `_send_json` / `_send_text` response gets an `X-Trace-Id` header and a
  buffered `http_request` event (method, path, status). Fully backward
  compatible — omitting `observability` behaves exactly as before
- `MasterNode.status()` embeds the full observability summary under
  `"observability"`
- New REST endpoints:
  `GET /v1/observability/status` — config, counters, recent events;
  `GET /v1/observability/logs` — queryable event buffer
  (`?limit=&trace_id=&event=&node_id=`)
- `ClusterObservability` exported from `huddle_cluster_pkg` top-level
- This completes Level 4 (Observability & Control Plane): Circuit
  Breaker, Rate Limiter, Canary Deployment, Observability

**Tests** (`tests/test_cluster_observability.py`, new file)
- 5 trace-context unit tests (mint, propagate, blank incoming, clear)
- 11 event-buffer unit tests (record, filter, limit, cap, validation)
- 4 JSON log formatter unit tests
- 2 `configure_logging()` idempotency/wiring tests
- 14 HTTP integration tests (status/logs endpoints, X-Trace-Id
  propagation and header echo, request-to-event correlation, GET and
  POST tracing)
- 36 total new tests

---

## [4.2.0] - 2026-06-26

### Added — Level 4: Observability & Control Plane (in progress, 3/4)

**Canary Deployment** (`huddle_cluster_pkg.cluster_canary_deployment`)
- New `ClusterCanaryDeployment` class: weight-based traffic splitting that
  gradually shifts requests from stable nodes to canary (new-version) nodes
- Nodes are tagged as canary via join metadata (`canary=true`) or via
  `POST /v1/canary/announce` at runtime; untagged nodes are stable
- Configurable weight steps (default 5 → 25 → 50 → 100 %); each call
  to `advance()` moves to the next level
- Phases: `idle` → `active` (deployment in progress) → `promoted`
  (canary graduated) or `aborted` (traffic returned to stable)
- `set_weight()` for direct weight control outside the step ladder
- `on_promote`, `on_abort`, `on_weight_change` callbacks
- `ClusterScheduler` gained a `canary=` parameter; when active, each
  `pick()` call is routed probabilistically — weight% to canary pool,
  remainder to stable pool — using the scheduler's existing thermal
  fitness scoring within each pool.  Fully backward compatible.
- `ClusterScheduler.scheduler_stats()` reports
  `"canary": "enabled"/"disabled"`
- `MasterNode.status()` embeds the full canary status dict under `"canary"`
- New REST endpoints:
  `POST /v1/canary/start` — begin deployment (optional `{"weight": N}`);
  `GET /v1/canary/status` — phase, weight, pool sizes, history;
  `POST /v1/canary/advance` — step up to next weight level;
  `POST /v1/canary/promote` — graduate canary to stable;
  `POST /v1/canary/abort` — return all traffic to stable immediately;
  `POST /v1/canary/announce` — runtime-tag a node as canary
- `ClusterCanaryDeployment` exported from `huddle_cluster_pkg` top-level

**Tests** (`tests/test_cluster_canary_deployment.py`, new file)
- 21 lifecycle unit tests (start/advance/promote/abort/callbacks/history)
- 7 `pick_pool()` traffic-splitting tests including probabilistic check
- 4 scheduler integration tests
- 10 HTTP integration tests including full ramp workflow
- 42 total new tests

---

## [4.1.0] - 2026-06-25

### Added — Level 4: Observability & Control Plane (in progress, 2/4)

**Cluster Rate Limiter** (`huddle_cluster_pkg.cluster_rate_limiter`)
- New `ClusterRateLimiter` class: per-node token-bucket rate limiting.
  Each node gets its own bucket with configurable capacity (max burst) and
  refill rate (sustained throughput).  When a node's bucket is empty the
  scheduler skips it and picks the next best eligible node instead, so
  burst traffic is spread across the cluster rather than hammering one node.
- Token bucket algorithm: continuous refill at `refill_rate` tokens/second,
  capped at `capacity`; `consume()` deducts 1 token per scheduler pick.
- Buckets are created lazily on first use; nodes that have never been
  picked have a full bucket.
- `is_rate_limited(node_id)` — True when fewer than 1 token remains.
- `reset(node_id)` — operator-driven refill to capacity.
- `on_rate_limited` callback fires when a node's bucket first empties.
- `ClusterScheduler` gained a `rate_limiter=` parameter; when set,
  rate-limited nodes are excluded from `pick()` before scoring.
  Fully backward compatible — omitting `rate_limiter` behaves as before.
- `ClusterScheduler.scheduler_stats()` reports
  `"rate_limiter": "enabled"/"disabled"`.
- Plug-in design: `MasterNode(rate_limiter=ClusterRateLimiter(...))`;
  disabled by default, backward-compatible.
- `MasterNode.status()` reports `"rate_limiter": "enabled"/"disabled"`.
- New REST endpoints:
  `GET /v1/ratelimits` — all bucket states + rate-limited count;
  `GET /v1/ratelimits/{node_id}` — single node bucket;
  `POST /v1/ratelimits/{node_id}/reset` — refill to capacity.
- `ClusterRateLimiter` exported from `huddle_cluster_pkg` top-level.

**Tests** (`tests/test_cluster_rate_limiter.py`, new file)
- 7 `TokenBucket` unit tests (consume, refill, cap, fill, to_dict)
- 9 `ClusterRateLimiter` unit tests (init, consume, callback, reset,
  per-node independence, summary)
- 5 scheduler exclusion tests
- 9 HTTP integration tests
- 35 total new tests

---

## [4.0.0] - 2026-06-25

### Added — Level 4: Observability & Control Plane (in progress, 1/4)

**Cluster Circuit Breaker** (`huddle_cluster_pkg.cluster_circuit_breaker`)
- New `ClusterCircuitBreaker` class: tracks per-node error rates forwarded
  via heartbeat metrics and automatically trips when a node exceeds
  `trip_threshold`; tripped nodes are excluded from the scheduler's
  eligible pool so traffic reroutes before clients experience failures
- Three-state model: `closed` (healthy), `open` (tripped, excluded),
  `half-open` (probe window after `reset_timeout_sec`)
- Only acts on nodes that actually forward `error_rate` in heartbeat
  metrics — nodes without this metric are always treated as healthy
  (circuit breaker only acts on evidence)
- Auto-reset: when `error_rate` recovers below `trip_threshold` the
  breaker closes automatically without manual intervention
- Manual reset: `POST /v1/breakers/{node_id}/reset` and
  `ClusterCircuitBreaker.reset()` for operator-driven recovery
- `on_trip(node_id, error_rate)` / `on_reset(node_id)` callbacks
- `ClusterScheduler` gained a `circuit_breaker=` parameter; when set,
  `pick()` excludes all nodes whose breaker is open before scoring —
  fully backward compatible when `circuit_breaker` is omitted
- `ClusterScheduler.scheduler_stats()` now reports
  `"circuit_breaker": "enabled"/"disabled"`
- Plug-in design: `MasterNode(circuit_breaker=ClusterCircuitBreaker(...))`;
  disabled by default, backward-compatible
- `MasterNode.status()` reports `"circuit_breaker": "enabled"/"disabled"`
- New REST endpoints:
  `GET /v1/breakers` — all breaker states + open count;
  `GET /v1/breakers/{node_id}` — single node state;
  `POST /v1/breakers/{node_id}/reset` — manual reset
- `ClusterCircuitBreaker` exported from `huddle_cluster_pkg` top-level
- This is a major version bump (3.x → 4.0.0) marking the start of
  Level 4 (Observability & Control Plane)

**Tests** (`tests/test_cluster_circuit_breaker.py`, new file)
- 5 constructor / validation tests
- 7 evaluation tests (trip, no-trip, no-metric, callbacks, auto-reset,
  half-open, trip count)
- 4 manual reset tests
- 4 scheduler exclusion tests
- 9 HTTP integration tests
- 29 total new tests

**Level 4 roadmap (planned)**
- Rate limiter — per-node token bucket rate limiting
- Canary deployment — weight-based traffic splitting via scheduler
- Observability — structured JSON logging, distributed trace IDs

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