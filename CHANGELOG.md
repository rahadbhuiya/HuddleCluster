# Changelog

All notable changes to HuddleCluster are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

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