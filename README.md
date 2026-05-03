# 🐧 HuddleCluster

> A **Penguin-inspired**, self-organizing server load balancer — a novel DSA for fair, rotation-based request distribution.

---

## 🧠 The Idea

Emperor penguins survive Antarctic winters by forming a **huddle**. The coldest penguins on the outside rotate inward to warm up, while warm ones on the inside naturally move outward. No leader. No coordinator. Just threshold-based self-organization — and every penguin gets equal warmth over time.

**HuddleCluster** maps this directly to servers:

| Penguin World | Server World |
|---|---|
| Warm center penguin | Active server (handling requests) |
| Cold outer penguin | Resting server (recovering) |
| Overheated → moves out | High-load server self-evicts |
| Coldest outer moves in | Recovered server enters active ring |
| Every penguin gets rest | Every server gets fair rest time |

---

## 🏗 Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        HuddleCluster                             │
│                                                                  │
│   ┌─ Inner Ring (Active) ──────────────────────────────────────┐ │
│   │   [S1] ──► [S2] ──► [S3] ──► (circular round-robin)        │ │
│   │    ▲                              │                        │ │
│   │  cooled,                     overheated,                   │ │
│   │  enters                       evicts                       │ │
│   └──────────────────────────────────│─────────────────────────┘ │
│                                      ▼                           │
│   ┌─ Outer Ring (Resting) — min-heap by temperature ───────────┐ │
│   │   [S4 temp=0.10]   [S5 temp=0.22]   [S6 temp=0.28]         │ │
│   │              (cooling down, waiting to re-enter)           │ │
│   └────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
```

**Two rings. Two rules. No coordinator needed.**

| Ring | Role | Data Structure |
|------|------|----------------|
| Inner | Active — handles requests | `deque` (O(1) round-robin) |
| Outer | Resting — cooling down | `min-heap` by temperature |

---

##  Key Features

-  **Self-rotating** — servers evict and re-enter automatically based on load
-  **Fairness tracking** — Gini-based fairness score ensures no server is always active
-  **EMA temperature** — Exponential Moving Average prevents spike-based false evictions
-  **Thundering herd protection** — max evictions per cycle capped at ⅓ of inner ring
-  **Flapping prevention** — hysteresis gap + minimum outer dwell time
-  **Health-check eviction** — unhealthy servers immediately moved to outer ring
-  **Built-in observability** — `health_report()` ready to expose as `/huddle/health`
-  **Thread-safe** — RLock with fine-grained critical sections
-  **Pluggable metrics** — bring your own `metrics_updater` callback

---

## 📁 Project Structure

```
huddle/
├── huddle_cluster.py          ← Core DSA implementation (Python)
├── HuddleSimulation.jsx       ← Interactive React visualisation
├── requirements.txt           ← Python dependencies
├── README.md                  ← This file
├── tests/
│   ├── conftest.py            ← Import from here
│   ├── test_rotation.py       ← Unit tests: eviction, pull, round-robin
│   ├── test_fairness.py       ← Fairness score & time-accounting tests
│   └── test_stress.py         ← Load & chaos tests (threads, deadlock)
└── examples/
    ├── fastapi_example.py     ← FastAPI integration with /health endpoint
    └── simulation.py          ← Terminal visual simulation (rich)
```

---

## 🚀 Quick Start

### Python Library

```bash
pip install -r requirements.txt
```

```python
from huddle_cluster import create_cluster, Server, HuddleCluster

# 1. Create cluster from server list
cluster = create_cluster([
    ("s1", "10.0.0.1", 8080),
    ("s2", "10.0.0.2", 8080),
    ("s3", "10.0.0.3", 8080),
    ("s4", "10.0.0.4", 8080),
    ("s5", "10.0.0.5", 8080),
])

# 2. Start background rotation daemon
cluster.start(rotation_interval_sec=1.0)

# 3. Route requests
server = cluster.get_server()
if server:
    response = requests.get(f"http://{server.host}:{server.port}/api/data")

# 4. Check health
print(cluster.health_report())

# 5. Stop cleanly
cluster.stop()
```

### Run Tests

```bash
cd huddle/

# All tests
pytest tests/ -v

# Individual suites
pytest tests/test_rotation.py -v   # rotation logic
pytest tests/test_fairness.py -v   # fairness & health report
pytest tests/test_stress.py   -v   # concurrency & chaos
```

### Terminal Simulation

```bash
python examples/simulation.py                    # 8 servers, infinite
python examples/simulation.py --servers 12       # 12 servers
python examples/simulation.py --cycles 30        # stop after 30 cycles
python examples/simulation.py --interval 0.5     # faster rotation
```

### FastAPI Server

```bash
uvicorn examples.fastapi_example:app --reload --port 8000
```

| Endpoint | Description |
|----------|-------------|
| `GET  /health` | Full cluster health report |
| `GET  /servers` | All registered servers |
| `POST /request` | Route a request (round-robin) |
| `POST /admin/evict/{id}` | Manually evict a server |
| `POST /admin/add` | Add server at runtime |
| `DELETE /admin/remove/{id}` | Remove a server |

### React Visualisation (`HuddleSimulation.jsx`)

Drop the file into any React project (Vite, CRA, Next.js):

```jsx
import HuddleSimulation from "./HuddleSimulation";

export default function App() {
  return <HuddleSimulation />;
}
```

**UI Controls:**

| Control | Action |
|---------|--------|
| ▶ START / ⏸ PAUSE | Toggle simulation |
| 0.5× / 1× / 2× / 3× | Adjust speed |
| 📨 REQUEST | Send a request (round-robin) |
| Click server card | Toggle stress mode |

---

## ⚙️ Configuration

```python
cluster = HuddleCluster(
    heat_threshold        = 0.75,  # Server evicts when temp >= this
    cool_threshold        = 0.30,  # Server re-enters when temp <= this
    #                               ↑ Gap between these two PREVENTS flapping
    min_inner_size        = 2,     # Never let inner ring go below this
    max_inner_size        = 5,     # Inner ring won't exceed this
    rotation_cooldown_sec = 5.0,   # Min seconds between evictions per server
    min_outer_dwell_sec   = 10.0,  # Min seconds a server must rest in outer
    metrics_updater       = my_metrics_fn,  # Called every cycle per server
    on_rotation           = my_event_fn,    # Called on every ring change
)
```

### Tuning Guide

| Scenario | Adjustment |
|---|---|
| Servers flap in/out rapidly | Increase `min_outer_dwell_sec`, widen threshold gap |
| Under heavy load, inner ring empties | Increase `max_inner_size`, add more servers |
| Servers evict too aggressively | Lower EMA `_ALPHA` in `Server` class, raise `heat_threshold` |
| Fairness score > 0.3 | Check if some servers are permanently too hot; scale horizontally |

---

## 📐 Temperature Formula

Temperature is a composite score (0.0–1.0) calculated via **Exponential Moving Average**:

```
raw_score = (CPU × 0.35) + (Memory × 0.25) + (Connections/1000 × 0.20)
          + (AvgResponseMs/5000 × 0.15) + (ErrorRate × 0.05)

temperature = 0.25 × raw_score + 0.75 × previous_temperature
              ↑ EMA alpha                ↑ Smoothing prevents spike evictions
```

Override weights in `Server._W_CPU`, `_W_MEM`, etc. to match your workload.

---

## 🩺 Health Endpoint

Expose `health_report()` as a REST endpoint:

```python
@app.get("/huddle/health")
def health():
    return cluster.health_report()
```

Response shape:
```json
{
  "status": "healthy",
  "inner_count": 3,
  "outer_count": 2,
  "avg_inner_temp": 0.42,
  "fairness_score": 0.08,
  "total_rotations": 47,
  "inner_ring": [
    { "id": "s1", "temp": 0.38, "rotations": 12, "inner_time_sec": 340.2 }
  ],
  "recent_rotations": [
    { "server_id": "s3", "direction": "inner→outer", "reason": "overheated" }
  ]
}
```

**Alert rules:**
- `status == "degraded"` → inner ring below minimum
- `fairness_score > 0.3` → one server is doing too much work
- `avg_inner_temp > 0.85` → cluster is under-provisioned

---

## 🔴 Known Issues Fixed

### 1. Thundering Herd ✅
**Problem:** Multiple servers overheat simultaneously → all evict → inner ring empties → crash.

**Fix:** Max evictions per cycle = `max(1, inner_size // 3)`. Always preserves `min_inner_size`.

```python
max_evict = max(1, len(self._inner_ring) // 3)
safe_evict = len(self._inner_ring) - self.min_inner_size
to_evict = candidates[: min(max_evict, max(0, safe_evict))]
```

---

### 2. Oscillation / Flapping ✅
**Problem:** Server heats → evicts → cools → re-enters → heats again. Infinite loop.

**Fix (3 layers):**
- **EMA smoothing** — temperature can't spike in one cycle
- **Hysteresis gap** — `cool_threshold` (0.30) << `heat_threshold` (0.75)
- **Minimum outer dwell** — server must rest in outer for `min_outer_dwell_sec` before re-entry

---

### 3. Lock Contention ✅
**Problem:** Single `threading.Lock` blocks all reads during rotation, causing latency spikes.

**Fix:** `threading.RLock` (reentrant) allows internal helper methods to acquire without deadlock. For extreme throughput (>100k RPS), consider sharding into multiple independent `HuddleCluster` instances.

---

### 4. Metrics Staleness ✅
**Problem:** Stale metrics from 5–10 seconds ago cause incorrect eviction decisions.

**Fix:** EMA inherently weights recent readings more heavily. Old stale readings decay exponentially.

---

### 5. Memory Leak ✅
**Problem:** Rotation log grows unboundedly over days of operation.

**Fix:** `deque(maxlen=1000)` — oldest events are automatically dropped. For persistence, hook `on_rotation` callback to push to Redis/InfluxDB.

---

### 6. Empty Cluster ✅
**Problem:** All servers fail health checks → `get_server()` returns `None` → NullPointerException.

**Fix:** `_emergency_server()` scans all servers (inner + outer) and returns the least-hot one. Logs a critical warning.

---

### 7. Operator Precedence Bug in `rotate()` ✅
**Problem:** Candidate eviction logic used ambiguous `and not … or (…)` which Python parsed incorrectly — healthy servers were never evicted even when overheated and cooldown had elapsed.

**Fix:** Explicit parentheses enforce correct logic:

```python
# Before (bug):
if s.is_overheated(...) and not s.metrics.is_healthy
or (s.is_overheated(...) and elapsed >= cooldown)

# After (fixed):
if s.is_overheated(...) and (
    not s.metrics.is_healthy
    or (now - s.last_rotated) >= self.rotation_cooldown_sec
)
```

---

### 8. React `setState` Side-Effects (`HuddleSimulation.jsx`) ✅
**Problem:** `setTotalRotations`, `addLog`, and `setLastServedId` were called inside the `setServers` updater callback — a React anti-pattern that causes double-invocation in Strict Mode and unpredictable behaviour.

**Fix:** All side-effects are deferred via `setTimeout(0)` so they fire after React finishes the current render batch:

```js
// Before (bug): setState inside setState
setServers(prev => {
  setTotalRotations(r => r + rotDelta);  // ❌ nested setState
  addLog(...);                            // ❌ side effect in updater
  return next;
});

// After (fixed): side-effects deferred
setServers(prev => {
  setTimeout(() => {
    setTotalRotations(r => r + rotDelta);
    addLog(...);
  }, 0);
  return next;
});
```

---

### 9. Stale Ref Mutation in `sendRequest` (`HuddleSimulation.jsx`) ✅
**Problem:** `innerCursorRef.current++` was mutated inside the `setServers` pure updater, risking stale closure reads in concurrent renders. `setLastServedId` was also nested inside `setServers`.

**Fix:** Ref mutation and all side-effects (`setRequestCount`, `setLastServedId`, `addLog`) moved outside the updater. A `lastServedTimer` ref prevents stacked `setTimeout` calls on rapid clicks:

```js
// Before (bug):
setServers(prev => {
  innerCursorRef.current++;     // ❌ ref mutation in pure updater
  setLastServedId(target.id);   // ❌ nested setState
  return prev;
});

// After (fixed):
setServers(prev => {
  const idx = innerCursorRef.current % inner.length;
  innerCursorRef.current += 1;  // ✅ mutated outside updater
  setTimeout(() => {
    setRequestCount(r => r + 1);
    setLastServedId(target.id);
    clearTimeout(lastServedTimer.current);
    lastServedTimer.current = setTimeout(() => setLastServedId(null), 600);
  }, 0);
  return prev;
});
```

---

## 📊 Comparison with Existing Approaches

| Feature | Round Robin | Least Connections | Consistent Hash | **HuddleCluster** |
|---|:---:|:---:|:---:|:---:|
| Self-regulating | ❌ | Partial | ❌ | ✅ |
| Server rest periods | ❌ | ❌ | ❌ | ✅ |
| Fairness guarantee | ❌ | ❌ | ❌ | ✅ |
| Thermal memory (EMA) | ❌ | ❌ | ❌ | ✅ |
| No central coordinator | ✅ | ✅ | ✅ | ✅ |
| Health-aware eviction | ❌ | Partial | ❌ | ✅ |
| Adaptive thresholds | ❌ | ❌ | ❌ | 🔜 |
| Predictive rotation | ❌ | ❌ | ❌ | 🔜 |

---

## 🔬 Testing

```bash
pip install pytest
pytest tests/ -v
```

### Recommended Test Scenarios

```python
# 1. Thundering herd — all servers hot simultaneously
def test_thundering_herd():
    for s in cluster.inner_servers():
        s.metrics.cpu_usage = 0.99
        s.update_temperature()
    cluster.rotate()
    assert len(cluster.inner_servers()) >= cluster.min_inner_size

# 2. Flapping — server should not oscillate rapidly
def test_no_flapping():
    s = cluster.inner_servers()[0]
    s.metrics.cpu_usage = 0.99
    s.update_temperature()
    cluster.rotate()  # evicted
    s.metrics.cpu_usage = 0.0
    s.update_temperature()
    cluster.rotate()  # should NOT re-enter yet (dwell time not met)
    assert s not in cluster.inner_servers()

# 3. Fairness — after N cycles, all servers have similar inner time
def test_fairness_over_time():
    time.sleep(60)
    assert cluster.fairness_score() < 0.3
```

---

## 🗺️ Roadmap — New Features to Build

### Phase 1 — Core Enhancements
- [ ] **Adaptive Thresholds** — auto-tune `heat_threshold` based on P95 latency of the cluster
- [ ] **Weighted Temperature** — let servers declare capacity (`weight=2.0` for bigger instances)
- [ ] **Zone Awareness** — prefer inner-ring rotation within the same availability zone

### Phase 2 — Intelligence Layer
- [ ] **Predictive Rotation** — time-series trend on temperature; evict before overheating
- [ ] **Burst Shielding** — temporary immunity from eviction during known traffic bursts (e.g., deploys)
- [ ] **Cold Start Protection** — new servers get a warm-up period in outer ring before handling real traffic

### Phase 3 — Distributed Mode
- [ ] **Distributed Huddle** — gossip protocol so multiple HuddleCluster nodes share state
- [ ] **Raft-backed Leader** — elect a coordinator only for conflict resolution, not routing
- [ ] **Cross-cluster Federation** — multiple clusters federate as a meta-huddle

### Phase 4 — Observability
- [ ] **Prometheus Exporter** — `/metrics` endpoint with Gauge for inner/outer counts, temperature histograms
- [ ] **OpenTelemetry Tracing** — trace which server handled which request
- [ ] **Grafana Dashboard Template** — pre-built dashboard for cluster health

---

## 📜 License

MIT License — use freely, contribute back.

---

## 💡 Inspiration

> *"No individual penguin survives alone. The huddle is the algorithm."*
> — Emperor Penguin (probably)

This DSA is inspired by the thermoregulation behavior of **Aptenodytes forsteri** (Emperor Penguin) as documented in:
- Zitterbart et al. (2011) — *Coordinated movements prevent jamming in an emperor penguin huddle*
- Waters et al. (2012) — *Optimal huddle dynamics in Antarctic penguins*

---

**Author:** Rahad Bhuiya · Version `1.0.1` · MIT License