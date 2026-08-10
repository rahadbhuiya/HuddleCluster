# HuddleCluster — Interactive Demos

> **Note (v4.13.0+):** if you just want to turn a feature on for a real
> master, you no longer need any of these scripts — see
> `--features` in `docs/CLUSTER.md`'s CLI reference
> (`huddle-cluster master start --features features.json`). These demo
> scripts remain useful for interactive exploration (typing `fail
> web-2 0.9` at a prompt, watching events fire) and for custom callback
> behavior, which isn't expressible from the CLI's JSON config.

Hands-on scripts for exploring individual HuddleCluster features
locally, one master process at a time. These were written for manual
exploration/testing (v4.4.0–v4.12.0 development), not as reference
examples of the public API — see the important caveat below before
copying patterns from these into real code.

## Important: these use private (internal) methods

Several scripts call `master._handle_join()`, `master._handle_heartbeat()`,
etc. directly — methods prefixed with `_` are internal implementation
details, not the public API. They're used here purely as a shortcut to
register/heartbeat fake nodes without needing to spin up real
`AgentNode` processes for a quick local demo.

**Do not use `_handle_join()`/`_handle_heartbeat()` in real code.** For
an actual deployment, use `AgentNode` (see `docs/CLUSTER.md`) or the
REST API (`POST /v1/nodes/join`, `POST /v1/nodes/<id>/heartbeat`) —
both are stable, public, and won't break across versions the way
calling a private method might.

## What's here

| Script | Feature | Run |
|---|---|---|
| `gen_cert.py` | Generates a throwaway self-signed TLS cert (`server.crt`/`server.key`), pure Python, no `openssl` CLI needed | `python demos/gen_cert.py` |
| `run_master1/2/3.py` | HA — 3-master election + failover demo | one per terminal: `python demos/run_master1.py` etc. |
| `run_canary_demo.py` + `route_sample.py` | Canary deployment — weight-based traffic split via the real scheduler | demo in one terminal, `route_sample.py` in another |
| `fake_otlp_collector.py` + `run_observability_demo.py` | Observability — JSON logs + trace IDs + OTLP export to a fake collector | collector first, then the demo master |
| `run_circuit_breaker_demo.py` | Circuit breaker — trip/half-open/reset lifecycle, interactive | `python demos/run_circuit_breaker_demo.py` |
| `run_rate_limiter_demo.py` + `burst_sample.py` | Rate limiter — token bucket exhaustion + refill, concurrent burst | demo in one terminal, burst script in another |
| `run_autoscaler_demo.py` | Auto scaler — node-count-based scale up/down, interactive | `python demos/run_autoscaler_demo.py` |
| `run_service_discovery_demo.py` + `dns_query.py` | Service discovery — REST + DNS responder | demo in one terminal, `dns_query.py` in another |

All of these listen on `127.0.0.1:7070` by default — run one at a time
(stop the previous one, Ctrl-C, before starting the next).

## Why `dns_query.py` exists instead of using `nslookup`

Windows' built-in `nslookup.exe` doesn't support querying a
non-standard port (the `-port` flag is a Linux/BIND-nslookup feature).
Since the demo's DNS responder deliberately runs on `8053` (not the
privileged port `53`, so it doesn't need admin rights), `nslookup` on
Windows can't reach it. `dns_query.py` sends the same kind of A-record
query directly over a UDP socket instead.