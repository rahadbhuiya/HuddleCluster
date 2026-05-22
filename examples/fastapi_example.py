"""
fastapi_example.py — FastAPI integration for HuddleCluster
===========================================================
Demonstrates how to wire HuddleCluster into a FastAPI application as a
transparent load-balancing middleware layer.

Architecture
------------
                    ┌─────────────────────────────┐
  Client Request ──►│  FastAPI (this file)        │
                    │  POST /request              │
                    │   └─► HuddleCluster         │
                    │        └─► inner server     │──► upstream proxy
                    │  GET  /health               │
                    │  POST /admin/evict/{id}     │
                    └─────────────────────────────┘

Running
-------
    pip install fastapi uvicorn httpx
    uvicorn fastapi_example:app --reload --port 8000

Endpoints
---------
  GET  /health               → cluster health report
  GET  /servers              → all registered servers
  POST /request              → route a request to the next inner-ring server
  POST /admin/evict/{id}     → manually evict a server to the outer ring
  POST /admin/add            → add a new server at runtime
  DELETE /admin/remove/{id}  → remove a server from the cluster
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import time
from contextlib import asynccontextmanager
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from huddle_cluster import HuddleCluster, Server, ServerMetrics, create_cluster

log = logging.getLogger("huddle.fastapi")



# Cluster Setup


# Seed servers — replace with your real backend addresses
SEED_SERVERS = [
    ("backend-1", "10.0.1.1", 9000),
    ("backend-2", "10.0.1.2", 9000),
    ("backend-3", "10.0.1.3", 9000),
    ("backend-4", "10.0.1.4", 9000),
    ("backend-5", "10.0.1.5", 9000),
]


def _build_metrics_updater():
    """
    Real-world metrics updater.

    Replace the simulated values below with actual calls to your
    metrics provider (Prometheus, Datadog, CloudWatch, etc.).

    Example using Prometheus:
        response = requests.get(f"http://{server.host}:{server.port}/metrics")
        parsed = parse_prometheus(response.text)
        server.metrics.cpu_usage = parsed["process_cpu_seconds_total"]
    """
    def updater(server: Server) -> None:
        #  SIMULATED ── replace with real metrics collection 
        server.metrics.cpu_usage    = random.uniform(0.0, 0.95)
        server.metrics.memory_usage = random.uniform(0.1, 0.80)
        server.metrics.active_connections = random.randint(0, 500)
        server.metrics.avg_response_ms    = random.uniform(10, 800)
        server.metrics.error_rate         = random.uniform(0.0, 0.05)
        server.metrics.is_healthy         = random.random() > 0.05   # 95 % healthy
        
    return updater


def _build_rotation_callback():
    def on_rotation(event):
        log.info(
            "Rotation event | server=%s  direction=%s  reason=%s  temp=%.3f",
            event.server_id, event.direction, event.reason, event.temperature,
        )
        # Hook into your alerting / PagerDuty / Slack webhook here
    return on_rotation


# Cluster singleton — created at startup
cluster: HuddleCluster = create_cluster(
    server_addresses=SEED_SERVERS,
    heat_threshold=0.75,
    cool_threshold=0.30,
    min_inner_size=2,
    max_inner_size=4,
    rotation_cooldown_sec=5.0,
    min_outer_dwell_sec=10.0,
    metrics_updater=_build_metrics_updater(),
    on_rotation=_build_rotation_callback(),
)



# App Lifecycle


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start the rotation daemon on app startup; stop on shutdown."""
    cluster.start(rotation_interval_sec=1.0)
    log.info("HuddleCluster rotation daemon started.")
    yield
    cluster.stop(timeout=5.0)
    log.info("HuddleCluster rotation daemon stopped.")


app = FastAPI(
    title="HuddleCluster Load Balancer",
    description="Penguin-inspired self-organizing load balancer via FastAPI",
    version="1.0.0",
    lifespan=lifespan,
)



# Middleware — optional per-request latency logging


@app.middleware("http")
async def log_latency(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    elapsed_ms = (time.perf_counter() - start) * 1000
    log.debug("%-6s %-40s  %.1f ms  %d",
              request.method, request.url.path, elapsed_ms, response.status_code)
    return response



# Pydantic schemas


class AddServerRequest(BaseModel):
    id:   str
    host: str
    port: int
    force_inner: bool = False


class RequestBody(BaseModel):
    payload: dict = {}



# Routes


@app.get("/health", summary="Cluster health report")
async def health():
    """
    Returns the full cluster health snapshot including inner/outer ring
    composition, temperatures, fairness score, and recent rotation events.
    """
    return cluster.health_report()


@app.get("/servers", summary="List all registered servers")
async def list_servers():
    all_servers = cluster.all_servers()
    return {
        "total": len(all_servers),
        "servers": [
            {
                "id":       s.id,
                "host":     s.host,
                "port":     s.port,
                "position": s.position.value,
                "temp":     round(s.temperature, 4),
                "healthy":  s.metrics.is_healthy,
            }
            for s in all_servers
        ],
    }


@app.post("/request", summary="Route request to the next inner-ring server")
async def proxy_request(body: RequestBody):
    """
    Picks the next available inner-ring server via round-robin and
    forwards the request to it.  In production, swap the simulated
    httpx call for your real upstream proxy logic.
    """
    server = cluster.get_server()
    if server is None:
        raise HTTPException(status_code=503, detail="No servers available")

    upstream_url = f"http://{server.host}:{server.port}/handle"

    try:
        #  In production: actually proxy here 
        # async with httpx.AsyncClient(timeout=5.0) as client:
        #     resp = await client.post(upstream_url, json=body.payload)
        #     return resp.json()
        #  SIMULATED response for demo 
        await asyncio.sleep(random.uniform(0.01, 0.05))   # simulate latency
        return {
            "routed_to":   server.id,
            "upstream_url": upstream_url,
            "status":      "ok",
            "server_temp": round(server.temperature, 4),
        }
        

    except httpx.RequestError as exc:
        log.warning("Upstream %s unreachable: %s", server.id, exc)
        server.metrics.is_healthy = False   # mark for health-fail eviction
        raise HTTPException(status_code=502, detail=f"Upstream {server.id} unreachable")


@app.post("/admin/evict/{server_id}", summary="Manually evict a server")
async def admin_evict(server_id: str):
    """
    Force-evict a server from the inner ring to the outer ring.
    Useful for draining a server before maintenance.
    """
    success = cluster.force_evict(server_id)
    if not success:
        raise HTTPException(
            status_code=404,
            detail=f"Server {server_id!r} not found in inner ring",
        )
    return {"evicted": server_id, "status": "moved to outer ring"}


@app.post("/admin/add", summary="Add a new server at runtime")
async def admin_add(req: AddServerRequest):
    """Dynamically register a new backend server into the cluster."""
    server = Server(id=req.id, host=req.host, port=req.port)
    cluster.add_server(server, force_inner=req.force_inner)
    return {
        "added":    req.id,
        "position": server.position.value,
    }


@app.delete("/admin/remove/{server_id}", summary="Remove a server")
async def admin_remove(server_id: str):
    """Gracefully remove a server from the cluster (inner or outer)."""
    success = cluster.remove_server(server_id)
    if not success:
        raise HTTPException(status_code=404, detail=f"Server {server_id!r} not found")
    return {"removed": server_id}



# Dev entry-point


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("fastapi_example:app", host="0.0.0.0", port=8000, reload=True)