"""
upstream_server.py — Simulated upstream HTTP server
=====================================================
Run multiple instances on different ports to simulate
a real server pool for HuddleCluster HTTP benchmarking.

Usage:
  python upstream_server.py --port 8001 --latency 15 --id s0
  python upstream_server.py --port 8002 --latency 17 --id s1
  ... etc
"""

import argparse
import asyncio
import random
import time

try:
    from fastapi import FastAPI
    import uvicorn
except ImportError:
    print("Installing fastapi and uvicorn...")
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install",
                           "fastapi", "uvicorn", "--break-system-packages", "-q"])
    from fastapi import FastAPI
    import uvicorn

parser = argparse.ArgumentParser()
parser.add_argument("--port",    type=int,   default=8001)
parser.add_argument("--latency", type=float, default=15.0,  help="Base latency in ms")
parser.add_argument("--id",      type=str,   default="s0")
args, _ = parser.parse_known_args()

app = FastAPI()

# Shared state
state = {
    "requests":    0,
    "is_slow":     False,   # SET via /admin/slow
    "is_dead":     False,   # SET via /admin/kill
    "base_latency": args.latency,
}


@app.get("/health")
async def health():
    return {"status": "ok", "server_id": args.id, "requests": state["requests"]}


@app.get("/api/work")
async def work():
    if state["is_dead"]:
        await asyncio.sleep(5.0)
        return {"error": "server dead"}

    state["requests"] += 1
    base = state["base_latency"] * 5.0 if state["is_slow"] else state["base_latency"]
    jitter = random.gauss(0, 2)
    latency_ms = max(1.0, base + jitter)
    await asyncio.sleep(latency_ms / 1000.0)
    return {
        "server_id": args.id,
        "latency_ms": round(latency_ms, 2),
        "requests":  state["requests"],
    }


@app.post("/admin/slow")
async def make_slow():
    state["is_slow"] = True
    return {"server_id": args.id, "is_slow": True}


@app.post("/admin/normal")
async def make_normal():
    state["is_slow"] = False
    return {"server_id": args.id, "is_slow": False}


@app.post("/admin/kill")
async def kill():
    state["is_dead"] = True
    return {"server_id": args.id, "is_dead": True}


@app.post("/admin/revive")
async def revive():
    state["is_dead"] = False
    return {"server_id": args.id, "is_dead": False}


if __name__ == "__main__":
    print(f"Starting server {args.id} on port {args.port} "
          f"(base_latency={args.latency}ms)")
    uvicorn.run(app, host="127.0.0.1", port=args.port,
                log_level="warning", access_log=False)
