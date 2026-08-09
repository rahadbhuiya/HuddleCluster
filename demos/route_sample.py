"""
Calls the REAL scheduler via GET /v1/scheduler/next, N times, and
counts how many picks landed on canary vs stable nodes — this is the
actual server-side scheduler.pick() + canary weighting logic running,
not a local simulation.

Prints progress as it goes (every 10 requests) so it's visibly working
rather than looking stuck if any individual request is slow.

Usage: python route_sample.py   (run while run_canary_demo.py is up)
"""
import json
import sys
import urllib.error
import urllib.request

BASE = "http://localhost:7070/v1"
N = 60          # fewer than before — enough to see the split, finishes fast
TIMEOUT = 2.0   # per-request timeout


def get(path, timeout=3.0):
    with urllib.request.urlopen(f"{BASE}{path}", timeout=timeout) as r:
        return json.loads(r.read())


print("Fetching current canary status and node list...")
sys.stdout.flush()
try:
    status = get("/canary/status")
    nodes = {n["node_id"]: n for n in get("/nodes")["nodes"]}
except urllib.error.URLError as e:
    print(f"ERROR: couldn't reach the master at {BASE} — is run_canary_demo.py "
          f"still running? ({e})")
    sys.exit(1)

print(f"Canary phase: {status.get('phase')}, weight: {status.get('weight_pct', 0):.0f}%")
print(f"Alive canary pool size:  {status.get('canary_nodes')}")
print(f"Alive stable pool size:  {status.get('stable_nodes')}\n")
sys.stdout.flush()

print(f"Calling GET /v1/scheduler/next {N} times (real routing, not simulated)...")
sys.stdout.flush()

picks = {}
errors = 0
for i in range(N):
    try:
        result = get("/scheduler/next", timeout=TIMEOUT)
        node_id = result["node"]["node_id"]
        picks[node_id] = picks.get(node_id, 0) + 1
    except Exception as e:
        errors += 1
        if errors <= 3:   # don't spam if many fail the same way
            print(f"  [request {i+1}] failed: {e}")
    if (i + 1) % 10 == 0:
        print(f"  ...{i+1}/{N} done")
        sys.stdout.flush()

canary_hits = sum(
    c for nid, c in picks.items()
    if nodes.get(nid, {}).get("metadata", {}).get("canary") == "true"
)
stable_hits = sum(picks.values()) - canary_hits
total = canary_hits + stable_hits

print(f"\nOut of {total} successful scheduler.pick() calls "
      f"({errors} failed out of {N} attempted):")
if total > 0:
    print(f"  Canary: {canary_hits} ({canary_hits/total*100:.1f}%)")
    print(f"  Stable: {stable_hits} ({stable_hits/total*100:.1f}%)")
else:
    print("  No successful calls — check that run_canary_demo.py is still "
          "running and reachable at http://localhost:7070")

print("\nPer-node breakdown:")
for node_id, count in sorted(picks.items()):
    tag = "canary" if nodes.get(node_id, {}).get("metadata", {}).get("canary") == "true" else "stable"
    print(f"  {node_id:<12} ({tag:<7}) {count:>4} picks")