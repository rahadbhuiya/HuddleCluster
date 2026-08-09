"""
Fires a burst of scheduler.pick() calls at the rate limiter demo master
CONCURRENTLY (25 threads at once) — more than the total token capacity
(15) — to show some getting rejected, then waits and shows the buckets
refilling.

Concurrent, not sequential: with sequential calls, if each individual
HTTP round-trip takes long enough (e.g. due to local network/AV
overhead on some machines), the token bucket can refill between calls
faster than you're consuming it, and you'd never see a rejection even
though the limiter is working correctly — that's a timing artifact of
the test, not a bug in the limiter. Firing all requests at once removes
that dependency on per-request latency.

Usage: python burst_sample.py   (run while run_rate_limiter_demo.py is up)
"""
import json
import threading
import time
import urllib.error
import urllib.request

BASE = "http://localhost:7070/v1"
N = 25


def get(path, timeout=3.0):
    with urllib.request.urlopen(f"{BASE}{path}", timeout=timeout) as r:
        return json.loads(r.read())


results = [None] * N
lock = threading.Lock()


def fire(i):
    try:
        result = get("/scheduler/next")
        with lock:
            results[i] = ("ok", result["node"]["node_id"])
    except urllib.error.HTTPError as e:
        with lock:
            results[i] = ("rejected", e.code)
    except Exception as e:
        with lock:
            results[i] = ("error", str(e))


print(f"Firing {N} CONCURRENT scheduler.pick() calls (15 tokens available total)...\n")
threads = [threading.Thread(target=fire, args=(i,)) for i in range(N)]
start = time.time()
for t in threads: t.start()
for t in threads: t.join()
elapsed = time.time() - start

ok = sum(1 for r in results if r and r[0] == "ok")
rejected = sum(1 for r in results if r and r[0] == "rejected")
errors = sum(1 for r in results if r and r[0] == "error")

for i, r in enumerate(results):
    if r is None:
        print(f"  [{i+1:2}] -> (no result?)")
    elif r[0] == "ok":
        print(f"  [{i+1:2}] -> {r[1]}")
    elif r[0] == "rejected":
        print(f"  [{i+1:2}] -> REJECTED ({r[1]}: no eligible node — all rate-limited)")
    else:
        print(f"  [{i+1:2}] -> error: {r[1]}")

print(f"\nCompleted in {elapsed:.2f}s — {ok} succeeded, {rejected} rejected, {errors} errored")
if elapsed > 3.0:
    print("(That took a while for a local burst — if you saw 0 rejections above,")
    print(" try again; slow local networking can still let the bucket refill")
    print(" faster than expected even with concurrency.)")

print("\nBucket states right now:")
for b in get("/ratelimits")["buckets"]:
    print(f"  {b['node_id']:<10} tokens={b['tokens']:.1f}/{b['capacity']:.0f}")

print("\nWaiting 5s for refill (1 token/sec)...")
time.sleep(5)
print("Bucket states after waiting:")
for b in get("/ratelimits")["buckets"]:
    print(f"  {b['node_id']:<10} tokens={b['tokens']:.1f}/{b['capacity']:.0f}")