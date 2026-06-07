"""
tests/test_integration.py — End-to-end integration tests
==========================================================
Tests HuddleCluster against real HTTP upstream servers using
FastAPI + uvicorn running in background threads.

These tests verify the complete pipeline:
  request -> get_server() -> real HTTP call -> record_latency() -> anomaly detection -> eviction

Requirements:
  pip install fastapi uvicorn httpx

Skip gracefully if dependencies are missing.
"""

import sys
import threading
import time
import unittest

# Skip entire module if FastAPI/httpx not available
try:
    import httpx
    import uvicorn
    from fastapi import FastAPI
    from fastapi.responses import JSONResponse
    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False


if HAS_FASTAPI:
    sys.path.insert(0, ".")
    from huddle_cluster import create_cluster, HuddleCluster, Server, Position


#  Mini upstream server factory 

def make_upstream(port: int, base_latency_ms: float = 15.0) -> dict:
    """
    Create a minimal FastAPI upstream server.
    Returns a dict with start(), stop(), make_slow(), make_dead() controls.
    """
    app   = FastAPI()
    state = {"is_slow": False, "is_dead": False, "requests": 0}

    @app.get("/health")
    def health():
        return {"status": "ok", "port": port}

    @app.get("/api/work")
    def work():
        import asyncio, random
        state["requests"] += 1
        if state["is_dead"]:
            time.sleep(5.0)
            return JSONResponse({"error": "dead"}, status_code=503)
        base   = base_latency_ms * 5 if state["is_slow"] else base_latency_ms
        delay  = max(0.001, (base + random.gauss(0, 2)) / 1000.0)
        time.sleep(delay)
        return {"port": port, "requests": state["requests"]}

    server = uvicorn.Server(uvicorn.Config(
        app, host="127.0.0.1", port=port,
        log_level="error", access_log=False
    ))
    thread = threading.Thread(target=server.run, daemon=True)

    def start():
        thread.start()
        # Wait until ready
        for _ in range(30):
            try:
                with httpx.Client(timeout=0.5) as c:
                    if c.get(f"http://127.0.0.1:{port}/health").status_code == 200:
                        return True
            except Exception:
                pass
            time.sleep(0.2)
        return False

    def stop():
        server.should_exit = True

    return {
        "start": start, "stop": stop,
        "state": state,
        "url": f"http://127.0.0.1:{port}",
    }


def route_request(cluster, servers_by_id: dict, timeout: float = 3.0) -> float:
    """Route one real HTTP request through HuddleCluster. Returns latency_ms."""
    server = cluster.get_server()
    if server is None:
        return 0.0
    url    = f"http://127.0.0.1:{server.port}/api/work"
    t0     = time.perf_counter()
    try:
        with httpx.Client(timeout=timeout) as c:
            c.get(url)
        elapsed = (time.perf_counter() - t0) * 1000.0
    except Exception:
        elapsed = timeout * 1000.0
    cluster.record_latency(server, elapsed)
    return elapsed


#  Base test case 

@unittest.skipUnless(HAS_FASTAPI, "fastapi/uvicorn/httpx not installed")
class IntegrationBase(unittest.TestCase):
    BASE_PORTS = [19001, 19002, 19003, 19004]

    def setUp(self):
        self.upstreams = {}
        for i, port in enumerate(self.BASE_PORTS):
            u = make_upstream(port, base_latency_ms=12.0 + i * 2)
            ok = u["start"]()
            self.assertTrue(ok, f"Upstream on port {port} failed to start")
            self.upstreams[f"s{i}"] = u

    def tearDown(self):
        for u in self.upstreams.values():
            u["stop"]()
        time.sleep(0.2)

    def _cluster(self, **kwargs) -> HuddleCluster:
        addrs = [(f"s{i}", "127.0.0.1", 19001 + i) for i in range(len(self.BASE_PORTS))]
        defaults = dict(
            min_inner_size=2, max_inner_size=4,
            rotation_cooldown_sec=0.0,
            min_outer_dwell_sec=0.3,
            ema_alpha=0.6,
        )
        defaults.update(kwargs)
        c = create_cluster(addrs, **defaults)
        c.start(rotation_interval_sec=0.2)
        time.sleep(0.3)
        return c


#  Test Cases 

@unittest.skipUnless(HAS_FASTAPI, "fastapi/uvicorn/httpx not installed")
class TestNormalLoad(IntegrationBase):
    """Under normal load, all servers should receive requests and latency should be low."""

    def test_all_servers_receive_requests(self):
        cluster = self._cluster()
        try:
            for _ in range(40):
                route_request(cluster, self.upstreams)
        finally:
            cluster.stop()

        total_requests = sum(u["state"]["requests"] for u in self.upstreams.values())
        self.assertGreater(total_requests, 30, "Too few requests routed")

        # Each server should have received some requests
        for sid, u in self.upstreams.items():
            self.assertGreater(
                u["state"]["requests"], 0,
                f"Server {sid} received no requests"
            )

    def test_health_report_populated(self):
        cluster = self._cluster()
        try:
            for _ in range(20):
                route_request(cluster, self.upstreams)
            report = cluster.health_report()
        finally:
            cluster.stop()

        self.assertEqual(report["status"], "healthy")
        self.assertGreater(report["inner_count"], 0)
        # After requests, avg_latency_ms should be populated
        for s in report["inner_ring"]:
            self.assertGreater(s["avg_latency_ms"], 0.0,
                f"Server {s['id']} has no latency data")

    def test_fairness_score_low(self):
        cluster = self._cluster()
        try:
            for _ in range(60):
                route_request(cluster, self.upstreams)
            score = cluster.fairness_score()
        finally:
            cluster.stop()

        self.assertLess(score, 0.30,
            f"Fairness score {score:.3f} too high under normal load")

    def test_prometheus_metrics_populated(self):
        cluster = self._cluster()
        try:
            for _ in range(20):
                route_request(cluster, self.upstreams)
            metrics = cluster.prometheus_metrics()
        finally:
            cluster.stop()

        self.assertIn("huddle_server_temperature", metrics)
        self.assertIn("huddle_cluster_inner_count", metrics)
        self.assertIn("huddle_cluster_fairness_gini", metrics)


@unittest.skipUnless(HAS_FASTAPI, "fastapi/uvicorn/httpx not installed")
class TestSlowServerDetection(IntegrationBase):
    """Slow server should be detected and evicted via latency feedback."""

    def test_slow_server_evicted(self):
        # Cross-platform fix: on Windows, HTTP overhead (~30-60ms) raises all
        # server latencies equally, so the relative anomaly between slow and
        # normal servers is smaller than on Linux.
        #
        # Two complementary eviction mechanisms are used:
        #   1. absolute_latency_floor_ms=90: catches Windows where
        #      s2_slow (80ms delay + overhead) exceeds the floor.
        #   2. heat_threshold=0.4 / cool_threshold=0.15: catches lower-overhead
        #      environments where the relative anomaly (~0.4-0.6) is enough.
        #
        # Normal servers (12-18ms + overhead) stay well below the 90ms floor,
        # so no false evictions occur on typical machines.
        cluster = self._cluster(
            absolute_latency_floor_ms=90.0,
            heat_threshold=0.4,
            cool_threshold=0.15,
        )

        # Find s2
        s2 = next(s for s in cluster.all_servers() if s.id == "s2")

        try:
            # Phase 1: 20 normal requests to establish baseline
            for _ in range(20):
                route_request(cluster, self.upstreams)

            # Inject slowness into s2
            self.upstreams["s2"]["state"]["is_slow"] = True

            # Phase 2: 40 more requests -- s2 should self-evict
            for _ in range(40):
                route_request(cluster, self.upstreams, timeout=2.0)
                time.sleep(0.05)

            time.sleep(0.5)  # allow rotation cycle

        finally:
            cluster.stop()

        self.assertEqual(
            s2.position, Position.OUTER,
            f"Slow server s2 was not evicted. temp={s2.temperature:.3f}, "
            f"avg_ms={s2.metrics.avg_response_ms:.1f}"
        )

    def test_on_eviction_fires(self):
        evicted = []
        cluster = self._cluster(
            absolute_latency_floor_ms=90.0,
            heat_threshold=0.4,
            cool_threshold=0.15,
            on_eviction=lambda s, r: evicted.append(s.id)
        )
        try:
            for _ in range(15):
                route_request(cluster, self.upstreams)
            self.upstreams["s2"]["state"]["is_slow"] = True
            for _ in range(40):
                route_request(cluster, self.upstreams, timeout=2.0)
                time.sleep(0.05)
            time.sleep(0.5)
        finally:
            cluster.stop()

        self.assertIn("s2", evicted,
            "on_eviction callback was not fired for slow server s2")


@unittest.skipUnless(HAS_FASTAPI, "fastapi/uvicorn/httpx not installed")
class TestCircuitBreaker(IntegrationBase):
    """High error rate should trigger circuit breaker eviction."""

    def test_high_error_rate_evicts_server(self):
        cluster = self._cluster(circuit_breaker_threshold=0.4)

        # Find s1 and manually set high error rate
        s1 = next(s for s in cluster.all_servers() if s.id == "s1")
        s1.metrics.error_rate = 0.8

        try:
            cluster.rotate()
        finally:
            cluster.stop()

        self.assertEqual(s1.position, Position.OUTER,
            "Server with high error_rate should be circuit-breaker evicted")


@unittest.skipUnless(HAS_FASTAPI, "fastapi/uvicorn/httpx not installed")
class TestGracefulShutdown(IntegrationBase):
    """Graceful drain should wait for in-flight requests."""

    def test_stop_with_drain(self):
        cluster = self._cluster()
        try:
            for _ in range(10):
                route_request(cluster, self.upstreams)
        finally:
            t0 = time.time()
            cluster.stop(drain_timeout_sec=1.0)
            elapsed = time.time() - t0

        # Should return in reasonable time
        self.assertLess(elapsed, 3.0,
            f"Graceful stop took too long: {elapsed:.2f}s")


@unittest.skipUnless(HAS_FASTAPI, "fastapi/uvicorn/httpx not installed")
class TestContextManager(IntegrationBase):
    """get_server_context() should auto-record latency and handle errors."""

    def test_context_records_latency(self):
        cluster = self._cluster()
        try:
            for _ in range(15):
                with cluster.get_server_context() as server:
                    if server:
                        with httpx.Client(timeout=2.0) as c:
                            c.get(f"http://127.0.0.1:{server.port}/api/work")

            inner = cluster.inner_servers()
        finally:
            cluster.stop()

        # At least some servers should have latency data
        populated = [s for s in inner if s.metrics.avg_response_ms > 0]
        self.assertGreater(len(populated), 0,
            "No inner-ring servers have latency data after 15 requests")

    def test_context_increments_error_on_exception(self):
        cluster = self._cluster()
        s = cluster.get_server()

        try:
            for _ in range(5):
                try:
                    with cluster.get_server_context() as sv:
                        if sv and sv.id == s.id:
                            raise ConnectionError("simulated timeout")
                except ConnectionError:
                    pass
        finally:
            cluster.stop()

        self.assertGreater(s.metrics.error_rate, 0.0,
            "Error rate should increase after repeated exceptions")


@unittest.skipUnless(HAS_FASTAPI, "fastapi/uvicorn/httpx not installed")
class TestBatchLatency(IntegrationBase):
    """batch_record_latency should update all servers efficiently."""

    def test_batch_updates_anomaly(self):
        cluster = self._cluster()
        servers = {s.id: s for s in cluster.all_servers()}

        # Feed: s0 normal, s1 normal, s2 very slow
        cluster.batch_record_latency([
            (servers["s0"], 12.0),
            (servers["s1"], 14.0),
            (servers["s2"], 150.0),  # 10x slower
        ])

        self.assertGreater(
            servers["s2"].metrics.latency_anomaly_score,
            servers["s0"].metrics.latency_anomaly_score,
            "Slow server should have higher anomaly score"
        )

        cluster.stop()


if __name__ == "__main__":
    unittest.main(verbosity=2)