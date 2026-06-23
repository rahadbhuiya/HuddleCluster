"""
Tests for ClusterScheduler (huddle_cluster_pkg.cluster_scheduler).
"""

import json
import time
import urllib.error
import urllib.request

import pytest

from huddle_cluster_pkg.cluster_master    import MasterNode
from huddle_cluster_pkg.cluster_scheduler import ClusterScheduler, _node_fitness



# Helpers


def _free_port():
    import socket
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def _get(port, path):
    url = f"http://127.0.0.1:{port}/v1{path}"
    with urllib.request.urlopen(url, timeout=3) as r:
        return json.loads(r.read())


def _post(port, path, payload):
    data = json.dumps(payload).encode()
    req  = urllib.request.Request(
        f"http://127.0.0.1:{port}/v1{path}",
        data=data, method="POST",
    )
    req.add_header("Content-Type",   "application/json")
    req.add_header("Content-Length", str(len(data)))
    try:
        with urllib.request.urlopen(req, timeout=3) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return json.loads(e.read())


def _join(port, node_id, p=9800):
    return _post(port, "/nodes/join", {
        "node_id": node_id, "address": "127.0.0.1", "port": p,
    })


def _make_master(scheduler=None, timeout=60):
    port = _free_port()
    m = MasterNode(
        host="127.0.0.1", port=port,
        heartbeat_timeout_sec=timeout,
        scheduler=scheduler,
    )
    m.start()
    time.sleep(0.1)
    return m


def _alive_node(node_id="n1", hb=5, deaths=0, last_seen=0.0):
    """Build a minimal node dict that _node_fitness can score."""
    return {
        "node_id": node_id,
        "address": "127.0.0.1",
        "port": 8080,
        "status": "alive",
        "heartbeat_count": hb,
        "death_count": deaths,
        "last_seen_ago_sec": last_seen,
        "metrics": {},
    }



# Unit tests: fitness scoring


class TestFitnessScoring:
    def test_alive_node_has_positive_score(self):
        assert _node_fitness(_alive_node(), time.time()) > 0

    def test_dead_node_returns_negative(self):
        n = _alive_node(); n["status"] = "dead"
        assert _node_fitness(n, time.time()) < 0

    def test_leaving_node_returns_negative(self):
        n = _alive_node(); n["status"] = "leaving"
        assert _node_fitness(n, time.time()) < 0

    def test_quarantined_node_scores_lower_than_alive(self):
        alive_score = _node_fitness(_alive_node("a"), time.time())
        q = _alive_node("q"); q["status"] = "quarantined"
        q_score = _node_fitness(q, time.time())
        assert alive_score > q_score > 0

    def test_more_deaths_lower_score(self):
        stable = _node_fitness(_alive_node(deaths=0), time.time())
        flappy = _node_fitness(_alive_node(deaths=10), time.time())
        assert stable > flappy

    def test_stale_node_lower_score(self):
        fresh = _node_fitness(_alive_node(last_seen=1.0), time.time())
        stale = _node_fitness(_alive_node(last_seen=300.0), time.time())
        assert fresh > stale

    def test_high_rps_lowers_score(self):
        base   = _alive_node(); base["metrics"] = {}
        loaded = _alive_node(); loaded["metrics"] = {"requests_per_sec": 500}
        assert _node_fitness(base, time.time()) > _node_fitness(loaded, time.time())

    def test_warmup_bonus_for_low_heartbeat_count(self):
        new_node = _node_fitness(_alive_node(hb=0), time.time())
        veteran  = _node_fitness(_alive_node(hb=200), time.time())
        assert new_node > veteran



# Unit tests: ClusterScheduler.pick()


class TestSchedulerPick:
    def test_returns_none_when_no_nodes(self):
        s = ClusterScheduler()
        assert s.pick([]) is None

    def test_returns_none_when_all_dead(self):
        s = ClusterScheduler()
        n = _alive_node(); n["status"] = "dead"
        assert s.pick([n]) is None

    def test_picks_single_eligible_node(self):
        s = ClusterScheduler()
        n = _alive_node("solo")
        result = s.pick([n])
        assert result is not None
        assert result["node_id"] == "solo"

    def test_prefers_alive_over_quarantined(self):
        s = ClusterScheduler(prefer_alive=True)
        alive = _alive_node("a"); alive["status"] = "alive"
        q = _alive_node("q"); q["status"] = "quarantined"
        result = s.pick([q, alive])
        assert result["node_id"] == "a"

    def test_prefer_alive_false_can_pick_quarantined(self):
        s = ClusterScheduler(prefer_alive=False)
        q = _alive_node("q"); q["status"] = "quarantined"
        result = s.pick([q])
        assert result is not None
        assert result["node_id"] == "q"

    def test_heat_penalises_recently_used_node(self):
        s = ClusterScheduler(cooldown_sec=60)
        n1 = _alive_node("n1")
        n2 = _alive_node("n2")
        picks = [s.pick([n1, n2])["node_id"] for _ in range(10)]
        # After first pick of n1 it should not keep winning every time
        assert "n1" in picks and "n2" in picks

    def test_heat_decays_over_time(self):
        s = ClusterScheduler(cooldown_sec=0.1)
        n = _alive_node("n1")
        s.pick([n])
        time.sleep(0.5)     # >> one cooldown cycle
        # Heat should have decayed; pick should succeed again
        result = s.pick([n])
        assert result["node_id"] == "n1"

    def test_affinity_returns_same_node(self):
        s = ClusterScheduler()
        nodes = [_alive_node("a"), _alive_node("b")]
        first  = s.pick(nodes, affinity_key="user-42")
        second = s.pick(nodes, affinity_key="user-42")
        assert first["node_id"] == second["node_id"]

    def test_affinity_different_keys_can_pick_different_nodes(self):
        s = ClusterScheduler()
        nodes = [_alive_node("a"), _alive_node("b")]
        picks = {s.pick(nodes, affinity_key=f"user-{i}")["node_id"]
                 for i in range(20)}
        # With 20 users and 2 nodes, both should be bound
        assert len(picks) >= 2

    def test_affinity_falls_back_when_bound_node_dies(self):
        s = ClusterScheduler()
        n1 = _alive_node("n1")
        n2 = _alive_node("n2")
        s.pick([n1, n2], affinity_key="sess")   # bind to one node
        # Mark whichever got bound as dead
        bound_id = s._affinity_map.get("sess")
        n_dead = _alive_node(bound_id); n_dead["status"] = "dead"
        n_alive = _alive_node("n2" if bound_id == "n1" else "n1")
        result = s.pick([n_dead, n_alive], affinity_key="sess")
        assert result is not None
        assert result["node_id"] != bound_id

    def test_scheduler_stats_tracks_counts(self):
        s = ClusterScheduler()
        nodes = [_alive_node("a"), _alive_node("b")]
        for _ in range(5):
            s.pick(nodes)
        stats = s.scheduler_stats()
        total = sum(stats["workload_count"].values())
        assert total == 5



# Unit tests: ClusterScheduler.record_report()


class TestSchedulerReport:
    def test_report_increments_count(self):
        s = ClusterScheduler()
        s.record_report("n1", duration_ms=45.0, success=True)
        s.record_report("n1", duration_ms=30.0, success=False)
        stats = s.scheduler_stats()
        assert stats["report_count"] == 2

    def test_report_buffer_bounded_at_1000(self):
        s = ClusterScheduler()
        for i in range(1100):
            s.record_report("n1")
        assert s.scheduler_stats()["report_count"] == 1000



# Integration tests: HTTP endpoints via MasterNode


class TestSchedulerHttp:
    def test_next_returns_503_when_scheduler_disabled(self):
        m = _make_master()
        try:
            with pytest.raises(urllib.error.HTTPError) as exc:
                _get(m.port, "/scheduler/next")
            assert exc.value.code == 503
        finally:
            m.stop()

    def test_next_returns_503_when_no_nodes(self):
        m = _make_master(scheduler=ClusterScheduler())
        try:
            with pytest.raises(urllib.error.HTTPError) as exc:
                _get(m.port, "/scheduler/next")
            assert exc.value.code == 503
        finally:
            m.stop()

    def test_next_returns_node_when_available(self):
        m = _make_master(scheduler=ClusterScheduler())
        try:
            _join(m.port, "ws1")
            result = _get(m.port, "/scheduler/next")
            assert result["ok"] is True
            assert result["node"]["node_id"] == "ws1"
        finally:
            m.stop()

    def test_next_with_affinity_returns_same_node(self):
        m = _make_master(scheduler=ClusterScheduler())
        try:
            _join(m.port, "ws2")
            _join(m.port, "ws3")
            r1 = _get(m.port, "/scheduler/next?affinity=session-abc")
            r2 = _get(m.port, "/scheduler/next?affinity=session-abc")
            assert r1["node"]["node_id"] == r2["node"]["node_id"]
        finally:
            m.stop()

    def test_next_distributes_across_nodes(self):
        m = _make_master(scheduler=ClusterScheduler())
        try:
            for i in range(4):
                _join(m.port, f"ws{i}")
            picks = {_get(m.port, "/scheduler/next")["node"]["node_id"]
                     for _ in range(20)}
            assert len(picks) >= 2
        finally:
            m.stop()

    def test_stats_endpoint_returns_heat_map(self):
        m = _make_master(scheduler=ClusterScheduler())
        try:
            _join(m.port, "ws4")
            _get(m.port, "/scheduler/next")
            stats = _get(m.port, "/scheduler/stats")
            assert "heat" in stats
            assert "workload_count" in stats
        finally:
            m.stop()

    def test_report_endpoint_records(self):
        m = _make_master(scheduler=ClusterScheduler())
        try:
            _join(m.port, "ws5")
            r = _post(m.port, "/scheduler/report", {
                "node_id": "ws5", "duration_ms": 120.0, "success": True,
            })
            assert r["ok"] is True
            stats = _get(m.port, "/scheduler/stats")
            assert stats["report_count"] == 1
        finally:
            m.stop()

    def test_report_missing_node_id_returns_400(self):
        m = _make_master(scheduler=ClusterScheduler())
        try:
            r = _post(m.port, "/scheduler/report", {"duration_ms": 10})
            assert r["ok"] is False
        finally:
            m.stop()

    def test_status_reports_scheduler_enabled(self):
        m = _make_master(scheduler=ClusterScheduler())
        try:
            status = _get(m.port, "/status")
            assert status["scheduler"] == "enabled"
        finally:
            m.stop()

    def test_status_reports_scheduler_disabled(self):
        m = _make_master()
        try:
            status = _get(m.port, "/status")
            assert status["scheduler"] == "disabled"
        finally:
            m.stop()

    def test_next_skips_dead_nodes(self):
        m = _make_master(scheduler=ClusterScheduler(), timeout=0.3)
        try:
            _join(m.port, "dead1")
            _join(m.port, "alive1")
            time.sleep(0.5)   # dead1 times out (no heartbeats)
            _post(m.port, "/nodes/alive1/heartbeat", {})  # keep alive1 alive
            result = _get(m.port, "/scheduler/next")
            assert result["node"]["node_id"] == "alive1"
        finally:
            m.stop()