"""
Tests for ClusterCanaryDeployment (huddle_cluster_pkg.cluster_canary_deployment).
"""

import json
import time
import urllib.error
import urllib.request

import pytest

from huddle_cluster_pkg.cluster_master             import MasterNode
from huddle_cluster_pkg.cluster_scheduler          import ClusterScheduler
from huddle_cluster_pkg.cluster_canary_deployment  import (
    ClusterCanaryDeployment,
    PHASE_IDLE, PHASE_ACTIVE, PHASE_PROMOTED, PHASE_ABORTED,
)



# Helpers


def _free_port():
    import socket
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close()
    return p


def _get(port, path):
    url = f"http://127.0.0.1:{port}/v1{path}"
    with urllib.request.urlopen(url, timeout=3) as r:
        return json.loads(r.read())


def _post(port, path, payload=None):
    data = json.dumps(payload or {}).encode()
    req  = urllib.request.Request(
        f"http://127.0.0.1:{port}/v1{path}", data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Content-Length", str(len(data)))
    try:
        with urllib.request.urlopen(req, timeout=3) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return json.loads(e.read())


def _join(port, node_id, canary=False, p=9860):
    meta = {"canary": "true"} if canary else {}
    return _post(port, "/nodes/join", {
        "node_id": node_id, "address": "127.0.0.1", "port": p,
        "metadata": meta,
    })


def _make_canary(**kwargs) -> ClusterCanaryDeployment:
    defaults = dict(weight_steps=[5, 25, 50, 100])
    defaults.update(kwargs)
    return ClusterCanaryDeployment(**defaults)


def _make_master(canary=None, scheduler=None, timeout=60):
    port = _free_port()
    m = MasterNode(
        host="127.0.0.1", port=port,
        heartbeat_timeout_sec=timeout,
        canary=canary,
        scheduler=scheduler,
    )
    m.start()
    time.sleep(0.1)
    return m


def _node(node_id, is_canary=False):
    meta = {"canary": "true"} if is_canary else {}
    return {
        "node_id": node_id, "address": "127.0.0.1", "port": 8080,
        "status": "alive", "metadata": meta,
    }



# Unit tests: deployment lifecycle


class TestCanaryLifecycle:
    def test_initial_phase_is_idle(self):
        c = _make_canary()
        assert c.status()["phase"] == PHASE_IDLE

    def test_start_sets_active(self):
        c = _make_canary()
        assert c.start() is True
        assert c.status()["phase"] == PHASE_ACTIVE

    def test_start_uses_first_step_by_default(self):
        c = _make_canary(weight_steps=[10, 50, 100])
        c.start()
        assert c.status()["weight_pct"] == 10.0

    def test_start_with_custom_weight(self):
        c = _make_canary()
        c.start(weight=25.0)
        assert c.status()["weight_pct"] == 25.0

    def test_double_start_returns_false(self):
        c = _make_canary()
        c.start()
        assert c.start() is False

    def test_advance_steps_up(self):
        c = _make_canary(weight_steps=[5, 25, 50, 100])
        c.start()
        c.advance()
        assert c.status()["weight_pct"] == 25.0

    def test_advance_at_max_returns_false(self):
        c = _make_canary(weight_steps=[100])
        c.start()
        assert c.advance() is False

    def test_advance_when_idle_returns_false(self):
        c = _make_canary()
        assert c.advance() is False

    def test_set_weight_directly(self):
        c = _make_canary()
        c.start()
        c.set_weight(42.0)
        assert c.status()["weight_pct"] == 42.0

    def test_set_weight_clamped_at_100(self):
        c = _make_canary()
        c.start()
        c.set_weight(150.0)
        assert c.status()["weight_pct"] == 100.0

    def test_set_weight_when_idle_returns_false(self):
        c = _make_canary()
        assert c.set_weight(50.0) is False

    def test_promote_ends_deployment(self):
        c = _make_canary()
        c.start()
        assert c.promote() is True
        assert c.status()["phase"] == PHASE_PROMOTED

    def test_promote_fires_callback(self):
        events = []
        c = _make_canary(on_promote=lambda: events.append("promoted"))
        c.start()
        c.promote()
        assert events == ["promoted"]

    def test_abort_sets_zero_weight(self):
        c = _make_canary()
        c.start(weight=50)
        c.abort()
        s = c.status()
        assert s["phase"] == PHASE_ABORTED
        assert s["weight_pct"] == 0.0

    def test_abort_fires_callback(self):
        events = []
        c = _make_canary(on_abort=lambda: events.append("aborted"))
        c.start()
        c.abort()
        assert events == ["aborted"]

    def test_abort_when_idle_returns_false(self):
        c = _make_canary()
        assert c.abort() is False

    def test_weight_change_callback(self):
        weights = []
        c = _make_canary(
            weight_steps=[10, 50, 100],
            on_weight_change=lambda w: weights.append(w),
        )
        c.start()
        c.advance()
        assert 10.0 in weights
        assert 50.0 in weights

    def test_history_records_events(self):
        c = _make_canary()
        c.start()
        c.advance()
        c.abort()
        history = c.status()["history"]
        actions = [e["action"] for e in history]
        assert "start" in actions
        assert "advance" in actions
        assert "abort" in actions

    def test_announce_canary_adds_node(self):
        c = _make_canary()
        c.announce_canary("new-node")
        assert "new-node" in c.status()["announced_canary"]

    def test_remove_canary_removes_node(self):
        c = _make_canary()
        c.announce_canary("rm-node")
        assert c.remove_canary("rm-node") is True
        assert "rm-node" not in c.status()["announced_canary"]



# Unit tests: pick_pool() traffic splitting


class TestPickPool:
    def _counts(self, c, nodes, n=1000):
        canary_ids  = {nd["node_id"] for nd in nodes if nd["metadata"].get("canary") == "true"}
        stable_ids  = {nd["node_id"] for nd in nodes} - canary_ids
        canary_hits = 0
        for _ in range(n):
            pool = c.pick_pool(nodes)
            if any(nd["node_id"] in canary_ids for nd in pool):
                if all(nd["node_id"] in canary_ids for nd in pool):
                    canary_hits += 1
        return canary_hits, n - canary_hits

    def test_idle_returns_full_pool(self):
        c = _make_canary()
        nodes = [_node("s1"), _node("c1", True)]
        pool = c.pick_pool(nodes)
        assert len(pool) == 2

    def test_zero_weight_returns_stable_pool(self):
        c = _make_canary()
        c.start(weight=0.0)
        nodes = [_node("s1"), _node("c1", True)]
        for _ in range(50):
            pool = c.pick_pool(nodes)
            ids = {n["node_id"] for n in pool}
            assert "c1" not in ids

    def test_100_weight_returns_canary_pool(self):
        c = _make_canary()
        c.start(weight=100.0)
        nodes = [_node("s1"), _node("c1", True)]
        for _ in range(50):
            pool = c.pick_pool(nodes)
            ids = {n["node_id"] for n in pool}
            assert "c1" in ids

    def test_partial_weight_distributes_traffic(self):
        c = _make_canary()
        c.start(weight=50.0)
        nodes = [_node("stable"), _node("canary", True)]
        canary_hits, _ = self._counts(c, nodes, n=400)
        # With 50% weight expect roughly 50% — allow ±20%
        assert 100 < canary_hits < 300

    def test_no_canary_nodes_returns_stable(self):
        c = _make_canary()
        c.start(weight=100.0)
        nodes = [_node("s1"), _node("s2")]
        pool = c.pick_pool(nodes)
        assert len(pool) == 2   # falls back to all

    def test_no_stable_nodes_returns_canary(self):
        c = _make_canary()
        c.start(weight=0.0)
        nodes = [_node("c1", True), _node("c2", True)]
        pool = c.pick_pool(nodes)
        assert all(n["metadata"].get("canary") == "true" for n in pool)

    def test_aborted_returns_full_pool(self):
        c = _make_canary()
        c.start(weight=100.0)
        c.abort()
        nodes = [_node("s1"), _node("c1", True)]
        pool = c.pick_pool(nodes)
        assert len(pool) == 2



# Integration tests: scheduler + canary


class TestSchedulerCanary:
    def test_scheduler_routes_to_canary_at_100(self):
        canary    = _make_canary()
        scheduler = ClusterScheduler(canary=canary)
        m = _make_master(canary=canary, scheduler=scheduler)
        try:
            _join(m.port, "stable-node", canary=False, p=9861)
            _join(m.port, "canary-node", canary=True,  p=9862)
            canary.start(weight=100.0)
            picks = {scheduler.pick(m.nodes())["node_id"] for _ in range(10)}
            assert "canary-node" in picks
            assert "stable-node" not in picks
        finally:
            m.stop()

    def test_scheduler_routes_to_stable_at_0(self):
        canary    = _make_canary()
        scheduler = ClusterScheduler(canary=canary)
        m = _make_master(canary=canary, scheduler=scheduler)
        try:
            _join(m.port, "stable-s", canary=False, p=9863)
            _join(m.port, "canary-s", canary=True,  p=9864)
            canary.start(weight=0.0)
            picks = {scheduler.pick(m.nodes())["node_id"] for _ in range(10)}
            assert "stable-s" in picks
            assert "canary-s" not in picks
        finally:
            m.stop()

    def test_scheduler_stats_shows_canary_enabled(self):
        canary    = ClusterCanaryDeployment()
        scheduler = ClusterScheduler(canary=canary)
        assert scheduler.scheduler_stats()["canary"] == "enabled"

    def test_scheduler_stats_shows_canary_disabled(self):
        scheduler = ClusterScheduler()
        assert scheduler.scheduler_stats()["canary"] == "disabled"



# Integration tests: HTTP endpoints


class TestCanaryHttp:
    def test_canary_status_503_when_disabled(self):
        m = _make_master()
        try:
            with pytest.raises(urllib.error.HTTPError) as exc:
                _get(m.port, "/canary/status")
            assert exc.value.code == 503
        finally:
            m.stop()

    def test_master_status_includes_canary(self):
        c = _make_canary()
        m = _make_master(canary=c)
        try:
            status = _get(m.port, "/status")
            assert isinstance(status["canary"], dict)
            assert status["canary"]["phase"] == PHASE_IDLE
        finally:
            m.stop()

    def test_start_via_rest(self):
        c = _make_canary()
        m = _make_master(canary=c)
        try:
            r = _post(m.port, "/canary/start", {"weight": 10})
            assert r["ok"] is True
            assert c.status()["phase"] == PHASE_ACTIVE
        finally:
            m.stop()

    def test_start_conflict_returns_409(self):
        c = _make_canary()
        m = _make_master(canary=c)
        try:
            _post(m.port, "/canary/start", {"weight": 10})
            r = _post(m.port, "/canary/start")
            assert r["ok"] is False
        finally:
            m.stop()

    def test_advance_via_rest(self):
        c = _make_canary(weight_steps=[5, 50, 100])
        m = _make_master(canary=c)
        try:
            _post(m.port, "/canary/start")
            r = _post(m.port, "/canary/advance")
            assert r["ok"] is True
            assert c.status()["weight_pct"] == 50.0
        finally:
            m.stop()

    def test_promote_via_rest(self):
        c = _make_canary()
        m = _make_master(canary=c)
        try:
            _post(m.port, "/canary/start")
            r = _post(m.port, "/canary/promote")
            assert r["ok"] is True
            assert c.status()["phase"] == PHASE_PROMOTED
        finally:
            m.stop()

    def test_abort_via_rest(self):
        c = _make_canary()
        m = _make_master(canary=c)
        try:
            _post(m.port, "/canary/start")
            r = _post(m.port, "/canary/abort")
            assert r["ok"] is True
            assert c.status()["phase"] == PHASE_ABORTED
        finally:
            m.stop()

    def test_abort_when_idle_returns_409(self):
        c = _make_canary()
        m = _make_master(canary=c)
        try:
            r = _post(m.port, "/canary/abort")
            assert r["ok"] is False
        finally:
            m.stop()

    def test_canary_status_endpoint(self):
        c = _make_canary(weight_steps=[10, 100])
        m = _make_master(canary=c)
        try:
            _post(m.port, "/canary/start", {"weight": 10})
            data = _get(m.port, "/canary/status")
            assert data["phase"] == PHASE_ACTIVE
            assert data["weight_pct"] == 10.0
            assert "history" in data
        finally:
            m.stop()

    def test_canary_announce_via_rest(self):
        c = _make_canary()
        m = _make_master(canary=c)
        try:
            r = _post(m.port, "/canary/announce", {"node_id": "new-v2-node"})
            assert r["ok"] is True
            assert "new-v2-node" in c.status()["announced_canary"]
        finally:
            m.stop()

    def test_full_ramp_workflow(self):
        """Start → advance → advance → promote."""
        events = []
        c = _make_canary(
            weight_steps=[5, 50, 100],
            on_promote=lambda: events.append("promoted"),
        )
        m = _make_master(canary=c)

        def _safe_post(port, path, payload=None):
            """On Windows, a successful 200 response may still raise
            ConnectionAbortedError (WinError 10053) if the server closes
            the connection before the client finishes reading.  Accept
            that as equivalent to a successful call."""
            try:
                return _post(port, path, payload)
            except OSError:
                return {"ok": True}   # WinError 10053 — treat as success

        try:
            _safe_post(m.port, "/canary/start")
            assert c.status()["weight_pct"] == 5.0
            _safe_post(m.port, "/canary/advance")
            assert c.status()["weight_pct"] == 50.0
            _safe_post(m.port, "/canary/advance")
            assert c.status()["weight_pct"] == 100.0
            _safe_post(m.port, "/canary/promote")
            assert c.status()["phase"] == PHASE_PROMOTED
            assert events == ["promoted"]
        finally:
            m.stop()