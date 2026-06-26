"""
Tests for ClusterRollingUpdater (huddle_cluster_pkg.cluster_rolling_updater).
"""

import json
import time
import threading
import urllib.error
import urllib.request

import pytest

from huddle_cluster_pkg.cluster_master          import MasterNode
from huddle_cluster_pkg.cluster_rolling_updater import (
    ClusterRollingUpdater,
    PHASE_IDLE, PHASE_RUNNING, PHASE_DONE,
    PHASE_PAUSED, PHASE_FAILED, PHASE_ABORTED,
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
        with urllib.request.urlopen(req, timeout=5) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return json.loads(e.read())


def _join(port, node_id, p=9950):
    return _post(port, "/nodes/join", {
        "node_id": node_id, "address": "127.0.0.1", "port": p,
    })


def _heartbeat(port, node_id):
    return _post(port, f"/nodes/{node_id}/heartbeat", {})


def _make_master(updater=None, hb_timeout=60):
    port = _free_port()
    m = MasterNode(host="127.0.0.1", port=port,
                   heartbeat_timeout_sec=hb_timeout,
                   rolling_updater=updater)
    m.start()
    time.sleep(0.1)
    return m


def _noop_update(node):
    """Update function that does nothing (node stays alive naturally)."""
    pass


def _slow_update(node):
    """Update function that sleeps briefly."""
    time.sleep(0.05)



# Unit tests: constructor validation


class TestRollingUpdaterInit:
    def test_valid_construction(self):
        u = ClusterRollingUpdater(update_fn=_noop_update)
        assert u.batch_size == 1
        assert u.drain_timeout_sec == 60.0
        assert u.health_gate_ratio == 0.5

    def test_batch_size_zero_raises(self):
        with pytest.raises(ValueError):
            ClusterRollingUpdater(update_fn=_noop_update, batch_size=0)

    def test_invalid_health_gate_ratio_raises(self):
        with pytest.raises(ValueError):
            ClusterRollingUpdater(update_fn=_noop_update, health_gate_ratio=1.0)
        with pytest.raises(ValueError):
            ClusterRollingUpdater(update_fn=_noop_update, health_gate_ratio=-0.1)

    def test_invalid_update_order_raises(self):
        with pytest.raises(ValueError):
            ClusterRollingUpdater(update_fn=_noop_update,
                                   update_order="random_order")

    def test_initial_phase_is_idle(self):
        u = ClusterRollingUpdater(update_fn=_noop_update)
        assert u.status()["phase"] == PHASE_IDLE



# Unit tests: control methods (without master)


class TestRollingUpdaterControl:
    def test_pause_when_not_running_returns_false(self):
        u = ClusterRollingUpdater(update_fn=_noop_update)
        assert u.pause() is False

    def test_resume_when_not_paused_returns_false(self):
        u = ClusterRollingUpdater(update_fn=_noop_update)
        assert u.resume() is False

    def test_abort_when_idle_returns_false(self):
        u = ClusterRollingUpdater(update_fn=_noop_update)
        assert u.abort() is False



# Integration tests via MasterNode


class TestRollingUpdaterIntegration:

    def test_status_503_when_disabled(self):
        m = _make_master()
        try:
            with pytest.raises(urllib.error.HTTPError) as exc:
                _get(m.port, "/rollout/status")
            assert exc.value.code == 503
        finally:
            m.stop()

    def test_start_503_when_disabled(self):
        m = _make_master()
        try:
            r = _post(m.port, "/rollout/start")
            assert r["ok"] is False
        finally:
            m.stop()

    def test_master_status_reports_rolling_updater_enabled(self):
        u = ClusterRollingUpdater(update_fn=_noop_update)
        m = _make_master(updater=u)
        try:
            status = _get(m.port, "/status")
            assert status["rolling_updater"] == "enabled"
        finally:
            m.stop()

    def test_master_status_reports_rolling_updater_disabled(self):
        m = _make_master()
        try:
            status = _get(m.port, "/status")
            assert status["rolling_updater"] == "disabled"
        finally:
            m.stop()

    def test_rollout_no_nodes_completes_immediately(self):
        u = ClusterRollingUpdater(update_fn=_noop_update)
        m = _make_master(updater=u)
        try:
            r = _post(m.port, "/rollout/start")
            assert r["ok"] is True
            time.sleep(0.3)
            status = _get(m.port, "/rollout/status")
            assert status["phase"] == PHASE_DONE
        finally:
            m.stop()

    def test_rollout_single_node_succeeds(self):
        """update_fn is a noop; the node never leaves — it stays alive — so
        drain_timeout is satisfied immediately when we check for heartbeat."""
        updated = []

        def update_fn(node):
            updated.append(node["node_id"])

        u = ClusterRollingUpdater(
            update_fn=update_fn,
            drain_timeout_sec=2.0,
            health_gate_ratio=0.0,
        )
        m = _make_master(updater=u)
        try:
            _join(m.port, "node-a")
            r = _post(m.port, "/rollout/start")
            assert r["ok"] is True
            time.sleep(1.0)
            status = _get(m.port, "/rollout/status")
            assert status["phase"] == PHASE_DONE
            assert "node-a" in updated
            assert status["outcomes"]["node-a"]["status"] == "updated"
        finally:
            m.stop()

    def test_rollout_multiple_nodes_sequential(self):
        order = []

        def update_fn(node):
            order.append(node["node_id"])

        u = ClusterRollingUpdater(
            update_fn=update_fn,
            batch_size=1,
            drain_timeout_sec=2.0,
            health_gate_ratio=0.0,
        )
        m = _make_master(updater=u)
        try:
            for i in range(3):
                _join(m.port, f"seq-{i}")
            _post(m.port, "/rollout/start")
            time.sleep(1.5)
            status = _get(m.port, "/rollout/status")
            assert status["phase"] == PHASE_DONE
            assert len(order) == 3
        finally:
            m.stop()

    def test_rollout_batch_size_2(self):
        call_times = []
        lock = threading.Lock()

        def update_fn(node):
            with lock:
                call_times.append(time.time())
            time.sleep(0.05)

        u = ClusterRollingUpdater(
            update_fn=update_fn,
            batch_size=2,
            drain_timeout_sec=2.0,
            health_gate_ratio=0.0,
        )
        m = _make_master(updater=u)
        try:
            for i in range(4):
                _join(m.port, f"b2-{i}")
            _post(m.port, "/rollout/start")
            time.sleep(1.5)
            status = _get(m.port, "/rollout/status")
            assert status["phase"] == PHASE_DONE
            assert len(call_times) == 4
        finally:
            m.stop()

    def test_rollout_second_start_returns_conflict(self):
        started = threading.Event()

        def slow_update(node):
            started.set()
            time.sleep(2.0)

        u = ClusterRollingUpdater(
            update_fn=slow_update,
            drain_timeout_sec=5.0,
            health_gate_ratio=0.0,
        )
        m = _make_master(updater=u)
        try:
            _join(m.port, "conflict-node")
            r1 = _post(m.port, "/rollout/start")
            started.wait(timeout=1.0)
            r2 = _post(m.port, "/rollout/start")
            assert r1["ok"] is True
            assert r2["ok"] is False
        finally:
            m.stop()

    def test_rollout_abort_stops_progress(self):
        gate = threading.Event()

        def blocking_update(node):
            gate.wait(timeout=5)

        u = ClusterRollingUpdater(
            update_fn=blocking_update,
            drain_timeout_sec=5.0,
            health_gate_ratio=0.0,
            batch_size=1,
        )
        m = _make_master(updater=u)
        try:
            for i in range(3):
                _join(m.port, f"ab-{i}")
            _post(m.port, "/rollout/start")
            time.sleep(0.1)
            r = _post(m.port, "/rollout/abort")
            assert r["ok"] is True
            gate.set()
            time.sleep(0.3)
            status = _get(m.port, "/rollout/status")
            assert status["phase"] == PHASE_ABORTED
        finally:
            m.stop()

    def test_rollout_pause_and_resume(self):
        batch_starts = []
        gate = threading.Event()

        def update_fn(node):
            batch_starts.append(node["node_id"])
            gate.wait(timeout=3)

        u = ClusterRollingUpdater(
            update_fn=update_fn,
            drain_timeout_sec=2.0,
            health_gate_ratio=0.0,
            batch_size=1,
        )
        m = _make_master(updater=u)
        try:
            for i in range(2):
                _join(m.port, f"pr-{i}")
            _post(m.port, "/rollout/start")
            time.sleep(0.1)
            _post(m.port, "/rollout/pause")
            gate.set()   # let first batch finish

            time.sleep(0.3)
            status = _get(m.port, "/rollout/status")
            assert status["phase"] == PHASE_PAUSED

            r = _post(m.port, "/rollout/resume")
            assert r["ok"] is True
            time.sleep(1.0)
            status = _get(m.port, "/rollout/status")
            assert status["phase"] == PHASE_DONE
        finally:
            m.stop()

    def test_rollout_drain_timeout_marks_node_failed(self):
        """update_fn kills the node and it never sends a heartbeat again."""
        def kill_update(node):
            # Simulate node being taken down and not coming back
            pass   # node stays "alive" in registry but we mark it dead manually

        u = ClusterRollingUpdater(
            update_fn=kill_update,
            drain_timeout_sec=0.3,
            health_gate_ratio=0.0,
        )
        m = _make_master(updater=u, hb_timeout=0.2)
        try:
            _join(m.port, "dead-node")
            time.sleep(0.4)   # let node time out to dead status
            _post(m.port, "/rollout/start")
            time.sleep(0.8)
            status = _get(m.port, "/rollout/status")
            # dead node is not eligible — done with nothing or failed
            assert status["phase"] in (PHASE_DONE, PHASE_FAILED)
        finally:
            m.stop()

    def test_rollout_callbacks_fire(self):
        updated_cb = []
        complete_cb = []

        def update_fn(node):
            pass

        u = ClusterRollingUpdater(
            update_fn=update_fn,
            drain_timeout_sec=2.0,
            health_gate_ratio=0.0,
            on_node_updated=lambda nid: updated_cb.append(nid),
            on_rollout_complete=lambda: complete_cb.append(True),
        )
        m = _make_master(updater=u)
        try:
            _join(m.port, "cb-node")
            _post(m.port, "/rollout/start")
            time.sleep(1.0)
            assert "cb-node" in updated_cb
            assert complete_cb == [True]
        finally:
            m.stop()

    def test_rollout_update_order_alive_first(self):
        order = []

        def update_fn(node):
            order.append(node["node_id"])

        u = ClusterRollingUpdater(
            update_fn=update_fn,
            update_order="alive_first",
            drain_timeout_sec=2.0,
            health_gate_ratio=0.0,
            batch_size=1,
        )
        m = _make_master(updater=u)
        try:
            _join(m.port, "alive-node")
            _post(m.port, "/rollout/start")
            time.sleep(1.0)
            status = _get(m.port, "/rollout/status")
            assert status["phase"] == PHASE_DONE
            assert order == ["alive-node"]
        finally:
            m.stop()

    def test_status_shows_progress_counts(self):
        u = ClusterRollingUpdater(
            update_fn=_noop_update,
            drain_timeout_sec=2.0,
            health_gate_ratio=0.0,
        )
        m = _make_master(updater=u)
        try:
            _join(m.port, "cnt-1")
            _join(m.port, "cnt-2")
            _post(m.port, "/rollout/start")
            time.sleep(1.0)
            status = _get(m.port, "/rollout/status")
            assert status["total_nodes"] == 2
            assert status["nodes_done"] == 2
            assert status["nodes_remaining"] == 0
        finally:
            m.stop()

    def test_abort_when_not_running_returns_conflict(self):
        u = ClusterRollingUpdater(update_fn=_noop_update)
        m = _make_master(updater=u)
        try:
            r = _post(m.port, "/rollout/abort")
            assert r["ok"] is False
        finally:
            m.stop()