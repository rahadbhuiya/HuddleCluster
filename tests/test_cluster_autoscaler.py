"""
Tests for ClusterAutoScaler (huddle_cluster_pkg.cluster_autoscaler).
"""

import json
import time
import urllib.error
import urllib.request

import pytest

from huddle_cluster_pkg.cluster_master     import MasterNode
from huddle_cluster_pkg.cluster_scheduler  import ClusterScheduler
from huddle_cluster_pkg.cluster_autoscaler import (
    ClusterAutoScaler, SCALE_UP, SCALE_DOWN, SCALE_NONE,
)



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


def _join(port, node_id, p=9900):
    return _post(port, "/nodes/join", {
        "node_id": node_id, "address": "127.0.0.1", "port": p,
    })


def _make_autoscaler(**kwargs) -> ClusterAutoScaler:
    defaults = dict(
        min_nodes=1,
        max_nodes=5,
        scale_up_heat_threshold=0.7,
        scale_down_heat_threshold=0.2,
        scale_up_cooldown_sec=0,    # no cooldown in most unit tests
        scale_down_cooldown_sec=0,
        check_interval_sec=60,      # prevent background loop firing in tests
    )
    defaults.update(kwargs)
    return ClusterAutoScaler(**defaults)


def _make_master(autoscaler=None, scheduler=None, timeout=60):
    port = _free_port()
    m = MasterNode(
        host="127.0.0.1", port=port,
        heartbeat_timeout_sec=timeout,
        autoscaler=autoscaler,
        scheduler=scheduler,
    )
    m.start()
    time.sleep(0.1)
    return m



# Unit tests: constructor validation


class TestAutoScalerInit:
    def test_valid_defaults(self):
        a = ClusterAutoScaler()
        assert a.min_nodes == 1
        assert a.max_nodes == 10

    def test_min_nodes_zero_raises(self):
        with pytest.raises(ValueError):
            ClusterAutoScaler(min_nodes=0)

    def test_max_less_than_min_raises(self):
        with pytest.raises(ValueError):
            ClusterAutoScaler(min_nodes=5, max_nodes=3)

    def test_invalid_heat_threshold_raises(self):
        with pytest.raises(ValueError):
            ClusterAutoScaler(scale_up_heat_threshold=0.0)

    def test_down_threshold_above_up_threshold_raises(self):
        with pytest.raises(ValueError):
            ClusterAutoScaler(
                scale_up_heat_threshold=0.3,
                scale_down_heat_threshold=0.6,
            )



# Unit tests: evaluate() logic


class TestAutoScalerEvaluate:

    def test_scale_up_when_below_min(self):
        a = _make_autoscaler(min_nodes=3)
        decision = a.evaluate(alive_nodes=2)
        assert decision == SCALE_UP

    def test_scale_up_when_heat_exceeds_threshold(self):
        a = _make_autoscaler(
            min_nodes=1, max_nodes=5,
            scale_up_heat_threshold=0.5,
        )
        decision = a.evaluate(alive_nodes=2, avg_heat=0.8)
        assert decision == SCALE_UP

    def test_no_scale_up_when_at_max_nodes(self):
        a = _make_autoscaler(
            min_nodes=1, max_nodes=2,
            scale_up_heat_threshold=0.5,
        )
        decision = a.evaluate(alive_nodes=2, avg_heat=0.9)
        assert decision == SCALE_NONE

    def test_scale_down_when_above_max(self):
        a = _make_autoscaler(max_nodes=3)
        decision = a.evaluate(alive_nodes=5)
        assert decision == SCALE_DOWN

    def test_scale_down_when_heat_below_threshold(self):
        a = _make_autoscaler(
            min_nodes=1, max_nodes=5,
            scale_down_heat_threshold=0.3,
        )
        decision = a.evaluate(alive_nodes=3, avg_heat=0.1)
        assert decision == SCALE_DOWN

    def test_no_scale_down_at_min_nodes(self):
        a = _make_autoscaler(min_nodes=2, scale_down_heat_threshold=0.3)
        decision = a.evaluate(alive_nodes=2, avg_heat=0.05)
        assert decision == SCALE_NONE

    def test_no_action_when_healthy(self):
        a = _make_autoscaler(
            min_nodes=1, max_nodes=5,
            scale_up_heat_threshold=0.7,
            scale_down_heat_threshold=0.2,
        )
        decision = a.evaluate(alive_nodes=3, avg_heat=0.4)
        assert decision == SCALE_NONE

    def test_no_action_without_heat_data(self):
        a = _make_autoscaler(min_nodes=1, max_nodes=5)
        decision = a.evaluate(alive_nodes=3, avg_heat=None)
        assert decision == SCALE_NONE

    def test_scale_up_fires_callback(self):
        events = []
        a = _make_autoscaler(
            min_nodes=3,
            on_scale_up=lambda delta: events.append(("up", delta)),
        )
        a.evaluate(alive_nodes=1)
        assert events == [("up", 1)]

    def test_scale_down_fires_callback(self):
        events = []
        a = _make_autoscaler(
            max_nodes=2,
            on_scale_down=lambda delta: events.append(("down", delta)),
        )
        a.evaluate(alive_nodes=4)
        assert events == [("down", 1)]

    def test_scale_up_step_respected(self):
        events = []
        a = _make_autoscaler(
            min_nodes=3, scale_up_step=2,
            on_scale_up=lambda delta: events.append(delta),
        )
        a.evaluate(alive_nodes=1)
        assert events == [2]

    def test_callback_exception_does_not_crash(self):
        def bad(_):
            raise RuntimeError("oops")
        a = _make_autoscaler(min_nodes=3, on_scale_up=bad)
        # Should not raise
        a.evaluate(alive_nodes=1)

    def test_cooldown_suppresses_second_scale_up(self):
        events = []
        a = _make_autoscaler(
            min_nodes=3,
            scale_up_cooldown_sec=60,
            on_scale_up=lambda d: events.append(d),
        )
        a.evaluate(alive_nodes=1)
        a.evaluate(alive_nodes=1)   # still below min, but in cooldown
        assert len(events) == 1

    def test_cooldown_suppresses_second_scale_down(self):
        events = []
        a = _make_autoscaler(
            max_nodes=2,
            scale_down_cooldown_sec=60,
            on_scale_down=lambda d: events.append(d),
        )
        a.evaluate(alive_nodes=5)
        a.evaluate(alive_nodes=5)
        assert len(events) == 1

    def test_history_records_events(self):
        a = _make_autoscaler(min_nodes=3)
        a.evaluate(alive_nodes=1)
        s = a.status()
        assert s["scale_event_count"] == 1
        assert s["history"][0]["direction"] == SCALE_UP

    def test_history_bounded_at_200(self):
        a = _make_autoscaler(min_nodes=3, scale_up_cooldown_sec=0)
        for _ in range(220):
            a._record(SCALE_UP, "test", 1, 1, time.time())
        assert a.status()["scale_event_count"] == 200



# Integration tests: HTTP endpoints via MasterNode


class TestAutoScalerHttp:

    def test_status_returns_503_when_disabled(self):
        m = _make_master()
        try:
            with pytest.raises(urllib.error.HTTPError) as exc:
                _get(m.port, "/autoscaler/status")
            assert exc.value.code == 503
        finally:
            m.stop()

    def test_status_returns_config_and_state(self):
        a = _make_autoscaler(min_nodes=2, max_nodes=8)
        m = _make_master(autoscaler=a)
        try:
            data = _get(m.port, "/autoscaler/status")
            assert data["min_nodes"] == 2
            assert data["max_nodes"] == 8
            assert "last_decision" in data
            assert "history" in data
        finally:
            m.stop()

    def test_master_status_reports_autoscaler_enabled(self):
        a = _make_autoscaler()
        m = _make_master(autoscaler=a)
        try:
            status = _get(m.port, "/status")
            assert status["autoscaler"] == "enabled"
        finally:
            m.stop()

    def test_master_status_reports_autoscaler_disabled(self):
        m = _make_master()
        try:
            status = _get(m.port, "/status")
            assert status["autoscaler"] == "disabled"
        finally:
            m.stop()

    def test_autoscaler_fires_scale_up_when_below_min(self):
        events = []
        a = _make_autoscaler(
            min_nodes=2,
            scale_up_cooldown_sec=0,
            check_interval_sec=0.1,
            on_scale_up=lambda d: events.append(d),
        )
        m = _make_master(autoscaler=a)
        try:
            _join(m.port, "only-one")   # 1 alive < min_nodes=2
            time.sleep(0.5)
            assert len(events) >= 1
        finally:
            m.stop()

    def test_autoscaler_no_action_when_healthy(self):
        """1 alive node with min_nodes=1 — no scale action should fire."""
        events = []
        a = ClusterAutoScaler(
            min_nodes=1, max_nodes=5,
            scale_up_heat_threshold=0.7,
            scale_down_heat_threshold=0.2,
            scale_up_cooldown_sec=0,
            scale_down_cooldown_sec=0,
            check_interval_sec=0.2,
            on_scale_up=lambda d: events.append(("up", d)),
            on_scale_down=lambda d: events.append(("down", d)),
        )
        port = _free_port()
        m = MasterNode(host="127.0.0.1", port=port, heartbeat_timeout_sec=60,
                        autoscaler=a)
        m.start()
        # Join the node immediately so the autoscaler's first tick sees 1 alive
        _join(port, "healthy-node")
        time.sleep(0.7)   # wait several tick cycles
        try:
            assert events == [], f"Unexpected events: {events}"
        finally:
            m.stop()

    def test_autoscaler_with_scheduler_reads_heat(self):
        events = []
        scheduler = ClusterScheduler(cooldown_sec=0.01)
        a = ClusterAutoScaler(
            min_nodes=1, max_nodes=5,
            scale_up_heat_threshold=0.01,   # very sensitive — any heat triggers
            scale_down_heat_threshold=0.005, # must be < scale_up threshold
            check_interval_sec=0.15,
            scale_up_cooldown_sec=0,
            scale_down_cooldown_sec=0,
            on_scale_up=lambda d: events.append(d),
        )
        m = _make_master(autoscaler=a, scheduler=scheduler)
        try:
            _join(m.port, "hot-node")
            # Force a pick so heat builds up
            scheduler.pick(m.nodes())
            time.sleep(0.5)
            assert len(events) >= 1
        finally:
            m.stop()

    def test_autoscaler_stops_cleanly(self):
        a = _make_autoscaler(check_interval_sec=60)
        m = _make_master(autoscaler=a)
        try:
            assert a._running is True
        finally:
            m.stop()
        assert a._running is False