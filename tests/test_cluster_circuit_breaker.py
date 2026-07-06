"""
Tests for ClusterCircuitBreaker (huddle_cluster_pkg.cluster_circuit_breaker).
"""

import json
import time
import urllib.error
import urllib.request

import pytest

from huddle_cluster_pkg.cluster_master          import MasterNode
from huddle_cluster_pkg.cluster_scheduler       import ClusterScheduler
from huddle_cluster_pkg.cluster_circuit_breaker import (
    ClusterCircuitBreaker, CLOSED, OPEN, HALF_OPEN,
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


def _join(port, node_id, error_rate=None, p=9800):
    return _post(port, "/nodes/join", {
        "node_id": node_id, "address": "127.0.0.1", "port": p,
    })


def _heartbeat_with_metrics(port, node_id, error_rate):
    return _post(port, f"/nodes/{node_id}/heartbeat",
                  {"metrics": {"error_rate": error_rate}})


def _make_breaker(**kwargs) -> ClusterCircuitBreaker:
    defaults = dict(
        trip_threshold=0.5,
        reset_timeout_sec=60,     # long — prevent auto half-open in tests
        check_interval_sec=0.1,
    )
    defaults.update(kwargs)
    return ClusterCircuitBreaker(**defaults)


def _make_master(breaker=None, scheduler=None, timeout=60):
    port = _free_port()
    m = MasterNode(
        host="127.0.0.1", port=port,
        heartbeat_timeout_sec=timeout,
        circuit_breaker=breaker,
        scheduler=scheduler,
    )
    m.start()
    time.sleep(0.1)
    return m



# Unit tests: constructor & validation


class TestCircuitBreakerInit:
    def test_valid_defaults(self):
        b = ClusterCircuitBreaker()
        assert b.trip_threshold == 0.5
        assert b.reset_timeout_sec == 30.0

    def test_zero_threshold_raises(self):
        with pytest.raises(ValueError):
            ClusterCircuitBreaker(trip_threshold=0.0)

    def test_above_one_threshold_raises(self):
        with pytest.raises(ValueError):
            ClusterCircuitBreaker(trip_threshold=1.1)

    def test_initially_closed_for_unknown_node(self):
        b = ClusterCircuitBreaker()
        assert b.is_open("unknown-node") is False

    def test_all_states_empty_initially(self):
        b = ClusterCircuitBreaker()
        assert b.all_states() == []



# Unit tests: evaluate() logic via _evaluate() and direct state mutation


class TestBreakerEvaluation:

    def _make_with_master(self, trip=0.5, reset=60, interval=0.1):
        b = _make_breaker(trip_threshold=trip,
                          reset_timeout_sec=reset,
                          check_interval_sec=interval)
        m = _make_master(breaker=b)
        return b, m

    def test_trip_when_error_rate_exceeds_threshold(self):
        b, m = self._make_with_master()
        try:
            _join(m.port, "bad-node")
            _heartbeat_with_metrics(m.port, "bad-node", error_rate=0.8)
            time.sleep(0.4)
            assert b.is_open("bad-node") is True
            assert b.state_for("bad-node")["state"] == OPEN
        finally:
            m.stop()

    def test_no_trip_when_error_rate_below_threshold(self):
        b, m = self._make_with_master()
        try:
            _join(m.port, "good-node")
            _heartbeat_with_metrics(m.port, "good-node", error_rate=0.2)
            time.sleep(0.4)
            assert b.is_open("good-node") is False
        finally:
            m.stop()

    def test_no_trip_when_no_error_rate_metric(self):
        b, m = self._make_with_master()
        try:
            _join(m.port, "no-metrics-node")
            _post(m.port, "/nodes/no-metrics-node/heartbeat", {})
            time.sleep(0.4)
            assert b.is_open("no-metrics-node") is False
        finally:
            m.stop()

    def test_trip_callback_fires(self):
        tripped = []
        b = _make_breaker(
            trip_threshold=0.5,
            on_trip=lambda nid, er: tripped.append((nid, er)),
        )
        m = _make_master(breaker=b)
        try:
            _join(m.port, "cb-node")
            _heartbeat_with_metrics(m.port, "cb-node", error_rate=0.9)
            time.sleep(0.4)
            assert len(tripped) == 1
            assert tripped[0][0] == "cb-node"
        finally:
            m.stop()

    def test_auto_reset_when_error_rate_recovers(self):
        reset_events = []
        b = _make_breaker(
            trip_threshold=0.5,
            on_reset=lambda nid: reset_events.append(nid),
        )
        m = _make_master(breaker=b)
        try:
            _join(m.port, "recover-node")
            _heartbeat_with_metrics(m.port, "recover-node", error_rate=0.9)
            time.sleep(0.4)   # trip
            assert b.is_open("recover-node") is True
            _heartbeat_with_metrics(m.port, "recover-node", error_rate=0.1)
            time.sleep(0.4)   # recover
            assert b.is_open("recover-node") is False
            assert "recover-node" in reset_events
        finally:
            m.stop()

    def test_half_open_after_reset_timeout(self):
        b = _make_breaker(trip_threshold=0.5, reset_timeout_sec=0.3)
        m = _make_master(breaker=b)
        try:
            _join(m.port, "halfopen-node")
            _heartbeat_with_metrics(m.port, "halfopen-node", error_rate=0.9)
            time.sleep(0.4)   # trip
            time.sleep(0.5)   # wait for reset timeout → half_open
            state = b.state_for("halfopen-node")
            assert state["state"] in (HALF_OPEN, CLOSED)
        finally:
            m.stop()

    def test_trip_count_increments(self):
        b, m = self._make_with_master()
        try:
            _join(m.port, "count-node")
            _heartbeat_with_metrics(m.port, "count-node", error_rate=0.9)
            time.sleep(0.4)
            assert b.state_for("count-node")["trip_count"] >= 1
        finally:
            m.stop()



# Unit tests: manual reset


class TestManualReset:
    def test_manual_reset_returns_true(self):
        b = _make_breaker()
        m = _make_master(breaker=b)
        try:
            _join(m.port, "reset-me")
            _heartbeat_with_metrics(m.port, "reset-me", error_rate=0.9)
            time.sleep(0.4)
            assert b.is_open("reset-me") is True
            assert b.reset("reset-me") is True
            assert b.is_open("reset-me") is False
        finally:
            m.stop()

    def test_manual_reset_fires_callback(self):
        events = []
        b = _make_breaker(on_reset=lambda nid: events.append(nid))
        m = _make_master(breaker=b)
        try:
            _join(m.port, "cb-reset")
            _heartbeat_with_metrics(m.port, "cb-reset", error_rate=0.9)
            time.sleep(0.4)
            b.reset("cb-reset")
            assert "cb-reset" in events
        finally:
            m.stop()

    def test_reset_unknown_node_returns_false(self):
        b = ClusterCircuitBreaker()
        assert b.reset("nobody") is False

    def test_reset_already_closed_returns_true(self):
        b = _make_breaker()
        m = _make_master(breaker=b)
        try:
            _join(m.port, "already-ok")
            _heartbeat_with_metrics(m.port, "already-ok", error_rate=0.1)
            time.sleep(0.4)
            assert b.reset("already-ok") is True   # already CLOSED
        finally:
            m.stop()



# Integration tests: scheduler exclusion


class TestSchedulerExclusion:
    def test_tripped_node_excluded_from_pick(self):
        breaker   = _make_breaker(trip_threshold=0.5)
        scheduler = ClusterScheduler(circuit_breaker=breaker)
        m = _make_master(breaker=breaker, scheduler=scheduler)
        try:
            _join(m.port, "healthy", p=9810)
            _join(m.port, "sick", p=9811)
            _heartbeat_with_metrics(m.port, "sick", error_rate=0.9)
            time.sleep(0.4)   # breaker trips for "sick"
            picks = {scheduler.pick(m.nodes())["node_id"] for _ in range(10)}
            assert "sick" not in picks
            assert "healthy" in picks
        finally:
            m.stop()

    def test_no_eligible_when_all_tripped_returns_none(self):
        breaker   = _make_breaker(trip_threshold=0.5)
        scheduler = ClusterScheduler(circuit_breaker=breaker)
        m = _make_master(breaker=breaker, scheduler=scheduler)
        try:
            _join(m.port, "all-sick", p=9812)
            _heartbeat_with_metrics(m.port, "all-sick", error_rate=0.9)
            time.sleep(0.4)
            result = scheduler.pick(m.nodes())
            assert result is None
        finally:
            m.stop()

    def test_scheduler_stats_shows_circuit_breaker_enabled(self):
        breaker   = ClusterCircuitBreaker()
        scheduler = ClusterScheduler(circuit_breaker=breaker)
        stats = scheduler.scheduler_stats()
        assert stats["circuit_breaker"] == "enabled"

    def test_scheduler_without_breaker_shows_disabled(self):
        scheduler = ClusterScheduler()
        stats = scheduler.scheduler_stats()
        assert stats["circuit_breaker"] == "disabled"



# Integration tests: HTTP endpoints


class TestCircuitBreakerHttp:
    def test_breakers_503_when_disabled(self):
        m = _make_master()
        try:
            with pytest.raises(urllib.error.HTTPError) as exc:
                _get(m.port, "/breakers")
            assert exc.value.code == 503
        finally:
            m.stop()

    def test_master_status_reports_breaker_enabled(self):
        b = _make_breaker()
        m = _make_master(breaker=b)
        try:
            status = _get(m.port, "/status")
            assert status["circuit_breaker"] == "enabled"
        finally:
            m.stop()

    def test_master_status_reports_breaker_disabled(self):
        m = _make_master()
        try:
            status = _get(m.port, "/status")
            assert status["circuit_breaker"] == "disabled"
        finally:
            m.stop()

    def test_breakers_summary_endpoint(self):
        b = _make_breaker()
        m = _make_master(breaker=b)
        try:
            data = _get(m.port, "/breakers")
            assert "trip_threshold" in data
            assert "open_breakers" in data
            assert "states" in data
        finally:
            m.stop()

    def test_breakers_node_endpoint(self):
        b = _make_breaker()
        m = _make_master(breaker=b)
        try:
            _join(m.port, "n1")
            _heartbeat_with_metrics(m.port, "n1", error_rate=0.9)
            time.sleep(0.4)
            data = _get(m.port, "/breakers/n1")
            assert data["node_id"] == "n1"
            assert data["state"] == OPEN
        finally:
            m.stop()

    def test_breakers_node_404_for_unknown(self):
        b = _make_breaker()
        m = _make_master(breaker=b)
        try:
            with pytest.raises(urllib.error.HTTPError) as exc:
                _get(m.port, "/breakers/nobody")
            assert exc.value.code == 404
        finally:
            m.stop()

    def test_reset_endpoint_resets_breaker(self):
        b = _make_breaker()
        m = _make_master(breaker=b)
        try:
            _join(m.port, "reset-via-http")
            _heartbeat_with_metrics(m.port, "reset-via-http", error_rate=0.9)
            time.sleep(0.4)
            assert b.is_open("reset-via-http") is True
            r = _post(m.port, "/breakers/reset-via-http/reset")
            assert r["ok"] is True
            assert b.is_open("reset-via-http") is False
        finally:
            m.stop()

    def test_reset_endpoint_404_for_unknown(self):
        b = _make_breaker()
        m = _make_master(breaker=b)
        try:
            r = _post(m.port, "/breakers/nobody/reset")
            assert r["ok"] is False
        finally:
            m.stop()

    def test_breakers_summary_shows_open_count(self):
        b = _make_breaker()
        m = _make_master(breaker=b)
        try:
            _join(m.port, "open1", p=9820)
            _join(m.port, "open2", p=9821)
            for nid in ("open1", "open2"):
                _heartbeat_with_metrics(m.port, nid, error_rate=0.9)
            time.sleep(0.4)
            data = _get(m.port, "/breakers")
            assert data["open_breakers"] == 2
        finally:
            m.stop()