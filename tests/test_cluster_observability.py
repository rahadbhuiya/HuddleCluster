"""
Tests for ClusterObservability (huddle_cluster_pkg.cluster_observability).
"""

import json
import logging
import time
import urllib.error
import urllib.request

import pytest

from huddle_cluster_pkg.cluster_master import MasterNode
from huddle_cluster_pkg.cluster_observability import (
    ClusterObservability,
    JsonLogFormatter,
    LogEvent,
    current_trace_id,
    new_trace_id,
)



# Helpers


def _free_port():
    import socket
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close()
    return p


def _get(port, path, headers=None):
    url = f"http://127.0.0.1:{port}/v1{path}"
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=3) as r:
        return json.loads(r.read()), dict(r.headers)


def _get_raw(port, path, headers=None):
    url = f"http://127.0.0.1:{port}/v1{path}"
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=3) as r:
            return r.status, dict(r.headers)
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers)


def _post(port, path, payload=None):
    data = json.dumps(payload or {}).encode()
    req  = urllib.request.Request(
        f"http://127.0.0.1:{port}/v1{path}", data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=3) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return json.loads(e.read())



# Unit tests — trace context


class TestTraceContext:
    def test_new_trace_id_unique(self):
        ids = {new_trace_id() for _ in range(50)}
        assert len(ids) == 50

    def test_current_trace_id_default_none(self):
        assert current_trace_id() is None

    def test_start_trace_mints_when_no_incoming(self):
        obs = ClusterObservability(json_logs=False)
        trace_id = obs.start_trace(None)
        assert trace_id
        assert current_trace_id() == trace_id
        obs.end_trace()

    def test_start_trace_propagates_incoming(self):
        obs = ClusterObservability(json_logs=False)
        trace_id = obs.start_trace("upstream-trace-123")
        assert trace_id == "upstream-trace-123"
        assert current_trace_id() == "upstream-trace-123"
        obs.end_trace()

    def test_start_trace_ignores_blank_incoming(self):
        obs = ClusterObservability(json_logs=False)
        trace_id = obs.start_trace("   ")
        assert trace_id != "   "
        assert trace_id
        obs.end_trace()

    def test_end_trace_clears_context(self):
        obs = ClusterObservability(json_logs=False)
        obs.start_trace(None)
        obs.end_trace()
        assert current_trace_id() is None


# Unit tests — event buffer


class TestEventBuffer:
    def test_record_event_appends(self):
        obs = ClusterObservability(json_logs=False)
        obs.record_event("node_join", node_id="web-1", region="us-east")
        events = obs.events(limit=10)
        assert len(events) == 1
        assert events[0]["event"] == "node_join"
        assert events[0]["node_id"] == "web-1"
        assert events[0]["fields"] == {"region": "us-east"}

    def test_record_event_uses_current_trace_by_default(self):
        obs = ClusterObservability(json_logs=False)
        obs.start_trace("trace-abc")
        obs.record_event("heartbeat", node_id="web-2")
        obs.end_trace()
        events = obs.events(trace_id="trace-abc")
        assert len(events) == 1
        assert events[0]["trace_id"] == "trace-abc"

    def test_record_event_explicit_trace_overrides_context(self):
        obs = ClusterObservability(json_logs=False)
        evt = obs.record_event("custom", trace_id="explicit-trace")
        assert evt.trace_id == "explicit-trace"

    def test_events_filter_by_event_name(self):
        obs = ClusterObservability(json_logs=False)
        obs.record_event("a")
        obs.record_event("b")
        obs.record_event("a")
        assert len(obs.events(event="a", limit=10)) == 2

    def test_events_filter_by_node_id(self):
        obs = ClusterObservability(json_logs=False)
        obs.record_event("x", node_id="n1")
        obs.record_event("x", node_id="n2")
        assert len(obs.events(node_id="n1", limit=10)) == 1

    def test_events_respects_limit(self):
        obs = ClusterObservability(json_logs=False)
        for i in range(10):
            obs.record_event(f"e{i}")
        assert len(obs.events(limit=3)) == 3

    def test_events_limit_zero_returns_empty(self):
        obs = ClusterObservability(json_logs=False)
        obs.record_event("e")
        assert obs.events(limit=0) == []

    def test_events_newest_last(self):
        obs = ClusterObservability(json_logs=False)
        obs.record_event("first")
        obs.record_event("second")
        events = obs.events(limit=10)
        assert events[-1]["event"] == "second"

    def test_buffer_size_capped(self):
        obs = ClusterObservability(json_logs=False, buffer_size=5)
        for i in range(20):
            obs.record_event(f"e{i}")
        assert len(obs.events(limit=100)) == 5
        # oldest events evicted — most recent 5 remain
        names = [e["event"] for e in obs.events(limit=100)]
        assert names == [f"e{i}" for i in range(15, 20)]

    def test_buffer_size_must_be_positive(self):
        with pytest.raises(ValueError):
            ClusterObservability(buffer_size=0)

    def test_summary_reports_counts(self):
        obs = ClusterObservability(json_logs=False)
        obs.record_event("a")
        obs.record_event("b")
        summary = obs.summary()
        assert summary["events_recorded"] == 2
        assert summary["events_buffered"] == 2
        assert len(summary["recent_events"]) == 2
        assert summary["json_logs"] is False


# Unit tests — JSON log formatter


class TestJsonLogFormatter:
    def _make_record(self, **extra):
        record = logging.LogRecord(
            name="huddle_cluster_pkg.test", level=logging.INFO,
            pathname=__file__, lineno=1, msg="hello", args=(), exc_info=None,
        )
        for k, v in extra.items():
            setattr(record, k, v)
        return record

    def test_format_produces_valid_json(self):
        fmt = JsonLogFormatter(service_name="svc")
        record = self._make_record()
        parsed = json.loads(fmt.format(record))
        assert parsed["message"] == "hello"
        assert parsed["level"] == "info"
        assert parsed["service"] == "svc"

    def test_format_includes_trace_id_when_present(self):
        fmt = JsonLogFormatter()
        record = self._make_record(trace_id="tid-1")
        parsed = json.loads(fmt.format(record))
        assert parsed["trace_id"] == "tid-1"

    def test_format_omits_trace_id_when_absent(self):
        fmt = JsonLogFormatter()
        record = self._make_record()
        parsed = json.loads(fmt.format(record))
        assert "trace_id" not in parsed

    def test_format_includes_fields(self):
        fmt = JsonLogFormatter()
        record = self._make_record(fields={"status": 200})
        parsed = json.loads(fmt.format(record))
        assert parsed["fields"] == {"status": 200}


class TestConfigureLogging:
    def test_configure_logging_is_idempotent(self):
        name = "huddle_test_logger_idempotent"
        obs = ClusterObservability(json_logs=True, logger_name=name)
        obs.configure_logging(name)
        target = logging.getLogger(name)
        handler_count = len(target.handlers)
        obs.configure_logging(name)   # second call should no-op
        assert len(target.handlers) == handler_count

    def test_configure_logging_installs_json_formatter(self):
        name = "huddle_test_logger_formatter"
        obs = ClusterObservability(json_logs=True, logger_name=name)
        obs.configure_logging(name)
        target = logging.getLogger(name)
        assert any(isinstance(h.formatter, JsonLogFormatter) for h in target.handlers)


# HTTP integration


@pytest.fixture
def master_with_obs():
    port = _free_port()
    obs = ClusterObservability(json_logs=False, buffer_size=50)
    master = MasterNode(port=port, observability=obs)
    master.start()
    time.sleep(0.1)
    yield master, port, obs
    master.stop()


@pytest.fixture
def master_without_obs():
    port = _free_port()
    master = MasterNode(port=port)
    master.start()
    time.sleep(0.1)
    yield master, port
    master.stop()


class TestHTTPIntegration:
    def test_status_disabled_without_observability(self, master_without_obs):
        _, port = master_without_obs
        code, _ = _get_raw(port, "/observability/status")
        assert code == 503

    def test_status_enabled(self, master_with_obs):
        _, port, _ = master_with_obs
        body, _ = _get(port, "/observability/status")
        assert body["json_logs"] is False
        assert "events_recorded" in body

    def test_logs_endpoint_enabled(self, master_with_obs):
        _, port, _ = master_with_obs
        body, _ = _get(port, "/observability/logs")
        assert "events" in body
        assert isinstance(body["events"], list)

    def test_logs_endpoint_disabled_without_observability(self, master_without_obs):
        _, port = master_without_obs
        code, _ = _get_raw(port, "/observability/logs")
        assert code == 503

    def test_logs_limit_validation(self, master_with_obs):
        _, port, _ = master_with_obs
        code, _ = _get_raw(port, "/observability/logs?limit=-1")
        assert code == 400

    def test_response_carries_trace_id_header(self, master_with_obs):
        _, port, _ = master_with_obs
        _, headers = _get(port, "/status")
        assert "X-Trace-Id" in headers
        assert len(headers["X-Trace-Id"]) > 0

    def test_incoming_trace_id_is_propagated(self, master_with_obs):
        _, port, _ = master_with_obs
        _, headers = _get(port, "/status", headers={"X-Trace-Id": "my-fixed-trace"})
        assert headers["X-Trace-Id"] == "my-fixed-trace"

    def test_no_trace_header_without_observability(self, master_without_obs):
        _, port = master_without_obs
        _, headers = _get(port, "/status")
        assert "X-Trace-Id" not in headers

    def test_requests_are_recorded_as_events(self, master_with_obs):
        master, port, obs = master_with_obs
        _get(port, "/status")
        events = obs.events(event="http_request", limit=50)
        assert len(events) >= 1
        assert events[-1]["fields"]["path"] == "/v1/status"

    def test_trace_id_correlates_request_and_buffered_event(self, master_with_obs):
        master, port, obs = master_with_obs
        _, headers = _get(port, "/status", headers={"X-Trace-Id": "corr-trace-99"})
        events = obs.events(trace_id="corr-trace-99", limit=50)
        assert len(events) >= 1

    def test_status_reports_observability_summary(self, master_with_obs):
        master, port, obs = master_with_obs
        _get(port, "/status")
        body, _ = _get(port, "/status")
        assert isinstance(body["observability"], dict)
        assert "events_recorded" in body["observability"]

    def test_status_reports_disabled_without_observability(self, master_without_obs):
        master, port = master_without_obs
        body, _ = _get(port, "/status")
        assert body["observability"] == "disabled"

    def test_post_request_also_traced(self, master_with_obs):
        master, port, obs = master_with_obs
        _post(port, "/nodes/join", {
            "node_id": "obs-node-1", "address": "127.0.0.1", "port": 9911,
        })
        events = obs.events(event="http_request", limit=50)
        paths = [e["fields"]["path"] for e in events]
        assert "/v1/nodes/join" in paths