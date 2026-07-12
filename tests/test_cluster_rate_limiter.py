"""
Tests for ClusterRateLimiter (huddle_cluster_pkg.cluster_rate_limiter).
"""

import json
import time
import urllib.error
import urllib.request

import pytest

from huddle_cluster_pkg.cluster_master      import MasterNode
from huddle_cluster_pkg.cluster_scheduler   import ClusterScheduler
from huddle_cluster_pkg.cluster_rate_limiter import ClusterRateLimiter, TokenBucket



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


def _join(port, node_id, p=9870):
    return _post(port, "/nodes/join", {
        "node_id": node_id, "address": "127.0.0.1", "port": p,
    })


def _make_master(limiter=None, scheduler=None, timeout=60):
    port = _free_port()
    m = MasterNode(
        host="127.0.0.1", port=port,
        heartbeat_timeout_sec=timeout,
        rate_limiter=limiter,
        scheduler=scheduler,
    )
    m.start()
    time.sleep(0.1)
    return m



# Unit tests: TokenBucket


class TestTokenBucket:
    def test_starts_full(self):
        b = TokenBucket(capacity=10, refill_rate=1)
        assert b.consume(1)   # should succeed immediately

    def test_consume_reduces_tokens(self):
        b = TokenBucket(capacity=3, refill_rate=0.01)
        b.consume(1); b.consume(1); b.consume(1)
        assert not b.consume(1)   # bucket now empty

    def test_refill_over_time(self):
        b = TokenBucket(capacity=10, refill_rate=100)
        # Drain completely
        for _ in range(10):
            b.consume(1)
        assert not b.consume(1)
        time.sleep(0.15)    # 100 tokens/s → ~15 tokens added
        assert b.consume(1)

    def test_fill_resets_to_capacity(self):
        b = TokenBucket(capacity=5, refill_rate=0.01)
        for _ in range(5):
            b.consume(1)
        b.fill()
        for _ in range(5):
            assert b.consume(1)

    def test_tokens_capped_at_capacity(self):
        b = TokenBucket(capacity=5, refill_rate=100)
        time.sleep(0.2)   # would add 20 tokens at rate 100, but capped at 5
        count = sum(1 for _ in range(10) if b.consume(1))
        assert count == 5

    def test_to_dict_shape(self):
        b = TokenBucket(capacity=10, refill_rate=5)
        d = b.to_dict("n1")
        assert d["node_id"] == "n1"
        assert d["capacity"] == 10
        assert "tokens" in d
        assert "rate_limited" in d
        assert "consumed_total" in d

    def test_consumed_total_tracks(self):
        b = TokenBucket(capacity=10, refill_rate=1)
        b.consume(1); b.consume(1)
        assert b.to_dict("n")["consumed_total"] == 2



# Unit tests: ClusterRateLimiter


class TestRateLimiterInit:
    def test_valid_defaults(self):
        l = ClusterRateLimiter()
        assert l.capacity == 100.0
        assert l.refill_rate == 50.0

    def test_zero_capacity_raises(self):
        with pytest.raises(ValueError):
            ClusterRateLimiter(capacity=0)

    def test_zero_refill_raises(self):
        with pytest.raises(ValueError):
            ClusterRateLimiter(refill_rate=0)

    def test_all_buckets_empty_initially(self):
        l = ClusterRateLimiter()
        assert l.all_buckets() == []

    def test_unknown_node_not_rate_limited(self):
        l = ClusterRateLimiter()
        assert l.is_rate_limited("nobody") is False


class TestRateLimiterConsume:
    def test_consume_returns_true_when_tokens_available(self):
        l = ClusterRateLimiter(capacity=10, refill_rate=1)
        assert l.consume("n1") is True

    def test_consume_returns_false_when_empty(self):
        l = ClusterRateLimiter(capacity=2, refill_rate=0.01)
        l.consume("n1"); l.consume("n1")
        assert l.consume("n1") is False

    def test_is_rate_limited_true_when_empty(self):
        l = ClusterRateLimiter(capacity=1, refill_rate=0.01)
        l.consume("n1")
        assert l.is_rate_limited("n1") is True

    def test_is_rate_limited_false_when_tokens_available(self):
        l = ClusterRateLimiter(capacity=10, refill_rate=1)
        assert l.is_rate_limited("n1") is False

    def test_callback_fires_when_rate_limited(self):
        events = []
        l = ClusterRateLimiter(
            capacity=1, refill_rate=0.01,
            on_rate_limited=lambda nid: events.append(nid),
        )
        l.consume("n1")      # drains bucket
        l.consume("n1")      # fires callback
        assert "n1" in events

    def test_buckets_are_per_node_independent(self):
        l = ClusterRateLimiter(capacity=1, refill_rate=0.01)
        l.consume("n1")   # drain n1
        assert l.is_rate_limited("n1") is True
        assert l.is_rate_limited("n2") is False   # n2 still full

    def test_reset_refills_to_capacity(self):
        l = ClusterRateLimiter(capacity=2, refill_rate=0.01)
        l.consume("n1"); l.consume("n1")
        assert l.is_rate_limited("n1") is True
        l.reset("n1")
        assert l.is_rate_limited("n1") is False

    def test_reset_unknown_node_returns_false(self):
        l = ClusterRateLimiter()
        assert l.reset("nobody") is False

    def test_summary_shape(self):
        l = ClusterRateLimiter(capacity=5, refill_rate=1)
        l.consume("n1")
        s = l.summary()
        assert s["capacity"] == 5
        assert s["refill_rate"] == 1
        assert "rate_limited_nodes" in s
        assert len(s["buckets"]) == 1



# Integration tests: scheduler exclusion


class TestSchedulerExclusion:
    def test_rate_limited_node_excluded_from_pick(self):
        limiter   = ClusterRateLimiter(capacity=20, refill_rate=0.01)
        scheduler = ClusterScheduler(rate_limiter=limiter)
        m = _make_master(limiter=limiter, scheduler=scheduler)
        try:
            _join(m.port, "fast", p=9875)
            _join(m.port, "slow", p=9876)
            # Drain "slow"s bucket completely
            for _ in range(25):
                limiter.consume("slow")
            assert limiter.is_rate_limited("slow") is True
            picks = [scheduler.pick(m.nodes())["node_id"] for _ in range(5)]
            assert all(p == "fast" for p in picks)
        finally:
            m.stop()

    def test_none_available_when_all_rate_limited(self):
        limiter   = ClusterRateLimiter(capacity=1, refill_rate=0.01)
        scheduler = ClusterScheduler(rate_limiter=limiter)
        m = _make_master(limiter=limiter, scheduler=scheduler)
        try:
            _join(m.port, "rl-only", p=9877)
            limiter.consume("rl-only"); limiter.consume("rl-only")
            result = scheduler.pick(m.nodes())
            assert result is None
        finally:
            m.stop()

    def test_pick_consumes_token(self):
        limiter   = ClusterRateLimiter(capacity=3, refill_rate=0.01)
        scheduler = ClusterScheduler(rate_limiter=limiter)
        m = _make_master(limiter=limiter, scheduler=scheduler)
        try:
            _join(m.port, "token-node", p=9878)
            for _ in range(3):
                scheduler.pick(m.nodes())
            # Bucket should now be empty
            assert limiter.is_rate_limited("token-node") is True
        finally:
            m.stop()

    def test_scheduler_stats_shows_rate_limiter_enabled(self):
        limiter   = ClusterRateLimiter()
        scheduler = ClusterScheduler(rate_limiter=limiter)
        assert scheduler.scheduler_stats()["rate_limiter"] == "enabled"

    def test_scheduler_stats_shows_rate_limiter_disabled(self):
        scheduler = ClusterScheduler()
        assert scheduler.scheduler_stats()["rate_limiter"] == "disabled"



# Integration tests: HTTP endpoints


class TestRateLimiterHttp:
    def test_ratelimits_503_when_disabled(self):
        m = _make_master()
        try:
            with pytest.raises(urllib.error.HTTPError) as exc:
                _get(m.port, "/ratelimits")
            assert exc.value.code == 503
        finally:
            m.stop()

    def test_master_status_reports_rate_limiter_enabled(self):
        l = ClusterRateLimiter()
        m = _make_master(limiter=l)
        try:
            assert _get(m.port, "/status")["rate_limiter"] == "enabled"
        finally:
            m.stop()

    def test_master_status_reports_rate_limiter_disabled(self):
        m = _make_master()
        try:
            assert _get(m.port, "/status")["rate_limiter"] == "disabled"
        finally:
            m.stop()

    def test_ratelimits_summary_endpoint(self):
        l = ClusterRateLimiter(capacity=50, refill_rate=10)
        m = _make_master(limiter=l)
        try:
            data = _get(m.port, "/ratelimits")
            assert data["capacity"] == 50
            assert data["refill_rate"] == 10
            assert "rate_limited_nodes" in data
        finally:
            m.stop()

    def test_ratelimits_node_endpoint(self):
        l = ClusterRateLimiter(capacity=5, refill_rate=0.01)
        m = _make_master(limiter=l)
        try:
            _join(m.port, "rl-n1")
            l.consume("rl-n1")
            data = _get(m.port, "/ratelimits/rl-n1")
            assert data["node_id"] == "rl-n1"
            assert "tokens" in data
        finally:
            m.stop()

    def test_ratelimits_node_404_for_unknown(self):
        l = ClusterRateLimiter()
        m = _make_master(limiter=l)
        try:
            with pytest.raises(urllib.error.HTTPError) as exc:
                _get(m.port, "/ratelimits/nobody")
            assert exc.value.code == 404
        finally:
            m.stop()

    def test_reset_endpoint_refills_bucket(self):
        l = ClusterRateLimiter(capacity=1, refill_rate=0.01)
        m = _make_master(limiter=l)
        try:
            _join(m.port, "rl-reset")
            l.consume("rl-reset"); l.consume("rl-reset")
            assert l.is_rate_limited("rl-reset") is True
            r = _post(m.port, "/ratelimits/rl-reset/reset")
            assert r["ok"] is True
            assert l.is_rate_limited("rl-reset") is False
        finally:
            m.stop()

    def test_reset_endpoint_404_for_unknown(self):
        l = ClusterRateLimiter()
        m = _make_master(limiter=l)
        try:
            r = _post(m.port, "/ratelimits/nobody/reset")
            assert r["ok"] is False
        finally:
            m.stop()

    def test_ratelimits_summary_shows_rate_limited_count(self):
        l = ClusterRateLimiter(capacity=1, refill_rate=0.01)
        m = _make_master(limiter=l)
        try:
            _join(m.port, "rl-a", p=9880)
            _join(m.port, "rl-b", p=9881)
            l.consume("rl-a"); l.consume("rl-a")   # drain a
            data = _get(m.port, "/ratelimits")
            assert data["rate_limited_nodes"] == 1
        finally:
            m.stop()