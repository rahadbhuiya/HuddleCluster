"""
Tests for MultiRegionManager (huddle_cluster_pkg.cluster_multi_region).
"""

import json
import time
import urllib.error
import urllib.request

import pytest

from huddle_cluster_pkg.cluster_master       import MasterNode
from huddle_cluster_pkg.cluster_scheduler    import ClusterScheduler
from huddle_cluster_pkg.cluster_multi_region import MultiRegionManager



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


def _join(port, node_id, region=None, p=9990):
    meta = {}
    if region:
        meta["region"] = region
    return _post(port, "/nodes/join", {
        "node_id": node_id, "address": "127.0.0.1", "port": p,
        "metadata": meta,
    })


def _make_master(mr=None, timeout=60, scheduler=None):
    port = _free_port()
    m = MasterNode(host="127.0.0.1", port=port,
                   heartbeat_timeout_sec=timeout,
                   multi_region=mr,
                   scheduler=scheduler)
    m.start()
    time.sleep(0.1)
    return m


def _make_mr(**kwargs) -> MultiRegionManager:
    defaults = dict(refresh_interval_sec=0.2)
    defaults.update(kwargs)
    return MultiRegionManager(**defaults)


# Unit tests: MultiRegionManager API without master


class TestMultiRegionUnit:
    def test_announce_and_regions_list(self):
        mr = _make_mr()
        mr.announce("n1", "us-east")
        mr.announce("n2", "eu-west")
        assert "us-east" in mr.regions()
        assert "eu-west" in mr.regions()

    def test_announce_normalises_case(self):
        mr = _make_mr()
        mr.announce("n1", "US-East")
        assert "us-east" in mr.regions()

    def test_announce_dedupes(self):
        mr = _make_mr()
        mr.announce("n1", "us-east")
        mr.announce("n1", "us-east")
        with mr._lock:
            assert len(mr._registry["us-east"]) == 1

    def test_alive_nodes_for_unknown_region_empty(self):
        mr = _make_mr()
        assert mr.alive_nodes_for_region("nowhere") == []

    def test_summary_includes_region(self):
        mr = _make_mr()
        mr.announce("n1", "ap-south")
        summary = mr.summary()
        assert "ap-south" in summary["regions"]

    def test_preferred_nodes_without_master_returns_empty(self):
        mr = _make_mr()
        assert mr.preferred_nodes("us-east") == []



# Integration tests via MasterNode


class TestMultiRegionHttp:

    def test_regions_503_when_disabled(self):
        m = _make_master()
        try:
            with pytest.raises(urllib.error.HTTPError) as exc:
                _get(m.port, "/regions")
            assert exc.value.code == 503
        finally:
            m.stop()

    def test_master_status_reports_multi_region_enabled(self):
        mr = _make_mr()
        m  = _make_master(mr=mr)
        try:
            status = _get(m.port, "/status")
            assert status["multi_region"] == "enabled"
        finally:
            m.stop()

    def test_master_status_reports_multi_region_disabled(self):
        m = _make_master()
        try:
            status = _get(m.port, "/status")
            assert status["multi_region"] == "disabled"
        finally:
            m.stop()

    def test_regions_empty_initially(self):
        mr = _make_mr()
        m  = _make_master(mr=mr)
        try:
            data = _get(m.port, "/regions")
            assert data["regions"] == {}
        finally:
            m.stop()

    def test_announce_and_lookup_via_rest(self):
        mr = _make_mr()
        m  = _make_master(mr=mr)
        try:
            _join(m.port, "us-1")
            r = _post(m.port, "/regions/announce",
                       {"node_id": "us-1", "region": "us-east"})
            assert r["ok"] is True
            data = _get(m.port, "/regions/us-east")
            assert data["region"] == "us-east"
            assert data["alive_count"] == 1
            assert data["nodes"][0]["node_id"] == "us-1"
        finally:
            m.stop()

    def test_metadata_region_picked_up_on_refresh(self):
        mr = _make_mr(refresh_interval_sec=0.2)
        m  = _make_master(mr=mr)
        try:
            _join(m.port, "eu-1", region="eu-west")
            time.sleep(0.6)
            data = _get(m.port, "/regions")
            assert "eu-west" in data["regions"]
        finally:
            m.stop()

    def test_dead_node_excluded_from_region_results(self):
        mr = _make_mr(refresh_interval_sec=0.2)
        m  = _make_master(mr=mr, timeout=0.3)
        try:
            _join(m.port, "dead-region-node")
            r = _post(m.port, "/regions/announce",
                       {"node_id": "dead-region-node", "region": "us-west"})
            assert r["ok"] is True
            time.sleep(0.6)   # node times out
            data = _get(m.port, "/regions/us-west")
            assert data["alive_count"] == 0
        finally:
            m.stop()

    def test_multiple_nodes_same_region(self):
        mr = _make_mr()
        m  = _make_master(mr=mr)
        try:
            for i in range(3):
                _join(m.port, f"multi-{i}", p=9991+i)
                _post(m.port, "/regions/announce",
                       {"node_id": f"multi-{i}", "region": "ap-south"})
            data = _get(m.port, "/regions/ap-south")
            assert data["alive_count"] == 3
        finally:
            m.stop()

    def test_on_region_up_callback_fires(self):
        events = []
        mr = _make_mr(
            refresh_interval_sec=0.2,
            on_region_up=lambda r, nodes: events.append(("up", r)),
        )
        m = _make_master(mr=mr)
        try:
            _join(m.port, "cb-node")
            _post(m.port, "/regions/announce",
                   {"node_id": "cb-node", "region": "sa-east"})
            time.sleep(0.6)
            assert ("up", "sa-east") in events
        finally:
            m.stop()

    def test_on_region_down_callback_fires(self):
        down_events = []
        mr = _make_mr(
            refresh_interval_sec=0.2,
            on_region_down=lambda r: down_events.append(r),
        )
        m = _make_master(mr=mr, timeout=0.3)
        try:
            _join(m.port, "dying-region-node")
            _post(m.port, "/regions/announce",
                   {"node_id": "dying-region-node", "region": "ca-central"})
            time.sleep(0.3)
            time.sleep(0.6)   # let the node die
            time.sleep(0.5)   # give refresh a cycle
            assert "ca-central" in down_events
        finally:
            m.stop()

    def test_announce_missing_node_id_returns_400(self):
        mr = _make_mr()
        m  = _make_master(mr=mr)
        try:
            r = _post(m.port, "/regions/announce", {"region": "us-east"})
            assert r["ok"] is False
        finally:
            m.stop()

    def test_regions_summary_includes_counts(self):
        mr = _make_mr()
        m  = _make_master(mr=mr)
        try:
            _join(m.port, "sum-node")
            _post(m.port, "/regions/announce",
                   {"node_id": "sum-node", "region": "eu-central"})
            data = _get(m.port, "/regions")
            assert data["regions"]["eu-central"]["alive_count"] == 1
        finally:
            m.stop()



# Integration tests: region-aware scheduling


class TestRegionAwareScheduling:

    def test_pick_prefers_matching_region(self):
        scheduler = ClusterScheduler()
        m = _make_master(scheduler=scheduler)
        try:
            _join(m.port, "us-node", region="us-east", p=9995)
            _join(m.port, "eu-node", region="eu-west", p=9996)
            picks = {scheduler.pick(m.nodes(), preferred_region="us-east")["node_id"]
                     for _ in range(10)}
            assert picks == {"us-node"}
        finally:
            m.stop()

    def test_pick_falls_back_when_region_empty(self):
        scheduler = ClusterScheduler()
        m = _make_master(scheduler=scheduler)
        try:
            _join(m.port, "only-eu", region="eu-west", p=9997)
            # No us-east nodes exist — should fall back to the eu node
            result = scheduler.pick(m.nodes(), preferred_region="us-east")
            assert result is not None
            assert result["node_id"] == "only-eu"
        finally:
            m.stop()

    def test_pick_without_preferred_region_uses_full_pool(self):
        scheduler = ClusterScheduler()
        m = _make_master(scheduler=scheduler)
        try:
            _join(m.port, "any-node", region="us-east", p=9998)
            result = scheduler.pick(m.nodes())
            assert result is not None
        finally:
            m.stop()

    def test_preferred_nodes_with_fallback(self):
        mr = _make_mr(preferred_region="us-east", fallback_to_global=True)
        m  = _make_master(mr=mr)
        try:
            _join(m.port, "fallback-node", p=9999)
            _post(m.port, "/regions/announce",
                   {"node_id": "fallback-node", "region": "eu-west"})
            nodes = mr.preferred_nodes()   # no region — uses preferred default
            assert len(nodes) >= 1   # falls back since no us-east nodes exist
        finally:
            m.stop()

    def test_preferred_nodes_no_fallback_returns_empty(self):
        mr = _make_mr(preferred_region="us-east", fallback_to_global=False)
        m  = _make_master(mr=mr)
        try:
            _join(m.port, "no-fallback-node", p=10000)
            _post(m.port, "/regions/announce",
                   {"node_id": "no-fallback-node", "region": "eu-west"})
            nodes = mr.preferred_nodes()
            assert nodes == []
        finally:
            m.stop()