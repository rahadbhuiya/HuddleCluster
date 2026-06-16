"""
Tests for MasterNode (huddle_cluster_pkg.cluster_master).
"""

import json
import time
import urllib.error
import urllib.request

import pytest

from huddle_cluster_pkg.cluster_master import MasterNode, NodeRecord



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
    """POST and return the JSON body regardless of HTTP status code."""
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
        # Read the error body — server always returns JSON
        return json.loads(e.read())


def _delete(port, path):
    """DELETE and return the JSON body regardless of HTTP status code."""
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/v1{path}",
        method="DELETE",
    )
    try:
        with urllib.request.urlopen(req, timeout=3) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return json.loads(e.read())



# Fixtures


@pytest.fixture
def master():
    port = _free_port()
    m = MasterNode(host="127.0.0.1", port=port, heartbeat_timeout_sec=60)
    m.start()
    time.sleep(0.1)
    yield m
    m.stop()



# Tests


class TestHealth:
    def test_health_ok(self, master):
        resp = _get(master.port, "/health")
        assert resp["status"] == "ok"

    def test_status_structure(self, master):
        resp = _get(master.port, "/status")
        assert "master" in resp
        assert "total_nodes" in resp
        assert resp["total_nodes"] == 0

    def test_nodes_empty(self, master):
        resp = _get(master.port, "/nodes")
        assert resp["nodes"] == []


class TestNodeJoin:
    def test_join_success(self, master):
        resp = _post(master.port, "/nodes/join", {
            "node_id": "n1", "address": "127.0.0.1", "port": 9001,
        })
        assert resp["ok"] is True
        assert resp["action"] == "joined"
        assert resp["node_id"] == "n1"

    def test_join_appears_in_list(self, master):
        _post(master.port, "/nodes/join", {
            "node_id": "n2", "address": "127.0.0.1", "port": 9002,
        })
        nodes = _get(master.port, "/nodes")["nodes"]
        assert any(n["node_id"] == "n2" for n in nodes)

    def test_join_missing_node_id(self, master):
        resp = _post(master.port, "/nodes/join", {
            "address": "127.0.0.1", "port": 9003,
        })
        assert resp["ok"] is False
        assert "error" in resp

    def test_join_missing_port(self, master):
        resp = _post(master.port, "/nodes/join", {
            "node_id": "n3", "address": "127.0.0.1",
        })
        assert resp["ok"] is False

    def test_join_invalid_port(self, master):
        resp = _post(master.port, "/nodes/join", {
            "node_id": "n4", "address": "127.0.0.1", "port": 0,
        })
        assert resp["ok"] is False

    def test_join_port_out_of_range(self, master):
        resp = _post(master.port, "/nodes/join", {
            "node_id": "n5", "address": "127.0.0.1", "port": 99999,
        })
        assert resp["ok"] is False

    def test_rejoin_updates_record(self, master):
        _post(master.port, "/nodes/join", {
            "node_id": "n6", "address": "10.0.0.1", "port": 9006,
        })
        resp2 = _post(master.port, "/nodes/join", {
            "node_id": "n6", "address": "10.0.0.2", "port": 9006,
        })
        assert resp2["action"] == "re-joined"
        node = _get(master.port, "/nodes/n6")
        assert node["address"] == "10.0.0.2"

    def test_join_with_metadata(self, master):
        resp = _post(master.port, "/nodes/join", {
            "node_id": "n7", "address": "127.0.0.1", "port": 9007,
            "metadata": {"region": "eu-west", "role": "worker"},
        })
        assert resp["ok"] is True
        node = _get(master.port, "/nodes/n7")
        assert node["metadata"]["region"] == "eu-west"

    def test_join_returns_heartbeat_timeout(self, master):
        resp = _post(master.port, "/nodes/join", {
            "node_id": "n8", "address": "127.0.0.1", "port": 9008,
        })
        assert "heartbeat_timeout_sec" in resp


class TestHeartbeat:
    def test_heartbeat_ok(self, master):
        _post(master.port, "/nodes/join", {
            "node_id": "hb1", "address": "127.0.0.1", "port": 9010,
        })
        resp = _post(master.port, "/nodes/hb1/heartbeat", {})
        assert resp["ok"] is True
        assert resp["heartbeat"] == 1

    def test_heartbeat_increments(self, master):
        _post(master.port, "/nodes/join", {
            "node_id": "hb2", "address": "127.0.0.1", "port": 9011,
        })
        for _ in range(5):
            _post(master.port, "/nodes/hb2/heartbeat", {})
        node = _get(master.port, "/nodes/hb2")
        assert node["heartbeat_count"] == 5

    def test_heartbeat_unknown_node(self, master):
        resp = _post(master.port, "/nodes/unknown/heartbeat", {})
        assert resp["ok"] is False
        assert "error" in resp

    def test_heartbeat_stores_metrics(self, master):
        _post(master.port, "/nodes/join", {
            "node_id": "hb3", "address": "127.0.0.1", "port": 9012,
        })
        _post(master.port, "/nodes/hb3/heartbeat",
              {"metrics": {"inner_servers": 3, "fairness_score": 0.95}})
        node = _get(master.port, "/nodes/hb3")
        assert node["metrics"]["inner_servers"] == 3
        assert node["metrics"]["fairness_score"] == 0.95

    def test_heartbeat_updates_last_seen(self, master):
        _post(master.port, "/nodes/join", {
            "node_id": "hb4", "address": "127.0.0.1", "port": 9013,
        })
        time.sleep(0.5)
        before_node = _get(master.port, "/nodes/hb4")
        _post(master.port, "/nodes/hb4/heartbeat", {})
        after_node = _get(master.port, "/nodes/hb4")
        # after heartbeat, last_seen_ago should be smaller
        assert after_node["last_seen_ago_sec"] <= before_node["last_seen_ago_sec"]


class TestNodeLeave:
    def test_leave_removes_node(self, master):
        _post(master.port, "/nodes/join", {
            "node_id": "lv1", "address": "127.0.0.1", "port": 9020,
        })
        resp = _delete(master.port, "/nodes/lv1")
        assert resp["ok"] is True
        nodes = _get(master.port, "/nodes")["nodes"]
        assert all(n["node_id"] != "lv1" for n in nodes)

    def test_leave_unknown_node(self, master):
        resp = _delete(master.port, "/nodes/ghost")
        assert resp["ok"] is False
        assert "error" in resp

    def test_leave_returns_action(self, master):
        _post(master.port, "/nodes/join", {
            "node_id": "lv2", "address": "127.0.0.1", "port": 9021,
        })
        resp = _delete(master.port, "/nodes/lv2")
        assert resp["action"] == "left"
        assert resp["node_id"] == "lv2"


class TestNodeDetail:
    def test_get_node(self, master):
        _post(master.port, "/nodes/join", {
            "node_id": "det1", "address": "127.0.0.1", "port": 9030,
            "metadata": {"role": "lb"},
        })
        node = _get(master.port, "/nodes/det1")
        assert node["node_id"]            == "det1"
        assert node["metadata"]["role"]   == "lb"
        assert node["status"]             == "alive"
        assert "url"               in node
        assert "last_seen_ago_sec" in node
        assert "joined_at"         in node
        assert "heartbeat_count"   in node

    def test_get_unknown_node_404(self, master):
        with pytest.raises(urllib.error.HTTPError) as exc:
            _get(master.port, "/nodes/ghost")
        assert exc.value.code == 404

    def test_node_url_field(self, master):
        _post(master.port, "/nodes/join", {
            "node_id": "det2", "address": "10.0.1.5", "port": 9031,
        })
        node = _get(master.port, "/nodes/det2")
        assert node["url"] == "http://10.0.1.5:9031"


class TestStatusCounts:
    def test_alive_count_increases(self, master):
        _post(master.port, "/nodes/join", {
            "node_id": "sc1", "address": "127.0.0.1", "port": 9040,
        })
        _post(master.port, "/nodes/join", {
            "node_id": "sc2", "address": "127.0.0.1", "port": 9041,
        })
        status = _get(master.port, "/status")
        assert status["total_nodes"] == 2
        assert status["alive_nodes"] == 2
        assert status["dead_nodes"]  == 0

    def test_total_decreases_after_leave(self, master):
        _post(master.port, "/nodes/join", {
            "node_id": "sc3", "address": "127.0.0.1", "port": 9042,
        })
        _post(master.port, "/nodes/join", {
            "node_id": "sc4", "address": "127.0.0.1", "port": 9043,
        })
        _delete(master.port, "/nodes/sc3")
        status = _get(master.port, "/status")
        assert status["total_nodes"] == 1

    def test_uptime_positive(self, master):
        status = _get(master.port, "/status")
        assert status["uptime_sec"] >= 0


class TestCallbacks:
    def test_on_join_fires(self, master):
        events = []
        master._on_join = lambda node: events.append(("join", node.node_id))
        _post(master.port, "/nodes/join", {
            "node_id": "cb1", "address": "127.0.0.1", "port": 9050,
        })
        assert ("join", "cb1") in events

    def test_on_leave_fires(self, master):
        events = []
        master._on_leave = lambda node: events.append(("leave", node.node_id))
        _post(master.port, "/nodes/join", {
            "node_id": "cb2", "address": "127.0.0.1", "port": 9051,
        })
        _delete(master.port, "/nodes/cb2")
        assert ("leave", "cb2") in events

    def test_on_dead_fires(self):
        port   = _free_port()
        events = []
        m = MasterNode(
            host="127.0.0.1", port=port,
            heartbeat_timeout_sec=1,
            on_node_dead=lambda node: events.append(("dead", node.node_id)),
        )
        m.start()
        time.sleep(0.1)
        try:
            _post(port, "/nodes/join", {
                "node_id": "cb3", "address": "127.0.0.1", "port": 9052,
            })
            time.sleep(2.5)
            assert ("dead", "cb3") in events
        finally:
            m.stop()


class TestHeartbeatTimeout:
    def test_node_marked_dead_on_timeout(self):
        port = _free_port()
        m = MasterNode(host="127.0.0.1", port=port, heartbeat_timeout_sec=1)
        m.start()
        time.sleep(0.1)
        try:
            _post(port, "/nodes/join", {
                "node_id": "td1", "address": "127.0.0.1", "port": 9060,
            })
            time.sleep(2.5)
            node = _get(port, "/nodes/td1")
            assert node["status"] == "dead"
        finally:
            m.stop()

    def test_node_recovers_after_heartbeat(self):
        port = _free_port()
        m = MasterNode(host="127.0.0.1", port=port, heartbeat_timeout_sec=1)
        m.start()
        time.sleep(0.1)
        try:
            _post(port, "/nodes/join", {
                "node_id": "td2", "address": "127.0.0.1", "port": 9061,
            })
            time.sleep(2.5)
            node = _get(port, "/nodes/td2")
            assert node["status"] == "dead"
            # Heartbeat revives the node
            _post(port, "/nodes/td2/heartbeat", {})
            node2 = _get(port, "/nodes/td2")
            assert node2["status"] == "alive"
        finally:
            m.stop()


class TestConcurrency:
    def test_concurrent_joins(self, master):
        import threading
        errors = []

        def do_join(i):
            try:
                _post(master.port, "/nodes/join", {
                    "node_id": f"conc-{i}",
                    "address": "127.0.0.1",
                    "port": 9100 + i,
                })
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=do_join, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        nodes = _get(master.port, "/nodes")["nodes"]
        assert len(nodes) == 10