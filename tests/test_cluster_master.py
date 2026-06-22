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


def _get(port, path, api_key=None):
    url = f"http://127.0.0.1:{port}/v1{path}"
    req = urllib.request.Request(url)
    if api_key:
        req.add_header("Authorization", f"Bearer {api_key}")
    with urllib.request.urlopen(req, timeout=3) as r:
        return json.loads(r.read())


def _get_text(port, path, api_key=None):
    url = f"http://127.0.0.1:{port}/v1{path}"
    req = urllib.request.Request(url)
    if api_key:
        req.add_header("Authorization", f"Bearer {api_key}")
    with urllib.request.urlopen(req, timeout=3) as r:
        return r.read().decode()


def _post(port, path, payload, api_key=None):
    """POST and return the JSON body regardless of HTTP status code."""
    data = json.dumps(payload).encode()
    req  = urllib.request.Request(
        f"http://127.0.0.1:{port}/v1{path}",
        data=data, method="POST",
    )
    req.add_header("Content-Type",   "application/json")
    req.add_header("Content-Length", str(len(data)))
    if api_key:
        req.add_header("Authorization", f"Bearer {api_key}")
    try:
        with urllib.request.urlopen(req, timeout=3) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        # Read the error body — server always returns JSON
        return json.loads(e.read())


def _delete(port, path, api_key=None):
    """DELETE and return the JSON body regardless of HTTP status code."""
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/v1{path}",
        method="DELETE",
    )
    if api_key:
        req.add_header("Authorization", f"Bearer {api_key}")
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


class TestAutoRecoveryFlapping:
    """
    Auto recovery (Level 2): a node that dies and comes back too many times
    within flap_window_sec is quarantined instead of trusted immediately,
    and must prove itself with quarantine_recovery_heartbeats consecutive
    heartbeats before being promoted back to 'alive'.
    """

    def _make_master(self, **overrides):
        port = _free_port()
        kwargs = dict(
            host="127.0.0.1", port=port,
            heartbeat_timeout_sec=1,
            flap_window_sec=60,
            flap_threshold=3,
            quarantine_recovery_heartbeats=2,
        )
        kwargs.update(overrides)
        m = MasterNode(**kwargs)
        m.start()
        time.sleep(0.1)
        return m

    def test_single_recovery_does_not_quarantine(self):
        """One death/recovery cycle (below threshold) should go straight to alive."""
        m = self._make_master(flap_threshold=3)
        try:
            _post(m.port, "/nodes/join", {
                "node_id": "fl1", "address": "127.0.0.1", "port": 9200,
            })
            time.sleep(1.3)   # let it time out once
            node = _get(m.port, "/nodes/fl1")
            assert node["status"] == "dead"

            _post(m.port, "/nodes/fl1/heartbeat", {})
            node = _get(m.port, "/nodes/fl1")
            assert node["status"] == "alive"
        finally:
            m.stop()

    def test_repeated_deaths_trigger_quarantine(self):
        """Dying flap_threshold times within the window quarantines the node."""
        m = self._make_master(flap_threshold=3, heartbeat_timeout_sec=0.5)
        try:
            _post(m.port, "/nodes/join", {
                "node_id": "fl2", "address": "127.0.0.1", "port": 9201,
            })
            for _ in range(3):
                time.sleep(0.7)                       # let it die
                node = _get(m.port, "/nodes/fl2")
                assert node["status"] == "dead"
                _post(m.port, "/nodes/fl2/heartbeat", {})  # bring it back
                node = _get(m.port, "/nodes/fl2")
                # after the 3rd death+recover, it must be quarantined not alive
            assert node["status"] == "quarantined"
            assert node["death_count"] == 3
        finally:
            m.stop()

    def test_quarantine_promotes_after_n_heartbeats(self):
        """
        After quarantine_recovery_heartbeats consecutive heartbeats, node
        becomes alive. The heartbeat that triggers quarantine counts as
        recovery heartbeat #1.
        """
        m = self._make_master(
            flap_threshold=2, quarantine_recovery_heartbeats=3,
            heartbeat_timeout_sec=0.5,
        )
        try:
            _post(m.port, "/nodes/join", {
                "node_id": "fl3", "address": "127.0.0.1", "port": 9202,
            })
            for _ in range(2):
                time.sleep(0.7)
                _post(m.port, "/nodes/fl3/heartbeat", {})

            node = _get(m.port, "/nodes/fl3")
            assert node["status"] == "quarantined"
            assert node["consecutive_alive_heartbeats"] == 1

            # 2nd heartbeat — still quarantined (need 3 total)
            _post(m.port, "/nodes/fl3/heartbeat", {})
            node = _get(m.port, "/nodes/fl3")
            assert node["status"] == "quarantined"
            assert node["consecutive_alive_heartbeats"] == 2

            # 3rd heartbeat — now promoted to alive
            _post(m.port, "/nodes/fl3/heartbeat", {})
            node = _get(m.port, "/nodes/fl3")
            assert node["status"] == "alive"
            assert node["recent_deaths"] == []   # clean slate after promotion
        finally:
            m.stop()

    def test_quarantine_callback_fires(self):
        m = self._make_master(flap_threshold=2, heartbeat_timeout_sec=0.5)
        events = []
        m._on_quarantined = lambda node: events.append(node.node_id)
        try:
            _post(m.port, "/nodes/join", {
                "node_id": "fl4", "address": "127.0.0.1", "port": 9203,
            })
            for _ in range(2):
                time.sleep(0.7)
                _post(m.port, "/nodes/fl4/heartbeat", {})
            assert "fl4" in events
        finally:
            m.stop()

    def test_quarantined_excluded_from_alive_nodes(self):
        m = self._make_master(flap_threshold=2, heartbeat_timeout_sec=0.5)
        try:
            _post(m.port, "/nodes/join", {
                "node_id": "fl5", "address": "127.0.0.1", "port": 9204,
            })
            for _ in range(2):
                time.sleep(0.7)
                _post(m.port, "/nodes/fl5/heartbeat", {})

            alive = m.alive_nodes()
            quarantined = m.quarantined_nodes()
            assert all(n["node_id"] != "fl5" for n in alive)
            assert any(n["node_id"] == "fl5" for n in quarantined)
        finally:
            m.stop()

    def test_status_counts_quarantined(self):
        m = self._make_master(flap_threshold=2, heartbeat_timeout_sec=0.5)
        try:
            _post(m.port, "/nodes/join", {
                "node_id": "fl6", "address": "127.0.0.1", "port": 9205,
            })
            for _ in range(2):
                time.sleep(0.7)
                _post(m.port, "/nodes/fl6/heartbeat", {})

            status = _get(m.port, "/status")
            assert status["quarantined_nodes"] == 1
            assert status["alive_nodes"] == 0
        finally:
            m.stop()

    def test_quarantined_node_can_die_again(self):
        """A quarantined node that stops heartbeating should go back to dead."""
        m = self._make_master(flap_threshold=2, heartbeat_timeout_sec=0.5)
        try:
            _post(m.port, "/nodes/join", {
                "node_id": "fl7", "address": "127.0.0.1", "port": 9206,
            })
            for _ in range(2):
                time.sleep(0.7)
                _post(m.port, "/nodes/fl7/heartbeat", {})
            node = _get(m.port, "/nodes/fl7")
            assert node["status"] == "quarantined"

            time.sleep(0.7)   # stop heartbeating — should time out again
            node = _get(m.port, "/nodes/fl7")
            assert node["status"] == "dead"
            assert node["death_count"] == 3
        finally:
            m.stop()

    def test_rejoin_while_dead_triggers_quarantine(self):
        """Crash-looping via rejoin (not just heartbeat) is also flap-detected."""
        m = self._make_master(flap_threshold=3, heartbeat_timeout_sec=0.5)
        try:
            payload = {"node_id": "fl8", "address": "127.0.0.1", "port": 9207}
            _post(m.port, "/nodes/join", payload)
            for _ in range(2):
                time.sleep(0.7)
                _post(m.port, "/nodes/join", payload)   # rejoin instead of heartbeat
                node = _get(m.port, "/nodes/fl8")
                if node["status"] != "quarantined":
                    assert node["status"] == "alive"

            time.sleep(0.7)
            _post(m.port, "/nodes/join", payload)   # 3rd rejoin-while-dead
            node = _get(m.port, "/nodes/fl8")
            assert node["status"] == "quarantined"
        finally:
            m.stop()

    def test_old_deaths_outside_window_dont_count(self):
        """Deaths older than flap_window_sec should not count toward the threshold."""
        m = self._make_master(
            flap_threshold=2, flap_window_sec=0.5, heartbeat_timeout_sec=0.3,
        )
        try:
            _post(m.port, "/nodes/join", {
                "node_id": "fl9", "address": "127.0.0.1", "port": 9208,
            })
            time.sleep(0.5)                          # 1st death
            node = _get(m.port, "/nodes/fl9")
            assert node["status"] == "dead"
            _post(m.port, "/nodes/fl9/heartbeat", {})
            node = _get(m.port, "/nodes/fl9")
            assert node["status"] == "alive"

            # Stay alive (keep heartbeating under the timeout) long enough
            # for the 1st death to fall outside the flap window.
            for _ in range(4):
                time.sleep(0.15)
                _post(m.port, "/nodes/fl9/heartbeat", {})

            time.sleep(0.5)                           # 2nd death, window reset
            node = _get(m.port, "/nodes/fl9")
            assert node["status"] == "dead"
            _post(m.port, "/nodes/fl9/heartbeat", {})
            node = _get(m.port, "/nodes/fl9")
            # Only 1 death inside the current window — should be alive, not quarantined
            assert node["status"] == "alive"
        finally:
            m.stop()


class TestAutoRecoveryPurge:
    def test_purge_removes_long_dead_node(self):
        port = _free_port()
        m = MasterNode(
            host="127.0.0.1", port=port,
            heartbeat_timeout_sec=0.5,
            purge_after_sec=1.0,
        )
        m.start()
        time.sleep(0.1)
        try:
            _post(port, "/nodes/join", {
                "node_id": "pg1", "address": "127.0.0.1", "port": 9300,
            })
            time.sleep(0.8)
            node = _get(port, "/nodes/pg1")
            assert node["status"] == "dead"

            time.sleep(1.5)   # exceed purge_after_sec since it died
            with pytest.raises(urllib.error.HTTPError) as exc:
                _get(port, "/nodes/pg1")
            assert exc.value.code == 404
        finally:
            m.stop()

    def test_purge_disabled_by_default(self):
        """Without purge_after_sec, dead nodes stay in the registry forever."""
        port = _free_port()
        m = MasterNode(host="127.0.0.1", port=port, heartbeat_timeout_sec=0.5)
        m.start()
        time.sleep(0.1)
        try:
            _post(port, "/nodes/join", {
                "node_id": "pg2", "address": "127.0.0.1", "port": 9301,
            })
            time.sleep(1.5)
            node = _get(port, "/nodes/pg2")   # should still exist, just dead
            assert node["status"] == "dead"
        finally:
            m.stop()

    def test_purge_callback_fires(self):
        port = _free_port()
        events = []
        m = MasterNode(
            host="127.0.0.1", port=port,
            heartbeat_timeout_sec=0.5,
            purge_after_sec=1.0,
            on_node_purged=lambda node: events.append(node.node_id),
        )
        m.start()
        time.sleep(0.1)
        try:
            _post(port, "/nodes/join", {
                "node_id": "pg3", "address": "127.0.0.1", "port": 9302,
            })
            time.sleep(2.0)
            assert "pg3" in events
        finally:
            m.stop()

    def test_quarantined_node_not_purged(self):
        """Quarantined nodes are actively heartbeating — purge must not touch them."""
        port = _free_port()
        m = MasterNode(
            host="127.0.0.1", port=port,
            heartbeat_timeout_sec=0.4,
            flap_threshold=2,
            purge_after_sec=2.0,     # must be > timeout, see warning in __init__
        )
        m.start()
        time.sleep(0.1)
        try:
            _post(port, "/nodes/join", {
                "node_id": "pg4", "address": "127.0.0.1", "port": 9303,
            })
            for _ in range(2):
                time.sleep(0.6)
                _post(port, "/nodes/pg4/heartbeat", {})
            node = _get(port, "/nodes/pg4")
            assert node["status"] == "quarantined"

            # Keep sending on-time heartbeats (comfortable margin under the
            # 0.4s timeout) — since it never goes back to "dead", purge logic
            # (which only ever applies to status == "dead") must never apply.
            for _ in range(4):
                time.sleep(0.15)
                _post(port, "/nodes/pg4/heartbeat", {})

            node = _get(port, "/nodes/pg4")   # must still exist, never purged
            assert node["node_id"] == "pg4"
            assert node["status"] in ("quarantined", "alive")
        finally:
            m.stop()


class TestPrometheusMetrics:
    def test_metrics_endpoint_content_type(self, master):
        url = f"http://127.0.0.1:{master.port}/v1/metrics"
        with urllib.request.urlopen(url, timeout=3) as r:
            ctype = r.headers.get("Content-Type", "")
            assert ctype.startswith("text/plain")

    def test_metrics_contains_master_gauges(self, master):
        text = _get_text(master.port, "/metrics")
        assert "huddle_master_uptime_seconds" in text
        assert "huddle_master_total_nodes" in text
        assert "huddle_master_alive_nodes" in text
        assert "huddle_master_dead_nodes" in text
        assert "huddle_master_quarantined_nodes" in text
        assert "huddle_master_unhealthy" in text

    def test_metrics_has_help_and_type_lines(self, master):
        text = _get_text(master.port, "/metrics")
        assert "# HELP huddle_master_total_nodes" in text
        assert "# TYPE huddle_master_total_nodes gauge" in text

    def test_metrics_reflects_node_count(self, master):
        _post(master.port, "/nodes/join", {
            "node_id": "m1", "address": "127.0.0.1", "port": 9400,
        })
        text = _get_text(master.port, "/metrics")
        assert "huddle_master_total_nodes 1" in text
        assert "huddle_master_alive_nodes 1" in text

    def test_metrics_contains_per_node_up_gauge(self, master):
        _post(master.port, "/nodes/join", {
            "node_id": "m2", "address": "127.0.0.1", "port": 9401,
        })
        text = _get_text(master.port, "/metrics")
        assert 'huddle_node_up{node_id="m2"} 1' in text

    def test_metrics_contains_heartbeat_and_death_counters(self, master):
        _post(master.port, "/nodes/join", {
            "node_id": "m3", "address": "127.0.0.1", "port": 9402,
        })
        _post(master.port, "/nodes/m3/heartbeat", {})
        _post(master.port, "/nodes/m3/heartbeat", {})
        text = _get_text(master.port, "/metrics")
        assert 'huddle_node_heartbeat_count{node_id="m3"} 2' in text
        assert 'huddle_node_death_count{node_id="m3"} 0' in text

    def test_metrics_includes_forwarded_node_metrics_when_present(self, master):
        _post(master.port, "/nodes/join", {
            "node_id": "m4", "address": "127.0.0.1", "port": 9403,
        })
        _post(master.port, "/nodes/m4/heartbeat", {
            "metrics": {"fairness_score": 0.87, "inner_servers": 4}
        })
        text = _get_text(master.port, "/metrics")
        assert 'huddle_node_fairness_score{node_id="m4"} 0.87' in text
        assert 'huddle_node_inner_servers{node_id="m4"} 4' in text

    def test_metrics_omits_forwarded_fields_when_absent(self, master):
        """A node that never forwarded fairness_score shouldn't get a line for it."""
        _post(master.port, "/nodes/join", {
            "node_id": "m5", "address": "127.0.0.1", "port": 9404,
        })
        _post(master.port, "/nodes/m5/heartbeat", {})   # no metrics payload
        text = _get_text(master.port, "/metrics")
        # No node in this test ever forwarded fairness_score, so the whole
        # metric family should be absent rather than printed as 0.
        assert "huddle_node_fairness_score" not in text

    def test_metrics_quarantined_node_shows_half(self):
        port = _free_port()
        m = MasterNode(host="127.0.0.1", port=port,
                        heartbeat_timeout_sec=0.4, flap_threshold=2)
        m.start()
        time.sleep(0.1)
        try:
            _post(port, "/nodes/join", {
                "node_id": "m6", "address": "127.0.0.1", "port": 9405,
            })
            for _ in range(2):
                time.sleep(0.6)
                _post(port, "/nodes/m6/heartbeat", {})
            text = _get_text(port, "/metrics")
            assert 'huddle_node_up{node_id="m6"} 0.5' in text
        finally:
            m.stop()

    def test_metrics_empty_cluster_does_not_error(self, master):
        text = _get_text(master.port, "/metrics")
        assert "huddle_master_total_nodes 0" in text


class TestClusterHealthMonitoring:
    def test_disabled_by_default_never_fires(self):
        port = _free_port()
        events = []
        m = MasterNode(
            host="127.0.0.1", port=port,
            heartbeat_timeout_sec=0.3,
            on_cluster_unhealthy=lambda s: events.append(s),
        )
        m.start()
        time.sleep(0.1)
        try:
            _post(port, "/nodes/join", {
                "node_id": "ch1", "address": "127.0.0.1", "port": 9500,
            })
            time.sleep(0.8)   # node dies, ratio drops to 0%, but feature is off
            assert events == []
        finally:
            m.stop()

    def test_empty_cluster_never_unhealthy(self):
        port = _free_port()
        events = []
        m = MasterNode(
            host="127.0.0.1", port=port,
            heartbeat_timeout_sec=0.3,
            unhealthy_alive_ratio=0.5,
            on_cluster_unhealthy=lambda s: events.append(s),
        )
        m.start()
        time.sleep(0.5)   # no nodes ever joined
        try:
            assert events == []
        finally:
            m.stop()

    def test_unhealthy_fires_when_ratio_drops(self):
        port = _free_port()
        events = []
        m = MasterNode(
            host="127.0.0.1", port=port,
            heartbeat_timeout_sec=0.3,
            unhealthy_alive_ratio=0.5,   # need >=50% alive
            on_cluster_unhealthy=lambda s: events.append(s),
        )
        m.start()
        time.sleep(0.1)
        try:
            _post(port, "/nodes/join", {
                "node_id": "ch2", "address": "127.0.0.1", "port": 9501,
            })
            _post(port, "/nodes/join", {
                "node_id": "ch3", "address": "127.0.0.1", "port": 9502,
            })
            # keep ch3 alive, let ch2 die -> 1/2 = 50%, not below threshold yet
            for _ in range(3):
                time.sleep(0.1)
                _post(port, "/nodes/ch3/heartbeat", {})
            time.sleep(0.5)   # ch2 times out, ch3 also stops -> 0/2 alive
            assert len(events) >= 1
            assert events[-1]["alive_ratio"] < 0.5
        finally:
            m.stop()

    def test_recovered_fires_after_ratio_restores(self):
        port = _free_port()
        unhealthy_events = []
        recovered_events = []
        m = MasterNode(
            host="127.0.0.1", port=port,
            heartbeat_timeout_sec=0.3,
            unhealthy_alive_ratio=0.5,
            on_cluster_unhealthy=lambda s: unhealthy_events.append(s),
            on_cluster_recovered=lambda s: recovered_events.append(s),
        )
        m.start()
        time.sleep(0.1)
        try:
            _post(port, "/nodes/join", {
                "node_id": "ch4", "address": "127.0.0.1", "port": 9503,
            })
            time.sleep(0.6)   # dies -> 0/1 alive -> unhealthy
            assert len(unhealthy_events) >= 1

            _post(port, "/nodes/ch4/heartbeat", {})   # recovers -> 1/1 alive
            time.sleep(0.2)
            assert len(recovered_events) >= 1
        finally:
            m.stop()

    def test_status_reports_unhealthy_flag(self):
        port = _free_port()
        m = MasterNode(
            host="127.0.0.1", port=port,
            heartbeat_timeout_sec=0.3,
            unhealthy_alive_ratio=0.5,
        )
        m.start()
        time.sleep(0.1)
        try:
            _post(port, "/nodes/join", {
                "node_id": "ch5", "address": "127.0.0.1", "port": 9504,
            })
            time.sleep(0.6)
            status = _get(port, "/status")
            assert status["cluster_unhealthy"] is True
            assert status["unhealthy_alive_ratio"] == 0.5
        finally:
            m.stop()

    def test_metrics_unhealthy_gauge_reflects_state(self):
        port = _free_port()
        m = MasterNode(
            host="127.0.0.1", port=port,
            heartbeat_timeout_sec=0.3,
            unhealthy_alive_ratio=0.5,
        )
        m.start()
        time.sleep(0.1)
        try:
            _post(port, "/nodes/join", {
                "node_id": "ch6", "address": "127.0.0.1", "port": 9505,
            })
            time.sleep(0.6)
            text = _get_text(port, "/metrics")
            assert "huddle_master_unhealthy 1" in text
        finally:
            m.stop()


class TestAuthentication:
    """RBAC: api_keys=None means open (default); when set, requires
    Authorization: Bearer <key>, with 'admin' or 'viewer' role checks."""

    def _make_master(self, api_keys):
        port = _free_port()
        m = MasterNode(host="127.0.0.1", port=port, heartbeat_timeout_sec=60,
                        api_keys=api_keys)
        m.start()
        time.sleep(0.1)
        return m

    def test_open_by_default(self, master):
        """No api_keys configured -> every request succeeds with no header."""
        resp = _post(master.port, "/nodes/join", {
            "node_id": "auth0", "address": "127.0.0.1", "port": 9600,
        })
        assert resp["ok"] is True
        status = _get(master.port, "/status")
        assert "total_nodes" in status

    def test_missing_header_rejected(self):
        m = self._make_master({"secret-admin": "admin"})
        try:
            with pytest.raises(urllib.error.HTTPError) as exc:
                _get(m.port, "/status")
            assert exc.value.code == 401
        finally:
            m.stop()

    def test_invalid_key_rejected(self):
        m = self._make_master({"secret-admin": "admin"})
        try:
            with pytest.raises(urllib.error.HTTPError) as exc:
                _get(m.port, "/status", api_key="wrong-key")
            assert exc.value.code == 401
        finally:
            m.stop()

    def test_health_never_requires_auth(self):
        m = self._make_master({"secret-admin": "admin"})
        try:
            resp = _get(m.port, "/health")   # no api_key passed
            assert resp["status"] == "ok"
        finally:
            m.stop()

    def test_viewer_can_read(self):
        m = self._make_master({"view-key": "viewer"})
        try:
            status = _get(m.port, "/status", api_key="view-key")
            assert "total_nodes" in status
            nodes = _get(m.port, "/nodes", api_key="view-key")
            assert "nodes" in nodes
            text = _get_text(m.port, "/metrics", api_key="view-key")
            assert "huddle_master_total_nodes" in text
        finally:
            m.stop()

    def test_viewer_cannot_join(self):
        m = self._make_master({"view-key": "viewer"})
        try:
            resp = _post(m.port, "/nodes/join", {
                "node_id": "auth1", "address": "127.0.0.1", "port": 9601,
            }, api_key="view-key")
            assert resp["ok"] is False
            assert "permission" in resp.get("error", "")
        finally:
            m.stop()

    def test_viewer_cannot_heartbeat(self):
        m = self._make_master({"admin-key": "admin", "view-key": "viewer"})
        try:
            _post(m.port, "/nodes/join", {
                "node_id": "auth2", "address": "127.0.0.1", "port": 9602,
            }, api_key="admin-key")
            resp = _post(m.port, "/nodes/auth2/heartbeat", {}, api_key="view-key")
            assert resp["ok"] is False
            assert "permission" in resp.get("error", "")
        finally:
            m.stop()

    def test_viewer_cannot_delete(self):
        m = self._make_master({"admin-key": "admin", "view-key": "viewer"})
        try:
            _post(m.port, "/nodes/join", {
                "node_id": "auth3", "address": "127.0.0.1", "port": 9603,
            }, api_key="admin-key")
            resp = _delete(m.port, "/nodes/auth3", api_key="view-key")
            assert resp["ok"] is False
            assert "permission" in resp.get("error", "")
            # Confirm the node was NOT actually removed by the rejected attempt
            node = _get(m.port, "/nodes/auth3", api_key="admin-key")
            assert node["node_id"] == "auth3"
        finally:
            m.stop()

    def test_admin_can_do_everything(self):
        m = self._make_master({"admin-key": "admin"})
        try:
            r1 = _post(m.port, "/nodes/join", {
                "node_id": "auth4", "address": "127.0.0.1", "port": 9604,
            }, api_key="admin-key")
            assert r1["ok"] is True

            r2 = _post(m.port, "/nodes/auth4/heartbeat", {}, api_key="admin-key")
            assert r2["ok"] is True

            status = _get(m.port, "/status", api_key="admin-key")
            assert status["total_nodes"] == 1

            r3 = _delete(m.port, "/nodes/auth4", api_key="admin-key")
            assert r3["ok"] is True
        finally:
            m.stop()

    def test_malformed_auth_header_rejected(self):
        m = self._make_master({"secret-admin": "admin"})
        try:
            url = f"http://127.0.0.1:{m.port}/v1/status"
            req = urllib.request.Request(url)
            req.add_header("Authorization", "Basic notabearertoken")
            with pytest.raises(urllib.error.HTTPError) as exc:
                urllib.request.urlopen(req, timeout=3)
            assert exc.value.code == 401
        finally:
            m.stop()

    def test_unrecognized_role_has_no_access(self):
        """A key mapped to a typo'd/unknown role fails closed, not open."""
        m = self._make_master({"odd-key": "superadmin"})
        try:
            with pytest.raises(urllib.error.HTTPError) as exc:
                _get(m.port, "/status", api_key="odd-key")
            assert exc.value.code == 403
        finally:
            m.stop()

    def test_multiple_admin_keys_all_work(self):
        m = self._make_master({"key-a": "admin", "key-b": "admin"})
        try:
            r1 = _get(m.port, "/status", api_key="key-a")
            r2 = _get(m.port, "/status", api_key="key-b")
            assert "total_nodes" in r1
            assert "total_nodes" in r2
        finally:
            m.stop()

    def test_node_detail_requires_auth(self):
        m = self._make_master({"admin-key": "admin"})
        try:
            _post(m.port, "/nodes/join", {
                "node_id": "auth5", "address": "127.0.0.1", "port": 9605,
            }, api_key="admin-key")
            with pytest.raises(urllib.error.HTTPError) as exc:
                _get(m.port, "/nodes/auth5")   # no key
            assert exc.value.code == 401
        finally:
            m.stop()


class TestDashboard:
    """GET /dashboard serves a self-contained HTML page. The page itself
    never requires auth (it's a static shell); only the /v1/ API calls it
    makes from the browser are subject to RBAC, exactly like any other
    client."""

    def _fetch_dashboard(self, port):
        url = f"http://127.0.0.1:{port}/dashboard"
        with urllib.request.urlopen(url, timeout=3) as r:
            return r.status, dict(r.headers), r.read().decode()

    def test_dashboard_returns_200(self, master):
        status, headers, body = self._fetch_dashboard(master.port)
        assert status == 200

    def test_dashboard_content_type_is_html(self, master):
        _, headers, _ = self._fetch_dashboard(master.port)
        assert headers.get("Content-Type", "").startswith("text/html")

    def test_dashboard_is_well_formed_html(self, master):
        _, _, body = self._fetch_dashboard(master.port)
        assert body.strip().startswith("<!DOCTYPE html>")
        assert "<html" in body
        assert "</html>" in body
        assert body.count("<html") == body.count("</html>")
        assert body.count("<head>") == body.count("</head>")
        assert body.count("<body>") == body.count("</body>")

    def test_dashboard_parses_without_html_errors(self, master):
        """Run it through Python's strict HTML parser to catch unclosed tags."""
        from html.parser import HTMLParser

        class _StrictParser(HTMLParser):
            def __init__(self):
                super().__init__(convert_charrefs=True)
                self.stack = []
                self.void_tags = {
                    "meta", "link", "br", "img", "input", "hr",
                }

            def handle_starttag(self, tag, attrs):
                if tag not in self.void_tags:
                    self.stack.append(tag)

            def handle_startendtag(self, tag, attrs):
                # Self-closed tag like <meta .../> — never push, and the
                # parser's own endtag-on-startendtag behavior must not pop
                # a real open element either.
                pass

            def handle_endtag(self, tag):
                if tag in self.void_tags:
                    return
                assert self.stack and self.stack[-1] == tag, (
                    f"Mismatched closing tag </{tag}>, stack was {self.stack}"
                )
                self.stack.pop()

        _, _, body = self._fetch_dashboard(master.port)
        parser = _StrictParser()
        parser.feed(body)
        assert parser.stack == [], f"Unclosed tags remain: {parser.stack}"

    def test_dashboard_fetches_v1_status_and_nodes(self, master):
        """The page's JS should reference the v1 endpoints it polls."""
        _, _, body = self._fetch_dashboard(master.port)
        assert "/v1/status" in body
        assert "/v1/nodes" in body

    def test_dashboard_works_without_auth_when_open(self, master):
        status, _, _ = self._fetch_dashboard(master.port)
        assert status == 200

    def test_dashboard_loads_even_when_master_requires_auth(self):
        """The dashboard SHELL itself is never gated — only its API calls are."""
        port = _free_port()
        m = MasterNode(host="127.0.0.1", port=port, heartbeat_timeout_sec=60,
                        api_keys={"secret": "admin"})
        m.start()
        time.sleep(0.1)
        try:
            status, _, body = self._fetch_dashboard(port)
            assert status == 200
            assert "<!DOCTYPE html>" in body
        finally:
            m.stop()

    def test_dashboard_does_not_leak_configured_api_keys(self):
        """The static HTML template must never embed real key values."""
        port = _free_port()
        secret_key = "super-secret-value-12345"
        m = MasterNode(host="127.0.0.1", port=port, heartbeat_timeout_sec=60,
                        api_keys={secret_key: "admin"})
        m.start()
        time.sleep(0.1)
        try:
            _, _, body = self._fetch_dashboard(port)
            assert secret_key not in body
        finally:
            m.stop()

    def test_dashboard_html_method_directly(self, master):
        """MasterNode.dashboard_html() is callable directly, not just via HTTP."""
        html = master.dashboard_html()
        assert isinstance(html, str)
        assert "<!DOCTYPE html>" in html
        assert "HuddleCluster" in html

    def test_dashboard_includes_huddle_themed_styling(self, master):
        """Sanity check the design intent (penguin-huddle metaphor, dark theme)."""
        _, _, body = self._fetch_dashboard(master.port)
        assert "huddle-dot" in body  # the per-node visual cluster strip
        assert "#0E1217" in body or "0E1217" in body  # dark control-room bg


class TestNodesFilteringAndPagination:
    """GET /v1/nodes supports ?status=, ?limit=, ?offset= while staying
    backward compatible with plain GET /v1/nodes (no params)."""

    def _join(self, port, node_id, api_key=None):
        return _post(port, "/nodes/join", {
            "node_id": node_id, "address": "127.0.0.1", "port": 9700,
        }, api_key=api_key)

    def test_plain_request_unchanged_shape(self, master):
        """No query params -> same 'nodes' key as before, now with extra metadata."""
        self._join(master.port, "rp1")
        data = _get(master.port, "/nodes")
        assert "nodes" in data
        assert len(data["nodes"]) == 1
        assert data["total"] == 1
        assert data["limit"] is None
        assert data["offset"] == 0

    def test_results_sorted_by_node_id(self, master):
        for nid in ["zebra", "alpha", "mike"]:
            self._join(master.port, nid)
        data = _get(master.port, "/nodes")
        ids = [n["node_id"] for n in data["nodes"]]
        assert ids == sorted(ids)

    def test_filter_by_single_status(self):
        port = _free_port()
        m = MasterNode(host="127.0.0.1", port=port, heartbeat_timeout_sec=0.4)
        m.start()
        time.sleep(0.1)
        try:
            self._join(port, "fs1")
            self._join(port, "fs2")
            time.sleep(0.7)   # both die
            data = _get(port, "/nodes?status=dead")
            assert {n["node_id"] for n in data["nodes"]} == {"fs1", "fs2"}
            assert data["total"] == 2
        finally:
            m.stop()

    def test_filter_by_multiple_statuses(self):
        port = _free_port()
        m = MasterNode(host="127.0.0.1", port=port, heartbeat_timeout_sec=0.4)
        m.start()
        time.sleep(0.1)
        try:
            self._join(port, "fm1")
            self._join(port, "fm2")
            time.sleep(0.7)
            _post(port, "/nodes/fm1/heartbeat", {})   # fm1 alive, fm2 dead
            data = _get(port, "/nodes?status=alive,dead")
            assert {n["node_id"] for n in data["nodes"]} == {"fm1", "fm2"}
        finally:
            m.stop()

    def test_filter_excludes_nonmatching(self):
        port = _free_port()
        m = MasterNode(host="127.0.0.1", port=port, heartbeat_timeout_sec=0.4)
        m.start()
        time.sleep(0.1)
        try:
            self._join(port, "fx1")
            data = _get(port, "/nodes?status=dead")   # fx1 is alive
            assert data["nodes"] == []
            assert data["total"] == 0
        finally:
            m.stop()

    def test_unknown_status_value_returns_400(self, master):
        with pytest.raises(urllib.error.HTTPError) as exc:
            _get(master.port, "/nodes?status=zombie")
        assert exc.value.code == 400

    def test_limit_returns_subset(self, master):
        for i in range(5):
            self._join(master.port, f"lim{i}")
        data = _get(master.port, "/nodes?limit=2")
        assert len(data["nodes"]) == 2
        assert data["total"] == 5
        assert data["limit"] == 2

    def test_offset_skips_results(self, master):
        for i in range(5):
            self._join(master.port, f"off{i}")
        full = _get(master.port, "/nodes")["nodes"]
        offset_data = _get(master.port, "/nodes?offset=2")
        assert offset_data["nodes"] == full[2:]
        assert offset_data["offset"] == 2

    def test_limit_and_offset_together(self, master):
        for i in range(5):
            self._join(master.port, f"page{i}")
        data = _get(master.port, "/nodes?limit=2&offset=2")
        full = _get(master.port, "/nodes")["nodes"]
        assert data["nodes"] == full[2:4]

    def test_limit_zero_returns_empty_but_valid(self, master):
        self._join(master.port, "lz1")
        data = _get(master.port, "/nodes?limit=0")
        assert data["nodes"] == []
        assert data["total"] == 1

    def test_negative_limit_returns_400(self, master):
        with pytest.raises(urllib.error.HTTPError) as exc:
            _get(master.port, "/nodes?limit=-1")
        assert exc.value.code == 400

    def test_negative_offset_returns_400(self, master):
        with pytest.raises(urllib.error.HTTPError) as exc:
            _get(master.port, "/nodes?offset=-1")
        assert exc.value.code == 400

    def test_non_integer_limit_returns_400(self, master):
        with pytest.raises(urllib.error.HTTPError) as exc:
            _get(master.port, "/nodes?limit=abc")
        assert exc.value.code == 400

    def test_offset_beyond_total_returns_empty(self, master):
        self._join(master.port, "ob1")
        data = _get(master.port, "/nodes?offset=999")
        assert data["nodes"] == []
        assert data["total"] == 1

    def test_filtering_respects_auth(self):
        port = _free_port()
        m = MasterNode(host="127.0.0.1", port=port, heartbeat_timeout_sec=60,
                        api_keys={"k": "viewer"})
        m.start()
        time.sleep(0.1)
        try:
            with pytest.raises(urllib.error.HTTPError) as exc:
                _get(port, "/nodes?status=alive")   # no key
            assert exc.value.code == 401
            data = _get(port, "/nodes?status=alive", api_key="k")
            assert data["nodes"] == []
        finally:
            m.stop()

    def test_nodes_method_direct_filter(self, master):
        """MasterNode.nodes(status=...) is usable directly, not just via HTTP."""
        self._join(master.port, "direct1")
        result = master.nodes(status="alive")
        assert len(result) == 1
        assert result[0]["node_id"] == "direct1"
        empty = master.nodes(status="dead")
        assert empty == []


class TestOpenApiSpec:
    def test_openapi_endpoint_returns_200(self, master):
        data = _get(master.port, "/openapi.json")
        assert data["openapi"].startswith("3.")

    def test_openapi_never_requires_auth(self):
        port = _free_port()
        m = MasterNode(host="127.0.0.1", port=port, heartbeat_timeout_sec=60,
                        api_keys={"k": "admin"})
        m.start()
        time.sleep(0.1)
        try:
            data = _get(port, "/openapi.json")   # no key passed
            assert "paths" in data
        finally:
            m.stop()

    def test_openapi_lists_all_documented_paths(self, master):
        data = _get(master.port, "/openapi.json")
        paths = set(data["paths"].keys())
        assert "/health" in paths
        assert "/status" in paths
        assert "/metrics" in paths
        assert "/nodes" in paths
        assert "/nodes/{node_id}" in paths
        assert "/nodes/join" in paths
        assert "/nodes/{node_id}/heartbeat" in paths

    def test_openapi_has_required_top_level_fields(self, master):
        data = _get(master.port, "/openapi.json")
        assert "info" in data
        assert "paths" in data
        assert "components" in data
        assert data["info"]["title"]
        assert data["info"]["version"]

    def test_openapi_method_direct(self, master):
        """MasterNode.openapi_spec() is usable directly, not just via HTTP."""
        spec = master.openapi_spec()
        assert isinstance(spec, dict)
        assert spec["openapi"].startswith("3.")

    def test_status_reports_api_version(self, master):
        status = _get(master.port, "/status")
        assert "api_version" in status
        assert status["api_version"]


class TestSwaggerDocs:
    """GET /v1/docs serves an interactive Swagger UI shell pointed at this
    master's own /v1/openapi.json. The page itself never requires auth,
    same reasoning as /dashboard and /v1/openapi.json."""

    def _fetch_docs(self, port):
        url = f"http://127.0.0.1:{port}/v1/docs"
        with urllib.request.urlopen(url, timeout=3) as r:
            return r.status, dict(r.headers), r.read().decode()

    def test_docs_returns_200(self, master):
        status, _, _ = self._fetch_docs(master.port)
        assert status == 200

    def test_docs_content_type_is_html(self, master):
        _, headers, _ = self._fetch_docs(master.port)
        assert headers.get("Content-Type", "").startswith("text/html")

    def test_docs_is_well_formed_html(self, master):
        _, _, body = self._fetch_docs(master.port)
        assert body.strip().startswith("<!DOCTYPE html>")
        assert "<html" in body and "</html>" in body
        assert body.count("<html") == body.count("</html>")

    def test_docs_points_at_own_openapi_spec(self, master):
        _, _, body = self._fetch_docs(master.port)
        assert "/v1/openapi.json" in body

    def test_docs_never_requires_auth(self):
        port = _free_port()
        m = MasterNode(host="127.0.0.1", port=port, heartbeat_timeout_sec=60,
                        api_keys={"secret": "admin"})
        m.start()
        time.sleep(0.1)
        try:
            status, _, body = self._fetch_docs(port)
            assert status == 200
            assert "<!DOCTYPE html>" in body
        finally:
            m.stop()

    def test_swagger_html_method_direct(self, master):
        html = master.swagger_html()
        assert isinstance(html, str)
        assert "<!DOCTYPE html>" in html
        assert "SwaggerUIBundle" in html