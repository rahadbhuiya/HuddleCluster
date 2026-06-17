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