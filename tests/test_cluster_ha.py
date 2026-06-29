"""
Tests for ClusterHA (huddle_cluster_pkg.cluster_ha).
"""

import json
import time
import threading
import urllib.error
import urllib.request

import pytest

from huddle_cluster_pkg.cluster_master import MasterNode
from huddle_cluster_pkg.cluster_ha     import ClusterHA, LEADER, FOLLOWER, CANDIDATE



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
        try:
            return json.loads(e.read())
        except Exception:
            return {"ok": False, "http_code": e.code}


def _make_master_with_ha(ha, timeout=60):
    port = _free_port()
    m = MasterNode(host="127.0.0.1", port=port,
                   heartbeat_timeout_sec=timeout,
                   ha=ha)
    m.start()
    time.sleep(0.1)
    return m


def _wait_leader(ha, timeout=8.0):
    """Block until ha becomes leader (or timeout)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if ha.is_leader():
            return True
        time.sleep(0.1)
    return False


def _wait_follower(ha, timeout=8.0):
    """Block until ha becomes follower (or timeout)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if ha.role() == FOLLOWER:
            return True
        time.sleep(0.1)
    return False



# Unit tests: ClusterHA without master


class TestClusterHAInit:
    def test_default_role_is_follower(self):
        ha = ClusterHA(node_id="n1")
        assert ha.role() == FOLLOWER

    def test_not_leader_initially(self):
        ha = ClusterHA(node_id="n1", peers=["http://host2:7071"])
        assert ha.is_leader() is False

    def test_no_leader_url_initially(self):
        ha = ClusterHA(node_id="n1")
        assert ha.leader_url() is None

    def test_status_returns_expected_keys(self):
        ha = ClusterHA(node_id="n1", peers=["http://host2:7071"])
        s = ha.status()
        assert s["node_id"] == "n1"
        assert s["role"] == FOLLOWER
        assert s["term"] == 0
        assert s["peer_count"] == 1


# Unit tests: Raft RPC methods


class TestRaftRPCs:
    def test_vote_granted_to_higher_term_candidate(self):
        ha = ClusterHA(node_id="n1")
        result = ha.handle_vote_request(
            candidate_id="n2", candidate_term=1
        )
        assert result["vote_granted"] is True
        assert result["term"] == 1

    def test_vote_denied_for_lower_term(self):
        ha = ClusterHA(node_id="n1")
        ha._term = 5
        result = ha.handle_vote_request(
            candidate_id="n2", candidate_term=3
        )
        assert result["vote_granted"] is False

    def test_vote_denied_after_already_voted(self):
        ha = ClusterHA(node_id="n1")
        ha.handle_vote_request(candidate_id="n2", candidate_term=1)
        result = ha.handle_vote_request(candidate_id="n3", candidate_term=1)
        assert result["vote_granted"] is False

    def test_same_candidate_can_get_second_vote(self):
        ha = ClusterHA(node_id="n1")
        ha.handle_vote_request(candidate_id="n2", candidate_term=1)
        result = ha.handle_vote_request(candidate_id="n2", candidate_term=1)
        assert result["vote_granted"] is True

    def test_append_entries_updates_leader_info(self):
        ha = ClusterHA(node_id="n1")
        result = ha.handle_append_entries(
            leader_id="n2", leader_url="http://n2:7071", term=1
        )
        assert result["success"] is True
        assert ha.role() == FOLLOWER
        with ha._lock:
            assert ha._leader_id == "n2"
            assert ha._leader_url == "http://n2:7071"

    def test_append_entries_rejected_for_lower_term(self):
        ha = ClusterHA(node_id="n1")
        ha._term = 5
        result = ha.handle_append_entries(
            leader_id="n2", leader_url="http://n2:7071", term=3
        )
        assert result["success"] is False

    def test_higher_term_in_vote_response_causes_step_down(self):
        ha = ClusterHA(node_id="n1")
        # Simulate receiving higher term from append_entries
        ha.handle_append_entries(
            leader_id="n2", leader_url="http://n2:7071", term=10
        )
        with ha._lock:
            assert ha._term == 10
            assert ha._role == FOLLOWER

    def test_step_down_clears_vote(self):
        ha = ClusterHA(node_id="n1")
        ha._voted_for = "n2"
        with ha._lock:
            ha._step_down(5)
        with ha._lock:
            assert ha._voted_for is None



# Integration tests: solo master (no peers → always leader)


class TestSoloMaster:
    def test_solo_master_becomes_leader_immediately(self):
        ha = ClusterHA(node_id="solo", peers=[])
        m  = _make_master_with_ha(ha)
        try:
            assert ha.is_leader() is True
        finally:
            m.stop()

    def test_ha_status_endpoint_returns_leader(self):
        ha = ClusterHA(node_id="solo", peers=[])
        m  = _make_master_with_ha(ha)
        try:
            data = _get(m.port, "/ha/status")
            assert data["role"] == LEADER
            assert data["node_id"] == "solo"
        finally:
            m.stop()

    def test_master_status_includes_ha_info(self):
        ha = ClusterHA(node_id="solo", peers=[])
        m  = _make_master_with_ha(ha)
        try:
            status = _get(m.port, "/status")
            assert isinstance(status["ha"], dict)
            assert status["ha"]["role"] == LEADER
        finally:
            m.stop()

    def test_solo_leader_accepts_write_and_read(self):
        ha = ClusterHA(node_id="solo", peers=[])
        m  = _make_master_with_ha(ha)
        try:
            r = _post(m.port, "/nodes/join", {
                "node_id": "agent-1", "address": "127.0.0.1", "port": 8080,
            })
            assert r["ok"] is True
            nodes = _get(m.port, "/nodes")
            assert any(n["node_id"] == "agent-1" for n in nodes["nodes"])
        finally:
            m.stop()

    def test_ha_status_503_when_disabled(self):
        m = MasterNode(host="127.0.0.1", port=_free_port(),
                       heartbeat_timeout_sec=60)
        m.start()
        time.sleep(0.1)
        try:
            with pytest.raises(urllib.error.HTTPError) as exc:
                _get(m.port, "/ha/status")
            assert exc.value.code == 503
        finally:
            m.stop()

    def test_master_status_reports_ha_disabled(self):
        m = MasterNode(host="127.0.0.1", port=_free_port(),
                       heartbeat_timeout_sec=60)
        m.start()
        time.sleep(0.1)
        try:
            status = _get(m.port, "/status")
            assert status["ha"] == "disabled"
        finally:
            m.stop()



# Integration tests: multi-master election


class TestMultiMasterElection:
    def _make_pair(self, timeout=2.0):
        """Two masters that know about each other."""
        port1 = _free_port()
        port2 = _free_port()

        ha1 = ClusterHA(
            node_id="m1",
            peers=[f"http://127.0.0.1:{port2}"],
            election_timeout_sec=timeout,
            heartbeat_interval_sec=0.2,
            sync_interval_sec=0.5,
            request_timeout_sec=0.5,
        )
        ha2 = ClusterHA(
            node_id="m2",
            peers=[f"http://127.0.0.1:{port1}"],
            election_timeout_sec=timeout,
            heartbeat_interval_sec=0.2,
            sync_interval_sec=0.5,
            request_timeout_sec=0.5,
        )
        m1 = MasterNode(host="127.0.0.1", port=port1,
                         heartbeat_timeout_sec=60, ha=ha1)
        m2 = MasterNode(host="127.0.0.1", port=port2,
                         heartbeat_timeout_sec=60, ha=ha2)
        m1.start()
        m2.start()
        time.sleep(0.3)
        return (m1, ha1), (m2, ha2)

    def test_exactly_one_leader_elected(self):
        (m1, ha1), (m2, ha2) = self._make_pair()
        try:
            # Wait for election to complete
            assert _wait_leader(ha1, timeout=8) or _wait_leader(ha2, timeout=8), \
                "No leader elected within timeout"
            time.sleep(0.3)
            leaders = sum(1 for ha in (ha1, ha2) if ha.is_leader())
            assert leaders == 1, f"Expected 1 leader, got {leaders}"
        finally:
            m1.stop(); m2.stop()

    def test_leader_follower_roles_complementary(self):
        (m1, ha1), (m2, ha2) = self._make_pair()
        try:
            deadline = time.time() + 8
            while time.time() < deadline:
                roles = {ha1.role(), ha2.role()}
                if roles == {LEADER, FOLLOWER}:
                    break
                time.sleep(0.2)
            roles = {ha1.role(), ha2.role()}
            assert roles == {LEADER, FOLLOWER}, f"Roles: {roles}"
        finally:
            m1.stop(); m2.stop()

    def test_state_replicated_to_follower(self):
        (m1, ha1), (m2, ha2) = self._make_pair()
        try:
            # Wait for stable election
            deadline = time.time() + 8
            while time.time() < deadline:
                if ha1.is_leader() != ha2.is_leader():
                    break
                time.sleep(0.2)

            leader_m   = m1 if ha1.is_leader() else m2
            follower_m = m2 if ha1.is_leader() else m1

            # Join a node on the leader
            r = _post(leader_m.port, "/nodes/join", {
                "node_id": "replicated-node",
                "address": "10.0.0.1", "port": 8080,
            })
            assert r["ok"] is True

            # Wait for sync interval to push to follower
            time.sleep(1.5)

            # Follower's local node list should now have the node
            follower_nodes = [
                n for n in follower_m.nodes()
                if n["node_id"] == "replicated-node"
            ]
            assert len(follower_nodes) == 1
        finally:
            m1.stop(); m2.stop()

    def test_follower_redirects_write(self):
        (m1, ha1), (m2, ha2) = self._make_pair()
        try:
            deadline = time.time() + 8
            while time.time() < deadline:
                if ha1.is_leader() != ha2.is_leader():
                    break
                time.sleep(0.2)

            follower_m = m2 if ha1.is_leader() else m1

            # Write to follower — should get 307 + leader_url
            result = _post(follower_m.port, "/nodes/join", {
                "node_id": "write-to-follower",
                "address": "10.0.0.1", "port": 8080,
            })
            # Response should contain leader info
            assert result.get("leader_url") is not None or \
                   result.get("ok") is False or \
                   "leader" in str(result).lower()
        finally:
            m1.stop(); m2.stop()

    def test_new_leader_after_old_stops(self):
        """
        3-node cluster: stop the leader — one of the two remaining nodes
        should elect itself.  A 2-node cluster cannot do this (1 vote is
        never a majority of 2) — that is correct Raft behaviour, not a bug.
        """
        p1, p2, p3 = _free_port(), _free_port(), _free_port()
        kw = dict(election_timeout_sec=1.5, heartbeat_interval_sec=0.2,
                  sync_interval_sec=0.5, request_timeout_sec=0.5)
        ha1 = ClusterHA("m1", peers=[f"http://127.0.0.1:{p2}",
                                      f"http://127.0.0.1:{p3}"], **kw)
        ha2 = ClusterHA("m2", peers=[f"http://127.0.0.1:{p1}",
                                      f"http://127.0.0.1:{p3}"], **kw)
        ha3 = ClusterHA("m3", peers=[f"http://127.0.0.1:{p1}",
                                      f"http://127.0.0.1:{p2}"], **kw)
        masters = []
        for port, ha in ((p1, ha1), (p2, ha2), (p3, ha3)):
            m = MasterNode(host="127.0.0.1", port=port,
                            heartbeat_timeout_sec=60, ha=ha)
            m.start()
            masters.append((m, ha))
        time.sleep(0.5)
        try:
            deadline = time.time() + 10
            while time.time() < deadline:
                if sum(1 for _, ha in masters if ha.is_leader()) == 1:
                    break
                time.sleep(0.2)
            leaders = [(m, ha) for m, ha in masters if ha.is_leader()]
            assert len(leaders) == 1, "No single leader after initial election"
            leader_m, leader_ha = leaders[0]
            followers = [(m, ha) for m, ha in masters if ha is not leader_ha]
            leader_m.stop()
            deadline = time.time() + 12
            new_leader = False
            while time.time() < deadline:
                if any(ha.is_leader() for _, ha in followers):
                    new_leader = True
                    break
                time.sleep(0.2)
            assert new_leader, "No new leader elected after old leader stopped"
        finally:
            for m, _ in masters:
                try:
                    m.stop()
                except Exception:
                    pass


    def test_vote_rpc_endpoint_works(self):
        ha = ClusterHA(node_id="solo", peers=[])
        m  = _make_master_with_ha(ha)
        try:
            r = _post(m.port, "/ha/vote", {
                "candidate_id": "other-master",
                "candidate_term": 99,
            })
            assert "vote_granted" in r
        finally:
            m.stop()

    def test_sync_rpc_endpoint_works(self):
        ha = ClusterHA(node_id="solo", peers=[])
        m  = _make_master_with_ha(ha)
        try:
            r = _post(m.port, "/ha/sync", {
                "leader_id": "master-2",
                "leader_url": "http://127.0.0.1:7071",
                "term": 1,
            })
            assert "success" in r
        finally:
            m.stop()