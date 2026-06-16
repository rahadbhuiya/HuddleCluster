"""
Tests for AgentNode (huddle_cluster_pkg.cluster_agent).
"""

import json
import threading
import time
import urllib.request

import pytest

from huddle_cluster_pkg.cluster_master import MasterNode
from huddle_cluster_pkg.cluster_agent  import AgentNode



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



# Fixtures


@pytest.fixture
def master():
    port = _free_port()
    m = MasterNode(host="127.0.0.1", port=port, heartbeat_timeout_sec=60)
    m.start()
    time.sleep(0.1)
    yield m
    m.stop()



# Tests — init validation


class TestAgentInit:
    def test_empty_node_id_raises(self, master):
        with pytest.raises(ValueError):
            AgentNode(node_id="",
                      master_url=f"http://127.0.0.1:{master.port}",
                      port=8080)

    def test_whitespace_node_id_raises(self, master):
        with pytest.raises(ValueError):
            AgentNode(node_id="   ",
                      master_url=f"http://127.0.0.1:{master.port}",
                      port=8080)

    def test_bad_port_zero_raises(self, master):
        with pytest.raises(ValueError):
            AgentNode(node_id="n1",
                      master_url=f"http://127.0.0.1:{master.port}",
                      port=0)

    def test_bad_port_too_high_raises(self, master):
        with pytest.raises(ValueError):
            AgentNode(node_id="n1",
                      master_url=f"http://127.0.0.1:{master.port}",
                      port=70000)

    def test_missing_master_url_raises(self):
        with pytest.raises(ValueError):
            AgentNode(node_id="n1", master_url="", port=8080)



# Tests — join


class TestAgentJoin:
    def test_agent_joins_master(self, master):
        agent = AgentNode(
            node_id="a1",
            master_url=f"http://127.0.0.1:{master.port}",
            port=8080,
            address="127.0.0.1",
        )
        agent.start()
        time.sleep(0.2)
        try:
            assert agent.joined is True
            nodes = _get(master.port, "/nodes")["nodes"]
            assert any(n["node_id"] == "a1" for n in nodes)
        finally:
            agent.stop()

    def test_agent_address_stored(self, master):
        agent = AgentNode(
            node_id="a2",
            master_url=f"http://127.0.0.1:{master.port}",
            port=8082,
            address="10.0.0.5",
        )
        agent.start()
        time.sleep(0.2)
        try:
            node = _get(master.port, "/nodes/a2")
            assert node["address"] == "10.0.0.5"
            assert node["port"]    == 8082
        finally:
            agent.stop()

    def test_agent_metadata_stored(self, master):
        agent = AgentNode(
            node_id="a3",
            master_url=f"http://127.0.0.1:{master.port}",
            port=8083,
            address="127.0.0.1",
            metadata={"region": "us-east", "role": "lb"},
        )
        agent.start()
        time.sleep(0.2)
        try:
            node = _get(master.port, "/nodes/a3")
            assert node["metadata"]["region"] == "us-east"
            assert node["metadata"]["role"]   == "lb"
        finally:
            agent.stop()

    def test_agent_node_id_stripped(self, master):
        agent = AgentNode(
            node_id="  a4  ",
            master_url=f"http://127.0.0.1:{master.port}",
            port=8084,
            address="127.0.0.1",
        )
        assert agent.node_id == "a4"
        agent.start()
        time.sleep(0.2)
        try:
            nodes = _get(master.port, "/nodes")["nodes"]
            assert any(n["node_id"] == "a4" for n in nodes)
        finally:
            agent.stop()

    def test_agent_fails_gracefully_when_master_down(self):
        """No exception raised — agent starts in degraded mode."""
        agent = AgentNode(
            node_id="a-offline",
            master_url="http://127.0.0.1:1",   # nothing listening
            port=8090,
            address="127.0.0.1",
        )
        agent.start(retry=1)
        time.sleep(0.1)
        try:
            assert agent.joined is False
        finally:
            agent.stop()

    def test_double_start_raises(self, master):
        agent = AgentNode(
            node_id="a5",
            master_url=f"http://127.0.0.1:{master.port}",
            port=8085,
            address="127.0.0.1",
        )
        agent.start()
        try:
            with pytest.raises(RuntimeError):
                agent.start()
        finally:
            agent.stop()

    def test_start_returns_quickly_even_when_master_dead(self):
        """
        Regression test: start() must return almost immediately regardless
        of how long the underlying socket takes to detect "unreachable" —
        even with a long request_timeout_sec, start() itself must not block.
        """
        agent = AgentNode(
            node_id="a-slow-fail",
            master_url="http://127.0.0.1:1",   # nothing listening
            port=8091,
            address="127.0.0.1",
            request_timeout_sec=5.0,           # deliberately long
        )
        t0 = time.time()
        agent.start(retry=1)
        elapsed = time.time() - t0
        try:
            assert elapsed < 0.5, (
                f"start() blocked for {elapsed:.2f}s — should return almost "
                f"instantly regardless of request_timeout_sec"
            )
        finally:
            agent.stop()

    def test_restart_after_stop(self, master):
        """start() -> stop() -> start() again on the same instance must work."""
        agent = AgentNode(
            node_id="a-restart",
            master_url=f"http://127.0.0.1:{master.port}",
            port=8086,
            address="127.0.0.1",
            heartbeat_interval_sec=0.2,
        )
        agent.start()
        time.sleep(0.2)
        agent.stop()
        time.sleep(0.1)

        agent.start()
        time.sleep(0.2)
        try:
            assert agent.joined is True
        finally:
            agent.stop()



# Tests — heartbeat


class TestAgentHeartbeat:
    def test_heartbeats_increment(self, master):
        agent = AgentNode(
            node_id="hb-a1",
            master_url=f"http://127.0.0.1:{master.port}",
            port=8100,
            address="127.0.0.1",
            heartbeat_interval_sec=0.3,
        )
        agent.start()
        time.sleep(1.5)
        try:
            assert agent.heartbeat_count >= 2
            node = _get(master.port, "/nodes/hb-a1")
            assert node["heartbeat_count"] >= 2
        finally:
            agent.stop()

    def test_last_heartbeat_ok_set(self, master):
        agent = AgentNode(
            node_id="hb-a2",
            master_url=f"http://127.0.0.1:{master.port}",
            port=8101,
            address="127.0.0.1",
            heartbeat_interval_sec=0.3,
        )
        agent.start()
        time.sleep(0.8)
        try:
            assert agent._last_hb_ok is not None
            assert time.time() - agent._last_hb_ok < 5
        finally:
            agent.stop()

    def test_heartbeat_updates_master_metrics(self, master):
        agent = AgentNode(
            node_id="hb-a3",
            master_url=f"http://127.0.0.1:{master.port}",
            port=8102,
            address="127.0.0.1",
            heartbeat_interval_sec=0.3,
        )
        agent.start()
        time.sleep(0.8)
        try:
            node = _get(master.port, "/nodes/hb-a3")
            assert "metrics" in node   # dict is present (may be empty without cluster)
        finally:
            agent.stop()



# Tests — leave


class TestAgentLeave:
    def test_agent_deregisters_on_stop(self, master):
        agent = AgentNode(
            node_id="lv-a1",
            master_url=f"http://127.0.0.1:{master.port}",
            port=8110,
            address="127.0.0.1",
        )
        agent.start()
        time.sleep(0.2)
        assert agent.joined
        agent.stop()
        time.sleep(0.1)
        nodes = _get(master.port, "/nodes")["nodes"]
        assert all(n["node_id"] != "lv-a1" for n in nodes)

    def test_stop_when_not_started(self, master):
        """stop() on an agent that was never started must not raise."""
        agent = AgentNode(
            node_id="lv-a2",
            master_url=f"http://127.0.0.1:{master.port}",
            port=8111,
            address="127.0.0.1",
        )
        agent.stop()   # should be a no-op

    def test_running_false_after_stop(self, master):
        agent = AgentNode(
            node_id="lv-a3",
            master_url=f"http://127.0.0.1:{master.port}",
            port=8112,
            address="127.0.0.1",
        )
        agent.start()
        time.sleep(0.1)
        agent.stop()
        assert agent._running is False



# Tests — status dict


class TestAgentStatus:
    def test_status_dict_shape(self, master):
        agent = AgentNode(
            node_id="st-a1",
            master_url=f"http://127.0.0.1:{master.port}",
            port=8120,
            address="127.0.0.1",
            metadata={"env": "test"},
        )
        agent.start()
        time.sleep(0.2)
        try:
            s = agent.status()
            assert s["node_id"]          == "st-a1"
            assert s["port"]             == 8120
            assert s["joined"]           is True
            assert s["running"]          is True
            assert s["metadata"]["env"]  == "test"
            assert "heartbeat_count"     in s
            assert "consecutive_failures" in s
        finally:
            agent.stop()

    def test_custom_request_timeout_reflected(self, master):
        agent = AgentNode(
            node_id="st-a2",
            master_url=f"http://127.0.0.1:{master.port}",
            port=8121,
            address="127.0.0.1",
            request_timeout_sec=1.5,
        )
        agent.start()
        time.sleep(0.2)
        try:
            assert agent.status()["request_timeout_sec"] == 1.5
        finally:
            agent.stop()



# Tests — callbacks


class TestAgentCallbacks:
    def test_unreachable_callback_fires(self):
        """
        on_master_unreachable is called on the first consecutive heartbeat
        failure.  Use threading.Event so the assertion doesn't depend on
        arbitrary sleep timing.
        """
        event = threading.Event()

        # No master running on this port.
        dead_port = _free_port()

        agent = AgentNode(
            node_id="cb-a1",
            master_url=f"http://127.0.0.1:{dead_port}",
            port=8130,
            address="127.0.0.1",
            heartbeat_interval_sec=0.2,
            request_timeout_sec=0.3,
            on_master_unreachable=event.set,
        )
        agent.start(retry=1)
        try:
            fired = event.wait(timeout=2.0)
            assert fired, "on_master_unreachable was not called within 2s"
        finally:
            agent.stop()

    def test_unreachable_callback_fires_only_once_per_outage(self):
        """Callback is called exactly once per outage (consecutive=1 check)."""
        counter = {"n": 0}
        lock    = threading.Lock()

        dead_port = _free_port()

        def cb():
            with lock:
                counter["n"] += 1

        agent = AgentNode(
            node_id="cb-a2",
            master_url=f"http://127.0.0.1:{dead_port}",
            port=8131,
            address="127.0.0.1",
            heartbeat_interval_sec=0.2,
            request_timeout_sec=0.3,
            on_master_unreachable=cb,
        )
        agent.start(retry=1)
        time.sleep(1.2)    # enough for ~6 heartbeat attempts
        try:
            with lock:
                assert counter["n"] == 1, (
                    f"Expected callback exactly once, got {counter['n']}"
                )
        finally:
            agent.stop()

    def test_recovered_callback_fires(self):
        """
        on_recovered is called when heartbeats resume after a gap.
        We simulate this by starting master AFTER the agent has failed once.
        """
        recovered_event = threading.Event()

        # Start master on a known port
        port = _free_port()

        # Agent starts BEFORE master — first heartbeats will fail
        agent = AgentNode(
            node_id="cb-a3",
            master_url=f"http://127.0.0.1:{port}",
            port=8132,
            address="127.0.0.1",
            heartbeat_interval_sec=0.3,
            request_timeout_sec=0.3,
            on_recovered=recovered_event.set,
        )
        agent.start(retry=1)

        # Let one heartbeat fail
        time.sleep(0.5)

        # Now bring up the master
        m = MasterNode(host="127.0.0.1", port=port, heartbeat_timeout_sec=60)
        m.start()
        time.sleep(0.1)

        try:
            fired = recovered_event.wait(timeout=3.0)
            assert fired, "on_recovered was not called within 3s"
        finally:
            agent.stop()
            m.stop()



# Tests — multiple agents
class TestMultipleAgents:
    def test_three_agents_all_registered(self, master):
        agents = [
            AgentNode(
                node_id=f"multi-{i}",
                master_url=f"http://127.0.0.1:{master.port}",
                port=8200 + i,
                address="127.0.0.1",
            )
            for i in range(3)
        ]
        for a in agents:
            a.start()
        time.sleep(0.3)
        try:
            nodes = _get(master.port, "/nodes")["nodes"]
            ids   = {n["node_id"] for n in nodes}
            assert {"multi-0", "multi-1", "multi-2"}.issubset(ids)
        finally:
            for a in agents:
                a.stop()

    def test_agents_leave_independently(self, master):
        agents = [
            AgentNode(
                node_id=f"ind-{i}",
                master_url=f"http://127.0.0.1:{master.port}",
                port=8210 + i,
                address="127.0.0.1",
            )
            for i in range(3)
        ]
        for a in agents:
            a.start()
        time.sleep(0.2)

        # Stop middle agent
        agents[1].stop()
        time.sleep(0.1)

        try:
            nodes = _get(master.port, "/nodes")["nodes"]
            ids   = {n["node_id"] for n in nodes}
            assert "ind-0" in ids
            assert "ind-1" not in ids
            assert "ind-2" in ids
        finally:
            agents[0].stop()
            agents[2].stop()