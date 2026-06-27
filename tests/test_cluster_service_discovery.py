"""
Tests for ServiceDiscovery (huddle_cluster_pkg.cluster_service_discovery).
"""

import json
import socket
import time
import urllib.error
import urllib.request

import pytest

from huddle_cluster_pkg.cluster_master            import MasterNode
from huddle_cluster_pkg.cluster_service_discovery import ServiceDiscovery



# Helpers

def _free_port():
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


def _delete(port, path):
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/v1{path}", method="DELETE")
    try:
        with urllib.request.urlopen(req, timeout=3) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return json.loads(e.read())


def _join(port, node_id, services=None, p=9980):
    meta = {}
    if services:
        meta["services"] = ",".join(services)
    return _post(port, "/nodes/join", {
        "node_id": node_id, "address": "127.0.0.1", "port": p,
        "metadata": meta,
    })


def _make_master(sd=None, timeout=60):
    port = _free_port()
    m = MasterNode(host="127.0.0.1", port=port,
                   heartbeat_timeout_sec=timeout,
                   service_discovery=sd)
    m.start()
    time.sleep(0.1)
    return m


def _make_sd(**kwargs) -> ServiceDiscovery:
    defaults = dict(refresh_interval_sec=0.2)
    defaults.update(kwargs)
    return ServiceDiscovery(**defaults)



# Unit tests: ServiceDiscovery API without master


class TestServiceDiscoveryUnit:
    def test_announce_and_services_list(self):
        sd = _make_sd()
        sd.announce("node-1", "api")
        sd.announce("node-2", "web")
        assert "api" in sd.services()
        assert "web" in sd.services()

    def test_announce_normalises_case(self):
        sd = _make_sd()
        sd.announce("n1", "MyService")
        assert "myservice" in sd.services()

    def test_announce_same_service_twice_deduplicates(self):
        sd = _make_sd()
        sd.announce("n1", "api")
        sd.announce("n1", "api")
        with sd._lock:
            assert len(sd._registry["api"]) == 1

    def test_deregister_returns_true_when_found(self):
        sd = _make_sd()
        sd.announce("n1", "api")
        assert sd.deregister("n1", "api") is True

    def test_deregister_returns_false_when_not_found(self):
        sd = _make_sd()
        assert sd.deregister("n1", "api") is False

    def test_alive_nodes_for_empty_service(self):
        sd = _make_sd()
        assert sd.alive_nodes_for("unknown") == []

    def test_summary_includes_service(self):
        sd = _make_sd()
        sd.announce("n1", "cache")
        summary = sd.summary()
        assert "cache" in summary["services"]



# Integration tests via MasterNode


class TestServiceDiscoveryHttp:

    def test_services_503_when_disabled(self):
        m = _make_master()
        try:
            with pytest.raises(urllib.error.HTTPError) as exc:
                _get(m.port, "/discovery/services")
            assert exc.value.code == 503
        finally:
            m.stop()

    def test_master_status_reports_sd_enabled(self):
        sd = _make_sd()
        m = _make_master(sd=sd)
        try:
            status = _get(m.port, "/status")
            assert status["service_discovery"] == "enabled"
        finally:
            m.stop()

    def test_master_status_reports_sd_disabled(self):
        m = _make_master()
        try:
            status = _get(m.port, "/status")
            assert status["service_discovery"] == "disabled"
        finally:
            m.stop()

    def test_services_empty_initially(self):
        sd = _make_sd()
        m = _make_master(sd=sd)
        try:
            data = _get(m.port, "/discovery/services")
            assert data["services"] == {}
        finally:
            m.stop()

    def test_announce_and_lookup_via_rest(self):
        sd = _make_sd()
        m = _make_master(sd=sd)
        try:
            _join(m.port, "api-1")
            r = _post(m.port, "/discovery/announce",
                       {"node_id": "api-1", "service": "api"})
            assert r["ok"] is True
            data = _get(m.port, "/discovery/services/api")
            assert data["service"] == "api"
            assert data["alive_count"] == 1
            assert data["nodes"][0]["node_id"] == "api-1"
        finally:
            m.stop()

    def test_metadata_services_picked_up_on_refresh(self):
        sd = _make_sd(refresh_interval_sec=0.2)
        m = _make_master(sd=sd)
        try:
            _join(m.port, "web-1", services=["web", "api"])
            time.sleep(0.6)   # wait for refresh cycle
            data = _get(m.port, "/discovery/services")
            assert "web" in data["services"]
            assert "api" in data["services"]
        finally:
            m.stop()

    def test_dead_node_excluded_from_results(self):
        sd = _make_sd(refresh_interval_sec=0.2)
        m = _make_master(sd=sd, timeout=0.3)
        try:
            _join(m.port, "dead-svc")
            r = _post(m.port, "/discovery/announce",
                       {"node_id": "dead-svc", "service": "cache"})
            assert r["ok"] is True
            time.sleep(0.6)   # node times out
            data = _get(m.port, "/discovery/services/cache")
            assert data["alive_count"] == 0
        finally:
            m.stop()

    def test_deregister_via_rest_delete(self):
        sd = _make_sd()
        m = _make_master(sd=sd)
        try:
            _join(m.port, "dereg-node")
            _post(m.port, "/discovery/announce",
                   {"node_id": "dereg-node", "service": "api"})
            r = _delete(m.port, "/discovery/services/api/dereg-node")
            assert r["ok"] is True
            data = _get(m.port, "/discovery/services/api")
            assert data["alive_count"] == 0
        finally:
            m.stop()

    def test_deregister_unknown_node_returns_404(self):
        sd = _make_sd()
        m = _make_master(sd=sd)
        try:
            r = _delete(m.port, "/discovery/services/api/nobody")
            assert r["ok"] is False
        finally:
            m.stop()

    def test_multiple_nodes_same_service(self):
        sd = _make_sd()
        m = _make_master(sd=sd)
        try:
            for i in range(3):
                _join(m.port, f"multi-{i}", p=9981+i)
                _post(m.port, "/discovery/announce",
                       {"node_id": f"multi-{i}", "service": "api"})
            data = _get(m.port, "/discovery/services/api")
            assert data["alive_count"] == 3
        finally:
            m.stop()

    def test_on_service_up_callback_fires(self):
        events = []
        sd = _make_sd(
            refresh_interval_sec=0.2,
            on_service_up=lambda svc, nodes: events.append(("up", svc)),
        )
        m = _make_master(sd=sd)
        try:
            _join(m.port, "cb-node")
            _post(m.port, "/discovery/announce",
                   {"node_id": "cb-node", "service": "payments"})
            time.sleep(0.6)
            assert ("up", "payments") in events
        finally:
            m.stop()

    def test_on_service_down_callback_fires(self):
        down_events = []
        sd = _make_sd(
            refresh_interval_sec=0.2,
            on_service_down=lambda svc: down_events.append(svc),
        )
        m = _make_master(sd=sd, timeout=0.3)
        try:
            _join(m.port, "dying-node")
            _post(m.port, "/discovery/announce",
                   {"node_id": "dying-node", "service": "auth"})
            time.sleep(0.3)   # let service come up first
            time.sleep(0.6)   # let node die
            time.sleep(0.5)   # give refresh a cycle to detect the down
            assert "auth" in down_events
        finally:
            m.stop()

    def test_announce_missing_node_id_returns_400(self):
        sd = _make_sd()
        m = _make_master(sd=sd)
        try:
            r = _post(m.port, "/discovery/announce",
                       {"service": "api"})
            assert r["ok"] is False
        finally:
            m.stop()

    def test_services_summary_includes_counts(self):
        sd = _make_sd()
        m = _make_master(sd=sd)
        try:
            _join(m.port, "sum-node")
            _post(m.port, "/discovery/announce",
                   {"node_id": "sum-node", "service": "worker"})
            data = _get(m.port, "/discovery/services")
            assert "worker" in data["services"]
            assert data["services"]["worker"]["alive_count"] == 1
        finally:
            m.stop()



# DNS responder tests (standalone — no MasterNode needed)


class TestDnsResponder:
    def test_dns_responds_to_a_record_query(self):
        port = _free_port()
        sd = ServiceDiscovery(dns_port=port, dns_domain="test.local",
                               refresh_interval_sec=60)

        # Manually attach a minimal stub master
        class _FakeMaster:
            _lock = __import__("threading").Lock()
            _nodes = {}
            def nodes(self):
                return [{"node_id": "n1", "address": "10.0.0.1",
                          "port": 8080, "status": "alive",
                          "metadata": {}}]

        sd.attach(_FakeMaster())
        sd.announce("n1", "api")
        time.sleep(0.1)

        try:
            # Build a minimal DNS A-record query for api.test.local
            txn_id  = b"\x12\x34"
            flags   = b"\x01\x00"   # standard query
            counts  = b"\x00\x01\x00\x00\x00\x00\x00\x00"
            # QNAME: 3 api 4 test 5 local 0
            qname   = b"\x03api\x04test\x05local\x00"
            qtype   = b"\x00\x01"   # A
            qclass  = b"\x00\x01"   # IN
            query   = txn_id + flags + counts + qname + qtype + qclass

            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(2.0)
            sock.sendto(query, ("127.0.0.1", port))
            response, _ = sock.recvfrom(512)
            sock.close()

            # Response transaction ID should match
            assert response[:2] == txn_id
            # Flags byte 2 bit 7 set = it's a response
            assert response[2] & 0x80

        finally:
            sd.stop()

    def test_dns_ignores_wrong_domain(self):
        port = _free_port()
        sd = ServiceDiscovery(dns_port=port, dns_domain="cluster.local",
                               refresh_interval_sec=60)

        class _FakeMaster:
            _lock = __import__("threading").Lock()
            _nodes = {}
            def nodes(self):
                return []

        sd.attach(_FakeMaster())
        sd.announce("n1", "api")
        time.sleep(0.1)

        try:
            # Query for api.wrong.domain — should get no response
            txn_id  = b"\xab\xcd"
            flags   = b"\x01\x00"
            counts  = b"\x00\x01\x00\x00\x00\x00\x00\x00"
            qname   = b"\x03api\x05wrong\x06domain\x00"
            qtype   = b"\x00\x01"
            qclass  = b"\x00\x01"
            query   = txn_id + flags + counts + qname + qtype + qclass

            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(0.5)
            sock.sendto(query, ("127.0.0.1", port))
            try:
                sock.recvfrom(512)
                assert False, "Should not have received a response"
            except socket.timeout:
                pass   # expected
            finally:
                sock.close()
        finally:
            sd.stop()