import json
import socket
import time
import unittest
import urllib.error
import urllib.request
from typing import Tuple, Dict, Any

from huddle_cluster_pkg.cluster_master import MasterNode
from huddle_cluster_pkg.cluster_ha import ClusterHA, LEADER, FOLLOWER, CANDIDATE


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def _http_get(port: int, path: str, api_key: str = None) -> Tuple[int, Dict[str, Any]]:
    url = f"http://127.0.0.1:{port}{path}"
    req = urllib.request.Request(url)
    if api_key:
        req.add_header("Authorization", f"Bearer {api_key}")
    try:
        with urllib.request.urlopen(req, timeout=3) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())


class TestCloudProbes(unittest.TestCase):
    def setUp(self):
        self.port = _free_port()
        self.master = MasterNode(
            host="127.0.0.1",
            port=self.port,
            heartbeat_timeout_sec=60,
            api_keys={"secret-token": "admin"},
        )
        self.master.start()
        time.sleep(0.1)

    def tearDown(self):
        self.master.stop()

    def test_root_liveness_probes_no_auth(self):
        code, body = _http_get(self.port, "/healthz")
        self.assertEqual(code, 200)
        self.assertEqual(body.get("status"), "ok")

        code, body = _http_get(self.port, "/livez")
        self.assertEqual(code, 200)
        self.assertEqual(body.get("status"), "ok")

    def test_root_readiness_probe_no_auth(self):
        code, body = _http_get(self.port, "/readyz")
        self.assertEqual(code, 200)
        self.assertEqual(body.get("status"), "ready")

    def test_v1_probe_aliases_and_legacy_health(self):
        code, body = _http_get(self.port, "/v1/healthz")
        self.assertEqual(code, 200)
        self.assertEqual(body.get("status"), "ok")

        code, body = _http_get(self.port, "/v1/livez")
        self.assertEqual(code, 200)
        self.assertEqual(body.get("status"), "ok")

        code, body = _http_get(self.port, "/v1/readyz")
        self.assertEqual(code, 200)
        self.assertEqual(body.get("status"), "ready")

        code, body = _http_get(self.port, "/v1/health")
        self.assertEqual(code, 200)
        self.assertEqual(body.get("status"), "ok")

    def test_openapi_spec_documents_probes(self):
        spec = self.master.openapi_spec()
        paths = spec.get("paths", {})
        self.assertIn("/health", paths)
        self.assertIn("/healthz", paths)
        self.assertIn("/livez", paths)
        self.assertIn("/readyz", paths)

    def test_master_is_ready_method(self):
        self.assertTrue(self.master.is_ready())
        self.master.stop()
        self.assertFalse(self.master.is_ready())


class TestHAClusterReadiness(unittest.TestCase):
    def test_ha_readiness_states(self):
        ha = ClusterHA(node_id="node-1", peers=["http://127.0.0.1:7071"])
        self.assertFalse(ha.is_ready())

        ha._running = True

        ha._role = LEADER
        self.assertTrue(ha.is_ready())

        ha._role = CANDIDATE
        self.assertFalse(ha.is_ready())

        ha._role = FOLLOWER
        ha._leader_id = None
        self.assertFalse(ha.is_ready())

        ha._leader_id = "node-2"
        self.assertTrue(ha.is_ready())

        ha_solo = ClusterHA(node_id="solo-1", peers=[])
        ha_solo._running = True
        ha_solo._role = FOLLOWER
        ha_solo._leader_id = None
        self.assertTrue(ha_solo.is_ready())

    def test_master_delegates_to_ha_readiness(self):
        port = _free_port()
        ha = ClusterHA(node_id="m1", peers=["http://127.0.0.1:7072"])
        master = MasterNode(host="127.0.0.1", port=port, ha=ha)
        master.start()
        time.sleep(0.1)

        try:
            ha._role = CANDIDATE
            self.assertFalse(master.is_ready())
            code, body = _http_get(port, "/readyz")
            self.assertEqual(code, 503)
            self.assertEqual(body.get("status"), "not_ready")

            ha._role = LEADER
            self.assertTrue(master.is_ready())
            code, body = _http_get(port, "/readyz")
            self.assertEqual(code, 200)
            self.assertEqual(body.get("status"), "ready")
        finally:
            master.stop()


if __name__ == "__main__":
    unittest.main()
