import hashlib
import hmac
import http.server
import json
import socket
import threading
import time
import unittest
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

from huddle_cluster_pkg.cluster_master import MasterNode
from huddle_cluster_pkg.cluster_webhooks import (
    ClusterWebhooks,
    EVENT_ALL,
    EVENT_NODE_DEAD,
    EVENT_NODE_JOINED,
    EVENT_WEBHOOK_TEST,
)


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


class _MockWebhookReceiver:
    def __init__(self):
        self.port = _free_port()
        self.received_requests: List[Dict[str, Any]] = []
        self.response_code = 200
        self.response_counts = 0
        self._httpd: Optional[http.server.HTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    def start(self):
        receiver = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, fmt, *args):
                pass

            def do_POST(self):
                receiver.response_counts += 1
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length)
                headers = {k: v for k, v in self.headers.items()}
                receiver.received_requests.append({
                    "headers": headers,
                    "body": json.loads(body.decode("utf-8")),
                    "raw_body": body,
                })
                self.send_response(receiver.response_code)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"status":"received"}')

        self._httpd = http.server.HTTPServer(("127.0.0.1", self.port), Handler)
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._thread.start()

    def stop(self):
        if self._httpd:
            self._httpd.shutdown()
            self._httpd.server_close()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}/webhook"


def _http_request(port: int, method: str, path: str, body: Any = None, api_key: str = None) -> Tuple[int, Dict[str, Any]]:
    url = f"http://127.0.0.1:{port}{path}"
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    if api_key:
        req.add_header("Authorization", f"Bearer {api_key}")
    try:
        with urllib.request.urlopen(req, timeout=3) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())


class TestClusterWebhooksUnit(unittest.TestCase):
    def test_register_and_list_and_unregister(self):
        wh = ClusterWebhooks()
        sub = wh.register(
            url="http://127.0.0.1:9090/hook",
            events=["node.joined", "node.dead"],
            secret="super-secret-key",
            headers={"X-Custom": "test"},
        )
        self.assertTrue(sub.webhook_id.startswith("wh-"))
        self.assertEqual(sub.url, "http://127.0.0.1:9090/hook")
        self.assertEqual(sub.events, ["node.joined", "node.dead"])

        # Check list with masked secret
        subs = wh.list_subscriptions(mask_secret=True)
        self.assertEqual(len(subs), 1)
        self.assertEqual(subs[0]["secret"], "********")
        self.assertEqual(subs[0]["headers"]["X-Custom"], "test")

        # Get subscription
        single = wh.get_subscription(sub.webhook_id, mask_secret=False)
        self.assertIsNotNone(single)
        self.assertEqual(single["secret"], "super-secret-key")

        # Unregister
        ok = wh.unregister(sub.webhook_id)
        self.assertTrue(ok)
        self.assertEqual(len(wh.list_subscriptions()), 0)
        self.assertFalse(wh.unregister("non-existent"))

    def test_invalid_url_and_events(self):
        wh = ClusterWebhooks()
        with self.assertRaises(ValueError):
            wh.register(url="ftp://invalid.com")
        with self.assertRaises(ValueError):
            wh.register(url="http://example.com", events=["invalid.event.name"])

    def test_event_matching(self):
        wh = ClusterWebhooks()
        sub1 = wh.register(url="http://example.com/1", events=["*"])
        sub2 = wh.register(url="http://example.com/2", events=["node.dead"])

        self.assertTrue(sub1.matches("node.joined"))
        self.assertTrue(sub1.matches("node.dead"))
        self.assertFalse(sub2.matches("node.joined"))
        self.assertTrue(sub2.matches("node.dead"))


class TestClusterWebhooksDelivery(unittest.TestCase):
    def setUp(self):
        self.receiver = _MockWebhookReceiver()
        self.receiver.start()

    def tearDown(self):
        self.receiver.stop()

    def test_async_delivery_and_hmac_signature(self):
        secret = "secret-12345"
        wh = ClusterWebhooks()
        wh.start()
        wh.register(
            url=self.receiver.url,
            events=["*"],
            secret=secret,
            headers={"X-Custom-Header": "HuddleVal"},
        )

        try:
            dispatched = wh.dispatch("node.joined", {"node_id": "test-node-1", "port": 8080})
            self.assertEqual(dispatched, 1)

            # Wait for worker delivery
            timeout = time.time() + 3.0
            while not self.receiver.received_requests and time.time() < timeout:
                time.sleep(0.05)

            self.assertEqual(len(self.receiver.received_requests), 1)
            req = self.receiver.received_requests[0]
            self.assertEqual(req["body"]["event"], "node.joined")
            self.assertEqual(req["body"]["data"]["node_id"], "test-node-1")
            self.assertEqual(req["headers"]["X-Huddle-Event"], "node.joined")
            self.assertEqual(req["headers"]["X-Custom-Header"], "HuddleVal")

            # Verify HMAC-SHA256 signature
            raw_body = req["raw_body"]
            expected_sig = "sha256=" + hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
            self.assertEqual(req["headers"]["X-Huddle-Signature"], expected_sig)

            # Check delivery history
            deliveries = wh.deliveries()
            self.assertEqual(len(deliveries), 1)
            self.assertEqual(deliveries[0]["status"], "success")
            self.assertEqual(deliveries[0]["status_code"], 200)
            self.assertEqual(deliveries[0]["attempts"], 1)
        finally:
            wh.stop()

    def test_retry_on_server_error(self):
        self.receiver.response_code = 500
        wh = ClusterWebhooks(max_retries=2)
        wh.start()
        wh.register(url=self.receiver.url, events=["*"])

        try:
            wh.dispatch("node.dead", {"node_id": "failed-node"})

            # Wait for retries to complete
            timeout = time.time() + 3.0
            while len(self.receiver.received_requests) < 2 and time.time() < timeout:
                time.sleep(0.05)

            # Should have made 2 attempts
            self.assertEqual(len(self.receiver.received_requests), 2)

            time.sleep(0.1)
            deliveries = wh.deliveries()
            self.assertEqual(len(deliveries), 1)
            self.assertEqual(deliveries[0]["status"], "failed")
            self.assertEqual(deliveries[0]["attempts"], 2)
            self.assertEqual(deliveries[0]["status_code"], 500)
        finally:
            wh.stop()


class TestMasterNodeWebhooksIntegration(unittest.TestCase):
    def setUp(self):
        self.port = _free_port()
        self.receiver = _MockWebhookReceiver()
        self.receiver.start()

        self.webhooks = ClusterWebhooks()
        self.master = MasterNode(
            host="127.0.0.1",
            port=self.port,
            webhooks=self.webhooks,
            api_keys={
                "admin-key": "admin",
                "viewer-key": "viewer",
            },
        )
        self.master.start()
        time.sleep(0.1)

    def tearDown(self):
        self.master.stop()
        self.receiver.stop()

    def test_webhooks_rest_api_crud_and_rbac(self):
        # 1. Viewer key can read webhooks list
        code, body = _http_request(self.port, "GET", "/v1/webhooks", api_key="viewer-key")
        self.assertEqual(code, 200)
        self.assertEqual(body["count"], 0)

        # 2. Viewer key cannot register (lacks webhooks:write)
        code, body = _http_request(
            self.port, "POST", "/v1/webhooks",
            body={"url": self.receiver.url, "events": ["*"]},
            api_key="viewer-key",
        )
        self.assertEqual(code, 403)

        # 3. Admin key can register webhook
        code, body = _http_request(
            self.port, "POST", "/v1/webhooks",
            body={"url": self.receiver.url, "events": ["*"], "secret": "sec123"},
            api_key="admin-key",
        )
        self.assertEqual(code, 201)
        self.assertTrue(body["ok"])
        wh_id = body["webhook"]["webhook_id"]
        self.assertEqual(body["webhook"]["secret"], "********")

        # 4. Get single webhook
        code, body = _http_request(self.port, "GET", f"/v1/webhooks/{wh_id}", api_key="viewer-key")
        self.assertEqual(code, 200)
        self.assertEqual(body["webhook"]["webhook_id"], wh_id)

        # 5. Dispatch synthetic test ping
        code, body = _http_request(
            self.port, "POST", "/v1/webhooks/test",
            body={"webhook_id": wh_id},
            api_key="admin-key",
        )
        self.assertEqual(code, 200)
        self.assertEqual(body["dispatched_to"], 1)

        # Wait for receiver to catch test ping
        timeout = time.time() + 3.0
        while not self.receiver.received_requests and time.time() < timeout:
            time.sleep(0.05)

        self.assertEqual(len(self.receiver.received_requests), 1)
        self.assertEqual(self.receiver.received_requests[0]["body"]["event"], EVENT_WEBHOOK_TEST)

        # 6. Check deliveries endpoint
        code, body = _http_request(self.port, "GET", "/v1/webhooks/deliveries", api_key="viewer-key")
        self.assertEqual(code, 200)
        self.assertGreaterEqual(body["count"], 1)
        self.assertEqual(body["deliveries"][0]["status"], "success")

        # 7. Delete webhook
        code, body = _http_request(self.port, "DELETE", f"/v1/webhooks/{wh_id}", api_key="admin-key")
        self.assertEqual(code, 200)
        self.assertTrue(body["ok"])

        # Check list is empty
        code, body = _http_request(self.port, "GET", "/v1/webhooks", api_key="viewer-key")
        self.assertEqual(body["count"], 0)

    def test_lifecycle_event_hook_triggers_webhook(self):
        # Register receiver URL
        self.webhooks.register(url=self.receiver.url, events=[EVENT_NODE_JOINED])

        # Simulate agent join
        code, body = _http_request(
            self.port, "POST", "/v1/nodes/join",
            body={"node_id": "test-live-agent", "address": "127.0.0.1", "port": 8888},
            api_key="admin-key",
        )
        self.assertEqual(code, 200)

        # Wait for webhook delivery
        timeout = time.time() + 3.0
        while not self.receiver.received_requests and time.time() < timeout:
            time.sleep(0.05)

        self.assertGreaterEqual(len(self.receiver.received_requests), 1)
        req = self.receiver.received_requests[0]
        self.assertEqual(req["body"]["event"], EVENT_NODE_JOINED)
        self.assertEqual(req["body"]["data"]["node_id"], "test-live-agent")


if __name__ == "__main__":
    unittest.main()
