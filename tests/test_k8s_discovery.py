"""
Tests for K8sDiscovery (v1.4.0).

All tests mock the kubernetes client so no real K8s cluster is needed.
"""

import threading
import time
import unittest
from unittest.mock import MagicMock, patch, PropertyMock

from huddle_cluster import create_cluster
from huddle_cluster_pkg.discovery_k8s import K8sDiscovery


def _make_endpoint(ips: list):
    """Build a fake kubernetes Endpoints object."""
    if not ips:
        ep = MagicMock()
        ep.subsets = None
        return ep

    addrs = []
    for ip in ips:
        addr = MagicMock()
        addr.ip = ip
        addrs.append(addr)

    subset = MagicMock()
    subset.addresses = addrs

    ep = MagicMock()
    ep.subsets = [subset]
    return ep


def _patch_k8s(ips: list):
    """
    Context manager / decorator that patches the kubernetes API to return
    the given list of ready IPs, and also patches config loading.
    """
    ep_list = MagicMock()
    ep_list.items = [_make_endpoint(ips)]

    api_instance = MagicMock()
    api_instance.list_namespaced_endpoints.return_value = ep_list

    patches = [
        patch("kubernetes.client.CoreV1Api", return_value=api_instance),
        patch("kubernetes.config.load_incluster_config", side_effect=Exception("not in cluster")),
        patch("kubernetes.config.load_kube_config"),
    ]
    return patches, api_instance


class TestK8sDiscoveryInit(unittest.TestCase):

    def test_raises_without_kubernetes_package(self):
        import sys
        with patch.dict(sys.modules, {"kubernetes": None}):
            # Force reimport
            import importlib
            import huddle_cluster_pkg.discovery_k8s as mod
            importlib.reload(mod)
            with self.assertRaises(ImportError):
                mod.K8sDiscovery(
                    namespace="default",
                    label_selector="app=test",
                    port=8080,
                )
            importlib.reload(mod)  # restore

    def test_init_sets_attributes(self):
        d = K8sDiscovery(
            namespace="production",
            label_selector="app=api",
            port=8080,
            server_prefix="svc",
            resync_sec=15.0,
        )
        self.assertEqual(d.namespace,      "production")
        self.assertEqual(d.label_selector, "app=api")
        self.assertEqual(d.port,           8080)
        self.assertEqual(d.server_prefix,  "svc")
        self.assertEqual(d.resync_sec,     15.0)

    def test_raises_if_started_twice(self):
        cluster = create_cluster([], min_inner_size=1)
        cluster.start()
        d = K8sDiscovery("default", "app=x", 8080)

        patches, _ = _patch_k8s([])
        for p in patches:
            p.start()
        try:
            d.start(cluster)
            with self.assertRaises(RuntimeError):
                d.start(cluster)
        finally:
            d.stop()
            cluster.stop()
            for p in patches:
                p.stop()


class TestSync(unittest.TestCase):
    """_sync() adds and removes servers based on K8s state."""

    def setUp(self):
        self.cluster = create_cluster([], min_inner_size=1)
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def _make_discovery(self) -> K8sDiscovery:
        return K8sDiscovery(
            namespace="default",
            label_selector="app=api",
            port=8080,
            in_cluster=False,
        )

    def _with_k8s(self, ips, fn):
        patches, api = _patch_k8s(ips)
        for p in patches:
            p.start()
        try:
            fn(api)
        finally:
            for p in patches:
                p.stop()

    def test_adds_servers_from_k8s(self):
        d = self._make_discovery()

        def run(api):
            d._load_k8s_config()
            d._cluster = self.cluster
            d._sync()
            all_ids = {s.id for s in self.cluster.all_servers()}
            self.assertTrue(
                any("10-0-0-1" in sid for sid in all_ids),
                f"Server for 10.0.0.1 not found. Servers: {all_ids}"
            )

        self._with_k8s(["10.0.0.1", "10.0.0.2"], run)

    def test_removes_servers_no_longer_in_k8s(self):
        d = self._make_discovery()
        d._cluster = self.cluster

        def add_two(api):
            d._load_k8s_config()
            d._sync()

        def remove_one(api):
            d._sync()

        # Add two servers
        self._with_k8s(["10.0.0.1", "10.0.0.2"], add_two)
        self.assertEqual(len(self.cluster.all_servers()), 2)

        # One disappears from K8s
        self._with_k8s(["10.0.0.1"], remove_one)
        all_ids = {s.id for s in self.cluster.all_servers()}
        self.assertEqual(len(all_ids), 1)
        self.assertTrue(any("10-0-0-1" in sid for sid in all_ids))

    def test_no_change_when_same_ips(self):
        d = self._make_discovery()
        d._cluster = self.cluster

        def run(api):
            d._load_k8s_config()
            d._sync()
            count_before = len(self.cluster.all_servers())
            d._sync()   # second sync with same IPs
            count_after  = len(self.cluster.all_servers())
            self.assertEqual(count_before, count_after)

        self._with_k8s(["10.0.0.1"], run)

    def test_empty_k8s_removes_all(self):
        d = self._make_discovery()
        d._cluster = self.cluster

        def add(api):
            d._load_k8s_config()
            d._sync()

        def remove_all(api):
            d._sync()

        self._with_k8s(["10.0.0.1"], add)
        self.assertEqual(len(self.cluster.all_servers()), 1)

        self._with_k8s([], remove_all)
        self.assertEqual(len(self.cluster.all_servers()), 0)


class TestServerId(unittest.TestCase):

    def test_server_id_format(self):
        d = K8sDiscovery("default", "app=x", 8080, server_prefix="k8s")
        self.assertEqual(d._server_id("10.0.0.1"), "k8s-10-0-0-1-8080")

    def test_custom_prefix(self):
        d = K8sDiscovery("default", "app=x", 9090, server_prefix="prod")
        self.assertEqual(d._server_id("192.168.1.5"), "prod-192-168-1-5-9090")


class TestKnownServers(unittest.TestCase):

    def setUp(self):
        self.cluster = create_cluster([], min_inner_size=1)
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_known_servers_empty_initially(self):
        d = K8sDiscovery("default", "app=x", 8080)
        self.assertEqual(d.known_servers(), {})

    def test_known_servers_after_add(self):
        d = K8sDiscovery("default", "app=x", 8080)
        d._cluster = self.cluster
        patches, _ = _patch_k8s(["10.0.0.1"])
        for p in patches:
            p.start()
        try:
            d._load_k8s_config()
            d._sync()
        finally:
            for p in patches:
                p.stop()

        known = d.known_servers()
        self.assertIn("10.0.0.1", known)

    def test_known_servers_returns_copy(self):
        d = K8sDiscovery("default", "app=x", 8080)
        copy1 = d.known_servers()
        copy1["injected"] = "bad"
        self.assertNotIn("injected", d.known_servers())


class TestStatus(unittest.TestCase):

    def test_status_keys(self):
        d = K8sDiscovery("prod", "app=api", 8080)
        status = d.status()
        for key in ("running", "namespace", "label_selector",
                    "port", "known_count", "known_servers"):
            self.assertIn(key, status)

    def test_status_not_running_initially(self):
        d = K8sDiscovery("default", "app=x", 8080)
        self.assertFalse(d.status()["running"])


class TestStopSafety(unittest.TestCase):

    def test_stop_when_not_started(self):
        d = K8sDiscovery("default", "app=x", 8080)
        d.stop()  # must not raise

    def test_stop_twice(self):
        cluster = create_cluster([], min_inner_size=1)
        cluster.start()
        d = K8sDiscovery("default", "app=x", 8080, resync_sec=0.1)

        patches, _ = _patch_k8s([])
        for p in patches:
            p.start()
        try:
            d.start(cluster)
            d.stop()
            d.stop()  # second stop must not raise
        finally:
            cluster.stop()
            for p in patches:
                p.stop()


class TestInClusterConfig(unittest.TestCase):

    def test_in_cluster_true_loads_incluster(self):
        d = K8sDiscovery("default", "app=x", 8080, in_cluster=True)
        with patch("kubernetes.config.load_incluster_config") as mock_ic:
            with patch("kubernetes.config.load_kube_config") as mock_kc:
                d._load_k8s_config()
                mock_ic.assert_called_once()
                mock_kc.assert_not_called()

    def test_in_cluster_false_loads_kubeconfig(self):
        d = K8sDiscovery("default", "app=x", 8080, in_cluster=False)
        with patch("kubernetes.config.load_incluster_config") as mock_ic:
            with patch("kubernetes.config.load_kube_config") as mock_kc:
                d._load_k8s_config()
                mock_ic.assert_not_called()
                mock_kc.assert_called_once()


class TestServerTags(unittest.TestCase):

    def setUp(self):
        self.cluster = create_cluster([], min_inner_size=1)
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_server_has_k8s_source_tag(self):
        d = K8sDiscovery("default", "app=x", 8080)
        d._cluster = self.cluster
        patches, _ = _patch_k8s(["10.0.0.1"])
        for p in patches:
            p.start()
        try:
            d._load_k8s_config()
            d._sync()
        finally:
            for p in patches:
                p.stop()

        servers = {s.id: s for s in self.cluster.all_servers()}
        sid = d._server_id("10.0.0.1")
        self.assertIn(sid, servers)
        self.assertEqual(servers[sid].tags.get("source"), "k8s")
        self.assertEqual(servers[sid].tags.get("ip"), "10.0.0.1")


if __name__ == "__main__":
    unittest.main()