"""
HuddleCluster Kubernetes Service Discovery (v1.4.0).

Automatically adds and removes servers as Kubernetes pods come and go.
Requires: pip install kubernetes

Usage:
    from huddle_cluster import create_cluster
    from huddle_cluster_pkg.discovery_k8s import K8sDiscovery

    discovery = K8sDiscovery(
        namespace="production",
        label_selector="app=api-server",
        port=8080,
    )
    cluster = create_cluster([], min_inner_size=1)
    cluster.start()
    discovery.start(cluster)   # begins watching K8s API
    # ... serve traffic ...
    discovery.stop()
    cluster.stop()
"""

from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from huddle_cluster import HuddleCluster, Server

log = logging.getLogger("huddle.k8s")


class K8sDiscovery:
    """
    Watches a Kubernetes Endpoints resource and keeps a HuddleCluster
    in sync with the ready pod addresses.

    When a pod becomes ready its address is added to the cluster.
    When a pod is removed or becomes not-ready its address is removed.

    Works both inside a cluster (in-cluster config) and outside
    (kubeconfig / KUBECONFIG env var).

    Args:
        namespace:      Kubernetes namespace to watch (e.g. "production").
        label_selector: Label selector for the target pods
                        (e.g. "app=api-server,tier=backend").
        port:           Container port to route traffic to.
        server_prefix:  Prefix for generated server IDs. Default "k8s".
        resync_sec:     Full resync interval in seconds. Default 30.
        kubeconfig:     Optional path to kubeconfig file. None = auto-detect.
        in_cluster:     Force in-cluster config. None = auto-detect.
    """

    def __init__(
        self,
        namespace:      str,
        label_selector: str,
        port:           int,
        server_prefix:  str   = "k8s",
        resync_sec:     float = 30.0,
        kubeconfig:     Optional[str] = None,
        in_cluster:     Optional[bool] = None,
    ) -> None:
        try:
            import kubernetes  # noqa: F401
        except ImportError:
            raise ImportError(
                "kubernetes package is required for K8sDiscovery. "
                "Install it with: pip install kubernetes"
            )

        self.namespace      = namespace
        self.label_selector = label_selector
        self.port           = port
        self.server_prefix  = server_prefix
        self.resync_sec     = resync_sec
        self.kubeconfig     = kubeconfig
        self.in_cluster     = in_cluster

        self._cluster:    Optional[HuddleCluster] = None
        self._running:    bool                    = False
        self._thread:     Optional[threading.Thread] = None
        # known_ips tracks IPs currently registered in the cluster
        # so we can compute add/remove deltas on each resync.
        self._known_ips:  dict = {}   # {ip: server_id}
        self._lock        = threading.Lock()

    
    # Lifecycle
    

    def start(self, cluster: "HuddleCluster") -> None:
        """
        Attach to a running HuddleCluster and begin watching the K8s API.

        Args:
            cluster: A started HuddleCluster instance.
        """
        if self._running:
            raise RuntimeError("K8sDiscovery is already running.")
        self._cluster = cluster
        self._running = True
        self._thread  = threading.Thread(
            target=self._watch_loop,
            name="huddle-k8s-discovery",
            daemon=True,
        )
        self._thread.start()
        log.info(
            f"K8sDiscovery started (namespace={self.namespace!r}, "
            f"selector={self.label_selector!r}, port={self.port})"
        )

    def stop(self) -> None:
        """Stop the discovery watcher. Safe to call multiple times."""
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5.0)
        self._thread = None
        log.info("K8sDiscovery stopped")

    
    # Internal watch loop
    

    def _load_k8s_config(self) -> None:
        from kubernetes import config
        if self.in_cluster is True:
            config.load_incluster_config()
            return
        if self.in_cluster is False:
            config.load_kube_config(config_file=self.kubeconfig)
            return
        # Auto-detect: try in-cluster first, fall back to kubeconfig
        try:
            config.load_incluster_config()
        except Exception:
            config.load_kube_config(config_file=self.kubeconfig)

    def _fetch_ready_ips(self) -> list:
        """
        Query the K8s API for all ready pod IPs matching label_selector.

        Returns a list of IP strings for pods in Ready state.
        """
        from kubernetes import client as k8s_client
        v1 = k8s_client.CoreV1Api()
        endpoints = v1.list_namespaced_endpoints(
            namespace=self.namespace,
            label_selector=self.label_selector,
        )
        ips = []
        for ep in endpoints.items:
            if not ep.subsets:
                continue
            for subset in ep.subsets:
                # subsets.addresses = ready pods
                if subset.addresses:
                    for addr in subset.addresses:
                        ips.append(addr.ip)
        return ips

    def _watch_loop(self) -> None:
        """Periodically sync cluster servers with K8s ready pod IPs."""
        try:
            self._load_k8s_config()
        except Exception as exc:
            log.error(f"K8sDiscovery: failed to load config: {exc}")
            return

        while self._running:
            try:
                self._sync()
            except Exception as exc:
                log.warning(f"K8sDiscovery: sync error: {exc}")

            # Sleep in short increments so stop() is responsive
            deadline = time.monotonic() + self.resync_sec
            while self._running and time.monotonic() < deadline:
                time.sleep(0.5)

    def _sync(self) -> None:
        """Compute and apply the delta between K8s state and cluster state."""
        current_ips = set(self._fetch_ready_ips())

        with self._lock:
            known_ips = set(self._known_ips.keys())

        added   = current_ips - known_ips
        removed = known_ips - current_ips

        for ip in added:
            self._add_server(ip)

        for ip in removed:
            self._remove_server(ip)

    def _server_id(self, ip: str) -> str:
        safe = ip.replace(".", "-")
        return f"{self.server_prefix}-{safe}-{self.port}"

    def _add_server(self, ip: str) -> None:
        from huddle_cluster import Server
        sid = self._server_id(ip)
        server = Server(
            id=sid,
            host=ip,
            port=self.port,
            tags={"source": "k8s", "ip": ip},
        )
        try:
            self._cluster.add_server(server)
            with self._lock:
                self._known_ips[ip] = sid
            log.info(f"K8sDiscovery: added server {sid!r} ({ip}:{self.port})")
        except Exception as exc:
            log.warning(f"K8sDiscovery: failed to add {ip}: {exc}")

    def _remove_server(self, ip: str) -> None:
        with self._lock:
            sid = self._known_ips.pop(ip, None)
        if sid is None:
            return
        try:
            self._cluster.remove_server(sid)
            log.info(f"K8sDiscovery: removed server {sid!r} ({ip})")
        except Exception as exc:
            log.warning(f"K8sDiscovery: failed to remove {sid}: {exc}")

    
    # Introspection
    

    def known_servers(self) -> dict:
        """Return a copy of the current IP -> server_id mapping."""
        with self._lock:
            return dict(self._known_ips)

    def status(self) -> dict:
        return {
            "running":        self._running,
            "namespace":      self.namespace,
            "label_selector": self.label_selector,
            "port":           self.port,
            "known_count":    len(self._known_ips),
            "known_servers":  self.known_servers(),
        }