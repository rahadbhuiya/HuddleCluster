"""
HuddleCluster — Service Discovery
====================================
Health-aware service registry built on top of the existing node system.

Nodes advertise which services they provide by including a ``services`` list
in their join metadata::

    agent = AgentNode(
        node_id="web-01",
        master_url="http://master:7070",
        port=8080,
        metadata={"services": ["api", "web"]},
    )

The ServiceDiscovery layer watches the master's node registry and exposes
only **alive** nodes for each named service.  Dead and quarantined nodes are
automatically excluded until they recover.

REST endpoints (mounted by MasterNode when a ServiceDiscovery is attached):

    GET  /v1/discovery/services              → list all known service names
    GET  /v1/discovery/services/<name>       → alive nodes for a service
    POST /v1/discovery/announce              → node self-announces a service
                                               (alternative to metadata)
    DELETE /v1/discovery/services/<name>/<node_id>
                                             → deregister one node from a service

Optional DNS responder (``dns_port`` param): answers A-record queries for
``<service>.cluster.local`` using the addresses of alive nodes.  Requires
no external libraries — pure stdlib ``socket``.

Author : Rahad Bhuiya
Version: 3.3.0
License: MIT
"""

from __future__ import annotations

import logging
import socket
import struct
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)



# DNS helpers (minimal A-record responder, no external deps)


def _build_dns_response(query: bytes, ip_addresses: List[str]) -> bytes:
    """
    Build a minimal DNS response for an A-record query.
    Follows RFC 1035 — enough for simple ``dig``/``nslookup`` queries.
    """
    # Transaction ID — echo from query
    txn_id = query[:2]
    # Flags: response, authoritative, no error
    flags  = b"\x84\x00"
    # Counts: 1 question, N answers, 0 authority, 0 additional
    counts = struct.pack("!HHHH", 1, len(ip_addresses), 0, 0)
    # Echo the question section (everything after the 12-byte header)
    question = query[12:]

    answers = b""
    for ip in ip_addresses:
        # Name: pointer to offset 12 (the question name)
        answers += b"\xc0\x0c"
        # Type A, Class IN
        answers += b"\x00\x01\x00\x01"
        # TTL: 10 seconds
        answers += struct.pack("!I", 10)
        # RDLENGTH: 4 bytes (IPv4)
        answers += b"\x00\x04"
        # RDATA: IPv4 address
        answers += socket.inet_aton(ip)

    return txn_id + flags + counts + question + answers



# ServiceDiscovery


class ServiceDiscovery:
    """
    Health-aware service registry for HuddleCluster.

    Attach to a MasterNode::

        from huddle_cluster_pkg import MasterNode
        from huddle_cluster_pkg.cluster_service_discovery import ServiceDiscovery

        sd = ServiceDiscovery(
            refresh_interval_sec=5.0,
            on_service_up=lambda svc, nodes: print(f"{svc} up: {len(nodes)} node(s)"),
            on_service_down=lambda svc: alert_ops(f"{svc} has no alive nodes!"),
        )
        master = MasterNode(port=7070, service_discovery=sd)
        master.start()

    Nodes advertise services via join metadata::

        huddle-cluster agent start --id web-01 --port 8080 \\
            --meta services=api,web

    Or via the REST API at runtime::

        POST /v1/discovery/announce   {"node_id": "web-01", "service": "api"}

    DNS (optional)::

        sd = ServiceDiscovery(dns_port=8053)
        # dig @localhost -p 8053 api.cluster.local A
    """

    def __init__(
        self,
        refresh_interval_sec: float = 5.0,
        dns_port: Optional[int] = None,
        dns_domain: str = "cluster.local",
        on_service_up: Optional[Callable[[str, List[Dict]], None]] = None,
        on_service_down: Optional[Callable[[str], None]] = None,
    ) -> None:
        """
        Args:
            refresh_interval_sec: How often to sync the registry from the
                                  master's node list.
            dns_port:             UDP port for the built-in DNS A-record
                                  responder.  None (default) disables DNS.
            dns_domain:           Domain suffix answered by the DNS responder.
                                  Default: ``cluster.local``.
            on_service_up:        Callback(service_name, alive_nodes) the first
                                  time at least one alive node provides a service.
            on_service_down:      Callback(service_name) when the last alive
                                  node for a service disappears.
        """
        self.refresh_interval_sec = refresh_interval_sec
        self.dns_port             = dns_port
        self.dns_domain           = dns_domain

        self._on_service_up   = on_service_up
        self._on_service_down = on_service_down

        self._lock    = threading.RLock()
        self._master: Optional[Any] = None
        self._running = False

        # service_name → set of node_ids that provide it (alive or dead)
        self._registry: Dict[str, Set[str]] = {}
        # services currently considered "up" (have >= 1 alive node)
        self._services_up: Set[str] = set()

        self._refresh_thread: Optional[threading.Thread] = None
        self._dns_thread:     Optional[threading.Thread] = None
        self._dns_socket:     Optional[socket.socket]    = None

    
    # Lifecycle
    

    def attach(self, master: Any) -> None:
        """Called automatically by MasterNode.start()."""
        self._master  = master
        self._running = True

        self._refresh_thread = threading.Thread(
            target=self._refresh_loop,
            name="service-discovery-refresh",
            daemon=True,
        )
        self._refresh_thread.start()

        if self.dns_port is not None:
            self._dns_thread = threading.Thread(
                target=self._dns_loop,
                name="service-discovery-dns",
                daemon=True,
            )
            self._dns_thread.start()
            logger.info(
                "ServiceDiscovery DNS responder started on UDP port %d (.%s)",
                self.dns_port, self.dns_domain,
            )

        logger.info(
            "ServiceDiscovery started (refresh=%.0fs%s)",
            self.refresh_interval_sec,
            f", DNS=:{self.dns_port}" if self.dns_port else "",
        )

    def stop(self) -> None:
        self._running = False
        if self._dns_socket:
            try:
                self._dns_socket.close()
            except OSError:
                pass

    
    # Public API
    

    def announce(self, node_id: str, service: str) -> None:
        """Register node_id as providing service (runtime, no heartbeat needed)."""
        service = service.strip().lower()
        with self._lock:
            self._registry.setdefault(service, set()).add(node_id)
        logger.debug("ServiceDiscovery: '%s' announced for service '%s'",
                     node_id, service)

    def deregister(self, node_id: str, service: str) -> bool:
        """Remove a node from a service.  Returns True if it was registered."""
        service = service.strip().lower()
        with self._lock:
            svc_nodes = self._registry.get(service, set())
            if node_id in svc_nodes:
                svc_nodes.discard(node_id)
                return True
        return False

    def services(self) -> List[str]:
        """All known service names (regardless of health)."""
        with self._lock:
            return sorted(self._registry.keys())

    def alive_nodes_for(self, service: str) -> List[Dict[str, Any]]:
        """Alive nodes providing the named service, sorted by node_id."""
        service = service.strip().lower()
        with self._lock:
            registered = self._registry.get(service, set())
            if not registered or self._master is None:
                return []

        # Ask master for full node records; filter to registered + alive
        all_nodes = self._master.nodes()
        result = [
            n for n in all_nodes
            if n["node_id"] in registered and n["status"] == "alive"
        ]
        result.sort(key=lambda n: n["node_id"])
        return result

    def summary(self) -> Dict[str, Any]:
        """All services with their alive-node counts — for monitoring."""
        with self._lock:
            services = sorted(self._registry.keys())
        out = {}
        for svc in services:
            nodes = self.alive_nodes_for(svc)
            out[svc] = {
                "alive_count": len(nodes),
                "nodes": [{"node_id": n["node_id"],
                            "address": n["address"],
                            "port":    n["port"]} for n in nodes],
            }
        return {
            "services":  out,
            "dns_port":  self.dns_port,
            "dns_domain": self.dns_domain,
        }

    
    # Internal refresh loop
    

    def _refresh_loop(self) -> None:
        while self._running:
            time.sleep(self.refresh_interval_sec)
            if not self._running:
                break
            try:
                self._sync_from_metadata()
                self._check_service_health()
            except Exception:
                logger.exception("ServiceDiscovery refresh raised")

    def _sync_from_metadata(self) -> None:
        """
        Pull `services` from node metadata on every refresh.
        Supports both comma-string ("api,web") and list formats.
        """
        if self._master is None:
            return
        nodes = self._master.nodes()
        for node in nodes:
            raw = (node.get("metadata") or {}).get("services")
            if not raw:
                continue
            if isinstance(raw, str):
                service_names = [s.strip().lower()
                                 for s in raw.split(",") if s.strip()]
            elif isinstance(raw, list):
                service_names = [str(s).strip().lower() for s in raw if s]
            else:
                continue

            for svc in service_names:
                with self._lock:
                    self._registry.setdefault(svc, set()).add(node["node_id"])

    def _check_service_health(self) -> None:
        """Fire on_service_up / on_service_down on transitions."""
        with self._lock:
            services = list(self._registry.keys())

        for svc in services:
            nodes = self.alive_nodes_for(svc)
            currently_up = len(nodes) > 0

            with self._lock:
                was_up = svc in self._services_up

            if currently_up and not was_up:
                with self._lock:
                    self._services_up.add(svc)
                logger.info(
                    "Service '%s' is UP (%d alive node(s))", svc, len(nodes)
                )
                if self._on_service_up:
                    try:
                        self._on_service_up(svc, nodes)
                    except Exception:
                        logger.exception("on_service_up callback raised")

            elif not currently_up and was_up:
                with self._lock:
                    self._services_up.discard(svc)
                logger.warning("Service '%s' is DOWN (no alive nodes)", svc)
                if self._on_service_down:
                    try:
                        self._on_service_down(svc)
                    except Exception:
                        logger.exception("on_service_down callback raised")

    
    # DNS responder (optional, pure stdlib)
    

    def _dns_loop(self) -> None:
        try:
            self._dns_socket = socket.socket(
                socket.AF_INET, socket.SOCK_DGRAM
            )
            self._dns_socket.setsockopt(
                socket.SOL_SOCKET, socket.SO_REUSEADDR, 1
            )
            self._dns_socket.bind(("0.0.0.0", self.dns_port))
            self._dns_socket.settimeout(1.0)
        except OSError as exc:
            logger.error(
                "ServiceDiscovery DNS: cannot bind to UDP port %d: %s",
                self.dns_port, exc,
            )
            return

        while self._running:
            try:
                data, addr = self._dns_socket.recvfrom(512)
                self._handle_dns_query(data, addr)
            except socket.timeout:
                continue
            except OSError:
                if self._running:
                    logger.exception("ServiceDiscovery DNS socket error")
                break

    def _handle_dns_query(
        self, data: bytes, addr: tuple
    ) -> None:
        """Parse the QNAME from a DNS query and return alive node IPs."""
        try:
            # Decode QNAME from offset 12
            qname_parts = []
            idx = 12
            while idx < len(data):
                length = data[idx]
                if length == 0:
                    break
                idx += 1
                qname_parts.append(data[idx: idx + length].decode("ascii"))
                idx += length

            qname = ".".join(qname_parts).lower()
            suffix = f".{self.dns_domain}".lower()

            if not qname.endswith(suffix):
                return   # not our domain, ignore

            service = qname[: -len(suffix)]
            nodes   = self.alive_nodes_for(service)
            ips     = [n["address"] for n in nodes
                       if self._is_valid_ipv4(n["address"])]

            if not ips:
                return   # NXDOMAIN or NODATA — just drop the query

            response = _build_dns_response(data, ips)
            self._dns_socket.sendto(response, addr)
            logger.debug(
                "DNS query for '%s': returned %d address(es)", service, len(ips)
            )
        except Exception:
            logger.debug("ServiceDiscovery DNS: failed to handle query")

    @staticmethod
    def _is_valid_ipv4(address: str) -> bool:
        try:
            socket.inet_aton(address)
            return True
        except OSError:
            return False