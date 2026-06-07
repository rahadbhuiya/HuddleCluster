"""
HuddleCluster gRPC Support (v1.4.0).

Thermal-aware gRPC channel routing using the same dual-ring algorithm
as the HTTP cluster. Instead of (host, port) routing, this module manages
gRPC channels and provides a context manager that returns a ready channel.

Requires: pip install grpcio

Usage:
    from huddle_cluster_pkg.grpc_cluster import create_grpc_cluster

    cluster = create_grpc_cluster([
        ("s1", "10.0.0.1", 50051),
        ("s2", "10.0.0.2", 50051),
        ("s3", "10.0.0.3", 50051),
    ])
    cluster.start()

    # Get a channel for one RPC call
    with cluster.get_channel() as channel:
        stub = MyService.Stub(channel)
        response = stub.MyMethod(request)

    # Or get the underlying server for full control
    server = cluster.get_server()
    channel = cluster.channel_for(server)

    cluster.stop()
"""

from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from typing import Generator, List, Optional

log = logging.getLogger("huddle.grpc")


class GrpcCluster:
    """
    A HuddleCluster wrapper that manages gRPC channels alongside server routing.

    Each registered server gets a persistent gRPC channel that is created
    once and reused. Channels for evicted servers are gracefully shut down.
    Channels for recovered servers are re-created automatically.

    Args:
        cluster:          An underlying HuddleCluster instance (already configured).
        channel_options:  Optional list of (key, value) grpc channel options,
                          e.g. [("grpc.max_receive_message_length", 10 * 1024 * 1024)].
        credentials:      Optional grpc.ChannelCredentials for TLS. If None,
                          insecure channels are used.
        connect_timeout:  Seconds to wait for channel readiness on first use.
                          Default 5.0.
    """

    def __init__(
        self,
        cluster,
        channel_options:  Optional[list] = None,
        credentials=None,
        connect_timeout:  float = 5.0,
    ) -> None:
        try:
            import grpc  # noqa: F401
        except ImportError:
            raise ImportError(
                "grpcio package is required for GrpcCluster. "
                "Install it with: pip install grpcio"
            )

        self._cluster         = cluster
        self._channel_options = channel_options or []
        self._credentials     = credentials
        self._connect_timeout = connect_timeout
        # {server_id: grpc.Channel}
        self._channels: dict  = {}
        self._lock            = __import__("threading").Lock()

    
    # Lifecycle (delegates to underlying HuddleCluster)
    

    def start(self, rotation_interval_sec: float = 1.0) -> None:
        """Start the cluster and pre-create gRPC channels for all servers."""
        self._cluster.start(rotation_interval_sec=rotation_interval_sec)
        for server in self._cluster.all_servers():
            self._ensure_channel(server)
        log.info(
            f"GrpcCluster started with {len(self._channels)} channels"
        )

    def stop(self, timeout: float = 5.0) -> None:
        """Stop the cluster and close all gRPC channels."""
        self._cluster.stop(timeout=timeout)
        self._close_all_channels()
        log.info("GrpcCluster stopped")

    
    # Routing
    

    @contextmanager
    def get_channel(
        self, affinity_key: Optional[str] = None
    ) -> "Generator":
        """
        Context manager: pick a server and yield its gRPC channel.

        Records latency automatically on exit (same as get_server_context).

        Args:
            affinity_key: Optional sticky-session key.

        Example::

            with cluster.get_channel() as channel:
                stub = MyService.Stub(channel)
                response = stub.GetUser(request)
        """
        server = self._cluster.get_server(affinity_key=affinity_key)
        if server is None:
            raise RuntimeError("No servers available in the gRPC cluster.")

        channel = self._ensure_channel(server)
        t0 = time.perf_counter()
        try:
            yield channel
            elapsed_ms = (time.perf_counter() - t0) * 1000
            self._cluster.record_latency(server, elapsed_ms)
        except Exception:
            elapsed_ms = (time.perf_counter() - t0) * 1000
            self._cluster.record_latency(server, elapsed_ms)
            server.metrics.error_rate = min(
                1.0, server.metrics.error_rate * 0.9 + 0.1
            )
            if server.metrics.error_rate > 0.5:
                server.metrics.is_healthy = False
                self._close_channel(server.id)
            raise

    def channel_for(self, server) -> "grpc.Channel":
        """
        Return the gRPC channel for a specific server.

        Creates the channel if it does not yet exist.

        Args:
            server: A Server object from get_server().

        Returns:
            A grpc.Channel object ready for stub creation.
        """
        return self._ensure_channel(server)

    def get_server(self, affinity_key: Optional[str] = None):
        """Delegate to the underlying HuddleCluster.get_server()."""
        return self._cluster.get_server(affinity_key=affinity_key)

    
    # Channel management
    def _ensure_channel(self, server) -> "grpc.Channel":
        """Return existing channel or create a new one for the server."""
        import grpc

        with self._lock:
            channel = self._channels.get(server.id)
            if channel is not None:
                return channel

            target = f"{server.host}:{server.port}"
            if self._credentials is not None:
                channel = grpc.secure_channel(
                    target, self._credentials, options=self._channel_options
                )
            else:
                channel = grpc.insecure_channel(
                    target, options=self._channel_options
                )

            self._channels[server.id] = channel
            log.debug(f"GrpcCluster: created channel for {server.id!r} ({target})")
            return channel

    def _close_channel(self, server_id: str) -> None:
        """Close and remove one channel."""
        with self._lock:
            channel = self._channels.pop(server_id, None)
        if channel is not None:
            try:
                channel.close()
                log.debug(f"GrpcCluster: closed channel for {server_id!r}")
            except Exception as exc:
                log.warning(f"GrpcCluster: error closing channel {server_id!r}: {exc}")

    def _close_all_channels(self) -> None:
        with self._lock:
            ids = list(self._channels.keys())
        for sid in ids:
            self._close_channel(sid)

    
    # Inspection (delegates to underlying cluster)
    

    def all_servers(self):
        return self._cluster.all_servers()

    def inner_servers(self):
        return self._cluster.inner_servers()

    def outer_servers(self):
        return self._cluster.outer_servers()

    def health_report(self) -> dict:
        report = self._cluster.health_report()
        with self._lock:
            report["grpc_channels"] = {
                sid: str(ch) for sid, ch in self._channels.items()
            }
            report["grpc_channel_count"] = len(self._channels)
        return report

    def prometheus_metrics(self) -> str:
        base = self._cluster.prometheus_metrics()
        with self._lock:
            count = len(self._channels)
        return (
            base
            + "# HELP huddle_grpc_channels_total Open gRPC channels\n"
            + "# TYPE huddle_grpc_channels_total gauge\n"
            + f"huddle_grpc_channels_total {count}\n"
        )

    
    # Proxy remaining HuddleCluster attributes
    

    def __getattr__(self, name: str):
        """Proxy any unknown attribute to the underlying HuddleCluster."""
        return getattr(self._cluster, name)


def create_grpc_cluster(
    servers,
    channel_options:  Optional[list] = None,
    credentials=None,
    connect_timeout:  float = 5.0,
    **cluster_kwargs,
) -> GrpcCluster:
    """
    Convenience factory for a thermal-aware gRPC cluster.

    Args:
        servers:         List of (id, host, port) or (id, host, port, weight) tuples.
        channel_options: Optional gRPC channel options list.
        credentials:     Optional grpc.ChannelCredentials for TLS.
        connect_timeout: Channel connection timeout in seconds.
        **cluster_kwargs: All HuddleCluster constructor kwargs (heat_threshold,
                          state_file, alert_webhooks, etc.)

    Returns:
        A GrpcCluster instance (not yet started -- call .start() first).

    Example::

        cluster = create_grpc_cluster(
            [("s1", "10.0.0.1", 50051), ("s2", "10.0.0.2", 50051)],
            heat_threshold=0.6,
            state_file="/var/lib/huddle/grpc_state.json",
        )
        cluster.start()

        with cluster.get_channel() as ch:
            stub = MyService.Stub(ch)
            resp = stub.Ping(PingRequest())

        cluster.stop()
    """
    from huddle_cluster import create_cluster as _create_cluster

    underlying = _create_cluster(servers, **cluster_kwargs)
    return GrpcCluster(
        cluster=underlying,
        channel_options=channel_options,
        credentials=credentials,
        connect_timeout=connect_timeout,
    )