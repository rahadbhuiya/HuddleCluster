"""
HuddleCluster Redis State Backend (v1.4.0).

Replaces the local JSON file used by save_state()/load_state() with a
shared Redis key. Multiple HuddleCluster nodes can read from the same
Redis key so all nodes start with the same temperature baseline after a
rolling restart.

Requires: pip install redis

Usage:
    from huddle_cluster import create_cluster
    from huddle_cluster_pkg.backends_redis import RedisBackend

    backend = RedisBackend(url="redis://localhost:6379", key="huddle:state")
    cluster = create_cluster([...])
    cluster.start()

    # Save state to Redis
    backend.save(cluster)

    # Load state from Redis into cluster
    backend.load(cluster)

    # Or configure automatic periodic sync
    backend.start_auto_sync(cluster, interval_sec=30.0)
    # ...
    backend.stop_auto_sync()
"""

from __future__ import annotations

import json
import logging
import threading
import time
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from huddle_cluster import HuddleCluster

log = logging.getLogger("huddle.redis")


class RedisBackend:
    """
    Redis-backed state store for HuddleCluster.

    State is serialised to JSON and stored under a single Redis key.
    This makes it compatible with cluster.save_state() / load_state()
    semantics while allowing multiple nodes to share state.

    Args:
        url:           Redis URL. Supports redis://, rediss://, unix://.
                       Default "redis://localhost:6379".
        key:           Redis key to store state under. Default "huddle:cluster:state".
        db:            Redis database index. Default 0.
        ttl_sec:       TTL for the Redis key in seconds. 0 = no expiry.
                       Default 0.
        password:      Optional Redis password. Can also be embedded in url.
        socket_timeout: Seconds before a Redis operation times out. Default 3.
    """

    def __init__(
        self,
        url:            str   = "redis://localhost:6379",
        key:            str   = "huddle:cluster:state",
        db:             int   = 0,
        ttl_sec:        int   = 0,
        password:       Optional[str] = None,
        socket_timeout: float = 3.0,
    ) -> None:
        try:
            import redis as _redis  # noqa: F401
        except ImportError:
            raise ImportError(
                "redis package is required for RedisBackend. "
                "Install it with: pip install redis"
            )

        self.url            = url
        self.key            = key
        self.db             = db
        self.ttl_sec        = ttl_sec
        self.password       = password
        self.socket_timeout = socket_timeout

        self._client       = None   # lazily created
        self._sync_thread: Optional[threading.Thread] = None
        self._running:     bool = False

    
    # Redis client
    

    def _get_client(self):
        """Return a thread-safe Redis client (creates once, reuses)."""
        if self._client is None:
            import redis as _redis
            self._client = _redis.from_url(
                self.url,
                db=self.db,
                password=self.password,
                socket_timeout=self.socket_timeout,
                decode_responses=True,
            )
        return self._client

    def ping(self) -> bool:
        """
        Test the Redis connection.

        Returns:
            True if Redis responded, False on any error.
        """
        try:
            return bool(self._get_client().ping())
        except Exception as exc:
            log.warning(f"RedisBackend.ping() failed: {exc}")
            return False

    
    # Core save / load
    

    def save(self, cluster: "HuddleCluster") -> bool:
        """
        Serialize cluster state to Redis.

        Uses the same JSON format as cluster.save_state() so the two are
        interchangeable. The Redis key is set atomically via SET ... EX.

        Args:
            cluster: A running HuddleCluster instance.

        Returns:
            True on success, False on Redis error.
        """
        import json as _json

        # Build state dict (same structure as save_state())
        servers_data: dict = {}
        for s in cluster.all_servers():
            servers_data[s.id] = {
                "temperature":           s.temperature,
                "avg_response_ms":       s.metrics.avg_response_ms,
                "error_rate":            s.metrics.error_rate,
                "latency_anomaly_score": s.metrics.latency_anomaly_score,
                "rotation_count":        s.rotation_count,
                "total_inner_time":      s.total_inner_time,
                "total_outer_time":      s.total_outer_time,
                "histogram_samples":     list(s.metrics._histogram_window),
            }

        from huddle_cluster import __version__
        state = {
            "version":        __version__,
            "saved_at":       time.time(),
            "heat_threshold": cluster.heat_threshold,
            "servers":        servers_data,
        }

        payload = _json.dumps(state)

        try:
            client = self._get_client()
            if self.ttl_sec > 0:
                client.set(self.key, payload, ex=self.ttl_sec)
            else:
                client.set(self.key, payload)
            log.info(
                f"RedisBackend.save(): {len(servers_data)} servers "
                f"-> key={self.key!r}"
            )
            return True
        except Exception as exc:
            log.warning(f"RedisBackend.save() failed: {exc}")
            return False

    def load(self, cluster: "HuddleCluster") -> int:
        """
        Restore cluster state from Redis.

        Servers present in Redis but not in the cluster are silently skipped.
        Servers present in the cluster but not in Redis are left unchanged.

        Args:
            cluster: A running HuddleCluster instance.

        Returns:
            Number of servers whose state was restored. 0 if key missing.

        Raises:
            json.JSONDecodeError: Redis value is not valid JSON.
        """
        try:
            raw = self._get_client().get(self.key)
        except Exception as exc:
            log.warning(f"RedisBackend.load() failed: {exc}")
            return 0

        if raw is None:
            log.info(f"RedisBackend.load(): key {self.key!r} not found; starting fresh.")
            return 0

        state = json.loads(raw)
        servers_data: dict = state.get("servers", {})
        restored = 0

        current = {s.id: s for s in cluster.all_servers()}
        for sid, data in servers_data.items():
            s = current.get(sid)
            if s is None:
                log.debug(f"RedisBackend.load(): server {sid!r} not in cluster; skipped.")
                continue

            s.temperature                   = float(data.get("temperature", 0.0))
            s.metrics.avg_response_ms       = float(data.get("avg_response_ms", 0.0))
            s.metrics.error_rate            = float(data.get("error_rate", 0.0))
            s.metrics.latency_anomaly_score = float(data.get("latency_anomaly_score", 0.0))
            s.rotation_count                = int(data.get("rotation_count", 0))
            s.total_inner_time              = float(data.get("total_inner_time", 0.0))
            s.total_outer_time              = float(data.get("total_outer_time", 0.0))

            samples = [float(v) for v in data.get("histogram_samples", [])]
            s.metrics._histogram_window.clear()
            s.metrics._histogram_window.extend(samples)
            s.metrics._latency_window.clear()
            s.metrics._latency_window.extend(samples[-10:])

            restored += 1

        log.info(
            f"RedisBackend.load(): restored {restored}/{len(servers_data)} servers "
            f"from key={self.key!r}"
        )
        return restored

    def delete(self) -> bool:
        """
        Delete the state key from Redis.

        Returns:
            True if the key existed and was deleted, False otherwise.
        """
        try:
            result = self._get_client().delete(self.key)
            return bool(result)
        except Exception as exc:
            log.warning(f"RedisBackend.delete() failed: {exc}")
            return False

    def exists(self) -> bool:
        """Return True if the state key exists in Redis."""
        try:
            return bool(self._get_client().exists(self.key))
        except Exception:
            return False

    
    # Auto-sync
    

    def start_auto_sync(
        self,
        cluster:      "HuddleCluster",
        interval_sec: float = 30.0,
        direction:    str   = "save",
    ) -> None:
        """
        Start a background thread that periodically saves or loads state.

        Args:
            cluster:      HuddleCluster to sync.
            interval_sec: Seconds between syncs.
            direction:    "save" (push to Redis) or "load" (pull from Redis).
                          Use "save" on the primary node, "load" on replicas.
        """
        if self._running:
            raise RuntimeError("Auto-sync is already running.")
        if direction not in ("save", "load"):
            raise ValueError("direction must be 'save' or 'load'")

        self._running = True
        self._sync_thread = threading.Thread(
            target=self._sync_loop,
            args=(cluster, interval_sec, direction),
            name="huddle-redis-sync",
            daemon=True,
        )
        self._sync_thread.start()
        log.info(
            f"RedisBackend auto-sync started (direction={direction}, "
            f"interval={interval_sec}s, key={self.key!r})"
        )

    def stop_auto_sync(self) -> None:
        """Stop the background sync thread. Safe to call when not running."""
        self._running = False
        if self._sync_thread and self._sync_thread.is_alive():
            self._sync_thread.join(timeout=5.0)
        self._sync_thread = None
        log.info("RedisBackend auto-sync stopped")

    def _sync_loop(
        self,
        cluster:      "HuddleCluster",
        interval_sec: float,
        direction:    str,
    ) -> None:
        while self._running:
            deadline = time.monotonic() + interval_sec
            while self._running and time.monotonic() < deadline:
                time.sleep(0.25)
            if not self._running:
                break
            try:
                if direction == "save":
                    self.save(cluster)
                else:
                    self.load(cluster)
            except Exception as exc:
                log.warning(f"RedisBackend sync error: {exc}")

    
    # Info
    

    def info(self) -> dict:
        """Return connection info and key metadata."""
        result = {
            "url":      self.url,
            "key":      self.key,
            "db":       self.db,
            "ttl_sec":  self.ttl_sec,
            "running":  self._running,
            "exists":   self.exists(),
        }
        try:
            client = self._get_client()
            ttl = client.ttl(self.key)
            result["key_ttl_remaining"] = ttl
        except Exception:
            result["key_ttl_remaining"] = None
        return result