"""
HuddleCluster — Extensible Plugin Architecture & Lifecycle Hook System
======================================================================
v4.29.0

Provides a standardized, decoupled plugin framework for HuddleCluster.
Third-party developers, observability providers, alerting systems,
and custom infrastructure components can hook directly into master
and agent lifecycles without monkey-patching or modifying core logic.

Features:
  - HuddlePlugin: Abstract base class for lifecycle extensions.
  - PluginRegistry: Thread-safe discovery, ordering, and execution manager.
  - Lifecycle Hooks:
      * on_plugin_loaded(master)
      * on_node_join(node_record)
      * on_node_heartbeat(node_id, metrics)
      * on_node_dead(node_record)
      * on_node_leave(node_record)
      * on_thermal_eviction(server_id, reason, metrics)
      * on_failover(old_leader, new_leader, term)
      * on_cluster_shutdown()
  - Zero performance regression: Exception-isolated async/sync dispatch.
  - Built-in Example: WebhookAlertPlugin (HTTP alerting on cluster events).

Author : Rahad Bhuiya
Version: 4.29.0
License: MIT
"""

from __future__ import annotations

import abc
import json
import logging
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Callable, Dict, List, Optional, Set, Type

logger = logging.getLogger("HuddleCluster.Plugin")



# Base Plugin Abstract Interface


class HuddlePlugin(abc.ABC):
    """
    Abstract base class for all HuddleCluster extensions.

    Subclasses can override any combination of lifecycle hook methods.
    Exceptions raised within a plugin are safely trapped and logged by the
    PluginRegistry to guarantee cluster orchestrator stability.
    """

    @property
    def name(self) -> str:
        """Unique plugin identifier (defaults to class name)."""
        return self.__class__.__name__

    @property
    def version(self) -> str:
        """Plugin semantic version string."""
        return "1.0.0"

    @property
    def description(self) -> str:
        """Brief summary of plugin functionality."""
        return "Custom HuddleCluster Plugin"


    # Lifecycle Hooks
  

    def on_load(self, master: Any) -> None:
        """Called when plugin is attached to the running MasterNode."""
        pass

    def on_unload(self) -> None:
        """Called when cluster orchestrator is stopping or plugin removed."""
        pass

    def on_node_join(self, node: Any) -> None:
        """Called when a new node successfully enrolls into the cluster."""
        pass

    def on_node_heartbeat(self, node_id: str, metrics: Optional[Dict[str, Any]] = None) -> None:
        """Called upon successful node heartbeat receipt with telemetry."""
        pass

    def on_node_dead(self, node: Any) -> None:
        """Called when a node heartbeat times out and is marked dead."""
        pass

    def on_node_leave(self, node: Any) -> None:
        """Called when a node gracefully leaves the cluster."""
        pass

    def on_thermal_eviction(
        self,
        server_id: str,
        reason: str,
        metrics: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Called when thermal pressure causes a server eviction / ring drift."""
        pass

    def on_failover(
        self,
        old_leader: Optional[str],
        new_leader: str,
        term: int,
    ) -> None:
        """Called when Raft HA leader changes during failover."""
        pass



# Plugin Registry Manager


class PluginRegistry:
    """
    Thread-safe registry managing plugin lifecycle, hook dispatch, and isolation.
    """

    def __init__(self, master: Optional[Any] = None) -> None:
        self._master = master
        self._plugins: List[HuddlePlugin] = []
        self._plugin_names: Set[str] = set()
        self._lock = threading.RLock()

    def register(self, plugin: HuddlePlugin) -> None:
        """
        Register a new plugin instance and execute its `on_load` hook.
        """
        if not isinstance(plugin, HuddlePlugin):
            raise TypeError(f"Expected HuddlePlugin subclass instance, got {type(plugin)}")

        with self._lock:
            if plugin.name in self._plugin_names:
                raise ValueError(f"Plugin with name '{plugin.name}' is already registered")

            self._plugins.append(plugin)
            self._plugin_names.add(plugin.name)

        logger.info("Plugin '%s' (v%s) registered successfully", plugin.name, plugin.version)

        try:
            plugin.on_load(self._master)
        except Exception:
            logger.exception("Plugin '%s' raised an exception during on_load", plugin.name)

    def unregister(self, plugin_name: str) -> Optional[HuddlePlugin]:
        """
        Unregister and unload a plugin by name.
        """
        with self._lock:
            found = None
            for p in self._plugins:
                if p.name == plugin_name:
                    found = p
                    break

            if found:
                self._plugins.remove(found)
                self._plugin_names.remove(found.name)

        if found:
            try:
                found.on_unload()
            except Exception:
                logger.exception("Plugin '%s' raised an exception during on_unload", plugin_name)
            logger.info("Plugin '%s' unregistered successfully", plugin_name)

        return found

    def get_plugin(self, plugin_name: str) -> Optional[HuddlePlugin]:
        """Retrieve registered plugin instance by name."""
        with self._lock:
            for p in self._plugins:
                if p.name == plugin_name:
                    return p
            return None

    def list_plugins(self) -> List[Dict[str, Any]]:
        """Return metadata summary for all currently registered plugins."""
        with self._lock:
            return [
                {
                    "name": p.name,
                    "version": p.version,
                    "description": p.description,
                    "class": p.__class__.__name__,
                }
                for p in self._plugins
            ]


    # Dispatchers with Exception Isolation


    def dispatch_node_join(self, node: Any) -> None:
        self._dispatch("on_node_join", node)

    def dispatch_node_heartbeat(self, node_id: str, metrics: Optional[Dict[str, Any]] = None) -> None:
        self._dispatch("on_node_heartbeat", node_id, metrics)

    def dispatch_node_dead(self, node: Any) -> None:
        self._dispatch("on_node_dead", node)

    def dispatch_node_leave(self, node: Any) -> None:
        self._dispatch("on_node_leave", node)

    def dispatch_thermal_eviction(
        self,
        server_id: str,
        reason: str,
        metrics: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._dispatch("on_thermal_eviction", server_id, reason, metrics)

    def dispatch_failover(
        self,
        old_leader: Optional[str],
        new_leader: str,
        term: int,
    ) -> None:
        self._dispatch("on_failover", old_leader, new_leader, term)

    def dispatch_shutdown(self) -> None:
        with self._lock:
            plugins = list(self._plugins)
        for p in plugins:
            try:
                p.on_unload()
            except Exception:
                logger.exception("Plugin '%s' error on shutdown", p.name)

    def _dispatch(self, hook_name: str, *args: Any, **kwargs: Any) -> None:
        with self._lock:
            plugins = list(self._plugins)

        for p in plugins:
            hook: Optional[Callable] = getattr(p, hook_name, None)
            if hook and callable(hook):
                try:
                    hook(*args, **kwargs)
                except Exception:
                    logger.exception(
                        "Plugin '%s' raised an unhandled exception in '%s'",
                        p.name, hook_name,
                    )



# Built-in Standard Plugin: WebhookAlertPlugin


class WebhookAlertPlugin(HuddlePlugin):
    """
    Production-grade reference plugin that dispatches HTTP webhook alerts
    (e.g., to Slack, Discord, PagerDuty, or custom monitoring endpoints)
    on critical cluster lifecycle transitions.
    """

    def __init__(
        self,
        webhook_url: str,
        events: Optional[List[str]] = None,
        timeout_sec: float = 3.0,
    ) -> None:
        self._webhook_url = webhook_url
        self._events = set(events or ["node_dead", "thermal_eviction", "failover"])
        self._timeout = timeout_sec
        self.dispatched_count = 0

    @property
    def name(self) -> str:
        return "WebhookAlertPlugin"

    @property
    def description(self) -> str:
        return "Automated HTTP webhook notifier for cluster lifecycle events"

    def _send_alert(self, event_type: str, payload: Dict[str, Any]) -> None:
        if event_type not in self._events:
            return

        body = {
            "source": "huddle-cluster",
            "event": event_type,
            "timestamp": time.time(),
            "data": payload,
        }
        data_bytes = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(
            self._webhook_url,
            data=data_bytes,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self._timeout):
                self.dispatched_count += 1
        except Exception as e:
            logger.debug("WebhookAlertPlugin failed to send alert to %s: %s", self._webhook_url, e)

    def on_node_dead(self, node: Any) -> None:
        node_id = getattr(node, "node_id", str(node))
        self._send_alert("node_dead", {"node_id": node_id, "status": "dead"})

    def on_thermal_eviction(
        self,
        server_id: str,
        reason: str,
        metrics: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._send_alert("thermal_eviction", {
            "server_id": server_id,
            "reason": reason,
            "metrics": metrics or {},
        })

    def on_failover(
        self,
        old_leader: Optional[str],
        new_leader: str,
        term: int,
    ) -> None:
        self._send_alert("failover", {
            "old_leader": old_leader,
            "new_leader": new_leader,
            "term": term,
        })
