"""
Tests for HuddleCluster Plugin Architecture & Lifecycle Hook System (v4.29.0)
=============================================================================
"""

import json
import pytest
from unittest.mock import MagicMock, patch

from huddle_cluster_pkg.cluster_plugin import (
    HuddlePlugin,
    PluginRegistry,
    WebhookAlertPlugin,
)
from huddle_cluster_pkg.cluster_master import MasterNode, NodeRecord


class DummyAuditPlugin(HuddlePlugin):
    """Test plugin that records invocations of all lifecycle hooks."""

    def __init__(self, name="DummyAuditPlugin"):
        self._name = name
        self.events = []
        self.loaded_with = None
        self.unloaded = False

    @property
    def name(self) -> str:
        return self._name

    @property
    def version(self) -> str:
        return "2.1.0"

    @property
    def description(self) -> str:
        return "Dummy Audit Plugin for tests"

    def on_load(self, master):
        self.loaded_with = master
        self.events.append(("on_load", master))

    def on_unload(self):
        self.unloaded = True
        self.events.append(("on_unload",))

    def on_node_join(self, node):
        self.events.append(("on_node_join", node))

    def on_node_heartbeat(self, node_id, metrics):
        self.events.append(("on_node_heartbeat", node_id, metrics))

    def on_node_dead(self, node):
        self.events.append(("on_node_dead", node))

    def on_node_leave(self, node):
        self.events.append(("on_node_leave", node))

    def on_thermal_eviction(self, server_id, reason, metrics):
        self.events.append(("on_thermal_eviction", server_id, reason, metrics))

    def on_failover(self, old_leader, new_leader, term):
        self.events.append(("on_failover", old_leader, new_leader, term))


class FaultyPlugin(HuddlePlugin):
    """Plugin that raises exceptions in all hooks to test exception isolation."""

    @property
    def name(self) -> str:
        return "FaultyPlugin"

    def on_load(self, master):
        raise RuntimeError("Crash on load")

    def on_node_join(self, node):
        raise ValueError("Crash on join")

    def on_node_heartbeat(self, node_id, metrics):
        raise KeyError("Crash on heartbeat")

    def on_node_dead(self, node):
        raise ZeroDivisionError("Crash on dead")

    def on_node_leave(self, node):
        raise IndexError("Crash on leave")

    def on_thermal_eviction(self, server_id, reason, metrics):
        raise Exception("Crash on thermal eviction")

    def on_failover(self, old_leader, new_leader, term):
        raise Exception("Crash on failover")

    def on_unload(self):
        raise RuntimeError("Crash on unload")


def test_huddle_plugin_defaults():
    class DefaultPlugin(HuddlePlugin):
        pass

    plugin = DefaultPlugin()
    assert plugin.name == "DefaultPlugin"
    assert plugin.version == "1.0.0"
    assert plugin.description == "Custom HuddleCluster Plugin"
    # Default hooks should be no-ops without error
    plugin.on_load(None)
    plugin.on_node_join(None)
    plugin.on_node_heartbeat("node-1", None)
    plugin.on_node_dead(None)
    plugin.on_node_leave(None)
    plugin.on_thermal_eviction("node-1", "temp", None)
    plugin.on_failover("a", "b", 1)
    plugin.on_unload()


def test_plugin_registry_lifecycle():
    registry = PluginRegistry(master="mock_master")
    p1 = DummyAuditPlugin("p1")

    # Invalid type
    with pytest.raises(TypeError):
        registry.register("not_a_plugin")

    # Successful registration
    registry.register(p1)
    assert p1.loaded_with == "mock_master"
    assert registry.get_plugin("p1") is p1

    # Duplicate registration
    with pytest.raises(ValueError):
        registry.register(DummyAuditPlugin("p1"))

    # Listing
    plugins_meta = registry.list_plugins()
    assert len(plugins_meta) == 1
    assert plugins_meta[0]["name"] == "p1"
    assert plugins_meta[0]["version"] == "2.1.0"
    assert plugins_meta[0]["class"] == "DummyAuditPlugin"

    # Unregister
    removed = registry.unregister("p1")
    assert removed is p1
    assert p1.unloaded is True
    assert registry.get_plugin("p1") is None
    assert len(registry.list_plugins()) == 0

    # Unregister nonexistent
    assert registry.unregister("nonexistent") is None


def test_plugin_registry_dispatch_and_isolation():
    registry = PluginRegistry()
    good = DummyAuditPlugin("good")
    bad = FaultyPlugin()

    # Register both - bad plugin raising in on_load shouldn't prevent registration
    registry.register(bad)
    registry.register(good)

    # Node join
    registry.dispatch_node_join("node-1")
    assert any(e[0] == "on_node_join" and e[1] == "node-1" for e in good.events)

    # Node heartbeat
    registry.dispatch_node_heartbeat("node-1", {"temp": 45.0})
    assert any(e[0] == "on_node_heartbeat" and e[1] == "node-1" for e in good.events)

    # Node dead
    registry.dispatch_node_dead("node-1")
    assert any(e[0] == "on_node_dead" and e[1] == "node-1" for e in good.events)

    # Node leave
    registry.dispatch_node_leave("node-1")
    assert any(e[0] == "on_node_leave" and e[1] == "node-1" for e in good.events)

    # Thermal eviction
    registry.dispatch_thermal_eviction("srv-1", "GPU temp > 85C", {"temp": 88})
    assert any(e[0] == "on_thermal_eviction" and e[1] == "srv-1" for e in good.events)

    # Failover
    registry.dispatch_failover("leader-1", "leader-2", 3)
    assert any(e[0] == "on_failover" and e[2] == "leader-2" for e in good.events)

    # Shutdown
    registry.dispatch_shutdown()
    assert good.unloaded is True


def test_webhook_alert_plugin_dispatch():
    webhook = WebhookAlertPlugin(
        webhook_url="https://alerts.internal.corp/webhook",
        events=["node_dead", "thermal_eviction"],
        timeout_sec=2.0,
    )
    assert webhook.name == "WebhookAlertPlugin"

    with patch("urllib.request.urlopen") as mock_urlopen:
        # Mock successful 200 response
        mock_resp = MagicMock()
        mock_resp.__enter__.return_value = mock_resp
        mock_urlopen.return_value = mock_resp

        # 1. node_dead event (included in events)
        webhook.on_node_dead("node-42")
        assert mock_urlopen.call_count == 1
        req = mock_urlopen.call_args[0][0]
        assert req.get_full_url() == "https://alerts.internal.corp/webhook"
        data = json.loads(req.data.decode("utf-8"))
        assert data["event"] == "node_dead"
        assert data["data"]["node_id"] == "node-42"
        assert webhook.dispatched_count == 1

        # 2. thermal_eviction event (included in events)
        webhook.on_thermal_eviction("gpu-box-1", "NVML temp 92C", {"gpu_temp": 92.0})
        assert mock_urlopen.call_count == 2
        req = mock_urlopen.call_args[0][0]
        data = json.loads(req.data.decode("utf-8"))
        assert data["event"] == "thermal_eviction"
        assert data["data"]["server_id"] == "gpu-box-1"
        assert webhook.dispatched_count == 2

        # 3. failover event (excluded from configured events list)
        webhook.on_failover("m1", "m2", 5)
        # Call count should not increase
        assert mock_urlopen.call_count == 2
        assert webhook.dispatched_count == 2

        # 4. Handle HTTP failure gracefully
        mock_urlopen.side_effect = Exception("Network unreachable")
        webhook.on_node_dead("node-99")
        # Should not raise exception
        assert webhook.dispatched_count == 2


def test_master_node_plugin_integration():
    audit_plugin = DummyAuditPlugin("master_audit")
    master = MasterNode(
        host="127.0.0.1",
        port=0,
        plugins=[audit_plugin],
    )

    # Verify registered
    plugins = master.list_plugins()
    assert len(plugins) == 1
    assert plugins[0]["name"] == "master_audit"

    # Dynamic registration and unregistration
    dynamic_plugin = DummyAuditPlugin("dynamic_plugin")
    master.register_plugin(dynamic_plugin)
    assert len(master.list_plugins()) == 2

    removed = master.unregister_plugin("dynamic_plugin")
    assert removed is dynamic_plugin
    assert len(master.list_plugins()) == 1

    # Simulate node join
    join_resp = master._handle_join({"node_id": "test-node-1", "address": "10.0.0.1", "port": 8080})
    assert join_resp["ok"] is True
    assert any(e[0] == "on_node_join" for e in audit_plugin.events)

    # Simulate node heartbeat
    hb_resp = master._handle_heartbeat("test-node-1", {"metrics": {"cpu": 20.0}})
    assert hb_resp["ok"] is True
    assert any(e[0] == "on_node_heartbeat" and e[1] == "test-node-1" for e in audit_plugin.events)

    # Simulate node leave
    leave_resp = master._handle_leave("test-node-1")
    assert leave_resp["ok"] is True
    assert any(e[0] == "on_node_leave" for e in audit_plugin.events)
