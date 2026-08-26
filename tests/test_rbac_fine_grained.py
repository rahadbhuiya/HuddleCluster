"""
Tests for fine-grained RBAC permission scopes (v4.15.0) —
huddle_cluster_pkg.cluster_master's _resolve_api_key_permissions() and
scope-based _check_auth().

Covers: custom scope lists alongside the legacy "admin"/"viewer" role
strings, validation/fail-fast behavior, and real HTTP requests proving
a key with only specific scopes can do exactly what it was granted and
nothing else.
"""

import json
import time
import urllib.error
import urllib.request

import pytest

from huddle_cluster_pkg.cluster_master import (
    MasterNode,
    _ALL_PERMISSIONS,
    _READ_PERMISSIONS,
    _BUILTIN_ROLES,
    _resolve_api_key_permissions,
)


def _free_port():
    import socket
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close()
    return p


def _get(port, path, api_key=None):
    url = f"http://127.0.0.1:{port}/v1{path}"
    req = urllib.request.Request(url)
    if api_key:
        req.add_header("Authorization", f"Bearer {api_key}")
    with urllib.request.urlopen(req, timeout=3) as r:
        return json.loads(r.read())


def _post(port, path, payload, api_key=None):
    data = json.dumps(payload).encode()
    req  = urllib.request.Request(
        f"http://127.0.0.1:{port}/v1{path}", data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    if api_key:
        req.add_header("Authorization", f"Bearer {api_key}")
    try:
        with urllib.request.urlopen(req, timeout=3) as r:
            return r.getcode(), json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


# Unit tests — _resolve_api_key_permissions


class TestResolveApiKeyPermissions:
    def test_none_input_returns_none(self):
        assert _resolve_api_key_permissions(None) is None

    def test_admin_role_expands_to_all_permissions(self):
        resolved = _resolve_api_key_permissions({"k": "admin"})
        assert resolved["k"] == _ALL_PERMISSIONS

    def test_viewer_role_expands_to_read_only(self):
        resolved = _resolve_api_key_permissions({"k": "viewer"})
        assert resolved["k"] == _READ_PERMISSIONS
        assert "nodes:write" not in resolved["k"]
        assert "nodes:read" in resolved["k"]

    def test_explicit_scope_list(self):
        resolved = _resolve_api_key_permissions(
            {"k": ["nodes:read", "canary:control"]})
        assert resolved["k"] == frozenset({"nodes:read", "canary:control"})

    def test_explicit_scope_set(self):
        resolved = _resolve_api_key_permissions({"k": {"nodes:read"}})
        assert resolved["k"] == frozenset({"nodes:read"})

    def test_unknown_role_string_raises(self):
        with pytest.raises(ValueError, match="unknown role"):
            _resolve_api_key_permissions({"k": "superadmin"})

    def test_unknown_scope_in_list_raises(self):
        with pytest.raises(ValueError, match="unknown permission scope"):
            _resolve_api_key_permissions({"k": ["nodes:read", "nodes:delete_everything"]})

    def test_non_string_non_iterable_value_raises(self):
        with pytest.raises(ValueError, match="must be a role name string"):
            _resolve_api_key_permissions({"k": 12345})

    def test_multiple_keys_mixed_roles_and_scopes(self):
        resolved = _resolve_api_key_permissions({
            "admin-key": "admin",
            "viewer-key": "viewer",
            "canary-only-key": ["canary:control", "canary:read"],
        })
        assert resolved["admin-key"] == _ALL_PERMISSIONS
        assert resolved["viewer-key"] == _READ_PERMISSIONS
        assert resolved["canary-only-key"] == frozenset({"canary:control", "canary:read"})

    def test_builtin_roles_cover_every_permission(self):
        """Sanity check on the constants themselves: admin's bundle must
        be a superset of viewer's, and viewer must be exactly the
        :read-suffixed subset of all permissions."""
        assert _BUILTIN_ROLES["viewer"] <= _BUILTIN_ROLES["admin"]
        assert _BUILTIN_ROLES["admin"] == _ALL_PERMISSIONS
        assert all(p.endswith(":read") for p in _BUILTIN_ROLES["viewer"])


# Construction-time fail-fast


class TestConstructionValidation:
    def test_master_construction_fails_fast_on_unknown_role(self):
        with pytest.raises(ValueError, match="unknown role"):
            MasterNode(port=_free_port(), api_keys={"k": "owner"})

    def test_master_construction_fails_fast_on_unknown_scope(self):
        with pytest.raises(ValueError, match="unknown permission scope"):
            MasterNode(port=_free_port(), api_keys={"k": ["not:a:real:scope"]})

    def test_master_construction_succeeds_with_valid_scopes(self):
        # Should not raise
        m = MasterNode(port=_free_port(), api_keys={"k": ["nodes:read"]})
        assert m is not None


# HTTP integration — a key with a narrow scope list can do exactly
# what it was granted, nothing more


class TestFineGrainedHTTP:
    @pytest.fixture
    def master(self):
        port = _free_port()
        m = MasterNode(
            host="127.0.0.1", port=port, heartbeat_timeout_sec=60,
            api_keys={
                "nodes-read-only":   ["nodes:read"],
                "canary-controller": ["canary:read", "canary:control"],
                "breaker-resetter":  ["breakers:reset"],
                "full-admin":        "admin",
                "read-all":          "viewer",
            },
        )
        m.start()
        time.sleep(0.1)
        yield m
        m.stop()

    def test_nodes_read_only_key_can_read_nodes(self, master):
        code, body = _post(master.port, "/nodes/join",
                            {"node_id": "n1", "address": "1.2.3.4", "port": 1},
                            api_key="full-admin")
        assert code == 200

        result = _get(master.port, "/nodes", api_key="nodes-read-only")
        assert "nodes" in result

    def test_nodes_read_only_key_cannot_join_nodes(self, master):
        code, body = _post(master.port, "/nodes/join",
                            {"node_id": "n2", "address": "1.2.3.4", "port": 1},
                            api_key="nodes-read-only")
        assert code == 403
        assert "nodes:write" in body["error"]

    def test_nodes_read_only_key_cannot_read_canary(self, master):
        with pytest.raises(urllib.error.HTTPError) as exc:
            _get(master.port, "/canary/status", api_key="nodes-read-only")
        assert exc.value.code == 403

    def test_canary_controller_can_read_and_control_canary(self, master):
        code, body = _post(master.port, "/canary/start", {"weight": 5},
                            api_key="canary-controller")
        # 503 if canary feature isn't enabled on this master (it isn't,
        # in this fixture) — that's fine, the point is it's NOT a 401/403
        assert code != 401
        assert code != 403

        with pytest.raises(urllib.error.HTTPError) as exc:
            _get(master.port, "/status", api_key="canary-controller")
        assert exc.value.code == 403   # status:read wasn't granted

    def test_breaker_resetter_can_reset_but_not_read(self, master):
        code, body = _post(master.port, "/breakers/some-node/reset", {},
                            api_key="breaker-resetter")
        assert code != 401
        assert code != 403

        with pytest.raises(urllib.error.HTTPError) as exc:
            _get(master.port, "/breakers", api_key="breaker-resetter")
        assert exc.value.code == 403   # breakers:read wasn't granted

    def test_legacy_admin_role_still_has_full_access(self, master):
        for path in ("/status", "/nodes"):
            result = _get(master.port, path, api_key="full-admin")
            assert result is not None
        # /metrics is Prometheus text exposition, not JSON — just
        # confirm the admin key can reach it (no 401/403).
        url = f"http://127.0.0.1:{master.port}/v1/metrics"
        req = urllib.request.Request(url)
        req.add_header("Authorization", "Bearer full-admin")
        with urllib.request.urlopen(req, timeout=3) as r:
            assert r.getcode() == 200

    def test_legacy_viewer_role_still_read_only(self, master):
        result = _get(master.port, "/status", api_key="read-all")
        assert result is not None

        code, _ = _post(master.port, "/nodes/join",
                         {"node_id": "n3", "address": "1.2.3.4", "port": 1},
                         api_key="read-all")
        assert code == 403

    def test_invalid_key_rejected(self, master):
        with pytest.raises(urllib.error.HTTPError) as exc:
            _get(master.port, "/status", api_key="not-a-real-key")
        assert exc.value.code == 401

    def test_error_message_names_the_missing_scope(self, master):
        with pytest.raises(urllib.error.HTTPError) as exc:
            _get(master.port, "/status", api_key="nodes-read-only")
        body = json.loads(exc.value.read())
        assert "status:read" in body["error"]