"""
Tests for the CLI's --api-key parsing supporting fine-grained
permission scopes (v4.15.0), alongside the legacy admin/viewer role
strings — huddle_cluster_pkg.cli's cmd_master_start().
"""

import json
import subprocess
import sys
import time
import urllib.error
import urllib.request

import pytest


def _free_port():
    import socket
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close()
    return p


def _wait_reachable(port, timeout=8):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/v1/health", timeout=1):
                return True
        except Exception:
            time.sleep(0.2)
    return False


def _get(port, path, api_key=None):
    req = urllib.request.Request(f"http://127.0.0.1:{port}/v1{path}")
    if api_key:
        req.add_header("Authorization", f"Bearer {api_key}")
    try:
        with urllib.request.urlopen(req, timeout=3) as r:
            return r.getcode(), json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


class TestCLIFineGrainedApiKey:
    def test_legacy_role_still_works(self):
        port = _free_port()
        proc = subprocess.Popen(
            [sys.executable, "-m", "huddle_cluster_pkg.cli", "master", "start",
             "--port", str(port), "--api-key", "admin-key=admin"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        try:
            assert _wait_reachable(port)
            code, body = _get(port, "/status", api_key="admin-key")
            assert code == 200
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()

    def test_comma_separated_scopes(self):
        port = _free_port()
        proc = subprocess.Popen(
            [sys.executable, "-m", "huddle_cluster_pkg.cli", "master", "start",
             "--port", str(port),
             "--api-key", "narrow-key=nodes:read,canary:control"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        try:
            assert _wait_reachable(port)
            code, _ = _get(port, "/nodes", api_key="narrow-key")
            assert code == 200
            code, body = _get(port, "/status", api_key="narrow-key")
            assert code == 403
            assert "status:read" in body["error"]
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()

    def test_single_specific_scope_not_confused_with_role(self):
        """A single non-role scope (e.g. "nodes:read" alone, no comma)
        must still be treated as a scope list, not misparsed."""
        port = _free_port()
        proc = subprocess.Popen(
            [sys.executable, "-m", "huddle_cluster_pkg.cli", "master", "start",
             "--port", str(port), "--api-key", "reader=nodes:read"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        try:
            assert _wait_reachable(port)
            code, _ = _get(port, "/nodes", api_key="reader")
            assert code == 200
            code, _ = _get(port, "/status", api_key="reader")
            assert code == 403
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()

    def test_multiple_keys_mixed_roles_and_scopes(self):
        port = _free_port()
        proc = subprocess.Popen(
            [sys.executable, "-m", "huddle_cluster_pkg.cli", "master", "start",
             "--port", str(port),
             "--api-key", "admin-key=admin",
             "--api-key", "viewer-key=viewer",
             "--api-key", "narrow-key=breakers:reset"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        try:
            assert _wait_reachable(port)
            assert _get(port, "/status", api_key="admin-key")[0] == 200
            assert _get(port, "/status", api_key="viewer-key")[0] == 200
            assert _get(port, "/status", api_key="narrow-key")[0] == 403
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()

    def test_invalid_scope_exits_cleanly_not_traceback(self):
        port = _free_port()
        proc = subprocess.run(
            [sys.executable, "-m", "huddle_cluster_pkg.cli", "master", "start",
             "--port", str(port),
             "--api-key", "bad-key=nodes:read,not:a:real:scope"],
            capture_output=True, text=True, timeout=10,
        )
        assert proc.returncode != 0
        combined = proc.stdout + proc.stderr
        assert "unknown permission scope" in combined
        assert "Traceback" not in combined

    def test_invalid_role_exits_cleanly_not_traceback(self):
        port = _free_port()
        proc = subprocess.run(
            [sys.executable, "-m", "huddle_cluster_pkg.cli", "master", "start",
             "--port", str(port), "--api-key", "bad-key=superadmin"],
            capture_output=True, text=True, timeout=10,
        )
        assert proc.returncode != 0
        combined = proc.stdout + proc.stderr
        assert "unknown role" in combined
        assert "Traceback" not in combined