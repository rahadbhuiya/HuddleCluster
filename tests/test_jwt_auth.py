"""
Unit tests for JWT Authentication & RBAC Middleware in HuddleCluster (v4.28.0)
==============================================================================
Covers:
  1. JWT token generation, HMAC-SHA256 signing, and claims validation.
  2. Cryptographic signature tampering detection.
  3. Token expiration (exp) and clock skew tolerance.
  4. Token pair (access + refresh) generation and rotation lifecycle.
  5. Granular scope-based permission evaluation (viewer, operator, admin).
  6. Malformed and invalid token rejection guards.
  7. MasterNode REST API integration (POST /v1/auth/token, POST /v1/auth/refresh).
  8. MasterNode route protection with Bearer JWT token.
  9. GET /v1/auth/me user inspection endpoint.
 10. Backward compatibility with static API keys alongside JWT.
"""

import json
import time
import unittest
import urllib.error
import urllib.request
from unittest.mock import MagicMock

from huddle_cluster_pkg.auth import (
    JWTAuthError,
    JWTExpiredError,
    JWTFormatError,
    JWTInvalidClaimsError,
    JWTInvalidSignatureError,
    JWTManager,
    TokenClaims,
)
from huddle_cluster_pkg.cluster_master import MasterNode



# Helpers


def _free_port():
    import socket
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def _request(url, method="GET", payload=None, headers=None):
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            body = r.read()
            return r.status, json.loads(body.decode("utf-8")) if body else {}
    except urllib.error.HTTPError as e:
        body = e.read()
        try:
            return e.code, json.loads(body.decode("utf-8"))
        except Exception:
            return e.code, {"error": str(e)}



# Unit Tests: JWTManager Engine


class TestJWTManagerEngine(unittest.TestCase):
    """Test cryptographic token creation and verification."""

    def setUp(self):
        self.mgr = JWTManager(secret_key="test-secret-key-12345", issuer="test-cluster")

    def test_create_and_verify_access_token(self):
        tok = self.mgr.create_access_token(sub="alice", role="admin")
        self.assertIsInstance(tok, str)
        self.assertEqual(tok.count("."), 2)

        claims = self.mgr.decode_and_verify(tok)
        self.assertEqual(claims.sub, "alice")
        self.assertEqual(claims.role, "admin")
        self.assertEqual(claims.iss, "test-cluster")
        self.assertEqual(claims.token_type, "access")
        self.assertFalse(claims.is_expired())

    def test_tampered_payload_rejected(self):
        tok = self.mgr.create_access_token(sub="alice", role="viewer")
        parts = tok.split(".")
        # Tamper payload by replacing 'viewer' with 'admin' in base64
        tampered = f"{parts[0]}.eyJyZXBsYWNlZCI6InRydWUifQ.{parts[2]}"
        with self.assertRaises(JWTInvalidSignatureError):
            self.mgr.decode_and_verify(tampered)

    def test_tampered_signature_rejected(self):
        tok = self.mgr.create_access_token(sub="alice")
        parts = tok.split(".")
        bad_sig = "A" * len(parts[2])
        with self.assertRaises(JWTInvalidSignatureError):
            self.mgr.decode_and_verify(f"{parts[0]}.{parts[1]}.{bad_sig}")

    def test_expired_token_raises_error(self):
        short_mgr = JWTManager(secret_key="sec", access_token_expiry_sec=-10, clock_skew_sec=0)
        tok = short_mgr.create_access_token(sub="bob")
        with self.assertRaises(JWTExpiredError):
            short_mgr.decode_and_verify(tok)

    def test_malformed_tokens(self):
        with self.assertRaises(JWTFormatError):
            self.mgr.decode_and_verify("not.a.valid.jwt.with.too.many.dots")
        with self.assertRaises(JWTFormatError):
            self.mgr.decode_and_verify("onlyonedot.here")
        with self.assertRaises(JWTFormatError):
            self.mgr.decode_and_verify("nodotsatall")

    def test_refresh_token_lifecycle(self):
        pair = self.mgr.create_token_pair(sub="charlie", role="operator")
        self.assertIn("access_token", pair)
        self.assertIn("refresh_token", pair)

        # Refresh token cannot be used as access token
        with self.assertRaises(JWTInvalidClaimsError):
            self.mgr.decode_and_verify(pair["refresh_token"], expected_type="access")

        # Can be verified as refresh token
        rf_claims = self.mgr.decode_and_verify(pair["refresh_token"], expected_type="refresh")
        self.assertEqual(rf_claims.sub, "charlie")

        # Rotate refresh token to new access token
        new_pair = self.mgr.refresh(pair["refresh_token"])
        self.assertIn("access_token", new_pair)
        new_claims = self.mgr.decode_and_verify(new_pair["access_token"])
        self.assertEqual(new_claims.sub, "charlie")
        self.assertEqual(new_claims.role, "operator")


class TestTokenClaimsPermissions(unittest.TestCase):
    """Test scope-based access checks on TokenClaims."""

    def test_admin_has_all_permissions(self):
        claims = TokenClaims(sub="root", role="admin", scopes=["*:*"])
        self.assertTrue(claims.has_permission("nodes:read"))
        self.assertTrue(claims.has_permission("nodes:write"))
        self.assertTrue(claims.has_permission("canary:control"))

    def test_viewer_has_read_only(self):
        claims = TokenClaims(sub="guest", role="viewer", scopes=["status:read", "nodes:read"])
        self.assertTrue(claims.has_permission("nodes:read"))
        self.assertFalse(claims.has_permission("nodes:write"))
        self.assertFalse(claims.has_permission("canary:control"))

    def test_operator_permissions(self):
        claims = TokenClaims(sub="op", role="operator", scopes=["nodes:read", "nodes:write"])
        self.assertTrue(claims.has_permission("nodes:read"))
        self.assertTrue(claims.has_permission("nodes:write"))
        self.assertFalse(claims.has_permission("webhooks:write"))



# Integration Tests: MasterNode REST API with JWT Auth

class TestMasterNodeJWTAuth(unittest.TestCase):
    """Integration test against a live MasterNode with JWT authentication."""

    @classmethod
    def setUpClass(cls):
        cls.port = _free_port()
        cls.users = {
            "admin_user": {
                "password": "secret-admin-pass",
                "role": "admin",
            },
            "viewer_user": {
                "password": "secret-viewer-pass",
                "role": "viewer",
            },
        }
        cls.master = MasterNode(
            host="127.0.0.1",
            port=cls.port,
            jwt_secret="master-secret-key-secure",
            jwt_users=cls.users,
            api_keys={"static-legacy-key": "admin"},
        )
        cls.master.start()
        time.sleep(0.2)
        cls.base_url = f"http://127.0.0.1:{cls.port}/v1"

    @classmethod
    def tearDownClass(cls):
        cls.master.stop()

    def test_token_issuance_success(self):
        code, body = _request(
            f"{self.base_url}/auth/token",
            method="POST",
            payload={"username": "admin_user", "password": "secret-admin-pass"},
        )
        self.assertEqual(code, 200)
        self.assertIn("access_token", body)
        self.assertIn("refresh_token", body)
        self.assertEqual(body["token_type"], "Bearer")
        self.assertEqual(body["role"], "admin")

    def test_token_issuance_invalid_credentials(self):
        code, body = _request(
            f"{self.base_url}/auth/token",
            method="POST",
            payload={"username": "admin_user", "password": "wrong-password"},
        )
        self.assertEqual(code, 401)
        self.assertFalse(body.get("ok"))

    def test_unauthenticated_request_rejected(self):
        code, body = _request(f"{self.base_url}/status")
        self.assertEqual(code, 401)
        self.assertIn("missing Authorization header", body.get("error", ""))

    def test_authenticated_access_with_valid_jwt(self):
        # 1. Login
        _, token_body = _request(
            f"{self.base_url}/auth/token",
            method="POST",
            payload={"username": "admin_user", "password": "secret-admin-pass"},
        )
        token = token_body["access_token"]

        # 2. Access protected endpoint
        code, body = _request(
            f"{self.base_url}/status",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(code, 200)
        self.assertIn("api_version", body)

    def test_viewer_jwt_cannot_perform_write(self):
        # 1. Login as viewer
        _, token_body = _request(
            f"{self.base_url}/auth/token",
            method="POST",
            payload={"username": "viewer_user", "password": "secret-viewer-pass"},
        )
        token = token_body["access_token"]

        # 2. Try write operation (join node) -> 403 Forbidden
        code, body = _request(
            f"{self.base_url}/nodes/join",
            method="POST",
            payload={"node_id": "rogue-agent", "address": "127.0.0.1", "port": 8080},
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(code, 403)
        self.assertIn("permission", body.get("error", ""))

    def test_auth_me_endpoint(self):
        _, token_body = _request(
            f"{self.base_url}/auth/token",
            method="POST",
            payload={"username": "admin_user", "password": "secret-admin-pass"},
        )
        token = token_body["access_token"]

        code, body = _request(
            f"{self.base_url}/auth/me",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(code, 200)
        self.assertEqual(body["sub"], "admin_user")
        self.assertEqual(body["role"], "admin")

    def test_token_refresh_endpoint(self):
        _, token_body = _request(
            f"{self.base_url}/auth/token",
            method="POST",
            payload={"username": "viewer_user", "password": "secret-viewer-pass"},
        )
        refresh_token = token_body["refresh_token"]

        # Refresh
        code, ref_body = _request(
            f"{self.base_url}/auth/refresh",
            method="POST",
            payload={"refresh_token": refresh_token},
        )
        self.assertEqual(code, 200)
        self.assertIn("access_token", ref_body)

        # Access with new access token
        new_token = ref_body["access_token"]
        code, body = _request(
            f"{self.base_url}/status",
            headers={"Authorization": f"Bearer {new_token}"},
        )
        self.assertEqual(code, 200)

    def test_legacy_static_api_key_still_works(self):
        code, body = _request(
            f"{self.base_url}/status",
            headers={"Authorization": "Bearer static-legacy-key"},
        )
        self.assertEqual(code, 200)
        self.assertIn("api_version", body)


if __name__ == "__main__":
    unittest.main()
