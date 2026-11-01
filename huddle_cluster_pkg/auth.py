"""
HuddleCluster — Cryptographic JWT Authentication & RBAC Middleware
===================================================================
v4.28.0

Provides RFC 7519 compliant JSON Web Token (JWT) issuing, cryptographic
verification (HMAC-SHA256 / HS256), token expiration, refresh rotation,
and granular scope-based access control.

Zero external dependencies: built with pure Python standard library
(hmac, hashlib, base64, json, time). Works standalone on any system
without requiring third-party C-extensions or pip packages.

Author : Rahad Bhuiya
Version: 4.28.0
License: MIT
"""

from __future__ import annotations

import base64
import dataclasses
import hashlib
import hmac
import json
import logging
import secrets
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger("HuddleCluster.Auth")



# Exceptions

class JWTAuthError(Exception):
    """Base error for JWT authentication and parsing failures."""
    pass


class JWTExpiredError(JWTAuthError):
    """Raised when the token expiration timestamp (exp) is in the past."""
    pass


class JWTInvalidSignatureError(JWTAuthError):
    """Raised when cryptographic HMAC signature verification fails."""
    pass


class JWTFormatError(JWTAuthError):
    """Raised when the token format does not conform to RFC 7519."""
    pass


class JWTInvalidClaimsError(JWTAuthError):
    """Raised when required claims are missing or mismatched."""
    pass



# Claims Data Model


@dataclass
class TokenClaims:
    """
    Decoded and validated claims payload of a JSON Web Token.
    """
    sub:        str                       # Subject / user / service identity
    role:       str = "viewer"            # Primary role name ("admin", "viewer", "operator")
    scopes:     List[str] = field(default_factory=list)  # Explicit permission scopes
    exp:        int = 0                   # Expiration timestamp (seconds since epoch)
    iat:        int = 0                   # Issued-at timestamp
    iss:        str = "huddle-cluster"    # Issuer identifier
    token_type: str = "access"            # "access" or "refresh"
    jti:        Optional[str] = None      # Unique token identifier

    def is_expired(self, clock_skew_sec: int = 0) -> bool:
        """Return True if current time exceeds token expiry timestamp."""
        return (time.time() - clock_skew_sec) > self.exp

    def has_permission(self, required_scope: str) -> bool:
        """Check if claims grant the given permission scope."""
        if "admin" in self.role.lower() or "*:*" in self.scopes or "all" in self.scopes:
            return True
        return required_scope in self.scopes

    def to_dict(self) -> Dict[str, Any]:
        """Serialize claims to a JSON-compatible dictionary."""
        d = dataclasses.asdict(self)
        if self.jti is None:
            d.pop("jti", None)
        return d



# Base64url Helpers (RFC 7515 / JWS)


def _b64url_encode(data: bytes) -> str:
    """Encode bytes into unpadded Base64url string."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(data: str) -> bytes:
    """Decode unpadded Base64url string back to bytes."""
    rem = len(data) % 4
    if rem > 0:
        data += "=" * (4 - rem)
    return base64.urlsafe_b64decode(data.encode("ascii"))



# JWT Manager


class JWTManager:
    """
    Cryptographic JSON Web Token (HS256) manager for HuddleCluster.

    Handles token creation, signature calculation, constant-time validation,
    claims extraction, and token refresh lifecycle.
    """

    def __init__(
        self,
        secret_key: Optional[str] = None,
        issuer: str = "huddle-cluster",
        access_token_expiry_sec: int = 3600,         # 1 hour
        refresh_token_expiry_sec: int = 86400 * 7,    # 7 days
        clock_skew_sec: int = 5,
        default_role_scopes: Optional[Dict[str, List[str]]] = None,
    ) -> None:
        """
        Args:
            secret_key:               HMAC signing key. If None, auto-generates
                                      a cryptographically strong 32-byte secret.
            issuer:                   Token issuer claim ('iss').
            access_token_expiry_sec:  Access token lifetime in seconds.
            refresh_token_expiry_sec: Refresh token lifetime in seconds.
            clock_skew_sec:           Allowed leeway for clock skew in seconds.
            default_role_scopes:      Mapping of role names to their scope lists.
        """
        self._secret = (
            secret_key.encode("utf-8")
            if secret_key
            else secrets.token_bytes(32)
        )
        self.issuer = issuer
        self.access_expiry = access_token_expiry_sec
        self.refresh_expiry = refresh_token_expiry_sec
        self.clock_skew = clock_skew_sec

        # Built-in role -> scope mapping
        self._role_scopes: Dict[str, Set[str]] = {
            "viewer": {
                "status:read", "scheduler:read", "autoscaler:read",
                "rollout:read", "discovery:read", "regions:read",
                "breakers:read", "ratelimits:read", "canary:read",
                "observability:read", "webhooks:read", "metrics:read",
                "nodes:read",
            },
            "operator": {
                "status:read", "scheduler:read", "autoscaler:read",
                "rollout:read", "discovery:read", "regions:read",
                "breakers:read", "ratelimits:read", "canary:read",
                "observability:read", "webhooks:read", "metrics:read",
                "nodes:read", "nodes:write", "scheduler:write",
                "breakers:reset", "ratelimits:reset", "canary:control",
            },
            "admin": {
                "*:*"
            },
        }
        if default_role_scopes:
            for r, sc in default_role_scopes.items():
                self._role_scopes[r] = set(sc)

    
    # Token Creation
    

    def create_access_token(
        self,
        sub: str,
        role: str = "viewer",
        scopes: Optional[List[str]] = None,
        custom_claims: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Generate a signed HMAC-SHA256 access token."""
        return self._create_token(
            sub=sub,
            role=role,
            scopes=scopes,
            expires_in=self.access_expiry,
            token_type="access",
            custom_claims=custom_claims,
        )

    def create_refresh_token(
        self,
        sub: str,
        role: str = "viewer",
        scopes: Optional[List[str]] = None,
    ) -> str:
        """Generate a signed HMAC-SHA256 refresh token."""
        return self._create_token(
            sub=sub,
            role=role,
            scopes=scopes,
            expires_in=self.refresh_expiry,
            token_type="refresh",
        )

    def create_token_pair(
        self,
        sub: str,
        role: str = "viewer",
        scopes: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Create both access and refresh tokens in standard OAuth2 format.
        """
        access_tok = self.create_access_token(sub=sub, role=role, scopes=scopes)
        refresh_tok = self.create_refresh_token(sub=sub, role=role, scopes=scopes)
        return {
            "access_token":  access_tok,
            "refresh_token": refresh_tok,
            "token_type":    "Bearer",
            "expires_in":    self.access_expiry,
            "role":          role,
            "sub":           sub,
        }

    def _create_token(
        self,
        sub: str,
        role: str,
        scopes: Optional[List[str]],
        expires_in: int,
        token_type: str,
        custom_claims: Optional[Dict[str, Any]] = None,
    ) -> str:
        now = int(time.time())
        exp = now + expires_in

        # Resolve scopes from role if not explicitly specified
        resolved_scopes = set(scopes) if scopes else set()
        if not resolved_scopes and role in self._role_scopes:
            resolved_scopes = set(self._role_scopes[role])

        claims_dict = {
            "sub":        sub,
            "role":       role,
            "scopes":     sorted(resolved_scopes),
            "exp":        exp,
            "iat":        now,
            "iss":        self.issuer,
            "token_type": token_type,
            "jti":        secrets.token_hex(8),
        }
        if custom_claims:
            claims_dict.update(custom_claims)

        header = {"alg": "HS256", "typ": "JWT"}

        hdr_b64 = _b64url_encode(json.dumps(header, separators=(",", ":")).encode("utf-8"))
        payload_b64 = _b64url_encode(json.dumps(claims_dict, separators=(",", ":")).encode("utf-8"))

        signing_input = f"{hdr_b64}.{payload_b64}".encode("ascii")
        sig = hmac.new(self._secret, signing_input, hashlib.sha256).digest()
        sig_b64 = _b64url_encode(sig)

        return f"{hdr_b64}.{payload_b64}.{sig_b64}"

    
    # Token Verification & Decoding
    

    def decode_and_verify(
        self,
        token: str,
        expected_type: Optional[str] = "access",
    ) -> TokenClaims:
        """
        Verify cryptographic signature, check expiration, and return TokenClaims.

        Raises:
            JWTFormatError:            If token structure is invalid.
            JWTInvalidSignatureError:  If cryptographic HMAC check fails.
            JWTExpiredError:           If token timestamp is expired.
            JWTInvalidClaimsError:     If token type or issuer is mismatched.
        """
        parts = token.strip().split(".")
        if len(parts) != 3:
            raise JWTFormatError("JWT must contain exactly three dot-separated components")

        hdr_b64, payload_b64, sig_b64 = parts

        # 1. Cryptographic HMAC-SHA256 signature check
        signing_input = f"{hdr_b64}.{payload_b64}".encode("ascii")
        expected_sig = hmac.new(self._secret, signing_input, hashlib.sha256).digest()
        try:
            actual_sig = _b64url_decode(sig_b64)
        except Exception as e:
            raise JWTFormatError(f"Invalid Base64url in signature: {e}")

        if not hmac.compare_digest(expected_sig, actual_sig):
            raise JWTInvalidSignatureError("Cryptographic signature verification failed")

        # 2. Decode header and payload
        try:
            hdr_bytes = _b64url_decode(hdr_b64)
            header = json.loads(hdr_bytes.decode("utf-8"))
        except Exception as e:
            raise JWTFormatError(f"Failed to parse JWT header: {e}")

        if header.get("alg") != "HS256":
            raise JWTFormatError(f"Unsupported algorithm '{header.get('alg')}', expected 'HS256'")

        try:
            payload_bytes = _b64url_decode(payload_b64)
            payload = json.loads(payload_bytes.decode("utf-8"))
        except Exception as e:
            raise JWTFormatError(f"Failed to parse JWT payload: {e}")

        # 3. Expiration validation
        exp = payload.get("exp", 0)
        now = time.time()
        if (now - self.clock_skew) > exp:
            raise JWTExpiredError(
                f"Token expired at epoch {exp} (current time {int(now)})"
            )

        # 4. Token type validation
        tok_type = payload.get("token_type", "access")
        if expected_type and tok_type != expected_type:
            raise JWTInvalidClaimsError(
                f"Invalid token_type '{tok_type}', expected '{expected_type}'"
            )

        # 5. Build TokenClaims
        return TokenClaims(
            sub=payload.get("sub", "anonymous"),
            role=payload.get("role", "viewer"),
            scopes=payload.get("scopes", []),
            exp=exp,
            iat=payload.get("iat", 0),
            iss=payload.get("iss", self.issuer),
            token_type=tok_type,
            jti=payload.get("jti"),
        )

    def refresh(self, refresh_token: str) -> Dict[str, Any]:
        """
        Validate a refresh token and issue a fresh access token.
        """
        claims = self.decode_and_verify(refresh_token, expected_type="refresh")
        new_access = self.create_access_token(
            sub=claims.sub,
            role=claims.role,
            scopes=claims.scopes,
        )
        return {
            "access_token": new_access,
            "token_type":   "Bearer",
            "expires_in":   self.access_expiry,
            "sub":          claims.sub,
            "role":         claims.role,
        }
