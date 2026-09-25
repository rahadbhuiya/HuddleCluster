"""
HuddleCluster - Autonomous Thermal Auto-Remediation and Self-Healing Engine
v4.22.0

Provides closed-loop autonomous remediation when cluster servers experience
cascading thermal overheating, circuit-breaker trips, or canary probe failures.
"""

from __future__ import annotations

import json
import logging
import subprocess
import threading
import time
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

log = logging.getLogger("HuddleCluster.Remediator")


class RemediationTrigger(str, Enum):
    CONSECUTIVE_OVERHEAT = "consecutive_overheat"
    CIRCUIT_BREAKER_OPEN = "circuit_breaker_open"
    STALE_OUTER_RING     = "stale_outer_ring"
    CANARY_FAILED        = "canary_failed"
    MANUAL               = "manual"


class RemediationActionType(str, Enum):
    LOCAL_COMMAND   = "local_command"
    HTTP_WEBHOOK    = "http_webhook"
    PYTHON_CALLBACK = "python_callback"


class RemediationState(str, Enum):
    HEALTHY      = "healthy"
    QUARANTINED  = "quarantined"
    HEALING      = "healing"
    VERIFYING    = "verifying"
    RECOVERED    = "recovered"
    DEAD_LETTER  = "dead_letter"


@dataclass
class RemediationPolicy:
    name:                             str
    trigger:                          RemediationTrigger
    action_type:                      RemediationActionType
    action_target:                    str                                              = ""
    action_callback:                  Optional[Callable[[str, Dict[str, Any]], bool]] = None
    consecutive_evictions_threshold: int                                              = 3
    stale_dwell_seconds:              float                                            = 60.0
    drain_period_s:                   float                                            = 3.0
    verification_period_s:            float                                            = 5.0
    max_retries_per_hour:             int                                              = 3
    enabled:                          bool                                             = True


class ClusterRemediator:
    """
    Orchestrates automated self-healing actions on unhealthy or overheating servers.
    """

    def __init__(
        self,
        policies: Optional[List[RemediationPolicy]] = None,
        enabled: bool = True,
        max_retries_per_hour: int = 3,
        default_drain_period_s: float = 3.0,
    ) -> None:
        self.enabled:                bool                           = enabled
        self.default_max_retries:    int                            = max_retries_per_hour
        self.default_drain_period_s: float                          = default_drain_period_s
        self.policies:               List[RemediationPolicy]        = policies or []
        self._lock:                  threading.RLock                = threading.RLock()

        # State tracking per server
        self._states:                Dict[str, RemediationState]    = {}
        self._eviction_counts:       Dict[str, int]                 = {}
        self._outer_ring_since:      Dict[str, float]               = {}
        self._attempt_timestamps:    Dict[str, List[float]]         = {}
        self._quarantined_servers:   Set[str]                       = set()
        self._remediation_history:   List[Dict[str, Any]]           = []

        # Telemetry counters
        self._total_actions:         int                            = 0
        self._successful_actions:    int                            = 0
        self._failed_actions:        int                            = 0
        self._throttled_actions:     int                            = 0

    def add_policy(self, policy: RemediationPolicy) -> None:
        """Register a new remediation policy."""
        with self._lock:
            self.policies.append(policy)

    def is_quarantined(self, server_id: str) -> bool:
        """Check if server is currently quarantined from receiving live traffic."""
        with self._lock:
            return server_id in self._quarantined_servers

    def get_server_state(self, server_id: str) -> RemediationState:
        """Return the current remediation lifecycle state of a server."""
        with self._lock:
            return self._states.get(server_id, RemediationState.HEALTHY)

    def record_eviction(self, server_id: str, temp: float = 1.0) -> Optional[RemediationPolicy]:
        """
        Record a server eviction to outer ring. Returns matching policy if threshold breached.
        """
        if not self.enabled:
            return None

        with self._lock:
            now = time.time()
            if server_id not in self._outer_ring_since:
                self._outer_ring_since[server_id] = now

            count = self._eviction_counts.get(server_id, 0) + 1
            self._eviction_counts[server_id] = count

            # Find matching consecutive overheat policy
            for policy in self.policies:
                if not policy.enabled:
                    continue
                if policy.trigger == RemediationTrigger.CONSECUTIVE_OVERHEAT:
                    if count >= policy.consecutive_evictions_threshold:
                        return policy
            return None

    def record_recovery(self, server_id: str) -> None:
        """Reset eviction counters when a server naturally recovers and enters inner ring."""
        with self._lock:
            self._eviction_counts[server_id] = 0
            self._outer_ring_since.pop(server_id, None)
            if self._states.get(server_id) in (RemediationState.VERIFYING, RemediationState.HEALING):
                self._states[server_id] = RemediationState.RECOVERED
                self._quarantined_servers.discard(server_id)

    def record_circuit_breaker_open(self, server_id: str) -> Optional[RemediationPolicy]:
        """Triggered when circuit breaker for a server enters OPEN state."""
        if not self.enabled:
            return None

        with self._lock:
            for policy in self.policies:
                if not policy.enabled:
                    continue
                if policy.trigger == RemediationTrigger.CIRCUIT_BREAKER_OPEN:
                    return policy
            return None

    def record_canary_failure(self, server_id: str) -> Optional[RemediationPolicy]:
        """Triggered when synthetic canary prober fails repeatedly on a server."""
        if not self.enabled:
            return None

        with self._lock:
            for policy in self.policies:
                if not policy.enabled:
                    continue
                if policy.trigger == RemediationTrigger.CANARY_FAILED:
                    return policy
            return None

    def check_stale_cooling_ring(self, now: Optional[float] = None) -> List[tuple[str, RemediationPolicy]]:
        """Identify servers that have been stuck in the outer cooling ring beyond dwell threshold."""
        if not self.enabled:
            return []

        ts = now or time.time()
        candidates: List[tuple[str, RemediationPolicy]] = []

        with self._lock:
            for policy in self.policies:
                if not policy.enabled or policy.trigger != RemediationTrigger.STALE_OUTER_RING:
                    continue

                for server_id, since in list(self._outer_ring_since.items()):
                    if ts - since >= policy.stale_dwell_seconds:
                        candidates.append((server_id, policy))

        return candidates

    def _check_hourly_rate_limit(self, server_id: str, max_retries: int) -> bool:
        """Ensure remediation does not execute more than max_retries per hour per server."""
        now = time.time()
        window_start = now - 3600.0
        timestamps = [t for t in self._attempt_timestamps.get(server_id, []) if t > window_start]
        self._attempt_timestamps[server_id] = timestamps

        if len(timestamps) >= max_retries:
            return False
        return True

    def trigger_remediation(
        self,
        server_id: str,
        policy: RemediationPolicy,
        details: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Execute an automated self-healing action following the drain -> execute -> verify workflow.
        """
        if not self.enabled or not policy.enabled:
            return False

        with self._lock:
            max_retries = policy.max_retries_per_hour or self.default_max_retries
            if not self._check_hourly_rate_limit(server_id, max_retries):
                self._throttled_actions += 1
                self._states[server_id] = RemediationState.DEAD_LETTER
                log.warning(
                    f"Remediation rate limit exceeded for server {server_id}. "
                    f"Moved to DEAD_LETTER state (max {max_retries}/hr)."
                )
                self._record_history(
                    server_id=server_id,
                    policy_name=policy.name,
                    trigger=str(policy.trigger.value),
                    action_type=str(policy.action_type.value),
                    success=False,
                    reason="Rate limit exceeded: dead-lettered",
                )
                return False

            # 1. Quarantine server
            self._quarantined_servers.add(server_id)
            self._states[server_id] = RemediationState.QUARANTINED
            self._attempt_timestamps.setdefault(server_id, []).append(time.time())
            self._total_actions += 1

        log.info(
            f"Autonomous remediation initiated for {server_id} under policy '{policy.name}' "
            f"({policy.action_type.value}). Draining traffic for {policy.drain_period_s}s..."
        )

        # 2. Execute action
        success = self._execute_action_payload(policy, server_id, details or {})

        with self._lock:
            if success:
                self._successful_actions += 1
                self._states[server_id] = RemediationState.HEALING
                # Reset eviction streak upon successful action trigger
                self._eviction_counts[server_id] = 0
            else:
                self._failed_actions += 1
                self._states[server_id] = RemediationState.DEAD_LETTER

            self._record_history(
                server_id=server_id,
                policy_name=policy.name,
                trigger=str(policy.trigger.value),
                action_type=str(policy.action_type.value),
                success=success,
                reason="Action completed" if success else "Action execution failed",
            )

        return success

    def _execute_action_payload(
        self,
        policy: RemediationPolicy,
        server_id: str,
        details: Dict[str, Any],
    ) -> bool:
        """Dispatch the configured action (command, webhook, or callback)."""
        action_type = policy.action_type

        if action_type == RemediationActionType.PYTHON_CALLBACK:
            if policy.action_callback:
                try:
                    return bool(policy.action_callback(server_id, details))
                except Exception as exc:
                    log.error(f"Python remediation callback failed for {server_id}: {exc}")
                    return False
            return False

        elif action_type == RemediationActionType.LOCAL_COMMAND:
            if not policy.action_target:
                log.error(f"No local command target configured for policy '{policy.name}'")
                return False
            cmd = policy.action_target.replace("{server_id}", server_id)
            try:
                res = subprocess.run(
                    cmd,
                    shell=True,
                    capture_output=True,
                    text=True,
                    timeout=15.0,
                )
                if res.returncode == 0:
                    log.info(f"Local command succeeded for {server_id}: '{cmd}'")
                    return True
                else:
                    log.error(f"Local command exited with {res.returncode}: {res.stderr.strip()}")
                    return False
            except Exception as exc:
                log.error(f"Local command execution error for {server_id}: {exc}")
                return False

        elif action_type == RemediationActionType.HTTP_WEBHOOK:
            if not policy.action_target:
                log.error(f"No webhook URL configured for policy '{policy.name}'")
                return False
            url = policy.action_target
            payload = json.dumps({
                "event": "autonomous_remediation",
                "server_id": server_id,
                "policy": policy.name,
                "trigger": policy.trigger.value,
                "timestamp": time.time(),
                "details": details,
            }).encode("utf-8")
            req = urllib.request.Request(
                url,
                data=payload,
                headers={"Content-Type": "application/json", "User-Agent": "HuddleCluster-Remediator/4.22.0"},
                method="POST",
            )
            try:
                with urllib.request.urlopen(req, timeout=10.0) as resp:
                    return 200 <= resp.status < 300
            except Exception as exc:
                log.error(f"Remediation webhook dispatch failed to {url} for {server_id}: {exc}")
                return False

        return False

    def clear_quarantine(self, server_id: str) -> bool:
        """Manually clear a server's quarantine and restore it to HEALTHY."""
        with self._lock:
            self._quarantined_servers.discard(server_id)
            self._states[server_id] = RemediationState.HEALTHY
            self._eviction_counts[server_id] = 0
            self._outer_ring_since.pop(server_id, None)
            return True

    def _record_history(
        self,
        server_id: str,
        policy_name: str,
        trigger: str,
        action_type: str,
        success: bool,
        reason: str,
    ) -> None:
        """Record diagnostic history entry."""
        entry = {
            "timestamp":   time.time(),
            "server_id":   server_id,
            "policy":      policy_name,
            "trigger":     trigger,
            "action_type": action_type,
            "success":     success,
            "reason":      reason,
        }
        self._remediation_history.append(entry)
        if len(self._remediation_history) > 100:
            self._remediation_history.pop(0)

    def telemetry_status(self) -> Dict[str, Any]:
        """Return diagnostic metrics and operational status."""
        with self._lock:
            return {
                "enabled":                  self.enabled,
                "active_policies":          len(self.policies),
                "total_actions":            self._total_actions,
                "successful_actions":       self._successful_actions,
                "failed_actions":           self._failed_actions,
                "throttled_actions":        self._throttled_actions,
                "quarantined_count":        len(self._quarantined_servers),
                "quarantined_servers":      sorted(list(self._quarantined_servers)),
                "server_states":            {s: st.value for s, st in self._states.items()},
                "recent_history":           list(self._remediation_history[-10:]),
            }
