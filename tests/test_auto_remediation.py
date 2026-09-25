"""
Unit tests for Autonomous Thermal Auto-Remediation and Self-Healing Engine.
v4.22.0
"""

import unittest
import time
from huddle_cluster import EvictionReason, HuddleCluster, Server, ServerMetrics
from huddle_cluster_pkg.cluster_remediator import (
    ClusterRemediator,
    RemediationPolicy,
    RemediationActionType,
    RemediationTrigger,
    RemediationState,
)


class TestAutoRemediation(unittest.TestCase):
    def setUp(self):
        self.remediation_calls = []

        def _mock_callback(server_id: str, details: dict) -> bool:
            self.remediation_calls.append((server_id, details))
            return True

        self.mock_callback = _mock_callback

    def test_consecutive_overheat_trigger(self):
        policy = RemediationPolicy(
            name="test_overheat_heal",
            trigger=RemediationTrigger.CONSECUTIVE_OVERHEAT,
            action_type=RemediationActionType.PYTHON_CALLBACK,
            action_callback=self.mock_callback,
            consecutive_evictions_threshold=2,
            drain_period_s=0.1,
            max_retries_per_hour=5,
        )

        cluster = HuddleCluster(
            heat_threshold=0.6,
            cool_threshold=0.3,
            min_inner_size=1,
            max_inner_size=3,
            rotation_cooldown_sec=0.0,
            min_outer_dwell_sec=0.0,
            auto_remediation_enabled=True,
            remediation_policies=[policy],
        )

        s1 = Server("s1", "127.0.0.1", 8001, weight=1.0)
        s2 = Server("s2", "127.0.0.1", 8002, weight=1.0)
        cluster.add_server(s1, force_inner=True)
        cluster.add_server(s2, force_inner=True)

        # Eviction 1
        s1.temperature = 0.9
        s1.last_rotated = time.monotonic() - 10.0
        cluster.rotate()
        self.assertEqual(len(self.remediation_calls), 0)

        # Force move s1 back and evict again (Eviction 2)
        cluster._move_to_inner(s1)
        s1.last_rotated = time.monotonic() - 10.0
        s1.temperature = 0.9
        cluster.rotate()

        # Check remediation was triggered
        self.assertEqual(len(self.remediation_calls), 1)
        self.assertEqual(self.remediation_calls[0][0], "s1")

        status = cluster.remediation_status()
        self.assertEqual(status["total_actions"], 1)
        self.assertEqual(status["successful_actions"], 1)
        self.assertIn("s1", status["quarantined_servers"])

    def test_circuit_breaker_remediation(self):
        policy = RemediationPolicy(
            name="cb_heal",
            trigger=RemediationTrigger.CIRCUIT_BREAKER_OPEN,
            action_type=RemediationActionType.PYTHON_CALLBACK,
            action_callback=self.mock_callback,
            max_retries_per_hour=3,
        )

        cluster = HuddleCluster(
            circuit_breaker_threshold=0.5,
            min_inner_size=1,
            max_inner_size=3,
            auto_remediation_enabled=True,
            remediation_policies=[policy],
        )

        s1 = Server("s1", "127.0.0.1", 8001, weight=1.0)
        s2 = Server("s2", "127.0.0.1", 8002, weight=1.0)
        cluster.add_server(s1, force_inner=True)
        cluster.add_server(s2, force_inner=True)

        # Trip circuit breaker on s1
        s1.metrics.error_rate = 0.8

        cluster.rotate()

        # Remediation should be dispatched
        self.assertEqual(len(self.remediation_calls), 1)
        self.assertEqual(self.remediation_calls[0][0], "s1")
        self.assertEqual(self.remediation_calls[0][1]["reason"], "circuit_breaker_open")

    def test_quarantine_gate_blocks_promotion(self):
        policy = RemediationPolicy(
            name="manual_gate",
            trigger=RemediationTrigger.MANUAL,
            action_type=RemediationActionType.PYTHON_CALLBACK,
            action_callback=self.mock_callback,
        )

        cluster = HuddleCluster(
            min_inner_size=1,
            max_inner_size=3,
            auto_remediation_enabled=True,
            remediation_policies=[policy],
        )

        s1 = Server("s1", "127.0.0.1", 8001, weight=1.0)
        s2 = Server("s2", "127.0.0.1", 8002, weight=1.0)
        cluster.add_server(s1, force_inner=True)
        cluster.add_server(s2, force_inner=True)

        # Move s1 to outer and trigger manual remediation
        cluster._move_to_outer(s1, reason=EvictionReason.HEALTH_FAIL)
        cluster.trigger_manual_remediation("s1", reason="admin_investigation")

        # Confirm s1 is quarantined
        status = cluster.remediation_status()
        self.assertIn("s1", status["quarantined_servers"])

        # Cool down s1
        s1.temperature = 0.05
        s1.last_rotated = time.monotonic() - 10.0

        # Rotate should NOT promote s1 because it is quarantined
        cluster.rotate()
        self.assertIn(s1, cluster.outer_servers())

        # Clear quarantine
        cluster.clear_remediation_quarantine("s1")
        self.assertNotIn("s1", cluster.remediation_status()["quarantined_servers"])

        # Now rotate should promote s1
        cluster.rotate()
        self.assertIn(s1, cluster.inner_servers())

    def test_rate_limiting_and_dead_letter(self):
        policy = RemediationPolicy(
            name="throttled_heal",
            trigger=RemediationTrigger.MANUAL,
            action_type=RemediationActionType.PYTHON_CALLBACK,
            action_callback=self.mock_callback,
            max_retries_per_hour=2,
        )

        cluster = HuddleCluster(
            auto_remediation_enabled=True,
            remediation_policies=[policy],
            remediation_max_retries_per_hour=2,
        )

        # First 2 triggers should succeed
        res1 = cluster.trigger_manual_remediation("s1", reason="test1")
        res2 = cluster.trigger_manual_remediation("s1", reason="test2")
        self.assertTrue(res1)
        self.assertTrue(res2)

        # 3rd trigger should be throttled and moved to dead letter
        res3 = cluster.trigger_manual_remediation("s1", reason="test3")
        self.assertFalse(res3)

        status = cluster.remediation_status()
        self.assertEqual(status["throttled_actions"], 1)
        self.assertEqual(status["server_states"]["s1"], RemediationState.DEAD_LETTER.value)

    def test_telemetry_and_metrics_exposition(self):
        cluster = HuddleCluster(
            auto_remediation_enabled=True,
        )
        report = cluster.health_report()
        self.assertIn("remediation", report)
        self.assertTrue(report["remediation"]["enabled"])

        prom = cluster.prometheus_metrics()
        self.assertIn("huddle_remediation_actions_total", prom)
        self.assertIn("huddle_remediation_quarantined_servers", prom)


if __name__ == "__main__":
    unittest.main()
