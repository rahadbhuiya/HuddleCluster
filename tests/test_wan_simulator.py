"""
Unit tests for WanLatencySimulator and Multi-Region Network Jitter (v4.26.0)
============================================================================
Covers:
  1. LatencyProfile parameter validations.
  2. Built-in industry presets sanity.
  3. WanLatencySimulator topology & link configurations (symmetric/asymmetric).
  4. calculate_delay_ms calculation with jitter distributions & bandwidth.
  5. evaluate_loss & evaluate_corruption statistical bounds.
  6. simulate_transmission telemetry report generation.
  7. transmit() callable execution, delay, and exception handling.
  8. attach_to_ha() ClusterHA monkey-patch and restore.
  9. get_metrics() and reset_metrics() tracking.
"""


import math
import time
import unittest
from unittest.mock import MagicMock

from huddle_cluster_pkg.wan_simulator import (
    LatencyProfile,
    PRESET_CROSS_CONTINENT,
    PRESET_DEGRADED_WAN,
    PRESET_LOCAL_LAN,
    PRESET_TRANSATLANTIC,
    PRESET_TRANSPACIFIC,
    TransmissionReport,
    WanConnectionTimeoutException,
    WanDataCorruptedException,
    WanLatencySimulator,
    WanPacketLossException,
)


class TestLatencyProfileValidation(unittest.TestCase):
    """Test LatencyProfile argument guards."""

    def test_valid_profile(self):
        p = LatencyProfile(mean_latency_ms=100.0, jitter_ms=15.0, packet_loss_rate=0.05)
        self.assertEqual(p.mean_latency_ms, 100.0)
        self.assertEqual(p.jitter_ms, 15.0)
        self.assertEqual(p.packet_loss_rate, 0.05)

    def test_negative_mean_latency_raises(self):
        with self.assertRaises(ValueError):
            LatencyProfile(mean_latency_ms=-1.0)

    def test_negative_jitter_raises(self):
        with self.assertRaises(ValueError):
            LatencyProfile(jitter_ms=-5.0)

    def test_invalid_loss_rate_raises(self):
        with self.assertRaises(ValueError):
            LatencyProfile(packet_loss_rate=1.5)
        with self.assertRaises(ValueError):
            LatencyProfile(packet_loss_rate=-0.1)

    def test_invalid_distribution_raises(self):
        with self.assertRaises(ValueError):
            LatencyProfile(distribution="poisson")


class TestBuiltinPresets(unittest.TestCase):
    """Verify built-in presets conform to expected network tiers."""

    def test_local_lan(self):
        self.assertLess(PRESET_LOCAL_LAN.mean_latency_ms, 5.0)
        self.assertEqual(PRESET_LOCAL_LAN.packet_loss_rate, 0.0)

    def test_transatlantic(self):
        self.assertGreater(PRESET_TRANSATLANTIC.mean_latency_ms, 50.0)
        self.assertLess(PRESET_TRANSATLANTIC.mean_latency_ms, 120.0)

    def test_transpacific(self):
        self.assertGreater(PRESET_TRANSPACIFIC.mean_latency_ms, 120.0)
        self.assertLess(PRESET_TRANSPACIFIC.mean_latency_ms, 200.0)

    def test_cross_continent(self):
        self.assertGreater(PRESET_CROSS_CONTINENT.mean_latency_ms, 200.0)


class TestSimulatorTopology(unittest.TestCase):
    """Test link matrix configuration."""

    def setUp(self):
        self.sim = WanLatencySimulator(random_seed=42)

    def test_same_region_uses_same_region_profile(self):
        prof = self.sim.get_link("us-east", "us-east")
        self.assertEqual(prof.mean_latency_ms, PRESET_LOCAL_LAN.mean_latency_ms)

    def test_unconfigured_link_uses_default(self):
        prof = self.sim.get_link("us-east", "sa-east")
        self.assertEqual(prof.mean_latency_ms, 45.0)

    def test_symmetric_link_configuration(self):
        custom = LatencyProfile(mean_latency_ms=180.0, jitter_ms=20.0)
        self.sim.set_link("us-east", "ap-south", custom, symmetric=True)

        prof_fwd = self.sim.get_link("us-east", "ap-south")
        prof_rev = self.sim.get_link("ap-south", "us-east")
        self.assertEqual(prof_fwd.mean_latency_ms, 180.0)
        self.assertEqual(prof_rev.mean_latency_ms, 180.0)

    def test_asymmetric_link_configuration(self):
        prof_a = LatencyProfile(mean_latency_ms=70.0)
        prof_b = LatencyProfile(mean_latency_ms=120.0)

        self.sim.set_link("us-west", "eu-central", prof_a, symmetric=False)
        self.sim.set_link("eu-central", "us-west", prof_b, symmetric=False)

        self.assertEqual(self.sim.get_link("us-west", "eu-central").mean_latency_ms, 70.0)
        self.assertEqual(self.sim.get_link("eu-central", "us-west").mean_latency_ms, 120.0)



class TestDelayAndJitterCalculations(unittest.TestCase):
    """Test statistical delay calculation."""

    def setUp(self):
        self.sim = WanLatencySimulator(random_seed=123)

    def test_uniform_jitter_bounds(self):
        prof = LatencyProfile(mean_latency_ms=100.0, jitter_ms=10.0, distribution="uniform")
        self.sim.set_link("r1", "r2", prof)

        for _ in range(50):
            d = self.sim.calculate_delay_ms("r1", "r2")
            self.assertGreaterEqual(d, 90.0)
            self.assertLessEqual(d, 110.0)


    def test_bandwidth_serialization_delay(self):
        # 10 Mbps = 1.25 MB/s
        prof = LatencyProfile(mean_latency_ms=50.0, jitter_ms=0.0, bandwidth_mbps=10.0, distribution="uniform")
        self.sim.set_link("r1", "r2", prof)

        # 1.25 MB (1,250,000 bytes) should add ~1000ms
        delay_small = self.sim.calculate_delay_ms("r1", "r2", payload_bytes=0)
        delay_large = self.sim.calculate_delay_ms("r1", "r2", payload_bytes=1_250_000)

        self.assertAlmostEqual(delay_small, 50.0, delta=1.0)
        self.assertAlmostEqual(delay_large, 1050.0, delta=5.0)

    def test_minimum_delay_floor(self):
        prof = LatencyProfile(mean_latency_ms=0.0, jitter_ms=0.0)
        self.sim.set_link("r1", "r2", prof)
        d = self.sim.calculate_delay_ms("r1", "r2")
        self.assertGreaterEqual(d, 0.1)



class TestPacketLossAndCorruption(unittest.TestCase):
    """Test stochastic loss and corruption rates."""

    def test_zero_loss_never_drops(self):
        sim = WanLatencySimulator(random_seed=99)
        prof = LatencyProfile(mean_latency_ms=10.0, packet_loss_rate=0.0)
        sim.set_link("r1", "r2", prof)

        for _ in range(100):
            self.assertFalse(sim.evaluate_loss("r1", "r2"))

    def test_full_loss_always_drops(self):
        sim = WanLatencySimulator(random_seed=99)
        prof = LatencyProfile(mean_latency_ms=10.0, packet_loss_rate=1.0)
        sim.set_link("r1", "r2", prof)

        for _ in range(50):
            self.assertTrue(sim.evaluate_loss("r1", "r2"))

    def test_stochastic_loss_rate_approximation(self):
        sim = WanLatencySimulator(random_seed=42)
        prof = LatencyProfile(mean_latency_ms=10.0, packet_loss_rate=0.30)
        sim.set_link("r1", "r2", prof)

        drops = sum(1 for _ in range(1000) if sim.evaluate_loss("r1", "r2"))
        rate = drops / 1000.0
        # Expect ~30% with tolerance
        self.assertGreater(rate, 0.25)
        self.assertLess(rate, 0.35)


    def test_corruption_evaluation(self):
        sim = WanLatencySimulator(random_seed=42)
        prof = LatencyProfile(mean_latency_ms=10.0, corruption_rate=1.0)
        sim.set_link("r1", "r2", prof)
        self.assertTrue(sim.evaluate_corruption("r1", "r2"))



class TestTransmissionExecution(unittest.TestCase):
    """Test transmit() behavior and simulated delays."""

    def setUp(self):
        self.sim = WanLatencySimulator(random_seed=1)


    def test_successful_transmission(self):
        prof = LatencyProfile(mean_latency_ms=5.0, jitter_ms=1.0, packet_loss_rate=0.0)
        self.sim.set_link("a", "b", prof)

        result = self.sim.transmit(
            "a", "b",
            fn=lambda: "payload_ok",
            apply_delay=True,
            delay_multiplier=0.001,  # sub-millisecond sleep
        )
        self.assertEqual(result, "payload_ok")


    def test_packet_loss_raises_exception(self):
        prof = LatencyProfile(mean_latency_ms=5.0, packet_loss_rate=1.0)
        self.sim.set_link("a", "b", prof)

        with self.assertRaises(WanPacketLossException):
            self.sim.transmit(
                "a", "b",
                fn=lambda: "never_reached",
                apply_delay=False,
            )


    def test_corruption_raises_exception(self):
        prof = LatencyProfile(mean_latency_ms=5.0, packet_loss_rate=0.0, corruption_rate=1.0)
        self.sim.set_link("a", "b", prof)

        with self.assertRaises(WanDataCorruptedException):
            self.sim.transmit(
                "a", "b",
                fn=lambda: "corrupted_payload",
                apply_delay=False,
            )




class TestRaftHaAttachment(unittest.TestCase):
    """Test monkey patching ClusterHA with attach_to_ha()."""

    def setUp(self):
        self.sim = WanLatencySimulator(random_seed=7)

    def test_attach_and_restore(self):
        class DummyHA:
            def __init__(self):
                self._id = "master-1"

            def _rpc_post(self, url, payload):
                return {"received_from": url, "payload": payload}

        ha = DummyHA()
        original_method = ha._rpc_post

        region_map = {"master-1": "us-east", "master-2": "eu-central"}
        self.sim.set_link("us-east", "eu-central", PRESET_TRANSATLANTIC)

        restore = self.sim.attach_to_ha(ha, region_map, delay_multiplier=0.001)

        # Test call goes through hooked method
        res = ha._rpc_post("http://master-2:7071/v1/ha/vote", {"term": 1})
        self.assertIn("received_from", res)

        # Metrics should have recorded the call
        metrics = self.sim.get_metrics()
        self.assertEqual(metrics["total_transmissions"], 1)

        # Restore original method
        restore()
        self.assertEqual(ha._rpc_post, original_method)



class TestTelemetryAndMetrics(unittest.TestCase):
    """Test transmission counter accumulation and reset."""


    def setUp(self):
        self.sim = WanLatencySimulator(random_seed=10)

    def test_metrics_accumulation(self):
        prof = LatencyProfile(mean_latency_ms=50.0, packet_loss_rate=0.5)
        self.sim.set_link("r1", "r2", prof)

        for _ in range(20):
            self.sim.simulate_transmission("r1", "r2", payload_bytes=100)

        metrics = self.sim.get_metrics()
        self.assertEqual(metrics["total_transmissions"], 20)
        self.assertEqual(metrics["total_bytes"], 2000)
        self.assertGreater(metrics["total_dropped"], 0)
        self.assertIn("r1->r2", metrics["links"])


    def test_reset_metrics(self):
        self.sim.simulate_transmission("r1", "r2", payload_bytes=500)
        self.assertEqual(self.sim.get_metrics()["total_transmissions"], 1)

        self.sim.reset_metrics()
        metrics = self.sim.get_metrics()
        self.assertEqual(metrics["total_transmissions"], 0)
        self.assertEqual(metrics["total_bytes"], 0)
        self.assertEqual(len(metrics["links"]), 0)




if __name__ == "__main__":
    unittest.main()
