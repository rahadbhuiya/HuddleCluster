"""
Unit tests for HuddleCluster AI/LLM Token-Aware Routing & Streaming (v4.20.0)
=============================================================================
Verifies:
1. Token accounting: Prompt and completion tokens accurately recorded on server metrics.
2. Token thermal penalty: Long-context generations raise server temperature proportionally.
3. Streaming TTFT and ITL telemetry: Rolling TTFT and ITL windows calculate averages and P95.
4. Hedged LLM Gateway: Speculative execution triggers when primary TTFT stalls.
5. Telemetry and Prometheus metrics: llm_status() and Prometheus gauge/counter metrics.
"""

import time
import unittest

from huddle_cluster import HuddleCluster, Position, Server


class TestLLMRouting(unittest.TestCase):
    def setUp(self):
        self.cluster = HuddleCluster(
            heat_threshold=0.6,
            cool_threshold=0.3,
            min_inner_size=2,
            max_inner_size=3,
            hedging_enabled=True,
            llm_routing_enabled=True,
            llm_token_cost_multiplier=0.001,
            llm_ttft_hedging_delay_ms=25.0,
        )
        self.gpu1 = Server("gpu-1", "127.0.0.1", 9001)
        self.gpu2 = Server("gpu-2", "127.0.0.1", 9002)
        self.gpu3 = Server("gpu-3", "127.0.0.1", 9003)
        self.gpu1.position = Position.INNER
        self.gpu2.position = Position.INNER
        self.gpu3.position = Position.OUTER

        self.gpu1.temperature = 0.2
        self.gpu2.temperature = 0.1
        self.gpu3.temperature = 0.05

        self.cluster._inner_ring.append(self.gpu1)
        self.cluster._inner_ring.append(self.gpu2)
        self.cluster._outer_ring.append(self.gpu3)

    def tearDown(self):
        self.cluster.stop()

    def test_token_accounting(self):
        """Prompt and completion tokens are recorded in ServerMetrics and cluster totals."""
        self.cluster.record_tokens(self.gpu1, prompt_tokens=500, completion_tokens=150)
        self.assertEqual(self.gpu1.metrics.prompt_tokens, 500)
        self.assertEqual(self.gpu1.metrics.completion_tokens, 150)
        self.assertEqual(self.gpu1.metrics.total_tokens_processed, 650)

        status = self.cluster.llm_status()
        self.assertEqual(status["total_tokens_routed"], 650)
        self.assertEqual(status["total_llm_requests"], 1)

    def test_token_thermal_penalty(self):
        """Large token batches increase server temperature proportionally."""
        initial_temp = self.gpu1.temperature
        self.cluster.record_tokens(self.gpu1, prompt_tokens=200, completion_tokens=100)
        # Expected increase: 300 * 0.001 = 0.3
        self.assertAlmostEqual(self.gpu1.temperature, initial_temp + 0.3, places=3)

    def test_streaming_ttft_and_itl_tracking(self):
        """Streaming telemetry tracks TTFT and ITL rolling averages and P95."""
        self.cluster.record_streaming(self.gpu1, ttft_ms=20.0, itl_ms=8.0)
        self.cluster.record_streaming(self.gpu1, ttft_ms=30.0, itl_ms=10.0)

        self.assertAlmostEqual(self.gpu1.metrics.avg_ttft_ms, 25.0, places=1)
        self.assertAlmostEqual(self.gpu1.metrics.avg_itl_ms, 9.0, places=1)
        self.assertGreater(self.gpu1.metrics.ttft_p95(), 0.0)

    def test_hedged_llm_request_fast_primary(self):
        """Hedged LLM request returns primary result and records token stats when fast."""
        def fast_llm_call(server: Server):
            time.sleep(0.002)
            return f"response_from_{server.id}"

        res = self.cluster.hedged_llm_request(
            fast_llm_call,
            prompt_tokens=100,
            estimated_completion_tokens=50,
            ttft_hedging_delay_ms=50.0,
        )
        self.assertEqual(res, "response_from_gpu-1")
        self.assertEqual(self.gpu1.metrics.total_tokens_processed, 150)

    def test_hedged_llm_request_stalled_primary_speculative_win(self):
        """When primary GPU node stalls past TTFT threshold, coolest backup GPU responds."""
        def stalled_primary_llm_call(server: Server):
            if server.id == "gpu-1":
                time.sleep(0.12)  # stalled TTFT
                return f"slow_{server.id}"
            else:
                time.sleep(0.005)  # fast coolest backup
                return f"fast_{server.id}"

        res = self.cluster.hedged_llm_request(
            stalled_primary_llm_call,
            prompt_tokens=80,
            estimated_completion_tokens=40,
            ttft_hedging_delay_ms=20.0,
            timeout_ms=500.0,
        )
        self.assertEqual(res, "fast_gpu-2")
        # Backup server token consumption recorded
        self.assertEqual(self.gpu2.metrics.total_tokens_processed, 120)

    def test_llm_status_and_prometheus_metrics(self):
        """Health report and Prometheus exposition include LLM metrics."""
        self.cluster.record_tokens(self.gpu1, prompt_tokens=100, completion_tokens=50)
        self.cluster.record_streaming(self.gpu1, ttft_ms=15.0, itl_ms=6.0)

        report = self.cluster.health_report()
        self.assertIn("llm", report)
        self.assertTrue(report["llm"]["enabled"])
        self.assertEqual(report["llm"]["total_tokens_routed"], 150)

        metrics = self.cluster.prometheus_metrics()
        self.assertIn("huddle_llm_total_tokens_routed", metrics)
        self.assertIn("huddle_llm_total_requests", metrics)
        self.assertIn("huddle_server_ttft_avg_ms", metrics)
        self.assertIn("huddle_server_ttft_p95_ms", metrics)
        self.assertIn("huddle_server_itl_avg_ms", metrics)


if __name__ == "__main__":
    unittest.main()
