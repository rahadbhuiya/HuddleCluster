"""
Tests for GrpcCluster (v1.4.0).

gRPC channels are mocked -- no real gRPC server needed.
"""

import threading
import time
import unittest
from unittest.mock import MagicMock, patch, call

from huddle_cluster import create_cluster
from huddle_cluster_pkg.grpc_cluster import GrpcCluster, create_grpc_cluster


def _mock_grpc():
    """Patch grpc module with a minimal mock."""
    grpc_mock = MagicMock()
    channel_mock = MagicMock()
    grpc_mock.insecure_channel.return_value = channel_mock
    grpc_mock.secure_channel.return_value   = channel_mock
    return grpc_mock, channel_mock


class TestGrpcClusterInit(unittest.TestCase):

    def test_raises_without_grpcio(self):
        import sys
        with patch.dict(sys.modules, {"grpc": None}):
            import importlib
            import huddle_cluster_pkg.grpc_cluster as mod
            importlib.reload(mod)
            cluster = create_cluster([("s1", "127.0.0.1", 50051)])
            cluster.start()
            try:
                with self.assertRaises(ImportError):
                    mod.GrpcCluster(cluster)
            finally:
                cluster.stop()
                importlib.reload(mod)

    def test_create_grpc_cluster_factory(self):
        grpc_mock, _ = _mock_grpc()
        with patch.dict("sys.modules", {"grpc": grpc_mock}):
            gc = create_grpc_cluster([("s1", "127.0.0.1", 50051)])
            self.assertIsInstance(gc, GrpcCluster)

    def test_channel_options_stored(self):
        grpc_mock, _ = _mock_grpc()
        with patch.dict("sys.modules", {"grpc": grpc_mock}):
            opts = [("grpc.max_receive_message_length", 1024)]
            gc = create_grpc_cluster([("s1", "127.0.0.1", 50051)], channel_options=opts)
            self.assertEqual(gc._channel_options, opts)


class TestStartStop(unittest.TestCase):

    def test_start_creates_channels_for_all_servers(self):
        grpc_mock, chan = _mock_grpc()
        with patch.dict("sys.modules", {"grpc": grpc_mock}):
            gc = create_grpc_cluster([
                ("s1", "127.0.0.1", 50051),
                ("s2", "127.0.0.1", 50052),
            ])
            gc.start()
            try:
                self.assertEqual(len(gc._channels), 2)
            finally:
                gc.stop()

    def test_stop_closes_channels(self):
        grpc_mock, chan = _mock_grpc()
        with patch.dict("sys.modules", {"grpc": grpc_mock}):
            gc = create_grpc_cluster([("s1", "127.0.0.1", 50051)])
            gc.start()
            gc.stop()
            chan.close.assert_called()
            self.assertEqual(len(gc._channels), 0)


class TestChannelCreation(unittest.TestCase):

    def test_insecure_channel_created_without_credentials(self):
        grpc_mock, chan = _mock_grpc()
        with patch.dict("sys.modules", {"grpc": grpc_mock}):
            gc = create_grpc_cluster([("s1", "127.0.0.1", 50051)])
            gc.start()
            try:
                grpc_mock.insecure_channel.assert_called()
                grpc_mock.secure_channel.assert_not_called()
            finally:
                gc.stop()

    def test_secure_channel_created_with_credentials(self):
        grpc_mock, chan = _mock_grpc()
        creds = MagicMock()
        with patch.dict("sys.modules", {"grpc": grpc_mock}):
            gc = create_grpc_cluster(
                [("s1", "127.0.0.1", 50051)],
                credentials=creds,
            )
            gc.start()
            try:
                grpc_mock.secure_channel.assert_called()
                grpc_mock.insecure_channel.assert_not_called()
            finally:
                gc.stop()

    def test_channel_reused_on_second_call(self):
        grpc_mock, chan = _mock_grpc()
        with patch.dict("sys.modules", {"grpc": grpc_mock}):
            gc = create_grpc_cluster([("s1", "127.0.0.1", 50051)])
            gc.start()
            try:
                call_count_before = grpc_mock.insecure_channel.call_count
                server = gc.inner_servers()[0]
                gc.channel_for(server)
                gc.channel_for(server)
                # No new channels should have been created
                self.assertEqual(
                    grpc_mock.insecure_channel.call_count, call_count_before
                )
            finally:
                gc.stop()

    def test_target_format(self):
        grpc_mock, chan = _mock_grpc()
        with patch.dict("sys.modules", {"grpc": grpc_mock}):
            gc = create_grpc_cluster([("s1", "10.0.0.1", 50051)])
            gc.start()
            try:
                call_args = grpc_mock.insecure_channel.call_args
                target = call_args[0][0]
                self.assertEqual(target, "10.0.0.1:50051")
            finally:
                gc.stop()


class TestGetChannel(unittest.TestCase):

    def setUp(self):
        self.grpc_mock, self.chan = _mock_grpc()

    def test_get_channel_yields_grpc_channel(self):
        with patch.dict("sys.modules", {"grpc": self.grpc_mock}):
            gc = create_grpc_cluster([("s1", "127.0.0.1", 50051)])
            gc.start()
            try:
                with gc.get_channel() as channel:
                    self.assertIs(channel, self.chan)
            finally:
                gc.stop()

    def test_get_channel_records_latency(self):
        with patch.dict("sys.modules", {"grpc": self.grpc_mock}):
            gc = create_grpc_cluster([("s1", "127.0.0.1", 50051)])
            gc.start()
            try:
                server = gc.inner_servers()[0]
                before = server.metrics.avg_response_ms

                with gc.get_channel():
                    time.sleep(0.01)

                self.assertGreaterEqual(
                    server.metrics.avg_response_ms, before
                )
            finally:
                gc.stop()

    def test_get_channel_increments_error_rate_on_exception(self):
        with patch.dict("sys.modules", {"grpc": self.grpc_mock}):
            gc = create_grpc_cluster([("s1", "127.0.0.1", 50051)])
            gc.start()
            try:
                server = gc.inner_servers()[0]
                initial_rate = server.metrics.error_rate

                try:
                    with gc.get_channel():
                        raise ConnectionError("rpc failed")
                except ConnectionError:
                    pass

                self.assertGreater(server.metrics.error_rate, initial_rate)
            finally:
                gc.stop()

    def test_raises_when_no_servers(self):
        with patch.dict("sys.modules", {"grpc": self.grpc_mock}):
            gc = create_grpc_cluster([])
            gc._cluster.start()
            try:
                with self.assertRaises(RuntimeError):
                    with gc.get_channel():
                        pass
            finally:
                gc.stop()


class TestHealthReport(unittest.TestCase):

    def test_health_report_has_grpc_fields(self):
        grpc_mock, chan = _mock_grpc()
        with patch.dict("sys.modules", {"grpc": grpc_mock}):
            gc = create_grpc_cluster([("s1", "127.0.0.1", 50051)])
            gc.start()
            try:
                report = gc.health_report()
                self.assertIn("grpc_channels", report)
                self.assertIn("grpc_channel_count", report)
                self.assertEqual(report["grpc_channel_count"], 1)
            finally:
                gc.stop()

    def test_prometheus_has_grpc_metric(self):
        grpc_mock, chan = _mock_grpc()
        with patch.dict("sys.modules", {"grpc": grpc_mock}):
            gc = create_grpc_cluster([("s1", "127.0.0.1", 50051)])
            gc.start()
            try:
                output = gc.prometheus_metrics()
                self.assertIn("huddle_grpc_channels_total", output)
            finally:
                gc.stop()


class TestProxyAttributes(unittest.TestCase):

    def test_all_servers_delegates(self):
        grpc_mock, _ = _mock_grpc()
        with patch.dict("sys.modules", {"grpc": grpc_mock}):
            gc = create_grpc_cluster([
                ("s1", "127.0.0.1", 50051),
                ("s2", "127.0.0.1", 50052),
            ])
            gc.start()
            try:
                self.assertEqual(len(gc.all_servers()), 2)
            finally:
                gc.stop()

    def test_inner_servers_delegates(self):
        grpc_mock, _ = _mock_grpc()
        with patch.dict("sys.modules", {"grpc": grpc_mock}):
            gc = create_grpc_cluster([("s1", "127.0.0.1", 50051)])
            gc.start()
            try:
                self.assertGreater(len(gc.inner_servers()), 0)
            finally:
                gc.stop()

    def test_getattr_proxies_to_cluster(self):
        grpc_mock, _ = _mock_grpc()
        with patch.dict("sys.modules", {"grpc": grpc_mock}):
            gc = create_grpc_cluster([("s1", "127.0.0.1", 50051)])
            gc.start()
            try:
                # affinity_map_size is on the underlying cluster
                result = gc.affinity_map_size()
                self.assertEqual(result, 0)
            finally:
                gc.stop()


class TestChannelForServer(unittest.TestCase):

    def test_channel_for_returns_channel(self):
        grpc_mock, chan = _mock_grpc()
        with patch.dict("sys.modules", {"grpc": grpc_mock}):
            gc = create_grpc_cluster([("s1", "127.0.0.1", 50051)])
            gc.start()
            try:
                server = gc.inner_servers()[0]
                ch = gc.channel_for(server)
                self.assertIs(ch, chan)
            finally:
                gc.stop()


class TestThreadSafety(unittest.TestCase):

    def test_concurrent_get_channel_no_crash(self):
        grpc_mock, chan = _mock_grpc()
        errors = []

        with patch.dict("sys.modules", {"grpc": grpc_mock}):
            gc = create_grpc_cluster([
                ("s1", "127.0.0.1", 50051),
                ("s2", "127.0.0.1", 50052),
            ])
            gc.start()
            try:
                def worker():
                    try:
                        for _ in range(10):
                            with gc.get_channel():
                                time.sleep(0.001)
                    except Exception as e:
                        errors.append(str(e))

                threads = [threading.Thread(target=worker) for _ in range(8)]
                for t in threads:
                    t.start()
                for t in threads:
                    t.join()
            finally:
                gc.stop()

        self.assertEqual(errors, [], f"Errors: {errors}")


if __name__ == "__main__":
    unittest.main()