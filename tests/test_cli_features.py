"""
Tests for huddle_cluster_pkg.cli's --features flag: loading/validating
a JSON feature config and instantiating the corresponding classes.
"""

import json
import subprocess
import sys
import time

import pytest

from huddle_cluster_pkg.cli import _load_features_config, _build_features, _FEATURE_CLASSES
from huddle_cluster_pkg.cluster_autoscaler import ClusterAutoScaler
from huddle_cluster_pkg.cluster_circuit_breaker import ClusterCircuitBreaker
from huddle_cluster_pkg.cluster_ha import ClusterHA


def _free_port():
    import socket
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close()
    return p



# Unit tests — _load_features_config


class TestLoadFeaturesConfig:
    def test_inline_json_string(self):
        config = _load_features_config('{"autoscaler": {"min_nodes": 2}}')
        assert config == {"autoscaler": {"min_nodes": 2}}

    def test_json_file_path(self, tmp_path):
        path = tmp_path / "features.json"
        path.write_text('{"rate_limiter": {"capacity": 10}}')
        config = _load_features_config(str(path))
        assert config == {"rate_limiter": {"capacity": 10}}

    def test_invalid_json_raises_systemexit(self):
        with pytest.raises(SystemExit, match="could not parse as JSON"):
            _load_features_config("{not valid json")

    def test_non_object_top_level_rejected(self):
        with pytest.raises(SystemExit, match="must be an object"):
            _load_features_config("[1, 2, 3]")

    def test_unknown_feature_key_rejected(self):
        with pytest.raises(SystemExit, match="unknown key"):
            _load_features_config('{"not_a_real_feature": {}}')

    def test_all_valid_keys_accepted(self):
        config = {name: {} for name in _FEATURE_CLASSES}
        # ha requires node_id but _load_features_config only validates
        # key names, not per-feature required args — that's _build_features's job.
        result = _load_features_config(json.dumps(config))
        assert set(result) == set(_FEATURE_CLASSES)


# Unit tests — _build_features


class TestBuildFeatures:
    def test_builds_requested_instances(self):
        built = _build_features({
            "circuit_breaker": {"trip_threshold": 0.6},
            "autoscaler": {"min_nodes": 2, "max_nodes": 8},
        })
        assert isinstance(built["circuit_breaker"], ClusterCircuitBreaker)
        assert isinstance(built["autoscaler"], ClusterAutoScaler)
        assert built["circuit_breaker"].trip_threshold == 0.6
        assert built["autoscaler"].min_nodes == 2

    def test_empty_config_builds_nothing(self):
        assert _build_features({}) == {}

    def test_non_dict_value_rejected(self):
        with pytest.raises(SystemExit, match="must be a JSON object"):
            _build_features({"autoscaler": "not a dict"})

    def test_bad_kwarg_rejected_with_clear_message(self):
        with pytest.raises(SystemExit, match="bad arguments for 'autoscaler'"):
            _build_features({"autoscaler": {"not_a_real_param": 5}})

    def test_invalid_value_rejected_with_clear_message(self):
        # buffer_size <= 0 raises ValueError inside ClusterObservability
        with pytest.raises(SystemExit, match="invalid config for 'observability'"):
            _build_features({"observability": {"buffer_size": 0}})

    def test_ha_requires_node_id(self):
        with pytest.raises(SystemExit, match="bad arguments for 'ha'"):
            _build_features({"ha": {"peers": []}})   # missing required node_id

    def test_ha_builds_with_node_id(self):
        built = _build_features({"ha": {"node_id": "m1", "peers": []}})
        assert isinstance(built["ha"], ClusterHA)


# Integration — actually starting `huddle-cluster master start --features`


class TestMasterStartWithFeatures:
    def test_multiple_features_enabled_via_cli(self, tmp_path):
        port = _free_port()
        config = tmp_path / "features.json"
        config.write_text(json.dumps({
            "circuit_breaker": {"trip_threshold": 0.5},
            "rate_limiter": {"capacity": 20, "refill_rate": 5.0},
            "autoscaler": {"min_nodes": 2, "max_nodes": 10},
        }))
        proc = subprocess.Popen(
            [sys.executable, "-m", "huddle_cluster_pkg.cli", "master", "start",
             "--port", str(port), "--features", str(config)],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        try:
            import urllib.request
            deadline = time.time() + 8
            status = None
            while time.time() < deadline:
                try:
                    with urllib.request.urlopen(
                        f"http://127.0.0.1:{port}/v1/status", timeout=1) as r:
                        status = json.loads(r.read())
                    break
                except Exception:
                    time.sleep(0.2)
            assert status is not None, "master never became reachable"
            assert status["circuit_breaker"] == "enabled"
            assert status["rate_limiter"] == "enabled"
            assert status["autoscaler"] == "enabled"
            assert status["scheduler"] == "enabled"   # auto-wired
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)

    def test_bad_features_json_exits_nonzero_with_clear_stderr(self):
        port = _free_port()
        proc = subprocess.run(
            [sys.executable, "-m", "huddle_cluster_pkg.cli", "master", "start",
             "--port", str(port), "--features", '{"nonexistent_feature": {}}'],
            capture_output=True, text=True, timeout=10,
        )
        assert proc.returncode != 0
        assert "unknown key" in (proc.stdout + proc.stderr)

    def test_no_features_flag_behaves_as_before(self, tmp_path):
        """Regression guard: omitting --features entirely must still work
        exactly as it did before this feature existed."""
        port = _free_port()
        proc = subprocess.Popen(
            [sys.executable, "-m", "huddle_cluster_pkg.cli", "master", "start",
             "--port", str(port)],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        try:
            import urllib.request
            deadline = time.time() + 8
            status = None
            while time.time() < deadline:
                try:
                    with urllib.request.urlopen(
                        f"http://127.0.0.1:{port}/v1/status", timeout=1) as r:
                        status = json.loads(r.read())
                    break
                except Exception:
                    time.sleep(0.2)
            assert status is not None
            assert status["circuit_breaker"] == "disabled"
            assert status["scheduler"] == "disabled"
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)