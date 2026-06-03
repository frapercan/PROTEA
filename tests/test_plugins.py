"""Tests for the generic plugin-discovery helper (T2A.5 + T2A.8).

The helper is consumed by:
- ``protea.core.operations.compute_embeddings._get_backend_plugins`` (backends)
- ``protea.core.runners.get_runner_plugins`` (runners)

These tests pin the cache + name-mismatch invariants without standing up
a real entry-point group; they patch ``importlib.metadata.entry_points``
directly so the suite stays deterministic regardless of which plugin
packages happen to be installed in the test environment.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from protea.core.plugins import discover_plugins, reset_plugin_cache


@pytest.fixture(autouse=True)
def _isolate_cache():
    """Reset the per-group cache around each test so ordering doesn't matter."""
    reset_plugin_cache()
    yield
    reset_plugin_cache()


def _ep(name: str, plugin_obj):
    """Build a fake entry-point that returns ``plugin_obj`` from ``.load()``."""
    ep = MagicMock()
    ep.name = name
    ep.value = f"fake_module.{name}:plugin"
    ep.load.return_value = plugin_obj
    return ep


class TestDiscoverPlugins:
    def test_returns_dict_keyed_by_name(self) -> None:
        plugin = SimpleNamespace(name="esm")
        with patch(
            "importlib.metadata.entry_points",
            return_value=[_ep("esm", plugin)],
        ):
            out = discover_plugins("protea.backends")
        assert out == {"esm": plugin}

    def test_caches_within_group(self) -> None:
        plugin = SimpleNamespace(name="esm")
        with patch(
            "importlib.metadata.entry_points",
            return_value=[_ep("esm", plugin)],
        ) as mock_eps:
            discover_plugins("protea.backends")
            discover_plugins("protea.backends")
        # Second call hits the cache; entry_points is called only once.
        assert mock_eps.call_count == 1

    def test_name_mismatch_is_hard_error(self) -> None:
        # entry_point name "esm" but the plugin's ``name`` attribute says "mismatch".
        plugin = SimpleNamespace(name="mismatch")
        with patch(
            "importlib.metadata.entry_points",
            return_value=[_ep("esm", plugin)],
        ):
            with pytest.raises(RuntimeError, match="Plugin name mismatch"):
                discover_plugins("protea.backends")

    def test_separate_groups_have_separate_caches(self) -> None:
        backend = SimpleNamespace(name="esm")
        runner = SimpleNamespace(name="knn")

        def fake_eps(group: str) -> list:
            return {
                "protea.backends": [_ep("esm", backend)],
                "protea.runners": [_ep("knn", runner)],
            }[group]

        with patch("importlib.metadata.entry_points", side_effect=fake_eps):
            backends = discover_plugins("protea.backends")
            runners = discover_plugins("protea.runners")

        assert backends == {"esm": backend}
        assert runners == {"knn": runner}

    def test_empty_group_returns_empty_dict(self) -> None:
        with patch("importlib.metadata.entry_points", return_value=[]):
            out = discover_plugins("protea.backends")
        assert out == {}


class TestResetPluginCache:
    def test_reset_specific_group_forces_rediscovery(self) -> None:
        plugin = SimpleNamespace(name="esm")
        with patch(
            "importlib.metadata.entry_points",
            return_value=[_ep("esm", plugin)],
        ) as mock_eps:
            discover_plugins("protea.backends")
            reset_plugin_cache("protea.backends")
            discover_plugins("protea.backends")
        assert mock_eps.call_count == 2

    def test_reset_all_clears_every_group(self) -> None:
        plugin_a = SimpleNamespace(name="esm")
        plugin_b = SimpleNamespace(name="knn")

        def fake_eps(group: str) -> list:
            return {
                "protea.backends": [_ep("esm", plugin_a)],
                "protea.runners": [_ep("knn", plugin_b)],
            }[group]

        with patch("importlib.metadata.entry_points", side_effect=fake_eps) as mock_eps:
            discover_plugins("protea.backends")
            discover_plugins("protea.runners")
            reset_plugin_cache()  # global reset
            discover_plugins("protea.backends")
            discover_plugins("protea.runners")
        # Each group queried twice (initial + post-reset).
        assert mock_eps.call_count == 4
