"""Tests for the runner adapter (T2A.8).

Pins the resolver contract: ``resolve_runner("knn")`` returns the plugin
matching that entry-point name, and unknown identifiers raise a
``ValueError`` listing the discovered set so the failure message is
actionable.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from protea.core.plugins import reset_plugin_cache
from protea.core.runners import (
    PROTEA_RUNNERS_GROUP,
    get_runner_plugins,
    resolve_runner,
)


@pytest.fixture(autouse=True)
def _isolate_cache():
    reset_plugin_cache()
    yield
    reset_plugin_cache()


def _ep(name: str, plugin_obj):
    ep = MagicMock()
    ep.name = name
    ep.value = f"protea_runners.{name}:plugin"
    ep.load.return_value = plugin_obj
    return ep


def _patched_runners(plugins: dict[str, object]):
    eps = [_ep(name, plugin) for name, plugin in plugins.items()]

    def fake_eps(group: str):
        return eps if group == PROTEA_RUNNERS_GROUP else []

    return patch("importlib.metadata.entry_points", side_effect=fake_eps)


class TestGetRunnerPlugins:
    def test_returns_discovered_plugins(self) -> None:
        knn = SimpleNamespace(name="knn")
        baseline = SimpleNamespace(name="baseline")
        with _patched_runners({"knn": knn, "baseline": baseline}):
            out = get_runner_plugins()
        assert out == {"knn": knn, "baseline": baseline}


class TestResolveRunner:
    def test_returns_plugin_for_known_name(self) -> None:
        knn = SimpleNamespace(name="knn")
        with _patched_runners({"knn": knn}):
            assert resolve_runner("knn") is knn

    def test_unknown_name_raises_value_error_listing_discovered(self) -> None:
        knn = SimpleNamespace(name="knn")
        baseline = SimpleNamespace(name="baseline")
        with _patched_runners({"knn": knn, "baseline": baseline}):
            with pytest.raises(ValueError) as exc_info:
                resolve_runner("rgcn")
        msg = str(exc_info.value)
        assert "rgcn" in msg
        # Discovered list is sorted in the error so callers see a
        # stable suggestion order.
        assert "baseline" in msg
        assert "knn" in msg
        assert msg.find("baseline") < msg.find("knn")

    def test_empty_registry_raises(self) -> None:
        with _patched_runners({}):
            with pytest.raises(ValueError, match="Discovered: \\[\\]"):
                resolve_runner("knn")
