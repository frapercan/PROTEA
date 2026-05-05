"""Regression tests for the F2A.5 plugin-based backend dispatch in
``compute_embeddings._load_model``.

The legacy hardcoded ``if/elif config.model_backend == ...`` chain was
replaced with discovery via the ``protea.backends`` entry_points group.
These tests pin the contract:

* All four bootstrap backends (esm, t5, ankh, esm3c) are discoverable
  from the PROTEA venv when ``protea-backends`` is installed.
* The legacy ``"auto"`` alias still maps to the ``esm`` plugin.
* Unknown backends raise ``ValueError`` (not silent fall-through).
* ``_load_model`` delegates to ``plugin.load_model`` and emits the
  expected start/done events.

Heavy ML deps (torch / transformers / esm) are NOT required: the tests
mock the plugin's ``load_model`` so the lazy-import path inside the
plugin never fires.
"""

from __future__ import annotations

import importlib
from unittest.mock import MagicMock

import pytest

from protea.core.operations import compute_embeddings as ce_module


def _reset_plugin_cache() -> None:
    """Force the next call to repopulate ``_BACKEND_PLUGINS`` from
    entry_points so individual tests don't bleed cached state."""
    ce_module._BACKEND_PLUGINS = None


def test_bootstrap_backends_discoverable_via_entry_points() -> None:
    _reset_plugin_cache()
    plugins = ce_module._get_backend_plugins()
    assert set(plugins) >= {"esm", "t5", "ankh", "esm3c"}


def test_plugin_name_attribute_matches_entry_point_name() -> None:
    _reset_plugin_cache()
    plugins = ce_module._get_backend_plugins()
    for ep_name, plugin in plugins.items():
        assert plugin.name == ep_name


def test_resolve_auto_maps_to_esm_plugin() -> None:
    _reset_plugin_cache()
    plugins = ce_module._get_backend_plugins()
    assert ce_module._resolve_backend("auto") is plugins["esm"]


def test_resolve_unknown_backend_raises_value_error() -> None:
    _reset_plugin_cache()
    with pytest.raises(ValueError, match="Unknown model_backend"):
        ce_module._resolve_backend("xgboost-on-proteins")


def test_load_model_delegates_to_resolved_plugin() -> None:
    _reset_plugin_cache()
    fake_plugin = MagicMock()
    fake_plugin.name = "esm"
    fake_plugin.load_model.return_value = ("fake_model", "fake_tokenizer")

    ce_module._BACKEND_PLUGINS = {"esm": fake_plugin}

    config = MagicMock()
    config.model_backend = "esm"
    config.model_name = "facebook/esm2_t6_8M_UR50D"
    emit_calls: list[tuple[str, object, dict[str, object] | None, str]] = []

    def emit(event: str, payload: object, fields: dict[str, object] | None, level: str) -> None:
        emit_calls.append((event, payload, fields, level))

    model, tokenizer = ce_module._load_model(config, "cpu", emit)
    assert model == "fake_model"
    assert tokenizer == "fake_tokenizer"
    fake_plugin.load_model.assert_called_once_with(
        "facebook/esm2_t6_8M_UR50D", "cpu", emit
    )
    event_names = {call[0] for call in emit_calls}
    assert "compute_embeddings.model_load_start" in event_names
    assert "compute_embeddings.model_load_done" in event_names


def test_plugin_cache_persists_across_calls() -> None:
    _reset_plugin_cache()
    first = ce_module._get_backend_plugins()
    second = ce_module._get_backend_plugins()
    assert first is second  # cached identity, not just equality


def test_module_re_import_redoes_discovery() -> None:
    importlib.reload(ce_module)
    assert ce_module._BACKEND_PLUGINS is None
    plugins = ce_module._get_backend_plugins()
    assert "esm" in plugins
