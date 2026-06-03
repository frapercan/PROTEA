"""Regression tests for the F2A.5 plugin-based backend dispatch in
``compute_embeddings._load_model``.

The legacy hardcoded ``if/elif config.model_backend == ...`` chain was
replaced with discovery via the ``protea.backends`` entry_points group.
T2A.8 then refactored the discovery to share a generic helper in
``protea.core.plugins`` with the runners adapter; these tests still pin
the backend-specific contract:

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

from unittest.mock import MagicMock, patch

import pytest

from protea.core.operations import compute_embeddings as ce_module
from protea.core.plugins import reset_plugin_cache


def _reset_plugin_cache() -> None:
    """Force the next call to repopulate the backend-plugins cache from
    entry_points so individual tests don't bleed cached state."""
    reset_plugin_cache("protea.backends")


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

    config = MagicMock()
    config.model_backend = "esm"
    config.model_name = "facebook/esm2_t6_8M_UR50D"
    emit_calls: list[tuple[str, object, dict[str, object] | None, str]] = []

    def emit(event: str, payload: object, fields: dict[str, object] | None, level: str) -> None:
        emit_calls.append((event, payload, fields, level))

    with patch.object(ce_module, "_get_backend_plugins", return_value={"esm": fake_plugin}):
        model, tokenizer = ce_module._load_model(config, "cpu", emit)

    assert model == "fake_model"
    assert tokenizer == "fake_tokenizer"
    fake_plugin.load_model.assert_called_once_with("facebook/esm2_t6_8M_UR50D", "cpu", emit)
    event_names = {call[0] for call in emit_calls}
    assert "compute_embeddings.model_load_start" in event_names
    assert "compute_embeddings.model_load_done" in event_names


def test_plugin_cache_persists_across_calls() -> None:
    _reset_plugin_cache()
    first = ce_module._get_backend_plugins()
    second = ce_module._get_backend_plugins()
    assert first is second  # cached identity, not just equality


def test_reset_cache_forces_rediscovery() -> None:
    """The shared reset hook drops the cached map and the next call
    re-runs ``entry_points`` (T2A.8 replacement for the previous
    module-reload check)."""
    _reset_plugin_cache()
    first = ce_module._get_backend_plugins()
    _reset_plugin_cache()
    second = ce_module._get_backend_plugins()
    # Identity is no longer guaranteed across cache resets, but the
    # bootstrap plugin set must still be present.
    assert "esm" in first
    assert "esm" in second


# ---------------------------------------------------------------------------
# T2A.1 — esm dispatch goes through plugin.embed_chunks
# ---------------------------------------------------------------------------


def test_dispatch_embed_routes_esm_to_plugin_embed_chunks() -> None:
    """T2A.1: the esm branch in ``_dispatch_embed`` calls
    ``plugin.embed_chunks`` instead of the local ``_embed_esm`` shim."""
    _reset_plugin_cache()
    fake_plugin = MagicMock()
    fake_plugin.name = "esm"
    fake_plugin.embed_chunks.return_value = [["chunk"]]

    config = MagicMock()
    config.model_backend = "esm"

    with (
        patch.object(ce_module, "_get_backend_plugins", return_value={"esm": fake_plugin}),
        patch.object(ce_module, "_embed_esm") as legacy_shim,
    ):
        out = ce_module._dispatch_embed("model", "tok", ["MSEQ"], config, "cpu")

    assert out == [["chunk"]]
    fake_plugin.embed_chunks.assert_called_once_with("model", "tok", ["MSEQ"], config, "cpu")
    legacy_shim.assert_not_called()


def test_dispatch_embed_auto_alias_routes_to_esm_plugin() -> None:
    """``auto`` keeps mapping to the esm plugin's ``embed_chunks``."""
    _reset_plugin_cache()
    fake_plugin = MagicMock()
    fake_plugin.name = "esm"
    fake_plugin.embed_chunks.return_value = [[]]

    config = MagicMock()
    config.model_backend = "auto"

    with patch.object(ce_module, "_get_backend_plugins", return_value={"esm": fake_plugin}):
        ce_module._dispatch_embed("model", "tok", ["MSEQ"], config, "cpu")

    fake_plugin.embed_chunks.assert_called_once()


def test_plugin_embed_chunks_matches_legacy_embed_esm_bit_exact() -> None:
    """T2A.1 acceptance: ``plugin.embed_chunks`` and the legacy
    ``_embed_esm`` shim produce bit-exact ``ChunkEmbedding`` vectors.

    The plugin's implementation is a literal port of the local function;
    this guard catches accidental drift during the T2A.2-T2A.4 sibling
    migrations (e.g. a shared helper changing one branch but not the
    other). Uses a synthetic mock model + tokenizer so no real PLM
    weights are downloaded. Activated by T2A.5b after the
    protea-backends pin advanced past #9 (every plugin now exposes
    ``embed_chunks``).
    """
    from typing import Any

    import numpy as np
    import torch
    from protea_backends.esm import plugin as esm_plugin

    _reset_plugin_cache()

    # Minimal config object compatible with both _embed_esm and embed_chunks.
    cfg = MagicMock()
    cfg.model_name = "facebook/esm2_t6_8M_UR50D"
    cfg.model_backend = "esm"
    cfg.layer_indices = [0]
    cfg.layer_agg = "mean"
    cfg.pooling = "mean"
    cfg.normalize = False
    cfg.normalize_residues = False
    cfg.max_length = 1022
    cfg.use_chunking = False
    cfg.chunk_size = 512
    cfg.chunk_overlap = 0

    dim = 16
    seq_len = 7  # CLS + 5 content + EOS

    def _make_inputs() -> tuple[Any, Any]:
        torch.manual_seed(0)
        hidden = torch.randn(1, seq_len, dim)
        tokens_dict = {
            "input_ids": torch.zeros(1, seq_len, dtype=torch.long),
            "attention_mask": torch.ones(1, seq_len, dtype=torch.long),
        }
        mock_outputs = MagicMock()
        mock_outputs.hidden_states = [hidden]
        mock_model = MagicMock()
        mock_model.return_value = mock_outputs
        mock_tokenizer = MagicMock()
        mock_tokenizer.return_value = tokens_dict
        return mock_model, mock_tokenizer

    # Reference: legacy _embed_esm shim.
    model_a, tok_a = _make_inputs()
    ref = ce_module._embed_esm(model_a, tok_a, ["ACDEF"], cfg, "cpu")

    # Plugin port — fresh model so the random tensor matches via seed.
    model_b, tok_b = _make_inputs()
    got = esm_plugin.embed_chunks(model_b, tok_b, ["ACDEF"], cfg, "cpu")

    assert len(ref) == 1 == len(got)
    assert len(ref[0]) == 1 == len(got[0])
    np.testing.assert_array_equal(ref[0][0].vector, got[0][0].vector)
    assert ref[0][0].chunk_index_s == got[0][0].chunk_index_s
    assert ref[0][0].chunk_index_e == got[0][0].chunk_index_e


# ---------------------------------------------------------------------------
# T2A.2 — t5 dispatch goes through plugin.embed_chunks
# ---------------------------------------------------------------------------


def test_dispatch_embed_routes_t5_to_plugin_embed_chunks() -> None:
    """T2A.2: the t5 branch in ``_dispatch_embed`` calls
    ``plugin.embed_chunks`` instead of the local ``_embed_t5`` shim."""
    _reset_plugin_cache()
    fake_plugin = MagicMock()
    fake_plugin.name = "t5"
    fake_plugin.embed_chunks.return_value = [["chunk"]]

    config = MagicMock()
    config.model_backend = "t5"

    with (
        patch.object(ce_module, "_get_backend_plugins", return_value={"t5": fake_plugin}),
        patch.object(ce_module, "_embed_t5") as legacy_shim,
    ):
        out = ce_module._dispatch_embed("model", "tok", ["MSEQ"], config, "cpu")

    assert out == [["chunk"]]
    fake_plugin.embed_chunks.assert_called_once_with("model", "tok", ["MSEQ"], config, "cpu")
    legacy_shim.assert_not_called()


def test_plugin_embed_chunks_matches_legacy_embed_t5_bit_exact() -> None:
    """T2A.2 acceptance: ``T5Backend.embed_chunks`` and the legacy
    ``_embed_t5`` shim produce bit-exact ``ChunkEmbedding`` vectors.

    The plugin's implementation is a literal port of the local function;
    this guard catches accidental drift during the T2A.3-T2A.4 sibling
    migrations (e.g. a shared helper changing one branch but not the
    other). Uses a synthetic mock model + tokenizer so no real PLM
    weights are downloaded. Activated by T2A.5b.
    """
    from typing import Any

    import numpy as np
    import torch
    from protea_backends.t5 import plugin as t5_plugin

    _reset_plugin_cache()

    cfg = MagicMock()
    cfg.model_name = "Rostlab/prot_t5_xl_uniref50"
    cfg.model_backend = "t5"
    cfg.layer_indices = [0]
    cfg.layer_agg = "mean"
    cfg.pooling = "mean"
    cfg.normalize = False
    cfg.normalize_residues = False
    cfg.max_length = 1022
    cfg.use_chunking = False
    cfg.chunk_size = 512
    cfg.chunk_overlap = 0

    dim = 16
    seq_len = 6  # 5 content + EOS (no AA2fold on prot_t5)

    def _make_inputs() -> tuple[Any, Any]:
        torch.manual_seed(0)
        hidden = torch.randn(1, seq_len, dim)
        tokens_dict = {
            "input_ids": torch.zeros(1, seq_len, dtype=torch.long),
            "attention_mask": torch.ones(1, seq_len, dtype=torch.long),
        }
        mock_outputs = MagicMock()
        mock_outputs.hidden_states = [hidden]
        mock_model = MagicMock()
        mock_model.return_value = mock_outputs
        mock_tokenizer = MagicMock()
        mock_tokenizer.batch_encode_plus.return_value = tokens_dict
        return mock_model, mock_tokenizer

    # Reference: legacy _embed_t5 shim.
    model_a, tok_a = _make_inputs()
    ref = ce_module._embed_t5(model_a, tok_a, ["ACDEF"], cfg, "cpu")

    # Plugin port — fresh model so the random tensor matches via seed.
    model_b, tok_b = _make_inputs()
    got = t5_plugin.embed_chunks(model_b, tok_b, ["ACDEF"], cfg, "cpu")

    assert len(ref) == 1 == len(got)
    assert len(ref[0]) == 1 == len(got[0])
    np.testing.assert_array_equal(ref[0][0].vector, got[0][0].vector)
    assert ref[0][0].chunk_index_s == got[0][0].chunk_index_s
    assert ref[0][0].chunk_index_e == got[0][0].chunk_index_e


# ---------------------------------------------------------------------------
# T2A.3 — ankh dispatch goes through plugin.embed_chunks
# ---------------------------------------------------------------------------


def test_dispatch_embed_routes_ankh_to_plugin_embed_chunks() -> None:
    """T2A.3: the ankh branch in ``_dispatch_embed`` calls
    ``plugin.embed_chunks`` instead of the local ``_embed_ankh`` shim."""
    _reset_plugin_cache()
    fake_plugin = MagicMock()
    fake_plugin.name = "ankh"
    fake_plugin.embed_chunks.return_value = [["chunk"]]

    config = MagicMock()
    config.model_backend = "ankh"

    with (
        patch.object(ce_module, "_get_backend_plugins", return_value={"ankh": fake_plugin}),
        patch.object(ce_module, "_embed_ankh") as legacy_shim,
    ):
        out = ce_module._dispatch_embed("model", "tok", ["MSEQ"], config, "cpu")

    assert out == [["chunk"]]
    fake_plugin.embed_chunks.assert_called_once_with("model", "tok", ["MSEQ"], config, "cpu")
    legacy_shim.assert_not_called()


def test_plugin_embed_chunks_matches_legacy_embed_ankh_bit_exact() -> None:
    """T2A.3 acceptance: ``AnkhBackend.embed_chunks`` and the legacy
    ``_embed_ankh`` shim produce bit-exact ``ChunkEmbedding`` vectors.

    The plugin delegates to ``protea_backends.t5.embed_chunks_with_mode``
    with the same Ankh mode flags PROTEA's ``_embed_ankh`` shim used, so
    this guard catches accidental drift if either side changes one
    branch but not the other. Uses a synthetic mock model + tokenizer
    so no real PLM weights are downloaded. Activated by T2A.5b.
    """
    from typing import Any

    import numpy as np
    import torch
    from protea_backends.ankh import plugin as ankh_plugin

    _reset_plugin_cache()

    cfg = MagicMock()
    cfg.model_name = "ElnaggarLab/ankh-base"
    cfg.model_backend = "ankh"
    cfg.layer_indices = [0]
    cfg.layer_agg = "mean"
    cfg.pooling = "mean"
    cfg.normalize = False
    cfg.normalize_residues = False
    cfg.max_length = 1022
    cfg.use_chunking = False
    cfg.chunk_size = 512
    cfg.chunk_overlap = 0

    dim = 16
    seq_len = 6  # 5 content + EOS (Ankh has no AA2fold prefix)

    def _make_inputs() -> tuple[Any, Any]:
        torch.manual_seed(0)
        hidden = torch.randn(1, seq_len, dim)
        tokens_dict = {
            "input_ids": torch.zeros(1, seq_len, dtype=torch.long),
            "attention_mask": torch.ones(1, seq_len, dtype=torch.long),
        }
        mock_outputs = MagicMock()
        mock_outputs.hidden_states = [hidden]
        mock_model = MagicMock()
        mock_model.return_value = mock_outputs
        mock_tokenizer = MagicMock()
        mock_tokenizer.batch_encode_plus.return_value = tokens_dict
        return mock_model, mock_tokenizer

    # Reference: legacy _embed_ankh shim.
    model_a, tok_a = _make_inputs()
    ref = ce_module._embed_ankh(model_a, tok_a, ["ACDEF"], cfg, "cpu")

    # Plugin port — fresh model so the random tensor matches via seed.
    model_b, tok_b = _make_inputs()
    got = ankh_plugin.embed_chunks(model_b, tok_b, ["ACDEF"], cfg, "cpu")

    assert len(ref) == 1 == len(got)
    assert len(ref[0]) == 1 == len(got[0])
    np.testing.assert_array_equal(ref[0][0].vector, got[0][0].vector)
    assert ref[0][0].chunk_index_s == got[0][0].chunk_index_s
    assert ref[0][0].chunk_index_e == got[0][0].chunk_index_e


# ---------------------------------------------------------------------------
# T2A.4 — esm3c dispatch goes through plugin.embed_chunks
# ---------------------------------------------------------------------------


def test_dispatch_embed_routes_esm3c_to_plugin_embed_chunks() -> None:
    """T2A.4: the esm3c branch in ``_dispatch_embed`` calls
    ``plugin.embed_chunks`` instead of the local ``_embed_esm3c`` shim."""
    _reset_plugin_cache()
    fake_plugin = MagicMock()
    fake_plugin.name = "esm3c"
    fake_plugin.embed_chunks.return_value = [["chunk"]]

    config = MagicMock()
    config.model_backend = "esm3c"

    with (
        patch.object(ce_module, "_get_backend_plugins", return_value={"esm3c": fake_plugin}),
        patch.object(ce_module, "_embed_esm3c") as legacy_shim,
    ):
        out = ce_module._dispatch_embed("model", "tok", ["MSEQ"], config, "cpu")

    assert out == [["chunk"]]
    fake_plugin.embed_chunks.assert_called_once_with("model", "tok", ["MSEQ"], config, "cpu")
    legacy_shim.assert_not_called()


def test_plugin_embed_chunks_matches_legacy_embed_esm3c_bit_exact() -> None:
    """T2A.4 acceptance: ``EsmcBackend.embed_chunks`` and the legacy
    ``_embed_esm3c`` shim produce bit-exact ``ChunkEmbedding`` vectors.

    The plugin's implementation is a literal port of PROTEA's pre-plugin
    ``_embed_esm3c_one`` (per-sequence SDK loop, max_length truncation,
    BOS / EOS stripping, layer selection, residue / CLS pooling); this
    guard catches accidental drift in either side. Uses a synthetic
    stub for ``model.encode`` / ``model.logits`` so no real ESM-C
    weights are downloaded. Activated by T2A.5b.
    """
    from typing import Any
    from unittest.mock import MagicMock

    import numpy as np
    import torch
    from protea_backends.esm3c import plugin as esm3c_plugin

    _reset_plugin_cache()

    cfg = MagicMock()
    cfg.model_name = "esmc_300m"
    cfg.model_backend = "esm3c"
    cfg.layer_indices = [0]
    cfg.layer_agg = "mean"
    cfg.pooling = "mean"
    cfg.normalize = False
    cfg.normalize_residues = False
    cfg.max_length = 1022
    cfg.use_chunking = False
    cfg.chunk_size = 512
    cfg.chunk_overlap = 0

    dim = 16
    seq_len_with_special = 7  # 5 content + BOS + EOS

    def _make_model() -> Any:
        """Stub ESM-C model: ``logits`` returns a deterministic
        ``hidden_states`` list shaped ``[(1, L, D)]``."""
        torch.manual_seed(0)
        hidden = torch.randn(1, seq_len_with_special, dim)
        mock_logits_output = MagicMock()
        mock_logits_output.hidden_states = [hidden]
        mock_model = MagicMock()
        # ``parameters`` is consulted by the plugin to read device_obj
        # via ``next(model.parameters()).device``; PROTEA's shim does
        # not need it (uses the ``device`` arg directly).
        mock_param = torch.nn.Parameter(torch.zeros(1))
        mock_model.parameters.return_value = iter([mock_param])
        mock_model.encode.return_value = "encoded"
        mock_model.logits.return_value = mock_logits_output
        return mock_model

    # Reference: legacy _embed_esm3c shim (4-arg signature: no tokenizer).
    model_a = _make_model()
    ref = ce_module._embed_esm3c(model_a, ["ACDEF"], cfg, "cpu")

    # Plugin port: 5-arg signature (tokenizer slot accepted and ignored).
    model_b = _make_model()
    got = esm3c_plugin.embed_chunks(model_b, None, ["ACDEF"], cfg, "cpu")

    assert len(ref) == 1 == len(got)
    assert len(ref[0]) == 1 == len(got[0])
    np.testing.assert_array_equal(ref[0][0].vector, got[0][0].vector)
    assert ref[0][0].chunk_index_s == got[0][0].chunk_index_s
    assert ref[0][0].chunk_index_e == got[0][0].chunk_index_e
