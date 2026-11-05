"""The raw f32 reference copy is materialised only when something reads it.

Under cosine the KNN search reads the L2-normalised copy, and the only other
reader of the raw one is the embedding-PCA fit, which runs only for the v6
features and ignores its pool once the PCA state is cached. Exploration 1
(2026-11-05) ran the production recipe, cosine with v6 off and no PCA
artefact, and held the raw copy for a fit that never ran.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

from protea.core import pca_cache
from protea.core.operations.predict_go_terms._reference_needs import reference_dtype_needs


def _needs(state: Path, monkeypatch: pytest.MonkeyPatch, **payload: object) -> tuple[bool, bool]:
    monkeypatch.setattr(pca_cache, "_pca_state_path", lambda _config: state)
    p = SimpleNamespace(embedding_config_id=uuid.uuid4(), **payload)
    return reference_dtype_needs(p)


@pytest.mark.parametrize(
    ("metric", "v6", "pca_cached", "expected"),
    [
        pytest.param("cosine", False, False, (True, False), id="production recipe"),
        pytest.param("cosine", True, False, (True, True), id="the fit still runs"),
        pytest.param("cosine", True, True, (True, False), id="the fit reads its cache"),
        pytest.param("l2", False, True, (False, True), id="the search reads it"),
    ],
)
def test_the_raw_copy_follows_its_readers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    metric: str,
    v6: bool,
    pca_cached: bool,
    expected: tuple[bool, bool],
) -> None:
    state = tmp_path / "pca.npz"
    if pca_cached:
        state.write_bytes(b"")
    got = _needs(state, monkeypatch, metric=metric, compute_v6_features=v6)
    assert got == expected


def test_a_payload_that_does_not_say_keeps_the_copy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert _needs(tmp_path / "pca.npz", monkeypatch, metric="cosine") == (True, True)
