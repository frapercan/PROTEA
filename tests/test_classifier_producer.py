"""Unit tests for the native full-vocabulary classifier producer (INT-4).

Targets ``protea.core.classifier_producer``. The 320 MB real checkpoint is
NOT used: a tiny synthetic ``Hybrid`` state_dict + a tiny anc2vec npz are
written to a temp dir so the real load path (``build_hybrid`` ->
``load_state_dict`` -> label-matrix rebuild -> ``predict``) is exercised
end-to-end while staying light.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import numpy as np
import torch

from protea.core.classifier_producer import (
    CLASSIFIER_INPUT_DIM,
    PLM_CONCAT_ORDER,
    FullVocabClassifier,
    build_hybrid,
    load_concat_features,
)

_IN = 12
_HID = 8
_LABEL = 4
_VOCAB = ["GO:0000001", "GO:0000002", "GO:0000003"]


def _write_tiny_artifacts(tmp_path: Path) -> tuple[str, str]:
    """Write a tiny self-contained checkpoint + anc2vec npz; return paths."""
    model = build_hybrid(_IN, _HID, len(_VOCAB), _LABEL)
    ckpt = {
        "state_dict": model.state_dict(),
        "mu": np.zeros((1, _IN), np.float32),
        "sd": np.ones((1, _IN), np.float32),
        "in_dim": _IN,
        "hidden": _HID,
        "label_dim": _LABEL,
        "vocab": _VOCAB,
        "arch": "hybrid_anc2vec",
    }
    model_path = tmp_path / "tiny.pt"
    torch.save(ckpt, model_path)
    anc_path = tmp_path / "anc.npz"
    np.savez(
        anc_path,
        go_ids=np.array(_VOCAB, dtype=object),
        embeddings=np.random.RandomState(0).randn(len(_VOCAB), _LABEL).astype(np.float32),
    )
    return str(model_path), str(anc_path)


def test_input_dim_is_8320() -> None:
    assert CLASSIFIER_INPUT_DIM == 8320
    assert len(PLM_CONCAT_ORDER) == 6


def test_loads_architecture_and_produces_topn(tmp_path: Path) -> None:
    model_path, anc_path = _write_tiny_artifacts(tmp_path)
    clf = FullVocabClassifier(model_path, anc_path)
    assert clf.vocab == _VOCAB
    feats = np.random.RandomState(1).randn(2, _IN).astype(np.float32)
    preds = clf.predict(feats, ["Q1", "Q2"], top_n=2, min_score=0.0)
    # 2 proteins x top_n=2 = 4 rows, each a vocab term with a [0,1] score.
    assert len(preds) == 4
    assert {pr.accession for pr in preds} == {"Q1", "Q2"}
    assert all(pr.go_id in _VOCAB for pr in preds)
    assert all(0.0 <= pr.score <= 1.0 for pr in preds)


def test_min_score_filters_low_terms(tmp_path: Path) -> None:
    model_path, anc_path = _write_tiny_artifacts(tmp_path)
    clf = FullVocabClassifier(model_path, anc_path)
    feats = np.zeros((1, _IN), dtype=np.float32)
    # An impossible threshold drops every term.
    assert clf.predict(feats, ["Q1"], top_n=3, min_score=1.01) == []


def test_empty_features_returns_empty(tmp_path: Path) -> None:
    model_path, anc_path = _write_tiny_artifacts(tmp_path)
    clf = FullVocabClassifier(model_path, anc_path)
    assert clf.predict(np.empty((0, _IN), np.float32), []) == []


class _FakeVec:
    def __init__(self, dim: int) -> None:
        self._v = [0.1] * dim

    def to_list(self) -> list[float]:
        return self._v


class _FakeQuery:
    """Minimal stand-in for the SQLAlchemy query chain in ``_load_one_plm``."""

    def __init__(self, rows: list[tuple[str, _FakeVec]]) -> None:
        self._rows = rows

    def join(self, *_a, **_k) -> _FakeQuery:
        return self

    def filter(self, *_a, **_k) -> _FakeQuery:
        return self

    def all(self) -> list[tuple[str, _FakeVec]]:
        return self._rows


class _FakeSession:
    """Returns one embedding per (config, accession) so the concat is full."""

    def __init__(self, dims: list[int]) -> None:
        self._dims = dims
        self._call = 0

    def query(self, *_a, **_k) -> _FakeQuery:
        dim = self._dims[self._call % len(self._dims)]
        self._call += 1
        return _FakeQuery([("Q1", _FakeVec(dim))])


def test_load_concat_features_builds_8320_in_order() -> None:
    dims = [dim for _, _, dim in PLM_CONCAT_ORDER]
    session = _FakeSession(dims)
    feats, valid = load_concat_features(session, ["Q1"])
    assert valid == ["Q1"]
    assert feats.shape == (1, CLASSIFIER_INPUT_DIM)


def test_load_concat_drops_protein_missing_a_plm() -> None:
    # First config returns nothing for Q1 -> the protein is dropped entirely.
    class _Empty(_FakeSession):
        def query(self, *_a, **_k) -> _FakeQuery:
            self._call += 1
            return _FakeQuery([] if self._call == 1 else [("Q1", _FakeVec(768))])

    out_feats, out_valid = load_concat_features(_Empty([768]), ["Q1"])
    assert out_valid == []
    assert out_feats.shape == (0, CLASSIFIER_INPUT_DIM)


def test_resolve_go_term_ids_maps_via_snapshot() -> None:
    from protea.core.classifier_producer import resolve_go_term_ids

    class _Result:
        def all(self) -> list[tuple[str, int]]:
            return [("GO:0000001", 11), ("GO:0000002", 22)]

    class _Sess:
        def execute(self, *_a, **_k) -> _Result:
            return _Result()

    out = resolve_go_term_ids(_Sess(), {"GO:0000001", "GO:0000002"}, uuid.uuid4())
    assert out == {"GO:0000001": 11, "GO:0000002": 22}
