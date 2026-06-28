"""Unit tests for the opt-in two-tower sparse classifier (iteration ``sparse-classifier``).

Targets :mod:`protea.core.two_tower_classifier` plus the
``PROTEA_CLASSIFIER_IMPL`` dispatch in :mod:`protea.core.classifier_producer`.
The real 7 x 25 MB head checkpoints and the 165 MB GO-codes file are NOT used:
tiny self-contained ``ProjHead`` checkpoints, a small GO-codes npz and a vocab
file are written on the fly, so the full load + GO-matrix build + score +
top-``k`` path runs while staying light. No DB and no lab import.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import torch

from protea.core import classifier_producer as cp
from protea.core import two_tower_classifier as tt
from protea.core.classifier_producer import ClassifierPrediction
from protea.core.two_tower_classifier import (
    TwoTowerSparseClassifier,
    reset_two_tower_cache,
)

_DIN = tt.TWO_TOWER_INPUT_DIM  # 2048
_DOUT = tt.TWO_TOWER_GO_DIM  # 1024
_HID = 16


def _go_id(i: int) -> str:
    return f"GO:{i:07d}"


def _write_artifacts(
    tmp_path: Path, n_seeds: int = 3, n_full: int = 130, n_train: int = 120
) -> tuple[str, str, str]:
    """Write tiny {n_seeds head ckpts, go_sparse_codes.npz, vocab_go.npy}.

    The trained vocab is a REORDERED SUBSET of the full GO-codes vocab so the
    GO-matrix selection + bias alignment path is exercised.
    """
    full_ids = [_go_id(i) for i in range(n_full)]
    rng = np.random.RandomState(0)
    full_codes = rng.randn(n_full, _DOUT).astype(np.float32)
    go_codes_path = tmp_path / "go_sparse_codes.npz"
    np.savez(go_codes_path, go_ids=np.array(full_ids, dtype=object), codes=full_codes)

    # trained vocab: a subset, deliberately not the leading rows and reordered.
    train_ids = list(reversed(full_ids[:n_train]))
    vocab_path = tmp_path / "vocab_go.npy"
    np.save(vocab_path, np.array(train_ids, dtype="<U10"))

    seed_dir = tmp_path / "heads"
    seed_dir.mkdir()
    for s in range(n_seeds):
        torch.manual_seed(s)
        model = tt.ProjHead(_DIN, _DOUT, vocab_size=n_train, hidden=_HID, kwta=0)
        torch.save(
            {
                "state_dict": model.state_dict(),
                "cfg": {"hidden": _HID, "kwta": 0},
                "V": n_train,
                "seed": s,
            },
            seed_dir / f"head_seed{s}.pt",
        )
    return str(seed_dir), str(go_codes_path), str(vocab_path)


def _make(tmp_path: Path, **kw) -> TwoTowerSparseClassifier:
    seed_dir, go_codes_path, vocab_path = _write_artifacts(tmp_path, **kw)
    return TwoTowerSparseClassifier(seed_dir, go_codes_path, vocab_path)


# --------------------------------------------------------------------------- #
# load + score
# --------------------------------------------------------------------------- #
def test_loads_and_scores_top_n_finite(tmp_path: Path) -> None:
    clf = _make(tmp_path, n_train=120)
    assert len(clf.vocab) == 120
    assert clf.n_seeds == 3
    feats = np.random.RandomState(1).randn(3, _DIN).astype(np.float32)
    accs = ["Q1", "Q2", "Q3"]
    preds = clf.predict(feats, accs, top_n=100)
    assert all(isinstance(p, ClassifierPrediction) for p in preds)
    # 3 proteins x exactly 100 candidates each.
    assert len(preds) == 300
    vocab_set = set(clf.vocab)
    for acc in accs:
        rows = [p for p in preds if p.accession == acc]
        assert len(rows) == 100
        assert len({p.go_id for p in rows}) == 100  # distinct terms
        assert all(p.go_id in vocab_set for p in rows)
        assert all(np.isfinite(p.score) for p in rows)
        # rows are emitted in descending score order (topk).
        scores = [p.score for p in rows]
        assert scores == sorted(scores, reverse=True)


def test_top_n_capped_to_vocab_size(tmp_path: Path) -> None:
    clf = _make(tmp_path, n_full=12, n_train=8)
    feats = np.random.RandomState(2).randn(2, _DIN).astype(np.float32)
    preds = clf.predict(feats, ["A", "B"], top_n=100)
    assert len(preds) == 16  # 2 proteins x min(100, 8)


def test_predict_empty_returns_empty(tmp_path: Path) -> None:
    clf = _make(tmp_path)
    assert clf.predict(np.empty((0, _DIN), dtype=np.float32), []) == []


def test_checkpoint_paths_cover_all_artifacts(tmp_path: Path) -> None:
    seed_dir, go_codes_path, vocab_path = _write_artifacts(tmp_path, n_seeds=3)
    clf = TwoTowerSparseClassifier(seed_dir, go_codes_path, vocab_path)
    assert len(clf.checkpoint_paths) == 5  # 3 heads + go codes + vocab
    assert go_codes_path in clf.checkpoint_paths
    assert vocab_path in clf.checkpoint_paths


def test_go_matrix_aligned_to_trained_vocab(tmp_path: Path) -> None:
    clf = _make(tmp_path, n_full=20, n_train=10)
    # _go_t is (1024 x V); column j must equal the GO code of clf.vocab[j].
    full = np.load(str(tmp_path / "go_sparse_codes.npz"), allow_pickle=True)
    idx = {str(g): i for i, g in enumerate(full["go_ids"])}
    go_t = clf._go_t.cpu().numpy()
    for j, term in enumerate(clf.vocab):
        assert np.allclose(go_t[:, j], full["codes"][idx[term]], atol=1e-5)


# --------------------------------------------------------------------------- #
# env-flag dispatch (M2 stays the default)
# --------------------------------------------------------------------------- #
def test_classifier_impl_default_is_m2(monkeypatch) -> None:
    monkeypatch.delenv(cp._IMPL_ENV, raising=False)
    assert cp.classifier_impl() == "m2"
    monkeypatch.setenv(cp._IMPL_ENV, "two_tower_sparse")
    assert cp.classifier_impl() == "two_tower_sparse"


def test_get_classifier_dispatches_two_tower_when_flag_set(tmp_path: Path, monkeypatch) -> None:
    reset_two_tower_cache()
    seed_dir, go_codes_path, vocab_path = _write_artifacts(tmp_path)
    monkeypatch.setenv(cp._IMPL_ENV, "two_tower_sparse")
    monkeypatch.setenv(tt._SEED_DIR_ENV, seed_dir)
    monkeypatch.setenv(tt._GO_CODES_ENV, go_codes_path)
    monkeypatch.setenv(tt._VOCAB_ENV, vocab_path)
    clf = cp.get_classifier()
    assert isinstance(clf, TwoTowerSparseClassifier)
    # cached: a second call returns the same instance.
    assert cp.get_classifier() is clf
    reset_two_tower_cache()


def test_get_classifier_default_is_not_two_tower(monkeypatch) -> None:
    """With the flag unset, get_classifier must NOT build the two-tower head."""
    monkeypatch.delenv(cp._IMPL_ENV, raising=False)

    sentinel = object()
    monkeypatch.setattr(cp, "FullVocabClassifier", lambda *a, **k: sentinel)
    monkeypatch.setattr(cp, "resolve_seed_paths", lambda *a, **k: [])
    monkeypatch.setattr(cp, "_CLASSIFIER_CACHE", {})
    assert cp.get_classifier() is sentinel


def test_load_classifier_features_routes_by_impl(monkeypatch) -> None:
    m2_marker = (np.zeros((1, 1), np.float32), ["m2"])
    tt_marker = (np.zeros((1, 1), np.float32), ["tt"])
    monkeypatch.setattr(cp, "load_concat_features", lambda *a, **k: m2_marker)
    monkeypatch.setattr(tt, "load_two_tower_features", lambda *a, **k: tt_marker)

    monkeypatch.delenv(cp._IMPL_ENV, raising=False)
    assert cp.load_classifier_features(object(), ["X"])[1] == ["m2"]

    monkeypatch.setenv(cp._IMPL_ENV, "two_tower_sparse")
    assert cp.load_classifier_features(object(), ["X"])[1] == ["tt"]


def test_missing_env_raises(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv(tt._SEED_DIR_ENV, raising=False)
    monkeypatch.delenv(tt._GO_CODES_ENV, raising=False)
    monkeypatch.delenv(tt._VOCAB_ENV, raising=False)
    import pytest

    with pytest.raises(ValueError, match="requires"):
        TwoTowerSparseClassifier()


def test_two_tower_config_id_env_override(monkeypatch) -> None:
    monkeypatch.delenv(tt._CONFIG_ID_ENV, raising=False)
    assert tt.two_tower_config_id() == tt.TWO_TOWER_INPUT_CONFIG_ID
    monkeypatch.setenv(tt._CONFIG_ID_ENV, "other-config")
    assert tt.two_tower_config_id() == "other-config"
