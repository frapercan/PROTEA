"""Unit tests for the seed-averaged classifier (lafa-integrate INT-7).

Targets ``protea.core.classifier_producer.SeedAveragedClassifier`` plus the
``resolve_seed_paths`` resolver and the ``get_classifier`` dispatch. The real
320 MB seed checkpoints are NOT used: 2-3 TINY self-contained ``Hybrid``
checkpoints (same format the producer ships) and a tiny anc2vec npz are written
on the fly, so the full load + predict + consensus path is exercised while
staying light.

The averaging contract under test is a faithful port of
``protea-reranker-lab/fullgo/seed_average.py``: union of each seed's top-K
(protein, term) pairs, per-pair score = sum(present scores) / n_total_seeds.
"""

from __future__ import annotations

import os
from collections import OrderedDict
from pathlib import Path

import numpy as np
import torch

from protea.core import classifier_producer as cp
from protea.core.classifier_producer import (
    FullVocabClassifier,
    SeedAveragedClassifier,
    build_hybrid,
    get_classifier,
    resolve_seed_paths,
)

_IN = 12
_HID = 8
_LABEL = 4
_VOCAB = ["GO:0000001", "GO:0000002", "GO:0000003", "GO:0000004"]


def _write_anc2vec(tmp_path: Path) -> str:
    anc_path = tmp_path / "anc.npz"
    np.savez(
        anc_path,
        go_ids=np.array(_VOCAB, dtype=object),
        embeddings=np.random.RandomState(0).randn(len(_VOCAB), _LABEL).astype(np.float32),
    )
    return str(anc_path)


def _write_seed(tmp_path: Path, name: str, torch_seed: int) -> str:
    """Write one tiny self-contained seed checkpoint with seeded weights."""
    torch.manual_seed(torch_seed)
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
    path = tmp_path / name
    torch.save(ckpt, path)
    return str(path)


# --------------------------------------------------------------------------- #
# resolve_seed_paths
# --------------------------------------------------------------------------- #
def test_resolve_seed_paths_empty_when_unset(monkeypatch) -> None:
    monkeypatch.delenv(cp._SEED_DIR_ENV, raising=False)
    monkeypatch.delenv(cp._SEED_PATHS_ENV, raising=False)
    assert resolve_seed_paths() == []


def test_resolve_seed_paths_sorts_dir_glob(tmp_path: Path, monkeypatch) -> None:
    # Write in non-sorted creation order; resolver must return sorted.
    _write_seed(tmp_path, "seed_b.pt", 2)
    _write_seed(tmp_path, "seed_a.pt", 1)
    (tmp_path / "ignore.txt").write_text("not a checkpoint")
    monkeypatch.delenv(cp._SEED_PATHS_ENV, raising=False)
    monkeypatch.setenv(cp._SEED_DIR_ENV, str(tmp_path))
    paths = resolve_seed_paths()
    assert [Path(p).name for p in paths] == ["seed_a.pt", "seed_b.pt"]


def test_resolve_seed_paths_explicit_list_takes_precedence(tmp_path: Path, monkeypatch) -> None:
    p1 = _write_seed(tmp_path, "one.pt", 1)
    p2 = _write_seed(tmp_path, "two.pt", 2)
    monkeypatch.setenv(cp._SEED_DIR_ENV, str(tmp_path))
    monkeypatch.setenv(cp._SEED_PATHS_ENV, os.pathsep.join([p2, p1]))
    # Explicit env wins over dir; still sorted.
    assert resolve_seed_paths() == sorted([p1, p2])


# --------------------------------------------------------------------------- #
# SeedAveragedClassifier consensus contract
# --------------------------------------------------------------------------- #
def test_single_seed_equals_baseline(tmp_path: Path) -> None:
    """One seed: consensus == the single-checkpoint baseline (modulo order)."""
    anc = _write_anc2vec(tmp_path)
    path = _write_seed(tmp_path, "s0.pt", 7)
    feats = np.random.RandomState(3).randn(2, _IN).astype(np.float32)
    accs = ["Q1", "Q2"]

    baseline = FullVocabClassifier(path, anc).predict(feats, accs, top_n=4, min_score=0.0)
    avg = SeedAveragedClassifier([path], anc).predict(feats, accs, top_n=4, min_score=0.0)

    base_map = {(p.accession, p.go_id): p.score for p in baseline}
    avg_map = {(p.accession, p.go_id): p.score for p in avg}
    assert base_map.keys() == avg_map.keys()
    for key in base_map:
        assert avg_map[key] == base_map[key]  # n=1 -> sum/1 is exact


def test_seedenv_unset_matches_single_checkpoint(tmp_path: Path, monkeypatch) -> None:
    """get_classifier with no seed env returns the single-checkpoint class."""
    anc = _write_anc2vec(tmp_path)
    path = _write_seed(tmp_path, "single.pt", 11)
    monkeypatch.delenv(cp._SEED_DIR_ENV, raising=False)
    monkeypatch.delenv(cp._SEED_PATHS_ENV, raising=False)
    cp._CLASSIFIER_CACHE.clear()
    clf = get_classifier(model_path=path, anc2vec_path=anc)
    assert isinstance(clf, FullVocabClassifier)
    assert not isinstance(clf, SeedAveragedClassifier)


def test_get_classifier_dispatches_to_seed_avg(tmp_path: Path, monkeypatch) -> None:
    anc = _write_anc2vec(tmp_path)
    _write_seed(tmp_path, "a.pt", 1)
    _write_seed(tmp_path, "b.pt", 2)
    monkeypatch.delenv(cp._SEED_PATHS_ENV, raising=False)
    monkeypatch.setenv(cp._SEED_DIR_ENV, str(tmp_path))
    monkeypatch.setenv(cp._ANC2VEC_ENV, anc)
    cp._SEED_CLASSIFIER_CACHE.clear()
    clf = get_classifier()
    assert isinstance(clf, SeedAveragedClassifier)
    assert clf.n_seeds == 2


def test_two_seed_union_and_mean_present(tmp_path: Path) -> None:
    """Hand-computed 2-seed consensus: union of pairs, score = sum/n.

    Drives the consensus directly off two fake single-seed predictors so the
    expected values are computable by hand without depending on torch weights.
    """
    anc = _write_anc2vec(tmp_path)
    path = _write_seed(tmp_path, "s0.pt", 1)
    avg = SeedAveragedClassifier([path], anc)  # construct, then swap the seeds

    class _FakeSeed:
        def __init__(self, rows):
            self._rows = rows

        def predict(self, *_a, **_k):
            return [cp.ClassifierPrediction(a, g, s) for a, g, s in self._rows]

    seed1 = _FakeSeed(
        [
            ("Q1", "GO:0000001", 0.8),  # present in both
            ("Q1", "GO:0000002", 0.4),  # present in seed1 only
        ]
    )
    seed2 = _FakeSeed(
        [
            ("Q1", "GO:0000001", 0.6),  # present in both
            ("Q1", "GO:0000003", 0.9),  # present in seed2 only
        ]
    )
    avg.seeds = [seed1, seed2]
    avg.n_seeds = 2

    feats = np.zeros((1, _IN), np.float32)
    out = {(p.accession, p.go_id): p.score for p in avg.predict(feats, ["Q1"])}

    # Union of three pairs.
    assert set(out) == {("Q1", "GO:0000001"), ("Q1", "GO:0000002"), ("Q1", "GO:0000003")}
    # Present in both seeds: (0.8 + 0.6) / 2 = 0.7
    assert out[("Q1", "GO:0000001")] == 0.7
    # Present in seed1 only: 0.4 / 2 = 0.2 (mean over TOTAL seeds, not present)
    assert out[("Q1", "GO:0000002")] == 0.2
    # Present in seed2 only: 0.9 / 2 = 0.45
    assert out[("Q1", "GO:0000003")] == 0.45


def test_term_in_one_of_two_seeds_gets_half_score(tmp_path: Path) -> None:
    """A term surfaced by exactly 1 of 2 seeds scores its_score / 2."""
    anc = _write_anc2vec(tmp_path)
    path = _write_seed(tmp_path, "s0.pt", 1)
    avg = SeedAveragedClassifier([path], anc)

    class _FakeSeed:
        def __init__(self, rows):
            self._rows = rows

        def predict(self, *_a, **_k):
            return [cp.ClassifierPrediction(a, g, s) for a, g, s in self._rows]

    avg.seeds = [_FakeSeed([("Q1", "GO:0000002", 0.5)]), _FakeSeed([])]
    avg.n_seeds = 2
    out = avg.predict(np.zeros((1, _IN), np.float32), ["Q1"])
    assert len(out) == 1
    assert out[0].go_id == "GO:0000002"
    assert out[0].score == 0.25


def test_deterministic_two_runs_identical(tmp_path: Path) -> None:
    anc = _write_anc2vec(tmp_path)
    p1 = _write_seed(tmp_path, "a.pt", 1)
    p2 = _write_seed(tmp_path, "b.pt", 2)
    p3 = _write_seed(tmp_path, "c.pt", 3)
    feats = np.random.RandomState(5).randn(3, _IN).astype(np.float32)
    accs = ["Q1", "Q2", "Q3"]
    clf = SeedAveragedClassifier([p1, p2, p3], anc)
    run1 = [(p.accession, p.go_id, p.score) for p in clf.predict(feats, accs)]
    run2 = [(p.accession, p.go_id, p.score) for p in clf.predict(feats, accs)]
    assert run1 == run2
    # Output is sorted by (accession, go_id), so the order is stable too.
    assert run1 == sorted(run1, key=lambda r: (r[0], r[1]))


def test_seed_order_independent(tmp_path: Path) -> None:
    """Permuting the seed list yields the same consensus (commutative sum)."""
    anc = _write_anc2vec(tmp_path)
    p1 = _write_seed(tmp_path, "a.pt", 1)
    p2 = _write_seed(tmp_path, "b.pt", 2)
    feats = np.random.RandomState(9).randn(2, _IN).astype(np.float32)
    accs = ["Q1", "Q2"]
    forward = SeedAveragedClassifier([p1, p2], anc).predict(feats, accs)
    reverse = SeedAveragedClassifier([p2, p1], anc).predict(feats, accs)
    fm = OrderedDict(((p.accession, p.go_id), p.score) for p in forward)
    rm = OrderedDict(((p.accession, p.go_id), p.score) for p in reverse)
    assert fm == rm


def test_empty_seed_paths_rejected(tmp_path: Path) -> None:
    anc = _write_anc2vec(tmp_path)
    try:
        SeedAveragedClassifier([], anc)
    except ValueError:
        return
    raise AssertionError("expected ValueError on empty seed list")
