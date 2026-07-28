"""Unit tests for the P2 export classifier-output cache.

Targets ``protea.core.classifier_producer.predict_proteins_cached`` and its use
inside ``_export_features._union_classifier_candidates``. The classifier output
is t0-independent (it reads only the protein's frozen 6-PLM concat + the
checkpoint), so the export must compute it once per protein and reuse it across
the 13 train snapshot pairs instead of recomputing it per pair.

The tests prove:

* the cached output equals the fresh
  ``get_classifier().predict(load_concat_features(...))`` output (same go_ids +
  scores) for the same protein,
* a SECOND snapshot pair reuses the cache, so the underlying ``predict`` runs
  ONCE across two pairs for the same protein (spied),
* value-preserving: the export union records are byte-identical with vs without
  the cache.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import torch

from protea.core.classifier_producer import (
    CLASSIFIER_INPUT_DIM,
    PLM_CONCAT_ORDER,
    ClassifierPrediction,
    FullVocabClassifier,
    build_hybrid,
    predict_proteins_cached,
    reset_classifier_output_cache,
)

# Match the real 8320-d 6-PLM concat ``load_concat_features`` produces so the
# tiny checkpoint accepts the (fake) full embeddings.
_IN = CLASSIFIER_INPUT_DIM
_HID = 8
_LABEL = 4
_VOCAB = ["GO:0000001", "GO:0000002", "GO:0000003"]


def _write_tiny_classifier(tmp_path: Path) -> FullVocabClassifier:
    """A real tiny checkpoint so ``checkpoint_paths`` yields a real file sha."""
    tmp_path.mkdir(parents=True, exist_ok=True)
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
    return FullVocabClassifier(str(model_path), str(anc_path))


def _halfvec_text(dim: int) -> str:
    """Bracketed comma list, the wire shape pgvector emits for a halfvec."""
    return "[" + ",".join(["0.1"] * dim) + "]"


class _FakeCopy:
    def __init__(self, rows: list[tuple[str, str]]) -> None:
        self._rows = rows

    def __enter__(self) -> _FakeCopy:
        return self

    def __exit__(self, *_a: Any) -> None:
        return None

    def rows(self) -> Any:
        return iter(self._rows)


class _FakeCursor:
    def __init__(self, rows: list[tuple[str, str]]) -> None:
        self._rows = rows

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *_a: Any) -> None:
        return None

    def copy(self, _sql: str) -> _FakeCopy:
        return _FakeCopy(self._rows)


class _FakeRawConn:
    def __init__(self, session: _FakeSession) -> None:
        self._session = session

    def cursor(self) -> _FakeCursor:
        s = self._session
        dim = s._dims[s._call % len(s._dims)]
        s._call += 1
        s.query_calls += 1
        return _FakeCursor([(acc, _halfvec_text(dim)) for acc in s._accessions])


class _FakeConnection:
    def __init__(self, raw: _FakeRawConn) -> None:
        self.connection = raw


class _FakeSession:
    """Returns one full-dim embedding per (config, accession) so concat is full.

    Exposes the raw psycopg3 ``connection().connection.cursor().copy(...)`` chain
    the COPY-based ``_load_one_plm`` walks, and counts those COPY cursors
    (``query_calls``) so a test can still prove the embedding load (and hence the
    classifier compute) ran once across two pairs.
    """

    def __init__(self, accessions: list[str]) -> None:
        self._accessions = accessions
        self._dims = [dim for _, _, dim in PLM_CONCAT_ORDER]
        self._call = 0
        self.query_calls = 0
        self._raw = _FakeRawConn(self)

    def connection(self) -> _FakeConnection:
        return _FakeConnection(self._raw)


def _as_pairs(preds: list[ClassifierPrediction]) -> set[tuple[str, str, float]]:
    return {(p.accession, p.go_id, p.score) for p in preds}


def test_cached_equals_fresh_predict(tmp_path: Path) -> None:
    """Cached output == fresh predict(load_concat_features(...)) per protein."""
    from protea.core.classifier_producer import load_concat_features

    reset_classifier_output_cache()
    clf = _write_tiny_classifier(tmp_path)
    accs = ["Q1", "Q2"]

    fresh_sess = _FakeSession(accs)
    feats, valid = load_concat_features(fresh_sess, accs)
    fresh = clf.predict(feats, valid)

    cache_sess = _FakeSession(accs)
    cached = predict_proteins_cached(cache_sess, accs, classifier=clf)

    assert _as_pairs(cached) == _as_pairs(fresh)
    # Every protein with all six PLMs is scored.
    assert {p.accession for p in cached} == set(accs)


def test_second_pair_reuses_cache_no_recompute(tmp_path: Path) -> None:
    """A 2nd snapshot pair hits the cache: predict runs ONCE across two pairs."""
    reset_classifier_output_cache()
    clf = _write_tiny_classifier(tmp_path)
    calls: list[int] = []
    orig_predict = clf.predict

    def _spy(features: np.ndarray, accessions: list[str], **kw: Any) -> Any:
        calls.append(len(accessions))
        return orig_predict(features, accessions, **kw)

    clf.predict = _spy  # type: ignore[method-assign]

    accs = ["Q1", "Q2"]
    # Pair 1: cold -> one predict over both proteins.
    sess1 = _FakeSession(accs)
    out1 = predict_proteins_cached(sess1, accs, classifier=clf)
    # Pair 2: same proteins -> fully served from cache.
    sess2 = _FakeSession(accs)
    out2 = predict_proteins_cached(sess2, accs, classifier=clf)

    assert _as_pairs(out1) == _as_pairs(out2)
    # predict fired exactly once (pair 1); pair 2 hit the cache entirely.
    assert len(calls) == 1
    # And pair 2 never even loaded embeddings (no query() call).
    assert sess2.query_calls == 0


def test_checkpoint_change_invalidates_cache(tmp_path: Path) -> None:
    """A different checkpoint keys a different cache entry (re-computes)."""
    reset_classifier_output_cache()
    clf_a = _write_tiny_classifier(tmp_path / "a")
    clf_b = _write_tiny_classifier(tmp_path / "b")
    accs = ["Q1"]

    predict_proteins_cached(_FakeSession(accs), accs, classifier=clf_a)
    sess_b = _FakeSession(accs)
    predict_proteins_cached(sess_b, accs, classifier=clf_b)
    # The second checkpoint missed the cache, so it loaded embeddings.
    assert sess_b.query_calls > 0


def test_disk_cache_survives_process_reset(tmp_path: Path, monkeypatch: Any) -> None:
    """With PROTEA_CLASSIFIER_CACHE_DIR set, output survives a memo reset."""
    monkeypatch.setenv("PROTEA_CLASSIFIER_CACHE_DIR", str(tmp_path / "clf_cache"))
    reset_classifier_output_cache()
    clf = _write_tiny_classifier(tmp_path / "ckpt")
    accs = ["Q1"]

    first = predict_proteins_cached(_FakeSession(accs), accs, classifier=clf)
    # Drop the in-process memo: the next call must hydrate from disk, not recompute.
    reset_classifier_output_cache()
    sess = _FakeSession(accs)
    second = predict_proteins_cached(sess, accs, classifier=clf)

    assert _as_pairs(first) == _as_pairs(second)
    assert sess.query_calls == 0  # served from the on-disk tier, no embedding load


def _union(records: list[dict], session: Any, snapshot_id: Any, gid_by_go: dict) -> list[dict]:
    """Run the export union with go-term resolution stubbed to ``gid_by_go``."""
    from unittest.mock import patch

    from protea.core.training_dump._export_features import (
        ClassifierUnionSpec,
        _union_classifier_candidates,
    )

    def _factory(acc: str, go_id: str, aspect: str, score: float) -> dict:
        return {
            "protein_accession": acc,
            "go_id": go_id,
            "aspect": aspect,
            "classifier_score": score,
            "classifier_present": 1.0,
        }

    spec = ClassifierUnionSpec(
        ontology_snapshot_id=snapshot_id,
        record_factory=_factory,
        aspect_by_term_id={v: "F" for v in gid_by_go.values()},
    )
    with patch(
        "protea.core.classifier_producer.resolve_go_term_ids",
        return_value=gid_by_go,
    ):
        return _union_classifier_candidates(
            session, [r["protein_accession"] for r in records], [dict(r) for r in records], spec
        )


def test_export_union_byte_identical_with_vs_without_cache(tmp_path: Path) -> None:
    """The export union records are identical with vs without the cache."""
    import uuid

    reset_classifier_output_cache()
    clf = _write_tiny_classifier(tmp_path)
    snapshot_id = uuid.uuid4()
    gid_by_go = {go: i + 1 for i, go in enumerate(_VOCAB)}
    base_records = [
        {"protein_accession": "Q1", "go_term_id": 1},
        {"protein_accession": "Q2", "go_term_id": 2},
    ]

    from unittest.mock import patch

    # WITHOUT cache: force a fresh compute each call (cleared cache + classifier).
    with patch("protea.core.classifier_producer.get_classifier", return_value=clf):
        reset_classifier_output_cache()
        no_cache = _union(base_records, _FakeSession(["Q1", "Q2"]), snapshot_id, gid_by_go)
        # WITH cache warmed by a prior pair on the SAME proteins.
        reset_classifier_output_cache()
        predict_proteins_cached(_FakeSession(["Q1", "Q2"]), ["Q1", "Q2"], classifier=clf)
        with_cache = _union(base_records, _FakeSession(["Q1", "Q2"]), snapshot_id, gid_by_go)

    assert no_cache == with_cache


# --------------------------------------------------------------------------- #
# per-cut GO codes: cache-key isolation + impl-guarded resolution
# --------------------------------------------------------------------------- #
def test_clf_cache_key_isolates_go_codes() -> None:
    """Distinct go_codes shas (distinct cuts) key distinct entries; same -> same."""
    from protea.core.classifier_producer import _clf_cache_key

    k_v227 = _clf_cache_key("Q1", "ckpt", "codes227")
    k_v230 = _clf_cache_key("Q1", "ckpt", "codes230")
    assert k_v227 != k_v230  # different t0 -> different key
    assert k_v227 == _clf_cache_key("Q1", "ckpt", "codes227")  # same t0 -> same key
    # Empty codes sha (M2 / single-artifact two-tower) keeps today's 3-part key.
    assert _clf_cache_key("Q1", "ckpt") == _clf_cache_key("Q1", "ckpt", "")
    assert "codes227" not in _clf_cache_key("Q1", "ckpt")


def _spec_with_t0(t0: Any) -> Any:
    import uuid

    from protea.core.training_dump._export_features import ClassifierUnionSpec

    return ClassifierUnionSpec(ontology_snapshot_id=uuid.uuid4(), t0_annotation_set_id=t0)


def test_resolve_per_cut_go_codes_noop_for_m2() -> None:
    """The M2 impl never selects per-cut codes even with a t0 on the spec."""
    import uuid

    from protea.core.training_dump._export_features import _resolve_per_cut_go_codes

    spec = _spec_with_t0(uuid.uuid4())
    assert _resolve_per_cut_go_codes(object(), spec, "m2") is None


def test_resolve_per_cut_go_codes_two_tower(tmp_path: Path, monkeypatch: Any) -> None:
    """Two-tower impl + configured dir resolves this cut's t0 to its codes file."""
    import uuid

    from protea.core import two_tower_classifier as tt
    from protea.core.training_dump._export_features import _resolve_per_cut_go_codes

    codes_dir = tmp_path / "per_cut"
    codes_dir.mkdir()
    target = codes_dir / "go_sparse_codes_v227.npz"
    target.write_bytes(b"x")
    monkeypatch.setenv(tt._GO_CODES_DIR_ENV, str(codes_dir))

    spec = _spec_with_t0(uuid.uuid4())

    def _fake_resolve(_session: Any, t0: Any) -> str:
        assert t0 == spec.t0_annotation_set_id
        return str(target)

    monkeypatch.setattr(tt, "resolve_per_cut_go_codes_path", _fake_resolve)
    assert _resolve_per_cut_go_codes(object(), spec, "two_tower_sparse") == str(target)


def test_resolve_per_cut_go_codes_none_without_t0() -> None:
    """No t0 on the spec -> no per-cut selection (single fixed artifact)."""
    from protea.core.training_dump._export_features import _resolve_per_cut_go_codes

    spec = _spec_with_t0(None)
    assert _resolve_per_cut_go_codes(object(), spec, "two_tower_sparse") is None
