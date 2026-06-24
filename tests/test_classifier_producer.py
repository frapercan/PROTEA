"""Unit tests for the native full-vocabulary classifier producer (INT-4).

Targets ``protea.core.classifier_producer``. The 320 MB real checkpoint is
NOT used: a tiny synthetic ``Hybrid`` state_dict + a tiny anc2vec npz are
written to a temp dir so the real load path (``build_hybrid`` ->
``load_state_dict`` -> label-matrix rebuild -> ``predict``) is exercised
end-to-end while staying light.
"""

from __future__ import annotations

import hashlib
import uuid
from pathlib import Path

import numpy as np
import pytest
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


class _FakeCopy:
    """Mimics ``cursor.copy(sql)`` as a context manager yielding ``cp.rows()``.

    ``_load_one_plm`` now reads embeddings with a raw psycopg3
    ``COPY (SELECT ...) TO STDOUT`` (mirroring ``_association_loader``) instead
    of an ORM ``.all()``. Each row is ``(accession, halfvec_text)`` where the
    halfvec literal is the bracketed comma list pgvector emits. These fakes feed
    that exact wire shape so the ``load_concat_features`` concat / drop logic
    stays unit-tested without standing up Postgres; a real bit-exactness parity
    test against a live ``halfvec`` column is
    ``test_load_one_plm_copy_is_byte_identical_to_orm`` (gated --with-postgres).
    """

    def __init__(self, rows: list[tuple[str, str]]) -> None:
        self._rows = rows

    def __enter__(self) -> _FakeCopy:
        return self

    def __exit__(self, *_a) -> None:
        return None

    def rows(self):  # noqa: ANN201 - test stub
        return iter(self._rows)


class _FakeCursor:
    def __init__(self, rows: list[tuple[str, str]]) -> None:
        self._rows = rows

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *_a) -> None:
        return None

    def copy(self, _sql: str) -> _FakeCopy:
        return _FakeCopy(self._rows)


class _FakeRawConn:
    def __init__(self, rows_per_call: list[list[tuple[str, str]]]) -> None:
        self._rows_per_call = rows_per_call
        self._call = 0

    def cursor(self) -> _FakeCursor:
        rows = self._rows_per_call[self._call % len(self._rows_per_call)]
        self._call += 1
        return _FakeCursor(rows)


class _FakeConnection:
    def __init__(self, raw: _FakeRawConn) -> None:
        self.connection = raw


class _FakeSession:
    """Yields one halfvec text literal per (config, accession) call."""

    def __init__(self, dims: list[int]) -> None:
        rows_per_call = [[("Q1", _halfvec_text([0.1] * dim))] for dim in dims]
        self._raw = _FakeRawConn(rows_per_call)

    def connection(self) -> _FakeConnection:
        return _FakeConnection(self._raw)


def _halfvec_text(values: list[float]) -> str:
    """Bracketed comma list, the wire shape pgvector emits for a halfvec."""
    return "[" + ",".join(repr(v) for v in values) + "]"


def test_load_concat_features_builds_8320_in_order() -> None:
    dims = [dim for _, _, dim in PLM_CONCAT_ORDER]
    session = _FakeSession(dims)
    feats, valid = load_concat_features(session, ["Q1"])
    assert valid == ["Q1"]
    assert feats.shape == (1, CLASSIFIER_INPUT_DIM)


def test_load_concat_drops_protein_missing_a_plm() -> None:
    # First config returns nothing for Q1 -> the protein is dropped entirely.
    rows_per_call = [[]] + [[("Q1", _halfvec_text([0.1] * 768))]] * (len(PLM_CONCAT_ORDER) - 1)

    class _Empty:
        def __init__(self) -> None:
            self._raw = _FakeRawConn(rows_per_call)

        def connection(self) -> _FakeConnection:
            return _FakeConnection(self._raw)

    out_feats, out_valid = load_concat_features(_Empty(), ["Q1"])
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


def test_quote_accession_list_rejects_injection() -> None:
    from protea.core.classifier_producer import _quote_accession_list

    # Normal UniProt accessions + an isoform + a synthetic test accession quote
    # cleanly; the surface is bracketed and escaped.
    assert _quote_accession_list(["P12345", "P12345-2", "Q1"]) == "'P12345', 'P12345-2', 'Q1'"
    # Anything outside the accession shape is a programming error, not silently
    # inlined into the COPY SELECT.
    for bad in ["P1' OR '1'='1", "drop table protein;--", "", "P1 P2"]:
        with pytest.raises(ValueError, match="non-accession"):
            _quote_accession_list([bad])


# ---------------------------------------------------------------------------
# Bit-exactness parity (--with-postgres): the COPY-based ``_load_one_plm`` must
# return byte-identical float32 vectors to the prior ORM ``.all()`` +
# ``emb.to_list()`` path against a REAL ``halfvec`` column. This is the
# load-bearing test for the #660-style COPY optimization: pgvector emits each
# halfvec element as the shortest round-tripping decimal, and parsing it as
# float32 recovers the exact stored half-precision value.
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_load_one_plm_copy_is_byte_identical_to_orm(postgres_url: str) -> None:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    import protea.infrastructure.orm.models  # noqa: F401
    from protea.core.classifier_producer import _load_one_plm
    from protea.infrastructure.orm.base import Base
    from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
    from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding
    from protea.infrastructure.orm.models.protein.protein import Protein
    from protea.infrastructure.orm.models.sequence.sequence import Sequence

    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    dim = 16
    rng = np.random.RandomState(7)
    # A spread of values incl. edge halfs (max half, subnormals, zeros, signs)
    # so the parity check is not a happy-path-only assertion.
    raw_vectors: dict[str, np.ndarray] = {}
    accessions = [f"Q{i:05d}" for i in range(40)]
    accessions.append("P12345-2")  # an isoform-shaped accession
    config_id = uuid.uuid4()

    with Session(engine, future=True) as session:
        config = EmbeddingConfig(
            id=config_id,
            model_name="test/copy-parity",
            model_backend="esm",
            layer_indices=[0],
            layer_agg="mean",
            pooling="mean",
            normalize_residues=False,
            normalize=True,
            max_length=1022,
            use_chunking=False,
            chunk_size=512,
            chunk_overlap=0,
        )
        session.add(config)
        for idx, acc in enumerate(accessions):
            if idx == 0:
                v = np.zeros(dim, np.float16)
            elif idx == 1:
                v = np.array([65504, -65504, 6e-8, -6e-8, 1.0, -1.0, 0.0, 0.5] * 2, np.float16)[
                    :dim
                ]
            else:
                v = (rng.randn(dim) * rng.choice([1e-4, 1.0, 1e3])).astype(np.float16)
            raw_vectors[acc] = v
            seq_text = f"SEQ{idx}"
            seq = Sequence(
                sequence=seq_text,
                sequence_hash=hashlib.md5(seq_text.encode()).hexdigest(),  # noqa: S324
            )
            session.add(seq)
            session.flush()
            session.add(
                Protein(
                    accession=acc,
                    canonical_accession=acc.split("-")[0],
                    is_canonical="-" not in acc,
                    sequence_id=seq.id,
                )
            )
            session.add(
                SequenceEmbedding(
                    sequence_id=seq.id,
                    embedding_config_id=config_id,
                    embedding=v.astype(np.float32).tolist(),
                    embedding_dim=dim,
                )
            )
        session.commit()

    with Session(engine, future=True) as session:
        # Reference: the historical ORM ``.all()`` + ``emb.to_list()`` path.
        orm_rows = (
            session.query(Protein.accession, SequenceEmbedding.embedding)
            .join(Protein.sequence)
            .join(
                SequenceEmbedding,
                (SequenceEmbedding.sequence_id == Protein.sequence_id)
                & (SequenceEmbedding.embedding_config_id == config_id),
            )
            .filter(Protein.accession.in_(accessions))
            .all()
        )
        orm_map = {acc: np.asarray(emb.to_list(), dtype=np.float32) for acc, emb in orm_rows}

        # Optimized COPY path.
        copy_map = _load_one_plm(session, str(config_id), accessions)

    assert set(copy_map) == set(orm_map) == set(accessions)
    for acc in accessions:
        a = orm_map[acc]
        b = copy_map[acc]
        assert a.dtype == b.dtype == np.float32
        # Byte-for-byte equality (compare the raw float32 bit patterns).
        assert a.tobytes() == b.tobytes(), f"halfvec parity mismatch for {acc}: {a} != {b}"
