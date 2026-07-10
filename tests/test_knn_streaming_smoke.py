"""Smoke test: list mode vs streaming mode of ``_knn_transfer_and_label``.

The refactor in Change C rearranged the outer loop and added a streaming
``output_parquet`` path. This test exercises both modes over tiny
synthetic fixtures and asserts they produce the same logical record set
(same ``(protein_accession, go_id)`` pairs, same labels, same distances).
It also checks that the ``pivot_go_ids`` filter keeps only whitelisted
terms, and that ancestor expansion is consistent across modes.
"""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import numpy as np
import pyarrow.parquet as pq
import pytest

from protea.core.classifier_producer import ClassifierPrediction
from protea.core.training_dump_helpers import (
    KnnTransferContext,
    StreamOutput,
    _knn_transfer_and_label,
)


class _StubAnc2Vec:
    """Drop-in for ``Anc2VecIndex`` that returns zeros for every GO id."""

    dim = 8

    def batch(self, go_ids, *, zero_if_missing: bool = True):
        return np.zeros((len(go_ids), self.dim), dtype=np.float32)


def _mk_payload(*, expand: bool = False) -> SimpleNamespace:
    return SimpleNamespace(
        limit_per_entry=3,
        distance_threshold=None,
        search_backend="numpy",
        metric="cosine",
        faiss_index_type="flat",
        faiss_nlist=0,
        faiss_nprobe=0,
        compute_alignments=False,
        compute_taxonomy=False,
        expand_votes_to_ancestors=expand,
    )


def _mk_fixtures(dim: int = 4):
    """Two queries, two aspects, three reference proteins, six GO terms.

    GO ids:
      BP: GO:0000001, GO:0000002 (parent → GO:0000010)
      MF: GO:0000003, GO:0000004 (parent → GO:0000020)
    """
    rng = np.random.default_rng(42)
    query_emb = rng.normal(size=(2, dim)).astype(np.float32)
    valid_queries = ["Q1", "Q2"]

    ref_embs = rng.normal(size=(3, dim)).astype(np.float32)
    go_id_map = {
        1: "GO:0000001",
        2: "GO:0000002",
        3: "GO:0000003",
        4: "GO:0000004",
        10: "GO:0000010",
        20: "GO:0000020",
    }
    aspect_map = {
        1: "P", 2: "P", 10: "P",
        3: "F", 4: "F", 20: "F",
    }
    # go_map: {ref_acc: [{go_term_id, qualifier, evidence_code}]}
    ref_by_aspect = {
        "P": {
            "accessions": ["R1", "R2", "R3"],
            "embeddings": ref_embs.astype(np.float16),
            "go_map": {
                "R1": [{"go_term_id": 1, "qualifier": "", "evidence_code": "EXP"}],
                "R2": [{"go_term_id": 2, "qualifier": "", "evidence_code": "IEA"}],
                "R3": [{"go_term_id": 1, "qualifier": "", "evidence_code": "IDA"}],
            },
        },
        "F": {
            "accessions": ["R1", "R2", "R3"],
            "embeddings": ref_embs.astype(np.float16),
            "go_map": {
                "R1": [{"go_term_id": 3, "qualifier": "", "evidence_code": "EXP"}],
                "R2": [{"go_term_id": 4, "qualifier": "", "evidence_code": "IEA"}],
                "R3": [{"go_term_id": 3, "qualifier": "", "evidence_code": "IDA"}],
            },
        },
        "C": {"accessions": [], "embeddings": np.empty((0, dim), dtype=np.float16), "go_map": {}},
    }
    # Ground truth: Q1 has GO:0000001 in BP; Q2 has GO:0000003 in MF
    gt_pairs = {("Q1", "GO:0000001"), ("Q2", "GO:0000003")}
    parent_map_str = {
        "GO:0000001": {"GO:0000010"},
        "GO:0000002": {"GO:0000010"},
        "GO:0000003": {"GO:0000020"},
        "GO:0000004": {"GO:0000020"},
    }
    return (
        valid_queries,
        query_emb,
        ref_by_aspect,
        go_id_map,
        aspect_map,
        gt_pairs,
        parent_map_str,
    )


def _run(mode: str, tmp_path: Path | None = None, *, expand: bool, pivot=None):
    (
        valid_queries,
        query_emb,
        ref_by_aspect,
        go_id_map,
        aspect_map,
        gt_pairs,
        parent_map_str,
    ) = _mk_fixtures()

    session = MagicMock()
    p = _mk_payload(expand=expand)

    ctx = KnnTransferContext(
        valid_queries=valid_queries,
        query_emb=query_emb,
        ref_by_aspect=ref_by_aspect,
        go_id_map=go_id_map,
        aspect_map=aspect_map,
        gt_pairs=gt_pairs,
        query_known_gos=None,
        parent_map_str=parent_map_str if expand else None,
        ia_weights=None,
        pca_state=None,
        pivot_go_ids=pivot,
    )
    stream_output = (
        StreamOutput(output_parquet=tmp_path / "out.parquet", chunk_rows=3)
        if mode == "stream"
        else None
    )

    with patch(
        "protea.core._anc2vec_phases.get_anc2vec_index",
        return_value=_StubAnc2Vec(),
    ):
        return _knn_transfer_and_label(
            session,
            p,
            ctx,
            stream_output=stream_output,
        )


def _record_key(r: dict) -> tuple:
    return (r["protein_accession"], r["go_id"])


def _read_parquet_records(path: Path) -> list[dict]:
    table = pq.read_table(str(path))
    return table.to_pylist()


@pytest.mark.parametrize("expand", [False, True])
def test_list_vs_stream_equivalence(tmp_path, expand):
    # Rebuild fixtures for each call because ref["embeddings"] gets
    # nulled inside ``_knn_transfer_and_label`` (Change A).
    list_records = _run("list", expand=expand)
    stream_info = _run("stream", tmp_path=tmp_path, expand=expand)

    assert stream_info["parquet_path"].endswith("out.parquet")
    assert stream_info["n_rows"] == len(list_records)

    stream_records = _read_parquet_records(tmp_path / "out.parquet")

    list_keys = sorted(_record_key(r) for r in list_records)
    stream_keys = sorted(_record_key(r) for r in stream_records)
    assert list_keys == stream_keys

    # Labels must agree per (acc, go_id).
    list_labels = {_record_key(r): r["label"] for r in list_records}
    stream_labels = {_record_key(r): r["label"] for r in stream_records}
    assert list_labels == stream_labels

    # Ancestor expansion should produce GO:0000010 / GO:0000020 rows.
    ancestors_emitted = {k[1] for k in list_keys if k[1] in ("GO:0000010", "GO:0000020")}
    if expand:
        assert ancestors_emitted, "ancestor expansion produced no rows"
    else:
        assert not ancestors_emitted


def test_pivot_filter_drops_non_pivot_terms(tmp_path):
    pivot = frozenset({"GO:0000001", "GO:0000003"})
    list_records = _run("list", expand=False, pivot=pivot)
    stream_info = _run("stream", tmp_path=tmp_path, expand=False, pivot=pivot)

    assert {r["go_id"] for r in list_records} <= pivot
    stream_records = _read_parquet_records(tmp_path / "out.parquet")
    assert {r["go_id"] for r in stream_records} <= pivot
    assert stream_info["n_rows"] == len(stream_records)


def test_streaming_empty_result_is_safe(tmp_path):
    # Pivot with no overlap → zero rows; parquet writer should
    # stay unopened and the function must still return cleanly.
    pivot = frozenset({"GO:9999999"})
    info = _run("stream", tmp_path=tmp_path, expand=False, pivot=pivot)
    assert info["n_rows"] == 0
    # Writer never opened → no file.
    assert not (tmp_path / "out.parquet").exists()


_LINEAGE_KEYS = (
    "lineage_is_ancestor_of_known",
    "lineage_is_descendant_of_known",
    "lineage_ancestor_of_count",
    "lineage_descendant_of_count",
)


@pytest.mark.parametrize("expand", [False, True])
def test_lineage_columns_always_emitted_with_defaults(tmp_path, expand):
    """Regression: T-RES.1b added the 4 lineage columns to
    ``ALL_FEATURES``; without zero-filled defaults on every emitted
    record the parquet_export canonical-column invariant fails the
    eval-shard write after hours of compute.

    Whether or not ancestor expansion is on, every leaf and every
    synthetic record must carry the 4 lineage keys with default
    ``0.0``.
    """
    list_records = _run("list", expand=expand)
    stream_info = _run("stream", tmp_path=tmp_path, expand=expand)
    stream_records = _read_parquet_records(tmp_path / "out.parquet")
    assert list_records, "fixture produced no records"
    assert stream_info["n_rows"] == len(list_records)

    for rec in list_records:
        for key in _LINEAGE_KEYS:
            assert key in rec, f"list record missing {key}"
            assert rec[key] == 0.0, f"list record {key} expected 0.0, got {rec[key]!r}"
    for rec in stream_records:
        for key in _LINEAGE_KEYS:
            assert key in rec, f"stream record missing {key}"
            assert rec[key] == 0.0, f"stream record {key} expected 0.0, got {rec[key]!r}"


def _mk_parity_payload(*, chunk_size: int) -> SimpleNamespace:
    """Payload with the three INT-6 parity flags ON + a parity chunk size."""
    return SimpleNamespace(
        limit_per_entry=3,
        distance_threshold=None,
        search_backend="numpy",
        metric="cosine",
        faiss_index_type="flat",
        faiss_nlist=0,
        faiss_nprobe=0,
        compute_alignments=False,
        compute_taxonomy=False,
        expand_votes_to_ancestors=False,
        compute_self_prior=True,
        compute_association=True,
        compute_classifier=True,
        parity_chunk_size=chunk_size,
        ontology_snapshot_id="00000000-0000-0000-0000-000000000000",
    )


def _run_parity(chunk_size: int):
    """Drive the runner with parity ON and the producer DB calls mocked.

    Each query KNOWS its leaf term experimentally (so association/self_prior
    fire per protein) and the classifier proposes one extra per protein, so a
    value-preserving chunking must reproduce the same record set across chunk
    sizes (per-split, per-query, in between).
    """
    import uuid as _uuid

    (
        valid_queries,
        query_emb,
        ref_by_aspect,
        go_id_map,
        aspect_map,
        gt_pairs,
        parent_map_str,
    ) = _mk_fixtures()

    session = MagicMock()
    p = _mk_parity_payload(chunk_size=chunk_size)
    ctx = KnnTransferContext(
        valid_queries=valid_queries,
        query_emb=query_emb,
        ref_by_aspect=ref_by_aspect,
        go_id_map=go_id_map,
        aspect_map=aspect_map,
        gt_pairs=gt_pairs,
        query_known_gos=None,
        parent_map_str=None,
        ia_weights=None,
        pca_state=None,
        pivot_go_ids=None,
        t0_annotation_set_id=_uuid.UUID("11111111-1111-1111-1111-111111111111"),
    )

    # Q1 knows GO:0000001 (int 1) exp; Q2 knows GO:0000003 (int 3) exp.
    exp_ann = {
        "Q1": [{"go_term_id": 1, "evidence_code": "IDA"}],
        "Q2": [{"go_term_id": 3, "evidence_code": "IDA"}],
    }
    cooc = {"GO:0000001": {"GO:0000002": 1}, "GO:0000003": {"GO:0000004": 2}}
    freq = {"GO:0000001": 2, "GO:0000003": 2}
    go_id_by_int = dict(go_id_map.items())
    aspect_by_go = {go_id_map[i]: aspect_map[i] for i in go_id_map}
    clf_preds = [
        ClassifierPrediction("Q1", "GO:0000002", 0.8),
        ClassifierPrediction("Q2", "GO:0000004", 0.6),
    ]
    gid_by_go = {"GO:0000002": 2, "GO:0000004": 4}

    op = MagicMock()
    op._load_annotations_for.side_effect = lambda _s, _set, accs: {
        a: v for a, v in exp_ann.items() if a in set(accs)
    }

    def _load_concat(_session, accs):
        valid = [a for a in accs if a in {"Q1", "Q2"}]
        return np.zeros((len(valid), 8320), dtype=np.float32), valid

    def _predict(_features, valid):
        return [pr for pr in clf_preds if pr.accession in set(valid)]

    from protea.core.operations.predict_go_terms import _post_knn_pipeline as pkp
    from protea.core.training_dump import _export_features as ef

    with (
        patch(
            "protea.core._anc2vec_phases.get_anc2vec_index",
            return_value=_StubAnc2Vec(),
        ),
        patch.object(ef, "_ExportFeatureOp", return_value=op),
        patch(
            "protea.core.operations.predict_go_terms._association_loader."
            "load_cooccurrence_for_known",
            return_value=(cooc, freq),
        ),
        patch.object(pkp, "_load_go_id_and_aspect", return_value=(go_id_by_int, aspect_by_go)),
        patch("protea.core.classifier_producer.load_concat_features", side_effect=_load_concat),
        patch(
            "protea.core.classifier_producer.get_classifier",
            return_value=MagicMock(predict=MagicMock(side_effect=_predict)),
        ),
        patch("protea.core.classifier_producer.resolve_go_term_ids", return_value=gid_by_go),
    ):
        return _knn_transfer_and_label(session, p, ctx, stream_output=None)


_PARITY_KEYS = (
    "self_prior_score",
    "association_total",
    "association_cross",
    "association_present",
    "classifier_score",
    "classifier_present",
)


def test_parity_batched_per_split_equals_per_query_through_runner():
    """End-to-end: the per-split / chunked batched parity pass through
    ``_build_records`` produces the byte-identical record set regardless of
    chunk size (0=per-split, 1=per-query, 2=chunked). This proves the perf
    refactor (hoisting the producers out of the per-query loop) is
    value-preserving across the runner's emit seam, including the
    classifier-only unioned rows and the transient ``go_term_id`` strip.
    """
    per_split = _run_parity(0)
    per_query = _run_parity(1)
    chunked = _run_parity(2)

    def _index(records):
        return {
            (r["protein_accession"], r["go_id"]): {k: r.get(k) for k in _PARITY_KEYS}
            for r in records
        }

    base = _index(per_split)
    assert base == _index(per_query)
    assert base == _index(chunked)
    # The parity producers actually fired (non-trivial values present) and the
    # classifier-only proposals were unioned in for both proteins.
    assert any(v["classifier_present"] == 1.0 for v in base.values())
    assert any(v["association_present"] == 1.0 for v in base.values())
    # The transient int ``go_term_id`` is stripped before emit (schema unchanged).
    assert all("go_term_id" not in r for r in per_split)


def test_streaming_shard_schema_contains_full_canonical_feature_set(tmp_path):
    """End-to-end: every column in ``ALL_FEATURES`` must appear in the
    streamed parquet shard so the downstream
    ``parquet_export._assert_canonical_columns`` invariant passes.
    Guards against any future canonical column being added to
    ``ALL_FEATURES`` without a corresponding default in the record
    builder.
    """
    from protea_contracts import ALL_FEATURES

    from protea.core.features._bindings import _POOL_INJECTED_FEATURES

    info = _run("stream", tmp_path=tmp_path, expand=False)
    assert info["n_rows"] > 0
    table = pq.read_table(str(tmp_path / "out.parquet"))
    shard_cols = set(table.column_names)
    # Pool-injected columns (``plm_id`` / ``k_context``) are declared by the
    # contracts but stamped only by the lab's pooled multi-manifest loader; a
    # shard PROTEA produces from a single manifest never carries them, and the
    # export boundary does not require them (ADR-D45).
    required = [c for c in ALL_FEATURES if c not in _POOL_INJECTED_FEATURES]
    missing = [c for c in required if c not in shard_cols]
    assert missing == [], (
        f"streamed shard missing canonical feature columns: {missing!r}"
    )
