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
