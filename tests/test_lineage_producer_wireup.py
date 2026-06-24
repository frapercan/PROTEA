"""CI guard: the lineage producer is invoked from the dump pipeline.

The canonical FeatureRegistry maps the four ``lineage_*`` columns to
``protea_method.lineage.compute_lineage_features``. The mapping is
inert until something in the dump pipeline actually calls the
producer; without an explicit invocation the columns never land on
the records and ``_assert_canonical_columns`` in
``parquet_export.consolidate_shards`` blows up with
``missing=['lineage_*']``. This module is the producer-consumer guard
the dump pipeline needs to keep the invariant honest.

The tests exercise the same payload shape that crashed production
(``compute_taxonomy=false, expand_votes_to_ancestors=true``) and assert
that every record emitted by ``_knn_transfer_and_label`` carries the
four canonical lineage feature keys with well-defined float zeros for
queries whose pre-cutoff known-GO set is empty.

The fixture mirrors ``test_knn_streaming_smoke``: synthetic two-query
fixtures, mocked Anc2Vec index, no database. The CI cost is one
runner pass on ~10 records.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest

from protea.core.training_dump_helpers import (
    KnnTransferContext,
    StreamOutput,
    _knn_transfer_and_label,
)

_LINEAGE_COLS = (
    "lineage_is_ancestor_of_known",
    "lineage_is_descendant_of_known",
    "lineage_ancestor_of_count",
    "lineage_descendant_of_count",
)


class _StubAnc2Vec:
    """Drop-in for ``Anc2VecIndex`` (mirrors test_knn_streaming_smoke)."""

    dim = 8

    def batch(self, go_ids: list[str], *, zero_if_missing: bool = True) -> np.ndarray:
        return np.zeros((len(go_ids), self.dim), dtype=np.float32)


def _mk_payload(*, expand: bool, taxonomy: bool) -> SimpleNamespace:
    """Mirror the production payload that crashed: taxonomy off, expand on."""
    return SimpleNamespace(
        limit_per_entry=3,
        distance_threshold=None,
        search_backend="numpy",
        metric="cosine",
        faiss_index_type="flat",
        faiss_nlist=0,
        faiss_nprobe=0,
        compute_alignments=False,
        compute_taxonomy=taxonomy,
        expand_votes_to_ancestors=expand,
    )


def _mk_fixtures(dim: int = 4) -> dict[str, Any]:
    """Two queries, two aspects, three references, six GO terms.

    DAG topology (so the lineage producer has something non-trivial to
    walk when ``known_by_protein`` is populated):

        GO:0000010 (root, BP)
        └── GO:0000001
        └── GO:0000002

        GO:0000020 (root, MF)
        └── GO:0000003
        └── GO:0000004
    """
    rng = np.random.default_rng(42)
    query_emb = rng.normal(size=(2, dim)).astype(np.float32)
    ref_embs = rng.normal(size=(3, dim)).astype(np.float32)
    go_id_map = {
        1: "GO:0000001",
        2: "GO:0000002",
        3: "GO:0000003",
        4: "GO:0000004",
        10: "GO:0000010",
        20: "GO:0000020",
    }
    aspect_map = {1: "P", 2: "P", 10: "P", 3: "F", 4: "F", 20: "F"}
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
        "C": {
            "accessions": [],
            "embeddings": np.empty((0, dim), dtype=np.float16),
            "go_map": {},
        },
    }
    parent_map_str: dict[str, set[str]] = {
        "GO:0000001": {"GO:0000010"},
        "GO:0000002": {"GO:0000010"},
        "GO:0000003": {"GO:0000020"},
        "GO:0000004": {"GO:0000020"},
    }
    return {
        "valid_queries": ["Q1", "Q2"],
        "query_emb": query_emb,
        "ref_by_aspect": ref_by_aspect,
        "go_id_map": go_id_map,
        "aspect_map": aspect_map,
        "gt_pairs": {("Q1", "GO:0000001"), ("Q2", "GO:0000003")},
        "parent_map_str": parent_map_str,
    }


def _run(
    *,
    expand: bool,
    taxonomy: bool = False,
    known: dict[str, set[str]] | None = None,
) -> list[dict[str, Any]]:
    """Drive the runner end-to-end against the synthetic fixtures."""
    fx = _mk_fixtures()
    session = MagicMock()
    p = _mk_payload(expand=expand, taxonomy=taxonomy)
    ctx = KnnTransferContext(
        valid_queries=fx["valid_queries"],
        query_emb=fx["query_emb"],
        ref_by_aspect=fx["ref_by_aspect"],
        go_id_map=fx["go_id_map"],
        aspect_map=fx["aspect_map"],
        gt_pairs=fx["gt_pairs"],
        query_known_gos=known,
        # Always pass the parent map: the lineage producer is canonical
        # and the runtime gate on ``expand_votes_to_ancestors`` lives
        # inside the runner now, not in the context plumbing.
        parent_map_str=fx["parent_map_str"],
        ia_weights=None,
        pca_state=None,
    )
    with patch(
        "protea.core._anc2vec_phases.get_anc2vec_index",
        return_value=_StubAnc2Vec(),
    ):
        out = _knn_transfer_and_label(session, p, ctx)
    assert isinstance(out, list)
    return out


def test_lineage_columns_present_on_every_record_no_known() -> None:
    """Reproduce the crashed payload: taxonomy off, expand on, no known.

    Without the wireup, the four ``lineage_*`` keys are absent and
    the parquet exporter raises the canonical column invariant when
    the dump shards are consolidated. The CI guard pins them
    present with well-defined zeros.
    """
    records = _run(expand=True, taxonomy=False, known=None)
    assert records, "fixture produced no records"
    for rec in records:
        for col in _LINEAGE_COLS:
            assert col in rec, f"record missing {col}: {rec!r}"
            assert rec[col] == 0.0, f"expected 0.0 for {col} (no known), got {rec[col]!r}"


def test_lineage_columns_present_with_known_ancestor_signal() -> None:
    """With known terms in the same DAG branch, lineage flags should fire.

    Q1's known set is {GO:0000010} (root). Any candidate GO whose
    ancestor closure contains GO:0000010 (i.e. GO:0000001 /
    GO:0000002) gets ``lineage_is_descendant_of_known = 1.0``. The
    flag must be present on every record regardless.
    """
    records = _run(
        expand=True,
        taxonomy=False,
        known={"Q1": {"GO:0000010"}},
    )
    for rec in records:
        for col in _LINEAGE_COLS:
            assert col in rec
    # Q1 candidate GO:0000001 (descendant of known GO:0000010) must
    # carry the descendant flag. (The exact set of emitted records
    # depends on KNN topology; we only require the flag is set on the
    # candidate when it appears.)
    q1_desc_signal = [
        r
        for r in records
        if r["protein_accession"] == "Q1" and r["go_id"] in {"GO:0000001", "GO:0000002"}
    ]
    if q1_desc_signal:
        for rec in q1_desc_signal:
            assert rec["lineage_is_descendant_of_known"] == 1.0
            assert rec["lineage_descendant_of_count"] >= 1.0


def test_lineage_present_in_streaming_mode(tmp_path: Any) -> None:
    """Streaming-mode records hit a ParquetWriter; the columns must
    appear in the on-disk shard too.

    This is the closest in-process analogue of the production crash:
    the rows that ``_assert_canonical_columns`` validates come from
    parquet shards on disk, not the in-memory list. If lineage is not
    written into the streamed shard, the boundary invariant fails.
    """
    fx = _mk_fixtures()
    session = MagicMock()
    p = _mk_payload(expand=True, taxonomy=False)
    ctx = KnnTransferContext(
        valid_queries=fx["valid_queries"],
        query_emb=fx["query_emb"],
        ref_by_aspect=fx["ref_by_aspect"],
        go_id_map=fx["go_id_map"],
        aspect_map=fx["aspect_map"],
        gt_pairs=fx["gt_pairs"],
        query_known_gos=None,
        parent_map_str=fx["parent_map_str"],
        ia_weights=None,
        pca_state=None,
    )
    out_path = tmp_path / "stream.parquet"
    with patch(
        "protea.core._anc2vec_phases.get_anc2vec_index",
        return_value=_StubAnc2Vec(),
    ):
        info = _knn_transfer_and_label(
            session,
            p,
            ctx,
            stream_output=StreamOutput(output_parquet=out_path, chunk_rows=3),
        )
    assert isinstance(info, dict) and info["n_rows"] > 0
    streamed = pq.read_table(str(out_path)).to_pylist()
    for rec in streamed:
        for col in _LINEAGE_COLS:
            assert col in rec, f"streamed record missing {col}: {rec!r}"
            assert rec[col] == 0.0


def test_lineage_records_pass_canonical_invariant(tmp_path: Any) -> None:
    """End-to-end: feed the runner's output to the boundary check.

    Builds a shard from the runner's emitted records, fills out the
    remaining canonical features with zero placeholders, and asserts
    ``export_reranker_parquets`` runs through ``_assert_canonical_columns``
    without raising. This is the exact failure mode that crashed
    production (``missing=['lineage_*']``) — passing here means the
    producer wireup is sufficient to satisfy the invariant.
    """
    from protea_contracts import ALL_FEATURES

    from protea.core.parquet_export import (
        ParquetExportContext,
        export_reranker_parquets,
    )

    records = _run(expand=True, taxonomy=False, known=None)
    df = pd.DataFrame(records)
    # Fill any canonical column the runner did not populate (e.g.
    # alignment / taxonomy features when those toggles are off) with
    # zeros so the boundary check only depends on lineage columns
    # being present.
    for col in ALL_FEATURES:
        if col not in df.columns:
            if col in {"qualifier", "evidence_code", "taxonomic_relation"}:
                df[col] = "x"
            else:
                df[col] = 0.0
    # Confirm the columns the producer must emit really did land.
    for col in _LINEAGE_COLS:
        assert col in df.columns, f"runner did not emit {col}"

    train_path = tmp_path / "_train_nk.parquet"
    eval_path = tmp_path / "_eval_nk.parquet"
    df.to_parquet(train_path, index=False)
    df.to_parquet(eval_path, index=False)
    result = export_reranker_parquets(
        ParquetExportContext(
            stage_dir=tmp_path,
            split_files={"nk": [train_path]},
            valid_split_versions=[(220, 221)],
            test_files={"nk": eval_path},
            test_old_v=221,
            test_new_v=222,
            name="lineage-wireup-guard",
            k=3,
            embedding_config_id="00000000-0000-0000-0000-000000000001",
            ontology_snapshot_id="00000000-0000-0000-0000-000000000002",
            annotation_source="goa",
            store=None,
            producer_version="wireup-guard",
            producer_git_sha=None,
            validate_with_contracts=False,
        )
    )
    assert "schema_sha" in result
    assert (tmp_path / "manifest.json").exists()


@pytest.mark.parametrize(
    "known",
    [
        None,
        {"Q1": {"GO:0000010"}},
        {"Q1": {"GO:0000001", "GO:0000010"}, "Q2": {"GO:0000020"}},
        {"Q1": {"GO:0000010", "GO:0000020"}, "Q2": {"GO:0000003", "GO:0000004"}},
    ],
)
def test_lineage_cached_path_byte_identical_to_library(
    known: dict[str, set[str]] | None,
) -> None:
    """Bit-exactness guard for the runner-scoped lineage closure cache.

    The runner replaced the per-(query, aspect) call into
    ``protea_method.lineage.compute_lineage_features`` (which allocates a fresh
    closure cache on every call) with an in-runner reimplementation that reuses
    the library's ``_ancestor_closure`` against a persistent cache. The
    reimplementation must be byte-identical to the library for any input.

    This recomputes the four ``lineage_*`` columns from scratch with the
    UNMODIFIED library function over the runner's own emitted records and
    asserts every value matches exactly (same float, same type).
    """
    from protea_method.lineage import compute_lineage_features

    records = _run(expand=True, taxonomy=False, known=known)
    assert records

    # Group the runner's records by protein and recompute lineage with the
    # stock library function (fresh cache per call, the original behaviour).
    # The runner builds records per-query, so grouping by protein_accession
    # reproduces the original per-query call boundary exactly.
    by_protein: dict[str, list[dict[str, Any]]] = {}
    for rec in records:
        by_protein.setdefault(rec["protein_accession"], []).append(rec)

    for prot, recs in by_protein.items():
        # Clone so the library writes into a separate copy we can diff against.
        clones = [dict(r) for r in recs]
        compute_lineage_features(
            clones,
            parents={
                "GO:0000001": ["GO:0000010"],
                "GO:0000002": ["GO:0000010"],
                "GO:0000003": ["GO:0000020"],
                "GO:0000004": ["GO:0000020"],
            },
            known_by_protein={prot: (known or {}).get(prot, set())},
        )
        for runner_rec, lib_rec in zip(recs, clones, strict=True):
            for col in _LINEAGE_COLS:
                assert runner_rec[col] == lib_rec[col], (
                    f"lineage mismatch for {prot}/{runner_rec['go_id']} {col}: "
                    f"runner={runner_rec[col]!r} library={lib_rec[col]!r}"
                )
                assert type(runner_rec[col]) is type(lib_rec[col])


@pytest.mark.parametrize("expand", [False, True])
def test_lineage_emitted_in_both_expand_modes(expand: bool) -> None:
    """The four columns must land regardless of the expand toggle.

    The invariant is canonical: it does not look at payload flags. If
    a future change skips lineage when expand is off, the dump pipeline
    breaks the same way it did with expand on.
    """
    records = _run(expand=expand, taxonomy=False, known=None)
    assert records
    for rec in records:
        for col in _LINEAGE_COLS:
            assert col in rec, f"expand={expand}: record missing {col!r}: {rec!r}"
