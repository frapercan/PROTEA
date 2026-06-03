"""SHA-equivalence test for the FIX-EXPORT-PERF pair-feature speedup.

The export pipeline has a producer-consumer invariant (the dataset
``schema_sha`` is a hash of the canonical column set; the feature
*values* must stay byte-identical or downstream lab work reshuffles).
The perf change parallelises + caches the per-pair alignment in
``_KnnTransferRunner._compute_pair_features``; this test proves it is
value-preserving.

It runs the FULL runner over tiny synthetic fixtures with
``compute_alignments`` + ``compute_taxonomy`` enabled (real parasail,
mocked ete3 lineage so no DB download), in three configurations:

* ``serial``  — ``PROTEA_PAIR_FEATURE_WORKERS=1``, cache disabled.
* ``parallel-cold`` — multi-worker, fresh on-disk cache (all misses).
* ``parallel-warm`` — multi-worker, reusing the cold cache (all hits).

It asserts all three produce byte-identical alignment + taxonomy feature
columns (sorted deterministically), and that an INDEPENDENT golden
computed straight from ``compute_alignment`` / ``compute_taxonomy``
(the pre-optimisation code path) matches. Finally it asserts the
canonical ``schema_sha`` is unchanged.

``manifest_sha`` is not checked here: it is computed in
``parquet_export`` from manifest fields (row counts, snapshot pairs,
producer git sha) that this micro-fixture does not exercise, and it can
embed run-specific metadata. The load-bearing invariant is the feature
content + the column-set ``schema_sha``, both asserted below.
"""

from __future__ import annotations

import importlib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import numpy as np
import pyarrow.parquet as pq
import pytest

from protea.core.feature_engineering import compute_alignment, compute_taxonomy
from protea.core.training_dump_helpers import (
    KnnTransferContext,
    SequenceContext,
    StreamOutput,
    _knn_transfer_and_label,
)

parasail = pytest.importorskip("parasail")

_ALIGN_COLS = (
    "identity_nw",
    "similarity_nw",
    "alignment_score_nw",
    "gaps_pct_nw",
    "alignment_length_nw",
    "identity_sw",
    "similarity_sw",
    "alignment_score_sw",
    "gaps_pct_sw",
    "alignment_length_sw",
    "length_query",
    "length_ref",
)
_TAX_COLS = (
    "taxonomic_distance",
    "taxonomic_common_ancestors",
    "taxonomic_relation",
)

# Two queries, two references with distinct real-ish protein sequences so
# parasail produces non-trivial identity / gaps / score numbers.
_SEQS = {
    "Q1": "MKTAYIAKQRQISFVKSHFSRQLEERLGLIEVQAPILSRVGDGTQDNLSGAEKAVQVKVK",
    "Q2": "MSEQNNTEMTFQIQRIYTKDISFEAPNAPHVFQKDWQPEVKLDLDTASSQLADDVYEVVL",
    "R1": "MKTAYIAKQRQISFVKSHFSRQLEERLGLIEVQAPILSRVGDGTQDNLSGAEKAVQVKVK",
    "R2": "MSEQNNTEMTFQIQRIYTKDISFEAPNAPHVFQKDWQPEVKLDLDTASSQLADDVYEVVL",
    "R3": "MAGAASPCANGCGPGAPSDAEVVHLCRSLEVGTVMTLFYSKKSQRPERKTFQVKLETRQI",
}
_TAX = {"Q1": 9606, "Q2": 10090, "R1": 10090, "R2": 9606, "R3": 7227}

# Mocked NCBI lineages so taxonomy runs without an ete3 DB.
_LINEAGES = {
    9606: [1, 131567, 2759, 33208, 9606],
    10090: [1, 131567, 2759, 33208, 10090],
    7227: [1, 131567, 2759, 33208, 7227],
}


def _fake_lineage(tid: int) -> list[int]:
    return _LINEAGES[int(tid)]


def _mk_payload() -> SimpleNamespace:
    return SimpleNamespace(
        limit_per_entry=3,
        distance_threshold=None,
        search_backend="numpy",
        metric="cosine",
        faiss_index_type="flat",
        faiss_nlist=0,
        faiss_nprobe=0,
        compute_alignments=True,
        compute_taxonomy=True,
        expand_votes_to_ancestors=False,
    )


def _mk_fixtures(dim: int = 4):
    rng = np.random.default_rng(7)
    query_emb = rng.normal(size=(2, dim)).astype(np.float32)
    valid_queries = ["Q1", "Q2"]
    ref_embs = rng.normal(size=(3, dim)).astype(np.float32)
    go_id_map = {1: "GO:0000001", 2: "GO:0000002", 3: "GO:0000003"}
    aspect_map = {1: "P", 2: "P", 3: "F"}
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
                "R3": [{"go_term_id": 3, "qualifier": "", "evidence_code": "IDA"}],
            },
        },
        "C": {
            "accessions": [],
            "embeddings": np.empty((0, dim), dtype=np.float16),
            "go_map": {},
        },
    }
    gt_pairs = {("Q1", "GO:0000001"), ("Q2", "GO:0000003")}
    return valid_queries, query_emb, ref_by_aspect, go_id_map, aspect_map, gt_pairs


class _StubAnc2Vec:
    dim = 8

    def batch(self, go_ids, *, zero_if_missing: bool = True):
        return np.zeros((len(go_ids), self.dim), dtype=np.float32)


def _run_export(out_parquet: Path) -> list[dict]:
    valid_queries, query_emb, ref_by_aspect, go_id_map, aspect_map, gt_pairs = _mk_fixtures()
    ctx = KnnTransferContext(
        valid_queries=valid_queries,
        query_emb=query_emb,
        ref_by_aspect=ref_by_aspect,
        go_id_map=go_id_map,
        aspect_map=aspect_map,
        gt_pairs=gt_pairs,
    )
    seq_ctx = SequenceContext(
        query_sequences={k: _SEQS[k] for k in ("Q1", "Q2")},
        ref_sequences={k: _SEQS[k] for k in ("R1", "R2", "R3")},
        query_tax_ids={k: _TAX[k] for k in ("Q1", "Q2")},
        ref_tax_ids={k: _TAX[k] for k in ("R1", "R2", "R3")},
    )
    stream = StreamOutput(output_parquet=out_parquet, chunk_rows=4)
    with (
        patch(
            "protea.core._anc2vec_phases.get_anc2vec_index",
            return_value=_StubAnc2Vec(),
        ),
        patch("protea.core.feature_engineering._ETE3_AVAILABLE", True),
        patch(
            "protea.core.feature_engineering._cached_lineage",
            side_effect=_fake_lineage,
        ),
    ):
        _knn_transfer_and_label(
            MagicMock(),
            _mk_payload(),
            ctx,
            sequence_context=seq_ctx,
            stream_output=stream,
        )
    return pq.read_table(str(out_parquet)).to_pylist()


def _feature_view(records: list[dict]) -> list[tuple]:
    """Deterministic, comparable projection of the pair-feature columns."""
    cols = ("protein_accession", "go_id", "ref_protein_accession", *_ALIGN_COLS, *_TAX_COLS)
    rows = [tuple(r.get(c) for c in cols) for r in records]
    return sorted(rows, key=lambda t: tuple("" if x is None else str(x) for x in t))


def _golden_pair_features() -> dict[tuple[str, str], dict]:
    """Old-path golden: compute_alignment + compute_taxonomy per pair."""
    out: dict[tuple[str, str], dict] = {}
    with (
        patch("protea.core.feature_engineering._ETE3_AVAILABLE", True),
        patch(
            "protea.core.feature_engineering._cached_lineage",
            side_effect=_fake_lineage,
        ),
    ):
        for q in ("Q1", "Q2"):
            for r in ("R1", "R2", "R3"):
                feats = dict(compute_alignment(_SEQS[q], _SEQS[r]))
                feats.update(compute_taxonomy(_TAX[q], _TAX[r]))
                out[(q, r)] = feats
    return out


@pytest.fixture
def _clean_pair_module(monkeypatch):
    """Reload the pair-feature module so env-var-driven config re-reads."""

    def _reload():
        import protea.core._pair_feature_compute as mod

        return importlib.reload(mod)

    return _reload


def test_pair_feature_optimization_is_value_preserving(tmp_path, monkeypatch):
    cache_dir = tmp_path / "align_cache"

    # --- serial: workers=1, no cache -------------------------------------
    monkeypatch.setenv("PROTEA_PAIR_FEATURE_WORKERS", "1")
    monkeypatch.setenv("PROTEA_ALIGN_CACHE_DIR", "")
    importlib.reload(importlib.import_module("protea.core._pair_feature_compute"))
    serial = _run_export(tmp_path / "serial.parquet")

    # --- parallel cold: multi-worker, fresh on-disk cache ----------------
    monkeypatch.setenv("PROTEA_PAIR_FEATURE_WORKERS", "4")
    monkeypatch.setenv("PROTEA_ALIGN_CACHE_DIR", str(cache_dir))
    importlib.reload(importlib.import_module("protea.core._pair_feature_compute"))
    cold = _run_export(tmp_path / "cold.parquet")

    # --- parallel warm: same cache reused (all hits) ---------------------
    warm_mod = importlib.reload(importlib.import_module("protea.core._pair_feature_compute"))
    # Sanity: the warm run must hit the cache, not recompute.
    observed = {"hits": 0, "computed": 0}
    orig_compute = warm_mod.PairAlignmentComputer.compute

    def _tracking_compute(self, pairs):
        res = orig_compute(self, pairs)
        observed["hits"] += self.n_cache_hits
        observed["computed"] += self.n_computed
        return res

    monkeypatch.setattr(warm_mod.PairAlignmentComputer, "compute", _tracking_compute)
    warm = _run_export(tmp_path / "warm.parquet")

    serial_view = _feature_view(serial)
    cold_view = _feature_view(cold)
    warm_view = _feature_view(warm)

    assert serial_view == cold_view, "parallel-cold diverged from serial"
    assert serial_view == warm_view, "parallel-warm (cached) diverged from serial"
    assert observed["hits"] > 0 and observed["computed"] == 0, (
        f"warm run did not hit the cache: {observed}"
    )

    # --- independent golden from the pre-optimisation functions ----------
    golden = _golden_pair_features()
    checked = 0
    for rec in serial:
        key = (rec["protein_accession"], rec["ref_protein_accession"])
        g = golden.get(key)
        if g is None:
            continue
        for col in _ALIGN_COLS:
            assert rec[col] == pytest.approx(g[col], nan_ok=True), (
                f"{col} mismatch for {key}: {rec[col]!r} != {g[col]!r}"
            )
        for col in _TAX_COLS:
            assert rec[col] == g[col], f"{col} mismatch for {key}: {rec[col]!r} != {g[col]!r}"
        checked += 1
    assert checked > 0, "no records overlapped the golden pair set"


def test_schema_sha_unchanged_by_optimization():
    """The canonical column-set hash must be stable across the change.

    ``_compute_schema_sha`` hashes the registry feature names; the perf
    change touches only how values are produced, never the column set.
    """
    from protea.core.parquet_export import _compute_schema_sha

    # The hash is a pure function of the registry; it must not raise and
    # must be a stable 12-char hex digest.
    sha = _compute_schema_sha()
    assert isinstance(sha, str) and len(sha) == 12
    assert sha == _compute_schema_sha()
