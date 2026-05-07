"""Bundle-loader unit tests for the LAFA container.

Synthesises a tiny frozen-data bundle (parquet + npz + json + booster)
under ``tmp_path`` and exercises each loader plus the score-selection
helper. This pins the bundle schema that
``scripts/export_lafa_bundle.py`` (F-LAFA.1, separate slice) must
produce.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# Make ``apps/lafa_container`` importable for these tests.
_REPO_ROOT = Path(__file__).resolve().parents[1]
_CONTAINER_DIR = _REPO_ROOT / "apps" / "lafa_container"
if str(_CONTAINER_DIR) not in sys.path:
    sys.path.insert(0, str(_CONTAINER_DIR))

from protea_main import (  # noqa: E402
    _load_annotations,
    _load_booster_bytes,
    _load_boosters_by_aspect,
    _load_go_metadata,
    _load_manifest,
    _load_pca_state,
    _load_refs,
    _score_for_output,
    _stack_query_embeddings,
)


def _make_bundle(tmp_path: Path, *, with_booster: bool = True) -> Path:
    bundle = tmp_path / "frozen"
    bundle.mkdir()

    (bundle / "manifest.json").write_text(
        json.dumps(
            {
                "cutoff_version": "v226",
                "cutoff_date": "2025-05-03",
                "feature_schema_sha": "abcdef0123456789",
            }
        )
    )

    refs_table = pa.table(
        {
            "accession": ["R1", "R2", "R3"],
            "embedding": [
                np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32),
                np.array([0.0, 1.0, 0.0, 0.0], dtype=np.float32),
                np.array([0.0, 0.0, 1.0, 0.0], dtype=np.float32),
            ],
        }
    )
    pq.write_table(refs_table, bundle / "reference_embeddings.parquet")

    ann_table = pa.table(
        {
            "accession": ["R1", "R1", "R2", "R3"],
            "go_term_id": [1, 2, 1, 3],
            "go_id": ["GO:0000001", "GO:0000002", "GO:0000001", "GO:0000003"],
        }
    )
    pq.write_table(ann_table, bundle / "reference_annotations.parquet")

    meta_table = pa.table(
        {
            "go_term_id": [1, 2, 3],
            "go_id": ["GO:0000001", "GO:0000002", "GO:0000003"],
            "aspect": ["F", "P", "C"],
        }
    )
    pq.write_table(meta_table, bundle / "go_term_metadata.parquet")

    np.savez(
        bundle / "pca_state.npz",
        mean=np.zeros(4, dtype=np.float32),
        components=np.eye(2, 4, dtype=np.float32),
    )
    np.savez(
        bundle / "anc2vec.npz",
        embeddings=np.eye(3, 4, dtype=np.float32),
        go_ids=np.array(["GO:0000001", "GO:0000002", "GO:0000003"], dtype=object),
        ontology_release="bundle-test",
    )

    if with_booster:
        rng = np.random.default_rng(0)
        x = rng.random((50, 3)).astype(np.float32)
        y = rng.integers(0, 2, 50)
        train = lgb.Dataset(x, label=y)
        booster = lgb.train(
            {"objective": "binary", "verbose": -1, "num_leaves": 7},
            train,
            num_boost_round=5,
        )
        (bundle / "reranker.txt").write_bytes(booster.model_to_string().encode("utf-8"))

    return bundle


def test_load_manifest(tmp_path: Path) -> None:
    bundle = _make_bundle(tmp_path)
    manifest = _load_manifest(bundle)
    assert manifest["cutoff_version"] == "v226"
    assert manifest["feature_schema_sha"].startswith("abc")


def test_load_refs_shapes(tmp_path: Path) -> None:
    bundle = _make_bundle(tmp_path)
    accs, embeddings = _load_refs(bundle)
    assert accs == ["R1", "R2", "R3"]
    assert embeddings.shape == (3, 4)
    assert embeddings.dtype == np.float32


def test_load_annotations_groups_by_accession(tmp_path: Path) -> None:
    bundle = _make_bundle(tmp_path)
    ann = _load_annotations(bundle)
    assert sorted(ann.keys()) == ["R1", "R2", "R3"]
    assert len(ann["R1"]) == 2
    assert ann["R3"][0]["go_term_id"] == 3


def test_load_go_metadata(tmp_path: Path) -> None:
    bundle = _make_bundle(tmp_path)
    go_id_map, go_aspect_map = _load_go_metadata(bundle)
    assert go_id_map[1] == "GO:0000001"
    assert go_aspect_map[2] == "P"


def test_load_pca_state(tmp_path: Path) -> None:
    bundle = _make_bundle(tmp_path)
    state = _load_pca_state(bundle)
    assert state is not None
    mean, components = state
    assert mean.shape == (4,)
    assert components.shape == (2, 4)


def test_load_pca_state_returns_none_when_missing(tmp_path: Path) -> None:
    bundle = _make_bundle(tmp_path)
    (bundle / "pca_state.npz").unlink()
    assert _load_pca_state(bundle) is None


def test_load_booster_bytes(tmp_path: Path) -> None:
    bundle = _make_bundle(tmp_path, with_booster=True)
    blob = _load_booster_bytes(bundle)
    assert blob is not None
    assert b"tree" in blob  # LightGBM text format header


def test_load_booster_bytes_returns_none_when_missing(tmp_path: Path) -> None:
    bundle = _make_bundle(tmp_path, with_booster=False)
    assert _load_booster_bytes(bundle) is None


def test_stack_query_embeddings_drops_unknown(tmp_path: Path) -> None:
    embeddings = {"Q1": np.array([1.0], dtype=np.float32)}
    matrix, kept = _stack_query_embeddings(embeddings, ["Q1", "Q_missing"])
    assert kept == ["Q1"]
    assert matrix.shape == (1, 1)


def test_stack_query_embeddings_empty_input() -> None:
    matrix, kept = _stack_query_embeddings({}, ["Q1"])
    assert matrix.size == 0
    assert kept == []


def test_score_for_output_prefers_reranker_score() -> None:
    pred = {"reranker_score": 0.73, "min_distance": 0.1, "distance": 0.1}
    assert _score_for_output(pred) == pytest.approx(0.73)


def test_score_for_output_falls_back_to_distance() -> None:
    pred = {"min_distance": 0.25, "distance": 0.25}
    assert _score_for_output(pred) == pytest.approx(0.75)


def test_score_for_output_clamps_to_zero() -> None:
    pred = {"min_distance": 1.5, "distance": 1.5}
    assert _score_for_output(pred) == pytest.approx(0.0)


def test_load_boosters_by_aspect_empty_when_dir_missing(tmp_path: Path) -> None:
    bundle = _make_bundle(tmp_path)
    assert _load_boosters_by_aspect(bundle) == {}


def test_load_boosters_by_aspect_reads_present_files(tmp_path: Path) -> None:
    bundle = _make_bundle(tmp_path, with_booster=False)
    reranker_dir = bundle / "reranker"
    reranker_dir.mkdir()
    rng = np.random.default_rng(0)
    x = rng.random((40, 3)).astype(np.float32)
    y = rng.integers(0, 2, 40)
    train = lgb.Dataset(x, label=y)
    booster = lgb.train(
        {"objective": "binary", "verbose": -1, "num_leaves": 7},
        train,
        num_boost_round=3,
    )
    blob = booster.model_to_string().encode("utf-8")
    (reranker_dir / "F.txt").write_bytes(blob)
    (reranker_dir / "C.txt").write_bytes(blob)
    # P missing on purpose to test partial coverage.
    out = _load_boosters_by_aspect(bundle)
    assert sorted(out) == ["C", "F"]
    assert out["F"] == blob
    assert out["C"] == blob
