"""Smoke tests for the protea-knn-8plm LAFA submission container.

The container itself is built in CI by
``.github/workflows/lafa-knn-8plm-container.yml``; these tests guard
the build inputs (Dockerfile, entrypoint, workflow) and the
ensemble-driver helpers without launching a docker daemon and
without loading any PLM weights.

Coverage:

* Dockerfile extends ``protea-method-runtime`` and installs the ESM
  SDK needed by the ESM-C path,
* entrypoint script is executable, parses with POSIX ``sh -n``, and
  surfaces stable exit codes on missing bind mounts,
* workflow file is valid YAML and publishes to the expected GHCR path,
* the PLM registry exposes eight canonical entries with HF / SDK
  checkpoints,
* the ensemble driver's bundle loaders, score conversion, query
  stacker, and TSV writer behave on a synthetic bundle,
* the ``mean`` and ``max`` aggregation strategies combine candidates
  correctly (including the "divide by full ensemble size" calibration
  documented in METHOD_CARD.md).
"""

from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess
import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

_REPO_ROOT = Path(__file__).resolve().parents[1]
_CONTAINER_DIR = _REPO_ROOT / "apps" / "lafa_knn_8plm"
_DOCKERFILE = _CONTAINER_DIR / "Dockerfile"
_ENTRYPOINT = _CONTAINER_DIR / "lafa_entrypoint.sh"
_README = _CONTAINER_DIR / "README.md"
_METHOD_CARD = _CONTAINER_DIR / "METHOD_CARD.md"
_WORKFLOW = _REPO_ROOT / ".github" / "workflows" / "lafa-knn-8plm-container.yml"

# Make ``apps.lafa_knn_8plm`` importable by inserting the repo root.
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# Imports below are intentionally inside the helper to keep test
# discovery cheap when the optional pyarrow / lightgbm graph fails.
from apps.lafa_knn_8plm import plm_encoders  # noqa: E402
from apps.lafa_knn_8plm.ensemble_driver import (  # noqa: E402
    AGGREGATIONS,
    EnsembleConfig,
    aggregate_predictions,
    build_arg_parser,
    load_annotations,
    load_go_metadata,
    load_manifest,
    load_plm_refs,
    stack_queries,
    write_predictions_tsv,
)

# --- container build inputs ------------------------------------------


def test_container_dir_layout() -> None:
    assert _DOCKERFILE.is_file()
    assert _ENTRYPOINT.is_file()
    assert _README.is_file()
    assert _METHOD_CARD.is_file()
    assert (_CONTAINER_DIR / "__init__.py").is_file()
    assert (_CONTAINER_DIR / "ensemble_driver.py").is_file()
    assert (_CONTAINER_DIR / "plm_encoders.py").is_file()


def test_dockerfile_extends_method_runtime() -> None:
    text = _DOCKERFILE.read_text()
    assert "ARG METHOD_RUNTIME_REF=" in text
    assert "FROM ghcr.io/frapercan/protea/method-runtime:" in text
    assert "COPY apps/lafa_knn_8plm/ensemble_driver.py" in text
    assert "COPY apps/lafa_knn_8plm/plm_encoders.py" in text
    assert "COPY apps/lafa_knn_8plm/lafa_entrypoint.sh" in text
    assert 'ENTRYPOINT ["/app/lafa_entrypoint.sh"]' in text


def test_dockerfile_installs_esm_sdk() -> None:
    text = _DOCKERFILE.read_text()
    # ESM-C path needs the EvolutionaryScale SDK; everything else is
    # covered by the base image's transformers install.
    assert '"esm>=3.0.0,<4.0.0"' in text


def test_dockerfile_creates_lafa_mountpoints() -> None:
    text = _DOCKERFILE.read_text()
    for mount in ("/input", "/output", "/bundle", "/hf-cache"):
        assert mount in text, f"missing mount point {mount} in Dockerfile"


def test_dockerfile_carries_lafa_labels() -> None:
    text = _DOCKERFILE.read_text()
    assert 'org.opencontainers.image.title="protea-knn-8plm"' in text
    assert 'net.functionbench.lafa.method="protea-knn-8plm"' in text
    assert 'net.functionbench.lafa.adr="D23"' in text


def test_entrypoint_is_executable() -> None:
    mode = _ENTRYPOINT.stat().st_mode
    assert mode & stat.S_IXUSR, "entrypoint script must be user-executable"


def test_entrypoint_parses_under_posix_sh() -> None:
    sh = shutil.which("sh")
    if sh is None:
        pytest.skip("POSIX sh not available in this environment")
    result = subprocess.run(
        [sh, "-n", str(_ENTRYPOINT)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, f"sh -n failed: {result.stderr}"


def test_entrypoint_invokes_ensemble_driver() -> None:
    text = _ENTRYPOINT.read_text()
    # Driver is invoked via -m to honour the apps/ package layout.
    assert "python -m apps.lafa_knn_8plm.ensemble_driver" in text
    # Caller-supplied args must be forwarded.
    assert '"$@"' in text
    # Mean is the default aggregation per ADR-D23.
    assert "PROTEA_KNN_8PLM_AGGREGATION:-mean" in text


def test_entrypoint_exits_when_bundle_missing(tmp_path: Path) -> None:
    sh = shutil.which("sh")
    if sh is None:
        pytest.skip("POSIX sh not available in this environment")
    env = os.environ.copy()
    env["PROTEA_KNN_8PLM_BUNDLE"] = str(tmp_path / "missing_bundle")
    env["PROTEA_KNN_8PLM_QUERY"] = str(tmp_path / "queries.fasta")
    env["PROTEA_KNN_8PLM_OUTPUT"] = str(tmp_path / "out" / "predictions.tsv")
    result = subprocess.run(
        [sh, str(_ENTRYPOINT)],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert result.returncode == 64, f"expected exit 64, got {result.returncode}"
    assert "frozen bundle" in result.stderr


def test_entrypoint_exits_when_query_missing(tmp_path: Path) -> None:
    sh = shutil.which("sh")
    if sh is None:
        pytest.skip("POSIX sh not available in this environment")
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    env = os.environ.copy()
    env["PROTEA_KNN_8PLM_BUNDLE"] = str(bundle)
    env["PROTEA_KNN_8PLM_QUERY"] = str(tmp_path / "missing.fasta")
    env["PROTEA_KNN_8PLM_OUTPUT"] = str(tmp_path / "out" / "predictions.tsv")
    result = subprocess.run(
        [sh, str(_ENTRYPOINT)],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert result.returncode == 65, f"expected exit 65, got {result.returncode}"
    assert "queries FASTA" in result.stderr


def test_entrypoint_exits_when_output_dir_missing(tmp_path: Path) -> None:
    sh = shutil.which("sh")
    if sh is None:
        pytest.skip("POSIX sh not available in this environment")
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    query = tmp_path / "queries.fasta"
    query.write_text(">Q1\nMKT\n")
    env = os.environ.copy()
    env["PROTEA_KNN_8PLM_BUNDLE"] = str(bundle)
    env["PROTEA_KNN_8PLM_QUERY"] = str(query)
    env["PROTEA_KNN_8PLM_OUTPUT"] = str(tmp_path / "missing_dir" / "predictions.tsv")
    result = subprocess.run(
        [sh, str(_ENTRYPOINT)],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert result.returncode == 66, f"expected exit 66, got {result.returncode}"
    assert "output dir" in result.stderr


def test_workflow_yaml_valid() -> None:
    with _WORKFLOW.open() as fh:
        spec = yaml.safe_load(fh)
    assert spec["name"] == "LAFA knn-8plm Container"
    triggers = spec.get("on") or spec.get(True)
    assert triggers is not None, "workflow missing 'on:' triggers"
    assert "push" in triggers
    assert "release" in triggers
    assert "workflow_dispatch" in triggers


def test_workflow_targets_expected_ghcr_path() -> None:
    text = _WORKFLOW.read_text()
    assert "${{ github.repository }}/knn-8plm" in text
    assert "apps/lafa_knn_8plm/Dockerfile" in text
    assert "cache-from: type=gha,scope=lafa-knn-8plm" in text


# --- PLM registry ----------------------------------------------------


def test_plm_registry_has_eight_canonical_entries() -> None:
    keys = plm_encoders.list_plm_keys()
    assert len(keys) == 8
    expected = {
        "esm2_150m",
        "esm2_650m",
        "esm2_3b",
        "prot_t5",
        "prostt5",
        "ankh_base",
        "ankh_large",
        "esmc_600m",
    }
    assert set(keys) == expected


def test_plm_registry_spec_lookup() -> None:
    spec = plm_encoders.spec_for("prot_t5")
    assert spec.family == "prot_t5"
    assert spec.checkpoint.startswith("Rostlab/prot_t5")
    with pytest.raises(KeyError):
        plm_encoders.spec_for("bogus")


def test_plm_registry_build_registry_keys() -> None:
    reg = plm_encoders.build_registry()
    assert set(reg) == set(plm_encoders.list_plm_keys())
    # Each value must be callable; we cannot invoke without weights.
    for fn in reg.values():
        assert callable(fn)


# --- ensemble config -------------------------------------------------


def test_ensemble_config_defaults() -> None:
    cfg = EnsembleConfig()
    assert cfg.aggregation == "mean"
    assert cfg.metric == "cosine"
    assert cfg.k == 5
    assert len(cfg.plms) == 8


def test_ensemble_config_rejects_unknown_aggregation() -> None:
    with pytest.raises(ValueError):
        EnsembleConfig(aggregation="harmonic")


def test_ensemble_config_rejects_unknown_metric() -> None:
    with pytest.raises(ValueError):
        EnsembleConfig(metric="manhattan")


def test_ensemble_config_rejects_unknown_plm() -> None:
    with pytest.raises(ValueError):
        EnsembleConfig(plms=("bogus",))


def test_ensemble_config_rejects_invalid_k() -> None:
    with pytest.raises(ValueError):
        EnsembleConfig(k=0)


# --- argparse surface ------------------------------------------------


def test_arg_parser_defaults() -> None:
    parser = build_arg_parser()
    ns = parser.parse_args(
        [
            "--query_file",
            "q.fa",
            "--frozen_data_dir",
            "/bundle",
            "--output",
            "out.tsv",
        ]
    )
    assert ns.k == 5
    assert ns.metric == "cosine"
    assert ns.aggregation == "mean"
    # Default --plms must list all eight canonical members.
    assert sorted(ns.plms.split(",")) == sorted(plm_encoders.list_plm_keys())


def test_arg_parser_aggregation_choice_validation() -> None:
    parser = build_arg_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "--query_file",
                "q.fa",
                "--frozen_data_dir",
                "/b",
                "--output",
                "o.tsv",
                "--aggregation",
                "harmonic",
            ]
        )


def test_arg_parser_required_flags() -> None:
    parser = build_arg_parser()
    with pytest.raises(SystemExit):
        parser.parse_args([])


# --- bundle helpers --------------------------------------------------


def _write_per_plm_bundle(tmp_path: Path) -> Path:
    """Synthesise an 8plm bundle with two PLMs populated."""
    bundle = tmp_path / "frozen"
    bundle.mkdir()

    (bundle / "manifest.json").write_text(
        json.dumps(
            {
                "cutoff_version": "v226",
                "cutoff_date": "2025-05-03",
                "feature_schema_sha": "abc1234567890def",
            }
        )
    )

    annotations = pa.table(
        {
            "accession": ["R1", "R1", "R2", "R3"],
            "go_term_id": [1, 2, 1, 3],
            "go_id": ["GO:0000001", "GO:0000002", "GO:0000001", "GO:0000003"],
        }
    )
    pq.write_table(annotations, bundle / "reference_annotations.parquet")

    meta = pa.table(
        {
            "go_term_id": [1, 2, 3],
            "go_id": ["GO:0000001", "GO:0000002", "GO:0000003"],
            "aspect": ["F", "P", "C"],
        }
    )
    pq.write_table(meta, bundle / "go_term_metadata.parquet")

    for plm_key in ("esm2_650m", "prot_t5"):
        plm_dir = bundle / "plms" / plm_key
        plm_dir.mkdir(parents=True)
        refs = pa.table(
            {
                "accession": ["R1", "R2", "R3"],
                "embedding": [
                    np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32),
                    np.array([0.0, 1.0, 0.0, 0.0], dtype=np.float32),
                    np.array([0.0, 0.0, 1.0, 0.0], dtype=np.float32),
                ],
            }
        )
        pq.write_table(refs, plm_dir / "reference_embeddings.parquet")
    return bundle


def test_load_manifest(tmp_path: Path) -> None:
    bundle = _write_per_plm_bundle(tmp_path)
    manifest = load_manifest(bundle)
    assert manifest["cutoff_version"] == "v226"


def test_load_plm_refs_finds_per_plm_path(tmp_path: Path) -> None:
    bundle = _write_per_plm_bundle(tmp_path)
    accs, embeddings = load_plm_refs(bundle, "esm2_650m")
    assert accs == ["R1", "R2", "R3"]
    assert embeddings.shape == (3, 4)
    assert embeddings.dtype == np.float32


def test_load_plm_refs_falls_back_to_root(tmp_path: Path) -> None:
    bundle = _write_per_plm_bundle(tmp_path)
    # Drop the per-PLM file for esm2_3b and stage a root-level fallback.
    refs = pa.table(
        {
            "accession": ["R1"],
            "embedding": [np.array([1.0, 0.0], dtype=np.float32)],
        }
    )
    pq.write_table(refs, bundle / "reference_embeddings.parquet")
    accs, embeddings = load_plm_refs(bundle, "esm2_3b")
    assert accs == ["R1"]
    assert embeddings.shape == (1, 2)


def test_load_plm_refs_raises_when_missing(tmp_path: Path) -> None:
    bundle = _write_per_plm_bundle(tmp_path)
    # Wipe both candidate paths for esm2_3b.
    (bundle / "reference_embeddings.parquet").unlink(missing_ok=True)
    with pytest.raises(FileNotFoundError):
        load_plm_refs(bundle, "esm2_3b")


def test_load_annotations_groups_by_accession(tmp_path: Path) -> None:
    bundle = _write_per_plm_bundle(tmp_path)
    ann = load_annotations(bundle)
    assert sorted(ann.keys()) == ["R1", "R2", "R3"]
    assert len(ann["R1"]) == 2


def test_load_go_metadata(tmp_path: Path) -> None:
    bundle = _write_per_plm_bundle(tmp_path)
    go_id_map, go_aspect_map = load_go_metadata(bundle)
    assert go_id_map[1] == "GO:0000001"
    assert go_aspect_map[2] == "P"


# --- stack_queries / aggregation ------------------------------------


def test_stack_queries_preserves_order() -> None:
    embeddings = {
        "Q2": np.array([2.0], dtype=np.float32),
        "Q1": np.array([1.0], dtype=np.float32),
    }
    matrix, kept = stack_queries(embeddings, ["Q1", "Q2"])
    assert kept == ["Q1", "Q2"]
    assert matrix[0, 0] == pytest.approx(1.0)
    assert matrix[1, 0] == pytest.approx(2.0)


def test_stack_queries_drops_missing() -> None:
    matrix, kept = stack_queries({"Q1": np.array([1.0], dtype=np.float32)}, ["Q1", "Q_missing"])
    assert kept == ["Q1"]
    assert matrix.shape == (1, 1)


def test_stack_queries_empty() -> None:
    matrix, kept = stack_queries({}, ["Q1"])
    assert matrix.size == 0
    assert kept == []


def test_aggregate_predictions_mean_divides_by_full_ensemble() -> None:
    # GO term 1 is voted by 2 PLMs at scores 0.8 and 0.6; with n_plms=8
    # the mean must be 1.4 / 8 = 0.175, not 1.4 / 2 = 0.7.
    rows = [
        {"protein_accession": "Q1", "go_term_id": 1, "min_distance": 0.2, "plm": "a"},
        {"protein_accession": "Q1", "go_term_id": 1, "min_distance": 0.4, "plm": "b"},
        {"protein_accession": "Q1", "go_term_id": 2, "min_distance": 0.0, "plm": "a"},
    ]
    agg = aggregate_predictions(rows, aggregation="mean", n_plms=8)
    by_term = {(r["protein_accession"], r["go_term_id"]): r for r in agg}
    assert by_term[("Q1", 1)]["ensemble_score"] == pytest.approx((0.8 + 0.6) / 8)
    assert by_term[("Q1", 1)]["n_plms_voted"] == 2
    assert by_term[("Q1", 2)]["ensemble_score"] == pytest.approx(1.0 / 8)
    assert by_term[("Q1", 2)]["n_plms_voted"] == 1


def test_aggregate_predictions_max() -> None:
    rows = [
        {"protein_accession": "Q1", "go_term_id": 1, "min_distance": 0.2, "plm": "a"},
        {"protein_accession": "Q1", "go_term_id": 1, "min_distance": 0.4, "plm": "b"},
    ]
    agg = aggregate_predictions(rows, aggregation="max", n_plms=8)
    assert agg[0]["ensemble_score"] == pytest.approx(0.8)


def test_aggregate_predictions_orders_descending_then_stable() -> None:
    rows = [
        {"protein_accession": "Q2", "go_term_id": 5, "min_distance": 0.9, "plm": "a"},
        {"protein_accession": "Q1", "go_term_id": 7, "min_distance": 0.1, "plm": "a"},
    ]
    agg = aggregate_predictions(rows, aggregation="max", n_plms=2)
    assert [r["go_term_id"] for r in agg] == [7, 5]


def test_aggregate_predictions_rejects_unknown_aggregation() -> None:
    with pytest.raises(ValueError):
        aggregate_predictions([], aggregation="harmonic", n_plms=8)


def test_aggregate_predictions_clamps_negative_distance() -> None:
    rows = [
        {"protein_accession": "Q1", "go_term_id": 1, "min_distance": 1.5, "plm": "a"},
    ]
    agg = aggregate_predictions(rows, aggregation="mean", n_plms=8)
    assert agg[0]["ensemble_score"] == pytest.approx(0.0)


# --- TSV writer ------------------------------------------------------


def test_write_predictions_tsv_plain(tmp_path: Path) -> None:
    out_path = str(tmp_path / "out.tsv")
    predictions = [
        {"protein_accession": "Q1", "go_term_id": 1, "ensemble_score": 0.5},
        {"protein_accession": "Q1", "go_term_id": 99, "ensemble_score": 0.9},  # unknown id skipped
    ]
    n = write_predictions_tsv(predictions, {1: "GO:0000001"}, out_path)
    assert n == 1
    assert Path(out_path).read_text().strip() == "Q1\tGO:0000001\t0.5000"


def test_write_predictions_tsv_gzipped(tmp_path: Path) -> None:
    import gzip

    out_path = str(tmp_path / "out.tsv.gz")
    predictions = [
        {"protein_accession": "Q2", "go_term_id": 1, "ensemble_score": 0.25},
    ]
    n = write_predictions_tsv(predictions, {1: "GO:0000001"}, out_path)
    assert n == 1
    with gzip.open(out_path, "rt") as fh:
        assert fh.read().strip() == "Q2\tGO:0000001\t0.2500"


# --- prose constraints -----------------------------------------------


def test_method_card_calls_out_an_phan() -> None:
    text = _METHOD_CARD.read_text()
    assert "An Phan" in text
    assert "protea-knn-8plm" in text
    # Must identify itself as the ensemble, not the v1 baseline.
    assert "ensemble" in text.lower()


def test_method_card_has_no_em_dashes() -> None:
    text = _METHOD_CARD.read_text()
    assert "—" not in text, "em-dash found in METHOD_CARD.md"
    assert " -- " not in text, "ASCII double-dash em-dash found in METHOD_CARD.md"


def test_readme_has_no_em_dashes() -> None:
    text = _README.read_text()
    assert "—" not in text, "em-dash found in README.md"
    assert " -- " not in text, "ASCII double-dash em-dash found in README.md"


# --- sanity: AGGREGATIONS tuple is the authoritative list ------------


def test_aggregations_tuple_locked() -> None:
    """If the supported strategies change, the docs need to follow."""
    assert AGGREGATIONS == ("mean", "max")
