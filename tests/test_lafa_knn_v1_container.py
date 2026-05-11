"""Smoke tests for the protea-knn-v1 LAFA submission container.

The container itself is built in CI by
``.github/workflows/lafa-knn-v1-container.yml``; these tests guard
the build inputs (Dockerfile, entrypoint script, workflow file)
without launching a docker daemon. They run on every PR via the
standard pytest suite.

Coverage:

* the Dockerfile extends the ``protea-method-runtime`` base image and
  copies the entrypoint script,
* the entrypoint script is executable, parses with POSIX ``sh -n``,
  and pins the three baseline KNN flags
  (``--aspect_separated``, ``--no_v6``, ``--no_reranker``),
* the entrypoint exits with stable non-zero codes when the LAFA
  bind mounts are missing,
* the GitHub Actions workflow is valid YAML and publishes to the
  expected GHCR path.
"""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
from pathlib import Path

import pytest
import yaml

_REPO_ROOT = Path(__file__).resolve().parents[1]
_CONTAINER_DIR = _REPO_ROOT / "apps" / "lafa_knn_v1"
_DOCKERFILE = _CONTAINER_DIR / "Dockerfile"
_ENTRYPOINT = _CONTAINER_DIR / "lafa_entrypoint.sh"
_README = _CONTAINER_DIR / "README.md"
_METHOD_CARD = _CONTAINER_DIR / "METHOD_CARD.md"
_WORKFLOW = _REPO_ROOT / ".github" / "workflows" / "lafa-knn-v1-container.yml"


def test_container_dir_layout() -> None:
    assert _DOCKERFILE.is_file()
    assert _ENTRYPOINT.is_file()
    assert _README.is_file()
    assert _METHOD_CARD.is_file()
    assert (_CONTAINER_DIR / "__init__.py").is_file()


def test_dockerfile_extends_method_runtime() -> None:
    text = _DOCKERFILE.read_text()
    assert "ARG METHOD_RUNTIME_REF=" in text
    assert "FROM ghcr.io/frapercan/protea/method-runtime:" in text
    assert "COPY apps/lafa_knn_v1/lafa_entrypoint.sh" in text
    assert 'ENTRYPOINT ["/app/lafa_entrypoint.sh"]' in text


def test_dockerfile_creates_lafa_mountpoints() -> None:
    text = _DOCKERFILE.read_text()
    # LAFA evaluator harness expects these four bind-mount points.
    for mount in ("/input", "/output", "/bundle", "/hf-cache"):
        assert mount in text, f"missing mount point {mount} in Dockerfile"


def test_dockerfile_carries_lafa_labels() -> None:
    text = _DOCKERFILE.read_text()
    assert 'org.opencontainers.image.title="protea-knn-v1"' in text
    assert 'net.functionbench.lafa.method="protea-knn-v1"' in text
    assert 'net.functionbench.lafa.adr="D23"' in text


def test_entrypoint_is_executable() -> None:
    mode = _ENTRYPOINT.stat().st_mode
    assert mode & stat.S_IXUSR, "entrypoint script must be user-executable"


def test_entrypoint_parses_under_posix_sh() -> None:
    """``sh -n`` syntax-checks the entrypoint without executing it."""
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


def test_entrypoint_pins_baseline_flags() -> None:
    text = _ENTRYPOINT.read_text()
    for flag in ("--aspect_separated", "--no_v6", "--no_reranker"):
        assert flag in text, f"baseline flag {flag} not pinned in entrypoint"
    # The entrypoint must call protea-predict (or its script form).
    assert "/app/protea_predict.py" in text
    # Caller-supplied args must be forwarded so K / metric overrides
    # work without rebuilding the image.
    assert '"$@"' in text


def test_entrypoint_exits_when_bundle_missing(tmp_path: Path) -> None:
    """The entrypoint must surface a clear error when /bundle is absent."""
    sh = shutil.which("sh")
    if sh is None:
        pytest.skip("POSIX sh not available in this environment")

    env = os.environ.copy()
    env["PROTEA_KNN_V1_BUNDLE"] = str(tmp_path / "missing_bundle")
    env["PROTEA_KNN_V1_QUERY"] = str(tmp_path / "queries.fasta")
    env["PROTEA_KNN_V1_OUTPUT"] = str(tmp_path / "out" / "predictions.tsv")
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
    env["PROTEA_KNN_V1_BUNDLE"] = str(bundle)
    env["PROTEA_KNN_V1_QUERY"] = str(tmp_path / "missing.fasta")
    env["PROTEA_KNN_V1_OUTPUT"] = str(tmp_path / "out" / "predictions.tsv")
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
    env["PROTEA_KNN_V1_BUNDLE"] = str(bundle)
    env["PROTEA_KNN_V1_QUERY"] = str(query)
    env["PROTEA_KNN_V1_OUTPUT"] = str(tmp_path / "missing_dir" / "predictions.tsv")
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
    assert spec["name"] == "LAFA knn-v1 Container"
    # GH Actions parses ``on`` into the Python literal ``True`` because
    # YAML 1.1 treats bare ``on`` as a boolean key; both forms are
    # accepted here so the test is resilient to a future migration.
    triggers = spec.get("on") or spec.get(True)
    assert triggers is not None, "workflow missing 'on:' triggers"
    assert "push" in triggers
    assert "release" in triggers
    assert "workflow_dispatch" in triggers


def test_workflow_targets_expected_ghcr_path() -> None:
    text = _WORKFLOW.read_text()
    assert "${{ github.repository }}/knn-v1" in text
    assert "apps/lafa_knn_v1/Dockerfile" in text
    assert "cache-from: type=gha,scope=lafa-knn-v1" in text


def test_method_card_calls_out_an_phan() -> None:
    """The method card must cite An Phan as the recipient (per slice brief)."""
    text = _METHOD_CARD.read_text()
    assert "An Phan" in text
    # The card must identify itself as the v1 baseline, not the full pipeline.
    assert "protea-knn-v1" in text
    assert "no learned reranker" in text.lower() or "no reranker" in text.lower()


def test_method_card_has_no_em_dashes() -> None:
    """Hard constraint: no em-dashes in publishable prose."""
    text = _METHOD_CARD.read_text()
    assert "—" not in text, "em-dash found in method card"


def test_readme_has_no_em_dashes() -> None:
    text = _README.read_text()
    assert "—" not in text, "em-dash found in README"
