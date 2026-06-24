"""Smoke tests for the protea-v18 LAFA submission container.

The container itself is built in CI by
``.github/workflows/lafa-v18-container.yml``; these tests guard the
build inputs (Dockerfile, entrypoint script, workflow file) without
launching a docker daemon. They run on every PR via the standard
pytest suite.

Coverage:

* the Dockerfile extends the ``protea-method-runtime`` base image and
  copies the entrypoint script,
* the entrypoint script is executable, parses with POSIX ``sh -n``,
  pins ``--aspect_separated``, and does NOT pin the v1 ``--no_v6`` or
  ``--no_reranker`` opt-outs,
* the entrypoint exits with stable non-zero codes when the LAFA
  bind mounts are missing,
* the GitHub Actions workflow is valid YAML and publishes to the
  expected GHCR path,
* prose files (METHOD_CARD.md, README.md) cite An Phan and stay free
  of em-dashes (hard constraint per ~/Thesis2/CLAUDE.md).
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
_CONTAINER_DIR = _REPO_ROOT / "apps" / "lafa_v18"
_DOCKERFILE = _CONTAINER_DIR / "Dockerfile"
_ENTRYPOINT = _CONTAINER_DIR / "lafa_entrypoint.sh"
_README = _CONTAINER_DIR / "README.md"
_METHOD_CARD = _CONTAINER_DIR / "METHOD_CARD.md"
_WORKFLOW = _REPO_ROOT / ".github" / "workflows" / "lafa-v18-container.yml"


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
    assert "COPY apps/lafa_v18/lafa_entrypoint.sh" in text
    assert 'ENTRYPOINT ["/app/lafa_entrypoint.sh"]' in text


def test_dockerfile_creates_lafa_mountpoints() -> None:
    text = _DOCKERFILE.read_text()
    # LAFA evaluator harness expects these four bind-mount points.
    for mount in ("/input", "/output", "/bundle", "/hf-cache"):
        assert mount in text, f"missing mount point {mount} in Dockerfile"


def test_dockerfile_carries_lafa_labels() -> None:
    text = _DOCKERFILE.read_text()
    assert 'org.opencontainers.image.title="protea-v18"' in text
    assert 'net.functionbench.lafa.method="protea-v18"' in text
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


def test_entrypoint_pins_full_pipeline_flags() -> None:
    """v18 enables the full pipeline: aspect-separated KNN, v6, reranker.

    Header comments are allowed to reference the v1 opt-outs by name
    (the script documents WHY they are absent); the actual ``exec``
    invocation must not pass them. The check splits the file on the
    POSIX ``set -eu`` separator so the prose preamble does not pollute
    the assertion.
    """
    text = _ENTRYPOINT.read_text()
    # Aspect-separated KNN is the only positively pinned flag.
    assert "--aspect_separated" in text, "v18 must pin --aspect_separated"
    # Drop the header comment so we only check the actual command body.
    _, _, body = text.partition("set -eu")
    assert body, "entrypoint must contain a 'set -eu' command body"
    # v6 features and the reranker MUST be on; the v1 opt-outs must not appear
    # in the executable portion of the script.
    assert "--no_v6" not in body, "v18 must not pin --no_v6 (v6 features ON)"
    assert "--no_reranker" not in body, "v18 must not pin --no_reranker (reranker ON)"
    # The entrypoint must call protea-predict (or its script form).
    assert "/app/protea_predict.py" in body
    # Caller-supplied args must be forwarded so K / metric overrides
    # work without rebuilding the image.
    assert '"$@"' in body


def test_entrypoint_exits_when_bundle_missing(tmp_path: Path) -> None:
    """The entrypoint must surface a clear error when /bundle is absent."""
    sh = shutil.which("sh")
    if sh is None:
        pytest.skip("POSIX sh not available in this environment")

    env = os.environ.copy()
    env["PROTEA_V18_BUNDLE"] = str(tmp_path / "missing_bundle")
    env["PROTEA_V18_QUERY"] = str(tmp_path / "queries.fasta")
    env["PROTEA_V18_OUTPUT"] = str(tmp_path / "out" / "predictions.tsv")
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
    env["PROTEA_V18_BUNDLE"] = str(bundle)
    env["PROTEA_V18_QUERY"] = str(tmp_path / "missing.fasta")
    env["PROTEA_V18_OUTPUT"] = str(tmp_path / "out" / "predictions.tsv")
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
    env["PROTEA_V18_BUNDLE"] = str(bundle)
    env["PROTEA_V18_QUERY"] = str(query)
    env["PROTEA_V18_OUTPUT"] = str(tmp_path / "missing_dir" / "predictions.tsv")
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
    assert spec["name"] == "LAFA v18 Container"
    # GH Actions parses ``on`` into the Python literal ``True`` because
    # YAML 1.1 treats bare ``on`` as a boolean key; both forms are
    # accepted here so the test is resilient to a future migration.
    triggers = spec.get("on") or spec.get(True)
    assert triggers is not None, "workflow missing 'on:' triggers"
    assert "push" not in triggers  # release-gated: container builds on release only
    assert "release" in triggers
    assert "workflow_dispatch" in triggers


def test_workflow_targets_expected_ghcr_path() -> None:
    text = _WORKFLOW.read_text()
    assert "${{ github.repository }}/v18" in text
    assert "apps/lafa_v18/Dockerfile" in text
    assert "cache-from: type=gha,scope=lafa-v18" in text


def test_method_card_calls_out_an_phan() -> None:
    """The method card must cite An Phan as the recipient (per slice brief)."""
    text = _METHOD_CARD.read_text()
    assert "An Phan" in text
    # The card must identify itself as the v18 full pipeline.
    assert "protea-v18" in text
    # And must mention the per-aspect rerank that distinguishes it
    # from the v1 baseline and the 8plm ensemble.
    lowered = text.lower()
    assert "per-aspect" in lowered or "per aspect" in lowered
    assert "reranker" in lowered or "rerank" in lowered


def test_method_card_has_no_em_dashes() -> None:
    """Hard constraint: no em-dashes in publishable prose."""
    text = _METHOD_CARD.read_text()
    assert "—" not in text, "em-dash found in METHOD_CARD.md"
    assert " -- " not in text, "ASCII double-dash em-dash found in METHOD_CARD.md"


def test_readme_has_no_em_dashes() -> None:
    text = _README.read_text()
    assert "—" not in text, "em-dash found in README.md"
    assert " -- " not in text, "ASCII double-dash em-dash found in README.md"
