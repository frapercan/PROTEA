"""Tests for the Anc2Vec path resolution chain.

The chain is: env var > repo-relative fallback. These tests exercise
both branches plus the FileNotFoundError raised when neither resolves.
The anc2vec npz is a static pre-trained artefact and is not tracked as
a Dataset row, so there is no artifact-store resolution path.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import pytest

from protea.core import anc2vec_embeddings


@pytest.fixture(autouse=True)
def _reset_module_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """Clear env vars and the one-shot fallback-warned flag between tests."""
    monkeypatch.delenv(anc2vec_embeddings._ENV_VAR, raising=False)
    monkeypatch.setattr(anc2vec_embeddings, "_FALLBACK_WARNED", False, raising=False)


def _make_npz(tmp_path: Path, name: str = "anc2vec.npz") -> Path:
    """Create a sentinel file that stands in for the npz artefact."""
    p = tmp_path / name
    p.write_bytes(b"not-a-real-npz-but-readable")
    return p


def test_env_var_wins_when_file_exists(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    npz = _make_npz(tmp_path)
    monkeypatch.setenv(anc2vec_embeddings._ENV_VAR, str(npz))

    resolved = anc2vec_embeddings._resolve_default_path()

    assert resolved == npz


def test_env_var_with_missing_file_falls_through_to_repo_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(anc2vec_embeddings._ENV_VAR, str(tmp_path / "missing.npz"))

    if anc2vec_embeddings._DEFAULT_PATH.is_file():
        resolved = anc2vec_embeddings._resolve_default_path()
        assert resolved == anc2vec_embeddings._DEFAULT_PATH
    else:
        with pytest.raises(FileNotFoundError) as exc:
            anc2vec_embeddings._resolve_default_path()
        assert anc2vec_embeddings._ENV_VAR in str(exc.value)


def test_unset_env_falls_back_to_repo_relative_when_file_present(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    if not anc2vec_embeddings._DEFAULT_PATH.is_file():
        pytest.skip("repo-relative anc2vec npz not present in this checkout")

    resolved = anc2vec_embeddings._resolve_default_path()

    assert resolved == anc2vec_embeddings._DEFAULT_PATH


def test_fallback_emits_one_shot_warning(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    if not anc2vec_embeddings._DEFAULT_PATH.is_file():
        pytest.skip("repo-relative anc2vec npz not present in this checkout")

    monkeypatch.setattr(anc2vec_embeddings, "_FALLBACK_WARNED", False, raising=False)
    with caplog.at_level(logging.WARNING, logger=anc2vec_embeddings.__name__):
        anc2vec_embeddings._resolve_default_path()
        first_warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        caplog.clear()
        anc2vec_embeddings._resolve_default_path()
        second_warnings = [r for r in caplog.records if r.levelno == logging.WARNING]

    assert len(first_warnings) == 1
    assert anc2vec_embeddings._ENV_VAR in first_warnings[0].getMessage()
    assert second_warnings == []


def test_all_missing_raises_clear_filenotfound(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(anc2vec_embeddings, "_DEFAULT_PATH", tmp_path / "does-not-exist.npz")
    monkeypatch.delenv(anc2vec_embeddings._ENV_VAR, raising=False)

    with pytest.raises(FileNotFoundError) as exc:
        anc2vec_embeddings._resolve_default_path()

    message = str(exc.value)
    assert anc2vec_embeddings._ENV_VAR in message
    assert "repo-relative fallback" in message
    assert "artifact store" in message


def test_artifact_store_stub_always_returns_none() -> None:
    """The artifact-store stub always returns None.

    The anc2vec npz is not tracked as a Dataset row, so there is no
    artifact-store resolution path; the stub documents that fact.
    """
    assert anc2vec_embeddings._resolve_from_artifact_store() is None


def test_empty_env_var_is_treated_as_unset(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(anc2vec_embeddings._ENV_VAR, "   ")
    monkeypatch.setattr(anc2vec_embeddings, "_DEFAULT_PATH", tmp_path / "does-not-exist.npz")

    with pytest.raises(FileNotFoundError):
        anc2vec_embeddings._resolve_default_path()


def test_get_index_uses_resolved_path_when_no_argument(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    npz = _make_npz(tmp_path)
    monkeypatch.setenv(anc2vec_embeddings._ENV_VAR, str(npz))

    captured: dict[str, str] = {}

    def _fake_get(path: str) -> str:
        captured["path"] = path
        return "sentinel-index"  # type: ignore[return-value]

    monkeypatch.setattr(anc2vec_embeddings, "_get_index_lib", _fake_get)
    result = anc2vec_embeddings.get_index()

    assert captured["path"] == str(npz)
    assert result == "sentinel-index"


def test_get_index_passes_explicit_path_through(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, str] = {}

    def _fake_get(path: str) -> str:
        captured["path"] = path
        return "sentinel-index"  # type: ignore[return-value]

    monkeypatch.setattr(anc2vec_embeddings, "_get_index_lib", _fake_get)
    anc2vec_embeddings.get_index("/explicit/override.npz")

    assert captured["path"] == "/explicit/override.npz"


def test_default_path_constant_still_repo_relative() -> None:
    """Guard against accidental rewiring of the exported _DEFAULT_PATH."""
    assert anc2vec_embeddings._DEFAULT_PATH.name == "anc2vec_2020-10.npz"
    assert os.path.basename(anc2vec_embeddings._DEFAULT_PATH.parent) == "anc2vec"
