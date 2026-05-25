"""Regression test for FIX-MINIO-DEP.

Background: ``minio`` used to be declared as an *optional* PEP-621 extra
(``[project.optional-dependencies] storage = ["minio (>=7.2,<8.0)"]``),
so a plain ``poetry install --sync`` purged it from the deployed venv.
That silently broke every ``export_research_dataset`` job the moment the
worker tried to import ``MinioArtifactStore`` (9 FAILED EXP.13 cells on
2026-05-25 with ``ModuleNotFoundError: No module named 'minio'``).

This test pins ``minio`` as a mandatory transitive of the test
environment: if anyone ever re-demotes it back to an extra (or drops it
outright), CI fails here before the export pipeline does in prod.
"""

from __future__ import annotations


def test_minio_is_importable() -> None:
    import minio  # noqa: F401  - presence is the assertion

    version = getattr(minio, "__version__", None)
    assert version is None or isinstance(version, str)


def test_minio_artifact_store_uses_real_client() -> None:
    """The lazy import inside ``MinioArtifactStore.__init__`` must succeed.

    The class catches ``ImportError`` and re-raises a friendlier
    ``RuntimeError``; if ``minio`` is missing we would hit the
    ``RuntimeError`` path. Here we verify the symbol resolves.
    """
    from minio import Minio

    assert callable(Minio)
