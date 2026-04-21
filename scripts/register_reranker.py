"""Register a trained reranker from ``protea-reranker-lab`` in PROTEA.

Reads the artefacts of a lab run (``runs/<name>/``) — ``run.json``,
``spec.yaml``, ``model.txt`` — uploads the booster to the configured
:class:`~protea.infrastructure.storage.ArtifactStore` under
``rerankers/<run_id>/model.txt``, and inserts a ``RerankerModel`` row
that links the artefact URI + feature schema sha + provenance
(``producer_version`` / ``producer_git_sha`` / ``spec_yaml``).

Usage::

    poetry run python scripts/register_reranker.py \\
        --run-dir ../protea-reranker-lab/runs/smoke-K5_pk-bpo_knn-only \\
        [--prediction-set-id <uuid>] [--evaluation-set-id <uuid>] \\
        [--name-override <str>] [--force]

On success, prints the new RerankerModel UUID to stdout.
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from pathlib import Path
from typing import Any

# Ensure the project root is importable when running the script directly.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from protea.infrastructure.orm.models.embedding.reranker_model import RerankerModel
from protea.infrastructure.session import build_session_factory, session_scope
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store

try:
    from protea_reranker_lab.contracts import compute_feature_schema_sha
except Exception as exc:  # pragma: no cover — dev-time dep must be installed
    raise SystemExit(
        "protea_reranker_lab is not importable — install the editable dev "
        "dependency (pyproject.toml already declares it). "
        f"Underlying error: {exc}"
    ) from exc


def _args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--run-dir", required=True, type=Path,
                   help="Path to runs/<name>/ containing run.json + spec.yaml + model.txt")
    p.add_argument("--prediction-set-id", type=str, default=None,
                   help="Optional UUID of an existing PredictionSet to link")
    p.add_argument("--evaluation-set-id", type=str, default=None,
                   help="Optional UUID of an existing EvaluationSet to link")
    p.add_argument("--name-override", type=str, default=None,
                   help="Override the RerankerModel.name (default: run_id from run.json)")
    p.add_argument("--force", action="store_true",
                   help="If a RerankerModel with the same name exists, delete+replace it")
    return p.parse_args()


def _parse_cell(cell: str | None) -> tuple[str, str | None]:
    """Parse ``training.cell`` from spec.yaml into (category, aspect)."""
    if not cell:
        return ("pk", None)  # reasonable default; category is NOT NULL
    cell = cell.strip().lower()
    if "-" in cell:
        cat, asp = cell.split("-", 1)
        return (cat, asp)
    return (cell, None)


def _load_manifest(run_dir: Path, run_json: dict[str, Any]) -> dict[str, Any]:
    """Best-effort load of the dataset manifest referenced by the run.

    The run records a relative manifest path; try resolving it against the
    lab repo root (run_dir.parents[1]) and against run_dir itself.
    """
    rel = run_json.get("dataset", {}).get("manifest_path")
    if not rel:
        return {}
    candidates = [
        run_dir.parents[1] / rel,   # <lab>/runs/<name>/ → <lab>
        run_dir / rel,
        Path(rel),
    ]
    for path in candidates:
        if path.is_file():
            try:
                return json.loads(path.read_text())
            except Exception:
                pass
    return {}


def main() -> None:
    a = _args()
    run_dir = a.run_dir.resolve()

    run_json_path = run_dir / "run.json"
    spec_yaml_path = run_dir / "spec.yaml"
    model_txt_path = run_dir / "model.txt"

    for path in (run_json_path, spec_yaml_path, model_txt_path):
        if not path.is_file():
            raise SystemExit(f"missing required artefact: {path}")

    run = json.loads(run_json_path.read_text())
    spec_yaml_text = spec_yaml_path.read_text()
    manifest = _load_manifest(run_dir, run)

    run_id: str = run.get("run_id") or run_dir.name
    name: str = a.name_override or run_id

    # Provenance — pull PROTEA version/git sha from the dataset's manifest
    # (written by export_research_dataset). Lab git sha is in
    # run.environment but we persist PROTEA's sha here, not the lab's.
    producer_version = manifest.get("producer_version")
    producer_git_sha = manifest.get("producer_git_sha")

    # Feature schema sha — family-aware fingerprint. Falls back to the
    # dataset's schema_sha when families aren't recorded (older runs).
    feature_families: list[str] | None = (
        run.get("features", {}).get("families_enabled")
        or run.get("dataset", {}).get("feature_families")
    )
    drop_features: list[str] = run.get("features", {}).get("drop_features") or []
    if feature_families:
        feature_schema_sha = compute_feature_schema_sha(feature_families, drop_features or None)
    else:
        feature_schema_sha = run.get("dataset", {}).get("schema_sha")

    dataset = run.get("dataset", {})
    embedding_config_id = dataset.get("embedding_config_id")
    ontology_snapshot_id = dataset.get("ontology_snapshot_id")

    category, aspect = _parse_cell(_extract_cell_from_spec(spec_yaml_text))

    metrics = run.get("metrics", {}) or {}
    feature_importance = run.get("feature_importance", {}) or {}

    # ── 1. Upload booster to the artifact store ─────────────────────────
    settings = load_settings(PROJECT_ROOT)
    store = get_artifact_store(settings)
    artifact_key = f"rerankers/{run_id}/model.txt"
    artifact_uri = store.put(artifact_key, model_txt_path)
    print(f"[register] uploaded booster → {artifact_uri}")

    # ── 2. Insert RerankerModel row ──────────────────────────────────────
    factory = build_session_factory(settings.db_url)
    with session_scope(factory) as session:
        existing = session.query(RerankerModel).filter(RerankerModel.name == name).first()
        if existing is not None:
            if not a.force:
                raise SystemExit(
                    f"RerankerModel name={name!r} already exists (id={existing.id}). "
                    "Pass --force to replace."
                )
            session.delete(existing)
            session.flush()

        row = RerankerModel(
            name=name,
            prediction_set_id=uuid.UUID(a.prediction_set_id) if a.prediction_set_id else None,
            evaluation_set_id=uuid.UUID(a.evaluation_set_id) if a.evaluation_set_id else None,
            category=category,
            aspect=aspect,
            model_data=None,
            artifact_uri=artifact_uri,
            feature_schema_sha=feature_schema_sha,
            embedding_config_id=uuid.UUID(embedding_config_id) if embedding_config_id else None,
            ontology_snapshot_id=uuid.UUID(ontology_snapshot_id) if ontology_snapshot_id else None,
            producer_version=producer_version,
            producer_git_sha=producer_git_sha,
            spec_yaml=spec_yaml_text,
            metrics=metrics,
            feature_importance=feature_importance,
        )
        session.add(row)
        session.flush()
        reranker_id = row.id

    # Emit the new row id to stdout so callers can capture it.
    print(str(reranker_id))


def _extract_cell_from_spec(spec_yaml_text: str) -> str | None:
    """Cheap YAML peek — avoids dragging PyYAML dep into this script when
    the only field we need is ``training.cell``.
    """
    in_training = False
    for raw in spec_yaml_text.splitlines():
        line = raw.rstrip()
        if not line or line.lstrip().startswith("#"):
            continue
        if not line.startswith(" "):  # top-level key
            in_training = line.strip().rstrip(":") == "training"
            continue
        if in_training:
            stripped = line.strip()
            if stripped.startswith("cell:"):
                return stripped.split(":", 1)[1].strip().strip('"').strip("'") or None
    return None


if __name__ == "__main__":
    main()
