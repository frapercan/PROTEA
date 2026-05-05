"""Export a frozen re-ranker dataset for protea-reranker-lab.

Runs the same KNN + feature-generation pipeline as the now-renamed
research dataset dump helper (``training_dump_helpers``), skips the
LightGBM training stage, and publishes the resulting parquets +
manifest via the configured
:class:`~protea.infrastructure.storage.ArtifactStore` (local FS by
default, MinIO when enabled via the ``storage`` compose profile).

Why a dedicated operation instead of dump-only mode of the helper?

* Narrower payload: only the knobs that matter for export, no
  LightGBM-specific fields.
* Routes output through the storage abstraction, so the lab can consume
  from MinIO without every export having to know a local dump path on
  the API host.
* Records ``producer_version`` / ``producer_git_sha`` in the manifest
  so any lab run can be traced back to a PROTEA HEAD.
"""

from __future__ import annotations

import hashlib
import tempfile
import uuid
from pathlib import Path
from typing import Annotated, Any

from pydantic import Field, field_validator
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.training_dump_helpers import TrainRerankerAutoOperation
from protea.infrastructure.orm.models.embedding.dataset import Dataset
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store

PositiveInt = Annotated[int, Field(gt=0)]


class ExportResearchDatasetPayload(ProteaPayload, frozen=True):
    """Payload for the ``export_research_dataset`` operation."""

    embedding_config_id: str
    ontology_snapshot_id: str
    train_versions: list[int]
    test_versions: list[int]
    annotation_source: str = "goa"

    # Human label for the published dataset; also the ``name`` field in
    # the lab manifest and the key prefix ``datasets/{output_name}/``.
    output_name: str

    # KNN + feature generation knobs (mirror the dump helper).
    k: PositiveInt = 5
    search_backend: str = "faiss"
    compute_alignments: bool = False
    compute_taxonomy: bool = False
    expand_votes_to_ancestors: bool = False
    use_embedding_pca: bool = False

    @field_validator("output_name", "embedding_config_id", "ontology_snapshot_id", mode="before")
    @classmethod
    def must_be_non_empty(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("must be a non-empty string")
        return v.strip()

    @field_validator("train_versions", mode="before")
    @classmethod
    def at_least_two_train(cls, v: list[int]) -> list[int]:
        if len(v) < 2:
            raise ValueError("train_versions must have at least 2 entries to form pairs")
        return sorted(v)

    @field_validator("test_versions", mode="before")
    @classmethod
    def at_least_one_test(cls, v: list[int]) -> list[int]:
        if not v:
            raise ValueError("test_versions must have at least 1 entry")
        return sorted(v)


class ExportResearchDatasetOperation:
    name = "export_research_dataset"
    description = (
        "Generate a frozen reranker dataset (train/eval parquets + "
        "manifest) and publish it to the configured artifact store."
    )

    _auto = TrainRerankerAutoOperation()

    def summarize_payload(
        self, payload: dict[str, Any], *, session: Session | None = None
    ) -> str:
        p = payload or {}
        bits: list[str] = []
        if p.get("output_name"):
            bits.append(str(p["output_name"]))
        train = p.get("train_versions") or []
        test = p.get("test_versions") or []
        if train:
            bits.append(f"train={train[0]}→{train[-1]} (n={len(train)})")
        if test:
            bits.append(f"test={','.join(str(v) for v in test)}")
        if p.get("k"):
            bits.append(f"k={p['k']}")
        return " · ".join(bits)

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = ExportResearchDatasetPayload.model_validate(payload)
        raw_job_id = payload.get("_job_id") if isinstance(payload, dict) else None
        job_uuid = uuid.UUID(raw_job_id) if raw_job_id else None

        settings = load_settings(_resolve_project_root())
        store = get_artifact_store(settings)
        key_prefix = f"datasets/{p.output_name}/"

        # Reject duplicate names up-front so a half-succeeded run doesn't
        # silently leave orphan blobs in the store.
        existing = (
            session.query(Dataset.id).filter(Dataset.name == p.output_name).first()
        )
        if existing is not None:
            raise ValueError(f"Dataset {p.output_name!r} already exists")

        def _relay(event: str, scope: str | None, evt_payload: dict[str, Any], level: str) -> None:
            # Surface the underlying dump-helper events under this
            # operation's namespace so the job event log reads naturally.
            if event.startswith("dump_helper."):
                event = "export_research_dataset." + event[len("dump_helper."):]
            emit(event, scope, evt_payload, level)  # type: ignore[arg-type]

        with tempfile.TemporaryDirectory(prefix="protea_export_") as tmp:
            stage_dir = Path(tmp)
            auto_payload: dict[str, Any] = {
                "name": p.output_name,
                "embedding_config_id": p.embedding_config_id,
                "ontology_snapshot_id": p.ontology_snapshot_id,
                "train_versions": p.train_versions,
                "test_versions": p.test_versions,
                "annotation_source": p.annotation_source,
                "limit_per_entry": p.k,
                "search_backend": p.search_backend,
                "compute_alignments": p.compute_alignments,
                "compute_taxonomy": p.compute_taxonomy,
                "expand_votes_to_ancestors": p.expand_votes_to_ancestors,
                "use_embedding_pca": p.use_embedding_pca,
                "training_scope": "per_cell",
                "dump_to": str(stage_dir),
                "dump_only": True,
            }
            auto_result = self._auto.execute(session, auto_payload, emit=_relay)

            uploaded: dict[str, str] = {}
            for fname in ("train.parquet", "eval.parquet", "manifest.json"):
                p_path = stage_dir / fname
                if p_path.exists():
                    uploaded[fname] = store.put(key_prefix + fname, p_path)

            # Read manifest bytes while the staging dir still exists so the
            # Dataset row can record a content-addressed fingerprint. The
            # manifest is written by ``export_reranker_parquets`` and is
            # never empty on a successful run.
            import json as _json

            manifest_path = stage_dir / "manifest.json"
            if not manifest_path.exists():
                raise RuntimeError(
                    "export_research_dataset: manifest.json missing from stage dir — "
                    "dump path did not produce the expected layout"
                )
            manifest_bytes = manifest_path.read_bytes()
            manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()
            manifest_data = _json.loads(manifest_bytes)

            emit(
                "export_research_dataset.published",
                None,
                {
                    "backend": settings.storage_backend,
                    "key_prefix": key_prefix,
                    "files": list(uploaded.keys()),
                    "manifest_sha": manifest_sha,
                },
                "info",
            )

        # Register the dataset in the DB so the lab can pull by name/id.
        dataset = Dataset(
            name=p.output_name,
            operation=self.name,
            job_id=job_uuid,
            storage_backend=settings.storage_backend,
            key_prefix=key_prefix,
            train_uri=uploaded.get("train.parquet"),
            eval_uri=uploaded.get("eval.parquet"),
            manifest_uri=uploaded["manifest.json"],
            schema_sha=manifest_data.get("schema_sha", ""),
            manifest_sha=manifest_sha,
            n_train_rows=int(manifest_data.get("n_train_rows", 0)),
            n_eval_rows=int(manifest_data.get("n_eval_rows", 0)),
            k=int(manifest_data.get("k", p.k)),
            annotation_source=manifest_data.get("annotation_source", p.annotation_source),
            embedding_config_id=uuid.UUID(p.embedding_config_id),
            ontology_snapshot_id=uuid.UUID(p.ontology_snapshot_id),
            train_snapshot_pairs=list(manifest_data.get("train_snapshot_pairs", [])),
            eval_snapshot_pair=manifest_data.get("eval_snapshot_pair"),
            producer_version=manifest_data.get("producer_version"),
            producer_git_sha=manifest_data.get("producer_git_sha"),
            meta={},
        )
        session.add(dataset)
        session.flush()
        dataset_id = dataset.id
        emit(
            "export_research_dataset.registered",
            None,
            {"dataset_id": str(dataset_id), "name": p.output_name},
            "info",
        )

        merged: dict[str, Any] = dict(auto_result.result)
        merged.update(
            {
                "dataset_id": str(dataset_id),
                "output_name": p.output_name,
                "key_prefix": key_prefix,
                "storage_backend": settings.storage_backend,
                "train_uri": uploaded.get("train.parquet"),
                "eval_uri": uploaded.get("eval.parquet"),
                "manifest_uri": uploaded.get("manifest.json"),
                "manifest_sha": manifest_sha,
            }
        )
        return OperationResult(result=merged)


def _resolve_project_root() -> Path:
    # protea/core/operations/export_research_dataset.py → parents[3] = repo root
    return Path(__file__).resolve().parents[3]
