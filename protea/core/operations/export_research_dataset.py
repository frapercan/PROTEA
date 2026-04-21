"""Export a frozen re-ranker dataset for ``protea-reranker-lab``.

This operation runs the same KNN + feature-generation pipeline as
``train_reranker_auto`` but skips the LightGBM training stage and
publishes the resulting parquets + manifest via the configured
:class:`~protea.infrastructure.storage.ArtifactStore` (local FS by
default, MinIO when enabled via the ``storage`` compose profile).

Why a dedicated operation instead of ``train_reranker_auto --dump-only``?

* Narrower payload — only the knobs that matter for export, no
  LightGBM-specific fields.
* Routes output through the storage abstraction, so the lab can consume
  from MinIO without every export having to know a local dump path on
  the API host.
* Records ``producer_version`` / ``producer_git_sha`` in the manifest
  so any lab run can be traced back to a PROTEA HEAD.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Annotated, Any

from pydantic import Field, field_validator
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.operations.train_reranker import TrainRerankerAutoOperation
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

    # KNN + feature generation knobs (mirror train_reranker_auto).
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

        settings = load_settings(_resolve_project_root())
        store = get_artifact_store(settings)
        key_prefix = f"datasets/{p.output_name}/"

        def _relay(event: str, scope: str | None, evt_payload: dict[str, Any], level: str) -> None:
            # Surface the underlying train_reranker_auto events under this
            # operation's namespace so the job event log reads naturally.
            if event.startswith("train_reranker_auto."):
                event = "export_research_dataset." + event[len("train_reranker_auto."):]
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

            emit(
                "export_research_dataset.published",
                None,
                {
                    "backend": settings.storage_backend,
                    "key_prefix": key_prefix,
                    "files": list(uploaded.keys()),
                },
                "info",
            )

        merged: dict[str, Any] = dict(auto_result.result)
        merged.update(
            {
                "output_name": p.output_name,
                "key_prefix": key_prefix,
                "storage_backend": settings.storage_backend,
                "train_uri": uploaded.get("train.parquet"),
                "eval_uri": uploaded.get("eval.parquet"),
                "manifest_uri": uploaded.get("manifest.json"),
            }
        )
        return OperationResult(result=merged)


def _resolve_project_root() -> Path:
    # protea/core/operations/export_research_dataset.py → parents[3] = repo root
    return Path(__file__).resolve().parents[3]
