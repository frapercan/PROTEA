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
import json
import tempfile
import uuid
from pathlib import Path
from typing import Annotated, Any, NamedTuple

from pydantic import Field, field_validator
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.schema_sha_v2 import maybe_v2
from protea.core.training_dump_helpers import TrainRerankerAutoOperation
from protea.infrastructure.orm.models.embedding.dataset import Dataset
from protea.infrastructure.session import build_session_factory, session_scope
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store

PositiveInt = Annotated[int, Field(gt=0)]


class _DumpOutcome(NamedTuple):
    """Bundle of artefact-store + manifest metadata produced by the dump."""

    settings: Any
    key_prefix: str
    uploaded: dict[str, str]
    manifest_data: dict[str, Any]
    manifest_sha: str


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

    # lafa-integrate INT-6: train/serve feature parity. When set, the exported
    # train/eval parquet carries the REAL self_prior / association / classifier
    # feature values the predict path serves (not the zero-fill defaults), so
    # the lab trains the per-category boosters on exactly the served features
    # (NFR-REPRO). Default False so existing exports stay bit-identical.
    compute_self_prior: bool = False
    compute_association: bool = False
    compute_classifier: bool = False
    # ProtST text-to-GO family, gated like the other compute_* flags.
    compute_protst: bool = False

    # NFR-ARCH "export decouple". When True, the STABLE per-candidate feature
    # table (every column except the classifier columns) is content-addressed
    # and cached in the artifact store under ``stable_features/<hash>/``. A
    # later export with the SAME stable config (embedding_config / ontology /
    # versions / k / distance / stable feature flags) but a DIFFERENT
    # classifier reuses the cached table and only recomputes the classifier
    # column(s), turning a classifier A/B from ~8h into the classifier-compute
    # time alone. Default False so the default export path and the golden
    # parquet are byte-identical. See ``training_dump._stable_feature_cache``.
    stable_feature_cache: bool = False
    # Phase-2 SKETCH (carried so the payload contract is stable): a list of
    # classifier-variant specs, each ``{"label": ..., "seed_dir"|"seed_paths"|
    # "model_path": ...}``. When given, the export emits one
    # ``classifier_score__<label>`` family per variant in a single pass over the
    # cached stable table. ``None`` keeps the canonical single classifier
    # columns. Multi-column emission is wired in phase 2.
    classifier_variants: list[dict[str, str]] | None = None

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

    def summarize_payload(self, payload: dict[str, Any], *, session: Session | None = None) -> str:
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
        """Run the export in three short session windows.

        The compute phase (KNN search + parquet staging + upload) can
        take 3-4 hours. Holding a single SQLAlchemy session open across
        that span ends in tears: pool recycle / idle timeouts / pika
        reconnects all conspire to detach the session by the time we
        try to insert the Dataset row, which then raises
        ``DetachedInstanceError`` and loses the work.

        Layout:

        * Window 1 (uniqueness check) uses the session handed in by
          BaseWorker. We touch it just long enough to verify the name
          is free, then release it.
        * Window 2 (compute) opens its own short-lived session for the
          dump helper, since its SQL is structured as many small reads
          rather than one long transaction.
        * Window 3 (register) opens a fresh ``session_scope`` to insert
          the Dataset row. That session is born after the long compute
          phase, so its connection is guaranteed healthy.

        The BaseWorker-supplied session is left untouched after window 1
        so the success/failure commit at the end of the worker loop
        runs against a fresh-enough connection (``_rebind_job`` in
        BaseWorker handles the residual detachment risk for the Job
        ORM itself).
        """
        p = ExportResearchDatasetPayload.model_validate(payload)
        raw_job_id = payload.get("_job_id") if isinstance(payload, dict) else None
        job_uuid = uuid.UUID(raw_job_id) if raw_job_id else None

        settings = load_settings(_resolve_project_root())

        self._reject_duplicate_name(session, p.output_name)
        factory = build_session_factory(settings.db_url)

        outcome, auto_result = self._compute_and_upload(p, factory, settings, emit)
        with session_scope(factory) as register_session:
            dataset_id = self._register_dataset(register_session, p, outcome, job_uuid, emit)

        return OperationResult(result=self._merge_result(auto_result, p, outcome, dataset_id))

    @staticmethod
    def _reject_duplicate_name(session: Session, output_name: str) -> None:
        """Window 1: pre-flight uniqueness check on the passed session.

        Touches the session only for one read so the long compute phase
        does not hold it open across pool-recycle / idle-timeout windows.
        """
        if session.query(Dataset.id).filter(Dataset.name == output_name).first() is not None:
            raise ValueError(f"Dataset {output_name!r} already exists")
        session.expire_all()

    def _compute_and_upload(
        self,
        p: ExportResearchDatasetPayload,
        factory: Any,
        settings: Any,
        emit: EmitFn,
    ) -> tuple[_DumpOutcome, OperationResult]:
        """Window 2: KNN dump + parquet upload.

        Dump helper opens its own short session_scope; the parquet
        writers and the upload do not touch the DB. Returns the dump
        outcome plus the inner ``OperationResult`` for merging.
        """
        store = get_artifact_store(settings)
        key_prefix = f"datasets/{p.output_name}/"
        with tempfile.TemporaryDirectory(prefix="protea_export_") as tmp:
            stage_dir = Path(tmp)
            if p.stable_feature_cache:
                auto_result = self._compute_with_stable_cache(p, factory, store, stage_dir, emit)
            else:
                with session_scope(factory) as compute_session:
                    auto_result = self._dump_to_stage(compute_session, p, stage_dir, emit)
            uploaded, manifest_data, manifest_sha = self._upload_outputs(
                stage_dir, store, key_prefix
            )
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

        outcome = _DumpOutcome(
            settings=settings,
            key_prefix=key_prefix,
            uploaded=uploaded,
            manifest_data=manifest_data,
            manifest_sha=manifest_sha,
        )
        return outcome, auto_result

    def _compute_with_stable_cache(
        self,
        p: ExportResearchDatasetPayload,
        factory: Any,
        store: Any,
        stage_dir: Path,
        emit: EmitFn,
    ) -> OperationResult:
        """NFR-ARCH decouple: stage parquets via the stable-feature cache.

        Delegates to ``_export_stable_cache_runner`` (kept out of this module so
        the operation stays under the smell budget). The runner hits the cache
        on the stable config and only recomputes the classifier column(s),
        producing the SAME staged ``train.parquet`` / ``eval.parquet`` the
        default path would.
        """
        from protea.core.operations._export_stable_cache_runner import run_with_stable_cache

        def _dump_fn(
            session: Session,
            payload: ExportResearchDatasetPayload,
            stage: Path,
            relay: EmitFn,
            *,
            classifier_on: bool,
        ) -> OperationResult:
            # Heavy pass on a cache MISS runs the classifier OFF so the staged
            # parquet is purely stable / cacheable; the runner re-stamps it.
            return self._dump_to_stage(
                session, payload, stage, relay, compute_classifier_override=classifier_on
            )

        return run_with_stable_cache(p, factory, store, stage_dir, emit, _dump_fn)

    @staticmethod
    def _merge_result(
        auto_result: OperationResult,
        p: ExportResearchDatasetPayload,
        outcome: _DumpOutcome,
        dataset_id: uuid.UUID,
    ) -> dict[str, Any]:
        """Compose the final OperationResult dict from the dump output."""
        merged: dict[str, Any] = dict(auto_result.result)
        merged.update(
            {
                "dataset_id": str(dataset_id),
                "output_name": p.output_name,
                "key_prefix": outcome.key_prefix,
                "storage_backend": outcome.settings.storage_backend,
                "train_uri": outcome.uploaded.get("train.parquet"),
                "eval_uri": outcome.uploaded.get("eval.parquet"),
                "manifest_uri": outcome.uploaded.get("manifest.json"),
                "manifest_sha": outcome.manifest_sha,
            }
        )
        return merged

    def _dump_to_stage(
        self,
        session: Session,
        p: ExportResearchDatasetPayload,
        stage_dir: Path,
        emit: EmitFn,
        *,
        compute_classifier_override: bool | None = None,
    ) -> OperationResult:
        """Run the inner dump-only path of TrainRerankerAutoOperation.

        Routes its events under this operation's namespace via the
        ``dump_helper.`` → ``export_research_dataset.`` prefix swap so the
        job event log reads naturally.

        ``compute_classifier_override`` lets the stable-feature-cache path force
        the classifier OFF for the heavy pass (so the staged parquet is purely
        stable / cacheable); ``None`` keeps the payload's own flag for the
        default path, leaving it byte-identical.
        """

        def _relay(event: str, scope: str | None, evt_payload: dict[str, Any], level: str) -> None:
            if event.startswith("dump_helper."):
                event = "export_research_dataset." + event[len("dump_helper.") :]
            emit(event, scope, evt_payload, level)  # type: ignore[arg-type]

        compute_classifier = (
            p.compute_classifier
            if compute_classifier_override is None
            else compute_classifier_override
        )
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
            "compute_self_prior": p.compute_self_prior,
            "compute_association": p.compute_association,
            "compute_classifier": compute_classifier,
            "compute_protst": p.compute_protst,
            "training_scope": "per_cell",
            "dump_to": str(stage_dir),
            "dump_only": True,
        }
        return self._auto.execute(session, auto_payload, emit=_relay)

    @staticmethod
    def _upload_outputs(
        stage_dir: Path, store: Any, key_prefix: str
    ) -> tuple[dict[str, str], dict[str, Any], str]:
        """Upload train/eval parquets + manifest, return ``(uploaded, manifest_data, manifest_sha)``.

        Reads manifest bytes while the staging dir still exists so the
        Dataset row can record a content-addressed fingerprint.
        """
        uploaded: dict[str, str] = {}
        for fname in ("train.parquet", "eval.parquet", "manifest.json"):
            p_path = stage_dir / fname
            if p_path.exists():
                uploaded[fname] = store.put(key_prefix + fname, p_path)
        manifest_path = stage_dir / "manifest.json"
        if not manifest_path.exists():
            raise RuntimeError(
                "export_research_dataset: manifest.json missing from stage dir; "
                "dump path did not produce the expected layout"
            )
        manifest_bytes = manifest_path.read_bytes()
        manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()
        manifest_data = json.loads(manifest_bytes)
        return uploaded, manifest_data, manifest_sha

    def _register_dataset(
        self,
        session: Session,
        p: ExportResearchDatasetPayload,
        outcome: _DumpOutcome,
        job_uuid: uuid.UUID | None,
        emit: EmitFn,
    ) -> uuid.UUID:
        """Insert the ``Dataset`` row + emit ``registered``; returns its UUID."""
        manifest_data = outcome.manifest_data
        legacy_schema_sha = manifest_data.get("schema_sha", "")
        dataset = Dataset(
            name=p.output_name,
            operation=self.name,
            job_id=job_uuid,
            storage_backend=outcome.settings.storage_backend,
            key_prefix=outcome.key_prefix,
            train_uri=outcome.uploaded.get("train.parquet"),
            eval_uri=outcome.uploaded.get("eval.parquet"),
            manifest_uri=outcome.uploaded["manifest.json"],
            schema_sha=legacy_schema_sha,
            # T1.6 (ADR D10) dual-write gated by
            # ``PROTEA_SCHEMA_SHA_V2_WRITE_ENABLED``; when off the
            # column stays NULL and the existing read paths keep using
            # ``schema_sha`` unchanged.
            schema_sha_v2=maybe_v2(legacy_schema_sha or None),
            manifest_sha=outcome.manifest_sha,
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
        emit(
            "export_research_dataset.registered",
            None,
            {"dataset_id": str(dataset.id), "name": p.output_name},
            "info",
        )
        return dataset.id


def _resolve_project_root() -> Path:
    # protea/core/operations/export_research_dataset.py → parents[3] = repo root
    return Path(__file__).resolve().parents[3]
