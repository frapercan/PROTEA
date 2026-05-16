"""Per-execution Method Object plus the public auto operation.

Extracted from ``protea/core/training_dump_helpers.py`` as part of
T2B.6. Behaviour is unchanged.
"""

from __future__ import annotations

import gc
import shutil
import tempfile
import time
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy.orm import Session

from protea.core._training_dump_loaders import (
    _check_reranker_name_collisions,
    _collect_cat_gt_pairs,
    _DumpRequest,
    _load_go_maps,
    _load_ia_weights,
    _maybe_fit_pca_state,
    _perform_dataset_dump,
    _resolve_annotation_set_ids,
    _resolve_test_eval_inputs,
    _TestSplitContext,
    _TrainSplitContext,
)
from protea.core.contracts.operation import EmitFn, OperationResult
from protea.core.domain.aspect import ASPECT_CODES as _ASPECTS
from protea.core.reranker import ALL_FEATURES, LABEL_COLUMN
from protea.core.training_dump._constants import _ASPECT_NAMES, _CATEGORIES
from protea.core.training_dump._data_loaders import (
    _load_parent_map,
    _preload_all_embeddings,
)
from protea.core.training_dump._payload import TrainRerankerAutoPayload
from protea.core.training_dump._test_split import _run_test_split
from protea.core.training_dump._train_split import _run_train_split
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig

if TYPE_CHECKING:
    from protea.core.parquet_export import ParquetExportContext


class _DumpRunner:
    """Method Object for ``TrainRerankerAutoOperation.execute``.

    Holds per-run state as attributes so the per-phase methods do
    not need to thread 13+ variables through their signatures.
    """

    def __init__(
        self,
        session: Session,
        payload: dict[str, Any],
        emit: EmitFn,
        dump_fn: Any,
    ) -> None:
        self.session = session
        self.p = TrainRerankerAutoPayload.model_validate(payload)
        self.emit = emit
        self._dump_fn = dump_fn
        self.t0 = time.perf_counter()

    def run(self) -> OperationResult:
        self._resolve_setup()
        self._load_inputs()
        self._init_accumulators()
        self.tmp_dir = Path(tempfile.mkdtemp(prefix="protea_reranker_"))
        try:
            self._run_train_splits()
            if not any(self.split_files[c] for c in _CATEGORIES):
                raise ValueError("No training data produced from any split")
            self._run_test_split()
            del self.all_embeddings, self.all_accessions, self.acc_to_idx
            gc.collect()
            return self._dump()
        finally:
            shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def _resolve_setup(self) -> None:
        """Phase 1: validate IDs, resolve annotation sets, emit start."""
        self.emb_config_id = uuid.UUID(self.p.embedding_config_id)
        self.ontology_snapshot_id = uuid.UUID(self.p.ontology_snapshot_id)
        all_versions = sorted(set(self.p.train_versions + self.p.test_versions))
        self.version_to_set, self.version_to_native = _resolve_annotation_set_ids(
            self.session, self.p.annotation_source, all_versions
        )
        if self.session.get(EmbeddingConfig, self.emb_config_id) is None:
            raise ValueError(f"EmbeddingConfig {self.emb_config_id} not found")
        candidate_names: list[str] = [f"{self.p.name}-{cat}" for cat in _CATEGORIES]
        if self.p.training_scope == "per_cell":
            candidate_names.extend(
                f"{self.p.name}-{cat}-{_ASPECT_NAMES[asp]}"
                for cat in _CATEGORIES
                for asp in _ASPECTS
            )
        if not self.p.dump_only:
            _check_reranker_name_collisions(self.session, candidate_names)
        self.ia_weights = _load_ia_weights(self.p.ia_file)
        if self.ia_weights is not None:
            self.emit(
                "dump_helper.ia_loaded",
                None,
                {"ia_file": self.p.ia_file, "n_terms": len(self.ia_weights)},
                "info",
            )
        self.emit(
            "dump_helper.start",
            None,
            {
                "name": self.p.name,
                "train_versions": self.p.train_versions,
                "test_versions": self.p.test_versions,
                "n_pairs": len(self.p.train_versions) - 1,
                "training_scope": self.p.training_scope,
                "max_models": len(candidate_names),
                "per_cell_min_positives": int(self.p.per_cell_min_positives)
                if self.p.training_scope == "per_cell"
                else None,
                "ia_weighted": self.ia_weights is not None,
            },
            "info",
        )

    def _load_inputs(self) -> None:
        """Phase 2: GO maps, parent map, embedding preload, optional PCA fit."""
        self.go_id_map, self.aspect_map, self.pivot_go_ids = _load_go_maps(
            self.session,
            self.ontology_snapshot_id,
            set(self.version_to_native.values()),
        )
        self.parent_map = _load_parent_map(self.session, self.ontology_snapshot_id)
        self.all_embeddings, self.all_accessions, self.acc_to_idx = _preload_all_embeddings(
            self.session, self.emb_config_id, self.emit
        )
        self.pca_state = _maybe_fit_pca_state(
            self.emb_config_id, self.all_embeddings, self.p.use_embedding_pca, self.emit
        )

    def _init_accumulators(self) -> None:
        """Initialise per-split accumulator state."""
        self.keep_cols: list[str] = (
            ["protein_accession", "go_id"] + ALL_FEATURES + [LABEL_COLUMN]
        )
        self.per_split_stats: list[dict[str, Any]] = []
        self.split_files: dict[str, list[Path]] = {c: [] for c in _CATEGORIES}
        self.valid_split_versions: list[tuple[int, int]] = []

    def _train_split_context(self) -> _TrainSplitContext:
        return _TrainSplitContext(
            payload=self.p,
            version_to_set=self.version_to_set,
            embedding_pool=self.all_embeddings,
            all_accessions=self.all_accessions,
            acc_to_idx=self.acc_to_idx,
            go_id_map=self.go_id_map,
            aspect_map=self.aspect_map,
            parent_map=self.parent_map,
            ia_weights=self.ia_weights,
            pca_state=self.pca_state,
            pivot_go_ids=self.pivot_go_ids,
            keep_cols=self.keep_cols,
            tmp_dir=self.tmp_dir,
        )

    def _run_train_splits(self) -> None:
        train_ctx = self._train_split_context()
        for i in range(len(self.p.train_versions) - 1):
            outcome = _run_train_split(self.session, train_ctx, i, self.emit)
            for cat, path in outcome.split_files.items():
                self.split_files[cat].append(path)
            if not outcome.skipped:
                self.valid_split_versions.append(
                    (self.p.train_versions[i], self.p.train_versions[i + 1])
                )
            self.per_split_stats.append(outcome.stats)

    def _run_test_split(self) -> None:
        _eset, test_eval_data, self.test_old_v, self.test_new_v = _resolve_test_eval_inputs(
            self.session, self.p.train_versions, self.p.test_versions, self.version_to_set
        )
        test_old_set_id = self.version_to_set[self.test_old_v]
        self.emit(
            "dump_helper.test_knn",
            None,
            {"test_old": self.test_old_v, "test_new": self.test_new_v},
            "info",
        )
        test_cat_gt, test_all_queries = _collect_cat_gt_pairs(test_eval_data)
        self.test_files = _run_test_split(
            self.session,
            _TestSplitContext(
                payload=self.p,
                test_eval_data=test_eval_data,
                test_cat_gt=test_cat_gt,
                test_all_queries=test_all_queries,
                test_old_set_id=test_old_set_id,
                embedding_pool=self.all_embeddings,
                all_accessions=self.all_accessions,
                acc_to_idx=self.acc_to_idx,
                go_id_map=self.go_id_map,
                aspect_map=self.aspect_map,
                parent_map=self.parent_map,
                ia_weights=self.ia_weights,
                pca_state=self.pca_state,
                pivot_go_ids=self.pivot_go_ids,
                keep_cols=self.keep_cols,
                tmp_dir=self.tmp_dir,
            ),
            self.emit,
        )

    def _dump(self) -> OperationResult:
        return _perform_dataset_dump(
            _DumpRequest(
                payload=self.p,
                split_files=self.split_files,
                valid_split_versions=self.valid_split_versions,
                test_files=self.test_files,
                test_old_v=self.test_old_v,
                test_new_v=self.test_new_v,
                emb_config_id=self.emb_config_id,
                ontology_snapshot_id=self.ontology_snapshot_id,
            ),
            self._dump_fn,
            self.t0,
            self.emit,
        )


class TrainRerankerAutoOperation:
    """Automated multi-split temporal holdout re-ranker training.

    Trains **3 per-category models** (NK, LK, PK) in a single execution.
    Each model trains on all aspects combined, giving it ~3x more data
    than per-aspect models and better convergence.

    Pipeline:

    1. Resolve annotation set IDs from version numbers.
    2. Load GO maps once; optionally load IA weights for sample weighting.
    3. For each consecutive pair in ``train_versions``, compute the
       evaluation delta (all 3 categories at once), load references and
       query embeddings, run KNN + GO transfer, and label predictions
       against each category's ground truth.
    4. For each category (NK, LK, PK), concatenate the labeled data from
       all splits, train one LightGBM model with optional IA sample
       weights, evaluate on the test split, and store a ``RerankerModel``
       as ``{name}-{category}``.
    """

    # Unregistered since LightGBM training moved to protea-reranker-lab.
    # Kept as in-process helper invoked from ExportResearchDatasetOperation.
    name = "research_dataset_dump_helper"
    description = (
        "Run KNN + feature generation across multiple temporal holdout "
        "pairs and emit frozen parquets. Originally also trained "
        "LightGBM models; that path now lives in protea-reranker-lab."
    )

    def summarize_payload(self, payload: dict[str, Any], *, session: Session | None = None) -> str:
        p = payload or {}
        bits: list[str] = []
        if p.get("name"):
            bits.append(str(p["name"]))

        cfg_id_raw = p.get("embedding_config_id")
        if cfg_id_raw and session is not None:
            try:
                cfg = session.get(EmbeddingConfig, uuid.UUID(str(cfg_id_raw)))
            except Exception:
                cfg = None
            if cfg is not None:
                model_label = cfg.display_name or cfg.model_name or str(cfg.id)[:8]
                bits.append(model_label)

        train = p.get("train_versions") or []
        test = p.get("test_versions") or []
        if train:
            bits.append(f"train={train[0]}→{train[-1]} (n={len(train)})")
        if test:
            bits.append(f"test={','.join(str(v) for v in test)}")
        if p.get("num_boost_round"):
            bits.append(f"rounds={p['num_boost_round']}")
        return " · ".join(bits)

    @staticmethod
    def _dump_frozen_dataset(ctx: ParquetExportContext) -> dict[str, Any]:
        """Thin wrapper that delegates to ``parquet_export``; kept so
        ``dump_helper`` can still dump a frozen dataset to a local
        path via ``dump_to=...``. New code should prefer the
        ``export_research_dataset`` operation which publishes via the
        configured ``ArtifactStore``.

        The caller is responsible for filling ``producer_version`` and
        ``producer_git_sha`` on the context. Pass ``store=None`` to
        skip artifact-store upload.
        """
        from protea.core.parquet_export import export_reranker_parquets

        result = export_reranker_parquets(ctx)
        # Preserve the historical return contract: callers rely on
        # ``dump_dir`` instead of ``stage_dir``.
        result["dump_dir"] = result.pop("stage_dir", str(ctx.stage_dir))
        return result

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        return _DumpRunner(session, payload, emit, self._dump_frozen_dataset).run()
