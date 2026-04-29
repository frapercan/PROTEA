"""Benchmark matrix endpoints.

Exposes a per-embedding, per-stage view of every ``EvaluationResult`` in the
database so the UI can render the full PLM comparison grid for the thesis
benchmark.

Where the ``/showcase`` endpoint collapses all models into a few method
buckets and takes the maximum across every embedding, this module preserves
**which** embedding produced each number and **which scoring config** was
used — one stage per distinct ``scoring_config.name`` found in the DB, plus
an implicit ``"reranker"`` stage for evaluations that used a reranker.

Zero domain constants are hardcoded here: stage labels, preferred default,
baseline tag, GO categories and aspects all come from
``protea/config/benchmark.yaml`` via :class:`BenchmarkConfig`. Model display
metadata (display name, family, param count) comes from the dedicated columns
on ``embedding_config`` — no HF-name regex heuristics.

Two endpoints are provided:

``GET /benchmark/embeddings``
    One row per ``EmbeddingConfig`` with its persisted display metadata.

``GET /benchmark/matrix``
    One row per
    ``(embedding_config, evaluation_set, stage, category, aspect)`` tuple —
    best-Fmax only. Response also includes:

    - ``stages``:            every stage observed in the data (with label/kind)
    - ``evaluation_sets``:   per-eval-set metadata (stats, source, obo version)
    - ``best_per_cell``:     cross-model winner per (category, aspect) cell
    - ``categories`` / ``aspects``: from YAML config
"""

from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.orm import Session, aliased, sessionmaker

from protea.api.deps import get_benchmark_config, get_session_factory
from protea.api.stages import RERANKER_STAGE as _RERANKER_STAGE
from protea.api.stages import StageKind  # noqa: F401  (re-exported for type hints)
from protea.api.stages import stage_kind as _stage_kind
from protea.api.stages import stage_of as _stage_of
from protea.infrastructure.benchmark_config import BenchmarkConfig
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.evaluation_result import EvaluationResult
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.embedding.scoring_config import ScoringConfig
from protea.infrastructure.session import session_scope

router = APIRouter(prefix="/benchmark", tags=["benchmark"])


def _stage_sort_index(stage: str, preferred: tuple[str, ...]) -> tuple[int, int, str]:
    """Sort order: preferred (in YAML order) → other scorings (alpha) → reranker."""
    if stage == _RERANKER_STAGE:
        return (2, 0, stage)
    if stage in preferred:
        return (0, preferred.index(stage), stage)
    return (1, 0, stage)


def _embedding_display(cfg: EmbeddingConfig) -> dict[str, Any]:
    """Flatten the persisted display metadata for the API response.

    Falls back to raw ``model_name`` / ``model_backend`` only when the
    explicit columns are ``NULL`` (keeps the response non-empty for
    embeddings that were inserted before the display columns existed).
    """
    return {
        "id": str(cfg.id),
        "model_name": cfg.model_name,
        "model_backend": cfg.model_backend,
        "description": cfg.description,
        "pooling": cfg.pooling,
        "layer_agg": cfg.layer_agg,
        "display_name": cfg.display_name or cfg.model_name,
        "family": cfg.family or cfg.model_backend,
        "param_count": cfg.param_count,
    }


def _eval_set_label(
    es: EvaluationSet,
    old_src: str | None,
    new_src: str | None,
    old_src_version: str | None,
    new_src_version: str | None,
    override: str | None,
) -> str:
    """Human-readable label for an evaluation set.

    Precedence: explicit YAML override → ``source source_version`` delta →
    UUID prefix fallback.
    """
    if override:
        return override
    if old_src and new_src:
        old_tag = f"{old_src} {old_src_version}" if old_src_version else old_src
        new_tag = f"{new_src} {new_src_version}" if new_src_version else new_src
        return f"{old_tag} → {new_tag}"
    return f"eval_set {str(es.id)[:8]}…"


# ── Endpoints ──────────────────────────────────────────────────────────────


@router.get(
    "/embeddings",
    summary="List embedding configs with persisted display metadata",
)
def list_benchmark_embeddings(
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Return every ``EmbeddingConfig`` with its persisted display metadata.

    The metadata lives in ``embedding_config.display_name / family /
    param_count`` — filled at creation time by the seed scripts. No
    heuristic inference happens here.
    """
    with session_scope(factory) as session:
        cfgs = (
            session.execute(select(EmbeddingConfig).order_by(EmbeddingConfig.created_at.asc()))
            .scalars()
            .all()
        )
        out = [_embedding_display(cfg) for cfg in cfgs]
        return {"embeddings": out, "total": len(out)}


@router.get(
    "/matrix",
    summary="Per-embedding / per-stage Fmax matrix across all evaluation results",
)
def get_benchmark_matrix(
    evaluation_set_id: uuid.UUID | None = Query(
        default=None,
        description="If set, restrict rows to this evaluation set only.",
    ),
    stage: str | None = Query(
        default=None,
        description=(
            "If set, restrict rows to this pipeline stage "
            "(any scoring_config.name, or 'reranker')."
        ),
    ),
    k: int | None = Query(
        default=None,
        ge=1,
        description=(
            "If set, restrict rows to PredictionSets computed with this "
            "``limit_per_entry`` (K). Typical values: 3, 5, 10."
        ),
    ),
    factory: sessionmaker[Session] = Depends(get_session_factory),
    cfg: BenchmarkConfig = Depends(get_benchmark_config),
) -> dict[str, Any]:
    """Return a long-format table with one row per
    ``(embedding_config, evaluation_set, stage, category, aspect)`` tuple
    containing the best Fmax / precision / recall observed in the DB,
    plus per-eval-set metadata and a cross-model leaderboard.
    """
    with session_scope(factory) as session:
        # Single query: EvaluationResult joined to PredictionSet → embedding
        # and left-joined to ScoringConfig for the stage name.
        stmt = (
            select(
                EvaluationResult,
                PredictionSet.embedding_config_id,
                PredictionSet.limit_per_entry.label("k"),
                ScoringConfig.name.label("scoring_name"),
            )
            .join(PredictionSet, PredictionSet.id == EvaluationResult.prediction_set_id)
            .outerjoin(
                ScoringConfig, ScoringConfig.id == EvaluationResult.scoring_config_id
            )
        )
        if evaluation_set_id is not None:
            stmt = stmt.where(EvaluationResult.evaluation_set_id == evaluation_set_id)
        if k is not None:
            stmt = stmt.where(PredictionSet.limit_per_entry == k)

        # Dedupe on the Python side: row count is always small (O(hundreds)),
        # and "best-Fmax per cell" is clearest as an in-memory fold.
        # Key now includes K so the same (embedding, stage, cell) tuple yields
        # one row per K (3, 5, 10) instead of collapsing them.
        best: dict[tuple[str, str, str, int, str, str], dict[str, Any]] = {}
        eval_set_ids: set[str] = set()
        embedding_ids: set[str] = set()
        stages_seen: set[str] = set()
        ks_seen: set[int] = set()

        for er, embedding_config_id, row_k, scoring_name in session.execute(stmt).all():
            st = _stage_of(er, scoring_name)
            if st is None or st in cfg.hidden_stages:
                continue
            stages_seen.add(st)
            if stage is not None and st != stage:
                continue

            eid = str(embedding_config_id)
            esid = str(er.evaluation_set_id)
            embedding_ids.add(eid)
            eval_set_ids.add(esid)
            ks_seen.add(int(row_k))
            results = er.results or {}

            for cat in cfg.categories:
                cat_data = results.get(cat) or {}
                if not cat_data:
                    continue
                for asp in cfg.aspects:
                    cell = cat_data.get(asp) or {}
                    fmax = cell.get("fmax")
                    if fmax is None:
                        continue

                    key = (eid, esid, st, int(row_k), cat, asp)
                    cur = best.get(key)
                    if cur is None or fmax > cur["fmax"]:
                        best[key] = {
                            "embedding_config_id": eid,
                            "evaluation_set_id": esid,
                            "stage": st,
                            "k": int(row_k),
                            "category": cat,
                            "aspect": asp,
                            "fmax": round(float(fmax), 4),
                            "precision": _round(cell.get("precision")),
                            "recall": _round(cell.get("recall")),
                            "coverage": _round(cell.get("coverage")),
                            "n_proteins": cell.get("n_proteins"),
                            "evaluation_result_id": str(er.id),
                        }

        # Stable stage ordering based on YAML preferred_default_stages.
        stages_payload = [
            {
                "name": s,
                "label": cfg.label_for_stage(s),
                "kind": _stage_kind(s),
                "is_baseline": s == cfg.baseline_scoring_name,
            }
            for s in sorted(
                stages_seen, key=lambda x: _stage_sort_index(x, cfg.preferred_default_stages)
            )
        ]

        rows = sorted(
            best.values(),
            key=lambda r: (
                r["evaluation_set_id"],
                _stage_sort_index(r["stage"], cfg.preferred_default_stages),
                r["embedding_config_id"],
                r["k"],
                r["category"],
                r["aspect"],
            ),
        )

        # Cross-model leaderboard: for each (cat, asp) cell, pick the single
        # best row across every embedding and stage currently selected. When
        # a stage filter is active, the leaderboard is naturally restricted
        # to that stage — same semantics as the main table.
        leaderboard: dict[tuple[str, str], dict[str, Any]] = {}
        for r in rows:
            lkey = (r["category"], r["aspect"])
            cur = leaderboard.get(lkey)
            if cur is None or r["fmax"] > cur["fmax"]:
                leaderboard[lkey] = r
        best_per_cell = [
            {
                "category": cat,
                "aspect": asp,
                "fmax": entry["fmax"],
                "precision": entry["precision"],
                "recall": entry["recall"],
                "coverage": entry["coverage"],
                "embedding_config_id": entry["embedding_config_id"],
                "k": entry["k"],
                "stage": entry["stage"],
                "evaluation_result_id": entry["evaluation_result_id"],
                "evaluation_set_id": entry["evaluation_set_id"],
            }
            for cat in cfg.categories
            for asp in cfg.aspects
            if (entry := leaderboard.get((cat, asp))) is not None
        ]

        # Enrich eval set metadata. Only fetch the ones actually present in
        # the filtered result — the UI selector draws from the full catalog,
        # which the frontend gets via an unfiltered call.
        eval_sets_payload: list[dict[str, Any]] = []
        if eval_set_ids:
            old_as = aliased(AnnotationSet)
            new_as = aliased(AnnotationSet)
            old_os = aliased(OntologySnapshot)
            new_os = aliased(OntologySnapshot)
            es_stmt = (
                select(
                    EvaluationSet,
                    old_as.source.label("old_source"),
                    old_as.source_version.label("old_src_version"),
                    new_as.source.label("new_source"),
                    new_as.source_version.label("new_src_version"),
                    old_os.obo_version.label("old_obo"),
                    new_os.obo_version.label("new_obo"),
                )
                .join(old_as, old_as.id == EvaluationSet.old_annotation_set_id)
                .join(new_as, new_as.id == EvaluationSet.new_annotation_set_id)
                .outerjoin(old_os, old_os.id == old_as.ontology_snapshot_id)
                .outerjoin(new_os, new_os.id == new_as.ontology_snapshot_id)
                .where(EvaluationSet.id.in_([uuid.UUID(x) for x in eval_set_ids]))
            )
            for es, old_s, old_sv, new_s, new_sv, old_obo, new_obo in session.execute(
                es_stmt
            ).all():
                esid = str(es.id)
                stats = es.stats or {}
                eval_sets_payload.append(
                    {
                        "id": esid,
                        "label": _eval_set_label(
                            es, old_s, new_s, old_sv, new_sv,
                            cfg.eval_set_labels.get(esid),
                        ),
                        "old_source": old_s,
                        "old_source_version": old_sv,
                        "new_source": new_s,
                        "new_source_version": new_sv,
                        "old_obo_version": old_obo,
                        "new_obo_version": new_obo,
                        "stats": stats,
                    }
                )
            eval_sets_payload.sort(key=lambda e: e["label"])

        return {
            "rows": rows,
            "total": len(rows),
            "evaluation_sets": eval_sets_payload,
            "embedding_config_ids": sorted(embedding_ids),
            "stages": stages_payload,
            "categories": list(cfg.categories),
            "aspects": list(cfg.aspects),
            "ks": sorted(ks_seen),
            "best_per_cell": best_per_cell,
            "filters": {
                "evaluation_set_id": str(evaluation_set_id) if evaluation_set_id else None,
                "stage": stage,
                "k": k,
            },
        }


def _round(v: Any) -> float | None:
    return round(float(v), 4) if v is not None else None
