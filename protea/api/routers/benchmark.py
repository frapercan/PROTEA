"""Benchmark matrix endpoints.

Exposes a per-embedding, per-stage view of every ``EvaluationResult`` in the
database so the UI can render the full PLM comparison grid for the thesis
benchmark.

Where the ``/showcase`` endpoint collapses all models into a few method
buckets and takes the maximum across every embedding, this module preserves
**which** embedding produced each number and **which scoring config** was
used: one stage per distinct ``scoring_config.name`` found in the DB, plus
an implicit ``"reranker"`` stage for evaluations that used a reranker.

Zero domain constants are hardcoded here: stage labels, preferred default,
baseline tag, GO categories and aspects all come from
``protea/config/benchmark.yaml`` via :class:`BenchmarkConfig`. Model display
metadata (display name, family, param count) comes from the dedicated columns
on ``embedding_config``; no HF-name regex heuristics.

Two endpoints are provided:

``GET /benchmark/embeddings``
    One row per ``EmbeddingConfig`` with its persisted display metadata.

``GET /benchmark/matrix``
    One row per
    ``(embedding_config, evaluation_set, stage, category, aspect)`` tuple,
    best-Fmax only. Response also includes:

    - ``stages``:            every stage observed in the data (with label/kind)
    - ``evaluation_sets``:   per-eval-set metadata (stats, source, obo version)
    - ``best_per_cell``:     cross-model winner per (category, aspect) cell
                             within the active stage/K filter selection
    - ``best_per_cell_global``: same shape as ``best_per_cell`` but ignores the
                                user's stage/K filters. Stable across filter
                                changes; the per-cell champion across the entire
                                dataset for the current evaluation set.
    - ``categories`` / ``aspects``: from YAML config
"""

from __future__ import annotations

import uuid
from typing import Any, NamedTuple

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.orm import Session, aliased, sessionmaker

from protea.api.cache import cached, invalidate
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

# Primary ranking metric. The IA-weighted micro-averaged F (``f_micro_w``)
# is the LAFA / CAFA headline metric and the only one comparable to the
# external leaderboards (FIX-METRIC-IA). Rows evaluated before a real IA
# file was wired carry no ``f_micro_w``; for those we fall back to the
# unweighted ``fmax`` so historical cells still rank, flagged via
# ``primary_metric`` in the row payload.
PRIMARY_METRIC = "f_micro_w"
FALLBACK_METRIC = "fmax"


def _primary_score(cell: dict[str, Any]) -> tuple[float, str]:
    """Return ``(score, metric_name)`` for one ``(category, aspect)`` cell.

    Prefers the IA-weighted ``f_micro_w``; falls back to the unweighted
    ``fmax`` when the cell predates real-IA evaluation. Returns the metric
    name so the caller can label which number is driving the ranking.
    """
    fw = cell.get(PRIMARY_METRIC)
    if fw is not None:
        return float(fw), PRIMARY_METRIC
    fmax = cell.get(FALLBACK_METRIC)
    if fmax is not None:
        return float(fmax), FALLBACK_METRIC
    return 0.0, FALLBACK_METRIC

BENCHMARK_EMBEDDINGS_CACHE_KEY = "benchmark:embeddings"
BENCHMARK_EMBEDDINGS_TTL_SECONDS = 300.0
BENCHMARK_MATRIX_CACHE_KEY = "benchmark:matrix"
BENCHMARK_MATRIX_TTL_SECONDS = 300.0

# (embedding_id, evaluation_set_id, stage, k, category, aspect)
_BestKey = tuple[str, str, str, int, str, str]


class _RowKey(NamedTuple):
    """Identification tuple for one EvaluationResult row in the benchmark scan."""

    eid: str
    esid: str
    st: str
    row_k: int


class _BenchAggregation(NamedTuple):
    """Per-cell winners + observed dimensions extracted from one stmt scan."""

    best: dict[_BestKey, dict[str, Any]]
    best_global: dict[_BestKey, dict[str, Any]]
    embedding_ids: set[str]
    eval_set_ids: set[str]
    stages_seen: set[str]
    ks_seen: set[int]


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


def _make_leaderboard(
    rows: list[dict[str, Any]],
    categories: tuple[str, ...] | list[str],
    aspects: tuple[str, ...] | list[str],
) -> list[dict[str, Any]]:
    """Cross-model best-cell leaderboard per ``(category, aspect)`` cell.

    Iterates a flat list of row dicts (each containing at least ``category``,
    ``aspect`` and ``primary``) and returns one entry per cell with the
    winning embedding / stage / K, in canonical (categories × aspects) order.
    The winner is the cell with the highest IA-weighted ``primary`` metric
    (``f_micro_w``, fmax fallback for legacy rows). This is the *best-cell*
    maximum and must be labelled as such by the front-end, never as the
    headline number (winner's-curse, FIX-METRIC-IA).
    """
    leaderboard: dict[tuple[str, str], dict[str, Any]] = {}
    for r in rows:
        lkey = (r["category"], r["aspect"])
        cur = leaderboard.get(lkey)
        if cur is None or r["primary"] > cur["primary"]:
            leaderboard[lkey] = r
    return [
        {
            "category": cat,
            "aspect": asp,
            "primary": entry["primary"],
            "primary_metric": entry["primary_metric"],
            "f_micro_w": entry.get("f_micro_w"),
            "precision_w": entry.get("precision_w"),
            "recall_w": entry.get("recall_w"),
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
        for cat in categories
        for asp in aspects
        if (entry := leaderboard.get((cat, asp))) is not None
    ]


def _per_task_aggregate(
    rows: list[dict[str, Any]],
    categories: tuple[str, ...] | list[str],
    aspects: tuple[str, ...] | list[str],
) -> list[dict[str, Any]]:
    """Honest per-task summary: mean primary metric across models per cell.

    The best-cell leaderboard reports the single highest model per
    ``(category, aspect)``; on its own that is the winner's-curse headline
    the dashboard must stop leading with (FIX-METRIC-IA). This aggregate
    reports, per task, the mean of the primary metric across all models in
    the current selection plus a normal-approximation 95% CI half-width
    (``1.96 * sd / sqrt(n)``), so the front-end can headline a calibrated
    central tendency and only label the maximum as best-cell.
    """
    import math

    grouped: dict[tuple[str, str], list[float]] = {}
    for r in rows:
        grouped.setdefault((r["category"], r["aspect"]), []).append(float(r["primary"]))
    out: list[dict[str, Any]] = []
    for cat in categories:
        for asp in aspects:
            vals = grouped.get((cat, asp))
            if not vals:
                continue
            n = len(vals)
            mean = sum(vals) / n
            if n > 1:
                var = sum((v - mean) ** 2 for v in vals) / (n - 1)
                ci = 1.96 * math.sqrt(var) / math.sqrt(n)
            else:
                ci = 0.0
            out.append(
                {
                    "category": cat,
                    "aspect": asp,
                    "metric": PRIMARY_METRIC,
                    "mean": round(mean, 4),
                    "ci95": round(ci, 4),
                    "max": round(max(vals), 4),
                    "min": round(min(vals), 4),
                    "n_models": n,
                }
            )
    return out


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


def _compute_benchmark_embeddings(
    factory: sessionmaker[Session],
    hidden_embeddings: frozenset[str] = frozenset(),
) -> dict[str, Any]:
    """Run the ``EmbeddingConfig`` SELECT and shape the response payload.

    Module-scope so the prewarm hook can share the exact same producer
    the route caches under. ``hidden_embeddings`` (lowercase UUID strings
    from ``benchmark.yaml``) are omitted so the matrix shows only the
    canonical PLMs without empty / duplicate rows.
    """
    with session_scope(factory) as session:
        cfgs = (
            session.execute(select(EmbeddingConfig).order_by(EmbeddingConfig.created_at.asc()))
            .scalars()
            .all()
        )
        out = [
            _embedding_display(cfg) for cfg in cfgs if str(cfg.id).lower() not in hidden_embeddings
        ]
        return {"embeddings": out, "total": len(out)}


@router.get(
    "/embeddings",
    summary="List embedding configs with persisted display metadata",
)
def list_benchmark_embeddings(
    factory: sessionmaker[Session] = Depends(get_session_factory),
    cfg: BenchmarkConfig = Depends(get_benchmark_config),
) -> dict[str, Any]:
    """Return every ``EmbeddingConfig`` with its persisted display metadata.

    The metadata lives in ``embedding_config.display_name / family /
    param_count``: filled at creation time by the seed scripts. No
    heuristic inference happens here. Configs listed in
    ``benchmark.yaml: hidden_embeddings`` are suppressed. Cached for 5 min;
    the benchmark page is the first router touch on a fresh deploy so cold
    pg pages push this past several seconds without the cache.
    """
    return cached(
        BENCHMARK_EMBEDDINGS_CACHE_KEY,
        BENCHMARK_EMBEDDINGS_TTL_SECONDS,
        lambda: _compute_benchmark_embeddings(factory, cfg.hidden_embeddings),
        serve_stale_on_error=True,
    )


def prewarm_benchmark_embeddings(
    factory: sessionmaker[Session],
) -> dict[str, Any]:
    """Recompute and store ``benchmark:embeddings`` for the lifespan
    prewarm hook + background refresh loop."""
    from pathlib import Path

    from protea.infrastructure.benchmark_config import load_benchmark_config

    project_root = Path(__file__).resolve().parents[3]
    cfg = load_benchmark_config(project_root)
    invalidate(BENCHMARK_EMBEDDINGS_CACHE_KEY)
    return cached(
        BENCHMARK_EMBEDDINGS_CACHE_KEY,
        BENCHMARK_EMBEDDINGS_TTL_SECONDS,
        lambda: _compute_benchmark_embeddings(factory, cfg.hidden_embeddings),
    )


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
            "If set, restrict rows to this pipeline stage (any scoring_config.name, or 'reranker')."
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
        agg = _aggregate_benchmark_matrix(session, evaluation_set_id, cfg, stage, k)
        eval_sets_payload = _load_eval_set_metadata(session, agg.eval_set_ids, cfg)
    return _build_matrix_response(agg, eval_sets_payload, cfg, evaluation_set_id, stage, k)


def prewarm_benchmark_matrix(
    factory: sessionmaker[Session],
) -> None:
    """Pre-warm the benchmark matrix by running the full EvaluationResult
    scan with no filters. The underlying pg statement is filter-agnostic
    (stage/K filters are applied in Python on the materialised rows), so
    one warm pass populates the buffer cache for every filtered variant
    the UI requests next. The /matrix endpoint itself is not response-
    cached: pg-pages-hot is enough to keep the live response sub-100ms,
    and filter combos multiply too much for a useful in-process cache."""
    from pathlib import Path

    from protea.infrastructure.benchmark_config import load_benchmark_config

    project_root = Path(__file__).resolve().parents[3]
    cfg = load_benchmark_config(project_root)
    with session_scope(factory) as session:
        _aggregate_benchmark_matrix(session, None, cfg, None, None)


def _build_matrix_response(
    agg: _BenchAggregation,
    eval_sets_payload: list[dict[str, Any]],
    cfg: BenchmarkConfig,
    evaluation_set_id: uuid.UUID | None,
    stage: str | None,
    k: int | None,
) -> dict[str, Any]:
    """Compose the final ``/matrix`` response: stages payload, sorted rows,
    selection + global leaderboards, and the echo of active filters."""
    stages_payload = [
        {
            "name": s,
            "label": cfg.label_for_stage(s),
            "kind": _stage_kind(s),
            "is_baseline": s == cfg.baseline_scoring_name,
        }
        for s in sorted(
            agg.stages_seen, key=lambda x: _stage_sort_index(x, cfg.preferred_default_stages)
        )
    ]
    rows = sorted(
        agg.best.values(),
        key=lambda r: (
            r["evaluation_set_id"],
            _stage_sort_index(r["stage"], cfg.preferred_default_stages),
            r["embedding_config_id"],
            r["k"],
            r["category"],
            r["aspect"],
        ),
    )
    # Two leaderboards: one for the user's current selection (matches the
    # main table), one global across every stage/K (constant across filter
    # changes — anchors the "is this the absolute champion?" read).
    best_per_cell = _make_leaderboard(rows, cfg.categories, cfg.aspects)
    best_per_cell_global = _make_leaderboard(
        list(agg.best_global.values()), cfg.categories, cfg.aspects
    )
    per_task = _per_task_aggregate(rows, cfg.categories, cfg.aspects)
    return {
        "rows": rows,
        "total": len(rows),
        "primary_metric": PRIMARY_METRIC,
        "evaluation_sets": eval_sets_payload,
        "embedding_config_ids": sorted(agg.embedding_ids),
        "stages": stages_payload,
        "categories": list(cfg.categories),
        "aspects": list(cfg.aspects),
        "ks": sorted(agg.ks_seen),
        "per_task": per_task,
        "best_per_cell": best_per_cell,
        "best_per_cell_global": best_per_cell_global,
        "filters": {
            "evaluation_set_id": str(evaluation_set_id) if evaluation_set_id else None,
            "stage": stage,
            "k": k,
        },
    }


def _benchmark_aggregation_stmt(evaluation_set_id: uuid.UUID | None) -> Any:
    """Build the row-fetching select for the benchmark matrix.

    Returns a Select that yields ``(EvaluationResult, embedding_config_id,
    k, scoring_name)`` rows, optionally filtered to a single
    ``evaluation_set_id``. Kept separate from
    :func:`_aggregate_benchmark_matrix` so the SQL surface is auditable
    in isolation and the matrix folder stays under the §3 60-LOC ceiling.
    """
    stmt = (
        select(
            EvaluationResult,
            PredictionSet.embedding_config_id,
            PredictionSet.limit_per_entry.label("k"),
            ScoringConfig.name.label("scoring_name"),
        )
        .join(PredictionSet, PredictionSet.id == EvaluationResult.prediction_set_id)
        .outerjoin(ScoringConfig, ScoringConfig.id == EvaluationResult.scoring_config_id)
    )
    if evaluation_set_id is not None:
        stmt = stmt.where(EvaluationResult.evaluation_set_id == evaluation_set_id)
    return stmt


def _aggregate_benchmark_matrix(
    session: Session,
    evaluation_set_id: uuid.UUID | None,
    cfg: BenchmarkConfig,
    stage: str | None,
    k: int | None,
) -> _BenchAggregation:
    """Run the EvaluationResult × PredictionSet × ScoringConfig scan and
    fold per-cell winners into ``best`` (filter-aware) and ``best_global``
    (filter-agnostic, still honouring ``hidden_stages`` + ``evaluation_set_id``).

    Stage and K filters are applied in Python so the global leaderboard
    sees every row that passed ``hidden_stages``.
    """
    stmt = _benchmark_aggregation_stmt(evaluation_set_id)

    best: dict[_BestKey, dict[str, Any]] = {}
    best_global: dict[_BestKey, dict[str, Any]] = {}
    embedding_ids: set[str] = set()
    eval_set_ids: set[str] = set()
    stages_seen: set[str] = set()
    ks_seen: set[int] = set()
    for er, embedding_config_id, row_k, scoring_name in session.execute(stmt).all():
        eid = str(embedding_config_id)
        if eid.lower() in cfg.hidden_embeddings:
            continue
        st = _stage_of(er, scoring_name)
        if st is None or st in cfg.hidden_stages:
            continue
        stages_seen.add(st)
        ks_seen.add(int(row_k))
        passes_filter = (stage is None or st == stage) and (k is None or int(row_k) == k)
        esid = str(er.evaluation_set_id)
        embedding_ids.add(eid)
        eval_set_ids.add(esid)
        _fold_evaluation_cells(
            er,
            _RowKey(eid=eid, esid=esid, st=st, row_k=int(row_k)),
            cfg,
            best,
            best_global,
            passes_filter,
        )
    return _BenchAggregation(
        best=best,
        best_global=best_global,
        embedding_ids=embedding_ids,
        eval_set_ids=eval_set_ids,
        stages_seen=stages_seen,
        ks_seen=ks_seen,
    )


def _fold_evaluation_cells(
    er: EvaluationResult,
    row: _RowKey,
    cfg: BenchmarkConfig,
    best: dict[_BestKey, dict[str, Any]],
    best_global: dict[_BestKey, dict[str, Any]],
    passes_filter: bool,
) -> None:
    """For one EvaluationResult row, walk every ``(category, aspect)`` cell
    and update ``best`` / ``best_global`` with the per-cell winner."""
    results = er.results or {}
    for cat in cfg.categories:
        cat_data = results.get(cat) or {}
        if not cat_data:
            continue
        for asp in cfg.aspects:
            cell = cat_data.get(asp) or {}
            if cell.get("fmax") is None:
                continue
            primary, primary_metric = _primary_score(cell)
            key: _BestKey = (row.eid, row.esid, row.st, row.row_k, cat, asp)
            payload = {
                "embedding_config_id": row.eid,
                "evaluation_set_id": row.esid,
                "stage": row.st,
                "k": row.row_k,
                "category": cat,
                "aspect": asp,
                "primary": round(primary, 4),
                "primary_metric": primary_metric,
                "f_micro_w": _round(cell.get("f_micro_w")),
                "precision_w": _round(cell.get("precision_w")),
                "recall_w": _round(cell.get("recall_w")),
                "fmax": round(float(cell["fmax"]), 4),
                "precision": _round(cell.get("precision")),
                "recall": _round(cell.get("recall")),
                "coverage": _round(cell.get("coverage")),
                "n_proteins": cell.get("n_proteins"),
                "evaluation_result_id": str(er.id),
            }
            cur_g = best_global.get(key)
            if cur_g is None or payload["primary"] > cur_g["primary"]:
                best_global[key] = payload
            if passes_filter:
                cur = best.get(key)
                if cur is None or payload["primary"] > cur["primary"]:
                    best[key] = payload


def _load_eval_set_metadata(
    session: Session, eval_set_ids: set[str], cfg: BenchmarkConfig
) -> list[dict[str, Any]]:
    """Enrich every EvaluationSet present in the filtered result with its
    source / version / OBO metadata + persisted ``stats``. Returns an empty
    list when ``eval_set_ids`` is empty so the response shape stays stable."""
    if not eval_set_ids:
        return []
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
    payload: list[dict[str, Any]] = []
    for es, old_s, old_sv, new_s, new_sv, old_obo, new_obo in session.execute(es_stmt).all():
        esid = str(es.id)
        payload.append(
            {
                "id": esid,
                "label": _eval_set_label(
                    es, old_s, new_s, old_sv, new_sv, cfg.eval_set_labels.get(esid)
                ),
                "old_source": old_s,
                "old_source_version": old_sv,
                "new_source": new_s,
                "new_source_version": new_sv,
                "old_obo_version": old_obo,
                "new_obo_version": new_obo,
                "stats": es.stats or {},
            }
        )
    payload.sort(key=lambda e: e["label"])
    return payload


def _round(v: Any) -> float | None:
    return round(float(v), 4) if v is not None else None
