"""Analytical breakdown endpoints behind the /proteins/stats dashboard.

Sits alongside ``proteins.py`` so the deep-dive panels (per-PLM embedding
coverage, GO matrix, taxonomy top-20, sequence-length histogram, GO term
density, pipeline activity, dataset registry) get their own router, their
own cache keys, and their own TTLs without bloating the core proteins
file past the §3 800-LOC ceiling.

Every endpoint follows the same pattern as ``/proteins/stats``:

- a ``_compute_*`` helper does the raw SQL and shapes the payload;
- the route delegates to :func:`protea.api.cache.cached` with a per-section
  TTL that matches data volatility;
- ``serve_stale_on_error=True`` so a transient DB blip degrades to a
  slightly-out-of-date payload instead of a 500 that blocks the page;
- a sibling ``prewarm_*`` is exported so ``protea.api.app`` can rebuild
  each cache shortly before its TTL expires (no cold-path latency in
  steady-state operation).
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends
from sqlalchemy import distinct, func
from sqlalchemy.orm import Session, sessionmaker

from protea.api.cache import cached, invalidate
from protea.api.deps import get_session_factory
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.protein_go_annotation import ProteinGOAnnotation
from protea.infrastructure.orm.models.embedding.dataset import Dataset
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding
from protea.infrastructure.orm.models.job import Job
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.sequence.sequence import Sequence
from protea.infrastructure.session import session_scope

router = APIRouter(prefix="/proteins/stats", tags=["proteins"])

# Cache keys + TTLs (seconds). Volatility-matched: registry rarely moves
# vs pipeline activity which changes by the minute.
CACHE_KEY_PLM = "proteins:stats:embeddings-by-plm"
CACHE_KEY_GO_MATRIX = "proteins:stats:go-annotations-matrix"
CACHE_KEY_TAXONOMY = "proteins:stats:taxonomy-top-20"
CACHE_KEY_SEQ_LEN = "proteins:stats:sequence-length"
CACHE_KEY_GO_DENSITY = "proteins:stats:go-density"
CACHE_KEY_PIPELINE = "proteins:stats:pipeline-activity"
CACHE_KEY_DATASETS = "proteins:stats:dataset-registry"

TTL_PLM = 600.0
TTL_GO_MATRIX = 600.0
TTL_TAXONOMY = 3600.0
TTL_SEQ_LEN = 3600.0
TTL_GO_DENSITY = 600.0
TTL_PIPELINE = 60.0
TTL_DATASETS = 300.0

# Experimental evidence codes (CAFA-style).
_EXPERIMENTAL_CODES = {"EXP", "IDA", "IMP", "IGI", "IEP", "IPI"}

# Histogram bin count for sequence-length panel.
_SEQ_LEN_BINS = 50

# Pipeline activity window: last N days bucketed by day.
_PIPELINE_DAYS = 30


# ── Embeddings by PLM ────────────────────────────────────────────────────────


def _compute_embeddings_by_plm(factory: sessionmaker[Session]) -> dict[str, Any]:
    """Per-config coverage = distinct proteins with at least one embedding.

    GROUPs ``sequence_embedding`` by ``embedding_config_id``, JOINs back to
    ``protein`` via ``sequence_id`` so the count reflects PROTEIN coverage
    (sequence-dedup is folded back out). Total uses the canonical Swiss-Prot
    population (``is_canonical=True AND reviewed=True``) so the percent
    matches what the surrounding cards report.
    """
    with session_scope(factory) as session:
        total = (
            session.query(func.count(Protein.accession))
            .filter(Protein.is_canonical.is_(True))
            .filter(Protein.reviewed.is_(True))
            .scalar()
            or 0
        )
        rows = (
            session.query(
                EmbeddingConfig.id,
                EmbeddingConfig.model_name,
                EmbeddingConfig.display_name,
                EmbeddingConfig.family,
                EmbeddingConfig.param_count,
                func.count(distinct(Protein.accession)),
            )
            .join(SequenceEmbedding, SequenceEmbedding.embedding_config_id == EmbeddingConfig.id)
            .join(Protein, Protein.sequence_id == SequenceEmbedding.sequence_id)
            .group_by(
                EmbeddingConfig.id,
                EmbeddingConfig.model_name,
                EmbeddingConfig.display_name,
                EmbeddingConfig.family,
                EmbeddingConfig.param_count,
            )
            .order_by(EmbeddingConfig.model_name)
            .all()
        )
        items = [
            {
                "embedding_config_id": str(cid),
                "model_name": model,
                "display_name": display,
                "family": family,
                "param_count": param,
                "protein_count": int(cnt),
                "coverage_pct": round(100 * float(cnt) / total, 2) if total else 0.0,
            }
            for cid, model, display, family, param, cnt in rows
        ]
        return {"total_proteins": total, "items": items}


# ── GO annotation matrix ─────────────────────────────────────────────────────


def _compute_go_matrix(factory: sessionmaker[Session]) -> dict[str, Any]:
    """Cell = COUNT(*) of (source, version, aspect, experimental?) tuples.

    ``experimental`` is the standard CAFA split: True when the row's
    ``evidence_code`` is one of the 6 high-confidence codes, False otherwise.
    Returns flat rows so the frontend can pivot on whichever axis it wants
    (we don't presume one layout).
    """
    with session_scope(factory) as session:
        is_exp = func.coalesce(ProteinGOAnnotation.evidence_code, "").in_(_EXPERIMENTAL_CODES)
        rows = (
            session.query(
                AnnotationSet.source,
                AnnotationSet.source_version,
                GOTerm.aspect,
                is_exp.label("experimental"),
                func.count(ProteinGOAnnotation.id),
            )
            .join(GOTerm, ProteinGOAnnotation.go_term_id == GOTerm.id)
            .join(AnnotationSet, ProteinGOAnnotation.annotation_set_id == AnnotationSet.id)
            .group_by(
                AnnotationSet.source,
                AnnotationSet.source_version,
                GOTerm.aspect,
                is_exp,
            )
            .order_by(AnnotationSet.source, AnnotationSet.source_version, GOTerm.aspect)
            .all()
        )
        items = [
            {
                "source": src,
                "source_version": ver,
                "aspect": aspect,
                "experimental": bool(exp),
                "count": int(cnt),
            }
            for src, ver, aspect, exp, cnt in rows
        ]
        return {"items": items}


# ── Taxonomy top-20 ──────────────────────────────────────────────────────────


def _compute_taxonomy_top(factory: sessionmaker[Session]) -> dict[str, Any]:
    """Top 20 organisms by canonical-protein count.

    Only counts canonical rows so isoforms don't double-weight an organism.
    Returns ``taxonomy_id`` alongside the human-readable ``organism`` so the
    frontend can deep-link to a UniProt taxonomy page.
    """
    with session_scope(factory) as session:
        rows = (
            session.query(
                Protein.organism,
                Protein.taxonomy_id,
                func.count(Protein.accession),
            )
            .filter(Protein.is_canonical.is_(True))
            .filter(Protein.organism.is_not(None))
            .group_by(Protein.organism, Protein.taxonomy_id)
            .order_by(func.count(Protein.accession).desc())
            .limit(20)
            .all()
        )
        items = [
            {
                "organism": org,
                "taxonomy_id": tax,
                "protein_count": int(cnt),
            }
            for org, tax, cnt in rows
        ]
        return {"items": items}


# ── Sequence length distribution ─────────────────────────────────────────────


def _compute_sequence_length(factory: sessionmaker[Session]) -> dict[str, Any]:
    """Min / p10 / p50 / p90 / p99 / max + 50-bin histogram of canonical lengths.

    Uses ``Protein.length`` (already populated at insert time, no need to
    join into ``sequence``). Restricts to canonical rows for the same reason
    as taxonomy: isoforms inflate the population without adding signal.
    """
    with session_scope(factory) as session:
        base = (
            session.query(Protein.length)
            .filter(Protein.is_canonical.is_(True))
            .filter(Protein.length.is_not(None))
        )
        total_count = base.count()
        if total_count == 0:
            return {"count": 0, "percentiles": {}, "bins": []}
        pct_row = session.query(
            func.min(Protein.length),
            func.percentile_cont(0.1).within_group(Protein.length.asc()),
            func.percentile_cont(0.5).within_group(Protein.length.asc()),
            func.percentile_cont(0.9).within_group(Protein.length.asc()),
            func.percentile_cont(0.99).within_group(Protein.length.asc()),
            func.max(Protein.length),
        ).filter(Protein.is_canonical.is_(True)).filter(Protein.length.is_not(None)).one()
        pmin, p10, p50, p90, p99, pmax = pct_row
        bins = _histogram_bins(session, int(pmin or 0), int(pmax or 0))
        return {
            "count": total_count,
            "percentiles": {
                "min": int(pmin or 0),
                "p10": float(p10 or 0.0),
                "p50": float(p50 or 0.0),
                "p90": float(p90 or 0.0),
                "p99": float(p99 or 0.0),
                "max": int(pmax or 0),
            },
            "bins": bins,
        }


def _histogram_bins(session: Session, pmin: int, pmax: int) -> list[dict[str, Any]]:
    """Bucket ``Protein.length`` into ``_SEQ_LEN_BINS`` equal-width bins.

    Uses Postgres ``width_bucket`` so the bucketization runs server-side and
    the wire transfer is bin-count rows, not 600k integers.
    """
    if pmax <= pmin:
        return []
    bucket = func.width_bucket(Protein.length, pmin, pmax + 1, _SEQ_LEN_BINS).label("bucket")
    rows = (
        session.query(bucket, func.count(Protein.accession))
        .filter(Protein.is_canonical.is_(True))
        .filter(Protein.length.is_not(None))
        .group_by(bucket)
        .order_by(bucket)
        .all()
    )
    step = (pmax - pmin) / _SEQ_LEN_BINS
    bins: list[dict[str, Any]] = []
    for b, cnt in rows:
        if b is None or b <= 0 or b > _SEQ_LEN_BINS:
            continue
        lo = pmin + (b - 1) * step
        hi = pmin + b * step
        bins.append({"lo": round(lo, 1), "hi": round(hi, 1), "count": int(cnt)})
    return bins


# ── GO term density per protein per aspect ───────────────────────────────────


def _compute_go_density(factory: sessionmaker[Session]) -> dict[str, Any]:
    """Per-aspect average + p50 / p90 of (#go_terms per protein).

    Two-step (subquery counts, outer percentile) keeps the engine from
    sorting the raw join three times. Aspects normalised to BPO/MFO/CCO
    (matches the GO Consortium short-form used in the matrix endpoint).
    """
    aspect_map = {"F": "MFO", "P": "BPO", "C": "CCO"}
    with session_scope(factory) as session:
        sub = (
            session.query(
                GOTerm.aspect.label("aspect"),
                ProteinGOAnnotation.protein_accession.label("acc"),
                func.count(ProteinGOAnnotation.go_term_id).label("n"),
            )
            .join(GOTerm, ProteinGOAnnotation.go_term_id == GOTerm.id)
            .group_by(GOTerm.aspect, ProteinGOAnnotation.protein_accession)
            .subquery()
        )
        rows = (
            session.query(
                sub.c.aspect,
                func.count(sub.c.acc),
                func.avg(sub.c.n),
                func.percentile_cont(0.5).within_group(sub.c.n.asc()),
                func.percentile_cont(0.9).within_group(sub.c.n.asc()),
                func.max(sub.c.n),
            )
            .group_by(sub.c.aspect)
            .all()
        )
        items = [
            {
                "aspect": aspect_map.get(asp or "", asp or "unknown"),
                "protein_count": int(pc),
                "avg": round(float(avg or 0.0), 2),
                "p50": float(p50 or 0.0),
                "p90": float(p90 or 0.0),
                "max": int(mx or 0),
            }
            for asp, pc, avg, p50, p90, mx in rows
        ]
        return {"items": items}


# ── Pipeline activity ────────────────────────────────────────────────────────


def _compute_pipeline_activity(factory: sessionmaker[Session]) -> dict[str, Any]:
    """Daily job-count GROUP BY (operation, day) over the last ``_PIPELINE_DAYS`` days.

    Includes a sentinel ``last_24h`` block so the frontend doesn't need to
    re-aggregate from the timeline when it wants to emphasise today's run.
    """
    cutoff = datetime.now(UTC) - timedelta(days=_PIPELINE_DAYS)
    cutoff_24h = datetime.now(UTC) - timedelta(hours=24)
    with session_scope(factory) as session:
        day = func.date_trunc("day", Job.created_at).label("day")
        rows = (
            session.query(Job.operation, day, func.count(Job.id))
            .filter(Job.created_at >= cutoff)
            .group_by(Job.operation, day)
            .order_by(day, Job.operation)
            .all()
        )
        items = [
            {
                "operation": op,
                "day": d.isoformat() if d else None,
                "count": int(cnt),
            }
            for op, d, cnt in rows
        ]
        last_24h_rows = (
            session.query(Job.operation, func.count(Job.id))
            .filter(Job.created_at >= cutoff_24h)
            .group_by(Job.operation)
            .all()
        )
        last_24h = [
            {"operation": op, "count": int(cnt)} for op, cnt in last_24h_rows
        ]
        return {
            "window_days": _PIPELINE_DAYS,
            "items": items,
            "last_24h": last_24h,
        }


# ── Dataset registry summary ─────────────────────────────────────────────────


def _compute_dataset_registry(factory: sessionmaker[Session]) -> dict[str, Any]:
    """GROUP BY (k, eval_snapshot_pair) over the ``dataset`` registry.

    Counts datasets and sums ``n_train_rows`` / ``n_eval_rows`` per cell.
    The Dataset model does not track on-disk size, so we expose the row
    sums and let the frontend present them. Total cell shipping size is
    a known FARM-EXP gap (the artefact store has the bytes, the registry
    row does not duplicate them).
    """
    with session_scope(factory) as session:
        rows = (
            session.query(
                Dataset.k,
                Dataset.eval_snapshot_pair,
                func.count(Dataset.id),
                func.coalesce(func.sum(Dataset.n_train_rows), 0),
                func.coalesce(func.sum(Dataset.n_eval_rows), 0),
            )
            .group_by(Dataset.k, Dataset.eval_snapshot_pair)
            .order_by(Dataset.k, Dataset.eval_snapshot_pair)
            .all()
        )
        items = [
            {
                "k": int(k) if k is not None else None,
                "eval_snapshot_pair": pair,
                "dataset_count": int(cnt),
                "n_train_rows": int(rt),
                "n_eval_rows": int(re),
            }
            for k, pair, cnt, rt, re in rows
        ]
        return {"items": items}


# ── Registry of (key, ttl, compute) tuples for prewarm + routes ─────────────


def _make_compute(
    fn: Callable[[sessionmaker[Session]], dict[str, Any]],
    factory: sessionmaker[Session],
) -> Callable[[], dict[str, Any]]:
    """Bind ``factory`` so the cache lambda is a 0-arg ``Callable[[], Any]``."""
    return lambda: fn(factory)


def _sections() -> tuple[tuple[str, float, Callable[[sessionmaker[Session]], dict[str, Any]]], ...]:
    """Resolve compute functions at call time so ``unittest.mock.patch`` of
    individual helpers in tests actually swaps them out (a module-scope
    tuple would capture the originals at import).
    """
    return (
        (CACHE_KEY_PLM, TTL_PLM, _compute_embeddings_by_plm),
        (CACHE_KEY_GO_MATRIX, TTL_GO_MATRIX, _compute_go_matrix),
        (CACHE_KEY_TAXONOMY, TTL_TAXONOMY, _compute_taxonomy_top),
        (CACHE_KEY_SEQ_LEN, TTL_SEQ_LEN, _compute_sequence_length),
        (CACHE_KEY_GO_DENSITY, TTL_GO_DENSITY, _compute_go_density),
        (CACHE_KEY_PIPELINE, TTL_PIPELINE, _compute_pipeline_activity),
        (CACHE_KEY_DATASETS, TTL_DATASETS, _compute_dataset_registry),
    )


def prewarm_all(factory: sessionmaker[Session]) -> None:
    """Force-recompute every section. Called from the app startup hook and
    from the background-refresh loop so users never hit a cold cache.

    Per-section failures are swallowed so a single slow query does not
    starve the rest of the dashboard.
    """
    for key, ttl, fn in _sections():
        invalidate(key)
        try:
            cached(key, ttl, _make_compute(fn, factory))
        except Exception:  # pragma: no cover - logged by caller
            pass


# ── Routes ───────────────────────────────────────────────────────────────────


@router.get("/embeddings-by-plm", summary="Embedding coverage per PLM (config)")
def get_embeddings_by_plm(
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Per-PLM protein coverage. Cached 10 minutes."""
    return cached(
        CACHE_KEY_PLM,
        TTL_PLM,
        _make_compute(_compute_embeddings_by_plm, factory),
        serve_stale_on_error=True,
    )


@router.get("/go-annotations-matrix", summary="GO annotations by source/version/aspect/evidence")
def get_go_matrix(
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """4D-grouped annotation counts. Cached 10 minutes."""
    return cached(
        CACHE_KEY_GO_MATRIX,
        TTL_GO_MATRIX,
        _make_compute(_compute_go_matrix, factory),
        serve_stale_on_error=True,
    )


@router.get("/taxonomy-top-20", summary="Top 20 organisms by canonical-protein count")
def get_taxonomy_top(
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Top-20 organisms. Cached 60 minutes (organism mix rarely moves)."""
    return cached(
        CACHE_KEY_TAXONOMY,
        TTL_TAXONOMY,
        _make_compute(_compute_taxonomy_top, factory),
        serve_stale_on_error=True,
    )


@router.get("/sequence-length", summary="Sequence length percentiles and histogram")
def get_sequence_length(
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Length distribution. Cached 60 minutes."""
    return cached(
        CACHE_KEY_SEQ_LEN,
        TTL_SEQ_LEN,
        _make_compute(_compute_sequence_length, factory),
        serve_stale_on_error=True,
    )


@router.get("/go-density", summary="GO term density per protein per aspect")
def get_go_density(
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Per-aspect annotation density. Cached 10 minutes."""
    return cached(
        CACHE_KEY_GO_DENSITY,
        TTL_GO_DENSITY,
        _make_compute(_compute_go_density, factory),
        serve_stale_on_error=True,
    )


@router.get("/pipeline-activity", summary="Job activity by operation over the last 30 days")
def get_pipeline_activity(
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Last-30-days job activity. Cached 60 seconds (high-freshness expectation)."""
    return cached(
        CACHE_KEY_PIPELINE,
        TTL_PIPELINE,
        _make_compute(_compute_pipeline_activity, factory),
        serve_stale_on_error=True,
    )


@router.get("/dataset-registry", summary="Dataset registry summary by K and eval-snapshot pair")
def get_dataset_registry(
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Frozen reranker datasets summary. Cached 5 minutes."""
    return cached(
        CACHE_KEY_DATASETS,
        TTL_DATASETS,
        _make_compute(_compute_dataset_registry, factory),
        serve_stale_on_error=True,
    )


# Unused-import shim: ``Sequence`` is exported indirectly through other
# call sites that import this module's symbols; SQLAlchemy needs the
# class registered on ``Base.metadata`` for FK resolution.
_ = Sequence
