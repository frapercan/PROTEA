"""Per-PredictionSet query + TSV-formatting helpers extracted from
``embeddings_service``.

These pure functions own the GOPrediction/GOTerm join queries and the
output formatting (TSV columns, optional / float coercions). The
service layer keeps existence checks and re-exports the public API so
external callers continue importing
``protea.services.embeddings_service`` unchanged.
"""

from __future__ import annotations

import csv
import io
import uuid
from collections.abc import Iterator
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.orm.models.embedding.prediction_score import PredictionScore
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.method.prediction_explanation import PredictionExplanation
from protea.infrastructure.session import session_scope

PREDICTIONS_TSV_COLUMNS: tuple[str, ...] = (
    "protein_accession",
    "go_id",
    "go_name",
    "go_aspect",
    "distance",
    "ref_protein_accession",
    "qualifier",
    "evidence_code",
    "identity_nw",
    "similarity_nw",
    "alignment_score_nw",
    "gaps_pct_nw",
    "alignment_length_nw",
    "identity_sw",
    "similarity_sw",
    "alignment_score_sw",
    "gaps_pct_sw",
    "alignment_length_sw",
    "length_query",
    "length_ref",
    "query_taxonomy_id",
    "ref_taxonomy_id",
    "taxonomic_lca",
    "taxonomic_distance",
    "taxonomic_common_ancestors",
    "taxonomic_relation",
    "vote_count",
    "k_position",
    "go_term_frequency",
    "ref_annotation_density",
    "neighbor_distance_std",
)


def _format_float(v: float | None) -> str:
    """Format a nullable float for TSV output (``""`` for ``None``)."""
    if v is None:
        return ""
    return f"{v:.6g}"


def _format_optional(v: Any) -> Any:
    """Render ``None`` as the empty string; otherwise pass through."""
    return "" if v is None else v


def _predictions_tsv_query(
    session: Session,
    *,
    prediction_set_id: uuid.UUID,
    accession: str | None,
    aspect: str | None,
    max_distance: float | None,
) -> Any:
    """Build the (GOPrediction, GOTerm) join query with optional filters."""
    q = (
        session.query(GOPrediction, GOTerm)
        .join(GOTerm, GOPrediction.go_term_id == GOTerm.id)
        .filter(GOPrediction.prediction_set_id == prediction_set_id)
    )
    if accession:
        q = q.filter(GOPrediction.protein_accession == accession)
    if aspect:
        q = q.filter(GOTerm.aspect == aspect.upper())
    if max_distance is not None:
        q = q.filter(GOPrediction.distance <= max_distance)
    return q.order_by(GOPrediction.protein_accession, GOPrediction.distance)


def _format_prediction_row(pred: GOPrediction, gt: GOTerm) -> list[Any]:
    """Flatten one ``(GOPrediction, GOTerm)`` pair into the TSV row order."""
    return [
        pred.protein_accession,
        gt.go_id,
        gt.name,
        gt.aspect,
        pred.distance,
        pred.ref_protein_accession,
        pred.qualifier or "",
        pred.evidence_code or "",
        _format_float(pred.identity_nw),
        _format_float(pred.similarity_nw),
        _format_float(pred.alignment_score_nw),
        _format_float(pred.gaps_pct_nw),
        _format_float(pred.alignment_length_nw),
        _format_float(pred.identity_sw),
        _format_float(pred.similarity_sw),
        _format_float(pred.alignment_score_sw),
        _format_float(pred.gaps_pct_sw),
        _format_float(pred.alignment_length_sw),
        _format_optional(pred.length_query),
        _format_optional(pred.length_ref),
        _format_optional(pred.query_taxonomy_id),
        _format_optional(pred.ref_taxonomy_id),
        _format_optional(pred.taxonomic_lca),
        _format_optional(pred.taxonomic_distance),
        _format_optional(pred.taxonomic_common_ancestors),
        pred.taxonomic_relation or "",
        _format_optional(pred.vote_count),
        _format_optional(pred.k_position),
        _format_optional(pred.go_term_frequency),
        _format_optional(pred.ref_annotation_density),
        _format_float(pred.neighbor_distance_std),
    ]


def iter_predictions_tsv(
    factory: Any,
    *,
    prediction_set_id: uuid.UUID,
    accession: str | None = None,
    aspect: str | None = None,
    max_distance: float | None = None,
) -> Iterator[str]:
    """Yield TSV rows (as ``str``) of every GOPrediction in a set.

    Opens its own session inside the generator so the caller's
    existence-check session can close cleanly. The first yielded
    chunk is the header line; one row per ``(GOPrediction, GOTerm)``
    pair follows, ordered by ``(protein_accession, distance)``.

    Optional filters: ``accession`` (single query protein),
    ``aspect`` (``F`` / ``P`` / ``C``), ``max_distance``.
    """
    with session_scope(factory) as session:
        buf = io.StringIO()
        writer = csv.writer(buf, delimiter="\t", lineterminator="\n")
        writer.writerow(PREDICTIONS_TSV_COLUMNS)
        yield buf.getvalue()
        q = _predictions_tsv_query(
            session,
            prediction_set_id=prediction_set_id,
            accession=accession,
            aspect=aspect,
            max_distance=max_distance,
        )
        for pred, gt in q.yield_per(1000):
            buf = io.StringIO()
            writer = csv.writer(buf, delimiter="\t", lineterminator="\n")
            writer.writerow(_format_prediction_row(pred, gt))
            yield buf.getvalue()


def _prediction_row_to_dict(
    pred: GOPrediction,
    gt: GOTerm,
    explanation: dict[str, Any] | None = None,
    score: float | None = None,
) -> dict[str, Any]:
    """Shape one ``(GOPrediction, GOTerm)`` join row for the API response.

    Owns the column list so any future GOPrediction column reaches every
    consumer (currently just ``GET /v1/embeddings/prediction-sets/.../proteins/{accession}``,
    but the per-protein view is the authoritative wire shape).
    """
    return {
        "go_id": gt.go_id,
        "name": gt.name,
        "aspect": gt.aspect,
        # None, not 0.0: a method that scores its own candidates has no
        # neighbour to be a distance from, and a zero here would read on the
        # wire as the query being its own closest match.
        "distance": None if pred.distance is None else round(pred.distance, 4),
        "ref_protein_accession": pred.ref_protein_accession,
        "qualifier": pred.qualifier,
        "evidence_code": pred.evidence_code,
        "identity_nw": pred.identity_nw,
        "similarity_nw": pred.similarity_nw,
        "alignment_score_nw": pred.alignment_score_nw,
        "gaps_pct_nw": pred.gaps_pct_nw,
        "alignment_length_nw": pred.alignment_length_nw,
        "identity_sw": pred.identity_sw,
        "similarity_sw": pred.similarity_sw,
        "alignment_score_sw": pred.alignment_score_sw,
        "gaps_pct_sw": pred.gaps_pct_sw,
        "alignment_length_sw": pred.alignment_length_sw,
        "length_query": pred.length_query,
        "length_ref": pred.length_ref,
        "query_taxonomy_id": pred.query_taxonomy_id,
        "ref_taxonomy_id": pred.ref_taxonomy_id,
        "taxonomic_lca": pred.taxonomic_lca,
        "taxonomic_distance": pred.taxonomic_distance,
        "taxonomic_common_ancestors": pred.taxonomic_common_ancestors,
        "taxonomic_relation": pred.taxonomic_relation,
        "vote_count": pred.vote_count,
        "k_position": pred.k_position,
        "go_term_frequency": pred.go_term_frequency,
        "ref_annotation_density": pred.ref_annotation_density,
        "neighbor_distance_std": pred.neighbor_distance_std,
        # A method that scores its own candidates (ADR-D60): its score, and
        # the explanation it wrote for the row when its spec says it explains.
        "method_score": pred.method_score,
        "explanation": explanation,
        # The score a scored set stored for this candidate (score_prediction_set);
        # None on a candidate set, which holds candidates and no scores.
        "score": score,
    }


def build_predictions_for_protein(
    session: Session,
    *,
    prediction_set_id: uuid.UUID,
    accession: str,
) -> list[dict[str, Any]]:
    """Return all predicted GO terms for one protein.

    A candidate set's rows are its own, sorted by distance, then by the method's
    score. A scored set holds no rows of its own (``score_prediction_set``): its
    rows are its candidate set's, each with the score it stored, highest first,
    and a candidate the scoring removed is left out, because the scored set does
    not hold it. Read here, a scored set shows the scores the evaluator read
    rather than nothing.

    The PredictionSet existence check stays in the service layer (so
    the domain exception type lives next to the rest of the validation
    logic); this helper assumes the caller has already verified the id.
    """
    pset = session.get(PredictionSet, prediction_set_id)
    scored_from = (pset.meta or {}).get("scored_from") if pset is not None else None
    rows_of = uuid.UUID(str(scored_from)) if scored_from else prediction_set_id
    rows = (
        session.query(GOPrediction, GOTerm)
        .join(GOTerm, GOPrediction.go_term_id == GOTerm.id)
        .filter(
            GOPrediction.prediction_set_id == rows_of,
            GOPrediction.protein_accession == accession,
        )
        .order_by(GOPrediction.distance, GOPrediction.method_score.desc())
        .all()
    )
    if scored_from:
        stored = _stored_scores(session, prediction_set_id, accession)
        kept = [(pred, gt) for pred, gt in rows if pred.go_term_id in stored]
        kept.sort(key=lambda row: (-stored[row[0].go_term_id], row[1].go_id))
        return [
            _prediction_row_to_dict(pred, gt, score=stored[pred.go_term_id]) for pred, gt in kept
        ]
    why = _explanations(session, prediction_set_id, accession) if _from_a_method(rows) else {}
    return [_prediction_row_to_dict(pred, gt, why.get(gt.go_id)) for pred, gt in rows]


def _stored_scores(
    session: Session, prediction_set_id: uuid.UUID, accession: str
) -> dict[int, float]:
    """go_term_id -> the score a scored set stored for one protein; empty when none."""
    row = session.get(PredictionScore, (prediction_set_id, accession))
    if row is None:
        return {}
    return {int(t): float(s) for t, s in zip(row.go_term_ids, row.scores, strict=True)}


def _from_a_method(rows: list[Any]) -> bool:
    """Whether the rows carry a method's own score, the only ones that may be explained."""
    return any(getattr(pred, "method_score", None) is not None for pred, _ in rows)


def _explanations(
    session: Session, prediction_set_id: uuid.UUID, accession: str
) -> dict[str, dict[str, Any]]:
    """go_id -> the explanation the method wrote for one protein's row.

    Keyed by the term's go_id, never by ``go_term_id``: the surrogate is per
    snapshot, and two tables are only compared through the accession it names.
    """
    found = session.execute(
        select(GOTerm.go_id, PredictionExplanation.payload)
        .join(GOTerm, GOTerm.id == PredictionExplanation.go_term_id)
        .where(
            PredictionExplanation.prediction_set_id == prediction_set_id,
            PredictionExplanation.accession == accession,
        )
    ).all()
    return {str(go_id): payload for go_id, payload in found}
