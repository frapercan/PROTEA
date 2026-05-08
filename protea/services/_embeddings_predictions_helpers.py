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

from sqlalchemy.orm import Session

from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
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

        q = q.order_by(GOPrediction.protein_accession, GOPrediction.distance)

        for pred, gt in q.yield_per(1000):
            buf = io.StringIO()
            writer = csv.writer(buf, delimiter="\t", lineterminator="\n")
            writer.writerow(
                [
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
            )
            yield buf.getvalue()


def build_predictions_for_protein(
    session: Session,
    *,
    prediction_set_id: uuid.UUID,
    accession: str,
) -> list[dict[str, Any]]:
    """Return all predicted GO terms for one protein, sorted by distance.

    The PredictionSet existence check stays in the service layer (so
    the domain exception type lives next to the rest of the validation
    logic); this helper assumes the caller has already verified the id.
    """
    rows = (
        session.query(GOPrediction, GOTerm)
        .join(GOTerm, GOPrediction.go_term_id == GOTerm.id)
        .filter(
            GOPrediction.prediction_set_id == prediction_set_id,
            GOPrediction.protein_accession == accession,
        )
        .order_by(GOPrediction.distance)
        .all()
    )

    return [
        {
            "go_id": gt.go_id,
            "name": gt.name,
            "aspect": gt.aspect,
            "distance": round(pred.distance, 4),
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
        }
        for pred, gt in rows
    ]
