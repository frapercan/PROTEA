"""Training-data row formatting for ``scoring_service.iter_training_data``.

Lifts the 30-column TSV row builder out of the streaming generator
so the orchestrator stays under the §3 method-LOC ceiling.
"""

from __future__ import annotations

from typing import Any

TRAINING_TSV_COLUMNS: tuple[str, ...] = (
    "protein_accession",
    "go_id",
    "aspect",
    "label",
    "distance",
    "ref_protein_accession",
    "qualifier",
    "evidence_code",
    # NW alignment
    "identity_nw",
    "similarity_nw",
    "alignment_score_nw",
    "gaps_pct_nw",
    "alignment_length_nw",
    # SW alignment
    "identity_sw",
    "similarity_sw",
    "alignment_score_sw",
    "gaps_pct_sw",
    "alignment_length_sw",
    # Lengths
    "length_query",
    "length_ref",
    # Taxonomy
    "query_taxonomy_id",
    "ref_taxonomy_id",
    "taxonomic_lca",
    "taxonomic_distance",
    "taxonomic_common_ancestors",
    "taxonomic_relation",
    # Re-ranker features
    "vote_count",
    "k_position",
    "go_term_frequency",
    "ref_annotation_density",
    "neighbor_distance_std",
)


def format_training_row(
    pred: Any,
    go_id: str,
    aspect: str | None,
    label: int,
) -> str:
    """Render one labeled training-data TSV row (no trailing newline).

    Column order matches :data:`TRAINING_TSV_COLUMNS`. ``label`` is
    binary (0 / 1). Uses lazy import of ``_format_optional`` from
    ``_scoring_streaming_helpers`` (where it canonically lives) to
    avoid a circular dep with the re-exporting module.
    """
    from protea.services._scoring_streaming_helpers import _format_optional

    return "\t".join(
        [
            pred.protein_accession,
            go_id,
            aspect or "",
            str(label),
            _format_optional(pred.distance),
            pred.ref_protein_accession or "",
            pred.qualifier or "",
            pred.evidence_code or "",
            _format_optional(pred.identity_nw),
            _format_optional(pred.similarity_nw),
            _format_optional(pred.alignment_score_nw),
            _format_optional(pred.gaps_pct_nw),
            _format_optional(pred.alignment_length_nw),
            _format_optional(pred.identity_sw),
            _format_optional(pred.similarity_sw),
            _format_optional(pred.alignment_score_sw),
            _format_optional(pred.gaps_pct_sw),
            _format_optional(pred.alignment_length_sw),
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
            _format_optional(pred.neighbor_distance_std),
        ]
    )
