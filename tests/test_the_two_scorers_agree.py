"""The vectorised scorer and the per-row scorer give every candidate the same score.

A ScoringConfig's arithmetic has two implementations. ``vectorized_scores``
scores a frame of candidates at once, and is what ``score_prediction_set``
stores; ``compute_score`` scores one candidate at a time, and is what the
scoring endpoints of the API apply. A candidate that came out of the two with
different scores would be one method under two numbers, so they are compared
here on the same rows, through the same nearest-row-per-term choice, at the
four decimals a prediction file carries, across the linear formula with many
signals, the evidence-weighted formula, the rich alignment and ontology
signals, and the information prior.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from protea.core.operations._candidate_scoring import BASE_SCORE_COLS, vectorized_scores
from protea.core.scoring import compute_score
from protea.infrastructure.orm.models.embedding.scoring_config import ScoringConfig


def _row(
    protein: str,
    go_id: str,
    distance: float | None,
    identity_nw: float | None = None,
    identity_sw: float | None = None,
    evidence_code: str | None = None,
    taxonomic_distance: float | None = None,
    neighbor_vote_fraction: float | None = None,
    **rich: Any,
) -> dict[str, Any]:
    """One candidate, by name, with every column the scorer reads."""
    row = dict.fromkeys(BASE_SCORE_COLS)
    row.update(
        protein_accession=protein,
        go_id=go_id,
        distance=distance,
        identity_nw=identity_nw,
        identity_sw=identity_sw,
        evidence_code=evidence_code,
        taxonomic_distance=taxonomic_distance,
        neighbor_vote_fraction=neighbor_vote_fraction,
        **rich,
    )
    unknown = set(row) - set(BASE_SCORE_COLS)
    assert not unknown, f"the row builder names columns the scorer does not read: {unknown}"
    return row


#: Includes a (P1, GO:0000001) duplicate, to exercise the nearest-row choice,
#: plus missing signals and varied evidence codes.
_ROWS = [
    _row("P1", "GO:0000001", 0.4, 0.8, 0.9, "IDA", 2, 0.5),
    _row("P1", "GO:0000001", 0.6, 0.1, 0.2, "IEA", 5, 0.1),
    _row("P1", "GO:0000002", 0.2, None, 0.3, "IEA", None, 0.7),
    _row("P2", "GO:0000003", 1.0, 0.5, None, None, 1, None),
    _row("P2", "GO:0000001", 0.8, 0.2, 0.2, "ND", 0, 0.0),
]

_RICH_ROWS = [
    _row(
        "P1", "GO:0000001", 0.4, 0.8, 0.9, "IDA", 2, 0.5,
        alignment_length_sw=120.0, gaps_pct_sw=0.1, length_query=200,
        ref_annotation_density=7, anc2vec_neighbor_cos=0.6,
        anc2vec_neighbor_maxcos=0.8, go_term_frequency=5000,
    ),
    _row(
        "P2", "GO:0000003", 0.2, None, 0.3, "IEA", None, 0.7,
        alignment_length_nw=180.0, gaps_pct_nw=0.0, length_query=200,
        ref_annotation_density=0, anc2vec_neighbor_cos=-0.4, go_term_frequency=10,
    ),
    _row("P2", "GO:0000005", 0.8, 0.2, 0.2, "ND", 1, 0.1),
]  # fmt: skip


def _nearest(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The nearest row per (protein, term), ordered by protein and term."""
    winners: dict[tuple[str, str], dict[str, Any]] = {}
    for row in sorted(rows, key=lambda r: (r["protein_accession"], r["go_id"], r["distance"])):
        winners.setdefault((row["protein_accession"], row["go_id"]), row)
    return [winners[key] for key in sorted(winners)]


def _one_at_a_time(rows: list[dict[str, Any]], config: ScoringConfig) -> list[str]:
    return [
        f"{r['protein_accession']}\t{r['go_id']}\t{compute_score(dict(r), config):.4f}"
        for r in _nearest(rows)
    ]


def _all_at_once(rows: list[dict[str, Any]], config: ScoringConfig) -> list[str]:
    frame = pd.DataFrame(_nearest(rows), columns=list(BASE_SCORE_COLS))
    scores = vectorized_scores(frame, config)
    return [
        f"{p}\t{g}\t{float(score):.4f}"
        for p, g, score in zip(frame["protein_accession"], frame["go_id"], scores, strict=True)
    ]


def _config(formula: str = "linear", **fields: Any) -> ScoringConfig:
    return ScoringConfig(
        formula=formula,
        weights=fields.pop("weights", {"embedding_similarity": 1.0}),
        evidence_weights=fields.pop("evidence_weights", None),
        **fields,
    )


def test_the_linear_formula_over_many_signals() -> None:
    config = _config(
        weights={
            "embedding_similarity": 0.5,
            "identity_nw": 0.3,
            "identity_sw": 0.0,
            "evidence_weight": 0.2,
            "taxonomic_proximity": 0.1,
            "neighbor_vote_fraction": 0.4,
        },
        evidence_weights={"IEA": 0.3},
    )
    assert _all_at_once(_ROWS, config) == _one_at_a_time(_ROWS, config)


def test_the_evidence_weighted_formula() -> None:
    config = _config(
        "evidence_weighted",
        weights={"embedding_similarity": 1.0, "evidence_weight": 0.0},
        evidence_weights={"IEA": 0.5, "ND": 0.05},
    )
    assert _all_at_once(_ROWS, config) == _one_at_a_time(_ROWS, config)


def test_the_rich_signals() -> None:
    config = _config(
        weights={
            "embedding_similarity": 0.4,
            "coverage": 0.3,
            "ref_annotation_density": 0.1,
            "anc2vec_neighbor_cos": 0.1,
            "anc2vec_neighbor_maxcos": 0.1,
        }
    )
    assert _all_at_once(_RICH_ROWS, config) == _one_at_a_time(_RICH_ROWS, config)


def test_the_information_prior() -> None:
    config = _config(weights={"embedding_similarity": 1.0, "coverage": 0.5})
    config.params = {"ia_prior": {"enabled": True, "gamma": 1.5, "source": "frequency"}}
    assert _all_at_once(_RICH_ROWS, config) == _one_at_a_time(_RICH_ROWS, config)
