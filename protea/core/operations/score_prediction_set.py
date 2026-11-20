"""Score a candidate set and store the scores as a prediction set of their own.

WHY. The evaluator scores what a method stored and nothing else (the closure's
question 6, decided on 2026-11-20). A transfer method stores its candidates with
their evidence and no score: the distance to the donor, the alignment
identities, the share of neighbours that voted for the term. Turning that
evidence into one number per (protein, term) is a step of the method, so it
happens here, before anything is evaluated, and it leaves a record.

WHAT IT DOES, per protein, in the evaluator's own order:

1. reads the protein's candidates, cut to a depth when one is named, keeps the
   nearest row per term, and recounts the neighbour aggregates at the cut
   (``_base_frame_recount``);
2. checks that the cut was applied (``_depth_guard``);
3. drops what the bank denies (``protea.core.denials``), the bank being the
   candidate set's annotation set and the ontology its own;
4. scores each row with the ``ScoringConfig`` (``_candidate_scoring``);
5. writes one row per (protein, term) into a new prediction set, the score as
   ``method_score`` and the donor of the nearest row as ``ref_protein_accession``.

Proteins are independent at every step, so the set is scored in chunks of
proteins and the result does not depend on the chunk size.

WHAT THE SCORED SET RECORDS. Its columns are the candidate set's. Its ``meta``
is the candidate set's record with these keys set by this step:

- ``scored_from``: the candidate set's id;
- ``scoring``: the arithmetic and the cut (formula, weights, evidence weights,
  params, depth) and the code that applied them; part of the method;
- ``scoring_config_id``: the row the arithmetic was read from;
- ``denials`` and ``denied_removed``: the policy, and how many candidates it
  removed;
- ``job_id``: the job that scored it.

A failed run leaves nothing behind: the worker rolls the transaction back, so
no scored set is ever half written.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

from pydantic import Field, field_validator, model_validator
from sqlalchemy import insert, text
from sqlalchemy.orm import Session

from protea.core import denials
from protea.core.code_revision import code_revision, dependency_revisions
from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.operations._base_frame_recount import build_base_frame
from protea.core.operations._candidate_scoring import (
    BASE_SCORE_COLS,
    candidate_select,
    count_candidates,
    vectorized_scores,
)
from protea.core.operations._depth_guard import assert_depth_was_applied
from protea.core.operations._depth_unit_guard import (
    SEQUENCE_DEPTH_DESCRIPTION,
    assert_depth_unit_is_available,
    assert_one_depth_unit,
)
from protea.core.utils import contract_payload
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.embedding.scoring_config import (
    FORMULA_METHOD_SCORE,
    ScoringConfig,
)

__all__ = ["ScorePredictionSetOperation", "ScorePredictionSetPayload"]

#: The row's own columns the scored set needs beside the scorer's.
_ROW_COLS: tuple[str, ...] = ("go_term_id", "ref_protein_accession")
_FRAME_COLS: tuple[str, ...] = (*BASE_SCORE_COLS, *_ROW_COLS)

#: Rows per INSERT statement.
_INSERT_ROWS = 10_000

#: The candidate set's columns the scored set keeps: they name what was
#: predicted, for whom and from what, which scoring does not change.
_KEPT_COLUMNS: tuple[str, ...] = (
    "embedding_config_id",
    "method_id",
    "annotation_set_id",
    "ontology_snapshot_id",
    "query_set_id",
    "limit_per_entry",
    "distance_threshold",
)


class ScorePredictionSetPayload(ProteaPayload, frozen=True):
    """Which candidate set, which arithmetic, and how deep."""

    prediction_set_id: str = Field(description="The candidate set to score.")
    scoring_config_id: str = Field(
        description=(
            "The ScoringConfig whose arithmetic turns each candidate's evidence "
            "into a score. The method_score formula is refused: it returns a "
            "score the candidates do not have."
        )
    )
    max_distance: float | None = Field(
        default=None,
        ge=0.0,
        le=2.0,
        description="Score only candidates whose donor is at most this far.",
    )
    max_k_position: int | None = Field(
        default=None,
        ge=1,
        description=(
            "Score only the first N neighbours of each query. Candidates are "
            "written to a fixed depth and carry the rank they were retrieved at, "
            "so every smaller depth is a truncation of a list already stored; "
            "the neighbour aggregates are recounted at the cut."
        ),
    )
    max_sequence_rank: int | None = Field(
        default=None, ge=1, description=SEQUENCE_DEPTH_DESCRIPTION
    )
    proteins_per_chunk: int = Field(
        default=5_000,
        ge=1,
        description=(
            "How many proteins are read and scored at once. Execution only: "
            "proteins are independent, so the scores do not depend on it."
        ),
    )

    @model_validator(mode="after")
    def _a_depth_is_counted_in_one_unit(self) -> ScorePredictionSetPayload:
        assert_one_depth_unit(self.max_k_position, self.max_sequence_rank)
        return self

    @field_validator("prediction_set_id", "scoring_config_id", mode="before")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("must be a non-empty string")
        return v.strip()


@dataclass(frozen=True)
class _Chunk:
    """The candidates of some proteins of one set, at one depth.

    Shaped as the base frame's builder and the depth guard read it.
    """

    pred_set_id: uuid.UUID
    delta_proteins: set[str]
    max_distance: float | None
    max_k_position: int | None
    max_sequence_rank: int | None


@dataclass(frozen=True)
class _Run:
    """What every chunk of one scoring run shares."""

    source: PredictionSet
    config: ScoringConfig
    payload: ScorePredictionSetPayload
    scored_id: uuid.UUID


@dataclass
class _Totals:
    proteins: int = 0
    candidates_read: int = 0
    rows_written: int = 0
    denied_removed: int = 0
    chunks: list[int] = field(default_factory=list)


def _select(ctx: _Chunk, *, with_ledger: bool = False) -> Any:
    return candidate_select(ctx, with_ledger=with_ledger, extra=_ROW_COLS)


def _chunks(proteins: list[str], size: int) -> Iterator[list[str]]:
    for start in range(0, len(proteins), size):
        yield proteins[start : start + size]


def _arithmetic(config: ScoringConfig) -> dict[str, Any]:
    """The part of a ScoringConfig that decides a score, and nothing else."""
    return {
        "formula": config.formula,
        "weights": dict(config.weights or {}),
        "evidence_weights": dict(config.evidence_weights or {}),
        "params": dict(config.params or {}),
    }


class ScorePredictionSetOperation:
    """Score a candidate set and store the scores as a prediction set of their own."""

    name = "score_prediction_set"
    description = (
        "Score a candidate prediction set with a ScoringConfig, at a depth, with the "
        "bank's denials honoured, and store the scores as a prediction set of their own."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        cut = [
            f"{name} <= {payload[name]}"
            for name in ("max_distance", "max_k_position", "max_sequence_rank")
            if payload.get(name) is not None
        ]
        return (
            f"score prediction set {payload.get('prediction_set_id')} with scoring config "
            f"{payload.get('scoring_config_id')}" + (f", cut at {' and '.join(cut)}" if cut else "")
        )

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = ScorePredictionSetPayload.model_validate(contract_payload(payload))
        source = self._candidate_set(session, p.prediction_set_id)
        config = self._scoring_config(session, p.scoring_config_id)
        assert_depth_unit_is_available(
            session,
            source.id,
            max_sequence_rank=p.max_sequence_rank,
            max_k_position=p.max_k_position,
        )
        proteins = self._proteins(session, source.id)
        job_id = payload.get("_job_id")
        scored = self._create_scored_set(session, source, config, p, job_id)
        emit(
            "score_prediction_set.start",
            None,
            {
                "scored_from": str(source.id),
                "prediction_set_id": str(scored.id),
                "scoring_config_id": str(config.id),
                "proteins": len(proteins),
            },
            "info",
        )
        totals = _Totals(proteins=len(proteins))
        run = _Run(source=source, config=config, payload=p, scored_id=scored.id)
        for chunk in _chunks(proteins, p.proteins_per_chunk):
            self._score_chunk(session, run, chunk, totals)
            emit(
                "score_prediction_set.progress",
                None,
                {"proteins_done": sum(totals.chunks), "proteins": totals.proteins},
                "info",
            )
        scored.meta = {**scored.meta, "denied_removed": totals.denied_removed}
        session.add(scored)
        session.flush()
        result = {
            "prediction_set_id": str(scored.id),
            "scored_from": str(source.id),
            "proteins": totals.proteins,
            "candidates_read": totals.candidates_read,
            "rows_written": totals.rows_written,
            "denied_removed": totals.denied_removed,
        }
        emit("score_prediction_set.done", None, result, "info")
        return OperationResult(result=result)

    @staticmethod
    def _candidate_set(session: Session, set_id: str) -> PredictionSet:
        """The set to score, refused when it is missing, empty or scored already."""
        source = session.get(PredictionSet, uuid.UUID(set_id))
        if source is None:
            raise ValueError(f"prediction set {set_id} not found")
        has_rows, has_scores = session.execute(
            text(
                "SELECT EXISTS (SELECT 1 FROM go_prediction WHERE prediction_set_id = :s), "
                "EXISTS (SELECT 1 FROM go_prediction "
                "        WHERE prediction_set_id = :s AND method_score IS NOT NULL)"
            ),
            {"s": source.id},
        ).one()
        if not has_rows:
            raise ValueError(f"prediction set {set_id} holds no candidates to score")
        if has_scores:
            raise ValueError(
                f"prediction set {set_id} carries its own scores (method_score): it is "
                "scored already, and the evaluator reads it as it is"
            )
        return source

    @staticmethod
    def _scoring_config(session: Session, config_id: str) -> ScoringConfig:
        config = session.get(ScoringConfig, uuid.UUID(config_id))
        if config is None:
            raise ValueError(f"scoring config {config_id} not found")
        if config.formula == FORMULA_METHOD_SCORE:
            raise ValueError(
                f"scoring config {config_id} uses the {FORMULA_METHOD_SCORE!r} formula, "
                "which returns the method's own score; a candidate set has none. Name "
                "the arithmetic that turns the candidates' evidence into a score."
            )
        return config

    @staticmethod
    def _proteins(session: Session, set_id: uuid.UUID) -> list[str]:
        rows = session.execute(
            text(
                "SELECT DISTINCT protein_accession FROM go_prediction "
                "WHERE prediction_set_id = :s ORDER BY protein_accession"
            ),
            {"s": set_id},
        )
        return [row[0] for row in rows]

    @staticmethod
    def _create_scored_set(
        session: Session,
        source: PredictionSet,
        config: ScoringConfig,
        p: ScorePredictionSetPayload,
        job_id: str | None,
    ) -> PredictionSet:
        depth = {
            "max_distance": p.max_distance,
            "max_k_position": p.max_k_position,
            "max_sequence_rank": p.max_sequence_rank,
        }
        meta = {
            **(source.meta or {}),
            "job_id": job_id,
            "scored_from": str(source.id),
            "scoring_config_id": str(config.id),
            "scoring": {
                **_arithmetic(config),
                "depth": depth,
                "code_revision": code_revision(),
                "dependency_revisions": dependency_revisions(),
            },
            "denials": {"honoured": True, **denials.POLICY},
        }
        cut = [f"{name} <= {value}" for name, value in depth.items() if value is not None]
        scored = PredictionSet(
            **{name: getattr(source, name) for name in _KEPT_COLUMNS},
            meta=meta,
            description=(
                f"Prediction set {source.id} scored with {config.name!r} ({config.formula})"
                + (f", cut at {' and '.join(cut)}" if cut else "")
                + ", the bank's denials honoured."
            ),
            job_id=uuid.UUID(job_id) if job_id else None,
            producer_git_sha=code_revision(),
        )
        session.add(scored)
        session.flush()
        return scored

    @staticmethod
    def _score_chunk(session: Session, run: _Run, proteins: list[str], totals: _Totals) -> None:
        """Read, cut, deny and score the candidates of some proteins, and write them."""
        p = run.payload
        ctx = _Chunk(
            pred_set_id=run.source.id,
            delta_proteins=set(proteins),
            max_distance=p.max_distance,
            max_k_position=p.max_k_position,
            max_sequence_rank=p.max_sequence_rank,
        )
        frame, raw = build_base_frame(session, ctx, _select, _FRAME_COLS)
        assert_depth_was_applied(session, ctx, frame, count_candidates)
        denied = denials.denied_pairs(
            session, run.source.annotation_set_id, run.source.ontology_snapshot_id, proteins
        )
        frame, removed = denials.drop_denied_frame(frame, denied)
        totals.candidates_read += raw
        totals.denied_removed += removed
        totals.chunks.append(len(proteins))
        if not frame.empty:
            totals.rows_written += _write_rows(session, run.scored_id, frame, run.config)


def _write_rows(session: Session, scored_id: uuid.UUID, frame: Any, config: ScoringConfig) -> int:
    """One row per (protein, term): the score, and the donor of the nearest row."""
    rows = [
        {
            "prediction_set_id": scored_id,
            "protein_accession": protein,
            "go_term_id": int(term_id),
            "ref_protein_accession": donor,
            "method_score": float(score),
        }
        for protein, term_id, donor, score in zip(
            frame["protein_accession"].tolist(),
            frame["go_term_id"].tolist(),
            frame["ref_protein_accession"].tolist(),
            vectorized_scores(frame, config).tolist(),
            strict=True,
        )
    ]
    for start in range(0, len(rows), _INSERT_ROWS):
        session.execute(insert(GOPrediction.__table__), rows[start : start + _INSERT_ROWS])
    return len(rows)
