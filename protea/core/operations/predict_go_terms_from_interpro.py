"""IP.4b-PRED — InterPro-only GO term predictor.

A *parallel-signal* GO term predictor. Given a set of query proteins
whose InterProScan hits are already persisted as
:class:`InterProAnnotation` rows (IP.2 / IP.3), this operation:

1. Joins each protein's distinct InterPro accessions against the
   :class:`InterProGoMapping` table (IP.4a — InterPro2GO loader) at
   the requested ``source_version``.
2. Resolves the right-hand GO ids against an :class:`OntologySnapshot`
   so the output is keyed by ``go_term_id`` like every other
   :class:`GOPrediction`.
3. Aggregates the per-protein votes: if N distinct InterPro entries
   on the same protein map to the same GO term, that's N supporting
   votes and a stronger prediction than a single mapping.
4. Persists the result as a fresh :class:`PredictionSet` with
   ``meta["algorithm"] = "interpro_propagation"`` so downstream
   ensemble / cafaeval routers can pick it up alongside KNN-based
   :class:`PredictionSet` rows (T-RES.3 ensemble multi-K scaffolding).

The operation is single-stage (no batch fan-out): the join is a
cheap index scan and the per-protein InterPro hit count is tiny
(O(10-100)) so partitioning would be pure overhead.

Idempotency
-----------

Within one call: per-protein duplicate ``(go_term_id)`` votes are
merged before insert. Across calls: every call materialises a new
:class:`PredictionSet`; the *predictions* underneath it are unique
by ``uq_go_prediction_set_protein_term`` and we additionally
``ON CONFLICT DO NOTHING`` so a partial-failure retry on the same
prediction set inserts no duplicates.

GOPrediction column mapping
---------------------------

The :class:`GOPrediction` schema was designed for KNN transfer, so a
few NOT-NULL columns need a sensible value for the InterPro path:

* ``ref_protein_accession`` — the InterPro accession that voted
  for this term, e.g. ``"IPR000001"``. Multiple voters: the one with
  the lexicographically smallest accession (stable, observable).
* ``distance`` — the inverse vote count
  (``1.0 / vote_count``). Lower = stronger support, matching the
  KNN convention; a 1-voter prediction has distance 1.0, a 5-voter
  prediction 0.2.
* ``evidence_code`` — propagated from the
  :class:`InterProGoMapping.evidence` column (defaults to ``"IEA"``).
* ``vote_count`` — the literal number of distinct IPR entries on
  this protein that map to this GO id at this ``source_version``.

Out of scope (see IP.4 plan-first sub-plan):

* Reranker feature integration (IP.4c, ``requires_human=true``).
* Score blending across PredictionSets (T-RES.3 ensemble router).
* Ancestor / aspect propagation (the caller is expected to enable
  the existing post-process pipeline if needed).
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from pydantic import field_validator, model_validator
from sqlalchemy import and_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.utils import contract_payload
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.interpro_annotation import (
    InterProAnnotation,
)
from protea.infrastructure.orm.models.annotation.interpro_go_mapping import (
    InterProGoMapping,
)
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.query.query_set import QuerySet, QuerySetEntry

_ALGORITHM_TAG = "interpro_propagation"


class PredictGOTermsFromInterProPayload(ProteaPayload, frozen=True):
    """Inputs for ``predict_go_terms_from_interpro``.

    Either ``query_set_id`` or ``query_accessions`` is required. Both
    may be set — the union is used. ``embedding_config_id``,
    ``annotation_set_id``, ``ontology_snapshot_id`` are required by
    the existing :class:`PredictionSet` schema (NOT NULL FKs); for
    this parallel signal they act as trace pointers (the model wasn't
    actually used during prediction).

    ``source_version`` is the InterPro2GO mapping release tag
    (see :class:`LoadInterProGoMappingPayload`). Predictions are
    filtered to mappings tagged with this exact ``source_version`` so
    a release skew between snapshots is observable rather than
    silently mixed.
    """

    embedding_config_id: str
    annotation_set_id: str
    ontology_snapshot_id: str
    source_version: str
    query_set_id: str | None = None
    query_accessions: list[str] | None = None
    ipr_release_floor: str | None = None
    chunk_size: int = 1000

    @field_validator(
        "embedding_config_id",
        "annotation_set_id",
        "ontology_snapshot_id",
        "source_version",
        mode="before",
    )
    @classmethod
    def _non_empty(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("must be a non-empty string")
        return v.strip()

    @field_validator("query_set_id", "ipr_release_floor", mode="before")
    @classmethod
    def _opt_non_empty(cls, v: str | None) -> str | None:
        if v is None:
            return v
        if not isinstance(v, str) or not v.strip():
            raise ValueError("must be a non-empty string when provided")
        return v.strip()

    @field_validator("query_accessions", mode="before")
    @classmethod
    def _non_empty_list(cls, v: list[str] | None) -> list[str] | None:
        if v is None:
            return v
        cleaned = [s.strip() for s in v if isinstance(s, str) and s.strip()]
        if not cleaned:
            raise ValueError("query_accessions must be a non-empty list when provided")
        return cleaned

    @field_validator("chunk_size", mode="before")
    @classmethod
    def _chunk_size_positive(cls, v: int) -> int:
        if not isinstance(v, int) or v <= 0:
            raise ValueError("chunk_size must be a positive integer")
        return v

    @model_validator(mode="after")
    def _at_least_one_query_input(self) -> PredictGOTermsFromInterProPayload:
        if self.query_set_id is None and self.query_accessions is None:
            raise ValueError(
                "either query_set_id or query_accessions must be provided"
            )
        return self


@dataclass(frozen=True)
class _ResolvedIds:
    """FK UUIDs resolved + validated up front for type clarity."""

    embedding_config_id: uuid.UUID
    annotation_set_id: uuid.UUID
    ontology_snapshot_id: uuid.UUID


@dataclass
class _RunCounters:
    """Mutable accumulator threaded through the main loop."""

    proteins_with_ipr: int = 0
    proteins_with_predictions: int = 0
    candidate_pairs: int = 0
    predictions_inserted: int = 0
    ipr_releases: set[str] = field(default_factory=set)


@dataclass(frozen=True)
class _RunContext:
    """Immutable bundle of inputs threaded through the row-build helpers.

    Replaces a 6-argument signature on ``_build_prediction_rows`` with
    a single context object so the flake8-bugbear / smell-budget
    parameter ceiling stays satisfied.
    """

    p: PredictGOTermsFromInterProPayload
    ids: _ResolvedIds
    accessions: list[str]
    prediction_set_id: uuid.UUID


class PredictGOTermsFromInterProOperation:
    """Emit GO predictions for proteins via their persisted InterPro hits.

    Single-stage operation: validates FKs, materialises one
    :class:`PredictionSet`, performs an indexed three-table join
    (``interpro_annotation`` ⨝ ``interpro_go_mapping`` ⨝ ``go_term``),
    aggregates per-protein votes, and bulk-inserts the
    :class:`GOPrediction` rows.
    """

    name = "predict_go_terms_from_interpro"
    description = (
        "Predict GO terms for a query set by joining each protein's "
        "InterProAnnotation rows against the InterPro2GO mapping table; "
        "emits a PredictionSet tagged algorithm=interpro_propagation."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        p = payload or {}
        bits: list[str] = []
        if p.get("source_version"):
            bits.append(f"i2g={p['source_version']}")
        if p.get("ipr_release_floor"):
            bits.append(f"ipr>={p['ipr_release_floor']}")
        if p.get("query_set_id"):
            bits.append(f"qs={str(p['query_set_id'])[:8]}")
        accs = p.get("query_accessions") or []
        if accs:
            bits.append(f"n={len(accs)}")
        return " · ".join(bits)

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = PredictGOTermsFromInterProPayload.model_validate(contract_payload(payload))
        ids = self._validate_inputs(session, p)
        t0 = time.perf_counter()
        self._emit_start(emit, p)

        accessions = self._resolve_query_accessions(session, p, emit)
        if not accessions:
            emit("predict_go_terms_from_interpro.no_queries", None, {}, "warning")
            return OperationResult(result={"proteins": 0, "predictions": 0})

        prediction_set = self._create_prediction_set(session, p, ids)
        ctx = _RunContext(
            p=p, ids=ids, accessions=accessions, prediction_set_id=prediction_set.id
        )
        counters = self._run_pipeline(session, ctx)
        self._stamp_prediction_set_meta(session, prediction_set, p, counters)

        result = self._build_result(prediction_set, accessions, counters, t0)
        emit("predict_go_terms_from_interpro.done", None, result, "info")
        return OperationResult(result=result)

    @staticmethod
    def _emit_start(emit: EmitFn, p: PredictGOTermsFromInterProPayload) -> None:
        emit(
            "predict_go_terms_from_interpro.start",
            None,
            {
                "source_version": p.source_version,
                "ontology_snapshot_id": p.ontology_snapshot_id,
                "ipr_release_floor": p.ipr_release_floor,
            },
            "info",
        )

    def _run_pipeline(self, session: Session, ctx: _RunContext) -> _RunCounters:
        """Materialise + persist prediction rows; return the run counters."""
        counters = _RunCounters()
        rows = list(self._build_prediction_rows(session, ctx, counters))
        if rows:
            counters.predictions_inserted = self._bulk_insert(
                session, rows, ctx.p.chunk_size
            )
        return counters

    @staticmethod
    def _stamp_prediction_set_meta(
        session: Session,
        prediction_set: PredictionSet,
        p: PredictGOTermsFromInterProPayload,
        counters: _RunCounters,
    ) -> None:
        """Stamp the algorithm + source pointers into ``PredictionSet.meta``.

        Lets ensemble routers route on ``meta.algorithm`` without adding
        a column to the table.
        """
        prediction_set.meta = {
            "algorithm": _ALGORITHM_TAG,
            "interpro_mapping_source_version": p.source_version,
            "ipr_release_floor": p.ipr_release_floor,
            "ipr_releases_seen": sorted(counters.ipr_releases),
        }
        session.add(prediction_set)

    @staticmethod
    def _build_result(
        prediction_set: PredictionSet,
        accessions: list[str],
        counters: _RunCounters,
        t0: float,
    ) -> dict[str, Any]:
        return {
            "prediction_set_id": str(prediction_set.id),
            "algorithm": _ALGORITHM_TAG,
            "proteins_requested": len(accessions),
            "proteins_with_ipr": counters.proteins_with_ipr,
            "proteins_with_predictions": counters.proteins_with_predictions,
            "candidate_pairs": counters.candidate_pairs,
            "predictions_inserted": counters.predictions_inserted,
            "elapsed_seconds": time.perf_counter() - t0,
        }

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_inputs(
        session: Session, p: PredictGOTermsFromInterProPayload
    ) -> _ResolvedIds:
        ids = _ResolvedIds(
            embedding_config_id=uuid.UUID(p.embedding_config_id),
            annotation_set_id=uuid.UUID(p.annotation_set_id),
            ontology_snapshot_id=uuid.UUID(p.ontology_snapshot_id),
        )
        if session.get(EmbeddingConfig, ids.embedding_config_id) is None:
            raise ValueError(f"EmbeddingConfig {p.embedding_config_id} not found")
        if session.get(AnnotationSet, ids.annotation_set_id) is None:
            raise ValueError(f"AnnotationSet {p.annotation_set_id} not found")
        if session.get(OntologySnapshot, ids.ontology_snapshot_id) is None:
            raise ValueError(f"OntologySnapshot {p.ontology_snapshot_id} not found")
        return ids

    @staticmethod
    def _resolve_query_accessions(
        session: Session,
        p: PredictGOTermsFromInterProPayload,
        emit: EmitFn,
    ) -> list[str]:
        """Return the union of accessions implied by ``query_set_id`` and ``query_accessions``."""
        acc_set: set[str] = set()
        if p.query_accessions:
            acc_set.update(p.query_accessions)
        if p.query_set_id:
            qs_id = uuid.UUID(p.query_set_id)
            if session.get(QuerySet, qs_id) is None:
                raise ValueError(f"QuerySet {p.query_set_id} not found")
            rows = session.scalars(
                select(QuerySetEntry.accession).where(QuerySetEntry.query_set_id == qs_id)
            ).all()
            acc_set.update(rows)
        out = sorted(acc_set)
        emit(
            "predict_go_terms_from_interpro.queries_resolved",
            None,
            {"proteins": len(out)},
            "info",
        )
        return out

    # ------------------------------------------------------------------
    # PredictionSet + main join
    # ------------------------------------------------------------------

    @staticmethod
    def _create_prediction_set(
        session: Session,
        p: PredictGOTermsFromInterProPayload,
        ids: _ResolvedIds,
    ) -> PredictionSet:
        """Materialise the PredictionSet up-front so the FK is available for inserts.

        ``limit_per_entry`` carries no meaning for the InterPro path —
        each protein emits as many predictions as it has distinct GO
        mappings, with no top-K cap — but the column is NOT NULL on
        the table, so it's filled with ``0`` as a sentinel value that
        downstream readers can recognise (see RERANKER.md).
        """
        ps = PredictionSet(
            embedding_config_id=ids.embedding_config_id,
            annotation_set_id=ids.annotation_set_id,
            ontology_snapshot_id=ids.ontology_snapshot_id,
            query_set_id=uuid.UUID(p.query_set_id) if p.query_set_id else None,
            limit_per_entry=0,
            distance_threshold=None,
            meta={"algorithm": _ALGORITHM_TAG, "pending": True},
        )
        session.add(ps)
        session.flush()
        return ps

    def _build_prediction_rows(
        self,
        session: Session,
        ctx: _RunContext,
        counters: _RunCounters,
    ) -> Iterable[dict[str, Any]]:
        """Yield one GOPrediction-row dict per ``(protein, go_term)`` pair.

        Per-protein aggregation merges duplicate IPR votes for the
        same GO id and picks the lexicographically smallest IPR as
        the ``ref_protein_accession`` (stable, easy to eyeball).
        """
        for chunk in self._chunked(ctx.accessions, ctx.p.chunk_size):
            rows = self._fetch_join(session, ctx.p, ctx.ids, chunk, counters)
            yield from self._aggregate(rows, ctx.prediction_set_id, counters)

    @staticmethod
    def _chunked(items: list[str], size: int) -> Iterable[list[str]]:
        for i in range(0, len(items), size):
            yield items[i : i + size]

    @staticmethod
    def _fetch_join(
        session: Session,
        p: PredictGOTermsFromInterProPayload,
        ids: _ResolvedIds,
        accessions: list[str],
        counters: _RunCounters,
    ) -> list[tuple[str, str, str, int, str, str | None]]:
        """Single indexed join across the three annotation tables.

        Returns tuples of
        ``(protein_accession, ipr_accession, go_id, go_term_id,
        ipr_release, evidence)``.
        """
        ipr_release_filter = (
            [InterProAnnotation.ipr_release >= p.ipr_release_floor]
            if p.ipr_release_floor
            else []
        )

        stmt = (
            select(
                InterProAnnotation.protein_accession,
                InterProAnnotation.accession,
                GOTerm.go_id,
                GOTerm.id,
                InterProAnnotation.ipr_release,
                InterProGoMapping.evidence,
            )
            .join(
                InterProGoMapping,
                and_(
                    InterProGoMapping.ipr_accession == InterProAnnotation.accession,
                    InterProGoMapping.source_version == p.source_version,
                ),
            )
            .join(
                GOTerm,
                and_(
                    GOTerm.go_id == InterProGoMapping.go_id,
                    GOTerm.ontology_snapshot_id == ids.ontology_snapshot_id,
                ),
            )
            .where(InterProAnnotation.protein_accession.in_(accessions))
            .where(*ipr_release_filter)
        )

        results = list(session.execute(stmt).all())

        # Audit-trail: how many distinct proteins in this chunk had at
        # least one InterPro hit that yielded at least one mapping row.
        # Cheap to compute against the result list and useful in the
        # done-event payload, so worth the small overhead.
        proteins_in_results = {row[0] for row in results}
        counters.proteins_with_ipr += len(proteins_in_results)
        counters.ipr_releases.update(row[4] for row in results)
        return results

    @staticmethod
    def _aggregate(
        rows: list[tuple[str, str, str, int, str, str | None]],
        prediction_set_id: uuid.UUID,
        counters: _RunCounters,
    ) -> Iterable[dict[str, Any]]:
        """Collapse per-protein duplicate votes for the same GO term.

        Output schema matches a subset of :class:`GOPrediction`:
        ``prediction_set_id``, ``protein_accession``, ``go_term_id``,
        ``ref_protein_accession``, ``distance``, ``evidence_code``,
        ``vote_count``.
        """
        # bucket: (protein, go_term_id) → list of (ipr_accession, evidence)
        bucket: dict[tuple[str, int], list[tuple[str, str | None]]] = {}
        for protein, ipr, _go_id, go_term_id, _ipr_release, evidence in rows:
            bucket.setdefault((protein, go_term_id), []).append((ipr, evidence))

        proteins_with_preds: set[str] = set()
        for (protein, go_term_id), voters in bucket.items():
            voters.sort(key=lambda v: v[0])  # stable: smallest IPR wins ref slot
            vote_count = len({v[0] for v in voters})
            # Prefer the first non-null evidence; fall back to "IEA"
            evidence_code = next((ev for _, ev in voters if ev), "IEA")
            proteins_with_preds.add(protein)
            counters.candidate_pairs += 1
            yield {
                "prediction_set_id": prediction_set_id,
                "protein_accession": protein,
                "go_term_id": go_term_id,
                "ref_protein_accession": voters[0][0],
                "distance": 1.0 / vote_count,
                "evidence_code": evidence_code,
                "vote_count": vote_count,
            }
        counters.proteins_with_predictions += len(proteins_with_preds)

    # ------------------------------------------------------------------
    # Bulk insert
    # ------------------------------------------------------------------

    @staticmethod
    def _bulk_insert(
        session: Session, rows: list[dict[str, Any]], chunk_size: int
    ) -> int:
        """Insert prediction rows via ``ON CONFLICT DO NOTHING``.

        The :class:`GOPrediction` unique constraint
        ``uq_go_prediction_set_protein_term`` covers
        ``(prediction_set_id, protein_accession, go_term_id)`` — we
        let the DB reject duplicates so a partial-failure retry on
        the same prediction set is safe.
        """
        inserted = 0
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i : i + chunk_size]
            stmt = (
                pg_insert(GOPrediction.__table__)
                .values(chunk)
                .on_conflict_do_nothing(
                    constraint="uq_go_prediction_set_protein_term"
                )
                .returning(GOPrediction.__table__.c.id)
            )
            result = session.execute(stmt)
            inserted += len(result.fetchall())
        return inserted


