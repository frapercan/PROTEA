"""Feature / annotation / embedding loading mixin for the batch op.

Pulled out of the monolithic ``predict_go_terms.py`` as part of T2B.6.
Holds the methods that materialise per-batch query embeddings, the
non-negated annotation map, sequences / taxonomy fields for the
optional feature families, and the GO term metadata map consumed by the
adapter.
"""

from __future__ import annotations

import uuid
from typing import Any

import numpy as np
from sqlalchemy.orm import Session

from protea.core.annotation_intern import intern_string
from protea.core.contracts.operation import EmitFn
from protea.core.operations.predict_go_terms._batch_op_reference import (
    _restrict_annotations,
)
from protea.core.operations.predict_go_terms._common import (
    PredictGOTermsBatchPayload,
)
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.protein_go_annotation import ProteinGOAnnotation
from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.query.query_set import QuerySetEntry
from protea.infrastructure.orm.models.sequence.sequence import Sequence


class _FeatureLoadingMixin:
    """Annotation, sequence, taxonomy + GO-metadata loaders.

    Mixed into :class:`PredictGOTermsBatchOperation`.
    """

    def _load_feature_engineering_data(
        self,
        session: Session,
        p: PredictGOTermsBatchPayload,
        valid_accessions: list[str],
        all_unique_neighbors: set[str],
    ) -> tuple[
        dict[str, str],
        dict[str, str],
        dict[str, int | None],
        dict[str, int | None],
    ]:
        """Load sequences and taxonomy IDs for downstream feature engineering.

        Each tuple slot is empty when the corresponding flag
        (``compute_alignments`` / ``compute_taxonomy``) is False, so the
        caller can pass them straight into the per-pair feature builder
        without further conditionals.

        Returns ``(ref_sequences, query_sequences, ref_tax_ids, query_tax_ids)``.
        """
        ref_sequences: dict[str, str] = {}
        query_sequences: dict[str, str] = {}
        ref_tax_ids: dict[str, int | None] = {}
        query_tax_ids: dict[str, int | None] = {}

        if p.compute_alignments:
            ref_sequences = self._load_sequences_for_proteins(session, all_unique_neighbors)
            query_sequences = self._load_sequences_for_queries(session, p, valid_accessions)

        if p.compute_taxonomy:
            ref_tax_ids = self._load_taxonomy_ids_for_proteins(session, all_unique_neighbors)
            query_tax_ids = self._load_taxonomy_ids_for_queries(session, p, valid_accessions)

        return ref_sequences, query_sequences, ref_tax_ids, query_tax_ids

    def _load_annotations_for(
        self,
        session: Session,
        annotation_set_id: uuid.UUID,
        accessions: set[str],
        aspect: str | None = None,
        donor_policy: Any = None,
    ) -> dict[str, list[dict[str, Any]]]:
        """Load GO annotations for the given accessions, chunked to avoid param limits.

        Only non-negated annotations are loaded: rows with a NOT qualifier
        (e.g. ``'NOT'``, ``'NOT|involved_in'``) assert that the protein does
        *not* have the annotated function and must never be transferred as
        positive predictions. When ``aspect`` is provided (``'P'`` / ``'F'``
        / ``'C'``), only annotations whose GO term belongs to that aspect are
        returned (per-aspect KNN mode).
        """
        from protea.config.tuning import get_tuning

        chunk_size = get_tuning().operation.annotation_chunk_size
        go_map: dict[str, list[dict[str, Any]]] = {}
        accessions_list = list(accessions)
        for i in range(0, len(accessions_list), chunk_size):
            chunk = accessions_list[i : i + chunk_size]
            rows = self._fetch_annotation_chunk(
                session, annotation_set_id, chunk, aspect, donor_policy
            )
            for acc, go_term_id, qualifier, evidence_code in rows:
                go_map.setdefault(acc, []).append(
                    {
                        "go_term_id": go_term_id,
                        # Flyweight: qualifier / evidence_code take ~5-10
                        # distinct values across millions of rows; interning
                        # collapses every duplicate to one shared string.
                        "qualifier": intern_string(qualifier),
                        "evidence_code": intern_string(evidence_code),
                    }
                )
        return go_map

    def _fetch_annotation_chunk(
        self,
        session: Session,
        annotation_set_id: uuid.UUID,
        chunk: list[str],
        aspect: str | None,
        donor_policy: Any = None,
    ) -> list[Any]:
        """Query one accession chunk for the annotations the policy admits.

        Returns the raw rows; the join to ``go_term`` is added only when
        aspect filtering is requested so the common (non-aspect-separated)
        path stays as fast as before. ``qualifier IS NULL`` is preserved
        explicitly because SQL ``LIKE`` returns NULL for NULL inputs and
        would otherwise drop those rows.
        """
        q = session.query(
            ProteinGOAnnotation.protein_accession,
            ProteinGOAnnotation.go_term_id,
            ProteinGOAnnotation.qualifier,
            ProteinGOAnnotation.evidence_code,
        ).filter(
            ProteinGOAnnotation.annotation_set_id == annotation_set_id,
            ProteinGOAnnotation.protein_accession.in_(chunk),
            (
                ProteinGOAnnotation.qualifier.is_(None)
                | ~ProteinGOAnnotation.qualifier.like("%NOT%")
            ),
        )
        if aspect is not None:
            q = q.join(ProteinGOAnnotation.go_term).filter(GOTerm.aspect == aspect)
        # The policy restricts what may be DONATED, so it belongs here and not
        # only on the query that admits proteins to the pool. Admitting a
        # protein on the strength of one experimental annotation and then
        # letting it donate every annotation it has is how 1,523,939 of
        # 2,801,404 stored rows came to carry an evidence code the policy
        # excludes, and how 1,300 cells with no experimental prior were
        # predicted from the protein's own IEA row.
        return _restrict_annotations(q, donor_policy).all()

    def _load_query_embeddings(
        self,
        session: Session,
        query_accessions: list[str],
        embedding_config_id: uuid.UUID,
        p: PredictGOTermsBatchPayload,
        emit: EmitFn,
    ) -> tuple[np.ndarray, list[str]]:
        """Load embeddings for this batch's query accessions.

        Returns (embeddings, valid_accessions); only accessions that actually
        have an embedding are included.
        """
        if p.query_set_id:
            query_set_id = uuid.UUID(p.query_set_id)
            rows = (
                session.query(QuerySetEntry.accession, SequenceEmbedding.embedding)
                .join(
                    SequenceEmbedding,
                    (SequenceEmbedding.sequence_id == QuerySetEntry.sequence_id)
                    & (SequenceEmbedding.embedding_config_id == embedding_config_id),
                )
                .filter(
                    QuerySetEntry.query_set_id == query_set_id,
                    QuerySetEntry.accession.in_(query_accessions),
                )
                .all()
            )
        else:
            rows = (
                session.query(Protein.accession, SequenceEmbedding.embedding)
                .join(Protein.sequence)
                .join(
                    SequenceEmbedding,
                    (SequenceEmbedding.sequence_id == Protein.sequence_id)
                    & (SequenceEmbedding.embedding_config_id == embedding_config_id),
                )
                .filter(Protein.accession.in_(query_accessions))
                .all()
            )

        if not rows:
            return np.empty((0,)), []

        valid_accessions = [r[0] for r in rows]
        # Rows return pgvector HalfVector instances (halfvec column since 2026-04-11).
        embeddings = np.array([r[1].to_list() for r in rows], dtype=np.float32)
        return embeddings, valid_accessions

    def _load_sequences_for_proteins(
        self, session: Session, accessions: set[str]
    ) -> dict[str, str]:
        from protea.config.tuning import get_tuning

        chunk_size = get_tuning().operation.annotation_chunk_size
        result: dict[str, str] = {}
        acc_list = list(accessions)
        for i in range(0, len(acc_list), chunk_size):
            chunk = acc_list[i : i + chunk_size]
            rows = (
                session.query(Protein.accession, Sequence.sequence)
                .join(Protein.sequence)
                .filter(Protein.accession.in_(chunk))
                .all()
            )
            for acc, seq in rows:
                result[acc] = seq
        return result

    def _load_sequences_for_queries(
        self,
        session: Session,
        p: PredictGOTermsBatchPayload,
        accessions: list[str],
    ) -> dict[str, str]:
        if p.query_set_id:
            query_set_id = uuid.UUID(p.query_set_id)
            rows = (
                session.query(QuerySetEntry.accession, Sequence.sequence)
                .join(QuerySetEntry.sequence)
                .filter(QuerySetEntry.query_set_id == query_set_id)
                .all()
            )
            return {acc: seq for acc, seq in rows}
        return self._load_sequences_for_proteins(session, set(accessions))

    def _load_taxonomy_ids_for_proteins(
        self, session: Session, accessions: set[str]
    ) -> dict[str, int | None]:
        from protea.config.tuning import get_tuning

        chunk_size = get_tuning().operation.annotation_chunk_size
        result: dict[str, int | None] = {}
        acc_list = list(accessions)
        for i in range(0, len(acc_list), chunk_size):
            chunk = acc_list[i : i + chunk_size]
            rows = (
                session.query(Protein.accession, Protein.taxonomy_id)
                .filter(Protein.accession.in_(chunk))
                .all()
            )
            for acc, tid in rows:
                result[acc] = int(tid) if tid else None
        return result

    def _load_taxonomy_ids_for_queries(
        self,
        session: Session,
        p: PredictGOTermsBatchPayload,
        accessions: list[str],
    ) -> dict[str, int | None]:
        from protea.config.tuning import get_tuning

        chunk_size = get_tuning().operation.annotation_chunk_size
        acc_set = set(accessions)
        result: dict[str, int | None] = {acc: None for acc in acc_set}
        acc_list = list(acc_set)
        for i in range(0, len(acc_list), chunk_size):
            chunk = acc_list[i : i + chunk_size]
            rows = (
                session.query(Protein.accession, Protein.taxonomy_id)
                .filter(Protein.accession.in_(chunk))
                .all()
            )
            for acc, tid in rows:
                result[acc] = int(tid) if tid else None
        return result

    def _load_go_term_metadata(
        self,
        session: Session,
        annotations: dict[str, list[dict[str, Any]]],
    ) -> tuple[dict[int, str], dict[int, str]]:
        """Build ``(go_id_map, go_aspect_map)`` for every gtid in play."""
        gtids_in_play: set[int] = {
            int(ann["go_term_id"]) for anns in annotations.values() for ann in anns
        }
        go_id_map: dict[int, str] = {}
        go_aspect_map: dict[int, str] = {}
        if not gtids_in_play:
            return go_id_map, go_aspect_map
        from protea.config.tuning import get_tuning

        chunk_size = get_tuning().operation.annotation_chunk_size
        ids_list = list(gtids_in_play)
        for i in range(0, len(ids_list), chunk_size):
            chunk = ids_list[i : i + chunk_size]
            rows = (
                session.query(GOTerm.id, GOTerm.go_id, GOTerm.aspect)
                .filter(GOTerm.id.in_(chunk))
                .all()
            )
            for gid, go_str, aspect in rows:
                go_id_map[gid] = go_str
                go_aspect_map[gid] = aspect or ""
        return go_id_map, go_aspect_map

    def _attach_go_term_aspect(
        self,
        session: Session,
        prediction_dicts: list[dict[str, Any]],
    ) -> None:
        """Look up ``GOTerm.aspect`` for every unique ``go_term_id`` and
        write it back onto each prediction dict so the reranker's
        categorical feature is populated.
        """
        unique_ids = {
            rec["go_term_id"] for rec in prediction_dicts if rec.get("go_term_id") is not None
        }
        if not unique_ids:
            return
        aspect_by_id: dict[int, str] = dict(
            session.query(GOTerm.id, GOTerm.aspect).filter(GOTerm.id.in_(unique_ids)).all()
        )
        for rec in prediction_dicts:
            gid = rec.get("go_term_id")
            if gid is not None and gid in aspect_by_id:
                rec["aspect"] = aspect_by_id[gid]
