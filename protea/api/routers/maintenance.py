from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from protea.api.deps import get_session_factory
from protea.api.roles import ROLE_ADMIN, require_role
from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding
from protea.infrastructure.orm.models.sequence.sequence import Sequence
from protea.infrastructure.session import session_scope

router = APIRouter(prefix="/maintenance", tags=["maintenance"])


@router.get("/vacuum-sequences/preview")
def preview_orphan_sequences(
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Count orphan sequences without running the delete.

    A sequence is orphaned when it has no Protein rows pointing to it
    AND no QuerySetEntry rows pointing to it.
    """
    with session_scope(factory) as session:
        total = session.query(Sequence).count()
        orphan_count = session.execute(
            text("""
            SELECT COUNT(*)
            FROM sequence s
            WHERE NOT EXISTS (
                SELECT 1 FROM protein p WHERE p.sequence_id = s.id
            )
            AND NOT EXISTS (
                SELECT 1 FROM query_set_entry qse WHERE qse.sequence_id = s.id
            )
        """)
        ).scalar()

    return {
        "total_sequences": total,
        "orphan_sequences": orphan_count,
        "referenced_sequences": total - orphan_count,
    }


@router.post(
    "/vacuum-sequences",
    dependencies=[Depends(require_role(ROLE_ADMIN))],
)
def vacuum_sequences(
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Delete sequences not referenced by any Protein or QuerySetEntry.

    Destructive: removes rows from ``sequence``. Gated to ``admin`` so a
    compromised operator key cannot reduce the corpus. Orphan sequences
    have no embeddings reachable from any active protein or query set,
    but the deletion is permanent and feeds into downstream foreign
    keys, so it stays on the admin floor with the other DB-mutating
    housekeeping operations.
    """
    with session_scope(factory) as session:
        # Collect IDs first to do a targeted delete (avoids full-table lock)
        orphan_ids = [
            row[0]
            for row in session.execute(
                text("""
                SELECT s.id
                FROM sequence s
                WHERE NOT EXISTS (
                    SELECT 1 FROM protein p WHERE p.sequence_id = s.id
                )
                AND NOT EXISTS (
                    SELECT 1 FROM query_set_entry qse WHERE qse.sequence_id = s.id
                )
            """)
            ).fetchall()
        ]

        if not orphan_ids:
            return {"deleted_sequences": 0}

        deleted = (
            session.query(Sequence)
            .filter(Sequence.id.in_(orphan_ids))
            .delete(synchronize_session=False)
        )

    return {"deleted_sequences": deleted}


@router.get("/vacuum-embeddings/preview")
def preview_unindexed_embeddings(
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Count embeddings for sequences not referenced by any Protein.

    These are embeddings computed for query proteins (QuerySet uploads) or
    orphan sequences. They are safe to delete once predictions have been run.
    """
    with session_scope(factory) as session:
        total = session.query(SequenceEmbedding).count()
        unindexed_count = session.execute(
            text("""
            SELECT COUNT(*)
            FROM sequence_embedding se
            WHERE NOT EXISTS (
                SELECT 1 FROM protein p WHERE p.sequence_id = se.sequence_id
            )
        """)
        ).scalar()

    return {
        "total_embeddings": total,
        "unindexed_embeddings": unindexed_count,
        "indexed_embeddings": total - unindexed_count,
    }


@router.post(
    "/vacuum-embeddings",
    dependencies=[Depends(require_role(ROLE_ADMIN))],
)
def vacuum_embeddings(
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """Delete embeddings for sequences not referenced by any Protein.

    Destructive: removes rows from ``sequence_embedding``. Gated to
    ``admin`` so the embedding corpus (expensive to recompute on GPUs)
    cannot be wiped by an operator key. Safe to run once predictions
    have been generated; query-protein embeddings are only needed
    during the prediction job itself.
    """
    with session_scope(factory) as session:
        unindexed_ids = [
            row[0]
            for row in session.execute(
                text("""
                SELECT se.id
                FROM sequence_embedding se
                WHERE NOT EXISTS (
                    SELECT 1 FROM protein p WHERE p.sequence_id = se.sequence_id
                )
            """)
            ).fetchall()
        ]

        if not unindexed_ids:
            return {"deleted_embeddings": 0}

        deleted = (
            session.query(SequenceEmbedding)
            .filter(SequenceEmbedding.id.in_(unindexed_ids))
            .delete(synchronize_session=False)
        )

    return {"deleted_embeddings": deleted}
