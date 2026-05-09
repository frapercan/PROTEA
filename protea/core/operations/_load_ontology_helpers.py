"""Pure helpers extracted from ``LoadOntologySnapshotOperation.execute``.

The OBO load path branches into a fresh-snapshot insert and an
already-exists-but-needs-backfill case; both paths build
``GOTermRelationship`` rows via the same loop. This module hosts the
shared relationship builder plus the two branch handlers, so the
operation file (``load_ontology_snapshot.py``) can keep ``execute``
under the master plan v3.2 §3 method-LOC ceiling.
"""

from __future__ import annotations

import time
import uuid
from typing import Any

from sqlalchemy import func as sa_func
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.go_term_relationship import GOTermRelationship
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot


def build_relationships(
    terms: list[dict[str, Any]],
    go_id_to_db_id: dict[str, uuid.UUID],
    snapshot_id: uuid.UUID,
) -> list[GOTermRelationship]:
    """Materialise GOTermRelationship rows for the given parsed terms.

    ``terms`` carry their parent edges in ``("relation_type", "GO:nnn")``
    tuples under the ``relationships`` key (parser output). Edges
    pointing at GO ids absent from ``go_id_to_db_id`` are silently
    skipped so a partial snapshot does not abort the whole insert.
    """
    rows: list[GOTermRelationship] = []
    for t in terms:
        child_db_id = go_id_to_db_id.get(t["go_id"])
        if child_db_id is None:
            continue
        for rel_type, parent_go_id in t.get("relationships", []):
            parent_db_id = go_id_to_db_id.get(parent_go_id)
            if parent_db_id is None:
                continue
            rows.append(
                GOTermRelationship(
                    child_go_term_id=child_db_id,
                    parent_go_term_id=parent_db_id,
                    relation_type=rel_type,
                    ontology_snapshot_id=snapshot_id,
                )
            )
    return rows


def _count_relationships(session: Session, snapshot_id: uuid.UUID) -> int:
    return (
        session.query(sa_func.count(GOTermRelationship.id))
        .filter(GOTermRelationship.ontology_snapshot_id == snapshot_id)
        .scalar()
        or 0
    )


def _backfill_relationships(
    session: Session,
    existing: OntologySnapshot,
    terms: list[dict[str, Any]],
    obo_version: str,
    emit: EmitFn,
) -> OperationResult:
    emit(
        "load_ontology_snapshot.backfill_relationships",
        None,
        {"ontology_snapshot_id": str(existing.id)},
        "info",
    )
    go_id_to_db_id = {
        go_id: db_id
        for go_id, db_id in session.query(GOTerm.go_id, GOTerm.id)
        .filter(GOTerm.ontology_snapshot_id == existing.id)
        .all()
    }
    rows = build_relationships(terms, go_id_to_db_id, existing.id)
    session.add_all(rows)
    session.flush()
    emit(
        "load_ontology_snapshot.backfill_done",
        None,
        {"relationships_inserted": len(rows)},
        "info",
    )
    return OperationResult(
        result={
            "ontology_snapshot_id": str(existing.id),
            "obo_version": obo_version,
            "skipped": False,
            "relationships_inserted": len(rows),
        }
    )


def handle_existing_snapshot(
    session: Session,
    existing: OntologySnapshot,
    terms: list[dict[str, Any]],
    obo_version: str,
    emit: EmitFn,
) -> OperationResult:
    """Resolve an idempotent re-run of ``load_ontology_snapshot``.

    Returns the existing snapshot id as a no-op when its relationship
    rows are already present; otherwise dispatches to
    :func:`_backfill_relationships` (the GO terms themselves are
    assumed present from the original load).
    """
    if _count_relationships(session, existing.id) > 0:
        emit(
            "load_ontology_snapshot.already_exists",
            None,
            {"ontology_snapshot_id": str(existing.id), "obo_version": obo_version},
            "info",
        )
        return OperationResult(
            result={
                "ontology_snapshot_id": str(existing.id),
                "obo_version": obo_version,
                "skipped": True,
            }
        )

    return _backfill_relationships(session, existing, terms, obo_version, emit)


def insert_new_snapshot(
    session: Session,
    obo_url: str,
    obo_version: str,
    terms: list[dict[str, Any]],
    started_at: float,
    emit: EmitFn,
) -> OperationResult:
    """Create the snapshot, terms, and relationship rows in a single flow.

    The caller has already validated the payload, downloaded the OBO,
    and parsed the terms; this function commits the three inserts and
    emits the ``done`` audit event with the elapsed runtime.
    """
    snapshot = OntologySnapshot(obo_url=obo_url, obo_version=obo_version)
    session.add(snapshot)
    session.flush()

    go_terms = [
        GOTerm(
            go_id=t["go_id"],
            name=t["name"],
            aspect=t["aspect"],
            definition=t["definition"],
            is_obsolete=t["is_obsolete"],
            ontology_snapshot_id=snapshot.id,
        )
        for t in terms
    ]
    session.add_all(go_terms)
    session.flush()

    go_id_to_db_id = {gt.go_id: gt.id for gt in go_terms}
    relationships = build_relationships(terms, go_id_to_db_id, snapshot.id)
    session.add_all(relationships)
    session.flush()

    summary = {
        "ontology_snapshot_id": str(snapshot.id),
        "obo_version": obo_version,
        "terms_inserted": len(go_terms),
        "relationships_inserted": len(relationships),
        "elapsed_seconds": time.perf_counter() - started_at,
    }
    emit("load_ontology_snapshot.done", None, summary, "info")
    return OperationResult(result=summary)
