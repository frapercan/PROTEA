"""Record the ids a snapshot no longer uses as primary, from its archived bytes.

``load_ontology_snapshot`` parses an OBO into ``go_term`` rows and keeps
``go_id``, ``name``, ``aspect``, ``definition`` and ``is_obsolete``. Two
statements the same file makes are dropped: ``alt_id``, a secondary id of a
live term, and ``replaced_by``, the successor an obsolete term names. A third,
``consider``, is the ontology explicitly declining to name one.

WHAT THAT COSTS, measured on this store and not argued:

* a temporal window cannot ask "does this t1 term have a node at t0", because
  the answer can be yes through a successor it cannot see. On the 211 -> 227
  validation window that is nine entries and three proteins of 40,531, each a
  term obsolete in 2022 with a successor that GO REVIVED in 2025;
* a series over releases reads every merge as a retraction plus a gain: 1,657
  of 2016's terms are secondary ids by 2026, all with a known successor.

IT READS THE ARCHIVE, AND REFUSES WITHOUT ONE. The point of the table is to
answer questions about identity years later, so the bytes it is built from
have to be bytes something fixes. ``archive_ontology_snapshot`` records a
sha256 and this verifies it; a snapshot with no archive is refused rather than
re-fetched from ``release.geneontology.org``, which would make the aliases a
function of whatever that server serves today. That is the same reasoning
``resolve_obo`` applies to the OBO an evaluation scores against, held to the
stricter end because there is no deadline here.

THREE KINDS, KEPT APART. ``alt_id`` says "this id IS that term";
``replaced_by`` says "that is what this became"; ``consider`` says "we are not
telling you". A remapping may use the first two and must not use the third,
and collapsing them would make that distinction unavailable to the caller.
The rows are what the ontology said; which of them a rule may follow is the
rule's decision, written where the rule is.
"""

from __future__ import annotations

import hashlib
import tempfile
import uuid
from pathlib import Path
from typing import Any

from pydantic import field_validator
from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.utils import contract_payload
from protea.infrastructure.orm.models.annotation.go_term_alias import (
    KIND_ALT_ID,
    KIND_CONSIDER,
    KIND_REPLACED_BY,
    GOTermAlias,
)
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot


class LoadOntologyAliasesPayload(ProteaPayload, frozen=True):
    """Which snapshot to read, and whether to replace what is already there."""

    ontology_snapshot_id: str
    #: Re-read and replace the rows of a snapshot that already has them.
    #: Without it an already-loaded snapshot is reported and left alone, so a
    #: re-submission is cheap instead of being a rewrite.
    force: bool = False

    @field_validator("ontology_snapshot_id")
    @classmethod
    def _non_empty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("ontology_snapshot_id must be a non-empty string")
        return value.strip()


def parse_aliases(obo_text: str) -> list[tuple[str, str, str]]:
    """``[(alias_go_id, primary_go_id, kind)]`` as the file states them.

    Deliberately a second parser, not a reuse of
    ``LoadOntologySnapshotOperation._parse_terms``: that one answers "which
    terms exist", this one answers "what does this file say about an id it
    does not use", and a parser shared between the two questions would make a
    disagreement between them invisible. The same reasoning
    ``archive_ontology_snapshot`` gives for its congruence parser.

    ``alt_id`` is recorded with the term that lists it as the primary, which
    is the direction a lookup needs: given an old id, which term is it now.
    """
    rows: list[tuple[str, str, str]] = []
    current: str | None = None
    in_term = False
    for raw in obo_text.splitlines():
        line = raw.strip()
        if line == "[Term]":
            in_term, current = True, None
            continue
        if line.startswith("[") and line.endswith("]"):
            in_term, current = False, None
            continue
        if not in_term:
            continue
        if line.startswith("id:"):
            current = line.split(":", 1)[1].strip()
        elif current is None:
            continue
        elif line.startswith("alt_id:"):
            rows.append((line.split(":", 1)[1].strip(), current, KIND_ALT_ID))
        elif line.startswith("replaced_by:"):
            rows.append((current, line.split(":", 1)[1].strip(), KIND_REPLACED_BY))
        elif line.startswith("consider:"):
            rows.append((current, line.split(":", 1)[1].strip(), KIND_CONSIDER))
    return rows


class LoadOntologyAliasesOperation:
    """Fill ``go_term_alias`` for one snapshot from its archived OBO."""

    name = "load_ontology_aliases"
    description = (
        "Record a snapshot's alt_id, replaced_by and consider statements from "
        "its archived OBO, so a window can ask what an old id became."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        return f"read the alias map of snapshot {payload.get('ontology_snapshot_id')}"

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = LoadOntologyAliasesPayload.model_validate(contract_payload(payload))
        snapshot = session.get(OntologySnapshot, uuid.UUID(p.ontology_snapshot_id))
        if snapshot is None:
            raise ValueError(f"OntologySnapshot {p.ontology_snapshot_id} not found")
        if not snapshot.obo_uri or not snapshot.obo_sha256:
            raise ValueError(
                f"OntologySnapshot {snapshot.obo_version} has no archived OBO "
                "(obo_uri / obo_sha256 are unset). Run archive_ontology_snapshot "
                "first: an alias map built from bytes nothing fixes is a map of "
                "whatever release.geneontology.org serves today"
            )
        existing = self._count_existing(session, snapshot.id)
        if existing and not p.force:
            emit(
                "load_ontology_aliases.reused",
                f"{existing} rows already loaded",
                {"rows": existing, "obo_version": snapshot.obo_version},
                "info",
            )
            return OperationResult(
                result={
                    "ontology_snapshot_id": str(snapshot.id),
                    "obo_version": snapshot.obo_version,
                    "rows": existing,
                    "reused": True,
                }
            )

        rows = self._read(snapshot, emit)
        if existing:
            session.execute(
                delete(GOTermAlias).where(GOTermAlias.ontology_snapshot_id == snapshot.id)
            )
        inserted = self._insert(session, snapshot.id, rows)
        counts = {kind: sum(1 for r in rows if r[2] == kind) for kind in (
            KIND_ALT_ID, KIND_REPLACED_BY, KIND_CONSIDER
        )}
        result = {
            "ontology_snapshot_id": str(snapshot.id),
            "obo_version": snapshot.obo_version,
            "rows": inserted,
            "replaced_existing": existing,
            **{f"{kind}_rows": n for kind, n in counts.items()},
        }
        emit(
            "load_ontology_aliases.stored",
            f"{inserted} rows: {counts[KIND_ALT_ID]} alt_id, "
            f"{counts[KIND_REPLACED_BY]} replaced_by, {counts[KIND_CONSIDER]} consider",
            result,
            "info",
        )
        return OperationResult(result=result)

    @staticmethod
    def _count_existing(session: Session, snapshot_id: uuid.UUID) -> int:
        from sqlalchemy import func

        return int(
            session.execute(
                select(func.count())
                .select_from(GOTermAlias)
                .where(GOTermAlias.ontology_snapshot_id == snapshot_id)
            ).scalar_one()
        )

    @staticmethod
    def _read(snapshot: OntologySnapshot, emit: EmitFn) -> list[tuple[str, str, str]]:
        """Fetch the archived bytes, verify the digest, parse."""
        from protea.core.operations import _run_cafa_artifacts as _artifacts

        with tempfile.TemporaryDirectory(prefix="protea_aliases_") as tmp:
            local = Path(tmp) / "go.obo"
            _artifacts.download_tsv(str(snapshot.obo_uri), str(local))
            digest = hashlib.sha256(local.read_bytes()).hexdigest()
            if digest != snapshot.obo_sha256:
                raise ValueError(
                    f"the archived OBO of {snapshot.obo_version} hashes to {digest} and "
                    f"the snapshot records {snapshot.obo_sha256}; the alias map would "
                    "describe a different ontology from the one it is filed under"
                )
            emit(
                "load_ontology_aliases.read",
                f"sha256 {digest[:12]} verified",
                {"sha256": digest, "bytes": local.stat().st_size},
                "info",
            )
            return parse_aliases(local.read_text(errors="replace"))

    @staticmethod
    def _insert(
        session: Session, snapshot_id: uuid.UUID, rows: list[tuple[str, str, str]]
    ) -> int:
        """One INSERT, conflicts ignored: the same pair can be stated twice."""
        if not rows:
            return 0
        values = [
            {
                "ontology_snapshot_id": snapshot_id,
                "alias_go_id": alias,
                "primary_go_id": primary,
                "kind": kind,
            }
            for alias, primary, kind in rows
        ]
        stmt = (
            pg_insert(GOTermAlias.__table__)
            .values(values)
            .on_conflict_do_nothing(constraint="uq_go_term_alias_identity")
            .returning(GOTermAlias.__table__.c.id)
        )
        return len(session.execute(stmt).fetchall())
