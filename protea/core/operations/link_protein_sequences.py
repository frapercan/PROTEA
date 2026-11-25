"""Link each protein to its sequence from the cut photos, with no request to UniProt.

WHY THIS EXISTS. ``protein.sequence_id`` was empty for every protein in the clean
store and in dev (2026-11-24: 0 of 2,758,480), so the instrument counted no
embeddings while the banks held 20,191,896 vectors, and a protein page showed no
sequence. The operation that fills it, ``resolve_protein_sequences``, asks today's
UniProt and had never run. The author chose the photos instead (2026-11-24): every
photo records each member's sequence at its cut, by md5
(``cut_photo_member.sequence_md5``), and the embedding banks were built from exactly
those sequences, so a protein linked from them is linked to the vectors the banks hold.

WHICH SEQUENCE. A protein whose sequence changed between cuts has several; it takes
the one recorded by the LATEST cut that records one for it. The photos are the source
and the cut order is the rule, so two runs over the same photos link the same sequence.

THE POPULATION is every protein row with no ``sequence_id``, read when the job runs,
as in ``resolve_protein_sequences``: a protein already linked, by either operation, is
left alone. A protein that no photo gives a sequence for stays unlinked, and the
result counts it.
"""

from __future__ import annotations

import time
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, Operation, OperationResult, ProteaPayload
from protea.core.utils import contract_payload

#: Each protein's sequence at the latest cut that records one for it.
_LATEST = (
    "SELECT DISTINCT ON (m.accession) m.accession, s.id AS sequence_id "
    "FROM cut_photo_member m "
    "JOIN cut_photo p ON p.id = m.photo_id "
    "JOIN sequence s ON s.sequence_hash = m.sequence_md5 "
    "WHERE m.sequence_md5 IS NOT NULL "
    "ORDER BY m.accession, p.cut_release DESC"
)
LINK_SQL = text(
    f"WITH latest AS ({_LATEST}) "
    "UPDATE protein SET sequence_id = latest.sequence_id, updated_at = now() "
    "FROM latest WHERE protein.accession = latest.accession AND protein.sequence_id IS NULL"
)
COUNT_SQL = text(
    f"WITH latest AS ({_LATEST}) "
    "SELECT count(*) FROM protein JOIN latest ON protein.accession = latest.accession "
    "WHERE protein.sequence_id IS NULL"
)
UNLINKED_SQL = text("SELECT count(*) FROM protein WHERE sequence_id IS NULL")


class LinkProteinSequencesPayload(ProteaPayload, frozen=True):
    #: Count what would be linked and write nothing.
    dry_run: bool = False


class LinkProteinSequencesOperation(Operation):
    """Link every protein with no sequence to the one its latest cut photo records.

    Runs on ``protea.jobs``.
    """

    name = "link_protein_sequences"
    payload_model = LinkProteinSequencesPayload
    description = (
        "Link every protein with no sequence to the sequence its latest cut photo "
        "records, by md5, with no request to UniProt. Writes protein.sequence_id only; "
        "the sequences are the ones the embedding banks were built from."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        return "dry-run" if payload.get("dry_run") else "every protein without a sequence"

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        t0 = time.perf_counter()
        p = LinkProteinSequencesPayload.model_validate(contract_payload(payload))
        before = session.execute(UNLINKED_SQL).scalar_one()
        emit(
            "link_protein_sequences.start", None, {"unlinked": before, "dry_run": p.dry_run}, "info"
        )
        if p.dry_run:
            linked = session.execute(COUNT_SQL).scalar_one()
            after = before - linked
        else:
            linked = session.execute(LINK_SQL).rowcount
            session.commit()
            after = session.execute(UNLINKED_SQL).scalar_one()
        result = {
            "unlinked_before": before,
            "linked": linked,
            "unlinked_after": after,
            "dry_run": p.dry_run,
            "elapsed_seconds": round(time.perf_counter() - t0, 1),
        }
        emit("link_protein_sequences.done", None, result, "info")
        return OperationResult(result=result)
