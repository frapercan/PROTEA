"""What a homology transfer reads from the store at a window's start.

The targets' sequences, the donors behind every hit sequence (admitted by the
donor policy), each donor's state at t0, the self scores of the target sequences
and, for global identity, the sequences' lengths. Split from
:mod:`protea.core.operations.run_homology_transfer` so that the operation is the
order of the steps and each read is one function its tests can replace.

THE DONOR POLICY, READ AS THE PREDICT PATH READS IT. ``donor_set_id`` admits only
the members listed in that declared set (an ``EXISTS`` on its entries);
``evidence_channels`` admits only the rows whose channel at the cut is listed: a
code that decides its channel alone is matched against the photo's channel table
by code, and an electronic row is classified by ``evidence_channel_of`` with the
member's review state at the cut (its ``superset`` flag); ``evidence_codes`` admits
only those codes; ``exclude_reference_prefixes`` drops a row whose reference starts
with any of them, and keeps a row with none. A member with no admitted row has no
state and does not vote.
"""

from __future__ import annotations

import io
import uuid
from collections import Counter
from collections.abc import Iterable
from typing import Any, NamedTuple

import pyarrow.parquet as pq
from protea_contracts.payloads import DonorPolicy
from sqlalchemy import String, any_, bindparam, select, text
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Session

from protea.core import lafa_frame
from protea.core._artifact_identity import fetch_and_verify
from protea.core._lafa_frame_data import load_end_ontology
from protea.core.cut_photo.formats import AUTOMATIC_CODE
from protea.core.operations.run_term_frequency import EXPERIMENTAL_CODES
from protea.core.source_guards import SourceGuardError
from protea.core.term_frequency import ROOTS
from protea.infrastructure.orm.models.annotation.cut_photo import CutPhoto, CutPhotoMember
from protea.infrastructure.orm.models.annotation.homology_self_scores import HomologySelfScores


class TransferInputsRefused(SourceGuardError):
    """An input of a homology transfer that is missing or does not hold together."""


def _any_of(name: str, values: Iterable[str]) -> Any:
    """One array parameter rather than one per value: a window has thousands of targets."""
    return any_(bindparam(name, value=sorted(set(values)), type_=ARRAY(String)))


def target_md5s(session: Session, photo: CutPhoto, targets: Iterable[str]) -> dict[str, str]:
    """Each target's sequence md5 in the photo; a target without one is left out (and counted)."""
    rows = session.execute(
        select(CutPhotoMember.accession, CutPhotoMember.sequence_md5).where(
            CutPhotoMember.photo_id == photo.id,
            CutPhotoMember.accession == _any_of("targets", targets),
            CutPhotoMember.sequence_md5.is_not(None),
        )
    ).all()
    return {str(accession): str(md5) for accession, md5 in rows}


_MEMBERS_BY_MD5 = """
    SELECT m.accession, m.sequence_md5 FROM cut_photo_member m
    WHERE m.photo_id = :photo AND m.sequence_md5 = ANY(:md5s)
"""
_IN_THE_DONOR_SET = """
      AND EXISTS (SELECT 1 FROM query_set_entry e
                  WHERE e.query_set_id = :donor_set AND e.accession = m.accession)
"""


def donors_by_md5(
    session: Session, photo: CutPhoto, md5s: Iterable[str], donor_set_id: str | None
) -> dict[str, list[str]]:
    """The photo's members of each sequence, in the declared donor set when one is named."""
    sql = _MEMBERS_BY_MD5 + (_IN_THE_DONOR_SET if donor_set_id is not None else "")
    params: dict[str, Any] = {"photo": photo.id, "md5s": sorted(set(md5s))}
    if donor_set_id is not None:
        params["donor_set"] = uuid.UUID(donor_set_id)
    found: dict[str, list[str]] = {}
    for accession, md5 in session.execute(text(sql), params):
        found.setdefault(str(md5), []).append(str(accession))
    return {md5: sorted(accessions) for md5, accessions in found.items()}


def admitted_rows_sql(policy: DonorPolicy) -> tuple[str, dict[str, Any]]:
    """The predicate and parameters that admit a donor's row under ``policy``."""
    clauses: list[str] = []
    params: dict[str, Any] = {}
    if policy.evidence_channels is not None:
        clauses.append(
            "(pga.evidence_code IN (SELECT c.evidence_code FROM evidence_channel c "
            "WHERE c.table_sha256 = :channel_table AND c.reference_class = '*' "
            "AND c.channel = ANY(:channels)) "
            "OR (pga.evidence_code = :electronic AND evidence_channel_of(:channel_table, "
            "pga.evidence_code, pga.db_reference, pga.with_from, m.superset) = ANY(:channels)))"
        )
        params |= {"channels": list(policy.evidence_channels), "electronic": AUTOMATIC_CODE}
    if policy.evidence_codes is not None:
        clauses.append("pga.evidence_code = ANY(:codes)")
        params["codes"] = list(policy.evidence_codes)
    for i, prefix in enumerate(policy.exclude_reference_prefixes or ()):
        clauses.append(
            f"(pga.db_reference IS NULL OR NOT starts_with(pga.db_reference, :prefix_{i}))"
        )
        params[f"prefix_{i}"] = prefix
    return "".join(f" AND {clause}" for clause in clauses), params


def _rows_by_accession(session: Session, sql: str, params: dict[str, Any]) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for accession, go_id in session.execute(text(sql), params):
        out.setdefault(str(accession), set()).add(str(go_id))
    return out


def donor_rows(
    session: Session,
    photo: CutPhoto,
    accessions: Iterable[str],
    policy: DonorPolicy,
    *,
    experimental_not_only: bool,
) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    """The donors' admitted rows without NOT, and their NOT rows by the window's ``not_codes``."""
    predicate, extra = admitted_rows_sql(policy)
    base = (
        "SELECT pga.protein_accession, gt.go_id FROM protein_go_annotation pga "
        "JOIN go_term gt ON gt.id = pga.go_term_id "
        "JOIN cut_photo_member m ON m.photo_id = :photo AND m.accession = pga.protein_accession "
        "WHERE pga.annotation_set_id = :annotations AND pga.protein_accession = ANY(:donors)"
    )
    params = {
        "photo": photo.id,
        "annotations": photo.annotation_set_id,
        "donors": sorted(set(accessions)),
        "channel_table": photo.channel_table_sha256,
        **extra,
    }
    positives = _rows_by_accession(
        session,
        base + " AND (pga.qualifier IS NULL OR pga.qualifier NOT LIKE '%NOT%')" + predicate,
        params,
    )
    negations = _rows_by_accession(
        session,
        base + " AND pga.qualifier LIKE '%NOT%' AND (NOT :only_experimental OR "
        "pga.evidence_code = ANY(:experimental))",
        {
            **params,
            "only_experimental": experimental_not_only,
            "experimental": list(EXPERIMENTAL_CODES),
        },
    )
    return positives, negations


class DonorRows(NamedTuple):
    """Each donor's state as term indices, roots out, with every index's GO id and aspect."""

    terms: dict[str, frozenset[int]]
    go_ids: list[str]
    aspects: list[str]


def indexed_states(truth: dict[str, dict[str, set[str]]], aspect_of: dict[str, str]) -> DonorRows:
    """The states as term indices over the sorted GO ids they hold, roots out, empty states out."""
    go_ids = sorted({t for by in truth.values() for terms in by.values() for t in terms} - ROOTS)
    index = {go_id: i for i, go_id in enumerate(go_ids)}
    terms = {
        accession: frozenset(index[t] for found in by.values() for t in found if t in index)
        for accession, by in truth.items()
    }
    return DonorRows(
        {a: found for a, found in terms.items() if found},
        go_ids,
        [aspect_of[go_id] for go_id in go_ids],
    )


def donor_states(
    session: Session, src: Any, accessions: Iterable[str], policy: DonorPolicy
) -> DonorRows:
    """Each donor's state at t0 from its admitted rows, as LAFA's frame states an end."""
    positives, negations = donor_rows(
        session,
        src.photo,
        accessions,
        policy,
        experimental_not_only=src.eset.not_codes == "experimental",
    )
    t0 = load_end_ontology(session, src.eset.old_native_snapshot_id)
    truth = lafa_frame.end_truth(t0, positives, negations, Counter(), "t0")
    return indexed_states(truth, dict(t0.live))


def self_bits(store: Any, scores: HomologySelfScores, md5s: Iterable[str]) -> dict[str, float]:
    """The self scores of ``md5s``, read from the store and verified against the row's sha256."""
    blob = fetch_and_verify(
        store, scores.scores_uri, scores.scores_sha256, f"the self scores {scores.short_name}"
    )
    table = pq.read_table(io.BytesIO(blob), columns=["query_md5", "bits"])
    wanted = set(md5s)
    return {
        md5: float(bits)
        for md5, bits in zip(table["query_md5"].to_pylist(), table["bits"].to_pylist(), strict=True)
        if md5 in wanted
    }


def sequence_lengths(session: Session, md5s: Iterable[str]) -> dict[str, int]:
    """Each sequence's length by its md5, refusing one the store does not hold."""
    wanted = set(md5s)
    rows = session.execute(
        text("SELECT sequence_hash, length(sequence) FROM sequence WHERE sequence_hash = ANY(:m)"),
        {"m": sorted(wanted)},
    )
    lengths = {str(md5): int(length) for md5, length in rows}
    missing = sorted(wanted - set(lengths))
    if missing:
        raise TransferInputsRefused(
            f"{len(missing)} sequences have no row in sequence (first {missing[:3]}), so their "
            "length, and a global identity, cannot be read"
        )
    return lengths


__all__ = [
    "DonorRows",
    "TransferInputsRefused",
    "admitted_rows_sql",
    "donor_rows",
    "donor_states",
    "donors_by_md5",
    "indexed_states",
    "self_bits",
    "sequence_lengths",
    "target_md5s",
]
