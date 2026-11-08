# protea/core/operations/declare_query_set_provenance.py
"""Recover a hand-registered query set's derivation by VERIFYING it, never inferring it.

WHAT IS BEING REPAIRED. Four query sets were registered by a person through the
FASTA upload endpoint, with the provenance typed into the free-text
description: the window as an id prefix, the job as a word, the sha as a string
nothing compares against. Migration ``c5e8d1a30b72`` gave the table the columns
to hold that properly and deliberately declared NOTHING, leaving every row
``legacy_undeclared``, because recovering a derivation is a judgement and a
judgement belongs in a job with its events rather than inside a schema change.

WHY IT IS EVIDENCE AND NOT TRANSCRIPTION. Reading the window out of the
description would be believing a sentence. Instead, for each candidate export
job this fetches the FASTA it published, through the SAME verified door, and
requires the accession set in those bytes to equal the query set's entries
EXACTLY. A count that matches is a coincidence waiting to happen; a set that
matches is the population.

Three independent things have to agree before a row is written: the published
target count equals the entry count, the bytes hash to the digest the job
recorded, and the accessions in those bytes are the accessions in the table.

WHAT IT REFUSES TO GUESS. A query set with no candidate stays
``legacy_undeclared`` and is reported, which is the honest outcome for the one
that came from the quick-annotation flow: nothing recorded where its two
proteins came from, and inventing a kind for it would be the same defect one
level down. Several jobs publishing the SAME (window, digest) are one export
run twice, so they collapse to the earliest; several publishing DIFFERENT ones
is an ambiguity this refuses rather than resolves.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any, NamedTuple

from pydantic import field_validator
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from protea.core._artifact_identity import ArtifactMismatch, fetch_and_verify
from protea.core.contracts.operation import (
    EmitFn,
    Operation,
    OperationResult,
    ProteaPayload,
)
from protea.core.operations.export_evaluation_targets import targets_key_for
from protea.core.utils import contract_payload, job_id_from_payload
from protea.infrastructure.orm.models.query.query_set import QuerySet, QuerySetEntry
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store

#: Every SUCCEEDED export, with the window its payload declared, the digest and
#: count its event recorded, and the revision its provenance event carried.
#: Read from the job's own rows and never from a description.
_EXPORTS = text("""
    SELECT j.id::text            AS job_id,
           j.payload->>'evaluation_set_id' AS window,
           COALESCE(j.payload->>'output_name', 'targets') AS output_name,
           e.fields->>'sha256'   AS digest,
           (e.fields->>'targets')::int AS targets,
           (SELECT p.fields->>'code_revision' FROM job_event p
             WHERE p.job_id = j.id AND p.fields ? 'code_revision' LIMIT 1) AS code_revision
      FROM job j JOIN job_event e ON e.job_id = j.id AND e.fields ? 'sha256'
     WHERE j.operation = 'export_evaluation_targets' AND j.status = 'SUCCEEDED'
     ORDER BY j.created_at
""")


class _Candidate(NamedTuple):
    """One export run that could have produced a query set's bytes."""

    job_id: str
    window: str
    output_name: str
    digest: str
    targets: int
    code_revision: str | None


class _Run(NamedTuple):
    """What every query set in one run is checked against.

    Bundled for the reason :class:`ReconciledWindow` is: the three travel
    together through every candidate, and a signature that took them loose is
    one somebody can call with two.
    """

    store: Any
    exports: list[_Candidate]
    dry: bool


class DeclareQuerySetProvenancePayload(ProteaPayload, frozen=True):
    """Which query sets to try to declare. Named one by one."""

    query_set_ids: list[str]
    dry_run: bool = False

    @field_validator("query_set_ids")
    @classmethod
    def _named_and_distinct(cls, value: list[str]) -> list[str]:
        """Refuse an empty list rather than treat it as "every undeclared one"."""
        if not value:
            raise ValueError(
                "query_set_ids cannot be empty and there is no 'all the undeclared "
                "ones' mode: this writes a derivation, and a derivation nobody named "
                "is one nobody checked"
            )
        repetidos = sorted({q for q in value if value.count(q) > 1})
        if repetidos:
            raise ValueError(f"query_set_ids repeats {repetidos}")
        return value


def _accessions_of(session: Session, qs_id: uuid.UUID) -> set[str]:
    """What the table says this population is."""
    return {
        a
        for (a,) in session.execute(
            select(QuerySetEntry.accession).where(QuerySetEntry.query_set_id == qs_id)
        ).all()
    }


def _accessions_in(raw: bytes) -> set[str]:
    """What the published bytes say it is.

    Headers are bare accessions by construction, which is what makes this
    comparable without agreeing a convention twice.
    """
    return {
        line[1:].split()[0]
        for line in raw.decode("utf-8").splitlines()
        if line.startswith(">") and len(line) > 1
    }


def _verified(store: Any, cand: _Candidate, esperadas: set[str]) -> bool:
    """Whether this export's bytes ARE this population.

    Through ``fetch_and_verify``, so the digest is checked before the content
    is believed, and the key is DERIVED from what the job declared instead of
    parsed out of a stored uri.
    """
    key = targets_key_for(cand.window, cand.output_name, cand.digest)
    try:
        raw = fetch_and_verify(store, key, cand.digest, f"targets of job {cand.job_id[:8]}")
    except (ArtifactMismatch, OSError, ValueError):
        return False
    return _accessions_in(raw) == esperadas


def _collapse(candidatos: list[_Candidate]) -> list[_Candidate]:
    """One entry per (window, digest): the same bytes exported twice are one export."""
    vistos: dict[tuple[str, str], _Candidate] = {}
    for c in candidatos:
        vistos.setdefault((c.window, c.digest), c)
    return list(vistos.values())


def _describe(qs: QuerySet, cand: _Candidate, entradas: int) -> str:
    """What the row will say about itself, stating what was VERIFIED.

    It names the comparison rather than the conclusion. "Declared from job X"
    invites a reader to trust it; "the accessions in those bytes equal these
    entries" tells them what was actually checked.
    """
    return (
        f"The {entradas} targets of evaluation set {cand.window}, exported by job "
        f"{cand.job_id} with sha256 {cand.digest}. Provenance recovered on "
        f"2026-11-08 by declare_query_set_provenance, not transcribed: the published "
        f"bytes were fetched, their digest verified, and the {entradas} accessions in "
        f"them matched these entries exactly. Registered originally by hand as "
        f"{qs.name!r}."
    )


class DeclareQuerySetProvenanceOperation(Operation):
    name = "declare_query_set_provenance"
    description = (
        "Recover a hand-registered QuerySet's derivation by verifying it: for each "
        "named set, find the SUCCEEDED export whose published target count matches, "
        "fetch its FASTA through the verified door, and write the window, sha256, job "
        "and revision only when the accessions in those bytes equal the set's entries "
        "exactly. A set with no candidate, or more than one, stays legacy_undeclared "
        "and is reported. dry_run writes nothing."
    )

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = DeclareQuerySetProvenancePayload.model_validate(contract_payload(payload))
        store = get_artifact_store(load_settings(Path(__file__).resolve().parents[3]))
        exports = [_Candidate(**dict(r._mapping)) for r in session.execute(_EXPORTS)]
        declarados, sin_declarar = [], []
        for raw_id in p.query_set_ids:
            run = _Run(store=store, exports=exports, dry=p.dry_run)
            veredicto = self._one(session, raw_id, run, emit)
            (declarados if veredicto["declared"] else sin_declarar).append(veredicto)
        result = {
            "declared": len(declarados),
            "left_undeclared": len(sin_declarar),
            "asked": len(p.query_set_ids),
            "dry_run": p.dry_run,
            "undeclared": sin_declarar,
            "job_id": str(job_id_from_payload(payload) or ""),
        }
        emit("declare_query_set_provenance.done", None, result, "info")
        return OperationResult(result=result)

    def _one(self, session: Session, raw_id: str, run: _Run, emit: EmitFn) -> dict[str, Any]:
        """Try one query set, and say in one dict what happened and why."""
        qs = session.get(QuerySet, uuid.UUID(raw_id))
        if qs is None:
            raise ValueError(f"QuerySet {raw_id} does not exist, and it is named in the payload")
        esperadas = _accessions_of(session, qs.id)
        por_cuenta = _collapse([c for c in run.exports if c.targets == len(esperadas)])
        verificados = [c for c in por_cuenta if _verified(run.store, c, esperadas)]
        veredicto: dict[str, Any] = {
            "query_set_id": raw_id,
            "name": qs.name,
            "entries": len(esperadas),
            "candidates_by_count": len(por_cuenta),
            "verified": len(verificados),
            "declared": False,
        }
        if len(verificados) != 1:
            veredicto["why_not"] = (
                "no export's bytes hold exactly these accessions"
                if not verificados
                else "more than one export's bytes hold exactly these accessions, which "
                "is an ambiguity to resolve by hand and not by picking one"
            )
            emit("declare_query_set_provenance.left_undeclared", None, veredicto, "warning")
            return veredicto

        cand = verificados[0]
        veredicto.update(
            {"declared": True, "evaluation_set_id": cand.window, "sha256": cand.digest}
        )
        if not run.dry:
            self._write(qs, cand, len(esperadas))
        emit(
            "declare_query_set_provenance.would_declare"
            if run.dry
            else "declare_query_set_provenance.declared",
            f"{qs.name} is the {len(esperadas)} targets of {cand.window[:8]}",
            veredicto,
            "info",
        )
        return veredicto

    def _write(self, qs: QuerySet, cand: _Candidate, entradas: int) -> None:
        """Fill the columns. The description is replaced only when it is empty.

        A description somebody wrote is kept: it may say something the
        verification cannot, and overwriting a human sentence with a generated
        one loses information to gain tidiness.
        """
        qs.source_kind = "evaluation_targets"
        qs.evaluation_set_id = uuid.UUID(cand.window)
        qs.artifact_uri = f"s3://{targets_key_for(cand.window, cand.output_name, cand.digest)}"
        qs.content_sha256 = cand.digest
        qs.job_id = uuid.UUID(cand.job_id)
        qs.code_revision = cand.code_revision
        if not qs.description:
            qs.description = _describe(qs, cand, entradas)

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        n = len(payload.get("query_set_ids") or [])
        seco = " (dry run)" if payload.get("dry_run") else ""
        return f"verify and declare the provenance of {n} query sets{seco}"
