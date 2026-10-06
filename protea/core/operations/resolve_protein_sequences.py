"""Give the universe its sequences, its audit dates and its merged accessions.

WHY THIS IS ITS OWN OPERATION, AND WHY IT RUNS ONCE. ``extract_goa_universe``
walks a GAF and writes accessions; nothing it writes needs UniProt. The sequences
do, and they are needed ONCE, over the union of all 75 releases, after the series
is finished. Doing it per release instead -- which is what ``ensure_goa_universe``
did -- asked UniProt again at every release that annotates the same protein, and
asked again for accessions UniProt does not serve at all: measured at 34% of the
requests across the ten passes that ran that way.

So the shape here is the one 8 of the platform's 9 networked operations already
have: one declared source, one pass over the population, transport in the
``protea_sources`` plugin. ``protea.core.operations._universe_http`` -- a private
115-line copy of the plugin's retry logic, down to the same ``{429, 500, 502, 503,
504}`` set and the same ``Retry-After`` handling -- is gone with it.

WHAT "THE POPULATION" IS, and why it is a query rather than a payload. Every
``protein`` row with no ``sequence_id``. That is not a scope somebody chose, it is
the definition of what is missing, read from the table at the moment the job runs.
A list of accessions in a payload would be a hypothesis about the state of the
database frozen at submission time, and the campaign has already been bitten once
by a scope that lived in a payload (``search_criteria: reviewed:true``, 2026-09-15).

THREE PASSES, AND THEY ARE NOT THE SAME QUESTION.

*Sequences.* ``/uniprotkb/accessions`` in batches of a thousand, TSV, asking for
the sequence and the three audit dates in one request. An accession UniProt no
longer serves is simply absent from the answer -- a well-formed but unknown
identifier gives 200 and is omitted, while a MALFORMED one gives 400 for the whole
batch, which is why the grammar gate runs first. So ``fetched`` is smaller than
``candidates`` by exactly the number of proteins that existed when their GAF was
published and do not exist now. That difference is the quantity the old silent
filter threw away.

*Merges.* See SECONDARY ACCESSIONS below. Asked of whatever still has no sequence
after the first pass, and asked of the DATABASE rather than of arithmetic:
``candidates`` minus ``fetched`` counts correctly but does not say WHO is missing,
and who is missing is what has to be resolved and what has to be recorded if it
is not.

*Audit dates.* A separate pass with a separate predicate, ``date_created IS
NULL``, over the whole table and not just over what this run fetched. Every
protein ``insert_proteins`` loaded carries NULL in all three columns -- roughly
575.000 of some 680.000 rows at the time this was written -- so a date filter
would have silently excluded 85% of the corpus while looking like it worked. Four
independent reviewers flagged that on 2026-10-05 before it shipped. The request is
``fields``-only, no sequence, so the responses are small, and the pass is a no-op
once it has run.

SECONDARY ACCESSIONS, THE ROUTE THAT LOOKS LIKE A DELETION.
``/uniprotkb/accessions`` matches PRIMARY accessions only. An accession merged into
another entry -- a *secondary* -- is not returned, and UniProt counts it in
``X-Total-Results`` regardless, so the response reports "7 results" with an empty
body and a 200. Measured 2026-10-05 over seven accessions, in FASTA (0 bytes) and
in JSON (``{"results":[]}``). The single-entry route DOES follow the merge and
redirects, so the same accession looks alive one way and deleted the other:

    P30456  ->  secondary of P04439 (HLA-A, which carries 135 secondaries)
    Q9NPA5  ->  secondary of Q9NTW7
    E1BZ05  ->  secondary of P02542

HOW MUCH. On GOA 156, of 10,791 reliable accessions the batch endpoint did not
return, a systematic sample of 600 resolved **18.2%** as merges; the rest are
DELETED with no successor. Of the recoverable ones, 72% already had their primary
in ``protein`` and 28% did not. Extrapolated: ~1,960 recoverable, ~558 proteins
genuinely absent and ~1,403 annotations the foreign key would have dropped even
though the protein was present under its primary accession.

WHAT THIS FIXES BEYOND COVERAGE. If GOA used P30456 in 2016 and P04439 today, a
series over accessions sees one disappearance and one appearance where there is a
single protein. That contaminates the very delta the campaign measures, and no
amount of counting fixes it -- only recording the link does.

HOW IT IS STORED, AND WHY IT DOES NOT DUPLICATE EMBEDDINGS. Two rows exist: the
primary as usual, and the secondary with ``canonical_accession`` pointing at it,
``is_canonical=False`` and ``isoform_index=None``. Both share a ``sequence_id``,
because :func:`protea.core.operations._protein_store.store_records` deduplicates
sequences by hash -- and embeddings are keyed on ``Sequence`` rather than
``Protein`` (``compute_embeddings.py``), so two accessions over one sequence yield
ONE embedding and ONE neighbour in the KNN bank.

``is_canonical`` is the population filter in every code path that counts proteins
(``proteins_stats``, ``proteins``, ``showcase``), and a merged secondary is not a
distinct protein to count, so the value is the correct one. What distinguishes it
from an isoform is ``isoform_index``: an integer for an isoform, ``None`` for a
merge alias.

WHAT CANNOT BE RESCUED, AND WHY IT IS A LIST AND NOT A NUMBER. An accession
deleted from UniProt has no sequence to fetch, so it can never be embedded. Those
are written out as ``sin_resolver.txt`` in the artifact store, not just counted:
a number cannot be audited, a list can. Their annotations stay in the corpus --
the protein took part in the deltas -- and every query that needs a chain filters
on ``sequence_id IS NOT NULL``.
"""

from __future__ import annotations

import time
from typing import Annotated, Any

from protea_contracts import UniProtProteinRecord
from pydantic import Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, Operation, OperationResult, ProteaPayload
from protea.core.operations._protein_store import StoreCounts, store_records
from protea.core.operations._universe_sources import (
    _DATE_FIELDS,
    _TSV_FIELDS,
    ACCESSION_GRAMMAR,
    _audit_dates_of,
    _parse_dates_tsv,
    _parse_tsv,
    _Salida,
    _store_dates,
    candidates_from,
    classify,
    informe_de_resolucion,
)
from protea.core.utils import chunks, contract_payload
from protea.infrastructure.orm.models.protein.protein import Protein

#: Chunk size for the ``IN`` lookups. Well under the 65535 bind-parameter ceiling
#: of the Postgres wire protocol, which the candidate set reaches many times over.
_DB_CHUNK = 20000


def resolution_key_for(job_id: Any, nombre: str) -> str:
    """Clave de almacenamiento de un artefacto de esta operacion."""
    return f"protein_resolution/{job_id}/{nombre}"


class ResolveProteinSequencesPayload(ProteaPayload, frozen=True):
    timeout_seconds: Annotated[int, Field(gt=0)] = 120
    dry_run: bool = False
    #: Stop after this many candidates, for a smoke run before the real one. The
    #: value is REPORTED in the result as ``limit``, so a capped run can never be
    #: read as a complete one -- which is the whole hazard of a bound like this.
    max_accessions: Annotated[int, Field(gt=0)] | None = None


class ResolveProteinSequencesOperation(Operation):
    """Fetch from UniProt everything the GAF could not say.

    Runs ONCE, after the last ``extract_goa_universe`` pass of the series and
    before the embeddings. Safe to re-run: its population is "whatever still has
    no sequence", so a second run asks only about what a first run could not
    resolve -- which is bounded by the number of deleted accessions, some tens of
    thousands, a few seconds of requests.

    Runs on ``protea.jobs``.
    """

    name = "resolve_protein_sequences"
    payload_model = ResolveProteinSequencesPayload
    description = (
        "Fetch sequences, audit dates and merged-accession links from UniProt for "
        "every protein row that lacks them. Runs once, after the GAF series has "
        "been extracted, because the sequences are needed over the union of the "
        "releases and not per release. Accessions UniProt no longer serves are "
        "written out as a list of names rather than reported as a count."
    )

    def __init__(self) -> None:
        from protea_sources.uniprot import plugin as _uniprot_plugin

        self._uniprot = _uniprot_plugin

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        bits = []
        if payload.get("max_accessions"):
            bits.append(f"limit={payload['max_accessions']}")
        if payload.get("dry_run"):
            bits.append("dry-run")
        return " · ".join(bits) or "whole universe"

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        t0 = time.perf_counter()
        p = ResolveProteinSequencesPayload.model_validate(contract_payload(payload))
        salida = _Salida()

        candidatos = self._without_sequence(session, p.max_accessions)
        salida.candidatos = len(candidatos)
        emit(
            "resolve_protein_sequences.start",
            None,
            {"candidates": len(candidatos), "limit": p.max_accessions},
            "info",
        )

        if not p.dry_run and candidatos:
            self._fetch_sequences(session, candidatos, p, emit, salida)
            self._resolve_merges(session, candidatos, p, emit, salida)
        if not p.dry_run:
            self._backfill_dates(session, p, emit, salida)
            salida.artefactos = self._store_artifacts(payload.get("_job_id"), salida)

        result = informe_de_resolucion(
            dry_run=p.dry_run,
            salida=salida,
            elapsed=round(time.perf_counter() - t0, 1),
        )
        result["limit"] = p.max_accessions
        emit("resolve_protein_sequences.done", None, result, "info")
        return OperationResult(result=result)

    # ---- the population ----

    def _without_sequence(self, session: Session, limit: int | None) -> list[str]:
        """Accessions of every ``protein`` row with no sequence, in order.

        Ordered so a capped run is reproducible and a second capped run continues
        where the first stopped rather than re-asking the same thousand.

        Filtered through the accession grammar: a single malformed member answers
        400 for the whole batch of a thousand, and this table can be written by
        more than one operation.
        """
        stmt = (
            select(Protein.accession)
            .where(Protein.sequence_id.is_(None))
            .order_by(Protein.accession)
        )
        if limit is not None:
            stmt = stmt.limit(limit)
        return [a for a in session.scalars(stmt).all() if ACCESSION_GRAMMAR.match(a)]

    # ---- pass one: sequences and the dates that ride with them ----

    def _fetch_sequences(
        self,
        session: Session,
        candidatos: list[str],
        p: ResolveProteinSequencesPayload,
        emit: EmitFn,
        salida: _Salida,
    ) -> None:
        """Ask for the sequence and the three audit dates in one request per batch.

        One request, not two: the TSV route carries both, where the FASTA route the
        first version used carried no dates and would have doubled the traffic.
        """
        from protea_sources.uniprot import MAX_ACCESSIONS_PER_REQUEST

        contado = StoreCounts()
        for batch in chunks(candidatos, MAX_ACCESSIONS_PER_REQUEST):
            parsed = _parse_tsv(
                self._uniprot.fetch_accessions_tsv(
                    batch, fields=_TSV_FIELDS, emit=emit, knobs=self._knobs(p)
                )
            )
            records: list[UniProtProteinRecord] = [r for r, _d in parsed]
            salida.fetched += len(records)
            if records:
                contado.add(store_records(session, records, emit))
                _store_dates(session, [(r.accession, d) for r, d in parsed])
                session.commit()
            emit(
                "resolve_protein_sequences.batch",
                None,
                {
                    "requested": len(batch),
                    "returned": len(records),
                    "fetched_total": salida.fetched,
                    "_progress_current": salida.fetched,
                    "_progress_total": len(candidatos),
                },
                "info",
            )
        salida.updated += contado.proteins_updated
        salida.inserted += contado.proteins_inserted
        salida.sequences += contado.sequences_inserted

    # ---- pass two: what the batch endpoint does not return ----

    def _resolve_merges(
        self,
        session: Session,
        candidatos: list[str],
        p: ResolveProteinSequencesPayload,
        emit: EmitFn,
        salida: _Salida,
    ) -> None:
        """Rescue the FUSED accessions, and name the ones that are simply gone.

        The question is put to the database and not to the arithmetic: which of
        the candidates STILL has no sequence. ``candidatos`` minus ``fetched``
        counts right but does not say who.
        """
        from protea_sources.uniprot import MAX_OR_CONDITIONS

        pendientes = self._still_without_sequence(session, candidatos)
        if not pendientes:
            return
        contado = StoreCounts()
        for batch in chunks(pendientes, MAX_OR_CONDITIONS):
            contado.add(self._merge_batch(session, batch, p, emit, salida))
        salida.sin_resolver = sorted(set(pendientes) - set(salida.alias))
        salida.updated += contado.proteins_updated
        salida.inserted += contado.proteins_inserted
        salida.sequences += contado.sequences_inserted
        emit(
            "resolve_protein_sequences.secondary",
            None,
            {
                "pending": len(pendientes),
                "resolved_as_merge": len(salida.alias),
                "unresolved": len(salida.sin_resolver),
            },
            "info",
        )

    def _merge_batch(
        self,
        session: Session,
        batch: list[str],
        p: ResolveProteinSequencesPayload,
        emit: EmitFn,
        salida: _Salida,
    ) -> StoreCounts:
        """One ``sec_acc:`` query and what it resolves.

        Its own method because the batch is the unit UniProt's cap defines, and
        because the decision inside it is per accession rather than per batch: an
        accession that appears in SEVERAL entries is a demerge, not a merge, and
        that cannot be decided until the whole response has been read.
        """
        cuerpo = self._uniprot.search_secondary_accessions(
            batch, emit=emit, knobs=self._knobs(p)
        )
        candidates, entry_by_acc = candidates_from(cuerpo, set(batch))
        records = classify(candidates, salida.alias, salida.demerges)
        contado = StoreCounts()
        if records:
            contado.add(store_records(session, records, emit))
            _store_dates(
                session,
                [
                    (r.accession, _audit_dates_of(entry_by_acc[r.accession]))
                    for r in records
                    if r.accession in entry_by_acc
                ],
            )
            session.commit()
        emit(
            "resolve_protein_sequences.secondary_batch",
            None,
            {
                "requested": len(batch),
                "resolved": len(salida.alias),
                "demerged": len(salida.demerges),
                "rows": len(records),
            },
            "info",
        )
        return contado

    def _still_without_sequence(self, session: Session, candidatos: list[str]) -> list[str]:
        pendientes: list[str] = []
        for chunk in chunks(candidatos, _DB_CHUNK):
            pendientes.extend(
                session.scalars(
                    select(Protein.accession).where(
                        Protein.accession.in_(chunk),
                        Protein.sequence_id.is_(None),
                    )
                ).all()
            )
        return sorted(pendientes)

    # ---- pass three: the dates of everything that already had a sequence ----

    def _backfill_dates(
        self,
        session: Session,
        p: ResolveProteinSequencesPayload,
        emit: EmitFn,
        salida: _Salida,
    ) -> None:
        """Fill the audit dates of every row that still lacks them.

        A different predicate from the first pass and therefore a different
        population: ``date_created IS NULL`` includes every protein
        ``insert_proteins`` loaded, which has a sequence and no dates.
        """
        pendientes = session.scalars(
            select(Protein.accession)
            .where(Protein.date_created.is_(None))
            .order_by(Protein.accession)
        ).all()
        salida.fechas_pendientes = len(pendientes)
        if not pendientes:
            return
        from protea_sources.uniprot import MAX_ACCESSIONS_PER_REQUEST

        for batch in chunks(list(pendientes), MAX_ACCESSIONS_PER_REQUEST):
            filas = _parse_dates_tsv(
                self._uniprot.fetch_accessions_tsv(
                    batch, fields=_DATE_FIELDS, emit=emit, knobs=self._knobs(p)
                )
            )
            if filas:
                _store_dates(session, filas)
                session.commit()
                salida.fechas_escritas += len(filas)
        emit(
            "resolve_protein_sequences.dates_backfilled",
            None,
            {"pending": salida.fechas_pendientes, "written": salida.fechas_escritas},
            "info",
        )

    # ---- what the run leaves behind ----

    def _store_artifacts(self, job_id: Any, salida: _Salida) -> dict[str, Any]:
        """Persist the merge map and the accessions nothing could resolve.

        ``not_retrievable`` used to be a number with no names: proteins carrying
        curated experimental evidence that do not enter the corpus and could not
        be cited. A number cannot be audited; a list can.
        """
        if job_id is None:
            return {}
        import csv
        import tempfile
        from pathlib import Path

        from protea.infrastructure.settings import load_settings
        from protea.infrastructure.storage import get_artifact_store

        store = get_artifact_store(load_settings(Path(__file__).resolve().parents[3]))
        out: dict[str, Any] = {}
        with tempfile.TemporaryDirectory() as tmp:
            if salida.alias:
                ruta = Path(tmp) / "fusiones.tsv"
                with ruta.open("w", encoding="utf-8", newline="") as fh:
                    w = csv.writer(fh, delimiter="\t")
                    w.writerow(["accesion_gaf", "accesion_primaria"])
                    w.writerows(sorted(salida.alias.items()))
                out["fusiones"] = {
                    "uri": store.put(resolution_key_for(job_id, "fusiones.tsv"), str(ruta)),
                    "filas": len(salida.alias),
                }
            if salida.sin_resolver:
                ruta = Path(tmp) / "sin_resolver.txt"
                ruta.write_text("\n".join(salida.sin_resolver) + "\n", encoding="utf-8")
                out["sin_resolver"] = {
                    "uri": store.put(resolution_key_for(job_id, "sin_resolver.txt"), str(ruta)),
                    "filas": len(salida.sin_resolver),
                }
            if salida.demerges:
                # Aparte de las borradas, y con sus destinos: una borrada no tiene
                # a donde ir, un demerge tiene varios y elegir es una decision
                # curatorial que esta operacion no puede tomar.
                ruta = Path(tmp) / "demerges.tsv"
                with ruta.open("w", encoding="utf-8", newline="") as fh:
                    w = csv.writer(fh, delimiter="\t")
                    w.writerow(["accesion_gaf", "destinos"])
                    w.writerows((k, ",".join(v)) for k, v in sorted(salida.demerges.items()))
                out["demerges"] = {
                    "uri": store.put(resolution_key_for(job_id, "demerges.tsv"), str(ruta)),
                    "filas": len(salida.demerges),
                }
        return out

    @staticmethod
    def _knobs(p: ResolveProteinSequencesPayload) -> Any:
        """The plugin's transport knobs, with this payload's timeout.

        Everything else keeps the plugin's measured defaults: six retries,
        exponential backoff from 2s capped at 60s, half a second of jitter. Those
        are facts about UniProt's rate limiting, not choices a caller makes, which
        is why they are not payload fields.
        """
        from protea_sources.uniprot import RetryKnobs

        return RetryKnobs(timeout_seconds=p.timeout_seconds)
