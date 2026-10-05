"""Grow the protein universe from a GAF, so the loader stops dropping in silence.

THE DEFECT THIS CLOSES. ``load_goa_annotations`` stores an annotation only when
its accession is already in ``protein``, and silently skips the rest -- it has to,
because ``protein_go_annotation.protein_accession`` is a FOREIGN KEY. For the
whole clean campaign that filter meant "reviewed only", a scope nobody had
written down: it was a ``search_criteria`` in one ``insert_proteins`` payload from
2026-09-15. Measured 2026-10-05, UniProtKB with the thirteen lafa evidence codes:
**93.526 reviewed against 149.774 in total**, so roughly 56.000 proteins carrying
curated experimental labels never reached the corpus, and the count of how many
were dropped lived only in a job event.

WHY THE UNIVERSE COMES FROM THE GAF AND NOT FROM A UNIPROT QUERY. A query
describes today. The campaign spans 2016 to 2026, and a protein that held
experimental evidence in 2018 and lost it, or left UniProt entirely, does not
appear in today's answer -- yet it took part in the deltas the model learns from.
Deriving the universe from each GAF as it loads is the only definition that moves
with the series. Measured over three releases (160, 194, 235): the union is
**196.164** accessions, **46.390 more** than today's query returns, and the curve
was still climbing, so even that is a floor.

NOT-QUALIFIED ROWS COUNT. A NOT annotation is curated knowledge and a scarce kind
-- negative results are hard to publish -- and the evaluation already uses it:
``_reconcile_not_side`` propagates NOT to descendants and subtracts them. So a
protein whose only reliable annotation is a NOT belongs in the universe. The
donor policy still excludes NOT rows when picking neighbours, which is a
different question and stays as it is.

ALL 75 UNIVERSE PASSES BEFORE THE FIRST ANNOTATION LOAD. Interleaving them per
release -- universe 156, load 156, universe 157, load 157 -- would truncate the
history of every protein admitted late: a protein that first appears in release
200 would exist only from 200 onwards, and its annotations in 156 to 199 would be
dropped by the same foreign key this operation exists to satisfy. Running phase 1
over the whole series first makes the universe the union over all releases before
any annotation is stored, so each release loads against the final universe. It
also restores the parallelism: embeddings can start once phase 1 closes, instead
of waiting behind the loads.

WHAT CANNOT BE RESCUED, AND WHY IT IS NOW A NUMBER. An accession deleted from
UniProt has no sequence to fetch, so it can never be embedded. Those are reported
as ``not_retrievable`` instead of vanishing: the quantity the old filter threw
away becomes a measurement the run carries.
"""

from __future__ import annotations

import re
import time
from collections.abc import Callable, Iterator
from typing import Annotated, Any

from protea_contracts import GoaStreamPayload, UniProtProteinRecord
from pydantic import Field, field_validator
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, Operation, OperationResult, ProteaPayload
from protea.core.evidence_codes import EXPERIMENTAL
from protea.core.utils import chunks, contract_payload
from protea.infrastructure.orm.models.protein.protein import Protein

#: UniProtKB accession grammar, from UniProt's own documentation. Checked before
#: batching because the accessions endpoint answers 400 for the WHOLE request
#: when one member is malformed -- one stray identifier would cost a thousand
#: proteins, and GOA's object column is not guaranteed to hold only accessions.
_ACCESSION = re.compile(r"^(?:[OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9](?:[A-Z][A-Z0-9]{2}[0-9]){1,2})$")

#: Hard limit of ``/uniprotkb/accessions``: 1001 answers
#: "Only '1000' accessions are allowed in each request". Measured, not assumed.
_BATCH = 1000

#: 0-indexed GAF column holding the evidence code. We read the raw column
#: instead of the parsed record so the evidence test can run before the plugin
#: builds anything -- see ``_reliable_accessions``. The plugin keeps the same
#: index privately; ``test_the_evidence_column_is_where_we_think`` pins ours
#: against the plugin's own parse rather than against its private name.
_GAF_EVIDENCE = 6

_ACCESSIONS_URL = "https://rest.uniprot.org/uniprotkb/accessions"


class EnsureGoaUniversePayload(ProteaPayload, frozen=True):
    gaf_url: str
    timeout_seconds: Annotated[int, Field(gt=0)] = 120
    dry_run: bool = False

    @field_validator("gaf_url", mode="before")
    @classmethod
    def must_be_non_empty(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("gaf_url must be a non-empty string")
        return v.strip()


class EnsureGoaUniverseOperation(Operation):
    """Make every accession a GAF annotates reliably exist in ``protein``.

    Runs BEFORE ``load_goa_annotations`` for the same release, on the same cached
    file. Two passes over one GAF cost roughly fourteen minutes on top of the
    thirty-five a release already takes; the alternative -- asking UniProt from
    inside the annotation load -- puts network latency in the middle of a long
    paged transaction, which is the shape that OOM-killed the worker in
    September.

    Runs on ``protea.jobs``.
    """

    name = "ensure_goa_universe"
    payload_model = EnsureGoaUniversePayload
    description = (
        "Scan one GOA release and make every accession it annotates with a "
        "reliable evidence code exist in the protein table, fetching the absent "
        "ones from UniProt. Runs before load_goa_annotations for the same "
        "release, because protein_go_annotation.protein_accession is a foreign "
        "key and the loader can only skip what is not there. NOT-qualified rows "
        "count: a NOT is curated knowledge and the evaluation propagates it. "
        "Accessions UniProt no longer serves are reported as not_retrievable "
        "instead of vanishing."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        bits = [f"gaf={str(payload.get('gaf_url', ''))[-28:]}"]
        if payload.get("dry_run"):
            bits.append("dry-run")
        return " · ".join(bits)

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        t0 = time.perf_counter()
        p = EnsureGoaUniversePayload.model_validate(contract_payload(payload))
        emit("ensure_goa_universe.start", None, {"gaf_url": p.gaf_url}, "info")

        wanted, malformed, rows = self._reliable_accessions(p, emit)
        emit(
            "ensure_goa_universe.scanned",
            None,
            {"rows": rows, "reliable_accessions": len(wanted), "malformed": malformed},
            "info",
        )

        missing = self._missing(session, wanted)
        emit(
            "ensure_goa_universe.missing",
            None,
            {"already_present": len(wanted) - len(missing), "missing": len(missing)},
            "info",
        )

        fetched = inserted = sequences = 0
        if missing and not p.dry_run:
            fetched, inserted, sequences = self._fetch_and_store(session, missing, p, emit)

        result = {
            "rows_scanned": rows,
            "reliable_accessions": len(wanted),
            "malformed_skipped": malformed,
            "already_present": len(wanted) - len(missing),
            "missing": len(missing),
            "fetched": fetched,
            "not_retrievable": len(missing) - fetched if not p.dry_run else None,
            "proteins_inserted": inserted,
            "sequences_inserted": sequences,
            "dry_run": p.dry_run,
            "elapsed_seconds": round(time.perf_counter() - t0, 1),
        }
        emit("ensure_goa_universe.done", None, result, "info")
        return OperationResult(result=result)

    def _reliable_accessions(
        self, p: EnsureGoaUniversePayload, emit: EmitFn
    ) -> tuple[set[str], int, int]:
        """Every accession the GAF annotates with a reliable code, NOT included.

        Uses ``EXPERIMENTAL`` -- the eleven GO experimental codes -- plus ``IC``
        and ``TAS``, which is what LAFA's own ground truth filters on
        (``democafa/groundtruth/process_ground_truth.py``: ``selected=
        'Experimental,IC,TAS'``). Not the eight of classic CAFA, which omit the
        five high-throughput codes, and not the six a stats router still uses.
        """
        reliable = set(EXPERIMENTAL) | {"IC", "TAS"}
        wanted: set[str] = set()
        malformed = 0
        rows = 0

        def accept(cols: list[str]) -> bool:
            """Reject on the raw column, before a record exists.

            Of 280.922.738 lines in GOA 156, 671.138 carry a reliable code --
            0,24%. Testing ``rec.evidence_code`` instead would have the plugin
            validate a record for each of the other 99,76% and then drop it,
            which measured 11,5 minutes a release against 4,4.

            Counting here rather than in the loop keeps ``rows`` meaning exactly
            what it meant before the predicate existed: the plugin calls this for
            every line that is neither a comment nor short, which is precisely
            the set of lines that used to yield a record.
            """
            nonlocal rows
            rows += 1
            return cols[_GAF_EVIDENCE].strip() in reliable

        for rec in self._stream_gaf(p, emit, accept):
            accession = rec.accession.strip()
            if _ACCESSION.match(accession):
                wanted.add(accession)
            else:
                malformed += 1
        return wanted, malformed, rows

    def _stream_gaf(
        self,
        p: EnsureGoaUniversePayload,
        emit: EmitFn,
        accept: Callable[[list[str]], bool],
    ) -> Iterator[Any]:
        from protea_sources.goa import plugin as goa_plugin

        yield from goa_plugin.stream(
            GoaStreamPayload(gaf_url=p.gaf_url, timeout_seconds=p.timeout_seconds),
            emit=emit,
            accept=accept,
        )

    def _missing(self, session: Session, wanted: set[str]) -> list[str]:
        """Which of ``wanted`` are absent from ``protein``.

        Chunked because the Postgres wire protocol caps a statement at 65535 bind
        parameters, and a release brings more reliable accessions than that: 167573
        in GOA 235. An unchunked ``IN`` would be accepted by the payload validator
        and die here, hours into the run.
        """
        present: set[str] = set()
        for chunk in chunks(sorted(wanted), 20000):
            rows = session.query(Protein.accession).filter(Protein.accession.in_(chunk)).all()
            present.update(r[0] for r in rows)
        return sorted(wanted - present)

    def _fetch_and_store(
        self,
        session: Session,
        missing: list[str],
        p: EnsureGoaUniversePayload,
        emit: EmitFn,
    ) -> tuple[int, int, int]:
        """Fetch the absent accessions from UniProt and upsert them.

        An accession UniProt no longer serves is simply absent from the answer --
        measured: a well-formed but unknown identifier gives 200 and is omitted,
        while a MALFORMED one gives 400 for the whole batch, which is why the
        regex gate runs first. So ``fetched`` below is smaller than ``missing``
        by exactly the number of proteins that existed when the GAF was published
        and do not exist now. That difference is the quantity the old silent
        filter threw away.
        """
        from protea_sources.uniprot import parse_fasta_text

        from protea.core.operations.insert_proteins import InsertProteinsOperation

        # DELIBERATE COUPLING, PINNED BY A TEST. ``_store_records`` is private to
        # insert_proteins, and reaching into it is the lesser of two evils: the
        # alternative is a second copy of the MD5 dedup and the protein upsert,
        # and two copies of that logic would drift without anybody noticing,
        # which is worse than one import that can break loudly. It cannot break
        # loudly on its own -- the return is a plain 4-tuple, so a changed
        # signature would fail at runtime hours into a load -- so
        # ``tests/test_ensure_goa_universe.py`` asserts the method exists and
        # still returns four integers. Same device as the EXECUTED_SUBMODULE map
        # in count_backend_parameters, for the same reason.
        inserter = InsertProteinsOperation()
        fetched = proteins = sequences = 0
        for batch in chunks(missing, _BATCH):
            text = self._get_fasta(batch, p.timeout_seconds)
            records: list[UniProtProteinRecord] = list(parse_fasta_text(text))
            fetched += len(records)
            if records:
                ins_p, _upd, ins_s, _re = inserter._store_records(session, records, emit)
                proteins += ins_p
                sequences += ins_s
                session.commit()
            emit(
                "ensure_goa_universe.batch",
                None,
                {"requested": len(batch), "returned": len(records), "fetched_total": fetched},
                "info",
            )
        return fetched, proteins, sequences

    def _get_fasta(self, accessions: list[str], timeout: int) -> str:
        from urllib import error, request

        url = f"{_ACCESSIONS_URL}?accessions={','.join(accessions)}&format=fasta"
        req = request.Request(url, headers={"User-Agent": "PROTEA/ensure_goa_universe"})
        try:
            with request.urlopen(req, timeout=timeout) as resp:
                return resp.read().decode("utf-8")
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", "replace")[:300]
            raise RuntimeError(
                f"UniProt refused a batch of {len(accessions)} accessions "
                f"({exc.code}): {body}"
            ) from exc
