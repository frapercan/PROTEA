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

LAS ACCESIONES SECUNDARIAS, Y POR QUE NO BASTA PEDIRLAS. ``GET
/uniprotkb/accessions`` casa SOLO accesiones primarias. Una accesion fusionada en
otra entrada --una *secundaria*-- no se devuelve, y UniProt la cuenta de todas
formas en ``X-Total-Results``, asi que la respuesta dice "7 resultados" con el
cuerpo vacio y un 200. Medido el 2026-10-05 con siete accesiones, en fasta (0
bytes) y en JSON (``{"results":[]}``). La consulta por entrada SI sigue la fusion
y redirige, de modo que la misma accesion parece viva por un camino y borrada por
el otro:

    P30456  ->  secundaria de P04439 (HLA-A, que tiene 135 secundarias)
    Q9NPA5  ->  secundaria de Q9NTW7
    E1BZ05  ->  secundaria de P02542

CUANTO. Sobre GOA 156, de 10.791 accesiones fiables que el endpoint de lote no
devolvio, una muestra sistematica de 600 resolvio el **18,2%** como fusiones; el
resto esta DELETED sin sucesor. De las recuperables, el 72% tenia su primaria ya
en ``protein`` y el 28% no. Extrapolado: ~1.960 recuperables, ~558 proteinas que
faltaban de verdad y ~1.403 anotaciones que la clave ajena habria tirado aunque
la proteina si estuviera bajo su primaria.

LO QUE ARREGLA ADEMAS DE LA COBERTURA. Si GOA usaba P30456 en 2016 y P04439 hoy,
una serie sobre accesiones ve una desaparicion y una aparicion donde hay una sola
proteina. Eso contamina el delta que mide la campana, y no lo arregla ningun
recuento: lo arregla registrar el enlace.

COMO SE GUARDA, Y POR QUE NO DUPLICA EMBEDDINGS. Se insertan DOS filas: la
primaria normal, y la secundaria con ``canonical_accession`` apuntando a la
primaria, ``is_canonical=False`` e ``isoform_index=None``. Las dos comparten
``sequence_id``, porque ``_store_records`` deduplica secuencias por hash -- y los
embeddings se indexan por ``Sequence``, no por ``Protein``
(``compute_embeddings.py``), asi que dos accesiones sobre una secuencia dan UN
embedding y UN vecino en el banco KNN.

``is_canonical`` es el filtro de poblacion en todo el codigo que cuenta proteinas
(``proteins_stats``, ``proteins``, ``showcase``), y una secundaria fusionada no es
una proteina distinta que contar, asi que el valor es el correcto. Lo que la
distingue de una isoforma es ``isoform_index``: entero para una isoforma, ``None``
para un alias de fusion.

WHAT CANNOT BE RESCUED, AND WHY IT IS NOW A NUMBER. An accession deleted from
UniProt has no sequence to fetch, so it can never be embedded. Those are reported
as ``not_retrievable`` instead of vanishing: the quantity the old filter threw
away becomes a measurement the run carries.
"""

from __future__ import annotations

import re
import time
from collections import Counter
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

#: Endpoint de busqueda, para resolver accesiones secundarias. Ver
#: ``_resolve_secondary``: el endpoint de lote casa SOLO primarias.
_SEARCH_URL = "https://rest.uniprot.org/uniprotkb/search"

#: Tope de condiciones OR por consulta, dicho por UniProt en el cuerpo del 400:
#: "Too many OR conditions in query. Maximum allowed is 100." Medido, no supuesto.
_SEC_BATCH = 100

#: 0-indexed GAF column holding the DB Object Type: ``protein``, ``complex``,
#: ``rna``. Es la forma que el fichero tiene de decir lo que es cada fila, y
#: sustituye al uso de la regex de accesion como filtro de tipo.
_GAF_TYPE = 11

#: Tipos que NO son una proteina con una cadena que embeder. Se RECHAZAN por
#: nombre en vez de aceptar solo ``protein``, y la diferencia importa: un tipo
#: nuevo que GOA empiece a publicar entra al corpus y aparece en el histograma
#: del resultado, en vez de desaparecer en silencio. Dejar fuera a una proteina
#: de verdad es peor que dejar entrar a un complejo, porque al complejo lo frena
#: ademas la regex de accesion -- medido en GOA 156: 0 de 1.032 identificadores
#: de IntAct y RNAcentral pasan la regex.
_NOT_A_PROTEIN = frozenset({"complex", "protein_complex", "rna", "ncrna", "mrna",
                            "trna", "rrna", "snrna", "snorna", "lncrna",
                            "transcript", "gene", "small molecule"})

#: 0-indexed GAF column holding the evidence code. We read the raw column
#: instead of the parsed record so the evidence test can run before the plugin
#: builds anything -- see ``_reliable_accessions``. The plugin keeps the same
#: index privately; ``test_the_evidence_column_is_where_we_think`` pins ours
#: against the plugin's own parse rather than against its private name.
_GAF_EVIDENCE = 6

_ACCESSIONS_URL = "https://rest.uniprot.org/uniprotkb/accessions"


def _organism_of(entry: dict[str, Any]) -> str | None:
    return ((entry.get("organism") or {}).get("scientificName")) or None


def _taxon_of(entry: dict[str, Any]) -> str | None:
    t = (entry.get("organism") or {}).get("taxonId")
    return str(t) if t is not None else None


def _gene_of(entry: dict[str, Any]) -> str | None:
    genes = entry.get("genes") or []
    if not genes:
        return None
    return ((genes[0].get("geneName") or {}).get("value")) or None


def _records_for_merge(
    entry: dict[str, Any], pedidas: set[str]
) -> tuple[str, list[UniProtProteinRecord], list[str]] | None:
    """Las filas que una entrada de UniProt aporta: la primaria y sus alias.

    Solo genera alias para las secundarias que ALGUIEN PIDIO. Una entrada puede
    traer decenas -- P04439 tiene 135 -- y crear las demas inventaria proteinas
    que ningun GAF anoto.
    """
    from protea_contracts import compute_sequence_hash

    primary = entry.get("primaryAccession")
    seq = ((entry.get("sequence") or {}).get("value") or "").strip()
    if not primary or not seq:
        return None
    encontradas = [sec for sec in (entry.get("secondaryAccessions") or []) if sec in pedidas]
    if not encontradas:
        return None
    comun: dict[str, Any] = {
        "organism": _organism_of(entry),
        "taxonomy_id": _taxon_of(entry),
        "gene_name": _gene_of(entry),
        "reviewed": "reviewed" in (entry.get("entryType") or "").lower(),
        "sequence": seq,
        "length": len(seq),
        "sequence_hash": compute_sequence_hash(seq),
    }
    filas = [
        UniProtProteinRecord(
            accession=primary,
            canonical_accession=primary,
            is_canonical=True,
            isoform_index=None,
            **comun,
        )
    ]
    filas += [
        UniProtProteinRecord(
            accession=sec,
            canonical_accession=primary,
            is_canonical=False,
            isoform_index=None,
            **comun,
        )
        for sec in encontradas
    ]
    return primary, filas, encontradas


def _decidir(
    candidatos: dict[str, list[tuple[str, list[UniProtProteinRecord]]]],
    alias: dict[str, str],
    demerges: dict[str, list[str]],
) -> list[UniProtProteinRecord]:
    """Separa fusiones de demerges y devuelve las filas a guardar.

    UN DEMERGE NO TIENE UNA IDENTIDAD. Si la accesion sale en varias entradas es
    que se partio, asi que no hay UNA proteina a la que apunte: aliasarla a
    cualquiera de ellas elige arbitrariamente, y la secuencia que heredaria seria
    la de una de dos proteinas distintas. Se registra con sus destinos y se deja
    sin resolver, porque elegir es una decision curatorial.

    Y SE DEDUPLICA POR ACCESION. Una primaria puede venir en varios candidatos del
    mismo lote. ``_store_records`` separa inserts de updates mirando lo que ya hay
    en la base y NO deduplica su propia entrada, asi que una accesion repetida le
    llega como dos INSERT y revienta con ``duplicate key``. Paso con C8VQ65 el
    2026-10-05, tras 317 fusiones correctas.
    """
    records: list[UniProtProteinRecord] = []
    vistas: set[str] = set()
    for sec, opciones in candidatos.items():
        destinos = {prim for prim, _ in opciones}
        if len(destinos) > 1:
            demerges[sec] = sorted(destinos)
            continue
        for fila in opciones[0][1]:
            if fila.accession in vistas:
                continue
            vistas.add(fila.accession)
            records.append(fila)
        alias[sec] = opciones[0][0]
    return records


def universe_key_for(job_id: Any, nombre: str) -> str:
    """Clave de almacenamiento de un artefacto de esta operacion."""
    return f"goa_universe/{job_id}/{nombre}"


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
        alias: dict[str, str] = {}
        sin_resolver: list[str] = []
        artefactos: dict[str, Any] = {}
        if missing and not p.dry_run:
            fetched, inserted, sequences = self._fetch_and_store(session, missing, p, emit)
            alias, sin_resolver, mas_p, mas_s = self._segunda_pasada(session, missing, p, emit)
            inserted += mas_p
            sequences += mas_s
            artefactos = self._guardar_artefactos(
                payload.get("_job_id"), alias, sin_resolver, getattr(self, "_demerges", {})
            )

        result = {
            "rows_scanned": rows,
            "reliable_accessions": len(wanted),
            "malformed_skipped": malformed,
            "already_present": len(wanted) - len(missing),
            "missing": len(missing),
            "fetched": fetched,
            "resolved_as_merge": len(alias) if not p.dry_run else None,
            "not_retrievable": len(sin_resolver) if not p.dry_run else None,
            "demerged": len(getattr(self, "_demerges", {})) if not p.dry_run else None,
            "tipos_fiables": getattr(self, "_tipos_fiables", {}),
            "artefactos": artefactos,
            "proteins_inserted": inserted,
            "sequences_inserted": sequences,
            "dry_run": p.dry_run,
            "elapsed_seconds": round(time.perf_counter() - t0, 1),
        }
        emit("ensure_goa_universe.done", None, result, "info")
        return OperationResult(result=result)

    def _segunda_pasada(
        self,
        session: Session,
        missing: list[str],
        p: EnsureGoaUniversePayload,
        emit: EmitFn,
    ) -> tuple[dict[str, str], list[str], int, int]:
        """Lo que el endpoint de lote no devolvio: fusion o baja.

        La pregunta se hace a la base y no a la aritmetica. ``missing`` menos
        ``fetched`` cuenta bien, pero no dice QUIEN falta, y quien falta es lo que
        hay que resolver y lo que hay que registrar si no se resuelve.
        """
        pendientes = self._missing(session, set(missing))
        if not pendientes:
            return {}, [], 0, 0
        alias, mas_p, mas_s = self._resolve_secondary(session, pendientes, p, emit)
        sin_resolver = sorted(set(pendientes) - set(alias))
        emit(
            "ensure_goa_universe.secondary",
            None,
            {
                "pending": len(pendientes),
                "resolved_as_merge": len(alias),
                "unresolved": len(sin_resolver),
            },
            "info",
        )
        return alias, sin_resolver, mas_p, mas_s

    def _guardar_artefactos(
        self,
        job_id: Any,
        alias: dict[str, str],
        sin_resolver: list[str],
        demerges: dict[str, list[str]] | None = None,
    ) -> dict[str, Any]:
        """Persiste el mapa de fusiones y la lista de las que no se resolvieron.

        Hasta ahora ``not_retrievable`` era un numero sin nombres: proteinas con
        evidencia experimental curada que no entran al corpus y que no se podian
        citar. Un numero no se puede auditar; una lista si.
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
            if alias:
                ruta = Path(tmp) / "fusiones.tsv"
                with ruta.open("w", encoding="utf-8", newline="") as fh:
                    w = csv.writer(fh, delimiter="\t")
                    w.writerow(["accesion_gaf", "accesion_primaria"])
                    w.writerows(sorted(alias.items()))
                out["fusiones"] = {
                    "uri": store.put(universe_key_for(job_id, "fusiones.tsv"), str(ruta)),
                    "filas": len(alias),
                }
            if sin_resolver:
                ruta = Path(tmp) / "sin_resolver.txt"
                ruta.write_text("\n".join(sin_resolver) + "\n", encoding="utf-8")
                out["sin_resolver"] = {
                    "uri": store.put(universe_key_for(job_id, "sin_resolver.txt"), str(ruta)),
                    "filas": len(sin_resolver),
                }
            if demerges:
                # Aparte de las borradas, y con sus destinos: una borrada no tiene
                # a donde ir, un demerge tiene varios y elegir es una decision
                # curatorial que esta operacion no puede tomar.
                ruta = Path(tmp) / "demerges.tsv"
                with ruta.open("w", encoding="utf-8", newline="") as fh:
                    w = csv.writer(fh, delimiter="\t")
                    w.writerow(["accesion_gaf", "destinos"])
                    w.writerows((k, ",".join(v)) for k, v in sorted(demerges.items()))
                out["demerges"] = {
                    "uri": store.put(universe_key_for(job_id, "demerges.tsv"), str(ruta)),
                    "filas": len(demerges),
                }
        return out

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
        por_tipo: Counter[str] = Counter()

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
            if cols[_GAF_EVIDENCE].strip() not in reliable:
                return False
            tipo = cols[_GAF_TYPE].strip().lower()
            por_tipo[tipo or "(vacio)"] += 1
            return tipo not in _NOT_A_PROTEIN

        for rec in self._stream_gaf(p, emit, accept):
            accession = rec.accession.strip()
            if _ACCESSION.match(accession):
                wanted.add(accession)
            else:
                malformed += 1
        self._tipos_fiables = dict(por_tipo.most_common())
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

    def _resolve_secondary(
        self,
        session: Session,
        pending: list[str],
        p: EnsureGoaUniversePayload,
        emit: EmitFn,
    ) -> tuple[dict[str, str], int, int]:
        """Rescata las accesiones FUSIONADAS que el endpoint de lote no devuelve.

        El por que, el cuanto y el como se guardan estan en el docstring del
        modulo, bajo LAS ACCESIONES SECUNDARIAS.
        """
        from protea.core.operations.insert_proteins import InsertProteinsOperation

        inserter = InsertProteinsOperation()
        alias: dict[str, str] = {}
        demerges: dict[str, list[str]] = {}
        proteins = sequences = 0

        for batch in chunks(pending, _SEC_BATCH):
            payload = self._search_secondary(batch, p.timeout_seconds)
            pedidas = set(batch)
            # UNA ACCESION PUEDE SALIR EN VARIAS ENTRADAS, y entonces no es una
            # fusion. Se recoge todo primero y se decide despues, por accesion.
            candidatos: dict[str, list[tuple[str, list[UniProtProteinRecord]]]] = {}
            for entry in payload.get("results") or []:
                hecho = _records_for_merge(entry, pedidas)
                if hecho is None:
                    continue
                primary, filas, encontradas = hecho
                for sec in encontradas:
                    candidatos.setdefault(sec, []).append((primary, filas))

            records = _decidir(candidatos, alias, demerges)
            if records:
                ins_p, _upd, ins_s, _re = inserter._store_records(session, records, emit)
                proteins += ins_p
                sequences += ins_s
                session.commit()
            emit(
                "ensure_goa_universe.secondary_batch",
                None,
                {
                    "requested": len(batch),
                    "resolved": len(alias),
                    "demerged": len(demerges),
                    "rows": len(records),
                },
                "info",
            )
        self._demerges = demerges
        return alias, proteins, sequences

    def _search_secondary(self, accessions: list[str], timeout: int) -> dict[str, Any]:
        """Una consulta ``sec_acc:`` por lote. Sin ``fields``, a proposito.

        ``fields=accession,sec_acc`` da 400 ``Invalid fields parameter value
        'sec_acc'``: vale como campo de CONSULTA y no de retorno. Y hace falta
        ``secondaryAccessions`` en la respuesta para saber cual de las pedidas
        resolvio cada entrada, asi que se pide la entrada completa.
        """
        import json
        from urllib import error, parse, request

        q = " OR ".join(f"sec_acc:{a}" for a in accessions)
        url = f"{_SEARCH_URL}?query={parse.quote(q)}&format=json&size=500"
        req = request.Request(
            url,
            headers={"User-Agent": "PROTEA/ensure_goa_universe", "Accept": "application/json"},
        )
        try:
            with request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))  # type: ignore[no-any-return]
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", "replace")[:300]
            raise RuntimeError(f"UniProt sec_acc {exc.code}: {body}") from exc

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
