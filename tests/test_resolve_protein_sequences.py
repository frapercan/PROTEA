"""The UniProt pass: the universe gets its sequences once, not seventy-five times.

``extract_goa_universe`` writes accessions and nothing else. This operation is
what turns them into proteins with a chain to embed, and it runs ONE time over the
union of the series instead of once per release -- which is where the 34% of
repeated requests measured across the ten passes of ``ensure_goa_universe`` went.

These tests fake the plugin and NOTHING else. The transport -- retries, backoff,
``Retry-After``, which statuses are transient -- lives in ``protea_sources`` and is
tested there; a second copy of it lived in ``protea/core/operations/_universe_http.py``
until this split deleted it, which is why the retry tests that used to be in this
file are gone rather than rewritten. What is pinned here is everything the
operation decides: which proteins it asks about, what it does with a merge, what
it records about an accession that no longer exists, and that the audit dates
reach rows this run never fetched.
"""

import inspect
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from protea.core.operations._protein_store import StoreCounts
from protea.core.operations._universe_sources import _Salida
from protea.core.operations.resolve_protein_sequences import (
    ResolveProteinSequencesOperation,
    ResolveProteinSequencesPayload,
)


def _op():
    """The operation with its plugin reference replaced by a mock.

    ``__init__`` imports the real plugin, which is what production does; the tests
    swap the attribute afterwards so the import path itself stays exercised.
    """
    op = ResolveProteinSequencesOperation()
    op._uniprot = MagicMock()
    return op


class TestLaPoblacionEsUnaConsulta:
    """Que proteinas se preguntan NO es un campo del payload.

    Es "toda fila de ``protein`` sin ``sequence_id``", leido de la tabla en el
    momento de ejecutar. Una lista de accesiones en el payload seria una hipotesis
    sobre el estado de la base congelada al enviar el job, y esta campa�a ya se
    comio una de esas: ``search_criteria: reviewed:true``, el 2026-09-15, fijo el
    alcance de toda una campa�a sin que nadie lo declarase.
    """

    def test_la_consulta_filtra_por_secuencia_nula(self):
        src = inspect.getsource(ResolveProteinSequencesOperation._without_sequence)
        assert "Protein.sequence_id.is_(None)" in src

    def test_va_ordenada_para_que_un_limite_sea_reproducible(self):
        """Sin ``order_by`` dos ejecuciones acotadas piden conjuntos distintos y
        la segunda no continua donde acabo la primera."""
        src = inspect.getsource(ResolveProteinSequencesOperation._without_sequence)
        assert "order_by(Protein.accession)" in src

    def test_la_gramatica_de_accesiones_es_una_barrera_antes_del_lote(self):
        """Un solo miembro malformado hace que UniProt responda 400 a TODO el
        lote -- medido: 'Accession NOEXISTE1 has invalid format'. Son mil
        proteinas por un identificador suelto, y esta tabla la escribe mas de una
        operacion."""
        op = _op()
        session = MagicMock()
        session.scalars.return_value.all.return_value = ["P12345", "NOEXISTE1", "Q8CF25"]
        assert op._without_sequence(session, None) == ["P12345", "Q8CF25"]


class TestElTransporteEsDelPlugin:
    """Lo que esta operacion ya no contiene.

    ``_universe_http.py`` eran 115 lineas que duplicaban la logica de reintentos
    del plugin: el mismo conjunto ``{429, 500, 502, 503, 504}``, el mismo
    ``Retry-After``, el mismo backoff. Dos copias de eso divergen sin que nadie se
    entere, y la copia de aqui era la que no tenia tests propios.
    """

    def test_no_importa_ninguna_libreria_de_red(self):
        """Leido del AST y no del texto: la prosa del modulo HABLA de reintentos y
        de ``Retry-After`` para explicar por que no los implementa, y un grep
        sobre el fichero no sabe distinguir una explicacion de una implementacion.
        """
        import ast

        import protea.core.operations.resolve_protein_sequences as mod

        arbol = ast.parse(Path(mod.__file__).read_text(encoding="utf-8"))
        importados = set()
        for nodo in ast.walk(arbol):
            if isinstance(nodo, ast.Import):
                importados.update(a.name.split(".")[0] for a in nodo.names)
            elif isinstance(nodo, ast.ImportFrom) and nodo.module:
                importados.add(nodo.module.split(".")[0])
        assert not importados & {"requests", "urllib", "http", "httpx", "socket"}, importados

    def test_todo_lo_remoto_pasa_por_el_plugin(self):
        """Los dos metodos del plugin y ninguno mas. Un tercer camino de salida
        seria otro sitio donde los reintentos pueden faltar."""
        import ast

        import protea.core.operations.resolve_protein_sequences as mod

        arbol = ast.parse(Path(mod.__file__).read_text(encoding="utf-8"))
        llamadas = {
            n.func.attr
            for n in ast.walk(arbol)
            if isinstance(n, ast.Call)
            and isinstance(n.func, ast.Attribute)
            and isinstance(n.func.value, ast.Attribute)
            and n.func.value.attr == "_uniprot"
        }
        assert llamadas == {"fetch_accessions_tsv", "search_secondary_accessions"}, llamadas

    def test_el_modulo_duplicado_ya_no_existe(self):
        with pytest.raises(ModuleNotFoundError):
            __import__("protea.core.operations._universe_http")

    def test_los_tamanos_de_lote_son_constantes_medidas_del_plugin(self):
        """1001 contesta "Only '1000' accessions are allowed in each request", y
        101 condiciones OR contestan "Maximum allowed is 100". Son hechos sobre
        UniProt, no parametros: un llamante no puede elegirlos, asi que no estan
        en el payload."""
        from protea_sources.uniprot import MAX_ACCESSIONS_PER_REQUEST, MAX_OR_CONDITIONS

        assert MAX_ACCESSIONS_PER_REQUEST == 1000
        assert MAX_OR_CONDITIONS == 100

    def test_el_timeout_del_payload_llega_a_los_knobs(self):
        knobs = ResolveProteinSequencesOperation._knobs(
            ResolveProteinSequencesPayload(timeout_seconds=7)
        )
        assert knobs.timeout_seconds == 7
        assert knobs.max_retries == 6, "lo demas se queda en los valores medidos"


class TestLasSecundarias:
    """``/uniprotkb/accessions`` casa SOLO primarias: una accesion fusionada no se
    devuelve, y UniProt la cuenta igual en X-Total-Results, asi que la respuesta
    dice "7 resultados" con el cuerpo vacio. Sobre GOA 156 eso son 10.791
    accesiones de las que el 18,2% son fusiones recuperables."""

    _ENTRY = {
        "primaryAccession": "P04439",
        "secondaryAccessions": ["P30456", "P01892"],
        "entryType": "UniProtKB reviewed (Swiss-Prot)",
        "sequence": {"value": "MAVMAPRTLLLLLSG"},
        "organism": {"scientificName": "Homo sapiens", "taxonId": 9606},
        "genes": [{"geneName": {"value": "HLA-A"}}],
    }

    def _resolver(self, pedidas):
        op = _op()
        op._uniprot.search_secondary_accessions.return_value = {"results": [self._ENTRY]}
        guardadas = []

        def fake_store(_session, records, _emit):
            guardadas.extend(records)
            return StoreCounts(proteins_inserted=len(records), sequences_inserted=1)

        salida = _Salida()
        with (
            patch(
                "protea.core.operations.resolve_protein_sequences.store_records",
                side_effect=fake_store,
            ),
            patch.object(
                ResolveProteinSequencesOperation,
                "_still_without_sequence",
                return_value=list(pedidas),
            ),
        ):
            op._resolve_merges(
                MagicMock(), list(pedidas), ResolveProteinSequencesPayload(),
                MagicMock(), salida,
            )
        return salida, guardadas

    def test_resuelve_la_secundaria_a_su_primaria(self):
        salida, _ = self._resolver(["P30456"])
        assert salida.alias == {"P30456": "P04439"}

    def test_guarda_la_primaria_Y_el_alias(self):
        """Las dos filas hacen falta: la primaria es la proteina, y el alias es lo
        que satisface la clave ajena cuando la fase 2 cargue la anotacion de 2016,
        que viene con la accesion vieja."""
        _, guardadas = self._resolver(["P30456"])
        por_acc = {r.accession: r for r in guardadas}
        assert set(por_acc) == {"P04439", "P30456"}
        assert por_acc["P04439"].is_canonical is True
        assert por_acc["P30456"].is_canonical is False
        assert por_acc["P30456"].canonical_accession == "P04439"

    def test_el_alias_no_es_una_isoforma(self):
        """``isoform_index`` es lo que separa los dos casos: entero para una
        isoforma, None para un alias de fusion. Sin eso, cualquier recuento de
        isoformas por ``NOT is_canonical`` contaria fusiones."""
        _, guardadas = self._resolver(["P30456"])
        assert all(r.isoform_index is None for r in guardadas)

    def test_las_dos_filas_comparten_la_secuencia(self):
        """Mismo hash, asi que ``store_records`` inserta UNA fila de sequence. Y
        los embeddings se indexan por Sequence, no por Protein, de modo que esto
        no mete un vecino duplicado en el banco KNN."""
        _, guardadas = self._resolver(["P30456"])
        assert len({r.sequence_hash for r in guardadas}) == 1
        assert len({r.sequence for r in guardadas}) == 1

    def test_solo_las_secundarias_pedidas_generan_alias(self):
        """La entrada trae dos secundarias y solo se pidio una. Crear la otra
        inventaria una proteina que ningun GAF anoto."""
        salida, guardadas = self._resolver(["P30456"])
        assert "P01892" not in salida.alias
        assert "P01892" not in {r.accession for r in guardadas}

    def test_lo_que_no_resuelve_queda_nombrado(self):
        """Y se pregunta a la BASE, no a la aritmetica: ``candidatos`` menos
        ``fetched`` cuenta bien pero no dice QUIEN falta, y quien falta es lo que
        hay que registrar."""
        salida, _ = self._resolver(["P30456", "Q11111"])
        assert salida.sin_resolver == ["Q11111"]


class TestLoQueNoSeResuelveQuedaConNombre:
    """``not_retrievable: 10.791`` era un numero sin nombres: proteinas con
    evidencia experimental curada que no entran al corpus y que no se podian
    citar. Un numero no se audita; una lista si."""

    def _guardar(self, alias, sin_resolver, job_id="11111111-2222-3333-4444-555555555555"):
        op = _op()
        puestos = {}

        class _Store:
            def put(self, key, path):
                puestos[key] = open(path, encoding="utf-8").read()
                return f"s3://artifacts/{key}"

        salida = _Salida(alias=dict(alias), sin_resolver=list(sin_resolver))
        with (
            patch("protea.infrastructure.storage.get_artifact_store", return_value=_Store()),
            patch("protea.infrastructure.settings.load_settings", return_value=MagicMock()),
        ):
            out = op._store_artifacts(job_id, salida)
        return out, puestos

    def test_la_lista_de_no_resueltas_se_persiste(self):
        out, puestos = self._guardar({}, ["Q11111", "Q22222"])
        assert out["sin_resolver"]["filas"] == 2
        clave = next(k for k in puestos if k.endswith("sin_resolver.txt"))
        assert puestos[clave].split() == ["Q11111", "Q22222"]

    def test_el_mapa_de_fusiones_se_persiste_con_las_dos_columnas(self):
        """Sin el mapa no se puede canonicalizar despues, y canonicalizar es lo
        que une la historia de una proteina que cambio de accesion a mitad de la
        serie."""
        out, puestos = self._guardar({"P30456": "P04439"}, [])
        assert out["fusiones"]["filas"] == 1
        clave = next(k for k in puestos if k.endswith("fusiones.tsv"))
        filas = [ln.split("\t") for ln in puestos[clave].strip().split("\n")]
        assert filas[0] == ["accesion_gaf", "accesion_primaria"]
        assert filas[1] == ["P30456", "P04439"]

    def test_la_clave_nombra_a_esta_operacion(self):
        """El prefijo cambio de ``goa_universe/`` a ``protein_resolution/``: los
        artefactos de las diez pasadas viejas siguen donde estaban y no se mezclan
        con los de una operacion que ya no hace lo mismo."""
        _, puestos = self._guardar({}, ["Q11111"])
        assert all(k.startswith("protein_resolution/") for k in puestos), puestos

    def test_sin_job_id_no_escribe_nada(self):
        """El dry run y los tests llaman sin job: no hay donde colgar el
        artefacto, y no es un error."""
        out, puestos = self._guardar({"A": "B"}, ["C"], job_id=None)
        assert out == {}
        assert puestos == {}


class TestUnDemergeNoTieneUnaIdentidad:
    """`C8VQ65` tumbo la pasada de la 156 el 2026-10-05 tras 317 fusiones
    correctas: es `DEMERGED` con `mergeDemergeTo: [P9WEV8, P9WEV9]`, asi que sale
    en DOS entradas. Eso daba dos filas alias con la misma accesion en el mismo
    lote, y el almacen --que separa inserts de updates mirando la base y no
    deduplica su propia entrada-- las mandaba como dos INSERT: duplicate key.

    Pero el choque de claves es el sintoma. El fondo es que una accesion partida
    en dos entradas NO APUNTA A UNA PROTEINA, y la secuencia que heredaria seria
    la de una de dos distintas, elegida por el orden de la respuesta.
    """

    def _entry(self, primary, secundarias, seq="MAVM"):
        return {
            "primaryAccession": primary,
            "secondaryAccessions": list(secundarias),
            "entryType": "UniProtKB reviewed (Swiss-Prot)",
            "sequence": {"value": seq},
            "organism": {"scientificName": "Mycobacterium tuberculosis", "taxonId": 83332},
            "genes": [],
        }

    def _candidatos(self, entries, pedidas):
        from protea.core.operations._universe_sources import records_for_merge

        out = {}
        for e in entries:
            hecho = records_for_merge(e, set(pedidas))
            if hecho is None:
                continue
            primary, filas, encontradas = hecho
            for sec in encontradas:
                out.setdefault(sec, []).append((primary, filas))
        return out

    def test_un_demerge_no_genera_alias(self):
        from protea.core.operations._universe_sources import classify

        cand = self._candidatos(
            [self._entry("P9WEV8", ["C8VQ65"], "AAAA"), self._entry("P9WEV9", ["C8VQ65"], "BBBB")],
            ["C8VQ65"],
        )
        alias, demerges = {}, {}
        records = classify(cand, alias, demerges)
        assert alias == {}, "no se le puede asignar una primaria"
        assert demerges == {"C8VQ65": ["P9WEV8", "P9WEV9"]}, "queda registrado con sus destinos"
        assert "C8VQ65" not in {r.accession for r in records}

    def test_una_fusion_de_verdad_si_genera_alias(self):
        from protea.core.operations._universe_sources import classify

        cand = self._candidatos([self._entry("P04439", ["P30456"])], ["P30456"])
        alias, demerges = {}, {}
        records = classify(cand, alias, demerges)
        assert alias == {"P30456": "P04439"}
        assert demerges == {}
        assert {r.accession for r in records} == {"P04439", "P30456"}

    def test_ninguna_accesion_se_repite_en_las_filas(self):
        """La causa inmediata del duplicate key. Dos secundarias distintas que
        caen en la misma primaria producen esa primaria dos veces."""
        from protea.core.operations._universe_sources import classify

        cand = self._candidatos([self._entry("P04439", ["P30456", "P01892"])], ["P30456", "P01892"])
        alias, demerges = {}, {}
        records = classify(cand, alias, demerges)
        accs = [r.accession for r in records]
        assert len(accs) == len(set(accs)), f"accesion repetida: {accs}"
        assert set(accs) == {"P04439", "P30456", "P01892"}
        assert alias == {"P30456": "P04439", "P01892": "P04439"}

    def test_el_demerge_no_contamina_a_las_fusiones_del_mismo_lote(self):
        from protea.core.operations._universe_sources import classify

        cand = self._candidatos(
            [
                self._entry("P9WEV8", ["C8VQ65"], "AAAA"),
                self._entry("P9WEV9", ["C8VQ65"], "BBBB"),
                self._entry("P04439", ["P30456"], "CCCC"),
            ],
            ["C8VQ65", "P30456"],
        )
        alias, demerges = {}, {}
        records = classify(cand, alias, demerges)
        assert alias == {"P30456": "P04439"}
        assert list(demerges) == ["C8VQ65"]
        accs = [r.accession for r in records]
        assert len(accs) == len(set(accs))


class TestTheAuditDates:
    """``date_created`` is what separates knowledge gain from entry creation, and
    ``date_sequence_modified`` is what turns the sequence leak into a named
    subset. Both arrive in the same request as the sequence, and both must be
    written by BOTH fetch paths -- the batch TSV one and the secondary JSON one.
    If only one wrote them, half the corpus would carry NULL temporal columns and
    any date filter would exclude those proteins without saying so.
    """

    _TSV = (
        "Entry\tEntry Name\tReviewed\tOrganism\tOrganism (ID)\tGene Names\t"
        "Length\tSequence\tSequence version\tDate of creation\t"
        "Date of last sequence modification\n"
        "P04439\tHLA_A\treviewed\tHomo sapiens\t9606\tHLA-A\t"
        "4\tMAVM\t2\t1987-08-13\t2003-08-22\n"
    )

    def test_the_tsv_path_carries_them(self):
        from protea.core.operations._universe_sources import _parse_tsv

        parsed = _parse_tsv(self._TSV)
        assert len(parsed) == 1
        record, dates = parsed[0]
        assert record.accession == "P04439"
        assert record.reviewed is True
        assert record.sequence == "MAVM"
        assert dates.date_created == "1987-08-13"
        assert dates.date_sequence_modified == "2003-08-22"
        assert dates.sequence_version == 2

    def test_a_reordered_response_fails_loudly(self):
        """A silently reordered response would write dates into the wrong column,
        and nothing downstream would notice a plausible date in a plausible
        field."""
        from protea.core.operations._universe_sources import _parse_tsv

        with pytest.raises(RuntimeError, match="columns"):
            _parse_tsv("Entry\tSequence\nP04439\tMAVM\n")

    def test_the_json_path_carries_the_same_three(self):
        from protea.core.operations._universe_sources import _audit_dates_of

        dates = _audit_dates_of(
            {
                "entryAudit": {
                    "firstPublicDate": "1987-08-13",
                    "lastSequenceUpdateDate": "2003-08-22",
                    "sequenceVersion": 2,
                }
            }
        )
        assert dates.date_created == "1987-08-13"
        assert dates.date_sequence_modified == "2003-08-22"
        assert dates.sequence_version == 2

    def test_an_entry_without_audit_gives_nulls_not_a_crash(self):
        from protea.core.operations._universe_sources import _audit_dates_of

        dates = _audit_dates_of({})
        assert dates == (None, None, None)

    def test_the_two_paths_agree_on_the_field_names(self):
        """The TSV and the JSON reach the same NamedTuple. A field renamed on one
        side only would leave the other writing to a column that no longer
        exists."""
        from protea.core.operations._universe_sources import _audit_dates_of, _parse_tsv

        _, from_tsv = _parse_tsv(self._TSV)[0]
        from_json = _audit_dates_of(
            {"entryAudit": {"firstPublicDate": "1987-08-13",
                            "lastSequenceUpdateDate": "2003-08-22",
                            "sequenceVersion": 2}}
        )
        assert from_tsv._fields == from_json._fields
        assert from_tsv == from_json


class TestTheDatesReachEveryUniverseMember:
    """The defect four independent reviewers caught on 2026-10-05, before it shipped.

    The sequence pass only ever sees rows with no sequence, so only those would
    carry audit dates. Everything ``insert_proteins`` loaded -- roughly 575,000 of
    some 680,000 rows -- has a sequence and NULL dates, and a date filter would
    silently exclude 85% of the corpus while appearing to work.

    What makes it dangerous is that nothing fails: the columns exist, the pass
    succeeds, and the numbers it reports are all correct. Only a query that filters
    on a date would reveal it, by returning far too little.
    """

    def test_a_dates_only_response_parses(self):
        from protea.core.operations._universe_sources import _parse_dates_tsv

        rows = _parse_dates_tsv(
            "Entry\tSequence version\tDate of creation\t"
            "Date of last sequence modification\n"
            "P04439\t2\t1987-08-13\t2003-08-22\n"
            "Q9NTW7\t3\t2002-03-27\t2003-04-30\n"
        )
        assert [acc for acc, _ in rows] == ["P04439", "Q9NTW7"]
        assert rows[0][1].date_created == "1987-08-13"
        assert rows[0][1].sequence_version == 2

    def test_a_reordered_dates_response_fails_loudly(self):
        """A creation date and a sequence version are both plausible in either
        column, so a silent reorder would be unnoticeable in the data."""
        from protea.core.operations._universe_sources import _parse_dates_tsv

        with pytest.raises(RuntimeError, match="date columns"):
            _parse_dates_tsv("Entry\tDate of creation\nP04439\t1987-08-13\n")

    def test_the_backfill_has_its_own_predicate(self):
        """``date_created IS NULL``, not "what this run fetched". They are
        different populations and that difference IS the defect."""
        src = inspect.getsource(ResolveProteinSequencesOperation._backfill_dates)
        assert "Protein.date_created.is_(None)" in src
        assert "sequence_id" not in src, "la poblacion de las fechas no es la de las secuencias"

    def test_the_backfill_runs_after_the_fetch(self):
        """Order matters: the backfill reads what is in the table, so it has to
        run after the fetch inserted this run's new proteins."""
        src = inspect.getsource(ResolveProteinSequencesOperation.execute)
        assert src.index("_fetch_sequences") < src.index("_backfill_dates")

    def test_the_result_reports_how_many_were_backfilled(self):
        """A run that silently filled nothing and a run that had nothing to fill
        look identical without this number."""
        op = _op()
        with patch.object(
            ResolveProteinSequencesOperation, "_without_sequence", return_value=["P12345"]
        ):
            out = op.execute(MagicMock(), {"dry_run": True}, emit=MagicMock())
        assert "dates_backfilled" in out.result
        assert out.result["candidates"] == 1


class TestUnaPasadaAcotadaNoSeLeeComoCompleta:
    """``max_accessions`` existe para una prueba de humo antes de la de verdad.

    Un tope silencioso es peor que no tenerlo: el informe de una pasada acotada y
    el de una completa serian identicos, y el segundo es el que dice que el corpus
    esta entero.
    """

    def test_el_limite_viaja_al_informe(self):
        op = _op()
        with patch.object(
            ResolveProteinSequencesOperation, "_without_sequence", return_value=["P12345"]
        ):
            out = op.execute(
                MagicMock(), {"dry_run": True, "max_accessions": 500}, emit=MagicMock()
            )
        assert out.result["limit"] == 500

    def test_sin_limite_el_informe_lo_dice_tambien(self):
        op = _op()
        with patch.object(
            ResolveProteinSequencesOperation, "_without_sequence", return_value=[]
        ):
            out = op.execute(MagicMock(), {"dry_run": True}, emit=MagicMock())
        assert out.result["limit"] is None

    def test_un_limite_de_cero_es_un_error_no_una_pasada_vacia(self):
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            ResolveProteinSequencesPayload(max_accessions=0)

    def test_execute_acepta_la_forma_que_entrega_base_worker(self):
        """``base_worker`` entrega ``{**job.payload, "_job_id": ...}`` y
        ``ProteaPayload`` prohibe claves no declaradas, asi que ``execute`` tiene
        que quitar la clave de transporte con ``contract_payload``. Escrito sin
        eso, la operacion habria fallado en su primer job real."""
        import uuid

        op = _op()
        with patch.object(
            ResolveProteinSequencesOperation, "_without_sequence", return_value=[]
        ):
            out = op.execute(
                MagicMock(),
                {"dry_run": True, "_job_id": str(uuid.uuid4())},
                emit=MagicMock(),
            )
        assert out.result["dry_run"] is True
