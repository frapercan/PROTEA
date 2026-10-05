"""The universe pass: every accession a GAF annotates reliably must exist first.

``protein_go_annotation.protein_accession`` is a foreign key, so an annotation
whose accession is absent cannot be stored -- the old loader skipped those in
silence, and for the whole clean campaign that silence meant "reviewed only".
These tests pin the three things that make the pass trustworthy: which codes
count, that NOT rows count, and that a malformed identifier never reaches a
batch.

They fake the network and NOTHING else. ``_stream_gaf`` is replaced by the
plugin's own ``parse_gaf_text`` over real GAF lines, so the column mapping and
the ``accept`` predicate are the production ones. That matters here more than
usual: the evidence test lives inside ``accept`` now, and a double that yielded
records without calling the predicate would make every test in
``TestWhichCodesCount`` pass without testing anything.
"""

import inspect
import uuid
from unittest.mock import MagicMock, patch

import pytest
from protea_sources.goa import parse_gaf_text
from pydantic import ValidationError

from protea.core.operations.ensure_goa_universe import (
    _ACCESSION,
    _BATCH,
    _GAF_EVIDENCE,
    EnsureGoaUniverseOperation,
    EnsureGoaUniversePayload,
)

#: A minimal well-formed GAF 2.1 row: 15 tab-separated columns.
_COLS = [
    "UniProtKB", "", "SYM", "", "GO:0005515", "PMID:1", "", "", "F",
    "name", "syn", "protein", "taxon:9606", "20200101", "UniProt",
]


def _con(cols, accession, code, qualifier=""):
    """Una fila a partir de una plantilla de columnas ya modificada."""
    out = list(cols)
    out[1] = accession
    out[3] = qualifier
    out[_GAF_EVIDENCE] = code
    return out


def _line(accession, code, qualifier=""):
    cols = list(_COLS)
    cols[1] = accession
    cols[3] = qualifier
    cols[_GAF_EVIDENCE] = code
    return "\t".join(cols)


def _scan(lines):
    """Run the real scan over real GAF text, faking only the HTTP fetch."""
    op = EnsureGoaUniverseOperation()
    p = EnsureGoaUniversePayload(gaf_url="http://x/g.gz")
    text = "\n".join(lines)

    def fake_stream(_p, _emit, accept):
        return parse_gaf_text(text, accept)

    with patch.object(EnsureGoaUniverseOperation, "_stream_gaf", side_effect=fake_stream):
        return op._reliable_accessions(p, MagicMock())


class TestWhichCodesCount:
    def test_the_thirteen_lafa_codes_count(self):
        """Eleven experimental plus IC and TAS, which is what democafa filters on."""
        codes = ["EXP", "IDA", "IPI", "IMP", "IGI", "IEP", "HTP", "HDA", "HMP", "HGI", "HEP", "IC", "TAS"]
        wanted, malformed, rows = _scan([_line(f"P1234{i % 10}", c) for i, c in enumerate(codes)])
        assert rows == 13
        assert malformed == 0
        assert len(wanted) > 0

    def test_the_high_throughput_codes_are_not_dropped(self):
        """CAFA's classic eight omit these five; LAFA keeps them, so we keep them."""
        for code in ("HTP", "HDA", "HMP", "HGI", "HEP"):
            wanted, _, _ = _scan([_line("P12345", code)])
            assert wanted == {"P12345"}, f"{code} must count"

    def test_only_iea_is_excluded_under_the_curated_scope(self):
        """``curated`` admits every non-IEA code, because IEA is GO's only
        automatic category. ISS, RCA, IBA, ND and NAS were all assigned by a
        person -- with varying directness -- and they qualify a protein for the
        retrieval bank. The thirteen-code tier stays recoverable because
        ``evidence_code`` is persisted per annotation row."""
        for code in ("ISS", "RCA", "IBA", "ND", "NAS"):
            wanted, _, _ = _scan([_line("P12345", code)])
            assert wanted == {"P12345"}, f"{code} is curated"
        wanted, _, _ = _scan([_line("P12345", "IEA")])
        assert wanted == set(), "IEA is the automatic one"

    def test_the_reliable_scope_still_excludes_them(self):
        """The narrower tier is one payload field away, and it is what evaluation
        truth uses. If this stopped working, the LAFA parity would be gone with
        nothing failing."""
        op = EnsureGoaUniverseOperation()
        p = EnsureGoaUniversePayload(gaf_url="http://x/g.gz", evidence_scope="reliable")
        text = "\n".join(_line("P12345", c) for c in ("ISS", "RCA", "IBA", "ND", "NAS"))

        def fake_stream(_p, _emit, accept):
            return parse_gaf_text(text, accept)

        with patch.object(EnsureGoaUniverseOperation, "_stream_gaf", side_effect=fake_stream):
            wanted, _, _ = op._reliable_accessions(p, MagicMock())
        assert wanted == set()

    def test_an_unknown_scope_is_refused_not_defaulted(self):
        """A scope that quietly fell back to a default is how a search criterion
        fixed the corpus scope for a whole campaign without anybody declaring it."""
        from protea.core.operations._universe_sources import codes_for

        with pytest.raises(ValueError):
            codes_for("todo")

    def test_a_missing_evidence_code_does_not_count(self):
        """An empty column reaches the predicate as "", not None: the predicate
        runs before the record's ``or None`` normalisation."""
        wanted, _, _ = _scan([_line("P12345", "")])
        assert wanted == set()


class TestTheRawPredicate:
    def test_the_evidence_column_is_where_we_think(self):
        """``_GAF_EVIDENCE`` is our own copy of an index the plugin keeps private.
        Pin it against the plugin's parse, not its private name: if the mapping
        ever moves, this fails here instead of silently accepting every line.
        """
        text = _line("P12345", "IDA")
        rec = next(iter(parse_gaf_text(text)))
        assert rec.evidence_code == text.split("\t")[_GAF_EVIDENCE] == "IDA"

    def test_rows_counts_lines_seen_not_records_kept(self):
        """``rows`` is the denominator the run reports (280.922.738 on GOA 156),
        so it must keep counting every data line even though the predicate now
        discards 99,76% of them before a record exists."""
        lines = [_line("P12345", "IDA")] + [_line("P99999", "IEA")] * 9
        wanted, _, rows = _scan(lines)
        assert rows == 10, "every data line is seen"
        assert wanted == {"P12345"}, "only the reliable one is kept"

    def test_comments_and_short_lines_cost_nothing(self):
        wanted, _, rows = _scan(["!gaf-version: 2.1", "too\tfew", "", _line("P12345", "IDA")])
        assert rows == 1
        assert wanted == {"P12345"}


class TestNotRowsCount:
    def test_a_not_only_protein_enters_the_universe(self):
        """A NOT is curated knowledge, and the evaluation propagates it to
        descendants and subtracts them, so the protein has to exist to carry it.
        The donor policy excludes NOT rows when picking neighbours; that is a
        different question and is not this one."""
        wanted, _, _ = _scan([_line("P12345", "IDA", qualifier="NOT|involved_in")])
        assert wanted == {"P12345"}


class TestTheAccessionGate:
    def test_well_formed_accessions_pass(self):
        for acc in ("P12345", "Q8CF25", "A0A009IHW8", "O95786", "X5M5N0"):
            assert _ACCESSION.match(acc), acc

    def test_malformed_identifiers_are_counted_not_batched(self):
        """One malformed member makes UniProt answer 400 for the WHOLE request --
        measured: 'Accession NOEXISTE1 has invalid format'. So a single stray
        identifier in GOA's object column would cost a thousand proteins. They are
        gated out here and counted, never sent."""
        wanted, malformed, _ = _scan(
            [_line("P12345", "IDA"), _line("NOEXISTE1", "IDA"), _line("P12345-2", "IDA")]
        )
        assert wanted == {"P12345"}
        assert malformed == 2

    def test_the_batch_size_is_uniprots_measured_limit(self):
        """1001 answers "Only '1000' accessions are allowed in each request"."""
        assert _BATCH == 1000


class TestThePinnedCoupling:
    def test_insert_proteins_still_exposes_what_we_reuse(self):
        """This operation calls ``InsertProteinsOperation._store_records`` rather
        than keeping a second copy of the MD5 dedup and the protein upsert. The
        return is a plain 4-tuple, so a changed signature would fail at runtime
        hours into a load instead of here. This test is the loud failure."""
        from protea.core.operations.insert_proteins import InsertProteinsOperation

        fn = getattr(InsertProteinsOperation, "_store_records", None)
        assert fn is not None, "insert_proteins no longer exposes _store_records"

        params = list(inspect.signature(fn).parameters)
        assert params == ["self", "session", "records", "emit"], params
        src = inspect.getsource(fn)
        assert "tuple[int, int, int, int]" in src, "the 4-tuple return changed"

    def test_the_plugin_still_takes_a_raw_line_filter(self):
        """The 2,6x depends on ``stream`` accepting ``accept``. A plugin rev that
        dropped it would make ``_stream_gaf`` raise TypeError on the first
        release of a 75-release run."""
        from protea_sources.goa import plugin as goa_plugin

        assert "accept" in inspect.signature(goa_plugin.stream).parameters


class TestPayload:
    def test_a_blank_url_is_refused_at_the_door(self):
        """ValidationError specifically: a blind Exception would also pass on a
        TypeError from some future refactor, and then the gate would look alive
        while refusing nothing."""
        with pytest.raises(ValidationError):
            EnsureGoaUniversePayload(gaf_url="   ")

    def test_dry_run_defaults_off(self):
        assert EnsureGoaUniversePayload(gaf_url="http://x/g.gz").dry_run is False

    def test_execute_accepts_the_shape_base_worker_delivers(self):
        """``base_worker`` hands every operation ``{**job.payload, "_job_id": ...}``
        and ``ProteaPayload`` forbids undeclared keys, so ``execute`` has to strip
        the transport key with ``contract_payload``. Written without it, this
        operation validated the raw dict and would have raised on its first real
        job -- after the download, not before. A repo-wide source walk caught it;
        this test puts the failure where the operation is.
        """
        op = EnsureGoaUniverseOperation()
        delivered = {"gaf_url": "http://x/g.gz", "dry_run": True, "_job_id": str(uuid.uuid4())}
        with (
            patch.object(
                EnsureGoaUniverseOperation, "_reliable_accessions", return_value=({"P12345"}, 0, 7)
            ),
            patch.object(EnsureGoaUniverseOperation, "_missing", return_value=["P12345"]),
        ):
            out = op.execute(MagicMock(), delivered, emit=MagicMock())
        assert out.result["rows_scanned"] == 7
        assert out.result["dry_run"] is True


class TestElTipoDelObjeto:
    """El GAF dice en la columna 11 si la fila es una proteina, un complejo o un
    RNA. Hasta ahora los no-proteina se caian por la regex de formato de accesion,
    que acierta al 100% en GOA 156 (0 de 1.032 identificadores de IntAct y
    RNAcentral la pasan) pero es acierto por accidente: un espacio de nombres
    nuevo con ids que casaran el patron de UniProt entraria sin aviso. Y la serie
    ya renombro uno a mitad, IntAct a ComplexPortal en la release 171."""

    def test_un_complejo_no_entra(self):
        cols = list(_COLS)
        cols[11] = "complex"
        wanted, _, rows = _scan(["\t".join(_con(cols, accession="P12345", code="IPI"))])
        assert wanted == set()
        assert rows == 1, "se ha visto, no se ha ignorado"

    def test_un_rna_no_entra(self):
        cols = list(_COLS)
        cols[11] = "rna"
        wanted, _, _ = _scan(["\t".join(_con(cols, accession="P12345", code="IDA"))])
        assert wanted == set()

    def test_una_proteina_si(self):
        wanted, _, _ = _scan([_line("P12345", "IDA")])
        assert wanted == {"P12345"}

    def test_un_tipo_DESCONOCIDO_entra_y_se_cuenta(self):
        """Deliberado: dejar fuera a una proteina de verdad es peor que dejar
        entrar a un tipo nuevo, porque al tipo nuevo lo frena ademas la regex y
        aparece en el histograma del resultado. Aceptar solo 'protein' convertiria
        cualquier vocabulario nuevo de GOA en una perdida silenciosa."""
        cols = list(_COLS)
        cols[11] = "algo_que_goa_invente_en_2030"
        op = EnsureGoaUniverseOperation()
        p = EnsureGoaUniversePayload(gaf_url="http://x/g.gz")
        text = "\t".join(_con(cols, accession="P12345", code="IDA"))

        def fake_stream(_p, _emit, accept):
            return parse_gaf_text(text, accept)

        with patch.object(EnsureGoaUniverseOperation, "_stream_gaf", side_effect=fake_stream):
            wanted, _, _ = op._reliable_accessions(p, MagicMock())
        assert wanted == {"P12345"}, "un tipo desconocido no se descarta"
        assert "algo_que_goa_invente_en_2030" in op._tipos_fiables, "y queda contado"


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
        op = EnsureGoaUniverseOperation()
        p = EnsureGoaUniversePayload(gaf_url="http://x/g.gz")
        guardadas = []

        def fake_store(_self, _session, records, _emit):
            guardadas.extend(records)
            return len(records), 0, 1, 0

        from protea.core.operations.insert_proteins import InsertProteinsOperation

        with (
            patch.object(
                EnsureGoaUniverseOperation, "_search_secondary", return_value={"results": [self._ENTRY]}
            ),
            patch.object(InsertProteinsOperation, "_store_records", fake_store),
        ):
            alias, _prot, _seq = op._resolve_secondary(MagicMock(), pedidas, p, MagicMock())
        return alias, guardadas

    def test_resuelve_la_secundaria_a_su_primaria(self):
        alias, _ = self._resolver(["P30456"])
        assert alias == {"P30456": "P04439"}

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
        """Mismo hash, asi que ``_store_records`` inserta UNA fila de sequence. Y
        los embeddings se indexan por Sequence, no por Protein, de modo que esto
        no mete un vecino duplicado en el banco KNN."""
        _, guardadas = self._resolver(["P30456"])
        assert len({r.sequence_hash for r in guardadas}) == 1
        assert len({r.sequence for r in guardadas}) == 1

    def test_solo_las_secundarias_pedidas_generan_alias(self):
        """La entrada trae dos secundarias y solo se pidio una. Crear la otra
        inventaria una proteina que ningun GAF anoto."""
        alias, guardadas = self._resolver(["P30456"])
        assert "P01892" not in alias
        assert "P01892" not in {r.accession for r in guardadas}


class TestLoQueNoSeResuelveQuedaConNombre:
    """``not_retrievable: 10.791`` era un numero sin nombres: proteinas con
    evidencia experimental curada que no entran al corpus y que no se podian
    citar. Un numero no se audita; una lista si."""

    def _guardar(self, alias, sin_resolver, job_id="11111111-2222-3333-4444-555555555555"):
        op = EnsureGoaUniverseOperation()
        puestos = {}

        class _Store:
            def put(self, key, path):
                puestos[key] = open(path, encoding="utf-8").read()
                return f"s3://artifacts/{key}"

        with (
            patch("protea.infrastructure.storage.get_artifact_store", return_value=_Store()),
            patch("protea.infrastructure.settings.load_settings", return_value=MagicMock()),
        ):
            out = op._guardar_artefactos(job_id, alias, sin_resolver)
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
    lote, y `_store_records` --que separa inserts de updates mirando la base y no
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
        from protea.core.operations._universe_sources import records_for_merge as _records_for_merge

        out = {}
        for e in entries:
            hecho = _records_for_merge(e, set(pedidas))
            if hecho is None:
                continue
            primary, filas, encontradas = hecho
            for sec in encontradas:
                out.setdefault(sec, []).append((primary, filas))
        return out

    def test_un_demerge_no_genera_alias(self):
        from protea.core.operations._universe_sources import classify as _decidir

        cand = self._candidatos(
            [self._entry("P9WEV8", ["C8VQ65"], "AAAA"), self._entry("P9WEV9", ["C8VQ65"], "BBBB")],
            ["C8VQ65"],
        )
        alias, demerges = {}, {}
        records = _decidir(cand, alias, demerges)
        assert alias == {}, "no se le puede asignar una primaria"
        assert demerges == {"C8VQ65": ["P9WEV8", "P9WEV9"]}, "queda registrado con sus destinos"
        assert "C8VQ65" not in {r.accession for r in records}

    def test_una_fusion_de_verdad_si_genera_alias(self):
        from protea.core.operations._universe_sources import classify as _decidir

        cand = self._candidatos([self._entry("P04439", ["P30456"])], ["P30456"])
        alias, demerges = {}, {}
        records = _decidir(cand, alias, demerges)
        assert alias == {"P30456": "P04439"}
        assert demerges == {}
        assert {r.accession for r in records} == {"P04439", "P30456"}

    def test_ninguna_accesion_se_repite_en_las_filas(self):
        """La causa inmediata del duplicate key. Dos secundarias distintas que
        caen en la misma primaria producen esa primaria dos veces."""
        from protea.core.operations._universe_sources import classify as _decidir

        cand = self._candidatos([self._entry("P04439", ["P30456", "P01892"])], ["P30456", "P01892"])
        alias, demerges = {}, {}
        records = _decidir(cand, alias, demerges)
        accs = [r.accession for r in records]
        assert len(accs) == len(set(accs)), f"accesion repetida: {accs}"
        assert set(accs) == {"P04439", "P30456", "P01892"}
        assert alias == {"P30456": "P04439", "P01892": "P04439"}

    def test_el_demerge_no_contamina_a_las_fusiones_del_mismo_lote(self):
        from protea.core.operations._universe_sources import classify as _decidir

        cand = self._candidatos(
            [
                self._entry("P9WEV8", ["C8VQ65"], "AAAA"),
                self._entry("P9WEV9", ["C8VQ65"], "BBBB"),
                self._entry("P04439", ["P30456"], "CCCC"),
            ],
            ["C8VQ65", "P30456"],
        )
        alias, demerges = {}, {}
        records = _decidir(cand, alias, demerges)
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

    ``_fetch_and_store`` only ever sees ``missing`` -- the accessions absent from
    ``protein`` -- so only those would carry audit dates. Everything a prior
    release's pass admitted, and everything ``insert_proteins`` loaded, would keep
    NULL in all three columns. With the reviewed set in place that is roughly
    575,000 of some 680,000 rows, and a date filter would silently exclude 85% of
    the corpus while appearing to work.

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

    def test_the_backfill_queries_only_rows_with_no_date(self):
        """Steady state must be zero requests: after the first release every
        universe member already has its dates, and re-fetching 680,000 accessions
        per release would add hours to every pass for nothing."""
        import inspect

        src = inspect.getsource(EnsureGoaUniverseOperation._fill_dates)
        assert "date_created.is_(None)" in src, "the backfill must filter on NULL dates"
        assert "chunks(" in src, "chunked for the 65535 bind-parameter ceiling"

    def test_execute_calls_the_backfill_after_the_fetch(self):
        """Order matters: the backfill reads what is in the table, so it has to run
        after the fetch inserted this release's new proteins."""
        import inspect

        src = inspect.getsource(EnsureGoaUniverseOperation.execute)
        assert src.index("_fetch_and_store") < src.index("_fill_dates")

    def test_the_result_reports_how_many_were_backfilled(self):
        """A pass that silently filled nothing and a pass that had nothing to fill
        look identical without this number."""
        op = EnsureGoaUniverseOperation()
        delivered = {"gaf_url": "http://x/g.gz", "dry_run": True}
        with (
            patch.object(
                EnsureGoaUniverseOperation, "_reliable_accessions", return_value=({"P12345"}, 0, 7)
            ),
            patch.object(EnsureGoaUniverseOperation, "_missing", return_value=["P12345"]),
        ):
            out = op.execute(MagicMock(), delivered, emit=MagicMock())
        assert "dates_backfilled" in out.result
