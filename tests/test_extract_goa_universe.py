"""The GAF pass: every accession a release admits must exist in ``protein`` first.

``protein_go_annotation.protein_accession`` is a foreign key, so an annotation
whose accession is absent cannot be stored -- the old loader skipped those in
silence, and for the whole clean campaign that silence meant "reviewed only".
These tests pin the three things that make the pass trustworthy: which codes
count, that NOT rows count, and that a malformed identifier never reaches a
batch.

There is no network here to fake beyond the file itself, which is the point of
the split: this operation reads a GAF and writes accessions, and
``resolve_protein_sequences`` is the only one that talks to UniProt.

``_stream_gaf`` is replaced by the
plugin's own ``parse_gaf_text`` over real GAF lines, so the column mapping and
the ``accept`` predicate are the production ones. That matters here more than
usual: the evidence test lives inside ``accept`` now, and a double that yielded
records without calling the predicate would make every test in
``TestWhichCodesCount`` pass without testing anything.
"""

import inspect
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from protea_sources.goa import parse_gaf_text
from pydantic import ValidationError

from protea.core.operations._universe_sources import (
    ACCESSION_GRAMMAR,
    ALL_KNOWN_CODES,
    _Cuentas,
    CURATED_INFERENCE_CODES,
    TIER_CURATED_INFERENCE,
    TIER_SWISSPROT,
    TIER_TRUTH,
    TRUTH_CODES,
    codes_for_tiers,
    is_swissprot_entry,
)
from protea.core.operations.extract_goa_universe import (
    _GAF_EVIDENCE,
    _GAF_ID,
    _GAF_SYNONYM,
    _GAF_TYPE,
    ExtractGoaUniverseOperation,
    ExtractGoaUniversePayload,
)

#: A minimal well-formed GAF 2.1 row: 15 tab-separated columns.
_COLS = [
    "UniProtKB", "", "SYM", "", "GO:0005515", "PMID:1", "", "", "F",
    "name", "syn", "protein", "taxon:9606", "20200101", "UniProt",
]


def _con(cols, accession, code, qualifier="", entry_name=None):
    """Una fila a partir de una plantilla de columnas ya modificada."""
    out = list(cols)
    out[1] = accession
    out[3] = qualifier
    out[_GAF_EVIDENCE] = code
    out[_GAF_SYNONYM] = f"{entry_name or accession + '_HUMAN'}|gene"
    return out


def _line(accession, code, qualifier="", entry_name=None):
    """Una fila de GAF realista.

    ``entry_name`` por defecto es ``<accesion>_HUMAN``, que es la forma de
    TrEMBL. Importa: la plantilla ponia ``"syn"``, un nombre que no empieza por
    la accesion, asi que bajo el nivel ``swissprot_of_release`` TODA fila de test
    habria entrado como revisada y los tests del predicado de evidencia habrian
    dejado de medir lo que dicen medir. Un fixture irreal es un test que pasa por
    el motivo equivocado.
    """
    cols = list(_COLS)
    cols[1] = accession
    cols[3] = qualifier
    cols[_GAF_EVIDENCE] = code
    cols[_GAF_SYNONYM] = f"{entry_name or accession + '_HUMAN'}|gene"
    return "\t".join(cols)


def _scan(lines, admit=None):
    """Run the real scan over real GAF text, faking only the HTTP fetch."""
    op = ExtractGoaUniverseOperation()
    p = (
        ExtractGoaUniversePayload(gaf_url="http://x/g.gz", release=156, admit=admit)
        if admit is not None
        else ExtractGoaUniversePayload(gaf_url="http://x/g.gz", release=156)
    )
    text = "\n".join(lines)

    def fake_stream(_p, _emit, accept):
        return parse_gaf_text(text, accept)

    with patch.object(ExtractGoaUniverseOperation, "_stream_gaf", side_effect=fake_stream):
        wanted, malformed, cuentas = op._admissible_accessions(p, MagicMock())
    # Se devuelve ``rows`` y no el objeto para que las aserciones que ya existian
    # sigan midiendo exactamente lo que median; quien necesite los contadores usa
    # ``_scan_con_cuentas``.
    return wanted, malformed, cuentas.rows


def _scan_con_cuentas(lines, admit=None):
    """Como :func:`_scan` pero devolviendo el objeto de contadores."""
    op = ExtractGoaUniverseOperation()
    p = (
        ExtractGoaUniversePayload(gaf_url="http://x/g.gz", release=156, admit=admit)
        if admit is not None
        else ExtractGoaUniversePayload(gaf_url="http://x/g.gz", release=156)
    )
    text = "\n".join(lines)

    def fake_stream(_p, _emit, accept):
        return parse_gaf_text(text, accept)

    with patch.object(ExtractGoaUniverseOperation, "_stream_gaf", side_effect=fake_stream):
        return op._admissible_accessions(p, MagicMock())


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

    def test_the_excluded_three_are_excluded_each_for_its_own_reason(self):
        """``IEA``, ``IBA``/``IBD`` and ``ND`` are out, and not for the same reason.

        This replaces a test that asserted the OPPOSITE -- that ``ISS``, ``RCA``,
        ``IBA``, ``ND`` and ``NAS`` all counted, because the criterion was the
        complement of ``IEA``. That criterion was reverted on 2026-10-06 after
        measuring what the complement admitted: 58% of the corpus entered by
        ``IBA`` alone, and 83,950 accessions on GOA 156 carried only ``ND``, with
        83,949 of them annotated exclusively on ontology ROOT terms whose
        Information Accretion is zero by construction.

        The old expectation is kept here as a comment rather than deleted, so a
        reader can see that the behaviour changed deliberately.
        """
        for code in ("IEA", "IBA", "IBD", "ND"):
            wanted, _, _ = _scan([_line("P12345", code)])
            assert wanted == set(), f"{code} must not admit"

    def test_curated_inference_admits_but_is_not_truth(self):
        """T3 is in the corpus and out of the truth, and both halves matter."""
        for code in sorted(CURATED_INFERENCE_CODES):
            wanted, _, _ = _scan([_line("P12345", code)])
            assert wanted == {"P12345"}, f"{code} admits under curated_inference"
            assert code not in TRUTH_CODES, f"{code} must never be truth"

    def test_dropping_a_tier_narrows_the_corpus(self):
        """The tier list is the lever, so removing one has to be visible."""
        filas = [_line("P00001", "IDA"), _line("P00002", "ISS")]
        solo_verdad, _, _ = _scan(filas, admit=[TIER_TRUTH])
        assert solo_verdad == {"P00001"}
        con_inferencia, _, _ = _scan(filas, admit=[TIER_TRUTH, TIER_CURATED_INFERENCE])
        assert con_inferencia == {"P00001", "P00002"}

    def test_an_unknown_tier_is_refused_not_defaulted(self):
        """A scope that quietly fell back to a default is how a search criterion
        fixed the corpus scope for a whole campaign without anybody declaring it.
        A tier that quietly NARROWED it would be the same mistake mirrored."""
        with pytest.raises(ValueError):
            codes_for_tiers(["todo"])
        with pytest.raises(ValidationError):
            ExtractGoaUniversePayload(gaf_url="http://x/g.gz", admit=["todo"])
        with pytest.raises(ValidationError, match="at least one tier"):
            ExtractGoaUniversePayload(gaf_url="http://x/g.gz", admit=[])

    def test_the_partition_is_exact(self):
        """26 codes, 13 + 9 + 4, nothing unclassified and nothing invented.

        This is the invariant that makes the criterion an ENUMERATION instead of
        a complement. If GO adds a code, this fails and somebody has to put it in
        a tier -- which is the point: the previous criterion would have admitted
        it silently.
        """
        from protea.core.evidence_codes import ECO_TO_CODE
        from protea.core.operations._universe_sources import (
            ABSENCE_CODES,
            AUTOMATIC_CODES,
            PROPAGATED_CODES,
        )

        conocidos = set(ECO_TO_CODE.values())
        assert len(TRUTH_CODES) == 13
        assert len(CURATED_INFERENCE_CODES) == 9
        assert len(AUTOMATIC_CODES | PROPAGATED_CODES | ABSENCE_CODES) == 4
        assert ALL_KNOWN_CODES == conocidos, (
            f"sin clasificar: {sorted(conocidos - ALL_KNOWN_CODES)}; "
            f"inventados: {sorted(ALL_KNOWN_CODES - conocidos)}"
        )
        # Y los niveles no se solapan: un codigo esta en exactamente uno.
        cubos = [TRUTH_CODES, CURATED_INFERENCE_CODES, AUTOMATIC_CODES,
                 PROPAGATED_CODES, ABSENCE_CODES]
        for i, a in enumerate(cubos):
            for b in cubos[i + 1:]:
                assert not (a & b), f"solapan: {sorted(a & b)}"

    def test_truth_codes_are_exactly_the_lafa_regime(self):
        """Criterio de admision y criterio de verdad son el MISMO conjunto, y eso
        es deliberado: si divergen, el corpus admite por una regla y se puntua por
        otra, que es la clase de defecto que esta campana lleva corrigiendo."""
        from protea.core.ia_regimes import LAFA_EVIDENCE

        assert TRUTH_CODES == set(LAFA_EVIDENCE)


class TestSwissProtOfTheRelease:
    """La pertenencia a Swiss-Prot sale del GAF, fechada, sin descargar nada."""

    def test_the_synonym_column_is_where_we_think(self):
        """``_GAF_SYNONYM`` se ancla contra los campos que el plugin SI expone.

        El registro del plugin tiene ocho campos --accession, go_id, qualifier,
        evidence_code, db_reference, with_from, assigned_by, annotation_date-- y
        el sinonimo NO esta entre ellos. Tampoco el tipo de objeto, que
        ``_GAF_TYPE`` ya venia leyendo igual de a ciegas. Asi que no se puede
        fijar el indice 10 directamente, como si se fija el 6.

        Lo que si se puede: poner un marcador distinto en cada columna y
        comprobar que los seis campos que el plugin expone caen donde este
        modulo cree. Eso demuestra que el plugin trocea en el orden estandar de
        GAF 2.x, y los indices 8, 10 y 11 quedan determinados por ese mismo
        troceo. Si el plugin moviera su mapeo, los anclajes se romperian aqui.

        El arreglo fuerte seria que el plugin expusiera el nombre de entrada en
        su registro, ya que es parte del GAF y ahora decide el criterio. Es un
        cambio de contrato y va aparte, no de propina.
        """
        cols = [f"c{i}" for i in range(17)]
        cols[_GAF_ID] = "P12345"
        cols[4] = "GO:0005515"
        cols[_GAF_EVIDENCE] = "IDA"
        cols[_GAF_SYNONYM] = "FOO_HUMAN|foo"
        cols[_GAF_TYPE] = "protein"
        text = "\t".join(cols)
        rec = next(iter(parse_gaf_text(text)))

        # Los anclajes: si cualquiera se mueve, el troceo ya no es el que creemos.
        assert rec.accession == cols[_GAF_ID] == "P12345"
        assert rec.go_id == cols[4]
        assert rec.evidence_code == cols[_GAF_EVIDENCE] == "IDA"
        assert rec.db_reference == cols[5]
        assert rec.with_from == cols[7]
        assert rec.assigned_by == cols[14]
        assert rec.annotation_date == cols[13]
        # Y con el troceo fijado, el sinonimo es el 10 y el tipo el 11.
        assert text.split("\t")[_GAF_SYNONYM] == "FOO_HUMAN|foo"
        assert text.split("\t")[_GAF_TYPE] == "protein"

    def test_trembl_names_itself_after_its_accession(self):
        # Medido en el GAF real: A0A000 lleva A0A000_9ACTN|moeA5.
        assert is_swissprot_entry("A0A000", "A0A000_9ACTN|moeA5") is False
        assert is_swissprot_entry("A0A021WW64", "A0A021WW64_DROME|CG17162") is False

    def test_swissprot_names_itself_after_a_gene(self):
        # Medido: P34546 lleva VATL2_CAEEL.
        assert is_swissprot_entry("P34546", "VATL2_CAEEL") is True
        assert is_swissprot_entry("P12345", "FOO_HUMAN|foo") is True

    def test_an_empty_entry_name_does_not_guess(self):
        """Medido 0 de 280.916.291 filas en GOA 156, asi que esto no ocurre; pero
        si ocurriera, la fila la decide su codigo de evidencia y no una
        suposicion."""
        assert is_swissprot_entry("P12345", "") is False

    def test_a_prefix_that_is_not_the_whole_name_is_still_swissprot(self):
        """El corte es ``<accesion>_``, no ``startswith``. Un mnemonico que
        empiece por las mismas letras no es TrEMBL."""
        assert is_swissprot_entry("P12345", "P12345X_HUMAN") is True

    def test_swissprot_admits_a_row_its_evidence_would_reject(self):
        """Es el punto del nivel: una entrada revisada entra por PERTENENCIA,
        aunque su unica anotacion sea IEA."""
        wanted, _, _ = _scan([_line("P12345", "IEA", entry_name="FOO_HUMAN")])
        assert wanted == {"P12345"}

    def test_trembl_with_only_iea_stays_out(self):
        wanted, _, _ = _scan([_line("P12345", "IEA")])
        assert wanted == set()

    def test_dropping_the_swissprot_tier_drops_it(self):
        filas = [_line("P12345", "IEA", entry_name="FOO_HUMAN")]
        assert _scan(filas, admit=[TIER_TRUTH])[0] == set()
        assert _scan(filas, admit=[TIER_TRUTH, TIER_SWISSPROT])[0] == {"P12345"}


class TestAnUnknownCodeIsNotADefault:
    def test_it_is_rejected_and_counted(self):
        """Un codigo que GO anada despues de escribirse la particion necesita una
        DECISION. El criterio anterior, que era el complemento de IEA, lo habria
        admitido sin que nadie se enterase."""
        op = ExtractGoaUniverseOperation()
        p = ExtractGoaUniversePayload(gaf_url="http://x/g.gz", release=156)
        text = "\n".join([_line("P00001", "IDA"), _line("P00002", "XYZ")])
        emit = MagicMock()

        def fake_stream(_p, _emit, accept):
            return parse_gaf_text(text, accept)

        with patch.object(ExtractGoaUniverseOperation, "_stream_gaf", side_effect=fake_stream):
            wanted, _, cuentas = op._admissible_accessions(p, emit)
        assert wanted == {"P00001"}, "el desconocido no entra"
        assert dict(cuentas.desconocidos) == {"XYZ": 1}
        eventos = [c.args[0] for c in emit.call_args_list]
        assert "extract_goa_universe.unknown_evidence_codes" in eventos


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
            assert ACCESSION_GRAMMAR.match(acc), acc

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


class TestLoQueEstaOperacionNoHace:
    """El acoplamiento privado se fue, y con el su test.

    Hasta el 2026-10-06 esta operacion llamaba a
    ``InsertProteinsOperation._store_records`` -- un metodo privado de otra
    operacion -- y habia un test fijando su firma porque la rotura habria salido
    en tiempo de ejecucion, horas dentro de una carga. Ahora el almacen vive en
    ``_protein_store`` y lo importan las dos, asi que no hay nada que fijar: lo
    que hay que fijar es que esta operacion NO guarda secuencias en absoluto.
    """

    def test_no_toca_la_tabla_sequence(self):
        """Si volviera a insertar secuencias, volveria a necesitar la red en una
        pasada que se repite 75 veces, que es el defecto que la particion cerro."""
        import protea.core.operations.extract_goa_universe as mod

        texto = Path(mod.__file__).read_text(encoding="utf-8")
        assert "SequenceModel" not in texto
        assert "store_records" not in texto
        assert "protea_sources.uniprot" not in texto

    def test_el_plugin_sigue_aceptando_un_filtro_de_linea_cruda(self):
        """El 2,6x depende de que ``stream`` acepte ``accept``. Una rev del plugin
        que lo quitara haria que ``_stream_gaf`` lanzara TypeError en la primera
        release de una serie de 75."""
        from protea_sources.goa import plugin as goa_plugin

        assert "accept" in inspect.signature(goa_plugin.stream).parameters


class TestPayload:
    def test_a_blank_url_is_refused_at_the_door(self):
        """ValidationError specifically: a blind Exception would also pass on a
        TypeError from some future refactor, and then the gate would look alive
        while refusing nothing."""
        with pytest.raises(ValidationError):
            ExtractGoaUniversePayload(gaf_url="   ", release=156)

    def test_dry_run_defaults_off(self):
        assert ExtractGoaUniversePayload(gaf_url="http://x/g.gz", release=156).dry_run is False

    def test_execute_accepts_the_shape_base_worker_delivers(self):
        """``base_worker`` hands every operation ``{**job.payload, "_job_id": ...}``
        and ``ProteaPayload`` forbids undeclared keys, so ``execute`` has to strip
        the transport key with ``contract_payload``. Written without it, this
        operation validated the raw dict and would have raised on its first real
        job -- after the download, not before. A repo-wide source walk caught it;
        this test puts the failure where the operation is.
        """
        op = ExtractGoaUniverseOperation()
        delivered = {"gaf_url": "http://x/g.gz", "release": 156, "dry_run": True,
                     "_job_id": str(uuid.uuid4())}
        with (
            patch.object(
                ExtractGoaUniverseOperation, "_admissible_accessions", return_value=({"P12345"}, 0, _Cuentas(rows=7))
            ),
            patch.object(ExtractGoaUniverseOperation, "_missing", return_value=["P12345"]),
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
        op = ExtractGoaUniverseOperation()
        p = ExtractGoaUniversePayload(gaf_url="http://x/g.gz", release=156)
        text = "\t".join(_con(cols, accession="P12345", code="IDA"))

        def fake_stream(_p, _emit, accept):
            return parse_gaf_text(text, accept)

        with patch.object(ExtractGoaUniverseOperation, "_stream_gaf", side_effect=fake_stream):
            wanted, _, cuentas = op._admissible_accessions(p, MagicMock())
        assert wanted == {"P12345"}, "un tipo desconocido no se descarta"
        assert "algo_que_goa_invente_en_2030" in cuentas.por_tipo, "y queda contado"




class TestLaFilaQueSeInserta:
    """Una fila de ``protein`` con accesion y nada mas.

    Es lo que hace posible separar las secuencias: ``protein.sequence_id`` es
    nullable y ``canonical_accession`` no lo es, asi que la unica cosa que una
    accesion dice por si sola -- si es una isoforma de otra -- hay que parsearla
    aqui, y todo lo demas lo rellena ``resolve_protein_sequences`` cuando UniProt
    contesta. La fase 2 no necesita mas: ``load_goa_annotations`` filtra con
    ``select(Protein.accession)``.
    """

    def test_una_accesion_canonica(self):
        fila = ExtractGoaUniverseOperation._accession_row("P12345", 156)
        assert fila == {
            "accession": "P12345",
            "canonical_accession": "P12345",
            "is_canonical": True,
            "isoform_index": None,
            "first_admitted_release": 156,
        }

    def test_una_isoforma_apunta_a_su_canonica(self):
        fila = ExtractGoaUniverseOperation._accession_row("P12345-2", 194)
        assert fila["canonical_accession"] == "P12345"
        assert fila["is_canonical"] is False
        assert fila["isoform_index"] == 2

    def test_no_escribe_ninguna_columna_que_sea_de_uniprot(self):
        """Dejarlas en NULL no es un descuido: es lo que permite que
        ``apply_record_updates`` las rellene despues sin pisar nada. Escribir aqui
        un ``reviewed`` leido del GAF seria peor que no escribirlo, porque
        ``protein.reviewed`` es la instantanea de HOY y el GAF trae la de su
        release."""
        fila = ExtractGoaUniverseOperation._accession_row("P12345", 156)
        for columna in ("sequence_id", "reviewed", "entry_name", "length",
                        "organism", "taxonomy_id", "gene_name", "date_created"):
            assert columna not in fila

    def test_la_insercion_no_perdona_un_conflicto(self):
        """``missing`` se calculo contra esta misma tabla hace un momento, asi que
        un conflicto de clave significa que esa consulta minti�. Sin ``ON CONFLICT
        DO NOTHING`` eso es una excepcion, y eso es lo correcto: con el seria un
        salto silencioso y el recuento informado seria falso."""
        src = inspect.getsource(ExtractGoaUniverseOperation._insert_accessions)
        assert "on_conflict" not in src


class TestLosDosRechazosSeCuentanAparte:
    """``malformed_skipped`` mezclaba dos cosas distintas.

    Una era "esto no es una accesion de UniProtKB" y la otra "esto es un complejo
    o un RNA". El 2026-10-06 la cifra bajo de ~27.300 a ~13.500 entre las releases
    227 y 226 y nadie podia decir cual de las dos se habia movido, que es
    exactamente lo que un numero mezclado impide.
    """

    def test_una_accesion_que_no_parsea_cuenta_como_malformada(self):
        _wanted, malformed, cuentas = _scan_con_cuentas([_line("NOEXISTE1", "IDA")])
        assert malformed == 1
        assert cuentas.no_proteina == 0

    def test_un_tipo_que_no_es_proteina_cuenta_en_su_propio_cubo(self):
        cols = list(_COLS)
        cols[_GAF_TYPE] = "complex"
        _wanted, malformed, cuentas = _scan_con_cuentas(
            ["\t".join(_con(cols, accession="P12345", code="IPI"))]
        )
        assert malformed == 0
        assert cuentas.no_proteina == 1

    def test_el_informe_lleva_las_dos(self):
        op = ExtractGoaUniverseOperation()
        cuentas = _Cuentas(rows=9, no_proteina=4)
        with (
            patch.object(
                ExtractGoaUniverseOperation,
                "_admissible_accessions",
                return_value=({"P12345"}, 3, cuentas),
            ),
            patch.object(ExtractGoaUniverseOperation, "_missing", return_value=[]),
        ):
            out = op.execute(
                MagicMock(),
                {"gaf_url": "http://x/g.gz", "release": 156, "dry_run": True},
                emit=MagicMock(),
            )
        assert out.result["malformed_accessions"] == 3
        assert out.result["rows_not_a_protein"] == 4
        assert "malformed_skipped" not in out.result, "la cifra mezclada ya no existe"


class TestLaPrimeraReleaseEsUnMinimo:
    """``first_admitted_release`` se escribe con ``IS NULL OR > N``, no al insertar.

    La serie se recorre ascendente, asi que en una pasada limpia el minimo coincide
    con el primer escritor. Pero el orden es una propiedad del driver y la columna
    es una propiedad del corpus: si alguien procesa la 194 antes de la 156, el
    minimo sigue siendo 156 y una pasada repetida no sube el valor.
    """

    def test_la_consulta_baja_el_valor_y_nunca_lo_sube(self):
        src = inspect.getsource(ExtractGoaUniverseOperation._write_first_release)
        assert "first_admitted_release.is_(None)" in src
        assert "first_admitted_release > release" in src

    def test_la_fila_nueva_ya_trae_la_release(self):
        """Si no la trajera, el UPDATE posterior tendria que cubrirla y el numero
        informado como 'rellenadas' contaria tambien las nuevas."""
        assert ExtractGoaUniverseOperation._accession_row("P12345", 156)[
            "first_admitted_release"
        ] == 156

    def test_el_informe_separa_insertadas_de_rellenadas(self):
        op = ExtractGoaUniverseOperation()
        with (
            patch.object(
                ExtractGoaUniverseOperation,
                "_admissible_accessions",
                return_value=({"P12345", "Q99999"}, 0, _Cuentas(rows=2)),
            ),
            patch.object(ExtractGoaUniverseOperation, "_missing", return_value=["P12345"]),
            patch.object(ExtractGoaUniverseOperation, "_insert_accessions", return_value=1),
            patch.object(ExtractGoaUniverseOperation, "_write_first_release", return_value=1),
        ):
            out = op.execute(
                MagicMock(), {"gaf_url": "http://x/g.gz", "release": 156}, emit=MagicMock()
            )
        assert out.result["proteins_inserted"] == 1
        assert out.result["first_release_written"] == 1
        assert out.result["release"] == 156
