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
    CURATED_INFERENCE_CODES,
    TIER_CURATED_INFERENCE,
    TIER_SWISSPROT,
    TIER_TRUTH,
    TRUTH_CODES,
    _RowCounters,
    codes_for_tiers,
    entry_name_is_readable,
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
    """A row built from an already-modified column template."""
    out = list(cols)
    out[1] = accession
    out[3] = qualifier
    out[_GAF_EVIDENCE] = code
    out[_GAF_SYNONYM] = f"{entry_name or accession + '_HUMAN'}|gene"
    return out


def _line(accession, code, qualifier="", entry_name=None):
    """A realistic GAF row.

    ``entry_name`` defaults to ``<accession>_HUMAN``, which is the TrEMBL form.
    That matters: the template used to say ``"syn"``, a name that does not start
    with the accession, so under tier ``swissprot_of_release`` EVERY test row would
    have been admitted as reviewed and the evidence-predicate tests would have
    stopped measuring what they claim to. An unrealistic fixture is a test that
    passes for the wrong reason.
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
        wanted, malformed, counters = op._admissible_accessions(p, MagicMock())
    # ``rows`` is returned rather than the object so the assertions that already
    # existed keep measuring exactly what they measured; whoever needs the counters
    # uses ``_scan_with_counters``.
    return wanted, malformed, counters.rows


def _scan_with_counters(lines, admit=None):
    """Like :func:`_scan` but returning the counters object."""
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
        rows = [_line("P00001", "IDA"), _line("P00002", "ISS")]
        solo_verdad, _, _ = _scan(rows, admit=[TIER_TRUTH])
        assert solo_verdad == {"P00001"}
        con_inferencia, _, _ = _scan(rows, admit=[TIER_TRUTH, TIER_CURATED_INFERENCE])
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
    """Swiss-Prot membership comes from the GAF, dated, with no download."""

    def test_the_synonym_column_is_where_we_think(self):
        """``_GAF_SYNONYM`` is anchored against the fields the plugin DOES expose.

        The plugin's record has eight fields (accession, go_id, qualifier,
        evidence_code, db_reference, with_from, assigned_by, annotation_date) and
        the synonym is NOT among them. Neither is the object type, which
        ``_GAF_TYPE`` was already reading just as blindly. So index 10 cannot be
        pinned directly, the way index 6 can.

        What can be done: put a distinct marker in every column and check that the
        six fields the plugin does expose land where this module believes. That
        proves the plugin splits in the standard GAF 2.x order, and indices 8, 10
        and 11 are determined by that same split. If the plugin moved its mapping,
        the anchors would break here.

        The strong fix would be for the plugin to expose the entry name in its
        record, since it is part of the GAF and now decides the criterion. That is
        a contract change and goes separately, not as a freebie.
        """
        cols = [f"c{i}" for i in range(17)]
        cols[_GAF_ID] = "P12345"
        cols[4] = "GO:0005515"
        cols[_GAF_EVIDENCE] = "IDA"
        cols[_GAF_SYNONYM] = "FOO_HUMAN|foo"
        cols[_GAF_TYPE] = "protein"
        text = "\t".join(cols)
        rec = next(iter(parse_gaf_text(text)))

        # The anchors: if any of them moves, the split is not what we believe.
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
        """Measured 0 of 280,916,291 rows in GOA 156, so this does not happen; but
        if it did, the row is decided by its evidence code and not by a guess."""
        assert is_swissprot_entry("P12345", "") is False

    def test_a_prefix_that_is_not_the_whole_name_is_still_swissprot(self):
        """The cut is ``<accession>_``, not ``startswith``. A mnemonic that begins
        with the same letters is not TrEMBL."""
        assert is_swissprot_entry("P12345", "P12345X_HUMAN") is True

    def test_swissprot_admits_a_row_its_evidence_would_reject(self):
        """That is the point of the tier: a reviewed entry enters by MEMBERSHIP,
        even if its only annotation is IEA."""
        wanted, _, _ = _scan([_line("P12345", "IEA", entry_name="FOO_HUMAN")])
        assert wanted == {"P12345"}

    def test_trembl_with_only_iea_stays_out(self):
        wanted, _, _ = _scan([_line("P12345", "IEA")])
        assert wanted == set()

    def test_dropping_the_swissprot_tier_drops_it(self):
        rows = [_line("P12345", "IEA", entry_name="FOO_HUMAN")]
        assert _scan(rows, admit=[TIER_TRUTH])[0] == set()
        assert _scan(rows, admit=[TIER_TRUTH, TIER_SWISSPROT])[0] == {"P12345"}


class TestAnUnknownCodeIsNotADefault:
    def test_it_is_rejected_and_counted(self):
        """A code GO adds after the partition was written needs a DECISION. The
        previous criterion, the complement of IEA, would have admitted it without
        anybody noticing."""
        op = ExtractGoaUniverseOperation()
        p = ExtractGoaUniversePayload(gaf_url="http://x/g.gz", release=156)
        text = "\n".join([_line("P00001", "IDA"), _line("P00002", "XYZ")])
        emit = MagicMock()

        def fake_stream(_p, _emit, accept):
            return parse_gaf_text(text, accept)

        with patch.object(ExtractGoaUniverseOperation, "_stream_gaf", side_effect=fake_stream):
            wanted, _, counters = op._admissible_accessions(p, emit)
        assert wanted == {"P00001"}, "the unknown one does not enter"
        assert dict(counters.unknown_codes) == {"XYZ": 1}
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


class TestWhatThisOperationDoesNot:
    """The private coupling is gone, and so is the test that pinned it.

    Until 2026-10-06 this operation called
    ``InsertProteinsOperation._store_records``, a private method of another
    operation, and a test pinned its signature because a break would otherwise
    have surfaced at runtime, hours into a load. The store now lives in
    ``_protein_store`` and both import it, so there is nothing left to pin. What
    needs pinning is that this operation stores NO sequences at all.
    """

    def test_does_not_touch_the_sequence_table(self):
        """If it inserted sequences again it would need the network again, in a
        pass that runs 75 times, which is the defect the split closed."""
        import protea.core.operations.extract_goa_universe as mod

        text = Path(mod.__file__).read_text(encoding="utf-8")
        assert "SequenceModel" not in text
        assert "store_records" not in text
        assert "protea_sources.uniprot" not in text

    def test_the_plugin_still_takes_a_raw_line_filter(self):
        """The 2.6x depends on ``stream`` accepting ``accept``. A plugin rev that
        dropped it would make ``_stream_gaf`` raise TypeError on the first release
        of a 75-release run."""
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
                ExtractGoaUniverseOperation, "_admissible_accessions", return_value=({"P12345"}, 0, _RowCounters(rows=7))
            ),
            patch.object(ExtractGoaUniverseOperation, "_missing", return_value=["P12345"]),
        ):
            out = op.execute(MagicMock(), delivered, emit=MagicMock())
        assert out.result["rows_scanned"] == 7
        assert out.result["dry_run"] is True


class TestTheObjectType:
    """Column 11 of the GAF states whether the row is a protein, a complex or an
    RNA. Non-proteins used to be stopped by the accession-format regex, which is
    100% right on GOA 156 (0 of 1,032 IntAct and RNAcentral identifiers pass it)
    but right by accident: a new namespace whose ids matched UniProt's pattern
    would enter unannounced. And the series has already renamed one mid-way,
    IntAct to ComplexPortal at release 171."""

    def test_a_complex_does_not_enter(self):
        cols = list(_COLS)
        cols[11] = "complex"
        wanted, _, rows = _scan(["\t".join(_con(cols, accession="P12345", code="IPI"))])
        assert wanted == set()
        assert rows == 1, "it was seen, not ignored"

    def test_an_rna_does_not_enter(self):
        cols = list(_COLS)
        cols[11] = "rna"
        wanted, _, _ = _scan(["\t".join(_con(cols, accession="P12345", code="IDA"))])
        assert wanted == set()

    def test_a_protein_does(self):
        wanted, _, _ = _scan([_line("P12345", "IDA")])
        assert wanted == {"P12345"}

    def test_an_UNKNOWN_type_enters_and_is_counted(self):
        """Deliberate: leaving out a real protein is worse than letting a new type
        in, because the regex stops the new type anyway and it shows up in the
        result's histogram. Admitting only 'protein' would turn any new GOA
        vocabulary into a silent loss."""
        cols = list(_COLS)
        cols[11] = "something_goa_invents_in_2030"
        op = ExtractGoaUniverseOperation()
        p = ExtractGoaUniversePayload(gaf_url="http://x/g.gz", release=156)
        text = "\t".join(_con(cols, accession="P12345", code="IDA"))

        def fake_stream(_p, _emit, accept):
            return parse_gaf_text(text, accept)

        with patch.object(ExtractGoaUniverseOperation, "_stream_gaf", side_effect=fake_stream):
            wanted, _, counters = op._admissible_accessions(p, MagicMock())
        assert wanted == {"P12345"}, "an unknown type is not discarded"
        assert "something_goa_invents_in_2030" in counters.by_type, "and it stays counted"




class TestTheRowItInserts:
    """A ``protein`` row with an accession and nothing else.

    This is what makes separating the sequences possible: ``protein.sequence_id``
    is nullable and ``canonical_accession`` is not, so the one thing an accession
    states by itself -- whether it is an isoform of another -- has to be parsed
    here, and everything else is filled in by ``resolve_protein_sequences`` when
    UniProt answers. Phase 2 needs no more than this: ``load_goa_annotations``
    filters on ``select(Protein.accession)``.
    """

    def test_a_canonical_accession(self):
        row = ExtractGoaUniverseOperation._accession_row("P12345", 156)
        assert row == {
            "accession": "P12345",
            "canonical_accession": "P12345",
            "is_canonical": True,
            "isoform_index": None,
            "first_admitted_release": 156,
        }

    def test_an_isoform_points_at_its_canonical(self):
        row = ExtractGoaUniverseOperation._accession_row("P12345-2", 194)
        assert row["canonical_accession"] == "P12345"
        assert row["is_canonical"] is False
        assert row["isoform_index"] == 2

    def test_writes_no_column_that_belongs_to_uniprot(self):
        """Leaving them NULL is not an oversight: it is what lets
        ``apply_record_updates`` fill them later without clobbering anything.
        Writing a ``reviewed`` read from the GAF here would be worse than not
        writing it, because ``protein.reviewed`` is TODAY's snapshot and the GAF
        carries its own release's."""
        row = ExtractGoaUniverseOperation._accession_row("P12345", 156)
        for column in ("sequence_id", "reviewed", "entry_name", "length",
                       "organism", "taxonomy_id", "gene_name", "date_created"):
            assert column not in row

    def test_the_insert_does_not_forgive_a_conflict(self):
        """``missing`` was computed against this same table moments earlier, so a
        key conflict means that query lied. Without ``ON CONFLICT DO NOTHING`` that
        is an exception, and that is correct: with it, it would be a silent skip and
        the reported count would be false."""
        src = inspect.getsource(ExtractGoaUniverseOperation._insert_accessions)
        assert "on_conflict" not in src


class TestTheTwoRejectionsAreCountedApart:
    """``malformed_skipped`` conflated two different things.

    One was "this is not a UniProtKB accession" and the other "this is a complex or
    an RNA". On 2026-10-06 the figure dropped from ~27,300 to ~13,500 between
    releases 227 and 226 and nobody could say which of the two had moved, which is
    exactly what a conflated number prevents.
    """

    def test_an_accession_that_does_not_parse_counts_as_malformed(self):
        _wanted, malformed, counters = _scan_with_counters([_line("NOEXISTE1", "IDA")])
        assert malformed == 1
        assert counters.not_a_protein == 0

    def test_a_type_that_is_not_a_protein_counts_in_its_own_bucket(self):
        cols = list(_COLS)
        cols[_GAF_TYPE] = "complex"
        _wanted, malformed, counters = _scan_with_counters(
            ["\t".join(_con(cols, accession="P12345", code="IPI"))]
        )
        assert malformed == 0
        assert counters.not_a_protein == 1

    def test_the_report_carries_both(self):
        op = ExtractGoaUniverseOperation()
        counters = _RowCounters(rows=9, not_a_protein=4)
        with (
            patch.object(
                ExtractGoaUniverseOperation,
                "_admissible_accessions",
                return_value=({"P12345"}, 3, counters),
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
        assert "malformed_skipped" not in out.result, "the conflated figure is gone"


class TestTheFirstReleaseIsAMinimum:
    """``first_admitted_release`` is written with ``IS NULL OR > N``, not on insert.

    The series is walked ascending, so in a clean run the minimum coincides with
    the first writer. But the order is a property of the driver and the column is a
    property of the corpus: if someone processes 194 before 156, the minimum is
    still 156, and a repeated pass does not raise the value.
    """

    def test_the_query_lowers_the_value_and_never_raises_it(self):
        src = inspect.getsource(ExtractGoaUniverseOperation._write_first_release)
        assert "first_admitted_release.is_(None)" in src
        assert "first_admitted_release > release" in src

    def test_the_new_row_already_carries_the_release(self):
        """If it did not, the later UPDATE would have to cover it and the number
        reported as backfilled would count the new rows too."""
        assert ExtractGoaUniverseOperation._accession_row("P12345", 156)[
            "first_admitted_release"
        ] == 156

    def test_the_report_separates_inserted_from_backfilled(self):
        op = ExtractGoaUniverseOperation()
        with (
            patch.object(
                ExtractGoaUniverseOperation,
                "_admissible_accessions",
                return_value=({"P12345", "Q99999"}, 0, _RowCounters(rows=2)),
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


class TestGoaDroppedTheEntryNameAtRelease179:
    """The defect that stalled release 179 for three hours and would have put
    7,639,329 accessions in the corpus.

    GOA stopped putting the UniProtKB entry name first in DB Object Synonym at
    release 179 and started putting the GENE SYMBOL. The name is in no other
    column: the row that reads ``A0A021WW32_DROME|vtd|80Fh|...`` in release 178
    reads ``vtd|vtd|80Fh|...`` in 179.

    The rule inferred Swiss-Prot from the ABSENCE of the TrEMBL pattern, so with no
    entry name it said True for every row. Measured over the cached releases:
    164..178 carry the name in 100% of rows, 179 in 0%, 180 in 0%, 231 in 5%. That
    is 52 of the 75 releases of this series, including the evaluation window.
    """

    def test_the_real_row_from_179_is_not_swissprot(self):
        """Taken verbatim from the cached GAF, same protein as the 178 row below."""
        assert is_swissprot_entry("A0A021WW32", "vtd|vtd|80Fh|CG40222|DRAD21") is False

    def test_the_same_protein_in_178_is_correctly_trembl(self):
        assert is_swissprot_entry("A0A021WW32", "A0A021WW32_DROME|vtd|80Fh") is False

    def test_a_real_swissprot_name_still_passes(self):
        """The fix must not close the tier where the data IS there."""
        assert is_swissprot_entry("P12345", "HLA_A_HUMAN|hla") is True
        assert is_swissprot_entry("P04439", "1A01_HUMAN|HLA-A") is True

    def test_a_gene_symbol_is_not_a_readable_entry_name(self):
        """``moeA5``, ``vtd``: no underscore, so no organism code, so not a name."""
        for symbol in ("moeA5", "vtd", "CG40222", ""):
            assert entry_name_is_readable(symbol) is False, symbol

    def test_the_231_shape_is_not_a_readable_entry_name(self):
        """``GA0070216_102329`` has an underscore but the suffix is digits, not an
        uppercase organism code. It is a locus tag."""
        assert entry_name_is_readable("GA0070216_102329") is False

    def test_a_real_entry_name_is_readable(self):
        for name in ("A0A000_STRVD", "HLA_A_HUMAN", "1A01_HUMAN", "Q8CF25_MOUSE"):
            assert entry_name_is_readable(name) is True, name

    def test_cannot_tell_is_counted_apart_from_not_reviewed(self):
        """The whole point: a release with no tier must be distinguishable from a
        release where the tier admitted nobody."""
        cols = list(_COLS)
        cols[_GAF_SYNONYM] = "vtd|vtd"
        cols[_GAF_EVIDENCE] = "IEA"
        cols[_GAF_ID] = "A0A021WW32"
        _wanted, _malformed, counters = _scan_with_counters(["\t".join(cols)])
        assert counters.entry_name_unreadable == 1
        assert counters.by_tier["swissprot_of_release"] == 0

    def test_a_readable_name_that_is_trembl_is_not_counted_as_unreadable(self):
        cols = list(_COLS)
        cols[_GAF_SYNONYM] = "A0A021WW32_DROME|vtd"
        cols[_GAF_EVIDENCE] = "IEA"
        cols[_GAF_ID] = "A0A021WW32"
        _wanted, _malformed, counters = _scan_with_counters(["\t".join(cols)])
        assert counters.entry_name_unreadable == 0, "legible y TrEMBL no es 'no se puede saber'"

    def test_the_release_without_the_tier_emits_a_warning(self):
        """The only other symptom would be an admissible count lower than expected,
        which is exactly what nobody looks at."""
        op = ExtractGoaUniverseOperation()
        p = ExtractGoaUniversePayload(gaf_url="http://x/g.gz", release=179)
        filas = []
        for i in range(4):
            cols = list(_COLS)
            cols[_GAF_ID] = f"A0A0{i:02d}"
            cols[_GAF_EVIDENCE] = "IEA"
            cols[_GAF_SYNONYM] = "vtd|vtd"      # la forma de la 179: simbolo de gen
            filas.append("\t".join(cols))
        text = "\n".join(filas)
        emit = MagicMock()

        def fake_stream(_p, _emit, accept):
            return parse_gaf_text(text, accept)

        with patch.object(ExtractGoaUniverseOperation, "_stream_gaf", side_effect=fake_stream):
            op._admissible_accessions(p, emit)
        eventos = [c.args[0] for c in emit.call_args_list]
        assert "extract_goa_universe.swissprot_tier_unavailable" in eventos

    def test_the_report_carries_the_count(self):
        op = ExtractGoaUniverseOperation()
        counters = _RowCounters(rows=10, entry_name_unreadable=10)
        with (
            patch.object(
                ExtractGoaUniverseOperation,
                "_admissible_accessions",
                return_value=(set(), 0, counters),
            ),
            patch.object(ExtractGoaUniverseOperation, "_missing", return_value=[]),
        ):
            out = op.execute(
                MagicMock(),
                {"gaf_url": "http://x/g.gz", "release": 179, "dry_run": True},
                emit=MagicMock(),
            )
        assert out.result["rows_entry_name_unreadable"] == 10
