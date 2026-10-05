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

    def test_electronic_and_computational_do_not_count(self):
        wanted, _, _ = _scan([_line("P12345", c) for c in ("IEA", "ISS", "RCA", "IBA", "ND", "NAS")])
        assert wanted == set()

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
