"""
Tests for InsertProteinsOperation.
Unit tests use mocked HTTP + mocked session (no DB, no network).
Integration test uses a real Postgres via --with-postgres.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import protea.infrastructure.orm.models  # noqa: F401
from protea.core.operations.insert_proteins import (
    InsertProteinsOperation,
    InsertProteinsPayload,
)
from protea.infrastructure.orm.base import Base

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _noop_emit(event, message, fields, level):
    pass


def _capturing_emit():
    calls = []

    def emit(event, message, fields, level):
        calls.append({"event": event, "fields": fields, "level": level})

    emit.calls = calls  # type: ignore[attr-defined]
    return emit


FASTA_ONE = (
    ">sp|P12345|TEST_HUMAN Test protein OS=Homo sapiens OX=9606 GN=TEST PE=1 SV=1\n"
    "MKTAYIAKQRQISFVKSHFSRQLEERLGLIEVQAPILSRVGDGTQDNLSGAEKAVQVKVKALPDAQFEVVHSLAKWKRQTLGQHDFSAGEGLYTHMKALRPDEDRLSPLHSVYVDQWDWERVMGDGERQFSTLKSTVEAIWAGIKATEAAVSEEFGLAPFLPDQIHFVHSQELLSRYPDLDAKGRERAIAKDLGAVFLVGIGGKLSDGHRHDVRAPDYDDWSTPSELGHAGLNGDILVWNPVLEDAFELSSMGIRVDADTLKHQLALTGDENKVLHYFTQIV\n"
)

FASTA_TWO = FASTA_ONE + (
    ">tr|Q99999|TEST2_MOUSE Another protein OS=Mus musculus OX=10090 GN=T2 PE=2 SV=1\n"
    "MSTAYIAKQRQISFVKSHFSRQLEERLGLIEVQ\n"
)


def _make_mock_response(fasta_text: str, link_header: str = "") -> MagicMock:
    resp = MagicMock()
    resp.status_code = 200
    resp.content = fasta_text.encode("utf-8")
    resp.headers = {"link": link_header}
    resp.raise_for_status = MagicMock()
    return resp


def _make_mock_session():
    """Session mock that returns empty results for all DB queries."""
    session = MagicMock(spec=Session)
    session.query.return_value.filter.return_value.all.return_value = []
    session.query.return_value.filter.return_value.first.return_value = None
    return session


# ---------------------------------------------------------------------------
# Unit tests — InsertProteinsPayload
# ---------------------------------------------------------------------------

class TestInsertProteinsPayload:
    def test_minimal_valid(self):
        p = InsertProteinsPayload.model_validate({"search_criteria": "organism_id:9606"})
        assert p.search_criteria == "organism_id:9606"
        assert p.page_size == 500
        assert p.include_isoforms is True
        assert p.total_limit is None

    def test_all_fields(self):
        p = InsertProteinsPayload.model_validate({
            "search_criteria": "organism_id:9606",
            "page_size": 100,
            "total_limit": 50,
            "timeout_seconds": 30,
            "include_isoforms": False,
            "compressed": True,
            "max_retries": 2,
        })
        assert p.page_size == 100
        assert p.total_limit == 50
        assert p.include_isoforms is False

    def test_missing_search_criteria_raises(self):
        with pytest.raises(ValueError, match="search_criteria"):
            InsertProteinsPayload.model_validate({})

    def test_empty_search_criteria_raises(self):
        with pytest.raises(ValueError, match="search_criteria"):
            InsertProteinsPayload.model_validate({"search_criteria": "  "})

    def test_invalid_page_size_raises(self):
        with pytest.raises(ValueError, match="page_size"):
            InsertProteinsPayload.model_validate({"search_criteria": "q", "page_size": -1})

    def test_invalid_total_limit_raises(self):
        with pytest.raises(ValueError, match="total_limit"):
            InsertProteinsPayload.model_validate({"search_criteria": "q", "total_limit": 0})

    def test_null_total_limit_allowed(self):
        p = InsertProteinsPayload.model_validate({"search_criteria": "q", "total_limit": None})
        assert p.total_limit is None

    def test_search_criteria_stripped(self):
        p = InsertProteinsPayload.model_validate({"search_criteria": "  q  "})
        assert p.search_criteria == "q"


# ---------------------------------------------------------------------------
# Unit tests — _parse_fasta / _parse_header
# ---------------------------------------------------------------------------

class TestParseFasta:
    def setup_method(self):
        self.op = InsertProteinsOperation()

    def test_parses_single_record(self):
        records = self.op._parse_fasta(FASTA_ONE)
        assert len(records) == 1
        r = records[0]
        assert r["accession"] == "P12345"
        assert r["reviewed"] is True
        assert r["organism"] == "Homo sapiens"
        assert r["taxonomy_id"] == "9606"
        assert r["gene_name"] == "TEST"
        assert len(r["sequence"]) > 0
        assert r["length"] == len(r["sequence"])

    def test_parses_multiple_records(self):
        records = self.op._parse_fasta(FASTA_TWO)
        assert len(records) == 2
        assert records[1]["accession"] == "Q99999"
        assert records[1]["reviewed"] is False
        assert records[1]["taxonomy_id"] == "10090"

    def test_empty_fasta_returns_empty(self):
        assert self.op._parse_fasta("") == []

    def test_canonical_isoform_parsing(self):
        fasta = (
            ">sp|P12345-2|TEST_HUMAN Isoform 2 OS=Homo sapiens OX=9606\n"
            "MKTAYIAK\n"
        )
        records = self.op._parse_fasta(fasta)
        assert records[0]["canonical_accession"] == "P12345"
        assert records[0]["is_canonical"] is False
        assert records[0]["isoform_index"] == 2

    def test_sequence_hash_is_set(self):
        records = self.op._parse_fasta(FASTA_ONE)
        assert records[0]["sequence_hash"] is not None
        assert len(records[0]["sequence_hash"]) == 32  # MD5 hex

    def test_empty_sequence_skipped(self):
        """Lines 231-233: header with no sequence lines is skipped."""
        fasta = ">sp|P12345|TEST_HUMAN Test OS=Homo sapiens OX=9606\n\n"
        records = self.op._parse_fasta(fasta)
        assert records == []

    def test_header_without_pipe_separators(self):
        """Lines 264-265: header without | uses first word as accession."""
        fasta = ">SIMPLE_ACC some description\nMKTAYIAK\n"
        records = self.op._parse_fasta(fasta)
        assert len(records) == 1
        assert records[0]["accession"] == "SIMPLE_ACC"
        assert records[0]["entry_name"] is None

    def test_isoform_accession_parsed(self):
        fasta = (
            ">sp|P12345-3|TEST_HUMAN Isoform 3 OS=Homo sapiens OX=9606 GN=TEST\n"
            "MKTAYIAK\n"
        )
        records = self.op._parse_fasta(fasta)
        r = records[0]
        assert r["accession"] == "P12345-3"
        assert r["canonical_accession"] == "P12345"
        assert r["is_canonical"] is False
        assert r["isoform_index"] == 3

    def test_canonical_accession_flagged(self):
        records = self.op._parse_fasta(FASTA_ONE)
        r = records[0]
        assert r["canonical_accession"] == "P12345"
        assert r["is_canonical"] is True
        assert r["isoform_index"] is None

    def test_reviewed_vs_unreviewed(self):
        records = self.op._parse_fasta(FASTA_TWO)
        assert records[0]["reviewed"] is True   # sp|
        assert records[1]["reviewed"] is False   # tr|

    def test_sequence_deduplication_by_hash(self):
        """Two identical sequences produce the same hash."""
        fasta = (
            ">sp|P11111|A_HUMAN Prot A OS=Homo sapiens OX=9606\nMKTAYIAK\n"
            ">sp|P22222|B_HUMAN Prot B OS=Homo sapiens OX=9606\nMKTAYIAK\n"
        )
        records = self.op._parse_fasta(fasta)
        assert len(records) == 2
        assert records[0]["sequence_hash"] == records[1]["sequence_hash"]

    def test_multiline_sequence(self):
        fasta = (
            ">sp|P12345|TEST_HUMAN Test OS=Homo sapiens OX=9606\n"
            "MKTAY\n"
            "IAKQR\n"
        )
        records = self.op._parse_fasta(fasta)
        assert records[0]["sequence"] == "MKTAYIAKQR"
        assert records[0]["length"] == 10


# ---------------------------------------------------------------------------
# Unit tests — _decode_response
# ---------------------------------------------------------------------------

class TestDecodeResponse:
    def setup_method(self):
        self.op = InsertProteinsOperation()

    def test_decode_uncompressed(self):
        """Line 217: uncompressed path."""
        resp = MagicMock()
        resp.content = b"hello world"
        result = self.op._decode_response(resp, compressed=False)
        assert result == "hello world"

    def test_decode_compressed(self):
        """Lines 215-216: gzip decompression path."""
        import gzip
        from io import BytesIO

        raw = b"compressed content"
        buf = BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as f:
            f.write(raw)
        resp = MagicMock()
        resp.content = buf.getvalue()
        result = self.op._decode_response(resp, compressed=True)
        assert result == "compressed content"


# ---------------------------------------------------------------------------
# Unit tests — _store_records
# ---------------------------------------------------------------------------

class TestStoreRecords:
    def setup_method(self):
        self.op = InsertProteinsOperation()

    def test_empty_records_returns_zeros(self):
        """Line 300: empty records early return."""
        session = _make_mock_session()
        result = self.op._store_records(session, [], _noop_emit)
        assert result == (0, 0, 0, 0)
        session.query.assert_not_called()

    def test_updates_existing_protein(self):
        """Lines 350-394: existing protein gets conservative updates."""
        from protea.infrastructure.orm.models.sequence.sequence import (
            Sequence as SequenceModel,
        )

        seq_hash = SequenceModel.compute_hash("MKTAYIAK")
        record = {
            "accession": "P12345",
            "entry_name": "TEST_HUMAN",
            "canonical_accession": "P12345",
            "is_canonical": True,
            "isoform_index": None,
            "organism": "Homo sapiens",
            "taxonomy_id": "9606",
            "gene_name": "TEST",
            "reviewed": True,
            "sequence": "MKTAYIAK",
            "length": 8,
            "sequence_hash": seq_hash,
        }

        # Existing protein with missing fields (triggers updates)
        existing_prot = MagicMock()
        existing_prot.accession = "P12345"
        existing_prot.sequence_id = None  # will be updated
        existing_prot.entry_name = None  # will be updated
        existing_prot.canonical_accession = "OLD_ACC"  # will be updated
        existing_prot.is_canonical = False  # will be updated
        existing_prot.isoform_index = 2  # will be updated
        existing_prot.reviewed = None  # will be updated
        existing_prot.taxonomy_id = None  # will be updated
        existing_prot.organism = None  # will be updated
        existing_prot.gene_name = None  # will be updated
        existing_prot.length = None  # will be updated

        session = MagicMock(spec=Session)

        # _load_existing_sequences returns the hash → id map
        seq_query = MagicMock()
        seq_query.filter.return_value.all.return_value = [(seq_hash, 42)]

        # _load_existing_proteins returns the existing protein
        prot_query = MagicMock()
        prot_query.filter.return_value.all.return_value = [existing_prot]

        call_idx = {"n": 0}

        def query_side_effect(*args):
            call_idx["n"] += 1
            if call_idx["n"] == 1:
                return seq_query
            return prot_query

        session.query.side_effect = query_side_effect

        ins_p, upd_p, ins_s, re_s = self.op._store_records(session, [record], _noop_emit)

        assert ins_p == 0
        assert upd_p == 1  # existing protein was updated
        assert re_s == 1  # sequence was reused from DB
        assert ins_s == 0
        # Verify fields were updated
        assert existing_prot.sequence_id == 42
        assert existing_prot.entry_name == "TEST_HUMAN"
        assert existing_prot.canonical_accession == "P12345"
        assert existing_prot.is_canonical is True
        assert existing_prot.isoform_index is None
        assert existing_prot.reviewed is True

    def test_inserts_new_sequence_when_missing(self):
        """Lines 318-334: new sequence inserted when hash not in DB."""
        from protea.infrastructure.orm.models.sequence.sequence import (
            Sequence as SequenceModel,
        )

        seq_hash = SequenceModel.compute_hash("MKTAYIAK")
        record = {
            "accession": "P12345",
            "entry_name": "TEST_HUMAN",
            "canonical_accession": "P12345",
            "is_canonical": True,
            "isoform_index": None,
            "organism": "Homo sapiens",
            "taxonomy_id": "9606",
            "gene_name": "TEST",
            "reviewed": True,
            "sequence": "MKTAYIAK",
            "length": 8,
            "sequence_hash": seq_hash,
        }

        session = MagicMock(spec=Session)

        # No existing sequences
        seq_query = MagicMock()
        seq_query.filter.return_value.all.return_value = []

        # No existing proteins
        prot_query = MagicMock()
        prot_query.filter.return_value.all.return_value = []

        call_idx = {"n": 0}

        def query_side_effect(*args):
            call_idx["n"] += 1
            if call_idx["n"] == 1:
                return seq_query
            return prot_query

        session.query.side_effect = query_side_effect

        ins_p, upd_p, ins_s, re_s = self.op._store_records(session, [record], _noop_emit)

        assert ins_p == 1
        assert upd_p == 0
        assert ins_s == 1
        assert re_s == 0
        # add_all called twice: once for sequences, once for proteins
        assert session.add_all.call_count == 2


# ---------------------------------------------------------------------------
# Unit tests — execute() with mocked HTTP and session
# ---------------------------------------------------------------------------

class TestInsertProteinsOperationExecute:
    def setup_method(self):
        self.op = InsertProteinsOperation()

    def test_execute_returns_operation_result(self):
        session = _make_mock_session()
        emit = _capturing_emit()

        with patch.object(self.op._http, "get", return_value=_make_mock_response(FASTA_ONE)):
            result = self.op.execute(
                session,
                {"search_criteria": "organism_id:9606", "compressed": False},
                emit=emit,
            )

        assert result.result["pages"] == 1
        assert result.result["retrieved_records"] == 1
        assert result.result["proteins_inserted"] == 1
        assert result.result["http_requests"] == 1
        assert result.result["http_retries"] == 0

    def test_execute_emits_start_and_done(self):
        session = _make_mock_session()
        emit = _capturing_emit()

        with patch.object(self.op._http, "get", return_value=_make_mock_response(FASTA_ONE)):
            self.op.execute(
                session,
                {"search_criteria": "q", "compressed": False},
                emit=emit,
            )

        events = [c["event"] for c in emit.calls]
        assert "insert_proteins.start" in events
        assert "insert_proteins.done" in events

    def test_execute_respects_total_limit(self):
        session = _make_mock_session()
        emit = _capturing_emit()

        with patch.object(self.op._http, "get", return_value=_make_mock_response(FASTA_TWO)):
            result = self.op.execute(
                session,
                {"search_criteria": "q", "total_limit": 1, "compressed": False},
                emit=emit,
            )

        assert result.result["retrieved_records"] == 1
        limit_events = [c for c in emit.calls if c["event"] == "insert_proteins.limit_reached"]
        assert len(limit_events) == 1

    def test_execute_calls_session_add_all_for_new_protein(self):
        session = _make_mock_session()

        with patch.object(self.op._http, "get", return_value=_make_mock_response(FASTA_ONE)):
            self.op.execute(
                session,
                {"search_criteria": "q", "compressed": False},
                emit=_noop_emit,
            )

        session.add_all.assert_called()

    def test_two_records_counts_correctly(self):
        session = _make_mock_session()
        emit = _capturing_emit()

        with patch.object(self.op._http, "get", return_value=_make_mock_response(FASTA_TWO)):
            result = self.op.execute(
                session,
                {"search_criteria": "q", "compressed": False},
                emit=emit,
            )

        assert result.result["retrieved_records"] == 2
        assert result.result["proteins_inserted"] == 2

    def test_empty_page_continues(self):
        """Line 93: empty records list triggers continue."""
        session = _make_mock_session()
        emit = _capturing_emit()
        # First response is empty FASTA, no link header → single page with 0 records
        empty_resp = _make_mock_response("")
        with patch.object(self.op._http, "get", return_value=empty_resp):
            result = self.op.execute(
                session,
                {"search_criteria": "q", "compressed": False},
                emit=emit,
            )
        assert result.result["retrieved_records"] == 0
        assert result.result["pages"] == 1

    def test_total_limit_trims_to_zero_breaks(self):
        """Lines 96-98: when total_limit is already reached, records trimmed to empty → break."""
        session = _make_mock_session()
        emit = _capturing_emit()

        # Two pages: first has 2 records (we set limit=2), second also has records
        # but after retrieving 2 on page 1 we should stop
        page1_resp = _make_mock_response(
            FASTA_TWO,
            link_header='<https://rest.uniprot.org/?cursor=abc>; rel="next"',
        )
        page2_resp = _make_mock_response(FASTA_ONE)

        call_count = {"n": 0}

        def get_side_effect(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return page1_resp
            return page2_resp

        with patch.object(self.op._http, "get", side_effect=get_side_effect):
            result = self.op.execute(
                session,
                {"search_criteria": "q", "total_limit": 2, "compressed": False},
                emit=emit,
            )

        assert result.result["retrieved_records"] == 2

    def test_compressed_param_appended(self):
        """Line 180: compressed=true adds compressed=true to URL params."""
        session = _make_mock_session()
        emit = _capturing_emit()

        import gzip
        from io import BytesIO

        buf = BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as f:
            f.write(FASTA_ONE.encode("utf-8"))
        compressed_content = buf.getvalue()

        resp = MagicMock()
        resp.status_code = 200
        resp.content = compressed_content
        resp.headers = {"link": ""}
        resp.raise_for_status = MagicMock()

        with patch.object(self.op._http, "get", return_value=resp) as mock_get:
            self.op.execute(
                session,
                {"search_criteria": "q", "compressed": True},
                emit=emit,
            )

        called_url = mock_get.call_args[0][0]
        assert "compressed=true" in called_url

    def test_total_results_from_header(self):
        """Line 200: X-Total-Results header is captured."""
        session = _make_mock_session()
        emit = _capturing_emit()

        resp = _make_mock_response(FASTA_ONE)
        resp.headers["X-Total-Results"] = "42"

        op = InsertProteinsOperation()
        with patch.object(op._http, "get", return_value=resp):
            op.execute(
                session,
                {"search_criteria": "q", "compressed": False},
                emit=emit,
            )

        assert op._total_results == 42

    def test_total_results_invalid_header_ignored(self):
        """Line 200: non-numeric X-Total-Results doesn't crash."""
        session = _make_mock_session()
        emit = _capturing_emit()

        resp = _make_mock_response(FASTA_ONE)
        resp.headers["X-Total-Results"] = "not-a-number"

        op = InsertProteinsOperation()
        with patch.object(op._http, "get", return_value=resp):
            op.execute(
                session,
                {"search_criteria": "q", "compressed": False},
                emit=emit,
            )

        assert op._total_results is None

    def test_cursor_pagination(self):
        """Lines 208-210: cursor-based pagination follows link headers."""
        session = _make_mock_session()
        emit = _capturing_emit()

        page1_resp = _make_mock_response(
            FASTA_ONE,
            link_header='<https://rest.uniprot.org/?cursor=abc123>; rel="next"',
        )
        page2_resp = _make_mock_response(FASTA_ONE)  # no link header → last page

        call_count = {"n": 0}
        called_urls: list[str] = []

        def get_side_effect(url, **kwargs):
            call_count["n"] += 1
            called_urls.append(url)
            if call_count["n"] == 1:
                return page1_resp
            return page2_resp

        op = InsertProteinsOperation()
        with patch.object(op._http, "get", side_effect=get_side_effect):
            result = op.execute(
                session,
                {"search_criteria": "q", "compressed": False},
                emit=emit,
            )

        assert result.result["pages"] == 2
        assert result.result["retrieved_records"] == 2
        # Second call URL should contain cursor
        assert "cursor=abc123" in called_urls[1]

    def test_network_failure_propagates(self):
        """HTTP errors propagate to caller."""
        import requests as req

        session = _make_mock_session()
        op = InsertProteinsOperation()

        with patch.object(
            op._http,
            "get",
            side_effect=req.ConnectionError("network down"),
        ):
            with pytest.raises(req.ConnectionError):
                op.execute(
                    session,
                    {
                        "search_criteria": "q",
                        "compressed": False,
                        "max_retries": 1,
                        "backoff_base_seconds": 0.0,
                        "backoff_max_seconds": 0.0,
                        "jitter_seconds": 0.0,
                    },
                    emit=_noop_emit,
                )

    def test_isoform_records_counted(self):
        """Isoform records are counted in the result."""
        session = _make_mock_session()
        emit = _capturing_emit()

        fasta_with_isoform = (
            ">sp|P12345|TEST_HUMAN Test OS=Homo sapiens OX=9606\nMKTAYIAK\n"
            ">sp|P12345-2|TEST_HUMAN Isoform 2 OS=Homo sapiens OX=9606\nMKTAYIAKQR\n"
        )
        resp = _make_mock_response(fasta_with_isoform)
        op = InsertProteinsOperation()
        with patch.object(op._http, "get", return_value=resp):
            result = op.execute(
                session,
                {"search_criteria": "q", "compressed": False},
                emit=emit,
            )

        assert result.result["isoform_records"] == 1

    def test_progress_emission_with_total(self):
        """Progress events include _progress_current and _progress_total."""
        session = _make_mock_session()
        emit = _capturing_emit()

        resp = _make_mock_response(FASTA_ONE)
        resp.headers["X-Total-Results"] = "100"

        op = InsertProteinsOperation()
        with patch.object(op._http, "get", return_value=resp):
            op.execute(
                session,
                {"search_criteria": "q", "compressed": False},
                emit=emit,
            )

        page_done_events = [
            c for c in emit.calls if c["event"] == "insert_proteins.page_done"
        ]
        assert len(page_done_events) == 1
        fields = page_done_events[0]["fields"]
        assert fields["_progress_current"] == 1
        assert fields["_progress_total"] == 100

    def test_include_isoforms_false_omits_param(self):
        """include_isoforms=False does not add includeIsoform to URL."""
        session = _make_mock_session()
        resp = _make_mock_response(FASTA_ONE)
        op = InsertProteinsOperation()
        with patch.object(op._http, "get", return_value=resp) as mock_get:
            op.execute(
                session,
                {"search_criteria": "q", "compressed": False, "include_isoforms": False},
                emit=_noop_emit,
            )
        called_url = mock_get.call_args[0][0]
        assert "includeIsoform" not in called_url


# ---------------------------------------------------------------------------
# Integration test — full round-trip against real Postgres
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_insert_proteins_integration(postgres_url: str):
    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    op = InsertProteinsOperation()
    emit = _capturing_emit()

    with Session(engine, future=True) as session:
        with patch.object(op._http, "get", return_value=_make_mock_response(FASTA_TWO)):
            result = op.execute(
                session,
                {"search_criteria": "organism_id:9606", "compressed": False},
                emit=emit,
            )
            session.commit()

    assert result.result["proteins_inserted"] == 2
    assert result.result["sequences_inserted"] == 2

    # Idempotency: second run should update, not re-insert
    op2 = InsertProteinsOperation()
    with Session(engine, future=True) as session:
        with patch.object(op2._http, "get", return_value=_make_mock_response(FASTA_TWO)):
            result2 = op2.execute(
                session,
                {"search_criteria": "organism_id:9606", "compressed": False},
                emit=_noop_emit,
            )
            session.commit()

    assert result2.result["proteins_inserted"] == 0
    assert result2.result["sequences_reused"] == 2
