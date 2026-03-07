"""
Tests for InsertProteinsOperation.
Unit tests use mocked HTTP + mocked session (no DB, no network).
Integration test uses a real Postgres via --with-postgres.
"""
from __future__ import annotations

from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from protea.core.operations.insert_proteins import (
    InsertProteinsOperation,
    InsertProteinsPayload,
)
from protea.infrastructure.orm.base import Base
import protea.infrastructure.orm.models  # noqa: F401


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
# Unit tests — InsertProteinsPayload.from_dict
# ---------------------------------------------------------------------------

class TestInsertProteinsPayloadFromDict:
    def test_minimal_valid(self):
        p = InsertProteinsPayload.from_dict({"search_criteria": "organism_id:9606"})
        assert p.search_criteria == "organism_id:9606"
        assert p.page_size == 500
        assert p.include_isoforms is True
        assert p.total_limit is None

    def test_all_fields(self):
        p = InsertProteinsPayload.from_dict({
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
            InsertProteinsPayload.from_dict({})

    def test_empty_search_criteria_raises(self):
        with pytest.raises(ValueError, match="search_criteria"):
            InsertProteinsPayload.from_dict({"search_criteria": "  "})

    def test_invalid_page_size_raises(self):
        with pytest.raises(ValueError, match="page_size"):
            InsertProteinsPayload.from_dict({"search_criteria": "q", "page_size": -1})

    def test_invalid_total_limit_raises(self):
        with pytest.raises(ValueError, match="total_limit"):
            InsertProteinsPayload.from_dict({"search_criteria": "q", "total_limit": 0})

    def test_null_total_limit_allowed(self):
        p = InsertProteinsPayload.from_dict({"search_criteria": "q", "total_limit": None})
        assert p.total_limit is None

    def test_search_criteria_stripped(self):
        p = InsertProteinsPayload.from_dict({"search_criteria": "  q  "})
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
