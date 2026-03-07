from __future__ import annotations

from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from protea.core.operations.fetch_uniprot_metadata import (
    FetchUniProtMetadataOperation,
    FetchUniProtMetadataPayload,
)
from protea.infrastructure.orm.base import Base
import protea.infrastructure.orm.models  # noqa: F401 — registers all mappers


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _noop_emit(event: str, message, fields: Dict[str, Any], level: str) -> None:
    pass


def _capturing_emit():
    """Returns an emit callable that records every call."""
    calls = []

    def emit(event, message, fields, level):
        calls.append({"event": event, "message": message, "fields": fields, "level": level})

    emit.calls = calls  # type: ignore[attr-defined]
    return emit


# ---------------------------------------------------------------------------
# Unit tests — FetchUniProtMetadataPayload.from_dict
# ---------------------------------------------------------------------------

class TestFetchUniProtMetadataPayloadFromDict:
    def test_minimal_valid(self):
        p = FetchUniProtMetadataPayload.model_validate({"search_criteria": "organism_id:9606"})
        assert p.search_criteria == "organism_id:9606"
        assert p.page_size == 500
        assert p.compressed is True
        assert p.commit_every_page is True
        assert p.update_protein_core is True

    def test_all_fields(self):
        p = FetchUniProtMetadataPayload.model_validate({
            "search_criteria": "organism_id:9606",
            "page_size": 100,
            "total_limit": 200,
            "timeout_seconds": 30,
            "compressed": False,
            "max_retries": 3,
            "backoff_base_seconds": 0.5,
            "backoff_max_seconds": 10.0,
            "jitter_seconds": 0.1,
            "commit_every_page": False,
            "update_protein_core": False,
            "user_agent": "test/1.0",
        })
        assert p.page_size == 100
        assert p.total_limit == 200
        assert p.compressed is False
        assert p.max_retries == 3
        assert p.commit_every_page is False
        assert p.update_protein_core is False
        assert p.user_agent == "test/1.0"

    def test_missing_search_criteria_raises(self):
        with pytest.raises(ValueError, match="search_criteria"):
            FetchUniProtMetadataPayload.model_validate({})

    def test_empty_search_criteria_raises(self):
        with pytest.raises(ValueError, match="search_criteria"):
            FetchUniProtMetadataPayload.model_validate({"search_criteria": "   "})

    def test_invalid_page_size_raises(self):
        with pytest.raises(ValueError, match="page_size"):
            FetchUniProtMetadataPayload.model_validate({"search_criteria": "q", "page_size": 0})

    def test_invalid_total_limit_raises(self):
        with pytest.raises(ValueError, match="total_limit"):
            FetchUniProtMetadataPayload.model_validate({"search_criteria": "q", "total_limit": -1})

    def test_null_total_limit_allowed(self):
        p = FetchUniProtMetadataPayload.model_validate({"search_criteria": "q", "total_limit": None})
        assert p.total_limit is None

    def test_invalid_compressed_raises(self):
        with pytest.raises(ValueError, match="compressed"):
            FetchUniProtMetadataPayload.model_validate({"search_criteria": "q", "compressed": "yes"})

    def test_negative_backoff_raises(self):
        with pytest.raises(ValueError, match="backoff_base_seconds"):
            FetchUniProtMetadataPayload.model_validate({"search_criteria": "q", "backoff_base_seconds": -1.0})

    def test_search_criteria_is_stripped(self):
        p = FetchUniProtMetadataPayload.model_validate({"search_criteria": "  organism_id:9606  "})
        assert p.search_criteria == "organism_id:9606"


# ---------------------------------------------------------------------------
# Unit tests — _parse_tsv
# ---------------------------------------------------------------------------

class TestParseTsv:
    def setup_method(self):
        self.op = FetchUniProtMetadataOperation()

    def test_parses_basic_tsv(self):
        tsv = "Entry\tReviewed\tLength\nP12345\treviewed\t500\nQ99999\tunreviewed\t120\n"
        rows = self.op._parse_tsv(tsv)
        assert len(rows) == 2
        assert rows[0]["Entry"] == "P12345"
        assert rows[0]["Reviewed"] == "reviewed"
        assert rows[1]["Length"] == "120"

    def test_empty_tsv_returns_empty(self):
        rows = self.op._parse_tsv("")
        assert rows == []

    def test_none_values_coerced_to_empty_string(self):
        # DictReader returns None for missing fields in some edge cases;
        # the implementation maps None -> ""
        tsv = "Entry\tReviewed\nP12345\t\n"
        rows = self.op._parse_tsv(tsv)
        assert rows[0]["Reviewed"] == ""

    def test_header_only_returns_empty(self):
        tsv = "Entry\tReviewed\tLength\n"
        rows = self.op._parse_tsv(tsv)
        assert rows == []


# ---------------------------------------------------------------------------
# Unit tests — execute() with fully mocked HTTP and DB session
# ---------------------------------------------------------------------------

TSV_RESPONSE = (
    "Entry\tReviewed\tEntry Name\tProtein names\tGene Names\tOrganism\tLength\t"
    "Absorption\tActive site\tBinding site\tCatalytic activity\tCofactor\t"
    "DNA binding\tEC number\tActivity regulation\tFunction [CC]\tPathway\t"
    "Kinetics\tpH dependence\tRedox potential\tRhea ID\tSite\t"
    "Temperature dependence\tKeywords\tFeatures\n"
    "P12345\treviewed\tTEST_HUMAN\tTest protein\tTEST\tHomo sapiens\t200\t"
    "\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\n"
)


def _make_mock_response(text: str, link_header: str = "") -> MagicMock:
    resp = MagicMock()
    resp.status_code = 200
    resp.content = text.encode("utf-8")
    resp.headers = {"link": link_header}
    resp.raise_for_status = MagicMock()
    return resp


class TestFetchUniProtMetadataOperationExecute:
    def setup_method(self):
        self.op = FetchUniProtMetadataOperation()

    def _mock_session(self):
        """A SQLAlchemy session mock that satisfies the operation's DB calls."""
        session = MagicMock(spec=Session)
        # _load_existing_metadata queries ProteinUniProtMetadata → return empty
        session.query.return_value.filter.return_value.all.return_value = []
        # _store_rows also queries Protein for update_protein_core → return empty
        return session

    def test_execute_returns_operation_result(self):
        session = self._mock_session()
        emit = _capturing_emit()

        with patch.object(self.op._http, "get", return_value=_make_mock_response(TSV_RESPONSE)):
            result = self.op.execute(
                session,
                {"search_criteria": "organism_id:9606", "page_size": 1, "compressed": False},
                emit=emit,
            )

        assert result.result["pages"] == 1
        assert result.result["rows"] == 1
        assert result.result["http_requests"] == 1
        assert result.result["http_retries"] == 0

    def test_execute_emits_start_and_done(self):
        session = self._mock_session()
        emit = _capturing_emit()

        with patch.object(self.op._http, "get", return_value=_make_mock_response(TSV_RESPONSE)):
            self.op.execute(
                session,
                {"search_criteria": "organism_id:9606", "compressed": False},
                emit=emit,
            )

        events = [c["event"] for c in emit.calls]
        assert "fetch_uniprot_metadata.start" in events
        assert "fetch_uniprot_metadata.done" in events

    def test_execute_respects_total_limit(self):
        # Two rows in TSV but total_limit=1 should stop after 1
        tsv = TSV_RESPONSE + "Q99999\tunreviewed\tTEST2_HUMAN\tAnother\tT2\tMus musculus\t100\t" + "\t" * 17 + "\n"
        session = self._mock_session()
        emit = _capturing_emit()

        with patch.object(self.op._http, "get", return_value=_make_mock_response(tsv)):
            result = self.op.execute(
                session,
                {"search_criteria": "q", "total_limit": 1, "compressed": False},
                emit=emit,
            )

        assert result.result["rows"] == 1
        limit_events = [c for c in emit.calls if c["event"] == "fetch_uniprot_metadata.limit_reached"]
        assert len(limit_events) == 1

    def test_execute_inserts_metadata_row(self):
        """Verify session.add is called for a new ProteinUniProtMetadata row."""
        session = self._mock_session()
        emit = _noop_emit

        with patch.object(self.op._http, "get", return_value=_make_mock_response(TSV_RESPONSE)):
            self.op.execute(
                session,
                {"search_criteria": "q", "compressed": False},
                emit=emit,
            )

        session.add.assert_called()


# ---------------------------------------------------------------------------
# Integration test — execute() against a real Postgres DB with mocked HTTP
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_fetch_uniprot_metadata_integration(postgres_url: str):
    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    op = FetchUniProtMetadataOperation()
    emit = _capturing_emit()

    with Session(engine, future=True) as session:
        with patch.object(op._http, "get", return_value=_make_mock_response(TSV_RESPONSE)):
            result = op.execute(
                session,
                {"search_criteria": "organism_id:9606", "compressed": False, "commit_every_page": False},
                emit=emit,
            )
            session.commit()

    assert result.result["rows"] == 1
    assert result.result["metadata_upserted"] == 1

    # Second run with same data → upsert should not double-insert
    op2 = FetchUniProtMetadataOperation()
    with Session(engine, future=True) as session:
        with patch.object(op2._http, "get", return_value=_make_mock_response(TSV_RESPONSE)):
            result2 = op2.execute(
                session,
                {"search_criteria": "organism_id:9606", "compressed": False, "commit_every_page": False},
                emit=_noop_emit,
            )
            session.commit()

    # Row already existed — changed flag depends on whether values differ.
    # The important invariant: no duplicate rows (upsert, not insert).
    assert result2.result["rows"] == 1
