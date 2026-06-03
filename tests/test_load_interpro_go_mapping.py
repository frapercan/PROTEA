"""Unit tests for ``LoadInterProGoMappingOperation``.

No DB or network — HTTP fetch is patched, ``session`` is a MagicMock
that returns a fake RETURNING rowset so the inserted-count math is
exercised without touching Postgres.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from protea.core.contracts.operation import OperationResult
from protea.core.operations.load_interpro_go_mapping import (
    LoadInterProGoMappingOperation,
    LoadInterProGoMappingPayload,
)

_SAMPLE_LINES = """!date: 2024/01/15 09:00:00
!Mapping of InterPro entries to GO
!
InterPro:IPR000001 Kringle > GO:protein binding ; GO:0005515
InterPro:IPR000003 Retinoid X receptor/HNF4 > GO:DNA binding ; GO:0003677
InterPro:IPR000003 Retinoid X receptor/HNF4 > GO:nuclear receptor activity ; GO:0004879
malformed line with no arrow
InterPro:IPR000001 Kringle > GO:protein binding ; GO:0005515
"""


def _noop_emit(*_args, **_kwargs) -> None:
    return None


# ---------------------------------------------------------------------------
# Payload validation
# ---------------------------------------------------------------------------


class TestPayload:
    def test_valid_minimal(self) -> None:
        p = LoadInterProGoMappingPayload.model_validate({"source_version": "InterPro-104.0"})
        assert p.source_version == "InterPro-104.0"
        assert p.mapping_url.startswith("https://")
        assert p.evidence == "IEA"
        assert p.timeout_seconds == 120

    def test_overrides(self) -> None:
        p = LoadInterProGoMappingPayload.model_validate(
            {
                "source_version": "v1",
                "mapping_url": "https://example.org/i2g",
                "evidence": "CURATED",
                "timeout_seconds": 30,
                "chunk_size": 250,
            }
        )
        assert p.mapping_url == "https://example.org/i2g"
        assert p.evidence == "CURATED"
        assert p.timeout_seconds == 30
        assert p.chunk_size == 250

    def test_missing_version_raises(self) -> None:
        with pytest.raises(ValueError):
            LoadInterProGoMappingPayload.model_validate({})

    def test_empty_version_raises(self) -> None:
        with pytest.raises(ValueError):
            LoadInterProGoMappingPayload.model_validate({"source_version": "  "})

    def test_zero_timeout_raises(self) -> None:
        with pytest.raises(ValueError):
            LoadInterProGoMappingPayload.model_validate(
                {"source_version": "v1", "timeout_seconds": 0}
            )

    def test_strings_stripped(self) -> None:
        p = LoadInterProGoMappingPayload.model_validate(
            {"source_version": "  v1  ", "mapping_url": "  https://x  "}
        )
        assert p.source_version == "v1"
        assert p.mapping_url == "https://x"


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------


class TestParse:
    def _op(self) -> LoadInterProGoMappingOperation:
        return LoadInterProGoMappingOperation()

    def _payload(self, **overrides) -> LoadInterProGoMappingPayload:
        kwargs: dict[str, object] = {"source_version": "test"}
        kwargs.update(overrides)
        return LoadInterProGoMappingPayload.model_validate(kwargs)

    def test_skips_comments_and_blanks(self) -> None:
        rows = list(self._op()._parse(_SAMPLE_LINES, self._payload()))
        # The two unique pairs from IPR000001 and IPR000003 (×2) survive.
        assert len(rows) == 3

    def test_extracts_pair_fields(self) -> None:
        rows = list(self._op()._parse(_SAMPLE_LINES, self._payload()))
        first = rows[0]
        assert first["ipr_accession"] == "IPR000001"
        assert first["go_id"] == "GO:0005515"
        assert first["evidence"] == "IEA"
        assert first["source_version"] == "test"

    def test_deduplicates_within_file(self) -> None:
        rows = list(self._op()._parse(_SAMPLE_LINES, self._payload()))
        keys = [(r["ipr_accession"], r["go_id"]) for r in rows]
        assert len(keys) == len(set(keys))

    def test_malformed_lines_silently_skipped(self) -> None:
        rows = list(
            self._op()._parse(
                "InterPro:IPR000001 missing arrow\n!comment\n\n",
                self._payload(),
            )
        )
        assert rows == []

    def test_custom_evidence_propagates(self) -> None:
        rows = list(self._op()._parse(_SAMPLE_LINES, self._payload(evidence="CURATED")))
        assert all(r["evidence"] == "CURATED" for r in rows)


# ---------------------------------------------------------------------------
# Upsert
# ---------------------------------------------------------------------------


class TestUpsert:
    def _op(self) -> LoadInterProGoMappingOperation:
        return LoadInterProGoMappingOperation()

    def _mock_session(self, inserted_per_call: list[int]) -> MagicMock:
        """Session whose execute().fetchall() returns N fake rows."""
        session = MagicMock()
        results = []
        for n in inserted_per_call:
            r = MagicMock()
            r.fetchall.return_value = [(i,) for i in range(n)]
            results.append(r)
        session.execute.side_effect = results
        return session

    def test_returns_inserted_count(self) -> None:
        rows = [
            {
                "ipr_accession": f"IPR00000{i}",
                "go_id": "GO:0005515",
                "evidence": "IEA",
                "source_version": "v1",
            }
            for i in range(3)
        ]
        session = self._mock_session([3])
        n = self._op()._upsert(session, rows, chunk_size=1000)
        assert n == 3
        session.execute.assert_called_once()

    def test_chunked(self) -> None:
        rows = [
            {
                "ipr_accession": f"IPR{i:06d}",
                "go_id": "GO:0005515",
                "evidence": "IEA",
                "source_version": "v1",
            }
            for i in range(5)
        ]
        session = self._mock_session([2, 2, 1])
        n = self._op()._upsert(session, rows, chunk_size=2)
        assert n == 5
        assert session.execute.call_count == 3

    def test_idempotent_zero_returning(self) -> None:
        """A second load returns zero rows from RETURNING (all conflicts)."""
        rows = [
            {
                "ipr_accession": "IPR000001",
                "go_id": "GO:0005515",
                "evidence": "IEA",
                "source_version": "v1",
            }
        ]
        session = self._mock_session([0])
        n = self._op()._upsert(session, rows, chunk_size=1000)
        assert n == 0


# ---------------------------------------------------------------------------
# execute() end-to-end with HTTP patched
# ---------------------------------------------------------------------------


class TestExecute:
    def _payload_dict(self) -> dict:
        return {"source_version": "InterPro-104.0"}

    @patch("protea.core.operations.load_interpro_go_mapping.requests.get")
    def test_happy_path(self, mock_get: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.text = _SAMPLE_LINES
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        session = MagicMock()
        # 3 unique parsed rows → returning fetchall returns 3 inserted rows.
        execute_result = MagicMock()
        execute_result.fetchall.return_value = [(0,), (1,), (2,)]
        session.execute.return_value = execute_result

        op = LoadInterProGoMappingOperation()
        result = op.execute(session, self._payload_dict(), emit=_noop_emit)

        assert isinstance(result, OperationResult)
        assert result.result["rows_parsed"] == 3
        assert result.result["rows_inserted"] == 3
        assert result.result["rows_skipped_duplicate"] == 0
        assert result.result["source_version"] == "InterPro-104.0"
        assert "elapsed_seconds" in result.result

    @patch("protea.core.operations.load_interpro_go_mapping.requests.get")
    def test_empty_file_returns_zero(self, mock_get: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.text = "!only comments here\n!nothing to parse\n"
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        session = MagicMock()
        op = LoadInterProGoMappingOperation()
        result = op.execute(session, self._payload_dict(), emit=_noop_emit)

        assert result.result["rows_parsed"] == 0
        assert result.result["rows_inserted"] == 0
        session.execute.assert_not_called()

    @patch("protea.core.operations.load_interpro_go_mapping.requests.get")
    def test_http_error_propagates(self, mock_get: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = RuntimeError("HTTP 503")
        mock_get.return_value = mock_resp

        session = MagicMock()
        op = LoadInterProGoMappingOperation()
        with pytest.raises(RuntimeError):
            op.execute(session, self._payload_dict(), emit=_noop_emit)


# ---------------------------------------------------------------------------
# Operation metadata + registry wiring
# ---------------------------------------------------------------------------


def test_summarize_payload_contains_release_and_filename() -> None:
    op = LoadInterProGoMappingOperation()
    out = op.summarize_payload(
        {
            "source_version": "InterPro-104.0",
            "mapping_url": "https://example.org/path/interpro2go",
        }
    )
    assert "release=InterPro-104.0" in out
    assert "interpro2go" in out


def test_summarize_payload_handles_empty() -> None:
    assert LoadInterProGoMappingOperation().summarize_payload({}) == ""


def test_registered_in_catalog() -> None:
    from protea.core.operation_catalog import build_operation_registry

    registry = build_operation_registry()
    assert registry.get("load_interpro_go_mapping") is not None
