"""
Unit tests for core contracts and simple operations.
No DB or network required.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from protea.core.contracts.operation import OperationResult
from protea.core.contracts.registry import OperationRegistry
from protea.core.operations.ping import PingOperation
from protea.core.utils import UniProtHttpMixin, chunks

# ---------------------------------------------------------------------------
# OperationRegistry
# ---------------------------------------------------------------------------

class TestOperationRegistry:
    def test_register_and_get(self):
        reg = OperationRegistry()
        op = PingOperation()
        reg.register(op)
        assert reg.get("ping") is op

    def test_duplicate_register_raises(self):
        reg = OperationRegistry()
        reg.register(PingOperation())
        with pytest.raises(ValueError, match="already registered"):
            reg.register(PingOperation())

    def test_get_unknown_raises(self):
        reg = OperationRegistry()
        with pytest.raises(KeyError, match="Unknown operation"):
            reg.get("does_not_exist")


# ---------------------------------------------------------------------------
# PingOperation
# ---------------------------------------------------------------------------

class TestPingOperation:
    def setup_method(self):
        self.op = PingOperation()

    def test_name(self):
        assert self.op.name == "ping"

    def test_execute_returns_operation_result(self):
        events = []

        def emit(event, message, fields, level):
            events.append(event)

        result = self.op.execute(session=None, payload={}, emit=emit)
        assert isinstance(result, OperationResult)
        assert result.result == {"ok": True}
        assert "ping.start" in events
        assert "ping.done" in events

    def test_execute_emits_info_level(self):
        levels = []

        def emit(event, message, fields, level):
            levels.append(level)

        self.op.execute(session=None, payload={}, emit=emit)
        assert all(lv == "info" for lv in levels)


# ---------------------------------------------------------------------------
# chunks()
# ---------------------------------------------------------------------------

class TestChunks:
    def test_even_split(self) -> None:
        result = list(chunks([1, 2, 3, 4], 2))
        assert result == [[1, 2], [3, 4]]

    def test_remainder(self) -> None:
        result = list(chunks([1, 2, 3, 4, 5], 2))
        assert result == [[1, 2], [3, 4], [5]]

    def test_chunk_larger_than_seq(self) -> None:
        result = list(chunks([1, 2], 10))
        assert result == [[1, 2]]

    def test_empty_seq(self) -> None:
        assert list(chunks([], 5)) == []


# ---------------------------------------------------------------------------
# UniProtHttpMixin
# ---------------------------------------------------------------------------

def _make_payload(max_retries=3, backoff_base=0.01, backoff_max=0.1, jitter=0.0):
    p = MagicMock()
    p.user_agent = "PROTEA/test"
    p.timeout_seconds = 5
    p.max_retries = max_retries
    p.backoff_base_seconds = backoff_base
    p.backoff_max_seconds = backoff_max
    p.jitter_seconds = jitter
    return p


class _ConcreteHttp(UniProtHttpMixin):
    def __init__(self):
        self._http_requests = 0
        self._http_retries = 0
        self._http = MagicMock()


_noop_emit = lambda *_: None


class TestUniProtHttpMixin:
    def _obj(self) -> _ConcreteHttp:
        return _ConcreteHttp()

    def test_returns_response_on_200(self) -> None:
        obj = self._obj()
        resp = MagicMock()
        resp.status_code = 200
        obj._http.get.return_value = resp
        result = obj._get_with_retries("http://x", _make_payload(), _noop_emit)
        assert result is resp

    def test_retries_on_429(self) -> None:
        obj = self._obj()
        bad = MagicMock(); bad.status_code = 429
        bad.headers = {}
        good = MagicMock(); good.status_code = 200
        obj._http.get.side_effect = [bad, good]
        with patch("protea.core.utils.time.sleep"):
            result = obj._get_with_retries("http://x", _make_payload(), _noop_emit)
        assert result is good
        assert obj._http_retries == 1

    def test_uses_retry_after_header(self) -> None:
        obj = self._obj()
        bad = MagicMock(); bad.status_code = 429
        bad.headers = {"Retry-After": "5"}
        good = MagicMock(); good.status_code = 200
        obj._http.get.side_effect = [bad, good]
        sleep_calls = []
        with patch("protea.core.utils.time.sleep", side_effect=sleep_calls.append):
            obj._get_with_retries("http://x", _make_payload(backoff_max=30.0), _noop_emit)
        assert len(sleep_calls) == 1
        assert sleep_calls[0] == pytest.approx(5.0)

    def test_raises_after_max_retries(self) -> None:
        obj = self._obj()
        bad = MagicMock(); bad.status_code = 503
        bad.headers = {}
        bad.raise_for_status.side_effect = requests.HTTPError("503")
        obj._http.get.return_value = bad
        with patch("protea.core.utils.time.sleep"):
            with pytest.raises(requests.HTTPError):
                obj._get_with_retries("http://x", _make_payload(max_retries=2), _noop_emit)

    def test_retries_on_network_exception(self) -> None:
        obj = self._obj()
        good = MagicMock(); good.status_code = 200
        obj._http.get.side_effect = [requests.ConnectionError("down"), good]
        with patch("protea.core.utils.time.sleep"):
            result = obj._get_with_retries("http://x", _make_payload(), _noop_emit)
        assert result is good

    def test_extract_next_cursor_present(self) -> None:
        obj = self._obj()
        header = '<https://rest.uniprot.org/uniprotkb/search?cursor=ABCD1234>; rel="next"'
        assert obj._extract_next_cursor(header) == "ABCD1234"

    def test_extract_next_cursor_absent(self) -> None:
        obj = self._obj()
        assert obj._extract_next_cursor("") is None
        assert obj._extract_next_cursor('<http://x>; rel="prev"') is None

    def test_extract_next_cursor_no_cursor_param(self) -> None:
        obj = self._obj()
        assert obj._extract_next_cursor('<http://x?page=2>; rel="next"') is None


# ---------------------------------------------------------------------------
# evidence_codes — normalize and is_experimental
# ---------------------------------------------------------------------------

from protea.core.evidence_codes import normalize, is_experimental, ECO_TO_CODE, EXPERIMENTAL


class TestNormalize:
    def test_go_code_passthrough(self):
        assert normalize("IDA") == "IDA"

    def test_eco_id_to_go_code(self):
        assert normalize("ECO:0000314") == "IDA"
        assert normalize("ECO:0000501") == "IEA"

    def test_unknown_code_passthrough(self):
        assert normalize("UNKNOWN_CODE") == "UNKNOWN_CODE"

    def test_all_eco_ids_resolve(self):
        for eco, expected in ECO_TO_CODE.items():
            assert normalize(eco) == expected


class TestIsExperimental:
    def test_experimental_go_code(self):
        for code in EXPERIMENTAL:
            assert is_experimental(code) is True

    def test_non_experimental_code(self):
        assert is_experimental("IEA") is False
        assert is_experimental("ISS") is False
        assert is_experimental("ND") is False

    def test_eco_experimental(self):
        # ECO:0000314 → IDA → experimental
        assert is_experimental("ECO:0000314") is True

    def test_eco_non_experimental(self):
        # ECO:0000501 → IEA → not experimental
        assert is_experimental("ECO:0000501") is False

    def test_unknown_code_not_experimental(self):
        assert is_experimental("BADCODE") is False


# ---------------------------------------------------------------------------
# RetryLaterError
# ---------------------------------------------------------------------------

from protea.core.contracts.operation import RetryLaterError


class TestRetryLaterError:
    def test_default_delay(self):
        err = RetryLaterError("GPU busy")
        assert err.delay_seconds == 60
        assert str(err) == "GPU busy"

    def test_custom_delay(self):
        err = RetryLaterError("busy", delay_seconds=120)
        assert err.delay_seconds == 120

    def test_is_exception(self):
        with pytest.raises(RetryLaterError):
            raise RetryLaterError("test")


# ---------------------------------------------------------------------------
# FetchUniProtMetadataOperation
# ---------------------------------------------------------------------------

import gzip
from io import BytesIO

from protea.core.operations.fetch_uniprot_metadata import (
    FetchUniProtMetadataOperation,
    FetchUniProtMetadataPayload,
)


def _noop_emit(*_):
    pass


def _make_tsv_content(rows: list[dict[str, str]], compressed: bool = True) -> bytes:
    """Build a TSV byte string (optionally gzipped) from a list of dicts."""
    if not rows:
        header = "Entry\tReviewed\tEntry Name\tOrganism\tGene Names\tLength"
        text = header + "\n"
    else:
        headers = list(rows[0].keys())
        lines = ["\t".join(headers)]
        for row in rows:
            lines.append("\t".join(row.get(h, "") for h in headers))
        text = "\n".join(lines) + "\n"

    raw = text.encode("utf-8")
    if compressed:
        buf = BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as f:
            f.write(raw)
        return buf.getvalue()
    return raw




class TestFetchUniProtMetadataExecute:
    def _make_op(self):
        op = FetchUniProtMetadataOperation()
        op._http = MagicMock()
        return op

    def test_execute_empty_page_continues(self):
        """Line 108: when rows is empty, continue (skip store)."""
        op = self._make_op()
        events = []

        def emit(event, message, fields, level):
            events.append(event)

        # Return one page with no data rows, then stop
        resp = MagicMock()
        resp.status_code = 200
        resp.headers = {"X-Total-Results": "0"}
        resp.content = _make_tsv_content([], compressed=True)
        op._http.get.return_value = resp

        session = MagicMock()
        payload = {"search_criteria": "organism_id:9606", "page_size": 10}

        result = op.execute(session, payload, emit=emit)
        assert result.result["rows"] == 0
        assert result.result["pages"] == 1

    def test_execute_total_limit_truncation(self):
        """Lines 110-113: when total_limit is set and rows exceed it, truncate."""
        op = self._make_op()

        # Build 5 rows
        rows = []
        for i in range(5):
            row = {"Entry": f"P0000{i}", "Reviewed": "reviewed"}
            # Add all FIELD_MAP headers as empty
            for header in FetchUniProtMetadataOperation.FIELD_MAP.values():
                row[header] = ""
            row["Entry Name"] = ""
            row["Organism"] = ""
            row["Gene Names"] = ""
            row["Length"] = ""
            rows.append(row)

        resp = MagicMock()
        resp.status_code = 200
        resp.headers = {"X-Total-Results": "5"}
        resp.content = _make_tsv_content(rows, compressed=True)
        op._http.get.return_value = resp

        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = []

        payload = {
            "search_criteria": "organism_id:9606",
            "page_size": 10,
            "total_limit": 3,
        }

        result = op.execute(session, payload, emit=_noop_emit)
        # Should only process 3 rows despite page having 5
        assert result.result["rows"] == 3

    def test_execute_total_limit_zero_after_truncation(self):
        """Line 113: if truncation results in empty rows, break."""
        op = self._make_op()

        rows = [{"Entry": "P00001"}]
        for header in FetchUniProtMetadataOperation.FIELD_MAP.values():
            rows[0][header] = ""
        rows[0].update({"Reviewed": "", "Entry Name": "", "Organism": "", "Gene Names": "", "Length": ""})

        # First page returns 1 row, second page returns 1 row
        resp1 = MagicMock()
        resp1.status_code = 200
        resp1.headers = {"X-Total-Results": "2", "link": '<http://next?cursor=ABC>; rel="next"'}
        resp1.content = _make_tsv_content(rows, compressed=True)

        resp2 = MagicMock()
        resp2.status_code = 200
        resp2.headers = {"X-Total-Results": "2"}
        rows2 = [{"Entry": "P00002"}]
        for header in FetchUniProtMetadataOperation.FIELD_MAP.values():
            rows2[0][header] = ""
        rows2[0].update({"Reviewed": "", "Entry Name": "", "Organism": "", "Gene Names": "", "Length": ""})
        resp2.content = _make_tsv_content(rows2, compressed=True)

        op._http.get.side_effect = [resp1, resp2]

        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = []

        payload = {
            "search_criteria": "organism_id:9606",
            "page_size": 1,
            "total_limit": 1,
        }

        result = op.execute(session, payload, emit=_noop_emit)
        # Should stop after first page (total_limit=1, first page gives 1 row)
        assert result.result["rows"] == 1

    def test_x_total_results_none_on_invalid_header(self):
        """Line 227: X-Total-Results header with invalid value."""
        op = self._make_op()

        resp = MagicMock()
        resp.status_code = 200
        resp.headers = {"X-Total-Results": "not-a-number"}
        resp.content = _make_tsv_content([], compressed=True)
        op._http.get.return_value = resp

        session = MagicMock()
        payload = {"search_criteria": "test", "page_size": 10}

        result = op.execute(session, payload, emit=_noop_emit)
        assert op._total_results is None

    def test_decode_response_uncompressed(self):
        """Line 241-242: uncompressed response decoding."""
        op = self._make_op()
        resp = MagicMock()
        resp.content = b"Entry\tReviewed\nP00001\treviewed\n"
        text = op._decode_response(resp, compressed=False)
        assert "P00001" in text

    def test_store_rows_empty_accession_skipped(self):
        """Line 275: rows with empty Entry are skipped."""
        op = self._make_op()
        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = []

        p = FetchUniProtMetadataPayload(
            search_criteria="test",
            update_protein_core=False,
        )

        rows = [{"Entry": "", "Absorption": "test"}]
        for header in FetchUniProtMetadataOperation.FIELD_MAP.values():
            if header not in rows[0]:
                rows[0][header] = ""

        touched, upserted = op._store_rows(session, rows, p, _noop_emit)
        assert touched == 0
        assert upserted == 0

    def test_store_rows_update_protein_core_fields(self):
        """Lines 296-328: update_protein_core fills in missing fields on Protein."""
        op = self._make_op()
        session = MagicMock()

        # No existing metadata
        session.query.return_value.filter.return_value.all.return_value = []

        # Create a mock protein with all None fields
        protein = MagicMock()
        protein.accession = "P12345"
        protein.reviewed = None
        protein.entry_name = None
        protein.organism = None
        protein.gene_name = None
        protein.length = None

        # Second query().filter().all() returns proteins
        call_count = [0]
        def query_side_effect(*args):
            result = MagicMock()
            call_count[0] += 1
            if call_count[0] <= 1:
                # First call: metadata lookup
                result.filter.return_value.all.return_value = []
            else:
                # Second call: protein lookup
                result.filter.return_value.all.return_value = [protein]
            return result
        session.query.side_effect = query_side_effect

        p = FetchUniProtMetadataPayload(
            search_criteria="test",
            update_protein_core=True,
        )

        row = {"Entry": "P12345", "Reviewed": "reviewed", "Entry Name": "TEST_HUMAN",
               "Organism": "Homo sapiens", "Gene Names": "TEST GENE2", "Length": "500"}
        for header in FetchUniProtMetadataOperation.FIELD_MAP.values():
            row.setdefault(header, "")

        touched, upserted = op._store_rows(session, [row], p, _noop_emit)
        assert protein.reviewed is True
        assert protein.entry_name == "TEST_HUMAN"
        assert protein.organism == "Homo sapiens"
        assert protein.gene_name == "TEST"
        assert protein.length == 500
        assert touched == 1

    def test_store_rows_unreviewed_protein(self):
        """Lines 303-305: reviewed == 'unreviewed' sets pr.reviewed = False."""
        op = self._make_op()
        session = MagicMock()

        protein = MagicMock()
        protein.accession = "Q99999"
        protein.reviewed = None
        protein.entry_name = None
        protein.organism = None
        protein.gene_name = None
        protein.length = None

        call_count = [0]
        def query_side_effect(*args):
            result = MagicMock()
            call_count[0] += 1
            if call_count[0] <= 1:
                result.filter.return_value.all.return_value = []
            else:
                result.filter.return_value.all.return_value = [protein]
            return result
        session.query.side_effect = query_side_effect

        p = FetchUniProtMetadataPayload(
            search_criteria="test",
            update_protein_core=True,
        )

        row = {"Entry": "Q99999", "Reviewed": "unreviewed"}
        for header in FetchUniProtMetadataOperation.FIELD_MAP.values():
            row.setdefault(header, "")
        row.setdefault("Entry Name", "")
        row.setdefault("Organism", "")
        row.setdefault("Gene Names", "")
        row.setdefault("Length", "")

        touched, _ = op._store_rows(session, [row], p, _noop_emit)
        assert protein.reviewed is False
        assert touched == 1

    def test_store_rows_protein_not_in_db(self):
        """Lines 294-295: protein not found in protein_map, no core update."""
        op = self._make_op()
        session = MagicMock()

        call_count = [0]
        def query_side_effect(*args):
            result = MagicMock()
            call_count[0] += 1
            if call_count[0] <= 1:
                result.filter.return_value.all.return_value = []
            else:
                result.filter.return_value.all.return_value = []  # No proteins
            return result
        session.query.side_effect = query_side_effect

        p = FetchUniProtMetadataPayload(
            search_criteria="test",
            update_protein_core=True,
        )

        row = {"Entry": "UNKNOWN1", "Reviewed": "reviewed"}
        for header in FetchUniProtMetadataOperation.FIELD_MAP.values():
            row.setdefault(header, "")
        row.setdefault("Entry Name", "")
        row.setdefault("Organism", "")
        row.setdefault("Gene Names", "")
        row.setdefault("Length", "")

        touched, upserted = op._store_rows(session, [row], p, _noop_emit)
        assert touched == 0
        # Still upserted metadata
        assert upserted == 1

    def test_load_existing_metadata_chunks(self):
        """Line 346: _load_existing_metadata returns existing metadata by canonical."""
        op = self._make_op()
        session = MagicMock()

        m1 = MagicMock()
        m1.canonical_accession = "P12345"
        session.query.return_value.filter.return_value.all.return_value = [m1]

        result = op._load_existing_metadata(session, ["P12345"], chunk_size=10)
        assert "P12345" in result
        assert result["P12345"] is m1
