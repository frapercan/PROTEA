from __future__ import annotations

import uuid
from unittest.mock import MagicMock, patch

import pytest
import requests

from protea.core.operations.load_quickgo_annotations import (
    LoadQuickGOAnnotationsOperation,
    LoadQuickGOAnnotationsPayload,
)

_noop_emit = lambda *_: None  # noqa: E731
_SNAPSHOT_ID = str(uuid.uuid4())

# Simulates a QuickGO TSV response (header + 3 rows)
_QUICKGO_ROWS = [
    {
        "GENE PRODUCT DB": "UniProtKB",
        "GENE PRODUCT ID": "P12345",
        "SYMBOL": "GENE1",
        "QUALIFIER": "enables",
        "GO TERM": "GO:0003824",
        "GO ASPECT": "F",
        "ECO ID": "ECO:0000314",
        "REFERENCE": "PMID:123",
        "WITH/FROM": "",
        "TAXON ID": "9606",
        "ASSIGNED BY": "UniProt",
        "ANNOTATION EXTENSION": "",
        "DATE": "20240101",
    },
    {
        "GENE PRODUCT DB": "UniProtKB",
        "GENE PRODUCT ID": "Q67890",
        "SYMBOL": "GENE2",
        "QUALIFIER": "involved_in",
        "GO TERM": "GO:0008150",
        "GO ASPECT": "P",
        "ECO ID": "ECO:0000501",
        "REFERENCE": "PMID:456",
        "WITH/FROM": "",
        "TAXON ID": "9606",
        "ASSIGNED BY": "UniProt",
        "ANNOTATION EXTENSION": "",
        "DATE": "20240101",
    },
    {
        "GENE PRODUCT DB": "UniProtKB",
        "GENE PRODUCT ID": "XXXXXX",
        "SYMBOL": "UNKNOWN",
        "QUALIFIER": "enables",
        "GO TERM": "GO:0003824",
        "GO ASPECT": "F",
        "ECO ID": "ECO:0000314",
        "REFERENCE": "PMID:789",
        "WITH/FROM": "",
        "TAXON ID": "9606",
        "ASSIGNED BY": "UniProt",
        "ANNOTATION EXTENSION": "",
        "DATE": "20240101",
    },
]


class TestLoadQuickGOAnnotationsPayload:
    def test_valid_minimal(self) -> None:
        p = LoadQuickGOAnnotationsPayload.model_validate({
            "ontology_snapshot_id": _SNAPSHOT_ID,
            "source_version": "2026-01-11",
        })
        assert p.eco_mapping_url is None
        assert p.page_size == 10000

    def test_missing_snapshot_raises(self) -> None:
        with pytest.raises(ValueError):
            LoadQuickGOAnnotationsPayload.model_validate({"source_version": "2026-01"})

    def test_empty_source_version_raises(self) -> None:
        with pytest.raises(ValueError):
            LoadQuickGOAnnotationsPayload.model_validate({
                "ontology_snapshot_id": _SNAPSHOT_ID,
                "source_version": "",
            })


class TestStoreBuffer:
    def _op(self) -> LoadQuickGOAnnotationsOperation:
        return LoadQuickGOAnnotationsOperation()

    def test_skips_unknown_accession(self) -> None:
        op = self._op()
        session = MagicMock()
        inserted, skipped = op._store_buffer(
            session, _QUICKGO_ROWS, uuid.UUID(_SNAPSHOT_ID),
            valid_accessions={"P12345"},
            go_term_map={"GO:0003824": 1, "GO:0008150": 2},
            eco_map={},
        )
        assert inserted == 1
        assert skipped == 2

    def test_skips_unknown_go_term(self) -> None:
        op = self._op()
        session = MagicMock()
        inserted, skipped = op._store_buffer(
            session, _QUICKGO_ROWS, uuid.UUID(_SNAPSHOT_ID),
            valid_accessions={"P12345", "Q67890", "XXXXXX"},
            go_term_map={},
            eco_map={},
        )
        assert inserted == 0
        assert skipped == 3

    def test_inserts_all_valid(self) -> None:
        op = self._op()
        session = MagicMock()
        inserted, skipped = op._store_buffer(
            session, _QUICKGO_ROWS, uuid.UUID(_SNAPSHOT_ID),
            valid_accessions={"P12345", "Q67890", "XXXXXX"},
            go_term_map={"GO:0003824": 1, "GO:0008150": 2},
            eco_map={},
        )
        assert inserted == 3
        assert skipped == 0
        session.execute.assert_called_once()

    def test_eco_mapping_applied(self) -> None:
        op = self._op()
        session = MagicMock()
        eco_map = {"ECO:0000314": "IDA", "ECO:0000501": "IEA"}
        inserted, _ = op._store_buffer(
            session, _QUICKGO_ROWS[:1], uuid.UUID(_SNAPSHOT_ID),
            valid_accessions={"P12345"},
            go_term_map={"GO:0003824": 1},
            eco_map=eco_map,
        )
        assert inserted == 1
        call_stmt = session.execute.call_args[0][0]
        from sqlalchemy.dialects.postgresql import dialect as pg_dialect
        compiled = call_stmt.compile(dialect=pg_dialect())
        assert compiled.params["evidence_code_m0"] == "IDA"

    def test_raw_eco_stored_when_no_mapping(self) -> None:
        op = self._op()
        session = MagicMock()
        inserted, _ = op._store_buffer(
            session, _QUICKGO_ROWS[:1], uuid.UUID(_SNAPSHOT_ID),
            valid_accessions={"P12345"},
            go_term_map={"GO:0003824": 1},
            eco_map={},
        )
        assert inserted == 1
        call_stmt = session.execute.call_args[0][0]
        from sqlalchemy.dialects.postgresql import dialect as pg_dialect
        compiled = call_stmt.compile(dialect=pg_dialect())
        assert compiled.params["evidence_code_m0"] == "ECO:0000314"

    def test_empty_eco_id_becomes_none(self) -> None:
        op = self._op()
        session = MagicMock()
        row = dict(_QUICKGO_ROWS[0])
        row["ECO ID"] = ""
        inserted, _ = op._store_buffer(
            session, [row], uuid.UUID(_SNAPSHOT_ID),
            valid_accessions={"P12345"},
            go_term_map={"GO:0003824": 1},
            eco_map={},
        )
        assert inserted == 1

    def test_empty_accession_skipped(self) -> None:
        op = self._op()
        session = MagicMock()
        row = dict(_QUICKGO_ROWS[0])
        row["GENE PRODUCT ID"] = "  "
        inserted, skipped = op._store_buffer(
            session, [row], uuid.UUID(_SNAPSHOT_ID),
            valid_accessions={"P12345"},
            go_term_map={"GO:0003824": 1},
            eco_map={},
        )
        assert inserted == 0
        assert skipped == 1

    def test_chunked_insert_large_buffer(self) -> None:
        """When to_add > 5000, session.execute is called multiple times."""
        op = self._op()
        session = MagicMock()
        records = [dict(_QUICKGO_ROWS[0])] * 5001
        inserted, skipped = op._store_buffer(
            session, records, uuid.UUID(_SNAPSHOT_ID),
            valid_accessions={"P12345"},
            go_term_map={"GO:0003824": 1},
            eco_map={},
        )
        assert inserted == 5001
        assert skipped == 0
        assert session.execute.call_count == 2


# ---------------------------------------------------------------------------
# _load_accessions
# ---------------------------------------------------------------------------

class TestLoadAccessions:
    def test_returns_canonical_and_protein_sets(self) -> None:
        op = LoadQuickGOAnnotationsOperation()
        session = MagicMock()
        session.scalars.side_effect = [
            iter({"P12345", "Q99999"}),
            iter({"P12345", "P12345-2", "Q99999"}),
        ]
        events: list[str] = []
        emit = lambda event, msg, fields, level: events.append(event)

        canon, prots = op._load_accessions(session, emit)
        assert canon == {"P12345", "Q99999"}
        assert prots == {"P12345", "P12345-2", "Q99999"}
        assert "load_quickgo_annotations.load_accessions_start" in events
        assert "load_quickgo_annotations.load_accessions_done" in events

    def test_emits_counts(self) -> None:
        op = LoadQuickGOAnnotationsOperation()
        session = MagicMock()
        session.scalars.side_effect = [iter({"A", "B"}), iter({"A", "B", "C"})]
        fields_log: list[dict] = []
        emit = lambda event, msg, fields, level: fields_log.append(fields)

        op._load_accessions(session, emit)
        done_fields = fields_log[-1]
        assert done_fields["canonical_accessions"] == 2
        assert done_fields["protein_accessions"] == 3


# ---------------------------------------------------------------------------
# _load_go_term_map
# ---------------------------------------------------------------------------

class TestLoadGoTermMap:
    def test_returns_mapping(self) -> None:
        op = LoadQuickGOAnnotationsOperation()
        session = MagicMock()
        sid = uuid.uuid4()
        query_mock = MagicMock()
        query_mock.filter.return_value.all.return_value = [
            ("GO:0005634", 1), ("GO:0008150", 2),
        ]
        session.query.return_value = query_mock

        events: list[str] = []
        emit = lambda event, msg, fields, level: events.append(event)

        result = op._load_go_term_map(session, sid, emit)
        assert result == {"GO:0005634": 1, "GO:0008150": 2}
        assert "load_quickgo_annotations.load_go_terms_start" in events
        assert "load_quickgo_annotations.load_go_terms_done" in events

    def test_empty_terms(self) -> None:
        op = LoadQuickGOAnnotationsOperation()
        session = MagicMock()
        query_mock = MagicMock()
        query_mock.filter.return_value.all.return_value = []
        session.query.return_value = query_mock

        result = op._load_go_term_map(session, uuid.uuid4(), _noop_emit)
        assert result == {}


# ---------------------------------------------------------------------------
# _load_eco_mapping
# ---------------------------------------------------------------------------

class TestLoadEcoMapping:
    def test_no_url_returns_empty(self) -> None:
        op = LoadQuickGOAnnotationsOperation()
        p = LoadQuickGOAnnotationsPayload.model_validate({
            "ontology_snapshot_id": _SNAPSHOT_ID,
            "source_version": "v1",
        })
        assert op._load_eco_mapping(p, _noop_emit) == {}

    @patch("protea.core.operations.load_quickgo_annotations.requests.get")
    def test_parses_mapping_file(self, mock_get) -> None:
        resp = MagicMock()
        resp.text = "ECO:0000314 IDA\nECO:0000501 IEA\n# comment\nbadline\n"
        resp.raise_for_status = MagicMock()
        mock_get.return_value = resp

        op = LoadQuickGOAnnotationsOperation()
        p = LoadQuickGOAnnotationsPayload.model_validate({
            "ontology_snapshot_id": _SNAPSHOT_ID,
            "source_version": "v1",
            "eco_mapping_url": "https://eco.test/map.txt",
        })
        result = op._load_eco_mapping(p, _noop_emit)
        assert result == {"ECO:0000314": "IDA", "ECO:0000501": "IEA"}

    @patch("protea.core.operations.load_quickgo_annotations.requests.get")
    def test_http_error_raises(self, mock_get) -> None:
        resp = MagicMock()
        resp.raise_for_status.side_effect = requests.HTTPError("404")
        mock_get.return_value = resp

        op = LoadQuickGOAnnotationsOperation()
        p = LoadQuickGOAnnotationsPayload.model_validate({
            "ontology_snapshot_id": _SNAPSHOT_ID,
            "source_version": "v1",
            "eco_mapping_url": "https://eco.test/bad",
        })
        with pytest.raises(requests.HTTPError):
            op._load_eco_mapping(p, _noop_emit)

    @patch("protea.core.operations.load_quickgo_annotations.requests.get")
    def test_emits_start_and_done(self, mock_get) -> None:
        resp = MagicMock()
        resp.text = "ECO:0000314 IDA\n"
        resp.raise_for_status = MagicMock()
        mock_get.return_value = resp

        op = LoadQuickGOAnnotationsOperation()
        p = LoadQuickGOAnnotationsPayload.model_validate({
            "ontology_snapshot_id": _SNAPSHOT_ID,
            "source_version": "v1",
            "eco_mapping_url": "https://eco.test/map.txt",
        })
        events: list[str] = []
        emit = lambda event, msg, fields, level: events.append(event)
        op._load_eco_mapping(p, emit)
        assert "load_quickgo_annotations.eco_mapping_start" in events
        assert "load_quickgo_annotations.eco_mapping_done" in events

    @patch("protea.core.operations.load_quickgo_annotations.requests.get")
    def test_ignores_non_eco_lines(self, mock_get) -> None:
        resp = MagicMock()
        resp.text = "ECO:0000314 IDA\nNOT_ECO stuff\n  \nECO:0000501 IEA\n"
        resp.raise_for_status = MagicMock()
        mock_get.return_value = resp

        op = LoadQuickGOAnnotationsOperation()
        p = LoadQuickGOAnnotationsPayload.model_validate({
            "ontology_snapshot_id": _SNAPSHOT_ID,
            "source_version": "v1",
            "eco_mapping_url": "https://eco.test/map.txt",
        })
        result = op._load_eco_mapping(p, _noop_emit)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# _fetch_quickgo_page — TSV stream parsing
# ---------------------------------------------------------------------------

import io as _io

QUICKGO_HEADER_LINE = (
    "GENE PRODUCT ID\tGO TERM\tQUALIFIER\tECO ID\tREFERENCE\tWITH/FROM\tASSIGNED BY\tDATE"
)


def _tsv_row_str(
    accession: str = "P12345",
    go_term: str = "GO:0005634",
    qualifier: str = "enables",
    eco_id: str = "ECO:0000314",
    reference: str = "PMID:12345",
    with_from: str = "",
    assigned_by: str = "UniProt",
    date: str = "20240101",
) -> str:
    return f"{accession}\t{go_term}\t{qualifier}\t{eco_id}\t{reference}\t{with_from}\t{assigned_by}\t{date}"


def _make_tsv_text(*data_rows: str) -> str:
    return "\n".join([QUICKGO_HEADER_LINE] + list(data_rows)) + "\n"


def _make_stream_response(text: str, status_code: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = requests.HTTPError(f"{status_code}")
    raw = _io.BytesIO(text.encode("utf-8"))
    resp.raw = raw
    resp.raw.decode_content = True
    return resp


class TestFetchQuickgoPage:
    def _payload(self, **kw):
        return LoadQuickGOAnnotationsPayload.model_validate({
            "ontology_snapshot_id": _SNAPSHOT_ID,
            "source_version": "v1",
            **kw,
        })

    @patch("protea.core.operations.load_quickgo_annotations.requests.get")
    def test_parses_rows(self, mock_get) -> None:
        tsv = _make_tsv_text(
            _tsv_row_str("P12345", "GO:0005634"),
            _tsv_row_str("Q99999", "GO:0008150"),
        )
        mock_get.return_value = _make_stream_response(tsv)

        op = LoadQuickGOAnnotationsOperation()
        records = list(
            op._fetch_quickgo_page(self._payload(), _noop_emit, gp_ids=["P12345"], batch_index=0, total_batches=1)
        )
        assert len(records) == 2
        assert records[0]["GENE PRODUCT ID"] == "P12345"
        assert records[1]["GO TERM"] == "GO:0008150"

    @patch("protea.core.operations.load_quickgo_annotations.requests.get")
    def test_skips_empty_lines(self, mock_get) -> None:
        tsv = QUICKGO_HEADER_LINE + "\n\n" + _tsv_row_str() + "\n\n"
        mock_get.return_value = _make_stream_response(tsv)

        op = LoadQuickGOAnnotationsOperation()
        records = list(
            op._fetch_quickgo_page(self._payload(), _noop_emit, gp_ids=None, batch_index=0, total_batches=1)
        )
        assert len(records) == 1

    @patch("protea.core.operations.load_quickgo_annotations.requests.get")
    def test_skips_short_rows(self, mock_get) -> None:
        tsv = QUICKGO_HEADER_LINE + "\ntoo\tfew\n" + _tsv_row_str() + "\n"
        mock_get.return_value = _make_stream_response(tsv)

        op = LoadQuickGOAnnotationsOperation()
        records = list(
            op._fetch_quickgo_page(self._payload(), _noop_emit, gp_ids=None, batch_index=0, total_batches=1)
        )
        assert len(records) == 1

    @patch("protea.core.operations.load_quickgo_annotations.requests.get")
    def test_http_error_raises(self, mock_get) -> None:
        mock_get.return_value = _make_stream_response("", status_code=500)

        op = LoadQuickGOAnnotationsOperation()
        with pytest.raises(requests.HTTPError):
            list(
                op._fetch_quickgo_page(self._payload(), _noop_emit, gp_ids=None, batch_index=0, total_batches=1)
            )

    @patch("protea.core.operations.load_quickgo_annotations.requests.get")
    def test_sends_correct_params_with_gp_ids(self, mock_get) -> None:
        mock_get.return_value = _make_stream_response(_make_tsv_text())

        op = LoadQuickGOAnnotationsOperation()
        list(
            op._fetch_quickgo_page(self._payload(), _noop_emit, gp_ids=["P12345", "Q99999"], batch_index=0, total_batches=1)
        )
        _, kwargs = mock_get.call_args
        assert kwargs["params"]["geneProductId"] == "P12345,Q99999"
        assert kwargs["params"]["geneProductType"] == "protein"
        assert kwargs["headers"]["Accept"] == "text/tsv"
        assert kwargs["stream"] is True

    @patch("protea.core.operations.load_quickgo_annotations.requests.get")
    def test_no_gp_ids_omits_gene_product_param(self, mock_get) -> None:
        mock_get.return_value = _make_stream_response(_make_tsv_text())

        op = LoadQuickGOAnnotationsOperation()
        list(
            op._fetch_quickgo_page(self._payload(), _noop_emit, gp_ids=None, batch_index=0, total_batches=1)
        )
        _, kwargs = mock_get.call_args
        assert "geneProductId" not in kwargs["params"]

    @patch("protea.core.operations.load_quickgo_annotations.requests.get")
    def test_emits_download_start_with_progress(self, mock_get) -> None:
        mock_get.return_value = _make_stream_response(_make_tsv_text())
        events: list[tuple[str, dict]] = []
        emit = lambda event, msg, fields, level: events.append((event, fields))

        op = LoadQuickGOAnnotationsOperation()
        list(
            op._fetch_quickgo_page(self._payload(), emit, gp_ids=["X"], batch_index=2, total_batches=5)
        )
        start_events = [e for e in events if e[0] == "load_quickgo_annotations.download_start"]
        assert len(start_events) == 1
        assert start_events[0][1]["batch"] == 3
        assert start_events[0][1]["of"] == 5
        assert start_events[0][1]["_progress_current"] == 3
        assert start_events[0][1]["_progress_total"] == 5

    @patch("protea.core.operations.load_quickgo_annotations.requests.get")
    def test_header_only_yields_nothing(self, mock_get) -> None:
        tsv = QUICKGO_HEADER_LINE + "\n"
        mock_get.return_value = _make_stream_response(tsv)

        op = LoadQuickGOAnnotationsOperation()
        records = list(
            op._fetch_quickgo_page(self._payload(), _noop_emit, gp_ids=None, batch_index=0, total_batches=1)
        )
        assert records == []


# ---------------------------------------------------------------------------
# _stream_quickgo — batching logic
# ---------------------------------------------------------------------------

class TestStreamQuickgo:
    def _payload(self, **kw):
        return LoadQuickGOAnnotationsPayload.model_validate({
            "ontology_snapshot_id": _SNAPSHOT_ID,
            "source_version": "v1",
            **kw,
        })

    @patch("protea.core.operations.load_quickgo_annotations.requests.get")
    def test_batches_accessions(self, mock_get) -> None:
        mock_get.side_effect = lambda *a, **kw: _make_stream_response(_make_tsv_text())

        op = LoadQuickGOAnnotationsOperation()
        p = self._payload(gene_product_batch_size=2)
        list(op._stream_quickgo(p, _noop_emit, gene_product_ids=["A", "B", "C", "D", "E"]))
        assert mock_get.call_count == 3  # 2+2+1

    @patch("protea.core.operations.load_quickgo_annotations.requests.get")
    def test_no_ids_single_request(self, mock_get) -> None:
        mock_get.return_value = _make_stream_response(_make_tsv_text())

        op = LoadQuickGOAnnotationsOperation()
        p = self._payload(use_db_accessions=False)
        list(op._stream_quickgo(p, _noop_emit, gene_product_ids=None))
        assert mock_get.call_count == 1

    @patch("protea.core.operations.load_quickgo_annotations.requests.get")
    def test_emits_batching_event(self, mock_get) -> None:
        mock_get.side_effect = lambda *a, **kw: _make_stream_response(_make_tsv_text())

        events: list[tuple[str, dict]] = []
        emit = lambda event, msg, fields, level: events.append((event, fields))

        op = LoadQuickGOAnnotationsOperation()
        p = self._payload(gene_product_batch_size=2)
        list(op._stream_quickgo(p, emit, gene_product_ids=["A", "B", "C"]))
        batching = [e for e in events if e[0] == "load_quickgo_annotations.batching"]
        assert len(batching) == 1
        assert batching[0][1]["total_accessions"] == 3
        assert batching[0][1]["total_batches"] == 2
        assert batching[0][1]["batch_size"] == 2

    @patch("protea.core.operations.load_quickgo_annotations.requests.get")
    def test_yields_records_from_all_batches(self, mock_get) -> None:
        tsv = _make_tsv_text(_tsv_row_str("P12345"))
        mock_get.side_effect = lambda *a, **kw: _make_stream_response(tsv)

        op = LoadQuickGOAnnotationsOperation()
        p = self._payload(gene_product_batch_size=1)
        records = list(op._stream_quickgo(p, _noop_emit, gene_product_ids=["A", "B"]))
        # Each batch returns 1 record, 2 batches
        assert len(records) == 2


# ---------------------------------------------------------------------------
# Full execute flow
# ---------------------------------------------------------------------------

def _mock_session(
    canonical_accessions: set[str] | None = None,
    protein_accessions: set[str] | None = None,
    go_terms: list[tuple[str, int]] | None = None,
    snapshot_exists: bool = True,
) -> MagicMock:
    session = MagicMock()
    if snapshot_exists:
        session.get.return_value = MagicMock()
    else:
        session.get.return_value = None

    canon = canonical_accessions if canonical_accessions is not None else {"P12345"}
    prots = protein_accessions if protein_accessions is not None else {"P12345"}
    session.scalars.side_effect = [iter(canon), iter(prots)]

    terms = go_terms or [("GO:0003824", 1), ("GO:0008150", 2)]
    query_mock = MagicMock()
    query_mock.filter.return_value.all.return_value = terms
    session.query.return_value = query_mock

    def _set_id(obj):
        obj.id = uuid.uuid4()
    session.add.side_effect = _set_id

    return session


def _base_payload(**overrides) -> dict:
    d = {
        "ontology_snapshot_id": _SNAPSHOT_ID,
        "source_version": "2024-01-01",
        "quickgo_base_url": "https://quickgo.test/annotation/downloadSearch",
        "use_db_accessions": True,
        "eco_mapping_url": None,
        "page_size": 100,
        "timeout_seconds": 10,
        "commit_every_page": False,
        "gene_product_batch_size": 200,
    }
    d.update(overrides)
    return d


class TestExecute:
    def test_snapshot_not_found_raises(self) -> None:
        session = _mock_session(snapshot_exists=False)
        op = LoadQuickGOAnnotationsOperation()
        with pytest.raises(ValueError, match="not found"):
            op.execute(session, _base_payload(), emit=_noop_emit)

    def test_no_proteins_returns_zero(self) -> None:
        session = _mock_session(canonical_accessions=set())
        session.scalars.side_effect = [iter(set()), iter(set())]
        op = LoadQuickGOAnnotationsOperation()
        events: list[str] = []
        emit = lambda event, msg, fields, level: events.append(event)
        result = op.execute(session, _base_payload(), emit=emit)
        assert result.result["annotations_inserted"] == 0
        assert "load_quickgo_annotations.no_proteins" in events

    @patch("protea.core.operations.load_quickgo_annotations.requests.get")
    def test_full_run_inserts_and_skips(self, mock_get) -> None:
        tsv = _make_tsv_text(
            _tsv_row_str("P12345", "GO:0003824"),
            _tsv_row_str("UNKNOWN", "GO:0003824"),
            _tsv_row_str("P12345", "GO:9999999"),
        )
        mock_get.return_value = _make_stream_response(tsv)

        session = _mock_session(
            canonical_accessions={"P12345"},
            protein_accessions={"P12345"},
            go_terms=[("GO:0003824", 1)],
        )

        events: list[str] = []
        emit = lambda event, msg, fields, level: events.append(event)

        op = LoadQuickGOAnnotationsOperation()
        result = op.execute(session, _base_payload(), emit=emit)
        assert result.result["annotations_inserted"] == 1
        assert result.result["annotations_skipped"] == 2
        assert "load_quickgo_annotations.start" in events
        assert "load_quickgo_annotations.done" in events
        assert "load_quickgo_annotations.annotation_set_created" in events

    @patch("protea.core.operations.load_quickgo_annotations.requests.get")
    def test_total_limit_stops_early(self, mock_get) -> None:
        tsv = _make_tsv_text(
            _tsv_row_str("P12345", "GO:0003824"),
            _tsv_row_str("P12345", "GO:0008150"),
            _tsv_row_str("P12345", "GO:0003824"),
        )
        mock_get.return_value = _make_stream_response(tsv)

        session = _mock_session(
            canonical_accessions={"P12345"},
            protein_accessions={"P12345"},
        )

        events: list[str] = []
        emit = lambda event, msg, fields, level: events.append(event)

        op = LoadQuickGOAnnotationsOperation()
        result = op.execute(
            session, _base_payload(total_limit=1, page_size=1), emit=emit,
        )
        assert "load_quickgo_annotations.limit_reached" in events

    @patch("protea.core.operations.load_quickgo_annotations.requests.get")
    def test_commit_every_page(self, mock_get) -> None:
        tsv = _make_tsv_text(
            _tsv_row_str("P12345", "GO:0003824"),
            _tsv_row_str("P12345", "GO:0008150"),
        )
        mock_get.return_value = _make_stream_response(tsv)

        session = _mock_session(
            canonical_accessions={"P12345"},
            protein_accessions={"P12345"},
        )

        op = LoadQuickGOAnnotationsOperation()
        op.execute(
            session, _base_payload(commit_every_page=True, page_size=1), emit=_noop_emit,
        )
        assert session.commit.call_count >= 2

    @patch("protea.core.operations.load_quickgo_annotations.requests.get")
    def test_no_commit_when_disabled(self, mock_get) -> None:
        tsv = _make_tsv_text(_tsv_row_str("P12345", "GO:0003824"))
        mock_get.return_value = _make_stream_response(tsv)

        session = _mock_session(
            canonical_accessions={"P12345"},
            protein_accessions={"P12345"},
        )

        op = LoadQuickGOAnnotationsOperation()
        op.execute(
            session, _base_payload(commit_every_page=False, page_size=1), emit=_noop_emit,
        )
        session.commit.assert_not_called()

    @patch("protea.core.operations.load_quickgo_annotations.requests.get")
    def test_page_done_emitted(self, mock_get) -> None:
        tsv = _make_tsv_text(
            _tsv_row_str("P12345", "GO:0003824"),
            _tsv_row_str("P12345", "GO:0008150"),
            _tsv_row_str("P12345", "GO:0003824"),
        )
        mock_get.return_value = _make_stream_response(tsv)

        session = _mock_session(
            canonical_accessions={"P12345"},
            protein_accessions={"P12345"},
        )

        events: list[tuple[str, dict]] = []
        emit = lambda event, msg, fields, level: events.append((event, fields))

        op = LoadQuickGOAnnotationsOperation()
        result = op.execute(session, _base_payload(page_size=2), emit=emit)
        page_done = [e for e in events if e[0] == "load_quickgo_annotations.page_done"]
        assert len(page_done) >= 1
        assert result.result["pages"] == 2

    @patch("protea.core.operations.load_quickgo_annotations.requests.get")
    def test_result_contains_elapsed_seconds(self, mock_get) -> None:
        tsv = _make_tsv_text(_tsv_row_str("P12345", "GO:0003824"))
        mock_get.return_value = _make_stream_response(tsv)

        session = _mock_session(
            canonical_accessions={"P12345"},
            protein_accessions={"P12345"},
        )

        op = LoadQuickGOAnnotationsOperation()
        result = op.execute(session, _base_payload(), emit=_noop_emit)
        assert "elapsed_seconds" in result.result
        assert result.result["elapsed_seconds"] >= 0

    @patch("protea.core.operations.load_quickgo_annotations.requests.get")
    def test_use_db_accessions_false(self, mock_get) -> None:
        tsv = _make_tsv_text(_tsv_row_str("X00001", "GO:0003824"))
        mock_get.return_value = _make_stream_response(tsv)

        session = _mock_session(
            canonical_accessions={"P12345"},
            protein_accessions={"P12345", "X00001"},
            go_terms=[("GO:0003824", 1)],
        )

        op = LoadQuickGOAnnotationsOperation()
        result = op.execute(
            session,
            _base_payload(use_db_accessions=False, gene_product_ids=["X00001"]),
            emit=_noop_emit,
        )
        _, kwargs = mock_get.call_args
        assert "X00001" in kwargs["params"]["geneProductId"]
        assert result.result["annotations_inserted"] == 1

    @patch("protea.core.operations.load_quickgo_annotations.requests.get")
    def test_eco_mapping_integrated_in_execute(self, mock_get) -> None:
        eco_resp = MagicMock()
        eco_resp.text = "ECO:0000314 IDA\n"
        eco_resp.raise_for_status = MagicMock()

        tsv_resp = _make_stream_response(
            _make_tsv_text(_tsv_row_str("P12345", "GO:0003824", eco_id="ECO:0000314"))
        )

        mock_get.side_effect = [eco_resp, tsv_resp]

        session = _mock_session(
            canonical_accessions={"P12345"},
            protein_accessions={"P12345"},
            go_terms=[("GO:0003824", 1)],
        )

        op = LoadQuickGOAnnotationsOperation()
        result = op.execute(
            session,
            _base_payload(eco_mapping_url="https://eco.test/map.txt"),
            emit=_noop_emit,
        )
        assert result.result["annotations_inserted"] == 1

    @patch("protea.core.operations.load_quickgo_annotations.requests.get")
    def test_result_has_annotation_set_id(self, mock_get) -> None:
        tsv = _make_tsv_text(_tsv_row_str("P12345", "GO:0003824"))
        mock_get.return_value = _make_stream_response(tsv)

        session = _mock_session(
            canonical_accessions={"P12345"},
            protein_accessions={"P12345"},
        )

        op = LoadQuickGOAnnotationsOperation()
        result = op.execute(session, _base_payload(), emit=_noop_emit)
        assert "annotation_set_id" in result.result

    @patch("protea.core.operations.load_quickgo_annotations.requests.get")
    def test_remainder_buffer_flushed(self, mock_get) -> None:
        """Records that don't fill a full page are still flushed at the end."""
        tsv = _make_tsv_text(_tsv_row_str("P12345", "GO:0003824"))
        mock_get.return_value = _make_stream_response(tsv)

        session = _mock_session(
            canonical_accessions={"P12345"},
            protein_accessions={"P12345"},
        )

        op = LoadQuickGOAnnotationsOperation()
        # page_size much larger than record count → only remainder flush
        result = op.execute(session, _base_payload(page_size=10000), emit=_noop_emit)
        assert result.result["annotations_inserted"] == 1
        assert result.result["pages"] == 1

    def test_operation_name(self) -> None:
        assert LoadQuickGOAnnotationsOperation().name == "load_quickgo_annotations"
