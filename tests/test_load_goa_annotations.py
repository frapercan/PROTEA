"""
Unit tests for LoadGOAAnnotationsOperation.
No DB or network required — everything is mocked.
"""
from __future__ import annotations

import io
import uuid
from unittest.mock import MagicMock, patch

import pytest

from protea.core.contracts.operation import OperationResult
from protea.core.operations.load_goa_annotations import (
    LoadGOAAnnotationsOperation,
    LoadGOAAnnotationsPayload,
)

_noop_emit = lambda *_: None  # noqa: E731

_SNAPSHOT_ID = str(uuid.uuid4())
_ANNOTATION_SET_ID = uuid.uuid4()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_emit():
    """Return a recording emit function and its event list."""
    events = []

    def emit(event, message, fields, level):
        events.append({"event": event, "fields": fields, "level": level})

    return emit, events


def _gaf_line(
    accession="P12345",
    go_id="GO:0003674",
    qualifier="enables",
    evidence="IDA",
    db_ref="PMID:1234",
    with_from="",
    date="20240101",
    assigned_by="UniProt",
):
    """Build a valid 15-column GAF line."""
    cols = ["UniProtKB"] + [""] * 14
    cols[1] = accession
    cols[3] = qualifier
    cols[4] = go_id
    cols[5] = db_ref
    cols[6] = evidence
    cols[7] = with_from
    cols[13] = date
    cols[14] = assigned_by
    return "\t".join(cols)


# ---------------------------------------------------------------------------
# Payload validation
# ---------------------------------------------------------------------------


class TestLoadGOAAnnotationsPayload:
    def test_valid(self) -> None:
        p = LoadGOAAnnotationsPayload.model_validate({
            "ontology_snapshot_id": _SNAPSHOT_ID,
            "gaf_url": "https://ftp.ebi.ac.uk/goa_human.gaf.gz",
            "source_version": "2024-03",
        })
        assert p.source_version == "2024-03"
        assert p.page_size == 10000

    def test_missing_required_raises(self) -> None:
        with pytest.raises(ValueError):
            LoadGOAAnnotationsPayload.model_validate({
                "gaf_url": "https://example.org/goa.gaf.gz",
                "source_version": "2024-03",
            })

    def test_empty_snapshot_id_raises(self) -> None:
        with pytest.raises(ValueError):
            LoadGOAAnnotationsPayload.model_validate({
                "ontology_snapshot_id": "  ",
                "gaf_url": "https://example.org/goa.gaf.gz",
                "source_version": "2024-03",
            })

    def test_empty_gaf_url_raises(self) -> None:
        with pytest.raises(ValueError):
            LoadGOAAnnotationsPayload(
                ontology_snapshot_id=_SNAPSHOT_ID,
                gaf_url="",
                source_version="v1",
            )

    def test_empty_source_version_raises(self) -> None:
        with pytest.raises(ValueError):
            LoadGOAAnnotationsPayload(
                ontology_snapshot_id=_SNAPSHOT_ID,
                gaf_url="https://example.com/goa.gaf.gz",
                source_version="",
            )

    def test_page_size_must_be_positive(self) -> None:
        with pytest.raises(ValueError):
            LoadGOAAnnotationsPayload(
                ontology_snapshot_id=_SNAPSHOT_ID,
                gaf_url="https://example.com/goa.gaf.gz",
                source_version="v1",
                page_size=0,
            )

    def test_strings_are_stripped(self) -> None:
        p = LoadGOAAnnotationsPayload(
            ontology_snapshot_id=f"  {_SNAPSHOT_ID}  ",
            gaf_url="  https://example.com/goa.gaf.gz  ",
            source_version="  v1  ",
        )
        assert p.ontology_snapshot_id == _SNAPSHOT_ID
        assert p.gaf_url == "https://example.com/goa.gaf.gz"
        assert p.source_version == "v1"

    def test_defaults(self) -> None:
        p = LoadGOAAnnotationsPayload(
            ontology_snapshot_id=_SNAPSHOT_ID,
            gaf_url="https://example.com/goa.gaf.gz",
            source_version="v1",
        )
        assert p.timeout_seconds == 300
        assert p.commit_every_page is True
        assert p.total_limit is None


# ---------------------------------------------------------------------------
# _store_buffer
# ---------------------------------------------------------------------------


class TestStoreBuffer:
    def _op(self) -> LoadGOAAnnotationsOperation:
        return LoadGOAAnnotationsOperation()

    def _make_record(self, accession="P12345", go_id="GO:0003824", evidence="IDA"):
        return {
            "accession": accession,
            "go_id": go_id,
            "qualifier": "enables",
            "evidence_code": evidence,
            "db_reference": "PMID:1",
            "with_from": "",
            "assigned_by": "UniProt",
            "annotation_date": "20240101",
        }

    def test_skips_unknown_accession(self) -> None:
        op = self._op()
        session = MagicMock()
        records = [self._make_record(accession="UNKNOWN")]
        inserted, skipped = op._store_buffer(
            session,
            records,
            uuid.UUID(_SNAPSHOT_ID),
            valid_accessions={"P12345"},
            go_term_map={"GO:0003824": 1},
        )
        assert inserted == 0
        assert skipped == 1

    def test_skips_empty_accession(self) -> None:
        op = self._op()
        session = MagicMock()
        records = [self._make_record(accession="  ")]
        inserted, skipped = op._store_buffer(
            session,
            records,
            uuid.UUID(_SNAPSHOT_ID),
            valid_accessions={"P12345"},
            go_term_map={"GO:0003824": 1},
        )
        assert inserted == 0
        assert skipped == 1

    def test_skips_unknown_go_term(self) -> None:
        op = self._op()
        session = MagicMock()
        records = [self._make_record(go_id="GO:9999999")]
        inserted, skipped = op._store_buffer(
            session,
            records,
            uuid.UUID(_SNAPSHOT_ID),
            valid_accessions={"P12345"},
            go_term_map={"GO:0003824": 1},
        )
        assert inserted == 0
        assert skipped == 1

    def test_inserts_valid_records(self) -> None:
        op = self._op()
        session = MagicMock()
        records = [
            self._make_record(accession="P12345", go_id="GO:0003824"),
            self._make_record(accession="Q67890", go_id="GO:0008150", evidence="IEA"),
        ]
        inserted, skipped = op._store_buffer(
            session,
            records,
            uuid.UUID(_SNAPSHOT_ID),
            valid_accessions={"P12345", "Q67890"},
            go_term_map={"GO:0003824": 1, "GO:0008150": 2},
        )
        assert inserted == 2
        assert skipped == 0
        session.execute.assert_called()

    def test_deduplicates_within_buffer(self) -> None:
        op = self._op()
        session = MagicMock()
        rec = self._make_record()
        records = [rec.copy(), rec.copy(), rec.copy()]
        inserted, skipped = op._store_buffer(
            session,
            records,
            uuid.UUID(_SNAPSHOT_ID),
            valid_accessions={"P12345"},
            go_term_map={"GO:0003824": 1},
        )
        assert inserted == 1
        assert skipped == 2

    def test_different_evidence_codes_not_deduplicated(self) -> None:
        op = self._op()
        session = MagicMock()
        records = [
            self._make_record(evidence="IDA"),
            self._make_record(evidence="IEA"),
        ]
        inserted, skipped = op._store_buffer(
            session,
            records,
            uuid.UUID(_SNAPSHOT_ID),
            valid_accessions={"P12345"},
            go_term_map={"GO:0003824": 1},
        )
        assert inserted == 2
        assert skipped == 0

    def test_mixed_valid_and_invalid(self) -> None:
        op = self._op()
        session = MagicMock()
        records = [
            self._make_record(accession="P12345"),
            self._make_record(accession="UNKNOWN"),
            self._make_record(accession="Q67890", go_id="GO:0008150"),
            self._make_record(go_id="GO:INVALID"),
        ]
        inserted, skipped = op._store_buffer(
            session,
            records,
            uuid.UUID(_SNAPSHOT_ID),
            valid_accessions={"P12345", "Q67890"},
            go_term_map={"GO:0003824": 1, "GO:0008150": 2},
        )
        assert inserted == 2
        assert skipped == 2

    def test_empty_buffer(self) -> None:
        op = self._op()
        session = MagicMock()
        inserted, skipped = op._store_buffer(
            session, [], uuid.UUID(_SNAPSHOT_ID),
            valid_accessions={"P12345"}, go_term_map={"GO:0003824": 1},
        )
        assert inserted == 0
        assert skipped == 0
        session.execute.assert_not_called()

    def test_empty_evidence_treated_as_none_for_dedup(self) -> None:
        """Empty string evidence_code becomes None; two such records are duplicates."""
        op = self._op()
        session = MagicMock()
        records = [
            self._make_record(evidence=""),
            self._make_record(evidence=""),
        ]
        inserted, skipped = op._store_buffer(
            session,
            records,
            uuid.UUID(_SNAPSHOT_ID),
            valid_accessions={"P12345"},
            go_term_map={"GO:0003824": 1},
        )
        assert inserted == 1
        assert skipped == 1


# ---------------------------------------------------------------------------
# _stream_gaf
# ---------------------------------------------------------------------------


class TestStreamGaf:
    def setup_method(self):
        self.op = LoadGOAAnnotationsOperation()

    def _stream_from_text(self, text: str, url="https://example.com/goa.gaf"):
        """Mock requests.get and stream GAF text through _stream_gaf."""
        payload = LoadGOAAnnotationsPayload(
            ontology_snapshot_id=_SNAPSHOT_ID,
            gaf_url=url,
            source_version="v1",
        )
        emit, _ = _make_emit()

        raw = io.BytesIO(text.encode("utf-8"))
        mock_resp = MagicMock()
        mock_resp.raw = raw
        mock_resp.raise_for_status = MagicMock()

        with patch("protea.core.operations.load_goa_annotations.requests.get", return_value=mock_resp):
            return list(self.op._stream_gaf(payload, emit))

    def test_parses_valid_gaf_line(self):
        line = _gaf_line(accession="P12345", go_id="GO:0003674", evidence="IDA")
        records = self._stream_from_text(line + "\n")
        assert len(records) == 1
        assert records[0]["accession"] == "P12345"
        assert records[0]["go_id"] == "GO:0003674"
        assert records[0]["evidence_code"] == "IDA"

    def test_skips_comment_lines(self):
        text = "!this is a comment\n" + _gaf_line() + "\n"
        records = self._stream_from_text(text)
        assert len(records) == 1

    def test_skips_empty_lines(self):
        text = "\n\n" + _gaf_line() + "\n\n"
        records = self._stream_from_text(text)
        assert len(records) == 1

    def test_skips_short_lines(self):
        text = "col1\tcol2\tcol3\n" + _gaf_line() + "\n"
        records = self._stream_from_text(text)
        assert len(records) == 1

    def test_multiple_records(self):
        lines = [
            _gaf_line(accession="A1"),
            _gaf_line(accession="A2"),
            _gaf_line(accession="A3"),
        ]
        records = self._stream_from_text("\n".join(lines) + "\n")
        assert len(records) == 3
        assert [r["accession"] for r in records] == ["A1", "A2", "A3"]

    def test_extracts_all_fields(self):
        line = _gaf_line(
            accession="Q99999",
            go_id="GO:0005575",
            qualifier="located_in",
            evidence="IEA",
            db_ref="GO_REF:001",
            with_from="InterPro:IPR000001",
            date="20230615",
            assigned_by="InterPro",
        )
        records = self._stream_from_text(line + "\n")
        r = records[0]
        assert r["accession"] == "Q99999"
        assert r["go_id"] == "GO:0005575"
        assert r["qualifier"] == "located_in"
        assert r["evidence_code"] == "IEA"
        assert r["db_reference"] == "GO_REF:001"
        assert r["with_from"] == "InterPro:IPR000001"
        assert r["annotation_date"] == "20230615"
        assert r["assigned_by"] == "InterPro"

    def test_gzip_url_uses_gzip_decompression(self):
        import gzip as gzip_mod

        line = _gaf_line() + "\n"
        compressed = gzip_mod.compress(line.encode("utf-8"))

        payload = LoadGOAAnnotationsPayload(
            ontology_snapshot_id=_SNAPSHOT_ID,
            gaf_url="https://example.com/goa.gaf.gz",
            source_version="v1",
        )
        emit, _ = _make_emit()

        raw = io.BytesIO(compressed)
        mock_resp = MagicMock()
        mock_resp.raw = raw
        mock_resp.raise_for_status = MagicMock()

        with patch("protea.core.operations.load_goa_annotations.requests.get", return_value=mock_resp):
            records = list(self.op._stream_gaf(payload, emit))
        assert len(records) == 1

    def test_empty_file_returns_no_records(self):
        records = self._stream_from_text("")
        assert records == []

    def test_file_with_only_comments(self):
        text = "!comment1\n!comment2\n"
        records = self._stream_from_text(text)
        assert records == []


# ---------------------------------------------------------------------------
# _load_accessions
# ---------------------------------------------------------------------------


class TestLoadAccessions:
    def setup_method(self):
        self.op = LoadGOAAnnotationsOperation()

    def test_returns_set_of_accessions(self):
        session = MagicMock()
        session.scalars.return_value = iter(["P12345", "Q99999"])
        emit, events = _make_emit()

        result = self.op._load_accessions(session, emit)
        assert result == {"P12345", "Q99999"}
        event_names = [e["event"] for e in events]
        assert "load_goa_annotations.load_accessions_start" in event_names
        assert "load_goa_annotations.load_accessions_done" in event_names

    def test_returns_empty_set(self):
        session = MagicMock()
        session.scalars.return_value = iter([])
        emit, _ = _make_emit()

        result = self.op._load_accessions(session, emit)
        assert result == set()

    def test_emits_count_in_done_event(self):
        session = MagicMock()
        session.scalars.return_value = iter(["A", "B", "C"])
        emit, events = _make_emit()

        self.op._load_accessions(session, emit)
        done = [e for e in events if e["event"] == "load_goa_annotations.load_accessions_done"]
        assert len(done) == 1
        assert done[0]["fields"]["canonical_accessions"] == 3


# ---------------------------------------------------------------------------
# _load_go_term_map
# ---------------------------------------------------------------------------


class TestLoadGoTermMap:
    def setup_method(self):
        self.op = LoadGOAAnnotationsOperation()

    def _mock_session(self, rows):
        session = MagicMock()
        query_mock = MagicMock()
        session.query.return_value = query_mock
        query_mock.filter.return_value = query_mock
        query_mock.all.return_value = rows
        return session

    def test_returns_mapping(self):
        session = self._mock_session([("GO:0003674", 1), ("GO:0005575", 2)])
        emit, events = _make_emit()

        result = self.op._load_go_term_map(session, uuid.uuid4(), emit)
        assert result == {"GO:0003674": 1, "GO:0005575": 2}
        event_names = [e["event"] for e in events]
        assert "load_goa_annotations.load_go_terms_start" in event_names
        assert "load_goa_annotations.load_go_terms_done" in event_names

    def test_empty_ontology(self):
        session = self._mock_session([])
        emit, _ = _make_emit()

        result = self.op._load_go_term_map(session, uuid.uuid4(), emit)
        assert result == {}

    def test_emits_count_in_done_event(self):
        session = self._mock_session([("GO:0003674", 1)])
        emit, events = _make_emit()

        self.op._load_go_term_map(session, uuid.uuid4(), emit)
        done = [e for e in events if e["event"] == "load_goa_annotations.load_go_terms_done"]
        assert len(done) == 1
        assert done[0]["fields"]["go_terms"] == 1


# ---------------------------------------------------------------------------
# execute (full integration of all pieces, mocked)
# ---------------------------------------------------------------------------


class TestExecute:
    def setup_method(self):
        self.op = LoadGOAAnnotationsOperation()
        self.snapshot_id = uuid.uuid4()

    def _make_session(self, accessions, go_terms):
        session = MagicMock()
        # session.get(OntologySnapshot, id) returns a truthy mock
        session.get.return_value = MagicMock()
        # _load_accessions uses session.scalars
        session.scalars.return_value = iter(accessions)
        # _load_go_term_map uses session.query
        query_mock = MagicMock()
        session.query.return_value = query_mock
        query_mock.filter.return_value = query_mock
        query_mock.all.return_value = list(go_terms.items())
        return session

    def _run(self, gaf_text, accessions, go_terms,
             page_size=10000, total_limit=None, commit_every_page=True,
             store_buffer_side_effect=None):
        session = self._make_session(accessions, go_terms)
        emit, events = _make_emit()

        ann_set_mock = MagicMock()
        ann_set_mock.id = _ANNOTATION_SET_ID

        payload = {
            "ontology_snapshot_id": str(self.snapshot_id),
            "gaf_url": "https://example.com/goa.gaf",
            "source_version": "v1",
            "page_size": page_size,
            "commit_every_page": commit_every_page,
        }
        if total_limit is not None:
            payload["total_limit"] = total_limit

        raw = io.BytesIO(gaf_text.encode("utf-8"))
        mock_resp = MagicMock()
        mock_resp.raw = raw
        mock_resp.raise_for_status = MagicMock()

        # _store_buffer does a lazy import of pg_insert which needs a real
        # SQLAlchemy Table object. We mock the whole method and count
        # inserted/skipped via the records passed to it, using the real
        # filtering logic from the valid_accessions and go_terms sets.
        real_valid = set(accessions)
        real_go = dict(go_terms)

        def fake_store_buffer(_session, records, _ann_set_id, _valid, _go_map):
            inserted = 0
            skipped = 0
            seen = set()
            for rec in records:
                acc = rec["accession"].strip()
                if not acc or acc not in real_valid:
                    skipped += 1
                    continue
                go_id = rec["go_id"].strip()
                go_term_id = real_go.get(go_id)
                if go_term_id is None:
                    skipped += 1
                    continue
                ev = rec["evidence_code"] or None
                key = (_ann_set_id, acc, go_term_id, ev)
                if key in seen:
                    skipped += 1
                    continue
                seen.add(key)
                inserted += 1
            return inserted, skipped

        if store_buffer_side_effect is not None:
            fake_store_buffer = store_buffer_side_effect

        with patch(
            "protea.core.operations.load_goa_annotations.requests.get",
            return_value=mock_resp,
        ), patch(
            "protea.core.operations.load_goa_annotations.AnnotationSet",
            return_value=ann_set_mock,
        ), patch.object(
            self.op, "_store_buffer", side_effect=fake_store_buffer,
        ):
            result = self.op.execute(session, payload, emit=emit)

        return result, events, session

    def test_basic_execution(self):
        gaf = _gaf_line(accession="P12345", go_id="GO:0003674") + "\n"
        result, events, _ = self._run(
            gaf, accessions=["P12345"], go_terms={"GO:0003674": 1},
        )
        assert isinstance(result, OperationResult)
        assert result.result["annotations_inserted"] == 1
        assert result.result["annotations_skipped"] == 0
        event_names = [e["event"] for e in events]
        assert "load_goa_annotations.start" in event_names
        assert "load_goa_annotations.done" in event_names

    def test_snapshot_not_found_raises(self):
        session = MagicMock()
        session.get.return_value = None
        emit, _ = _make_emit()

        payload = {
            "ontology_snapshot_id": str(self.snapshot_id),
            "gaf_url": "https://example.com/goa.gaf",
            "source_version": "v1",
        }
        with pytest.raises(ValueError, match="not found"):
            self.op.execute(session, payload, emit=emit)

    def test_no_proteins_returns_zero(self):
        gaf = _gaf_line() + "\n"
        result, events, _ = self._run(
            gaf, accessions=[], go_terms={"GO:0003674": 1},
        )
        assert result.result == {"annotations_inserted": 0}
        event_names = [e["event"] for e in events]
        assert "load_goa_annotations.no_proteins" in event_names

    def test_skips_unmatched_accessions(self):
        gaf = _gaf_line(accession="UNKNOWN") + "\n"
        result, _, _ = self._run(
            gaf, accessions=["P12345"], go_terms={"GO:0003674": 1},
        )
        assert result.result["annotations_inserted"] == 0
        assert result.result["annotations_skipped"] == 1

    def test_skips_unmatched_go_ids(self):
        gaf = _gaf_line(accession="P12345", go_id="GO:UNKNOWN") + "\n"
        result, _, _ = self._run(
            gaf, accessions=["P12345"], go_terms={"GO:0003674": 1},
        )
        assert result.result["annotations_inserted"] == 0
        assert result.result["annotations_skipped"] == 1

    def test_pagination_emits_page_done(self):
        lines = [_gaf_line(accession="P12345", go_id="GO:0003674", evidence=f"E{i}")
                 for i in range(5)]
        gaf = "\n".join(lines) + "\n"
        result, events, _ = self._run(
            gaf, accessions=["P12345"], go_terms={"GO:0003674": 1},
            page_size=2,
        )
        page_events = [e for e in events if e["event"] == "load_goa_annotations.page_done"]
        # 5 records, page_size=2 -> 2 full pages emitted (remainder flushed separately)
        assert len(page_events) == 2
        assert result.result["annotations_inserted"] == 5
        assert result.result["pages"] == 3

    def test_commit_every_page(self):
        lines = [_gaf_line(accession="P12345", go_id="GO:0003674", evidence=f"E{i}")
                 for i in range(4)]
        gaf = "\n".join(lines) + "\n"
        _, _, session = self._run(
            gaf, accessions=["P12345"], go_terms={"GO:0003674": 1},
            page_size=2, commit_every_page=True,
        )
        # 4 records, page_size=2 -> 2 full pages -> 2 commits
        assert session.commit.call_count == 2

    def test_no_commit_when_disabled(self):
        lines = [_gaf_line(accession="P12345", go_id="GO:0003674", evidence=f"E{i}")
                 for i in range(4)]
        gaf = "\n".join(lines) + "\n"
        _, _, session = self._run(
            gaf, accessions=["P12345"], go_terms={"GO:0003674": 1},
            page_size=2, commit_every_page=False,
        )
        session.commit.assert_not_called()

    def test_total_limit_stops_early(self):
        lines = [_gaf_line(accession="P12345", go_id="GO:0003674", evidence=f"E{i}")
                 for i in range(10)]
        gaf = "\n".join(lines) + "\n"
        result, events, _ = self._run(
            gaf, accessions=["P12345"], go_terms={"GO:0003674": 1},
            page_size=3, total_limit=3,
        )
        assert result.result["annotations_inserted"] == 3
        event_names = [e["event"] for e in events]
        assert "load_goa_annotations.limit_reached" in event_names

    def test_empty_file(self):
        result, _, _ = self._run(
            "", accessions=["P12345"], go_terms={"GO:0003674": 1},
        )
        assert result.result["annotations_inserted"] == 0
        assert result.result["total_lines_read"] == 0
        assert result.result["pages"] == 0

    def test_result_contains_elapsed_seconds(self):
        gaf = _gaf_line() + "\n"
        result, _, _ = self._run(
            gaf, accessions=["P12345"], go_terms={"GO:0003674": 1},
        )
        assert "elapsed_seconds" in result.result
        assert result.result["elapsed_seconds"] >= 0

    def test_result_contains_annotation_set_id(self):
        gaf = _gaf_line() + "\n"
        result, _, _ = self._run(
            gaf, accessions=["P12345"], go_terms={"GO:0003674": 1},
        )
        assert result.result["annotation_set_id"] == str(_ANNOTATION_SET_ID)

    def test_duplicate_annotations_in_file(self):
        line = _gaf_line(accession="P12345", go_id="GO:0003674", evidence="IDA")
        gaf = (line + "\n") * 5
        result, _, _ = self._run(
            gaf, accessions=["P12345"], go_terms={"GO:0003674": 1},
        )
        assert result.result["annotations_inserted"] == 1
        assert result.result["annotations_skipped"] == 4

    def test_comments_and_short_lines_not_counted(self):
        text = (
            "!GAF header comment\n"
            "!another comment\n"
            "short\tline\n"
            + _gaf_line(accession="P12345", go_id="GO:0003674") + "\n"
        )
        result, _, _ = self._run(
            text, accessions=["P12345"], go_terms={"GO:0003674": 1},
        )
        # Only valid GAF lines are counted as total_lines_read
        assert result.result["total_lines_read"] == 1
        assert result.result["annotations_inserted"] == 1

    def test_annotation_set_created_event(self):
        gaf = _gaf_line() + "\n"
        _, events, _ = self._run(
            gaf, accessions=["P12345"], go_terms={"GO:0003674": 1},
        )
        event_names = [e["event"] for e in events]
        assert "load_goa_annotations.annotation_set_created" in event_names
        created = [e for e in events if e["event"] == "load_goa_annotations.annotation_set_created"]
        assert created[0]["fields"]["annotation_set_id"] == str(_ANNOTATION_SET_ID)

    def test_page_done_event_fields(self):
        lines = [_gaf_line(accession="P12345", go_id="GO:0003674", evidence=f"E{i}")
                 for i in range(3)]
        gaf = "\n".join(lines) + "\n"
        _, events, _ = self._run(
            gaf, accessions=["P12345"], go_terms={"GO:0003674": 1},
            page_size=2,
        )
        page_events = [e for e in events if e["event"] == "load_goa_annotations.page_done"]
        assert len(page_events) == 1
        fields = page_events[0]["fields"]
        assert fields["page"] == 1
        assert fields["total_lines"] == 2
        assert fields["total_inserted"] == 2

    def test_session_flush_called_after_annotation_set_add(self):
        gaf = _gaf_line() + "\n"
        _, _, session = self._run(
            gaf, accessions=["P12345"], go_terms={"GO:0003674": 1},
        )
        session.flush.assert_called()

    def test_multiple_pages_with_remainder(self):
        """7 records with page_size=3 -> 2 full pages + 1 remainder = 3 pages total."""
        lines = [_gaf_line(accession="P12345", go_id="GO:0003674", evidence=f"E{i}")
                 for i in range(7)]
        gaf = "\n".join(lines) + "\n"
        result, events, session = self._run(
            gaf, accessions=["P12345"], go_terms={"GO:0003674": 1},
            page_size=3,
        )
        assert result.result["pages"] == 3
        assert result.result["annotations_inserted"] == 7
        page_events = [e for e in events if e["event"] == "load_goa_annotations.page_done"]
        assert len(page_events) == 2  # only full pages emit page_done


# ---------------------------------------------------------------------------
# Operation name
# ---------------------------------------------------------------------------


class TestOperationName:
    def test_name(self):
        assert LoadGOAAnnotationsOperation.name == "load_goa_annotations"
