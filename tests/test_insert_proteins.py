"""
Tests for InsertProteinsOperation.
Unit tests use mocked HTTP + mocked session (no DB, no network).
Integration test uses a real Postgres via --with-postgres.
"""

from __future__ import annotations

import gzip
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


def _make_gz_response(fasta_text: str) -> MagicMock:
    """A release file as served: gzipped bytes, no Link header."""
    resp = MagicMock()
    resp.status_code = 200
    resp.content = gzip.compress(fasta_text.encode("utf-8"))
    resp.headers = {}
    resp.raise_for_status = MagicMock()
    return resp


def _make_mock_session():
    """Session mock that returns empty results for all DB queries."""
    session = MagicMock(spec=Session)
    session.query.return_value.filter.return_value.all.return_value = []
    session.query.return_value.filter.return_value.first.return_value = None
    return session


def _make_record(
    accession: str = "P12345",
    sequence: str = "MKTAYIAK",
    is_canonical: bool = True,
    isoform_index: int | None = None,
    canonical_accession: str | None = None,
):
    """Build a UniProtProteinRecord for store-records testing."""
    from protea_contracts import UniProtProteinRecord, compute_sequence_hash

    return UniProtProteinRecord(
        accession=accession,
        entry_name="TEST_HUMAN",
        canonical_accession=canonical_accession or accession,
        is_canonical=is_canonical,
        isoform_index=isoform_index,
        organism="Homo sapiens",
        taxonomy_id="9606",
        gene_name="TEST",
        reviewed=True,
        sequence=sequence,
        length=len(sequence),
        sequence_hash=compute_sequence_hash(sequence),
    )


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
        p = InsertProteinsPayload.model_validate(
            {
                "search_criteria": "organism_id:9606",
                "page_size": 100,
                "total_limit": 50,
                "timeout_seconds": 30,
                "include_isoforms": False,
                "compressed": True,
                "max_retries": 2,
            }
        )
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
# Unit tests — the release-file source path
# ---------------------------------------------------------------------------

_CANONICAL_URL = "https://ftp.uniprot.org/x/uniprot_sprot.fasta.gz"
_VARSPLIC_URL = "https://ftp.uniprot.org/x/uniprot_sprot_varsplic.fasta.gz"

FASTA_ISOFORM = (
    ">sp|P12345-2|TEST_HUMAN Isoform 2 OS=Homo sapiens OX=9606 GN=TEST PE=1 SV=2\n"
    "MKTAYIAKQRQ\n"
)


class TestReleaseFastaPayload:
    def test_defaults_to_no_release_files(self):
        p = InsertProteinsPayload(search_criteria="reviewed:true")
        assert p.release_fasta_urls == []

    def test_accepts_the_json_list_a_real_job_arrives_as(self):
        # POST /v1/jobs carries JSON, so the field arrives as a list.
        # ProteaPayload is strict=True on purpose and will not coerce a
        # list into a tuple, which is how this was caught.
        p = InsertProteinsPayload.model_validate(
            {
                "search_criteria": "reviewed:true",
                "release_fasta_urls": [_CANONICAL_URL, _VARSPLIC_URL],
            }
        )
        assert p.release_fasta_urls == [_CANONICAL_URL, _VARSPLIC_URL]

    def test_survives_a_json_round_trip(self):
        import json

        p = InsertProteinsPayload.model_validate(
            json.loads(
                json.dumps(
                    {
                        "search_criteria": "reviewed:true",
                        "release_fasta_urls": [_CANONICAL_URL],
                    }
                )
            )
        )
        assert p.release_fasta_urls == [_CANONICAL_URL]

    def test_rejects_a_non_http_url(self):
        # A local path would read whatever is on the worker's disk, which
        # no declaration can pin.
        with pytest.raises(Exception):
            InsertProteinsPayload(
                search_criteria="reviewed:true",
                release_fasta_urls=["/var/tmp/uniprot_sprot.fasta.gz"],
            )

    def test_still_requires_search_criteria(self):
        # It is what the files are asserted to be equivalent to, so the
        # declaration stays readable without opening the URLs.
        with pytest.raises(Exception):
            InsertProteinsPayload(release_fasta_urls=[_CANONICAL_URL])


class TestExecuteFromReleaseFiles:
    def setup_method(self):
        self.op = InsertProteinsOperation()

    def test_reads_the_release_files_instead_of_querying(self):
        session = _make_mock_session()
        emit = _capturing_emit()
        bodies = {
            _CANONICAL_URL: _make_gz_response(FASTA_TWO),
            _VARSPLIC_URL: _make_gz_response(FASTA_ISOFORM),
        }
        with patch.object(
            self.op._uniprot_plugin._client.session,
            "get",
            side_effect=lambda url, **_kw: bodies[url],
        ) as mock_get:
            result = self.op.execute(
                session,
                {
                    "search_criteria": "reviewed:true",
                    "release_fasta_urls": [_CANONICAL_URL, _VARSPLIC_URL],
                },
                emit=emit,
            )
        # Exactly the two files, and no search URL: dispatching on the
        # URL means this fails if the operation paginated instead.
        assert [c.args[0] for c in mock_get.call_args_list] == [
            _CANONICAL_URL,
            _VARSPLIC_URL,
        ]
        assert result.result["retrieved_records"] == 3
        assert result.result["isoform_records"] == 1
        assert result.result["source"] == "release_files"

    def test_cursor_path_is_still_the_default(self):
        session = _make_mock_session()
        emit = _capturing_emit()
        with patch.object(
            self.op._uniprot_plugin._client.session,
            "get",
            return_value=_make_mock_response(FASTA_ONE),
        ) as mock_get:
            result = self.op.execute(
                session,
                {"search_criteria": "organism_id:9606", "compressed": False},
                emit=emit,
            )
        assert result.result["source"] == "cursor_pagination"
        assert "format=fasta" in mock_get.call_args_list[0].args[0]

    def test_start_event_records_which_files_were_read(self):
        # The job's own log has to name the bytes, or a corpus cannot be
        # traced back to them.
        session = _make_mock_session()
        emit = _capturing_emit()
        with patch.object(
            self.op._uniprot_plugin._client.session,
            "get",
            return_value=_make_gz_response(FASTA_ONE),
        ):
            self.op.execute(
                session,
                {
                    "search_criteria": "reviewed:true",
                    "release_fasta_urls": [_CANONICAL_URL],
                },
                emit=emit,
            )
        (start,) = [c for c in emit.calls if c["event"] == "insert_proteins.start"]
        assert start["fields"]["source"] == "release_files"
        assert start["fields"]["release_fasta_urls"] == [_CANONICAL_URL]
        md5s = [
            c["fields"]["md5"]
            for c in emit.calls
            if c["event"] == "source.uniprot_release_fasta.file_done"
        ]
        assert len(md5s) == 1 and len(md5s[0]) == 32

    def test_isoforms_group_under_their_canonical_accession(self):
        # The varsplic file carries only isoforms; they must still land
        # grouped, which is what the aspect queries read.
        session = _make_mock_session()
        emit = _noop_emit
        bodies = {
            _CANONICAL_URL: _make_gz_response(FASTA_ONE),
            _VARSPLIC_URL: _make_gz_response(FASTA_ISOFORM),
        }
        added = []
        session.add_all.side_effect = lambda rows: added.extend(rows)
        with patch.object(
            self.op._uniprot_plugin._client.session,
            "get",
            side_effect=lambda url, **_kw: bodies[url],
        ):
            self.op.execute(
                session,
                {
                    "search_criteria": "reviewed:true",
                    "release_fasta_urls": [_CANONICAL_URL, _VARSPLIC_URL],
                },
                emit=emit,
            )
        proteins = {p.accession: p for p in added if hasattr(p, "accession")}
        assert proteins["P12345"].canonical_accession == "P12345"
        assert proteins["P12345"].is_canonical is True
        assert proteins["P12345-2"].canonical_accession == "P12345"
        assert proteins["P12345-2"].is_canonical is False
        assert proteins["P12345-2"].isoform_index == 2

    def test_summary_says_release_files_were_used(self):
        summary = self.op.summarize_payload(
            {
                "search_criteria": "reviewed:true",
                "release_fasta_urls": [_CANONICAL_URL, _VARSPLIC_URL],
            }
        )
        assert "release files=2" in summary
        summary_live = self.op.summarize_payload({"search_criteria": "reviewed:true"})
        assert "release files" not in summary_live


# ---------------------------------------------------------------------------
# Unit tests — execute() with mocked HTTP and session
# ---------------------------------------------------------------------------


class TestInsertProteinsOperationExecute:
    def setup_method(self):
        self.op = InsertProteinsOperation()

    def test_execute_returns_operation_result(self):
        session = _make_mock_session()
        emit = _capturing_emit()

        with patch.object(self.op._uniprot_plugin._client.session, "get", return_value=_make_mock_response(FASTA_ONE)):
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

        with patch.object(self.op._uniprot_plugin._client.session, "get", return_value=_make_mock_response(FASTA_ONE)):
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

        with patch.object(self.op._uniprot_plugin._client.session, "get", return_value=_make_mock_response(FASTA_TWO)):
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

        with patch.object(self.op._uniprot_plugin._client.session, "get", return_value=_make_mock_response(FASTA_ONE)):
            self.op.execute(
                session,
                {"search_criteria": "q", "compressed": False},
                emit=_noop_emit,
            )

        session.add_all.assert_called()

    def test_two_records_counts_correctly(self):
        session = _make_mock_session()
        emit = _capturing_emit()

        with patch.object(self.op._uniprot_plugin._client.session, "get", return_value=_make_mock_response(FASTA_TWO)):
            result = self.op.execute(
                session,
                {"search_criteria": "q", "compressed": False},
                emit=emit,
            )

        assert result.result["retrieved_records"] == 2
        assert result.result["proteins_inserted"] == 2

    def test_empty_page_does_not_flush(self):
        """Empty FASTA response → no records, no buffer flush, pages=0.

        Per F2A.6-real, ``pages`` counts DB-side buffer flushes; an
        HTTP page that returns zero records never triggers a flush.
        """
        session = _make_mock_session()
        emit = _capturing_emit()
        empty_resp = _make_mock_response("")
        with patch.object(self.op._uniprot_plugin._client.session, "get", return_value=empty_resp):
            result = self.op.execute(
                session,
                {"search_criteria": "q", "compressed": False},
                emit=emit,
            )
        assert result.result["retrieved_records"] == 0
        assert result.result["pages"] == 0

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

        with patch.object(self.op._uniprot_plugin._client.session, "get", side_effect=get_side_effect):
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

        with patch.object(self.op._uniprot_plugin._client.session, "get", return_value=resp) as mock_get:
            self.op.execute(
                session,
                {"search_criteria": "q", "compressed": True},
                emit=emit,
            )

        called_url = mock_get.call_args[0][0]
        assert "compressed=true" in called_url

    # NOTE: tests for ``op._total_results`` (X-Total-Results capture)
    # were removed in F2A.6-real step 3 (b). The plugin abstracts HTTP
    # away from the operation, and X-Total-Results was nice-to-have for
    # progress reporting, not load-bearing for correctness. Progress
    # totals now only flow when ``total_limit`` is set.

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
        with patch.object(op._uniprot_plugin._client.session, "get", side_effect=get_side_effect):
            result = op.execute(
                session,
                {"search_criteria": "q", "compressed": False},
                emit=emit,
            )

        # Per F2A.6-real, ``pages`` counts DB-side buffer flushes,
        # not HTTP pages. With 2 records and the default page_size=500,
        # only one final flush fires.
        assert result.result["pages"] == 1
        assert result.result["retrieved_records"] == 2
        # Second HTTP call URL should contain cursor (HTTP-level pagination
        # is the plugin's concern, but we verify the cursor was followed).
        assert "cursor=abc123" in called_urls[1]

    def test_network_failure_propagates(self):
        """HTTP errors propagate to caller."""
        import requests as req

        session = _make_mock_session()
        op = InsertProteinsOperation()

        with patch.object(
            op._uniprot_plugin._client.session,
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
        with patch.object(op._uniprot_plugin._client.session, "get", return_value=resp):
            result = op.execute(
                session,
                {"search_criteria": "q", "compressed": False},
                emit=emit,
            )

        assert result.result["isoform_records"] == 1

    def test_progress_emission_with_total_limit(self):
        """Progress events include _progress_total when ``total_limit`` is set.

        Per F2A.6-real, the operation no longer captures X-Total-Results
        from the HTTP response; only the user-set ``total_limit`` flows
        into ``_progress_total``. With ``page_size=1`` we force a flush
        so the page_done event actually fires.
        """
        session = _make_mock_session()
        emit = _capturing_emit()

        resp = _make_mock_response(FASTA_ONE)
        op = InsertProteinsOperation()
        with patch.object(op._uniprot_plugin._client.session, "get", return_value=resp):
            op.execute(
                session,
                {"search_criteria": "q", "compressed": False,
                 "total_limit": 100, "page_size": 1},
                emit=emit,
            )

        page_done_events = [c for c in emit.calls if c["event"] == "insert_proteins.page_done"]
        assert len(page_done_events) == 1
        fields = page_done_events[0]["fields"]
        assert fields["_progress_current"] == 1
        assert fields["_progress_total"] == 100

    def test_include_isoforms_false_omits_param(self):
        """include_isoforms=False does not add includeIsoform to URL."""
        session = _make_mock_session()
        resp = _make_mock_response(FASTA_ONE)
        op = InsertProteinsOperation()
        with patch.object(op._uniprot_plugin._client.session, "get", return_value=resp) as mock_get:
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
        with patch.object(op._uniprot_plugin._client.session, "get", return_value=_make_mock_response(FASTA_TWO)):
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
        with patch.object(op2._uniprot_plugin._client.session, "get", return_value=_make_mock_response(FASTA_TWO)):
            result2 = op2.execute(
                session,
                {"search_criteria": "organism_id:9606", "compressed": False},
                emit=_noop_emit,
            )
            session.commit()

    assert result2.result["proteins_inserted"] == 0
    assert result2.result["sequences_reused"] == 2
