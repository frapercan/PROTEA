"""Unit tests for ``RefreshGoaReleaseDatesOperation``.

The HTTP layer is fully mocked: tests cover the listing parser, the
upsert path against AnnotationSet, and the three idempotency
properties described in the slice (silent skip when the version has
no matching DB row; leave-as-NULL when the DB version is missing from
the listing; re-run is a no-op on the second pass).
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from protea.core.operations.refresh_goa_release_dates import (
    RefreshGoaReleaseDatesOperation,
    RefreshGoaReleaseDatesPayload,
    _parse_listing,
)
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet

_LISTING_HTML = """
<html><body>
<table>
<tr><td valign="top">&nbsp;</td>
<td><a href="goa_uniprot_all.gaf.225.gz">goa_uniprot_all.gaf.225.gz</a></td>
<td align="right">2025-04-01 12:30  </td><td align="right"> 21G</td></tr>

<tr><td valign="top">&nbsp;</td>
<td><a href="goa_uniprot_all.gaf.226.gz">goa_uniprot_all.gaf.226.gz</a></td>
<td align="right">2025-05-03 16:43  </td><td align="right"> 21G</td></tr>

<tr><td valign="top">&nbsp;</td>
<td><a href="goa_uniprot_all.gaf.227.gz">goa_uniprot_all.gaf.227.gz</a></td>
<td align="right">2025-06-04 09:11  </td><td align="right"> 21G</td></tr>

<tr><td valign="top">&nbsp;</td>
<td><a href="goa_uniprot_all.gpa.227.gz">goa_uniprot_all.gpa.227.gz</a></td>
<td align="right">2025-06-04 09:13  </td><td align="right"> 5G</td></tr>

<tr><td valign="top">&nbsp;</td>
<td><a href="goa_uniprot_all.gpi.227.gz">goa_uniprot_all.gpi.227.gz</a></td>
<td align="right">2025-06-04 09:15  </td><td align="right"> 2G</td></tr>

<tr><td valign="top">&nbsp;</td>
<td><a href="README">README</a></td>
<td align="right">2024-01-01 00:00  </td><td align="right"> 1K</td></tr>
</table>
</body></html>
"""


_noop_emit = lambda *_: None  # noqa: E731


class TestRefreshGoaReleaseDatesPayload:
    def test_defaults(self) -> None:
        p = RefreshGoaReleaseDatesPayload.model_validate({})
        assert p.listing_url.endswith("/UNIPROT/")
        assert p.timeout_seconds == 60

    def test_empty_url_rejected(self) -> None:
        with pytest.raises(ValueError):
            RefreshGoaReleaseDatesPayload.model_validate({"listing_url": "   "})

    def test_zero_timeout_rejected(self) -> None:
        with pytest.raises(ValueError):
            RefreshGoaReleaseDatesPayload.model_validate({"timeout_seconds": 0})


class TestParseListing:
    def test_extracts_gaf_releases(self) -> None:
        parsed = dict(_parse_listing(_LISTING_HTML))
        assert set(parsed.keys()) == {225, 226, 227}

    def test_excludes_gpa_and_gpi_and_readme(self) -> None:
        parsed = dict(_parse_listing(_LISTING_HTML))
        # the gpa/gpi rows mention 227 but the regex anchors on
        # ``goa_uniprot_all.gaf.`` so only one 227 entry survives, and
        # its timestamp comes from the ``.gaf.`` row, not from the
        # adjacent ``.gpa.`` row.
        assert parsed[227] == datetime(2025, 6, 4, 9, 11, tzinfo=UTC)

    def test_timestamps_are_utc(self) -> None:
        parsed = dict(_parse_listing(_LISTING_HTML))
        assert parsed[226] == datetime(2025, 5, 3, 16, 43, tzinfo=UTC)

    def test_empty_html(self) -> None:
        assert _parse_listing("") == []

    def test_malformed_date_is_skipped(self) -> None:
        bad = (
            '<a href="goa_uniprot_all.gaf.200.gz">goa_uniprot_all.gaf.200.gz</a>'
            "blah 9999-99-99 99:99"
        )
        assert _parse_listing(bad) == []


def _make_set(version: str | None, *, published=None) -> AnnotationSet:
    """Build a detached AnnotationSet without hitting the DB."""
    s = AnnotationSet(
        source="goa",
        source_version=version,
        ontology_snapshot_id=uuid.uuid4(),
    )
    s.id = uuid.uuid4()
    s.source_published_at = published
    return s


class TestExecute:
    def _fake_session(self, rows: list[AnnotationSet]) -> MagicMock:
        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = rows
        return session

    def _run(self, session, html=_LISTING_HTML):
        op = RefreshGoaReleaseDatesOperation()
        mock_resp = MagicMock()
        mock_resp.text = html
        mock_resp.raise_for_status = MagicMock()
        with patch(
            "protea.core.operations.refresh_goa_release_dates.requests.get",
            return_value=mock_resp,
        ):
            return op.execute(session, {}, emit=_noop_emit)

    def test_updates_matching_set(self) -> None:
        row = _make_set("226")
        session = self._fake_session([row])
        result = self._run(session)
        assert result.result["parsed_releases"] == 3
        assert result.result["matched_annotation_sets"] == 1
        assert result.result["updated"] == 1
        # one UPDATE statement was emitted
        assert session.execute.call_count == 1

    def test_silently_skips_unknown_versions(self) -> None:
        """DB has a version that doesn't appear in the FTP listing."""
        row = _make_set("999")
        session = self._fake_session([row])
        result = self._run(session)
        assert result.result["matched_annotation_sets"] == 0
        assert result.result["updated"] == 0
        session.execute.assert_not_called()

    def test_listing_version_missing_from_db_is_no_op(self) -> None:
        """FTP listing carries 225-227, DB has only 226: only 226 updates."""
        row = _make_set("226")
        session = self._fake_session([row])
        result = self._run(session)
        assert result.result["parsed_releases"] == 3
        assert result.result["updated"] == 1

    def test_repeat_run_is_no_op(self) -> None:
        already = datetime(2025, 5, 3, 16, 43, tzinfo=UTC)
        row = _make_set("226", published=already)
        session = self._fake_session([row])
        result = self._run(session)
        assert result.result["matched_annotation_sets"] == 1
        assert result.result["updated"] == 0
        session.execute.assert_not_called()

    def test_non_integer_source_version_is_skipped(self) -> None:
        row = _make_set("not-a-number")
        session = self._fake_session([row])
        result = self._run(session)
        assert result.result["matched_annotation_sets"] == 0
        session.execute.assert_not_called()

    def test_null_source_version_is_skipped(self) -> None:
        row = _make_set(None)
        session = self._fake_session([row])
        result = self._run(session)
        assert result.result["matched_annotation_sets"] == 0
        session.execute.assert_not_called()

    def test_emits_start_parsed_done(self) -> None:
        row = _make_set("226")
        session = self._fake_session([row])
        emit = MagicMock()
        op = RefreshGoaReleaseDatesOperation()
        mock_resp = MagicMock()
        mock_resp.text = _LISTING_HTML
        mock_resp.raise_for_status = MagicMock()
        with patch(
            "protea.core.operations.refresh_goa_release_dates.requests.get",
            return_value=mock_resp,
        ):
            op.execute(session, {}, emit=emit)
        events = [c.args[0] for c in emit.call_args_list]
        assert "refresh_goa_release_dates.start" in events
        assert "refresh_goa_release_dates.parsed" in events
        assert "refresh_goa_release_dates.applied" in events
        assert "refresh_goa_release_dates.done" in events

    def test_summarize_payload(self) -> None:
        op = RefreshGoaReleaseDatesOperation()
        summary = op.summarize_payload({})
        assert "ftp.ebi.ac.uk" in summary


class TestRegistry:
    def test_operation_is_registered(self) -> None:
        from protea.core.operation_catalog import build_operation_registry

        reg = build_operation_registry()
        op = reg.get("refresh_goa_release_dates")
        assert isinstance(op, RefreshGoaReleaseDatesOperation)
