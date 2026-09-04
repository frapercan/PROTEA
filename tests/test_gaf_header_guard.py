"""Unit tests for the GAF ``!go-version`` guard.

No network: the rule is pure and the fetch is patched.  These pin the defect
that produced GOA 220 — a GAF bound to an ontology eleven months after the
build it declares — and the two ways a guard can quietly stop guarding.
"""

from __future__ import annotations

import zlib
from datetime import date
from unittest.mock import MagicMock, patch

import pytest
import requests

from protea.core.operations._gaf_header import (
    assert_not_newer_than_declared,
    declared_release,
    fetch_header,
    release_date,
)

_URL = "https://example.invalid/goa_uniprot_all.gaf.220.gz"

_HEADER = (
    "!gaf-version: 2.2\n"
    "!\n"
    "!date-generated: 2024-04-16 17:12 \n"
    "!generated-by: UniProt\n"
    "!go-version: http://purl.obolibrary.org/obo/go/releases/2024-04-13/"
    "extensions/go-plus.owl\n"
    "!\n"
    "A0A024R161\tGO:0005515\t...\n"
)


class TestReleaseDate:
    def test_reads_the_stored_form(self):
        assert release_date("releases/2024-03-28") == date(2024, 3, 28)

    def test_reads_the_owl_url_form(self):
        url = "http://purl.obolibrary.org/obo/go/releases/2025-08-31/x/go-plus.owl"
        assert release_date(url) == date(2025, 8, 31)

    def test_returns_none_when_absent(self):
        assert release_date("some-unversioned-thing") is None
        assert release_date("") is None


class TestDeclaredRelease:
    def test_finds_the_go_version_line(self):
        assert declared_release(_HEADER) == date(2024, 4, 13)

    def test_none_when_the_header_lacks_it(self):
        assert declared_release("!gaf-version: 2.2\n!\ndata\n") is None

    def test_stops_at_the_first_data_line(self):
        """A ``!go-version`` appearing in the body is data, not a header."""
        body = "!gaf-version: 2.2\nA0A024R161\t!go-version: releases/2030-01-01\n"
        assert declared_release(body) is None


class TestTheRule:
    def test_accepts_a_snapshot_at_or_before_the_declared_build(self):
        checked = assert_not_newer_than_declared(
            gaf_url=_URL,
            obo_version="releases/2024-03-28",
            declared=date(2024, 4, 13),
            allow_unverified=False,
        )
        assert checked == {
            "declared": "releases/2024-04-13",
            "bound": "releases/2024-03-28",
        }

    def test_accepts_the_exact_declared_build(self):
        assert (
            assert_not_newer_than_declared(
                gaf_url=_URL,
                obo_version="releases/2024-04-13",
                declared=date(2024, 4, 13),
                allow_unverified=False,
            )["bound"]
            == "releases/2024-04-13"
        )

    def test_refuses_the_goa_220_defect(self):
        """The real incident: GOA 220 bound eleven months into its own future."""
        with pytest.raises(ValueError, match="which is later"):
            assert_not_newer_than_declared(
                gaf_url=_URL,
                obo_version="releases/2025-03-16",
                declared=date(2024, 4, 13),
                allow_unverified=False,
            )

    def test_proximity_is_not_the_rule(self):
        """2024-04-18 is five days away and still refused; 2024-03-28 is
        sixteen days away and accepted. Later is what matters, not closer."""
        with pytest.raises(ValueError, match="which is later"):
            assert_not_newer_than_declared(
                gaf_url=_URL,
                obo_version="releases/2024-04-18",
                declared=date(2024, 4, 13),
                allow_unverified=False,
            )

    def test_refuses_a_snapshot_with_no_release_date(self):
        with pytest.raises(ValueError, match="carries no release date"):
            assert_not_newer_than_declared(
                gaf_url=_URL,
                obo_version="some-local-build",
                declared=date(2024, 4, 13),
                allow_unverified=False,
            )


class TestUnverifiedIsNotVerified:
    def test_refuses_when_the_header_could_not_be_read(self):
        """A guard that passes silently when it cannot check is not a guard."""
        with pytest.raises(ValueError, match="allow_unverified_ontology"):
            assert_not_newer_than_declared(
                gaf_url=_URL,
                obo_version="releases/2024-03-28",
                declared=None,
                allow_unverified=False,
            )

    def test_passes_only_when_the_skip_is_declared(self):
        checked = assert_not_newer_than_declared(
            gaf_url=_URL,
            obo_version="releases/2024-03-28",
            declared=None,
            allow_unverified=True,
        )
        assert checked["declared"] is None


class TestFetchHeader:
    def _response(self, body: bytes) -> MagicMock:
        resp = MagicMock()
        resp.content = body
        resp.raise_for_status = MagicMock()
        return resp

    def test_asks_only_for_the_leading_bytes(self):
        gz = zlib.compressobj(wbits=16 + zlib.MAX_WBITS)
        body = gz.compress(_HEADER.encode()) + gz.flush()
        with patch(
            "protea.core.operations._gaf_header.requests.get",
            return_value=self._response(body),
        ) as get:
            assert declared_release(fetch_header(_URL, 30)) == date(2024, 4, 13)
        assert get.call_args.kwargs["headers"]["Range"].startswith("bytes=0-")

    def test_plain_text_gaf_is_not_gunzipped(self):
        with patch(
            "protea.core.operations._gaf_header.requests.get",
            return_value=self._response(_HEADER.encode()),
        ):
            assert declared_release(fetch_header(_URL[:-3], 30)) == date(2024, 4, 13)

    def test_a_truncated_gzip_still_yields_a_complete_header(self):
        """The ranged body ends mid-stream; that is expected, not an error.

        Here the cut lands in the annotation rows, well past the header.
        """
        gz = zlib.compressobj(wbits=16 + zlib.MAX_WBITS)
        rows = "".join(f"P{i:05d}\tGO:0005515\t\n" for i in range(4000))
        full = gz.compress((_HEADER + rows).encode()) + gz.flush()
        with patch(
            "protea.core.operations._gaf_header.requests.get",
            return_value=self._response(full[: int(len(full) * 0.9)]),
        ):
            assert declared_release(fetch_header(_URL, 30)) == date(2024, 4, 13)

    def test_a_cut_inside_the_version_line_reads_as_unverified(self):
        """Half of a release date must never parse as a different date.

        An unterminated tail is dropped, so the caller sees "could not
        verify" — which refuses unless the skip is declared — rather than a
        confident wrong answer.
        """
        cut = _HEADER[: _HEADER.index("releases/2024-04-13") + len("releases/2024-")]
        with patch(
            "protea.core.operations._gaf_header.requests.get",
            return_value=self._response(cut.encode()),
        ):
            assert declared_release(fetch_header(_URL[:-3], 30)) is None

    def test_network_failure_reads_as_unverified_not_as_valid(self):
        with patch(
            "protea.core.operations._gaf_header.requests.get",
            side_effect=requests.RequestException("boom"),
        ):
            assert fetch_header(_URL, 30) == ""
