"""The universe refuses a cut after the mark, a UniProt release from the ceiling on,
and a source published after the cut it serves (ADR-D60, decision 1).

The 227->230 test was spent on 2026-10-27, so 228 is the first cut no door may
open and 2025_04 the first UniProt release no door may read. Each door is tried
with exactly those two values, and with the last ones it still accepts, so a
guard that drifted by one release in either direction fails here.
"""

from __future__ import annotations

import gzip
import io
import tarfile
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from protea.core.cut_photo.measure import (
    SWISSPROT_FASTA_MEMBER,
    PassSource,
    PhotoGuardError,
    check_dates,
    neighbours_pass,
    superset_pass,
)
from protea.core.operations.load_interpro_go_mapping import (
    LoadInterProGoMappingOperation,
    LoadInterProGoMappingPayload,
)
from protea.core.operations.load_swissprot_release import (
    LoadSwissProtReleaseOperation,
    LoadSwissProtReleasePayload,
)
from protea.core.source_guards import (
    SourceGuardError,
    is_at_or_after_the_uniprot_ceiling,
    refuse_a_cut_after_the_mark,
    refuse_a_dated_source,
    refuse_a_source_published_after_its_cut,
    refuse_a_uniprot_release_at_or_after_the_ceiling,
    uniprot_release_key,
)

#: GOA 211 was published on 2022-09-16 and GOA 227 on 2025-09-04.
CUT_211 = date(2022, 9, 16)
CUT_227 = date(2025, 9, 4)


def _noop_emit(*_args: object, **_kwargs: object) -> None:
    return None


class TestTheGuards:
    def test_228_is_after_the_mark_and_227_is_not(self) -> None:
        with pytest.raises(SourceGuardError, match="cut 228 is after the mark 227"):
            refuse_a_cut_after_the_mark(228)
        refuse_a_cut_after_the_mark(227)

    def test_2025_04_is_the_ceiling_and_2025_03_is_below_it(self) -> None:
        assert is_at_or_after_the_uniprot_ceiling("2025_04")
        assert is_at_or_after_the_uniprot_ceiling("2026_01")
        assert not is_at_or_after_the_uniprot_ceiling("2025_03")
        assert not is_at_or_after_the_uniprot_ceiling("2016_09")

    def test_the_ceiling_refusal_names_the_source_and_the_release(self) -> None:
        with pytest.raises(SourceGuardError, match="UniRef 2025_04 is at or after 2025_04"):
            refuse_a_uniprot_release_at_or_after_the_ceiling("2025_04", source="UniRef")

    def test_releases_compare_as_numbers_not_as_text(self) -> None:
        assert uniprot_release_key("2025_10") > uniprot_release_key("2025_04")
        assert uniprot_release_key(" 2025_03 ") == (2025, 3)

    @pytest.mark.parametrize("bad", ["2025-04", "25_04", "2025_x", "latest", ""])
    def test_a_release_the_guard_cannot_place_is_refused(self, bad: str) -> None:
        with pytest.raises(SourceGuardError, match="not a UniProt release name"):
            is_at_or_after_the_uniprot_ceiling(bad)

    def test_a_source_after_its_cut_is_refused_and_the_same_day_is_not(self) -> None:
        with pytest.raises(SourceGuardError, match="after the cut it serves"):
            refuse_a_source_published_after_its_cut(
                source="NCBI taxonomy", published=date(2022, 10, 1), cut_date=CUT_211
            )
        refuse_a_source_published_after_its_cut(
            source="NCBI taxonomy", published=CUT_211, cut_date=CUT_211
        )

    def test_a_dated_source_is_refused_at_the_mark_first(self) -> None:
        # Both rules fail here; the mark is the one reported.
        with pytest.raises(SourceGuardError, match="after the mark"):
            refuse_a_dated_source(
                source="x", published=date(2026, 1, 1), cut_release=228, cut_date=CUT_227
            )

    def test_the_photo_refusal_is_a_source_refusal(self) -> None:
        assert issubclass(PhotoGuardError, SourceGuardError)

    @pytest.mark.parametrize(
        ("published", "cut_release"),
        [(CUT_227, 228), (date(2025, 9, 5), 227)],
        ids=["a cut after the mark", "a source after its cut"],
    )
    def test_the_photo_refuses_what_the_loaders_refuse_in_the_same_words(
        self, published: date, cut_release: int
    ) -> None:
        # One implementation of the rules where the photo is built and where
        # the universe is loaded: two copies drift apart the day one is edited.
        with pytest.raises(SourceGuardError) as loader:
            refuse_a_dated_source(
                source="GOA 227", published=published, cut_release=cut_release, cut_date=CUT_227
            )
        with pytest.raises(PhotoGuardError) as photo:
            check_dates(
                source="GOA 227", published=published, cut_release=cut_release, cut_date=CUT_227
            )
        assert str(photo.value) == str(loader.value)


class TestSwissProtStopsBelowTheCeiling:
    def _payload(self, release: str) -> dict[str, object]:
        return {
            "release": release,
            "published_at": "2025-06-18",
            "expected_sha256": "0" * 64,
        }

    @pytest.mark.parametrize("release", ["2025_04", "2026_01"])
    def test_the_payload_refuses_the_ceiling_and_after(self, release: str) -> None:
        with pytest.raises(ValidationError, match=f"Swiss-Prot {release} is at or after 2025_04"):
            LoadSwissProtReleasePayload.model_validate(self._payload(release))

    def test_2025_03_still_loads(self) -> None:
        # The release cut 227 pairs with, by the mark (ADR-D59).
        assert LoadSwissProtReleasePayload.model_validate(self._payload("2025_03")).release == (
            "2025_03"
        )

    def test_nothing_is_read_before_the_refusal(self) -> None:
        op = LoadSwissProtReleaseOperation()
        with patch.object(op, "_read") as read:
            with pytest.raises(ValidationError):
                op.execute(None, self._payload("2025_04"), emit=_noop_emit)  # type: ignore[arg-type]
        read.assert_not_called()


def _archive(tmp_path: Path) -> Path:
    path = tmp_path / "uniprot_sprot-only.tar.gz"
    payload = gzip.compress(b">sp|P11111|EX_HUMAN Example\nMKV\n")
    with tarfile.open(path, "w:gz") as tar:
        info = tarfile.TarInfo(SWISSPROT_FASTA_MEMBER)
        info.size = len(payload)
        tar.addfile(info, io.BytesIO(payload))
    return path


class TestThePhotoPassesStopBelowTheCeiling:
    """The two UniProt passes, with a publication date that alone would pass."""

    def _source(self, tmp_path: Path, release: str) -> PassSource:
        return PassSource(
            release=release, published=CUT_227, cut_release=227, cut_date=CUT_227, out=tmp_path
        )

    def test_the_superset_pass_refuses_2025_04(self, tmp_path: Path) -> None:
        with pytest.raises(PhotoGuardError, match="UniProt 2025_04"):
            superset_pass(_archive(tmp_path), source=self._source(tmp_path / "s", "2025_04"))

    def test_the_neighbours_pass_refuses_2025_04(self, tmp_path: Path) -> None:
        with pytest.raises(PhotoGuardError, match="UniRef 2025_04"):
            neighbours_pass([], superset=frozenset(), source=self._source(tmp_path, "2025_04"))

    def test_2025_03_passes_for_cut_227(self, tmp_path: Path) -> None:
        record = superset_pass(_archive(tmp_path), source=self._source(tmp_path / "s", "2025_03"))
        assert record["accessions"] == 1

    def test_a_release_name_it_cannot_place_is_refused(self, tmp_path: Path) -> None:
        with pytest.raises(PhotoGuardError, match="not a UniProt release name"):
            neighbours_pass([], superset=frozenset(), source=self._source(tmp_path, "latest"))


class TestAnInterProMappingDatedAgainstItsCut:
    #: A file that names its own release, unlike the default endpoint.
    ARCHIVED = "https://example.org/archive/2022-07-01/interpro2go"

    def _payload(self, **dating: object) -> dict[str, object]:
        return {"source_version": "go-2022-07-01", **dating}

    def test_a_mapping_for_cut_228_is_refused(self) -> None:
        with pytest.raises(ValidationError, match="cut 228 is after the mark 227"):
            LoadInterProGoMappingPayload.model_validate(
                self._payload(published_at="2025-09-01", cut_release=228, cut_date="2025-11-10")
            )

    def test_a_mapping_published_after_its_cut_is_refused(self) -> None:
        with pytest.raises(ValidationError, match="published on 2022-10-01, after the cut"):
            LoadInterProGoMappingPayload.model_validate(
                self._payload(published_at="2022-10-01", cut_release=211, cut_date="2022-09-16")
            )

    def test_a_mapping_dated_on_or_before_its_cut_is_accepted(self) -> None:
        p = LoadInterProGoMappingPayload.model_validate(
            self._payload(
                mapping_url=self.ARCHIVED,
                published_at="2022-07-01",
                cut_release=211,
                cut_date="2022-09-16",
            )
        )
        assert (p.published_at, p.cut_release, p.cut_date) == ("2022-07-01", 211, "2022-09-16")

    @pytest.mark.parametrize(
        "url",
        [
            None,
            "https://ftp.ebi.ac.uk/pub/databases/GO/goa/external2go/interpro2go",
            "http://ftp.ebi.ac.uk/pub/databases/GO/goa/external2go/interpro2go/",
        ],
        ids=["left out", "named", "another scheme"],
    )
    def test_a_dated_load_from_the_default_url_is_refused(self, url: str | None) -> None:
        # The default endpoint serves whatever file is current when the job
        # runs, so a date declared for it says nothing about the bytes read.
        dating: dict[str, object] = {
            "published_at": "2022-07-01",
            "cut_release": 211,
            "cut_date": "2022-09-16",
        }
        if url is not None:
            dating["mapping_url"] = url
        with pytest.raises(ValidationError, match="must name its own file"):
            LoadInterProGoMappingPayload.model_validate(self._payload(**dating))

    @pytest.mark.parametrize(
        "partial",
        [
            {"cut_release": 211},
            {"published_at": "2022-07-01"},
            {"cut_release": 211, "cut_date": "2022-09-16"},
        ],
    )
    def test_a_partial_dating_is_refused(self, partial: dict[str, object]) -> None:
        with pytest.raises(ValidationError, match="all three or none"):
            LoadInterProGoMappingPayload.model_validate(self._payload(**partial))

    def test_an_undated_load_runs_as_before(self) -> None:
        p = LoadInterProGoMappingPayload.model_validate(self._payload())
        assert (p.published_at, p.cut_release, p.cut_date) == (None, None, None)

    def test_nothing_is_downloaded_before_the_refusal(self) -> None:
        payload = self._payload(published_at="2025-09-01", cut_release=228, cut_date="2025-11-10")
        with patch("protea.core.operations.load_interpro_go_mapping.requests.get") as get:
            with pytest.raises(ValidationError):
                LoadInterProGoMappingOperation().execute(MagicMock(), payload, emit=_noop_emit)
        get.assert_not_called()

    def test_the_dating_is_recorded_when_the_load_starts(self) -> None:
        events: list[tuple[str, dict[str, object]]] = []

        def emit(event: str, _message: object, fields: dict[str, object], _level: str) -> None:
            events.append((event, fields))

        response = MagicMock(text="", status_code=200)
        payload = self._payload(
            mapping_url=self.ARCHIVED,
            published_at="2022-07-01",
            cut_release=211,
            cut_date="2022-09-16",
        )
        with patch(
            "protea.core.operations.load_interpro_go_mapping.requests.get",
            return_value=response,
        ):
            LoadInterProGoMappingOperation().execute(MagicMock(), payload, emit=emit)
        started = dict(events)["load_interpro_go_mapping.start"]
        assert started["cut_release"] == 211
        assert started["published_at"] == "2022-07-01"
