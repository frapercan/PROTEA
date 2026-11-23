"""Every member's own taxon, read from the cut's GOA rows: the rule, the load and the fill.

The rule is checked on a small rows file. The load and the fill run on a photo built
and loaded the way ``tests/test_load_cut_photo.py`` does it.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from protea.core.cut_photo.taxa import member_taxa, own_taxon
from protea.core.operations.declare_member_taxa import (
    DeclareMemberTaxaOperation,
    MemberTaxaRefused,
)
from protea.infrastructure.orm.models.annotation.cut_photo import CutPhoto, CutPhotoMember
from tests.test_load_cut_photo import _load, build_photo, pg_session  # noqa: F401


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("taxon:9606", 9606),
        ("taxon:9606|taxon:562", 9606),
        (" taxon:10090 ", 10090),
        ("", None),
        (None, None),
        ("9606", None),
        ("taxon:human", None),
    ],
)
def test_the_own_taxon_is_the_first_of_the_field(text: str | None, expected: int | None) -> None:
    assert own_taxon(text) == expected


def test_a_member_whose_rows_disagree_is_left_without_a_taxon(tmp_path: Path) -> None:
    rows = tmp_path / "rows.parquet"
    pq.write_table(
        pa.table(
            {
                "accession": ["P1", "P1", "P2", "P2", "P3", "P4"],
                "taxon": [
                    "taxon:9606",
                    "taxon:9606|taxon:562",
                    "taxon:9606",
                    "taxon:10090",
                    "taxon:4932",
                    "",
                ],
            }
        ),
        rows,
        row_group_size=2,
    )
    for batch_rows in (1, 3, 10):
        taxa, counts = member_taxa(rows, batch_rows=batch_rows)
        assert taxa == {"P1": 9606, "P3": 4932}
        assert counts["members_with_two_own_taxa"] == 1
        assert counts["members_with_two_own_taxa_examples"] == ["P2"]
        assert counts["rows_without_a_readable_taxon"] == 1


def _members(session: Session) -> dict[str, int | None]:
    rows = session.execute(select(CutPhotoMember.accession, CutPhotoMember.taxon_id)).all()
    return dict(rows)


@pytest.mark.integration
def test_a_photo_loads_with_its_members_taxa(pg_session: Session, tmp_path: Path) -> None:  # noqa: F811
    photo_dir, photo_sha = build_photo(tmp_path)
    _load(pg_session, photo_dir, photo_sha)
    expected, _ = member_taxa(photo_dir / "rows.parquet")
    members = _members(pg_session)
    assert expected and all(members[a] == t for a, t in expected.items())
    assert all(members[a] is None for a in members if a not in expected)


@pytest.mark.integration
def test_the_fill_gives_a_loaded_photo_the_same_taxa(pg_session: Session, tmp_path: Path) -> None:  # noqa: F811
    photo_dir, photo_sha = build_photo(tmp_path)
    _load(pg_session, photo_dir, photo_sha)
    loaded = _members(pg_session)
    pg_session.execute(update(CutPhotoMember).values(taxon_id=None))
    pg_session.commit()
    payload = {"photo_dir": str(photo_dir), "manifest_sha256": photo_sha}
    result = DeclareMemberTaxaOperation().execute(pg_session, payload, emit=lambda *a, **k: None)
    pg_session.commit()
    assert _members(pg_session) == loaded
    with_taxon = sum(t is not None for t in loaded.values())
    assert result.result["members_with_taxon"] == with_taxon
    assert result.result["members_without_taxon"] == len(loaded) - with_taxon


@pytest.mark.integration
def test_the_fill_refuses_rows_the_photo_did_not_record(
    pg_session: Session,  # noqa: F811
    tmp_path: Path,
) -> None:
    photo_dir, photo_sha = build_photo(tmp_path)
    _load(pg_session, photo_dir, photo_sha)
    table = pq.read_table(photo_dir / "rows.parquet")
    pq.write_table(table.slice(1), photo_dir / "rows.parquet")
    payload = {"photo_dir": str(photo_dir), "manifest_sha256": photo_sha}
    with pytest.raises(MemberTaxaRefused, match="is not the rows.parquet"):
        DeclareMemberTaxaOperation().execute(pg_session, payload, emit=lambda *a, **k: None)
    unknown = {"photo_dir": str(photo_dir), "manifest_sha256": hashlib.sha256(b"x").hexdigest()}
    with pytest.raises(MemberTaxaRefused, match="no loaded photo"):
        DeclareMemberTaxaOperation().execute(pg_session, unknown, emit=lambda *a, **k: None)
    assert pg_session.execute(select(CutPhoto.id)).scalar_one() is not None
