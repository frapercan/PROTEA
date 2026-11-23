"""A kNN run that transfers from a photo searches with the photo's sequences (ADR-D59, ADR-D62).

The photo is built and loaded the way ``tests/test_load_cut_photo.py`` does it; its
members carry their sequence at the cut, while their ``protein.sequence_id`` (today's)
is empty, which is the development universe.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.core.operations.predict_go_terms._photo_sequences import member_sequences, photo_of
from protea.core.operations.predict_go_terms._sequence_identity import load_sequence_identities
from protea.infrastructure.orm.models.annotation.cut_photo import CutPhoto, CutPhotoMember
from protea.infrastructure.orm.models.sequence.sequence import Sequence
from tests.test_load_cut_photo import _load, build_photo, pg_session  # noqa: F401


@pytest.fixture()
def db(pg_session: Session) -> Session:  # noqa: F811
    return pg_session


@pytest.mark.integration
def test_a_photos_set_resolves_its_members_sequences_at_the_cut(
    db: Session, tmp_path: Path
) -> None:
    photo_dir, sha = build_photo(tmp_path)
    _load(db, photo_dir, sha)
    photo = db.execute(select(CutPhoto)).scalar_one()
    assert photo_of(db, photo.annotation_set_id) == photo.id
    assert photo_of(db, uuid.uuid4()) is None and photo_of(db, "not-an-id") is None

    members = member_sequences(photo.id)
    rows = dict(db.execute(select(members.c.accession, members.c.sequence_id)).all())
    expected = dict(
        db.execute(
            select(CutPhotoMember.accession, Sequence.id)
            .join(Sequence, Sequence.sequence_hash == CutPhotoMember.sequence_md5)
            .where(CutPhotoMember.photo_id == photo.id)
        ).all()
    )
    assert rows == expected and rows

    accessions = set(rows)
    by_photo = load_sequence_identities(db, accessions, photo.annotation_set_id)
    assert by_photo == {a: str(s) for a, s in rows.items()}
    # without the photo, today's sequences: none in this universe
    assert load_sequence_identities(db, accessions) is None
