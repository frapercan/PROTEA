"""A batch worker reads a photo's sequence identities once, not once per batch.

A run that excludes each query's own sequence asks for the identity of every
accession in the pool, in every batch. On the bank of cut 211 that is 1.5 M
accessions, and reading them in chunks took about 100 s of each batch's two
minutes while the search waited (via 1, batch 1, 2026-11-21). A photo never
changes once written, so ``load_sequence_identities`` now reads the whole photo
once per process (about 4 to 6 s on that photo) and answers every later batch
from memory. On the real photo the answer is identical to the chunked read.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest

from protea.core.operations.predict_go_terms import _sequence_identity as identity

PHOTOS = [uuid.uuid4() for _ in range(6)]
BANKS = {str(uuid.uuid4()): photo for photo in PHOTOS}
MEMBERS = {"P1": 11, "P2": 12, "P3": 11}


class _Session:
    """Answers the whole-photo read with ``MEMBERS`` and counts the reads."""

    def __init__(self) -> None:
        self.reads = 0

    def execute(self, _statement: Any) -> list[tuple[str, int]]:
        self.reads += 1
        return list(MEMBERS.items())


@pytest.fixture(autouse=True)
def _a_fresh_process(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(identity, "photo_of", lambda _session, bank: BANKS.get(str(bank)))
    identity.forget_photo_identities()
    yield
    identity.forget_photo_identities()


def _bank(photo: uuid.UUID) -> str:
    return next(bank for bank, p in BANKS.items() if p == photo)


def test_the_photo_is_read_once_for_every_batch() -> None:
    session = _Session()
    bank = _bank(PHOTOS[0])
    first = identity.load_sequence_identities(session, {"P1", "P2"}, bank)
    second = identity.load_sequence_identities(session, {"P2", "P3"}, bank)
    assert first == {"P1": "11", "P2": "12"}
    assert second == {"P2": "12", "P3": "11"}
    assert session.reads == 1


def test_an_accession_the_photo_does_not_hold_is_left_out() -> None:
    session = _Session()
    bank = _bank(PHOTOS[0])
    assert identity.load_sequence_identities(session, {"P1", "NOPE"}, bank) == {"P1": "11"}
    assert identity.load_sequence_identities(session, {"NOPE"}, bank) is None


def test_only_the_photos_used_last_are_kept() -> None:
    session = _Session()
    for photo in PHOTOS[:5]:
        identity.load_sequence_identities(session, {"P1"}, _bank(photo))
    assert session.reads == 5
    # The first photo was the least recently used of five, with four kept.
    identity.load_sequence_identities(session, {"P1"}, _bank(PHOTOS[0]))
    assert session.reads == 6
    identity.load_sequence_identities(session, {"P1"}, _bank(PHOTOS[4]))
    assert session.reads == 6
