"""PROTEA's half of counting depth in sequences.

The method ranks a neighbour list by distinct sequence when it is handed
a map from accession to sequence identity. This is the producer of that
map. What matters is that it is complete for the neighbours it is asked
about, and that it is read unconditionally rather than riding on a
feature that is usually off.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from protea.core.operations.predict_go_terms._sequence_identity import (
    load_sequence_identities,
)
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.sequence.sequence import Sequence


@pytest.fixture()
def session() -> Session:
    """A store holding two proteins on one sequence and one on another."""
    engine = create_engine("sqlite:///:memory:")
    Sequence.__table__.create(engine)
    Protein.__table__.create(engine)
    with Session(engine) as s:
        shared = Sequence(sequence="MKV", sequence_hash="h-shared")
        alone = Sequence(sequence="MQT", sequence_hash="h-alone")
        s.add_all([shared, alone])
        s.flush()
        s.add_all([
            Protein(accession="P1", canonical_accession="P1", sequence_id=shared.id),
            Protein(accession="P2", canonical_accession="P2", sequence_id=shared.id),
            Protein(accession="P3", canonical_accession="P3", sequence_id=alone.id),
        ])
        s.commit()
        yield s


def test_two_proteins_on_one_sequence_get_one_identity(session: Session) -> None:
    """The whole point: P1 and P2 are one point of the space, not two."""
    identities = load_sequence_identities(session, {"P1", "P2", "P3"})
    assert identities["P1"] == identities["P2"]
    assert identities["P3"] != identities["P1"]


def test_it_maps_every_accession_it_was_asked_about(session: Session) -> None:
    """The method refuses a partial map, so a gap here would stop a run."""
    asked = {"P1", "P2", "P3"}
    assert set(load_sequence_identities(session, asked)) == asked


def test_an_accession_that_is_not_a_protein_here_is_simply_absent(
    session: Session,
) -> None:
    """Absent, not invented. A partial map is what the method refuses."""
    identities = load_sequence_identities(session, {"P1", "NOPE"})
    assert identities is not None
    assert "NOPE" not in identities
    assert identities["P1"]


def test_a_bank_with_no_proteins_here_reads_as_absent_not_as_partial(
    session: Session,
) -> None:
    """Nothing mapped is "this bank cannot be counted in sequences", said
    once for the whole run. A partial map is a different statement and the
    method raises on it; an empty dict would be read as the second."""
    assert load_sequence_identities(session, {"NOPE", "ALSO_NOPE"}) is None


def test_asking_about_nothing_returns_nothing(session: Session) -> None:
    assert load_sequence_identities(session, set()) is None


def test_the_identity_is_a_string_so_the_method_stays_free_of_our_types(
    session: Session,
) -> None:
    """protea_method must not learn PROTEA's key types to rank a list."""
    identities = load_sequence_identities(session, {"P1"})
    assert isinstance(identities["P1"], str)
