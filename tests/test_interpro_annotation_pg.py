"""Postgres-backed integration tests for the IP.2 ``InterProAnnotation``
ORM model.

Two acceptance checks from the IP.2 slice:

1. Round-trip: insert + read returns identical values, including the
   server-side ``created_at`` default and the auto-generated UUID.
2. Coordinate invariants: rows that violate ``start >= 1`` or
   ``end >= start`` are rejected by the CHECK constraints baked into
   the table.

These tests require ``--with-postgres`` because they exercise SQL
constraints that the SQLite-in-memory style smoke tests cannot
enforce (Postgres CHECK constraints, ``server_default=func.now()``).
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

import protea.infrastructure.orm.models  # noqa: F401 — register tables
from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.annotation.interpro_annotation import (
    InterProAnnotation,
)
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.sequence.sequence import Sequence

_TEST_ACCESSION = "P12345"
_TEST_SEQUENCE = "MKTIIALSYIFCLVFA"
_TEST_IPR_RELEASE = "InterProScan-5.66-98.0"


def _seed_protein(session: Session) -> Protein:
    """Insert one Sequence + Protein row that the InterPro rows can
    point at via the protein_accession FK."""
    seq = Sequence(
        sequence=_TEST_SEQUENCE,
        sequence_hash=Sequence.compute_hash(_TEST_SEQUENCE),
    )
    session.add(seq)
    session.flush()

    protein = Protein(
        accession=_TEST_ACCESSION,
        entry_name="TEST_HUMAN",
        reviewed=True,
        canonical_accession=_TEST_ACCESSION,
        is_canonical=True,
        length=len(_TEST_SEQUENCE),
        sequence_id=seq.id,
    )
    session.add(protein)
    session.flush()
    return protein


@pytest.fixture()
def pg_session(postgres_url: str):
    """Fresh schema + a session bound to the live engine."""
    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with Session(engine, future=True) as session:
        yield session
    Base.metadata.drop_all(engine)


@pytest.mark.integration
def test_interpro_annotation_round_trip(pg_session: Session) -> None:
    """A row inserted via the ORM is readable with all fields intact."""
    _seed_protein(pg_session)

    row = InterProAnnotation(
        protein_accession=_TEST_ACCESSION,
        source_db="Pfam",
        accession="PF00001",
        start=3,
        end=12,
        evidence="IEA",
        ipr_release=_TEST_IPR_RELEASE,
    )
    pg_session.add(row)
    pg_session.commit()

    assert row.id is not None
    assert isinstance(row.id, uuid.UUID)

    fetched = pg_session.scalar(select(InterProAnnotation).where(InterProAnnotation.id == row.id))
    assert fetched is not None
    assert fetched.protein_accession == _TEST_ACCESSION
    assert fetched.source_db == "Pfam"
    assert fetched.accession == "PF00001"
    assert fetched.start == 3
    assert fetched.end == 12
    assert fetched.evidence == "IEA"
    assert fetched.ipr_release == _TEST_IPR_RELEASE
    assert fetched.created_at is not None
    # Relationship load returns the parent protein.
    assert fetched.protein.accession == _TEST_ACCESSION


@pytest.mark.integration
def test_interpro_annotation_allows_null_evidence(pg_session: Session) -> None:
    """``evidence`` is nullable (free-form, optional)."""
    _seed_protein(pg_session)

    row = InterProAnnotation(
        protein_accession=_TEST_ACCESSION,
        source_db="Gene3D",
        accession="G3DSA:3.40.50.300",
        start=1,
        end=200,
        evidence=None,
        ipr_release=_TEST_IPR_RELEASE,
    )
    pg_session.add(row)
    pg_session.commit()

    fetched = pg_session.scalar(select(InterProAnnotation).where(InterProAnnotation.id == row.id))
    assert fetched is not None
    assert fetched.evidence is None


@pytest.mark.integration
def test_interpro_annotation_rejects_start_below_one(pg_session: Session) -> None:
    """``start >= 1`` CHECK constraint rejects zero / negative starts."""
    _seed_protein(pg_session)

    bad = InterProAnnotation(
        protein_accession=_TEST_ACCESSION,
        source_db="Pfam",
        accession="PF00001",
        start=0,
        end=5,
        ipr_release=_TEST_IPR_RELEASE,
    )
    pg_session.add(bad)
    with pytest.raises(IntegrityError):
        pg_session.commit()
    pg_session.rollback()


@pytest.mark.integration
def test_interpro_annotation_rejects_end_before_start(pg_session: Session) -> None:
    """``end >= start`` CHECK constraint rejects inverted coordinates."""
    _seed_protein(pg_session)

    bad = InterProAnnotation(
        protein_accession=_TEST_ACCESSION,
        source_db="Pfam",
        accession="PF00001",
        start=10,
        end=3,
        ipr_release=_TEST_IPR_RELEASE,
    )
    pg_session.add(bad)
    with pytest.raises(IntegrityError):
        pg_session.commit()
    pg_session.rollback()


@pytest.mark.integration
def test_interpro_annotation_accepts_single_residue_hit(pg_session: Session) -> None:
    """``end == start`` is accepted (single-residue domain hit)."""
    _seed_protein(pg_session)

    row = InterProAnnotation(
        protein_accession=_TEST_ACCESSION,
        source_db="PROSITE",
        accession="PS00001",
        start=7,
        end=7,
        ipr_release=_TEST_IPR_RELEASE,
    )
    pg_session.add(row)
    pg_session.commit()

    fetched = pg_session.scalar(select(InterProAnnotation).where(InterProAnnotation.id == row.id))
    assert fetched is not None
    assert fetched.start == 7
    assert fetched.end == 7
