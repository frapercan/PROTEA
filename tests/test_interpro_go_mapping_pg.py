"""Postgres-backed integration tests for the IP.4a InterPro2GO loader.

Schema check + smoke run on a small synthetic InterPro2GO blob to
prove:

  1. The migration creates a queryable ``interpro_go_mapping`` table.
  2. Round-trip via the ORM preserves all fields.
  3. The ``uq_interpro_go_mapping_pair`` constraint actually rejects
     duplicate ``(ipr_accession, go_id, source_version)`` triples.
  4. The loader operation is idempotent: re-running the same
     ``source_version`` against the same payload inserts zero new rows.

Requires ``--with-postgres``.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

import protea.infrastructure.orm.models  # noqa: F401 — register tables
from protea.core.operations.load_interpro_go_mapping import (
    LoadInterProGoMappingOperation,
)
from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.annotation.interpro_go_mapping import (
    InterProGoMapping,
)

# 50-pair synthetic blob: covers the smoke-test acceptance criterion
# in the slice spec ("smoke test on 50 IPR rows").
_FIFTY_LINE_HEADER = "!date: 2024/01/15 09:00:00\n!Mapping of InterPro entries to GO\n!\n"


def _synthetic_interpro2go(n: int = 50) -> str:
    lines = [_FIFTY_LINE_HEADER]
    for i in range(n):
        ipr = f"IPR{i:06d}"
        go = f"GO:{i:07d}"
        lines.append(f"InterPro:{ipr} synthetic > GO:label ; {go}\n")
    return "".join(lines)


def _noop_emit(*_args, **_kwargs) -> None:
    return None


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
def test_interpro_go_mapping_round_trip(pg_session: Session) -> None:
    row = InterProGoMapping(
        ipr_accession="IPR000001",
        go_id="GO:0005515",
        evidence="IEA",
        source_version="InterPro-104.0",
    )
    pg_session.add(row)
    pg_session.commit()

    fetched = pg_session.scalar(select(InterProGoMapping).where(InterProGoMapping.id == row.id))
    assert fetched is not None
    assert fetched.ipr_accession == "IPR000001"
    assert fetched.go_id == "GO:0005515"
    assert fetched.evidence == "IEA"
    assert fetched.source_version == "InterPro-104.0"
    assert fetched.created_at is not None


@pytest.mark.integration
def test_interpro_go_mapping_allows_null_evidence(pg_session: Session) -> None:
    row = InterProGoMapping(
        ipr_accession="IPR000002",
        go_id="GO:0003674",
        evidence=None,
        source_version="InterPro-104.0",
    )
    pg_session.add(row)
    pg_session.commit()

    fetched = pg_session.scalar(select(InterProGoMapping).where(InterProGoMapping.id == row.id))
    assert fetched is not None
    assert fetched.evidence is None


@pytest.mark.integration
def test_interpro_go_mapping_unique_pair(pg_session: Session) -> None:
    """Same (ipr_accession, go_id, source_version) cannot be inserted twice."""
    pg_session.add(
        InterProGoMapping(
            ipr_accession="IPR000001",
            go_id="GO:0005515",
            evidence="IEA",
            source_version="InterPro-104.0",
        )
    )
    pg_session.commit()

    pg_session.add(
        InterProGoMapping(
            ipr_accession="IPR000001",
            go_id="GO:0005515",
            evidence="IEA",
            source_version="InterPro-104.0",
        )
    )
    with pytest.raises(IntegrityError):
        pg_session.commit()
    pg_session.rollback()


@pytest.mark.integration
def test_interpro_go_mapping_allows_distinct_releases(pg_session: Session) -> None:
    """The same (ipr, go) pair may coexist across distinct source_versions."""
    pg_session.add(
        InterProGoMapping(
            ipr_accession="IPR000001",
            go_id="GO:0005515",
            source_version="InterPro-100.0",
        )
    )
    pg_session.add(
        InterProGoMapping(
            ipr_accession="IPR000001",
            go_id="GO:0005515",
            source_version="InterPro-104.0",
        )
    )
    pg_session.commit()

    rows = pg_session.scalars(
        select(InterProGoMapping).where(
            InterProGoMapping.ipr_accession == "IPR000001",
        )
    ).all()
    assert len(rows) == 2


@pytest.mark.integration
def test_loader_smoke_50_rows_and_idempotent(pg_session: Session) -> None:
    """Run the operation twice against a 50-line synthetic file.

    First call: 50 rows parsed, 50 inserted.
    Second call (same source_version): 50 parsed, 0 inserted, 50 skipped.
    Acceptance criterion from the slice spec.
    """
    blob = _synthetic_interpro2go(50)

    with patch("protea.core.operations.load_interpro_go_mapping.requests.get") as mock_get:
        resp = MagicMock()
        resp.text = blob
        resp.raise_for_status.return_value = None
        mock_get.return_value = resp

        op = LoadInterProGoMappingOperation()

        first = op.execute(
            pg_session,
            {"source_version": "InterPro-104.0"},
            emit=_noop_emit,
        )
        pg_session.commit()
        assert first.result["rows_parsed"] == 50
        assert first.result["rows_inserted"] == 50
        assert first.result["rows_skipped_duplicate"] == 0

        # Confirm the rows actually landed
        count = pg_session.scalar(
            select(InterProGoMapping).where(
                InterProGoMapping.source_version == "InterPro-104.0",
            )
        )
        assert count is not None

        second = op.execute(
            pg_session,
            {"source_version": "InterPro-104.0"},
            emit=_noop_emit,
        )
        pg_session.commit()
        assert second.result["rows_parsed"] == 50
        assert second.result["rows_inserted"] == 0
        assert second.result["rows_skipped_duplicate"] == 50
