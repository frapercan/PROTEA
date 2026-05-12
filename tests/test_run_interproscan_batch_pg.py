"""Postgres-backed integration tests for ``run_interproscan_batch``.

Exercises the full round-trip: seed proteins, mock the
``InterProSource.run`` plugin to yield fixture records, run the
operation, assert DB rows. The plugin mock is the load-bearing seam:
IP.3 owns DB-side logic only, the subprocess call lives in IP.1b.

Two scenarios beyond the basic round-trip:

* ``ipr_release_floor`` filters out proteins whose latest persisted
  release already meets the floor.
* Resume: a half-finished run can be re-dispatched and the operation
  picks up exactly where it left off without re-annotating.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from protea_sources.interpro.parser import (
    InterProAnnotation as InterProRecord,
)
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

import protea.infrastructure.orm.models  # noqa: F401 — register tables
from protea.core.operations.run_interproscan_batch import (
    RunInterProScanBatchOperation,
)
from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.annotation.interpro_annotation import (
    InterProAnnotation,
)
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.query.query_set import (
    QuerySet,
    QuerySetEntry,
)
from protea.infrastructure.orm.models.sequence.sequence import Sequence

_IPR_RELEASE = "InterProScan-5.66-98.0"
_IPR_RELEASE_NEXT = "InterProScan-5.67-99.0"


def _seed_protein(session: Session, accession: str, sequence_str: str) -> Protein:
    """Insert one Sequence + Protein row keyed by ``accession``."""
    seq = Sequence(
        sequence=sequence_str,
        sequence_hash=Sequence.compute_hash(sequence_str + accession),
    )
    session.add(seq)
    session.flush()
    protein = Protein(
        accession=accession,
        entry_name=f"TEST_{accession}",
        reviewed=True,
        canonical_accession=accession,
        is_canonical=True,
        length=len(sequence_str),
        sequence_id=seq.id,
    )
    session.add(protein)
    session.flush()
    return protein


def _fixture_record(accession: str, *, ipr_release: str = _IPR_RELEASE) -> InterProRecord:
    """Build one parser-shaped record for the plugin mock to yield."""
    return InterProRecord(
        source_db="Pfam",
        accession=accession,
        start=1,
        end=50,
        evidence="1.2e-10",
        ipr_version="IPR000123",
        ipr_release_version=ipr_release,
    )


def _make_mock_run(
    records_by_accession: dict[str, list[InterProRecord]],
) -> Any:
    """Return a side_effect callable that the patched ``run`` will use.

    Parses the FASTA the operation hands the plugin, reads the
    accessions, and yields the matching fixture records. This keeps
    the mock honest: if the operation forgets a chunk, the mock won't
    emit annotations for it.
    """

    def _runner(payload: Any, *, emit: Any) -> Iterator[InterProRecord]:
        from pathlib import Path

        accessions: list[str] = []
        for line in Path(payload.fasta_path).read_text().splitlines():
            if line.startswith(">"):
                accessions.append(line[1:].strip())
        for acc in accessions:
            yield from records_by_accession.get(acc, [])

    return _runner


def _make_tracking_runner(
    inner: Any,
) -> tuple[MagicMock, Any]:
    """Wrap a ``(payload, *, emit)`` runner so call_count + call_args are observable.

    ``patch(..., new=...)`` with a plain ``lambda self, payload, *, emit`` works
    fine but loses ``MagicMock``'s call tracking. This helper bundles the two:
    the returned mock counts every call, the returned bound function does the
    real work and discards the ``self`` that an unbound-method patch injects.
    """
    tracker = MagicMock()

    def _bound(self: Any, payload: Any, *, emit: Any) -> Iterator[InterProRecord]:
        tracker(payload, emit=emit)
        yield from inner(payload, emit=emit)

    return tracker, _bound


@pytest.fixture()
def pg_session(postgres_url: str) -> Iterator[Session]:
    """Fresh schema + a session bound to the live engine."""
    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with Session(engine, future=True) as session:
        yield session
    Base.metadata.drop_all(engine)


@pytest.mark.integration
def test_round_trip_with_accessions_list(pg_session: Session) -> None:
    """Two proteins, two fixture records → two DB rows."""
    _seed_protein(pg_session, "P00001", "MKTIIA")
    _seed_protein(pg_session, "P00002", "MQYDPP")
    pg_session.commit()

    records = {
        "P00001": [_fixture_record("P00001")],
        "P00002": [_fixture_record("P00002")],
    }
    op = RunInterProScanBatchOperation()

    with patch(
        "protea_sources.interpro.source.InterProSource.run",
        new=lambda self, payload, *, emit: _make_mock_run(records)(payload, emit=emit),
    ):
        result = op.execute(
            pg_session,
            {"accessions": ["P00001", "P00002"], "chunk_size": 50},
            emit=lambda *_a, **_k: None,
        )

    pg_session.commit()
    rows = pg_session.scalars(select(InterProAnnotation)).all()
    assert len(rows) == 2
    accessions = sorted(r.protein_accession for r in rows)
    assert accessions == ["P00001", "P00002"]
    assert all(r.ipr_release == _IPR_RELEASE for r in rows)
    assert result.result["annotations_inserted"] == 2
    assert result.result["chunks_done"] == 1
    assert result.result["proteins_processed"] == 2


@pytest.mark.integration
def test_chunking_splits_runs(pg_session: Session) -> None:
    """``chunk_size=2`` over 5 proteins → 3 InterProSource.run calls."""
    for i in range(1, 6):
        _seed_protein(pg_session, f"P0000{i}", f"MKTII{i}")
    pg_session.commit()

    records = {f"P0000{i}": [_fixture_record(f"P0000{i}")] for i in range(1, 6)}
    op = RunInterProScanBatchOperation()
    tracker, bound = _make_tracking_runner(_make_mock_run(records))

    with patch(
        "protea_sources.interpro.source.InterProSource.run",
        new=bound,
    ):
        result = op.execute(
            pg_session,
            {
                "accessions": [f"P0000{i}" for i in range(1, 6)],
                "chunk_size": 2,
            },
            emit=lambda *_a, **_k: None,
        )

    pg_session.commit()
    assert tracker.call_count == 3  # ceil(5/2)
    rows = pg_session.scalars(select(InterProAnnotation)).all()
    assert len(rows) == 5
    assert result.result["chunks_done"] == 3
    assert result.result["proteins_processed"] == 5


@pytest.mark.integration
def test_query_set_input(pg_session: Session) -> None:
    """A ``QuerySet`` resolves to its member proteins."""
    p1 = _seed_protein(pg_session, "P00001", "MKTIIA")
    p2 = _seed_protein(pg_session, "P00002", "MQYDPP")
    _seed_protein(pg_session, "P00003", "MGGGGG")  # not in query set
    qs = QuerySet(name="test_set")
    pg_session.add(qs)
    pg_session.flush()
    pg_session.add(
        QuerySetEntry(query_set_id=qs.id, sequence_id=p1.sequence_id, accession=p1.accession)
    )
    pg_session.add(
        QuerySetEntry(query_set_id=qs.id, sequence_id=p2.sequence_id, accession=p2.accession)
    )
    pg_session.commit()

    records = {
        "P00001": [_fixture_record("P00001")],
        "P00002": [_fixture_record("P00002")],
    }
    op = RunInterProScanBatchOperation()
    with patch(
        "protea_sources.interpro.source.InterProSource.run",
        new=lambda self, payload, *, emit: _make_mock_run(records)(payload, emit=emit),
    ):
        result = op.execute(
            pg_session,
            {"query_set_id": str(qs.id)},
            emit=lambda *_a, **_k: None,
        )

    pg_session.commit()
    rows = pg_session.scalars(select(InterProAnnotation)).all()
    accessions = sorted(r.protein_accession for r in rows)
    assert accessions == ["P00001", "P00002"]
    assert result.result["resolved"] == 2


@pytest.mark.integration
def test_ipr_release_floor_skips_already_annotated(pg_session: Session) -> None:
    """Proteins with a persisted release >= floor are skipped."""
    _seed_protein(pg_session, "P00001", "MKTIIA")
    _seed_protein(pg_session, "P00002", "MQYDPP")
    # P00001 already has an annotation at _IPR_RELEASE; P00002 has none.
    pg_session.add(
        InterProAnnotation(
            id=uuid.uuid4(),
            protein_accession="P00001",
            source_db="Pfam",
            accession="PF00001",
            start=1,
            end=10,
            ipr_release=_IPR_RELEASE,
        )
    )
    pg_session.commit()

    op = RunInterProScanBatchOperation()
    records = {"P00002": [_fixture_record("P00002")]}
    tracker, bound = _make_tracking_runner(_make_mock_run(records))
    with patch(
        "protea_sources.interpro.source.InterProSource.run",
        new=bound,
    ):
        result = op.execute(
            pg_session,
            {
                "accessions": ["P00001", "P00002"],
                "ipr_release_floor": _IPR_RELEASE,
            },
            emit=lambda *_a, **_k: None,
        )

    pg_session.commit()
    # Plugin called for the single chunk covering P00002 only.
    assert tracker.call_count == 1
    # P00001 keeps its existing row (no new one inserted by this run).
    rows_p1 = pg_session.scalars(
        select(InterProAnnotation).where(InterProAnnotation.protein_accession == "P00001")
    ).all()
    assert len(rows_p1) == 1  # the pre-existing one
    rows_p2 = pg_session.scalars(
        select(InterProAnnotation).where(InterProAnnotation.protein_accession == "P00002")
    ).all()
    assert len(rows_p2) == 1
    assert result.result["proteins_skipped"] == 1
    assert result.result["proteins_processed"] == 1


@pytest.mark.integration
def test_ipr_release_floor_below_existing_re_annotates(
    pg_session: Session,
) -> None:
    """An older floor lets a re-run at the SAME release re-annotate."""
    _seed_protein(pg_session, "P00001", "MKTIIA")
    pg_session.add(
        InterProAnnotation(
            id=uuid.uuid4(),
            protein_accession="P00001",
            source_db="Pfam",
            accession="PF00001",
            start=1,
            end=10,
            ipr_release=_IPR_RELEASE,
        )
    )
    pg_session.commit()

    op = RunInterProScanBatchOperation()
    records = {"P00001": [_fixture_record("P00001", ipr_release=_IPR_RELEASE_NEXT)]}
    with patch(
        "protea_sources.interpro.source.InterProSource.run",
        new=lambda self, payload, *, emit: _make_mock_run(records)(payload, emit=emit),
    ):
        op.execute(
            pg_session,
            {
                "accessions": ["P00001"],
                "ipr_release_floor": _IPR_RELEASE_NEXT,
            },
            emit=lambda *_a, **_k: None,
        )

    pg_session.commit()
    rows = pg_session.scalars(
        select(InterProAnnotation).where(InterProAnnotation.protein_accession == "P00001")
    ).all()
    # The pre-existing row plus the new one at the newer release.
    assert len(rows) == 2
    releases = sorted(r.ipr_release for r in rows)
    assert releases == [_IPR_RELEASE, _IPR_RELEASE_NEXT]


@pytest.mark.integration
def test_resume_picks_up_where_left_off(pg_session: Session) -> None:
    """A half-finished run re-dispatches and only processes the rest.

    Simulates: the first dispatch processed P00001 (committed) but the
    worker died before P00002. Re-dispatch with the SAME release floor
    is expected to skip P00001 (already annotated at that release) and
    process P00002 alone.
    """
    _seed_protein(pg_session, "P00001", "MKTIIA")
    _seed_protein(pg_session, "P00002", "MQYDPP")
    # Simulate a successful first pass for P00001 only.
    pg_session.add(
        InterProAnnotation(
            id=uuid.uuid4(),
            protein_accession="P00001",
            source_db="Pfam",
            accession="PF00001",
            start=1,
            end=10,
            ipr_release=_IPR_RELEASE,
        )
    )
    pg_session.commit()

    op = RunInterProScanBatchOperation()
    records = {"P00002": [_fixture_record("P00002")]}
    captured_fasta: list[str] = []

    def _capturing_runner(payload: Any, *, emit: Any) -> Iterator[InterProRecord]:
        from pathlib import Path

        captured_fasta.append(Path(payload.fasta_path).read_text())
        yield from _make_mock_run(records)(payload, emit=emit)

    tracker, bound = _make_tracking_runner(_capturing_runner)
    with patch(
        "protea_sources.interpro.source.InterProSource.run",
        new=bound,
    ):
        result = op.execute(
            pg_session,
            {
                "accessions": ["P00001", "P00002"],
                "ipr_release_floor": _IPR_RELEASE,
            },
            emit=lambda *_a, **_k: None,
        )

    pg_session.commit()
    # Exactly one chunk, covering only P00002.
    assert tracker.call_count == 1
    assert ">P00002" in captured_fasta[0]
    assert ">P00001" not in captured_fasta[0]
    assert result.result["proteins_skipped"] == 1
    assert result.result["proteins_processed"] == 1
    # P00001 still has its single legacy row, P00002 has its new row.
    rows = pg_session.scalars(select(InterProAnnotation)).all()
    assert len(rows) == 2


@pytest.mark.integration
def test_unknown_query_set_raises(pg_session: Session) -> None:
    """A bogus ``query_set_id`` surfaces as ``ValueError`` immediately."""
    op = RunInterProScanBatchOperation()
    with pytest.raises(ValueError, match="QuerySet"):
        op.execute(
            pg_session,
            {"query_set_id": str(uuid.uuid4())},
            emit=lambda *_a, **_k: None,
        )


@pytest.mark.integration
def test_no_targets_returns_empty_result(pg_session: Session) -> None:
    """Accessions absent from the ``protein`` table → no plugin calls."""
    op = RunInterProScanBatchOperation()
    tracker, bound = _make_tracking_runner(_make_mock_run({}))
    with patch(
        "protea_sources.interpro.source.InterProSource.run",
        new=bound,
    ):
        result = op.execute(
            pg_session,
            {"accessions": ["P99999"]},
            emit=lambda *_a, **_k: None,
        )
    # Accession not in protein, but the operation does not filter for
    # protein presence before chunking (it filters on sequence
    # availability inside _process_chunk). The plugin call should be a
    # no-op chunk that produced no FASTA → mock not invoked.
    assert tracker.call_count == 0
    assert result.result["annotations_inserted"] == 0
    assert result.result["proteins_skipped"] >= 1
