"""Each window places its targets on the sequence axes once, for every method to read.

The rule (:mod:`protea.core.window_placement`) is checked on its own first: an identical
sequence under another accession places a target at identity 100, the target itself
never counts, and otherwise the best hit among the truth members' sequences decides,
by bit score and then identity, e-value and md5, wherever the hit sits in the file.
Then the operation runs against Postgres on a real photo of cut 211 and a homology
table loaded by its own loader, with the hits it reads written here.
"""

from __future__ import annotations

import hashlib
import uuid
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.core._artifact_identity import ArtifactMismatch, fetch_verified_to_file
from protea.core.operations import place_window_targets as op
from protea.core.operations.place_window_targets import (
    PlaceWindowTargetsOperation,
    PlaceWindowTargetsPayload,
    WindowPlacementRefused,
)
from protea.core.strata import HomologyBand, homology_band_for
from protea.core.window_placement import (
    HIT_COLUMNS,
    Hit,
    best_truth_hits,
    identity_percent,
    needs_a_hit,
    place,
)
from protea.infrastructure.orm.models.annotation import window_placement as model
from protea.infrastructure.orm.models.annotation.cut_photo import CutPhoto
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.annotation.window_placement import (
    WindowPlacement,
    WindowPlacementTarget,
)
from protea.infrastructure.storage.local import LocalFsArtifactStore
from tests.test_load_cut_photo import MD5, _load, build_photo, pg_session  # noqa: F401
from tests.test_load_homology_table import _load_table, _table, stored  # noqa: F401

_MIGRATION = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions/2ee1c12a5e0c_each_window_places_its_targets.py"
)


def _hits(path: Path, rows: list[tuple[Any, ...]], row_group_size: int = 2) -> Path:
    """A homology table's hits parquet, in its own column types, from (query, target, ...) rows."""
    columns = list(zip(*rows, strict=True)) if rows else [[] for _ in HIT_COLUMNS]
    types = [
        pa.large_string(),
        pa.large_string(),
        pa.float32(),
        pa.int32(),
        pa.float32(),
        pa.float32(),
        pa.float64(),
        pa.float32(),
    ]
    table = pa.table(
        {
            name: pa.array(values, type=kind)
            for name, values, kind in zip(HIT_COLUMNS, columns, types, strict=True)
        }
    )
    pq.write_table(table, path, row_group_size=row_group_size)
    return path


def _row(query: str, target: str, fident: float, bits: float, evalue: float = 1e-10):
    return (query, target, fident, 100, 0.9, 0.8, evalue, bits)


class TestTheIdentity:
    def test_it_is_the_printed_value_and_does_not_cross_a_band_edge(self) -> None:
        stored_as = float(np.float32(0.300))
        assert stored_as * 100.0 > 30.0  # the float32 of 0.300 reads above the edge
        assert identity_percent(stored_as) == 30.0
        assert homology_band_for(identity_percent(stored_as)) is HomologyBand.TWILIGHT

    def test_three_decimals_become_one(self) -> None:
        assert identity_percent(float(np.float32(0.854))) == 85.4
        assert identity_percent(1.0) == 100.0


class TestTheBestTruthHit:
    def test_bits_first_then_identity_then_evalue_then_md5_across_row_groups(
        self, tmp_path: Path
    ) -> None:
        rows = [
            _row("q1", "untruth", 0.99, 900.0),  # higher bits, but not a truth sequence
            _row("q1", "t-a", 0.50, 50.0),
            _row("q1", "t-b", 0.60, 50.0),  # same bits, higher identity: wins over t-a
            _row("q2", "t-c", 0.70, 40.0, 1e-5),
            _row("q2", "t-d", 0.70, 40.0, 1e-9),  # same bits and identity, lower e-value
            _row("q3", "t-f", 0.40, 30.0),
            _row("q3", "t-e", 0.40, 30.0),  # a full tie: the lower md5
            _row("q4", "untruth", 0.90, 80.0),  # only a non-truth hit: no best hit
            _row("q9", "t-a", 0.99, 999.0),  # not asked about
        ]
        path = _hits(tmp_path / "hits.parquet", rows)
        assert pq.ParquetFile(path).metadata.num_row_groups > 1
        best = best_truth_hits(
            path, {"q1", "q2", "q3", "q4"}, {"t-a", "t-b", "t-c", "t-d", "t-e", "t-f"}
        )
        assert {q: h.target_md5 for q, h in best.items()} == {
            "q1": "t-b",
            "q2": "t-d",
            "q3": "t-e",
        }

    def test_the_row_group_size_does_not_change_the_answer(self, tmp_path: Path) -> None:
        rows = [_row("q", f"t{i}", 0.5 + i / 100, 10.0 + (i % 3)) for i in range(9)]
        answers = {
            size: best_truth_hits(
                _hits(tmp_path / f"hits-{size}.parquet", rows, size),
                {"q"},
                {f"t{i}" for i in range(9)},
            )
            for size in (1, 4, 100)
        }
        assert len({tuple(a.items()) for a in answers.values()}) == 1


class TestThePlacement:
    HIT = Hit("m-x", 0.854, 120, 0.9, 0.8, 1e-20, 210.0)

    def test_a_twin_places_at_identity_one_hundred_and_the_target_itself_never_counts(
        self,
    ) -> None:
        targets = {"A": "m1", "C": "m2", "D": "m3"}
        truth = {"m1": frozenset({"A", "B"}), "m2": frozenset({"C"})}
        placed = {
            p.accession: p
            for p in place(targets, {"m1": 10, "m2": 20, "m3": 30}, truth, {"m2": self.HIT})
        }
        assert (placed["A"].closest_by, placed["A"].closest_identity) == ("twin", 100.0)
        assert placed["A"].closest_md5 == "m1" and placed["A"].bits is None
        assert placed["C"].closest_by == "hit"  # its only truth twin is itself
        assert (placed["C"].closest_identity, placed["C"].closest_md5) == (85.4, "m-x")
        assert placed["D"].closest_by is None and placed["D"].closest_identity is None
        assert [p.residues for p in placed.values()] == [10, 20, 30]

    def test_the_hits_are_read_only_for_targets_no_twin_places(self) -> None:
        targets = {"A": "m1", "C": "m2"}
        truth = {"m1": frozenset({"A", "B"}), "m2": frozenset({"C"})}
        assert needs_a_hit(targets, truth) == {"m2"}

    def test_the_order_is_the_accessions(self) -> None:
        targets = {"Z": "m1", "A": "m2"}
        assert [p.accession for p in place(targets, {"m1": 1, "m2": 1}, {}, {})] == ["A", "Z"]


class TestTheStreamedFetch:
    def test_the_bytes_land_when_they_hash_as_declared(self, tmp_path: Path) -> None:
        store = LocalFsArtifactStore(tmp_path / "store")
        store.put("homology/hits.parquet", b"the hits")
        expected = hashlib.sha256(b"the hits").hexdigest()
        dest = fetch_verified_to_file(
            store, "homology/hits.parquet", expected, tmp_path / "hits.parquet", "the hits"
        )
        assert dest.read_bytes() == b"the hits"

    def test_a_mismatch_is_refused_and_leaves_nothing_to_open(self, tmp_path: Path) -> None:
        store = LocalFsArtifactStore(tmp_path / "store")
        uri = store.put("homology/hits.parquet", b"other bytes")
        with pytest.raises(ArtifactMismatch, match="the hits"):
            fetch_verified_to_file(store, uri, "0" * 64, tmp_path / "hits.parquet", "the hits")
        assert not (tmp_path / "hits.parquet").exists()


class TestTheSchema:
    def test_the_migration_writes_the_models_checks(self) -> None:
        import importlib.util

        spec = importlib.util.spec_from_file_location("migration", _MIGRATION)
        assert spec is not None and spec.loader is not None
        migration = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(migration)
        for name in ("RULES_CHECK", "COUNTS_CHECK", "CLOSEST_CHECK"):
            assert getattr(migration, name) == getattr(model, name), name

    def test_a_window_is_named_by_id(self) -> None:
        with pytest.raises(ValueError):
            PlaceWindowTargetsPayload(evaluation_set_id="not-a-window")


def _window(session: Session) -> EvaluationSet:
    photo = session.execute(select(CutPhoto)).scalar_one()
    snapshot = session.execute(select(OntologySnapshot.id)).scalar_one()
    window = EvaluationSet(
        id=uuid.uuid4(),
        old_annotation_set_id=photo.annotation_set_id,
        new_annotation_set_id=photo.annotation_set_id,
        pivot_snapshot_id=snapshot,
        old_native_snapshot_id=snapshot,
        new_native_snapshot_id=snapshot,
    )
    session.add(window)
    session.commit()
    return window


def _run(session: Session, window: EvaluationSet) -> dict[str, Any]:
    result = PlaceWindowTargetsOperation().execute(
        session, {"evaluation_set_id": str(window.id)}, emit=lambda *a, **k: None
    )
    session.commit()
    return result.result


@pytest.mark.integration
class TestTheOperation:
    """On the photo of cut 211 the loader tests build: P1 has a truth row, P2 does not."""

    @pytest.fixture()
    def placed_window(
        self,
        pg_session: Session,  # noqa: F811
        stored: list[tuple[int, str]],  # noqa: F811
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> EvaluationSet:
        photo_dir, sha = build_photo(tmp_path)
        _load(pg_session, photo_dir, sha)
        table_dir, table_sha = _table(tmp_path, photo_dir)
        _load_table(pg_session, table_dir, table_sha)
        hits = _hits(
            tmp_path / "window-hits.parquet",
            [
                _row(MD5["P2"], hashlib.md5(b"MQQLK").hexdigest(), 0.95, 40.0),  # Q9: no truth
                _row(MD5["P2"], MD5["P1"], 0.667, 20.0),
            ],
        )
        monkeypatch.setattr(op, "window_targets", lambda _s, _w: {"P1", "P2"})
        monkeypatch.setattr(op, "fetch_hits", lambda _t, _scratch: hits)
        monkeypatch.setattr(op, "_scratch_root", lambda: tmp_path)
        return _window(pg_session)

    def test_each_target_is_placed_and_the_placement_says_how(
        self,
        pg_session: Session,  # noqa: F811
        placed_window: EvaluationSet,
    ) -> None:
        result = _run(pg_session, placed_window)
        counts = ("targets", "targets_with_twin", "targets_with_hit")
        assert [result[name] for name in counts] == [2, 0, 1]
        placement = pg_session.execute(select(WindowPlacement)).scalar_one()
        assert placement.rule == "closest_truth_member"
        assert placement.targets_without_homolog == 1
        assert placement.short_name.startswith("placement-211-")
        rows = {r.accession: r for r in pg_session.execute(select(WindowPlacementTarget)).scalars()}
        # P1 is the photo's only truth member, and a target is never its own closest one.
        assert rows["P1"].closest_by is None and rows["P1"].residues == 3
        assert rows["P2"].closest_by == "hit" and rows["P2"].closest_md5 == MD5["P1"]
        assert rows["P2"].closest_identity == 66.7 and rows["P2"].residues == 5

    def test_the_same_window_again_is_a_no_op(
        self,
        pg_session: Session,  # noqa: F811
        placed_window: EvaluationSet,
    ) -> None:
        first = _run(pg_session, placed_window)
        again = _run(pg_session, placed_window)
        assert again == {"placement_id": first["placement_id"], "skipped": True}

    def test_a_target_outside_the_superset_refuses_before_writing(
        self,
        pg_session: Session,  # noqa: F811
        placed_window: EvaluationSet,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(op, "window_targets", lambda _s, _w: {"P1", "Q9"})
        with pytest.raises(WindowPlacementRefused, match="no sequence in the superset"):
            _run(pg_session, placed_window)
        pg_session.rollback()
        assert pg_session.execute(select(WindowPlacement)).first() is None
