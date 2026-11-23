"""Every reader of the length and homology axes reads the window's placement.

The homology band used to be read from each method's own neighbours, so two arms on
one window could place a protein in different bands, and an arm that retrieved
nothing had no band at all. Now the stratification, the paired comparison restricted
to a stratum and the two stratum routes read the window's placement
(:mod:`protea.core.window_placement`), and only the three retrieval axes read an
arm's neighbours.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from protea.core.operations._paired_panels_artifact import PanelComparabilityError
from protea.core.operations._paired_panels_stratum import _place, stratum_population
from protea.core.operations.stratify_evaluation import StratifyEvaluationPayload, _window_of
from protea.core.strata import Neighbourhood
from protea.core.window_placement import (
    TargetPlacement,
    WindowNotPlacedError,
    placement_of_window,
)
from protea.infrastructure.orm.base import Base

_NEAR = TargetPlacement(residues=300, identity=95.0)
_FAR = TargetPlacement(residues=300, identity=25.0)
_NONE = TargetPlacement(residues=3000, identity=None)
_DONOR = Neighbourhood(best_identity=40.0, donor_is_experimental=False)


class TestThePairedRestriction:
    def test_a_homology_band_is_the_windows_whatever_the_baseline_retrieved(self) -> None:
        placement = {"A": _NEAR, "B": _FAR, "C": _NONE}
        for hoods in ({}, {"A": _DONOR, "B": _DONOR, "C": _DONOR}):
            keep, unplaceable = _place(placement, hoods, {"homology": ">90"}, needs_donor=False)
            assert (keep, unplaceable) == ({"A"}, 0)
        keep, _ = _place(placement, {}, {"homology": "none", "length": ">2048"}, needs_donor=False)
        assert keep == {"C"}

    def test_a_retrieval_axis_reads_the_baseline_and_counts_who_it_cannot_place(self) -> None:
        placement = {"A": _NEAR, "B": _FAR}
        keep, unplaceable = _place(
            placement, {"A": _DONOR}, {"donor_evidence": "other"}, needs_donor=True
        )
        assert (keep, unplaceable) == ({"A"}, 1)

    def test_a_baseline_with_no_window_is_refused(self) -> None:
        with pytest.raises(PanelComparabilityError, match="names no window"):
            stratum_population(
                None, {"homology": ">90"}, {"prediction_set_id": "p"}, lambda *a, **k: None
            )


class TestTheMembersRoute:
    def test_it_walks_the_windows_targets_and_shows_the_arms_donor_beside(self) -> None:
        from protea.api.routers.stratum_members import _walk, _Where

        at = _Where(
            category="NK",
            aspect="F",
            evaluation_set_id=uuid.uuid4(),
            length=None,
            homology=">90",
        )
        placement = {"A": _NEAR, "B": _FAR}
        members, population = _walk(placement, {"A": _DONOR}, at, "F", limit=10)
        assert population == 1
        assert members == [
            {
                "accession": "A",
                "residues": 300,
                "length_band": "<=512",
                "homology_band": ">90",
                "identity": 95.0,
                "best_identity": 40.0,
                "donor_is_experimental": False,
                "taxonomic_relation": None,
            }
        ]

    def test_the_population_is_counted_before_the_page_is_cut(self) -> None:
        from protea.api.routers.stratum_members import _walk, _Where

        at = _Where(
            category="NK",
            aspect="F",
            evaluation_set_id=uuid.uuid4(),
            length=None,
            homology=None,
        )
        placement = {f"P{i}": _FAR for i in range(5)}
        members, population = _walk(placement, {}, at, "F", limit=2)
        assert (population, [m["accession"] for m in members]) == (5, ["P0", "P1"])


class _OneValue:
    def __init__(self, value: Any) -> None:
        self._value = value

    def scalar_one_or_none(self) -> Any:
        return self._value


class _ResultRow:
    def __init__(self, window: str | None) -> None:
        self.window = window

    def execute(self, *_a: Any, **_k: Any) -> _OneValue:
        return _OneValue(self.window)


class TestTheStratificationsWindow:
    def _payload(self, **extra: Any) -> StratifyEvaluationPayload:
        return StratifyEvaluationPayload(prediction_set_id="p", variant="unweighted", **extra)

    def test_a_result_names_its_own_window(self) -> None:
        window = str(uuid.uuid4())
        payload = self._payload(evaluation_result_id=str(uuid.uuid4()))
        assert _window_of(_ResultRow(window), payload) == window

    def test_a_payload_naming_another_window_than_the_results_is_refused(self) -> None:
        payload = self._payload(
            evaluation_result_id=str(uuid.uuid4()), evaluation_set_id=str(uuid.uuid4())
        )
        with pytest.raises(ValueError, match="was scored on window"):
            _window_of(_ResultRow(str(uuid.uuid4())), payload)

    def test_a_local_root_must_name_its_window(self) -> None:
        with pytest.raises(ValueError, match="give evaluation_set_id"):
            _window_of(_ResultRow(None), self._payload(artifacts_root="/tmp"))


@pytest.mark.integration
class TestTheReaderAgainstPostgres:
    @pytest.fixture()
    def pg(self, postgres_url: str):
        engine = create_engine(postgres_url, future=True)
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
        yield engine
        Base.metadata.drop_all(engine)
        engine.dispose()

    def _store(self, engine: Any, window: uuid.UUID, rows: list[tuple[Any, ...]]) -> None:
        placement = uuid.uuid4()
        with Session(engine, future=True) as session:
            session.execute(text("SET session_replication_role = 'replica'"))
            session.execute(
                text(
                    "INSERT INTO window_placement (id, evaluation_set_id, cut_photo_id, "
                    "homology_table_id, rule, targets, targets_with_twin, targets_with_hit, "
                    "targets_without_homolog, short_name, description) VALUES (:id, :w, :p, "
                    ":h, 'closest_truth_member', :n, 0, :hits, :none, :s, 'a placement')"
                ),
                {
                    "id": placement,
                    "w": window,
                    "p": uuid.uuid4(),
                    "h": uuid.uuid4(),
                    "n": len(rows),
                    "hits": sum(1 for r in rows if r[2] is not None),
                    "none": sum(1 for r in rows if r[2] is None),
                    "s": f"placement-{str(window)[:8]}",
                },
            )
            for accession, residues, identity in rows:
                session.execute(
                    text(
                        "INSERT INTO window_placement_target (placement_id, accession, "
                        "sequence_md5, residues, closest_by, closest_identity, closest_md5, "
                        "bits) VALUES (:p, :a, :m, :r, :by, :i, :cm, :b)"
                    ),
                    {
                        "p": placement,
                        "a": accession,
                        "m": "a" * 32,
                        "r": residues,
                        "by": None if identity is None else "hit",
                        "i": identity,
                        "cm": None if identity is None else "b" * 32,
                        "b": None if identity is None else 50.0,
                    },
                )
            session.commit()

    def test_it_reads_one_windows_placement_by_accession(self, pg: Any) -> None:
        window, other = uuid.uuid4(), uuid.uuid4()
        self._store(pg, window, [("A", 300, 95.0), ("B", 900, None)])
        self._store(pg, other, [("A", 300, 12.0)])
        with Session(pg, future=True) as session:
            assert placement_of_window(session, str(window)) == {
                "A": TargetPlacement(300, 95.0),
                "B": TargetPlacement(900, None),
            }

    def test_a_window_with_no_placement_is_refused_with_what_to_run(self, pg: Any) -> None:
        with Session(pg, future=True) as session:
            with pytest.raises(WindowNotPlacedError, match="place_window_targets"):
                placement_of_window(session, str(uuid.uuid4()))


def test_the_recorded_membership_names_both_sources() -> None:
    """A restricted delta says where each half of its stratum came from."""
    import inspect

    from protea.core.operations import compare_paired_panels

    source = inspect.getsource(compare_paired_panels)
    assert '"window_axes": "window_placement"' in source
    assert '"retrieval_axes": "baseline"' in source
