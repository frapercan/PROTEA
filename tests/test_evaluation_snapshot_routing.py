"""One snapshot cannot resolve two annotation sets, and failing to says nothing.

``compute_evaluation_data`` builds one ``go_id`` map from one ontology snapshot
and resolves BOTH annotation sets through it. ``go_term.id`` is a surrogate
scoped to a snapshot, and in this database the id spaces of different snapshots
are DISJOINT, not overlapping: of the six pairs among the 220, 226, 227 and 230
annotation sets, exactly one shares a single internal id, and that pair is the
two sets that sit on the same snapshot. Neither frame the campaign uses is that
pair.

So the failure is total rather than partial, and it is silent: the loader drops
unresolvable ids where it tests ``if go_id and aspect``. Measured against the
live database on the 226 to 227 window:

    caller passes the OLD snapshot     NK=0       LK=0    PK=0
    caller passes the NEW snapshot     NK=88,193  LK=0    PK=0
    reconciled path                    NK=523     LK=622  PK=5,672

The second is the dangerous one. Nothing raises, every protein is classified
NK, and a request for one category is answered over the whole annotation set:
88,193 where the truth is 523. It returns a number that looks like a result.

``generate_evaluation_set`` has always branched correctly, which is why the
published board is sound. ``compute_prediction_metrics`` did not, and it is
reachable as ``GET /prediction-sets/{set_id}/metrics`` with all three ids as
free query parameters. The router docstring asked the caller to align them.
A contract addressed to the caller in prose is the defect these tests close.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from protea.core import evaluation as ev


def _session(old_snap: uuid.UUID, new_snap: uuid.UUID, *, pivot_exists: bool = True):
    """Session double dispatching on the requested model, not on call order."""
    old_set = SimpleNamespace(ontology_snapshot_id=old_snap)
    new_set = SimpleNamespace(ontology_snapshot_id=new_snap)
    sets = {}

    def _get(model, ident):
        name = getattr(model, "__name__", "")
        if "AnnotationSet" in name:
            return sets.get(ident)
        if "OntologySnapshot" in name:
            return object() if pivot_exists else None
        return None

    session = MagicMock()
    session.get.side_effect = _get
    return session, sets, old_set, new_set


OLD_ID, NEW_ID = uuid.uuid4(), uuid.uuid4()
SNAP_A, SNAP_B = uuid.uuid4(), uuid.uuid4()


def _run(old_snap, new_snap, pivot):
    session, sets, old_set, new_set = _session(old_snap, new_snap)
    sets[OLD_ID], sets[NEW_ID] = old_set, new_set
    with (
        patch.object(ev, "compute_evaluation_data") as direct,
        patch.object(ev, "compute_evaluation_data_reconciled") as reconciled,
    ):
        ev.compute_evaluation_data_for_sets(session, OLD_ID, NEW_ID, pivot)
    return direct, reconciled


def test_a_temporal_window_takes_the_reconciled_path() -> None:
    # The case that was broken: two sets on different snapshots. Routing this
    # to the direct builder is what returned 88,193 NK against a truth of 523.
    direct, reconciled = _run(SNAP_A, SNAP_B, SNAP_B)
    reconciled.assert_called_once()
    direct.assert_not_called()


def test_the_natives_come_from_the_sets_and_not_from_the_caller() -> None:
    # The caller's snapshot is the pivot. It is not evidence about where the
    # annotation sets live, and believing it was is the whole defect.
    _, reconciled = _run(SNAP_A, SNAP_B, SNAP_B)
    # (session, old_set_id, new_set_id, old_native, new_native, pivot)
    args = reconciled.call_args.args
    assert args[3] == SNAP_A, "old native must come from the old set"
    assert args[4] == SNAP_B, "new native must come from the new set"
    assert args[5] == SNAP_B, "the caller's snapshot is the pivot"


def test_a_pivot_that_is_neither_native_still_reconciles() -> None:
    third = uuid.uuid4()
    direct, reconciled = _run(SNAP_A, SNAP_B, third)
    reconciled.assert_called_once()
    assert reconciled.call_args.args[5] == third
    direct.assert_not_called()


def test_one_snapshot_everywhere_keeps_the_direct_path() -> None:
    # Same snapshot on both sides and as pivot is the only case where one
    # go_id map can resolve both sets, and it stays on the cheaper path.
    direct, reconciled = _run(SNAP_A, SNAP_A, SNAP_A)
    direct.assert_called_once()
    reconciled.assert_not_called()


def test_matching_natives_but_a_different_pivot_reconciles() -> None:
    # Both sets share a snapshot, but the answer is wanted in another term
    # universe. The direct builder cannot express that.
    direct, reconciled = _run(SNAP_A, SNAP_A, SNAP_B)
    reconciled.assert_called_once()
    direct.assert_not_called()


@pytest.mark.parametrize("missing", ["old", "new"])
def test_a_missing_annotation_set_is_refused_by_name(missing: str) -> None:
    session, sets, old_set, new_set = _session(SNAP_A, SNAP_B)
    if missing == "new":
        sets[OLD_ID] = old_set
    else:
        sets[NEW_ID] = new_set
    with pytest.raises(ev.SnapshotMismatchError, match="not found"):
        ev.compute_evaluation_data_for_sets(session, OLD_ID, NEW_ID, SNAP_B)


def test_a_pivot_snapshot_that_does_not_exist_is_refused() -> None:
    session, sets, old_set, new_set = _session(SNAP_A, SNAP_B, pivot_exists=False)
    sets[OLD_ID], sets[NEW_ID] = old_set, new_set
    with pytest.raises(ev.SnapshotMismatchError, match="term universe"):
        ev.compute_evaluation_data_for_sets(session, OLD_ID, NEW_ID, uuid.uuid4())
