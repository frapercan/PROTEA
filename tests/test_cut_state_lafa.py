"""The state at a window's start, as the window's own frame states it.

A count map fits on every donor's state at t0 and keeps every target from being
given a term it is denied there. A window of LAFA's frame has no pivot and
states each end under its own ontology, so its t0 is read that way: t0's rows
named under t0's ontology, t0's NOT terms and everything under them taken out,
closed upwards. Only t0's NOT rows count: one written at t1 is information from
after the cut.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest

from protea.core import _cut_state as cs
from protea.core._lafa_frame_data import EndOntology

T0 = EndOntology(
    live={
        "GO:0008150": "P",
        "GO:0000002": "P",
        "GO:0000003": "P",
        "GO:0000005": "P",
        "GO:0000006": "P",
    },
    parents={
        "GO:0000003": frozenset({"GO:0000002"}),
        "GO:0000005": frozenset({"GO:0000002"}),
        "GO:0000006": frozenset({"GO:0000005"}),
        "GO:0000002": frozenset({"GO:0008150"}),
    },
    alt_ids={"GO:0000013": "GO:0000003"},
    replaced_by={},
)


def _eset(profile: str) -> SimpleNamespace:
    return SimpleNamespace(
        truth_profile=profile,
        old_native_snapshot_id=uuid.uuid4(),
        old_annotation_set_id=uuid.uuid4(),
        not_codes="experimental",
    )


def test_a_lafa_window_states_t0_under_t0s_ontology_with_t0s_not_terms(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    eset = _eset("own_graph")
    asked: dict[str, object] = {}

    def ontology(_session: object, snapshot: object) -> EndOntology:
        asked["snapshot"] = snapshot
        return T0

    def rows(_session: object, set_id: object, experimental_only: bool) -> tuple:
        asked["set"], asked["experimental_only"] = set_id, experimental_only
        return {"P1": {"GO:0000013", "GO:0000005"}}, {"P1": {"GO:0000005"}}

    monkeypatch.setattr(cs, "load_end_ontology", ontology)
    monkeypatch.setattr(cs, "load_end_rows", rows)

    state = cs.window_state(None, eset)  # type: ignore[arg-type]

    # The alt id is named as its primary and closed upwards; the NOT term left
    # first, and it and everything under it are what P1 is denied.
    assert state.positives == {"P1": {"P": {"GO:0000003", "GO:0000002", "GO:0008150"}}}
    assert state.negatives == {"P1": {"GO:0000005", "GO:0000006"}}
    assert state.aspect == dict(T0.live)
    assert asked == {
        "snapshot": eset.old_native_snapshot_id,
        "set": eset.old_annotation_set_id,
        "experimental_only": True,
    }


def test_every_other_window_keeps_the_reconciled_state(monkeypatch: pytest.MonkeyPatch) -> None:
    eset = _eset("as_closed")
    built = object()
    seen: dict[str, object] = {}

    def reconciled(_session: object, window: object) -> object:
        seen["window"] = window
        return built

    monkeypatch.setattr(cs, "cut_state", reconciled)
    monkeypatch.setattr(cs, "window_of", lambda e: ("the window of", e))
    assert cs.window_state(None, eset) is built  # type: ignore[arg-type]
    assert seen["window"] == ("the window of", eset)
