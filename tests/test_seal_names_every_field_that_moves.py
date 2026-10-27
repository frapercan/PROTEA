"""The seal must name every field that changes the number.

A digest answers one question: are these two results measuring the same thing.
It can only answer it about the fields it is made of, so a field that moves the
number and is absent from the material makes the seal assert comparability it
never checked.

Three stored results are the reason this file exists. They share all six fields
the seal used to cover -- one evaluation set, one pivot, one accretion set, and
all three caps null -- and sealed to one digest. One ran with
``restrict_gt_to_predicted`` at its default and the other two with it off, so
one scored the predicted cohort and two scored the whole ground truth; two of
them carry 107 proteins. The seal called all three comparable, which is the
failure the operation exists to end rather than to commit.

The mirror defect gets its own tests. Resolving absence to a default is only
right for fields whose absence means that default VALUE: a payload with no
``th_step`` swept the same grid as one passing ``0.01``, so they must seal
alike, while a payload with no ``max_distance`` did not cap at a default
distance -- it did not cap -- so it must keep sealing apart from one that did.
Getting that backwards splits one frame in two over how a request was spelled.
"""

from __future__ import annotations

import hashlib
import json
import re

from protea.core.operations.seal_evaluation_frames import (
    _DIGEST_WIDTH,
    _FRAME_FIELDS,
    _PAYLOAD_DEFAULTS,
    _ROWS,
    frame_digest,
    frame_material,
)

#: A row with everything the seal requires, so each test below can vary one
#: field and attribute the difference to it. Shaped on the three real results.
_BASE = {
    "evaluation_set_id": "c35ee267-0000-0000-0000-000000000000",
    "pivot_snapshot_id": "2ba77c97-0000-0000-0000-000000000000",
    "information_accretion_set_id": "978530d8-0000-0000-0000-000000000000",
    "temporal_window": None,
    "max_terms": None,
    "max_distance": None,
    "restrict_gt_to_predicted": None,
    "th_step": None,
    "toi_file": None,
}


def _seal(**overrides: object) -> str | None:
    return frame_digest({**_BASE, **overrides})


def test_the_cohort_switch_seals_two_frames() -> None:
    """The regression. Scoring the predicted cohort and scoring the whole
    ground truth are not one frame, whatever else the two runs agree on."""
    assert _seal(restrict_gt_to_predicted="false") != _seal(restrict_gt_to_predicted="true")


def test_the_three_colliding_results_no_longer_share_a_seal() -> None:
    """The exact shape that collided: all six old fields equal, the cohort
    switch the only difference, and the two spellings of off agreeing."""
    default_on = _seal()
    off_a = _seal(restrict_gt_to_predicted="false")
    off_b = _seal(restrict_gt_to_predicted="false")
    assert off_a == off_b, "two runs with the same cohort must seal alike"
    assert default_on != off_a
    assert len({default_on, off_a, off_b}) == 2


def test_the_threshold_grid_seals_two_frames() -> None:
    """A finer grid optimises over more thresholds and reports a higher Fmax,
    which is the whole reason the payload documents keeping it at 0.01."""
    assert _seal(th_step="0.005") != _seal(th_step="0.01")


def test_the_terms_of_interest_file_seals_two_frames() -> None:
    """TOI decides which terms count toward precision and recall. Deriving
    them from the pivot is a different frame from supplying a file."""
    derived = _seal()
    supplied = _seal(toi_file="/handoff/toi-window.txt")
    other = _seal(toi_file="/handoff/toi-lafa.txt")
    assert len({derived, supplied, other}) == 3


def test_absence_of_a_defaulted_field_seals_as_its_default() -> None:
    """The mirror defect. A payload that omitted the field ran with the
    default, and scored exactly what a payload passing it explicitly scored,
    so the two must not be told apart by how the request was written."""
    for field, default in _PAYLOAD_DEFAULTS.items():
        assert _seal(**{field: None}) == _seal(**{field: str(default).lower()}), (
            f"{field} absent must seal as {field}={default}"
        )


def test_a_number_seals_by_its_value_and_not_its_spelling() -> None:
    """JSONB hands these back as text, so one grid arrives as several
    strings. They are one frame."""
    assert _seal(th_step="0.01") == _seal(th_step="0.010") == _seal(th_step=0.01)
    assert _seal(max_distance="0.5") == _seal(max_distance="0.50")


def test_an_absent_cap_still_seals_apart_from_a_cap_that_was_set() -> None:
    """The documented behaviour the defaults must not sweep up: absence of a
    cap is the absence of a choice, not a default value, so it keeps its own
    frame. ``max_distance`` is deliberately outside ``_PAYLOAD_DEFAULTS``."""
    assert "max_distance" not in _PAYLOAD_DEFAULTS
    assert "max_terms" not in _PAYLOAD_DEFAULTS
    assert _seal(max_distance=None) != _seal(max_distance="2.0")
    assert _seal(max_terms=None) != _seal(max_terms="500")


def test_every_sealed_field_is_one_the_query_actually_selects() -> None:
    """Pinned to the SQL rather than to a remembered list. A field named in
    the material but never selected arrives as ``None`` on every row, so it
    would silently contribute a constant and seal nothing apart."""
    selected = set(re.findall(r"AS\s+(\w+)", str(_ROWS)))
    missing = [field for field in _FRAME_FIELDS if field not in selected]
    assert not missing, f"sealed but never selected: {missing}"


def test_the_stored_material_hashes_to_the_digest_it_is_filed_under() -> None:
    """``evaluation_frame`` keeps the fields beside the key so a reader can
    recover what a seal stands for. That only works while the fields hash to
    the key: the operation records what :func:`frame_material` returns, and
    the digest must be taken over the same thing. Recording the raw row would
    file a frame under a key its own contents do not produce."""
    row = {**_BASE, "restrict_gt_to_predicted": "false", "th_step": "0.010"}
    canonical = json.dumps(frame_material(row), sort_keys=True, separators=(",", ":"))
    recomputed = "f-" + hashlib.sha256(canonical.encode()).hexdigest()[:_DIGEST_WIDTH]
    assert recomputed == frame_digest(row)


def test_the_material_is_resolved_and_not_the_raw_row() -> None:
    """The point of recording material rather than the row: a reader seeing
    the frame learns the grid cafaeval swept and the cohort it scored, not
    which keys the request happened to omit."""
    material = frame_material({**_BASE})
    assert material["th_step"] == 0.01
    assert material["restrict_gt_to_predicted"] is True
    assert material["max_distance"] is None


def test_an_unrecoverable_frame_is_still_refused() -> None:
    """Widening the material must not turn a row that cannot be attributed
    into one that can."""
    assert _seal(evaluation_set_id=None) is None
    assert _seal(information_accretion_set_id=None) is None
