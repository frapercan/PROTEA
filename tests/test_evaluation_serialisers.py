"""What the evaluation endpoints say about a number's provenance.

Both serialisers used to omit the fields that decide whether two numbers may
be read side by side: the result's seal, and six of the eight columns that
identify a window. A page cannot check attributability against fields it never
receives, and the record holds the case that proves it matters -- two windows
over the same pair and the same graphs, differing only in ``truth_profile``,
whose deltas are 39.885 and 40.531.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from protea.infrastructure.orm.models.annotation.evaluation_result import EvaluationResult
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.services._annotations_evaluation_helpers import (
    evaluation_result_to_dict,
    evaluation_set_to_dict,
)

#: The eight columns ``uq_evaluation_set_identity`` names. Written here rather
#: than imported so that adding a ninth to the constraint fails this test
#: instead of silently leaving the new one out of the API.
IDENTITY = (
    "old_annotation_set_id",
    "new_annotation_set_id",
    "pivot_snapshot_id",
    "old_native_snapshot_id",
    "new_native_snapshot_id",
    "truth_profile",
    "toi_rule",
    "not_codes",
)


def _result(**over: object) -> EvaluationResult:
    fields: dict[str, object] = {
        "id": uuid.uuid4(),
        "evaluation_set_id": uuid.uuid4(),
        "prediction_set_id": uuid.uuid4(),
        "scoring_config_id": None,
        "reranker_model_id": None,
        "reranker_config": None,
        "job_id": None,
        "created_at": datetime(2026, 10, 28, 0, 58, tzinfo=UTC),
        "frame": None,
        "frame_digest": "f-c353408429d4871b759c1072",
        "temporal_window": None,
        "arms_enabled": None,
        "leakage_role": "test",
        "results": {},
    }
    fields.update(over)
    return EvaluationResult(**fields)


def _window(**over: object) -> EvaluationSet:
    fields: dict[str, object] = {
        "id": uuid.uuid4(),
        "old_annotation_set_id": uuid.uuid4(),
        "new_annotation_set_id": uuid.uuid4(),
        "pivot_snapshot_id": uuid.uuid4(),
        "old_native_snapshot_id": uuid.uuid4(),
        "new_native_snapshot_id": uuid.uuid4(),
        "truth_profile": "projected",
        "toi_rule": "live_in_both_ends",
        "not_codes": "experimental",
        "job_id": None,
        "created_at": datetime(2026, 10, 28, 0, 58, tzinfo=UTC),
        "stats": {"delta_proteins": 40531},
        "window_role": "valid",
    }
    fields.update(over)
    return EvaluationSet(**fields)


def test_a_result_carries_its_seal():
    """The one field that says whether two numbers are comparable. Without it a
    reader has the harness label, which is a different quantity: all eight live
    rows carry NULL in ``frame`` and a digest in ``frame_digest``."""
    body = evaluation_result_to_dict(_result())

    assert body["frame_digest"] == "f-c353408429d4871b759c1072"
    # And the label stays, separately, because it is not the same thing.
    assert body["frame"] is None


def test_an_unsealed_result_says_so_rather_than_omitting_the_field():
    """An absent key reads as "this API does not know about seals"; a null reads
    as "this row has none". Only the second is true, and only the second lets a
    page show that the comparison was never made."""
    body = evaluation_result_to_dict(_result(frame_digest=None))

    assert "frame_digest" in body
    assert body["frame_digest"] is None


def test_a_window_carries_all_eight_columns_of_its_identity():
    """Serving two of eight meant two windows could not be told apart from the
    API. ``7aa37929`` and ``ea0f4334`` are that case in the live record."""
    body = evaluation_set_to_dict(_window())

    missing = [name for name in IDENTITY if name not in body]
    assert missing == []
    assert body["truth_profile"] == "projected"
    assert body["toi_rule"] == "live_in_both_ends"
    assert body["not_codes"] == "experimental"


def test_two_windows_that_differ_only_in_the_truth_profile_serialise_apart():
    """The reason the profile is in the key at all. The same pair under the same
    graphs is a different measurement, and before this the API said they were
    the same window with two ids."""
    shared = {
        "old_annotation_set_id": uuid.uuid4(),
        "new_annotation_set_id": uuid.uuid4(),
        "pivot_snapshot_id": uuid.uuid4(),
        "old_native_snapshot_id": uuid.uuid4(),
        "new_native_snapshot_id": uuid.uuid4(),
    }
    closed = evaluation_set_to_dict(_window(truth_profile="as_closed", **shared))
    projected = evaluation_set_to_dict(_window(truth_profile="projected", **shared))

    assert {name: closed[name] for name in IDENTITY} != {
        name: projected[name] for name in IDENTITY
    }


def test_an_absent_snapshot_serialises_as_null_and_not_as_the_string_none():
    """A legacy row may carry no native snapshot. ``str(None)`` would publish
    the four characters "None" as if they were an id."""
    body = evaluation_set_to_dict(_window(old_native_snapshot_id=None))

    assert body["old_native_snapshot_id"] is None
