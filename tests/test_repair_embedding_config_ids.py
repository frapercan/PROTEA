"""The repair moves exactly the three measured configs, onto the ids pinned for them.

Targets ``scripts/repair_embedding_config_ids.py`` without a database. The rows
are the live registry's own, as read on 2026-11-04, so these pin that the plan
reproduces each target from the row it will find, and refuses any other row.
"""

from __future__ import annotations

import importlib.util
import sys
import uuid
from pathlib import Path
from typing import Any

import pytest

from protea.infrastructure.orm.models.embedding.embedding_config import UNCHUNKED

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "repair_embedding_config_ids.py"
_spec = importlib.util.spec_from_file_location("repair_embedding_config_ids", _SCRIPT)
assert _spec and _spec.loader
repair = importlib.util.module_from_spec(_spec)
sys.modules["repair_embedding_config_ids"] = repair
_spec.loader.exec_module(repair)

_SHAPE: dict[str, Any] = {
    "normalize": False,
    "normalize_residues": False,
    "embedding_scale": 1.0,
    **UNCHUNKED,
}

LIVE_ROWS: dict[str, dict[str, Any]] = {
    "22166300-a178-599e-835c-afe1c007feb5": {
        "model_name": "residue-vocabulary:06fed75ca76d17be:f3a7fe16",
        "model_backend": "residue-vocabulary",
        "layer_indices": [46, 38, 26, 8, 0],
        "layer_agg": "concat",
        "pooling": "per-residue",
        "max_length": 1022,
        **_SHAPE,
    },
    "76792dc2-52fc-501f-9370-cf7a58e8336c": {
        "model_name": "learned-code:mean:sfjepa-world-holes-topk:cut211:e7ba37da",
        "model_backend": "learned-code",
        "layer_indices": [0],
        "layer_agg": "none",
        "pooling": "learned",
        "max_length": 1022,
        **_SHAPE,
    },
    "9d72dc44-7eed-517c-b45d-82b6e9f73bb1": {
        "model_name": "learned-code:mean:sfjepa-world-holes-topk:cut227:e7ba37da",
        "model_backend": "learned-code",
        "layer_indices": [0],
        "layer_agg": "none",
        "pooling": "learned",
        "max_length": 1022,
        **_SHAPE,
    },
}


def test_the_plan_moves_exactly_the_three_measured_configs() -> None:
    assert {str(m.old) for m in repair.PLAN} == set(LIVE_ROWS)


@pytest.mark.parametrize("move", repair.PLAN, ids=lambda m: str(m.old)[:8])
def test_each_corrected_row_derives_its_pinned_target(move: Any) -> None:
    row = LIVE_ROWS[str(move.old)]
    assert repair.corrected_target(row, move) == move.new
    repair.refuse_unless_it_derives(row, move)


def test_only_the_residue_vocabulary_is_corrected_and_only_to_full_length() -> None:
    corrected = {str(m.old)[:8]: m.corrections for m in repair.PLAN if m.corrections}
    assert corrected == {"22166300": {"max_length": None}}


def test_a_row_that_is_not_the_measured_one_is_refused() -> None:
    move = repair.PLAN[1]
    other = {**LIVE_ROWS[str(move.old)], "pooling": "attention"}
    with pytest.raises(SystemExit, match="not the pinned"):
        repair.refuse_unless_it_derives(other, move)


def test_without_its_correction_the_vocabulary_lands_somewhere_else() -> None:
    # The reason the plan is pinned: derived from the row as it stands, the
    # residue vocabulary would move to ae931b10, which describes a truncation
    # its pass never applied.
    move = repair.PLAN[0]
    bare = repair.Move(old=move.old, new=move.new)
    derived = repair.corrected_target(LIVE_ROWS[str(move.old)], bare)
    assert str(derived).startswith("ae931b10")


def test_a_correction_other_than_null_is_refused_before_anything_is_written() -> None:
    class _NoSession:
        def execute(self, *_a: Any, **_k: Any) -> Any:
            raise AssertionError("nothing may be read or written")

    move = repair.Move(old=uuid.uuid4(), new=uuid.uuid4(), corrections={"max_length": 512})
    original_row = repair._row
    repair._row = lambda _s, _id: None
    try:
        with pytest.raises(SystemExit, match="only corrections to NULL"):
            repair._copy(_NoSession(), move)
    finally:
        repair._row = original_row
