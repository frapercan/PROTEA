"""What ``transitions.query_state`` promises a reader of the training report.

THE FIGURE THIS EVENT CARRIES WAS MISREAD, and the misreading was written down.
A comment beside ``discarded_off_axis`` said the count was expected to be zero
and was the first thing to read before trusting a bundle. Measured over the 65
published windows it is 689.055 rows, 1,54 per cent of the raw cards, and it
tracks the window's distance from the axis's era: 0,02 per cent on ``205->211``
and 4,54 on ``170->171``. Only the end AT THE CUT sits on the pivot's own era.

So the event has to carry two things a total cannot carry, and these tests pin
both because the reader is in another repository and its silence is the failure
mode: a dropped field leaves a report that still renders, with a column of
blanks nobody notices.

1. **The split per moment.** The explanation is about the two ends being at
   different distances from the axis, and a total cannot be crossed against it.
2. **Both eras of the window.** A drop of 4,54 per cent means nothing until it
   sits beside the age it tracks, and the age is not derivable from the window
   id.

And the window is the FULL id in both, because the loader joins by that string
as text: a prefix would join to nothing and nothing would fail.
"""

from __future__ import annotations

import uuid
from typing import Any

from protea.core.operations import export_transitions_dataset as E
from protea.core.transitions_export._query_state import (
    Collapse,
    Discards,
    WindowFacts,
    WindowState,
)


class _Capturado:
    """An ``EmitFn`` that keeps what it was handed."""

    def __init__(self) -> None:
        self.eventos: list[tuple[str, str | None, dict[str, Any], str]] = []

    def __call__(self, event: str, message: str | None, fields: dict[str, Any], level: Any) -> None:
        self.eventos.append((event, message, fields, str(level)))

    @property
    def campos(self) -> dict[str, Any]:
        assert len(self.eventos) == 1, f"expected one event, got {len(self.eventos)}"
        return self.eventos[0][2]


VENTANA = str(uuid.uuid4())


def _state() -> WindowState:
    """A window that drops at ``t`` and not at ``t'``, which is the live shape."""
    return WindowState(
        window=VENTANA,
        rows=1200,
        by_moment={"t": 500, "t'": 700},
        evidence={"t": {"IDA": 500}, "t'": {"IDA": 700}},
        distinct_terms=43,
        negated=4,
        collapse=Collapse(gaf=300, remap=7),
        discards=Discards(off_axis=55, crossed_aspect=0, no_evidence=0),
        off_axis_by_moment={"t": 55, "t'": 0},
    )


def _facts() -> WindowFacts:
    return WindowFacts(
        eset=None,
        window=VENTANA,
        data=None,
        version_t="170",
        version_t_prime="171",
        horizon_days=61,
        old_end_snapshot=None,
        new_end_snapshot=None,
    )


def test_the_event_carries_the_off_axis_split_per_moment():
    """The total and both of its halves, so the explanation can be checked."""
    emit = _Capturado()

    E._emit_state(_state(), _facts(), emit)

    campos = emit.campos
    assert campos["discarded_off_axis"] == 55
    assert campos["off_axis_t"] == 55
    assert campos["off_axis_t_prime"] == 0
    assert campos["off_axis_t"] + campos["off_axis_t_prime"] == campos["discarded_off_axis"]


def test_the_event_carries_both_eras_of_the_window():
    """The age the drop tracks, beside the drop.

    Without these two fields a report can print 4,54 per cent and cannot say
    whether that is a window ten years from the axis or one beside it.
    """
    emit = _Capturado()

    E._emit_state(_state(), _facts(), emit)

    campos = emit.campos
    assert campos["version_t"] == "170"
    assert campos["version_t_prime"] == "171"


def test_the_event_names_the_window_by_its_full_id():
    """The join key, as text, in the field and not only in the message.

    The message abbreviates to eight characters for a human reading a log. A
    reader that joined on that would join to nothing, which is why the field is
    separate from the message and is asserted here.
    """
    emit = _Capturado()

    E._emit_state(_state(), _facts(), emit)

    assert emit.campos["window"] == VENTANA
    assert len(emit.campos["window"]) == 36
