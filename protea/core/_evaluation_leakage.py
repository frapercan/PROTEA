"""Refuse to score an encoding the frame cannot certify.

``leakage_guard`` has existed since the morning of 2026-08-20 with a
correct rule, a full docstring and its own tests, and until this module
nothing called it. ``plans/rungs.yaml`` declares for rung 2 that "CI
refuses one whose cut falls inside the frame it is about to be scored
in", and CI did not, because the refusal lived in a function with no
caller. Three fitted artifacts were about to be evaluated against a gate
that was decorative.

This is the caller. It sits at the start of an evaluation, where the
frame is known and the encoding is reachable through the prediction set.

WHAT ``fitted`` MEANS, AND WHY IT IS NOT CIRCULAR

The guard asks the caller to assert whether the encoding was fitted, and
that assertion cannot come from the training-cut column: an artifact that
was fitted and failed to declare has NULL there, and reading NULL as "not
fitted" would wave through precisely the case the guard exists to catch.

It comes from the backend tag instead. ``residue-sparse`` and
``learned-code`` are produced only by operations that read a frozen,
fitted artifact; the pretrained backbones carry their own family name.
The tag is a fact about how a code was computed rather than a label
anyone chose, which is what makes it usable as evidence here.

A METHOD IS CERTIFIED BY WHAT IT DECLARES (ADR-D60, decision 3). A
prediction set that names a method is checked on everything the method
declares that has a release: its instance cut, every training window it
was fitted or held out on, and every pin reference that names a dated
platform row (``DATED_REFERENCES``). Each must end at or before the
release where the frame starts, the boundary ``training_cut_verdict``
draws, and one that cannot be dated is refused like an undeclared cut. The
silence above for a frame without release ordinals does not extend to a
method: there is nothing to certify its declarations against, so it is
refused. A pin whose references name nothing datable (a store key, an
image, a weights row that declares no training cut) makes no claim about
labels and is not refused here; under the author's rule on weights,
label-free pretrained weights enter as declared exceptions and weights
fitted on GO labels are refit in the platform, where they arrive with
training windows this guard does date.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from sqlalchemy.orm import Session

from protea.core.contracts.method import row_reference
from protea.core.leakage_guard import (
    Frame,
    LeakageRefusal,
    check_training_cut,
    training_cut_verdict,
)
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.model_weights import ModelWeights
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.method.method import Method, MethodPin

#: Backends produced only by fitting against annotations. Anything else is a
#: pretrained backbone used as it ships, which has no cut to declare.
#:
#: ``ingested-method`` joins them for the same reason and not by analogy. The
#: rows under it are written by ``ingest_method_predictions``, which exists
#: only for a method whose score the platform did not compute: a model fitted
#: elsewhere, against some release of our annotations. Leaving it out would
#: have made the rule of the research screens -- a method appears only on
#: windows whose t0 is at or after its cut -- unenforceable for exactly the
#: arms it was written for, because an ingested set reaches its cut the same
#: way a learned code does, through ``trained_on_annotation_set_id``.
FITTED_BACKENDS: frozenset[str] = frozenset(
    {"residue-sparse", "learned-code", "residue-vocabulary", "ingested-method"}
)


def _release(session: Session, annotation_set_id: uuid.UUID | None) -> int | None:
    """The release ordinal of an annotation set, or None if it has none."""
    if annotation_set_id is None:
        return None
    row = session.get(AnnotationSet, annotation_set_id)
    if row is None or row.source_version is None:
        return None
    try:
        return int(row.source_version)
    except ValueError:
        return None


def frame_of(session: Session, eval_set: EvaluationSet) -> Frame | None:
    """The window an evaluation set scores, as release ordinals."""
    start = _release(session, eval_set.old_annotation_set_id)
    end = _release(session, eval_set.new_annotation_set_id)
    if start is None or end is None:
        return None
    return Frame(start=start, end=end)


def refuse_uncertifiable_encoding(
    session: Session, pred_set: PredictionSet, eval_set: EvaluationSet
) -> None:
    """Raise ``LeakageRefusal`` unless this encoding may be scored on this frame.

    For a configuration, silent when the frame cannot be expressed in release
    ordinals, which is a property of older evaluation sets rather than of the
    encoding, and refusing those would block work the guard has nothing to
    say about.

    A set that names a method is also held to :func:`refuse_uncertifiable_method`,
    before and independently of its configuration (a method of the
    embedding-reranker family carries both), and that check refuses a frame
    without ordinals.
    """
    frame = frame_of(session, eval_set)
    method_id = getattr(pred_set, "method_id", None)
    if method_id is not None:
        refuse_uncertifiable_method(session, method_id, frame)
    if frame is None:
        return
    config_id = pred_set.embedding_config_id
    config = session.get(EmbeddingConfig, config_id) if config_id is not None else None
    if config is None:
        return
    check_training_cut(
        fitted=config.model_backend in FITTED_BACKENDS,
        training_release=_release(session, config.trained_on_annotation_set_id),
        frame=frame,
        name=f"encoding {config.model_name!r}",
    )


@dataclass(frozen=True)
class DeclaredCut:
    """One release a method declares it read, and what declared it."""

    what: str
    release: int | None


def _window_end(session: Session, evaluation_set_id: uuid.UUID) -> int | None:
    """The release a window ends at, or None when it cannot be read."""
    window = session.get(EvaluationSet, evaluation_set_id)
    return None if window is None else _release(session, window.new_annotation_set_id)


def _pin_cut(session: Session, pin: MethodPin, ref: str) -> DeclaredCut | None:
    """The release a pin reference dates, or None when it names nothing datable.

    A weights row that declares no training cut makes no claim, like a
    pretrained backbone's NULL in ``embedding_config``. A dated reference to a
    row that is gone, or whose release cannot be read, is a claim nobody can
    check, and comes back undated so the caller refuses it.
    """
    row = row_reference(ref)
    if row is None:
        return None
    table, row_id = row
    what = f"its {pin.role} pin {pin.sha256[:12]} through {ref}"
    if table == "annotation_set":
        return DeclaredCut(what, _release(session, row_id))
    if table == "evaluation_set":
        return DeclaredCut(what, _window_end(session, row_id))
    weights = session.get(ModelWeights, row_id)
    if weights is None:
        return DeclaredCut(what, None)
    if weights.trained_on_annotation_set_id is None:
        return None
    return DeclaredCut(what, _release(session, weights.trained_on_annotation_set_id))


def declared_cuts(session: Session, method: Method) -> list[DeclaredCut]:
    """Every release ``method`` declares: instance, windows and dated pins."""
    cuts = [DeclaredCut("its instance cut", method.instance_release)]
    for window in method.training_windows:
        cuts.append(
            DeclaredCut(
                f"its {window.role} window {window.evaluation_set_id}",
                _window_end(session, window.evaluation_set_id),
            )
        )
    for pin in method.pins:
        for ref in pin.refs or ():
            cut = _pin_cut(session, pin, ref)
            if cut is not None:
                cuts.append(cut)
    return cuts


def refuse_uncertifiable_method(
    session: Session, method_id: uuid.UUID, frame: Frame | None
) -> None:
    """Raise ``LeakageRefusal`` unless everything the method declares ends by t0.

    Every offending declaration is named in one refusal, so a method with a
    late window and a late pin is not fixed one round trip at a time.
    """
    method = session.get(Method, method_id)
    if method is None:
        raise LeakageRefusal(
            f"the prediction set names method {method_id}, which is not in the record, "
            "so nothing it read can be dated."
        )
    name = f"method {method.code!r} ({method_id})"
    if frame is None:
        raise LeakageRefusal(
            f"{name} produced this prediction set, and the frame it would be scored on "
            "cannot be expressed in release ordinals, so its instance cut, its training "
            "windows and its pins cannot be certified against it."
        )
    late: list[str] = []
    for cut in declared_cuts(session, method):
        verdict = training_cut_verdict(fitted=True, training_release=cut.release, frame=frame)
        if verdict == "contaminated":
            late.append(f"{cut.what} ends at release {cut.release}")
        elif verdict == "undeclared":
            late.append(f"{cut.what} cannot be dated")
    if late:
        raise LeakageRefusal(
            f"{name} cannot be scored on the {frame.start} to {frame.end} frame: "
            + "; ".join(late)
            + f". Everything a method read must end at or before release {frame.start}, "
            "where the frame starts, or the evaluation measures its memory of the terms "
            "it scores."
        )
