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
"""

from __future__ import annotations

import uuid

from sqlalchemy.orm import Session

from protea.core.leakage_guard import Frame, check_training_cut
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet

#: Backends produced only by fitting against annotations. Anything else is a
#: pretrained backbone used as it ships, which has no cut to declare.
FITTED_BACKENDS: frozenset[str] = frozenset({"residue-sparse", "learned-code"})


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

    Silent when the frame cannot be expressed in release ordinals, which is
    a property of older evaluation sets rather than of the encoding, and
    refusing those would block work the guard has nothing to say about.
    """
    frame = frame_of(session, eval_set)
    if frame is None:
        return
    config = session.get(EmbeddingConfig, pred_set.embedding_config_id)
    if config is None:
        return
    check_training_cut(
        fitted=config.model_backend in FITTED_BACKENDS,
        training_release=_release(session, config.trained_on_annotation_set_id),
        frame=frame,
        name=f"encoding {config.model_name!r}",
    )
