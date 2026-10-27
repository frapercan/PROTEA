"""Whether each cell of the matrix may be read at all, computed per row.

``leakage_role`` travels on every ``evaluation_result`` and is a LABEL: the
caller stamped it in the payload. It says what the window was meant to be,
not whether this arm is entitled to it. The rule that decides that lives in
``protea.core.leakage_guard`` and it compares two things the row does not
carry: the release the arm was fitted against, and the two releases the
window spans.

So the matrix computes it. Four states, from
:func:`protea.core.leakage_guard.training_cut_verdict`, which is the same
function the dispatch guard refuses on:

    not_fitted     a pretrained backbone saw none of our annotations
    clean          fitted, and the cut is outside the window
    contaminated   fitted inside the window: the cell measures the model's
                   memory of the answer
    undeclared     fitted and the cut is unrecorded, so it cannot be
                   certified either way. Worse than known-dirty, because
                   known-dirty can be excluded

WHY A BADGE AND NOT A FILTER. Dropping contaminated cells would make the
screen disagree with the database for a reader who then queries it, and the
interesting fact about this campaign is precisely which arms are entitled to
which windows. The row says what it is; what the page does with it is the
page's decision.

TWO QUERIES AT MOST. The cuts come from one pass over ``embedding_config``
(there are a dozen), and the window releases are already in the payload
``_load_eval_set_metadata`` built, so no second scan of the evaluation sets.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, aliased

from protea.core._evaluation_leakage import FITTED_BACKENDS
from protea.core.leakage_guard import Frame, training_cut_verdict
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig


def _as_release(value: Any) -> int | None:
    """A GOA release ordinal, or None when the version is not one.

    Sources other than GOA carry versions that are not ordinals, and a window
    whose ends cannot be ordered has no inside for a cut to fall into.

    DIGITS ONLY, not ``int()``. Python accepts underscores as digit
    separators, so ``int("2026_01")`` is 202601: the Swiss-Prot release
    naming this store also holds would have been read as an enormous GOA
    ordinal, every cut would have fallen outside every window, and the badge
    would have said "clean" for all of them. A test caught it; the parse is
    narrow now because the failure was silent and in the safe-looking
    direction.
    """
    text = str(value).strip()
    return int(text) if text.isdigit() else None


def cuts_by_config(session: Session) -> dict[str, tuple[bool, int | None]]:
    """``{config_id: (fitted, training_release)}`` for every config.

    ``fitted`` comes from the backend tag, not from the cut column, for the
    reason ``_evaluation_leakage`` gives: an artifact that was fitted and
    failed to declare has NULL in the column, and reading NULL as "not
    fitted" would wave through exactly the case the guard exists to catch.
    """
    cut_set = aliased(AnnotationSet)
    stmt = select(
        EmbeddingConfig.id,
        EmbeddingConfig.model_backend,
        cut_set.source_version,
    ).outerjoin(cut_set, cut_set.id == EmbeddingConfig.trained_on_annotation_set_id)
    return {
        str(config_id): (backend in FITTED_BACKENDS, _as_release(version))
        for config_id, backend, version in session.execute(stmt).all()
    }


def frames_by_eval_set(eval_sets_payload: Iterable[dict[str, Any]]) -> dict[str, Frame]:
    """``{eval_set_id: Frame}`` from the metadata the response already holds."""
    frames: dict[str, Frame] = {}
    for entry in eval_sets_payload:
        start = _as_release(entry.get("old_source_version"))
        end = _as_release(entry.get("new_source_version"))
        if start is not None and end is not None:
            frames[str(entry["id"])] = Frame(start, end)
    return frames


def stamp_leak_verdicts(
    session: Session, rows: Iterable[dict[str, Any]], eval_sets_payload: Iterable[dict[str, Any]]
) -> None:
    """Add ``leak`` and the two numbers behind it to every row, in place.

    ``leak`` is ``None`` rather than a verdict when the window's ends are not
    release ordinals or the config is unknown: a verdict nobody can derive is
    not the same as a clean one, and the page has to be able to tell.
    """
    cuts = cuts_by_config(session)
    frames = frames_by_eval_set(eval_sets_payload)
    for row in rows:
        frame = frames.get(str(row.get("evaluation_set_id")))
        known = cuts.get(str(row.get("embedding_config_id")))
        if frame is None or known is None:
            row["leak"] = None
            row["training_release"] = None
            row["window_releases"] = None
            continue
        fitted, cut = known
        row["leak"] = training_cut_verdict(fitted=fitted, training_release=cut, frame=frame)
        row["training_release"] = cut
        row["window_releases"] = [frame.start, frame.end]


__all__ = ["cuts_by_config", "frames_by_eval_set", "stamp_leak_verdicts"]
