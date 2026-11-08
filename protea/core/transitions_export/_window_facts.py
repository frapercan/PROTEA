"""Everything about one window that every table needs, read ONCE.

WHY ONE OBJECT AND ONE READ. The first table written here loaded the window's
truth to check its own invariant, and the second needed the same truth for its
population. Two loads mean two fetches of the same parquet, two digest
verifications, and -- the part that matters -- two chances for the tables of one
bundle to be about slightly different reads. One read per window, passed
around, is the only shape where that cannot happen.

IT REFUSES A WINDOW WHOSE VERSIONS OR DATES ARE MISSING, rather than writing
NULL. The laboratory's leakage rule compares ``version_t_prime <= cut_version``,
so a NULL version does not fail the rule: it SKIPS it, and a bundle whose
leakage rule was never evaluated looks exactly like one that passed. A window's
release is indeterminate in 23 of the 130 evaluation sets in this database, so
this is a case that exists rather than a defensive flourish. Measured
2026-11-08: all 64 windows of revision ``d5416224`` carry both versions and both
dates, with horizons from 16 to 539 days.
"""

from __future__ import annotations

from datetime import date
from typing import Any, NamedTuple

from sqlalchemy.orm import Session

from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet


class WindowFacts(NamedTuple):
    """One window, read once: its truth, its two releases and its span."""

    eset: EvaluationSet
    #: The FULL evaluation set id as a string, which is the join key the
    #: laboratory's loader uses to match this window across tables. Never the
    #: semantic name and never an eight-character prefix: if one table carried
    #: the id and another the name, the negatives restriction would simply not
    #: apply and nothing would fail.
    window: str
    #: The verified truth of the window, fetched through its single door.
    data: Any
    version_t: str
    version_t_prime: str
    horizon_days: int


def _end(session: Session, annotation_set_id: Any, lado: str, window: Any) -> tuple[str, date]:
    """One end's release and publication date, both validated.

    It returns the VALUES and not the row so the caller's types narrow: a check
    inside a different function does not narrow ``str | None`` at the call
    site, and silencing that with a cast would be asserting what this function
    is here to establish.
    """
    aset = session.get(AnnotationSet, annotation_set_id)
    if aset is None:
        raise ValueError(f"window {window}: its {lado} annotation set {annotation_set_id} is gone")
    if not aset.source_version:
        raise ValueError(
            f"window {window}: its {lado} corpus records no source_version, so this "
            "window has no release to compare a leakage rule against. A NULL there "
            "does not fail the rule, it skips it"
        )
    if aset.source_published_at is None:
        raise ValueError(
            f"window {window}: its {lado} corpus release {aset.source_version} has no "
            "publication date, so the horizon cannot be computed. Run "
            "refresh_goa_release_dates first"
        )
    return aset.source_version, aset.source_published_at.date()


def refuse_after_the_cut(facts: WindowFacts, cut_version: str) -> None:
    """A window whose t\' is after the cut cannot train a model cut there.

    THE LEAKAGE RULE, APPLIED AT EXPORT. The laboratory's loader already checks
    ``version_t_prime <= cut_version`` and would refuse the whole bundle, so
    this is not the only guard; it is the one that stops the bundle being
    BUILT, the same way the pivot check already does.

    AND IT IS NOT HYPOTHETICAL. The first bundle written here declared
    ``cut_version`` 211 over 64 windows, and **15 of them ended after 211**:
    every transition from 211 to 212 through 225 to 226. All fifteen fall
    INSIDE the span of the long validation window, 211 to 227, so their gains
    are part of the truth that window scores. The table that run published
    carries ontology terms rather than annotations, so nothing leaked, but the
    bundle declared a scope that was not its own and the next table would have
    leaked. The desktop session found it by reading the manifest against the
    design.
    """
    if int(facts.version_t_prime) <= int(cut_version):
        return
    raise ValueError(
        f"window {facts.window} runs {facts.version_t} -> {facts.version_t_prime}, "
        f"and this bundle declares cut_version {cut_version}. A window ending after "
        "the cut cannot train a model cut there: its gain is in the model's future, "
        "and for this campaign it is also inside the span the long validation window "
        "scores. Declare a later cut_version, or name windows that end at or before "
        "this one"
    )


def facts_of(session: Session, eset: EvaluationSet) -> WindowFacts:
    """Read one window's truth and its two releases, or refuse and say which.

    The truth comes through ``load_evaluation_data_for_set``, which is the one
    door that verifies the parquet's digest before believing its content.
    """
    from protea.core.evaluation import load_evaluation_data_for_set

    version_t, fecha_t = _end(session, eset.old_annotation_set_id, "old", eset.id)
    version_t1, fecha_t1 = _end(session, eset.new_annotation_set_id, "new", eset.id)
    horizonte = (fecha_t1 - fecha_t).days
    if horizonte <= 0:
        raise ValueError(
            f"window {eset.id} spans {horizonte} days, from release "
            f"{version_t} to {version_t1}. Releases 192 and 193 "
            "are byte-identical and share a date, so a transition between them is a "
            "window over one file counted twice"
        )
    data, _ = load_evaluation_data_for_set(session, eset)
    return WindowFacts(
        eset=eset,
        window=str(eset.id),
        data=data,
        version_t=version_t,
        version_t_prime=version_t1,
        horizon_days=horizonte,
    )
