"""The targets a native writer reads on a window, and how its prediction set names them.

A native writer (the count map today; term frequency, homology transfer and the own
annotations read windows the same way) asks about a window's targets and records
which proteins it asked about, the population a comparison checks
(``protea.core.method_seal``). Predicted sets name it by a query set; native sets named
it so far by the digest of the list (``query_population``). One population should have
one spelling, so a native set uses the query set wherever one declares exactly its
proteins:

- **a window kept to a declared sample** (``evaluation_set.target_query_set_id``, a
  ``window_sample`` set): the targets are the sample's proteins, read from the set and
  checked against its sha256. The window's truth is never read. The set names them by
  that query set and records no digest.
- **any other window**: the targets are the proteins its truth scores (``nk``, ``lk``
  and ``pk``), as before. The set names them by the window's ``evaluation_targets``
  query set when one holds exactly those proteins, and by their digest otherwise,
  which is what every native set wrote until now.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.core._evaluation_groundtruth import load_evaluation_data_for_set
from protea.core.operations._window_on_sample import declared_sample
from protea.core.query_population import population_of
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.query.query_set import QuerySet, QuerySetEntry

__all__ = ["WindowTargets", "declared_targets_set", "window_targets"]


@dataclass(frozen=True)
class WindowTargets:
    """A window's targets, sorted, and the one way its set names them: a query set or a digest."""

    accessions: list[str]
    query_set_id: uuid.UUID | None
    population: dict[str, Any] | None

    @property
    def from_a_declared_set(self) -> bool:
        return self.query_set_id is not None


def declared_targets_set(session: Session, window_id: uuid.UUID, digest: str) -> uuid.UUID | None:
    """The window's ``evaluation_targets`` set whose entries are exactly the proteins of ``digest``."""
    candidates = session.execute(
        select(QuerySet.id)
        .where(QuerySet.evaluation_set_id == window_id)
        .where(QuerySet.source_kind == "evaluation_targets")
        .order_by(QuerySet.created_at, QuerySet.id)
    ).scalars()
    for candidate in list(candidates):
        entries = session.execute(
            select(QuerySetEntry.accession).where(QuerySetEntry.query_set_id == candidate)
        ).scalars()
        if population_of(entries)["sha256"] == digest:
            return candidate
    return None


def window_targets(session: Session, eset: EvaluationSet) -> WindowTargets:
    """The window's targets and their spelling, by the rules of this module's docstring."""
    if eset.target_query_set_id is not None:
        sample = declared_sample(session, eset.target_query_set_id)
        return WindowTargets(sorted(sample.accessions), sample.query_set.id, None)
    truth, _ = load_evaluation_data_for_set(session, eset)
    targets = sorted(set(truth.nk) | set(truth.lk) | set(truth.pk))
    population = population_of(targets)
    declared = declared_targets_set(session, eset.id, population["sha256"])
    if declared is not None:
        return WindowTargets(targets, declared, None)
    return WindowTargets(targets, None, population)
