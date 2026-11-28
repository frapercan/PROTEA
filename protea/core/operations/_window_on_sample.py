"""A window kept to a declared sample of its targets: its parent's truth, filtered.

``generate_evaluation_set`` with a ``target_query_set_id`` computes no delta. The
sample (a ``window_sample`` query set) names its parent window; the identity asked
for must be the parent's (the five ids, the four truth rules and the target
population), and so must the rules the running code applies to it; the parent's
groundtruth parquet is streamed from the store and verified against its recorded
sha256; and every bucket (``nk``, ``lk``, ``pk``, ``known``, ``pk_known``,
``removed``) is kept to the sample's proteins, as
:func:`protea.core._window_population.restricted_to` keeps a population. The truth
is exact by construction, cheaper than a build, and the same whatever revision built
the parent.

The stats start as a copy of the parent's (its mode, pivot, rules, terms of
interest, LAFA steps and population counts), so every reader resolves the same pivot
and the same terms of interest as on the parent; the counts the truth itself gives
(:meth:`protea.core.evaluation.EvaluationData.stats`) are then the kept truth's; and
``target_set_restriction`` records the parent, its groundtruth sha256, the proteins
kept and left out per bucket, and the copied keys that count the parent's build
rather than this window's.
"""

from __future__ import annotations

import tempfile
import uuid
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn
from protea.core.evaluation import EvaluationData, fetch_groundtruth_to_file
from protea.core.query_population import population_of
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.query.query_set import QuerySet, QuerySetEntry

__all__ = [
    "BUCKETS",
    "FRAME_INPUTS",
    "DeclaredSample",
    "RestrictedTruth",
    "TargetSetRefused",
    "declared_sample",
    "kept_to_the_sample",
    "refuse_another_identity",
    "restricted_groundtruth",
    "restricted_stats",
    "truth_on_sample",
]

#: The buckets of a window's truth, every one of them kept to the sample.
BUCKETS: tuple[str, ...] = ("nk", "lk", "pk", "known", "pk_known", "removed")

#: The stats keys a restricted window shares with its parent as what the window is
#: (its build's inputs), not as counts of the parent's build.
FRAME_INPUTS: frozenset[str] = frozenset(
    {
        "mode",
        "pivot_ontology_snapshot_id",
        "truth_profile",
        "toi_rule",
        "not_codes",
        "binding_rule",
        "target_population",
        "terms_of_interest",
        "lafa_workflow",
        "lafa_package",
    }
)


class TargetSetRefused(ValueError):
    """A window on a declared sample the operation will not build, with the reason."""


@dataclass(frozen=True)
class DeclaredSample:
    """The sample a window is kept to, the window it was drawn from, and its proteins."""

    query_set: QuerySet
    parent: EvaluationSet
    accessions: frozenset[str]


@dataclass(frozen=True)
class RestrictedTruth:
    """The parent's truth kept to the sample, and the proteins kept and left out per bucket."""

    data: EvaluationData
    kept: dict[str, int]
    outside: dict[str, int]


def declared_sample(session: Session, query_set_id: uuid.UUID) -> DeclaredSample:
    """The ``window_sample`` set named, its parent window and its entries' accessions.

    The entries are checked against the set's own sha256 (the population digest of its
    accession list), so the proteins read are the ones the set declares.
    """
    query_set = session.get(QuerySet, query_set_id)
    if query_set is None:
        raise TargetSetRefused(f"no query set {query_set_id}")
    if query_set.source_kind != "window_sample" or query_set.evaluation_set_id is None:
        raise TargetSetRefused(
            f"query set {query_set_id} is a {query_set.source_kind} set, not a window_sample: "
            "a window is kept only to a sample drawn from its own targets"
        )
    parent = session.get(EvaluationSet, query_set.evaluation_set_id)
    if parent is None:
        raise TargetSetRefused(f"the window of sample {query_set_id} is gone")
    accessions = frozenset(
        session.execute(
            select(QuerySetEntry.accession).where(QuerySetEntry.query_set_id == query_set.id)
        ).scalars()
    )
    if population_of(accessions)["sha256"] != query_set.content_sha256:
        raise TargetSetRefused(
            f"the entries of sample {query_set_id} do not hash to its sha256 "
            f"{query_set.content_sha256}: the set does not hold the proteins it declares"
        )
    return DeclaredSample(query_set, parent, accessions)


def refuse_another_identity(
    parent: EvaluationSet, asked: Mapping[str, Any], applied_rules: Mapping[str, str]
) -> None:
    """Refuse a window whose identity, or the rules applied to it, are not its parent's.

    ``asked`` holds the ten identity columns the request resolves to, by column name.
    The rules matter as well as the columns: the kept truth is the one the parent's
    build applied, and the row declares them as its own.
    """
    differ = {
        column: (str(value), str(getattr(parent, column)))
        for column, value in asked.items()
        if value != getattr(parent, column)
    }
    if differ:
        raise TargetSetRefused(
            f"the window asked for differs from the sample's window {parent.id} in "
            f"{sorted(differ)} (asked, parent: {differ}); a window on a sample is its "
            "window's identity over fewer proteins"
        )
    if dict(parent.truth_rules or {}) != dict(applied_rules):
        raise TargetSetRefused(
            f"window {parent.id} declares the truth rules {parent.truth_rules} and this "
            f"revision applies {dict(applied_rules)} to its identity: its truth, kept to the "
            "sample, would be declared under rules nobody applied"
        )


def _proteins_by_bucket(table: pa.Table) -> dict[str, int]:
    """How many distinct proteins each bucket of a truth table holds."""
    counted = table.group_by("bucket").aggregate([("protein_accession", "count_distinct")])
    found = {
        str(bucket): int(n)
        for bucket, n in zip(
            counted.column("bucket").to_pylist(),
            counted.column("protein_accession_count_distinct").to_pylist(),
            strict=True,
        )
    }
    return {bucket: found.get(bucket, 0) for bucket in BUCKETS}


def restricted_groundtruth(path: Path, accessions: frozenset[str]) -> RestrictedTruth:
    """The truth parquet at ``path`` with every bucket kept to ``accessions``.

    Read with pyarrow, so only the kept rows become Python objects: the parent's
    ``known`` holds millions of rows and the sample a few thousand proteins.
    """
    table = pq.read_table(path, columns=["protein_accession", "go_id", "bucket"])
    table = table.set_column(2, "bucket", pc.cast(table.column("bucket"), pa.string()))
    whole = _proteins_by_bucket(table)
    kept_rows = table.filter(
        pc.is_in(table.column("protein_accession"), value_set=pa.array(sorted(accessions)))
    )
    kept = _proteins_by_bucket(kept_rows)
    buckets: dict[str, dict[str, set[str]]] = {bucket: defaultdict(set) for bucket in BUCKETS}
    for protein, go_id, bucket in zip(
        kept_rows.column("protein_accession").to_pylist(),
        kept_rows.column("go_id").to_pylist(),
        kept_rows.column("bucket").to_pylist(),
        strict=True,
    ):
        buckets[bucket][protein].add(go_id)
    data = EvaluationData(**{bucket: dict(rows) for bucket, rows in buckets.items()})
    outside = {bucket: whole[bucket] - kept[bucket] for bucket in BUCKETS}
    return RestrictedTruth(data=data, kept=kept, outside=outside)


def restricted_stats(
    parent: EvaluationSet, truth: RestrictedTruth, sample: QuerySet
) -> dict[str, Any]:
    """The parent's stats, with the kept truth's own counts and the restriction recorded."""
    inherited = dict(parent.stats or {})
    own = truth.data.stats()
    return {
        **inherited,
        **own,
        "target_set_restriction": {
            "parent_evaluation_set_id": str(parent.id),
            "parent_groundtruth_sha256": parent.groundtruth_sha256,
            "query_set_id": str(sample.id),
            "query_set_sha256": sample.content_sha256,
            "kept": truth.kept,
            "outside": truth.outside,
            "copied_keys": sorted(
                key for key in inherited if key not in own and key not in FRAME_INPUTS
            ),
        },
    }


def truth_on_sample(sample: DeclaredSample, scratch: Path) -> tuple[EvaluationData, dict[str, Any]]:
    """The parent's verified truth kept to the sample, and the stats that say so.

    Refused when a sampled protein is not a target of the parent: a sample is drawn
    from its window's targets, so one that is not names another window's.
    """
    path = fetch_groundtruth_to_file(sample.parent, scratch / "parent-groundtruth.parquet")
    truth = restricted_groundtruth(path, sample.accessions)
    targets = set(truth.data.nk) | set(truth.data.lk) | set(truth.data.pk)
    strangers = sorted(sample.accessions - targets)
    if strangers:
        raise TargetSetRefused(
            f"{len(strangers)} proteins of sample {sample.query_set.id} are not targets of "
            f"window {sample.parent.id} (first {strangers[:3]})"
        )
    return truth.data, restricted_stats(sample.parent, truth, sample.query_set)


def kept_to_the_sample(
    session: Session, identity: Mapping[str, Any], applied_rules: Mapping[str, str], emit: EmitFn
) -> tuple[EvaluationData, dict[str, Any]]:
    """A window's truth on its declared sample: its parent's, verified and filtered.

    ``identity`` is the window asked for, by the columns of its identity, the
    declared sample among them; ``applied_rules`` the truth rules the running code
    applies to it.
    """
    asked = dict(identity)
    sample = declared_sample(session, asked.pop("target_query_set_id"))
    refuse_another_identity(sample.parent, asked, applied_rules)
    with tempfile.TemporaryDirectory(prefix="protea_eval_parent_") as tmp:
        data, stats = truth_on_sample(sample, Path(tmp))
    emit(
        "generate_evaluation_set.kept_to_the_sample",
        None,
        stats["target_set_restriction"],
        "info",
    )
    return data, stats
