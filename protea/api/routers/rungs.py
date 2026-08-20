"""The campaign, as a line a reader can follow.

The instrument is organised by artifact: embeddings here, prediction sets
there, evaluations somewhere else. Nobody works that way. The work is a
sequence of rungs, each asking one question, and a reader arriving at the
board has no way to see which question they are looking at the answer to,
or what came before it.

Everything here is derived. A rung is not a table: it is the set of
prediction jobs that declared the same ``rung`` and ``window`` in their
meta, and its results are whatever those jobs' prediction sets were
evaluated into. That chain only closes because a prediction set now
records the job that produced it; before that receipt existed there was no
join from a published score back to the campaign that asked for it.

Nothing is written here and nothing is hardcoded. A rung with no finished
arms reports none rather than a placeholder, and the question each rung
asks is read off the axes that actually vary in it rather than from a
label somebody has to remember to update.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from protea.api.deps import get_session_factory

router = APIRouter(prefix="/rungs", tags=["rungs"])

#: Every job that declared a campaign, whatever it computed.
#:
#: A rung's arms are not always prediction jobs. Rung 1's third axis, the
#: score weighting, re-scores prediction sets that already exist and creates
#: no predictions at all, so a query rooted in predict_go_terms sees an
#: empty rung while 384 evaluations run inside it. The union is over what
#: DECLARED a rung, not over what happened to produce a file.
#:
#: The two halves reach the results differently. A prediction job owns a
#: prediction set (through the receipt it now writes) and the evaluations of
#: it; an evaluation job names its own result directly.
_ARMS = text(
    """
    SELECT j.meta ->> 'rung'      AS rung,
           j.meta ->> 'window'    AS window,
           j.meta ->> 'model'     AS model,
           (j.meta ->> 'k')::int  AS k,
           j.meta ->> 'scorer'    AS scorer,
           j.status::text         AS status,
           j.created_at           AS created_at,
           er.id                  AS evaluation_result_id,
           er.evaluation_set_id   AS evaluation_set_id,
           er.results             AS results
    FROM job j
    LEFT JOIN prediction_set ps
           ON ps.meta ->> 'job_id' = j.id::text
          -- A finished run, asserted rather than inspected. A cancelled
          -- job leaves its written batches behind and the prediction set
          -- carries no mark saying so: the completion state lives on the
          -- job. One ankh-base K=30 job was cancelled after one batch and
          -- left a 1,024-protein set that is indistinguishable from a
          -- finished one when you hold only the set.
          --
          -- batches rather than status alone, because SUCCEEDED is the
          -- job's verdict and the batch counts are its arithmetic, and a
          -- gate wants both.
          AND j.status::text = 'SUCCEEDED'
          AND (j.meta ->> 'batches_completed') IS NOT DISTINCT FROM (j.meta ->> 'expected_batches')
    LEFT JOIN evaluation_result er
           ON er.prediction_set_id = ps.id
    WHERE j.operation = 'predict_go_terms'
      AND j.parent_job_id IS NULL
      AND j.meta ? 'rung'

    UNION ALL

    SELECT j.meta ->> 'rung',
           j.meta ->> 'window',
           j.meta ->> 'model',
           (j.meta ->> 'k')::int,
           j.meta ->> 'scorer',
           j.status::text,
           j.created_at,
           er.id,
           er.evaluation_set_id,
           er.results
    FROM job j
    LEFT JOIN evaluation_result er
           ON er.job_id = j.id
    WHERE j.operation = 'run_cafa_evaluation'
      AND j.meta ? 'rung'
    """
)


#: Publication dates of the releases a window names.
#:
#: The campaign's naming discipline forbids release numbers in published
#: prose: "a reader must be able to follow the entire argument without
#: meeting a single identifier". A window is stored as "220-230" and has to
#: be rendered as dates, so the dates travel with it rather than being
#: looked up by whoever is writing.
_WINDOW_DATES = text(
    """
    SELECT source_version, source_published_at
    FROM annotation_set
    WHERE source_published_at IS NOT NULL
    """
)


def _window_dates(window: str | None, published: dict[str, Any]) -> dict[str, Any] | None:
    """The window's endpoints as dates, or None when either is unknown.

    None rather than a partial range: "Apr 2024 to (unknown)" reads as a
    frame with an open end, which is a different claim from one endpoint
    being unrecorded.
    """
    if not window or "-" not in window:
        return None
    lo, hi = window.split("-", 1)
    a, b = published.get(lo), published.get(hi)
    if a is None or b is None:
        return None
    return {"from": a.date().isoformat(), "to": b.date().isoformat()}


def _arm_key(row: dict[str, Any]) -> tuple[Any, ...]:
    """What makes two jobs the same arm.

    Read off the axes the job actually declares rather than fixed to
    (model, K). A rung that varies the scorer has three-part arms, and
    keying on two would collapse eight scorers into one cell and report a
    grid an eighth of its real size.
    """
    return (row.get("model"), row.get("k"), row.get("scorer"))


def _arm_counts(arms: list[dict[str, Any]]) -> dict[str, int]:
    """How the rung's grid stands, one verdict per arm.

    An arm is a (model, K) cell of the grid and can carry several jobs: a
    retry, or a run cancelled and reissued. Its verdict is the best thing
    that happened to it, because an arm whose second attempt succeeded is
    done, not simultaneously done and failed.
    """
    by_arm: dict[tuple[Any, ...], set[str]] = {}
    for a in arms:
        by_arm.setdefault(_arm_key(a), set()).add(a["status"])
    counts = {"arms": len(by_arm), "succeeded": 0, "running": 0, "failed": 0}
    for statuses in by_arm.values():
        if "SUCCEEDED" in statuses:
            counts["succeeded"] += 1
        elif "RUNNING" in statuses:
            counts["running"] += 1
        elif "FAILED" in statuses:
            counts["failed"] += 1
    return counts


def _question(models: set[str], ks: set[int], scorers: set[str]) -> str:
    """What the rung asks, read off what varies in it.

    Derived rather than declared so it cannot go stale: a rung that grows
    an axis says so the moment the arm lands.
    """
    axes = []
    if len(models) > 1:
        axes.append(f"which of {len(models)} representations")
    if len(ks) > 1:
        axes.append(f"how many neighbours ({min(ks)} to {max(ks)})")
    if len(scorers) > 1:
        axes.append(f"which of {len(scorers)} score weightings")
    if not axes:
        # One arm, or every arm identical: the rung is a measurement, not
        # a comparison, and saying "which of 1" would be silly.
        return "a single configuration, measured"
    return " and ".join(axes)


def _best(rows: list[dict[str, Any]], metric: str) -> dict[str, Any] | None:
    """The strongest arm in the rung, by the metric, over the whole grid.

    Averaged across every populated cell rather than read off one, because
    a rung's headline should not be the cell that happened to flatter it.
    """
    best: dict[str, Any] | None = None
    for row in rows:
        results = row.get("results") or {}
        values = [
            cell[metric]
            for cat in results.values()
            if isinstance(cat, dict)
            for cell in cat.values()
            if isinstance(cell, dict) and isinstance(cell.get(metric), (int, float))
        ]
        if not values:
            continue
        mean = sum(values) / len(values)
        if best is None or mean > best["value"]:
            best = {
                "model": row["model"],
                "k": row["k"],
                "scorer": row.get("scorer"),
                "value": round(mean, 4),
                "metric": metric,
                "cells": len(values),
                "evaluation_result_id": str(row["evaluation_result_id"]),
            }
    return best


@router.get("")
def list_rungs(
    factory: sessionmaker[Session] = Depends(get_session_factory),
    metric: str = Query(
        default="f_micro_w",
        description=(
            "Metric to rank each rung's arms by, averaged over every populated "
            "cell of the arm. Defaults to the IA-weighted micro F, the one the "
            "CAFA and LAFA leaderboards are quoted in."
        ),
    ),
) -> dict[str, Any]:
    """Every rung the record knows about, newest last."""
    with factory() as session:
        rows = [dict(r) for r in session.execute(_ARMS).mappings().all()]
        published = {
            r["source_version"]: r["source_published_at"]
            for r in session.execute(_WINDOW_DATES).mappings().all()
        }

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault((row["rung"], row["window"]), []).append(row)

    out = []
    for (rung, window), arms in sorted(grouped.items(), key=lambda kv: kv[0][0]):
        models = {a["model"] for a in arms if a["model"]}
        ks = {a["k"] for a in arms if a["k"] is not None}
        scorers = {a["scorer"] for a in arms if a["scorer"]}
        evaluated = [a for a in arms if a["evaluation_result_id"] is not None]
        eval_sets = {str(a["evaluation_set_id"]) for a in evaluated if a["evaluation_set_id"]}
        out.append(
            {
                "rung": rung,
                "window": window,
                "window_dates": _window_dates(window, published),
                "question": _question(models, ks, scorers),
                "models": sorted(models),
                "ks": sorted(ks),
                "scorers": sorted(scorers),
                # Arms, not jobs, throughout. Several jobs can be the same
                # arm when one was retried, and counting jobs makes a rung
                # of 48 arms report 49 successes, which reads as a grid
                # that grew rather than as a retry that worked.
                **_arm_counts(arms),
                "evaluated": len({_arm_key(a) for a in evaluated}),
                "evaluation_set_ids": sorted(eval_sets),
                "best": _best(evaluated, metric),
                "started_at": min(a["created_at"] for a in arms).isoformat(),
            }
        )
    return {"rungs": out, "metric": metric}
