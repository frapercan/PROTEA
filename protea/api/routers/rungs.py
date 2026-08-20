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

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from protea.api.deps import get_session_factory

router = APIRouter(prefix="/rungs", tags=["rungs"])

#: Prediction jobs that declared a campaign, with the arm each one is and
#: whatever it was evaluated into. LEFT JOINs throughout: an arm that is
#: still running is part of the rung and has to be counted as one.
_ARMS = text(
    """
    SELECT j.meta ->> 'rung'      AS rung,
           j.meta ->> 'window'    AS window,
           j.meta ->> 'model'     AS model,
           (j.meta ->> 'k')::int  AS k,
           j.status::text         AS status,
           j.created_at           AS created_at,
           ps.id                  AS prediction_set_id,
           er.id                  AS evaluation_result_id,
           er.evaluation_set_id   AS evaluation_set_id,
           er.results             AS results
    FROM job j
    LEFT JOIN prediction_set ps
           ON ps.meta ->> 'job_id' = j.id::text
    LEFT JOIN evaluation_result er
           ON er.prediction_set_id = ps.id
    WHERE j.operation = 'predict_go_terms'
      AND j.parent_job_id IS NULL
      AND j.meta ? 'rung'
    """
)


def _arm_counts(arms: list[dict[str, Any]]) -> dict[str, int]:
    """How the rung's grid stands, one verdict per arm.

    An arm is a (model, K) cell of the grid and can carry several jobs: a
    retry, or a run cancelled and reissued. Its verdict is the best thing
    that happened to it, because an arm whose second attempt succeeded is
    done, not simultaneously done and failed.
    """
    by_arm: dict[tuple[str | None, int | None], set[str]] = {}
    for a in arms:
        by_arm.setdefault((a["model"], a["k"]), set()).add(a["status"])
    counts = {"arms": len(by_arm), "succeeded": 0, "running": 0, "failed": 0}
    for statuses in by_arm.values():
        if "SUCCEEDED" in statuses:
            counts["succeeded"] += 1
        elif "RUNNING" in statuses:
            counts["running"] += 1
        elif "FAILED" in statuses:
            counts["failed"] += 1
    return counts


def _question(models: set[str], ks: set[int]) -> str:
    """What the rung asks, read off what varies in it.

    Derived rather than declared so it cannot go stale: a rung that grows
    an axis says so the moment the arm lands.
    """
    axes = []
    if len(models) > 1:
        axes.append(f"which of {len(models)} representations")
    if len(ks) > 1:
        axes.append(f"how many neighbours ({min(ks)} to {max(ks)})")
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
                "value": round(mean, 4),
                "metric": metric,
                "cells": len(values),
                "evaluation_result_id": str(row["evaluation_result_id"]),
            }
    return best


@router.get("")
def list_rungs(
    factory: sessionmaker[Session] = Depends(get_session_factory),
    metric: str = "f_micro_w",
) -> dict[str, Any]:
    """Every rung the record knows about, newest last."""
    with factory() as session:
        rows = [dict(r) for r in session.execute(_ARMS).mappings().all()]

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault((row["rung"], row["window"]), []).append(row)

    out = []
    for (rung, window), arms in sorted(grouped.items(), key=lambda kv: kv[0][0]):
        models = {a["model"] for a in arms if a["model"]}
        ks = {a["k"] for a in arms if a["k"] is not None}
        evaluated = [a for a in arms if a["evaluation_result_id"] is not None]
        eval_sets = {str(a["evaluation_set_id"]) for a in evaluated if a["evaluation_set_id"]}
        out.append(
            {
                "rung": rung,
                "window": window,
                "question": _question(models, ks),
                "models": sorted(models),
                "ks": sorted(ks),
                # Arms, not jobs, throughout. Several jobs can be the same
                # arm when one was retried, and counting jobs makes a rung
                # of 48 arms report 49 successes, which reads as a grid
                # that grew rather than as a retry that worked.
                **_arm_counts(arms),
                "evaluated": len({(a["model"], a["k"]) for a in evaluated}),
                "evaluation_set_ids": sorted(eval_sets),
                "best": _best(evaluated, metric),
                "started_at": min(a["created_at"] for a in arms).isoformat(),
            }
        )
    return {"rungs": out, "metric": metric}
