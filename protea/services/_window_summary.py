"""A window's results as its panel reads them: each arm's factors, its nine cells, its profiles.

The page that listed a window's results downloaded every result's whole row,
artefact keys and digests included, to show nine numbers a row: about 11 KB a
result, 4.5 MB at the 420 results of a batch. The panel needs less and needs it
side by side: what each arm is (model, scoring, method, how many neighbours it
read, whether a target's own sequence could vote, how its scores were read),
the f_micro_w of each cell with the threshold it peaks at and whether that
threshold is the grid's edge, and the strata profiles the stratification left
on the result (``results["strata"]``). One query gives all of it.
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

#: The cells of a result, as the evaluator stores them: ``results[setting][aspect]``.
SETTINGS = ("NK", "LK", "PK")

_SUMMARY = text(
    """
    SELECT r.id::text AS id,
           r.created_at,
           r.prediction_set_id::text AS prediction_set_id,
           r.job_id::text AS job_id,
           jsonb_build_object(
               'NK', r.results -> 'NK', 'LK', r.results -> 'LK', 'PK', r.results -> 'PK',
               'reading', r.results -> 'reading', 'strata', r.results -> 'strata'
           ) AS results,
           ec.model_name AS model,
           ec.id::text AS embedding_config_id,
           sc.name AS scoring,
           m.short_name AS method,
           ps.meta -> 'scoring' -> 'depth' ->> 'max_k_position' AS depth_cut,
           coalesce(cand.limit_per_entry, ps.limit_per_entry) AS depth_retrieved,
           coalesce(cand.meta ->> 'exclude_self_neighbour',
                    ps.meta ->> 'exclude_self_neighbour') AS exclude_self,
           j.payload ->> 'score_reading' AS job_reading
    FROM evaluation_result r
    JOIN prediction_set ps ON ps.id = r.prediction_set_id
    LEFT JOIN prediction_set cand ON cand.id::text = ps.meta ->> 'scored_from'
    LEFT JOIN embedding_config ec ON ec.id = ps.embedding_config_id
    LEFT JOIN scoring_config sc ON sc.id::text = ps.meta ->> 'scoring_config_id'
    LEFT JOIN method m ON m.id = ps.method_id
    LEFT JOIN job j ON j.id = r.job_id
    WHERE r.evaluation_set_id = :window
    ORDER BY r.created_at, r.id
    """
)


def _cells(results: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """``"NK:MFO"`` -> its f_micro_w, the threshold it peaks at, the edge flag and the targets."""
    out: dict[str, dict[str, Any]] = {}
    for setting in SETTINGS:
        for aspect, cell in (results.get(setting) or {}).items():
            if not isinstance(cell, dict):
                continue
            best = (cell.get("maxima") or {}).get("f_micro_w") or {}
            out[f"{setting}:{aspect}"] = {
                "f": cell.get("f_micro_w"),
                "tau": best.get("tau"),
                "edge": best.get("edge"),
                "targets": cell.get("n_targets"),
            }
    return out


def summary_row(row: dict[str, Any]) -> dict[str, Any]:
    """One result as the panel shows it.

    ``depth`` is how many neighbours the arm read: the scored set's cut when it
    has one, else what its candidate set retrieved. A method's set has no
    neighbours, so its depth is ``None`` whatever its limit per entry says.
    ``reading`` falls back on the job's payload for results stored before the
    result recorded it, and on ``raw`` before readings existed.
    """
    results = row["results"] or {}
    neighbours = row["method"] is None and row["model"] is not None
    if not neighbours:
        depth = None
    elif row["depth_cut"] is not None:
        depth = int(row["depth_cut"])
    else:
        depth = row["depth_retrieved"]
    exclude = row["exclude_self"]
    strata = results.get("strata")
    return {
        "id": row["id"],
        "created_at": row["created_at"].isoformat() if row["created_at"] else None,
        "prediction_set_id": row["prediction_set_id"],
        "job_id": row["job_id"],
        "arm": {
            "model": row["model"],
            "embedding_config_id": row["embedding_config_id"],
            "scoring": row["scoring"],
            "method": row["method"],
            "depth": depth,
            "exclude_self": None if exclude is None else exclude == "true",
            "reading": (results.get("reading") or {}).get("score_reading")
            or row["job_reading"]
            or "raw",
        },
        "cells": _cells(results),
        "strata": None
        if not strata
        else {
            "job_id": strata.get("job_id"),
            "variant": strata.get("variant"),
            "profiles": strata.get("profiles") or {},
        },
    }


def window_summary_data(session: Session, eval_id: uuid.UUID) -> dict[str, Any]:
    """Every result of the window, oldest first, as :func:`summary_row` shows it.

    Raises :class:`EntityNotFoundError` when the window does not exist.
    """
    from protea.services._annotations_evaluation_helpers import assert_evaluation_set_exists

    assert_evaluation_set_exists(session, eval_id)
    rows = session.execute(_SUMMARY, {"window": eval_id}).mappings().all()
    return {
        "evaluation_set_id": str(eval_id),
        "results": [summary_row(dict(r)) for r in rows],
    }
