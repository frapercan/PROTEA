"""The receipt of an exploration, computed from rows and never written by hand (ADR-D55).

It is computed while the arms are chosen and again in the report, and what no
row records says ``not recorded`` rather than a guess. This first version has
the four layers that existing rows already answer:

* **method**: whether the two arms differ in exactly the declared axis, by the
  same seal ``compare_paired_panels`` applies (``protea.core.method_seal``);
* **recipe**: each arm's embedding config, its identity fields compared field
  by field, and whether each id is the one its own recipe derives;
* **frame**: the frame digest of every linked result, and whether they agree;
* **scale**: the cells whose F maximum sits at an edge of the thresholds, from
  the ``maxima`` each result records, and the reading its scores were taken in.

Population, coverage and execution need machinery of their own and say
``not recorded`` until it exists.

Every read goes through a primary key or an indexed foreign key, a handful of
rows per exploration.
"""

from __future__ import annotations

import uuid
from collections import defaultdict
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from protea.core.embedding_identity import IDENTITY_FIELDS, derive_embedding_config_id
from protea.core.method_seal import CrossedMethodAxes, assert_one_axis, method_identity
from protea.infrastructure.orm.models.annotation.evaluation_result import EvaluationResult
from protea.infrastructure.orm.models.exploration.exploration import Exploration
from protea.infrastructure.orm.models.exploration.exploration_link import ExplorationLink

__all__ = ["NOT_RECORDED", "compute", "launch_blockers", "method_layer"]

NOT_RECORDED = "not recorded"

_PREDICTION_SET_SQL = text("SELECT * FROM prediction_set WHERE id = CAST(:id AS uuid)")
_CONFIG_SQL = text("SELECT * FROM embedding_config WHERE id = CAST(:id AS uuid)")
_READING_SQL = text(
    "SELECT COALESCE(payload ->> 'score_reading', 'raw') FROM job WHERE id = CAST(:id AS uuid)"
)


def _arm_sets(session: Session, exploration: Exploration) -> dict[str, list[uuid.UUID]]:
    """The prediction sets linked as arms, by arm name, in the order they were linked."""
    rows = session.execute(
        select(ExplorationLink.arm, ExplorationLink.prediction_set_id)
        .where(
            ExplorationLink.exploration_id == exploration.id,
            ExplorationLink.role == "arm",
            ExplorationLink.prediction_set_id.is_not(None),
        )
        .order_by(ExplorationLink.created_at, ExplorationLink.id)
    ).all()
    arms: dict[str, list[uuid.UUID]] = defaultdict(list)
    for arm, pset in rows:
        arms[arm or str(pset)].append(pset)
    return dict(arms)


def _row(session: Session, sql: Any, ident: Any) -> dict[str, Any] | None:
    found = session.execute(sql, {"id": str(ident)}).mappings().first()
    return dict(found) if found is not None else None


def method_layer(kind: str, axis: list[str], arms: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Whether two arms differ in exactly the declared axis.

    ``one_axis`` is held to the seal and reports ``crossed`` when a field
    outside the axis moved. ``full_method`` and ``instrument`` are not refused:
    the fields that differ are listed, which is what marks a full method as
    attributable to no component.
    """
    if len(arms) != 2:
        return {
            "status": NOT_RECORDED,
            "detail": f"the seal compares two arms with a prediction set each; {len(arms)} linked",
        }
    (name_a, a), (name_b, b) = arms.items()
    if kind == "one_axis":
        try:
            held = assert_one_axis(axis, a, b, names=(name_a, name_b))
        except CrossedMethodAxes as exc:
            return {"status": "crossed", "axis": axis, "detail": str(exc)}
        return {"status": "one axis", "axis": list(held)}
    try:
        ids = (method_identity(a, name=name_a), method_identity(b, name=name_b))
    except CrossedMethodAxes as exc:
        return {"status": "unreadable", "detail": str(exc)}
    return {
        "status": kind.replace("_", " "),
        "differs_in": sorted(k for k in ids[0] if ids[0][k] != ids[1][k]),
    }


def _reproduced(config: dict[str, Any]) -> str:
    """How the config's id is reproduced: by its recipe, by its name, or not at all."""
    from protea.core.operations.register_ingested_method import config_id_for

    live = uuid.UUID(str(config["id"]))
    if derive_embedding_config_id({f: config[f] for f in IDENTITY_FIELDS}) == live:
        return "recipe"
    if (
        config.get("model_backend") == "ingested-method"
        and config_id_for(config["model_name"]) == live
    ):
        return "name"
    return "nothing"


def _recipe_layer(session: Session, sets: dict[str, dict[str, Any]]) -> dict[str, Any]:
    configs = {
        arm: _row(session, _CONFIG_SQL, row["embedding_config_id"]) for arm, row in sets.items()
    }
    if not configs or any(c is None for c in configs.values()):
        return {"status": NOT_RECORDED}
    recipes: dict[str, dict[str, Any]] = {
        arm: {f: c[f] for f in IDENTITY_FIELDS} for arm, c in configs.items() if c is not None
    }
    arms = {
        arm: {
            "embedding_config_id": str(c["id"]),
            "reproduced_by": _reproduced(c),
            "recipe": recipes[arm],
        }
        for arm, c in configs.items()
        if c is not None
    }
    differs = sorted(f for f in IDENTITY_FIELDS if len({repr(r[f]) for r in recipes.values()}) > 1)
    broken = sorted(arm for arm, a in arms.items() if a["reproduced_by"] == "nothing")
    status = "an id its recipe does not derive" if broken else "reproduced"
    return {"status": status, "differs_in": differs, "arms": arms, "unreproduced": broken}


def _results(session: Session, exploration: Exploration) -> list[EvaluationResult]:
    linked = select(ExplorationLink.evaluation_result_id).where(
        ExplorationLink.exploration_id == exploration.id,
        ExplorationLink.evaluation_result_id.is_not(None),
    )
    return list(
        session.execute(select(EvaluationResult).where(EvaluationResult.id.in_(linked))).scalars()
    )


def _frame_layer(results: list[EvaluationResult]) -> dict[str, Any]:
    if not results:
        return {"status": NOT_RECORDED}
    digests = {str(r.id): r.frame_digest for r in results}
    if any(d is None for d in digests.values()):
        return {"status": "unsealed", "digests": digests}
    status = "one frame" if len(set(digests.values())) == 1 else "crossed"
    return {"status": status, "digests": digests}


def _censored(results: dict[str, Any]) -> list[str]:
    """Every ``setting/namespace/variant`` whose maximum sits at an edge."""
    cells = []
    for setting, namespaces in results.items():
        if not isinstance(namespaces, dict):
            continue
        for ns, metrics in namespaces.items():
            maxima = metrics.get("maxima") if isinstance(metrics, dict) else None
            for variant, at in (maxima or {}).items():
                if isinstance(at, dict) and at.get("edge"):
                    cells.append(f"{setting}/{ns}/{variant} at the {at['edge']}")
    return sorted(cells)


def _scale_layer(session: Session, results: list[EvaluationResult]) -> dict[str, Any]:
    if not results:
        return {"status": NOT_RECORDED}
    per_result: dict[str, Any] = {}
    for r in results:
        reading = (
            session.execute(_READING_SQL, {"id": str(r.job_id)}).scalar() if r.job_id else None
        )
        has_maxima = any(
            isinstance(m, dict) and "maxima" in m
            for ns in (r.results or {}).values()
            if isinstance(ns, dict)
            for m in ns.values()
        )
        per_result[str(r.id)] = {
            "score_reading": reading or NOT_RECORDED,
            "censored": _censored(r.results or {}) if has_maxima else NOT_RECORDED,
        }
    readings = {v["score_reading"] for v in per_result.values()}
    status = "one reading" if len(readings) == 1 else "readings differ"
    return {"status": status, "results": per_result}


def _declared_axis(exploration: Exploration) -> list[str]:
    """The rules' ``axis``, written as one field or as a list of them."""
    raw = (exploration.rules or {}).get("axis")
    return [raw] if isinstance(raw, str) else [str(f) for f in raw or []]


def _arm_rows(session: Session, exploration: Exploration) -> dict[str, dict[str, Any]]:
    """Each arm's whole prediction set row; an arm linked twice is read at its last link."""
    return {
        arm: row
        for arm, psets in _arm_sets(session, exploration).items()
        if (row := _row(session, _PREDICTION_SET_SQL, psets[-1])) is not None
    }


def launch_blockers(session: Session, exploration: Exploration) -> tuple[list[str], dict[str, Any]]:
    """What forbids freezing the rules now, and the method layer that says so.

    Only the method layer can block a launch: a one-axis exploration whose arms
    already differ in more than the declared axis. Arms not yet linked as
    prediction sets leave it ``not recorded``, and the check runs again when
    they are linked.
    """
    if exploration.kind != "one_axis" or len(_arm_sets(session, exploration)) < 2:
        return [], {"status": NOT_RECORDED}
    layer = method_layer(
        exploration.kind, _declared_axis(exploration), _arm_rows(session, exploration)
    )
    blockers = (
        ["the arms differ in more than the declared axis"] if layer["status"] == "crossed" else []
    )
    return blockers, layer


def compute(session: Session, exploration: Exploration) -> dict[str, Any]:
    """The receipt as the rows stand now, with what blocks a launch or a verdict."""
    sets = _arm_rows(session, exploration)
    results = _results(session, exploration)
    layers = {
        "method": method_layer(exploration.kind, _declared_axis(exploration), sets),
        "recipe": _recipe_layer(session, sets),
        "population": {"status": NOT_RECORDED},
        "frame": _frame_layer(results),
        "coverage": {"status": NOT_RECORDED},
        "execution": {"status": NOT_RECORDED},
        "scale": _scale_layer(session, results),
    }
    blocks_launch = []
    if exploration.kind == "one_axis" and layers["method"]["status"] == "crossed":
        blocks_launch.append("the arms differ in more than the declared axis")
    blocks_verdict = list(blocks_launch)
    if layers["frame"]["status"] in ("crossed", "unsealed"):
        blocks_verdict.append(f"the results are {layers['frame']['status']}")
    if any(
        v["censored"] not in (NOT_RECORDED, []) for v in layers["scale"].get("results", {}).values()
    ):
        blocks_verdict.append("a maximum sits at an edge of the thresholds")
    return {"layers": layers, "blocks_launch": blocks_launch, "blocks_verdict": blocks_verdict}
