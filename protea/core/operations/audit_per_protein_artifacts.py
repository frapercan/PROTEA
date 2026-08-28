# protea/core/operations/audit_per_protein_artifacts.py
"""What a re-run of the per-protein artefact would cost, and what it would refuse.

Two numbers, and they answer two different questions. Only the second is a
calibration.

**The migration surface.** ``compare_paired_panels`` reads
``<SETTING>/per_protein_grid.parquet`` and refuses ``<SETTING>/per_protein.parquet``.
The census below counts, per (result, setting) pair, which of the two is there.
Its answer today is knowable before it runs: nothing has ever written the grid
file, this change introduces the name, so every pair holding a legacy file is a
pair that needs re-running. That makes it a recompute-cost estimate, which is
worth having and is not a guard hit-rate. Reading it as one would apply this
project's "a large hit count means the rule is wrong" test to a quantity that
is large by construction, and conclude the schema is wrong when the census
simply asked a question whose answer was already known.

**The refusal rate, which is the calibration.** The producer refuses a
namespace it cannot write faithfully, and the number that belongs in the pull
request body is how often that happens on the real corpus. Most of it is
readable TODAY, from artefacts already on the store, without re-running an
evaluation: ``_run_cafa_per_protein.rows_from_sink`` never filtered on ground
truth, so every stored ``per_protein.parquet`` carries one row per kernel array
row with its ``n_gt_w``. The rows with ``n_gt_w <= 0`` are exactly the ones
cafaeval kept in ``P`` and left out of the population it normalised by, and
their ``pred_w`` is the mass at stake. Pass ``probe_legacy_rows`` and this
counts them.

What that count does NOT cover, stated because a calibration that hides its
exclusions is worse than none:

- It is the WEIGHTED variant at ONE threshold, the tau the legacy file is
  sliced at. The producer refuses on the whole curve and on both variants, so
  this is a lower bound.
- It cannot see the variant-population refusal (a namespace whose weighted and
  unweighted branches scored different proteins), because the legacy file
  carries no unweighted column at all.
- It cannot see the row-numbering refusal on the NK/LK path, because a
  mis-numbered file looks perfectly well formed.

The exact upper bound needs a re-run, and a re-run reports it: every drop is a
``run_cafa_evaluation.per_protein_grid_namespace_dropped`` event at error level
carrying a ``code`` from ``_run_cafa_per_protein``, so counting refusals over a
re-run is a query against ``job_event`` and not a grep over prose.

**Access.** The census calls ``exists``, and ``get`` on the legacy files when
``probe_legacy_rows`` is set. It never calls ``put`` or ``delete`` and writes no
row. It is not, however, read-only "by construction": obtaining the store
constructs it, and construction creates the MinIO bucket or the local root when
one is absent. That is the only mutation on this path and it is named here
rather than denied, because a narrated guarantee whose test cannot see the one
write it misses is the exact shape this project has a rule about.

**Why probing rather than listing.** ``ArtifactStore`` has no ``list``: the
protocol is ``put`` / ``get`` / ``url`` / ``exists`` / ``delete`` and both
backends implement exactly those. So a prefix cannot be enumerated. The
settings are not listed either, for the same reason and because NK, LK and PK
are the only settings an evaluation writes. Three to six cheap existence probes
per result is the access pattern ``stratify_evaluation`` and
``compare_paired_panels`` already use.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Annotated, Any

from pydantic import Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, Operation, OperationResult, ProteaPayload
from protea.core.operations._run_cafa_helpers import eval_artifact_key
from protea.core.operations._run_cafa_per_protein import GRID_FILENAME

#: What exists today and cannot support a re-selected operating point.
LEGACY_FILENAME = "per_protein.parquet"

#: The only settings an evaluation writes.
SETTINGS: tuple[str, ...] = ("NK", "LK", "PK")

_RESULTS = (
    "SELECT id::text AS id FROM evaluation_result ORDER BY created_at DESC LIMIT :limit"
)


class AuditPerProteinArtifactsPayload(ProteaPayload, frozen=True):
    """What to probe, and how much of it.

    The cap is not a convenience. Every result costs up to six existence probes
    against the object store, and the per-result detail is written into a
    ``JobEvent`` as JSONB, so an uncapped census is unbounded input to a row. A
    truncated census that does not say it was truncated reads exactly like a
    complete one, so the count that did not fit is reported beside the ones that
    did.
    """

    max_results: Annotated[int, Field(default=1000, gt=0)]
    settings: Annotated[
        list[str],
        Field(
            default_factory=lambda: list(SETTINGS),
            # Constrained rather than free text: the value is interpolated into
            # an object-store key, and an unconstrained string there probes a
            # path no evaluation ever writes and reports it as "absent", which
            # is a wrong answer that looks like a right one.
            description="CAFA settings to probe; the three an evaluation writes",
        ),
    ]
    max_detail: Annotated[
        int,
        Field(default=200, ge=0, description="per-result rows carried in the result block"),
    ]
    probe_legacy_rows: Annotated[
        bool,
        Field(
            default=False,
            description=(
                "read each legacy per_protein.parquet and count the rows with no ground "
                "truth; this is the calibration number, and it costs one get per file"
            ),
        ),
    ] = False

    def validated_settings(self) -> list[str]:
        unknown = [s for s in self.settings if s not in SETTINGS]
        if unknown:
            raise ValueError(
                f"unknown settings {unknown}; an evaluation writes only {list(SETTINGS)} and a "
                "name outside that list probes a key nothing ever wrote, which this census "
                "would report as 'absent' rather than as a mistake"
            )
        return list(dict.fromkeys(self.settings))


def _result_ids(session: Session, limit: int) -> tuple[list[str], bool]:
    """The newest ``limit`` result ids, and whether there were more.

    One row over the cap is fetched and thrown away. Comparing the returned
    count against the cap cannot answer this on its own: the query carries the
    cap in its own ``LIMIT``, so a full page means "at least this many" and an
    equality test over-reports a store holding exactly ``limit`` results while a
    strict test can never fire at all.
    """
    rows = [row[0] for row in session.execute(text(_RESULTS), {"limit": limit + 1})]
    return rows[:limit], len(rows) > limit


def _probe(store: Any, result_id: str, settings: list[str]) -> dict[str, str]:
    """One result's settings, each labelled by what it actually holds.

    Four states, and they are not three plus a remainder. ``grid`` means the
    consumer can read it. ``both`` means the same, and additionally that
    ``stratify_evaluation`` still works, which is the state a correct producer
    leaves behind. ``legacy`` is the population this whole change exists for:
    the consumer raises on it by name. ``absent`` is a setting the evaluation
    never scored, which is normal and must not be counted as a rejection.
    """
    out: dict[str, str] = {}
    rid = uuid.UUID(result_id)
    for setting in settings:
        grid = store.exists(eval_artifact_key(rid, f"{setting}/{GRID_FILENAME}"))
        legacy = store.exists(eval_artifact_key(rid, f"{setting}/{LEGACY_FILENAME}"))
        if grid and legacy:
            out[setting] = "both"
        elif grid:
            out[setting] = "grid"
        elif legacy:
            out[setting] = "legacy"
        else:
            out[setting] = "absent"
    return out


def _tally(per_result: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"both": 0, "grid": 0, "legacy": 0, "absent": 0}
    for row in per_result:
        for state in row["settings"].values():
            counts[state] += 1
    return counts


def _verdict(per_result: list[dict[str, Any]]) -> tuple[list[str], list[str]]:
    """Which results the grid schema would reject, and which it can read throughout.

    Counted per RESULT because a comparison is a whole result and the recompute
    cost is per result, but a result counts as readable only when EVERY setting
    it scored holds a grid file. A half-migrated result whose NK is readable and
    whose LK is not still raises on LK, so calling it readable would under-report
    rejections the moment a partial migration exists. "Rejected" is one or more
    legacy settings with no grid file beside them; a setting holding neither is a
    setting the evaluation never scored and is not a rejection.
    """
    rejected: list[str] = []
    readable: list[str] = []
    for row in per_result:
        states = set(row["settings"].values())
        scored = states - {"absent"}
        if scored and scored <= {"grid", "both"}:
            readable.append(row["evaluation_result_id"])
        elif "legacy" in states:
            rejected.append(row["evaluation_result_id"])
    return rejected, readable


def _legacy_rows(store: Any, result_id: str, setting: str) -> dict[str, float] | None:
    """One legacy file's population gap, or None when it cannot be read.

    ``rows_from_sink`` never filtered on ground truth, so this file holds one row
    per kernel array row. The rows with ``n_gt_w <= 0`` are the ones cafaeval
    counted in ``P`` and left out of the population it normalised by, which is
    the state the producer refuses a namespace for.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    key = eval_artifact_key(uuid.UUID(result_id), f"{setting}/{LEGACY_FILENAME}")
    try:
        table = pq.read_table(pa.BufferReader(store.get(key)), columns=["n_gt_w", "pred_w"])
    except Exception:
        return None
    n_gt = table["n_gt_w"].to_numpy(zero_copy_only=False)
    pred = table["pred_w"].to_numpy(zero_copy_only=False)
    empty = n_gt <= 0.0
    total = float(pred.sum())
    return {
        "rows": float(len(n_gt)),
        "rows_without_ground_truth": float(empty.sum()),
        "predicted_mass": total,
        "predicted_mass_without_ground_truth": float(pred[empty].sum()),
    }


def _row_census(
    store: Any, per_result: list[dict[str, Any]], settings: list[str]
) -> dict[str, Any]:
    """Aggregate the population gap over every legacy file the census found."""
    totals = {
        setting: {
            "files_read": 0,
            "files_unreadable": 0,
            "files_with_a_gap": 0,
            "rows": 0.0,
            "rows_without_ground_truth": 0.0,
            "predicted_mass": 0.0,
            "predicted_mass_without_ground_truth": 0.0,
        }
        for setting in settings
    }
    for row in per_result:
        for setting, state in row["settings"].items():
            if state not in ("legacy", "both"):
                continue
            counts = _legacy_rows(store, row["evaluation_result_id"], setting)
            bucket = totals[setting]
            if counts is None:
                bucket["files_unreadable"] += 1
                continue
            bucket["files_read"] += 1
            if counts["rows_without_ground_truth"] > 0:
                bucket["files_with_a_gap"] += 1
            for key, value in counts.items():
                bucket[key] += value
    return totals


def _resolve_store(emit: EmitFn) -> tuple[Any, Path]:
    """Resolve the artifact store and say where the configuration came from.

    The root is derived from this module's own path, so a tree that carries no
    configuration points the store at nothing and every probe answers absent.
    Emitting the root is what lets a reader tell a real zero from a misdirected
    one; refusing the all-absent case is what stops the misdirected one being
    acted on.
    """
    from protea.infrastructure.settings import load_settings
    from protea.infrastructure.storage.factory import get_artifact_store

    config_root = Path(__file__).resolve().parents[3]
    store = get_artifact_store(load_settings(config_root))
    emit(
        "audit_per_protein_artifacts.config",
        f"artifact store resolved from {config_root}",
        {"config_root": str(config_root)},
        "info",
    )
    return store, config_root


def _refuse_a_store_pointing_nowhere(
    counts: dict[str, int], n_results: int, config_root: Path
) -> None:
    """A zero and an inability to read must not look alike.

    This audit's answer is acted on: "nothing to migrate" licenses a merge. But
    every probe is an ``exists()`` against a store resolved from this module's
    own path, so a store pointing nowhere returns absent for all of them and
    produces a perfectly well-formed zero that is the opposite of the truth.

    That happened on the first real run: 360 absent and 0 legacy, when the true
    answer was 360 legacy. The cause was a tree carrying no configuration, and
    nothing in the output said so.

    A system that has run evaluations does not hold zero artefacts across every
    result and every setting. If the emptiness is real, the operator says so by
    probing a single result rather than the whole surface.
    """
    total = sum(counts.values())
    if n_results > 1 and total and counts.get("absent") == total:
        raise RuntimeError(
            f"every one of {total} probes across {n_results} results came back absent, with "
            f"the artifact store resolved from {config_root}. That is indistinguishable from "
            "a store pointing nowhere, and reporting it as 'nothing to migrate' is the one "
            "wrong answer this audit can give, because a merge would be licensed by it. Check "
            "the configuration at that path, or probe a single result if the emptiness is real."
        )


class AuditPerProteinArtifactsOperation(Operation):
    """Read-only census of the per-protein artefacts on the store."""

    name = "audit_per_protein_artifacts"
    description = (
        "Census of the per-protein evaluation artefacts: which settings hold "
        "the legacy single-threshold file, which hold the whole-grid file, and "
        "with probe_legacy_rows, how many stored rows carry no ground truth, "
        "which is the number of namespaces the grid producer would refuse. "
        "Writes no row and no artefact. The numbers belong in the pull request "
        "body before the grid schema lands."
    )
    payload_model = AuditPerProteinArtifactsPayload

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        rows = " and read their rows" if payload.get("probe_legacy_rows") else ""
        return f"probe {payload.get('max_results', 1000)} evaluation results{rows}, writes nothing"

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = AuditPerProteinArtifactsPayload.model_validate(payload)
        settings = p.validated_settings()
        # Resolve the configuration root explicitly and SAY WHERE IT CAME FROM.
        # The root is derived from this module's own path, so running from a tree
        # that carries no configuration points the store at nothing. Emitting it
        # is what lets a reader tell a real zero from a misdirected one.
        store, config_root = _resolve_store(emit)
        ids, truncated = _result_ids(session, p.max_results)
        emit(
            "audit_per_protein_artifacts.start",
            f"probing {len(ids)} evaluation results across {len(settings)} settings",
            {"results": len(ids), "settings": settings, "rows": p.probe_legacy_rows},
            "info",
        )
        per_result = [
            {"evaluation_result_id": rid, "settings": _probe(store, rid, settings)}
            for rid in ids
        ]
        counts = _tally(per_result)
        _refuse_a_store_pointing_nowhere(counts, len(ids), config_root)
        rejected, readable = _verdict(per_result)
        rows = _row_census(store, per_result, settings) if p.probe_legacy_rows else None
        emit(
            "audit_per_protein_artifacts.verdict",
            f"{len(rejected)} of {len(ids)} results hold a legacy file the grid schema "
            f"refuses; {len(readable)} are readable throughout",
            {
                "results": len(ids),
                "rejected_results": len(rejected),
                "fully_readable_results": len(readable),
                "settings": counts,
                "legacy_rows": rows,
            },
            "info",
        )
        return OperationResult(
            result={
                "results_probed": len(ids),
                "max_results": p.max_results,
                "truncated": truncated,
                "settings": counts,
                "rejected_results": len(rejected),
                "fully_readable_results": len(readable),
                "legacy_rows": rows,
                "detail": per_result[: p.max_detail],
                "detail_omitted": max(0, len(per_result) - p.max_detail),
            }
        )
