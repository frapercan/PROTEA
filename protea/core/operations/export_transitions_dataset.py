# protea/core/operations/export_transitions_dataset.py
"""Write the D56 transitions dataset, one declared table at a time.

WHAT IT IS AND IS NOT. A new operation on ``protea.training``. It does NOT
touch ``export_research_dataset``, which writes the D45 pair table and keeps
writing it: two datasets, and mixing them would make one ``dataset`` row unable
to say which it is.

WINDOWS ARE NAMED BY ID, NEVER BY A PAIR OF VERSIONS. The pair stopped
identifying a window: 62 of 69 pairs carry more than one. So the payload lists
ids, and each one is checked against the declared axis rather than resolved from
a version.

AND THE AXIS IS TWO THINGS, WHICH IS A CORRECTION. ``cut`` is the
``ontology_snapshot_id`` that indexes the term tables; ``cut_version`` is the
GOA release the laboratory's leakage rule compares against. They were one field
in the first design and the desktop session caught it. Neither is derived from
the other here, because deriving one would be exactly the step that makes a
number stop being attributable.

THIS RUN WRITES NO ``dataset`` ROW, ON PURPOSE. The design's own rule is that
there is no incremental export: a run writes the whole bundle or fails, because
a half bundle with a sha is a bundle that lies. So until the other nine tables
exist, a run of this operation is a declared TABLE with its digest and its
counters -- useful, quotable, and not a dataset. ``tables`` has to be named,
and asking for one that is not written yet is refused rather than silently
skipped.
"""

from __future__ import annotations

import hashlib
import tempfile
import uuid
from pathlib import Path
from typing import Any

from pydantic import field_validator
from sqlalchemy.orm import Session

from protea.core.code_revision import code_revision, is_identifying, revisions_conflict
from protea.core.contracts.operation import (
    EmitFn,
    Operation,
    OperationResult,
    ProteaPayload,
)
from protea.core.training_dump._window_choice import _refuse_wrong_pivot, window_identity
from protea.core.transitions_export._window_terms_out import (
    WindowTerms,
    axis_live_terms,
    terms_out,
    write_table,
)
from protea.core.utils import contract_payload, job_id_from_payload
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store

#: The ten tables of ADR-D56, in the order the design writes them.
TABLES: tuple[str, ...] = (
    "window_terms_out",
    "queries",
    "query_state",
    "neighbourhood",
    "neighbour_state",
    "gains",
    "terms",
    "term_ancestors",
    "feature_tokens",
    "residue_refs",
)

#: What this revision can actually write. Named separately from ``TABLES`` so
#: the refusal can say "declared but not written yet" instead of "unknown",
#: which are different answers for whoever reads the error.
IMPLEMENTED: frozenset[str] = frozenset({"window_terms_out"})


class ExportTransitionsDatasetPayload(ProteaPayload, frozen=True):
    """Which windows, on which axis, and which tables."""

    windows: list[str]
    cut: str
    cut_version: str
    tables: list[str] = ["window_terms_out"]
    output_name: str = "transitions"
    accept_revisions: list[str] = []

    @field_validator("windows")
    @classmethod
    def _named_and_distinct(cls, value: list[str]) -> list[str]:
        """Refuse an empty list and refuse repeats.

        There is no "all the windows" mode. A scope living in a default is a
        scope nobody declared, and this one decides which transitions a model
        is trained on.
        """
        if not value:
            raise ValueError(
                "windows cannot be empty and has no 'all' mode: the scope of a "
                "training set is not something to leave to a default"
            )
        repetidas = sorted({w for w in value if value.count(w) > 1})
        if repetidas:
            raise ValueError(
                f"windows repeats {repetidas}. A window exported twice would write "
                "its terms twice and the row count would stop being a count"
            )
        return value

    @field_validator("tables")
    @classmethod
    def _declared_and_written(cls, value: list[str]) -> list[str]:
        """Refuse an empty list, an unknown name, and a known-but-unwritten one."""
        if not value:
            raise ValueError(f"tables cannot be empty; name some of {list(TABLES)}")
        desconocidas = sorted(set(value) - set(TABLES))
        if desconocidas:
            raise ValueError(f"unknown tables {desconocidas}; D56 declares {list(TABLES)}")
        pendientes = sorted(set(value) - IMPLEMENTED)
        if pendientes:
            raise ValueError(
                f"tables {pendientes} are declared by ADR-D56 and this revision does "
                f"not write them yet. It writes {sorted(IMPLEMENTED)}. Refused rather "
                "than skipped: a bundle missing a table it was asked for is one "
                "nobody can tell from a complete one"
            )
        return value


def _window_or_refuse(session: Session, raw: str) -> EvaluationSet:
    """The named window, or a refusal that says the id is the thing missing."""
    try:
        wid = uuid.UUID(raw)
    except ValueError as exc:
        raise ValueError(f"{raw!r} is not an evaluation set id. Windows are named by id") from exc
    eset = session.get(EvaluationSet, wid)
    if eset is None:
        raise ValueError(
            f"EvaluationSet {raw} does not exist. It is named in the payload, so this "
            "is a declaration about a window nobody can read"
        )
    return eset


def _refuse_unstamped(eset: EvaluationSet, accepted: list[str], running: str) -> None:
    """A window whose code is unknown, or known and different, unless named.

    The rules a window declares say what it IS by declaration; the revision
    says what it is by IMPLEMENTATION, and those are different quantities: a
    fix to the synonym map moved the delta of a whole class of windows with
    every identity field untouched. So an unstamped window is refused too,
    because ``revisions_conflict`` answers False when either side is
    unidentifying and that is not agreement.

    Admitting one is done BY NAMING ITS REVISION, never with a boolean.
    Admitting them in bulk would be the same as not checking.
    """
    recorded = eset.code_revision
    if recorded in accepted or (is_identifying(recorded) and recorded[:12] in accepted):
        return
    if not is_identifying(recorded):
        raise ValueError(
            f"window {eset.id} records code_revision={recorded!r}, which cannot say "
            "what built it. Name that revision in accept_revisions to export it "
            "anyway, or regenerate the window"
        )
    if revisions_conflict(recorded, running):
        raise ValueError(
            f"window {eset.id} was built by {recorded[:12]} and this process is "
            f"{running[:12]}. Name {recorded[:12]} in accept_revisions if the "
            "difference is known to be irrelevant to its truth"
        )


class ExportTransitionsDatasetOperation(Operation):
    name = "export_transitions_dataset"
    description = (
        "Write the D56 transitions dataset's declared tables for named windows on a "
        "declared term axis. Windows are named by id, never by a version pair, and "
        "each is refused if it pivots on another axis or was built by another "
        "revision than the process unless the payload names it. Writes no dataset "
        "row while any of the ten tables is still unwritten: a half bundle with a "
        "sha is a bundle that lies."
    )

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = ExportTransitionsDatasetPayload.model_validate(contract_payload(payload))
        running = code_revision()
        axis = axis_live_terms(session, uuid.UUID(p.cut))
        emit(
            "transitions.axis",
            f"{len(axis)} live terms on the declared axis",
            {"cut": p.cut, "cut_version": p.cut_version, "axis_live_terms": len(axis)},
            "info",
        )
        resultados = self._per_window(session, p, axis, running, emit)
        return self._publish(p, resultados, running, payload, emit)

    def _per_window(
        self,
        session: Session,
        p: ExportTransitionsDatasetPayload,
        axis: set[str],
        running: str,
        emit: EmitFn,
    ) -> list[WindowTerms]:
        """Check and compute every window before anything is written."""
        resultados: list[WindowTerms] = []
        for raw in p.windows:
            eset = _window_or_refuse(session, raw)
            _refuse_wrong_pivot(eset, p.cut, f"window {raw}")
            _refuse_unstamped(eset, p.accept_revisions, running)
            r = terms_out(session, eset, axis)
            resultados.append(r)
            emit(
                "transitions.window_terms_out",
                f"{raw[:8]}: {len(r.out)} of {r.axis_size} axis terms out",
                {
                    **window_identity(eset),
                    "terms_out": len(r.out),
                    "axis_live_terms": r.axis_size,
                    "toi_terms": r.toi_size,
                    # The TOI is NOT a subset of the axis, so this is emitted
                    # rather than left to be inferred from the other two.
                    "toi_terms_off_axis": r.toi_off_axis,
                },
                "info",
            )
        return resultados

    def _publish(
        self,
        p: ExportTransitionsDatasetPayload,
        resultados: list[WindowTerms],
        running: str,
        payload: dict[str, Any],
        emit: EmitFn,
    ) -> OperationResult:
        """Write the parquet, publish it, and report its digest."""
        store = get_artifact_store(load_settings(Path(__file__).resolve().parents[3]))
        with tempfile.TemporaryDirectory(prefix="transitions-") as tmp:
            local = Path(tmp) / "window_terms_out.parquet"
            filas = write_table(resultados, str(local))
            raw = local.read_bytes()
        digest = hashlib.sha256(raw).hexdigest()
        key = f"transitions/{p.output_name}/{p.cut[:8]}/window_terms_out-{digest[:12]}.parquet"
        uri = store.put(key, raw)
        result = {
            "table": "window_terms_out",
            "uri": uri,
            "key": key,
            "sha256": digest,
            "rows": filas,
            "windows": len(resultados),
            "cut": p.cut,
            "cut_version": p.cut_version,
            "code_revision": running,
            "job_id": str(job_id_from_payload(payload) or ""),
            "dataset_row": None,
            "why_no_dataset_row": (
                "nine of the ten D56 tables are not written yet, and a dataset row "
                "over a partial bundle is a row that claims to be a dataset"
            ),
        }
        emit("transitions.published", f"{filas} rows, sha256 {digest[:12]}", result, "info")
        return OperationResult(result=result)

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        ventanas = payload.get("windows") or []
        tablas = payload.get("tables") or ["window_terms_out"]
        return f"write {tablas} for {len(ventanas)} windows on axis {payload.get('cut')}"
