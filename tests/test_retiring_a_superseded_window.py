"""Retiring a window of the old frame, and everything it refuses to retire.

WHY AN OPERATION AT ALL. A raw ``DELETE`` is how 124 campaign rows disappear
with nothing recording that they did. The project's rule is that every
operation goes through ``POST /v1/jobs`` because a direct ``execute()`` leaves
no job, and a DELETE is the case where that matters most: afterwards there is
no row left to ask.

THE TEST THAT MATTERS MOST is ``test_one_refusal_stops_the_whole_run``. All the
windows are checked BEFORE any is deleted, because a partial retirement of a
named list leaves nobody able to say which half went.

AND THE HOLDOUT. ``cb46395a`` carries ``window_role='test'`` and has ZERO
results in PROTEA, because its single declared look was spent in the
laboratory on 2026-10-26. A "no results" filter would have swept it up, and I
wrote such a filter once. The role is the protection; the result count is not.
"""

from __future__ import annotations

import uuid

import pytest

from protea.core.operations.retire_evaluation_sets import (
    RetireEvaluationSetsOperation,
    RetireEvaluationSetsPayload,
)

VIEJO = ("as_closed", "pivot_terms", "any")
VIVO = ("projected", "live_in_both_ends", "experimental")


class _Eset:
    def __init__(self, rules=VIEJO, role: str | None = None) -> None:
        self.id = uuid.uuid4()
        self.truth_profile, self.toi_rule, self.not_codes = rules
        self.window_role = role


class _Session:
    """Answers ``get`` and the one count query the refusal check makes."""

    def __init__(self, rows, referencias: dict[str, int] | None = None) -> None:
        self.rows = {e.id: e for e in rows}
        self.referencias = referencias or {}
        self.borradas: list[uuid.UUID] = []

    def get(self, _model, ident):
        return self.rows.get(ident)

    def execute(self, statement, params=None):
        sql = str(statement)
        if sql.startswith("DELETE"):
            self.borradas.append(params["v"])
            return _Uno(0)
        tabla = next(t for t in self.referencias if t in sql) if any(
            t in sql for t in self.referencias
        ) else None
        return _Uno(self.referencias.get(tabla, 0))


class _Uno:
    def __init__(self, v: int) -> None:
        self._v = v

    def scalar_one(self) -> int:
        return self._v


class _Store:
    def __init__(self, falla: bool = False) -> None:
        self.borrados: list[str] = []
        self.falla = falla

    def delete(self, key: str) -> bool:
        if self.falla:
            raise OSError("el almacen no responde")
        self.borrados.append(key)
        return True


def _run(session, store, ids, monkeypatch):
    monkeypatch.setattr(
        "protea.core.operations.retire_evaluation_sets.get_artifact_store", lambda _s: store
    )
    monkeypatch.setattr(
        "protea.core.operations.retire_evaluation_sets.load_settings", lambda _r: object()
    )
    eventos: list[tuple[str, str]] = []
    out = RetireEvaluationSetsOperation().execute(
        session,
        {"evaluation_set_ids": ids},
        emit=lambda e, _p, f, lvl: eventos.append((e, lvl)) or f,
    )
    return out.result, eventos


class TestWhatItRefuses:
    def test_an_empty_list_is_refused(self) -> None:
        with pytest.raises(ValueError, match="cannot be empty"):
            RetireEvaluationSetsPayload.model_validate({"evaluation_set_ids": []})

    def test_a_window_with_a_role_is_refused_even_with_no_results(self, monkeypatch) -> None:
        # THE HOLDOUT CASE. Zero results in PROTEA because the look was spent
        # in the laboratory. A "no results" filter would have taken it.
        e = _Eset(role="test")
        with pytest.raises(ValueError, match="window_role"):
            _run(_Session([e]), _Store(), [str(e.id)], monkeypatch)

    def test_a_live_frame_window_is_refused(self, monkeypatch) -> None:
        e = _Eset(rules=VIVO)
        with pytest.raises(ValueError, match="not the superseded frame"):
            _run(_Session([e]), _Store(), [str(e.id)], monkeypatch)

    def test_a_referenced_window_is_refused_and_names_the_table(self, monkeypatch) -> None:
        e = _Eset()
        session = _Session([e], referencias={"evaluation_result": 3})
        with pytest.raises(ValueError, match="evaluation_result"):
            _run(session, _Store(), [str(e.id)], monkeypatch)
        assert session.borradas == []

    def test_an_id_that_is_not_a_window_raises(self, monkeypatch) -> None:
        with pytest.raises(ValueError, match="does not exist"):
            _run(_Session([]), _Store(), [str(uuid.uuid4())], monkeypatch)


class TestWhatItDoes:
    def test_a_clean_superseded_window_goes_with_its_artefact(self, monkeypatch) -> None:
        e = _Eset()
        session, store = _Session([e]), _Store()
        res, eventos = _run(session, store, [str(e.id)], monkeypatch)
        assert session.borradas == [e.id]
        assert store.borrados == [f"eval_groundtruth/{e.id}/groundtruth.parquet"]
        assert res == {"retired": 1, "orphaned_artifacts": 0, "asked": 1}
        assert ("retire_evaluation_sets.done", "warning") in eventos

    def test_one_refusal_stops_the_whole_run(self, monkeypatch) -> None:
        # THE PROPERTY THAT MATTERS. A partial retirement of a named list
        # leaves nobody able to say which half went, so every window is
        # checked before any is deleted.
        buena, mala = _Eset(), _Eset(role="valid")
        session = _Session([buena, mala])
        with pytest.raises(ValueError, match="window_role"):
            _run(session, _Store(), [str(buena.id), str(mala.id)], monkeypatch)
        assert session.borradas == [], "borro antes de terminar de comprobar"

    def test_a_store_failure_leaves_an_orphan_and_not_a_dangling_row(
        self, monkeypatch
    ) -> None:
        # The safe failure: a file nobody can attribute, rather than a row
        # pointing at nothing.
        e = _Eset()
        session = _Session([e])
        res, _ = _run(session, _Store(falla=True), [str(e.id)], monkeypatch)
        assert session.borradas == [e.id]
        assert res["orphaned_artifacts"] == 1
