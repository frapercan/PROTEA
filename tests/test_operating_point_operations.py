"""Declaring a threshold on an arm's fitting windows, and reading a result at it.

``declare_operating_threshold`` pools one arm's raw fitting-window grids into a
threshold per cell (the grid argmax of the pooled ``f_micro_w``) and registers
it as ``operating_threshold`` weights; ``read_operating_point`` reads a result
at those thresholds, per cell and per stratum, into one
``evaluation_operating_point`` row. Without a database: the payloads, the
routing, the sets gate, the migration against the model, and the arithmetic of a
cell and its strata. With ``--with-postgres``: three fitting windows and a
selection window, one arm's sets on each, its grids in a store kept in memory;
the declaration against a hand pooling, every refusal before anything is
written, and the reading against a hand reading, read once.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import uuid
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import numpy as np
import pytest
import sqlalchemy as sa
from pydantic import ValidationError
from sqlalchemy import CheckConstraint, UniqueConstraint, create_engine, select
from sqlalchemy.orm import Session

import protea.infrastructure.orm.models  # noqa: F401  (registers every table)
from protea.core import queues
from protea.core.contracts.method import Family, InstanceCut, MethodSpec, Pin
from protea.core.operating_point import load_declared_threshold, parse_declared_threshold
from protea.core.operating_point.grid import pooled_threshold, tau_grid_of
from protea.core.operating_point.window_fields import WindowArmsDiffer
from protea.core.operation_catalog import CATALOG, build_operation_registry
from protea.core.operations import _operating_point_reads as reads
from protea.core.operations import declare_operating_threshold as declare_module
from protea.core.operations import read_operating_point as read_module
from protea.core.operations._paired_panels_bootstrap import PanelArrays
from protea.core.operations.declare_operating_threshold import (
    DeclareOperatingThresholdOperation,
    DeclareOperatingThresholdPayload,
)
from protea.core.operations.read_operating_point import (
    ReadOperatingPointOperation,
    ReadOperatingPointPayload,
    cell_reading,
)
from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.evaluation_operating_point import (
    EvaluationOperatingPoint,
)
from protea.infrastructure.orm.models.annotation.evaluation_result import EvaluationResult
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.embedding.model_weights import WEIGHT_KINDS, ModelWeights
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.method.method import Method
from tests.helpers.paired_panels import write_grid_parquet
from tests.test_method_tables import alembic_config, fresh_database  # noqa: F401

_REPO = Path(__file__).resolve().parents[1]
_MIGRATION = _REPO / "alembic/versions/41ad8122de7d_a_result_is_read_at_a_declared_threshold.py"
_spec = importlib.util.spec_from_file_location("migration_operating_point", _MIGRATION)
assert _spec and _spec.loader
migration = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(migration)

TABLE = "evaluation_operating_point"
TH_STEP = 0.25
TAUS = tau_grid_of(TH_STEP)
BP, MF = "biological_process", "molecular_function"


# Without a database ------------------------------------------------------------------


class TestThePayloads:
    def test_a_threshold_names_its_weights_and_distinct_results(self) -> None:
        ok = DeclareOperatingThresholdPayload.model_validate(
            {
                "name": "ladder-toy-transport-threshold-ours-raw",
                "evaluation_result_ids": [str(uuid.uuid4())],
            }
        )
        assert (ok.variant, ok.ties, ok.reading) == ("f_micro_w", "lowest_tau", "raw")
        same = str(uuid.uuid4())
        with pytest.raises(ValidationError, match="more than once"):
            DeclareOperatingThresholdPayload.model_validate(
                {"name": "t", "evaluation_result_ids": [same, same]}
            )
        with pytest.raises(ValidationError, match="reading"):
            DeclareOperatingThresholdPayload.model_validate(
                {"name": "t", "evaluation_result_ids": [same], "reading": "percentile_rank"}
            )
        with pytest.raises(ValidationError):
            DeclareOperatingThresholdPayload.model_validate(
                {"name": "a name with spaces", "evaluation_result_ids": [same]}
            )

    def test_a_reading_is_per_cell_and_its_axes_are_closed(self) -> None:
        ids = {
            "evaluation_result_id": str(uuid.uuid4()),
            "declared_threshold_id": str(uuid.uuid4()),
        }
        ok = ReadOperatingPointPayload.model_validate(ids)
        assert ok.axes == ["category", "aspect", "homology", "length"] and ok.min_population == 30
        with pytest.raises(ValidationError, match="include category and aspect"):
            ReadOperatingPointPayload.model_validate({**ids, "axes": ["homology"]})
        with pytest.raises(ValidationError, match="unknown axes"):
            ReadOperatingPointPayload.model_validate(
                {**ids, "axes": ["category", "aspect", "taxon"]}
            )


class TestTheRouting:
    @pytest.mark.parametrize("name", ["declare_operating_threshold", "read_operating_point"])
    def test_both_run_on_the_evaluations_queue_as_shared_evaluation_jobs(self, name: str) -> None:
        registry = build_operation_registry()
        assert registry.get(name).name == name
        spec = {build().name: spec for build, spec in CATALOG}[name]
        assert (spec.domain, spec.kind, spec.queue, spec.family) == (
            "evaluation",
            "job",
            queues.EVALUATIONS,
            None,
        )

    def test_the_weights_kind_exists(self) -> None:
        assert "operating_threshold" in WEIGHT_KINDS


class TestTheTableAndItsMigration:
    def test_the_sets_gate_passes_it(self) -> None:
        import scripts.check_sets as gate

        assert TABLE in gate.EXPECTED
        verdict = gate._audit(Base.metadata.tables[TABLE])
        assert {criterion: ok for criterion, (_, ok) in verdict.items()} == {
            "declarado": True,
            "descrito": True,
            "verificable": True,
            "producido_por": True,
        }

    def test_the_migration_builds_what_the_model_describes(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        recorder = _Recorder()
        monkeypatch.setattr(migration, "op", recorder)
        migration.upgrade()
        assert _shape(recorder.tables[TABLE]) == _shape(Base.metadata.tables[TABLE])
        modelled = {
            index.name: (TABLE, tuple(c.name for c in index.columns))
            for index in Base.metadata.tables[TABLE].indexes
        }
        assert recorder.indexes == modelled
        migration.downgrade()
        assert sorted(recorder.dropped) == sorted(
            [("table", TABLE)] + [("index", name) for name in recorder.indexes]
        )

    def test_it_follows_the_head_it_was_written_on(self) -> None:
        assert migration.revision == "41ad8122de7d"
        assert migration.down_revision == "532d2c1db500"


class _Recorder:
    def __init__(self) -> None:
        self.tables: dict[str, sa.Table] = {}
        self.indexes: dict[str, tuple[str, tuple[str, ...]]] = {}
        self.dropped: list[tuple[str, str]] = []

    def create_table(self, name: str, *elements: Any, **_kw: Any) -> None:
        self.tables[name] = sa.Table(name, sa.MetaData(), *elements)

    def create_index(self, name: str, table: str, columns: list[str], **_kw: Any) -> None:
        self.indexes[name] = (table, tuple(columns))

    def drop_index(self, name: str, **_kw: Any) -> None:
        self.dropped.append(("index", name))

    def drop_table(self, name: str, **_kw: Any) -> None:
        self.dropped.append(("table", name))


def _shape(table: sa.Table) -> dict[str, Any]:
    import re

    return {
        "columns": {
            c.name: (type(c.type).__name__, getattr(c.type, "length", None), c.nullable)
            for c in table.columns
        },
        "foreign_keys": sorted(
            (fk.parent.name, fk.target_fullname, fk.ondelete) for fk in table.foreign_keys
        ),
        "unique": sorted(
            (c.name, tuple(col.name for col in c.columns))
            for c in table.constraints
            if isinstance(c, UniqueConstraint)
        ),
        "checks": {
            c.name: re.sub(r"\s+", " ", str(c.sqltext)).strip()
            for c in table.constraints
            if isinstance(c, CheckConstraint)
        },
    }


def _panel(
    tp: list[list[float]], pred: list[list[float]], gt: list[float], tag: str
) -> PanelArrays:
    return PanelArrays(
        tuple(f"{tag}{i}" for i in range(len(gt))),
        np.asarray(tp, dtype=np.float64),
        np.asarray(pred, dtype=np.float64),
        np.asarray(gt, dtype=np.float64),
    )


class TestTheArithmeticOfAReading:
    def test_a_cell_at_its_declared_column_with_smin_and_the_oracle(self) -> None:
        unweighted = _panel([[2, 2, 1], [1, 1, 0]], [[4, 3, 1], [3, 2, 0]], [3, 2], "P")
        weighted = _panel([[4, 4, 2], [1, 1, 0]], [[8, 6, 2], [3, 2, 0]], [6, 2], "P")
        reading = cell_reading(weighted, unweighted, 1, TAUS)
        assert reading["tau"] == TAUS[1] and reading["tau_index"] == 1
        assert reading["f_micro_w"] == pytest.approx(round(2 * 5 / (8 + 8), 6))
        assert reading["f_micro"] == pytest.approx(round(2 * 3 / (5 + 5), 6))
        assert reading["cov_w"] == 1.0 and reading["n_targets_w"] == 2
        assert reading["pr_w"] == pytest.approx(round((4 / 6 + 1 / 2) / 2, 6))
        curve = [2 * t / (p + 8) for t, p in ((5, 11), (5, 8), (2, 2))]
        assert reading["oracle_f_micro_w"] == pytest.approx(round(max(curve), 6))
        assert reading["oracle_tau_f_micro_w"] == TAUS[int(np.argmax(curve))]
        assert reading["oracle_distance_from_half"] == pytest.approx(
            round(abs(reading["oracle_tau_f_micro_w"] - reading["oracle_f_micro_w"] / 2), 6)
        )
        s_w = [np.hypot((8 - t) / 2, (p - t) / 2) for t, p in ((5, 11), (5, 8), (2, 2))]
        assert reading["smin_w"] == pytest.approx(round(min(s_w), 6))
        assert reading["tau_smin_w"] == TAUS[int(np.argmin(s_w))]


class TestTheStrata:
    def _read(self, weighted: PanelArrays, unweighted: PanelArrays) -> SimpleNamespace:
        grid = SimpleNamespace(
            tau_grid=TAUS,
            tau_index=lambda tau: int(np.flatnonzero(np.isclose(TAUS, tau))[0]),
        )
        cells = {"weighted": {"NK:BPO": weighted}, "unweighted": {"NK:BPO": unweighted}}
        return SimpleNamespace(cells=lambda variant: cells[variant], tau_grid=lambda: grid)

    def test_each_stratum_is_read_at_the_cells_threshold_and_small_ones_are_flagged(
        self,
    ) -> None:
        from protea.core.operating_point import DeclaredThreshold
        from protea.core.operations.read_operating_point import strata_rows
        from protea.core.window_placement import TargetPlacement

        unweighted = _panel(
            [[2, 2, 1], [1, 1, 0], [3, 1, 1]], [[4, 3, 1], [3, 2, 0], [3, 2, 1]], [3, 2, 4], "A"
        )
        weighted = _panel(
            [[4, 4, 2], [1, 1, 0], [3, 1, 1]], [[8, 6, 2], [3, 2, 0], [3, 2, 1]], [6, 2, 4], "A"
        )
        declared = DeclaredThreshold(
            "w", "operating_threshold", TH_STEP, {"NK:BPO": TAUS[1]}, {"NK:BPO": False}
        )
        placement = {
            "A0": TargetPlacement(300, None),
            "A1": TargetPlacement(300, 95.0),
            "A2": TargetPlacement(300, 95.0),
        }
        p = SimpleNamespace(axes=["category", "aspect", "homology", "length"], min_population=2)
        rows = list(strata_rows(self._read(weighted, unweighted), declared, placement, p))
        assert [(r["homology"], r["length"], r["n"], r["small"]) for r in rows] == [
            (">90", "<=512", 2, False),
            ("none", "<=512", 1, True),
        ]
        close = cell_reading(
            weighted.take(np.array([1, 2])), unweighted.take(np.array([1, 2])), 1, TAUS
        )
        assert rows[0]["f_micro_w"] == close["f_micro_w"] and rows[0]["tau"] == TAUS[1]

    def test_a_scored_protein_the_placement_does_not_hold_is_refused(self) -> None:
        from protea.core.operating_point import DeclaredThreshold
        from protea.core.operations.read_operating_point import strata_rows

        one = _panel([[1, 1, 1]], [[1, 1, 1]], [1], "A")
        declared = DeclaredThreshold(
            "w", "operating_threshold", TH_STEP, {"NK:BPO": TAUS[0]}, {"NK:BPO": True}
        )
        p = SimpleNamespace(axes=["category", "aspect", "length"], min_population=30)
        with pytest.raises(reads.OperatingPointRefused, match="not a target of the window"):
            list(strata_rows(self._read(one, one), declared, {}, p))


# Against Postgres --------------------------------------------------------------------


class _MemoryStore:
    """The artifact store, kept in memory: what was put, by key."""

    def __init__(self) -> None:
        self.blobs: dict[str, bytes] = {}

    def put(self, key: str, blob: bytes) -> str:
        self.blobs[key] = blob
        return key

    def get(self, key: str) -> bytes:
        return self.blobs[key]

    def exists(self, key: str) -> bool:
        return key in self.blobs


@pytest.fixture()
def pg(postgres_url: str) -> Iterator[Session]:
    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    Base.metadata.drop_all(engine)
    engine.dispose()


@pytest.fixture()
def store(monkeypatch: pytest.MonkeyPatch) -> _MemoryStore:
    kept = _MemoryStore()
    for module in (reads, declare_module, read_module):
        monkeypatch.setattr(module, "operating_point_store", lambda: kept)
    return kept


#: Each window's rows, per cell: unweighted then weighted ``(tp, pred, n_gt)`` per protein.
def _cells(scale: float) -> dict[tuple[str, str], list[tuple]]:
    return {
        ("NK", BP): [
            ([2, 2, 1], [4, 3, 1], 3, [4 * scale, 4 * scale, 2], [8, 6, 2], 6),
            ([1, 1, 0], [3, 2, 0], 2, [1, 1, 0], [3, 2, 0], 2),
        ],
        ("PK", MF): [
            ([1, 0, 0], [2, 1, 0], 1, [2, 0, 0], [4, 2, 0], 2),
            ([3, 2, 2], [3, 3, 2], 4, [3, 2, 2], [3, 3, 2], 5),
        ],
    }


def _grids(
    store: _MemoryStore, result_id: uuid.UUID, window: uuid.UUID, scale: float, tmp: Path
) -> None:
    by_setting: dict[str, list[dict[str, Any]]] = {}
    for (setting, namespace), rows in _cells(scale).items():
        for i, (tp, pred, gt, tp_w, pred_w, gt_w) in enumerate(rows):
            by_setting.setdefault(setting, []).append(
                {
                    "accession": f"{namespace[:2]}{i}",
                    "namespace": namespace,
                    "tp": tp_w,
                    "pred": pred_w,
                    "n_gt": gt_w,
                    "tp_u": tp,
                    "pred_u": pred,
                    "n_gt_u": gt,
                }
            )
    for setting, rows in by_setting.items():
        path = write_grid_parquet(
            tmp / str(result_id) / setting / "per_protein_grid.parquet",
            rows,
            th_step=TH_STEP,
            setting=setting,
            variants=("weighted", "unweighted"),
            metadata={"evaluation_set_id": str(window)},
        )
        store.put(
            f"eval_artifacts/{result_id}/{setting}/per_protein_grid.parquet", path.read_bytes()
        )


def _window(pg: Session, start: int, end: int, role: str) -> EvaluationSet:
    snapshot = OntologySnapshot(obo_url="file:///none", obo_version=f"releases/{start}")
    pg.add(snapshot)
    pg.flush()
    old = AnnotationSet(source="goa", source_version=str(start), ontology_snapshot_id=snapshot.id)
    new = AnnotationSet(source="goa", source_version=str(end), ontology_snapshot_id=snapshot.id)
    pg.add_all([old, new])
    pg.flush()
    window = EvaluationSet(
        old_annotation_set_id=old.id,
        new_annotation_set_id=new.id,
        pivot_snapshot_id=snapshot.id,
        old_native_snapshot_id=snapshot.id,
        new_native_snapshot_id=snapshot.id,
        window_role=role,
        stats={},
    )
    pg.add(window)
    pg.flush()
    return window


def _arm(
    pg: Session, window: EvaluationSet, start: int, recipe: str = "propagated"
) -> PredictionSet:
    spec = MethodSpec(
        family=Family.EXPLAINABLE_UNITS,
        code="term-frequency",
        instance=InstanceCut(start, window.pivot_snapshot_id),
        recipe={"variant": recipe},
        pins=(Pin("photo", hashlib.sha256(str(start).encode()).hexdigest(), ()),),
        runtime="native",
        explains=True,
    )
    method = Method.from_spec(spec)
    pg.add(method)
    pg.flush()
    pset = PredictionSet(
        method_id=method.id,
        annotation_set_id=window.old_annotation_set_id,
        ontology_snapshot_id=window.pivot_snapshot_id,
        meta={
            "code_revision": "a" * 40,
            "dependency_revisions": {"protea-method": "69ea8af"},
            "query_population": {"count": 4, "sha256": str(start) * 8},
            "evaluation_set_id": str(window.id),
            "algorithm": "native_method",
        },
    )
    pg.add(pset)
    pg.flush()
    return pset


def _result(
    pg: Session, window: EvaluationSet, pset: PredictionSet, reading: str = "raw"
) -> EvaluationResult:
    result = EvaluationResult(
        evaluation_set_id=window.id,
        prediction_set_id=pset.id,
        results={"reading": {"score_reading": reading, "th_step": TH_STEP}},
    )
    pg.add(result)
    pg.flush()
    return result


FITTING = ((160, 194, 1.0), (177, 205, 0.5), (194, 211, 0.75))


def _fitting_results(
    pg: Session, store: _MemoryStore, tmp: Path, **changes: Any
) -> list[EvaluationResult]:
    out = []
    for number, (start, end, scale) in enumerate(FITTING):
        window = _window(pg, start, end, changes.get("roles", {}).get(number, "training"))
        pset = _arm(pg, window, start, changes.get("recipes", {}).get(number, "propagated"))
        result = _result(pg, window, pset, changes.get("readings", {}).get(number, "raw"))
        _grids(store, result.id, window.id, scale, tmp)
        out.append(result)
    pg.commit()
    return out


def _declare(pg: Session, results: list[EvaluationResult]) -> dict[str, Any]:
    payload = {
        "name": "ladder-toy-transport-threshold-naive-raw",
        "evaluation_result_ids": [str(r.id) for r in results],
    }
    out = DeclareOperatingThresholdOperation().execute(pg, payload, emit=lambda *a, **k: None)
    pg.commit()
    return out.result


def _hand_pooled(cell: tuple[str, str]) -> Any:
    windows = {}
    for start, _, scale in FITTING:
        rows = _cells(scale)[cell]
        windows[str(start)] = PanelArrays(
            tuple(f"P{i}" for i in range(len(rows))),
            np.asarray([r[3] for r in rows], dtype=np.float64),
            np.asarray([r[4] for r in rows], dtype=np.float64),
            np.asarray([r[5] for r in rows], dtype=np.float64),
        )
    return pooled_threshold(windows, TAUS)


@pytest.mark.integration
def test_a_threshold_is_the_argmax_of_the_pooled_fitting_windows(
    pg: Session, store: _MemoryStore, tmp_path: Path
) -> None:
    results = _fitting_results(pg, store, tmp_path)
    out = _declare(pg, results)
    weights = pg.get(ModelWeights, uuid.UUID(out["weights_id"]))
    assert weights is not None and weights.kind == "operating_threshold"
    document = json.loads(store.get(weights.uri))
    assert hashlib.sha256(store.get(weights.uri)).hexdigest() == weights.sha256
    declared = parse_declared_threshold(document, weights_id=str(weights.id), kind=weights.kind)
    for cell, key in ((("NK", BP), "NK:BPO"), (("PK", MF), "PK:MFO")):
        hand = _hand_pooled(cell)
        assert declared.tau(key) == hand.tau
        assert document["cells"][key]["f_micro_w"] == pytest.approx(hand.value)
        assert document["cells"][key]["units"] == 6
        assert document["cells"][key]["curve"] == pytest.approx(list(hand.curve))
    assert "LK:CCO" in document["absent_cells"]
    last = pg.get(EvaluationSet, results[-1].evaluation_set_id)
    assert weights.trained_on_annotation_set_id == last.new_annotation_set_id
    assert load_declared_threshold(pg, str(weights.id), store).taus == declared.taus
    assert _declare(pg, results)["weights_id"] == out["weights_id"]


@pytest.mark.integration
@pytest.mark.parametrize(
    ("changes", "refusal", "message"),
    [
        ({"readings": {1: "percentile_rank"}}, reads.OperatingPointRefused, "percentile_rank"),
        ({"roles": {2: "validation"}}, reads.OperatingPointRefused, "fitting windows"),
        ({"recipes": {0: "lafa_direct"}}, WindowArmsDiffer, "method_id"),
    ],
)
def test_what_cannot_be_pooled_is_refused_before_anything_is_written(
    pg: Session,
    store: _MemoryStore,
    tmp_path: Path,
    changes: dict[str, Any],
    refusal: type[Exception],
    message: str,
) -> None:
    results = _fitting_results(pg, store, tmp_path, **changes)
    before = set(store.blobs)
    with pytest.raises(refusal, match=message):
        _declare(pg, results)
    pg.rollback()
    assert set(store.blobs) == before
    assert pg.execute(select(sa.func.count(ModelWeights.id))).scalar_one() == 0


@pytest.mark.integration
def test_two_results_on_one_window_are_refused(
    pg: Session, store: _MemoryStore, tmp_path: Path
) -> None:
    results = _fitting_results(pg, store, tmp_path)
    window = pg.get(EvaluationSet, results[0].evaluation_set_id)
    again = _result(pg, window, pg.get(PredictionSet, results[0].prediction_set_id))
    _grids(store, again.id, window.id, 1.0, tmp_path)
    pg.commit()
    with pytest.raises(reads.OperatingPointRefused, match="on one window"):
        _declare(pg, [results[0], again])


@pytest.mark.integration
def test_a_result_is_read_at_the_declared_threshold_of_each_cell_once(
    pg: Session, store: _MemoryStore, tmp_path: Path
) -> None:
    weights_id = _declare(pg, _fitting_results(pg, store, tmp_path))["weights_id"]
    window = _window(pg, 211, 227, "validation")
    result = _result(pg, window, _arm(pg, window, 211))
    _grids(store, result.id, window.id, 0.6, tmp_path)
    pg.commit()
    payload = {
        "evaluation_result_id": str(result.id),
        "declared_threshold_id": weights_id,
        "axes": ["category", "aspect"],
        "_job_id": None,
    }
    out = ReadOperatingPointOperation().execute(pg, payload, emit=lambda *a, **k: None).result
    pg.commit()
    row = pg.get(EvaluationOperatingPoint, uuid.UUID(out["evaluation_operating_point_id"]))
    assert row is not None and row.code_revision and row.description.isascii()
    declared = load_declared_threshold(pg, weights_id, store)
    rows = _cells(0.6)[("NK", BP)]
    weighted = _panel([r[3] for r in rows], [r[4] for r in rows], [r[5] for r in rows], "B")
    unweighted = _panel([r[0] for r in rows], [r[1] for r in rows], [r[2] for r in rows], "B")
    index = int(np.flatnonzero(np.isclose(TAUS, declared.tau("NK:BPO")))[0])
    expected = cell_reading(weighted, unweighted, index, TAUS)
    cell = row.cells["NK:BPO"]
    assert cell["read"] is True and cell["at_grid_edge"] == declared.at_grid_edge["NK:BPO"]
    for key in ("tau", "f_micro_w", "f_w", "s_w", "cov", "oracle_f_micro_w", "smin"):
        assert cell[key] == pytest.approx(expected[key], abs=1e-6), key
    assert row.t0_category_disagreement is None
    strata = store.get(row.strata_uri)
    assert hashlib.sha256(strata).hexdigest() == row.strata_sha256
    import io

    import pyarrow.parquet as pq

    table = pq.read_table(io.BytesIO(strata)).to_pylist()
    assert {r["cell"] for r in table} == {"NK:BPO", "PK:MFO"}
    assert all(r["small"] for r in table)
    again = ReadOperatingPointOperation().execute(pg, payload, emit=lambda *a, **k: None).result
    assert again["evaluation_operating_point_id"] == str(row.id)
    assert pg.execute(select(sa.func.count(EvaluationOperatingPoint.id))).scalar_one() == 1


@pytest.mark.integration
def test_a_weights_row_that_declares_nothing_is_refused(pg: Session, store: _MemoryStore) -> None:
    from protea.core.operating_point import DeclaredThresholdUnreadable

    store.put("model-weights/lexicon/x.json", b"{}")
    lexicon = ModelWeights(
        kind="lexicon",
        name="x",
        format="json",
        uri="model-weights/lexicon/x.json",
        sha256=hashlib.sha256(b"{}").hexdigest(),
        size_bytes=2,
        meta={},
    )
    pg.add(lexicon)
    pg.commit()
    with pytest.raises(DeclaredThresholdUnreadable, match="declares no per-cell"):
        load_declared_threshold(pg, str(lexicon.id), store)
    lexicon.kind = "operating_threshold"
    pg.commit()
    with pytest.raises(DeclaredThresholdUnreadable, match="no declared-threshold block"):
        load_declared_threshold(pg, str(lexicon.id), store)
    store.put("model-weights/lexicon/x.json", b"{ }")
    with pytest.raises(DeclaredThresholdUnreadable, match="hashes to"):
        load_declared_threshold(pg, str(lexicon.id), store)


@pytest.mark.integration
def test_the_disagreement_table_is_read_on_a_window_of_lafas_frame(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from protea.core.t0_category import T0Categories, categories_of

    window = SimpleNamespace(truth_profile="own_graph", pivot_snapshot_id=uuid.uuid4())
    monkeypatch.setattr(
        read_module, "_benchmark_pairs", lambda _s, _w: [("P1", "P", "PK"), ("P2", "F", "LK")]
    )
    monkeypatch.setattr(
        read_module,
        "t0_categories",
        lambda _s, _w, accessions: T0Categories(
            "w", categories_of(accessions, {"P1": {"P": {"GO:1"}}})
        ),
    )
    found = read_module._disagreement(None, window)  # type: ignore[arg-type]
    assert found == {
        "table": {
            "NK": {"NK": 0, "LK": 1, "PK": 0},
            "LK": {"NK": 0, "LK": 0, "PK": 0},
            "PK": {"NK": 0, "LK": 0, "PK": 1},
        },
        "pairs": 2,
        "disagreements": 1,
    }
    other = SimpleNamespace(truth_profile="as_closed")
    assert read_module._disagreement(None, other) is None  # type: ignore[arg-type]


@pytest.mark.integration
def test_the_revision_upgrades_and_downgrades_against_postgres(
    alembic_config: Any,  # noqa: F811
    fresh_database: str,  # noqa: F811
) -> None:
    from alembic import command

    engine = create_engine(fresh_database)
    try:
        command.upgrade(alembic_config, migration.revision)
        inspector = sa.inspect(engine)
        assert TABLE in inspector.get_table_names()
        assert {c["name"] for c in inspector.get_check_constraints(TABLE)} == {
            "ck_evaluation_operating_point_descriptions",
            "ck_evaluation_operating_point_digests",
        }
        assert [u["name"] for u in inspector.get_unique_constraints(TABLE)] == [
            "uq_evaluation_operating_point_result_threshold"
        ]
        command.downgrade(alembic_config, migration.down_revision)
        assert TABLE not in sa.inspect(engine).get_table_names()
        command.upgrade(alembic_config, migration.revision)
        assert TABLE in sa.inspect(engine).get_table_names()
    finally:
        engine.dispose()
