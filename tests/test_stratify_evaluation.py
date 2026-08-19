"""Turning a finished evaluation into cells.

Reads the per-protein table `run_cafa_evaluation` writes, places each protein
on the strata axes, and pools each cell micro. Written as a separate operation
so it can stratify evaluations that already exist without rerunning them.
"""

from __future__ import annotations

import uuid

import pandas as pd
import pytest

from protea.core.operations.stratify_evaluation import (
    _ASPECT_FOR_NAMESPACE,
    StratifyEvaluationOperation,
    StratifyEvaluationPayload,
    _category_for,
    _strata_for_rows,
)
from protea.core.strata import Aspect, Category, Neighbourhood

_NB = Neighbourhood(best_identity=45.0, donor_is_experimental=True, taxonomic_relation="close")


def _row(acc: str, ns: str, tp: float = 1.0, pred: float = 2.0, n_gt: float = 2.0) -> dict:
    return {
        "protein_accession": acc, "namespace": ns, "tau": 0.5,
        "tp_w": tp, "pred_w": pred, "n_gt_w": n_gt,
        "precision_w": tp / pred, "recall_w": tp / n_gt, "f_w": 0.5,
    }


class TestTheSettingNamesTheCategory:
    @pytest.mark.parametrize(
        ("setting", "expected"),
        [("NK", Category.NO_KNOWLEDGE), ("LK", Category.LIMITED_KNOWLEDGE),
         ("PK", Category.PARTIAL_KNOWLEDGE), ("nk_something", Category.NO_KNOWLEDGE)],
    )
    def test_known_settings_resolve(self, setting: str, expected: Category) -> None:
        assert _category_for(setting) is expected

    def test_an_unknown_setting_is_none_rather_than_a_guess(self) -> None:
        """A setting whose category cannot be read is skipped and reported, not
        filed under some default that would move a published number."""
        assert _category_for("scratch") is None


class TestNamespacesMapToAspects:
    def test_all_three_cafaeval_namespaces_are_known(self) -> None:
        assert set(_ASPECT_FOR_NAMESPACE) == {
            "biological_process", "molecular_function", "cellular_component"
        }

    def test_they_map_to_distinct_aspects(self) -> None:
        assert len(set(_ASPECT_FOR_NAMESPACE.values())) == 3


class TestPlacingRows:
    def test_a_complete_row_gets_a_stratum(self) -> None:
        placed = _strata_for_rows(
            [_row("Q1", "molecular_function")],
            category=Category.NO_KNOWLEDGE,
            lengths={"Q1": 300},
            neighbourhoods={"Q1": _NB},
        )
        assert placed[0].aspect is Aspect.MOLECULAR_FUNCTION
        assert placed[0].category is Category.NO_KNOWLEDGE

    @pytest.mark.parametrize(
        ("lengths", "neighbourhoods", "namespace"),
        [
            ({}, {"Q1": _NB}, "molecular_function"),               # no length
            ({"Q1": 300}, {}, "molecular_function"),               # no non-self donor
            ({"Q1": 300}, {"Q1": _NB}, "not_a_namespace"),         # unknown aspect
        ],
    )
    def test_rows_that_cannot_be_placed_are_skipped(
        self, lengths: dict, neighbourhoods: dict, namespace: str
    ) -> None:
        """Skipped, never defaulted: every default here would move a number by
        an amount nobody chose."""
        assert _strata_for_rows(
            [_row("Q1", namespace)],
            category=Category.NO_KNOWLEDGE, lengths=lengths, neighbourhoods=neighbourhoods,
        ) == {}


class _FakeResult:
    def __init__(self, rows): self._rows = rows
    def mappings(self): return self._rows
    def __iter__(self): return iter(self._rows)


class _FakeSession:
    """Answers the two queries the operation makes, in order."""

    def __init__(self, neighbourhood_rows, length_rows):
        self._answers = [neighbourhood_rows, length_rows]

    def execute(self, *_a, **_k):
        return _FakeResult(self._answers.pop(0))


class TestTheOperationEndToEnd:
    def _artifacts(self, tmp_path, rows):
        d = tmp_path / "NK"
        d.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows).to_parquet(d / "per_protein.parquet", index=False)
        return tmp_path

    def _session(self):
        return _FakeSession(
            [{"acc": "Q1", "best_identity": 45.0, "evidence_code": "IDA",
              "taxonomic_relation": "close", "nearest_any": 0.1,
              "nearest_experimental": 0.1},
             {"acc": "Q2", "best_identity": 20.0, "evidence_code": "IEA",
              "taxonomic_relation": "close", "nearest_any": 0.3,
              "nearest_experimental": None}],
            [("Q1", 300), ("Q2", 300)],
        )

    def test_it_writes_one_row_per_cell(self, tmp_path) -> None:
        root = self._artifacts(
            tmp_path, [_row("Q1", "molecular_function"), _row("Q2", "molecular_function")]
        )
        out = StratifyEvaluationOperation().execute(
            self._session(),
            {"prediction_set_id": str(uuid.uuid4()), "artifacts_root": str(root),
             "axes": ["homology"], "min_population": 1},
            emit=lambda *a, **k: None,
        )
        frame = pd.read_parquet(root / "NK" / "strata.parquet")
        assert out.result["settings"]["NK"]["rows_placed"] == 2
        # two proteins in different homology bands -> two cells
        assert len(frame) == 2
        assert set(frame.homology) == {"30-60", "<=30"}

    def test_axes_are_written_as_their_canonical_values(self, tmp_path) -> None:
        """Category and Aspect are plain Enums, so str() would write
        'Aspect.MOLECULAR_FUNCTION' and the column would stop matching the
        vocabulary every other table uses."""
        root = self._artifacts(tmp_path, [_row("Q1", "molecular_function")])
        StratifyEvaluationOperation().execute(
            self._session(),
            {"prediction_set_id": str(uuid.uuid4()), "artifacts_root": str(root),
             "axes": ["category", "aspect"], "min_population": 1},
            emit=lambda *a, **k: None,
        )
        frame = pd.read_parquet(root / "NK" / "strata.parquet")
        assert frame.category.tolist() == ["NK"]
        assert frame.aspect.tolist() == ["F"]

    def test_thin_cells_are_written_and_flagged_not_dropped(self, tmp_path) -> None:
        """A table that prints only what survived looks identical to a table
        that covered everything."""
        root = self._artifacts(tmp_path, [_row("Q1", "molecular_function")])
        StratifyEvaluationOperation().execute(
            self._session(),
            {"prediction_set_id": str(uuid.uuid4()), "artifacts_root": str(root),
             "axes": ["homology"], "min_population": 99},
            emit=lambda *a, **k: None,
        )
        frame = pd.read_parquet(root / "NK" / "strata.parquet")
        assert len(frame) == 1
        assert frame.reportable.tolist() == [False]

    def test_the_pooled_score_is_micro(self, tmp_path) -> None:
        """Two proteins in one cell: sums then divides, not the mean of F."""
        root = self._artifacts(
            tmp_path,
            [_row("Q1", "molecular_function", tp=1.0, pred=1.0, n_gt=1.0),
             _row("Q2", "molecular_function", tp=1.0, pred=10.0, n_gt=10.0)],
        )
        StratifyEvaluationOperation().execute(
            self._session(),
            {"prediction_set_id": str(uuid.uuid4()), "artifacts_root": str(root),
             "axes": ["aspect"], "min_population": 1},
            emit=lambda *a, **k: None,
        )
        frame = pd.read_parquet(root / "NK" / "strata.parquet")
        assert frame.precision_w.tolist() == [pytest.approx(2 / 11)]
        assert frame.recall_w.tolist() == [pytest.approx(2 / 11)]


class TestThePayload:
    def test_the_default_axes_are_the_four_a_first_table_reports(self) -> None:
        p = StratifyEvaluationPayload(prediction_set_id="x", artifacts_root="/tmp")
        assert p.axes == ["category", "aspect", "length", "homology"]

    def test_the_population_floor_cannot_be_zero(self) -> None:
        with pytest.raises(ValueError):
            StratifyEvaluationPayload(
                prediction_set_id="x", artifacts_root="/tmp", min_population=0
            )


class TestItIsRegistered:
    def test_the_operation_can_be_dispatched(self) -> None:
        from protea.core.operation_catalog import build_operation_registry

        assert "stratify_evaluation" in build_operation_registry()._ops


class TestReadingFromTheArtifactStore:
    """The normal path: a finished evaluation's temporary directory is gone,
    and its files live in the store under eval_artifacts/<result_id>/."""

    def test_neither_source_is_refused_rather_than_guessed(self) -> None:
        from protea.core.operations.stratify_evaluation import _resolve_source

        p = StratifyEvaluationPayload(prediction_set_id="x")
        with pytest.raises(ValueError, match="nothing to read"):
            _resolve_source(p, lambda *a, **k: None)

    def test_a_local_root_needs_no_store(self, tmp_path) -> None:
        from protea.core.operations.stratify_evaluation import _resolve_source

        root, store, tmp = _resolve_source(
            StratifyEvaluationPayload(prediction_set_id="x", artifacts_root=str(tmp_path)),
            lambda *a, **k: None,
        )
        assert (root, store, tmp) == (tmp_path, None, None)

    def test_only_the_settings_present_in_the_store_are_fetched(self, tmp_path) -> None:
        from protea.core.operations.stratify_evaluation import _settings_from_store

        class _Store:
            def __init__(self): self.asked: list[str] = []
            def exists(self, key: str) -> bool:
                self.asked.append(key)
                return "/NK/" in key
            def get(self, key: str) -> bytes: return b"parquet-bytes"

        store = _Store()
        rid = str(uuid.uuid4())
        found = _settings_from_store(store, rid, tmp_path)

        assert found == ["NK"]
        assert (tmp_path / "NK" / "per_protein.parquet").read_bytes() == b"parquet-bytes"
        # the three CAFA categories are probed, and nothing else
        assert len(store.asked) == 3
        assert all(rid in key for key in store.asked)
