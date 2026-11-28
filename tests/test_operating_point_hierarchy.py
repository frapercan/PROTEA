"""Hierarchy restoration: each predicted term's ancestors take the maximum of themselves and it.

Three properties, each against the evaluator itself rather than a second reading
of the ontology:

- the ancestors are cafaeval-protea's: propagating one prediction through the
  evaluator's own propagation lights exactly the ancestors restoration raises;
- on a restored set the evaluator's ``fill`` and ``max`` propagation give
  byte-identical per-protein grids, which is what restoration is for;
- on a set that is already consistent, restoration changes nothing.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pyarrow as pa
import pytest

from protea.core.operating_point import grid
from protea.core.operating_point.hierarchy import (
    ancestry_from_obo,
    restoration_counts,
    restore,
    restore_with_counts,
)
from protea.core.operations._run_cafa_grid_artifact import grid_rows_from_sink
from tests.helpers.operating_point import TERMS, raw_scores, write_frame

pytest.importorskip("cafaeval")


@pytest.fixture()
def frame(tmp_path: Path) -> grid.EvaluatorInputs:
    return write_frame(tmp_path / "frame")


class TestTheAncestorsAreTheEvaluators:
    def test_one_prediction_propagated_by_the_evaluator_lights_exactly_its_ancestors(
        self, frame: grid.EvaluatorInputs
    ) -> None:
        from cafaeval.graph import propagate
        from cafaeval.parser import obo_parser

        ancestry = ancestry_from_obo(frame.obo_path)
        ontologies = obo_parser(frame.obo_path, ("is_a", "part_of"), None, True)
        for ontology in ontologies.values():
            for term in ontology.terms_list:
                matrix = np.zeros((1, ontology.idxs))
                matrix[0, ontology.terms_dict[term["id"]]["index"]] = 0.5
                propagate(matrix, ontology, ontology.order, mode="max")
                lit = {ontology.terms_list[j]["id"] for j in np.flatnonzero(matrix[0])}
                assert lit == ancestry.ancestors_of(term["id"]), term["id"]

    def test_an_alternative_id_is_read_as_its_primary_and_an_obsolete_term_is_unknown(
        self, frame: grid.EvaluatorInputs
    ) -> None:
        ancestry = ancestry_from_obo(frame.obo_path)
        assert ancestry.ancestors_of("GO:0000092") == ancestry.ancestors_of("GO:0000002")
        assert ancestry.ancestors_of("GO:0000005") == {
            "GO:0000005",
            "GO:0000003",
            "GO:0000002",
            "GO:0008150",
        }
        assert ancestry.ancestors_of("GO:0000099") == frozenset()
        assert set(ancestry.terms) == set(TERMS)


def _grid_rows(frame: grid.EvaluatorInputs, pred_dir: Path, prop: str) -> list[tuple]:
    """Every setting's per-protein grid rows, scored with the evaluator under ``prop``."""
    from cafaeval.evaluation import cafa_eval

    rows = []
    for setting in grid.SETTINGS:
        sink = grid.new_per_protein_sink()
        cafa_eval(
            frame.obo_path,
            str(pred_dir),
            frame.ground_truth[setting],
            ia=frame.ia_path,
            exclude=frame.known_for(setting),
            prop=prop,
            norm="cafa",
            no_orphans=True,
            toi_file=frame.toi_path,
            th_step=0.01,
            n_cpu=1,
            per_protein_sink=sink,
        )
        for row in grid_rows_from_sink(sink).rows:
            rows.append(
                (
                    setting,
                    row["protein_accession"],
                    row["namespace"],
                    tuple(np.asarray(row["tp"]).tolist()),
                    tuple(np.asarray(row["pred"]).tolist()),
                    tuple(np.asarray(row["tp_w"]).tolist()),
                    tuple(np.asarray(row["pred_w"]).tolist()),
                )
            )
    return sorted(rows)


def _written(table: pa.Table, folder: Path) -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    grid.write_prediction_file(grid.render_scores(table), folder / "predictions.tsv")
    return folder


class TestRestorationMakesFillAndMaxOneReading:
    def test_a_restored_set_gives_identical_grids_under_fill_and_max(
        self, frame: grid.EvaluatorInputs, tmp_path: Path
    ) -> None:
        restored = restore(raw_scores(), ancestry_from_obo(frame.obo_path))
        folder = _written(restored, tmp_path / "restored")
        assert _grid_rows(frame, folder, "fill") == _grid_rows(frame, folder, "max")

    def test_the_raw_fixture_is_not_consistent_so_the_two_differ_without_it(
        self, frame: grid.EvaluatorInputs, tmp_path: Path
    ) -> None:
        folder = _written(raw_scores(), tmp_path / "raw")
        assert _grid_rows(frame, folder, "fill") != _grid_rows(frame, folder, "max")


class TestWhatRestorationDoes:
    def test_ancestors_take_the_maximum_and_missing_ones_are_added(
        self, frame: grid.EvaluatorInputs
    ) -> None:
        ancestry = ancestry_from_obo(frame.obo_path)
        table = grid.score_table(
            ["P1", "P1", "P1"], ["GO:0000005", "GO:0000003", "GO:0008150"], [0.9, 0.4, 0.95]
        )
        restored, counts = restore_with_counts(table, ancestry)
        assert dict(
            zip(restored["go_id"].to_pylist(), restored["score"].to_pylist(), strict=True)
        ) == {
            "GO:0000002": 0.9,
            "GO:0000003": 0.9,
            "GO:0000005": 0.9,
            "GO:0008150": 0.95,
        }
        assert (counts.rows_in, counts.rows_out, counts.raised, counts.added) == (3, 4, 1, 1)
        assert counts.changed

    def test_an_alternative_id_row_becomes_its_primary_and_an_unknown_term_stays(
        self, frame: grid.EvaluatorInputs
    ) -> None:
        ancestry = ancestry_from_obo(frame.obo_path)
        table = grid.score_table(["P2", "P2"], ["GO:0000092", "GO:0000099"], [0.3, 0.8])
        restored = restore(table, ancestry)
        assert list(
            zip(restored["go_id"].to_pylist(), restored["score"].to_pylist(), strict=True)
        ) == [
            ("GO:0000002", 0.3),
            ("GO:0000099", 0.8),
            ("GO:0008150", 0.3),
        ]

    def test_restoration_is_the_identity_on_a_consistent_set(
        self, frame: grid.EvaluatorInputs
    ) -> None:
        ancestry = ancestry_from_obo(frame.obo_path)
        consistent = restore(raw_scores(), ancestry)
        again, counts = restore_with_counts(consistent, ancestry)
        assert again.equals(consistent)
        assert not counts.changed
        assert restoration_counts(consistent, ancestry).raised == 0

    def test_proteins_never_share_a_restoration(self, frame: grid.EvaluatorInputs) -> None:
        ancestry = ancestry_from_obo(frame.obo_path)
        table = grid.score_table(["P1", "P2"], ["GO:0000006", "GO:0000004"], [0.7, 0.2])
        restored = restore(table, ancestry)
        p2 = {
            go: score
            for protein, go, score in zip(
                restored["protein_accession"].to_pylist(),
                restored["go_id"].to_pylist(),
                restored["score"].to_pylist(),
                strict=True,
            )
            if protein == "P2"
        }
        assert p2 == {"GO:0000004": 0.2, "GO:0000002": 0.2, "GO:0008150": 0.2}

    def test_an_empty_table_restores_to_an_empty_table(self, frame: grid.EvaluatorInputs) -> None:
        restored = restore(grid.score_table([], [], []), ancestry_from_obo(frame.obo_path))
        assert restored.num_rows == 0
