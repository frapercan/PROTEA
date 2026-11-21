"""A window's results as its panel reads them, in one query.

Each result is its arm (model, scoring, method, how many neighbours it read,
whether a target's own sequence could vote, how its scores were read), its nine
cells (f_micro_w, the threshold it peaks at, whether that is the grid's edge,
the targets) and the strata profiles the stratification left on it.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
from sqlalchemy.orm import Session

from protea.infrastructure.orm.models.annotation.evaluation_result import EvaluationResult
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.services._window_summary import summary_row, window_summary_data
from protea.services.annotations_service import EntityNotFoundError
from tests.helpers.windows import make_window
from tests.test_run_count_map import pg  # noqa: F401

CELL = {
    "f_micro_w": 0.4405,
    "n_targets": 1045,
    "maxima": {"f_micro_w": {"tau": 0.57, "edge": None}, "f": {"tau": 0.31, "edge": None}},
}
PROFILES = {"NK": {"F": {"placed": [1045, 1.0, 2.0, 3.0, 0.4405]}}}


def _row(**over: Any) -> dict[str, Any]:
    row = {
        "id": "r1",
        "created_at": datetime(2026, 11, 21, 12, tzinfo=UTC),
        "prediction_set_id": "s1",
        "job_id": "j1",
        "results": {"NK": {"MFO": CELL}, "reading": {"score_reading": "percentile_rank"}},
        "model": "facebook/esm2_t33_650M_UR50D",
        "embedding_config_id": "e1",
        "scoring": "embedding_only",
        "method": None,
        "depth_cut": "3",
        "depth_retrieved": 30,
        "exclude_self": "true",
        "job_reading": None,
    }
    return {**row, **over}


def test_a_neighbour_arm_reads_its_cut_its_exclusion_and_its_reading() -> None:
    shown = summary_row(_row())
    assert shown["arm"] == {
        "model": "facebook/esm2_t33_650M_UR50D",
        "embedding_config_id": "e1",
        "scoring": "embedding_only",
        "method": None,
        "depth": 3,
        "exclude_self": True,
        "reading": "percentile_rank",
    }
    assert shown["cells"] == {"NK:MFO": {"f": 0.4405, "tau": 0.57, "edge": None, "targets": 1045}}
    assert shown["created_at"] == "2026-11-21T12:00:00+00:00"
    assert shown["strata"] is None


def test_an_uncut_set_reads_what_its_candidates_retrieved() -> None:
    assert summary_row(_row(depth_cut=None))["arm"]["depth"] == 30


def test_a_method_has_no_neighbours_whatever_its_limit_per_entry() -> None:
    shown = summary_row(_row(model=None, scoring=None, method="count-map-themes-lexico-93k-211"))
    assert shown["arm"]["depth"] is None


def test_the_reading_falls_back_on_the_job_then_on_raw() -> None:
    results = {"NK": {"MFO": CELL}}
    assert summary_row(_row(results=results, job_reading="percentile_rank"))["arm"]["reading"] == (
        "percentile_rank"
    )
    assert summary_row(_row(results=results))["arm"]["reading"] == "raw"


def test_the_profiles_the_stratification_left_are_passed_on() -> None:
    results = {
        "NK": {"MFO": CELL},
        "strata": {"job_id": "j9", "variant": "weighted", "profiles": PROFILES, "axes": []},
    }
    assert summary_row(_row(results=results))["strata"] == {
        "job_id": "j9",
        "variant": "weighted",
        "profiles": PROFILES,
    }


def test_a_missing_window_is_not_found(pg: Session) -> None:  # noqa: F811
    with pytest.raises(EntityNotFoundError):
        window_summary_data(pg, uuid.uuid4())


def test_the_window_is_read_in_one_query_oldest_first(pg: Session) -> None:  # noqa: F811
    window = pg.get(EvaluationSet, make_window(pg, snapshot_version="summary"))
    config = EmbeddingConfig(
        model_name="facebook/esm2_t33_650M_UR50D",
        model_backend="esm",
        layer_indices=[32],
        layer_agg="mean",
        pooling="mean",
        normalize_residues=False,
        normalize=True,
        use_chunking=False,
        chunk_size=512,
        chunk_overlap=0,
        max_length=1022,
        embedding_scale=1.0,
    )
    pg.add(config)
    pg.flush()
    parents = {
        "annotation_set_id": window.old_annotation_set_id,
        "ontology_snapshot_id": window.pivot_snapshot_id,
    }
    candidates = PredictionSet(
        embedding_config_id=config.id,
        limit_per_entry=30,
        meta={"exclude_self_neighbour": True},
        **parents,
    )
    pg.add(candidates)
    pg.flush()
    scored = PredictionSet(
        embedding_config_id=config.id,
        limit_per_entry=30,
        meta={
            "scored_from": str(candidates.id),
            "scoring": {"depth": {"max_k_position": 3}},
        },
        **parents,
    )
    pg.add(scored)
    pg.flush()
    for minute, pset in ((1, scored.id), (0, candidates.id)):
        pg.add(
            EvaluationResult(
                evaluation_set_id=window.id,
                prediction_set_id=pset,
                created_at=datetime(2026, 11, 21, 12, minute, tzinfo=UTC),
                results={"NK": {"MFO": CELL}, "reading": {"score_reading": "raw"}},
            )
        )
    pg.flush()

    summary = window_summary_data(pg, window.id)

    assert summary["evaluation_set_id"] == str(window.id)
    first, second = summary["results"]
    assert first["prediction_set_id"] == str(candidates.id)
    assert (first["arm"]["depth"], first["arm"]["exclude_self"]) == (30, True)
    assert second["prediction_set_id"] == str(scored.id)
    assert (second["arm"]["depth"], second["arm"]["exclude_self"]) == (3, True)
    assert second["cells"]["NK:MFO"]["f"] == 0.4405
