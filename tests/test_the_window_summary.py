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

from protea.core.leakage_guard import Frame, release_ordinal
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
        "backend": "esm",
        "config_release": None,
        "method_release": None,
    }
    return {**row, **over}


#: 211 -> 227, the window of via 1.
FRAME = Frame(211, 227)


def _method(release: int) -> dict[str, Any]:
    return _row(
        model=None,
        embedding_config_id=None,
        backend=None,
        method="own-annotations-nonexp-lafa-211",
        method_release=release,
    )


class TestEachArmSaysWhetherItMayBeRead:
    def test_a_pretrained_backbone_saw_none_of_our_annotations(self) -> None:
        arm = summary_row(_row(), FRAME)["arm"]
        assert (arm["leak"], arm["training_release"]) == ("not_fitted", None)

    def test_a_method_living_at_t0_read_what_every_arm_may_read(self) -> None:
        arm = summary_row(_method(211), FRAME)["arm"]
        assert (arm["leak"], arm["training_release"]) == ("clean", 211)

    def test_a_method_living_after_t0_has_read_the_answers(self) -> None:
        # Inside the window or past its end alike: the guard's rule, not a second one.
        assert summary_row(_method(227), FRAME)["arm"]["leak"] == "contaminated"
        assert summary_row(_method(230), FRAME)["arm"]["leak"] == "contaminated"

    def test_a_fitted_encoder_is_read_by_the_cut_its_configuration_declares(self) -> None:
        at_t0 = _row(backend="ingested-method", config_release="211")
        silent = _row(backend="learned-code", config_release=None)
        assert summary_row(at_t0, FRAME)["arm"]["leak"] == "clean"
        assert summary_row(silent, FRAME)["arm"]["leak"] == "undeclared"

    def test_an_arm_naming_no_producer_derives_nothing(self) -> None:
        orphan = _row(model=None, embedding_config_id=None, backend=None)
        assert summary_row(orphan, FRAME)["arm"]["leak"] is None

    def test_it_is_the_rule_the_dispatch_guard_refuses_on(self) -> None:
        # Two implementations of one rule drift the first time the boundary is
        # argued about again; the benchmark matrix that showed it before (its
        # badge, retired with it) called the same function.
        import inspect

        from protea.services import _window_summary

        assert "training_cut_verdict(" in inspect.getsource(_window_summary.read_verdict)

    def test_a_window_whose_ends_are_not_releases_derives_nothing(self) -> None:
        arm = summary_row(_method(211), None)["arm"]
        assert (arm["leak"], arm["training_release"]) == (None, None)


def test_a_release_is_read_from_its_digits_only() -> None:
    assert release_ordinal(" 227 ") == 227
    # int() would read the underscore as a digit separator: 202601, a "release"
    # after every window.
    assert release_ordinal("2026_01") is None
    assert release_ordinal(None) is None


def test_a_neighbour_arm_reads_its_cut_its_exclusion_and_its_reading() -> None:
    shown = summary_row(_row(), FRAME)
    assert shown["arm"] == {
        "model": "facebook/esm2_t33_650M_UR50D",
        "embedding_config_id": "e1",
        "scoring": "embedding_only",
        "method": None,
        "depth": 3,
        "exclude_self": True,
        "reading": "percentile_rank",
        "leak": "not_fitted",
        "training_release": None,
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
    assert summary["window_releases"] == [211, 227]
    first, second = summary["results"]
    assert first["arm"]["leak"] == "not_fitted"
    assert first["prediction_set_id"] == str(candidates.id)
    assert (first["arm"]["depth"], first["arm"]["exclude_self"]) == (30, True)
    assert second["prediction_set_id"] == str(scored.id)
    assert (second["arm"]["depth"], second["arm"]["exclude_self"]) == (3, True)
    assert second["cells"]["NK:MFO"]["f"] == 0.4405


def test_an_ingested_set_is_the_method_its_producer_names(pg: Session) -> None:  # noqa: F811
    """No method row and no neighbours: the producer's record names the arm.

    An ingested set's configuration carries the method's name as its model name,
    and its limit per entry is how many terms the busiest protein holds, so read
    as a retrieval it showed as "candidates" at a depth of hundreds.
    """
    window = pg.get(EvaluationSet, make_window(pg, snapshot_version="ingested"))
    config = EmbeddingConfig(
        model_name="own-annotations-ranked-194",
        model_backend="ingested-method",
        layer_indices=[0],
        layer_agg="mean",
        pooling="mean",
        normalize_residues=False,
        normalize=False,
        use_chunking=False,
        chunk_size=1,
        chunk_overlap=0,
        max_length=1,
        embedding_scale=1.0,
        trained_on_annotation_set_id=window.old_annotation_set_id,
    )
    pg.add(config)
    pg.flush()
    ingested = PredictionSet(
        embedding_config_id=config.id,
        annotation_set_id=window.old_annotation_set_id,
        ontology_snapshot_id=window.pivot_snapshot_id,
        limit_per_entry=893,
        meta={"producer": {"name": "own-annotations-ranked-194"}},
    )
    pg.add(ingested)
    pg.flush()
    pg.add(
        EvaluationResult(
            evaluation_set_id=window.id,
            prediction_set_id=ingested.id,
            results={"NK": {"MFO": CELL}, "reading": {"score_reading": "percentile_rank"}},
        )
    )
    pg.flush()

    (shown,) = window_summary_data(pg, window.id)["results"]

    assert shown["arm"]["method"] == "own-annotations-ranked-194"
    assert shown["arm"]["depth"] is None
    assert shown["arm"]["scoring"] is None
