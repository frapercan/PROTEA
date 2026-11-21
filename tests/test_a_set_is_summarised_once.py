"""A prediction set is counted once and its pages read the counts.

Against Postgres: a candidate set's rows and a scored set's arrays are counted
alike (rows, proteins, totals per aspect, most frequent terms, each protein's
terms and closest distance), the count is kept and not repeated, the page of
proteins reads the counted table, and the operation counts every set that has
no summary yet.
"""

from __future__ import annotations

import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.core.operations.summarize_prediction_sets import SummarizePredictionSetsOperation
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.orm.models.embedding.prediction_score import PredictionScore
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.embedding.prediction_set_protein import PredictionSetProtein
from protea.services._prediction_set_summary import go_term_distribution, protein_page, summarize
from tests.helpers.windows import make_window
from tests.test_run_count_map import pg  # noqa: F401

TERMS = {"GO:0000001": "P", "GO:0000002": "P", "GO:0000003": "F"}


def _parents(pg: Session) -> tuple[dict, dict[str, int]]:  # noqa: F811
    window = pg.get(EvaluationSet, make_window(pg, snapshot_version="summary"))
    ids = {}
    for go_id, aspect in TERMS.items():
        term = GOTerm(
            go_id=go_id,
            name=f"term {go_id[-1]}",
            aspect=aspect,
            ontology_snapshot_id=window.pivot_snapshot_id,
        )
        pg.add(term)
        pg.flush()
        ids[go_id] = term.id
    parents = {
        "annotation_set_id": window.old_annotation_set_id,
        "ontology_snapshot_id": window.pivot_snapshot_id,
    }
    return parents, ids


def _method_set(pg: Session, parents: dict) -> PredictionSet:  # noqa: F811
    from protea.core.contracts.method import Family, InstanceCut, MethodSpec, Pin
    from protea.infrastructure.orm.models.method.method import Method

    spec = MethodSpec(
        family=Family.EXPLAINABLE_UNITS,
        code=f"summary-test-{uuid.uuid4().hex[:6]}",
        instance=InstanceCut(211, parents["ontology_snapshot_id"]),
        recipe={"rule": "test"},
        pins=(Pin("photo", "a" * 64, ("cut_photo:test",)),),
        runtime="native",
        explains=False,
    )
    pg.add(Method.from_spec(spec))
    pg.flush()
    pset = PredictionSet(method_id=spec.method_id, limit_per_entry=None, meta={}, **parents)
    pg.add(pset)
    pg.flush()
    return pset


def test_a_candidate_set_and_a_scored_set_are_counted_alike(pg: Session) -> None:  # noqa: F811
    parents, ids = _parents(pg)
    candidates = _method_set(pg, parents)
    for acc, go_id, distance in (
        ("P1", "GO:0000001", 0.2),
        ("P1", "GO:0000003", 0.1),
        ("P2", "GO:0000001", 0.4),
    ):
        pg.add(
            GOPrediction(
                prediction_set_id=candidates.id,
                protein_accession=acc,
                ref_protein_accession="R",
                go_term_id=ids[go_id],
                distance=distance,
            )
        )
    scored = _method_set(pg, parents)
    pg.add(
        PredictionScore(
            prediction_set_id=scored.id,
            protein_accession="P1",
            go_term_ids=[ids["GO:0000001"], ids["GO:0000002"]],
            scores=[0.9, 0.5],
        )
    )
    pg.add(
        PredictionScore(
            prediction_set_id=scored.id,
            protein_accession="P3",
            go_term_ids=[ids["GO:0000001"]],
            scores=[0.7],
        )
    )
    pg.flush()

    first = summarize(pg, candidates.id)
    assert (first["rows"], first["proteins"]) == (3, 2)
    assert first["aspect_totals"] == {"P": 2, "F": 1}
    assert first["top_terms"][0] == {
        "go_id": "GO:0000001",
        "name": "term 1",
        "aspect": "P",
        "count": 2,
    }
    stats = {
        r.protein_accession: (r.go_count, r.min_distance)
        for r in pg.scalars(
            select(PredictionSetProtein).where(
                PredictionSetProtein.prediction_set_id == candidates.id
            )
        )
    }
    assert stats == {"P1": (2, 0.1), "P2": (1, 0.4)}

    other = summarize(pg, scored.id)
    assert (other["rows"], other["proteins"]) == (3, 2)
    assert other["aspect_totals"] == {"P": 3}
    total, rows = protein_page(pg, scored.id, None, 50, 0)
    assert (total, rows) == (2, [("P1", 2, None), ("P3", 1, None)])
    assert protein_page(pg, scored.id, "p3", 50, 0) == (1, [("P3", 1, None)])

    by_aspect = go_term_distribution(first, 50)["by_aspect"]
    assert [t["go_id"] for t in by_aspect["P"]] == ["GO:0000001"]
    assert [t["go_id"] for t in by_aspect["F"]] == ["GO:0000003"]


def test_a_set_is_counted_once(pg: Session) -> None:  # noqa: F811
    parents, ids = _parents(pg)
    pset = _method_set(pg, parents)
    pg.add(
        GOPrediction(
            prediction_set_id=pset.id,
            protein_accession="P1",
            ref_protein_accession="R",
            go_term_id=ids["GO:0000001"],
            distance=0.2,
        )
    )
    pg.flush()
    first = summarize(pg, pset.id)
    # A second count keeps the first: the summary is written only where there is none.
    again = summarize(pg, pset.id)
    assert again["computed_at"] == first["computed_at"]
    assert (
        pg.scalar(
            select(PredictionSetProtein.go_count).where(
                PredictionSetProtein.prediction_set_id == pset.id
            )
        )
        == 1
    )


def test_the_operation_counts_every_set_without_a_summary(pg: Session) -> None:  # noqa: F811
    parents, ids = _parents(pg)
    sets = [_method_set(pg, parents) for _ in range(2)]
    for pset in sets:
        pg.add(
            GOPrediction(
                prediction_set_id=pset.id,
                protein_accession="P1",
                ref_protein_accession="R",
                go_term_id=ids["GO:0000002"],
                distance=0.3,
            )
        )
    pg.commit()

    events: list[tuple] = []
    out = SummarizePredictionSetsOperation().execute(pg, {}, emit=lambda *a, **k: events.append(a))

    assert out.result == {"sets": 2, "rows": 2}
    assert all(pg.get(PredictionSet, s.id).summary["rows"] == 1 for s in sets)
    assert [e[0] for e in events].count("summarize_prediction_sets.set") == 2
