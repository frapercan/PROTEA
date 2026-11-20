"""A window's targets read from their own non-experimental annotations at t0 (democafa goa_nonexp).

The pure parts (codes through the closure, rows without roots and denials, the
side reconciliation fed other rows) are tested alone; the loader and the method's
writing run against Postgres, with the reconciliation stubbed in the latter the way
``tests/test_run_count_map.py`` stubs the count map's inputs.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from protea.core import evaluation
from protea.core._binding_rule import DESCENDANTS_STEP
from protea.core.operations import run_own_annotations as op
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.protein_go_annotation import ProteinGOAnnotation
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.method.prediction_explanation import PredictionExplanation
from protea.infrastructure.orm.models.protein.protein import Protein
from tests.helpers.windows import make_window
from tests.test_run_count_map import pg  # noqa: F401

PARENTS = {"GO:0000003": {"GO:0000002"}, "GO:0000002": {"GO:0008150"}}


def test_a_terms_codes_are_those_of_the_direct_terms_that_imply_it() -> None:
    codes = op.term_codes({"GO:0000003": {"IEA"}, "GO:0000002": {"IBA"}}, PARENTS)
    assert codes == {
        "GO:0000003": {"IEA"},
        "GO:0000002": {"IEA", "IBA"},
        "GO:0008150": {"IEA", "IBA"},
    }


def test_rows_leave_out_roots_denials_and_terms_the_pivot_lacks() -> None:
    positives = {"P1": {"P": {"GO:0008150", "GO:0000002", "GO:0000003", "GO:0000009"}}}
    codes = {"P1": {"GO:0000002": {"IBA"}, "GO:0000003": {"IEA"}}}
    ids = {"GO:0000002": 2, "GO:0000003": 3, "GO:0000004": 4}
    rows = list(op.prediction_rows(positives, {"P1": {"GO:0000003"}}, codes, ids))
    assert rows == [("P1", 2, 1.0, {"own": True, "aspect": "P", "codes": ["IBA"]})]
    # a term the reconciliation placed that no direct closure names (a remap) gets every code
    remapped = {"P1": {"P": {"GO:0000004"}}}
    (row,) = op.prediction_rows(remapped, {}, codes, ids)
    assert row[3]["codes"] == ["IBA", "IEA"]


def test_the_side_reconciles_the_rows_it_is_handed(monkeypatch: pytest.MonkeyPatch) -> None:
    def never(*a: object, **k: object) -> None:
        raise AssertionError("the experimental rows must not be read when raw is given")

    monkeypatch.setattr(evaluation, "_load_experimental_raw_go_ids", never)
    monkeypatch.setattr(evaluation, "_load_parents_by_go_id", lambda s, i: PARENTS)
    monkeypatch.setattr(evaluation, "_drop_binding_only", lambda s, i, raw: 0)
    side = SimpleNamespace(
        native_snapshot_id=uuid.uuid4(),
        annotation_set_id=uuid.uuid4(),
        frame=None,
        binding=DESCENDANTS_STEP,
    )
    out, _ = evaluation._reconcile_experimental_side(
        None,  # type: ignore[arg-type]
        side,  # type: ignore[arg-type]
        {"GO:0000002", "GO:0000003"},
        {"GO:0000002": "P", "GO:0000003": "P"},
        raw={"P1": {"GO:0000003"}},
    )
    assert out == {"P1": {"P": {"GO:0000002", "GO:0000003"}}}


def _window_with_rows(pg: Session) -> EvaluationSet:  # noqa: F811
    eset = pg.get(EvaluationSet, make_window(pg, snapshot_version="own-annotations"))
    terms = {}
    for go_id in ("GO:0000002", "GO:0000003", "GO:0008150"):
        term = GOTerm(go_id=go_id, aspect="P", ontology_snapshot_id=eset.pivot_snapshot_id)
        pg.add(term)
        terms[go_id] = term
    for acc in ("P1", "P2", "P3"):
        pg.add(Protein(accession=acc, canonical_accession=acc))
    pg.flush()
    rows = [
        ("P1", "GO:0000003", "IEA", None),
        ("P1", "GO:0000002", "IDA", None),
        ("P1", "GO:0008150", "ND", None),
        ("P1", "GO:0000002", "IEA", "NOT|involved_in"),
        ("P2", "GO:0000002", "IBA", None),
        ("P3", "GO:0000003", "IEA", None),
    ]
    for acc, go_id, code, qualifier in rows:
        pg.add(
            ProteinGOAnnotation(
                annotation_set_id=eset.old_annotation_set_id,
                protein_accession=acc,
                go_term_id=terms[go_id].id,
                evidence_code=code,
                qualifier=qualifier,
            )
        )
    pg.commit()
    return eset


@pytest.mark.integration
def test_the_loader_reads_the_targets_own_non_experimental_rows(pg: Session) -> None:  # noqa: F811
    eset = _window_with_rows(pg)
    got = op.own_rows(pg, eset.old_annotation_set_id, ["P1", "P2"])
    assert got == {"P1": {"GO:0000003": {"IEA"}}, "P2": {"GO:0000002": {"IBA"}}}


@pytest.mark.integration
def test_a_run_writes_its_method_rows_and_explanations_once(
    pg: Session,  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    eset = _window_with_rows(pg)
    photo = SimpleNamespace(
        id=uuid.uuid4(),
        cut_release=211,
        ontology_snapshot_id=eset.pivot_snapshot_id,
        annotation_set_id=eset.old_annotation_set_id,
        manifest_sha256="b" * 64,
        kind="full",
    )
    ids = op.term_ids(pg, eset.pivot_snapshot_id)
    rows = [
        ("P1", ids["GO:0000003"], 1.0, {"own": True, "aspect": "P", "codes": ["IEA"]}),
        ("P1", ids["GO:0000002"], 1.0, {"own": True, "aspect": "P", "codes": ["IEA"]}),
    ]
    monkeypatch.setattr(op, "resolve", lambda session, e: (eset, photo))
    monkeypatch.setattr(op, "read_targets", lambda session, e: (["P1", "P2"], rows))
    payload = {"evaluation_set_id": str(eset.id)}
    result = op.RunOwnAnnotationsOperation().execute(pg, payload, emit=lambda *a, **k: None).result
    pg.commit()
    assert result["rows"] == result["explanations"] == 2 and result["targets_with_a_term"] == 1
    pset = pg.get(PredictionSet, uuid.UUID(result["prediction_set_id"]))
    assert pset.meta["evaluation_set_id"] == str(eset.id)
    explained = pg.execute(select(func.count()).select_from(PredictionExplanation)).scalar_one()
    assert explained == 2
    again = op.RunOwnAnnotationsOperation().execute(pg, payload, emit=lambda *a, **k: None).result
    assert again["skipped"] is True and again["prediction_set_id"] == result["prediction_set_id"]
