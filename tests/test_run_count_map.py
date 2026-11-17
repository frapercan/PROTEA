"""The count map's operation writes a method, its weights, its predictions and their witnesses.

The heavy reads (the state at the cut, the members' sequences, the bank's units)
are replaced by a small cut; everything the operation writes goes to Postgres:
the ``count_map`` weights in a local store, the ``method`` row derived from its
spec, one prediction set with ``method_score`` rows and one explanation per row.
A second run over the same window is a no-op.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

import protea.infrastructure.orm.models  # noqa: F401  (registers every table)
from protea.core._cut_state import CutState
from protea.core.operations import run_count_map as op
from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.orm.models.embedding.model_weights import ModelWeights
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.method.method import Method
from protea.infrastructure.orm.models.method.prediction_explanation import PredictionExplanation
from protea.infrastructure.storage import LocalFsArtifactStore
from tests.helpers.windows import make_window

TERMS = {
    "GO:0000001": "P",
    "GO:0000002": "P",
    "GO:0000003": "F",
    "GO:0008150": "P",
    "GO:0003674": "F",
}


@pytest.fixture()
def pg(postgres_url: str) -> Iterator[Session]:
    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    Base.metadata.drop_all(engine)


def _scenario(pg: Session, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    eset = pg.get(EvaluationSet, make_window(pg, snapshot_version="count-map"))
    for go_id, aspect in TERMS.items():
        pg.add(GOTerm(go_id=go_id, aspect=aspect, ontology_snapshot_id=eset.pivot_snapshot_id))
    lexicon = ModelWeights(
        kind="lexicon",
        name="lexico-test",
        format="pt",
        uri="memory://lexico",
        sha256="a" * 64,
        size_bytes=1,
        meta={},
    )
    pg.add(lexicon)
    pg.commit()
    photo = SimpleNamespace(
        id=uuid.uuid4(),
        cut_release=211,
        ontology_snapshot_id=eset.pivot_snapshot_id,
        annotation_set_id=eset.old_annotation_set_id,
        manifest_sha256="b" * 64,
    )
    bank = SimpleNamespace(
        id=uuid.uuid4(), manifest_sha256="c" * 64, short_name="lexicon-test", shards=[]
    )
    monkeypatch.setattr(op, "resolve", lambda session, p: op.Sources(eset, photo, bank, lexicon))
    state = CutState(
        positives={
            "D1": {"P": {"GO:0000001", "GO:0008150"}, "F": {"GO:0000003", "GO:0003674"}},
            "D2": {"P": {"GO:0000001", "GO:0000002", "GO:0008150"}},
            "D3": {"P": {"GO:0000002", "GO:0008150"}},
        },
        negatives={"T2": {"GO:0000002"}},
        aspect=TERMS,
    )
    gathered = op.Gathered(
        state=state,
        donors=["D1", "D2", "D3"],
        targets=["D1", "T1", "T2", "T3"],
        md5_of={"D1": "m1", "D2": "m2", "D3": "m3", "T1": "m1", "T2": "m4"},
        units=(
            {"m1": np.array([0, 1]), "m2": np.array([0]), "m3": np.array([1]), "m4": np.array([1])},
            2,
        ),
        without_sequence=1,
    )
    monkeypatch.setattr(op, "gather", lambda session, store, src: gathered)
    store = LocalFsArtifactStore(tmp_path / "store")
    monkeypatch.setattr(op, "get_artifact_store", lambda settings: store)
    return str(eset.id)


def _run(pg: Session, eset_id: str) -> dict:
    payload = {"evaluation_set_id": eset_id, "lexicon_bank_id": str(uuid.uuid4()), "top": 5}
    result = op.RunCountMapOperation().execute(pg, payload, emit=lambda *a, **k: None)
    pg.commit()
    return result.result


@pytest.mark.integration
def test_a_run_writes_its_method_weights_predictions_and_witnesses(
    pg: Session, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    eset_id = _scenario(pg, tmp_path, monkeypatch)
    result = _run(pg, eset_id)
    method = pg.get(Method, uuid.UUID(result["method_id"]))
    assert method.short_name == "count-map-themes-lexico-test-211" and method.explains
    assert {p.role for p in method.pins} == {"lexicon", "lexicon-bank", "count-map"}
    weights = pg.execute(select(ModelWeights).where(ModelWeights.kind == "count_map")).scalar_one()
    assert weights.trained_on_annotation_set_id is not None
    assert next(p for p in method.pins if p.role == "count-map").sha256 == weights.sha256
    pset = pg.get(PredictionSet, uuid.UUID(result["prediction_set_id"]))
    assert pset.method_id == method.id and pset.embedding_config_id is None
    assert (
        pset.meta["evaluation_set_id"] == eset_id and pset.meta["targets_without_a_sequence"] == 1
    )
    rows = pg.execute(
        select(func.count(GOPrediction.id)).where(GOPrediction.prediction_set_id == pset.id)
    ).scalar_one()
    why = pg.execute(
        select(func.count())
        .select_from(PredictionExplanation)
        .where(PredictionExplanation.prediction_set_id == pset.id)
    ).scalar_one()
    assert rows == why == result["rows"] > 0
    # T2 is denied GO:0000002 at the cut, and no root is ever predicted
    denied = pg.execute(
        select(func.count(GOPrediction.id))
        .join(GOTerm, GOTerm.id == GOPrediction.go_term_id)
        .where(GOPrediction.prediction_set_id == pset.id)
        .where(
            ((GOPrediction.protein_accession == "T2") & (GOTerm.go_id == "GO:0000002"))
            | GOTerm.go_id.in_(["GO:0008150", "GO:0003674"])
        )
    ).scalar_one()
    assert denied == 0
    assert all(s is not None for (s,) in pg.execute(select(GOPrediction.method_score)))


@pytest.mark.integration
def test_the_same_method_over_the_same_window_is_a_no_op(
    pg: Session, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    eset_id = _scenario(pg, tmp_path, monkeypatch)
    first = _run(pg, eset_id)
    again = _run(pg, eset_id)
    assert again == {"prediction_set_id": first["prediction_set_id"], "skipped": True}
    assert pg.execute(select(func.count(PredictionSet.id))).scalar_one() == 1
    assert (
        pg.execute(
            select(func.count(ModelWeights.id)).where(ModelWeights.kind == "count_map")
        ).scalar_one()
        == 1
    )
