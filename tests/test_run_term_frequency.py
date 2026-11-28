"""Term frequency on a window of LAFA's frame: rows, records, targets and refusals.

Against Postgres (``--with-postgres``), on the small cut of
``tests/helpers/native_methods.py``, whose counts are worked out by hand below:

- LAFA's literal counts the direct rows of the reviewed experimental members (T1,
  TW, D1), the NOT row of TW dropped without negating, D1's alternative id named as
  its primary, and divides by the proteins with a row in each aspect;
- the propagated variant counts their closed states (TW's NOT term out first) over
  the proteins holding a term in each aspect, roots out;
- the vector goes to every target as compact rows, with the count table as the
  set's evidence and what was counted as its written counts, and the method seal
  reads the set whole;
- a window with a declared sample is read on the sample, which the set names as its
  query set; the same method over the same window again is a no-op.
"""

from __future__ import annotations

import hashlib
import io
import uuid
from collections.abc import Iterator
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from pydantic import ValidationError
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

import protea.infrastructure.orm.models  # noqa: F401  (registers every table)
from protea.core import method_write
from protea.core.method_seal import method_identity
from protea.core.operations import run_term_frequency as op
from protea.core.query_population import population_of
from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.embedding.prediction_score import PredictionScore
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.query.query_set import QuerySet, QuerySetEntry
from tests.helpers.cut_photos import make_job
from tests.helpers.native_methods import World, make_world, whole_row

_TRUTH_STORE = "protea.core._evaluation_groundtruth._store"


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
def world(pg: Session, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> World:
    built = make_world(pg, tmp_path)
    monkeypatch.setattr(_TRUTH_STORE, lambda: built.store)
    monkeypatch.setattr(op, "_store", lambda: built.store)
    monkeypatch.setattr(method_write, "dependency_revisions", lambda: {"protea-x": "c" * 40})
    return built


def _run(
    pg: Session, world: World, variant: str, population: str = "reviewed_experimental"
) -> dict:
    payload = {
        "evaluation_set_id": str(world.window.id),
        "population": population,
        "variant": variant,
        "_job_id": str(make_job(pg, "run_term_frequency")),
    }
    result = op.RunTermFrequencyOperation().execute(pg, payload, emit=lambda *a, **k: None).result
    pg.commit()
    return result


def _scores(pg: Session, set_id: str, world: World) -> dict[str, dict[str, float]]:
    go_id_of = {i: g for g, i in world.term_ids.items()}
    rows = pg.execute(
        select(PredictionScore).where(PredictionScore.prediction_set_id == uuid.UUID(set_id))
    ).scalars()
    return {
        r.protein_accession: {go_id_of[t]: s for t, s in zip(r.go_term_ids, r.scores, strict=True)}
        for r in rows
    }


def test_the_literal_variant_without_a_database() -> None:
    with pytest.raises(ValidationError, match="reviewed_experimental"):
        op.RunTermFrequencyPayload.model_validate(
            {
                "evaluation_set_id": str(uuid.uuid4()),
                "population": "experimental",
                "variant": "lafa_direct",
            }
        )
    direct = op.RunTermFrequencyPayload.model_validate(
        {
            "evaluation_set_id": str(uuid.uuid4()),
            "population": "reviewed_experimental",
            "variant": "lafa_direct",
        }
    )
    recipe = op.RunTermFrequencyPayload.model_validate(
        {
            "evaluation_set_id": str(uuid.uuid4()),
            "population": "experimental",
            "variant": "propagated",
        }
    )
    assert op.method_code(direct) == "term-frequency-lafa-direct"
    assert op.method_code(recipe) == "term-frequency-propagated-experimental"


@pytest.mark.integration
def test_the_literal_is_lafas_naive_on_the_reviewed_experimental_rows(
    pg: Session, world: World
) -> None:
    result = _run(pg, world, "lafa_direct")
    scores = _scores(pg, result["prediction_set_id"], world)
    vector = {"GO:0000001": 2 / 3, "GO:0000002": 2 / 3, "GO:0000006": 1.0, "GO:0000004": 1.0}
    assert set(scores) == {"T1", "T2", "T3"}
    assert all(got == pytest.approx(vector) for got in scores.values())
    pset = pg.get(PredictionSet, uuid.UUID(result["prediction_set_id"]))
    assert pset.meta["written_counts"]["branch"] == "proteins_per_aspect"
    assert pset.meta["written_counts"]["denominators"] == {"P": 3, "F": 1, "C": 1}
    assert pset.meta["written_counts"]["naming"] == {"row_renamed_alt_id": 1}
    assert pset.limit_per_entry == 4 and pset.query_set_id is None
    assert pset.meta["query_population"] == population_of(["T1", "T2", "T3"])
    # The count table is the evidence, named by its sha256 in the store.
    evidence = pset.meta["evidence_artifact"]
    blob = world.store.get(evidence["uri"])
    assert hashlib.sha256(blob).hexdigest() == evidence["sha256"]
    table = pq.read_table(io.BytesIO(blob)).to_pydict()
    assert dict(zip(table["go_id"], table["proteins"], strict=True)) == {
        "GO:0000001": 2,
        "GO:0000002": 2,
        "GO:0000004": 1,
        "GO:0000006": 1,
    }
    assert evidence["rows"] == 4
    method_identity(whole_row(pg, pset.id))
    again = _run(pg, world, "lafa_direct")
    assert again == {"prediction_set_id": result["prediction_set_id"], "skipped": True}


@pytest.mark.integration
def test_the_propagated_variant_counts_closed_states_at_the_floor(
    pg: Session, world: World
) -> None:
    result = _run(pg, world, "propagated")
    scores = _scores(pg, result["prediction_set_id"], world)
    vector = {
        "GO:0000001": 1.0,
        "GO:0000002": 2 / 3,
        "GO:0000003": 1.0,
        "GO:0000006": 1.0,
        "GO:0000004": 1.0,
    }
    assert scores["T1"] == pytest.approx(vector) and scores["T3"] == pytest.approx(vector)
    pset = pg.get(PredictionSet, uuid.UUID(result["prediction_set_id"]))
    assert pset.meta["written_counts"]["denominators"] == {"P": 3, "F": 1, "C": 1}
    assert pg.get(PredictionSet, uuid.UUID(result["prediction_set_id"])).method.short_name == (
        "term-frequency-propagated-reviewed-experimental-211"
    )
    # The experimental population adds D2, a TrEMBL member with an experimental row.
    wider = _scores(pg, _run(pg, world, "propagated", "experimental")["prediction_set_id"], world)
    assert wider["T1"]["GO:0000005"] == pytest.approx(1 / 4)


@pytest.mark.integration
def test_a_window_with_a_declared_sample_is_read_on_it(pg: Session, world: World) -> None:
    sample = QuerySet(
        name="sample",
        source_kind="window_sample",
        evaluation_set_id=world.window.id,
        artifact_uri="file:///sample.txt",
        content_sha256=population_of(["T1", "T3"])["sha256"],
        description="A sample.",
        job_id=make_job(pg, "declare_window_sample"),
    )
    pg.add(sample)
    pg.flush()
    from protea.infrastructure.orm.models.sequence.sequence import Sequence

    for accession in ("T1", "T3"):
        sequence = pg.execute(select(Sequence).limit(1)).scalar_one()
        pg.add(QuerySetEntry(query_set_id=sample.id, accession=accession, sequence_id=sequence.id))
    world.window.target_query_set_id = sample.id
    pg.commit()
    result = _run(pg, world, "lafa_direct")
    pset = pg.get(PredictionSet, uuid.UUID(result["prediction_set_id"]))
    assert set(_scores(pg, result["prediction_set_id"], world)) == {"T1", "T3"}
    assert pset.query_set_id == sample.id and "query_population" not in pset.meta
    method_identity(whole_row(pg, pset.id))


@pytest.mark.integration
def test_a_window_of_another_frame_is_refused(pg: Session, world: World) -> None:
    world.window.truth_profile = "projected"
    pg.commit()
    with pytest.raises(op.TermFrequencyRefused, match="LAFA's frame"):
        _run(pg, world, "lafa_direct")
