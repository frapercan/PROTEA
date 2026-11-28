"""A native writer's targets on a window, and how its set names them (design-final 2.6).

A window kept to a declared sample gives its sample's proteins, read from the query set
and never from the truth, and the set names them by that query set; any other window
gives the proteins its truth scores, named by its ``evaluation_targets`` set when one
holds exactly them and by their digest otherwise. The count map writes its set that
way, and the method seal reads it whole.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

import protea.infrastructure.orm.models  # noqa: F401  (registers every table)
from protea.core import method_write
from protea.core.evaluation import EvaluationData
from protea.core.method_seal import method_identity
from protea.core.operations import _window_targets as targets_module
from protea.core.operations import run_count_map as op
from protea.core.operations._window_targets import WindowTargets, window_targets
from protea.core.query_population import population_of
from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.query.query_set import QuerySet, QuerySetEntry
from protea.infrastructure.orm.models.sequence.sequence import Sequence
from tests.helpers.cut_photos import make_job
from tests.helpers.windows import make_window
from tests.test_run_count_map import _scenario

pytestmark = pytest.mark.integration

#: A prediction set as ``SELECT *`` returns it, which is what the method seal reads.
_WHOLE_ROW = text("SELECT * FROM prediction_set WHERE id = :p")


@pytest.fixture()
def pg(postgres_url: str) -> Iterator[Session]:
    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    Base.metadata.drop_all(engine)


def _query_set(session: Session, kind: str, window: uuid.UUID, accessions: list[str]) -> uuid.UUID:
    """A declared set of ``kind`` derived from ``window``, its entries and its sha256."""
    digest = population_of(accessions)["sha256"]
    row = QuerySet(
        name=f"{kind}-{digest[:8]}",
        description=f"{kind} of {window}",
        source_kind=kind,
        evaluation_set_id=window,
        artifact_uri=f"memory://{kind}/{digest}",
        content_sha256=digest,
        job_id=make_job(session, kind),
    )
    session.add(row)
    session.flush()
    for accession in accessions:
        sequence = Sequence(sequence=f"M{accession}", sequence_hash=uuid.uuid4().hex)
        session.add(sequence)
        session.flush()
        session.add(
            QuerySetEntry(query_set_id=row.id, sequence_id=sequence.id, accession=accession)
        )
    session.flush()
    return row.id


def _truth(monkeypatch: pytest.MonkeyPatch, targets: dict[str, list[str]] | None) -> None:
    def load(session: Session, eset: EvaluationSet) -> tuple[EvaluationData, uuid.UUID]:
        if targets is None:
            raise AssertionError("a window kept to a declared sample never reads its truth")
        data = EvaluationData(
            nk={a: {"GO:1"} for a in targets.get("nk", [])},
            lk={a: {"GO:2"} for a in targets.get("lk", [])},
            pk={a: {"GO:3"} for a in targets.get("pk", [])},
        )
        return data, eset.pivot_snapshot_id

    monkeypatch.setattr(targets_module, "load_evaluation_data_for_set", load)


def test_a_window_kept_to_a_sample_reads_the_sample_and_names_it(
    pg: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    parent = make_window(pg, snapshot_version="parent")
    sample = _query_set(pg, "window_sample", parent, ["Q2", "Q1", "Q3"])
    toy = pg.get(EvaluationSet, make_window(pg, snapshot_version="toy"))
    toy.target_query_set_id = sample
    pg.flush()
    _truth(monkeypatch, None)
    got = window_targets(pg, toy)
    assert got == WindowTargets(["Q1", "Q2", "Q3"], sample, None)
    assert got.from_a_declared_set


def test_a_sample_whose_entries_do_not_hash_as_declared_is_refused(
    pg: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    parent = make_window(pg, snapshot_version="parent")
    sample = _query_set(pg, "window_sample", parent, ["Q1", "Q2"])
    pg.get(QuerySet, sample).content_sha256 = "0" * 64
    toy = pg.get(EvaluationSet, make_window(pg, snapshot_version="toy"))
    toy.target_query_set_id = sample
    pg.flush()
    _truth(monkeypatch, None)
    with pytest.raises(ValueError, match="do not hash to its sha256"):
        window_targets(pg, toy)


def test_a_full_window_names_its_targets_by_the_set_that_holds_exactly_them(
    pg: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    window = pg.get(EvaluationSet, make_window(pg))
    _truth(monkeypatch, {"nk": ["T3"], "lk": ["T1"], "pk": ["T2", "T1"]})
    digest = population_of(["T1", "T2", "T3"])
    assert window_targets(pg, window) == WindowTargets(["T1", "T2", "T3"], None, digest)
    _query_set(pg, "evaluation_targets", window.id, ["T1", "T2"])  # one target short
    assert window_targets(pg, window).query_set_id is None
    declared = _query_set(pg, "evaluation_targets", window.id, ["T3", "T2", "T1"])
    assert window_targets(pg, window) == WindowTargets(["T1", "T2", "T3"], declared, None)


def test_the_count_maps_set_names_a_declared_sample_and_the_seal_reads_it(
    pg: Session, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(method_write, "dependency_revisions", lambda: {"protea-x": "c" * 40})
    eset_id = _scenario(pg, tmp_path, monkeypatch)
    sample = _query_set(pg, "window_sample", uuid.UUID(eset_id), ["D1", "T1", "T2", "T3"])
    pg.commit()
    real = op.gather

    def gathered(session: Session, store: object, src: op.Sources) -> op.Gathered:
        data = real(session, store, src)  # the scenario's cut
        spelling = WindowTargets(data.targets, sample, None)
        return op.Gathered(
            *[getattr(data, f) for f in op.Gathered.__dataclass_fields__][:6], spelling
        )

    monkeypatch.setattr(op, "gather", gathered)
    payload = {
        "evaluation_set_id": eset_id,
        "lexicon_bank_id": str(uuid.uuid4()),
        "top": None,
        "exclusion": "accession",
    }
    result = op.RunCountMapOperation().execute(pg, payload, emit=lambda *a, **k: None).result
    pg.commit()
    pset = pg.get(PredictionSet, uuid.UUID(result["prediction_set_id"]))
    assert pset.query_set_id == sample and "query_population" not in pset.meta
    assert result["method"] == "count-map-themes-lexico-test-accession-grid-211"
    per_target = pg.execute(
        text(
            "SELECT max(n) FROM (SELECT count(*) AS n FROM go_prediction "
            "WHERE prediction_set_id = :p GROUP BY protein_accession) counted"
        ),
        {"p": pset.id},
    ).scalar_one()
    assert pset.limit_per_entry == per_target
    assert all("rows_past_rank_100" in record for record in pset.meta["aspects"].values())
    method_identity(dict(pg.execute(_WHOLE_ROW, {"p": pset.id}).mappings().one()))
