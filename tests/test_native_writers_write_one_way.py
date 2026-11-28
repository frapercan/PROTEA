"""What every native writer shares: compact scores, the proteins asked about, the seal.

- ``copy_scores`` writes one ``prediction_score`` row per protein, its terms ascending,
  and refuses, before a row is written, a term id the set's snapshot does not hold, a
  pair given twice and a score that is no probability;
- a window's declared target set is read without its truth: a toy window's sample
  (checked against its sha256) or a full window's one ``evaluation_targets`` set; two
  such sets are refused; a set names its proteins one way;
- every meta key the three writers emit is classified by the method seal, and
  ``limit_per_entry`` is execution on a set that names a method and identity on one
  that does not.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

import protea.infrastructure.orm.models  # noqa: F401  (registers every table)
from protea.core import method_seal
from protea.core.method_seal import CrossedMethodAxes, assert_one_axis, method_identity
from protea.core.method_write import (
    NativeWriteRefused,
    SetContext,
    _by_protein,
    copy_scores,
    declared_targets,
    new_prediction_set,
    population_spelling,
)
from protea.core.query_population import population_of
from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.embedding.prediction_score import PredictionScore
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.method.method import Method
from protea.infrastructure.orm.models.query.query_set import QuerySet, QuerySetEntry
from protea.infrastructure.orm.models.sequence.sequence import Sequence
from tests.helpers.cut_photos import make_job
from tests.helpers.windows import make_window


def test_pairs_are_grouped_by_protein_once_each_with_a_probability() -> None:
    grouped = _by_protein([("P1", 3, 0.5), ("P1", 1, 1.0), ("P2", 2, 0.0)])
    assert grouped == {"P1": {3: 0.5, 1: 1.0}, "P2": {2: 0.0}}
    with pytest.raises(NativeWriteRefused, match="given twice"):
        _by_protein([("P1", 3, 0.5), ("P1", 3, 0.4)])
    for bad in (float("nan"), -0.1, 1.5):
        with pytest.raises(NativeWriteRefused, match="between 0 and 1"):
            _by_protein([("P1", 3, bad)])


def test_a_set_names_its_proteins_one_way() -> None:
    declared = SimpleNamespace(query_set_id=uuid.uuid4())
    assert population_spelling(["B", "A"], declared) == {  # type: ignore[arg-type]
        "query_set_id": declared.query_set_id,
        "population": None,
    }
    assert population_spelling(["B", "A"], None) == {
        "query_set_id": None,
        "population": population_of(["A", "B"]),
    }


#: Every meta key the three native writers of the ladder emit.
WRITERS_META = {
    "run_term_frequency": {"evaluation_set_id", "evidence_artifact", "written_counts"},
    "run_homology_transfer": {
        "evaluation_set_id",
        "donor_policy",
        "exclude_self_neighbour",
        "self_exclusion",
        "targets_with_a_term",
        "targets_without_a_sequence",
        "evidence_artifact",
        "written_counts",
    },
    "run_own_annotations": {"evaluation_set_id", "targets_with_a_term", "evidence_artifact"},
    "every native set": {
        "algorithm",
        "method",
        "code_revision",
        "dependency_revisions",
        "query_population",
    },
}


@pytest.mark.parametrize("writer", sorted(WRITERS_META))
def test_every_meta_key_a_writer_emits_is_classified(writer: str) -> None:
    classified = set(method_seal.METHOD_IDENTITY_FIELDS) | set(method_seal.EXECUTION_ONLY_FIELDS)
    assert WRITERS_META[writer] <= classified


def _set(method_id: uuid.UUID | None, limit: int, **meta: Any) -> dict[str, Any]:
    """A whole prediction set row, as the seal reads it."""
    return {
        "id": uuid.uuid4(),
        "created_at": None,
        "description": None,
        "job_id": None,
        "producer_git_sha": None,
        "summary": None,
        "annotation_set_id": "corpus",
        "distance_threshold": None,
        "embedding_config_id": None if method_id else "config",
        "limit_per_entry": limit,
        "method_id": method_id,
        "ontology_snapshot_id": "snapshot",
        "query_set_id": "targets",
        "meta": {"code_revision": "r", "dependency_revisions": {"protea-x": "c"}, **meta},
    }


def test_limit_per_entry_is_execution_on_a_method_set_and_identity_elsewhere() -> None:
    method = uuid.uuid4()
    native = (_set(method, 12), _set(method, 40))
    assert method_identity(native[0])["limit_per_entry"] is None
    assert assert_one_axis([], *native) == ()
    with pytest.raises(CrossedMethodAxes, match="do not differ"):
        assert_one_axis(["limit_per_entry"], *native)
    knn = (_set(None, 30), _set(None, 5))
    with pytest.raises(CrossedMethodAxes, match="limit_per_entry"):
        assert_one_axis([], *knn)
    # A native set against a retrieval: the depth is there on one side only.
    crossed = assert_one_axis(
        ["embedding_config_id", "limit_per_entry", "method_id"], native[0], knn[0]
    )
    assert crossed == ("embedding_config_id", "limit_per_entry", "method_id")


# Against Postgres --------------------------------------------------------------------


@pytest.fixture()
def pg(postgres_url: str) -> Iterator[Session]:
    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    Base.metadata.drop_all(engine)
    engine.dispose()


def _method_set(pg: Session, eset: EvaluationSet) -> PredictionSet:
    from protea.core.contracts.method import Family, InstanceCut, MethodSpec

    spec = MethodSpec(
        family=Family.EXPLAINABLE_UNITS,
        code="native-writer-test",
        instance=InstanceCut(211, eset.pivot_snapshot_id),
        recipe={},
        pins=(),
        runtime="native",
        explains=False,
    )
    pg.add(Method.from_spec(spec))
    pg.flush()
    return new_prediction_set(
        pg,
        pg.get(Method, spec.method_id),
        SetContext(
            annotation_set_id=eset.old_annotation_set_id,
            ontology_snapshot_id=eset.pivot_snapshot_id,
            limit_per_entry=2,
            description="A native set.",
        ),
    )


@pytest.mark.integration
def test_compact_scores_go_in_one_row_per_protein_or_not_at_all(pg: Session) -> None:
    eset = pg.get(EvaluationSet, make_window(pg, snapshot_version="native-writers"))
    terms = [
        GOTerm(go_id=f"GO:000000{i}", aspect="P", ontology_snapshot_id=eset.pivot_snapshot_id)
        for i in (1, 2)
    ]
    pg.add_all(terms)
    pg.flush()
    pset = _method_set(pg, eset)
    with pytest.raises(NativeWriteRefused, match="999999"):
        copy_scores(pg, pset, [("P1", terms[0].id, 0.5), ("P1", 999999, 0.5)])
    assert pg.execute(select(PredictionScore)).first() is None
    written = copy_scores(
        pg, pset, [("P1", terms[1].id, 0.2), ("P1", terms[0].id, 0.9), ("P2", terms[0].id, 1.0)]
    )
    rows = {
        r.protein_accession: (r.go_term_ids, r.scores)
        for r in pg.execute(select(PredictionScore)).scalars()
    }
    assert written == 3
    assert rows == {"P1": ([terms[0].id, terms[1].id], [0.9, 0.2]), "P2": ([terms[0].id], [1.0])}
    with pytest.raises(NativeWriteRefused, match="once"):
        new_prediction_set(
            pg,
            pset.method,
            SetContext(
                annotation_set_id=eset.old_annotation_set_id,
                ontology_snapshot_id=eset.pivot_snapshot_id,
                limit_per_entry=1,
                description="Both spellings.",
                population=population_of(["P1"]),
                query_set_id=uuid.uuid4(),
            ),
        )


def _targets_set(
    pg: Session, eset: EvaluationSet, kind: str, accessions: list[str], **over: Any
) -> QuerySet:
    sequence = Sequence(sequence="MKV", sequence_hash=Sequence.compute_hash("MKV"))
    existing = pg.execute(
        select(Sequence).where(Sequence.sequence_hash == sequence.sequence_hash)
    ).scalar_one_or_none()
    if existing is None:
        pg.add(sequence)
        pg.flush()
    else:
        sequence = existing
    row = QuerySet(
        name=f"{kind}-{uuid.uuid4().hex[:6]}",
        source_kind=kind,
        evaluation_set_id=eset.id,
        artifact_uri="file:///targets",
        content_sha256=over.get("sha256", population_of(accessions)["sha256"]),
        description="Targets.",
        job_id=make_job(pg, "export_evaluation_targets"),
    )
    pg.add(row)
    pg.flush()
    pg.add_all(
        QuerySetEntry(query_set_id=row.id, accession=a, sequence_id=sequence.id) for a in accessions
    )
    pg.flush()
    return row


@pytest.mark.integration
def test_a_windows_declared_targets_are_read_without_its_truth(pg: Session) -> None:
    eset = pg.get(EvaluationSet, make_window(pg, snapshot_version="declared-targets"))
    assert declared_targets(pg, eset) is None
    full = _targets_set(pg, eset, "evaluation_targets", ["P2", "P1"], sha256="f" * 64)
    found = declared_targets(pg, eset)
    assert found is not None and found.accessions == ("P1", "P2") and found.query_set_id == full.id
    _targets_set(pg, eset, "evaluation_targets", ["P3"], sha256="e" * 64)
    with pytest.raises(NativeWriteRefused, match="2 evaluation_targets sets"):
        declared_targets(pg, eset)
    parent = pg.get(EvaluationSet, make_window(pg, snapshot_version="declared-sample"))
    sample = _targets_set(pg, parent, "window_sample", ["P9", "P8"])
    eset.target_query_set_id = sample.id
    pg.flush()
    found = declared_targets(pg, eset)
    assert found is not None and found.source_kind == "window_sample"
    assert found.accessions == ("P8", "P9")
    forged = _targets_set(pg, parent, "window_sample", ["P7"], sha256="a" * 64)
    eset.target_query_set_id = forged.id
    pg.flush()
    with pytest.raises(NativeWriteRefused, match="do not hash"):
        declared_targets(pg, eset)
