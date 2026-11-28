"""A window kept to a declared sample of its targets: its parent's truth, filtered.

Without a database: the payload's field, the identity's eleven columns in the
constraint's order, the description and the reuse event naming the sample, the
migration against the model (recorder), the filter of a truth parquet against
:func:`protea.core._window_population.restricted_to` in every bucket, the stats a
restricted window inherits and the record it adds, and the refusal of another
identity.

With ``--with-postgres``: a parent of LAFA's frame, placed and sampled by
``declare_window_sample``, gives a window whose truth is the parent's filtered in
every bucket (``pk_known`` and ``known`` included) and whose stats resolve the
parent's pivot, pass the terms-of-interest check, keep the mode and the frame inputs
and record the restriction; the targets readers read are the sample; asking again
serves it and the unrestricted request serves the parent; the constraint keeps a
restricted window apart from its parent and two on one sample together; every
refusal comes before anything is written; and the revision upgrades, downgrades and
refuses to downgrade while a restricted window exists.
"""

from __future__ import annotations

import hashlib
import importlib.util
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa
from pydantic import ValidationError
from sqlalchemy import create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

import protea.infrastructure.orm.models  # noqa: F401  (registers every table)
from protea.core._artifact_identity import ArtifactMismatch
from protea.core._window_population import restricted_to
from protea.core.evaluation import (
    EvaluationData,
    load_evaluation_data_for_set,
    serialize_evaluation_data_to_parquet,
)
from protea.core.operations import declare_window_sample
from protea.core.operations import generate_evaluation_set as generate
from protea.core.operations._run_cafa_setup import window_terms_of_interest
from protea.core.operations._window_on_sample import (
    BUCKETS,
    FRAME_INPUTS,
    RestrictedTruth,
    TargetSetRefused,
    refuse_another_identity,
    restricted_groundtruth,
    restricted_stats,
)
from protea.core.operations.generate_evaluation_set import (
    GenerateEvaluationSetOperation,
    GenerateEvaluationSetPayload,
    _Ask,
    _Identity,
    _identity_fields,
    window_description,
)
from protea.core.operations.place_window_targets import window_targets
from protea.core.query_population import population_of
from protea.core.window_declarations import LAFA_FRAME
from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.go_term_alias import GOTermAlias
from protea.infrastructure.orm.models.query.query_set import QuerySet, QuerySetEntry
from protea.infrastructure.storage.local import LocalFsArtifactStore
from tests.helpers.cut_photos import make_job, make_photo, make_placed_window, member
from tests.test_method_tables import alembic_config, fresh_database  # noqa: F401
from tests.test_window_samples import ASPECTS, _truth

_REPO = Path(__file__).resolve().parents[1]
_MIGRATION = (
    _REPO / "alembic/versions/532d2c1db500_a_window_can_score_a_declared_sample_of_its_targets.py"
)
_spec = importlib.util.spec_from_file_location("migration_window_target_set", _MIGRATION)
assert _spec and _spec.loader
migration = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(migration)

#: The store the door to a window's truth reads, patched by its dotted name.
_TRUTH_STORE = "protea.core._evaluation_groundtruth._store"


def _identity_constraint() -> sa.UniqueConstraint:
    return next(
        c
        for c in EvaluationSet.__table__.constraints
        if getattr(c, "name", "") == "uq_evaluation_set_identity"
    )


# The payload, the identity and the migration ----------------------------------------


class TestThePayloadAndTheIdentity:
    def test_the_field_is_an_optional_query_set_id(self) -> None:
        base = {"old_annotation_set_id": "a", "new_annotation_set_id": "b"}
        assert GenerateEvaluationSetPayload.model_validate(base).target_query_set_id is None
        sample = uuid.uuid4()
        p = GenerateEvaluationSetPayload.model_validate(
            {**base, "target_query_set_id": str(sample).upper()}
        )
        assert p.target_query_set_id == str(sample)
        with pytest.raises(ValidationError):
            GenerateEvaluationSetPayload.model_validate({**base, "target_query_set_id": "toy"})

    def test_the_identity_has_the_constraints_columns_in_its_order(self) -> None:
        ident = _Identity(*(uuid.uuid4() for _ in range(5)))
        assert list(ident.as_columns()) == [c.name for c in _identity_constraint().columns]
        assert ident.as_columns()["target_query_set_id"] is None
        assert _identity_constraint().dialect_options["postgresql"]["nulls_not_distinct"]

    def test_the_description_and_the_reuse_event_name_the_sample(self) -> None:
        sample = uuid.uuid4()
        ident = _Identity(*(uuid.uuid4() for _ in range(5)), target_query_set_id=sample)
        ends = (
            type("Corpus", (), {"source": "goa", "source_version": "194"})(),
            type("Corpus", (), {"source": "goa", "source_version": "211"})(),
        )
        text = window_description(ends, ident, _Ask("training", LAFA_FRAME, "lafa-frame"))  # type: ignore[arg-type]
        assert f"kept to the declared sample {sample}" in text
        assert _identity_fields(uuid.uuid4(), ident)["target_query_set_id"] == str(sample)
        whole = _Identity(*(uuid.uuid4() for _ in range(5)))
        assert "declared sample" not in window_description(ends, whole, _Ask(None, LAFA_FRAME))  # type: ignore[arg-type]


class _Recorder:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    def add_column(self, table: str, column: sa.Column) -> None:
        self.calls.append(("add_column", column.name, {}))

    def drop_column(self, table: str, name: str) -> None:
        self.calls.append(("drop_column", name, {}))

    def create_foreign_key(self, name: str, *args: Any, **kw: Any) -> None:
        self.calls.append(("create_foreign_key", name, {"args": args, **kw}))

    def drop_constraint(self, name: str, table: str, type_: str) -> None:
        self.calls.append(("drop_constraint", name, {"type_": type_}))

    def create_unique_constraint(
        self, name: str, table: str, columns: list[str], **kw: Any
    ) -> None:
        self.calls.append(("create_unique_constraint", name, {"columns": columns, **kw}))

    def get_bind(self) -> Any:
        class _Bind:
            def execute(self, *_a: Any) -> Any:
                return type("R", (), {"scalar_one": staticmethod(lambda: 0)})()

        return _Bind()


class TestTheMigration:
    def test_it_follows_the_head_it_was_written_on(self) -> None:
        assert migration.revision == "532d2c1db500"
        assert migration.down_revision == "a0cc2a412ec0"

    def test_it_makes_the_models_identity(self, monkeypatch: pytest.MonkeyPatch) -> None:
        recorder = _Recorder()
        monkeypatch.setattr(migration, "op", recorder)
        migration.upgrade()
        (unique,) = [c for c in recorder.calls if c[0] == "create_unique_constraint"]
        assert unique[2]["columns"] == [c.name for c in _identity_constraint().columns]
        assert unique[2]["postgresql_nulls_not_distinct"] is True
        (fk,) = [c for c in recorder.calls if c[0] == "create_foreign_key"]
        assert fk[1] == "fk_evaluation_set_target_query_set_id"
        assert fk[2]["args"] == ("evaluation_set", "query_set", ["target_query_set_id"], ["id"])
        assert fk[2]["ondelete"] == "RESTRICT"
        column = EvaluationSet.__table__.c.target_query_set_id
        (model_fk,) = column.foreign_keys
        assert column.nullable and (model_fk.name, model_fk.ondelete, model_fk.use_alter) == (
            "fk_evaluation_set_target_query_set_id",
            "RESTRICT",
            True,
        )

    def test_the_downgrade_lands_on_the_ten_columns(self, monkeypatch: pytest.MonkeyPatch) -> None:
        recorder = _Recorder()
        monkeypatch.setattr(migration, "op", recorder)
        migration.downgrade()
        (unique,) = [c for c in recorder.calls if c[0] == "create_unique_constraint"]
        assert unique[2] == {"columns": list(migration.PREVIOUS_IDENTITY)}
        assert ("drop_column", "target_query_set_id", {}) in recorder.calls
        assert migration.IDENTITY == (*migration.PREVIOUS_IDENTITY, "target_query_set_id")


# The truth kept to a sample, and its stats ------------------------------------------


def _parent_truth() -> EvaluationData:
    return EvaluationData(
        nk={"A": {"GO:1", "GO:2"}, "B": {"GO:1"}},
        lk={"C": {"GO:3"}, "D": {"GO:3"}},
        pk={"C": {"GO:4"}, "E": {"GO:4", "GO:5"}},
        known={"C": {"GO:6", "GO:7"}, "E": {"GO:6"}, "F": {"GO:6"}},
        pk_known={"C": {"GO:6"}, "E": {"GO:6"}},
        removed={"E": {"GO:8"}, "G": {"GO:8"}},
    )


def _buckets(data: EvaluationData) -> dict[str, dict[str, set[str]]]:
    return {bucket: getattr(data, bucket) for bucket in BUCKETS}


class TestTheKeptTruth:
    def test_it_is_restricted_to_in_every_bucket(self, tmp_path: Path) -> None:
        parent = _parent_truth()
        path = serialize_evaluation_data_to_parquet(parent, tmp_path / "parent.parquet")
        sample = frozenset({"A", "C", "E"})
        kept = restricted_groundtruth(path, sample)
        reference, _ = restricted_to(parent, sample)
        assert _buckets(kept.data) == _buckets(reference)
        assert kept.kept == {"nk": 1, "lk": 1, "pk": 2, "known": 2, "pk_known": 2, "removed": 1}
        assert kept.outside == {
            "nk": 1,
            "lk": 1,
            "pk": 0,
            "known": 1,
            "pk_known": 0,
            "removed": 1,
        }

    def test_the_stats_inherit_the_frame_count_the_kept_truth_and_record_the_rest(
        self, tmp_path: Path
    ) -> None:
        parent_truth = _parent_truth()
        path = serialize_evaluation_data_to_parquet(parent_truth, tmp_path / "parent.parquet")
        kept = restricted_groundtruth(path, frozenset({"A", "C"}))
        parent = EvaluationSet(
            id=uuid.uuid4(),
            groundtruth_sha256="9" * 64,
            stats={
                **parent_truth.stats(),
                "mode": "lafa",
                "terms_of_interest": 39028,
                "lafa_t1_pairs_after_trim": 5432,
                "population_proteins": 559820,
            },
        )
        sample = QuerySet(id=uuid.uuid4(), content_sha256="8" * 64)
        stats = restricted_stats(parent, kept, sample)
        assert stats["mode"] == "lafa" and stats["terms_of_interest"] == 39028
        assert {key: stats[key] for key in kept.data.stats()} == kept.data.stats()
        assert stats["delta_proteins"] == 2 != parent_truth.stats()["delta_proteins"]
        record = stats["target_set_restriction"]
        assert record["parent_evaluation_set_id"] == str(parent.id)
        assert record["parent_groundtruth_sha256"] == "9" * 64
        assert (record["query_set_id"], record["query_set_sha256"]) == (str(sample.id), "8" * 64)
        assert (record["kept"], record["outside"]) == (kept.kept, kept.outside)
        assert record["copied_keys"] == ["lafa_t1_pairs_after_trim", "population_proteins"]
        assert not set(record["copied_keys"]) & FRAME_INPUTS

    def test_another_identity_or_other_applied_rules_are_refused(self) -> None:
        parent = EvaluationSet(id=uuid.uuid4(), not_codes="experimental", truth_rules={"a": "b"})
        with pytest.raises(TargetSetRefused, match="not_codes"):
            refuse_another_identity(parent, {"not_codes": "any"}, {"a": "b"})
        with pytest.raises(TargetSetRefused, match="rules nobody applied"):
            refuse_another_identity(parent, {"not_codes": "experimental"}, {"a": "c"})
        refuse_another_identity(parent, {"not_codes": "experimental"}, {"a": "b"})

    def test_an_empty_record_is_still_a_record(self) -> None:
        empty = RestrictedTruth(EvaluationData(), dict.fromkeys(BUCKETS, 0), {})
        parent = EvaluationSet(id=uuid.uuid4(), stats=None)
        sample = QuerySet(id=uuid.uuid4(), content_sha256="7" * 64)
        assert (
            restricted_stats(parent, empty, sample)["target_set_restriction"]["copied_keys"] == []
        )


# On Postgres ----------------------------------------------------------------------


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
def store(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> LocalFsArtifactStore:
    """One local store for the truths every window reads and writes, and the samples."""
    local = LocalFsArtifactStore(tmp_path / "store")
    monkeypatch.setattr(_TRUTH_STORE, lambda: local)
    monkeypatch.setattr(generate, "get_artifact_store", lambda _settings: local)
    monkeypatch.setattr(generate, "load_settings", lambda _root: None)
    monkeypatch.setattr(declare_window_sample, "publish", lambda key, blob: local.put(key, blob))
    monkeypatch.setattr(declare_window_sample, "_scratch_root", lambda: tmp_path)
    return local


def _parent(session: Session, tmp_path: Path) -> tuple[EvaluationSet, EvaluationData]:
    truth, placed = _truth(80)
    members = [
        member(accession, "ts", residues="MK" + "A" * (i % 9)) for i, accession in enumerate(placed)
    ]
    photo = make_photo(session, [*members, member("K0001", "t"), member("R0001", "s")])
    window = make_placed_window(session, photo, truth, ASPECTS, placed, tmp_path / "store")
    session.commit()
    return window, truth


def _sample(session: Session, window: EvaluationSet, units_per_cell: int = 4) -> QuerySet:
    payload = {
        "evaluation_set_id": str(window.id),
        "units_per_cell": units_per_cell,
        "seed": "ladder-toy-1",
        "rule": "equal_cells_proportional_strata",
        "output_name": "ladder-toy-targets",
        "_job_id": str(make_job(session, "declare_window_sample")),
    }
    result = declare_window_sample.DeclareWindowSampleOperation().execute(
        session, payload, emit=lambda *a, **k: None
    )
    session.commit()
    row = session.get(QuerySet, uuid.UUID(result.result["query_set_id"]))
    assert row is not None
    return row


def _request(window: EvaluationSet, sample: QuerySet | None, **over: Any) -> dict[str, Any]:
    """The payload of the design's sketch: the parent's five ids and rule set, and the sample."""
    payload = {
        "old_annotation_set_id": str(window.old_annotation_set_id),
        "new_annotation_set_id": str(window.new_annotation_set_id),
        "pivot_ontology_snapshot_id": str(window.pivot_snapshot_id),
        "old_native_snapshot_id": str(window.old_native_snapshot_id),
        "new_native_snapshot_id": str(window.new_native_snapshot_id),
        "truth_rule_set": "lafa-frame",
        "window_role": "training",
        **({"target_query_set_id": str(sample.id)} if sample is not None else {}),
        **over,
    }
    return {key: value for key, value in payload.items() if value is not None}


def _generate(session: Session, payload: dict[str, Any]) -> dict[str, Any]:
    result = GenerateEvaluationSetOperation().execute(session, payload, emit=lambda *a, **k: None)
    session.commit()
    return result.result


def _entries(session: Session, sample: QuerySet) -> frozenset[str]:
    return frozenset(
        session.execute(
            select(QuerySetEntry.accession).where(QuerySetEntry.query_set_id == sample.id)
        ).scalars()
    )


@pytest.mark.integration
def test_a_window_on_a_sample_is_its_parents_truth_filtered(
    pg: Session, tmp_path: Path, store: LocalFsArtifactStore
) -> None:
    parent, parent_truth = _parent(pg, tmp_path)
    sample = _sample(pg, parent)
    result = _generate(pg, _request(parent, sample))

    toy = pg.get(EvaluationSet, uuid.UUID(result["evaluation_set_id"]))
    assert toy is not None and toy.id != parent.id
    columns = [c.name for c in _identity_constraint().columns][:-1]
    assert {c: getattr(toy, c) for c in columns} == {c: getattr(parent, c) for c in columns}
    assert (toy.target_query_set_id, toy.window_role) == (sample.id, "training")
    assert toy.truth_rules == parent.truth_rules

    accessions = _entries(pg, sample)
    reference, _ = restricted_to(parent_truth, accessions)
    data, pivot = load_evaluation_data_for_set(pg, toy)
    assert _buckets(data) == _buckets(reference)
    assert data.pk_known and data.known
    assert pivot == parent.pivot_snapshot_id
    blob = store.get(toy.groundtruth_uri)
    assert hashlib.sha256(blob).hexdigest() == toy.groundtruth_sha256 != parent.groundtruth_sha256

    rule, terms = window_terms_of_interest(pg, toy, pivot)
    assert (rule, terms) == window_terms_of_interest(pg, parent, pivot)
    assert len(terms) == toy.stats["terms_of_interest"] == parent.stats["terms_of_interest"]
    for key in FRAME_INPUTS:
        assert toy.stats[key] == parent.stats[key], key
    assert toy.stats["delta_proteins"] == len(accessions) < parent.stats["delta_proteins"]
    record = toy.stats["target_set_restriction"]
    assert record["parent_evaluation_set_id"] == str(parent.id)
    assert record["parent_groundtruth_sha256"] == parent.groundtruth_sha256
    assert record["kept"]["nk"] + record["outside"]["nk"] == len(parent_truth.nk)
    assert record["kept"]["known"] + record["outside"]["known"] == len(parent_truth.known)
    assert "population_proteins" in record["copied_keys"]
    assert "lafa_t1_pairs_after_trim" in record["copied_keys"]
    assert f"kept to the declared sample {sample.id}" in (toy.description or "")


@pytest.mark.integration
def test_the_readers_of_a_windows_targets_read_the_sample(
    pg: Session, tmp_path: Path, store: LocalFsArtifactStore
) -> None:
    parent, _ = _parent(pg, tmp_path)
    sample = _sample(pg, parent)
    toy = pg.get(
        EvaluationSet, uuid.UUID(_generate(pg, _request(parent, sample))["evaluation_set_id"])
    )
    assert toy is not None
    assert window_targets(pg, toy) == set(_entries(pg, sample))
    from protea.core.operations.run_own_annotations import read_targets

    # The native writer's reader resolves alternative ids at t0: give the graph one.
    pg.add(
        GOTermAlias(
            ontology_snapshot_id=toy.old_native_snapshot_id,
            alias_go_id="GO:0009999",
            primary_go_id="GO:0000001",
            kind="alt_id",
        )
    )
    pg.flush()
    targets, _rows = read_targets(pg, toy)
    assert targets == sorted(_entries(pg, sample))


@pytest.mark.integration
def test_asking_again_serves_it_and_the_whole_population_is_the_parent(
    pg: Session, tmp_path: Path, store: LocalFsArtifactStore
) -> None:
    parent, _ = _parent(pg, tmp_path)
    sample = _sample(pg, parent)
    first = _generate(pg, _request(parent, sample))
    again = _generate(pg, _request(parent, sample))
    assert again["evaluation_set_id"] == first["evaluation_set_id"]
    whole = _generate(pg, _request(parent, None))
    assert whole["evaluation_set_id"] == str(parent.id)
    assert pg.execute(select(sa.func.count(EvaluationSet.id))).scalar_one() == 2


def _copy(window: EvaluationSet, **over: Any) -> EvaluationSet:
    columns = [c.name for c in _identity_constraint().columns]
    return EvaluationSet(**{c: getattr(window, c) for c in columns}, stats={}, **over)


@pytest.mark.integration
def test_the_constraint_keeps_a_sample_apart_from_its_window(
    pg: Session, tmp_path: Path, store: LocalFsArtifactStore
) -> None:
    parent, _ = _parent(pg, tmp_path)
    sample = _sample(pg, parent)
    pg.add(_copy(parent))
    with pytest.raises(IntegrityError, match="uq_evaluation_set_identity"):
        pg.flush()
    pg.rollback()
    restricted = _copy(parent)
    restricted.target_query_set_id = sample.id
    pg.add(restricted)
    pg.flush()
    twin = _copy(parent)
    twin.target_query_set_id = sample.id
    pg.add(twin)
    with pytest.raises(IntegrityError, match="uq_evaluation_set_identity"):
        pg.flush()


def _forged_sample(
    session: Session, window: EvaluationSet, accessions: list[str], sha: str
) -> QuerySet:
    """A window sample written by hand, so it can hold what the operation would refuse."""
    job = make_job(session, "declare_window_sample")
    row = QuerySet(
        name="forged",
        description="a forged sample",
        source_kind="window_sample",
        evaluation_set_id=window.id,
        artifact_uri="memory://forged",
        content_sha256=sha,
        job_id=job,
    )
    session.add(row)
    session.flush()
    sequence = session.execute(select(QuerySetEntry.sequence_id).limit(1)).scalar_one()
    session.add_all(
        QuerySetEntry(query_set_id=row.id, sequence_id=sequence, accession=a) for a in accessions
    )
    session.commit()
    return row


@pytest.mark.integration
def test_every_refusal_comes_before_anything_is_written(
    pg: Session, tmp_path: Path, store: LocalFsArtifactStore
) -> None:
    parent, _ = _parent(pg, tmp_path)
    sample = _sample(pg, parent)
    windows = pg.execute(select(sa.func.count(EvaluationSet.id))).scalar_one()

    targets = sorted(_entries(pg, sample))[:3]
    stranger = _forged_sample(
        pg, parent, [*targets, "K0001"], population_of([*targets, "K0001"])["sha256"]
    )
    with pytest.raises(TargetSetRefused, match="1 proteins of sample .* are not targets"):
        _generate(pg, _request(parent, stranger))
    pg.rollback()
    unsealed = _forged_sample(pg, parent, targets, "0" * 64)
    with pytest.raises(TargetSetRefused, match="do not hash to its sha256"):
        _generate(pg, _request(parent, unsealed))
    pg.rollback()
    with pytest.raises(TargetSetRefused, match="no query set"):
        _generate(pg, {**_request(parent, sample), "target_query_set_id": str(uuid.uuid4())})
    pg.rollback()
    other_kind = pg.get(QuerySet, stranger.id)
    assert other_kind is not None
    other_kind.source_kind = "fasta_upload"
    other_kind.evaluation_set_id = None
    pg.commit()
    with pytest.raises(TargetSetRefused, match="not a window_sample"):
        _generate(pg, _request(parent, other_kind))
    pg.rollback()
    loose = {
        **_request(parent, sample, truth_rule_set=None),
        "truth_profile": "own_graph",
        "toi_rule": "both_ends_trimmed",
        "not_codes": "any",
        "binding_rule": "cafa",
        "target_population": "swissprot_same_sequence",
    }
    with pytest.raises(TargetSetRefused, match="not_codes"):
        _generate(pg, loose)
    pg.rollback()
    parent.groundtruth_sha256 = "1" * 64
    pg.commit()
    with pytest.raises(ArtifactMismatch, match="does not match"):
        _generate(pg, _request(parent, sample))
    pg.rollback()
    assert pg.execute(select(sa.func.count(EvaluationSet.id))).scalar_one() == windows


@pytest.mark.integration
def test_the_revision_upgrades_and_downgrades_against_postgres(
    alembic_config: Any,  # noqa: F811
    fresh_database: str,  # noqa: F811
) -> None:
    from alembic import command

    engine = create_engine(fresh_database)
    definition = sa.text(
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'uq_evaluation_set_identity'"
    )
    try:
        command.upgrade(alembic_config, migration.revision)
        with engine.connect() as conn:
            installed = conn.execute(definition).scalar_one()
        assert (
            installed.startswith("UNIQUE NULLS NOT DISTINCT (")
            and "target_query_set_id" in installed
        )
        command.downgrade(alembic_config, migration.down_revision)
        with engine.connect() as conn:
            previous = conn.execute(definition).scalar_one()
        assert "NULLS NOT DISTINCT" not in previous and "target_query_set_id" not in previous
        assert "target_query_set_id" not in {
            c["name"] for c in sa.inspect(engine).get_columns("evaluation_set")
        }
        command.upgrade(alembic_config, migration.revision)
        with engine.begin() as conn:
            _a_restricted_window(conn)
        with pytest.raises(RuntimeError, match="score a declared sample"):
            command.downgrade(alembic_config, migration.down_revision)
    finally:
        engine.dispose()


def _a_restricted_window(conn: sa.Connection) -> None:
    """A window kept to a sample of its own, written in SQL at this revision."""
    snapshot, corpus, parent, toy, job, sample = (uuid.uuid4() for _ in range(6))
    conn.execute(
        sa.text(
            "INSERT INTO ontology_snapshot (id, obo_url, obo_version)"
            " VALUES (:id, 'file:///none', 'window-target-set')"
        ),
        {"id": snapshot},
    )
    conn.execute(
        sa.text(
            "INSERT INTO annotation_set (id, source, ontology_snapshot_id, meta)"
            " VALUES (:id, 'goa', :snapshot, CAST('{}' AS jsonb))"
        ),
        {"id": corpus, "snapshot": snapshot},
    )
    conn.execute(
        sa.text(
            "INSERT INTO evaluation_set (id, old_annotation_set_id, new_annotation_set_id,"
            " pivot_snapshot_id, old_native_snapshot_id, new_native_snapshot_id, stats)"
            " VALUES (:id, :corpus, :corpus, :s, :s, :s, CAST('{}' AS jsonb))"
        ),
        {"id": parent, "corpus": corpus, "s": snapshot},
    )
    conn.execute(
        sa.text(
            "INSERT INTO job (id, operation, queue_name, status, payload, meta)"
            " VALUES (:id, 'declare_window_sample', 'protea.jobs', 'SUCCEEDED',"
            " CAST('{}' AS jsonb), CAST('{}' AS jsonb))"
        ),
        {"id": job},
    )
    conn.execute(
        sa.text(
            "INSERT INTO query_set (id, name, description, source_kind, evaluation_set_id,"
            " artifact_uri, content_sha256, job_id) VALUES (:id, 'sample', 'a sample',"
            " 'window_sample', :window, 'memory://s', :sha, :job)"
        ),
        {"id": sample, "window": parent, "sha": "c" * 64, "job": job},
    )
    conn.execute(
        sa.text(
            "INSERT INTO evaluation_set (id, old_annotation_set_id, new_annotation_set_id,"
            " pivot_snapshot_id, old_native_snapshot_id, new_native_snapshot_id, stats,"
            " target_query_set_id) VALUES (:id, :corpus, :corpus, :s, :s, :s,"
            " CAST('{}' AS jsonb), :sample)"
        ),
        {"id": toy, "corpus": corpus, "s": snapshot, "sample": sample},
    )
