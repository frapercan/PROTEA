"""The space statistics operation: its payload, its table, its migration and a row it writes.

Without a database: the payload refuses what is not two ids, the table passes
the sets gate, and the migration builds exactly what the model describes (run
against a recorder, as ``tests/test_method_tables.py`` does).

With ``--with-postgres``: a photo and its bank are built and loaded the way
``tests/test_load_embedding_bank.py`` does it, the bank folder is named the way
the search's node names it, and the operation writes one row whose artifact
holds the statistics of the photo's distinct member sequences, read back from
the shards; asking again is a no-op, and every input it cannot trust is a
refusal before a shard is read. The revision also upgrades and downgrades on a
database of its own.
"""

from __future__ import annotations

import hashlib
import importlib.util
import re
import uuid
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow.parquet as pq
import pytest
import sqlalchemy as sa
from pydantic import ValidationError
from sqlalchemy import CheckConstraint, UniqueConstraint, create_engine, select
from sqlalchemy.orm import Session

import protea.infrastructure.orm.models  # noqa: F401  (registers every table)
from protea.core import queues
from protea.core.embedding_space.statistics import read_statistics_artifact
from protea.core.operation_catalog import build_operation_registry
from protea.core.operations import compute_embedding_space_statistics as operation
from protea.core.operations.compute_embedding_space_statistics import (
    ComputeEmbeddingSpaceStatisticsOperation,
    ComputeEmbeddingSpaceStatisticsPayload,
    EmbeddingSpaceStatisticsRefused,
    statistics_key_for,
)
from protea.core.operations.predict_go_terms._bank_shards import BANK_DIR_ENV
from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.annotation.cut_photo import CutPhoto, CutPhotoMember
from protea.infrastructure.orm.models.embedding.embedding_bank import EmbeddingBank
from protea.infrastructure.orm.models.embedding.embedding_space_statistics import (
    EmbeddingSpaceStatistics,
)
from tests.test_embedding_bank_build import CONFIG_ID
from tests.test_load_cut_photo import pg_session  # noqa: F401
from tests.test_load_embedding_bank import (  # noqa: F401
    _loaded_photo_and_bank,
    _run,
    backend,
    db,
)
from tests.test_method_tables import alembic_config, fresh_database  # noqa: F401

_REPO = Path(__file__).resolve().parents[1]
_MIGRATION = _REPO / "alembic/versions/f2e0fc2717f7_a_bank_records_the_statistics_of_its_space.py"
_spec = importlib.util.spec_from_file_location("migration_space_statistics", _MIGRATION)
assert _spec and _spec.loader
migration = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(migration)

TABLE = "embedding_space_statistics"
NAME = "compute_embedding_space_statistics"


# The payload and the routing -----------------------------------------------------


class TestThePayload:
    def test_two_ids_are_what_it_takes(self) -> None:
        config, photo = uuid.uuid4(), uuid.uuid4()
        p = ComputeEmbeddingSpaceStatisticsPayload.model_validate(
            {"embedding_config_id": str(config).upper(), "cut_photo_id": str(photo)}
        )
        assert (p.config_id, p.photo_id) == (config, photo)
        assert p.embedding_config_id == str(config)

    @pytest.mark.parametrize(
        ("payload", "field"),
        [
            (
                {"embedding_config_id": "not-a-uuid", "cut_photo_id": str(uuid.uuid4())},
                "embedding_config_id",
            ),
            ({"embedding_config_id": str(uuid.uuid4()), "cut_photo_id": "211"}, "cut_photo_id"),
            ({"embedding_config_id": str(uuid.uuid4())}, "cut_photo_id"),
        ],
    )
    def test_anything_else_is_refused(self, payload: dict[str, Any], field: str) -> None:
        with pytest.raises(ValidationError) as caught:
            ComputeEmbeddingSpaceStatisticsPayload.model_validate(payload)
        assert (field,) in [tuple(error["loc"]) for error in caught.value.errors()]

    def test_a_field_nobody_declared_is_refused(self) -> None:
        with pytest.raises(ValidationError):
            ComputeEmbeddingSpaceStatisticsPayload.model_validate(
                {
                    "embedding_config_id": str(uuid.uuid4()),
                    "cut_photo_id": str(uuid.uuid4()),
                    "seed": 3,
                }
            )

    def test_the_summary_names_both_ids(self) -> None:
        line = ComputeEmbeddingSpaceStatisticsOperation().summarize_payload(
            {"embedding_config_id": "a" * 36, "cut_photo_id": "b" * 36}
        )
        assert line == "space of aaaaaaaa at photo bbbbbbbb"
        assert ComputeEmbeddingSpaceStatisticsOperation().summarize_payload({}) == ""


def test_it_runs_as_a_job_of_the_training_queue() -> None:
    spec = build_operation_registry().spec(NAME)
    assert (spec.domain, spec.kind, spec.queue, spec.purity) == (
        "representation",
        "job",
        queues.TRAINING,
        "database",
    )
    assert spec.publishes_to == () and spec.family is None


def test_the_artifact_has_one_spelling() -> None:
    config, photo = uuid.uuid4(), uuid.uuid4()
    key = statistics_key_for(config, photo, "f" * 64)
    assert key == f"embedding-space-statistics/{config}/{photo}/{'f' * 64}.npz"


# The table: the sets gate and the migration ----------------------------------------


def _guard() -> Any:
    spec = importlib.util.spec_from_file_location("_check_sets", _REPO / "scripts/check_sets.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_table_is_a_declared_described_verifiable_set() -> None:
    guard = _guard()
    assert TABLE in guard.EXPECTED
    verdict = guard._audit(Base.metadata.tables[TABLE])
    assert {criterion: ok for criterion, (_, ok) in verdict.items()} == {
        "declarado": True,
        "descrito": True,
        "verificable": True,
        "producido_por": True,
    }
    assert verdict["declarado"][0] == "cut_photo_id, embedding_bank_id, embedding_config_id"
    assert verdict["verificable"][0] == "artifact_uri con su sha"


class _Recorder:
    """Stands in for ``alembic.op`` and keeps what each call would have built."""

    def __init__(self) -> None:
        self.tables: dict[str, sa.Table] = {}
        self.indexes: dict[str, tuple[str, tuple[str, ...]]] = {}
        self.dropped: list[tuple[str, str]] = []

    def create_table(self, name: str, *elements: Any, **_kw: Any) -> None:
        self.tables[name] = sa.Table(name, sa.MetaData(), *elements)

    def create_index(self, name: str, table: str, columns: list[str], **_kw: Any) -> None:
        self.indexes[name] = (table, tuple(columns))

    def drop_index(self, name: str, **_kw: Any) -> None:
        self.dropped.append(("index", name))

    def drop_table(self, name: str, **_kw: Any) -> None:
        self.dropped.append(("table", name))


def _squash(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip()


def _shape(table: sa.Table) -> dict[str, Any]:
    return {
        "columns": {
            c.name: (type(c.type).__name__, getattr(c.type, "length", None), c.nullable)
            for c in table.columns
        },
        "primary_key": [c.name for c in table.primary_key.columns],
        "foreign_keys": sorted(
            (fk.parent.name, fk.target_fullname, fk.ondelete) for fk in table.foreign_keys
        ),
        "unique": sorted(
            (c.name, tuple(col.name for col in c.columns))
            for c in table.constraints
            if isinstance(c, UniqueConstraint)
        ),
        "checks": {
            c.name: _squash(str(c.sqltext))
            for c in table.constraints
            if isinstance(c, CheckConstraint)
        },
    }


class TestTheMigrationBuildsWhatTheModelDescribes:
    def test_the_table(self, monkeypatch: pytest.MonkeyPatch) -> None:
        recorder = _Recorder()
        monkeypatch.setattr(migration, "op", recorder)
        migration.upgrade()
        assert _shape(recorder.tables[TABLE]) == _shape(Base.metadata.tables[TABLE])

    def test_the_indexes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        recorder = _Recorder()
        monkeypatch.setattr(migration, "op", recorder)
        migration.upgrade()
        modelled = {
            index.name: (TABLE, tuple(c.name for c in index.columns))
            for index in Base.metadata.tables[TABLE].indexes
        }
        assert recorder.indexes == modelled

    def test_the_downgrade_drops_what_the_upgrade_made(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        recorder = _Recorder()
        monkeypatch.setattr(migration, "op", recorder)
        migration.upgrade()
        migration.downgrade()
        assert sorted(recorder.dropped) == sorted(
            [("table", TABLE)] + [("index", name) for name in recorder.indexes]
        )

    def test_it_follows_the_head_it_was_written_on(self) -> None:
        assert migration.revision == "f2e0fc2717f7"
        assert migration.down_revision == "5d2e8c1b4a73"


# A row written on Postgres ---------------------------------------------------------


def _stored(monkeypatch: pytest.MonkeyPatch) -> dict[str, bytes]:
    """The store kept in memory: what the operation put, by key."""
    kept: dict[str, bytes] = {}

    def store(key: str, blob: bytes) -> str:
        kept[key] = blob
        return f"memory://{key}"

    monkeypatch.setattr(operation, "store_statistics", store)
    return kept


def _execute(session: Session, photo_id: uuid.UUID) -> dict[str, Any]:
    payload = {"embedding_config_id": CONFIG_ID, "cut_photo_id": str(photo_id)}
    result = ComputeEmbeddingSpaceStatisticsOperation().execute(
        session, payload, emit=lambda *a, **k: None
    )
    session.commit()
    return result.result


def _members_vectors(session: Session, bank_dir: Path, photo_id: uuid.UUID) -> np.ndarray:
    """The vectors of the photo's distinct member sequences, read from the shards directly."""
    md5s = set(
        session.execute(
            select(CutPhotoMember.sequence_md5)
            .where(CutPhotoMember.photo_id == photo_id)
            .where(CutPhotoMember.sequence_md5.is_not(None))
        ).scalars()
    )
    rows = []
    for path in sorted((bank_dir / "vectors" / CONFIG_ID).glob("shard-*.parquet")):
        table = pq.read_table(path)
        flat = table.column("vector").combine_chunks().flatten().to_numpy()
        vectors = flat.reshape(table.num_rows, -1)
        for md5, vector in zip(table.column("md5").to_pylist(), vectors, strict=True):
            if md5 in md5s:
                rows.append(vector.astype(np.float64))
    assert len(rows) == len(md5s)
    return np.array(rows)


@pytest.mark.integration
def test_a_loaded_bank_gets_the_statistics_of_its_photo(
    db: Session,  # noqa: F811
    tmp_path: Path,
    backend: Any,  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bank_dir, sha = _loaded_photo_and_bank(db, tmp_path)
    _run(db, bank_dir, sha)
    monkeypatch.setenv(BANK_DIR_ENV, str(bank_dir))
    kept = _stored(monkeypatch)
    photo = db.execute(select(CutPhoto)).scalar_one()
    result = _execute(db, photo.id)

    row = db.execute(select(EmbeddingSpaceStatistics)).scalar_one()
    bank = db.execute(select(EmbeddingBank)).scalar_one()
    rows = _members_vectors(db, bank_dir, photo.id)
    assert (row.embedding_config_id, row.cut_photo_id) == (uuid.UUID(CONFIG_ID), photo.id)
    assert row.embedding_bank_id == bank.id
    assert (row.n, row.d) == rows.shape == (result["n"], result["d"])
    blob = kept[statistics_key_for(row.embedding_config_id, photo.id, row.artifact_sha256)]
    assert hashlib.sha256(blob).hexdigest() == row.artifact_sha256
    assert row.artifact_uri.startswith("memory://embedding-space-statistics/")
    parameters, document = read_statistics_artifact(blob)
    assert np.allclose(parameters.mean, rows.mean(axis=0), atol=1e-12)
    assert np.allclose(parameters.variances, rows.var(axis=0), atol=1e-12)
    assert document["embedding_bank_id"] == str(bank.id)
    assert document["cut_release"] == photo.cut_release
    assert document["population"]["distinct_sequences"] == row.n
    assert result["n_below_storage"] == document["spectrum"]["n_below_storage"]
    assert row.description.isascii() and "Ledoit-Wolf shrinkage" in row.description
    assert row.code_revision

    again = _execute(db, photo.id)
    assert again["skipped"] is True and again["embedding_space_statistics_id"] == str(row.id)
    assert db.execute(select(sa.func.count(EmbeddingSpaceStatistics.id))).scalar_one() == 1


@pytest.mark.integration
def test_what_it_cannot_trust_is_refused_before_a_shard_is_read(
    db: Session,  # noqa: F811
    tmp_path: Path,
    backend: Any,  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bank_dir, sha = _loaded_photo_and_bank(db, tmp_path)
    photo = db.execute(select(CutPhoto)).scalar_one()
    _stored(monkeypatch)
    with pytest.raises(EmbeddingSpaceStatisticsRefused, match="has no loaded bank"):
        _execute(db, photo.id)
    db.rollback()
    _run(db, bank_dir, sha)
    monkeypatch.delenv(BANK_DIR_ENV, raising=False)
    with pytest.raises(EmbeddingSpaceStatisticsRefused, match="names no folder"):
        _execute(db, photo.id)
    db.rollback()
    monkeypatch.setenv(BANK_DIR_ENV, str(tmp_path / "elsewhere"))
    with pytest.raises(EmbeddingSpaceStatisticsRefused, match="not the one the store loaded"):
        _execute(db, photo.id)
    db.rollback()
    with pytest.raises(EmbeddingSpaceStatisticsRefused, match="no cut photo"):
        _execute(db, uuid.uuid4())
    db.rollback()
    assert db.execute(select(sa.func.count(EmbeddingSpaceStatistics.id))).scalar_one() == 0


@pytest.mark.integration
def test_the_revision_upgrades_and_downgrades_against_postgres(
    alembic_config: Any,  # noqa: F811
    fresh_database: str,  # noqa: F811
) -> None:
    from alembic import command

    engine = create_engine(fresh_database)
    try:
        command.upgrade(alembic_config, migration.revision)
        inspector = sa.inspect(engine)
        assert TABLE in inspector.get_table_names()
        assert {c["name"] for c in inspector.get_check_constraints(TABLE)} == {
            "ck_embedding_space_statistics_descriptions",
            "ck_embedding_space_statistics_digests",
            "ck_embedding_space_statistics_counts",
        }
        assert [u["name"] for u in inspector.get_unique_constraints(TABLE)] == [
            "uq_embedding_space_statistics_config_photo"
        ]
        command.downgrade(alembic_config, migration.down_revision)
        assert TABLE not in sa.inspect(engine).get_table_names()
        command.upgrade(alembic_config, migration.revision)
        assert TABLE in sa.inspect(engine).get_table_names()
    finally:
        engine.dispose()
