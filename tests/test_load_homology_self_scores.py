"""Loading a homology table's self scores: what is verified, what is written, what is refused.

Against Postgres (``--with-postgres``), on the small cut of
``tests/helpers/native_methods.py``: the scores of the table's queries are written as
the cluster stage writes them (a parquet and its manifest), loaded, stored and
declared in one ``homology_self_scores`` row derived from its table; the same
manifest again is a no-op, another one is refused, and every check refuses before
anything is written. The migration that adds the table upgrades and downgrades on a
database of its own, and its downgrade refuses while a row exists.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

import protea.infrastructure.orm.models  # noqa: F401  (registers every table)
from protea.core.operations import load_homology_self_scores as op
from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.annotation.homology_self_scores import HomologySelfScores
from tests.helpers.cut_photos import make_job
from tests.helpers.native_methods import (
    SELF_BITS,
    World,
    make_world,
    queries_fasta_sha256,
    self_scores_blob,
    table_queries,
)
from tests.test_method_tables import alembic_config, fresh_database  # noqa: F401

_REPO = Path(__file__).resolve().parents[1]
_MIGRATION = (
    _REPO / "alembic/versions/acfe472b787c_a_homology_table_records_its_queries_self_scores.py"
)
_spec = importlib.util.spec_from_file_location("migration_self_scores", _MIGRATION)
assert _spec and _spec.loader
migration = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(migration)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _scores_dir(root: Path, world: World, **over: Any) -> tuple[Path, str]:
    """One cut's folder as the cluster stage writes it, and its manifest's sha256."""
    folder = root / "self-scores" / "211"
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "self-scores.parquet").write_bytes(over.pop("blob", self_scores_blob()))
    manifest = {
        "table": "homology_self_scores",
        "cut_release": 211,
        "photo": {"cut_release": 211, "manifest_sha256": world.photo.manifest_sha256},
        "queries": table_queries(),
        "queries_fasta_sha256": queries_fasta_sha256(),
        "queries_without_self_score": table_queries() - len(SELF_BITS),
        "mmseqs": {
            "version": "8cc5ce367b5638c4306c2d7cfc652dd099a4643f",
            "binary_sha256": "b" * 64,
        },
        "command": [["mmseqs", "align", "union-db", "union-db", "pairs-db", "self-db"]],
        "outputs": {"self-scores.parquet": _sha256(folder / "self-scores.parquet")},
        "status": "complete",
        "revision": "builder-revision",
        **over,
    }
    (folder / "self-scores.json").write_text(json.dumps(manifest))
    return folder, _sha256(folder / "self-scores.json")


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
    monkeypatch.setattr(op, "get_artifact_store", lambda _settings: built.store)
    monkeypatch.setattr(op, "load_settings", lambda _root: None)
    return built


def _load(pg: Session, folder: Path, sha: str) -> dict[str, Any]:
    payload = {
        "scores_dir": str(folder),
        "manifest_sha256": sha,
        "_job_id": str(make_job(pg, "load_homology_self_scores")),
    }
    result = op.LoadHomologySelfScoresOperation().execute(pg, payload, emit=lambda *a, **k: None)
    pg.commit()
    return result.result


@pytest.mark.integration
def test_the_scores_of_the_tables_queries_load_once(
    pg: Session, world: World, tmp_path: Path
) -> None:
    folder, sha = _scores_dir(tmp_path, world)
    result = _load(pg, folder, sha)
    row = pg.execute(select(HomologySelfScores)).scalar_one()
    assert row.homology_table_id == world.table.id and row.manifest_sha256 == sha
    assert (row.queries, row.queries_without_self_score) == (table_queries(), table_queries() - 2)
    assert result["scored"] == 2 and row.short_name == "homology-self-scores-211"
    assert world.store.get(row.scores_uri) == self_scores_blob()
    assert row.scores_sha256 == hashlib.sha256(self_scores_blob()).hexdigest()
    assert row.builder_revision == "builder-revision" and row.job_id is not None
    assert "could not score" in row.description
    assert _load(pg, folder, sha)["skipped"] is True
    other, other_sha = _scores_dir(tmp_path / "other", world, revision="another-build")
    with pytest.raises(op.SelfScoresRefused, match="already has self scores"):
        _load(pg, other, other_sha)


@pytest.mark.parametrize(
    ("over", "match"),
    [
        ({"status": "measurement"}, "only a complete run loads"),
        ({"queries_fasta_sha256": "0" * 64}, "not this table's queries"),
        ({"queries_without_self_score": 0}, "the manifest counts"),
        ({"outputs": {"self-scores.parquet": "0" * 64}}, "does not hash"),
    ],
)
@pytest.mark.integration
def test_what_does_not_hold_together_is_refused_before_anything_is_written(
    pg: Session, world: World, tmp_path: Path, over: dict[str, Any], match: str
) -> None:
    folder, sha = _scores_dir(tmp_path, world, **over)
    with pytest.raises(op.SelfScoresRefused, match=match):
        _load(pg, folder, sha)
    pg.rollback()
    assert pg.execute(select(HomologySelfScores)).first() is None


@pytest.mark.integration
def test_a_manifest_that_is_not_the_one_named_and_a_strange_sequence_are_refused(
    pg: Session, world: World, tmp_path: Path
) -> None:
    folder, _ = _scores_dir(tmp_path, world)
    with pytest.raises(op.SelfScoresRefused, match="hashes to"):
        _load(pg, folder, "0" * 64)
    import io

    import pyarrow as pa
    import pyarrow.parquet as pq

    stranger = pa.table(
        {
            "query_md5": ["f" * 32, "e" * 32],
            "fident": pa.array([1.0, 1.0], pa.float32()),
            "alnlen": pa.array([10, 10], pa.int32()),
            "evalue": [1e-5, 1e-5],
            "bits": [30.0, 30.0],
        }
    )
    buffer = io.BytesIO()
    pq.write_table(stranger, buffer)
    folder, sha = _scores_dir(tmp_path / "strange", world, blob=buffer.getvalue())
    with pytest.raises(op.SelfScoresRefused, match="no superset sequence"):
        _load(pg, folder, sha)


class _Bind:
    def __init__(self, rows: int) -> None:
        self.rows = rows

    def execute(self, _statement: Any) -> Any:
        return type("Result", (), {"scalar_one": lambda _self: self.rows})()


class _Op:
    def __init__(self, rows: int) -> None:
        self.bind, self.dropped = _Bind(rows), []

    def get_bind(self) -> _Bind:
        return self.bind

    def drop_table(self, name: str) -> None:
        self.dropped.append(name)


def test_the_downgrade_refuses_while_a_row_exists(monkeypatch: pytest.MonkeyPatch) -> None:
    held = _Op(rows=1)
    monkeypatch.setattr(migration, "op", held)
    with pytest.raises(RuntimeError, match="pin them by sha256"):
        migration.downgrade()
    empty = _Op(rows=0)
    monkeypatch.setattr(migration, "op", empty)
    migration.downgrade()
    assert empty.dropped == ["homology_self_scores"]


def test_the_migration_holds_the_models_checks() -> None:
    from protea.infrastructure.orm.models.annotation import homology_self_scores as model

    assert migration.DIGESTS_CHECK == model.DIGESTS_CHECK
    assert migration.COUNTS_CHECK == model.COUNTS_CHECK
    assert migration.down_revision == "532d2c1db500"


@pytest.mark.integration
def test_the_revision_upgrades_and_downgrades_against_postgres(
    alembic_config: Any,  # noqa: F811
    fresh_database: str,  # noqa: F811
) -> None:
    from alembic import command

    engine = create_engine(fresh_database)
    try:
        command.upgrade(alembic_config, migration.revision)
        assert "homology_self_scores" in sa.inspect(engine).get_table_names()
        columns = {c["name"] for c in sa.inspect(engine).get_columns("homology_self_scores")}
        assert {
            "homology_table_id",
            "scores_uri",
            "scores_sha256",
            "queries_without_self_score",
        } <= columns
        command.downgrade(alembic_config, migration.down_revision)
        assert "homology_self_scores" not in sa.inspect(engine).get_table_names()
        command.upgrade(alembic_config, migration.revision)
        assert "homology_self_scores" in sa.inspect(engine).get_table_names()
    finally:
        engine.dispose()
