"""The migration that gives the sets ingested before it their producer's key.

Without a database: it follows the head it was written on. Against Postgres
(``--with-postgres``, CI): an ingested set's record moves from ``method`` to
``producer`` and its file digest gets a key of its own, a native method's set
keeps its ``method``, and the downgrade moves the record back.
"""

from __future__ import annotations

import importlib.util
import pathlib
from types import ModuleType

from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.orm import Session

from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from tests.test_a_set_is_summarised_once import _method_set, _parents
from tests.test_run_count_map import pg  # noqa: F401

_VERSIONS = pathlib.Path(__file__).resolve().parents[1] / "alembic" / "versions"
_MIGRATION = _VERSIONS / "3b9d4e2a7f15_an_ingested_set_names_its_producer.py"
RECORD = {"name": "own-annotations-ranked-194", "commit": "a" * 40}


def _module() -> ModuleType:
    """Loaded by path: ``alembic/`` is not a package and the library shadows the name."""
    spec = importlib.util.spec_from_file_location("_3b9d4e2a7f15", _MIGRATION)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_it_follows_the_head_it_was_written_on() -> None:
    module = _module()
    assert module.revision == "3b9d4e2a7f15"
    assert module.down_revision == "7c5e1f0a9d42"
    assert list(_VERSIONS.glob("7c5e1f0a9d42_*.py"))


def _migrate(pg: Session, step: str) -> None:  # noqa: F811
    with Operations.context(MigrationContext.configure(pg.connection())):
        getattr(_module(), step)()
    pg.expire_all()


def test_an_ingested_record_moves_to_its_key_and_back(pg: Session) -> None:  # noqa: F811
    parents, _ = _parents(pg)
    ingested = _method_set(pg, parents)
    ingested.meta = {
        "algorithm": "ingested_method",
        "method": RECORD,
        "source": {"sha256": "d" * 64, "rows": 3},
    }
    native = _method_set(pg, parents)
    native.meta = {"algorithm": "native_method", "method": "own-annotations-nonexp-lafa-211"}
    pg.flush()

    _migrate(pg, "upgrade")
    meta = {s.id: s.meta for s in pg.query(PredictionSet)}
    assert meta[ingested.id]["producer"] == RECORD and "method" not in meta[ingested.id]
    assert meta[ingested.id]["predictions_sha256"] == "d" * 64
    assert meta[native.id]["method"] == "own-annotations-nonexp-lafa-211"

    _migrate(pg, "downgrade")
    meta = {s.id: s.meta for s in pg.query(PredictionSet)}
    assert meta[ingested.id]["method"] == RECORD
    assert "producer" not in meta[ingested.id] and "predictions_sha256" not in meta[ingested.id]
