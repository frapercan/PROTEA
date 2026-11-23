"""The migration that gives the retrievals run before it the digest of their list.

Without a database: it follows the head it was written on and touches only what
it is for. Against Postgres (``--with-postgres``, CI): a retrieval handed a list
gets exactly the digest ``population_of`` computes for that list, whatever the
database's collation, its scored set gets the same, a set that already records a
population or whose job was handed no list keeps what it has, and the downgrade
takes the key away.
"""

from __future__ import annotations

import importlib.util
import pathlib
import uuid
from types import ModuleType

from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.orm import Session

from protea.core.query_population import population_of
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.job import Job
from tests.test_a_set_is_summarised_once import _method_set, _parents
from tests.test_a_set_records_its_population import ASKED
from tests.test_run_count_map import pg  # noqa: F401

_VERSIONS = pathlib.Path(__file__).resolve().parents[1] / "alembic" / "versions"
_MIGRATION = _VERSIONS / "7c5e1f0a9d42_a_set_asked_about_a_list_names_it.py"


def _module() -> ModuleType:
    """Loaded by path: ``alembic/`` is not a package and the library shadows the name."""
    spec = importlib.util.spec_from_file_location("_7c5e1f0a9d42", _MIGRATION)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_it_follows_the_head_it_was_written_on() -> None:
    module = _module()
    assert module.revision == "7c5e1f0a9d42"
    assert module.down_revision == "a5e954c5bab4"
    assert list(_VERSIONS.glob("a5e954c5bab4_*.py"))


def test_its_statements_touch_only_sets_that_declare_no_population() -> None:
    module = _module()
    for statement in (module.RETRIEVALS, module.SCORED):
        flat = " ".join(statement.split())
        assert "query_set_id IS NULL" in flat
        assert "NOT s.meta ? 'query_population'" in flat
    assert 'ORDER BY a.accession COLLATE "C"' in module.RETRIEVALS


def _ran(pg: Session, payload: dict, operation: str = "predict_go_terms") -> uuid.UUID:  # noqa: F811
    job = Job(operation=operation, queue_name="protea.jobs", payload=payload)
    pg.add(job)
    pg.flush()
    return job.id


def _migrate(pg: Session, step: str) -> None:  # noqa: F811
    with Operations.context(MigrationContext.configure(pg.connection())):
        getattr(_module(), step)()
    pg.expire_all()


def test_each_set_gets_the_population_of_the_list_its_job_was_handed(pg: Session) -> None:  # noqa: F811
    parents, _ = _parents(pg)
    retrieval = _method_set(pg, parents)
    retrieval.job_id = _ran(pg, {"query_accessions": ASKED})
    scored = _method_set(pg, parents)
    scored.job_id = _ran(pg, {"prediction_set_id": str(retrieval.id)}, "score_prediction_set")
    scored.meta = {"scored_from": str(retrieval.id)}
    recorded = _method_set(pg, parents)
    recorded.job_id = _ran(pg, {"query_accessions": ASKED})
    recorded.meta = {"query_population": {"proteins": 1, "sha256": "kept"}}
    unlisted = _method_set(pg, parents)
    unlisted.job_id = _ran(pg, {"annotation_set_id": str(uuid.uuid4())})
    pg.flush()

    _migrate(pg, "upgrade")

    meta = {s.id: s.meta for s in pg.query(PredictionSet)}
    assert meta[retrieval.id]["query_population"] == population_of(ASKED)
    assert meta[scored.id]["query_population"] == population_of(ASKED)
    assert meta[recorded.id]["query_population"] == {"proteins": 1, "sha256": "kept"}
    assert "query_population" not in meta[unlisted.id]

    _migrate(pg, "downgrade")
    assert not any("query_population" in s.meta for s in pg.query(PredictionSet))
