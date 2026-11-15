"""A method's prediction set, with no embedding configuration, is listed and not dropped.

ADR-D60, decision 3, makes ``prediction_set.embedding_config_id`` nullable so a
method that is not a vote over one embedding can own its predictions. Four
readers joined the set to its configuration with an INNER join: the prediction
set listing, the receipt, the strata arms and the showcase. Each would have
dropped such a set while the row sat in the table, and the receipt would have
answered 404 for a set that exists. They are outer joins now, and a set produced
by a method says which method.

The first class is the sweep that keeps it so: no SQL in the package joins a
prediction set to its configuration with an inner join. The rest exercise each
reader on a method's set, with mocks where the read is a shape and on Postgres
(``--with-postgres``) where it is a query.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Session

import protea.infrastructure.orm.models  # noqa: F401  (registers every table)
from protea.api.routers import receipts, showcase, strata
from protea.core.contracts.method import Family, InstanceCut, MethodSpec, Pin
from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.annotation.evaluation_result import EvaluationResult
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.method.method import Method
from protea.services import embeddings_service

_PACKAGE = Path(__file__).resolve().parents[1] / "protea"
_METHOD_ID = uuid.UUID("5d1f9a3e-7c2b-5e4f-8a6d-0b9c8e7f6a51")


def _pg(statement: Any) -> str:
    return str(statement.compile(dialect=postgresql.dialect()))


class TestNoReaderInnerJoinsAPredictionSetToItsConfiguration:
    #: A raw SQL join onto embedding_config whose condition reads a prediction
    #: set's ``embedding_config_id``, with the word before JOIN captured.
    _TEXT = re.compile(
        r"(\w+)\s+JOIN\s+embedding_config\s+(?:AS\s+)?\w+\s+ON\s+[^\n]*embedding_config_id",
        re.IGNORECASE,
    )
    #: An ORM inner join onto EmbeddingConfig on a prediction set's column.
    _ORM = re.compile(r"\.join\(\s*EmbeddingConfig\s*,[^)]*PredictionSet\.embedding_config_id")

    def test_the_sweep_finds_the_joins_it_is_about(self) -> None:
        # Not vacuous: the four readers this change converted are still found,
        # now as outer joins.
        found = [
            path.name
            for path in _PACKAGE.rglob("*.py")
            for match in self._TEXT.finditer(path.read_text(encoding="utf-8"))
            if match.group(1).upper() in {"LEFT", "OUTER"}
        ]
        assert {"receipts.py", "strata.py"} <= set(found)

    def test_no_sql_text_inner_joins_the_configuration(self) -> None:
        inner = [
            f"{path.relative_to(_PACKAGE.parent)}: {match.group(0).splitlines()[0]}"
            for path in _PACKAGE.rglob("*.py")
            for match in self._TEXT.finditer(path.read_text(encoding="utf-8"))
            if match.group(1).upper() not in {"LEFT", "OUTER"}
        ]
        assert inner == []

    def test_no_orm_query_inner_joins_the_configuration(self) -> None:
        inner = [
            str(path.relative_to(_PACKAGE.parent))
            for path in _PACKAGE.rglob("*.py")
            if self._ORM.search(path.read_text(encoding="utf-8"))
        ]
        assert inner == []

    def test_the_four_readers_join_the_producer_outer(self) -> None:
        listing = _pg(embeddings_service._prediction_sets_query(Session()).statement)
        best = _pg(showcase._EVALUATIONS)
        for sql in (listing, best):
            assert "LEFT OUTER JOIN embedding_config" in sql
            assert "LEFT OUTER JOIN method" in sql
        for query in (receipts._RECEIPT, strata._ARMS):
            assert "LEFT JOIN embedding_config ec" in str(query)
            assert "LEFT JOIN method m" in str(query)


def _method_row(code: str = "huellas-211", method_id: uuid.UUID = _METHOD_ID) -> Any:
    return SimpleNamespace(
        id=method_id, code=code, family="explainable-units", instance_release=211
    )


class TestTheListing:
    def _listed(self, rows: list[tuple[Any, ...]]) -> list[dict[str, Any]]:
        session = MagicMock()
        listing = MagicMock()
        (
            listing.outerjoin.return_value.outerjoin.return_value.join.return_value.join.return_value.order_by.return_value.limit.return_value.all.return_value
        ) = rows
        counts = MagicMock()
        counts.group_by.return_value.all.return_value = []
        session.query.side_effect = [listing, counts]
        return embeddings_service.list_prediction_sets_data(session)

    @staticmethod
    def _set(**over: Any) -> Any:
        fields = {
            "id": uuid.uuid4(),
            "embedding_config_id": None,
            "method_id": _METHOD_ID,
            "annotation_set_id": uuid.uuid4(),
            "ontology_snapshot_id": uuid.uuid4(),
            "query_set_id": None,
            "limit_per_entry": 30,
            "distance_threshold": None,
            "created_at": datetime(2026, 11, 15, tzinfo=UTC),
        }
        fields.update(over)
        return SimpleNamespace(**fields)

    def test_a_method_set_is_listed_with_its_method_and_no_configuration(self) -> None:
        corpus = SimpleNamespace(source="goa", source_version="211")
        snapshot = SimpleNamespace(obo_version="releases/2022-09-19")
        (entry,) = self._listed([(self._set(), None, _method_row(), corpus, snapshot)])
        assert entry["embedding_config_id"] is None
        assert entry["embedding_config_name"] is None
        assert entry["method_id"] == str(_METHOD_ID)
        assert entry["method_code"] == "huellas-211"

    def test_a_configuration_set_reads_as_it_did_with_no_method(self) -> None:
        config = SimpleNamespace(model_name="ElnaggarLab/ankh-base")
        corpus = SimpleNamespace(source="goa", source_version=None)
        snapshot = SimpleNamespace(obo_version="releases/2022-09-19")
        config_id = uuid.uuid4()
        row = self._set(embedding_config_id=config_id, method_id=None)
        (entry,) = self._listed([(row, config, None, corpus, snapshot)])
        assert entry["embedding_config_id"] == str(config_id)
        assert entry["embedding_config_name"] == "ElnaggarLab/ankh-base"
        assert entry["method_id"] is None and entry["method_code"] is None
        assert entry["annotation_set_label"] == "goa"


class TestTheReceipt:
    @staticmethod
    def _row(**over: Any) -> dict[str, Any]:
        row: dict[str, Any] = {
            "prediction_set_id": uuid.uuid4(),
            "k": 30,
            "distance_threshold": None,
            "created_at": datetime(2026, 11, 15, tzinfo=UTC),
            "model_name": None,
            "model_display_name": None,
            "model_family": None,
            "pooling": None,
            "max_length": None,
            "method_id": _METHOD_ID,
            "method_family": "explainable-units",
            "method_code": "huellas-211",
            "method_instance_release": 211,
            "method_runtime": "native",
            "method_explains": True,
            "bank_release": "211",
            "bank_published_at": None,
            "ontology_url": "file:///go.obo",
        }
        row.update(over)
        return row

    def test_a_method_set_is_described_by_its_method(self) -> None:
        out = receipts._payload(self._row(), {}, None, None)
        assert out["method"] == {
            "id": str(_METHOD_ID),
            "family": "explainable-units",
            "code": "huellas-211",
            "instance_release": 211,
            "runtime": "native",
            "explains": True,
        }
        assert out["model"]["name"] is None

    def test_a_configuration_set_has_no_method_block(self) -> None:
        out = receipts._payload(
            self._row(method_id=None, model_name="esm2_t33_650M_UR50D"), {}, None, None
        )
        assert out["method"] is None
        assert out["model"]["display_name"] == "esm2_t33_650M_UR50D"


class TestTheStrataArms:
    def _arm(self, *, model: str, config: str | None, method: str | None) -> dict[str, Any]:
        return {
            "evaluation_result_id": f"res-{model}-{config}-{method}",
            "embedding_config_id": config,
            "method_id": method,
            "model": model,
            "display_name": model,
            "layer_indices": None,
            "k": 30,
        }

    def test_two_methods_keep_their_own_codes(self) -> None:
        arms = strata._label(
            [
                self._arm(model="huellas-211", config=None, method="aaaa1111-m"),
                self._arm(model="fusion-211", config=None, method="bbbb2222-m"),
            ]
        )
        assert [a["display_name"] for a in arms] == ["huellas-211", "fusion-211"]

    def test_a_method_that_shares_a_name_with_a_configuration_is_told_apart(self) -> None:
        # Two producers under one label, one of them with no configuration id:
        # the label used to read the configuration id's prefix and could not.
        arms = strata._label(
            [
                self._arm(model="ankh-base", config="cccc3333-c", method=None),
                self._arm(model="ankh-base", config=None, method="dddd4444-m"),
            ]
        )
        assert [a["display_name"] for a in arms] == ["ankh-base [cccc3333]", "ankh-base [dddd4444]"]

    def test_the_loader_carries_a_method_arm_without_a_configuration(self) -> None:
        row = {
            "evaluation_result_id": uuid.uuid4(),
            "embedding_config_id": None,
            "method_id": _METHOD_ID,
            "model": "huellas-211",
            "display_name": "huellas-211",
            "layer_indices": None,
            "k": 30,
            "scoring_name": None,
            "donor_policy": "permissive",
            "metric": None,
        }
        session = MagicMock()
        session.execute.return_value.mappings.return_value.all.return_value = [row]
        (arm,) = strata._load_arms(session, str(uuid.uuid4()))
        assert arm["embedding_config_id"] is None
        assert arm["method_id"] == str(_METHOD_ID)
        assert arm["display_name"] == "huellas-211"


def test_the_showcase_names_a_method_spotlight() -> None:
    result = SimpleNamespace(
        id=uuid.uuid4(),
        evaluation_set_id=uuid.uuid4(),
        results={"NK": {"BPO": {"fmax": 0.5}}},
        scoring_config_id=None,
        reranker_model_id=None,
    )
    session = MagicMock()
    session.execute.return_value.all.return_value = [(result, None, _method_row(), None)]
    best, _per_task, total = showcase._pick_best_evaluation(session)
    assert total == 1
    assert best is not None
    assert best["embedding"] is None
    assert best["method"] == {
        "id": str(_METHOD_ID),
        "family": "explainable-units",
        "code": "huellas-211",
        "instance_release": 211,
    }


# ------------------------------------------------------------------- on Postgres


@pytest.fixture()
def pg(postgres_url: str) -> Iterator[Session]:
    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    Base.metadata.drop_all(engine)


def _scenario(pg: Session) -> dict[str, uuid.UUID]:
    """A configuration's set and a method's set, both scored on one window."""
    from tests.helpers.prediction_sets import make_parents, make_prediction_set
    from tests.helpers.windows import make_window

    parents = make_parents(pg)
    legacy = make_prediction_set(pg, parents, {"code_revision": "c83f327"})
    spec = MethodSpec(
        family=Family.EXPLAINABLE_UNITS,
        code="huellas-211",
        instance=InstanceCut(211, parents["snapshot"]),
        recipe={"channels": ["truth", "curated"], "min_votes": 2},
        pins=(Pin("lexicon", "c" * 64, ("s3://protea/lexicons/lexico-final.npz",)),),
        runtime="native",
        explains=True,
    )
    pg.add(Method.from_spec(spec))
    pg.flush()
    method_set = PredictionSet(
        embedding_config_id=None,
        method_id=spec.method_id,
        annotation_set_id=parents["annotation"],
        ontology_snapshot_id=parents["snapshot"],
        limit_per_entry=30,
        meta={},
    )
    pg.add(method_set)
    pg.flush()
    window = make_window(pg, snapshot_version="method-listing")
    for pset in (legacy, method_set.id):
        pg.add(
            EvaluationResult(
                evaluation_set_id=window,
                prediction_set_id=pset,
                results={"NK": {"BPO": {"fmax": 0.4}}},
            )
        )
    pg.flush()
    return {
        "legacy": legacy,
        "method_set": method_set.id,
        "method": spec.method_id,
        "window": window,
    }


def test_every_reader_lists_a_method_set_on_postgres(pg: Session) -> None:
    ids = _scenario(pg)

    listed = {e["id"]: e for e in embeddings_service.list_prediction_sets_data(pg)}
    assert {str(ids["legacy"]), str(ids["method_set"])} <= set(listed)
    method_entry = listed[str(ids["method_set"])]
    assert method_entry["embedding_config_id"] is None
    assert method_entry["method_id"] == str(ids["method"])
    assert method_entry["method_code"] == "huellas-211"
    assert listed[str(ids["legacy"])]["method_id"] is None

    row = pg.execute(receipts._RECEIPT, {"psid": ids["method_set"]}).mappings().first()
    assert row is not None, "the receipt dropped a set that exists"
    assert receipts._payload(row, {}, None, None)["method"]["code"] == "huellas-211"

    arms = strata._load_arms(pg, ids["window"])
    assert {a["method_id"] for a in arms} == {None, str(ids["method"])}
    method_arm = next(a for a in arms if a["method_id"] is not None)
    assert method_arm["embedding_config_id"] is None
    assert method_arm["model"] == "huellas-211"

    _best, _per_task, total = showcase._pick_best_evaluation(pg)
    assert total == 2
