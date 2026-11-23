"""The fourth axis of an information accretion table: which proteins of the corpus count.

``every_protein`` is what every stored table did; ``swissprot`` keeps the
corpus's own Swiss-Prot list, the cohort LAFA computes its information
accretion over. These pin the vocabulary against the model and the migration,
the payload, that a row outside the cohort is neither counted nor dropped, that
the cohort is part of the identity a table is reused by, and, against Postgres,
the migration's guarded downgrade.
"""

from __future__ import annotations

import importlib.util
import pathlib
import re
import uuid
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from pydantic import ValidationError
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from protea.core.ia_regimes import IA_COHORTS
from protea.core.operations import compute_information_accretion as op
from protea.core.operations.compute_information_accretion import (
    ComputeInformationAccretionOperation,
    ComputeInformationAccretionPayload,
)
from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.annotation import information_accretion_set as model
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.information_accretion_set import (
    InformationAccretionSet,
)
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot

_MIGRATION = (
    pathlib.Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "2b7e4d9a6c31_an_information_accretion_table_names_its_cohort.py"
)


def _migration() -> ModuleType:
    spec = importlib.util.spec_from_file_location("_2b7e4d9a6c31", _MIGRATION)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _ids() -> dict[str, str]:
    return {"ontology_snapshot_id": str(uuid.uuid4()), "annotation_set_id": str(uuid.uuid4())}


class TestTheVocabulary:
    def test_the_two_cohorts(self) -> None:
        assert IA_COHORTS == ("every_protein", "swissprot")

    def test_the_model_the_migration_and_the_vocabulary_agree(self) -> None:
        assert _migration().COHORT_CHECK == model.COHORT_CHECK
        assert re.findall(r"'([^']*)'", model.COHORT_CHECK) == list(IA_COHORTS)

    def test_the_cohort_is_part_of_the_identity(self) -> None:
        (key,) = [
            c
            for c in InformationAccretionSet.__table_args__
            if getattr(c, "name", None) == "uq_ia_set_snapshot_corpus_regime_cohort"
        ]
        assert [c.name for c in key.columns][-1] == "cohort"

    def test_the_migration_follows_lafas_frame(self) -> None:
        assert _migration().down_revision == "5d3a8c1f7b24"


class TestThePayload:
    def test_it_defaults_to_every_protein_so_no_stored_table_moves(self) -> None:
        assert ComputeInformationAccretionPayload(**_ids()).cohort == "every_protein"

    def test_lafas_cohort_is_asked_for_by_name(self) -> None:
        assert (
            ComputeInformationAccretionPayload(**_ids(), cohort="swissprot").cohort == "swissprot"
        )

    @pytest.mark.parametrize("cohort", ["reviewed", "", "SWISSPROT"])
    def test_another_name_is_refused(self, cohort: str) -> None:
        with pytest.raises(ValidationError, match="unknown cohort"):
            ComputeInformationAccretionPayload(**_ids(), cohort=cohort)

    def test_the_one_line_summary_names_the_cohort(self) -> None:
        line = ComputeInformationAccretionOperation().summarize_payload(
            {**_ids(), "cohort": "swissprot"}
        )
        assert "cohort=swissprot" in line


class TestTheRowsOutsideTheCohort:
    def _propagate(self, cohort: frozenset[str] | None):
        rows = [("P1", "GO:0000002"), ("P2", "GO:0000002"), ("P1", "GO:9999999")]
        return ComputeInformationAccretionOperation._propagate(
            rows,
            ancestors={"GO:0000002": frozenset({"GO:0000002", "GO:0000001"})},
            emit=lambda *a, **k: None,
            cohort=cohort,
        )

    def test_they_are_neither_counted_nor_dropped(self) -> None:
        per_term, raw, dropped, proteins, outside = self._propagate(frozenset({"P1"}))
        assert per_term == {"GO:0000002": {"P1"}, "GO:0000001": {"P1"}}
        assert (raw, dropped, outside) == (2, 1, 1)
        assert proteins == {"P1"}

    def test_every_protein_keeps_every_row(self) -> None:
        _, raw, dropped, proteins, outside = self._propagate(None)
        assert (raw, dropped, outside) == (3, 1, 0)
        assert proteins == {"P1", "P2"}


class TestTheRowSaysWhichCohortItCounted:
    def _shape(
        self, cohort: frozenset[str] | None, outside: int
    ) -> tuple[InformationAccretionSet, Any]:
        p = ComputeInformationAccretionPayload(
            **_ids(), cohort="every_protein" if cohort is None else "swissprot"
        )
        inputs = op._Inputs(
            p,
            {},
            SimpleNamespace(obo_version="releases/2022-07-01"),
            SimpleNamespace(source="goa", source_version="211"),
            ("IDA",),
            None,
            cohort,
        )
        dag = op._Dag(["GO:0000001"], {}, {}, {"GO:0000001": {"P1"}}, {"GO:0000001": 0.0})
        measured = op._Measured({"nonzero": 0}, 2, {"P1"}, 1, {"GO:0000001": "F"}, 0.0, outside)
        ia_set = InformationAccretionSet()
        ComputeInformationAccretionOperation._record_shape(
            ia_set, inputs, dag, measured, "memory://ia.tsv", "0" * 64
        )
        ia_set.evidence_regime, ia_set.cohort = p.evidence_regime, p.cohort
        return ia_set, p

    def test_the_stats_name_the_cohort_and_what_it_left_out(self) -> None:
        ia_set, p = self._shape(frozenset({"P1", "P2"}), 3)
        assert ia_set.stats["cohort"] == "swissprot"
        assert ia_set.stats["cohort_proteins"] == 2
        assert ia_set.stats["rows_outside_cohort"] == 3
        result = op._persisted_result(ia_set, p, 2, 1).result
        assert (result["cohort"], result["annotation_count"], result["reused"]) == (
            "swissprot",
            2,
            False,
        )

    def test_every_protein_has_no_list_to_count(self) -> None:
        ia_set, _ = self._shape(None, 0)
        assert ia_set.stats["cohort"] == "every_protein"
        assert ia_set.stats["cohort_proteins"] is None


class TestARunOverLafasCohort:
    """The whole operation on a two-term ontology: one protein of the corpus is outside the cohort."""

    def test_the_table_counts_the_cohort_and_says_so(self) -> None:
        root, binding = "GO:0003674", "GO:0005488"
        session = MagicMock()
        session.get.side_effect = [
            SimpleNamespace(id=uuid.uuid4(), obo_version="releases/2022-07-01"),
            SimpleNamespace(id=uuid.uuid4(), source="goa", source_version="211"),
        ]
        session.query.return_value.filter_by.return_value.one_or_none.return_value = None
        added: list[Any] = []
        session.add.side_effect = added.append
        rows = [("P1", binding), ("P2", root), ("P3", binding)]
        dag = ([root, binding], {root: "F", binding: "F"}, {binding: {root}})
        with (
            patch.object(ComputeInformationAccretionOperation, "_load_dag", return_value=dag),
            patch.object(op, "corpus_rows", return_value=rows),
            patch.object(op, "swissprot_of_corpus", return_value=frozenset({"P1", "P2"})),
            patch.object(
                ComputeInformationAccretionOperation,
                "_write_artifact",
                return_value=("key", "memory://ia.tsv", "0" * 64),
            ),
        ):
            result = (
                ComputeInformationAccretionOperation()
                .execute(session, {**_ids(), "cohort": "swissprot"}, emit=MagicMock())
                .result
            )
        (table,) = added
        assert (result["cohort"], result["protein_count"], result["annotation_count"]) == (
            "swissprot",
            2,
            2,
        )
        assert table.cohort == "swissprot"
        assert table.stats["rows_outside_cohort"] == 1
        assert "cohort swissprot" in table.description


class TestTheCohortIsResolvedAndLookedUpBy:
    def test_lafas_cohort_reads_the_corpus_swissprot_list(self) -> None:
        session = MagicMock()
        lookup = session.query.return_value.filter_by
        lookup.return_value.one_or_none.return_value = None
        payload = {**_ids(), "cohort": "swissprot"}
        p = ComputeInformationAccretionPayload(**payload)
        with patch.object(op, "swissprot_of_corpus", return_value=frozenset({"P1"})) as reader:
            inputs = ComputeInformationAccretionOperation._resolve(session, p, payload)
        assert reader.called
        assert inputs.cohort == frozenset({"P1"})
        assert lookup.call_args.kwargs["cohort"] == "swissprot"

    def test_every_protein_reads_no_list(self) -> None:
        session = MagicMock()
        session.query.return_value.filter_by.return_value.one_or_none.return_value = None
        payload = _ids()
        p = ComputeInformationAccretionPayload(**payload)
        with patch.object(op, "swissprot_of_corpus") as reader:
            inputs = ComputeInformationAccretionOperation._resolve(session, p, payload)
        assert not reader.called and inputs.cohort is None


# --------------------------------------------------------------- against Postgres


@pytest.fixture()
def pg_engine(postgres_url: str):
    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    yield engine
    Base.metadata.drop_all(engine)
    engine.dispose()


def _store_a_table(engine, cohort: str) -> None:
    with Session(engine, future=True) as session:
        snapshot = OntologySnapshot(
            obo_version=f"releases/{uuid.uuid4().hex[:8]}", obo_url="file:///dev/null"
        )
        session.add(snapshot)
        session.flush()
        corpus = AnnotationSet(source="goa", ontology_snapshot_id=snapshot.id)
        session.add(corpus)
        session.flush()
        for each in (cohort, "every_protein") if cohort != "every_protein" else (cohort,):
            session.add(
                InformationAccretionSet(
                    ontology_snapshot_id=snapshot.id,
                    annotation_set_id=corpus.id,
                    evidence_regime="lafa",
                    cohort=each,
                    artifact_uri="memory://ia.tsv",
                    content_sha256="0" * 64,
                    term_count=1,
                    nonzero_count=1,
                    annotation_count=1,
                    protein_count=1,
                    propagated_pairs=1,
                    ia_max=1.0,
                    ia_mean=1.0,
                    stats={},
                )
            )
        session.commit()


def _migrate(engine, step: str) -> None:
    with engine.begin() as connection:
        with Operations.context(MigrationContext.configure(connection)):
            getattr(_migration(), step)()


@pytest.mark.integration
def test_the_two_cohorts_of_one_corpus_coexist(pg_engine) -> None:
    _store_a_table(pg_engine, "swissprot")


@pytest.mark.integration
def test_the_downgrade_refuses_while_a_table_counts_another_cohort(pg_engine) -> None:
    _store_a_table(pg_engine, "swissprot")
    with pytest.raises(RuntimeError, match="another cohort"):
        _migrate(pg_engine, "downgrade")


@pytest.mark.integration
def test_the_downgrade_and_the_upgrade_round_trip_on_every_protein(pg_engine) -> None:
    _store_a_table(pg_engine, "every_protein")
    _migrate(pg_engine, "downgrade")
    _migrate(pg_engine, "upgrade")
    _store_a_table(pg_engine, "swissprot")
