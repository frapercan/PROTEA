"""Which proteins a window may score, and that the answer travels.

The author chose on 2026-11-16 that the four windows of the frame score the
superset of the photo of their start: the Swiss-Prot list of the release paired
with the cut that opens the window, fixed before it opens, as CAFA fixes its
targets (ADR-D60). Until now no window asked, so any protein that gained was a
target. Like the rules about terms, this changes the delta, so it is part of the
window's identity, and every bucket of the ground truth keeps the population
only.

The Postgres half, that the superset is read from the photo's members, lives in
``tests/test_load_cut_photo.py`` beside the loaded photo it reads.
"""

from __future__ import annotations

import importlib.util
import uuid
from datetime import date
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from protea.core import _window_population as population
from protea.core._window_population import (
    TARGET_POPULATIONS,
    Population,
    SameSequence,
    restricted_to,
    same_sequence_at_both_ends,
    superset_at_start,
    swissprot_of_corpus,
)
from protea.core.evaluation import EvaluationData
from protea.core.operations import generate_evaluation_set as gen
from protea.core.operations.generate_evaluation_set import (
    GenerateEvaluationSetOperation,
    GenerateEvaluationSetPayload,
    _Identity,
)
from protea.infrastructure.orm.models.annotation import evaluation_set as model
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet

_MIGRATION = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions/e5b1c9d3a7f2_a_window_says_which_proteins_it_scores.py"
)


def _migration(path: Path | None = None) -> ModuleType:
    spec = importlib.util.spec_from_file_location("target_population_migration", path or _MIGRATION)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _lafa_migration() -> ModuleType:
    """The migration that added LAFA's targets to the check."""
    return _migration(_MIGRATION.with_name("5d3a8c1f7b24_a_window_can_be_built_by_lafa_s_frame.py"))


def _delta() -> EvaluationData:
    """P1 in the superset gains NK, P2 in it gains LK and PK, Q9 and T1 outside gain too."""
    return EvaluationData(
        nk={"P1": {"GO:1"}, "Q9": {"GO:2"}},
        lk={"P2": {"GO:3"}, "T1": {"GO:4"}},
        pk={"P2": {"GO:5"}, "T1": {"GO:6"}},
        known={"P2": {"GO:7"}, "T1": {"GO:8"}, "P3": {"GO:9"}},
        pk_known={"P2": {"GO:7"}, "T1": {"GO:8"}},
        removed={"P3": {"GO:9"}, "Q9": {"GO:10"}},
        projection={"projected_entries": 2},
        binding_dropped={"old": 1},
    )


SUPERSET = frozenset({"P1", "P2", "P3"})


class TestTheVocabularyIsClosed:
    def test_the_three_populations(self) -> None:
        # The third is LAFA's targets (2026-11-20, protea.core.lafa_frame).
        assert TARGET_POPULATIONS == ("every_protein", "superset_at_t0", "swissprot_same_sequence")

    def test_every_protein_is_the_default_so_no_stored_window_moves(self) -> None:
        payload = GenerateEvaluationSetPayload(
            old_annotation_set_id=str(uuid.uuid4()),
            new_annotation_set_id=str(uuid.uuid4()),
        )
        assert payload.target_population == "every_protein"
        ident = _Identity(*(uuid.uuid4() for _ in range(5)))
        assert ident.target_population == "every_protein"

    @pytest.mark.parametrize("value", TARGET_POPULATIONS)
    def test_the_payload_accepts_both(self, value: str) -> None:
        payload = GenerateEvaluationSetPayload(
            old_annotation_set_id=str(uuid.uuid4()),
            new_annotation_set_id=str(uuid.uuid4()),
            target_population=value,
        )
        assert payload.target_population == value

    def test_the_payload_refuses_an_unknown_population(self) -> None:
        with pytest.raises(ValueError, match="unknown target_population"):
            GenerateEvaluationSetPayload(
                old_annotation_set_id=str(uuid.uuid4()),
                new_annotation_set_id=str(uuid.uuid4()),
                target_population="trembl_at_t1",
            )


class TestTheRestriction:
    def test_every_bucket_keeps_the_population_only(self) -> None:
        kept, _ = restricted_to(_delta(), SUPERSET)
        assert kept.nk == {"P1": {"GO:1"}}
        assert kept.lk == {"P2": {"GO:3"}} and kept.pk == {"P2": {"GO:5"}}
        assert kept.known == {"P2": {"GO:7"}, "P3": {"GO:9"}}
        assert kept.pk_known == {"P2": {"GO:7"}}
        assert kept.removed == {"P3": {"GO:9"}}

    def test_what_it_leaves_out_is_counted_by_bucket(self) -> None:
        _, outside = restricted_to(_delta(), SUPERSET)
        # Q9 (NK) and T1 (LK and PK) are two proteins of the delta, not three.
        assert outside == {
            "outside_population_delta_proteins": 2,
            "outside_population_nk_proteins": 1,
            "outside_population_lk_proteins": 1,
            "outside_population_pk_proteins": 1,
        }

    def test_the_build_counters_are_carried_as_the_build_reported_them(self) -> None:
        kept, _ = restricted_to(_delta(), SUPERSET)
        assert kept.projection == {"projected_entries": 2}
        assert kept.binding_dropped == {"old": 1}

    def test_a_population_that_holds_every_target_changes_nothing(self) -> None:
        data = _delta()
        kept, outside = restricted_to(data, frozenset({"P1", "P2", "P3", "Q9", "T1"}))
        assert kept.stats() == data.stats()
        assert set(outside.values()) == {0}


class _Answers:
    """A session that answers the two reads of ``superset_at_start`` in order."""

    def __init__(self, photos: list[tuple[uuid.UUID, int]], accessions: list[str]) -> None:
        self.photos, self.accessions = photos, accessions
        self.statements: list[str] = []

    def execute(self, statement: Any, _params: dict[str, Any]) -> Any:
        self.statements.append(str(statement))
        result = MagicMock()
        result.all.return_value = self.photos
        result.scalars.return_value = iter(self.accessions)
        return result


class TestTheStartLeadsToOnePhoto:
    def test_the_superset_of_the_photo_that_loaded_the_start(self) -> None:
        photo = uuid.uuid4()
        session = _Answers([(photo, 194)], ["P1", "P2", "P1"])
        found = superset_at_start(session, uuid.uuid4())  # type: ignore[arg-type]
        assert found == Population(194, photo, frozenset({"P1", "P2"}))
        assert "annotation_set_id = :set_id" in session.statements[0]
        assert "AND superset" in session.statements[1]

    def test_a_start_that_is_no_photo_is_refused(self) -> None:
        with pytest.raises(ValueError, match="no cut photo has annotation set"):
            superset_at_start(_Answers([], []), uuid.uuid4())  # type: ignore[arg-type]

    def test_a_start_two_photos_name_is_refused(self) -> None:
        photos = [(uuid.uuid4(), 194), (uuid.uuid4(), 194)]
        with pytest.raises(ValueError, match="2 cut photos"):
            superset_at_start(_Answers(photos, ["P1"]), uuid.uuid4())  # type: ignore[arg-type]

    def test_an_empty_superset_is_refused(self) -> None:
        with pytest.raises(ValueError, match="empty superset"):
            superset_at_start(_Answers([(uuid.uuid4(), 194)], []), uuid.uuid4())  # type: ignore[arg-type]


class _Rows:
    """One query's rows, readable the three ways the population functions read them."""

    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.rows = rows

    def all(self) -> list[tuple[Any, ...]]:
        return list(self.rows)

    def scalars(self) -> Any:
        return iter(row[0] for row in self.rows)

    def __iter__(self) -> Any:
        return iter(self.rows)


class _Queue:
    """A session that answers each read with the next list of rows, in order."""

    def __init__(self, *answers: list[tuple[Any, ...]]) -> None:
        self.answers = list(answers)
        self.statements: list[str] = []

    def execute(self, statement: Any, _params: dict[str, Any]) -> _Rows:
        self.statements.append(" ".join(str(statement).split()))
        return _Rows(self.answers.pop(0))


class TestLafasTargets:
    START, END = uuid.uuid4(), uuid.uuid4()

    def _ends(self, pairs: list[tuple[str, bool]]) -> _Queue:
        return _Queue(
            [(uuid.uuid4(), self.START)],
            [(uuid.uuid4(), self.END)],
            pairs,
            [(self.START, "2022_03"), (self.END, "2025_03")],
        )

    def test_the_same_sequence_in_both_releases(self) -> None:
        session = self._ends([("P1", True), ("P2", False), ("P3", True)])
        found = same_sequence_at_both_ends(session, uuid.uuid4(), uuid.uuid4())  # type: ignore[arg-type]
        assert found == SameSequence("2022_03", "2025_03", 3, frozenset({"P1", "P3"}))
        assert found.described() == {
            "population_swissprot_releases": ["2022_03", "2025_03"],
            "population_in_both": 3,
            "population_sequence_changed": 1,
            "population_proteins": 2,
        }
        assert "b.accession = a.accession" in session.statements[2]

    def test_nothing_left_is_refused(self) -> None:
        with pytest.raises(ValueError, match="no accession has the same sequence"):
            same_sequence_at_both_ends(self._ends([("P1", False)]), uuid.uuid4(), uuid.uuid4())  # type: ignore[arg-type]

    def test_an_end_two_photos_name_is_refused(self) -> None:
        session = _Queue([(uuid.uuid4(), self.START), (uuid.uuid4(), self.START)])
        with pytest.raises(ValueError, match="window's start needs exactly one"):
            same_sequence_at_both_ends(session, uuid.uuid4(), uuid.uuid4())  # type: ignore[arg-type]

    def test_a_corpus_swissprot_list_is_its_photos_release(self) -> None:
        session = _Queue([(uuid.uuid4(), self.START)], [("P1",), ("P2",)])
        assert swissprot_of_corpus(session, uuid.uuid4()) == frozenset({"P1", "P2"})  # type: ignore[arg-type]
        assert "FROM swissprot_membership WHERE release_id = :release" in session.statements[1]


def _run(payload_over: dict[str, Any], found: Population | None) -> tuple[dict[str, Any], Any]:
    """Execute the operation on mocks; return its result and the delta it persisted."""
    session = MagicMock()
    old_set, new_set = MagicMock(), MagicMock()
    # Dated before the board's mark, or the holdout guard refuses them.
    for side, version in ((old_set, "194"), (new_set, "211")):
        side.source_published_at = date(2019, 11, 12)
        side.source_version = version
        side.ontology_snapshot_id = uuid.uuid4()
    session.get.side_effect = [old_set, new_set, MagicMock()]
    lookup = session.query.return_value.filter_by
    lookup.return_value.one_or_none.return_value = None
    persisted: dict[str, Any] = {}

    def _persist(_self: Any, _row: Any, data: Any, _emit: Any) -> str:
        persisted["data"] = data
        return "memory://groundtruth"

    payload = {
        "old_annotation_set_id": str(uuid.uuid4()),
        "new_annotation_set_id": str(uuid.uuid4()),
        **payload_over,
    }
    reader = MagicMock(return_value=found)
    with (
        patch.object(gen, "compute_evaluation_data_reconciled", return_value=_delta()),
        patch.object(gen, "superset_at_start", reader),
        patch.object(GenerateEvaluationSetOperation, "_persist_groundtruth", _persist),
    ):
        result = GenerateEvaluationSetOperation().execute(
            session, payload, emit=lambda *a, **k: None
        )
    persisted["lookup"] = lookup.call_args.kwargs
    persisted["reader_calls"] = reader.call_count
    return result.result, persisted


class TestTheOperationAppliesIt:
    def test_a_window_over_the_superset_persists_and_publishes_it(self) -> None:
        photo = uuid.uuid4()
        result, persisted = _run(
            {"target_population": "superset_at_t0"}, Population(194, photo, SUPERSET)
        )
        assert persisted["data"].nk == {"P1": {"GO:1"}}
        assert result["target_population"] == "superset_at_t0"
        assert result["delta_proteins"] == 2 and result["nk_proteins"] == 1
        assert result["outside_population_delta_proteins"] == 2
        assert result["population_cut"] == 194
        assert result["population_photo_id"] == str(photo)
        assert result["population_proteins"] == 3

    def test_the_reuse_lookup_asks_for_the_population(self) -> None:
        _, persisted = _run(
            {"target_population": "superset_at_t0"}, Population(194, uuid.uuid4(), SUPERSET)
        )
        assert persisted["lookup"]["target_population"] == "superset_at_t0"

    def test_a_window_over_every_protein_reads_no_photo(self) -> None:
        result, persisted = _run({}, None)
        assert persisted["reader_calls"] == 0
        assert result["target_population"] == "every_protein"
        assert result["delta_proteins"] == 4
        assert "outside_population_delta_proteins" not in result

    def test_the_new_row_holds_the_population(self) -> None:
        ident = _Identity(*(uuid.uuid4() for _ in range(5)), target_population="superset_at_t0")
        ask = gen._Ask(None, MagicMock())
        row = GenerateEvaluationSetOperation._new_row(ident, ask, {}, None)
        assert row.target_population == "superset_at_t0"


class TestTheRowKeepsThemApart:
    def test_the_identity_holds_the_population(self) -> None:
        constraint = next(
            c
            for c in EvaluationSet.__table_args__
            if getattr(c, "name", None) == "uq_evaluation_set_identity"
        )
        assert "target_population" in [c.name for c in constraint.columns]

    def test_the_column_defaults_to_every_protein_at_the_database(self) -> None:
        column = EvaluationSet.__table__.c.target_population
        assert column.nullable is False
        assert column.server_default.arg == "every_protein"

    def test_the_model_the_migration_and_the_vocabulary_agree(self) -> None:
        migration = _migration()
        # The check moved on with LAFA's targets, whose migration restores this text.
        assert (
            migration.TARGET_POPULATION_CHECK == _lafa_migration().PREVIOUS_TARGET_POPULATION_CHECK
        )
        assert _lafa_migration().TARGET_POPULATION_CHECK == model.TARGET_POPULATION_CHECK
        quoted = ", ".join(f"'{value}'" for value in population.TARGET_POPULATIONS)
        assert model.TARGET_POPULATION_CHECK == f"target_population IN ({quoted})"

    def test_the_migration_follows_the_photo_kinds(self) -> None:
        migration = _migration()
        assert (migration.revision, migration.down_revision) == ("e5b1c9d3a7f2", "a6e1e05d3c8b")
