"""What an arm's identity has to carry, and what it may not be told.

The configuration behind the laboratory's cut-211 recipe was written without a
job and its identifier cannot be recomputed from this repository, so every
number published under that arm is attributed to a row whose own provenance is
missing. This operation is how the next arm avoids that, and these are the three
properties that make it work: the identifier is derived, a second registration
of one name either agrees or refuses, and the shape of a recipe this platform
did not run is not an argument.

Nothing here needs Postgres. The session is a double that answers the two
lookups the operation makes and records what it was told to add, which is what
lets "refuses without writing" be a test rather than a reading of the code.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest

from protea.core.operations.register_ingested_method import (
    ID_NAMESPACE,
    IDENTIFYING_FIELDS,
    NOT_APPLICABLE_SHAPE,
    RegisterIngestedMethodOperation,
    RegisterIngestedMethodPayload,
    config_id_for,
)
from protea.infrastructure.orm.models.embedding.embedding_config import (
    INGESTED_METHOD_BACKEND,
    VALID_KINDS,
    EmbeddingConfig,
)

CORPUS = uuid.uuid4()
PARENT = uuid.uuid4()
NAME = "sfjepa-recipe-ens8-cut230"


class _Corpus:
    def __init__(self, version: str = "230") -> None:
        self.id = CORPUS
        self.source_version = version


class _Query:
    """Stands in for the one-name lookup, which is a filter and a ``first``."""

    def __init__(self, clash: Any) -> None:
        self._clash = clash

    def filter(self, *_: Any) -> _Query:
        return self

    def first(self) -> Any:
        return self._clash


class _Session:
    """Answers ``get`` for the corpus, the parent and the config, records adds."""

    def __init__(
        self,
        *,
        corpus: Any = None,
        parent: Any = None,
        existing: Any = None,
        clash: Any = None,
    ) -> None:
        self._by_id: dict[Any, Any] = {}
        if corpus is not None:
            self._by_id[CORPUS] = corpus
        if parent is not None:
            self._by_id[PARENT] = parent
        self._existing = existing
        self._clash = clash
        self.added: list[Any] = []
        self.flushed = 0

    def get(self, model: Any, key: Any) -> Any:
        if model is EmbeddingConfig and key == config_id_for(NAME):
            return self._existing
        return self._by_id.get(key)

    def query(self, _: Any) -> _Query:
        return _Query(self._clash)

    def add(self, row: Any) -> None:
        self.added.append(row)

    def flush(self) -> None:
        self.flushed += 1


def _payload(**over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "method_name": NAME,
        "display_name": "Receta desplegable del laboratorio, corte 230",
        "description": "El promedio de ocho lectores JEPA con corte en GOA 230.",
        "trained_on_annotation_set_id": str(CORPUS),
    }
    base.update(over)
    return base


def _run(session: Any, **over: Any) -> dict[str, Any]:
    op = RegisterIngestedMethodOperation()
    outcome = op.execute(session, _payload(**over), emit=lambda *a, **k: None)
    assert isinstance(outcome.result, dict)
    return outcome.result


class TestTheIdentifierIsDerivedAndNotDrawn:
    """A random identifier makes re-registering one arm a second arm."""

    def test_the_same_name_is_always_the_same_identifier(self) -> None:
        """Recomputable from this file, on any machine, against any database.

        This is the half of the repair the cut-211 row cannot have: its
        identifier came from somewhere that is not in this repository, so no
        reader can check that it belongs to the name it carries.
        """
        assert config_id_for(NAME) == config_id_for(NAME)
        assert config_id_for(NAME) == uuid.uuid5(ID_NAMESPACE, f"ingested-method/{NAME}")

    def test_two_names_are_two_arms(self) -> None:
        """The cut is part of the name, so two cuts cannot collide."""
        assert config_id_for(NAME) != config_id_for("sfjepa-recipe-ens8-cut211")

    def test_the_derived_identifier_is_what_gets_written(self) -> None:
        session = _Session(corpus=_Corpus())
        result = _run(session)
        assert result["embedding_config_id"] == str(config_id_for(NAME))
        assert session.added[0].id == config_id_for(NAME)
        assert result["created"] is True
        assert session.flushed == 1


class TestASecondRegistrationAgreesOrRefuses:
    """Idempotent on identical input; a refusal on anything else."""

    def test_registering_the_same_arm_twice_writes_nothing_the_second_time(self) -> None:
        """Re-running a job must not be how a record grows a row.

        ``created`` false rather than a silent success, because the caller asked
        for a registration and is entitled to know it already existed.
        """
        existing = EmbeddingConfig(
            id=config_id_for(NAME),
            model_name=NAME,
            model_backend=INGESTED_METHOD_BACKEND,
            display_name="Receta desplegable del laboratorio, corte 230",
            description="El promedio de ocho lectores JEPA con corte en GOA 230.",
            family=None,
            kind="learned",
            trained_on_annotation_set_id=CORPUS,
            derived_from_embedding_config_id=None,
            **NOT_APPLICABLE_SHAPE,
        )
        session = _Session(corpus=_Corpus(), existing=existing)
        result = _run(session)
        assert result["created"] is False
        assert session.added == []

    def test_one_name_with_a_different_corpus_is_refused(self) -> None:
        """The cut is what a number gets placed on, so it cannot change under a name.

        An arm re-registered against a different release would leave every
        number already published attributed to a cut the row no longer states.
        """
        existing = EmbeddingConfig(
            id=config_id_for(NAME),
            model_name=NAME,
            model_backend=INGESTED_METHOD_BACKEND,
            display_name="Receta desplegable del laboratorio, corte 230",
            description="El promedio de ocho lectores JEPA con corte en GOA 230.",
            family=None,
            kind="learned",
            trained_on_annotation_set_id=uuid.uuid4(),
            derived_from_embedding_config_id=None,
            **NOT_APPLICABLE_SHAPE,
        )
        session = _Session(corpus=_Corpus(), existing=existing)
        with pytest.raises(ValueError, match="trained_on_annotation_set_id"):
            _run(session)
        assert session.added == []

    def test_a_name_already_held_at_another_identifier_is_refused(self) -> None:
        """Exactly the cut-211 situation, and it must not become two arms.

        ``eb7c1b13`` holds ``sfjepa-recipe-ens8-cut211`` at an identifier this
        file does not derive. Registering that arm here would add a second row,
        so the numbers already published and the ones published next would be
        attributed to two different arms and the graph would read them as two
        levels of the substrate: a contrast between a method and itself.
        """
        clash = EmbeddingConfig(id=uuid.uuid4(), model_name=NAME)
        session = _Session(corpus=_Corpus(), clash=clash)
        with pytest.raises(ValueError, match="SECOND row"):
            _run(session)
        assert session.added == []

    def test_the_identifying_fields_are_everything_the_caller_supplies(self) -> None:
        """The conflict check cannot be narrower than the payload.

        A field a caller can set and the check does not read is a field two
        registrations can disagree about silently.
        """
        supplied = set(RegisterIngestedMethodPayload.model_fields) - {
            "method_name",
            "display_name",
            "trained_on_annotation_set_id",
            "derived_from_embedding_config_id",
        }
        covered = set(IDENTIFYING_FIELDS)
        assert supplied - covered == set()
        assert {"model_name", "model_backend", "kind"} <= covered


class TestTheRecipeThisPlatformDidNotRunIsNotAnArgument:
    """The shape fields describe work that did not happen here."""

    def test_the_shape_is_fixed_and_cannot_be_asked_for(self) -> None:
        """A caller able to set them would be writing somebody else's recipe.

        And a reader would have no way to tell that invention from a
        measurement, because the fields read exactly alike either way.
        """
        asked = set(RegisterIngestedMethodPayload.model_fields)
        assert asked & set(NOT_APPLICABLE_SHAPE) == set()
        session = _Session(corpus=_Corpus())
        _run(session)
        for field, value in NOT_APPLICABLE_SHAPE.items():
            assert getattr(session.added[0], field) == value

    def test_the_arm_carries_the_backend_the_ingestion_demands(self) -> None:
        """``ingest_method_predictions`` refuses any other tag.

        So an arm registered here that carried a different one could never be
        ingested into, and the registration would be a row nobody can use.
        """
        session = _Session(corpus=_Corpus())
        _run(session)
        assert session.added[0].model_backend == INGESTED_METHOD_BACKEND

    def test_the_arm_is_learned_because_it_owes_a_cut(self) -> None:
        """``learned`` is the half that owes a training cut, and this one does."""
        session = _Session(corpus=_Corpus())
        _run(session)
        assert session.added[0].kind == "learned"
        assert session.added[0].kind in VALID_KINDS


class TestWhatCannotBePlacedIsRefused:
    """Every refusal leaves the record untouched."""

    def test_a_corpus_that_is_not_in_the_record_is_refused(self) -> None:
        """An arm that cannot name its cut cannot be placed on a window."""
        session = _Session()
        with pytest.raises(ValueError, match="cannot say which release"):
            _run(session)
        assert session.added == []

    def test_a_declared_parent_that_is_absent_is_refused(self) -> None:
        """Worse than declaring none: it names vectors that are not there."""
        session = _Session(corpus=_Corpus())
        with pytest.raises(ValueError, match="names no vectors"):
            _run(session, derived_from_embedding_config_id=str(PARENT))
        assert session.added == []

    def test_a_declared_parent_that_is_present_is_carried(self) -> None:
        session = _Session(corpus=_Corpus(), parent=EmbeddingConfig(id=PARENT))
        _run(session, derived_from_embedding_config_id=str(PARENT))
        assert session.added[0].derived_from_embedding_config_id == PARENT

    def test_no_parent_declared_stays_null_rather_than_a_sentinel(self) -> None:
        """ "No parent" and "parent not declared" are different facts about an arm."""
        session = _Session(corpus=_Corpus())
        _run(session)
        assert session.added[0].derived_from_embedding_config_id is None

    @pytest.mark.parametrize("name", ["sfjepa recipe cut230", "cut230\t", " cut230"])
    def test_whitespace_in_the_name_is_refused(self, name: str) -> None:
        """The name is written on every row and read back as a key.

        A leading space is stripped by the validator before this fires, so the
        case that matters is whitespace INSIDE: it round-trips through a TSV as
        two fields and the arm that comes back is not the arm that went in.
        """
        if name.strip() == "cut230":
            assert RegisterIngestedMethodPayload(**_payload(method_name=name)).method_name
            return
        with pytest.raises(ValueError, match="whitespace"):
            RegisterIngestedMethodPayload(**_payload(method_name=name))
