"""Register an arm whose predictions were computed somewhere else.

WHY THIS OPERATION EXISTS. A method that arrives as a file still needs an
identity in this record, because ``prediction_set`` pins an embedding
configuration NOT NULL and every candidate row carries it. For a method that
identity is not a recipe -- the recipe is somebody else's and ran on another
machine -- it is TRACEABILITY: which arm produced these numbers.

WHY IT IS AN OPERATION AND NOT A SCRIPT. The configuration for the laboratory's
cut-211 recipe, ``eb7c1b13``, has no job behind it. There is no row in ``job``
at the minute it was created, so the platform cannot say who registered that arm
or from what, and its identifier is not reproducible from anything in this
repository. Every number published under that arm is attributed to a row whose
own provenance is missing, which is the defect the whole ``POST /v1/jobs`` rule
exists to prevent. This is how the next one gets a job.

THE IDENTIFIER IS DERIVED FROM THE NAME, and that is the second half of the same
repair. A random identifier makes re-registering the same arm a second arm, and
two arms with one name is a comparison nobody can read. Derived, the operation is
idempotent on identical input, refuses on conflicting input, and the identifier
can be recomputed from this file by anybody who has the name.

WHAT IT REFUSES TO BE TOLD. The layer indices, the aggregation, the pooling and
the normalisation flags are not arguments. They describe work THIS platform did
to produce vectors, and for an ingested method it did none: the arm arrived as
scores. A caller allowed to set them would be writing a recipe for a method whose
recipe is not ours, and a reader would have no way to tell that invention from a
measurement. They are fixed below, to the values that mean "not applicable".
"""

from __future__ import annotations

import uuid
from typing import Any

from pydantic import field_validator
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, Operation, OperationResult, ProteaPayload
from protea.core.utils import contract_payload
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet

# The backend taken from the model that owns the word rather than respelled
# here. ``ingest_method_predictions`` refuses any other value, so this
# operation exists to produce exactly this one and does not take it as an
# argument; a second spelling of it would drift from the first in silence.
from protea.infrastructure.orm.models.embedding.embedding_config import (
    INGESTED_METHOD_BACKEND,
    EmbeddingConfig,
)

#: The namespace the identifier is derived in. A fixed string in this file rather
#: than a setting, because the derivation has to be recomputable from the
#: repository alone: a namespace read from the environment would make the
#: identifier depend on where the job happened to run.
ID_NAMESPACE = uuid.UUID("6f1a9c2e-4b8d-4f3a-9e7c-1d5b0a8e2f46")

#: What the shape fields mean for an arm that arrived as scores: nothing. Fixed
#: here, not accepted from the caller. See the module docstring.
NOT_APPLICABLE_SHAPE: dict[str, Any] = {
    "layer_indices": [0],
    "layer_agg": "none",
    "pooling": "reader",
    "normalize": False,
    "normalize_residues": False,
    "use_chunking": False,
    "chunk_size": 512,
    "chunk_overlap": 0,
}

#: The fields a second registration under one name may not disagree about. The
#: shape fields are absent because this operation fixes them, so they cannot
#: disagree; what can is everything the caller supplies.
IDENTIFYING_FIELDS: tuple[str, ...] = (
    "model_name",
    "model_backend",
    "display_name",
    "description",
    "family",
    "kind",
    "trained_on_annotation_set_id",
    "derived_from_embedding_config_id",
)


def config_id_for(method_name: str) -> uuid.UUID:
    """The identifier this arm will always have.

    A UUID5 over the method's name, so the same name is the same arm on any
    machine and in any database, and the value can be recomputed from this file
    without reading the record. ``eb7c1b13`` cannot be recomputed from anything
    and that is the hole this closes.
    """
    return uuid.uuid5(ID_NAMESPACE, f"ingested-method/{method_name}")


class RegisterIngestedMethodPayload(ProteaPayload, frozen=True):
    """Which arm, what it saw at t0, and what it was built on.

    The three fields that are not prose are the ones a number gets attributed
    by. ``trained_on_annotation_set_id`` is the corpus the method saw at its
    cut, and it is REQUIRED: an ingested method that cannot say which release it
    was fitted against cannot be placed on the window it is scored on, and a
    null there would read as "pretrained, owes no cut".
    """

    method_name: str
    display_name: str
    description: str
    trained_on_annotation_set_id: str
    #: The configuration whose vectors the method's own readers were built on,
    #: when the producer names one. Null rather than a sentinel: "no parent" and
    #: "parent not declared" are different facts about an arm.
    derived_from_embedding_config_id: str | None = None
    family: str | None = None

    @field_validator("method_name", "display_name", "description", "trained_on_annotation_set_id")
    @classmethod
    def _non_empty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must be a non-empty string")
        return value.strip()

    @field_validator("method_name")
    @classmethod
    def _no_whitespace_inside(cls, value: str) -> str:
        """The name is written on every candidate row and read back as a key.

        A name carrying a space or a tab round-trips through a TSV as two
        fields, and the arm that comes back is not the arm that went in.
        """
        if any(c.isspace() for c in value):
            raise ValueError(
                f"method_name {value!r} contains whitespace. It is written on every row and "
                "read back as a key, so a space makes one arm come back as two."
            )
        return value


def _conflicts(existing: EmbeddingConfig, wanted: dict[str, Any]) -> list[str]:
    """Which identifying fields a second registration disagrees about.

    Compared as strings because the stored value is a UUID and the asked-for one
    is its text; a type mismatch would report every registration as a conflict
    and the refusal would become noise nobody reads.
    """
    out: list[str] = []
    for field in IDENTIFYING_FIELDS:
        here, there = getattr(existing, field, None), wanted.get(field)
        if (None if here is None else str(here)) != (None if there is None else str(there)):
            out.append(f"{field}: registered {here!r}, asked for {there!r}")
    return out


def refuse_a_second_arm_under_one_name(
    session: Session, method_name: str, config_id: uuid.UUID
) -> None:
    """Raise if this name is already registered at a DIFFERENT identifier.

    The derivation is new and the record is not. ``sfjepa-recipe-ens8-cut211``
    exists as ``eb7c1b13``, which was written outside the platform and is not
    what this file derives, so registering that arm here would add a second row
    for one method rather than adopt the first. Every number already published
    stays attributed to the old row and every new one would go to the new one,
    and the two would sit side by side in the graph as two levels of the
    substrate: a contrast between a method and itself.

    Refused and not adopted. Adopting would mean taking the identifier from the
    caller, which is the hole this operation closes, and writing a job that says
    an arm was registered by work that did not register it. The old row keeps
    its missing provenance, which is a true statement about it.
    """
    clash = (
        session.query(EmbeddingConfig)
        .filter(EmbeddingConfig.model_name == method_name, EmbeddingConfig.id != config_id)
        .first()
    )
    if clash is not None:
        raise ValueError(
            f"{method_name!r} is already registered as {clash.id}, which is not the "
            f"identifier this operation derives ({config_id}). That row predates this "
            "operation and was written without a job. Registering here would add a "
            "SECOND row for one method, so every number published so far and every "
            "number published next would be attributed to two different arms, and the "
            "graph would read them as two levels of one axis."
        )


def resolve_arm(
    session: Session, p: RegisterIngestedMethodPayload
) -> tuple[AnnotationSet, dict[str, Any]]:
    """What this arm IS, with both of its references resolved against the record.

    Apart from ``execute`` because it answers a different question. This one is
    "what is being registered", and the caller's answer is refused here if
    either thing it points at is absent; ``execute`` then answers "does it
    already exist", which is the only question whose answer can be no without
    anything being wrong.
    """
    corpus = session.get(AnnotationSet, uuid.UUID(p.trained_on_annotation_set_id))
    if corpus is None:
        raise ValueError(
            f"AnnotationSet {p.trained_on_annotation_set_id} not found, so this arm "
            "cannot say which release it saw at its cut."
        )
    parent_id = (
        uuid.UUID(p.derived_from_embedding_config_id)
        if p.derived_from_embedding_config_id
        else None
    )
    if parent_id is not None and session.get(EmbeddingConfig, parent_id) is None:
        raise ValueError(
            f"EmbeddingConfig {parent_id} not found. A declared parent that is not in "
            "the record names no vectors, which is worse than declaring none."
        )
    return corpus, {
        "model_name": p.method_name,
        "model_backend": INGESTED_METHOD_BACKEND,
        "display_name": p.display_name,
        "description": p.description,
        "family": p.family,
        # Fitted against our annotations at a cut, which is what ``learned``
        # means here, and it is why the cut above is required.
        "kind": "learned",
        "trained_on_annotation_set_id": corpus.id,
        "derived_from_embedding_config_id": parent_id,
    }


class RegisterIngestedMethodOperation(Operation):
    """Give an externally computed arm an identity with a job behind it."""

    name = "register_ingested_method"
    description = (
        "Register an arm whose predictions were computed outside this platform, "
        "so the numbers ingested under it are attributable to a row with provenance."
    )
    payload_model = RegisterIngestedMethodPayload

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        """The method's name, which is the whole of what distinguishes two runs."""
        return str(payload.get("method_name", "?"))

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = RegisterIngestedMethodPayload.model_validate(contract_payload(payload))
        corpus, wanted = resolve_arm(session, p)
        config_id = config_id_for(p.method_name)
        refuse_a_second_arm_under_one_name(session, p.method_name, config_id)
        existing = session.get(EmbeddingConfig, config_id)
        if existing is not None:
            disagreements = _conflicts(existing, wanted)
            if disagreements:
                raise ValueError(
                    f"{p.method_name!r} is already registered as {config_id} and this "
                    "registration disagrees with it: "
                    + "; ".join(disagreements)
                    + ". Two arms under one name is a comparison nobody can read; pick a "
                    "name that says which of the two this is."
                )
            result = {
                "embedding_config_id": str(config_id),
                "method_name": p.method_name,
                "created": False,
                "corpus_version": corpus.source_version,
            }
            emit("register_ingested_method.already", None, result, "info")
            return OperationResult(result=result)
        session.add(EmbeddingConfig(id=config_id, **wanted, **NOT_APPLICABLE_SHAPE))
        session.flush()
        result = {
            "embedding_config_id": str(config_id),
            "method_name": p.method_name,
            "created": True,
            "corpus_version": corpus.source_version,
        }
        emit("register_ingested_method.done", None, result, "info")
        return OperationResult(result=result)
