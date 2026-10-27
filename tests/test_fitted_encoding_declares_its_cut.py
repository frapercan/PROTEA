"""A configuration that was fitted has to say which release it saw.

``embedding_config.trained_on_annotation_set_id`` is the only place an
encoding records the annotation release it was fitted against, and
``protea.core._evaluation_leakage`` reads exactly that to decide whether the
encoding may be scored on a frame: ``check_training_cut`` refuses a fitted
artifact that declares no release, with "cannot be certified clean or
contaminated".

The HTTP creation path could not express it. ``_canonicalise_config`` built
the row from thirteen keys and that was not among them, nor were ``kind``,
``family`` or the parent config, so EVERY configuration created over the API
arrived claiming to be a pretrained backbone with nothing to declare. For a
backend that only exists because something was fitted, that is not a missing
field: it is a row the guard will refuse later, after a run has been spent on
it.

So the cut is required exactly when the backend says the artifact was fitted,
refused when it says the opposite, and the refusal happens at creation.

THE OTHER HALF IS THE TAG. ``ingested-method`` is in ``FITTED_BACKENDS``
because the rows under it are written by ``ingest_method_predictions`` for a
method the platform did not score itself, which is a model fitted somewhere
against some release of our annotations. That claim is only worth anything
because the operation refuses a config carrying any other tag, which is
tested in :mod:`tests.test_ingest_method_predictions`; here the two halves
are checked against each other so they cannot drift apart.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest

from protea.core._evaluation_leakage import FITTED_BACKENDS
from protea.infrastructure.orm.models.embedding.embedding_config import (
    INGESTED_METHOD_BACKEND,
    VALID_BACKENDS,
    VALID_KINDS,
)
from protea.services.embeddings_service import (
    InvalidEmbeddingConfigError,
    validate_embedding_config_body,
)

PRETRAINED: dict[str, Any] = {
    "model_name": "ElnaggarLab/ankh-base",
    "model_backend": "ankh",
    "layer_indices": [0],
    "layer_agg": "mean",
    "pooling": "mean",
}


def _body(**over: Any) -> dict[str, Any]:
    body = dict(PRETRAINED)
    body.update(over)
    return body


def _errors(body: dict[str, Any]) -> list[str]:
    with pytest.raises(InvalidEmbeddingConfigError) as caught:
        validate_embedding_config_body(body)
    return list(getattr(caught.value, "errors", []) or [str(caught.value)])


class TestTheTwoHalvesAgree:
    def test_the_ingested_method_tag_is_a_registered_backend(self) -> None:
        assert INGESTED_METHOD_BACKEND in VALID_BACKENDS

    def test_and_it_counts_as_fitted(self) -> None:
        # If it did not, an ingested arm could be scored with no cut declared,
        # which is the one thing the leak rule of the research screens is
        # about.
        assert INGESTED_METHOD_BACKEND in FITTED_BACKENDS

    def test_every_fitted_backend_is_a_registered_one(self) -> None:
        # The guard reads a tag the API must be able to accept, or the only
        # way to create such a row is a direct insert.
        assert FITTED_BACKENDS <= VALID_BACKENDS

    def test_the_vocabularies_are_the_models_own(self) -> None:
        # One copy, not two. The service used to hold its own and the two had
        # already drifted by the two tags the encoder operations write.
        from protea.infrastructure.orm.models.embedding import embedding_config as model
        from protea.services import _embeddings_validation_helpers as helpers

        assert helpers.VALID_BACKENDS is model.VALID_BACKENDS
        assert helpers.VALID_POOLING is model.VALID_POOLING
        assert helpers.VALID_LAYER_AGG is model.VALID_LAYER_AGG


class TestAFittedEncodingOwesACut:
    @pytest.mark.parametrize("backend", sorted(FITTED_BACKENDS))
    def test_a_fitted_backend_without_a_cut_is_refused(self, backend: str) -> None:
        errors = _errors(_body(model_backend=backend))
        assert any("trained_on_annotation_set_id is required" in e for e in errors), errors

    @pytest.mark.parametrize("backend", sorted(FITTED_BACKENDS))
    def test_a_fitted_backend_with_a_cut_passes(self, backend: str) -> None:
        cut = str(uuid.uuid4())
        got = validate_embedding_config_body(
            _body(model_backend=backend, trained_on_annotation_set_id=cut)
        )
        assert got["trained_on_annotation_set_id"] == cut

    def test_kind_learned_owes_one_too(self) -> None:
        # The backend is the evidence the guard uses, but a caller that says
        # "learned" has asserted the same thing in the other field.
        errors = _errors(_body(kind="learned"))
        assert any("trained_on_annotation_set_id is required" in e for e in errors), errors

    def test_a_pretrained_backbone_may_not_claim_a_cut(self) -> None:
        # It saw none of our annotations, so a cut would be false. The
        # column's own note draws the line: NULL means "not fitted", not
        # "unknown".
        errors = _errors(_body(trained_on_annotation_set_id=str(uuid.uuid4())))
        assert any("was FITTED" in e for e in errors), errors

    def test_a_pretrained_backbone_passes_with_no_cut(self) -> None:
        got = validate_embedding_config_body(_body())
        assert got["trained_on_annotation_set_id"] is None
        assert got["kind"] == "pretrained"


class TestTheProvenanceFieldsSurvive:
    def test_the_five_fields_reach_the_canonical_row(self) -> None:
        # They were dropped on the floor: the row went in with thirteen keys
        # and these five were not among them.
        cut, parent = str(uuid.uuid4()), str(uuid.uuid4())
        got = validate_embedding_config_body(
            _body(
                model_backend=INGESTED_METHOD_BACKEND,
                model_name="sfjepa-recipe-ens8-cut211",
                layer_agg="none",
                pooling="reader",
                normalize=False,
                max_length=None,
                kind="learned",
                family="ankh",
                display_name="JEPA reader, 211 cut",
                derived_from_embedding_config_id=parent,
                trained_on_annotation_set_id=cut,
            )
        )
        assert got["kind"] == "learned"
        assert got["family"] == "ankh"
        assert got["display_name"] == "JEPA reader, 211 cut"
        assert got["derived_from_embedding_config_id"] == parent
        assert got["trained_on_annotation_set_id"] == cut
        # And the recipe itself, which is what the id is derived from.
        assert got["max_length"] is None
        assert got["pooling"] == "reader"

    def test_an_unknown_kind_is_refused(self) -> None:
        errors = _errors(_body(kind="borrowed"))
        assert any("kind must be one of" in e for e in errors), errors
        assert sorted(VALID_KINDS) == ["learned", "pretrained"]

    def test_an_id_that_is_not_a_uuid_is_refused(self) -> None:
        errors = _errors(
            _body(model_backend=INGESTED_METHOD_BACKEND, trained_on_annotation_set_id="goa 211")
        )
        assert any("must be a uuid" in e for e in errors), errors

    def test_the_serialiser_shows_the_cut(self) -> None:
        # A field that decides whether a configuration may be scored has to be
        # visible from the surface that creates it.
        from protea.services.embeddings_service import config_to_dict

        class _Config:
            id = uuid.uuid4()
            model_name = "sfjepa-recipe-ens8-cut211"
            model_backend = INGESTED_METHOD_BACKEND
            layer_indices = [0]
            layer_agg = "none"
            pooling = "reader"
            normalize_residues = False
            normalize = False
            embedding_scale = 1.0
            max_length = None
            use_chunking = False
            chunk_size = 512
            chunk_overlap = 0
            description = None
            display_name = "JEPA reader"
            family = "ankh"
            kind = "learned"
            derived_from_embedding_config_id = uuid.uuid4()
            trained_on_annotation_set_id = uuid.uuid4()

            class created_at:  # noqa: N801 - a stand-in for the datetime column
                @staticmethod
                def isoformat() -> str:
                    return "2026-10-27T12:00:00+00:00"

        out = config_to_dict(_Config())  # type: ignore[arg-type]
        assert out["trained_on_annotation_set_id"] == str(_Config.trained_on_annotation_set_id)
        assert out["kind"] == "learned"
