"""Regression test pinning ``extra="forbid"`` on every API request body model.

Pydantic's default for ``BaseModel`` is to silently drop unknown fields.
That hides client typos: ``{"oepration": "ping"}`` (typo for ``operation``)
on ``POST /jobs`` would parse as if the operation were missing rather
than telling the client they fat-fingered a key.

Every body that lands on the public API should reject unknown keys with
a 422 so the consumer has a chance to correct the mistake. The pattern
was first established on ``ScoringConfigCreate`` (T2D.1 service-layer
extraction); this test pins it across every other request body model so
new endpoints can not regress to silent drops.

Response models (``RerankerResponse``, ``ScoringConfigResponse``, etc.)
are deliberately NOT in scope: they are server-built and do not parse
client input.
"""
from __future__ import annotations

import uuid

import pytest
from pydantic import BaseModel, ValidationError

from protea.api.routers.datasets import CreateDatasetRequest
from protea.api.routers.experiment_runs import (
    CreateExperimentRunRequest,
    UpdateExperimentRunRequest,
)
from protea.api.routers.jobs import CreateJobCommentRequest, CreateJobRequest
from protea.api.routers.reranker_models import ImportRerankerByReferenceRequest
from protea.api.routers.support import SupportCreate
from protea.services._scoring_models import ScoringConfigCreate

# ---------------------------------------------------------------------------
# Model registry: each entry is a model + a minimum valid body so we can
# round-trip-test that a normal payload still parses cleanly.
# ---------------------------------------------------------------------------

_VALID_BODIES: list[tuple[type[BaseModel], dict]] = [
    (CreateJobRequest, {"operation": "ping", "queue_name": "test.q"}),
    (CreateJobCommentRequest, {"body": "looks good"}),
    (CreateExperimentRunRequest, {"name": "ablation-2026-05-09"}),
    (UpdateExperimentRunRequest, {"description": "updated"}),
    (
        CreateDatasetRequest,
        {
            "output_name": "bench-v1-K5",
            "embedding_config_id": str(uuid.uuid4()),
            "ontology_snapshot_id": str(uuid.uuid4()),
            "train_versions": [160, 170],
            "test_versions": [230],
        },
    ),
    (
        ImportRerankerByReferenceRequest,
        {
            "artifact_uri": "file:///tmp/model.txt",
            "spec_yaml": "name: test",
            "run": {"run_id": "r1"},
        },
    ),
    (SupportCreate, {"comment": "thanks"}),
    (ScoringConfigCreate, {"name": "preset"}),
]


@pytest.mark.parametrize("model_cls,valid_body", _VALID_BODIES)
def test_unknown_field_rejected(model_cls: type[BaseModel], valid_body: dict) -> None:
    """Adding a stray key to an otherwise valid body must raise ``ValidationError``."""
    bad = {**valid_body, "totally_unknown_key": "x"}
    with pytest.raises(ValidationError):
        model_cls.model_validate(bad)


@pytest.mark.parametrize("model_cls,valid_body", _VALID_BODIES)
def test_valid_body_still_parses(model_cls: type[BaseModel], valid_body: dict) -> None:
    """Sanity check: the canonical body shape continues to parse cleanly."""
    instance = model_cls.model_validate(valid_body)
    assert isinstance(instance, model_cls)


def test_every_request_model_has_extra_forbid() -> None:
    """Pin the invariant directly on the model_config dict.

    Catches the ``model_config = {}`` regression where a future PR drops
    the ``extra`` setting silently.
    """
    offenders = []
    for model_cls, _ in _VALID_BODIES:
        cfg = getattr(model_cls, "model_config", {})
        if cfg.get("extra") != "forbid":
            offenders.append(model_cls.__name__)
    assert offenders == [], (
        "request body models without extra='forbid': " + ", ".join(offenders)
    )
