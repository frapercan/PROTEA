"""The registry of model weights over HTTP (ADR-D54).

Reads are public, like the other registries. Every write is for operators:

* ``POST /model-weights/by-reference`` registers a file already in the artifact store, after
  hashing it there;
* ``POST /model-weights/uploads``, ``PUT /model-weights/uploads/{id}/parts/{n}`` and
  ``POST /model-weights/uploads/{id}/complete`` upload a file in parts, because the web
  proxy caps a request at 100 MB and vocabulary v1 alone is 260 MB;
* ``DELETE /model-weights/uploads/{id}`` abandons an upload.

The logic lives in :mod:`protea.services.model_weights_service`; this module maps its
errors to status codes: a wrong request is 422, a missing upload or store object 404, a key
already registered 409 with the existing id.
"""

from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session, sessionmaker

from protea.api.deps import get_session_factory
from protea.api.roles import ROLE_OPERATOR, require_role
from protea.infrastructure.session import session_scope
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store
from protea.services import model_weights_service as svc

router = APIRouter(prefix="/model-weights", tags=["model-weights"])


class RegisterByReferenceRequest(BaseModel):
    kind: str = Field(description="One of the registry's kinds, e.g. residue_vocabulary.")
    name: str = Field(description="A file-name stem; letters, digits, dot, dash, underscore.")
    uri: str = Field(
        description="The artifact store key, e.g. residue-vocabulary/vocabulary-v1-06fed75ca76d17be.npz."
    )
    sha256: str = Field(
        description="The digest its producer recorded; the object is hashed and must match."
    )
    description: str | None = None
    meta: dict[str, Any] = Field(default_factory=dict)
    trained_on_annotation_set_id: uuid.UUID | None = None


class CreateUploadRequest(BaseModel):
    kind: str
    name: str
    filename: str = Field(description="The original file name; its extension gives the format.")
    size_bytes: int = Field(gt=0)
    sha256: str | None = Field(
        default=None, description="When given, the joined file must hash to it."
    )
    description: str | None = None
    meta: dict[str, Any] = Field(default_factory=dict)
    trained_on_annotation_set_id: uuid.UUID | None = None


def _who(principal: Any) -> str | None:
    """A name for ``created_by``: the API key's name or the bearer's subject."""
    for attr in ("name", "sub", "email"):
        value = getattr(principal, attr, None)
        if isinstance(value, str) and value:
            return value
    return None


def _info(
    body: RegisterByReferenceRequest | CreateUploadRequest, principal: Any
) -> svc.WeightsInfo:
    return svc.WeightsInfo(
        kind=body.kind,
        name=body.name,
        description=body.description,
        meta=body.meta,
        trained_on_annotation_set_id=body.trained_on_annotation_set_id,
        created_by=_who(principal),
    )


def _http(exc: Exception) -> HTTPException:
    if isinstance(exc, svc.WeightsConflict):
        return HTTPException(
            status_code=409, detail={"message": str(exc), "existing_id": str(exc.existing_id)}
        )
    if isinstance(exc, svc.WeightsNotFound):
        return HTTPException(status_code=404, detail=str(exc))
    return HTTPException(status_code=422, detail=str(exc))


@router.get("", summary="List model weights")
def list_model_weights(
    kind: str | None = Query(default=None, description="Only this kind."),
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> list[dict[str, Any]]:
    """Every registered weight, newest first, with how many configurations were made from it."""
    try:
        with session_scope(factory) as session:
            return svc.list_weights(session, kind)
    except svc.WeightsError as exc:
        raise _http(exc) from exc


@router.get("/{weights_id}", summary="Get one model weight and what uses it")
def get_model_weights(
    weights_id: uuid.UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """The weight, the configurations made from it and the jobs whose payload names its key."""
    with session_scope(factory) as session:
        detail = svc.weights_detail(session, weights_id)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"no model weights {weights_id}")
    return detail


@router.post(
    "/by-reference",
    status_code=201,
    summary="Register a file of weights already in the artifact store",
)
def register_model_weights_by_reference(
    body: RegisterByReferenceRequest,
    factory: sessionmaker[Session] = Depends(get_session_factory),
    principal: Any = Depends(require_role(ROLE_OPERATOR)),
) -> dict[str, Any]:
    """Hash the object in the store and register it if it matches the sha256 given."""
    store = get_artifact_store(load_settings())
    try:
        with session_scope(factory) as session:
            row = svc.register_by_reference(
                session, store, _info(body, principal), body.uri, body.sha256
            )
            return svc.weights_dict(row)
    except (svc.WeightsError, svc.WeightsNotFound, svc.WeightsConflict) as exc:
        raise _http(exc) from exc


@router.post("/uploads", status_code=201, summary="Open an upload of model weights in parts")
def create_model_weights_upload(
    body: CreateUploadRequest,
    principal: Any = Depends(require_role(ROLE_OPERATOR)),
) -> dict[str, Any]:
    """Returns the upload id, the part size and how many parts to send."""
    try:
        return svc.create_upload(
            _info(body, principal), body.filename, body.size_bytes, body.sha256
        )
    except svc.WeightsError as exc:
        raise _http(exc) from exc


@router.put(
    "/uploads/{upload_id}/parts/{n}",
    summary="Send one part of an upload (the raw bytes as the body)",
    dependencies=[Depends(require_role(ROLE_OPERATOR))],
)
async def put_model_weights_part(upload_id: str, n: int, request: Request) -> dict[str, Any]:
    """Part ``n`` of the upload; every part but the last has exactly the part size."""
    data = await request.body()
    try:
        return svc.put_part(upload_id, n, data)
    except (svc.WeightsError, svc.WeightsNotFound) as exc:
        raise _http(exc) from exc


@router.post(
    "/uploads/{upload_id}/complete",
    status_code=201,
    summary="Join an upload's parts, store the file and register it",
    dependencies=[Depends(require_role(ROLE_OPERATOR))],
)
def complete_model_weights_upload(
    upload_id: str,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """The registered weight; the upload is removed whether this succeeds or fails."""
    store = get_artifact_store(load_settings())
    try:
        with session_scope(factory) as session:
            row = svc.complete_upload(session, store, upload_id)
            return svc.weights_dict(row)
    except (svc.WeightsError, svc.WeightsNotFound, svc.WeightsConflict) as exc:
        raise _http(exc) from exc


@router.delete(
    "/uploads/{upload_id}",
    summary="Abandon an upload",
    dependencies=[Depends(require_role(ROLE_OPERATOR))],
)
def abort_model_weights_upload(upload_id: str) -> dict[str, Any]:
    try:
        svc.abort_upload(upload_id)
    except (svc.WeightsError, svc.WeightsNotFound) as exc:
        raise _http(exc) from exc
    return {"upload_id": upload_id, "aborted": True}
