"""Pydantic models compartidos entre tools. Alineados con el ORM real."""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional
from uuid import UUID

from pydantic import BaseModel, Field

OperationKind = Literal[
    "compute_embeddings",
    "predict_go_terms",
    "insert_proteins",
    "fetch_uniprot_metadata",
    "load_ontology_snapshot",
    "load_goa_annotations",
    "load_quickgo_annotations",
    "ping",
]


class EnqueueRequest(BaseModel):
    operation: str
    queue_name: str
    payload: dict = Field(default_factory=dict)
    meta: dict = Field(default_factory=dict)
    dry_run: bool = True


class CancelRequest(BaseModel):
    job_id: UUID
    reason: str


class NotePost(BaseModel):
    channel: str
    author: str
    body: str
    tags: list[str] = Field(default_factory=list)


class ClaimRequest(BaseModel):
    task_key: str
    session_id: str
    ttl_seconds: int = 3600


class PredictionSetSummary(BaseModel):
    id: UUID
    model_name: str
    limit_per_entry: int
    distance_threshold: Optional[float]
    annotation_set_id: UUID
    ontology_snapshot_id: UUID
    created_at: datetime
    n_predictions: int


class QueueSnapshot(BaseModel):
    queue_name: str
    queued: int
    running: int
    succeeded: int
    failed: int
    cancelled: int
