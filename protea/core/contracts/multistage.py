"""Multi-stage pipeline contract for PROTEA (F-MULTISTAGE-COHERENCE.1).

Three production pipelines share the same coordinator-fan-out-collect
pattern but each grew its own boilerplate independently:

* ``compute_embeddings`` (coordinator + batch + write)
* ``predict_go_terms`` (coordinator + batch + write)
* ``export_minijobs`` (coordinator + knn-batch + features-batch + write)

This module defines the **shared abstractions** so future migrations
(F-MULTISTAGE-COHERENCE.2/.3/.4) can converge those pipelines onto a
single, auditable pattern without changing any on-the-wire message shapes.

Classes
-------
MultiStagePayload
    Pydantic base every batch-stage payload should inherit from.
    Carries the ``coordinator_job_id`` field that every child operation
    needs to report progress back to its parent.

StageArtifactStore
    Thin facade over :class:`~protea.infrastructure.storage.ArtifactStore`
    that encodes the canonical path convention::

        temp/<pipeline>/<coordinator_job_id>/<stage>/<key>

    so individual stages never need to agree on a path scheme ad-hoc.

publish_next_stage
    Helper that serialises a Pydantic payload and decides whether to
    inline it (below ``size_threshold_kb``) or upload it to the artifact
    store and hand a URI reference to the next worker instead.

PipelineStage
    Abstract base class for *batch* and *write* stage operations.
    Provides ``_artifact_store(settings)`` and wraps ``execute`` to emit
    consistent ``<stage>.start`` / ``<stage>.done`` events.

Coordinator
    Abstract base class for coordinator operations (the fan-out step).
    Provides ``dispatch_partition_with_progress`` which returns a deferred
    ``OperationResult`` with the right progress bookkeeping.

None of the existing pipelines are migrated in this slice; they continue
to run their own per-pipeline boilerplate and remain fully tested.
"""

from __future__ import annotations

import abc
import json
import logging
from typing import Any
from uuid import UUID

from protea_contracts import ProteaPayload

from protea.core.contracts.operation import EmitFn, OperationResult
from protea.infrastructure.storage import ArtifactStore

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# MultiStagePayload
# ---------------------------------------------------------------------------


class MultiStagePayload(ProteaPayload):
    """Base payload for every *batch* stage in a multi-stage pipeline.

    Every child operation dispatched by a :class:`Coordinator` should
    inherit from this so the ``coordinator_job_id`` field is always
    present and validated.

    Subclasses may add further required or optional fields::

        class MyBatchPayload(MultiStagePayload, frozen=True):
            items: list[int]
            device: str = "cpu"
    """

    coordinator_job_id: str
    """UUID string of the parent coordinator Job row.

    Used by the batch and write stages to call
    :func:`~protea.core.contracts.parent_progress.update_parent_progress`
    so the coordinator's progress counter advances atomically.
    """


# ---------------------------------------------------------------------------
# StageArtifactStore
# ---------------------------------------------------------------------------


class StageArtifactStore:
    """Artifact-store facade with a fixed path convention.

    Intermediate artefacts produced by a pipeline stage are stored under::

        temp/<pipeline>/<coordinator_job_id>/<stage>/<key>

    This eliminates ad-hoc path construction scattered across the three
    real pipeline implementations.

    Parameters
    ----------
    store:
        A concrete :class:`~protea.infrastructure.storage.ArtifactStore`
        instance (local-FS or MinIO).
    pipeline:
        Short identifier for the pipeline, e.g. ``"embeddings"`` or
        ``"predictions"``.  Becomes the second path component.
    coordinator_job_id:
        UUID of the parent coordinator job.  Becomes the third path
        component, guaranteeing that concurrent pipeline runs never
        collide on intermediate keys.
    """

    def __init__(
        self,
        store: ArtifactStore,
        pipeline: str,
        coordinator_job_id: str | UUID,
    ) -> None:
        self._store = store
        self._pipeline = pipeline
        self._coord_id = str(coordinator_job_id)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _key(self, stage: str, key: str) -> str:
        return f"temp/{self._pipeline}/{self._coord_id}/{stage}/{key}"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def write_intermediate(self, stage: str, key: str, data: bytes | Any) -> str:
        """Serialise *data* and store it; return the URI.

        If *data* is already ``bytes`` it is stored as-is.  Any other
        type is JSON-serialised first.

        Returns the opaque URI string (``file://…`` or ``s3://…``) that
        a downstream stage can pass to :meth:`read_intermediate`.
        """
        blob: bytes = data if isinstance(data, (bytes, bytearray)) else json.dumps(data).encode()
        full_key = self._key(stage, key)
        return self._store.put(full_key, blob)

    def read_intermediate(self, uri: str) -> bytes:
        """Return the raw bytes stored at *uri*.

        The URI is backend-opaque; this method is a thin wrapper that
        keeps callers from importing the raw ``ArtifactStore`` just for
        reads.
        """
        # URI format: "file:///abs/path/..." or "s3://bucket/key"
        # The store's ``get`` method expects the *key*, not the URI.
        # Reconstruct the key from the URI via a round-trip through ``url``.
        # We ask the store to resolve by finding the key from our known prefix.
        # For portability we rely on the fact that the key is everything after
        # the URI scheme prefix the store attaches.  A simpler approach: store
        # the key alongside, but that changes the public API shape.
        # Instead we use ``store.get`` which accepts the key via a small
        # private helper that reverses the ``url()`` transform.
        return self._store.get(self._uri_to_key(uri))

    def exists_intermediate(self, stage: str, key: str) -> bool:
        """Return True if the intermediate artefact already exists."""
        return self._store.exists(self._key(stage, key))

    def uri_for(self, stage: str, key: str) -> str:
        """Return the URI that *would* be used without uploading anything."""
        return self._store.url(self._key(stage, key))

    # ------------------------------------------------------------------
    # Internal: reverse URI -> key
    # ------------------------------------------------------------------

    def _uri_to_key(self, uri: str) -> str:
        """Recover the store key from a URI produced by this facade.

        Handles the two production backends:
        * ``file:///root/temp/...`` -> ``temp/...``
        * ``s3://bucket/temp/...`` -> ``temp/...``
        """
        if uri.startswith("file://"):
            # file:///abs_root/temp/<pipeline>/...
            # The root may be anywhere on the filesystem; we only care
            # about the "temp/<pipeline>/..." suffix.
            path_part = uri[len("file://"):]
            # Find "temp/" in the path and return from there.
            idx = path_part.find(f"temp/{self._pipeline}/")
            if idx == -1:
                raise ValueError(f"Cannot recover key from URI: {uri!r}")
            return path_part[idx:]
        if uri.startswith("s3://"):
            # s3://bucket/temp/<pipeline>/...
            without_scheme = uri[len("s3://"):]
            slash = without_scheme.find("/")
            if slash == -1:
                raise ValueError(f"Cannot recover key from URI: {uri!r}")
            path_part = without_scheme[slash + 1:]
            idx = path_part.find(f"temp/{self._pipeline}/")
            if idx == -1:
                raise ValueError(f"Cannot recover key from URI: {uri!r}")
            return path_part[idx:]
        raise ValueError(f"Unsupported URI scheme in: {uri!r}")


# ---------------------------------------------------------------------------
# publish_next_stage helper
# ---------------------------------------------------------------------------


def publish_next_stage(
    queue: str,
    payload: ProteaPayload | dict[str, Any],
    *,
    artifact_store: StageArtifactStore | None = None,
    stage: str = "payload",
    key: str = "next",
    size_threshold_kb: float = 64.0,
) -> tuple[str, dict[str, Any]]:
    """Return ``(queue, body)`` for ``OperationResult.publish_operations``.

    When the serialised body exceeds ``size_threshold_kb`` and an
    ``artifact_store`` is provided, the payload is uploaded and the body
    becomes ``{"_artifact_uri": "<uri>"}``.
    """
    body = payload.model_dump() if isinstance(payload, ProteaPayload) else dict(payload)
    raw = json.dumps(body).encode()
    if len(raw) <= size_threshold_kb * 1024:
        return (queue, body)
    if artifact_store is None:
        logger.warning(
            "publish_next_stage: payload %.1f KB exceeds %.0f KB threshold "
            "but no artifact_store provided; inlining anyway",
            len(raw) / 1024,
            size_threshold_kb,
        )
        return (queue, body)
    uri = artifact_store.write_intermediate(stage, key, raw)
    logger.debug(
        "publish_next_stage: payload %.1f KB exceeds %.0f KB threshold; stored at %s",
        len(raw) / 1024,
        size_threshold_kb,
        uri,
    )
    return (queue, {"_artifact_uri": uri})


# ---------------------------------------------------------------------------
# PipelineStage abstract base
# ---------------------------------------------------------------------------


class PipelineStage(abc.ABC):
    """Abstract base for *batch* and *write* stage operations.

    Concrete subclasses must set :attr:`name`, :attr:`description`, and
    :attr:`stage_name`, and implement :meth:`_execute_stage`.

    The :meth:`execute` wrapper emits ``<stage_name>.start`` before
    delegating and ``<stage_name>.done`` after, providing consistent
    observability across all multi-stage pipelines.

    The :meth:`_artifact_store` factory constructs a
    :class:`StageArtifactStore` bound to the coordinator job and pipeline
    name so concrete stages never hard-code path conventions.
    """

    #: Operation name (registered in OperationRegistry).
    name: str
    #: One-sentence human-readable description.
    description: str
    #: Short identifier used in event names and path components.
    stage_name: str

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        return ""

    def _artifact_store(
        self, raw_store: ArtifactStore, coordinator_job_id: str | UUID
    ) -> StageArtifactStore:
        """Return a :class:`StageArtifactStore` scoped to this stage."""
        return StageArtifactStore(
            store=raw_store,
            pipeline=self.stage_name,
            coordinator_job_id=coordinator_job_id,
        )

    def execute(
        self,
        session: Any,
        payload: dict[str, Any],
        *,
        emit: EmitFn,
    ) -> OperationResult:
        """Template: emit start, delegate, emit done."""
        emit(f"{self.stage_name}.start", None, {}, "info")
        result = self._execute_stage(session, payload, emit=emit)
        emit(f"{self.stage_name}.done", None, result.result, "info")
        return result

    @abc.abstractmethod
    def _execute_stage(
        self,
        session: Any,
        payload: dict[str, Any],
        *,
        emit: EmitFn,
    ) -> OperationResult:
        """Implement the stage-specific logic here."""


# ---------------------------------------------------------------------------
# Coordinator abstract base
# ---------------------------------------------------------------------------


class Coordinator(abc.ABC):
    """Abstract base for coordinator (fan-out) operations.

    Concrete subclasses must set :attr:`name`, :attr:`description`, and
    implement :meth:`partition`.

    :meth:`dispatch_partition_with_progress` builds the deferred
    :class:`~protea.core.contracts.operation.OperationResult` with
    ``progress_current=0``, ``progress_total=len(parts)``, and
    ``deferred=True``.  The child operations call
    :func:`~protea.core.contracts.parent_progress.update_parent_progress`
    to close the coordinator job when all children finish.
    """

    name: str
    description: str

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        return ""

    @abc.abstractmethod
    def partition(
        self,
        session: Any,
        payload: dict[str, Any],
        *,
        parent_job_id: UUID,
        emit: EmitFn,
    ) -> list[tuple[str, dict[str, Any]]]:
        """Return a list of ``(queue, body)`` pairs to dispatch.

        Each entry will be published to RabbitMQ via
        ``OperationResult.publish_operations``.
        """

    def dispatch_partition_with_progress(
        self,
        session: Any,
        payload: dict[str, Any],
        *,
        parent_job_id: UUID,
        emit: EmitFn,
        extra_result: dict[str, Any] | None = None,
    ) -> OperationResult:
        """Partition the payload and return a deferred OperationResult.

        Calls :meth:`partition`, emits a dispatch event with the batch
        count, and returns an ``OperationResult`` with:

        * ``deferred=True`` so BaseWorker does not mark the job SUCCEEDED
        * ``progress_current=0`` / ``progress_total=len(parts)``
        * ``publish_operations=parts``

        Parameters
        ----------
        extra_result:
            Optional extra keys merged into ``OperationResult.result``.
        """
        parts = self.partition(session, payload, parent_job_id=parent_job_id, emit=emit)
        n = len(parts)

        emit(
            f"{self.name}.dispatching",
            None,
            {"batches": n},
            "info",
        )

        result: dict[str, Any] = {"batches": n}
        if extra_result:
            result.update(extra_result)

        return OperationResult(
            result=result,
            progress_current=0,
            progress_total=n,
            deferred=True,
            publish_operations=parts,
        )

    def execute(
        self,
        session: Any,
        payload: dict[str, Any],
        *,
        emit: EmitFn,
    ) -> OperationResult:
        """Default execute: call dispatch_partition_with_progress."""
        parent_job_id = UUID(payload["_job_id"])
        return self.dispatch_partition_with_progress(
            session,
            payload,
            parent_job_id=parent_job_id,
            emit=emit,
        )
