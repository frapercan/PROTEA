"""Annotations service: pure-logic helpers extracted from
``protea.api.routers.annotations``.

ORM ↔ dict serialisers and the read-side handlers (snapshot/IA-url
operations) live here so non-router callers (CLI tools, batch
scripts) can reuse them without pulling FastAPI in.

The router translates the domain exceptions raised here to HTTP
responses:

- :class:`EntityNotFoundError` → ``404 Not Found`` (e.g. an
  ``OntologySnapshot`` or ``AnnotationSet`` UUID does not resolve).
"""

from __future__ import annotations

import hashlib
import uuid
from datetime import date
from typing import Any

from sqlalchemy import Date, cast, func, or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from protea.core.evaluation import load_evaluation_data_for_set
from protea.core.split_registry import BOARD_MARK, release
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.information_accretion_set import (
    InformationAccretionSet,
)
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.annotation.protein_go_annotation import ProteinGOAnnotation


class AnnotationsServiceError(Exception):
    """Base class for annotations-service domain errors."""


class EntityNotFoundError(AnnotationsServiceError):
    """Generic 404; a referenced entity does not exist.

    Pickle-safe via ``__reduce__`` so the structured ``entity`` /
    ``entity_id`` attrs survive a round-trip without tripping
    flake8-bugbear B042.
    """

    def __init__(self, entity: str, entity_id: uuid.UUID) -> None:  # noqa: B042
        super().__init__(f"{entity} not found")
        self.entity = entity
        self.entity_id = entity_id

    def __reduce__(self) -> tuple[type, tuple[str, uuid.UUID]]:
        return (self.__class__, (self.entity, self.entity_id))


class AnnotationSetReferencedError(AnnotationsServiceError):
    """An :class:`AnnotationSet` cannot be deleted because PredictionSet
    rows still reference it; the FK CASCADE is intentionally absent.
    Maps to HTTP 409 at the router boundary.
    """


def snapshot_to_dict(s: OntologySnapshot, term_count: int) -> dict[str, Any]:
    """Serialise an :class:`OntologySnapshot` to its API dict shape."""
    return {
        "id": str(s.id),
        "obo_url": s.obo_url,
        "obo_version": s.obo_version,
        "ia_url": s.ia_url,
        "loaded_at": s.loaded_at.isoformat(),
        "go_term_count": term_count,
    }


def list_snapshots_data(session: Session) -> list[dict[str, Any]]:
    """Return all loaded snapshots with their GO term counts (newest first).

    Pure read; the caller is responsible for caching at the API
    boundary if desired (the GROUP BY over the multi-million row
    ``go_term`` table is the slow part).
    """
    count_sub = (
        session.query(
            GOTerm.ontology_snapshot_id,
            func.count(GOTerm.id).label("cnt"),
        )
        .group_by(GOTerm.ontology_snapshot_id)
        .subquery()
    )
    rows = (
        session.query(OntologySnapshot, count_sub.c.cnt)
        .outerjoin(count_sub, OntologySnapshot.id == count_sub.c.ontology_snapshot_id)
        .order_by(OntologySnapshot.loaded_at.desc())
        .all()
    )
    return [snapshot_to_dict(s, cnt or 0) for s, cnt in rows]


def get_snapshot_data(
    session: Session,
    snapshot_id: uuid.UUID,
) -> dict[str, Any]:
    """Return a single snapshot with its GO term count.

    Raises :class:`EntityNotFoundError` when the UUID does not resolve.
    """
    s = session.get(OntologySnapshot, snapshot_id)
    if s is None:
        raise EntityNotFoundError("OntologySnapshot", snapshot_id)
    term_count = (
        session.query(func.count(GOTerm.id))
        .filter(GOTerm.ontology_snapshot_id == snapshot_id)
        .scalar()
    )
    return snapshot_to_dict(s, term_count or 0)


def set_snapshot_ia_url(
    session: Session,
    snapshot_id: uuid.UUID,
    ia_url: str | None,
) -> dict[str, Any]:
    """Update the IA URL on a snapshot. Empty string is treated as ``None``.

    Returns a small confirmation dict shape compatible with the
    legacy endpoint. Raises :class:`EntityNotFoundError` for the
    404 path. The caller (router) is responsible for validating
    request body shape (e.g. presence of the ``ia_url`` key) before
    calling.
    """
    s = session.get(OntologySnapshot, snapshot_id)
    if s is None:
        raise EntityNotFoundError("OntologySnapshot", snapshot_id)
    s.ia_url = ia_url or None
    session.flush()
    return {
        "id": str(s.id),
        "obo_version": s.obo_version,
        "ia_url": s.ia_url,
    }


def annotation_set_to_dict(a: AnnotationSet, count: int) -> dict[str, Any]:
    """Serialise an :class:`AnnotationSet` to its API dict shape."""
    return {
        "id": str(a.id),
        "source": a.source,
        "source_version": a.source_version,
        "source_published_at": (
            a.source_published_at.isoformat() if a.source_published_at else None
        ),
        "ontology_snapshot_id": str(a.ontology_snapshot_id),
        "job_id": str(a.job_id) if a.job_id else None,
        "created_at": a.created_at.isoformat(),
        "meta": a.meta,
        "annotation_count": count,
    }


def list_annotation_sets_data(
    session: Session,
    source: str | None = None,
) -> list[dict[str, Any]]:
    """List all annotation sets with their per-set annotation counts (newest first).

    Optionally filter by ``source`` (e.g. ``goa`` or ``quickgo``).
    Pure read; the caller caches at the API boundary.
    """
    count_sub = (
        session.query(
            ProteinGOAnnotation.annotation_set_id,
            func.count(ProteinGOAnnotation.id).label("cnt"),
        )
        .group_by(ProteinGOAnnotation.annotation_set_id)
        .subquery()
    )
    q = session.query(AnnotationSet, count_sub.c.cnt).outerjoin(
        count_sub, AnnotationSet.id == count_sub.c.annotation_set_id
    )
    if source is not None:
        q = q.filter(AnnotationSet.source == source)
    rows = q.order_by(AnnotationSet.created_at.desc()).all()
    return [annotation_set_to_dict(a, cnt or 0) for a, cnt in rows]


def get_annotation_set_data(
    session: Session,
    set_id: uuid.UUID,
) -> dict[str, Any]:
    """Return a single annotation set with its annotation count.

    Raises :class:`EntityNotFoundError` when the UUID does not resolve.
    """
    a = session.get(AnnotationSet, set_id)
    if a is None:
        raise EntityNotFoundError("AnnotationSet", set_id)
    annotation_count = (
        session.query(func.count(ProteinGOAnnotation.id))
        .filter(ProteinGOAnnotation.annotation_set_id == set_id)
        .scalar()
    )
    return annotation_set_to_dict(a, annotation_count or 0)


def delete_annotation_set_data(
    session: Session,
    set_id: uuid.UUID,
) -> dict[str, Any]:
    """Delete an annotation set and all its annotations.

    Returns the deletion summary dict, which carries ``source`` so the router
    can invalidate the per-source list cache without a second query.

    Raises:

    - :class:`EntityNotFoundError` if the UUID does not resolve.
    - :class:`AnnotationSetReferencedError` if a PredictionSet
      references this set (router maps to 409).
    """
    a = session.get(AnnotationSet, set_id)
    if a is None:
        raise EntityNotFoundError("AnnotationSet", set_id)
    annotation_count = (
        session.query(func.count(ProteinGOAnnotation.id))
        .filter(ProteinGOAnnotation.annotation_set_id == set_id)
        .scalar()
    )
    # Read before the delete: the router needs it to invalidate the per-source
    # list cache, and after the flush the instance is gone.
    source = a.source
    try:
        session.delete(a)
        session.flush()
    except IntegrityError as exc:
        raise AnnotationSetReferencedError(
            "This annotation set is referenced by one or more prediction "
            "sets. Delete those first."
        ) from exc
    return {
        "deleted": str(set_id),
        "source": source,
        "annotations_deleted": annotation_count or 0,
    }


def iter_groundtruth_tsv(
    session: Session,
    eval_id: uuid.UUID,
    category: str,
) -> list[str]:
    """Return the rows for a CAFA ``ground_truth_<CATEGORY>.tsv`` download.

    ``category`` is ``"nk"``, ``"lk"``, ``"pk"`` or ``"known"``.
    Each row is ``"<protein>\\t<go_id>\\n"``; sorted by protein then GO id
    so the output is deterministic.

    The list is materialised because the data it comes from already is:
    :func:`load_evaluation_data_for_set` reads the whole persisted parquet
    into memory, so streaming the rows out one at a time would not lower
    the high-water mark. It is NOT small, though -- ``"known"`` on
    evaluation set ``c35ee267`` is 5.206.444 rows, 95 MB, against 12.355
    for ``"nk"`` on the same set -- so the caller must batch it into blocks
    instead of handing this straight to a ``StreamingResponse``; see
    :func:`protea.api.routers.annotations._common.stream_groundtruth`.

    Raises :class:`EntityNotFoundError` when the EvaluationSet does
    not resolve.
    """
    e = session.get(EvaluationSet, eval_id)
    if e is None:
        raise EntityNotFoundError("EvaluationSet", eval_id)
    data, _ = load_evaluation_data_for_set(session, e)
    source: dict[str, set[str]] = getattr(data, category)
    return [
        f"{protein}\t{go_id}\n"
        for protein, go_ids in sorted(source.items())
        for go_id in sorted(go_ids)
    ]


# EvaluationSet + EvaluationResult helpers (serialisers, list/get/delete
# handlers, baseline-scoring auto-attach) live in
# _annotations_evaluation_helpers and are re-exported here so existing
# router/CLI imports keep working unchanged.
from protea.services._annotations_evaluation_helpers import (  # noqa: E402,F401
    apply_baseline_scoring_default,
    assert_evaluation_set_exists,
    delete_eval_result_collect_keys,
    delete_evaluation_set_collect_keys,
    evaluation_result_to_dict,
    evaluation_set_to_dict,
    get_eval_result_with_keys,
    get_evaluation_set_data,
    list_evaluation_results_data,
    list_evaluation_sets_data,
)

# iter_delta_proteins_fasta + get_go_subgraph_data live in
# _annotations_method_helpers and are re-exported here so existing
# router/CLI imports keep working unchanged.
from protea.services._annotations_method_helpers import (  # noqa: E402,F401
    get_go_subgraph_data,
    iter_delta_proteins_fasta,
)

# iter_evaluation_artifacts_zip + render_evaluation_metrics_tsv live in
# _annotations_streaming_helpers (no domain-exception coupling).
from protea.services._annotations_streaming_helpers import (  # noqa: E402,F401
    iter_evaluation_artifacts_zip,
    render_evaluation_metrics_tsv,
)

__all__ = [
    "AnnotationSetReferencedError",
    "AnnotationsServiceError",
    "EntityNotFoundError",
    "annotation_set_to_dict",
    "delete_annotation_set_data",
    "delete_evaluation_set_collect_keys",
    "evaluation_set_to_dict",
    "get_annotation_set_data",
    "get_evaluation_set_data",
    "get_snapshot_data",
    "delete_eval_result_collect_keys",
    "evaluation_result_to_dict",
    "get_eval_result_with_keys",
    "get_go_subgraph_data",
    "iter_delta_proteins_fasta",
    "iter_groundtruth_tsv",
    "list_evaluation_results_data",
    "render_evaluation_metrics_tsv",
    "list_annotation_sets_data",
    "list_evaluation_sets_data",
    "list_snapshots_data",
    "set_snapshot_ia_url",
    "snapshot_to_dict",
]


# ── Information Accretion sets ───────────────────────────────────────────────


class HoldoutArtifactError(AnnotationsServiceError):
    """The artefact asked for belongs to a release past the board's mark.

    Raised instead of served, and raised for the SAME reason the page that
    published release 235 was a problem: these routes answer to whoever holds a
    key, and an IA table is as much a fact about its release as the ground truth
    is. The waiver that lets a window read past the mark is a deliberate act
    recorded in one place; it is not a thing a download should perform silently.
    """

    def __init__(self, ia_set_id: uuid.UUID, published: date) -> None:
        super().__init__(
            f"InformationAccretionSet {ia_set_id} was computed on a release "
            f"published {published}, past the board's mark {BOARD_MARK} "
            f"({release(BOARD_MARK).published}); serving it here would hand out "
            "the holdout without the waiver that reading past the mark requires"
        )


def _ia_set_to_dict(s: InformationAccretionSet, source_version: str | None) -> dict[str, Any]:
    """One row as the API reports it.

    ``content_sha256`` is included on purpose: it is what a caller checks the
    bytes against after downloading, so listing it is what makes the download
    verifiable from the other side rather than only from ours.
    """
    return {
        "id": str(s.id),
        "ontology_snapshot_id": str(s.ontology_snapshot_id),
        "annotation_set_id": str(s.annotation_set_id),
        "source_version": source_version,
        "evidence_regime": s.evidence_regime,
        "content_sha256": s.content_sha256,
        "term_count": s.term_count,
        "nonzero_count": s.nonzero_count,
        "ia_max": s.ia_max,
        "ia_mean": s.ia_mean,
        "created_at": s.created_at.isoformat() if s.created_at else None,
    }


def list_information_accretion_sets_data(
    session: Session,
    *,
    ontology_snapshot_id: uuid.UUID | None = None,
    source_version: str | None = None,
    include_past_mark: bool = False,
) -> list[dict[str, Any]]:
    """List the IA tables, newest first, optionally narrowed.

    The two filters exist so a caller can find the IA OF A CUT without walking
    the whole list: a rolling frame asks for "the IA of r" once per cut, and
    making that a scan would make the obvious use the expensive one.

    ``include_past_mark`` defaults to False, so a plain listing never names a
    table computed on a release past the board's mark. Set it only where the
    waiver already applies.
    """
    q = (
        session.query(InformationAccretionSet, AnnotationSet.source_version)
        .join(AnnotationSet, AnnotationSet.id == InformationAccretionSet.annotation_set_id)
    )
    if ontology_snapshot_id is not None:
        q = q.filter(InformationAccretionSet.ontology_snapshot_id == ontology_snapshot_id)
    if source_version is not None:
        q = q.filter(AnnotationSet.source_version == source_version)
    if not include_past_mark:
        limite = release(BOARD_MARK).published
        q = q.filter(
            or_(
                AnnotationSet.source_published_at.is_(None),
                cast(AnnotationSet.source_published_at, Date) <= limite,
            )
        )
    rows = q.order_by(InformationAccretionSet.created_at.desc()).all()
    return [_ia_set_to_dict(s, ver) for s, ver in rows]


def fetch_information_accretion_tsv(
    session: Session,
    ia_set_id: uuid.UUID,
    *,
    store: Any,
    include_past_mark: bool = False,
) -> bytes:
    """The IA table's bytes, verified against the hash the row records.

    The verification is not ceremony. ``_evaluation_artifacts`` already refuses
    to score against an artefact whose bytes have drifted from
    ``content_sha256``; a download that skipped the same check would hand out
    the one copy nobody validated, and the caller would have no way to tell.

    Raises :class:`EntityNotFoundError` when the set does not resolve, and
    :class:`HoldoutArtifactError` when it belongs past the mark.
    """
    s = session.get(InformationAccretionSet, ia_set_id)
    if s is None:
        raise EntityNotFoundError("InformationAccretionSet", ia_set_id)
    a = session.get(AnnotationSet, s.annotation_set_id)
    publicada = getattr(a, "source_published_at", None)
    if not include_past_mark and publicada is not None:
        dia = publicada.date() if hasattr(publicada, "date") else publicada
        if dia > release(BOARD_MARK).published:
            raise HoldoutArtifactError(ia_set_id, dia)
    key = s.artifact_uri.split("/", 3)[-1] if "://" in s.artifact_uri else s.artifact_uri
    data = store.get(key)
    digest = hashlib.sha256(data).hexdigest()
    if digest != s.content_sha256:
        raise AnnotationsServiceError(
            f"InformationAccretionSet {ia_set_id} content hash mismatch: row "
            f"records {s.content_sha256}, stored bytes hash to {digest}; the "
            "stored artifact has drifted and is not served"
        )
    return data
