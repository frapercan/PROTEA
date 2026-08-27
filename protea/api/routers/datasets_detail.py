"""Helpers for the dataset detail endpoints (stats + artifact download).

Extracted from ``datasets.py`` to keep that module under the 800-LOC
smell-check ceiling. Both functions are called only from the
``/datasets/{id}/stats`` and ``/datasets/{id}/download`` routes.
"""

from __future__ import annotations

import os
from datetime import timedelta
from typing import Any
from urllib.parse import urlparse

from fastapi import HTTPException
from fastapi.responses import RedirectResponse, StreamingResponse
from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.infrastructure.orm.models.embedding.dataset import Dataset
from protea.infrastructure.settings import Settings

_ASPECT_MAP: dict[str, str] = {"P": "bpo", "F": "mfo", "C": "cco"}

_ARTIFACT_FILENAME: dict[str, str] = {
    "train": "train.parquet",
    "eval": "eval.parquet",
    "manifest": "manifest.json",
}


def compute_aspect_stats(row: Dataset, session: Session) -> dict[str, Any]:
    """Return per-aspect annotation counts for a dataset row.

    Reads from ``dataset.meta['aspect_stats']`` when the key is present
    (written by backfill or by a previous call). On a cache miss the
    counts are computed from ``protein_go_annotation`` via the
    dataset's ``ontology_snapshot_id`` and persisted back into ``meta``
    for subsequent requests.
    """
    meta: dict[str, Any] = dict(row.meta or {})

    if "aspect_stats" in meta:
        return {"aspect_stats": meta["aspect_stats"]}

    if row.ontology_snapshot_id is None:
        return {"aspect_stats": {}}

    sql = text(
        """
        SELECT
            gt.aspect,
            COUNT(DISTINCT pga.protein_accession) AS proteins,
            COUNT(DISTINCT pga.go_term_id)         AS go_terms,
            COUNT(*)                               AS annotations
        FROM protein_go_annotation pga
        JOIN go_term gt ON gt.id = pga.go_term_id
        WHERE gt.ontology_snapshot_id = :snap_id
          AND gt.aspect IS NOT NULL
        GROUP BY gt.aspect
        """
    )
    results = session.execute(sql, {"snap_id": str(row.ontology_snapshot_id)}).fetchall()

    aspect_stats: dict[str, dict[str, int]] = {}
    for aspect_raw, prot_count, go_count, ann_count in results:
        key = _ASPECT_MAP.get(aspect_raw, aspect_raw.lower() if aspect_raw else "unknown")
        aspect_stats[key] = {
            "proteins": int(prot_count),
            "go_terms": int(go_count),
            "annotations": int(ann_count),
        }

    meta["aspect_stats"] = aspect_stats
    row.meta = meta
    session.flush()
    return {"aspect_stats": aspect_stats}


def presigned_redirect(uri: str, filename: str, settings: Settings) -> RedirectResponse:
    """Mint a MinIO presigned GET URL and 302-redirect to it."""
    try:
        from minio import Minio
    except ImportError as exc:
        raise HTTPException(
            status_code=503,
            detail="MinIO client not installed; cannot mint presigned URL",
        ) from exc

    parsed = urlparse(uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    client = Minio(
        settings.minio_endpoint or "",
        access_key=settings.minio_access_key or "",
        secret_key=settings.minio_secret_key or "",
        secure=settings.minio_secure,
    )
    url = client.presigned_get_object(
        bucket,
        key,
        expires=timedelta(minutes=15),
        response_headers={"response-content-disposition": f'attachment; filename="{filename}"'},
    )
    return RedirectResponse(url=url, status_code=302)


def stream_local_file(uri: str, filename: str) -> StreamingResponse:
    """Stream a ``file://`` artifact as an attachment."""
    path = urlparse(uri).path
    if not os.path.isfile(path):
        raise HTTPException(
            status_code=404,
            detail=f"Artifact file not found on disk: {path!r}",
        )

    def _iter():
        with open(path, "rb") as f:  # noqa: WPS515
            while chunk := f.read(65536):
                yield chunk

    content_type = (
        "application/octet-stream" if filename.endswith(".parquet") else "application/json"
    )
    return StreamingResponse(
        _iter(),
        media_type=content_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
