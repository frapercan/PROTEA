"""What a published number was measured with, in words.

A prediction set records how it was produced, and until now nothing could
show it. The columns name the inputs as identifiers and the receipt names
the decisions, and both are unreadable: a reader looking at a score could
see a UUID for the embedding config and nothing at all for the search
backend, the distance metric or which evidence a donor needed to vote.

This joins the receipt to the human names and returns the run in the terms
someone would describe it in. It computes nothing and stores nothing: a
field missing here is missing in the record, and the fix is to record it
rather than to infer it.
"""

from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from protea.api.deps import get_session_factory

router = APIRouter(prefix="/receipts", tags=["receipts"])

#: One row: the set, its receipt, and the names behind its identifiers.
_RECEIPT = text(
    """
    SELECT ps.id                     AS prediction_set_id,
           ps.limit_per_entry        AS k,
           ps.distance_threshold     AS distance_threshold,
           ps.created_at             AS created_at,
           ps.meta                   AS receipt,
           ec.model_name             AS model_name,
           ec.display_name           AS model_display_name,
           ec.family                 AS model_family,
           ec.pooling                AS pooling,
           ec.max_length             AS max_length,
           a.source_version          AS bank_release,
           a.source_published_at     AS bank_published_at,
           os.obo_url                AS ontology_url
    FROM prediction_set ps
    JOIN embedding_config ec ON ec.id = ps.embedding_config_id
    JOIN annotation_set a    ON a.id = ps.annotation_set_id
    JOIN ontology_snapshot os ON os.id = ps.ontology_snapshot_id
    WHERE ps.id = :psid
    """
)

#: The campaign a job declared, if the receipt links to one.
_CAMPAIGN = text(
    """
    SELECT meta ->> 'rung' AS rung, meta ->> 'window' AS window,
           meta ->> 'axis' AS axis, meta ->> 'scorer' AS scorer
    FROM job WHERE id = :jid
    """
)


def _donors(policy: dict[str, Any] | None) -> dict[str, Any]:
    """Which donors were allowed to vote, said rather than listed.

    The evidence list is thirteen codes long and nobody reads it as a list.
    What a reader wants is whether electronic annotations were in, because
    that is the difference between two entirely different claims.
    """
    policy = policy or {}
    codes = policy.get("evidence_codes")
    if codes is None:
        return {"regime": "every annotation in the bank", "evidence_codes": None}
    codes = list(codes)
    electronic = "IEA" in codes
    return {
        "regime": (
            "experimental and curated, electronic included"
            if electronic
            else "experimental and curated only, no electronic"
        ),
        "evidence_codes": codes,
        "reviewed_only": bool(policy.get("reviewed_only")),
    }


@router.get("/prediction-set/{prediction_set_id}", summary="How a prediction set was produced")
def get_receipt(
    prediction_set_id: uuid.UUID,
    factory: sessionmaker[Session] = Depends(get_session_factory),
) -> dict[str, Any]:
    """The run behind a set of predictions, in readable terms.

    404 rather than an empty body when the set is unknown, because a set
    that does not exist and one that recorded nothing must not look alike.
    """
    with factory() as session:
        row = session.execute(_RECEIPT, {"psid": prediction_set_id}).mappings().first()
        if row is None:
            raise HTTPException(status_code=404, detail=f"no prediction set {prediction_set_id}")
        receipt = dict(row["receipt"] or {})
        campaign = None
        if receipt.get("job_id"):
            c = session.execute(_CAMPAIGN, {"jid": receipt["job_id"]}).mappings().first()
            campaign = dict(c) if c else None

    return {
        "prediction_set_id": str(row["prediction_set_id"]),
        # Absent rather than guessed. A set written before the receipt
        # existed says so, instead of reporting defaults it never used.
        "has_receipt": bool(receipt),
        "model": {
            "name": row["model_name"],
            "display_name": row["model_display_name"] or row["model_name"],
            "family": row["model_family"],
            "pooling": row["pooling"],
            "max_length": row["max_length"],
        },
        "search": {
            "neighbours": row["k"],
            "metric": receipt.get("metric"),
            "backend": receipt.get("search_backend"),
            "distance_threshold": row["distance_threshold"],
            "aspect_separated": receipt.get("aspect_separated_knn"),
        },
        "donors": {
            "bank_release": row["bank_release"],
            "bank_published_at": (
                row["bank_published_at"].date().isoformat() if row["bank_published_at"] else None
            ),
            **_donors(receipt.get("donor_policy")),
        },
        "ontology": {"obo_url": row["ontology_url"]},
        "features": receipt.get("features") or [],
        "campaign": campaign,
        "job_id": receipt.get("job_id"),
        "created_at": row["created_at"].isoformat(),
    }
