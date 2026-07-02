"""Value provenance for the JSONB-blob feature families (ADR D45).

The reranker's ``feature_schema_sha`` fingerprints only the *contract*
feature families (knn, alignment, taxonomy, anc2vec, emb_pca, go_context,
annotation_meta, lineage): it guards the column NAMES the lab parquet
carries. Four other families ride the ``GOPrediction.features`` JSONB blob
and are produced post-KNN behind their own ``compute_*`` payload flags in
``_post_knn_pipeline.py``:

- ``classifier``   (``compute_classifier``),
- ``self_prior``   (``compute_self_prior``),
- ``association``  (``compute_association``),
- ``IA``           (``compute_ia`` + ``ia_file``).

Those families are never named in ``feature_schema.py``, so they do not
enter ``compute_feature_schema_sha`` and the schema guard is structurally
blind to them. A change in their VALUES (a producer that was a no-op at
train time becoming populated at serve time, a config change, a different
IA file) does not move any sha and trips no check. That is the exact
train/serve VALUE skew that collapsed reranked ``f_micro_w`` from 0.3745 to
0.3462 (the "0.3462 incident"): the column NAME was unchanged so
``feature_schema_sha`` matched and the guard passed silently.

This module computes a stable *value provenance* descriptor over those blob
families from the live predict payload, plus a short digest, so the seam is
observable and a train/serve mismatch can be surfaced (warn-only) rather
than passing silently. It is the value-provenance counterpart to
``feature_schema_sha``: schema identity is governed there, blob value
provenance is described here.

Scope note (conservative, per ADR D45): the descriptor captures what the
live pipeline can see from the payload (which blob producers are active and
their reachable config markers, e.g. ``ia_file``) together with a producer
version constant per family. The full content marker for a producer's
inputs (the ``association`` cooccurrence build keyed to the training t0
sets, the IA file content, producer revisions) is recorded on the TRAINING
artifact in the D45 follow-up; bump the matching entry in
``BLOB_PRODUCER_VERSIONS`` when a producer's value semantics change so the
digest moves with it.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any

from protea.core.contracts.operation import EmitFn

logger = logging.getLogger(__name__)

#: Canonical machine-readable reason for a blob-feature VALUE provenance
#: mismatch (ADR D45). Carried on the ``reranker.blob_provenance_mismatch``
#: event and the stdlib warning so downstream consumers can branch on a
#: stable literal, paralleling ``SchemaShaMismatchError.reason`` for the
#: (schema-identity) governed surface. This guard is warn-only: unlike the
#: schema-sha mismatch it never raises, so a value skew is made LOUD without
#: taking serving down.
BLOB_PROVENANCE_MISMATCH_REASON = "blob_provenance_mismatch"

#: The four ungoverned blob families, in a stable order.
BLOB_FAMILIES: tuple[str, ...] = ("classifier", "self_prior", "association", "ia")

#: Payload ``compute_*`` flag name that gates each blob family.
_BLOB_FLAG_BY_FAMILY: dict[str, str] = {
    "classifier": "compute_classifier",
    "self_prior": "compute_self_prior",
    "association": "compute_association",
    "ia": "compute_ia",
}

#: Producer version per blob family: a stable descriptor of HOW that blob
#: value is computed. Bump the entry (a monotonic string) whenever a
#: producer's value semantics change so the provenance digest moves even
#: when the ``compute_*`` flag and column name are unchanged. Kept
#: deliberately coarse (one knob per family) so a maintainer changing an
#: ``apply_*`` producer has one obvious place to record it.
BLOB_PRODUCER_VERSIONS: dict[str, str] = {
    "classifier": "1",
    "self_prior": "1",
    "association": "1",
    "ia": "1",
}

#: Digest width, matching ``compute_feature_schema_sha`` (12 hex chars).
_SHA_WIDTH = 12


def blob_provenance_descriptor(payload: Any) -> dict[str, dict[str, Any]]:
    """Describe the live blob-feature value provenance from ``payload``.

    Returns one entry per family in :data:`BLOB_FAMILIES`. Each entry always
    carries ``active`` (whether the family's ``compute_*`` flag is set); an
    ACTIVE family additionally carries its ``producer_version`` and any
    reachable config marker (currently ``ia_file`` for the IA family). An
    inactive family contributes only ``{"active": False}`` so that a default
    run (every blob flag off) has a stable, minimal descriptor.

    Read defensively via ``getattr`` so the function accepts any payload-like
    object (the real ``PredictGOTermsBatchPayload`` or a stand-in) and never
    raises on a missing flag.
    """
    descriptor: dict[str, dict[str, Any]] = {}
    for family in BLOB_FAMILIES:
        active = bool(getattr(payload, _BLOB_FLAG_BY_FAMILY[family], False))
        entry: dict[str, Any] = {"active": active}
        if active:
            entry["producer_version"] = BLOB_PRODUCER_VERSIONS[family]
            if family == "ia":
                entry["ia_file"] = getattr(payload, "ia_file", None)
        descriptor[family] = entry
    return descriptor


def compute_blob_provenance_sha(descriptor: dict[str, dict[str, Any]]) -> str:
    """Return a stable 12-hex digest of a blob-provenance ``descriptor``.

    Canonicalises the descriptor with sorted keys so the digest is
    order-independent and reproducible across processes, then takes the
    leading :data:`_SHA_WIDTH` hex chars of its SHA-256, mirroring the width
    of ``compute_feature_schema_sha``.
    """
    canonical = json.dumps(descriptor, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:_SHA_WIDTH]


def live_blob_provenance(payload: Any) -> tuple[str, dict[str, dict[str, Any]]]:
    """Convenience: ``(sha, descriptor)`` for the live payload's blob families."""
    descriptor = blob_provenance_descriptor(payload)
    return compute_blob_provenance_sha(descriptor), descriptor


def record_blob_provenance(payload: Any, emit: EmitFn) -> str:
    """Record the live blob-feature VALUE provenance and warn on mismatch.

    Always emits a ``reranker.blob_provenance`` info event carrying the live
    digest and per-family descriptor (so every reranked batch records HOW its
    blob values were produced, visible in the Job event log). When ``payload``
    carries an expected provenance (``reranker_blob_feature_provenance``,
    recorded on the booster in the D45 follow-up) that disagrees with the live
    digest, additionally emits a LOUD ``reranker.blob_provenance_mismatch``
    warning event plus a stdlib ``warning`` log, then PROCEEDS. It never
    raises: the expected marker is nullable (legacy boosters carry none), so a
    hard refuse would take serving down. Warn-only mirrors the D45 decision.

    Returns the live provenance digest. Scoring numbers are untouched.
    """
    live_sha, descriptor = live_blob_provenance(payload)
    reranker_model_id = getattr(payload, "reranker_model_id", None)
    emit(
        "reranker.blob_provenance",
        None,
        {
            "reranker_model_id": reranker_model_id,
            "blob_provenance_sha": live_sha,
            "blob_families": descriptor,
        },
        "info",
    )
    expected = getattr(payload, "reranker_blob_feature_provenance", None)
    if expected is not None and expected != live_sha:
        emit(
            "reranker.blob_provenance_mismatch",
            None,
            {
                "reason": BLOB_PROVENANCE_MISMATCH_REASON,
                "reranker_model_id": reranker_model_id,
                "expected_blob_provenance_sha": expected,
                "live_blob_provenance_sha": live_sha,
                "blob_families": descriptor,
            },
            "warning",
        )
        logger.warning(
            "reranker blob-feature value provenance mismatch: "
            "reranker_model_id=%s expected=%s live=%s reason=%s "
            "(proceeding; D45 value-skew guard is warn-only)",
            reranker_model_id,
            expected,
            live_sha,
            BLOB_PROVENANCE_MISMATCH_REASON,
        )
    return live_sha


__all__ = [
    "BLOB_FAMILIES",
    "BLOB_PRODUCER_VERSIONS",
    "BLOB_PROVENANCE_MISMATCH_REASON",
    "blob_provenance_descriptor",
    "compute_blob_provenance_sha",
    "live_blob_provenance",
    "record_blob_provenance",
]
