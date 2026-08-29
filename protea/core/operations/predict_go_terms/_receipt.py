"""What a PredictionSet says about how it was produced.

A prediction set used to be stored with ``meta={}``. Its columns name the
inputs (which embedding config, which annotation set, which ontology, K,
the distance threshold) but not a single decision: not the search backend,
not the distance metric, not which evidence codes a donor needed to be
allowed to vote. Those lived only in the payload of the job that ran it,
and the job recorded no link back to what it produced, so the two could
not be joined at all.

The consequence was small until someone asked what a published number was
measuring. A reader looking at an evaluation could see a score and the
model that earned it, and could not see the regime it was earned under.
Two rows a few percent apart might differ in K, or in whether electronic
annotations were allowed to vote, and nothing on the surface said which.

``job_id`` is the load-bearing field. Everything else here is a
convenience copy that saves a join, but the job id is what makes a set
recoverable at all, including for parameters nobody thought to copy today.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from protea_contracts import PredictGOTermsPayload

from protea.core.code_revision import code_revision

#: Payload flags that turn on an extra feature block during prediction.
#: Recorded by name so a set can say what it computed without this module
#: having to grow a field per feature.
_FEATURE_FLAGS = (
    "compute_alignments",
    "compute_taxonomy",
    "compute_reranker_features",
    "compute_v6_features",
    "compute_lineage_features",
    "compute_classifier",
    "compute_self_prior",
    "compute_association",
    "compute_protst",
    "compute_ia",
)


def run_receipt(p: PredictGOTermsPayload, job_id: UUID) -> dict[str, Any]:
    """The record of how a prediction set was produced.

    Kept deliberately close to the payload rather than prettified: this is
    provenance, and a value that has been reworded on the way in cannot be
    compared against the payload it came from.
    """
    receipt: dict[str, Any] = {
        "job_id": str(job_id),
        # The revision the coordinator was running. Every batch reads it back
        # and refuses to write under a different one, which is the half that
        # matters: a revision recorded and never compared is how one node wrote
        # 193,303 rows of a foreign format into a set that reported success.
        "code_revision": code_revision(),
        "search_backend": p.search_backend,
        "metric": p.metric,
        "donor_policy": p.donor_policy.model_dump(),
        # Recorded unconditionally, including when false. False is the historical
        # behaviour and saying so is the point: without it, a receipt from before
        # the flag existed and a receipt from a run that deliberately allowed
        # self-retrieval are the same bytes, and the retriever's two levels
        # cannot be told apart in the record.
        "exclude_self_neighbour": getattr(p, "exclude_self_neighbour", False),
        "aspect_separated_knn": p.aspect_separated_knn,
        "expand_votes_to_ancestors": p.expand_votes_to_ancestors,
        "batch_size": p.batch_size,
        "features": [f for f in _FEATURE_FLAGS if getattr(p, f, False)],
    }
    if p.search_backend == "faiss":
        # Only meaningful under faiss, and misleading beside numpy, where
        # the search is exact and these knobs did nothing.
        receipt["faiss"] = {
            "index_type": p.faiss_index_type,
            "nlist": p.faiss_nlist,
            "nprobe": p.faiss_nprobe,
            "hnsw_m": p.faiss_hnsw_m,
            "hnsw_ef_search": p.faiss_hnsw_ef_search,
        }
    rerankers = {
        key: getattr(p, f"reranker_model_id{suffix}")
        for key, suffix in (("all", ""), ("nk", "_nk"), ("lk", "_lk"), ("pk", "_pk"))
        if getattr(p, f"reranker_model_id{suffix}", None)
    }
    if rerankers:
        receipt["rerankers"] = rerankers
    return receipt
