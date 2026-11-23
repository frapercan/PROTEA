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
from protea_method._self_by_sequence import resolve_self_exclusion

from protea.core import denials
from protea.core.code_revision import code_revision, dependency_revisions
from protea.core.query_population import population_of

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
        # The repository commit does not identify the code. PROTEA pins six
        # sibling packages by git commit, and a node can hold the right tree
        # with a stale sibling whose version string did not move.
        "dependency_revisions": dependency_revisions(),
        "search_backend": p.search_backend,
        "metric": p.metric,
        "donor_policy": p.donor_policy.model_dump(),
        # Recorded unconditionally, including when false. False is the historical
        # behaviour and saying so is the point: without it, a receipt from before
        # the flag existed and a receipt from a run that deliberately allowed
        # self-retrieval are the same bytes, and the retriever's two levels
        # cannot be told apart in the record.
        **_self_exclusion(p),
        "aspect_separated_knn": p.aspect_separated_knn,
        "expand_votes_to_ancestors": p.expand_votes_to_ancestors,
        "batch_size": p.batch_size,
        # Recorded on every receipt: this method honours the bank's denials when
        # it predicts, so the evaluator does not have to (protea.core.denials).
        "denials": {"honoured": True, **denials.POLICY},
        "features": [f for f in _FEATURE_FLAGS if getattr(p, f, False)],
    }
    if getattr(p, "query_accessions", None):
        # A list handed to the run declares the population as a query set would;
        # the set keeps its digest and the job keeps the list.
        receipt["query_population"] = population_of(p.query_accessions)
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
        graft = _interpro_bp_graft()
        if graft is not None:
            receipt["interpro_bp_graft"] = graft
    return receipt


def _self_exclusion(p: PredictGOTermsPayload) -> dict[str, Any]:
    """Whether the query was excluded, always, and by which rule, when the payload named it.

    ``exclude_self_neighbour`` records whether the query WAS excluded, whichever
    field asked for it, so a run that names only the rule does not read as a
    self-vote. ``self_exclusion`` names who left with it: its twins too
    ("sequence") or nobody ("accession"). It is written only by runs whose payload
    named the rule, as ``faiss`` is, so a run that left it to the flag reads
    exactly as it always did.
    """
    rule = resolve_self_exclusion(p)
    block: dict[str, Any] = {"exclude_self_neighbour": rule != "none"}
    if getattr(p, "self_exclusion", None) is not None:
        block["self_exclusion"] = rule
    return block


def _interpro_bp_graft() -> dict[str, Any] | None:
    """The InterPro BP graft as the deploy sets it, or None when it is off.

    A deploy setting (``serve.interpro_bp_graft``), not a payload field, and it
    changes the stored set: it rescores the reranker's BP candidates and adds
    InterPro's own. Recorded only beside a reranker, the only score it grafts
    onto.
    """
    from protea.config.tuning import get_tuning

    serve = get_tuning().serve
    if not serve.interpro_bp_graft:
        return None
    return {"weight": serve.interpro_bp_graft_weight}
