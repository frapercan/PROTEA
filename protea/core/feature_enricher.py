"""V6 feature enrichment for GO predictions.

The "v6" feature family is the set of 25 columns that the post-2026 lab
re-ranker consumes on top of the base KNN features. Computing them
requires three independent intermediates over the prediction batch:

* an Anc2Vec embedding pool covering every GO term seen as either a
  candidate or a voting-neighbor annotation,
* per-(query, aspect) neighbor centroids derived from that pool,
* per-(query, candidate) tax-voter counters built from the pair-features
  produced by the alignment / taxonomy pipeline.

A fourth intermediate, the PCA projection of query embeddings, is
optional — present only when the caller passes a fitted ``pca_state``.

The orchestrator :func:`enrich_v6_features` runs the four stages, then
walks the prediction list and merges each candidate's features in
place. The intermediates are exposed as module-private helpers so the
unit tests can exercise them in isolation if needed.

This module was extracted from ``predict_go_terms.py`` (Extract Class):
the seven functions below were originally instance methods on
``PredictGOTermsBatchOperation`` but used no instance state and only
ever ran in sequence — moving them out drops ~280 LOC from the batch
operation and makes the v6 pipeline independently testable.
"""

from __future__ import annotations

import uuid  # noqa: F401  # kept for the public type signatures
from typing import Any

import numpy as np
from sqlalchemy.orm import Session

from protea.core.anc2vec_embeddings import Anc2VecIndex
from protea.core.anc2vec_embeddings import get_index as get_anc2vec_index
from protea.core.domain.aspect import ASPECT_CODES as _ASPECTS
from protea.core.reranker import EMBEDDING_PCA_DIM
from protea.infrastructure.orm.models.annotation.go_term import GOTerm

_ANNOTATION_CHUNK_SIZE = 10_000

_TAX_CLOSE_RELATIONS = frozenset(
    {"same", "ancestor", "descendant", "child", "parent", "close"}
)

#: Feature columns the v6 enricher writes into each prediction dict.
#: Re-exported by :mod:`protea.core.operations.predict_go_terms` (it composes
#: them into the wider ``_STORE_FLOAT_KEYS`` set used by the bulk insert path).
NEW_V6_FEATURE_KEYS: tuple[str, ...] = (
    "anc2vec_neighbor_cos",
    "anc2vec_neighbor_maxcos",
    "anc2vec_has_emb",
    "anc2vec_query_known_cos",
    "anc2vec_query_known_maxcos",
    "anc2vec_query_known_count",
    "tax_voters_same_frac",
    "tax_voters_close_frac",
    "tax_voters_mean_common_ancestors",
    *(f"emb_pca_query_{i}" for i in range(EMBEDDING_PCA_DIM)),
)


# ---------------------------------------------------------------------------
# Stage 1 — collect GO term metadata
# ---------------------------------------------------------------------------


def _collect_gtids_in_play(
    predictions: list[dict[str, Any]],
    go_map_by_aspect: dict[str, dict[str, list[dict[str, Any]]]],
) -> set[int]:
    """Every go_term_id that appears either as a candidate in
    predictions or as an annotation of a voting neighbor — both are
    needed to compute neighbor Anc2Vec centroids.
    """
    gtids: set[int] = {int(pred["go_term_id"]) for pred in predictions}
    for go_map in go_map_by_aspect.values():
        for anns in go_map.values():
            for ann in anns:
                gtids.add(int(ann["go_term_id"]))
    return gtids


def _load_go_term_metadata(
    session: Session,
    go_term_ids: set[int],
) -> tuple[dict[int, str], dict[int, str]]:
    """Return ``(go_id_map, aspect_map)`` for the given ``GOTerm.id`` set.

    Both maps are keyed by the numeric ``go_term_id``. Values are the
    canonical ``GO:NNNNNNN`` string and the single-char aspect (``P``/
    ``F``/``C``), respectively. Chunked to stay within parameter limits.
    """
    go_id_map: dict[int, str] = {}
    aspect_map: dict[int, str] = {}
    if not go_term_ids:
        return go_id_map, aspect_map
    ids_list = list(go_term_ids)
    for i in range(0, len(ids_list), _ANNOTATION_CHUNK_SIZE):
        chunk = ids_list[i : i + _ANNOTATION_CHUNK_SIZE]
        rows = (
            session.query(GOTerm.id, GOTerm.go_id, GOTerm.aspect)
            .filter(GOTerm.id.in_(chunk))
            .all()
        )
        for gid, go_str, aspect in rows:
            go_id_map[gid] = go_str
            aspect_map[gid] = aspect or ""
    return go_id_map, aspect_map


# ---------------------------------------------------------------------------
# Stage 2 — Anc2Vec projection pool
# ---------------------------------------------------------------------------


def _build_anc2vec_pool(
    gtids_in_play: set[int],
    go_id_map: dict[int, str],
) -> tuple[dict[str, int], np.ndarray, np.ndarray]:
    """Materialise the Anc2Vec embedding matrix for every GO id seen.

    Returns ``(idx_of_go, all_norm, has_emb_mask)`` where ``all_norm``
    is the L2-normalised projection of every GO id (rows lacking an
    Anc2Vec vector are zeroed out and flagged in ``has_emb_mask``).
    """
    anc_idx: Anc2VecIndex = get_anc2vec_index()
    all_go_id_strs = {go_id_map[gid] for gid in gtids_in_play if gid in go_id_map}
    all_go_id_list = sorted(all_go_id_strs)
    idx_of_go: dict[str, int] = {g: i for i, g in enumerate(all_go_id_list)}
    all_emb = anc_idx.batch(all_go_id_list)
    raw_norms = np.linalg.norm(all_emb, axis=1)
    has_emb_mask = raw_norms > 0.0
    safe_norms = np.where(has_emb_mask, raw_norms, 1.0)[:, None]
    all_norm = (all_emb / safe_norms).astype(np.float32)
    all_norm[~has_emb_mask] = 0.0
    return idx_of_go, all_norm, has_emb_mask


# ---------------------------------------------------------------------------
# Stage 3 — neighbor centroids per (q_acc, aspect)
# ---------------------------------------------------------------------------


def _compute_neighbor_centroids(
    *,
    valid_accessions: list[str],
    neighbors_by_aspect: dict[str, list[list[tuple[str, float]]]],
    go_map_by_aspect: dict[str, dict[str, list[dict[str, Any]]]],
    go_id_map: dict[int, str],
    go_aspect_map: dict[int, str],
    idx_of_go: dict[str, int],
    all_norm: np.ndarray,
    has_emb_mask: np.ndarray,
) -> dict[tuple[str, str], tuple[np.ndarray | None, np.ndarray | None]]:
    """Per ``(q_acc, aspect)`` pair, compute the Anc2Vec centroid of
    the neighbor-side annotations. Returns ``(centroid_unit, nmat)``
    where ``nmat`` is the matrix of contributing neighbor vectors and
    ``centroid_unit`` is its L2-normalised mean.

    The aspect partition matches training: each centroid only mixes
    neighbor terms from the same GO aspect as the candidate.
    """
    info: dict[tuple[str, str], tuple[np.ndarray | None, np.ndarray | None]] = {}
    for aspect_key, nbs_all in neighbors_by_aspect.items():
        go_map = go_map_by_aspect.get(aspect_key, {})
        for q_idx, q_acc in enumerate(valid_accessions):
            if q_idx >= len(nbs_all):
                continue
            per_asp_rows: dict[str, list[int]] = {a: [] for a in _ASPECTS}
            per_asp_seen: dict[str, set[str]] = {a: set() for a in _ASPECTS}
            for ref_acc, _ in nbs_all[q_idx]:
                for ann in go_map.get(ref_acc, []):
                    gtid = int(ann["go_term_id"])
                    gid_str = go_id_map.get(gtid)
                    asp = go_aspect_map.get(gtid, "")
                    if not gid_str or asp not in per_asp_rows:
                        continue
                    if gid_str in per_asp_seen[asp]:
                        continue
                    per_asp_seen[asp].add(gid_str)
                    i = idx_of_go.get(gid_str)
                    if i is not None and has_emb_mask[i]:
                        per_asp_rows[asp].append(i)
            for asp, rows in per_asp_rows.items():
                if not rows:
                    info.setdefault((q_acc, asp), (None, None))
                    continue
                nmat = all_norm[rows]
                centroid = nmat.mean(axis=0)
                cn = float(np.linalg.norm(centroid))
                centroid_unit = (
                    (centroid / cn).astype(np.float32) if cn > 0.0 else None
                )
                # Aspect-separated mode may revisit the same aspect once
                # per KNN call, but per-aspect rows are disjoint across
                # aspect_keys — accumulate via setdefault.
                prev = info.get((q_acc, asp))
                if prev is None or prev == (None, None):
                    info[(q_acc, asp)] = (centroid_unit, nmat)
    return info


# ---------------------------------------------------------------------------
# Stage 4 — tax-voter counters
# ---------------------------------------------------------------------------


def _compute_tax_voter_counters(
    *,
    valid_accessions: list[str],
    neighbors_by_aspect: dict[str, list[list[tuple[str, float]]]],
    go_map_by_aspect: dict[str, dict[str, list[dict[str, Any]]]],
    pair_features: dict[tuple[str, str], dict[str, Any]],
    compute_taxonomy: bool,
) -> tuple[
    dict[str, dict[int, int]],
    dict[str, dict[int, int]],
    dict[str, dict[int, float]],
    dict[str, dict[int, int]],
    dict[str, dict[int, int]],
]:
    """Build the five per-(q_acc, gtid) counters that feed the
    tax_voters_* feature family.

    Returns ``(same_cnt, close_cnt, ca_sum, ca_n, vote_count_div)``.
    When ``compute_taxonomy`` is False, every dict comes back empty
    (the merge loop emits NaN for the corresponding columns).
    """
    same_cnt: dict[str, dict[int, int]] = {}
    close_cnt: dict[str, dict[int, int]] = {}
    ca_sum: dict[str, dict[int, float]] = {}
    ca_n: dict[str, dict[int, int]] = {}
    vc_div: dict[str, dict[int, int]] = {}
    if not compute_taxonomy:
        return same_cnt, close_cnt, ca_sum, ca_n, vc_div

    for aspect_key, nbs_all in neighbors_by_aspect.items():
        go_map = go_map_by_aspect.get(aspect_key, {})
        for q_idx, q_acc in enumerate(valid_accessions):
            if q_idx >= len(nbs_all):
                continue
            same_d = same_cnt.setdefault(q_acc, {})
            close_d = close_cnt.setdefault(q_acc, {})
            sum_d = ca_sum.setdefault(q_acc, {})
            n_d = ca_n.setdefault(q_acc, {})
            vc_d = vc_div.setdefault(q_acc, {})
            for ref_acc, _ in nbs_all[q_idx]:
                pf = pair_features.get((q_acc, ref_acc), {})
                rel = pf.get("taxonomic_relation") or ""
                ca = pf.get("taxonomic_common_ancestors")
                is_same = rel == "same"
                is_close = rel in _TAX_CLOSE_RELATIONS
                for ann in go_map.get(ref_acc, []):
                    gtid = int(ann["go_term_id"])
                    vc_d[gtid] = vc_d.get(gtid, 0) + 1
                    if is_same:
                        same_d[gtid] = same_d.get(gtid, 0) + 1
                    if is_close:
                        close_d[gtid] = close_d.get(gtid, 0) + 1
                    if isinstance(ca, int | float) and ca is not None:
                        sum_d[gtid] = sum_d.get(gtid, 0.0) + float(ca)
                        n_d[gtid] = n_d.get(gtid, 0) + 1
    return same_cnt, close_cnt, ca_sum, ca_n, vc_div


# ---------------------------------------------------------------------------
# Stage 5 — PCA query projection
# ---------------------------------------------------------------------------


def _compute_pca_projection(
    query_embeddings: np.ndarray,
    pca_state: tuple[np.ndarray, np.ndarray] | None,
) -> np.ndarray | None:
    """Project query embeddings into the PCA reference subspace.

    Returns ``None`` when the projection cannot be computed (no PCA
    state or no query embeddings); the merge loop emits NaN for the
    ``emb_pca_query_*`` columns in that case.
    """
    if pca_state is None or not query_embeddings.size:
        return None
    pca_mean, pca_components = pca_state
    return (
        (query_embeddings.astype(np.float32) - pca_mean) @ pca_components.T
    ).astype(np.float32)


# ---------------------------------------------------------------------------
# Public orchestrator
# ---------------------------------------------------------------------------


def enrich_v6_features(
    predictions: list[dict[str, Any]],
    *,
    session: Session,
    valid_accessions: list[str],
    query_embeddings: np.ndarray,
    neighbors_by_aspect: dict[str, list[list[tuple[str, float]]]],
    go_map_by_aspect: dict[str, dict[str, list[dict[str, Any]]]],
    pair_features: dict[tuple[str, str], dict[str, Any]],
    pca_state: tuple[np.ndarray, np.ndarray] | None,
    compute_taxonomy: bool,
) -> None:
    """Compute the 25 v6 features and merge them into each ``pred`` dict in place.

    ``neighbors_by_aspect`` / ``go_map_by_aspect`` may contain a single
    aspect key (``""``) in unified-KNN mode; the per-aspect groupings of
    candidate GO terms are resolved via the GO term aspect map.

    ``pair_features`` carries the ``taxonomic_relation`` and
    ``taxonomic_common_ancestors`` keys populated by ``compute_taxonomy``.
    When ``compute_taxonomy`` is ``False`` the tax_voters family stays NaN.

    Query-known Anc2Vec features are emitted as NaN / 0 at predict time —
    ``anc2vec_query_known_*`` is resolved at eval time from the split's
    pre-cutoff annotations (LK / PK only).

    The body is a five-stage pipeline (collect metadata → build Anc2Vec
    pool → neighbor centroids → tax-voter counters → PCA projection →
    merge); each stage is its own private helper.
    """
    if not predictions:
        return

    acc_to_idx = {acc: i for i, acc in enumerate(valid_accessions)}

    # ── 1. Collect GO term metadata ──────────────────────────────────
    gtids_in_play = _collect_gtids_in_play(predictions, go_map_by_aspect)
    go_id_map, go_aspect_map = _load_go_term_metadata(session, gtids_in_play)

    # ── 2. Anc2Vec projection pool ──────────────────────────────────
    idx_of_go, all_norm, has_emb_mask = _build_anc2vec_pool(gtids_in_play, go_id_map)

    # ── 3. Neighbor centroids per (q_acc, aspect) ───────────────────
    neighbor_info = _compute_neighbor_centroids(
        valid_accessions=valid_accessions,
        neighbors_by_aspect=neighbors_by_aspect,
        go_map_by_aspect=go_map_by_aspect,
        go_id_map=go_id_map,
        go_aspect_map=go_aspect_map,
        idx_of_go=idx_of_go,
        all_norm=all_norm,
        has_emb_mask=has_emb_mask,
    )

    # ── 4. Tax-voter counters per (q_acc, gtid) ──────────────────────
    tax_same_cnt, tax_close_cnt, tax_ca_sum, tax_ca_n, vote_count_div = (
        _compute_tax_voter_counters(
            valid_accessions=valid_accessions,
            neighbors_by_aspect=neighbors_by_aspect,
            go_map_by_aspect=go_map_by_aspect,
            pair_features=pair_features,
            compute_taxonomy=compute_taxonomy,
        )
    )

    # ── 5. PCA query projection ──────────────────────────────────────
    pca_query_proj = _compute_pca_projection(query_embeddings, pca_state)
    _nan_pca = [float("nan")] * EMBEDDING_PCA_DIM

    # ── 6. Merge per-row features ────────────────────────────────────
    for pred in predictions:
        q_acc = pred["protein_accession"]
        gtid = int(pred["go_term_id"])
        go_id = go_id_map.get(gtid)
        aspect = go_aspect_map.get(gtid, "")

        cand_i = idx_of_go.get(go_id, -1) if go_id else -1
        if cand_i >= 0 and has_emb_mask[cand_i]:
            cand_vec = all_norm[cand_i]
            centroid_unit, nmat = neighbor_info.get((q_acc, aspect), (None, None))
            anc_cos = (
                float(cand_vec @ centroid_unit)
                if centroid_unit is not None
                else float("nan")
            )
            anc_maxcos = (
                float((nmat @ cand_vec).max()) if nmat is not None else float("nan")
            )
            anc_has = 1.0
        else:
            anc_cos = float("nan")
            anc_maxcos = float("nan")
            anc_has = 0.0

        pred["anc2vec_neighbor_cos"] = anc_cos
        pred["anc2vec_neighbor_maxcos"] = anc_maxcos
        pred["anc2vec_has_emb"] = anc_has
        # Query-known is resolved at eval time from the split context.
        pred["anc2vec_query_known_cos"] = float("nan")
        pred["anc2vec_query_known_maxcos"] = float("nan")
        pred["anc2vec_query_known_count"] = 0.0

        if compute_taxonomy:
            vc_total = max(1, vote_count_div.get(q_acc, {}).get(gtid, 1))
            pred["tax_voters_same_frac"] = (
                tax_same_cnt.get(q_acc, {}).get(gtid, 0) / vc_total
            )
            pred["tax_voters_close_frac"] = (
                tax_close_cnt.get(q_acc, {}).get(gtid, 0) / vc_total
            )
            ca_n = tax_ca_n.get(q_acc, {}).get(gtid, 0)
            pred["tax_voters_mean_common_ancestors"] = (
                tax_ca_sum.get(q_acc, {}).get(gtid, 0.0) / max(1, ca_n)
                if ca_n > 0
                else float("nan")
            )
        else:
            pred["tax_voters_same_frac"] = float("nan")
            pred["tax_voters_close_frac"] = float("nan")
            pred["tax_voters_mean_common_ancestors"] = float("nan")

        q_idx = acc_to_idx.get(q_acc, -1)
        if pca_query_proj is not None and 0 <= q_idx < pca_query_proj.shape[0]:
            row = pca_query_proj[q_idx]
            for i in range(EMBEDDING_PCA_DIM):
                pred[f"emb_pca_query_{i}"] = float(row[i])
        else:
            for i in range(EMBEDDING_PCA_DIM):
                pred[f"emb_pca_query_{i}"] = _nan_pca[i]


def expand_predictions_to_ancestors(
    predictions: list[dict[str, Any]],
    *,
    parent_map: dict[str, set[str]] | dict[str, list[str]],
    k_limit: int,
    ia_weights: dict[str, float] | None = None,
    gt_pairs: set[tuple[str, str]] | None = None,
    label_column: str = "label",
    label_field_present: bool = False,
) -> list[dict[str, Any]]:
    """Expand each leaf prediction to its is_a / part_of ancestor closure.

    Mirrors the in-loop expansion in
    :func:`protea.core.operations.train_reranker._knn_transfer_and_label`,
    pulled out so :mod:`predict_go_terms` (live inference) and
    ``train_reranker`` (offline dataset generation) share a single canonical
    implementation. Without it the candidate sets diverge — the lab dump
    expanded to ancestors, live KNN didn't, and the v9/v10 boosters scored
    LK / PK candidates on a feature distribution they never saw at
    training time.

    The function processes predictions per (protein_accession, aspect)
    group: it adds the ancestor closure of each leaf go_id, merging into
    existing leaf records (bumping ``neighbor_vote_fraction`` /
    ``neighbor_min_distance``) when an ancestor is itself already a leaf
    candidate, and synthesizing new records (cloning the closest leaf and
    overriding ``go_id``) otherwise.

    Synthetic records inherit the leaf's per-pair features verbatim
    (alignment, taxonomy, anc2vec_*, emb_pca_*) — this is the same
    convention the train side used, so train and predict stay in lockstep.
    Recomputing those features for the ancestor would be the strictly
    correct semantics but is **not** what the booster was trained on.

    Parameters
    ----------
    predictions:
        Leaf prediction records, each with at least ``protein_accession``,
        ``aspect``, ``go_id``, ``distance``, ``neighbor_vote_fraction``,
        ``neighbor_min_distance``. Must be the unmodified output of the
        leaf record loop — both train_reranker and predict_go_terms emit
        this shape.
    parent_map:
        ``{child_go_id: {parent_go_id, ...}}`` for is_a / part_of edges.
        Caller responsible for loading from the active OntologySnapshot.
    k_limit:
        Same K used by the upstream KNN. Drives the ``w / k_limit``
        normalisation of inherited votes.
    ia_weights:
        Optional ``{go_id: ia_weight}`` for IA-weighted vote inheritance.
        ``w = ia_weights[ancestor] / ia_weights[leaf]`` when both present
        and non-zero; ``w = 1`` otherwise.
    gt_pairs / label_field_present:
        Train-time only. When ``label_field_present`` is True, ancestor
        records are labelled by membership of ``(protein, ancestor)`` in
        ``gt_pairs``. Predict callers leave both at default — the helper
        omits the label key entirely.
    label_column:
        Name of the label column on prediction dicts (train-only, defaults
        to ``"label"``).
    """
    if not predictions:
        return predictions

    # Coerce parent_map values to frozensets once (and only once) so the
    # closure walk doesn't keep paying conversion cost.
    pm: dict[str, frozenset[str]] = {
        c: frozenset(parents) for c, parents in (parent_map or {}).items()
    }
    closure: dict[str, frozenset[str]] = {}

    def _ancestors(gid: str) -> frozenset[str]:
        cached = closure.get(gid)
        if cached is not None:
            return cached
        seen: set[str] = set()
        stack = [gid]
        while stack:
            node = stack.pop()
            for parent in pm.get(node, ()):
                if parent not in seen:
                    seen.add(parent)
                    stack.append(parent)
        result = frozenset(seen)
        closure[gid] = result
        return result

    def _ia_weight(anc_gid: str, leaf_gid: str) -> float:
        if not ia_weights:
            return 1.0
        anc_w = float(ia_weights.get(anc_gid, 0.0))
        leaf_w = float(ia_weights.get(leaf_gid, 0.0))
        if leaf_w <= 0.0:
            return 1.0
        return anc_w / leaf_w

    k_limit_f = float(k_limit) if k_limit > 0 else 1.0

    # Group leaf records by (protein_accession, aspect). The grouping uses
    # the per-record ``aspect`` field (which the leaf-emit loops already
    # populate); for unified-KNN inputs this falls back to "" — the
    # expansion is still correct but everything ends up in one group.
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for rec in predictions:
        key = (rec.get("protein_accession", ""), rec.get("aspect", ""))
        groups.setdefault(key, []).append(rec)

    out: list[dict[str, Any]] = []
    for (q_acc, _aspect), recs in groups.items():
        leaf_by_gid: dict[str, dict[str, Any]] = {r["go_id"]: r for r in recs}
        synth: dict[str, dict[str, Any]] = {}
        for leaf_gid, leaf_rec in list(leaf_by_gid.items()):
            leaf_d = float(leaf_rec.get("distance", 1.0))
            for anc in _ancestors(leaf_gid):
                w = _ia_weight(anc, leaf_gid)
                if anc in leaf_by_gid:
                    leaf_anc = leaf_by_gid[anc]
                    leaf_anc["neighbor_vote_fraction"] = min(
                        1.0,
                        float(leaf_anc.get("neighbor_vote_fraction", 0.0))
                        + w / k_limit_f,
                    )
                    lmd = float(leaf_rec.get("neighbor_min_distance", leaf_d))
                    cur_md = float(leaf_anc.get("neighbor_min_distance", leaf_d))
                    if lmd < cur_md:
                        leaf_anc["neighbor_min_distance"] = lmd
                    continue
                entry = synth.get(anc)
                if entry is None or leaf_d < float(entry.get("distance", float("inf"))):
                    base = dict(leaf_rec)
                    base["go_id"] = anc
                    if label_field_present:
                        base[label_column] = (
                            1 if (gt_pairs and (q_acc, anc) in gt_pairs) else 0
                        )
                    prior_frac = (
                        float(entry["neighbor_vote_fraction"])
                        if entry is not None
                        else 0.0
                    )
                    base["neighbor_vote_fraction"] = min(1.0, prior_frac + w / k_limit_f)
                    synth[anc] = base
                else:
                    entry["neighbor_vote_fraction"] = min(
                        1.0,
                        float(entry["neighbor_vote_fraction"]) + w / k_limit_f,
                    )

        out.extend(leaf_by_gid.values())
        out.extend(synth.values())
    return out


def load_parent_map(session: Session, snapshot_id: uuid.UUID) -> dict[str, set[str]]:
    """``{child_go_id: {parent_go_id, ...}}`` for is_a + part_of edges in a
    given :class:`OntologySnapshot`. Used by the ancestor-expansion helper
    above; both the live ``predict_go_terms`` path and offline
    ``train_reranker`` should load it through this function so the closure
    they pass to :func:`expand_predictions_to_ancestors` is identical."""
    from sqlalchemy import text

    rows = session.execute(
        text(
            "SELECT c.go_id AS child, p.go_id AS parent "
            "FROM go_term_relationship r "
            "JOIN go_term c ON c.id = r.child_go_term_id "
            "JOIN go_term p ON p.id = r.parent_go_term_id "
            "WHERE r.ontology_snapshot_id = :snap_id "
            "AND r.relation_type IN ('is_a', 'part_of')"
        ),
        {"snap_id": snapshot_id},
    ).fetchall()
    parent_map: dict[str, set[str]] = {}
    for child, parent in rows:
        parent_map.setdefault(str(child), set()).add(str(parent))
    return parent_map


__all__ = [
    "NEW_V6_FEATURE_KEYS",
    "enrich_v6_features",
    "expand_predictions_to_ancestors",
    "load_parent_map",
]
